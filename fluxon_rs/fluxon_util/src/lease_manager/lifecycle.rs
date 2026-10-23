//! Lifecycle utilities for lease_manager:
//! - Debug helpers (register_by map, keepalive logs)
//! - Unified backend map + guard (AutoCleanMap-based)
//! - Per-TTL actor map (AutoCleanMap-based) and registration flows
//! - LeaseEntry Drop implementation
//! - register_lease_for_keepalive implementation

use std::collections::HashMap;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use anyhow::{Context, Result as AnyResult};
use etcd_client::Client;

use super::keepalive_actor::{
    self, ActorRegisterInvocation, EtcdState, LeaseActorMapKey, LeaseKeepaliveFailure,
    LeaseKeepaliveFailurePolicy, LeaseKeepaliveOperation, LeaseKey, OneTtlKeepAliveInner,
    ensure_inner_running,
};
use super::lease_backend_handle::{LeaseBackendHandle, LeaseBackendInner};
use super::lease_backend_uid::{KvKeepaliveLease, LeaseBackendUid, LeaseRegisterKind, LeaseType};
use super::lease_handle::{GeneralLease, LeaseEntry, LeaseEntryKind};
use crate::auto_clean_map::{AutoCleanMap, AutoCleanMapEntry};
use crate::etcd::{PooledEtcdClient, etcd_clients_pool};

const INITIAL_ETCD_KEEPALIVE_PROBE_RETRIES: usize = 5;
const INITIAL_ETCD_KEEPALIVE_PROBE_TOTAL_BUDGET_MS: u64 = 60_000;

// ---------- Debug Helpers: register_by / keepalive log ----------

// Use std::sync::Mutex here (not tokio::sync::Mutex). These debug helpers
// may be called while we are inside a Tokio runtime (e.g. from within
// Runtime::block_on), and tokio::sync::Mutex::blocking_lock() will panic in
// that situation. A plain std mutex is fine for these tiny critical sections
// and avoids entering any async blocking path.
fn reg_by_map() -> &'static std::sync::Mutex<HashMap<u64, String>> {
    static MAP: OnceLock<std::sync::Mutex<HashMap<u64, String>>> = OnceLock::new();
    MAP.get_or_init(|| std::sync::Mutex::new(HashMap::new()))
}

pub fn record_register_by(lease_id: u64, register_by: impl Into<String>) {
    let mut g = reg_by_map().lock().unwrap();
    g.insert(lease_id, register_by.into());
}

pub fn get_register_by(lease_id: u64) -> Option<String> {
    let g = reg_by_map().lock().unwrap();
    g.get(&lease_id).cloned()
}

pub fn debug_keepalive_log(lease_id: u64, note: impl AsRef<str>) {
    if let Some(by) = get_register_by(lease_id) {
        tracing::debug!(lease_id, by, msg = %note.as_ref(), "lease keepalive");
    } else {
        tracing::debug!(lease_id, msg = %note.as_ref(), "lease keepalive");
    }
}

/// Snapshot current active lease keepalive entries across all TTL buckets.
///
/// This is a diagnostics-only helper to aid tests and tooling to verify that
/// lease handles have been released properly. It does not introduce any new
/// control flow and does not mutate internal state.
///
/// Returned tuple fields:
/// - `ttl_seconds`: the TTL bucket this lease is registered under
/// - `backend_uid`: which backend this lease belongs to (Etcd or KvClient)
/// - `lease_id`: the numerical lease id
/// - `register_by`: optional human-readable label recorded at registration
///   time via `record_register_by()`; callers can use a convention like
///   "mpsc_*:chan_id=…" to attribute leases to a specific channel
pub fn snapshot_active_lease_debug() -> Vec<(i64, LeaseBackendUid, u64, Option<String>)> {
    // Iterate all TTL actors and flatten their registries.
    // AutoCleanMap::snapshot_map only reads strong entries; dropped
    // leases will not appear here even if an actor is still running
    // its final tick.
    let mut out = Vec::new();
    for (actor_key, inner) in actor_map().snapshot_map(|key, inner| (*key, inner.clone())) {
        let ttl_seconds = actor_key.ttl_seconds();
        let entries: Vec<(LeaseKey, ())> = inner.registry.snapshot_map(|k, _| (k.clone(), ())); // only need the key
        for (key, _) in entries.into_iter() {
            let backend = key.backend_uid().clone();
            let lease_id = key.lease_id();
            let label = get_register_by(lease_id);
            out.push((ttl_seconds, backend, lease_id, label));
        }
    }
    out
}

// ---------- Unified Backend Object Table (by LeaseBackendUid) ----------

fn backend_map() -> &'static AutoCleanMap<LeaseBackendUid, LeaseBackendInner> {
    static MAP: OnceLock<AutoCleanMap<LeaseBackendUid, LeaseBackendInner>> = OnceLock::new();
    MAP.get_or_init(|| AutoCleanMap::new())
}

/// Acquire a backend handle that carries the AutoCleanMapEntry guard.
pub fn acquire_backend_handle(
    uid: LeaseBackendUid,
    kv_keepalive: Option<KvKeepaliveLease>,
    etcd_client: Option<Client>,
    etcd_pool_entry: Option<PooledEtcdClient>,
    rt: tokio::runtime::Handle,
) -> LeaseBackendHandle {
    let entry: AutoCleanMapEntry<LeaseBackendUid, LeaseBackendInner> =
        backend_map().get_or_init(uid.clone(), || match &uid {
            LeaseBackendUid::KvClient {
                cluster,
                instance_key,
                ..
            } => {
                let keepalive = kv_keepalive.expect(
                    "kvclient backend acquire requires keepalive operation on first creation",
                );
                LeaseBackendInner::KvClient {
                    _cluster: cluster.clone(),
                    _instance_key: instance_key.clone(),
                    keepalive,
                    rt: rt.clone(),
                }
            }
            LeaseBackendUid::Etcd(_) => {
                let client =
                    etcd_client.expect("etcd backend acquire requires client on first creation");
                let endpoints = uid
                    .endpoints()
                    .expect("etcd uid must carry endpoints")
                    .to_vec();
                LeaseBackendInner::Etcd {
                    _endpoints: endpoints,
                    _pool_entry: etcd_pool_entry
                        .expect("etcd backend acquire requires pool entry on first creation"),
                    client,
                    states: AutoCleanMap::new(),
                    rt: rt.clone(),
                }
            }
        });
    LeaseBackendHandle::from_entry(entry)
}

fn acquire_existing_backend_handle(uid: &LeaseBackendUid) -> Option<LeaseBackendHandle> {
    backend_map()
        .get_existing(uid)
        .map(LeaseBackendHandle::from_entry)
}

/// Clone the client owned by a live registered etcd backend.
///
/// MPMC subchannels use this contract after their parent member lease has
/// registered the backend. Failing here exposes an ownership-ordering bug
/// instead of silently opening one connection per subchannel.
pub fn registered_etcd_client(uid: &LeaseBackendUid) -> AnyResult<Client> {
    if uid.kind() != LeaseType::Etcd {
        anyhow::bail!("registered_etcd_client requires an Etcd backend uid");
    }
    acquire_existing_backend_handle(uid)
        .and_then(|handle| handle.etcd_client())
        .ok_or_else(|| {
            anyhow::anyhow!(
                "etcd backend is not registered for endpoints {:?}",
                uid.endpoints().unwrap_or_default()
            )
        })
}

// ---------- Per-TTL Actor Map & Registration Helpers ----------

pub(crate) type OnKeepalive = KvKeepaliveLease;

fn register_periodic_keepalive(
    actor_guard: &AutoCleanMapEntry<LeaseActorMapKey, Arc<OneTtlKeepAliveInner>>,
    key: &LeaseKey,
    handle: LeaseBackendHandle,
) -> super::keepalive_actor::LeaseKeepaliveRegistration<LeaseKey> {
    let operation_handle = handle.clone();
    let operation: LeaseKeepaliveOperation<LeaseKey> = Arc::new(move |key| {
        let handle = operation_handle.clone();
        Box::pin(async move {
            handle
                .keepalive(key.lease_id())
                .await
                .map_err(|err| format!("{err:?}"))
        })
    });
    let lease_id = key.lease_id();
    let on_failure = Arc::new(move |failure: LeaseKeepaliveFailure| {
        keepalive_actor::log_keepalive_error_rate_limited(lease_id, || match failure {
            LeaseKeepaliveFailure::Operation(err) => tracing::error!(
                lease_id,
                error = %err,
                "lease keepalive operation failed; retrying this lease"
            ),
            LeaseKeepaliveFailure::Timeout { timeout } => tracing::error!(
                lease_id,
                timeout_ms = timeout.as_millis(),
                "lease keepalive operation timed out; retrying this lease"
            ),
        });
    });
    actor_guard
        .actor
        .register_replacing(
            key.clone(),
            operation,
            LeaseKeepaliveFailurePolicy::RetryAfter(keepalive_actor::MQ_KEEPALIVE_RETRY_DELAY),
            on_failure,
        )
        .expect("new MQ lease registry entry must own a unique keepalive registration")
}

fn actor_map() -> &'static AutoCleanMap<LeaseActorMapKey, Arc<OneTtlKeepAliveInner>> {
    static MAP: OnceLock<AutoCleanMap<LeaseActorMapKey, Arc<OneTtlKeepAliveInner>>> =
        OnceLock::new();
    MAP.get_or_init(|| AutoCleanMap::new())
}

/// Register a lease entry into `inner.registry`.
///
/// KvClient registration does not run an immediate keepalive here:
/// - the validated branch relies on its caller-owned control-plane contract;
/// - the regular branch probes synchronously in `register_lease_for_keepalive`;
/// - the actor only drives later TTL-cadence keepalives.
pub(crate) fn actor_register_entry(
    actor_guard: &AutoCleanMapEntry<LeaseActorMapKey, Arc<OneTtlKeepAliveInner>>,
    key: LeaseKey,
    inv: &ActorRegisterInvocation,
    rt: tokio::runtime::Handle,
) -> AutoCleanMapEntry<LeaseKey, LeaseEntry> {
    match inv {
        ActorRegisterInvocation::KvClient { keepalive, .. } => {
            let registry = &(**actor_guard).registry;
            let (entry, created) = registry.get_or_init_with(key.clone(), || {
                let handle = acquire_backend_handle(
                    key.backend_uid().clone(),
                    Some(keepalive.clone()),
                    None,
                    None,
                    rt.clone(),
                );
                let keepalive_registration =
                    register_periodic_keepalive(actor_guard, &key, handle.clone());
                LeaseEntry {
                    _keepalive_registration: keepalive_registration,
                    kind: LeaseEntryKind::KvClient { handle },
                    _actor_guard: actor_guard.clone(),
                    key: key.clone(),
                    _etcd_state_guard: None,
                }
            });
            if !created {
                tracing::debug!(
                    "reuse KvClient lease registration: backend={:?} lease_id={}",
                    key.backend_uid(),
                    key.lease_id()
                );
            }
            entry
        }
        ActorRegisterInvocation::Etcd { client } => {
            let registry = &(**actor_guard).registry;
            let (entry, created) = registry.get_or_init_with(key.clone(), || {
                let endpoints = key
                    .backend_uid()
                    .endpoints()
                    .expect("etcd backend uid must carry endpoints")
                    .to_vec();
                let handle = acquire_backend_handle(
                    key.backend_uid().clone(),
                    None,
                    Some(client.clone()),
                    Some(etcd_clients_pool().acquire(endpoints)),
                    rt.clone(),
                );
                let lid = key.lease_id();
                let state_guard = handle.ensure_etcd_state(lid, || {
                    Arc::new(tokio::sync::Mutex::new(EtcdState {
                        client: client.clone(),
                        lease_id: lid as i64,
                        keeper: None,
                        stream: None,
                        last_stage: "init",
                    }))
                });
                let keepalive_registration =
                    register_periodic_keepalive(actor_guard, &key, handle.clone());
                LeaseEntry {
                    _keepalive_registration: keepalive_registration,
                    kind: LeaseEntryKind::Etcd { handle },
                    _actor_guard: actor_guard.clone(),
                    key: key.clone(),
                    _etcd_state_guard: Some(state_guard),
                }
            });
            if !created {
                tracing::debug!(
                    "reuse Etcd lease registration: backend={:?} lease_id={}",
                    key.backend_uid(),
                    key.lease_id()
                );
            }
            entry
        }
    }
}

/// Ensure an actor exists for `ttl_seconds` and register the lease entry.
pub(crate) fn actor_get_or_spawn_and_register(
    ttl_seconds: i64,
    key: LeaseKey,
    inv: &ActorRegisterInvocation,
    rt: tokio::runtime::Handle,
) -> AutoCleanMapEntry<LeaseKey, LeaseEntry> {
    if let ActorRegisterInvocation::KvClient {
        label: Some(lbl), ..
    } = inv
    {
        record_register_by(key.lease_id(), lbl.clone());
    }

    let actor_key = LeaseActorMapKey::new(ttl_seconds, rt.id());
    let (actor_entry, _created) = actor_map().get_or_init_with(actor_key, || {
        Arc::new(OneTtlKeepAliveInner::new(ttl_seconds))
    });

    let entry = actor_register_entry(&actor_entry, key.clone(), inv, rt.clone());
    ensure_inner_running(rt, (*actor_entry).clone());
    entry
}

// ---------- LeaseEntry Drop (centralized lifecycle cleanup) ----------
// The user-facing GeneralLease holds an AutoCleanMapEntry guard. Registrations
// for the same key share one LeaseEntry, so the value is dropped only after the
// last user guard is released. Its `_keepalive_registration` field is declared
// first and therefore unregisters the exact actor generation before backend and
// actor-map guards are released. An already-polled keepalive future may finish,
// but its generation check prevents it from rescheduling a removed lease.
// Semantic owners still delete their own metadata during graceful close; lease
// TTL remains the crash-cleanup fallback.
impl Drop for LeaseEntry {
    fn drop(&mut self) {
        let lease_id = self.key.lease_id();
        match &self.kind {
            LeaseEntryKind::KvClient { .. } => {
                debug_keepalive_log(lease_id, "kvclient lease unregistered");
            }
            LeaseEntryKind::Etcd { .. } => {
                debug_keepalive_log(lease_id, "etcd lease unregistered");
            }
        }
    }
}

// ---------- LeaseManager facade helpers ----------

pub async fn register_lease_for_keepalive(
    backend_uid: LeaseBackendUid,
    ttl_seconds: i64,
    lease_id: u64,
    kind: LeaseRegisterKind,
    rt: tokio::runtime::Handle,
) -> AnyResult<GeneralLease> {
    let skip_initial_etcd_probe = matches!(&kind, LeaseRegisterKind::EtcdValidated);
    let skip_initial_kvclient_probe = matches!(&kind, LeaseRegisterKind::KvClientValidated { .. });
    match kind {
        LeaseRegisterKind::Etcd | LeaseRegisterKind::EtcdValidated => match backend_uid.kind() {
            LeaseType::Etcd => {
                if get_register_by(lease_id).is_none() {
                    record_register_by(lease_id, format!("{:?},ttl={}", &backend_uid, ttl_seconds));
                }
                let endpoints = backend_uid
                    .endpoints()
                    .expect("etcd backend must carry endpoints");
                let backend_handle = match acquire_existing_backend_handle(&backend_uid) {
                    Some(handle) => handle,
                    None => {
                        let pool_entry = etcd_clients_pool().acquire(endpoints.to_vec());
                        let client = pool_entry.client().await.with_context(|| {
                            format!("failed to connect etcd for endpoints {:?}", endpoints)
                        })?;
                        acquire_backend_handle(
                            backend_uid.clone(),
                            None,
                            Some(client),
                            Some(pool_entry),
                            rt.clone(),
                        )
                    }
                };
                let client = backend_handle
                    .etcd_client()
                    .expect("etcd backend handle must contain an etcd client");
                let shared_state_guard = backend_handle.ensure_etcd_state(lease_id, || {
                    Arc::new(tokio::sync::Mutex::new(EtcdState {
                        client: client.clone(),
                        lease_id: lease_id as i64,
                        keeper: None,
                        stream: None,
                        last_stage: "init",
                    }))
                });

                if skip_initial_etcd_probe {
                    tracing::debug!(
                        lease_id,
                        ttl_seconds,
                        "skip initial etcd keepalive probe for caller-validated lease"
                    );
                } else {
                    // Fail fast: validate the lease id is alive on the target etcd cluster.
                    // We assume keepalive is always expected to work; if it does not, surfacing
                    // an error here is preferable to letting later writes fail with "lease not found".
                    // Etcd RPCs normally return on their transport deadline. This timeout is
                    // one backstop for the complete retry loop; resetting it per attempt would
                    // silently turn the 60-second registration contract into five minutes.
                    let total_budget =
                        Duration::from_millis(INITIAL_ETCD_KEEPALIVE_PROBE_TOTAL_BUDGET_MS);
                    let mut current_attempt = 0;
                    let probe_result = tokio::time::timeout(total_budget, async {
                        let mut last_probe_err: Option<anyhow::Error> = None;
                        for attempt in 1..=INITIAL_ETCD_KEEPALIVE_PROBE_RETRIES {
                            current_attempt = attempt;
                            let mut st = shared_state_guard.lock().await;
                            match st.keepalive_once().await {
                                Ok(()) => {
                                    drop(st);
                                    if attempt > 1 {
                                        tracing::warn!(
                                            lease_id,
                                            attempt,
                                            total = INITIAL_ETCD_KEEPALIVE_PROBE_RETRIES,
                                            total_budget_ms =
                                                INITIAL_ETCD_KEEPALIVE_PROBE_TOTAL_BUDGET_MS,
                                            "initial etcd keepalive probe succeeded after retry"
                                        );
                                    }
                                    last_probe_err = None;
                                    break;
                                }
                                Err(err) => {
                                    let last_stage = st.last_stage();
                                    st.reset_stream();
                                    drop(st);
                                    tracing::warn!(
                                        lease_id,
                                        attempt,
                                        total = INITIAL_ETCD_KEEPALIVE_PROBE_RETRIES,
                                        total_budget_ms =
                                            INITIAL_ETCD_KEEPALIVE_PROBE_TOTAL_BUDGET_MS,
                                        stage = last_stage,
                                        "initial etcd keepalive probe failed, will {}: {:?}",
                                        if attempt < INITIAL_ETCD_KEEPALIVE_PROBE_RETRIES {
                                            "retry"
                                        } else {
                                            "stop"
                                        },
                                        err
                                    );
                                    last_probe_err = Some(err.context(format!(
                                        "initial etcd keepalive probe failed for lease_id={} attempt={}/{}",
                                        lease_id, attempt, INITIAL_ETCD_KEEPALIVE_PROBE_RETRIES
                                    )));
                                }
                            }
                        }
                        last_probe_err
                    })
                    .await;

                    match probe_result {
                        Ok(None) => {}
                        Ok(Some(err)) => return Err(err),
                        Err(_) => {
                            let last_stage = match shared_state_guard.try_lock() {
                                Ok(mut st) => {
                                    let stage = st.last_stage();
                                    st.reset_stream();
                                    stage
                                }
                                Err(_) => "state_lock_busy",
                            };
                            return Err(anyhow::anyhow!(
                                "initial etcd keepalive probe exceeded total budget for lease_id={} attempt={}/{} total_budget_ms={} stage={}",
                                lease_id,
                                current_attempt,
                                INITIAL_ETCD_KEEPALIVE_PROBE_RETRIES,
                                INITIAL_ETCD_KEEPALIVE_PROBE_TOTAL_BUDGET_MS,
                                last_stage
                            ));
                        }
                    }
                }

                let entry = keepalive_actor::actor_register_lease(
                    backend_uid.clone(),
                    lease_id,
                    ttl_seconds,
                    ActorRegisterInvocation::Etcd { client },
                    rt.clone(),
                );
                Ok(GeneralLease::Etcd {
                    id: lease_id,
                    backend_uid,
                    entry,
                })
            }
            LeaseType::KvClient => {
                let cluster = backend_uid
                    .cluster()
                    .expect("kvclient backend missing cluster");
                anyhow::bail!(
                    "LeaseRegisterKind::Etcd requires Etcd backend uid, got KvClient({})",
                    cluster
                );
            }
        },
        LeaseRegisterKind::KvClient { register_by }
        | LeaseRegisterKind::KvClientValidated { register_by } => match backend_uid.kind() {
            LeaseType::KvClient => {
                record_register_by(lease_id, register_by.clone());
                let keepalive = backend_uid.kv_keepalive().ok_or_else(|| {
                    anyhow::anyhow!(
                        "Fluxon KV keepalive operation missing from KvClient lease backend"
                    )
                })?;
                if skip_initial_kvclient_probe {
                    tracing::debug!(
                        lease_id,
                        ttl_seconds,
                        "skip initial kvclient keepalive for caller-validated lease"
                    );
                } else {
                    // Validate a regular registration synchronously. Retrying the
                    // same operation handles transient transport jitter without
                    // introducing another fallback path.
                    const INITIAL_KVCLIENT_KEEPALIVE_RETRIES: usize = 3;
                    let mut last_err: Option<anyhow::Error> = None;
                    for attempt in 1..=INITIAL_KVCLIENT_KEEPALIVE_RETRIES {
                        match (keepalive)(lease_id).await {
                            Ok(()) => {
                                if attempt > 1 {
                                    tracing::debug!(
                                        lease_id,
                                        attempt,
                                        total = INITIAL_KVCLIENT_KEEPALIVE_RETRIES,
                                        "initial kvclient keepalive succeeded after retry"
                                    );
                                }
                                last_err = None;
                                break;
                            }
                            Err(err) => {
                                tracing::warn!(
                                    lease_id,
                                    attempt,
                                    total = INITIAL_KVCLIENT_KEEPALIVE_RETRIES,
                                    "initial kvclient keepalive failed, will {}: {:?}",
                                    if attempt < INITIAL_KVCLIENT_KEEPALIVE_RETRIES {
                                        "retry"
                                    } else {
                                        "stop"
                                    },
                                    err
                                );
                                last_err = Some(err);
                            }
                        }

                        if last_err.is_none() {
                            break;
                        }
                    }
                    if let Some(err) = last_err {
                        anyhow::bail!(
                            "initial kvclient keepalive failed for lease_id={} after {} attempts: {:?}",
                            lease_id,
                            INITIAL_KVCLIENT_KEEPALIVE_RETRIES,
                            err
                        );
                    }
                }
                let entry = keepalive_actor::actor_register_lease(
                    backend_uid.clone(),
                    lease_id,
                    ttl_seconds,
                    ActorRegisterInvocation::KvClient {
                        keepalive,
                        label: Some(register_by),
                    },
                    rt,
                );
                Ok(GeneralLease::KvClient {
                    id: lease_id,
                    backend_uid,
                    entry,
                })
            }
            LeaseType::Etcd => {
                anyhow::bail!("LeaseRegisterKind::KvClient requires KvClient backend uid");
            }
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    static NEXT_TEST_ID: AtomicU64 = AtomicU64::new(9_000_000);

    #[test]
    fn new_actor_spawn_observes_first_registered_lease() {
        let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
        let id = NEXT_TEST_ID.fetch_add(1, Ordering::SeqCst);
        let ttl_seconds = 120_000 + id as i64;
        let backend_uid = LeaseBackendUid::kv_client(
            format!("lifecycle_test_cluster_{id}"),
            format!("lifecycle_test_client_{id}"),
            Arc::new(|_| Box::pin(async { Ok(1) })),
            Arc::new(|_| Box::pin(async { Ok(()) })),
        );
        let key = LeaseKey::new(backend_uid.clone(), id);
        let inv = ActorRegisterInvocation::KvClient {
            keepalive: backend_uid.kv_keepalive().expect("kv keepalive operation"),
            label: None,
        };
        let entry = actor_get_or_spawn_and_register(ttl_seconds, key, &inv, rt.handle().clone());

        assert_eq!(entry._actor_guard.actor.registered_lease_count(), 1);
    }

    #[test]
    fn caller_validated_kvclient_registration_skips_synchronous_probe() {
        let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
        let id = NEXT_TEST_ID.fetch_add(2, Ordering::SeqCst);
        let keepalive_calls = Arc::new(AtomicU64::new(0));
        let calls_from_operation = keepalive_calls.clone();
        let backend_uid = LeaseBackendUid::kv_client(
            format!("validated_kvclient_test_cluster_{id}"),
            format!("validated_kvclient_test_client_{id}"),
            Arc::new(|_| Box::pin(async { Ok(1) })),
            Arc::new(move |_| {
                calls_from_operation.fetch_add(1, Ordering::SeqCst);
                Box::pin(async { Ok(()) })
            }),
        );

        rt.block_on(async {
            let regular = register_lease_for_keepalive(
                backend_uid.clone(),
                120_000,
                id,
                LeaseRegisterKind::KvClient {
                    register_by: format!("regular_{id}"),
                },
                rt.handle().clone(),
            )
            .await
            .expect("regular registration");
            assert_eq!(keepalive_calls.load(Ordering::SeqCst), 1);

            let validated = register_lease_for_keepalive(
                backend_uid,
                120_000,
                id + 1,
                LeaseRegisterKind::KvClientValidated {
                    register_by: format!("validated_{}", id + 1),
                },
                rt.handle().clone(),
            )
            .await
            .expect("caller-validated registration");
            assert_eq!(
                keepalive_calls.load(Ordering::SeqCst),
                1,
                "caller-validated registration must not run a duplicate synchronous keepalive"
            );

            drop(validated);
            drop(regular);
        });
    }

    #[test]
    fn pooled_etcd_registration_exposes_parent_client_immediately() {
        let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
        let id = NEXT_TEST_ID.fetch_add(1, Ordering::SeqCst);
        let endpoints = vec![format!("http://127.0.0.1:{}", 10_000 + id % 50_000)];
        let backend_uid = LeaseBackendUid::etcd_from(endpoints.clone());

        rt.block_on(async {
            let lease = register_lease_for_keepalive(
                backend_uid.clone(),
                120_000,
                id,
                LeaseRegisterKind::EtcdValidated,
                rt.handle().clone(),
            )
            .await
            .expect("caller-validated pooled etcd registration");

            registered_etcd_client(&backend_uid)
                .expect("MPMC subchannels must synchronously reuse the parent etcd client");

            let expected_pool_entry = etcd_clients_pool().acquire(endpoints);
            let backend_handle = acquire_existing_backend_handle(&backend_uid)
                .expect("registered etcd backend handle");
            match &*backend_handle.entry {
                LeaseBackendInner::Etcd { _pool_entry, .. } => assert!(
                    _pool_entry.shares_entry_with(&expected_pool_entry),
                    "lease backend must retain the connected pool entry"
                ),
                LeaseBackendInner::KvClient { .. } => panic!("expected etcd backend"),
            }

            drop(lease);
        });
    }
}
