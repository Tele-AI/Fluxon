//! Lifecycle utilities for lease_manager:
//! - Debug helpers (register_by map, keepalive logs)
//! - Unified backend map + guard (AutoCleanMap-based)
//! - Per-TTL actor map (AutoCleanMap-based) and registration flows
//! - LeaseEntry Drop implementation
//! - register_lease_for_keepalive implementation

use std::collections::HashMap;
use std::sync::{Arc, OnceLock};
use std::time::Duration;
use tokio::sync::Mutex;

use anyhow::Result as AnyResult;

use super::keepalive_actor::{
    self, ActorRegisterInvocation, EtcdState, LeaseKey, OneTtlKeepAliveInner, ensure_inner_running,
};
use super::lease_backend_handle::{LeaseBackendHandle, LeaseBackendInner};
use super::lease_backend_uid::{LeaseBackendUid, LeaseRegisterKind, LeaseType};
use super::lease_handle::{GeneralLease, LeaseEntry, LeaseEntryKind};
use crate::auto_clean_map::{AutoCleanMap, AutoCleanMapEntry};
use crate::etcd::ManagedEtcdClient;

const INITIAL_ETCD_KEEPALIVE_PROBE_RETRIES: usize = 5;
const INITIAL_ETCD_KEEPALIVE_PROBE_BUDGET_MS: u64 = 60_000;

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
    for (ttl, inner) in actor_map().snapshot_map(|ttl, inner| (*ttl, inner.clone())) {
        let ttl_seconds = ttl;
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
    kv_cb: Option<Arc<dyn Fn(u64) -> AnyResult<()> + Send + Sync + 'static>>,
    etcd_client: Option<ManagedEtcdClient>,
    rt: tokio::runtime::Handle,
) -> LeaseBackendHandle {
    let entry: AutoCleanMapEntry<LeaseBackendUid, LeaseBackendInner> =
        backend_map().get_or_init(uid.clone(), || match &uid {
            LeaseBackendUid::KvClientWithCallbacks { cluster, .. } => {
                let cb = kv_cb.expect(
                    "kvclient backend acquire requires keepalive callback on first creation",
                );
                LeaseBackendInner::KvClient {
                    _cluster: cluster.clone(),
                    keepalive_cb: cb,
                    rt: rt.clone(),
                }
            }
            LeaseBackendUid::Etcd(_) => {
                let client =
                    etcd_client.expect("etcd backend acquire requires client on first creation");
                LeaseBackendInner::Etcd {
                    client,
                    states: AutoCleanMap::new(),
                    rt: rt.clone(),
                }
            }
        });
    LeaseBackendHandle::from_entry(entry)
}

// get_handle() removed: no external callers; backend acquisition flows through guards.

// ---------- Per-TTL Actor Map & Registration Helpers ----------

// Rust-side keepalive callback type for KvClient entries.
pub(crate) type OnKeepalive = Arc<dyn Fn(u64) -> AnyResult<()> + Send + Sync + 'static>;

fn actor_map() -> &'static AutoCleanMap<i64, Arc<OneTtlKeepAliveInner>> {
    static MAP: OnceLock<AutoCleanMap<i64, Arc<OneTtlKeepAliveInner>>> = OnceLock::new();
    MAP.get_or_init(|| AutoCleanMap::new())
}

/// Register a lease entry into `inner.registry`.
///
/// 注意：KvClient 路径下不再在这里做额外的“立即 keepalive”：
/// - 第一轮 keepalive 已在 `register_lease_for_keepalive` 中通过
///   `spawn_blocking` 同步执行，并将错误显式返回给调用方；
/// - actor 仅负责在后续 tick 中按 TTL 周期驱动 keepalive，避免出现
///   “入口和 actor 各自做一次 probe” 的双重语义。
pub(crate) fn actor_register_entry(
    actor_guard: &AutoCleanMapEntry<i64, Arc<OneTtlKeepAliveInner>>,
    key: LeaseKey,
    inv: &ActorRegisterInvocation,
    rt: tokio::runtime::Handle,
) -> AutoCleanMapEntry<LeaseKey, LeaseEntry> {
    match inv {
        ActorRegisterInvocation::KvClient { cb, .. } => {
            let registry = &(**actor_guard).registry;
            let (entry, created) = registry.get_or_init_with(key.clone(), || {
                let handle = acquire_backend_handle(
                    key.backend_uid().clone(),
                    Some(cb.clone()),
                    None,
                    rt.clone(),
                );
                LeaseEntry {
                    kind: LeaseEntryKind::KvClient { handle },
                    _actor_guard: actor_guard.clone(),
                    key: key.clone(),
                    _etcd_state_guard: None,
                }
            });
            if !created {
                tracing::warn!(
                    "duplicate KvClient lease registration ignored: backend={:?} lease_id={}",
                    key.backend_uid(),
                    key.lease_id()
                );
            }
            entry
        }
        ActorRegisterInvocation::Etcd {
            client,
            revoke_on_drop,
        } => {
            let registry = &(**actor_guard).registry;
            let (entry, created) = registry.get_or_init_with(key.clone(), || {
                let handle = acquire_backend_handle(
                    key.backend_uid().clone(),
                    None,
                    Some(client.clone()),
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
                LeaseEntry {
                    kind: LeaseEntryKind::Etcd {
                        handle,
                        revoke_on_drop: *revoke_on_drop,
                    },
                    _actor_guard: actor_guard.clone(),
                    key: key.clone(),
                    _etcd_state_guard: Some(state_guard),
                }
            });
            if !created {
                tracing::warn!(
                    "duplicate Etcd lease registration ignored: backend={:?} lease_id={}",
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
    spawn_cb: impl FnOnce(Arc<OneTtlKeepAliveInner>),
    rt: tokio::runtime::Handle,
) -> AutoCleanMapEntry<LeaseKey, LeaseEntry> {
    if let ActorRegisterInvocation::KvClient {
        label: Some(lbl), ..
    } = inv
    {
        record_register_by(key.lease_id(), lbl.clone());
    }

    let (actor_entry, created) = actor_map().get_or_init_with(ttl_seconds, || {
        let inner = Arc::new(OneTtlKeepAliveInner {
            ttl_seconds,
            registry: AutoCleanMap::new(),
            running_state: Mutex::new(false),
        });
        spawn_cb(inner.clone());
        inner
    });

    let entry = actor_register_entry(&actor_entry, key.clone(), inv, rt.clone());
    // If the actor existed previously but might be exiting, ensure it is running.
    if !created {
        let inner = (*actor_entry).clone();
        let rth = rt.clone();
        rt.spawn(async move {
            ensure_inner_running(rth, inner).await;
        });
    }
    entry
}

// ---------- LeaseEntry Drop (centralized lifecycle cleanup) ----------
// Why no interaction with the actor is needed on Drop:
// - The registry is an AutoCleanMap<LeaseKey, LeaseEntry>. The user-facing
//   GeneralLease holds an AutoCleanMapEntry<LeaseKey, LeaseEntry> guard. When
//   that guard is dropped, the map entry is removed and the value (LeaseEntry)
//   is dropped immediately.
// - The actor loop drives keepalives by taking a snapshot of the registry each
//   tick via snapshot_filter_map. Once an entry is removed, future snapshots no
//   longer include it, so there is no need to send an explicit "unregister"
//   message to the actor.
// - Concurrency window: if a snapshot containing this entry has already been
//   taken for the current tick while Drop happens, the snapshot holds clones
//   (e.g., Arc<EtcdState>). That snapshot may perform one last keepalive for
//   this tick and then release naturally; the next tick will not see the entry.
//   This one-last-tick behavior is benign and has no side effects beyond the
//   regular cadence (we do not perform any keepalive during Drop).
// - Therefore, Drop only performs local cleanup/logging and, for Etcd when
//   revoke_on_drop is true, triggers a one-shot revoke; keepalive stopping is
//   achieved by the entry removal itself.
impl Drop for LeaseEntry {
    fn drop(&mut self) {
        let lease_id = self.key.lease_id();
        match &self.kind {
            LeaseEntryKind::KvClient { .. } => {
                debug_keepalive_log(lease_id, "kvclient lease unregistered");
            }
            LeaseEntryKind::Etcd {
                handle,
                revoke_on_drop,
                ..
            } => {
                if *revoke_on_drop {
                    if let Some(client) = handle.managed_etcd_client() {
                        let lid = lease_id as i64;
                        // Use the runtime handle carried by the backend handle (LeaseBackendInner)
                        let rt = handle.runtime_handle();
                        rt.spawn(async move {
                            let mut cli = match client.client().await {
                                Ok(client) => client,
                                Err(e) => {
                                    tracing::warn!(
                                        "failed to acquire etcd client to revoke lease_id={} on drop: {:?}",
                                        lid,
                                        e
                                    );
                                    return;
                                }
                            };
                            if let Err(e) = cli.lease_revoke(lid).await {
                                tracing::warn!(
                                    "failed to revoke lease_id={} on drop: {:?}",
                                    lid,
                                    e
                                );
                            } else {
                                tracing::debug!("revoked lease_id={} on drop", lid);
                            }
                        });
                    } else {
                        tracing::warn!(
                            lease_id,
                            "etcd revoke_on_drop: missing managed etcd client in backend handle"
                        );
                    }
                }
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
    match kind {
        LeaseRegisterKind::Etcd { revoke_on_drop } => match backend_uid.kind() {
            LeaseType::Etcd => {
                record_register_by(lease_id, format!("{:?},ttl={}", &backend_uid, ttl_seconds));
                let endpoints = backend_uid
                    .etcd_endpoint_set()
                    .expect("etcd backend must carry endpoints")
                    .clone();
                let client = ManagedEtcdClient::acquire(endpoints);

                let backend_handle = acquire_backend_handle(
                    backend_uid.clone(),
                    None,
                    Some(client.clone()),
                    rt.clone(),
                );
                let shared_state_guard = backend_handle.ensure_etcd_state(lease_id, || {
                    Arc::new(tokio::sync::Mutex::new(EtcdState {
                        client: client.clone(),
                        lease_id: lease_id as i64,
                        keeper: None,
                        stream: None,
                        last_stage: "init",
                    }))
                });

                // Fail fast: validate the lease id is alive on the target etcd cluster.
                // We assume keepalive is always expected to work; if it does not, surfacing
                // an error here is preferable to letting later writes fail with "lease not found".
                let mut last_probe_err: Option<anyhow::Error> = None;
                for attempt in 1..=INITIAL_ETCD_KEEPALIVE_PROBE_RETRIES {
                    let mut st = shared_state_guard.lock().await;
                    match tokio::time::timeout(
                        Duration::from_millis(INITIAL_ETCD_KEEPALIVE_PROBE_BUDGET_MS),
                        st.keepalive_once(),
                    )
                    .await
                    {
                        Ok(Ok(())) => {
                            drop(st);
                            if attempt > 1 {
                                tracing::warn!(
                                    lease_id,
                                    attempt,
                                    total = INITIAL_ETCD_KEEPALIVE_PROBE_RETRIES,
                                    budget_ms = INITIAL_ETCD_KEEPALIVE_PROBE_BUDGET_MS,
                                    "initial etcd keepalive probe succeeded after retry"
                                );
                            }
                            last_probe_err = None;
                            break;
                        }
                        Ok(Err(err)) => {
                            let last_stage = st.last_stage();
                            st.reset_stream();
                            drop(st);
                            tracing::warn!(
                                lease_id,
                                attempt,
                                total = INITIAL_ETCD_KEEPALIVE_PROBE_RETRIES,
                                budget_ms = INITIAL_ETCD_KEEPALIVE_PROBE_BUDGET_MS,
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
                        Err(_) => {
                            let last_stage = st.last_stage();
                            st.reset_stream();
                            drop(st);
                            let err = anyhow::anyhow!(
                                "initial etcd keepalive probe timed out for lease_id={} attempt={}/{} budget_ms={} stage={}",
                                lease_id,
                                attempt,
                                INITIAL_ETCD_KEEPALIVE_PROBE_RETRIES,
                                INITIAL_ETCD_KEEPALIVE_PROBE_BUDGET_MS,
                                last_stage
                            );
                            tracing::warn!(
                                lease_id,
                                attempt,
                                total = INITIAL_ETCD_KEEPALIVE_PROBE_RETRIES,
                                budget_ms = INITIAL_ETCD_KEEPALIVE_PROBE_BUDGET_MS,
                                stage = last_stage,
                                "initial etcd keepalive probe timed out, will {}",
                                if attempt < INITIAL_ETCD_KEEPALIVE_PROBE_RETRIES {
                                    "retry"
                                } else {
                                    "stop"
                                }
                            );
                            last_probe_err = Some(err);
                        }
                    }
                }
                if let Some(err) = last_probe_err {
                    return Err(err);
                }

                let entry = keepalive_actor::actor_register_lease(
                    backend_uid.clone(),
                    lease_id,
                    ttl_seconds,
                    ActorRegisterInvocation::Etcd {
                        client,
                        revoke_on_drop,
                    },
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
        LeaseRegisterKind::KvClient { register_by } => match backend_uid.kind() {
            LeaseType::KvClient => {
                record_register_by(lease_id, register_by.clone());
                let cb = backend_uid.kv_keepalive_cb().ok_or_else(|| {
                    anyhow::anyhow!("kvclient keepalive callback missing in LeaseBackendUid; construct kv backend via kv_client_with_callbacks()")
                })?;
                // Perform a synchronous first keepalive with up to 3 attempts.
                // Only if all 3 attempts fail do we surface an error to the caller.
                // No extra fallback paths are added; we simply repeat the same
                // operation because transient network jitter during the very first
                // probe is common in distributed setups.
                const INITIAL_KVCLIENT_KEEPALIVE_RETRIES: usize = 3; // business requirement
                let mut last_err: Option<anyhow::Error> = None;
                for attempt in 1..=INITIAL_KVCLIENT_KEEPALIVE_RETRIES {
                    let res = limit_thirdparty::tokio::task::spawn_blocking({
                        let cb = cb.clone();
                        let lid = lease_id;
                        move || (cb)(lid)
                    })
                    .await;

                    match res {
                        Ok(Ok(())) => {
                            // success on attempt N; proceed to register into keepalive actor
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
                        Ok(Err(err)) => {
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
                        Err(join_err) => {
                            tracing::warn!(
                                lease_id,
                                attempt,
                                total = INITIAL_KVCLIENT_KEEPALIVE_RETRIES,
                                "spawn_blocking join failed for initial kvclient keepalive, will {}: {:?}",
                                if attempt < INITIAL_KVCLIENT_KEEPALIVE_RETRIES {
                                    "retry"
                                } else {
                                    "stop"
                                },
                                join_err
                            );
                            last_err = Some(anyhow::anyhow!(
                                "spawn_blocking join failed for initial keepalive: {:?}",
                                join_err
                            ));
                        }
                    }

                    if last_err.is_none() {
                        // succeeded, exit attempt loop
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
                let entry = keepalive_actor::actor_register_lease(
                    backend_uid.clone(),
                    lease_id,
                    ttl_seconds,
                    ActorRegisterInvocation::KvClient {
                        cb,
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
