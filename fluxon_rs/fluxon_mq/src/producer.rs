use anyhow::{Context, Result};
use etcd_client as etcd;
use fluxon_commu::{
    scan_etcd_prefix_paginated_with_retry, EtcdPrefixScanAction, EtcdPrefixScanError,
};
use serde::{Deserialize, Serialize};

use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use fluxon_observability::keys::{
    PROM_LABEL_MQ_CATEGORY, PROM_LABEL_MQ_CHAN_ID, PROM_LABEL_MQ_PRODUCER_IDX, PROM_LABEL_NODE,
    PROM_LABEL_ROLE, PROM_METRIC_MQ_PUT_WINDOW_BYTES, PROM_METRIC_MQ_PUT_WINDOW_CALLS,
    PROM_VALUE_MQ_CATEGORY_MPMC_SUB, PROM_VALUE_MQ_CATEGORY_MPSC,
};
use fluxon_observability::metrics_actor::MetricsHandle as ObserveMetricsHandle;
use fluxon_util::etcd::{
    is_transient_etcd_error, run_prefix_watch_loop, DistributeIdAllocator,
    EtcdPrefixWatchLoopControl, ETCD_PREFIX_WATCH_RESTART_SLEEP,
};
use fluxon_util::lease_manager::LeaseManager;
use fluxon_util::prom_remote_write::{Label, Sample, TimeSeries, LABEL_NAME as RW_LABEL_NAME};

use crate::error::MpscError;
use crate::keys::{self, MqCategory};
use crate::lifecycle::spawn_named;
use crate::manager::{
    etcd_rpc_attempt_limit, get_chan_meta_with_retry, ChanManager, ChanMemberMeta, ChanRole,
    PRODUCE_OFFSET_BEGIN,
};
use crate::nonblocking_monitor::{
    spawn_nonblocking_monitor, NonblockingMonitorHandle, NonblockingMonitorKind,
};
use crate::offset_commit::{MonotonicOffsetCommit, OffsetCommitProgress};
use crate::shutdown::ShutdownCtl;
use crate::LifecycleView;
use tokio::sync::watch;
use tracing::warn;

const PRODUCE_OFFSET_ETCD_SLOW_WARN_THRESHOLD: Duration = Duration::from_secs(1);
const PRODUCE_OFFSET_PUT_TIMEOUT: Duration = Duration::from_secs(5);
const PRODUCE_OFFSET_PUT_RETRY_DELAY: Duration = Duration::from_millis(100);
const PRODUCER_MEMBERSHIP_RPC_TIMEOUT: Duration = Duration::from_secs(5);
const PRODUCER_MEMBERSHIP_RETRY_DELAY: Duration = Duration::from_millis(100);

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ProducerMemberMeta {
    producer_idx: String,
    #[serde(default)]
    external_client_id: Option<String>,
}

#[derive(Debug, Clone)]
enum ConsumerBindState {
    NoneBound,
    OneBound {
        preferred_sub_cluster: Option<String>,
    },
    Invalid {
        reason: String,
    },
}

fn map_prefix_scan_error(err: EtcdPrefixScanError<MpscError>) -> MpscError {
    match err {
        EtcdPrefixScanError::Get { source, .. } => MpscError::Etcd(source),
        EtcdPrefixScanError::Callback(source) => source,
    }
}

fn producer_membership_txn(
    key: &str,
    member_meta_bytes: &[u8],
    member_lease_id: i64,
    weight_key: &str,
    weight_value: &str,
    global_lease_id: i64,
) -> etcd::Txn {
    etcd::Txn::new()
        .when(vec![etcd::Compare::create_revision(
            key,
            etcd::CompareOp::Equal,
            0,
        )])
        .and_then(vec![
            etcd::TxnOp::put(
                key,
                member_meta_bytes,
                Some(etcd::PutOptions::new().with_lease(member_lease_id)),
            ),
            etcd::TxnOp::put(
                weight_key,
                weight_value,
                Some(etcd::PutOptions::new().with_lease(global_lease_id)),
            ),
        ])
        .or_else(vec![
            etcd::TxnOp::get(key, None),
            etcd::TxnOp::get(weight_key, None),
        ])
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct EtcdKeyGeneration {
    key: String,
    value: Vec<u8>,
    lease_id: i64,
    mod_revision: i64,
}

impl EtcdKeyGeneration {
    fn from_kv(kv: &etcd::KeyValue, expected_key: &str) -> Result<Self> {
        if kv.key() != expected_key.as_bytes() {
            anyhow::bail!(
                "producer membership readback returned unexpected key for expected_key={}",
                expected_key
            );
        }
        Ok(Self {
            key: expected_key.to_string(),
            value: kv.value().to_vec(),
            lease_id: kv.lease(),
            mod_revision: kv.mod_revision(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ProducerMembershipGeneration {
    member: EtcdKeyGeneration,
    weight: EtcdKeyGeneration,
}

enum ProducerMembershipReadback {
    Absent,
    Owned(ProducerMembershipGeneration),
    Conflicting {
        member_count: usize,
        weight_count: usize,
    },
}

fn producer_membership_get_pair(
    txn_res: &etcd::TxnResponse,
    key: &str,
    weight_key: &str,
) -> Result<(Option<EtcdKeyGeneration>, Option<EtcdKeyGeneration>)> {
    let responses = txn_res.op_responses();
    let [etcd::TxnOpResponse::Get(member_get), etcd::TxnOpResponse::Get(weight_get)] =
        responses.as_slice()
    else {
        anyhow::bail!(
            "producer membership readback returned an invalid response shape: operations={}",
            responses.len()
        );
    };

    let member_kvs = member_get.kvs();
    let weight_kvs = weight_get.kvs();
    if member_kvs.len() > 1 || weight_kvs.len() > 1 {
        anyhow::bail!(
            "producer membership readback returned duplicate exact keys: member_count={} weight_count={}",
            member_kvs.len(),
            weight_kvs.len()
        );
    }

    let member = member_kvs
        .first()
        .map(|kv| EtcdKeyGeneration::from_kv(kv, key))
        .transpose()?;
    let weight = weight_kvs
        .first()
        .map(|kv| EtcdKeyGeneration::from_kv(kv, weight_key))
        .transpose()?;
    Ok((member, weight))
}

fn classify_producer_membership_readback(
    txn_res: &etcd::TxnResponse,
    key: &str,
    member_meta_bytes: &[u8],
    member_lease_id: i64,
    weight_key: &str,
    weight_value: &str,
    global_lease_id: i64,
) -> Result<ProducerMembershipReadback> {
    let (member, weight) = producer_membership_get_pair(txn_res, key, weight_key)?;
    match (member, weight) {
        (None, None) => Ok(ProducerMembershipReadback::Absent),
        (Some(member), Some(weight))
            if member.value == member_meta_bytes
                && member.lease_id == member_lease_id
                && weight.value == weight_value.as_bytes()
                && weight.lease_id == global_lease_id =>
        {
            Ok(ProducerMembershipReadback::Owned(
                ProducerMembershipGeneration { member, weight },
            ))
        }
        (member, weight) => Ok(ProducerMembershipReadback::Conflicting {
            member_count: usize::from(member.is_some()),
            weight_count: usize::from(weight.is_some()),
        }),
    }
}

fn published_producer_membership_generation(
    txn_res: &etcd::TxnResponse,
    key: &str,
    member_meta_bytes: &[u8],
    member_lease_id: i64,
    weight_key: &str,
    weight_value: &str,
    global_lease_id: i64,
) -> Result<ProducerMembershipGeneration> {
    let mod_revision = txn_res
        .header()
        .ok_or_else(|| anyhow::anyhow!("producer membership publish response has no header"))?
        .revision();
    if mod_revision <= 0 {
        anyhow::bail!(
            "producer membership publish returned invalid revision {}",
            mod_revision
        );
    }
    Ok(ProducerMembershipGeneration {
        member: EtcdKeyGeneration {
            key: key.to_string(),
            value: member_meta_bytes.to_vec(),
            lease_id: member_lease_id,
            mod_revision,
        },
        weight: EtcdKeyGeneration {
            key: weight_key.to_string(),
            value: weight_value.as_bytes().to_vec(),
            lease_id: global_lease_id,
            mod_revision,
        },
    })
}

fn producer_membership_cleanup_txn(generation: &ProducerMembershipGeneration) -> etcd::Txn {
    let member = &generation.member;
    let weight = &generation.weight;
    etcd::Txn::new()
        .when(vec![
            etcd::Compare::mod_revision(
                member.key.clone(),
                etcd::CompareOp::Equal,
                member.mod_revision,
            ),
            etcd::Compare::lease(member.key.clone(), etcd::CompareOp::Equal, member.lease_id),
            etcd::Compare::value(
                member.key.clone(),
                etcd::CompareOp::Equal,
                member.value.clone(),
            ),
            etcd::Compare::mod_revision(
                weight.key.clone(),
                etcd::CompareOp::Equal,
                weight.mod_revision,
            ),
            etcd::Compare::lease(weight.key.clone(), etcd::CompareOp::Equal, weight.lease_id),
            etcd::Compare::value(
                weight.key.clone(),
                etcd::CompareOp::Equal,
                weight.value.clone(),
            ),
        ])
        .and_then(vec![
            etcd::TxnOp::delete(member.key.clone(), None),
            etcd::TxnOp::delete(weight.key.clone(), None),
        ])
        .or_else(vec![
            etcd::TxnOp::get(member.key.clone(), None),
            etcd::TxnOp::get(weight.key.clone(), None),
        ])
}

fn reconcile_producer_membership_cleanup(
    txn_res: &etcd::TxnResponse,
    generation: &ProducerMembershipGeneration,
) -> Result<()> {
    let (member, weight) =
        producer_membership_get_pair(txn_res, &generation.member.key, &generation.weight.key)?;
    reconcile_producer_membership_cleanup_observation(member.as_ref(), weight.as_ref(), generation)
}

fn reconcile_producer_membership_cleanup_observation(
    member: Option<&EtcdKeyGeneration>,
    weight: Option<&EtcdKeyGeneration>,
    generation: &ProducerMembershipGeneration,
) -> Result<()> {
    let Some(member) = member else {
        return Ok(());
    };
    if member != &generation.member {
        // A different member generation owns this logical key. Never delete it.
        return Ok(());
    }

    match weight {
        Some(weight) if weight == &generation.weight => anyhow::bail!(
            "producer membership cleanup compare was false although both keys still match generation"
        ),
        _ => anyhow::bail!(
            "producer membership cleanup refused partial state: owned member still exists but weight generation differs"
        ),
    }
}

async fn cleanup_producer_membership(
    client: &mut etcd::Client,
    generation: &ProducerMembershipGeneration,
    etcd_rpc_max_retries: u32,
) -> Result<()> {
    let mut last_error = String::new();
    let max_attempts = etcd_rpc_attempt_limit(etcd_rpc_max_retries);
    for attempt in 1..=max_attempts {
        let txn = producer_membership_cleanup_txn(generation);
        match tokio::time::timeout(PRODUCER_MEMBERSHIP_RPC_TIMEOUT, client.txn(txn)).await {
            Ok(Ok(txn_res)) if txn_res.succeeded() => return Ok(()),
            Ok(Ok(txn_res)) => {
                reconcile_producer_membership_cleanup(&txn_res, generation)?;
                return Ok(());
            }
            Ok(Err(err)) if is_transient_etcd_error(&err) => last_error = err.to_string(),
            Ok(Err(err)) => {
                anyhow::bail!(
                    "failed to delete producer membership and weight keys {}, {} on attempt {}: {}",
                    generation.member.key,
                    generation.weight.key,
                    attempt,
                    err
                );
            }
            Err(_) => {
                last_error = format!(
                    "request timed out after {} ms",
                    PRODUCER_MEMBERSHIP_RPC_TIMEOUT.as_millis()
                )
            }
        }
        if attempt < max_attempts {
            tokio::time::sleep(PRODUCER_MEMBERSHIP_RETRY_DELAY).await;
        }
    }
    anyhow::bail!(
        "failed to delete producer membership and weight keys {}, {} after {} attempts: {}",
        generation.member.key,
        generation.weight.key,
        max_attempts,
        last_error
    )
}

async fn publish_producer_membership(
    client: &mut etcd::Client,
    key: &str,
    member_meta_bytes: &[u8],
    member_lease_id: i64,
    weight_key: &str,
    weight_value: &str,
    global_lease_id: i64,
    etcd_rpc_max_retries: u32,
    shutdown: &ShutdownCtl,
) -> Result<ProducerMembershipGeneration> {
    let mut last_error = String::new();
    let mut request_started = false;
    let max_attempts = etcd_rpc_attempt_limit(etcd_rpc_max_retries);
    let mut attempts_performed = 0u64;
    for attempt in 1..=max_attempts {
        attempts_performed = attempt;
        if shutdown.is_closed() {
            let cleanup = if request_started {
                "skipped: publish generation was never acknowledged"
            } else {
                "not needed"
            };
            anyhow::bail!(
                "producer binding stopped by shutdown during membership publish; cleanup={}",
                cleanup
            );
        }

        request_started = true;
        let txn = producer_membership_txn(
            key,
            member_meta_bytes,
            member_lease_id,
            weight_key,
            weight_value,
            global_lease_id,
        );
        let retryable =
            match tokio::time::timeout(PRODUCER_MEMBERSHIP_RPC_TIMEOUT, client.txn(txn)).await {
                Ok(Ok(txn_res)) if txn_res.succeeded() => {
                    return published_producer_membership_generation(
                        &txn_res,
                        key,
                        member_meta_bytes,
                        member_lease_id,
                        weight_key,
                        weight_value,
                        global_lease_id,
                    );
                }
                Ok(Ok(txn_res)) => match classify_producer_membership_readback(
                    &txn_res,
                    key,
                    member_meta_bytes,
                    member_lease_id,
                    weight_key,
                    weight_value,
                    global_lease_id,
                ) {
                    Ok(ProducerMembershipReadback::Owned(generation)) => return Ok(generation),
                    Ok(ProducerMembershipReadback::Absent) => {
                        last_error = "membership disappeared while reconciling a retry".to_string();
                        true
                    }
                    Ok(ProducerMembershipReadback::Conflicting {
                        member_count,
                        weight_count,
                    }) => anyhow::bail!(
                        "producer membership key already exists with conflicting state: member_count={} weight_count={}",
                        member_count,
                        weight_count
                    ),
                    Err(error) => return Err(error),
                },
                Ok(Err(err)) => {
                    let retryable = is_transient_etcd_error(&err);
                    last_error = err.to_string();
                    retryable
                }
                Err(_) => {
                    last_error = format!(
                        "request timed out after {} ms",
                        PRODUCER_MEMBERSHIP_RPC_TIMEOUT.as_millis()
                    );
                    true
                }
            };

        if !retryable {
            break;
        }

        if attempt < max_attempts {
            warn!(
                chan_membership_key = key,
                attempt,
                total = max_attempts,
                error = %last_error,
                "producer membership publish did not complete; retrying"
            );
            tokio::time::sleep(PRODUCER_MEMBERSHIP_RETRY_DELAY).await;
        }
    }

    anyhow::bail!(
        "failed to publish producer membership and weight keys {}, {} after {} attempts: {}; cleanup={}",
        key,
        weight_key,
        attempts_performed,
        last_error,
        "skipped: publish generation was never acknowledged",
    )
}

/// MPSC channel producer binding helper.
///
/// This struct focuses on etcd-side registration and lease management.
/// Data path (put/get) is intentionally left to upper layers.
pub struct MpscProducer {
    chan_id: i64,
    producer_idx: String,
    lease_manager: LeaseManager,
    chan_mgr: ChanManager,
    /// Next message id to use for this producer.
    ///
    /// Initialized based on PRODUCE_OFFSET_BEGIN and incremented on
    /// each put; this avoids per-call etcd reads for
    /// `produce_offset` and relies on the invariant that a given
    /// producer handle is single-writer.
    next_msg_id: i64,
    /// Shared shutdown controller used by higher layers (via PyO3
    /// handle) to signal that this producer should stop retrying and
    /// exit ongoing operations as soon as possible.
    shutdown: ShutdownCtl,
    category: MqCategory,
    consumer_bind_state_rx: watch::Receiver<ConsumerBindState>,
    nonblocking_monitor: NonblockingMonitorHandle,

    observe_node_id: String,
    observe_node_role: String,
    observe: ObserveMetricsHandle,
}

impl MpscProducer {
    /// Bind a producer for the given MPSC channel using the provided
    /// `ChanManager`.
    ///
    /// `chan_mgr` carries channel-level information (chan_id and
    /// global leases) constructed by `create_mpsc_channel` or by an
    /// equivalent loader. This API focuses on per-producer member
    /// lease and membership/weight registration.
    pub async fn bind_mpsc(
        chan_mgr: ChanManager,
        _ttl_seconds: i64,
        weight: Option<i64>,
        lifecycle: LifecycleView,
        shutdown: ShutdownCtl,
        external_client_id: Option<String>,
        category: MqCategory,
        parent_member_id_opt: Option<i64>,
        observe_node_id: String,
        observe_node_role: String,
        observe: ObserveMetricsHandle,
    ) -> Result<Self> {
        if shutdown.is_closed() {
            anyhow::bail!("producer binding stopped by shutdown before it started");
        }

        if let Some(id) = external_client_id.as_deref() {
            if id.trim().is_empty() {
                anyhow::bail!("external_client_id must be a non-empty string when provided");
            }
            if id != id.trim() {
                anyhow::bail!("external_client_id must not have leading/trailing whitespace");
            }
        }

        let chan_id = chan_mgr.chan_id;
        let etcd_rpc_max_retries = chan_mgr.etcd_rpc_max_retries();
        let lease_manager = chan_mgr.lease_manager.clone();
        let mut client = chan_mgr.etcd_client();

        if observe_node_id.trim().is_empty() {
            anyhow::bail!("observe_node_id must be a non-empty string");
        }
        if observe_node_id != observe_node_id.trim() {
            anyhow::bail!("observe_node_id must not have leading/trailing whitespace");
        }
        if observe_node_role.trim().is_empty() {
            anyhow::bail!("observe_node_role must be a non-empty string");
        }
        if observe_node_role != observe_node_role.trim() {
            anyhow::bail!("observe_node_role must not have leading/trailing whitespace");
        }

        // 1) Ensure channel meta exists (mirror Python ChanManager.bind step 1)
        let mut meta_client = chan_mgr.etcd_client();
        let meta_result = tokio::select! {
            biased;
            _ = shutdown.wait_closed() => {
                anyhow::bail!("producer binding stopped by shutdown while loading channel metadata");
            }
            result = get_chan_meta_with_retry(&mut meta_client, chan_id, etcd_rpc_max_retries) => result,
        };
        let _meta = meta_result
            .with_context(|| format!("channel meta not found for chan_id={}", chan_id))?;

        // 2) Reuse ChanManager's member lease instead of creating a
        // new one. ChanManager 在 channel 创建/绑定阶段已经为该
        // channel 准备了 member lease，这里直接拿到 lease_id 用于
        // membership key 绑定即可。
        let member_lease_id = chan_mgr.member_lease_id();

        // 3) Allocate producer idx using distributed ID allocator and
        // bind membership key. Re-use the per-channel long-lived
        // cluster lease managed by ChanManager instead of creating a
        // temporary lease.
        // Decide producer_idx based on category:
        // - Mpsc: allocate a fresh per-channel producer id
        // - MpmcSub: reuse parent MPMC member_id as the producer_idx for this channel
        let producer_idx = match category {
            MqCategory::Mpsc => {
                let local_id = allocate_producer_idx(&chan_mgr).await?;
                local_id.to_string()
            }
            MqCategory::MpmcSub { .. } => {
                let mid = parent_member_id_opt.ok_or_else(|| {
                    anyhow::anyhow!("parent_member_id is required in MpmcSub mode")
                })?;
                mid.to_string()
            }
        };
        let key = keys::etcd_producer_key(chan_id, &producer_idx);

        let member_meta = ProducerMemberMeta {
            producer_idx: producer_idx.clone(),
            external_client_id,
        };
        let member_meta_bytes = serde_json::to_vec(&member_meta)
            .map_err(|e| anyhow::anyhow!("serialize ProducerMemberMeta failed: {}", e))?;

        if shutdown.is_closed() {
            anyhow::bail!("producer binding stopped by shutdown before membership publish");
        }

        let weight = weight.unwrap_or(1);
        let weight_value = weight.to_string();
        let weight_key = keys::etcd_producer_weight_key(chan_id, &producer_idx);
        let global_lease_id = chan_mgr.global_lease.id() as i64;
        let membership_generation = publish_producer_membership(
            &mut client,
            &key,
            &member_meta_bytes,
            member_lease_id,
            &weight_key,
            &weight_value,
            global_lease_id,
            etcd_rpc_max_retries,
            &shutdown,
        )
        .await?;

        if shutdown.is_closed() {
            let cleanup = cleanup_producer_membership(
                &mut client,
                &membership_generation,
                etcd_rpc_max_retries,
            )
            .await;
            anyhow::bail!(
                "producer binding stopped by shutdown after membership publish; cleanup for {}, {}: {}",
                key,
                weight_key,
                match cleanup {
                    Ok(()) => "ok".to_string(),
                    Err(err) => err.to_string(),
                }
            );
        }

        let (consumer_bind_state_tx, consumer_bind_state_rx) =
            watch::channel(ConsumerBindState::NoneBound);
        spawn_consumer_meta_watch(
            chan_mgr.etcd_client(),
            chan_id,
            consumer_bind_state_tx,
            producer_idx.clone(),
            lifecycle.clone(),
            shutdown.clone(),
            etcd_rpc_max_retries,
        );
        let nonblocking_monitor = spawn_nonblocking_monitor(
            &lifecycle,
            shutdown.clone(),
            observe_node_id.clone(),
            observe_node_role.clone(),
            observe.clone(),
            category,
            NonblockingMonitorKind::Producer { chan_id },
            producer_idx.clone(),
        );

        Ok(Self {
            chan_id,
            producer_idx,
            lease_manager,
            chan_mgr,
            // First id = PRODUCE_OFFSET_BEGIN + 1
            next_msg_id: PRODUCE_OFFSET_BEGIN + 1,
            // shutdown 控制器由上层（例如 PyO3 层）构造并注入，
            // 这里直接复用同一个实例，以便 handle/重试循环
            // 共享关闭信号。
            shutdown,
            category,
            consumer_bind_state_rx,
            nonblocking_monitor,

            observe_node_id,
            observe_node_role,
            observe,
        })
    }

    pub fn chan_id(&self) -> i64 {
        self.chan_id
    }

    pub fn producer_idx(&self) -> &str {
        &self.producer_idx
    }

    pub fn lease_manager(&self) -> &LeaseManager {
        &self.lease_manager
    }

    /// kvclient payload lease id associated with this channel.
    ///
    /// ChanManager 在构造时已持有有效的 payload lease 句柄，
    /// 这里直接返回其 `id`。早期为兼容 Python 签名曾返回
    /// `Option<i64>`，现统一为必填的 `i64`，语义更清晰。
    pub fn payload_lease_id(&self) -> i64 {
        self.chan_mgr.payload_lease.id() as i64
    }

    /// Shared shutdown controller for this producer instance.
    pub fn shutdown_ctl(&self) -> ShutdownCtl {
        self.shutdown.clone()
    }

    pub fn record_nonblocking_put_success(&self, unix_ms: i64) {
        self.nonblocking_monitor.try_record_nonblocking(unix_ms);
    }

    pub fn record_blocking_put_observed(&self, unix_ms: i64) {
        self.nonblocking_monitor.try_record_blocking(unix_ms);
    }

    fn mq_category_str(&self) -> &'static str {
        match self.category {
            MqCategory::MpmcSub { .. } => PROM_VALUE_MQ_CATEGORY_MPMC_SUB,
            MqCategory::Mpsc => PROM_VALUE_MQ_CATEGORY_MPSC,
        }
    }

    fn now_ms() -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock is before UNIX_EPOCH")
            .as_millis() as i64
    }

    fn ts_one(
        &self,
        name: &'static str,
        extra_labels: &[(&'static str, &'static str)],
        value: f64,
        ts_ms: i64,
    ) -> TimeSeries {
        let mut labels: Vec<Label> = Vec::with_capacity(8 + extra_labels.len());
        labels.push(Label {
            name: RW_LABEL_NAME.to_string(),
            value: name.to_string(),
        });
        labels.push(Label {
            name: PROM_LABEL_NODE.to_string(),
            value: self.observe_node_id.clone(),
        });
        labels.push(Label {
            name: PROM_LABEL_ROLE.to_string(),
            value: self.observe_node_role.clone(),
        });
        labels.push(Label {
            name: PROM_LABEL_MQ_CATEGORY.to_string(),
            value: self.mq_category_str().to_string(),
        });
        labels.push(Label {
            name: PROM_LABEL_MQ_CHAN_ID.to_string(),
            value: self.chan_id.to_string(),
        });
        labels.push(Label {
            name: PROM_LABEL_MQ_PRODUCER_IDX.to_string(),
            value: self.producer_idx.clone(),
        });
        for (k, v) in extra_labels {
            labels.push(Label {
                name: (*k).to_string(),
                value: (*v).to_string(),
            });
        }
        TimeSeries {
            labels,
            samples: vec![Sample {
                value,
                timestamp: ts_ms,
            }],
        }
    }

    pub fn observe_put_window(&self, window_calls: u64, window_bytes: u64) {
        let ts_ms = Self::now_ms();
        let series: Vec<TimeSeries> = vec![
            self.ts_one(
                PROM_METRIC_MQ_PUT_WINDOW_CALLS,
                &[],
                window_calls as f64,
                ts_ms,
            ),
            self.ts_one(
                PROM_METRIC_MQ_PUT_WINDOW_BYTES,
                &[],
                window_bytes as f64,
                ts_ms,
            ),
        ];
        self.observe.try_submit_timeseries(series);
    }

    fn preferred_sub_cluster_for_put(&self) -> Result<Option<String>, MpscError> {
        match self.consumer_bind_state_rx.borrow().clone() {
            ConsumerBindState::NoneBound => Ok(None),
            ConsumerBindState::OneBound {
                preferred_sub_cluster,
            } => Ok(preferred_sub_cluster),
            ConsumerBindState::Invalid { reason } => Err(MpscError::Internal(format!(
                "invalid consumer binding state for chan_id={}: {}",
                self.chan_id, reason
            ))),
        }
    }

    /// High-level put interface that constructs the message key,
    /// delegates the actual KV put to a synchronous callback and, on
    /// success, updates the per-producer `produce_offset` key in
    /// etcd.
    ///
    /// The callback must perform the backend put using the
    /// given `(message_key, msg_id, preferred_sub_cluster)` and return a status code:
    ///   - 0: success
    ///   - 1: retryable error (e.g. backend space full)
    ///   - 2: non-retryable error
    ///
    /// Code `1` will be retried in a loop inside this function until
    /// it either succeeds (`0`) or yields a non-retryable result.
    /// Other codes are treated as unknown and mapped to
    /// `PutPayloadUnknownCode`.
    pub async fn put_with_payload<F>(&mut self, put_payload: F) -> Result<(), MpscError>
    where
        F: Fn(String, i64, Option<String>) -> i32 + Send + Sync + 'static,
    {
        use limit_thirdparty::tokio::task;
        use std::time::Duration;
        use tokio::time::sleep;

        let preferred_sub_cluster_for_call = self.preferred_sub_cluster_for_put()?;

        // 1) Reserve next message id from local counter. This avoids
        // per-call etcd reads for produce_offset. Gaps in msg_id are
        // acceptable: on failures the reserved id will simply remain
        // unused.
        let next_id = self.next_msg_id;
        self.next_msg_id = next_id + 1;

        let offset_key =
            keys::etcd_produce_offset_one_producer_key(self.chan_id, &self.producer_idx);
        let msg_key = keys::backend_message_key_with_category(
            self.chan_id,
            &self.producer_idx,
            next_id,
            &self.category,
        );

        // 2) Execute synchronous payload callback in a blocking task.
        // For code 1 (retryable, e.g. backend space full) we keep
        // retrying in a loop with a small backoff, reusing the same
        // reserved msg_id and message key.
        let put_payload = Arc::new(put_payload);
        loop {
            if self.shutdown.is_closed() {
                return Err(MpscError::Internal(
                    "producer closed during put_with_payload".to_string(),
                ));
            }
            let key_clone = msg_key.clone();
            let f = put_payload.clone();
            let hint = preferred_sub_cluster_for_call.clone();
            let code = task::spawn_blocking(move || (f)(key_clone, next_id, hint))
                .await
                .map_err(MpscError::JoinError)?;

            match code {
                0 => {
                    // success – update produce_offset
                    break;
                }
                1 => {
                    // retryable error: backend space full or similar.
                    sleep(Duration::from_millis(50)).await;
                    continue;
                }
                2 => return Err(MpscError::PutPayloadNonRetryable),
                other => {
                    return Err(MpscError::PutPayloadUnknownCode { code: other });
                }
            }
        }

        let mut client = self.chan_mgr.etcd_client();
        // 更新 produce_offset 时使用 channel 级别的 global lease，
        // 与 Python 版保持一致（等价于 self.chan_lease）。
        let global_lease_id = self.chan_mgr.global_lease.id() as i64;
        let offset_put_begin = Instant::now();
        let max_attempts = etcd_rpc_attempt_limit(self.chan_mgr.etcd_rpc_max_retries());
        let mut committed_attempt = 0u64;
        let mut offset_commit =
            MonotonicOffsetCommit::new(offset_key.clone(), next_id, global_lease_id);
        for attempt in 1..=max_attempts {
            let result = tokio::select! {
                biased;
                _ = self.shutdown.wait_closed() => {
                    return Err(MpscError::Internal(format!(
                        "producer closed while committing produce offset for key {} msg_id={}",
                        offset_key, next_id
                    )));
                }
                result = tokio::time::timeout(
                    PRODUCE_OFFSET_PUT_TIMEOUT,
                    offset_commit.attempt(&mut client),
                ) => result,
            };

            let retry_reason = match result {
                Ok(Ok(OffsetCommitProgress::Complete)) => {
                    committed_attempt = attempt;
                    break;
                }
                Ok(Ok(OffsetCommitProgress::Retry(_))) => {
                    "offset generation changed before the fenced commit".to_string()
                }
                Ok(Err(error)) => match error {
                    MpscError::Etcd(error) if is_transient_etcd_error(&error) => {
                        format!("transient etcd error: {error}")
                    }
                    error => {
                        return Err(MpscError::Internal(format!(
                            "failed to update produce offset for key {}, leaseid: {}, attempt: {}, err: {}",
                            offset_key, global_lease_id, attempt, error
                        )));
                    }
                },
                Err(_) => format!(
                    "timed out after {} ms",
                    PRODUCE_OFFSET_PUT_TIMEOUT.as_millis()
                ),
            };

            if attempt == max_attempts {
                return Err(MpscError::Internal(format!(
                    "failed to update produce offset for key {}, leaseid: {}, msg_id: {} after {} attempts: {}",
                    offset_key, global_lease_id, next_id, attempt, retry_reason
                )));
            }

            warn!(
                chan_id = self.chan_id,
                producer_idx = %self.producer_idx,
                msg_id = next_id,
                offset_key = %offset_key,
                attempt,
                max_attempts,
                reason = %retry_reason,
                "Retrying produce-offset commit"
            );
            tokio::select! {
                biased;
                _ = self.shutdown.wait_closed() => {
                    return Err(MpscError::Internal(format!(
                        "producer closed during produce-offset retry for key {} msg_id={}",
                        offset_key, next_id
                    )));
                }
                _ = sleep(PRODUCE_OFFSET_PUT_RETRY_DELAY) => {}
            }
        }
        assert!(
            committed_attempt > 0,
            "bounded produce-offset commit did not finish"
        );
        let offset_put_elapsed = offset_put_begin.elapsed();
        if committed_attempt > 1 || offset_put_elapsed >= PRODUCE_OFFSET_ETCD_SLOW_WARN_THRESHOLD {
            warn!(
                "[MpscProducer chan_id={} producer_idx={}] produce_offset committed: msg_id={} offset_key={} attempts={} elapsed_ms={}",
                self.chan_id,
                self.producer_idx,
                next_id,
                offset_key,
                committed_attempt,
                offset_put_elapsed.as_millis(),
            );
        }
        Ok(())
    }
}

fn spawn_consumer_meta_watch(
    client: etcd::Client,
    chan_id: i64,
    state_tx: watch::Sender<ConsumerBindState>,
    producer_idx: String,
    lifecycle: LifecycleView,
    shutdown: ShutdownCtl,
    max_retries: u32,
) {
    let name = format!(
        "fluxon_mq.producer.consumer_meta_watch.chan_id={}.producer_idx={}",
        chan_id, producer_idx
    );
    spawn_named(&lifecycle, name, async move {
        let prefix = keys::etcd_consumer_key_prefix(chan_id);
        let opts = etcd::WatchOptions::new().with_prefix();
        let mut initial_refresh_client = client.clone();

        let _ = refresh_consumer_bind_state(
            &mut initial_refresh_client,
            chan_id,
            &prefix,
            &state_tx,
            max_retries,
        )
        .await;

        let watch_label = format!("[MpscProducer chan_id={}] consumer meta watch", chan_id);
        let stop = shutdown;
        let resync_client = client.clone();
        let batch_client = client.clone();
        let resync_prefix = prefix.clone();
        let batch_prefix = prefix.clone();
        let resync_state_tx = state_tx.clone();
        let batch_state_tx = state_tx;

        run_prefix_watch_loop(
            client,
            prefix,
            opts,
            ETCD_PREFIX_WATCH_RESTART_SLEEP,
            watch_label,
            stop,
            move || {
                let mut refresh_client = resync_client.clone();
                let prefix = resync_prefix.clone();
                let state_tx = resync_state_tx.clone();
                async move {
                    refresh_consumer_bind_state(
                        &mut refresh_client,
                        chan_id,
                        &prefix,
                        &state_tx,
                        max_retries,
                    )
                    .await
                }
            },
            move |_events| {
                let mut refresh_client = batch_client.clone();
                let prefix = batch_prefix.clone();
                let state_tx = batch_state_tx.clone();
                async move {
                    refresh_consumer_bind_state(
                        &mut refresh_client,
                        chan_id,
                        &prefix,
                        &state_tx,
                        max_retries,
                    )
                    .await
                }
            },
        )
        .await;
    });
}

async fn refresh_consumer_bind_state(
    client: &mut etcd::Client,
    chan_id: i64,
    prefix: &str,
    state_tx: &watch::Sender<ConsumerBindState>,
    max_retries: u32,
) -> EtcdPrefixWatchLoopControl {
    let state = match load_consumer_bind_state_snapshot(client, chan_id, prefix, max_retries).await
    {
        Ok(v) => v,
        Err(e) => {
            let reason = format!(
                "failed to refresh consumer binding snapshot from etcd for prefix {}: {:?}",
                prefix, e
            );
            warn!("[MpscProducer chan_id={}] {}", chan_id, reason);
            ConsumerBindState::Invalid { reason }
        }
    };
    if state_tx.send(state).is_err() {
        return EtcdPrefixWatchLoopControl::Stop;
    }
    EtcdPrefixWatchLoopControl::Continue
}

async fn load_consumer_bind_state_snapshot(
    client: &mut etcd::Client,
    _chan_id: i64,
    prefix: &str,
    max_retries: u32,
) -> Result<ConsumerBindState, MpscError> {
    let mut binding_count = 0usize;
    let mut first_value: Option<Vec<u8>> = None;
    let mut keys_dbg: Vec<String> = Vec::new();
    scan_etcd_prefix_paginated_with_retry(client, prefix, max_retries, |key, value| {
        binding_count += 1;
        if keys_dbg.len() < 8 {
            match std::str::from_utf8(key) {
                Ok(s) => keys_dbg.push(s.to_string()),
                Err(_) => keys_dbg.push("<non-utf8-key>".to_string()),
            }
        }
        if binding_count == 1 {
            first_value = Some(value.to_vec());
        }
        Ok::<EtcdPrefixScanAction, MpscError>(EtcdPrefixScanAction::Continue)
    })
    .await
    .map_err(map_prefix_scan_error)?;

    if binding_count == 0 {
        return Ok(ConsumerBindState::NoneBound);
    }
    if binding_count != 1 {
        return Ok(ConsumerBindState::Invalid {
            reason: format!(
                "expected at most 1 consumer binding under prefix {}, got {} keys={:?}",
                prefix, binding_count, keys_dbg
            ),
        });
    }

    let meta: ChanMemberMeta = serde_json::from_slice(
        first_value
            .as_ref()
            .expect("exactly one consumer binding must preserve its payload"),
    )
    .map_err(|e| {
        MpscError::Internal(format!(
            "invalid consumer meta json under prefix {}: {}",
            prefix, e
        ))
    })?;
    if meta.role != ChanRole::Consumer {
        return Ok(ConsumerBindState::Invalid {
            reason: format!("unexpected consumer meta role: {:?}", meta.role),
        });
    }

    Ok(ConsumerBindState::OneBound {
        preferred_sub_cluster: meta.kvclient_sub_cluster,
    })
}

/// Allocate next producer id for a channel using the shared
/// distributed ID allocator.
///
/// This mirrors the Python usage of `DistributeIdAllocator` with a
/// per-channel prefix "channels/{chan_id}".
async fn allocate_producer_idx(chan_mgr: &ChanManager) -> Result<i64> {
    let chan_id = chan_mgr.chan_id;
    let client = chan_mgr.etcd_client();
    // 使用 ChanManager 上的长 TTL cluster lease，为该 channel 的
    // producer id allocator 提供稳定的 lease 语义。
    let lease_id = chan_mgr.global_long_lease.id() as i64;

    let allocator = DistributeIdAllocator::new_with_retry(
        client.clone(),
        format!("channels/{}", chan_id),
        lease_id,
        chan_mgr.etcd_rpc_max_retries(),
    );
    allocator
        .allocate_id()
        .await
        .with_context(|| format!("failed to allocate producer id for chan_id={}", chan_id))
}

#[cfg(test)]
mod tests {
    use super::{
        reconcile_producer_membership_cleanup_observation, EtcdKeyGeneration,
        ProducerMembershipGeneration,
    };

    fn generation() -> ProducerMembershipGeneration {
        ProducerMembershipGeneration {
            member: EtcdKeyGeneration {
                key: "member".to_string(),
                value: b"member-value".to_vec(),
                lease_id: 11,
                mod_revision: 101,
            },
            weight: EtcdKeyGeneration {
                key: "weight".to_string(),
                value: b"1".to_vec(),
                lease_id: 22,
                mod_revision: 101,
            },
        }
    }

    #[test]
    fn cleanup_replay_accepts_already_deleted_generation() {
        let expected = generation();
        reconcile_producer_membership_cleanup_observation(None, None, &expected).unwrap();
    }

    #[test]
    fn cleanup_replay_never_claims_a_new_member_generation() {
        let expected = generation();
        let mut newer_member = expected.member.clone();
        newer_member.mod_revision += 1;

        reconcile_producer_membership_cleanup_observation(
            Some(&newer_member),
            Some(&expected.weight),
            &expected,
        )
        .unwrap();
    }

    #[test]
    fn cleanup_replay_rejects_partial_owned_generation() {
        let expected = generation();
        let mut newer_weight = expected.weight.clone();
        newer_weight.mod_revision += 1;

        let error = reconcile_producer_membership_cleanup_observation(
            Some(&expected.member),
            Some(&newer_weight),
            &expected,
        )
        .unwrap_err();
        assert!(error.to_string().contains("refused partial state"));
    }

    #[test]
    fn cleanup_false_compare_rejects_impossible_exact_readback() {
        let expected = generation();
        let error = reconcile_producer_membership_cleanup_observation(
            Some(&expected.member),
            Some(&expected.weight),
            &expected,
        )
        .unwrap_err();
        assert!(error.to_string().contains("compare was false"));
    }
}
