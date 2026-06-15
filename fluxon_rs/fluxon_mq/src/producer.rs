use anyhow::{Context, Result};
use etcd_client as etcd;
use fluxon_commu::{scan_etcd_prefix_paginated, EtcdPrefixScanAction, EtcdPrefixScanError};
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
    run_prefix_watch_loop, DistributeIdAllocator, EtcdPrefixWatchLoopControl,
    ETCD_PREFIX_WATCH_RESTART_SLEEP,
};
use fluxon_util::lease_manager::LeaseManager;
use fluxon_util::prom_remote_write::{Label, Sample, TimeSeries, LABEL_NAME as RW_LABEL_NAME};

use crate::error::MpscError;
use crate::keys::{self, MqCategory};
use crate::lifecycle::spawn_named;
use crate::manager::{get_chan_meta, ChanManager, ChanMemberMeta, ChanRole, PRODUCE_OFFSET_BEGIN};
use crate::nonblocking_monitor::{
    spawn_nonblocking_monitor, NonblockingMonitorHandle, NonblockingMonitorKind,
};
use crate::shutdown::ShutdownCtl;
use crate::LifecycleView;
use tokio::sync::watch;
use tracing::warn;

const PRODUCE_OFFSET_ETCD_SLOW_WARN_THRESHOLD: Duration = Duration::from_secs(1);

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
        if let Some(id) = external_client_id.as_deref() {
            if id.trim().is_empty() {
                anyhow::bail!("external_client_id must be a non-empty string when provided");
            }
            if id != id.trim() {
                anyhow::bail!("external_client_id must not have leading/trailing whitespace");
            }
        }

        let chan_id = chan_mgr.chan_id;
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
        let _meta = get_chan_meta(&mut meta_client, chan_id)
            .await
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

        let compare = etcd::Compare::create_revision(key.clone(), etcd::CompareOp::Equal, 0);
        let put_op = etcd::TxnOp::put(
            key.clone(),
            member_meta_bytes,
            Some(etcd::PutOptions::new().with_lease(member_lease_id)),
        );
        let txn = etcd::Txn::new().when(vec![compare]).and_then(vec![put_op]);
        let txn_res = client
            .txn(txn)
            .await
            .with_context(|| format!("failed to bind producer membership key {}", key))?;
        if !txn_res.succeeded() {
            anyhow::bail!("producer membership key {} already exists", key);
        }

        // 4) Optionally write producer weight (default 1)，挂在
        // channel 级别的 global lease 上，生命周期与全局 chan 一致。
        let weight = weight.unwrap_or(1);
        let weight_key = keys::etcd_producer_weight_key(chan_id, &producer_idx);
        let global_lease_id = chan_mgr.global_lease.id() as i64;
        client
            .put(
                weight_key.clone(),
                weight.to_string(),
                Some(etcd::PutOptions::new().with_lease(global_lease_id)),
            )
            .await
            .with_context(|| format!("failed to write producer weight key {}", weight_key))?;

        let (consumer_bind_state_tx, consumer_bind_state_rx) =
            watch::channel(ConsumerBindState::NoneBound);
        spawn_consumer_meta_watch(
            chan_mgr.etcd_client(),
            chan_id,
            consumer_bind_state_tx,
            producer_idx.clone(),
            lifecycle.clone(),
            shutdown.clone(),
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
        client
            .put(
                offset_key.clone(),
                next_id.to_string(),
                Some(etcd::PutOptions::new().with_lease(global_lease_id)),
            )
            .await
            .map_err(|e| {
                MpscError::Internal(format!(
                    "failed to update produce offset for key {}, leaseid: {}, err:{}",
                    offset_key, global_lease_id, e
                ))
            })?;
        let offset_put_elapsed = offset_put_begin.elapsed();
        if offset_put_elapsed >= PRODUCE_OFFSET_ETCD_SLOW_WARN_THRESHOLD {
            warn!(
                "[MpscProducer chan_id={} producer_idx={}] produce_offset put slow: msg_id={} offset_key={} elapsed_ms={}",
                self.chan_id,
                self.producer_idx,
                next_id,
                offset_key,
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
) {
    let name = format!(
        "fluxon_mq.producer.consumer_meta_watch.chan_id={}.producer_idx={}",
        chan_id, producer_idx
    );
    spawn_named(&lifecycle, name, async move {
        let prefix = keys::etcd_consumer_key_prefix(chan_id);
        let opts = etcd::WatchOptions::new().with_prefix();
        let mut initial_refresh_client = client.clone();

        let _ =
            refresh_consumer_bind_state(&mut initial_refresh_client, chan_id, &prefix, &state_tx)
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
                    refresh_consumer_bind_state(&mut refresh_client, chan_id, &prefix, &state_tx)
                        .await
                }
            },
            move |_events| {
                let mut refresh_client = batch_client.clone();
                let prefix = batch_prefix.clone();
                let state_tx = batch_state_tx.clone();
                async move {
                    refresh_consumer_bind_state(&mut refresh_client, chan_id, &prefix, &state_tx)
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
) -> EtcdPrefixWatchLoopControl {
    let state = match load_consumer_bind_state_snapshot(client, chan_id, prefix).await {
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
) -> Result<ConsumerBindState, MpscError> {
    let mut binding_count = 0usize;
    let mut first_value: Option<Vec<u8>> = None;
    let mut keys_dbg: Vec<String> = Vec::new();
    scan_etcd_prefix_paginated(client, prefix, |key, value| {
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

    let allocator =
        DistributeIdAllocator::new(client.clone(), format!("channels/{}", chan_id), lease_id);
    allocator
        .allocate_id()
        .await
        .with_context(|| format!("failed to allocate producer id for chan_id={}", chan_id))
}
