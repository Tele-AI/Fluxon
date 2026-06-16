use crate::SharedJsonMeta;
use crate::client_kv_api::msg_pack::{
    ExternalInvalidateWeakIndexReq, ExternalInvalidateWeakIndexResp,
};
use crate::client_seg_pool::{ClientSegPool, SideTransferPeerFileMeta};
use crate::cluster_manager::app_logic_ext::ClusterManagerAppLogicExt;
use crate::cluster_manager::{
    META_KEY_SHARED_STORAGE_NODE_ID, META_KEY_SHARED_STORAGE_NODE_START_TIME,
};
use crate::rpcresp_kvresult_convert::ToResult;
use crate::{
    client_kv_api::msg_pack::{
        ExternalDeleteAckReq, ExternalDeleteReq, ExternalGetReq, ExternalIsExistReq,
        ExternalPutCommitReq, ExternalPutCommitResp, ExternalPutStartReq, ExternalPutStartResp,
        ExternalPutTransferEndReq, ExternalPutTransferEndResp, SyncKvToFileReq, SyncKvToFileResp,
        TestPutPhaseTrace,
    },
    cluster_manager::{
        ClusterManager, ClusterManagerAccessTrait, IpcBandwidthAttributorHandle, NodeRole,
    },
    master_lease_manager::msg_pack::{AllocateClientLeaseReq, ClientLeaseKeepaliveReq},
    memholder::ExternalMemHolder,
    p2p::{
        msg_pack::{MsgPack, RPCCaller, RPCHandler},
        p2p_module::{P2pModule, P2pModuleAccessTrait},
    },
    rpcresp_kvresult_convert::msg_and_error::{ApiError, KvError, KvResult, SharedMemError},
};
use async_trait::async_trait;
use core::panic;
use dashmap::DashMap;
use fluxon_commu::ShareGroupOwnerRef;
use fluxon_framework::{LogicalModule, define_module};
use fluxon_observability::kv_metrics_actor::{ObserveComponent, ObserveDirection};
use fluxon_util::semaphore_map::SemaphoreMap;
use libc::{MAP_SHARED, PROT_READ, PROT_WRITE, mmap};
use limit_thirdparty::tokio;
use limit_thirdparty::tokio::sync::{ARwLock, Notify};
use limit_thirdparty::tokio::time::sleep;
use parking_lot::Mutex;
use std::{
    fs::File,
    // path::PathBuf, // 不再使用PathBuf
    sync::{
        Arc, OnceLock, Weak,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

// #[cfg(test)]
#[cfg(feature = "test_bins")]
pub mod external_client_test;

type SharedMetaSignature = fluxon_util::fs_watch::FileSignature;

// External->Owner staged put consists of multiple potentially slow components:
// - ExternalPutStartReq triggers owner->master PutStart RPC (60s timeout).
// - ExternalPutTransferEndReq executes transfer (can be slow) and then owner->master PutEnd RPC (60s timeout).
// Use explicit timeouts to avoid the outer RPC timing out while the owner is still legitimately working.
const EXTERNAL_PUT_START_RPC_TIMEOUT_SECS: u64 = 30;
const EXTERNAL_PUT_TRANSFER_END_RPC_TIMEOUT_SECS: u64 = 30;
const EXTERNAL_RPC_P2P_RECOVER_MAX_ATTEMPTS: usize = 3;
const EXTERNAL_PUT_TRACE_LOG_WINDOW_SECS: u64 = 10;
const EXTERNAL_INIT_CONTROL_PLANE_READY_TIMEOUT_SECS: u64 = 30;
const EXTERNAL_INIT_CONTROL_PLANE_READY_POLL_MS: u64 = 100;
const EXTERNAL_INIT_CONTROL_PLANE_READY_CONSECUTIVE_SUCCESSES: usize = 2;

fn duration_to_i64_us(duration: std::time::Duration) -> i64 {
    duration.as_micros().min(i64::MAX as u128) as i64
}

struct ExternalPutTraceLogWindow {
    window_started_at: Option<Instant>,
    samples: Vec<TestPutPhaseTrace>,
}

impl ExternalPutTraceLogWindow {
    fn new() -> Self {
        Self {
            window_started_at: None,
            samples: Vec::new(),
        }
    }

    fn push_and_maybe_take(
        &mut self,
        sample: &TestPutPhaseTrace,
    ) -> Option<(Duration, Vec<TestPutPhaseTrace>)> {
        if self.window_started_at.is_none() {
            self.window_started_at = Some(Instant::now());
        }
        self.samples.push(sample.clone());
        let started_at = self
            .window_started_at
            .expect("window_started_at must exist after push");
        if started_at.elapsed() < Duration::from_secs(EXTERNAL_PUT_TRACE_LOG_WINDOW_SECS) {
            return None;
        }
        let elapsed = started_at.elapsed();
        self.window_started_at = Some(Instant::now());
        Some((elapsed, std::mem::take(&mut self.samples)))
    }
}

fn percentile_nearest_rank_us(sorted_values: &[i64], percentile: usize) -> i64 {
    let idx = ((sorted_values.len() * percentile + 99) / 100)
        .saturating_sub(1)
        .min(sorted_values.len().saturating_sub(1));
    sorted_values[idx]
}

fn summarize_external_put_trace_window(samples: &[TestPutPhaseTrace]) -> String {
    let specs: [(&str, fn(&TestPutPhaseTrace) -> i64); 13] = [
        ("external_total", |trace| trace.external_total_us),
        ("external_put_start_rpc", |trace| {
            trace.external_put_start_rpc_us
        }),
        ("external_write_payload", |trace| {
            trace.external_write_payload_us
        }),
        ("external_put_transfer_end_rpc", |trace| {
            trace.external_put_transfer_end_rpc_us
        }),
        ("owner_external_put_start_total", |trace| {
            trace.owner_external_put_start_total_us
        }),
        ("owner_put_start_total", |trace| {
            trace.owner_put_start_total_us
        }),
        ("owner_master_put_start_rpc", |trace| {
            trace.owner_master_put_start_rpc_us
        }),
        ("owner_master_put_start_server", |trace| {
            trace.owner_master_put_start_server_us
        }),
        ("owner_external_put_transfer_end_total", |trace| {
            trace.owner_external_put_transfer_end_total_us
        }),
        ("owner_put_transfer_total", |trace| {
            trace.owner_put_transfer_total_us
        }),
        ("owner_put_end_total", |trace| trace.owner_put_end_total_us),
        ("owner_master_put_end_rpc", |trace| {
            trace.owner_master_put_end_rpc_us
        }),
        ("owner_master_put_end_server", |trace| {
            trace.owner_master_put_end_server_us
        }),
    ];
    let mut parts = Vec::new();
    for (name, extract) in specs {
        let mut values: Vec<i64> = samples
            .iter()
            .map(extract)
            .filter(|value| *value > 0)
            .collect();
        if values.is_empty() {
            continue;
        }
        values.sort_unstable();
        let sum: i64 = values.iter().copied().sum();
        let avg = sum as f64 / values.len() as f64;
        let p95 = percentile_nearest_rank_us(&values, 95) as f64;
        parts.push(format!("{name}_avg_us={avg:.1} {name}_p95_us={p95:.1}"));
    }
    parts.join(" ")
}

fn stable_side_transfer_lane_for_put(put_id: (u64, u32), lane_count: usize) -> Option<u16> {
    if lane_count == 0 {
        return None;
    }
    Some((((put_id.0 ^ u64::from(put_id.1)) as usize) % lane_count) as u16)
}

#[derive(Debug, Clone)]
struct OwnerRestartPayload {
    meta: SharedJsonMeta,
    signature: SharedMetaSignature,
}

enum OwnerRestartProbe {
    Ready(OwnerRestartPayload),
    Pending(String),
}

/// Thread-safe wrapper for shared memory pointer
#[derive(Debug)]
struct SharedMemoryPtr {
    /// Start address of the writable mapped region
    ptr_rw: *mut u8,
    /// Start address of the read-only mapped region
    ptr_ro: *mut u8,
    /// Length of the mapping in bytes
    len: u64,
    /// Base directory of the shared-memory bundle (used to locate shared.json/mmap.file)
    _path: String,
    /// Handle to the mmap backing file. Keeping the FD open is harmless and simplifies lifecycle.
    _file: File,
    /// Metadata signature read from shared.json for change detection.
    memory_signature: SharedMetaSignature,
}

unsafe impl Send for SharedMemoryPtr {}
unsafe impl Sync for SharedMemoryPtr {}

impl SharedMemoryPtr {
    fn new(
        ptr_rw: *mut u8,
        ptr_ro: *mut u8,
        len: u64,
        path: String,
        file: File,
        memory_signature: SharedMetaSignature,
    ) -> Self {
        Self {
            ptr_rw,
            ptr_ro,
            len,
            _path: path,
            _file: file,
            memory_signature,
        }
    }

    fn as_ptr(&self) -> *mut u8 {
        self.ptr_rw
    }

    fn as_ptr_ro(&self) -> *mut u8 {
        self.ptr_ro
    }

    fn len(&self) -> u64 {
        self.len
    }

    fn memory_signature(&self) -> &SharedMetaSignature {
        &self.memory_signature
    }
}

define_module!(
    ExternalClientApi,
    (external_client_api, ExternalClientApi),
    (p2p, P2pModule),
    (cluster_manager, ClusterManager)
);

/// External Client configuration parameters
#[derive(Clone, Debug)]
pub struct ExternalClientApiNewArg {
    pub shared_memory_path: String,
    pub shared_file_path: String,
    pub expected_cluster_name: String,
    pub expected_protocol_version: String,
    pub enable_side_transfer: bool,
    pub short_circuit_put_payload_path: bool,
}

#[derive(Clone)]
struct CurrentOwner {
    node_id: String,
    /// Owner's node_start_time (seconds) observed from shared.json
    owner_start_time: i64,
    shared_memory: Arc<SharedMemoryPtr>,
}

struct ExternalClientApiViewHolder {
    view: OnceLock<ExternalClientApiView>,
}

impl ExternalClientApiViewHolder {
    fn new() -> Self {
        Self {
            view: OnceLock::new(),
        }
    }

    fn attach(&self, view: ExternalClientApiView) {
        // The framework attaches a module's PostView exactly once at the init barrier.
        // A second attach indicates a programming error.
        self.view
            .set(view)
            .unwrap_or_else(|_| panic!("ExternalClientApi view attached twice"));
    }

    fn clone_view(&self) -> ExternalClientApiView {
        self.view.get().unwrap().clone()
    }
}

impl std::ops::Deref for ExternalClientApiViewHolder {
    type Target = ExternalClientApiView;

    fn deref(&self) -> &Self::Target {
        self.view.get().unwrap()
    }
}

pub struct ExternalInner {
    view: ExternalClientApiViewHolder,
    current_owner: ARwLock<Option<CurrentOwner>>, // None until ready
    owner_remap_notify: Arc<Notify>,
    // Singleflight gate for waiting on owner recovery.
    // Without this, transient link jitter under high concurrency can cause a thundering herd:
    // many callers concurrently spawn owner-restart and p2p-ready wait loops.
    wait_owner_gate: ARwLock<()>,
    initial_sub_cluster: OnceLock<Option<String>>,
    expected_cluster_name: String,
    expected_protocol_version: String,
    external_shared_memory_path: String,
    external_shared_file_path: String,
    _enable_side_transfer: bool,
    short_circuit_put_payload_path: bool,
    side_rr_next: AtomicUsize,
    side_transfer_put_bindings: moka::sync::SegmentedCache<(u64, u32), (String, u16)>,
    rpc_caller_external_get: RPCCaller<ExternalGetReq>,
    rpc_caller_external_put_commit: RPCCaller<ExternalPutCommitReq>,
    rpc_caller_external_put_start: RPCCaller<ExternalPutStartReq>,
    rpc_caller_external_put_transfer_end: RPCCaller<ExternalPutTransferEndReq>,
    rpc_caller_external_delete: RPCCaller<ExternalDeleteReq>,
    rpc_caller_external_is_exist: RPCCaller<ExternalIsExistReq>,
    rpc_caller_external_delete_ack: RPCCaller<ExternalDeleteAckReq>,
    /// Lease RPC callers for external mode
    _rpc_caller_allocate_client_lease: RPCCaller<AllocateClientLeaseReq>,
    _rpc_caller_client_lease_keepalive: RPCCaller<ClientLeaseKeepaliveReq>,
    /// key -> Weak<ExternalMemHolder> index (dashmap-based)
    key_weak_memholder_index: DashMap<String, Weak<ExternalMemHolder>>,
    /// per-key semaphore (permits=1) to ensure single inflight per key
    inflight1_per_key: SemaphoreMap<String>,
    put_trace_log_window: Mutex<ExternalPutTraceLogWindow>,
}

pub struct ExternalClientApi(ExternalInner);

impl ExternalClientApi {
    /// Access inner external-only API. Safe to unwrap in external role.
    pub fn inner(&self) -> &ExternalInner {
        &self.0
    }

    pub fn attach_view(&self, view: ExternalClientApiView) {
        // This module is constructed only for the external variant; view attachment is
        // therefore an invariant.
        self.inner().view.attach(view);
    }

    pub async fn construct(arg: ExternalClientApiNewArg) -> Result<Self, KvError> {
        tracing::info!(
            "Constructing ExternalClientApi in ExternalClient mode (PreView): shm_dir={}",
            arg.shared_memory_path
        );

        Ok(Self(ExternalInner {
            view: ExternalClientApiViewHolder::new(),
            current_owner: ARwLock::new(None),
            owner_remap_notify: Arc::new(Notify::new()),
            wait_owner_gate: ARwLock::new(()),
            initial_sub_cluster: OnceLock::new(),
            expected_cluster_name: arg.expected_cluster_name,
            expected_protocol_version: arg.expected_protocol_version,
            external_shared_memory_path: arg.shared_memory_path,
            external_shared_file_path: arg.shared_file_path,
            _enable_side_transfer: arg.enable_side_transfer,
            short_circuit_put_payload_path: arg.short_circuit_put_payload_path,
            side_rr_next: AtomicUsize::new(0),
            side_transfer_put_bindings: moka::sync::Cache::builder()
                .time_to_live(Duration::from_secs(10 * 60))
                .segments(16)
                .build(),
            rpc_caller_external_get: RPCCaller::<ExternalGetReq>::new(),
            rpc_caller_external_put_commit: RPCCaller::<ExternalPutCommitReq>::new(),
            rpc_caller_external_put_start: RPCCaller::<ExternalPutStartReq>::new(),
            rpc_caller_external_put_transfer_end: RPCCaller::<ExternalPutTransferEndReq>::new(),
            rpc_caller_external_delete: RPCCaller::<ExternalDeleteReq>::new(),
            rpc_caller_external_is_exist: RPCCaller::<ExternalIsExistReq>::new(),
            rpc_caller_external_delete_ack: RPCCaller::<ExternalDeleteAckReq>::new(),
            _rpc_caller_allocate_client_lease: RPCCaller::<AllocateClientLeaseReq>::new(),
            _rpc_caller_client_lease_keepalive: RPCCaller::<ClientLeaseKeepaliveReq>::new(),
            key_weak_memholder_index: DashMap::new(),
            inflight1_per_key: SemaphoreMap::new(1, std::time::Duration::from_secs(120)),
            put_trace_log_window: Mutex::new(ExternalPutTraceLogWindow::new()),
        }))
    }

    pub async fn init2_prepare(&self) -> Result<(), KvError> {
        // Prepare external client api initialization without waiting for owner readiness.
        //
        // All owner readiness (shared.json + mmap.file + membership observation) is handled by
        // the init resource hook `owner_shared_mem_bundle_ready`.
        Ok(())
    }

    pub(crate) async fn wait_owner_shared_mem_bundle_ready_for_init_resource(
        &self,
    ) -> Result<(), KvError> {
        let ext = &self.0;

        if ext.current_owner.read().await.is_none() {
            // Initial attach: accept the current shared.json without requiring a post-wait write_ts.
            let wait_start_ts = i64::MIN;
            let OwnerRestartPayload { meta, signature } = task_wait_owner_restart(
                ext.view.clone_view(),
                ext.external_shared_memory_path.clone(),
                ext.external_shared_file_path.clone(),
                None,
                wait_start_ts,
                None,
                ext.expected_cluster_name.clone(),
                ext.expected_protocol_version.clone(),
            )
            .await?;

            let shared_memory_ptr = ExternalInner::init_shared_memory_from_meta(
                &ext.external_shared_memory_path,
                &meta,
                signature,
            )?;

            ext.initial_sub_cluster
                .set(meta.sub_cluster.clone())
                .unwrap();
            *ext.current_owner.write().await = Some(CurrentOwner {
                node_id: meta.owner_id.clone(),
                owner_start_time: meta.node_start_time,
                shared_memory: shared_memory_ptr,
            });
            ext.owner_remap_notify.notify_waiters();
        }

        // Make the resource include the cluster membership observation as well.
        self.init3_wait_owner_present().await?;
        Ok(())
    }

    pub async fn init2_after_owner_shared_mem_bundle_ready(&self) -> Result<(), KvError> {
        let ext = &self.0;

        let owner_id = ext.shared_storage_node_id().await.expect(
            "ExternalClientApi expects current_owner to be Some after owner_shared_mem_bundle_ready",
        );

        // English note:
        // Register inbound RPC handlers before any awaited etcd operations that publish or mutate
        // member metadata. Otherwise, other nodes can observe this member and send RPCs while the
        // handler set is still incomplete, leading to transient "No handler found" drops.
        //
        // Owner binding (current_owner) is already established by the init resource
        // `owner_shared_mem_bundle_ready`, so handler registration is safe here.
        ext.rpc_caller_external_get.regist(ext.view.p2p_module());
        ext.rpc_caller_external_put_commit
            .regist(ext.view.p2p_module());
        ext.rpc_caller_external_put_start
            .regist(ext.view.p2p_module());
        ext.rpc_caller_external_put_transfer_end
            .regist(ext.view.p2p_module());
        ext.rpc_caller_external_delete.regist(ext.view.p2p_module());
        ext.rpc_caller_external_is_exist
            .regist(ext.view.p2p_module());
        ext.rpc_caller_external_delete_ack
            .regist(ext.view.p2p_module());
        crate::key_prefix::init_for_p2p_owner(ext.view.p2p_module());
        crate::kvlease::init_for_p2p_owner(ext.view.p2p_module());
        crate::metrics::client::init_for_p2p_owner(ext.view.p2p_module());

        let view_ext = ext.view.clone_view();
        RPCHandler::<ExternalInvalidateWeakIndexReq>::new().regist(
            ext.view.p2p_module(),
            move |resp, msg| {
                let view = view_ext.clone();
                let view_task = view.clone();
                let _ = view.spawn("rpc_external_invalidate_weak_index", async move {
                    let result = handle_external_invalidate_weak_index(&view_task, &msg).await;
                    let _ = resp.send_resp(result).await;
                });
                Ok(())
            },
        );

        RPCCaller::<SyncKvToFileReq>::new().regist(ext.view.p2p_module());
        let view_ext = ext.view.clone_view();
        RPCHandler::<SyncKvToFileReq>::new().regist(ext.view.p2p_module(), move |resp, msg| {
            let view = view_ext.clone();
            let view_task = view.clone();
            let _ = view.spawn("rpc_sync_kv_to_file", async move {
                let result = handle_sync_kv_to_file_external(&view_task, &msg).await;
                let _ = resp.send_resp(result).await;
            });
            Ok(())
        });
        tracing::info!("ExternalClientApi RPC callers registered");

        ext.view
            .cluster_manager()
            .set_self_share_group_binding(ShareGroupOwnerRef {
                owner_id: owner_id.clone(),
                owner_start_time: ext.current_owner_start_time().await,
            })
            .await?;
        ext.view
            .cluster_manager()
            .set_self_sub_cluster(ext.initial_sub_cluster.get().unwrap().clone())
            .await
            .map_err(KvError::from)?;

        {
            let view = ext.view.clone_view();
            let view_task = view.clone();
            let _ = view.spawn("external_owner_remap_actor", async move {
                let shutdown_poller = view_task.register_shutdown_poller();
                let mut cluster_rx = view_task.cluster_manager().listen();
                let mut tick = tokio::time::interval(Duration::from_millis(200));

                loop {
                    if !shutdown_poller.is_running() {
                        tracing::info!("external owner remap actor stopped by shutdown");
                        break;
                    }

                    let Some(view_guard) = view_task.try_upgrade() else {
                        tracing::info!(
                            "external owner remap actor stopped because view was dropped"
                        );
                        break;
                    };
                    let _keep_view_alive = view_guard;

                    if let Err(err) = view_task
                        .external_client_api()
                        .inner()
                        .try_background_owner_remap_once()
                        .await
                    {
                        tracing::warn!("external owner remap actor probe failed: {}", err);
                    }

                    tokio::select! {
                        _ = tick.tick() => {}
                        recv = cluster_rx.recv() => {
                            if recv.is_err() {
                                sleep(Duration::from_millis(200)).await;
                                cluster_rx = view_task.cluster_manager().listen();
                            }
                        }
                    }
                }
            });
        }

        // Attribute local IPC bandwidth to the owner daemon (machine-level view).
        //
        // Causal chain:
        // - External<->external traffic can use the local IPC tier (iceoryx2) when both are in the
        //   same share-group (same owner_id + local_ipc_root).
        // - Topology aggregates bandwidth at the owner/machine level, so local IPC bytes must be
        //   charged to the owner, otherwise the UI under-reports throughput.
        // - We keep the P2P hot path allocation-free by recording bytes into atomics, and flush
        //   them periodically via a background task.
        {
            let cm = ext.view.cluster_manager();
            let handle = IpcBandwidthAttributorHandle::new();
            cm.attach_ipc_bandwidth_attributor_handle(handle.clone());
            if let Some(observe) = cm.observe_handle().cloned() {
                let self_member_id = cm.self_member_id().to_string();
                let owner_role = NodeRole::Client.to_string();
                let owner_id_for_task = owner_id.clone();
                let view_task = ext.view.clone_view();
                let view_task2 = view_task.clone();
                let _ = view_task.spawn("ipc_bandwidth_attributor", async move {
                    let mut shutdown_waiter = view_task2.register_shutdown_waiter();
                    let mut interval = tokio::time::interval(Duration::from_secs(
                        crate::metric_reporter::METRICS_FLUSH_INTERVAL_SECS,
                    ));

                    loop {
                        tokio::select! {
                            _ = interval.tick() => {
                                let tx_bytes = handle.take_tx_bytes();
                                if tx_bytes > 0 {
                                    observe.try_record_peer_network_bytes_override(
                                        ObserveComponent::LocalIpc,
                                        owner_id_for_task.as_str(),
                                        owner_role.as_str(),
                                        self_member_id.as_str(),
                                        ObserveDirection::Tx,
                                        tx_bytes,
                                    );
                                }
                                let rx_bytes = handle.take_rx_bytes();
                                if rx_bytes > 0 {
                                    observe.try_record_peer_network_bytes_override(
                                        ObserveComponent::LocalIpc,
                                        owner_id_for_task.as_str(),
                                        owner_role.as_str(),
                                        self_member_id.as_str(),
                                        ObserveDirection::Rx,
                                        rx_bytes,
                                    );
                                }
                            }
                            _ = shutdown_waiter.wait() => {
                                break;
                            }
                        }
                    }
                });
            } else {
                tracing::info!(
                    "ExternalClientApi local IPC bandwidth attribution disabled: ObserveHandle not attached"
                );
            }
        }
        Ok(())
    }
    pub async fn init3_wait_owner_present(&self) -> Result<(), KvError> {
        let ext = &self.0;
        let owner_id = ext
            .shared_storage_node_id()
            .await
            .expect("external role expects current_owner to be Some after init2");
        let owner_start_time = ext.current_owner_start_time().await;

        let cm = ext.view.cluster_manager();
        if cm
            .get_member_info_cached(&owner_id)
            .map(|member| member.node_start_time == owner_start_time)
            .unwrap_or(false)
        {
            return Ok(());
        }

        tracing::info!(
            "External init: waiting for owner generation to join (owner_id={} owner_start_time={})",
            owner_id,
            owner_start_time
        );
        let mut rx = cm.listen();
        loop {
            if cm
                .get_member_info_cached(&owner_id)
                .map(|member| member.node_start_time == owner_start_time)
                .unwrap_or(false)
            {
                tracing::info!(
                    "External init: owner generation observed (owner_id={} owner_start_time={})",
                    owner_id,
                    owner_start_time
                );
                return Ok(());
            }
            match rx.recv().await {
                Ok(_ev) => {
                    // Yield once to allow watcher to update member cache after emitting an event.
                    limit_thirdparty::tokio::task::yield_now().await;
                }
                Err(e) => {
                    return Err(KvError::Api(ApiError::Unknown {
                        detail: format!(
                            "cluster event channel closed while waiting for owner generation (owner_id={} owner_start_time={}): {}",
                            owner_id, owner_start_time, e
                        ),
                    }));
                }
            }
        }
    }
}

impl ExternalInner {
    async fn wait_initial_control_plane_ready(&self, phase: &'static str) -> KvResult<()> {
        let owner_id = self.shared_storage_node_id().await.expect(
            "ExternalClientApi expects current_owner to be Some before control-plane readiness wait",
        );
        self.wait_peer_send_ready(owner_id, "owner", phase).await?;

        let master_node_id = self
            .view
            .cluster_manager()
            .find_or_wait_master_node()
            .await?;
        self.wait_peer_send_ready(master_node_id, "master", phase)
            .await
    }

    async fn wait_peer_send_ready(
        &self,
        logical_target: String,
        target_role: &'static str,
        phase: &'static str,
    ) -> KvResult<()> {
        let deadline =
            Instant::now() + Duration::from_secs(EXTERNAL_INIT_CONTROL_PLANE_READY_TIMEOUT_SECS);
        let shutdown_poller = self.view.register_shutdown_poller();
        let mut attempts = 0u64;
        let mut consecutive_ready = 0usize;
        let mut last_transient_err = None;

        loop {
            if !shutdown_poller.is_running() {
                return Err(KvError::Api(ApiError::SystemShutdown {
                    detail: format!(
                        "external control-plane wait stopped during {phase}: target_role={target_role} target={logical_target}"
                    ),
                }));
            }

            let readiness = self
                .view
                .p2p_module()
                .ensure_peer_send_ready(&logical_target.clone().into())
                .await;
            match readiness {
                Ok(()) => {
                    consecutive_ready += 1;
                    if consecutive_ready >= EXTERNAL_INIT_CONTROL_PLANE_READY_CONSECUTIVE_SUCCESSES
                    {
                        if attempts > 0 {
                            tracing::info!(
                                "external control-plane route ready: phase={} target_role={} target={} attempts={}",
                                phase,
                                target_role,
                                logical_target,
                                attempts + 1,
                            );
                        }
                        return Ok(());
                    }
                }
                Err(err)
                    if matches!(
                        err,
                        crate::p2p::P2PError::NoConnectionReady { .. }
                            | crate::p2p::P2PError::NodeNotFound { .. }
                            | crate::p2p::P2PError::NodeNotConnected { .. }
                            | crate::p2p::P2PError::NodePortNotReady { .. }
                            | crate::p2p::P2PError::ConnectionError { .. }
                            | crate::p2p::P2PError::SendFailed { .. }
                            | crate::p2p::P2PError::Iceoryx2TransportNotStarted {}
                    ) =>
                {
                    consecutive_ready = 0;
                    last_transient_err = Some(err.to_string());
                }
                Err(err) => return Err(KvError::from(err)),
            }

            attempts += 1;
            if attempts == 1 || attempts % 20 == 0 {
                tracing::info!(
                    "waiting for external control-plane route: phase={} target_role={} target={} attempts={} last_transient_err={:?}",
                    phase,
                    target_role,
                    logical_target,
                    attempts,
                    last_transient_err,
                );
            }

            if Instant::now() >= deadline {
                return Err(KvError::Api(ApiError::Unknown {
                    detail: format!(
                        "timed out waiting for external control-plane route during {phase}: target_role={target_role} target={logical_target} attempts={attempts} last_transient_err={last_transient_err:?}"
                    ),
                }));
            }

            sleep(Duration::from_millis(
                EXTERNAL_INIT_CONTROL_PLANE_READY_POLL_MS,
            ))
            .await;
        }
    }

    fn maybe_log_external_put_trace_window(&self, sample: &TestPutPhaseTrace) {
        let maybe_window = {
            let mut guard = self.put_trace_log_window.lock();
            guard.push_and_maybe_take(sample)
        };
        let Some((elapsed, samples)) = maybe_window else {
            return;
        };
        if samples.is_empty() {
            return;
        }
        let summary = summarize_external_put_trace_window(&samples);
        if summary.is_empty() {
            return;
        }
        tracing::info!(
            "external_put_trace_window samples={} window_s={:.1} {}",
            samples.len(),
            elapsed.as_secs_f64(),
            summary
        );
    }

    async fn current_owner_start_time(&self) -> i64 {
        let g = self.current_owner.read().await;
        g.as_ref().map(|o| o.owner_start_time).unwrap_or_default()
    }

    async fn current_owner_snapshot(&self) -> Option<(String, i64, SharedMetaSignature)> {
        let guard = self.current_owner.read().await;
        let owner = guard.as_ref()?;
        Some((
            owner.node_id.clone(),
            owner.owner_start_time,
            owner.shared_memory.memory_signature().clone(),
        ))
    }

    async fn current_owner_base_if_advanced(
        &self,
        prev_owner_start_time: i64,
    ) -> Option<(i64, usize)> {
        let guard = self.current_owner.read().await;
        let owner = guard.as_ref()?;
        if owner.owner_start_time == prev_owner_start_time {
            return None;
        }
        Some((
            owner.owner_start_time,
            owner.shared_memory.as_ptr() as usize,
        ))
    }

    async fn owner_generation_changed_in_cluster(&self, prev_owner_start_time: i64) -> bool {
        let Some(owner_id) = self.shared_storage_node_id().await else {
            return false;
        };
        self.view
            .cluster_manager()
            .get_member_info_cached(&owner_id)
            .is_some_and(|member| member.node_start_time != prev_owner_start_time)
    }

    async fn try_background_owner_remap_once(&self) -> KvResult<bool> {
        let Some((owner_id, owner_start_time, current_signature)) =
            self.current_owner_snapshot().await
        else {
            return Ok(false);
        };

        let shared_memory_path = self.shared_memory_path();
        let shared_file_path = self.shared_file_path();
        let shared_meta_path = format!("{}/shared.json", shared_file_path);
        let probe = probe_owner_restart_payload(
            &self.view.clone_view(),
            &shared_memory_path,
            &shared_file_path,
            &shared_meta_path,
            Some(&current_signature),
            i64::MIN,
            Some(owner_id.as_str()),
            &self.expected_cluster_name,
            &self.expected_protocol_version,
        )
        .await?;

        let OwnerRestartProbe::Ready(payload) = probe else {
            return Ok(false);
        };
        if payload.meta.node_start_time == owner_start_time
            && payload.signature == current_signature
        {
            return Ok(false);
        }

        self.finish_owner_recover(&shared_memory_path, payload)
            .await?;
        Ok(true)
    }

    /// Try to get a live ExternalMemHolder from weak index.
    async fn try_get_from_weak_cache(&self, key: &str) -> Option<Arc<ExternalMemHolder>> {
        if let Some(w_ref) = self.key_weak_memholder_index.get(key) {
            let w = w_ref.value().clone();
            drop(w_ref);
            if let Some(h) = w.upgrade() {
                // Ensure holder belongs to current owner generation
                if h.owner_start_time == self.current_owner_start_time().await {
                    return Some(h);
                } else {
                    // Stale generation; remove and fall through
                    let _ = self.key_weak_memholder_index.remove(key);
                }
            } else {
                // Dead weak; remove to keep cache clean
                let _ = self.key_weak_memholder_index.remove(key);
            }
        }
        None
    }
    // Removed trivial helper: inline-match OwnerStartTimeMismatch directly where needed.
    /// 获取共享内存基址（以 usize 表示的地址）；未就绪时返回 NotConfigured
    async fn base_ptr(&self) -> KvResult<usize> {
        let lock = self.current_owner.read().await;
        if let Some(o) = lock.as_ref() {
            return Ok(o.shared_memory.as_ptr() as usize);
        }
        Err(KvError::SharedMem(SharedMemError::NotConfigured {
            node_id: self.shared_storage_node_id().await,
            detail: Some("Shared memory not ready".to_string()),
        }))
    }

    async fn base_ptr_ro(&self) -> KvResult<usize> {
        let lock = self.current_owner.read().await;
        if let Some(o) = lock.as_ref() {
            return Ok(o.shared_memory.as_ptr_ro() as usize);
        }
        Err(KvError::SharedMem(SharedMemError::NotConfigured {
            node_id: self.shared_storage_node_id().await,
            detail: Some("Shared memory not ready".to_string()),
        }))
    }

    async fn ensure_owner_ready(&self, prev_owner_start_time: &mut i64) -> KvResult<usize> {
        match self.base_ptr().await {
            Ok(addr) => Ok(addr),
            Err(_) => {
                let path = self.shared_memory_path();
                let (st, addr) = self
                    .wait_owner_recover_only(&path, *prev_owner_start_time)
                    .await?;
                *prev_owner_start_time = st;
                Ok(addr)
            }
        }
    }

    /// Note: ExternalInner is only constructed in ExternalClient role.

    async fn finish_owner_recover(
        &self,
        shared_memory_path: &str,
        payload: OwnerRestartPayload,
    ) -> KvResult<(i64, usize)> {
        self.remap_shared_memory_with_payload(shared_memory_path, &payload)
            .await?;
        self.view
            .cluster_manager()
            .set_self_share_group_binding(ShareGroupOwnerRef {
                owner_id: payload.meta.owner_id.clone(),
                owner_start_time: payload.meta.node_start_time,
            })
            .await?;
        self.view
            .cluster_manager()
            .set_self_sub_cluster(payload.meta.sub_cluster.clone())
            .await
            .map_err(KvError::from)?;
        self.wait_initial_control_plane_ready("owner_recover")
            .await?;
        let base_addr = self.base_ptr().await?;
        Ok((self.current_owner_start_time().await, base_addr))
    }

    async fn wait_owner_recover_only(
        &self,
        shared_memory_path: &str,
        prev_owner_start_time: i64,
    ) -> KvResult<(i64, usize)> {
        self.wait_owner_recover(shared_memory_path, prev_owner_start_time)
            .await
    }

    async fn recover_after_owner_start_time_mismatch(
        &self,
        prev_owner_start_time: &mut i64,
    ) -> KvResult<usize> {
        let path = self.shared_memory_path();
        let (st, addr) = self
            .wait_owner_recover_only(&path, *prev_owner_start_time)
            .await?;
        *prev_owner_start_time = st;
        Ok(addr)
    }

    async fn recover_after_p2p_error(&self, prev_owner_start_time: &mut i64) -> KvResult<usize> {
        if !self
            .owner_generation_changed_in_cluster(*prev_owner_start_time)
            .await
        {
            return match self.base_ptr().await {
                Ok(addr) => Ok(addr),
                Err(_) => {
                    let path = self.shared_memory_path();
                    let (st, addr) = self
                        .wait_owner_recover_only(&path, *prev_owner_start_time)
                        .await?;
                    *prev_owner_start_time = st;
                    Ok(addr)
                }
            };
        }

        let path = self.shared_memory_path();
        let (st, addr) = self
            .wait_owner_recover_only(&path, *prev_owner_start_time)
            .await?;
        *prev_owner_start_time = st;
        Ok(addr)
    }

    /// Wait for owner recovery until shared memory has been remapped and `owner_start_time`
    /// has advanced.
    async fn wait_owner_recover(
        &self,
        _shared_memory_path: &str,
        prev_owner_start_time: i64,
    ) -> KvResult<(i64, usize)> {
        if let Some(res) = self
            .current_owner_base_if_advanced(prev_owner_start_time)
            .await
        {
            return Ok(res);
        }

        let _wait_guard = self.wait_owner_gate.write().await;
        let shutdown_poller = self.view.register_shutdown_poller();
        let mut waited_ticks = 0u64;

        loop {
            if let Some(res) = self
                .current_owner_base_if_advanced(prev_owner_start_time)
                .await
            {
                return Ok(res);
            }
            if !shutdown_poller.is_running() {
                return Err(KvError::Api(ApiError::SystemShutdown {
                    detail: "Owner recovery wait aborted due to shutdown".to_string(),
                }));
            }

            let notified = self.owner_remap_notify.notified();
            if let Some(res) = self
                .current_owner_base_if_advanced(prev_owner_start_time)
                .await
            {
                return Ok(res);
            }
            tokio::select! {
                _ = notified => {}
                _ = sleep(Duration::from_millis(200)) => {}
            }
            waited_ticks += 1;

            if waited_ticks % 25 == 0 {
                tracing::warn!(
                    "[wait_owner_remap] waiting for owner remap... ({}s)",
                    waited_ticks / 5
                );
            }
        }
    }

    /// Read shared.json to get shared memory metadata
    fn read_shared_json(shared_meta_path: &str) -> KvResult<SharedJsonMeta> {
        let mut file = File::open(shared_meta_path).map_err(|e| {
            KvError::SharedMem(SharedMemError::MetaDataLoadError {
                path: shared_meta_path.to_string(),
                detail: format!("Failed to open shared.json: {}", e),
            })
        })?;
        let mut buf = String::new();
        use std::io::Read as _;
        file.read_to_string(&mut buf).map_err(|e| {
            KvError::SharedMem(SharedMemError::MetaDataLoadError {
                path: shared_meta_path.to_string(),
                detail: format!("Failed to read shared.json: {}", e),
            })
        })?;
        let meta: SharedJsonMeta = serde_json::from_str(&buf).map_err(|e| {
            KvError::SharedMem(SharedMemError::MetaDataLoadError {
                path: shared_meta_path.to_string(),
                detail: format!("Failed to parse shared.json: {}", e),
            })
        })?;

        Ok(meta)
    }

    fn get_shared_meta_signature(shared_meta_path: &str) -> KvResult<SharedMetaSignature> {
        fluxon_util::fs_watch::get_file_signature(shared_meta_path).map_err(KvError::from)
    }

    async fn remap_shared_memory_with_payload(
        &self,
        shared_memory_path: &str,
        payload: &OwnerRestartPayload,
    ) -> KvResult<()> {
        let shared_memory = Self::init_shared_memory_from_meta(
            shared_memory_path,
            &payload.meta,
            payload.signature.clone(),
        )?;
        let len = shared_memory.len();
        let mut lock = self.current_owner.write().await;
        if let Some(owner) = lock.as_mut() {
            owner.shared_memory = shared_memory;
            owner.owner_start_time = payload.meta.node_start_time;
            owner.node_id = payload.meta.owner_id.clone();
        } else {
            // If no owner set yet, set node_id from shared.json
            *lock = Some(CurrentOwner {
                node_id: payload.meta.owner_id.clone(),
                owner_start_time: payload.meta.node_start_time,
                shared_memory,
            });
        }
        tracing::info!(
            "[wait_owner_client_recover] Ownerclient recovered, mmap remapped: len={}",
            len
        );
        self.key_weak_memholder_index.clear();
        self.owner_remap_notify.notify_waiters();
        Ok(())
    }

    /// Initialize shared memory mapping using file path directly
    fn init_shared_memory(
        mmap_file_path: &str,
        len: u64,
        memory_signature: SharedMetaSignature,
    ) -> KvResult<Arc<SharedMemoryPtr>> {
        use std::fs::OpenOptions;
        use std::os::unix::io::AsRawFd;

        tracing::info!(
            "Initializing shared memory mapping: file={}, len={}",
            mmap_file_path,
            len
        );

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(mmap_file_path)
            .map_err(|e| {
                KvError::SharedMem(SharedMemError::MappingFailed {
                    path: mmap_file_path.to_string(),
                    len,
                    detail: format!("Failed to open shared memory file: {}", e),
                })
            })?;

        let fd = file.as_raw_fd();
        tracing::debug!("Opened shared memory file: fd={}", fd);

        unsafe {
            let addr_rw = mmap(
                std::ptr::null_mut(),
                len as usize,
                PROT_READ | PROT_WRITE,
                MAP_SHARED,
                fd,
                0,
            );

            if addr_rw == libc::MAP_FAILED {
                return Err(KvError::SharedMem(SharedMemError::MappingFailed {
                    path: mmap_file_path.to_string(),
                    len,
                    detail: "mmap failed".to_string(),
                }));
            }

            let addr_ro = mmap(
                std::ptr::null_mut(),
                len as usize,
                PROT_READ,
                MAP_SHARED,
                fd,
                0,
            );

            if addr_ro == libc::MAP_FAILED {
                libc::munmap(addr_rw, len as usize);
                return Err(KvError::SharedMem(SharedMemError::MappingFailed {
                    path: mmap_file_path.to_string(),
                    len,
                    detail: "mmap (read-only) failed".to_string(),
                }));
            }

            tracing::info!(
                "Successfully mapped shared memory: file={}, len={}, addr={:?}",
                mmap_file_path,
                len,
                addr_rw
            );
            // Store the directory path (shared memory base path), not the mmap file path.
            // Many recovery routines expect a directory path to locate memory.file and mmap.file.
            let dir_path = std::path::Path::new(mmap_file_path)
                .parent()
                .map(|p| p.to_string_lossy().to_string())
                .unwrap_or_else(|| String::new());

            Ok(Arc::new(SharedMemoryPtr::new(
                addr_rw as *mut u8,
                addr_ro as *mut u8,
                len,
                dir_path,
                file,
                memory_signature,
            )))
        }
    }

    fn init_shared_memory_from_meta(
        shared_memory_path: &str,
        meta: &SharedJsonMeta,
        memory_signature: SharedMetaSignature,
    ) -> KvResult<Arc<SharedMemoryPtr>> {
        let mmap_file_path = format!("{}/mmap.file", shared_memory_path);
        Self::init_shared_memory(&mmap_file_path, meta.segment_len, memory_signature)
    }
    /// Get the shared storage node ID this client connects to
    pub async fn shared_storage_node_id(&self) -> Option<String> {
        let g = self.current_owner.read().await;
        g.as_ref().map(|o| o.node_id.clone())
    }

    /// Get the configured shared-memory base path (external mode).
    /// Non-external modes return empty string.
    pub fn shared_memory_path(&self) -> String {
        self.external_shared_memory_path.clone()
    }

    /// Get the configured shared-file base path (external mode).
    /// Non-external modes return empty string.
    pub fn shared_file_path(&self) -> String {
        self.external_shared_file_path.clone()
    }

    fn should_fallback_side_p2p_error(err: &crate::p2p::P2PError) -> bool {
        matches!(
            err,
            crate::p2p::P2PError::NoConnectionReady { .. }
                | crate::p2p::P2PError::NodeNotFound { .. }
                | crate::p2p::P2PError::NodeNotConnected { .. }
                | crate::p2p::P2PError::NodePortNotReady { .. }
                | crate::p2p::P2PError::ConnectionError { .. }
                | crate::p2p::P2PError::SendFailed { .. }
                | crate::p2p::P2PError::StartServerError { .. }
                | crate::p2p::P2PError::Iceoryx2TransportNotStarted {}
        )
    }

    fn read_side_transfer_peer(path: &std::path::Path) -> KvResult<SideTransferPeerFileMeta> {
        let buf = std::fs::read_to_string(path).map_err(|e| {
            KvError::SharedMem(SharedMemError::MetaDataLoadError {
                path: path.to_string_lossy().to_string(),
                detail: format!("Failed to read side-transfer peer file: {}", e),
            })
        })?;
        serde_json::from_str(&buf).map_err(|e| {
            KvError::SharedMem(SharedMemError::MetaDataLoadError {
                path: path.to_string_lossy().to_string(),
                detail: format!("Failed to parse side-transfer peer file: {}", e),
            })
        })
    }

    async fn pick_side_transfer_peer(&self, put_id: Option<(u64, u32)>) -> Option<(String, u16)> {
        // External attach auto-detects owner side workers from the shared-memory peer files.
        // Owner-side config still controls whether workers exist; external callers should not
        // require an extra enable flag once the owner has published ready lanes.
        let owner_id = self.shared_storage_node_id().await?;
        let owner_start_time = self.current_owner_start_time().await;
        let peers_dir = ClientSegPool::side_transfer_peers_dir(&self.external_shared_file_path);
        let entries = std::fs::read_dir(&peers_dir).ok()?;
        let mut ready = Vec::new();
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().and_then(|s| s.to_str()) != Some("json") {
                continue;
            }
            let Ok(meta) = Self::read_side_transfer_peer(&path) else {
                continue;
            };
            if meta.owner_id != owner_id || meta.owner_start_time != owner_start_time {
                continue;
            }
            let Some(member) = self
                .view
                .cluster_manager()
                .get_member_info_cached(&meta.side_id)
            else {
                continue;
            };
            if member
                .metadata
                .get("side_transfer_worker")
                .is_some_and(|v| v == "true")
                == false
            {
                continue;
            }
            if member
                .metadata
                .get(META_KEY_SHARED_STORAGE_NODE_ID)
                .is_some_and(|v| v == &owner_id)
                == false
            {
                continue;
            }
            if member
                .metadata
                .get(META_KEY_SHARED_STORAGE_NODE_START_TIME)
                .and_then(|v| v.parse::<i64>().ok())
                != Some(owner_start_time)
            {
                continue;
            }
            let Some(lane_idx) = meta.worker_idx() else {
                continue;
            };
            ready.push((lane_idx, meta.side_id));
        }

        if ready.is_empty() {
            return None;
        }
        ready.sort_by(|lhs, rhs| lhs.cmp(rhs));
        if let Some(put_id) = put_id {
            let lane_space = ready
                .iter()
                .map(|(lane_idx, _)| usize::from(*lane_idx))
                .max()
                .map(|max_lane_idx| max_lane_idx + 1)?;
            let desired_lane = stable_side_transfer_lane_for_put(put_id, lane_space)?;
            let selected = ready
                .iter()
                .find(|(lane_idx, _)| *lane_idx == desired_lane)
                .cloned();
            if selected.is_none() {
                tracing::warn!(
                    "side-transfer desired lane not ready locally; falling back to owner: desired_lane={} lane_space={} owner_id={}",
                    desired_lane,
                    lane_space,
                    owner_id
                );
            }
            return selected.map(|(lane_idx, side_id)| (side_id, lane_idx));
        }

        let idx = self.side_rr_next.fetch_add(1, Ordering::Relaxed);
        let ready_len = ready.len();
        ready
            .into_iter()
            .nth(idx % ready_len)
            .map(|(lane_idx, side_id)| (side_id, lane_idx))
    }

    fn remember_side_transfer_binding(
        &self,
        put_id: Option<(u64, u32)>,
        binding: Option<(String, u16)>,
    ) {
        if let (Some(put_id), Some(binding)) = (put_id, binding) {
            self.side_transfer_put_bindings.insert(put_id, binding);
        }
    }

    fn bound_side_transfer_peer(&self, put_id: Option<(u64, u32)>) -> Option<(String, u16)> {
        put_id.and_then(|put_id| self.side_transfer_put_bindings.get(&put_id))
    }

    fn clear_side_transfer_binding(&self, put_id: Option<(u64, u32)>) {
        if let Some(put_id) = put_id {
            self.side_transfer_put_bindings.invalidate(&put_id);
        }
    }

    fn short_circuit_put_payload_path_enabled(&self) -> bool {
        self.short_circuit_put_payload_path
    }

    async fn call_put_start_with_side_fallback(
        &self,
        owner_id: String,
        req: MsgPack<ExternalPutStartReq>,
    ) -> KvResult<(MsgPack<ExternalPutStartResp>, Option<(String, u16)>)> {
        if let Some((side_id, lane_idx)) = self.pick_side_transfer_peer(None).await {
            match self
                .rpc_caller_external_put_start
                .call(
                    self.view.p2p_module(),
                    side_id.clone().into(),
                    req.clone(),
                    Some(Duration::from_secs(EXTERNAL_PUT_START_RPC_TIMEOUT_SECS)),
                    0,
                )
                .await
            {
                Ok(resp) => return Ok((resp, Some((side_id, lane_idx)))),
                Err(err) if Self::should_fallback_side_p2p_error(&err) => {
                    tracing::warn!(
                        "side-transfer peer unavailable for put_start; falling back to owner: side={} lane={} owner={} err={}",
                        side_id,
                        lane_idx,
                        owner_id,
                        err
                    );
                }
                Err(err) => return Err(KvError::from(err)),
            }
        }

        self.rpc_caller_external_put_start
            .call(
                self.view.p2p_module(),
                owner_id.into(),
                req,
                Some(Duration::from_secs(EXTERNAL_PUT_START_RPC_TIMEOUT_SECS)),
                0,
            )
            .await
            .map(|resp| (resp, None))
            .map_err(KvError::from)
    }

    async fn call_put_commit(
        &self,
        owner_id: String,
        req: MsgPack<ExternalPutCommitReq>,
    ) -> KvResult<MsgPack<ExternalPutCommitResp>> {
        self.rpc_caller_external_put_commit
            .call(
                self.view.p2p_module(),
                owner_id.into(),
                req,
                Some(Duration::from_secs(
                    EXTERNAL_PUT_TRANSFER_END_RPC_TIMEOUT_SECS,
                )),
                0,
            )
            .await
            .map_err(KvError::from)
    }

    async fn call_put_transfer_end_with_side_fallback(
        &self,
        owner_id: String,
        req: MsgPack<ExternalPutTransferEndReq>,
    ) -> KvResult<(MsgPack<ExternalPutTransferEndResp>, Option<(String, u16)>)> {
        let mut attempted_side = None;
        if let Some((side_id, lane_idx)) = self.bound_side_transfer_peer(req.serialize_part.put_id)
        {
            attempted_side = Some((side_id.clone(), lane_idx));
            match self
                .rpc_caller_external_put_transfer_end
                .call(
                    self.view.p2p_module(),
                    side_id.clone().into(),
                    req.clone(),
                    Some(Duration::from_secs(
                        EXTERNAL_PUT_TRANSFER_END_RPC_TIMEOUT_SECS,
                    )),
                    0,
                )
                .await
            {
                Ok(resp) => return Ok((resp, Some((side_id, lane_idx)))),
                Err(err) if Self::should_fallback_side_p2p_error(&err) => {
                    tracing::warn!(
                        "bound side-transfer peer unavailable for put_transfer_end; retrying alternate path: side={} lane={} owner={} err={}",
                        side_id,
                        lane_idx,
                        owner_id,
                        err
                    );
                }
                Err(err) => return Err(KvError::from(err)),
            }
        }

        if let Some((side_id, lane_idx)) = self
            .pick_side_transfer_peer(req.serialize_part.put_id)
            .await
        {
            if attempted_side.as_ref() != Some(&(side_id.clone(), lane_idx)) {
                match self
                    .rpc_caller_external_put_transfer_end
                    .call(
                        self.view.p2p_module(),
                        side_id.clone().into(),
                        req.clone(),
                        Some(Duration::from_secs(
                            EXTERNAL_PUT_TRANSFER_END_RPC_TIMEOUT_SECS,
                        )),
                        0,
                    )
                    .await
                {
                    Ok(resp) => return Ok((resp, Some((side_id, lane_idx)))),
                    Err(err) if Self::should_fallback_side_p2p_error(&err) => {
                        tracing::warn!(
                            "side-transfer peer unavailable; falling back to owner: side={} lane={} owner={} err={}",
                            side_id,
                            lane_idx,
                            owner_id,
                            err
                        );
                    }
                    Err(err) => return Err(KvError::from(err)),
                }
            }
        }

        self.rpc_caller_external_put_transfer_end
            .call(
                self.view.p2p_module(),
                owner_id.into(),
                req,
                Some(Duration::from_secs(
                    EXTERNAL_PUT_TRANSFER_END_RPC_TIMEOUT_SECS,
                )),
                0,
            )
            .await
            .map(|resp| (resp, None))
            .map_err(KvError::from)
    }

    /// Check if a key exists in the external storage (loop+wait)
    pub async fn is_exist(&self, key: &str) -> KvResult<bool> {
        tracing::debug!("External is_exist request for key: {}", key);
        let mut prev_owner_start_time = self.current_owner_start_time().await;
        let mut recover_attempts = 0usize;
        if self.base_ptr().await.is_err() {
            let path = self.shared_memory_path();
            tracing::info!("ExternalClientApi.is_exist waiting for owner at: {}", path);
            let _ = self.ensure_owner_ready(&mut prev_owner_start_time).await?;
        }

        loop {
            let req = MsgPack {
                serialize_part: ExternalIsExistReq {
                    key: key.to_string(),
                    started_time: self.current_owner_start_time().await,
                },
                raw_bytes: Vec::new(),
            };

            let owner = self.shared_storage_node_id().await.ok_or_else(|| {
                KvError::SharedMem(SharedMemError::NotConfigured {
                    node_id: None,
                    detail: Some("Shared storage node id unavailable".to_string()),
                })
            })?;
            let resp = match self
                .rpc_caller_external_is_exist
                .call(self.view.p2p_module(), owner.into(), req, None, 0)
                .await
            {
                Ok(resp) => resp,
                Err(e) => {
                    let err = KvError::from(e);
                    if matches!(&err, KvError::P2p(_))
                        && recover_attempts < EXTERNAL_RPC_P2P_RECOVER_MAX_ATTEMPTS
                    {
                        recover_attempts += 1;
                        tracing::warn!(
                            "is_exist: transient P2P error; retrying after owner-state recovery check: key={}, attempt={}/{}, err={}",
                            key,
                            recover_attempts,
                            EXTERNAL_RPC_P2P_RECOVER_MAX_ATTEMPTS,
                            err
                        );
                        let _ = self
                            .recover_after_p2p_error(&mut prev_owner_start_time)
                            .await?;
                        continue;
                    }
                    return Err(err);
                }
            };

            match resp.serialize_part.to_result() {
                Ok(exists) => break Ok(exists),
                Err(e) => {
                    if matches!(&e, KvError::Api(ApiError::OwnerStartTimeMismatch { .. })) {
                        tracing::warn!("is_exist: OwnerStartTimeMismatch; remapping and retrying");
                        let _ = self
                            .recover_after_owner_start_time_mismatch(&mut prev_owner_start_time)
                            .await?;
                        continue;
                    }
                    if matches!(&e, KvError::P2p(_))
                        && recover_attempts < EXTERNAL_RPC_P2P_RECOVER_MAX_ATTEMPTS
                    {
                        recover_attempts += 1;
                        tracing::warn!(
                            "is_exist: transient P2P error; retrying after owner-state recovery check: key={}, attempt={}/{}, err={}",
                            key,
                            recover_attempts,
                            EXTERNAL_RPC_P2P_RECOVER_MAX_ATTEMPTS,
                            e
                        );
                        let _ = self
                            .recover_after_p2p_error(&mut prev_owner_start_time)
                            .await?;
                        continue;
                    }
                    tracing::warn!("External is_exist failed for key: {}, error: {}", key, e);
                    break Err(e);
                }
            }
        }
    }

    /// External Get operation (outer): retry + wait wrapper around get_inner
    pub async fn get(
        &self,
        key: &str,
    ) -> KvResult<Option<Arc<crate::memholder::ExternalMemHolder>>> {
        tracing::debug!("External get request for key: {}", key);

        // Ensure external mode configured; if not, block until owner is ready once
        let mut prev_owner_start_time = self.current_owner_start_time().await;
        if self.base_ptr().await.is_err() {
            let path = self.shared_memory_path();
            tracing::info!(
                "ExternalClientApi.get detected unmapped shared memory; waiting at: {}",
                path
            );
            let _ = self.ensure_owner_ready(&mut prev_owner_start_time).await?;
        }

        // 1) Fast path: try weak-index lookup first
        if let Some(h) = self.try_get_from_weak_cache(key).await {
            return Ok(Some(h));
        }

        // 2) Ensure only one inflight get() per key using a keyed semaphore (permits=1)
        tracing::debug!(
            "External get request for key: {} acquire inflight semaphore",
            key
        );
        let permit = self.inflight1_per_key.acquire(key.to_string()).await;

        // 3) Re-check weak cache after acquiring the per-key lock
        if let Some(h) = self.try_get_from_weak_cache(key).await {
            tracing::debug!(
                "External get request for key: {} hit by other inflight",
                key
            );
            drop(permit);
            return Ok(Some(h));
        }

        let mut recover_attempts: usize = 0;

        loop {
            tracing::debug!(
                "External get request for key: {} inflight get start once",
                key
            );
            match self.get_inner(key, prev_owner_start_time).await {
                Ok(v) => {
                    // Update weak index on success if Some
                    if let Some(ref h) = v {
                        // let hex= &h.bytes()[..std::cmp::min(16, h.len as usize)];
                        // tracing::info!("external get done, key={}, partial_hex={:?}", key, hex);
                        self.key_weak_memholder_index
                            .insert(key.to_string(), Arc::downgrade(h));
                    } else {
                        tracing::debug!("external get no key={}", key);
                    }
                    drop(permit);
                    break Ok(v);
                }
                Err(e) => {
                    if matches!(&e, KvError::Api(ApiError::OwnerStartTimeMismatch { .. })) {
                        tracing::warn!("get: OwnerStartTimeMismatch; remapping and retrying");
                        let _ = self
                            .recover_after_owner_start_time_mismatch(&mut prev_owner_start_time)
                            .await?;
                        continue;
                    }
                    if matches!(&e, KvError::P2p(_))
                        && recover_attempts < EXTERNAL_RPC_P2P_RECOVER_MAX_ATTEMPTS
                    {
                        recover_attempts += 1;
                        tracing::warn!(
                            "get: transient P2P error; retrying after owner-state recovery check: \
key={}, attempt={}/{}, err={}",
                            key,
                            recover_attempts,
                            EXTERNAL_RPC_P2P_RECOVER_MAX_ATTEMPTS,
                            e
                        );
                        let _ = self
                            .recover_after_p2p_error(&mut prev_owner_start_time)
                            .await?;
                        continue;
                    }
                    drop(permit);
                    break Err(e);
                }
            }
        }
    }

    /// Single-attempt inner get: one RPC, compute base+offset to build memholder
    async fn get_inner(
        &self,
        key: &str,
        started_time: i64,
    ) -> KvResult<Option<Arc<crate::memholder::ExternalMemHolder>>> {
        // Ensure external mode configured and compute base address
        let base_ptr = self.base_ptr_ro().await.expect(
            "ExternalClientApi.get_inner called in non-external mode (no shared memory configured)",
        ) as u64;

        let req = MsgPack {
            serialize_part: ExternalGetReq {
                key: key.to_string(),
                req_node_id: self.view.cluster_manager().get_self_info().id.clone(),
                started_time,
            },
            raw_bytes: Vec::new(),
        };

        let owner = self.shared_storage_node_id().await.ok_or_else(|| {
            KvError::SharedMem(SharedMemError::NotConfigured {
                node_id: None,
                detail: Some("Shared storage node id unavailable".to_string()),
            })
        })?;
        tracing::debug!(
            "External get inner rpc start: key={}, owner={}, started_time={}",
            key,
            owner,
            started_time
        );
        let owner_node: crate::cluster_manager::NodeID = owner.clone().into();
        let resp = self
            .rpc_caller_external_get
            .call(self.view.p2p_module(), owner_node, req, None, 0)
            .await
            .map_err(KvError::from)?;
        tracing::debug!("External get inner rpc returned: key={}", key);

        let result = resp.serialize_part.to_result()?;
        tracing::debug!(
            "External get inner rpc parsed: key={}, has_memholder={}",
            key,
            result.is_some()
        );
        match result {
            Some(info) => {
                // Attribute external<->owner shared-memory payload bytes to the owner topology edge.
                //
                // Causal chain:
                // - External GET does not transfer the value bytes via P2P raw_bytes or transfer engines.
                // - The owner returns only (offset,len) and the external reads payload by mmap'ing the owner's
                //   shared memory file and slicing `base_ptr_ro + offset`.
                // - Therefore `kv_peer_network_bytes_total` would only reflect small RPC metadata unless we
                //   explicitly charge payload bytes here.
                // - We reuse the existing local IPC attributor (async flusher) to keep the hot path cheap and
                //   to attribute bytes under (node=owner_id, role=client, peer=external_id).
                if info.len > 0 {
                    let cm = self.view.cluster_manager();
                    let handle = cm.ipc_bandwidth_attributor_handle().expect(
                        "ExternalClientApi.get_inner expects IpcBandwidthAttributor handle to be attached",
                    );
                    handle.record_tx_bytes(info.len as u64);
                }

                let external_client_id = self.view.cluster_manager().get_self_info().id;
                let addr = base_ptr + info.offset;
                let external_memholder = Arc::new(ExternalMemHolder::new(
                    info.offset,
                    addr,
                    info.len,
                    info.holder_id,
                    key.to_string(),
                    external_client_id,
                    self.view.clone(),
                    started_time,
                ));
                tracing::debug!(
                    "External get inner memholder built: key={}, offset={}, len={}, holder_id={}",
                    key,
                    info.offset,
                    info.len,
                    info.holder_id
                );
                Ok(Some(external_memholder))
            }
            None => Ok(None),
        }
    }

    /// External Put operation using staged approach (PutStart -> Transfer -> PutEnd)
    pub async fn put(
        &self,
        key: &str,
        value: &[u8],
        opts: crate::client_kv_api::PutOptionalArgs,
    ) -> KvResult<()> {
        let lease_id = opts.lease_id();
        let reject_if_inflight_same_key = opts.reject_if_inflight_same_key();
        let preferred_sub_cluster = opts.preferred_sub_cluster().map(|s| s.to_string());
        let observe_sink = opts.test_observe_put_phases();
        let observe_enabled = true;
        let total_started_at = Instant::now();
        tracing::debug!(
            "External put request for key: {}, data length: {}",
            key,
            value.len()
        );
        let mut prev_owner_start_time = self.current_owner_start_time().await;
        let mut base_addr: usize = match self.base_ptr().await {
            Ok(addr) => addr,
            Err(_) => {
                let path = self.shared_memory_path();
                tracing::info!(
                    "ExternalClientApi.put detected unmapped shared memory; waiting for owner to be ready at path: {}",
                    path
                );
                self.ensure_owner_ready(&mut prev_owner_start_time).await?
            }
        };

        // Outer retry loop: remap + retry on recoverable conditions until success or non-retryable error.
        // Recoverable conditions:
        // - OwnerStartTimeMismatch (owner restarted)
        // - Any P2P transport error (owner offline / link down): NodeNotConnected, ConnectionError, Timeout, SendFailed, etc.
        loop {
            match self
                .put_inner(
                    key,
                    value,
                    prev_owner_start_time,
                    base_addr,
                    lease_id,
                    reject_if_inflight_same_key,
                    preferred_sub_cluster.as_deref(),
                    observe_enabled,
                )
                .await
            {
                Ok(mut trace) => {
                    trace.external_total_us = duration_to_i64_us(total_started_at.elapsed());
                    self.maybe_log_external_put_trace_window(&trace);
                    if let Some(sink) = observe_sink.as_ref() {
                        *sink.lock() = Some(trace);
                    }
                    break Ok(());
                }
                Err(e) => {
                    // If owner restarted, remap and retry
                    if matches!(&e, KvError::Api(ApiError::OwnerStartTimeMismatch { .. })) {
                        tracing::warn!("put: OwnerStartTimeMismatch; remapping and retrying");
                        base_addr = self
                            .recover_after_owner_start_time_mismatch(&mut prev_owner_start_time)
                            .await?;
                        continue;
                    }
                    // If P2P reports connectivity issues, re-check owner generation before retrying.
                    if matches!(&e, KvError::P2p(_)) {
                        tracing::warn!(
                            "put: P2P error (owner/link likely offline); retrying after owner-state recovery check: {}",
                            e
                        );
                        base_addr = self
                            .recover_after_p2p_error(&mut prev_owner_start_time)
                            .await?;
                        continue;
                    }
                    // Non-recoverable error: return immediately
                    break Err(e);
                }
            }
        }
    }

    /// External Put operation by encoding a flat dict from raw pointers directly into shared memory.
    ///
    /// # Safety
    /// The caller must guarantee the pointer ranges remain readable for the duration of this async call.
    pub async unsafe fn put_flat_dict_ptrs(
        &self,
        key: &str,
        ptrs: Vec<(u8, usize, u32, u64, u32, Option<u32>)>,
        opts: crate::client_kv_api::PutOptionalArgs,
    ) -> KvResult<()> {
        let lease_id = opts.lease_id();
        let reject_if_inflight_same_key = opts.reject_if_inflight_same_key();
        let preferred_sub_cluster = opts.preferred_sub_cluster().map(|s| s.to_string());
        let observe_sink = opts.test_observe_put_phases();
        let observe_enabled = true;
        let total_started_at = Instant::now();
        let payload_len = crate::memholder::kvclient_encode::calc_flat_dict_encoded_len(&ptrs)?;
        tracing::debug!(
            "External put_flat_dict_ptrs request for key: {}, data length: {}",
            key,
            payload_len
        );

        let mut prev_owner_start_time = self.current_owner_start_time().await;
        let mut base_addr: usize = match self.base_ptr().await {
            Ok(addr) => addr,
            Err(_) => {
                let path = self.shared_memory_path();
                tracing::info!(
                    "ExternalClientApi.put_flat_dict_ptrs detected unmapped shared memory; waiting for owner to be ready at path: {}",
                    path
                );
                self.ensure_owner_ready(&mut prev_owner_start_time).await?
            }
        };

        loop {
            match unsafe {
                self.put_inner_flat_dict_ptrs(
                    key,
                    &ptrs,
                    payload_len,
                    prev_owner_start_time,
                    base_addr,
                    lease_id,
                    reject_if_inflight_same_key,
                    preferred_sub_cluster.as_deref(),
                    observe_enabled,
                )
                .await
            } {
                Ok(mut trace) => {
                    trace.external_total_us = duration_to_i64_us(total_started_at.elapsed());
                    self.maybe_log_external_put_trace_window(&trace);
                    if let Some(sink) = observe_sink.as_ref() {
                        *sink.lock() = Some(trace);
                    }
                    break Ok(());
                }
                Err(e) => {
                    if matches!(&e, KvError::Api(ApiError::OwnerStartTimeMismatch { .. })) {
                        tracing::warn!(
                            "put_flat_dict_ptrs: OwnerStartTimeMismatch; remapping and retrying"
                        );
                        base_addr = self
                            .recover_after_owner_start_time_mismatch(&mut prev_owner_start_time)
                            .await?;
                        continue;
                    }
                    if matches!(&e, KvError::P2p(_)) {
                        tracing::warn!(
                            "put_flat_dict_ptrs: P2P error (owner/link likely offline); retrying after owner-state recovery check: {}",
                            e
                        );
                        base_addr = self
                            .recover_after_p2p_error(&mut prev_owner_start_time)
                            .await?;
                        continue;
                    }
                    break Err(e);
                }
            }
        }
    }

    async unsafe fn put_inner_flat_dict_ptrs(
        &self,
        key: &str,
        ptrs: &[(u8, usize, u32, u64, u32, Option<u32>)],
        payload_len: u64,
        started_time: i64,
        base_addr: usize,
        lease_id: Option<u64>,
        reject_if_inflight_same_key: bool,
        preferred_sub_cluster: Option<&str>,
        observe_enabled: bool,
    ) -> KvResult<TestPutPhaseTrace> {
        let mut trace = TestPutPhaseTrace::default();
        let put_start_req = MsgPack {
            serialize_part: ExternalPutStartReq {
                key: key.to_string(),
                len: payload_len,
                reject_if_inflight_same_key,
                preferred_sub_cluster: preferred_sub_cluster.map(|s| s.to_string()),
                started_time,
                test_observe_put_phases: true,
            },
            raw_bytes: Vec::new(),
        };
        let owner = self.shared_storage_node_id().await.ok_or_else(|| {
            KvError::SharedMem(SharedMemError::NotConfigured {
                node_id: None,
                detail: Some("Shared storage node id unavailable".to_string()),
            })
        })?;
        let put_start_rpc_started_at = observe_enabled.then(Instant::now);
        let (put_resp, put_start_side) = self
            .call_put_start_with_side_fallback(owner, put_start_req)
            .await?;
        if let Some(started_at) = put_start_rpc_started_at {
            trace.external_put_start_rpc_us = duration_to_i64_us(started_at.elapsed());
        }
        let put_start_trace = put_resp.serialize_part.test_put_phase_trace.clone();
        if let Some(owner_trace) = put_start_trace.as_ref() {
            trace.merge_from(owner_trace);
        }
        let put_start_ok = put_resp.serialize_part.clone().to_result()?;
        if let Some((side_id, lane_idx)) = put_start_side.clone() {
            trace.external_side_transfer_peer_id = Some(side_id);
            trace.external_side_transfer_lane_idx = Some(lane_idx);
        }
        self.remember_side_transfer_binding(put_start_ok.put_id, put_start_side);

        if self.short_circuit_put_payload_path_enabled() {
            let commit_req = MsgPack {
                serialize_part: ExternalPutCommitReq {
                    key: key.to_string(),
                    put_id: put_start_ok.put_id,
                    lease_id,
                    started_time,
                    test_observe_put_phases: true,
                },
                raw_bytes: Vec::new(),
            };
            let owner = self.shared_storage_node_id().await.ok_or_else(|| {
                KvError::SharedMem(SharedMemError::NotConfigured {
                    node_id: None,
                    detail: Some("Shared storage node id unavailable".to_string()),
                })
            })?;
            let commit_rpc_started_at = observe_enabled.then(Instant::now);
            let commit_resp = self.call_put_commit(owner, commit_req).await;
            self.clear_side_transfer_binding(put_start_ok.put_id);
            let commit_resp = commit_resp?;
            if let Some(started_at) = commit_rpc_started_at {
                trace.external_put_transfer_end_rpc_us = duration_to_i64_us(started_at.elapsed());
            }
            if let Some(owner_trace) = commit_resp.serialize_part.test_put_phase_trace.as_ref() {
                trace.merge_from(owner_trace);
            }
            commit_resp.serialize_part.to_result()?;
            tracing::debug!(
                "External put_flat_dict_ptrs short-circuited payload path for key: {}",
                key
            );
            return Ok(trace);
        }

        let write_started_at = observe_enabled.then(Instant::now);
        if put_start_ok.src_offset == put_start_ok.target_offset {
            tracing::debug!(
                "put_inner_flat_dict_ptrs(local): write to target_offset={}",
                put_start_ok.target_offset
            );
            let target_ptr = (base_addr + put_start_ok.target_offset as usize) as *mut u8;
            unsafe {
                crate::memholder::kvclient_encode::write_flat_dict_ptrs_to_ptr(target_ptr, ptrs);
            }
        } else {
            tracing::debug!(
                "put_inner_flat_dict_ptrs(remote): write to src_offset={}, then transfer",
                put_start_ok.src_offset
            );
            let src_ptr = (base_addr + put_start_ok.src_offset as usize) as *mut u8;
            unsafe {
                crate::memholder::kvclient_encode::write_flat_dict_ptrs_to_ptr(src_ptr, ptrs);
            }
        }
        if let Some(started_at) = write_started_at {
            trace.external_write_payload_us = duration_to_i64_us(started_at.elapsed());
        }

        let end_req = MsgPack {
            serialize_part: ExternalPutTransferEndReq {
                key: key.to_string(),
                len: payload_len,
                src_offset: put_start_ok.src_offset,
                target_offset: put_start_ok
                    .transfer_target_offset
                    .unwrap_or(put_start_ok.target_offset),
                peer_id: put_start_ok.peer_id.clone(),
                target_base_addr: if put_start_ok.peer_id.is_some() {
                    Some(put_start_ok.target_base_addr)
                } else {
                    None
                },
                put_id: put_start_ok.put_id.clone(),
                lease_id,
                started_time,
                test_observe_put_phases: true,
            },
            raw_bytes: Vec::new(),
        };
        let owner = self.shared_storage_node_id().await.ok_or_else(|| {
            KvError::SharedMem(SharedMemError::NotConfigured {
                node_id: None,
                detail: Some("Shared storage node id unavailable".to_string()),
            })
        })?;
        let end_rpc_started_at = observe_enabled.then(Instant::now);
        let end_result = self
            .call_put_transfer_end_with_side_fallback(owner, end_req)
            .await;
        self.clear_side_transfer_binding(put_start_ok.put_id);
        let (end_resp, selected_side) = end_result?;
        if let Some(started_at) = end_rpc_started_at {
            trace.external_put_transfer_end_rpc_us = duration_to_i64_us(started_at.elapsed());
        }
        if let Some((side_id, lane_idx)) = selected_side {
            trace.external_side_transfer_peer_id = Some(side_id);
            trace.external_side_transfer_lane_idx = Some(lane_idx);
        }
        if let Some(owner_trace) = end_resp.serialize_part.test_put_phase_trace.as_ref() {
            trace.merge_from(owner_trace);
        }
        end_resp.serialize_part.to_result()?;

        tracing::debug!("External put_flat_dict_ptrs successful for key: {}", key);
        Ok(trace)
    }

    /// Inner put without recovery/remap logic.
    /// Two phases per canvas: (1) compute addresses + copy, (2) trigger transfer and end
    async fn put_inner(
        &self,
        key: &str,
        value: &[u8],
        started_time: i64,
        base_addr: usize,
        lease_id: Option<u64>,
        reject_if_inflight_same_key: bool,
        preferred_sub_cluster: Option<&str>,
        observe_enabled: bool,
    ) -> KvResult<TestPutPhaseTrace> {
        let mut trace = TestPutPhaseTrace::default();
        // Phase 0: Put Start - request allocation (returns src/target offsets and optional peer)
        let put_start_req = MsgPack {
            serialize_part: ExternalPutStartReq {
                key: key.to_string(),
                len: value.len() as u64,
                reject_if_inflight_same_key,
                preferred_sub_cluster: preferred_sub_cluster.map(|s| s.to_string()),
                started_time,
                test_observe_put_phases: true,
            },
            raw_bytes: Vec::new(),
        };
        let owner = self.shared_storage_node_id().await.ok_or_else(|| {
            KvError::SharedMem(SharedMemError::NotConfigured {
                node_id: None,
                detail: Some("Shared storage node id unavailable".to_string()),
            })
        })?;
        let put_start_rpc_started_at = observe_enabled.then(Instant::now);
        let (put_resp, put_start_side) = self
            .call_put_start_with_side_fallback(owner, put_start_req)
            .await?;
        if let Some(started_at) = put_start_rpc_started_at {
            trace.external_put_start_rpc_us = duration_to_i64_us(started_at.elapsed());
        }
        if let Some(owner_trace) = put_resp.serialize_part.test_put_phase_trace.as_ref() {
            trace.merge_from(owner_trace);
        }
        let put_start_ok = put_resp.serialize_part.clone().to_result()?; // propagate error directly
        if let Some((side_id, lane_idx)) = put_start_side.clone() {
            trace.external_side_transfer_peer_id = Some(side_id);
            trace.external_side_transfer_lane_idx = Some(lane_idx);
        }
        self.remember_side_transfer_binding(put_start_ok.put_id, put_start_side);

        if self.short_circuit_put_payload_path_enabled() {
            let commit_req = MsgPack {
                serialize_part: ExternalPutCommitReq {
                    key: key.to_string(),
                    put_id: put_start_ok.put_id,
                    lease_id,
                    started_time,
                    test_observe_put_phases: true,
                },
                raw_bytes: Vec::new(),
            };
            let owner = self.shared_storage_node_id().await.ok_or_else(|| {
                KvError::SharedMem(SharedMemError::NotConfigured {
                    node_id: None,
                    detail: Some("Shared storage node id unavailable".to_string()),
                })
            })?;
            let commit_rpc_started_at = observe_enabled.then(Instant::now);
            let commit_resp = self.call_put_commit(owner, commit_req).await;
            self.clear_side_transfer_binding(put_start_ok.put_id);
            let commit_resp = commit_resp?;
            if let Some(started_at) = commit_rpc_started_at {
                trace.external_put_transfer_end_rpc_us = duration_to_i64_us(started_at.elapsed());
            }
            if let Some(owner_trace) = commit_resp.serialize_part.test_put_phase_trace.as_ref() {
                trace.merge_from(owner_trace);
            }
            commit_resp.serialize_part.to_result()?;
            tracing::debug!("External put short-circuited payload path for key: {}", key);
            return Ok(trace);
        }

        // Phase 1: compute addresses + copy
        let write_started_at = observe_enabled.then(Instant::now);
        unsafe {
            if put_start_ok.src_offset == put_start_ok.target_offset {
                // Local path: copy directly to target
                tracing::debug!(
                    "put_inner(local): memcpy to target_offset={}",
                    put_start_ok.target_offset
                );
                let target_ptr = (base_addr + put_start_ok.target_offset as usize) as *mut u8;
                std::ptr::copy_nonoverlapping(value.as_ptr(), target_ptr, value.len());
            } else {
                // Remote path: copy to src; owner will transfer from src->target via RPC below
                tracing::debug!(
                    "put_inner(remote): memcpy to src_offset={}, then transfer",
                    put_start_ok.src_offset
                );
                let src_ptr = (base_addr + put_start_ok.src_offset as usize) as *mut u8;
                std::ptr::copy_nonoverlapping(value.as_ptr(), src_ptr, value.len());
            }
        }
        if let Some(started_at) = write_started_at {
            trace.external_write_payload_us = duration_to_i64_us(started_at.elapsed());
        }

        // Attribute external<->owner shared-memory payload bytes to the owner topology edge.
        //
        // Causal chain:
        // - External PUT writes the payload directly into the owner's shared memory (memcpy into mmap).
        // - The control-plane RPC only carries offsets/ids, so peer network bytes would under-report without
        //   explicitly charging payload bytes here.
        // - Direction is "rx" on the owner->external edge (owner receives from external).
        if !value.is_empty() {
            let cm = self.view.cluster_manager();
            let handle = cm.ipc_bandwidth_attributor_handle().expect(
                "ExternalClientApi.put_inner expects IpcBandwidthAttributor handle to be attached",
            );
            handle.record_rx_bytes(value.len() as u64);
        }

        // Phase 2: trigger transfer (if needed) and end in one RPC
        let end_req = MsgPack {
            serialize_part: ExternalPutTransferEndReq {
                key: key.to_string(),
                len: value.len() as u64,
                src_offset: put_start_ok.src_offset,
                target_offset: put_start_ok
                    .transfer_target_offset
                    .unwrap_or(put_start_ok.target_offset),
                peer_id: put_start_ok.peer_id.clone(),
                target_base_addr: if put_start_ok.peer_id.is_some() {
                    Some(put_start_ok.target_base_addr)
                } else {
                    None
                },
                put_id: put_start_ok.put_id.clone(),
                lease_id,
                started_time,
                test_observe_put_phases: true,
            },
            raw_bytes: Vec::new(),
        };
        let owner = self.shared_storage_node_id().await.ok_or_else(|| {
            KvError::SharedMem(SharedMemError::NotConfigured {
                node_id: None,
                detail: Some("Shared storage node id unavailable".to_string()),
            })
        })?;
        let end_rpc_started_at = observe_enabled.then(Instant::now);
        let end_result = self
            .call_put_transfer_end_with_side_fallback(owner, end_req)
            .await;
        self.clear_side_transfer_binding(put_start_ok.put_id);
        let (end_resp, selected_side) = end_result?;
        if let Some(started_at) = end_rpc_started_at {
            trace.external_put_transfer_end_rpc_us = duration_to_i64_us(started_at.elapsed());
        }
        if let Some((side_id, lane_idx)) = selected_side {
            trace.external_side_transfer_peer_id = Some(side_id);
            trace.external_side_transfer_lane_idx = Some(lane_idx);
        }
        if let Some(owner_trace) = end_resp.serialize_part.test_put_phase_trace.as_ref() {
            trace.merge_from(owner_trace);
        }
        end_resp.serialize_part.to_result()?;

        tracing::debug!("External put successful for key: {}", key);
        Ok(trace)
    }
    /// External Delete operation
    pub async fn delete(&self, key: &str) -> KvResult<()> {
        tracing::debug!("External delete request for key: {}", key);
        let mut prev_owner_start_time = self.current_owner_start_time().await;
        let mut recover_attempts = 0usize;
        if self.base_ptr().await.is_err() {
            let path = self.shared_memory_path();
            tracing::info!("ExternalClientApi.delete waiting for owner at: {}", path);
            let _ = self.ensure_owner_ready(&mut prev_owner_start_time).await?;
        }

        loop {
            let req = MsgPack {
                serialize_part: ExternalDeleteReq {
                    key: key.to_string(),
                    started_time: self.current_owner_start_time().await,
                },
                raw_bytes: Vec::new(),
            };

            let owner = self.shared_storage_node_id().await.ok_or_else(|| {
                KvError::SharedMem(SharedMemError::NotConfigured {
                    node_id: None,
                    detail: Some("Shared storage node id unavailable".to_string()),
                })
            })?;
            let resp = match self
                .rpc_caller_external_delete
                .call(self.view.p2p_module(), owner.into(), req, None, 0)
                .await
            {
                Ok(resp) => resp,
                Err(e) => {
                    let err = KvError::from(e);
                    if matches!(&err, KvError::P2p(_))
                        && recover_attempts < EXTERNAL_RPC_P2P_RECOVER_MAX_ATTEMPTS
                    {
                        recover_attempts += 1;
                        tracing::warn!(
                            "delete: transient P2P error; retrying after owner-state recovery check: key={}, attempt={}/{}, err={}",
                            key,
                            recover_attempts,
                            EXTERNAL_RPC_P2P_RECOVER_MAX_ATTEMPTS,
                            err
                        );
                        let _ = self
                            .recover_after_p2p_error(&mut prev_owner_start_time)
                            .await?;
                        continue;
                    }
                    return Err(err);
                }
            };

            if let Err(e) = resp.serialize_part.to_result() {
                if matches!(&e, KvError::Api(ApiError::OwnerStartTimeMismatch { .. })) {
                    tracing::warn!("delete: OwnerStartTimeMismatch; remapping and retrying");
                    let _ = self
                        .recover_after_owner_start_time_mismatch(&mut prev_owner_start_time)
                        .await?;
                    continue;
                }
                if matches!(&e, KvError::P2p(_))
                    && recover_attempts < EXTERNAL_RPC_P2P_RECOVER_MAX_ATTEMPTS
                {
                    recover_attempts += 1;
                    tracing::warn!(
                        "delete: transient P2P error; retrying after owner-state recovery check: key={}, attempt={}/{}, err={}",
                        key,
                        recover_attempts,
                        EXTERNAL_RPC_P2P_RECOVER_MAX_ATTEMPTS,
                        e
                    );
                    let _ = self
                        .recover_after_p2p_error(&mut prev_owner_start_time)
                        .await?;
                    continue;
                }
                return Err(e);
            }
            tracing::debug!("External delete successful for key: {}", key);
            break Ok(());
        }
    }

    /// Send external_delete_ack to the main client
    /// 语义：
    /// - 用于通知 owner 端：external 侧不再持有该 memholder。
    /// - 若返回 OwnerStartTimeMismatch，说明 owner 已重启，旧 memholder 一定失效，直接视为“取消 ack”（无需重试、无需 remap），返回 Ok(())。
    /// - 其它错误正常向外返回。
    pub async fn send_external_delete_ack(
        &self,
        key: &str,
        external_client_id: &str,
        holder_id: u64,
        started_time: i64,
    ) -> KvResult<()> {
        tracing::debug!(
            "Sending external_delete_ack: key={}, external_client_id={}, holder_id={}",
            key,
            external_client_id,
            holder_id
        );
        // Assert: ensure external mode configured
        let _ = self
            .base_ptr().await
            .expect("ExternalClientApi.send_external_delete_ack called in non-external mode (no shared memory configured)");

        let req = MsgPack {
            serialize_part: ExternalDeleteAckReq {
                key: key.to_string(),
                external_client_id: external_client_id.to_string(),
                holder_id,
                started_time,
            },
            raw_bytes: Vec::new(),
        };

        let owner = self.shared_storage_node_id().await.ok_or_else(|| {
            KvError::SharedMem(SharedMemError::NotConfigured {
                node_id: None,
                detail: Some("Shared storage node id unavailable".to_string()),
            })
        })?;
        let resp = self
            .rpc_caller_external_delete_ack
            .call(self.view.p2p_module(), owner.into(), req, None, 0)
            .await
            .map_err(KvError::from)?;
        if let Err(e) = resp.serialize_part.to_result() {
            if matches!(&e, KvError::Api(ApiError::OwnerStartTimeMismatch { .. })) {
                tracing::info!(
                    "external_delete_ack: owner start_time mismatch; owner restarted; cancel ack and return Ok"
                );
                return Ok(());
            }
            return Err(e);
        }
        tracing::debug!(
            "External delete ack processed: key={}, external_client_id={}, holder_id={}",
            key,
            external_client_id,
            holder_id
        );
        Ok(())
    }

    /// Allocate a client lease (external role): send request to master via P2P.
    ///
    /// Semantics:
    /// - `ttl_seconds` must be >= the master-side minimum client lease TTL
    ///   (see MasterLeaseManager::MIN_CLIENT_TTL_SECONDS, currently 90 seconds).
    /// - Smaller values (including 0) are invalid and will cause the master
    ///   to return `LeaseMgrError::InvalidTTL`.
    pub async fn allocate_lease(&self, ttl_seconds: u64) -> KvResult<u64> {
        crate::kvlease::allocate_lease(
            self.view.p2p_module(),
            self.view.cluster_manager(),
            ttl_seconds,
        )
        .await
    }

    /// Keepalive a client lease using its existing TTL on the master.
    pub async fn keepalive_lease(&self, lease_id: u64) -> KvResult<()> {
        crate::kvlease::keepalive_lease(
            self.view.p2p_module(),
            self.view.cluster_manager(),
            lease_id,
        )
        .await
    }
}

// RPC handler: owner -> external to invalidate weak-index entries
async fn handle_external_invalidate_weak_index(
    view: &ExternalClientApiView,
    msg: &MsgPack<ExternalInvalidateWeakIndexReq>,
) -> MsgPack<ExternalInvalidateWeakIndexResp> {
    let req = msg.serialize_part.clone();
    // Invalidate local weak cache entries for provided keys. Best effort.
    let api = view.external_client_api();
    let inner = api.inner();
    let mut removed_total = 0usize;
    for k in req.keys.iter() {
        if let Some(_v) = inner.key_weak_memholder_index.remove(k) {
            removed_total += 1;
        }
    }
    tracing::debug!(
        "External invalidated weak_index for keys: {:?} (removed {} entries)",
        req.keys,
        removed_total
    );

    MsgPack {
        serialize_part: ExternalInvalidateWeakIndexResp {
            error_code: crate::rpcresp_kvresult_convert::msg_and_error::OK,
            error_json: String::new(),
        },
        raw_bytes: Vec::new(),
    }
}

fn write_all_at(file: &std::fs::File, mut buf: &[u8], mut offset: u64) -> std::io::Result<()> {
    use std::io::{Error, ErrorKind};
    use std::os::unix::fs::FileExt;

    while !buf.is_empty() {
        let n = file.write_at(buf, offset)?;
        if n == 0 {
            return Err(Error::new(ErrorKind::WriteZero, "write_at returned 0"));
        }
        offset = offset
            .checked_add(n as u64)
            .ok_or_else(|| Error::new(ErrorKind::InvalidInput, "offset overflow"))?;
        buf = &buf[n..];
    }
    Ok(())
}

fn sync_kv_bytes_field_to_file(
    encoded_flat_dict: &[u8],
    bytes_field_key: &str,
    filepath: &str,
    file_offset: u64,
) -> KvResult<()> {
    use crate::memholder::kvclient_encode::FlatKvValueRange;

    if bytes_field_key.is_empty() {
        return Err(KvError::Api(ApiError::InvalidArgument {
            detail: "bytes_field_key must be non-empty".to_string(),
        }));
    }
    if filepath.is_empty() {
        return Err(KvError::Api(ApiError::InvalidArgument {
            detail: "filepath must be non-empty".to_string(),
        }));
    }

    let entries = crate::memholder::kvclient_encode::flat_kv_decode_ranges(encoded_flat_dict)
        .map_err(|e| {
            KvError::Api(ApiError::InvalidArgument {
                detail: format!("flat dict decode failed: {}", e),
            })
        })?;

    let mut found: Option<(usize, usize)> = None;
    for (k, v) in entries {
        if k != bytes_field_key {
            continue;
        }
        match v {
            FlatKvValueRange::BytesRange { start, len } => {
                found = Some((start, len));
            }
            _ => {
                return Err(KvError::Api(ApiError::InvalidArgument {
                    detail: format!("field is not bytes: {}", bytes_field_key),
                }));
            }
        }
        break;
    }

    let Some((start, len)) = found else {
        return Err(KvError::Api(ApiError::InvalidArgument {
            detail: format!("missing bytes field: {}", bytes_field_key),
        }));
    };

    let end = start.checked_add(len).ok_or_else(|| {
        KvError::Api(ApiError::InvalidArgument {
            detail: "bytes range overflow".to_string(),
        })
    })?;
    if end > encoded_flat_dict.len() {
        return Err(KvError::Api(ApiError::InvalidArgument {
            detail: "bytes range out of bounds".to_string(),
        }));
    }

    let data = &encoded_flat_dict[start..end];

    let path = std::path::Path::new(filepath);
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent).map_err(|e| {
                KvError::Api(ApiError::FileWriteError {
                    path: filepath.to_string(),
                    offset: file_offset,
                    detail: format!("create parent dir failed: {}", e),
                })
            })?;
        }
    }

    let f = std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .open(path)
        .map_err(|e| {
            KvError::Api(ApiError::FileWriteError {
                path: filepath.to_string(),
                offset: file_offset,
                detail: e.to_string(),
            })
        })?;

    write_all_at(&f, data, file_offset).map_err(|e| {
        KvError::Api(ApiError::FileWriteError {
            path: filepath.to_string(),
            offset: file_offset,
            detail: e.to_string(),
        })
    })?;

    Ok(())
}

async fn handle_sync_kv_to_file_external(
    view: &ExternalClientApiView,
    msg: &MsgPack<SyncKvToFileReq>,
) -> MsgPack<SyncKvToFileResp> {
    let req = msg.serialize_part.clone();
    let key = req.key.clone();

    let result: KvResult<()> = async {
        if req.key.is_empty() {
            return Err(KvError::Api(ApiError::InvalidArgument {
                detail: "key must be non-empty".to_string(),
            }));
        }

        let got = view.external_client_api().inner().get(&req.key).await?;
        let Some(holder) = got else {
            return Err(KvError::Api(ApiError::KeyNotFound { key }));
        };

        sync_kv_bytes_field_to_file(
            holder.bytes(),
            req.bytes_field_key.as_str(),
            req.filepath.as_str(),
            req.file_offset,
        )?;
        Ok(())
    }
    .await;

    let (error_code, error_json) = match result {
        Ok(()) => (
            crate::rpcresp_kvresult_convert::msg_and_error::OK,
            String::new(),
        ),
        Err(e) => (e.code(), e.to_json()),
    };

    MsgPack {
        serialize_part: SyncKvToFileResp {
            error_code,
            error_json,
        },
        raw_bytes: Vec::new(),
    }
}

// --- Static sub tasks (non-self) for concurrent wait and spawn ---

async fn task_wait_owner_restart(
    view: ExternalClientApiView,
    shared_memory_path: String,
    shared_file_path: String,
    current_sig_snapshot: Option<SharedMetaSignature>,
    wait_start_ts: i64,
    old_owner_id: Option<String>,
    expected_cluster_name: String,
    expected_protocol_version: String,
) -> KvResult<OwnerRestartPayload> {
    let shutdown_poller = view.register_shutdown_poller();
    let mut cluster_rx = view.cluster_manager().listen();
    let shared_meta_path = format!("{}/shared.json", &shared_file_path);
    let mut waited = 0u64;
    loop {
        if !shutdown_poller.is_running() {
            return Err(KvError::Api(ApiError::SystemShutdown {
                detail: "Owner recovery wait aborted due to shutdown".to_string(),
            }));
        }

        match probe_owner_restart_payload(
            &view,
            &shared_memory_path,
            &shared_file_path,
            &shared_meta_path,
            current_sig_snapshot.as_ref(),
            wait_start_ts,
            old_owner_id.as_deref(),
            &expected_cluster_name,
            &expected_protocol_version,
        )
        .await?
        {
            OwnerRestartProbe::Ready(payload) => return Ok(payload),
            OwnerRestartProbe::Pending(reason) => {
                if waited % 25 == 0 {
                    tracing::warn!("[task_wait_owner_restart] {}", reason);
                }
            }
        }

        tokio::select! {
            _ = limit_thirdparty::tokio::time::sleep(std::time::Duration::from_millis(200)) => {}
            _ = async {
                let _ = cluster_rx.recv().await;
                limit_thirdparty::tokio::task::yield_now().await;
            } => {}
        }
        waited += 1;
        if waited % 25 == 0 {
            tracing::info!(
                "[task_wait_owner_restart] scanning owner restart... ({}s)",
                waited / 5
            );
        }
    }
}

fn read_shared_json_snapshot(
    shared_meta_path: &str,
) -> KvResult<Option<(SharedJsonMeta, SharedMetaSignature)>> {
    let signature_before = ExternalInner::get_shared_meta_signature(shared_meta_path)?;
    let meta = ExternalInner::read_shared_json(shared_meta_path)?;
    let signature_after = ExternalInner::get_shared_meta_signature(shared_meta_path)?;
    if signature_before != signature_after {
        return Ok(None);
    }
    Ok(Some((meta, signature_after)))
}

async fn probe_owner_restart_payload(
    view: &ExternalClientApiView,
    shared_memory_path: &str,
    shared_file_path: &str,
    shared_meta_path: &str,
    current_sig_snapshot: Option<&SharedMetaSignature>,
    wait_start_ts: i64,
    old_owner_id: Option<&str>,
    expected_cluster_name: &str,
    expected_protocol_version: &str,
) -> KvResult<OwnerRestartProbe> {
    if !fluxon_util::fs_watch::are_files_ready(shared_memory_path, &["mmap.file"]) {
        return Ok(OwnerRestartProbe::Pending(format!(
            "shared memory mmap.file not ready yet: path={}",
            shared_memory_path
        )));
    }
    if !fluxon_util::fs_watch::are_files_ready(shared_file_path, &["shared.json"]) {
        return Ok(OwnerRestartProbe::Pending(format!(
            "shared metadata shared.json not ready yet: path={}",
            shared_file_path
        )));
    }

    let (meta, signature) = match read_shared_json_snapshot(shared_meta_path) {
        Ok(Some(snapshot)) => snapshot,
        Ok(None) => {
            return Ok(OwnerRestartProbe::Pending(format!(
                "shared.json changed while being read; retrying: path={}",
                shared_meta_path
            )));
        }
        Err(err) => {
            return Ok(OwnerRestartProbe::Pending(format!(
                "shared.json not ready or invalid yet: path={} err={}",
                shared_meta_path, err
            )));
        }
    };

    if meta.protocol_version != expected_protocol_version {
        return Ok(OwnerRestartProbe::Pending(format!(
            "shared.json protocol_version mismatch; waiting: shm_dir='{}' shared='{}' local='{}'",
            shared_memory_path, meta.protocol_version, expected_protocol_version
        )));
    }
    if meta.cluster_name != expected_cluster_name {
        return Ok(OwnerRestartProbe::Pending(format!(
            "shared.json cluster_name mismatch; waiting: shm_dir='{}' shared='{}' local='{}'",
            shared_memory_path, meta.cluster_name, expected_cluster_name
        )));
    }
    if let Some(old_owner_id) = old_owner_id {
        if meta.owner_id != old_owner_id {
            return Err(KvError::Api(ApiError::Unknown {
                detail: format!(
                    "shared.json owner_id changed unexpectedly: old_owner_id={} new_owner_id={}",
                    old_owner_id, meta.owner_id
                ),
            }));
        }
    }
    if current_sig_snapshot.is_none() && meta.write_ts.unwrap_or_default() <= wait_start_ts {
        return Ok(OwnerRestartProbe::Pending(format!(
            "shared.json write_ts is not newer yet: path={} write_ts={} wait_start_ts={}",
            shared_meta_path,
            meta.write_ts.unwrap_or_default(),
            wait_start_ts
        )));
    }

    let Some(owner_member) = view
        .cluster_manager()
        .get_member_info_cached(&meta.owner_id)
    else {
        return Ok(OwnerRestartProbe::Pending(format!(
            "shared.json observed but owner member is not in cache yet: owner_id={} shared_start_time={}",
            meta.owner_id, meta.node_start_time
        )));
    };
    if owner_member.node_start_time != meta.node_start_time {
        return Ok(OwnerRestartProbe::Pending(format!(
            "owner generation mismatch: owner_id={} cluster_start_time={} shared_start_time={}",
            meta.owner_id, owner_member.node_start_time, meta.node_start_time
        )));
    }

    if let Some(prev_signature) = current_sig_snapshot {
        if signature == *prev_signature {
            return Ok(OwnerRestartProbe::Pending(format!(
                "shared.json unchanged after cluster convergence: owner_id={} start_time={} path={}",
                meta.owner_id, meta.node_start_time, shared_meta_path
            )));
        }
    }

    Ok(OwnerRestartProbe::Ready(OwnerRestartPayload {
        meta,
        signature,
    }))
}

#[async_trait]
impl LogicalModule for ExternalClientApi {
    type View = ExternalClientApiView;
    type NewArg = ExternalClientApiNewArg;
    type Error = KvError;

    fn name(&self) -> &str {
        "ExternalClientApi"
    }

    fn attach_view(&self, view: Self::View) {
        ExternalClientApi::attach_view(self, view);
    }

    async fn shutdown(&self) -> Result<(), Self::Error> {
        // 只在ExternalClient模式下清理共享内存映射
        let ext = &self.0;
        if ext.shared_memory_path().is_empty() {
            tracing::info!("ExternalClientApi shutdown (no shared memory path configured)");
            return Ok(());
        }
        let shared_opt = {
            let guard = ext.current_owner.read().await;
            guard.as_ref().map(|o| o.shared_memory.clone())
        };
        if let Some(shared) = shared_opt {
            unsafe {
                let len = shared.len() as libc::size_t;
                let ptr_rw = shared.as_ptr();
                if !ptr_rw.is_null() {
                    libc::munmap(ptr_rw as *mut libc::c_void, len);
                }
                let ptr_ro = shared.as_ptr_ro();
                if !ptr_ro.is_null() {
                    libc::munmap(ptr_ro as *mut libc::c_void, len);
                }
                tracing::info!("Unmapped shared memory: len={}", shared.len());
            }
        }
        // The File handle will be dropped when ExternalClientApi (and the Arc) is dropped.
        // We only need to munmap here; closing the File occurs via Drop.

        tracing::info!("ExternalClientApi shutdown completed");
        Ok(())
    }
}
