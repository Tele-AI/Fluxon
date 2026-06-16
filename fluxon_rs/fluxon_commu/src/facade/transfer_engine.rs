use std::any::Any;
use std::pin::Pin;
use std::sync::{Arc, OnceLock, RwLock};

use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use crate::NodeIDString;
use fluxon_commu_contract::{
    ClosedRuntimeHandle, ClosedRuntimePeerGen, ClosedRuntimeTransferEngineOpenRuntimeRequest,
    ClosedRuntimeTransferEngineOpenRuntimeResponse,
};
use fluxon_framework_compiled::shutdown::ShutdownPoller;
use tokio::sync::Mutex as AsyncMutex;

use crate::closed_sdk::{
    construct_transfer_engine_handle, drop_runtime_handle, transfer_engine_current_runtime_config,
    transfer_engine_init2_for_init_dag, transfer_engine_register_local_segment,
    transfer_engine_transfer_data_no_copy, transfer_engine_try_send_wire_direct,
    transfer_engine_unregister_local_segment, transfer_engine_update_runtime_config,
};

struct ClosedTransferEngineRuntime {
    construct_arg: ClientTransferEngineNewArg,
    handle: OnceLock<ClosedRuntimeHandle>,
    construct_lock: AsyncMutex<()>,
    current_config: RwLock<ClientTransferEngineRuntimeConfig>,
    local_segment_leases: Arc<ClosedLocalSegmentLeaseRegistry>,
    running: AtomicBool,
}

impl ClosedTransferEngineRuntime {
    fn new(arg: ClientTransferEngineNewArg) -> Self {
        Self {
            current_config: RwLock::new(ClientTransferEngineRuntimeConfig {
                transfer_engine: arg.transfer_engine,
                enable_transfer_rpc_fast_path: arg.enable_transfer_rpc_fast_path,
                protocol_type: arg.protocol_type,
                rdma_device_names: arg.rdma_device_names.clone(),
            }),
            construct_arg: arg,
            handle: OnceLock::new(),
            construct_lock: AsyncMutex::new(()),
            local_segment_leases: Arc::new(ClosedLocalSegmentLeaseRegistry::new()),
            running: AtomicBool::new(true),
        }
    }
}

struct ClosedLocalSegmentLeaseRegistry {
    next_handle: AtomicU64,
    guards: AsyncMutex<HashMap<u64, Box<dyn Any + Send + Sync>>>,
}

impl ClosedLocalSegmentLeaseRegistry {
    fn new() -> Self {
        Self {
            next_handle: AtomicU64::new(1),
            guards: AsyncMutex::new(HashMap::new()),
        }
    }

    async fn insert<G>(&self, guard: G) -> u64
    where
        G: Send + Sync + 'static,
    {
        let handle = self.next_handle.fetch_add(1, Ordering::Relaxed);
        self.guards.lock().await.insert(handle, Box::new(guard));
        handle
    }

    async fn take<G>(&self, handle: u64) -> Result<G, String>
    where
        G: Send + Sync + 'static,
    {
        let boxed = self
            .guards
            .lock()
            .await
            .remove(&handle)
            .ok_or_else(|| format!("closed sdk local segment lease handle {handle} not found"))?;
        boxed.downcast::<G>().map(|guard| *guard).map_err(|_| {
            format!(
                "closed sdk local segment lease handle {handle} has unexpected runtime guard type"
            )
        })
    }

    async fn drop_handle(&self, handle: u64) {
        self.guards.lock().await.remove(&handle);
    }
}

#[derive(Clone)]
struct ClosedTransferRpcFastPath {
    handle: ClosedRuntimeHandle,
}

#[async_trait]
impl TransferRpcFastPath for ClosedTransferRpcFastPath {
    async fn try_send_wire_direct(
        &self,
        peer_gen: &crate::p2p::PeerGen,
        peer_transfer_backend_epoch: u64,
        wire_bytes: Vec<u8>,
    ) -> TransferEngineResult<bool> {
        transfer_engine_try_send_wire_direct(
            self.handle,
            ClosedRuntimePeerGen {
                peer_id: peer_gen.peer_id.to_string(),
                node_start_time: peer_gen.node_start_time,
            },
            peer_transfer_backend_epoch,
            wire_bytes,
        )
        .await
        .map_err(transfer_engine_closed_sdk_error)
    }
}

fn transfer_engine_closed_sdk_error(
    error: crate::closed_sdk::ClosedSdkConsumerError,
) -> TransferEngineError {
    TransferEngineError::CreateEngineFailed {
        detail: format!("closed sdk transfer-engine call failed: {error}"),
    }
}

pub struct ClientTransferEngineCore {
    closed: ClosedTransferEngineRuntime,
}

impl ClientTransferEngineCore {
    pub async fn construct(arg: ClientTransferEngineNewArg) -> TransferEngineResult<Self> {
        {
            Ok(Self {
                closed: ClosedTransferEngineRuntime::new(arg),
            })
        }
    }

    pub fn rpc_fast_path(&self) -> Option<Arc<dyn TransferRpcFastPath>> {
        {
            let handle = self.closed.handle.get().copied()?;
            let config = self
                .closed
                .current_config
                .read()
                .expect("transfer-engine config cache poisoned")
                .clone();
            if !config.enable_transfer_rpc_fast_path {
                return None;
            }
            Some(Arc::new(ClosedTransferRpcFastPath { handle }) as Arc<dyn TransferRpcFastPath>)
        }
    }

    pub fn attach_shutdown_poller(&self, shutdown_poller: ShutdownPoller) {
        {
            let _ = shutdown_poller;
        }
    }

    #[inline]
    pub fn is_running(&self) -> bool {
        { self.closed.running.load(Ordering::Relaxed) }
    }

    pub async fn current_runtime_config(&self) -> ClientTransferEngineRuntimeConfig {
        {
            if let Some(handle) = self.closed.handle.get().copied() {
                match transfer_engine_current_runtime_config(handle).await {
                    Ok(config) => {
                        *self
                            .closed
                            .current_config
                            .write()
                            .expect("transfer-engine config cache poisoned") = config.clone();
                        config
                    }
                    Err(_) => self
                        .closed
                        .current_config
                        .read()
                        .expect("transfer-engine config cache poisoned")
                        .clone(),
                }
            } else {
                self.closed
                    .current_config
                    .read()
                    .expect("transfer-engine config cache poisoned")
                    .clone()
            }
        }
    }

    pub async fn update_runtime_config(&self, config: ClientTransferEngineRuntimeConfig) {
        {
            *self
                .closed
                .current_config
                .write()
                .expect("transfer-engine config cache poisoned") = config.clone();
            if let Some(handle) = self.closed.handle.get().copied() {
                let _ = transfer_engine_update_runtime_config(handle, config).await;
            }
        }
    }

    async fn ensure_closed_runtime_handle<R>(
        &self,
        runtime: &R,
    ) -> TransferEngineResult<ClosedRuntimeHandle>
    where
        R: ClientTransferEngineRuntime,
    {
        if let Some(handle) = self.closed.handle.get().copied() {
            return Ok(handle);
        }
        let _guard = self.closed.construct_lock.lock().await;
        if let Some(handle) = self.closed.handle.get().copied() {
            return Ok(handle);
        }
        let (cluster_manager, p2p_module) = runtime
            .closed_sdk_runtime_handles()
            .await
            .map_err(|detail| TransferEngineError::CreateEngineFailed { detail })?;
        let handle = construct_transfer_engine_handle(
            cluster_manager,
            p2p_module,
            self.closed.construct_arg.clone(),
        )
        .await
        .map_err(transfer_engine_closed_sdk_error)?;
        let _ = self.closed.handle.set(handle);
        let desired = self
            .closed
            .current_config
            .read()
            .expect("transfer-engine config cache poisoned")
            .clone();
        transfer_engine_update_runtime_config(handle, desired)
            .await
            .map_err(transfer_engine_closed_sdk_error)?;
        Ok(handle)
    }

    pub async fn init2_for_init_dag<R>(&self, runtime: R) -> TransferEngineResult<()>
    where
        R: ClientTransferEngineRuntime,
    {
        {
            let handle = self.ensure_closed_runtime_handle(&runtime).await?;
            let local_segment_leases = Arc::clone(&self.closed.local_segment_leases);
            transfer_engine_init2_for_init_dag(
                handle,
                runtime.supports_local_segment_transfer(),
                move |request| {
                    let runtime = runtime.clone();
                    let local_segment_leases = Arc::clone(&local_segment_leases);
                    async move {
                        match request {
                            ClosedRuntimeTransferEngineOpenRuntimeRequest::SupportsLocalSegmentTransfer => {
                                Ok(ClosedRuntimeTransferEngineOpenRuntimeResponse::BoolValue(
                                    runtime.supports_local_segment_transfer(),
                                ))
                            }
                            ClosedRuntimeTransferEngineOpenRuntimeRequest::RegisterP2pTransferRpc => {
                                runtime.register_p2p_transfer_rpc();
                                Ok(ClosedRuntimeTransferEngineOpenRuntimeResponse::Unit)
                            }
                            ClosedRuntimeTransferEngineOpenRuntimeRequest::EnsureLocalSegmentGuard {
                                local_addr,
                                previous_guard_handle,
                            } => {
                                let previous = if let Some(handle) = previous_guard_handle {
                                    Some(
                                        local_segment_leases
                                            .take::<R::LocalSegmentGuard>(handle)
                                            .await?,
                                    )
                                } else {
                                    None
                                };
                                let guard = runtime
                                    .ensure_local_segment_guard(local_addr, previous)
                                    .await?;
                                let guard_handle = local_segment_leases.insert(guard).await;
                                Ok(
                                    ClosedRuntimeTransferEngineOpenRuntimeResponse::GuardHandleValue(
                                        guard_handle,
                                    ),
                                )
                            }
                            ClosedRuntimeTransferEngineOpenRuntimeRequest::DropLocalSegmentGuard {
                                guard_handle,
                            } => {
                                local_segment_leases.drop_handle(guard_handle).await;
                                Ok(ClosedRuntimeTransferEngineOpenRuntimeResponse::Unit)
                            }
                            ClosedRuntimeTransferEngineOpenRuntimeRequest::P2pReadToLocal {
                                peer,
                                remote_src,
                                local_target,
                                len,
                                guard_handle,
                            } => {
                                let guard = local_segment_leases
                                    .take::<R::LocalSegmentGuard>(guard_handle)
                                    .await
                                    .map_err(|detail| {
                                        format!(
                                            "{detail} for p2p_read_to_local"
                                        )
                                    })?;
                                runtime
                                    .p2p_read_to_local(peer.into(), remote_src, local_target, len, guard)
                                    .await?;
                                Ok(ClosedRuntimeTransferEngineOpenRuntimeResponse::Unit)
                            }
                            ClosedRuntimeTransferEngineOpenRuntimeRequest::P2pWriteFromLocal {
                                peer,
                                local_src,
                                remote_target,
                                len,
                                guard_handle,
                            } => {
                                let guard = local_segment_leases
                                    .take::<R::LocalSegmentGuard>(guard_handle)
                                    .await
                                    .map_err(|detail| {
                                        format!(
                                            "{detail} for p2p_write_from_local"
                                        )
                                    })?;
                                runtime
                                    .p2p_write_from_local(peer.into(), local_src, remote_target, len, None, guard)
                                    .await?;
                                Ok(ClosedRuntimeTransferEngineOpenRuntimeResponse::Unit)
                            }
                            ClosedRuntimeTransferEngineOpenRuntimeRequest::RecordPeerNetworkBytes {
                                logical_peer,
                                direction,
                                bytes,
                            } => {
                                let logical_peer: crate::NodeID = logical_peer.into();
                                runtime.record_peer_network_bytes(
                                    &logical_peer,
                                    match direction {
                                        fluxon_commu_contract::ClosedRuntimeP2pLocalIpcDirection::Tx => "tx",
                                        fluxon_commu_contract::ClosedRuntimeP2pLocalIpcDirection::Rx => "rx",
                                    },
                                    bytes,
                                );
                                Ok(ClosedRuntimeTransferEngineOpenRuntimeResponse::Unit)
                            }
                        }
                    }
                },
            )
            .await
            .map_err(transfer_engine_closed_sdk_error)
        }
    }

    pub async fn close(&self) {
        {
            self.closed.running.store(false, Ordering::Relaxed);
            if let Some(handle) = self.closed.handle.get().copied() {
                let _ = drop_runtime_handle(handle).await;
            }
        }
    }

    pub async fn register_local_segment<R>(
        &self,
        runtime: R,
        cpu_mem: &CpuAllocatedMem,
    ) -> TransferEngineResult<()>
    where
        R: ClientTransferEngineRuntime,
    {
        {
            let handle = self.ensure_closed_runtime_handle(&runtime).await?;
            transfer_engine_register_local_segment(
                handle,
                cpu_mem.allocated_addr,
                cpu_mem.allocated_size,
            )
            .await
            .map_err(transfer_engine_closed_sdk_error)
        }
    }

    pub async fn unregister_local_segment(
        &self,
        cpu_mem: &CpuAllocatedMem,
    ) -> TransferEngineResult<()> {
        {
            if let Some(handle) = self.closed.handle.get().copied() {
                transfer_engine_unregister_local_segment(
                    handle,
                    cpu_mem.allocated_addr,
                    cpu_mem.allocated_size,
                )
                .await
                .map_err(transfer_engine_closed_sdk_error)
            } else {
                Ok(())
            }
        }
    }

    pub async fn write_data<R>(
        &self,
        runtime: R,
        data: Pin<&[u8]>,
        src_addr: u64,
        target_addr: u64,
        peer_id: Option<NodeIDString>,
        do_copy: bool,
        seg_guard: Option<R::LocalSegmentGuard>,
    ) -> TransferEngineResult<TransferBreakdown>
    where
        R: ClientTransferEngineRuntime,
    {
        {
            let len = data.get_ref().len() as u64;
            if do_copy && len > 0 {
                unsafe {
                    std::ptr::copy_nonoverlapping(
                        data.get_ref().as_ptr(),
                        src_addr as *mut u8,
                        len as usize,
                    );
                }
            }
            if peer_id.is_none() {
                if len > 0 && src_addr != target_addr {
                    unsafe {
                        std::ptr::copy_nonoverlapping(
                            src_addr as *const u8,
                            target_addr as *mut u8,
                            len as usize,
                        );
                    }
                }
                return Ok(TransferBreakdown {
                    local_noop: src_addr == target_addr,
                    ..TransferBreakdown::default()
                });
            }
            self.transfer_data_no_copy(
                runtime,
                peer_id,
                false,
                src_addr,
                target_addr,
                len,
                seg_guard,
            )
                .await
        }
    }

    pub async fn transfer_data_no_copy<R>(
        &self,
        runtime: R,
        peer_node: Option<NodeIDString>,
        peer_src_or_target: bool,
        src_addr: u64,
        target_addr: u64,
        len: u64,
        seg_guard: Option<R::LocalSegmentGuard>,
    ) -> TransferEngineResult<TransferBreakdown>
    where
        R: ClientTransferEngineRuntime,
    {
        {
            let initial_local_segment_guard = match seg_guard {
                Some(guard) => Some(guard),
                None if runtime.supports_local_segment_transfer() => {
                    let local_addr = if peer_src_or_target { target_addr } else { src_addr };
                    match runtime.ensure_local_segment_guard(local_addr, None).await {
                        Ok(guard) => Some(guard),
                        Err(_) => None,
                    }
                }
                None => None,
            };
            let initial_local_segment_guard_handle = match initial_local_segment_guard {
                Some(guard) => Some(self.closed.local_segment_leases.insert(guard).await),
                None => None,
            };
            let handle = self.ensure_closed_runtime_handle(&runtime).await?;
            let _ = runtime;
            let result = transfer_engine_transfer_data_no_copy(
                handle,
                peer_node.map(|value| value.to_string()),
                peer_src_or_target,
                src_addr,
                target_addr,
                len,
                initial_local_segment_guard_handle,
            )
            .await
            .map_err(transfer_engine_closed_sdk_error);
            if result.is_err() {
                if let Some(guard_handle) = initial_local_segment_guard_handle {
                    self.closed
                        .local_segment_leases
                        .drop_handle(guard_handle)
                        .await;
                }
            }
            result
        }
    }
}
pub use fluxon_commu_contract::transfer_engine::*;
