use super::lease::{Lease, LeaseID, LeaseKey, LeaseSecTTl, LeaseState};
use super::msg_pack::{
    AllocateClientLeaseReq, AllocateClientLeaseResp, ClientLeaseKeepaliveReq,
    ClientLeaseKeepaliveResp,
};
use crate::cluster_manager::{ClusterManager, ClusterManagerAccessTrait};
use crate::master_kv_router::put::PutIDForAKey;
use crate::master_kv_router::{MasterKvRouter, MasterKvRouterAccessTrait};
use crate::master_seg_manager::{MasterSegManager, MasterSegManagerAccessTrait};
use crate::p2p::msg_pack::{MsgPack, RPCHandler};
use crate::p2p::p2p_module::{P2pModule, P2pModuleAccessTrait};
use crate::rpcresp_kvresult_convert::msg_and_error::LeaseMgrError;
use async_trait::async_trait;
use dashmap::DashMap;
use fluxon_framework::{LogicalModule, define_module};
use limit_thirdparty::tokio::sync::Notify;
use limit_thirdparty::tokio::time::sleep;
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::sync::Mutex;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tracing::{debug, error, info, warn};
// use thiserror::Error; // replaced by centralized error group

// No local error alias; use LeaseMgrError directly

/// Master lease manager for unified backend
/// Provides lease management functionality similar to etcd
pub struct MasterLeaseManagerInner {
    view: OnceLock<MasterLeaseManagerView>,
    /// Active leases: lease_id -> Lease
    /// We store Lease directly; callers borrow via DashMap guards.
    pub leases: DashMap<LeaseID, Lease>,

    /// Next available lease ID
    next_lease_id: std::sync::atomic::AtomicU64,
    /// Default TTL for leases (in seconds)
    _default_ttl: u64,
    /// Cleanup interval in seconds
    _cleanup_interval: u64,
    /// Background task handle registered via view.spawn
    cleanup_handle: std::sync::Mutex<
        Option<fluxon_framework_compiled::util::ViewSpawnHandle<dyn MasterLeaseManagerViewTrait>>,
    >,

    /// Cleanup optimization: expire heap for efficient cleanup
    /// Stores (expire_time, lease_id) pairs, ordered by expire_time
    expire_heap: Mutex<BinaryHeap<Reverse<(u64, LeaseID)>>>,
    /// Notify to wake up cleanup task when new lease is added
    cleanup_notify: Notify,
}

pub struct MasterLeaseManager(MasterLeaseManagerInner);

impl MasterLeaseManagerInner {
    fn view(&self) -> &MasterLeaseManagerView {
        self.view.get().unwrap()
    }
}

impl MasterLeaseManager {
    /// Default TTL for leases (90 seconds)
    pub const DEFAULT_TTL: u64 = 90;
    /// Minimum allowed TTL for client leases in seconds.
    /// All externally visible lease APIs must enforce `ttl >= MIN_CLIENT_TTL_SECONDS`.
    pub const MIN_CLIENT_TTL_SECONDS: u64 = 90;

    /// Get the inner instance (panics if not initialized)
    pub fn inner(&self) -> &MasterLeaseManagerInner {
        &self.0
    }

    pub async fn construct(arg: MasterLeaseManagerNewArg) -> Result<Self, LeaseMgrError> {
        let inner = MasterLeaseManagerInner {
            view: OnceLock::new(),
            leases: DashMap::new(),
            next_lease_id: AtomicU64::new(1),
            _default_ttl: arg.default_ttl,
            _cleanup_interval: arg.cleanup_interval,
            cleanup_handle: Mutex::new(None),
            expire_heap: Mutex::new(BinaryHeap::new()),
            cleanup_notify: Notify::new(),
        };

        info!("MasterLeaseManager module constructed");
        Ok(Self(inner))
    }

    pub async fn init2_for_init_dag(&self) -> Result<(), LeaseMgrError> {
        // Register RPC handlers only after all modules are constructed.
        self.register_rpc_handlers(self.0.view())?;

        // Start the background cleanup task.
        self.start_cleanup_task().await?;

        info!(
            "MasterLeaseManager init2_for_init_dag completed: RPC handlers registered and cleanup task started"
        );
        Ok(())
    }

    // Note: no key+put_id lease probe is needed on the hot path; the
    // OneKvNodesRoutes carries `lease_id` alongside `put_id` for decisions.

    /// Register RPC handlers for lease management
    pub fn register_rpc_handlers(
        &self,
        view: &MasterLeaseManagerView,
    ) -> Result<(), LeaseMgrError> {
        let p2p_module = view.p2p_module();

        // Register Client Lease Handlers
        let view1 = view.clone();
        RPCHandler::<AllocateClientLeaseReq>::new().regist(p2p_module, move |resp, msg| {
            let view = view1.clone();
            let view_task = view.clone();
            let _ = view.spawn("rpc_allocate_client_lease", async move {
                let ack = handle_allocate_client_lease(view_task, msg).await;
                if let Err(e) = resp.send_resp(ack).await {
                    error!("Failed to send AllocateClientLeaseResp: {:?}", e);
                }
            });
            Ok(())
        });

        let view2 = view.clone();
        RPCHandler::<ClientLeaseKeepaliveReq>::new().regist(p2p_module, move |resp, msg| {
            let view = view2.clone();
            let view_task = view.clone();
            let _ = view.spawn("rpc_client_lease_keepalive", async move {
                let ack = handle_client_lease_keepalive(view_task, msg).await;
                if let Err(e) = resp.send_resp(ack).await {
                    error!("Failed to send ClientLeaseKeepaliveResp: {:?}", e);
                }
            });
            Ok(())
        });

        info!("Registered lease management RPC handlers");
        Ok(())
    }

    /// Start the cleanup background task with optimized heap-based approach
    pub async fn start_cleanup_task(&self) -> Result<(), LeaseMgrError> {
        let view = self.inner().view().clone();
        let handle = self.inner().view().spawn("lease_cleanup_task", async move {
            // 获取 shutdown_poller 用于检查 shutdown 信号
            let shutdown_poller = view.register_shutdown_poller();

            loop {
                // 在循环开始时检查 shutdown 状态
                if !shutdown_poller.is_running() {
                    debug!("Cleanup task received shutdown signal, exiting gracefully");
                    break;
                }

                let sleep_duration = {
                    // Check if there are any leases to clean up
                    let now = MasterLeaseManager::current_time_ms();
                    let mut cleaned_count = 0;

                    // ✅ 批量收集所有过期的 lease（一次性拿锁）
                    let expired_leases = {
                        let mut heap = view.master_lease_manager().inner().expire_heap.lock().unwrap();
                        let mut expired = Vec::new();
                        while let Some(Reverse((expire_time, lease_id))) = heap.peek() {
                            if *expire_time > now {
                                break; // No more expired leases
                            }
                            expired.push((*expire_time, *lease_id));
                            heap.pop(); // Remove from heap
                        }
                        expired
                    };

                    // ✅ 批量处理收集到的过期 lease
                    for (heap_expire_time, lease_id) in expired_leases {
                        // Check if lease is still valid (lazy deletion)
                        let should_cleanup = {
                            if let Some(lease_ref) = view.master_lease_manager().inner().leases.get(&lease_id) {
                                let current_expire_time = lease_ref.get_expiration_time();
                                current_expire_time == heap_expire_time
                            } else {
                                false
                            }
                        };

                        if should_cleanup {
                            // Lease is indeed expired, clean it up using the dedicated function
                            if let Err(e) = MasterLeaseManager::cleanup_single_lease_static(
                                &view,
                                lease_id
                            ).await {
                                error!("Failed to cleanup lease {}: {}", lease_id, e);
                            } else {
                                cleaned_count += 1;
                            }
                        }
                        // If times don't match, lease was renewed, discard this heap record
                    }

                    if cleaned_count > 0 {
                        info!("Cleaned up {} expired leases", cleaned_count);
                    }

                    // Calculate next sleep time based on heap's min entry
                    if let Some(Reverse((next_expire, _))) = view.master_lease_manager().inner().expire_heap.lock().unwrap().peek() {
                        if *next_expire > now {
                            Duration::from_millis(*next_expire - now)
                        } else {
                            Duration::from_millis(100) // Minimum interval
                        }
                    } else {
                        Duration::from_secs(60) // Default interval when no leases
                    }
                };

                // Use tokio::select! to wait for sleep, notify, or shutdown
                limit_thirdparty::tokio::select! {
                    _ = sleep(sleep_duration) => {
                        // Normal timeout, continue to next iteration
                    }
                    _ = view.master_lease_manager().inner().cleanup_notify.notified() => {
                        // Notified of new lease, immediately check for cleanup
                        debug!("Cleanup task notified of new lease");
                    }
                    _ = async {
                        // 创建一个异步任务来定期检查 shutdown 状态
                        loop {
                            if !shutdown_poller.is_running() {
                                break;
                            }
                            limit_thirdparty::tokio::time::sleep(limit_thirdparty::tokio::time::Duration::from_millis(50)).await;
                        }
                    } => {
                        // Shutdown signal received, exit immediately
                        debug!("Cleanup task received shutdown signal via select!, exiting gracefully");
                        break;
                    }
                }

                // 在循环结束时再次检查 shutdown 状态
                if !shutdown_poller.is_running() {
                    debug!("Cleanup task received shutdown signal, exiting gracefully");
                    break;
                }
            }

            info!("Cleanup task exited gracefully");
        });

        // 存储 handle
        self.inner().cleanup_handle.lock().unwrap().replace(handle);
        Ok(())
    }

    // --- Cleanup Optimization Methods ---

    /// Get current time in milliseconds since UNIX_EPOCH
    fn current_time_ms() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
    }

    /// Insert lease to cleanup heap
    fn insert_lease_to_cleanup(&self, lease_id: LeaseID) {
        let inner = self.inner();

        // Get expire_time from lease
        let expire_time = if let Some(lease_ref) = inner.leases.get(&lease_id) {
            lease_ref.get_expiration_time()
        } else {
            return; // Lease not found
        };

        // 智能 notify：只有当新插入的 lease 比当前最早的还早时才 notify
        let should_notify = {
            let mut heap = inner.expire_heap.lock().unwrap();
            let old_earliest = heap.peek().map(|Reverse((time, _))| *time);
            heap.push(Reverse((expire_time, lease_id)));
            let new_earliest = heap.peek().map(|Reverse((time, _))| *time);

            // 如果新插入的 lease 更早，或者之前没有 lease，则需要 notify
            old_earliest.is_none() || new_earliest < old_earliest
        };

        if should_notify {
            // Notify cleanup task to wake up and check the new lease
            inner.cleanup_notify.notify_one();
        }
    }

    /// Update cleanup heap when lease is renewed
    fn update_lease_in_cleanup_heap(&self, lease_id: LeaseID) {
        let inner = self.inner();

        // Get new expire_time from lease
        let new_expire_time = if let Some(lease_ref) = inner.leases.get(&lease_id) {
            lease_ref.get_expiration_time()
        } else {
            return; // Lease not found
        };

        // 智能 notify：只有当续约后的 lease 比当前最早的还早时才 notify
        let should_notify = {
            let mut heap = inner.expire_heap.lock().unwrap();
            let old_earliest = heap.peek().map(|Reverse((time, _))| *time);
            heap.push(Reverse((new_expire_time, lease_id)));
            let new_earliest = heap.peek().map(|Reverse((time, _))| *time);

            // 如果续约后的 lease 更早，或者之前没有 lease，则需要 notify
            old_earliest.is_none() || new_earliest < old_earliest
        };

        if should_notify {
            // Notify cleanup task to wake up and check the renewed lease
            inner.cleanup_notify.notify_one();
        }
    }

    /// Static version of cleanup_single_lease for use in background tasks
    async fn cleanup_single_lease_static(
        view: &MasterLeaseManagerView,
        lease_id: LeaseID,
    ) -> Result<(), LeaseMgrError> {
        let leases = &view.master_lease_manager().inner().leases;

        // 优先从 leases 中移除，获取到该 lease 的独占所有权，避免并发下的重复清理或后续绑定
        // DashMap::remove 返回 (key, value)，这里我们只关心 value（Lease）
        let lease = match leases.remove(&lease_id) {
            Some((_k, v)) => {
                debug!(
                    "Removed lease {} from active leases upfront for cleanup",
                    lease_id
                );
                v
            }
            None => {
                debug!(
                    "Lease {} not found in active leases; assume already cleaned",
                    lease_id
                );
                return Ok(());
            }
        };

        // 1) 删除时直接遍历 lease 下的关联 keys（无需先 detach / snapshot）
        //    我们已将 lease 从全局移除，后续只有本清理流程会访问它。
        for entry in lease.keys.iter() {
            let key: LeaseKey = entry.key().clone();
            let old_put_id: PutIDForAKey = *entry.value();

            // Delete keys by borrowing MasterKvRouterView from framework, but only
            // when the kv_routes put_id still matches the old put_id.
            let router_view_ref = view.master_kv_router().view();
            let should_delete =
                if let Some(one_kv_routes) = view.master_kv_router().inner().kv_routes.get(&key) {
                    // Compare put_id to ensure we don't delete keys that were re-put under a new lease
                    if one_kv_routes.put_id == old_put_id {
                        true
                    } else {
                        debug!(
                            "Skip deletion for key {}: put_id changed (old: {:?}, current: {:?})",
                            key, &old_put_id, one_kv_routes.put_id
                        );
                        false
                    }
                } else {
                    // kv_routes already missing; proceed to call delete for cleanup consistency
                    true
                };

            if should_delete {
                if let Err(_code) = crate::master_kv_router::delete::do_delete_one_kv_all_replicas(
                    router_view_ref,
                    key.clone(),
                ) {
                    warn!("Key not found during lease cleanup deletion: {}", key);
                }
            }
        }

        // 2) 最后将 lease 状态标记为过期，便于后续调试或状态检查
        lease.expire()?;

        // 3) 无需清理 key-to-lease 反向映射（该字段已移除）

        Ok(())
    }

    // Removed: get_lease returning Arc; prefer borrowing via DashMap guard when needed.

    // Removed test-only key-to-lease reverse map and helpers
    /// Generate the next unique lease ID atomically
    fn generate_next_lease_id(&self) -> LeaseID {
        self.inner().next_lease_id.fetch_add(1, Ordering::SeqCst)
    }

    /// Grant a new lease with auto-generated unique lease_id
    pub async fn grant_lease(&self, ttl: LeaseSecTTl) -> Result<LeaseID, LeaseMgrError> {
        if ttl < MasterLeaseManager::MIN_CLIENT_TTL_SECONDS {
            return Err(LeaseMgrError::InvalidTTL {
                ttl,
                message: format!(
                    "invalid ttl: client lease TTL must be >= {} seconds",
                    MasterLeaseManager::MIN_CLIENT_TTL_SECONDS
                ),
            });
        }

        self.grant_lease_unchecked(ttl).await
    }

    #[cfg(test)]
    pub async fn grant_lease_for_test(&self, ttl: LeaseSecTTl) -> Result<LeaseID, LeaseMgrError> {
        self.grant_lease_unchecked(ttl).await
    }

    #[cfg(test)]
    pub async fn expire_lease_for_test(&self, lease_id: LeaseID) -> Result<(), LeaseMgrError> {
        Self::cleanup_single_lease_static(self.inner().view(), lease_id).await
    }

    async fn grant_lease_unchecked(&self, ttl: LeaseSecTTl) -> Result<LeaseID, LeaseMgrError> {
        let inner = self.inner();

        // Generate unique lease ID atomically
        let lease_id = self.generate_next_lease_id();

        let lease = Lease::new(lease_id, ttl)?;

        // Store the lease
        inner.leases.insert(lease_id, lease);

        // Add to cleanup heap for optimized cleanup
        self.insert_lease_to_cleanup(lease_id);

        info!("Created new lease {} with TTL {}s", lease_id, ttl);
        Ok(lease_id)
    }

    /// Grant a new lease with explicit lease_id (create if not exists)
    pub async fn grant_lease_with_id(
        &self,
        ttl: LeaseSecTTl,
        lease_id: LeaseID,
    ) -> Result<LeaseID, LeaseMgrError> {
        let inner = self.inner();
        if ttl < MasterLeaseManager::MIN_CLIENT_TTL_SECONDS {
            return Err(LeaseMgrError::InvalidTTL {
                ttl,
                message: format!(
                    "invalid ttl: client lease TTL must be >= {} seconds",
                    MasterLeaseManager::MIN_CLIENT_TTL_SECONDS
                ),
            });
        }

        // Check if already exists
        if inner.leases.contains_key(&lease_id) {
            info!("Reusing existing lease {}", lease_id);
            return Ok(lease_id);
        }

        let lease = Lease::new(lease_id, ttl)?;

        // Store the lease
        inner.leases.insert(lease_id, lease);

        // Add to cleanup heap for optimized cleanup
        self.insert_lease_to_cleanup(lease_id);

        info!("Created new lease {} with TTL {}s", lease_id, ttl);
        Ok(lease_id)
    }

    // Removed client-lease mapping APIs as clients manage their own leases

    /// Attach a key to a lease with version information
    pub async fn attach_key(
        &self,
        lease_id: LeaseID,
        key: LeaseKey,
        put_id: PutIDForAKey,
    ) -> Result<(), LeaseMgrError> {
        let inner = self.inner();

        // Attach to lease with version information
        if let Some(lease_ref) = inner.leases.get(&lease_id) {
            lease_ref.attach_key(key.clone(), put_id)?;

            // No reverse map update (field removed)

            debug!(
                "Attached key {} to lease {} with put_id {:?}",
                key, lease_id, put_id
            );
            Ok(())
        } else {
            Err(LeaseMgrError::LeaseNotFound {
                lease_id,
                message: lease_id.to_string(),
            })
        }
    }

    /// Get lease information
    /// pub fn get_lease(&self, lease_id: LeaseID) -> Option<Arc<Lease>> {
    ///    let leases_guard = self.leases.read();
    ///    leases_guard.get(&lease_id).cloned()
    /// }
    /// }

    /// List all active leases
    pub fn list_leases(&self) -> Vec<LeaseID> {
        let inner = self.inner();
        inner.leases.iter().map(|entry| *entry.key()).collect()
    }

    // --- Client Lease Management Methods ---

    /// Handle client lease keepalive（简化权限检查）
    pub async fn client_lease_keepalive(
        &self,
        lease: &Lease,
        custom_ttl: Option<u64>,
    ) -> Result<(), LeaseMgrError> {
        // ✅ 简化权限检查：只要lease存在且活跃即可
        if !matches!(lease.get_state(), LeaseState::Active) {
            return Err(LeaseMgrError::LeaseExpired {
                lease_id: lease.id,
                message: lease.id.to_string(),
            });
        }

        // Refresh lease with optional custom TTL
        lease.refresh(custom_ttl)?;

        // Update cleanup heap for optimized cleanup
        self.update_lease_in_cleanup_heap(lease.id);

        if let Some(ttl) = custom_ttl {
            debug!("Lease {} keepalive with custom TTL {}s", lease.id, ttl);
        } else {
            debug!("Lease {} keepalive with default TTL", lease.id);
        }

        Ok(())
    }

    // Removed get_client_lease/get_all_client_leases and client revoke helpers
}
// --- RPC Handlers ---

/// Handle Client Lease allocation request
/// Now uses auto-generated unique lease_id from Master
/// Simplifies logic: always creates a new lease, no client_id tracking.
/// `requested_ttl_seconds` must be >= MIN_CLIENT_TTL_SECONDS; smaller values
/// will result in LeaseMgrError::InvalidTTL.
pub async fn handle_allocate_client_lease(
    view: MasterLeaseManagerView,
    req: MsgPack<AllocateClientLeaseReq>,
) -> MsgPack<AllocateClientLeaseResp> {
    debug!(
        "handle_allocate_client_lease begin: requested_ttl_seconds={}",
        req.serialize_part.requested_ttl_seconds
    );
    // Always allocate a new lease; no requested id support.
    // Determine TTL to use: prefer requested > 0, else fallback to module default
    let res = view
        .master_lease_manager()
        .grant_lease(req.serialize_part.requested_ttl_seconds)
        .await;
    match res {
        Ok(allocated_lease_id) => {
            // Get lease attributes
            let (ttl_seconds, code, json) = if let Some(lease_ref) = view
                .master_lease_manager()
                .inner()
                .leases
                .get(&allocated_lease_id)
            {
                (
                    lease_ref.ttl,
                    crate::rpcresp_kvresult_convert::msg_and_error::OK,
                    String::new(),
                )
            } else {
                let e = LeaseMgrError::LeaseNotFound {
                    lease_id: allocated_lease_id,
                    message: "allocated lease not found".to_string(),
                };
                let (c, j) = e.to_code_and_json();
                (0, c, j)
            };
            debug!(
                "handle_allocate_client_lease ok: lease_id={} ttl_seconds={} code={}",
                allocated_lease_id, ttl_seconds, code
            );
            MsgPack {
                serialize_part: AllocateClientLeaseResp {
                    error_code: code,
                    error_json: json,
                    lease_id: allocated_lease_id,
                    ttl_seconds,
                },
                raw_bytes: Vec::new(),
            }
        }
        Err(e) => {
            let (code, json) = e.to_code_and_json();
            warn!(
                "handle_allocate_client_lease err: code={} json={}",
                code, json
            );
            MsgPack {
                serialize_part: AllocateClientLeaseResp {
                    error_code: code,
                    error_json: json,
                    lease_id: 0,
                    ttl_seconds: 0,
                },
                raw_bytes: Vec::new(),
            }
        }
    }
}

/// Handle Client Lease Keepalive request
pub async fn handle_client_lease_keepalive(
    view: MasterLeaseManagerView,
    req: MsgPack<ClientLeaseKeepaliveReq>,
) -> MsgPack<ClientLeaseKeepaliveResp> {
    let custom_ttl = if req.serialize_part.custom_ttl > 0 {
        Some(req.serialize_part.custom_ttl)
    } else {
        None
    };

    debug!(
        "handle_client_lease_keepalive begin: lease_id={} custom_ttl={:?}",
        req.serialize_part.lease_id, custom_ttl
    );

    // 直接通过 lease_id 查找 lease
    match view
        .master_lease_manager()
        .inner()
        .leases
        .get(&req.serialize_part.lease_id)
    {
        Some(lease_ref) => {
            // 使用优化的方法进行 keepalive
            match view
                .master_lease_manager()
                .client_lease_keepalive(&lease_ref, custom_ttl)
                .await
            {
                Ok(()) => {
                    debug!(
                        "handle_client_lease_keepalive ok: lease_id={}",
                        req.serialize_part.lease_id
                    );
                    MsgPack {
                        serialize_part: ClientLeaseKeepaliveResp {
                            error_code: crate::rpcresp_kvresult_convert::msg_and_error::OK,
                            error_json: String::new(),
                        },
                        raw_bytes: Vec::new(),
                    }
                }
                Err(e) => {
                    let (code, json) = e.to_code_and_json();
                    warn!(
                        "handle_client_lease_keepalive err: lease_id={} code={} json={}",
                        req.serialize_part.lease_id, code, json
                    );
                    MsgPack {
                        serialize_part: ClientLeaseKeepaliveResp {
                            error_code: code,
                            error_json: json,
                        },
                        raw_bytes: Vec::new(),
                    }
                }
            }
        }
        None => {
            let e = LeaseMgrError::LeaseNotFound {
                lease_id: req.serialize_part.lease_id,
                message: "lease not found".to_string(),
            };
            let (code, json) = e.to_code_and_json();
            warn!(
                "handle_client_lease_keepalive err: lease not found lease_id={} code={} json={}",
                req.serialize_part.lease_id, code, json
            );
            MsgPack {
                serialize_part: ClientLeaseKeepaliveResp {
                    error_code: code,
                    error_json: json,
                },
                raw_bytes: Vec::new(),
            }
        }
    }
}

impl Drop for MasterLeaseManager {
    fn drop(&mut self) {
        if let Some(handle) = self.0.cleanup_handle.lock().unwrap().take() {
            handle.abort();
        }
    }
}

/// Configuration for MasterLeaseManager module
#[derive(Debug, Clone)]
pub struct MasterLeaseManagerNewArg {
    /// Cleanup interval in seconds
    pub cleanup_interval: u64,
    /// Default TTL for leases
    pub default_ttl: u64,
}

impl MasterLeaseManagerNewArg {}

// 定义模块
define_module!(
    MasterLeaseManager,
    (master_lease_manager, MasterLeaseManager),
    (master_kv_router, MasterKvRouter),
    (p2p, P2pModule),
    (cluster_manager, ClusterManager),
    (master_seg_manager, MasterSegManager)
);

#[async_trait]
impl LogicalModule for MasterLeaseManager {
    type View = MasterLeaseManagerView;
    type NewArg = MasterLeaseManagerNewArg;
    type Error = LeaseMgrError;

    fn name(&self) -> &str {
        "MasterLeaseManager"
    }

    fn attach_view(&self, view: Self::View) {
        let inner = &self.0;
        // The framework attaches a module's PostView exactly once at the init barrier.
        // A second attach indicates a programming error.
        inner
            .view
            .set(view)
            .unwrap_or_else(|_| panic!("MasterLeaseManager view attached twice"));
    }

    async fn shutdown(&self) -> Result<(), Self::Error> {
        info!("MasterLeaseManager shutting down...");

        // Stop cleanup task.
        let handle = {
            let mut guard = self.0.cleanup_handle.lock().unwrap();
            guard.take()
        };

        if let Some(handle) = handle {
            info!("Waiting for cleanup task to stop gracefully...");
            // Await the task; it observes shutdown via ShutdownPoller.
            handle.await;
            info!("Cleanup task stopped gracefully");
        }
        info!("MasterLeaseManager shutdown completed successfully");
        Ok(())
    }
}
