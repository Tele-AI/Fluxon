// Thin wrapper around fluxon_util::lease_manager.
// Reusable abstractions (OneTtlKeepAliveActorOwned, OneTtlKeepAliveInner, KvLeaseEntry, etc.)
// live in fluxon_util. This module only re-exports types/helpers and provides
// a small etcd convenience wrapper for synchronous callers.
//
// Do not hide a global tokio runtime here: callers must pass a &Runtime (or Arc<Runtime>)
// explicitly to avoid implicit singletons.

use anyhow::Result;
use fluxon_util::etcd::EtcdEndpointSet;
use fluxon_util::lease_manager;
use fluxon_util::run_async_from_sync::SyncAsyncBridge;
use tokio::runtime::Runtime;

// Re-export debug label helpers so callers don't depend on fluxon_util directly.
pub use fluxon_util::lease_manager::{debug_keepalive_log, get_register_by, record_register_by};

// Re-export actor-based lease types
pub use fluxon_util::lease_manager::{
    GLOBAL_LM, GeneralLease, LeaseBackendHandle, LeaseBackendUid, LeaseRegisterKind, LeaseType,
};

// Canonicalization is handled by LeaseBackendUid constructor.

/// Register an existing etcd lease id for keepalive and return the handle.
///
/// Design rule: the caller must provide the ttl explicitly; we do not
/// guess or fallback. This keeps behavior deterministic and surfaces
/// config issues early.
pub fn register_etcd_lease(
    rt: &Runtime,
    endpoints: EtcdEndpointSet,
    ttl_seconds: i64,
    lease_id: u64,
    revoke_on_drop: bool,
) -> Result<lease_manager::GeneralLease> {
    let rth = rt.handle().clone();
    let outer = rt
        .run_async_from_sync(async move {
            let uid = LeaseBackendUid::etcd(endpoints);
            let mgr = &lease_manager::GLOBAL_LM;
            let gl = mgr
                .register_lease_for_keepalive(
                    uid,
                    ttl_seconds,
                    lease_id,
                    lease_manager::LeaseRegisterKind::Etcd { revoke_on_drop },
                    rth,
                )
                .await?;
            anyhow::Ok(gl)
        })
        .map_err(|e| anyhow::anyhow!("runtime bridge failed in register_etcd_lease: {}", e))?;
    let h = outer?;
    Ok(h)
}
