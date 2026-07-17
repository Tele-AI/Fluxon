use etcd_client as etcd;
use fluxon_mq::lease_manager::{LeaseBackendUid, LeaseRegisterKind};
use fluxon_util::lease_manager::GLOBAL_LM;
use fluxon_util::lease_manager::snapshot_active_lease_debug as lm_snapshot_active_lease_debug;
use fluxon_util::run_async_from_sync::SyncAsyncBridge;
use pyo3::PyErr;
use pyo3::prelude::*;
use std::sync::Arc;
use std::time::Instant;
use tokio::runtime::Runtime;
use tracing::debug;

// ---------------- Python Wrapper: expose fluxon_mq leases in fluxon_pyo3 ----------------

#[pyclass]
pub struct LeaseManagerHandle {
    rt: Arc<Runtime>,
}

// 仅作为 fluxon_mq::lease_manager::Lease 的包装，避免在 fluxon_pyo3 中重复实现 RAII 逻辑。
#[pyclass]
pub struct PyGeneralLease {
    lease: fluxon_mq::lease_manager::GeneralLease,
}

#[pymethods]
impl PyGeneralLease {
    #[getter]
    fn id(&self) -> u64 {
        self.lease.id()
    }

    fn __repr__(&self) -> String {
        match self.lease.kind() {
            fluxon_util::lease_manager::LeaseType::Etcd => {
                format!("<Lease etcd id={}>", self.id())
            }
            fluxon_util::lease_manager::LeaseType::KvClient => {
                format!("<Lease kvclient id={}>", self.id())
            }
        }
    }
}

#[pymethods]
impl LeaseManagerHandle {
    #[new]
    fn new() -> Self {
        // Standalone etcd helpers use this runtime. Fluxon KV lease operations
        // use the runtime owned by the supplied native KvClient.
        let rt = crate::mpsc::get_global_runtime();
        LeaseManagerHandle { rt }
    }

    /// Allocate etcd lease and register keepalive via fluxon_util::GLOBAL_LM,
    /// returning a GeneralLease (with TTL actor handle included).
    fn allocate_etcd_lease(
        &self,
        endpoints: Vec<String>,
        ttl_seconds: i64,
        py: Python<'_>,
    ) -> PyResult<PyGeneralLease> {
        let t0 = Instant::now();
        debug!(
            target: "fluxon_pyo3::lease",
            "begin allocate_etcd_lease: endpoints={}, ttl_seconds={}",
            endpoints.join(","), ttl_seconds
        );
        let rth = self.rt.handle().clone();
        let outer = py
            .allow_threads(|| {
                self.rt.run_async_from_sync(async move {
                    let uid = LeaseBackendUid::etcd_from(endpoints.clone());
                    let mut client = etcd::Client::connect(endpoints, None).await.map_err(|e| {
                        anyhow::anyhow!("failed to connect etcd when allocating lease: {:?}", e)
                    })?;
                    let resp = client.lease_grant(ttl_seconds, None).await?;
                    let id = resp.id() as u64;
                    let rt = rth;
                    GLOBAL_LM
                        .register_lease_for_keepalive(
                            uid,
                            ttl_seconds,
                            id,
                            LeaseRegisterKind::Etcd,
                            rt,
                        )
                        .await
                })
            })
            .map_err(|e| anyhow::anyhow!("runtime bridge failed in allocate_etcd_lease: {}", e))
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
        let lease =
            outer.map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
        debug!(
            target: "fluxon_pyo3::lease",
            "end allocate_etcd_lease: id={}, elapsed_ms={}",
            lease.id(), t0.elapsed().as_millis()
        );
        Ok(PyGeneralLease { lease })
    }

    /// Register existing etcd lease id for keepalive and wrap the core Lease.
    ///
    /// Caller must provide ttl_seconds explicitly; no fallback.
    #[pyo3(signature = (endpoints, ttl_seconds, lease_id, *, register_by))]
    fn register_etcd_lease(
        &self,
        endpoints: Vec<String>,
        ttl_seconds: i64,
        lease_id: u64,
        register_by: String,
        py: Python<'_>,
    ) -> PyResult<PyGeneralLease> {
        let t0 = Instant::now();
        debug!(
            target: "fluxon_pyo3::lease",
            "begin register_etcd_lease: endpoints={}, ttl_seconds={}, lease_id={}, register_by={}",
            endpoints.join(","), ttl_seconds, lease_id, register_by
        );
        fluxon_mq::lease_manager::record_register_by(lease_id, register_by);
        let rth = self.rt.handle().clone();
        let outer = py
            .allow_threads(|| {
                self.rt.run_async_from_sync(async move {
                    let uid = LeaseBackendUid::etcd_from(endpoints);
                    let rt = rth;
                    GLOBAL_LM
                        .register_lease_for_keepalive(
                            uid,
                            ttl_seconds,
                            lease_id,
                            LeaseRegisterKind::Etcd,
                            rt,
                        )
                        .await
                })
            })
            .map_err(|e| anyhow::anyhow!("runtime bridge failed in register_etcd_lease: {}", e))
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
        let lease =
            outer.map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
        debug!(
            target: "fluxon_pyo3::lease",
            "end register_etcd_lease: id={}, elapsed_ms={}",
            lease.id(), t0.elapsed().as_millis()
        );
        Ok(PyGeneralLease { lease })
    }

    /// Register a caller-granted etcd lease id for keepalive and wrap the core Lease.
    ///
    /// The caller must have just granted this lease on the same etcd backend. This
    /// path skips the initial keepalive probe and only installs periodic keepalive.
    #[pyo3(signature = (endpoints, ttl_seconds, lease_id, *, register_by))]
    fn register_newly_granted_etcd_lease(
        &self,
        endpoints: Vec<String>,
        ttl_seconds: i64,
        lease_id: u64,
        register_by: String,
        py: Python<'_>,
    ) -> PyResult<PyGeneralLease> {
        let t0 = Instant::now();
        debug!(
            target: "fluxon_pyo3::lease",
            "begin register_newly_granted_etcd_lease: endpoints={}, ttl_seconds={}, lease_id={}, register_by={}",
            endpoints.join(","), ttl_seconds, lease_id, register_by
        );
        fluxon_mq::lease_manager::record_register_by(lease_id, register_by);
        let rth = self.rt.handle().clone();
        let outer = py
            .allow_threads(|| {
                self.rt.run_async_from_sync(async move {
                    let uid = LeaseBackendUid::etcd_from(endpoints);
                    let rt = rth;
                    GLOBAL_LM
                        .register_lease_for_keepalive(
                            uid,
                            ttl_seconds,
                            lease_id,
                            LeaseRegisterKind::EtcdValidated,
                            rt,
                        )
                        .await
                })
            })
            .map_err(|e| {
                anyhow::anyhow!(
                    "runtime bridge failed in register_newly_granted_etcd_lease: {}",
                    e
                )
            })
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
        let lease =
            outer.map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
        debug!(
            target: "fluxon_pyo3::lease",
            "end register_newly_granted_etcd_lease: id={}, elapsed_ms={}",
            lease.id(), t0.elapsed().as_millis()
        );
        Ok(PyGeneralLease { lease })
    }

    /// Register a Fluxon KV lease using the client's native Rust framework.
    #[pyo3(signature = (kv_client, lease_id, ttl_seconds, *, register_by))]
    fn register_kvclient_lease(
        &self,
        py: Python<'_>,
        kv_client: Py<crate::KvClient>,
        lease_id: u64,
        ttl_seconds: i64,
        register_by: String,
    ) -> PyResult<PyGeneralLease> {
        let lease_context = {
            let client = kv_client.borrow(py);
            crate::new_fluxon_kv_lease_context(&client)?
        };
        let backend_uid = lease_context.backend;
        let runtime = lease_context.runtime;
        let t0 = Instant::now();
        debug!(
            target: "fluxon_pyo3::lease",
            "begin register_kvclient_lease: lease_id={}, ttl_seconds={}, register_by={}",
            lease_id, ttl_seconds, register_by
        );
        let rth = runtime.clone();
        let outer = py
            .allow_threads(|| {
                runtime.run_async_from_sync(async move {
                    let rt = rth;
                    GLOBAL_LM
                        .register_lease_for_keepalive(
                            backend_uid,
                            ttl_seconds,
                            lease_id,
                            fluxon_util::lease_manager::LeaseRegisterKind::KvClient { register_by },
                            rt,
                        )
                        .await
                })
            })
            .map_err(|e| anyhow::anyhow!("runtime bridge failed in register_kvclient_lease: {}", e))
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
        let lease =
            outer.map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
        debug!(
            target: "fluxon_kv::lease",
            "end register_kvclient_lease: id={}, elapsed_ms={}",
            lease.id(), t0.elapsed().as_millis()
        );
        Ok(PyGeneralLease { lease })
    }

    /// Debug-only: dump current active lease entries from the keepalive actor.
    ///
    /// Return a list of tuples: (ttl_seconds, backend_repr, lease_id, register_by)
    /// where backend_repr is a human-readable string of the backend uid.
    #[allow(clippy::type_complexity)]
    fn debug_snapshot_active_leases(&self) -> Vec<(i64, String, u64, Option<String>)> {
        lm_snapshot_active_lease_debug()
            .into_iter()
            .map(|(ttl, backend_uid, lease_id, label)| {
                (ttl, format!("{:?}", backend_uid), lease_id, label)
            })
            .collect()
    }
}
