use anyhow::Result as AnyResult;
use fluxon_util::lease_manager::GLOBAL_LM;
use fluxon_util::lease_manager::snapshot_active_lease_debug as lm_snapshot_active_lease_debug;
use fluxon_util::run_async_from_sync::SyncAsyncBridge;
use pyo3::prelude::*;
use pyo3::types::PyAny;
use pyo3::{Py, PyErr};
use std::sync::Arc;
use std::time::Instant;
use tokio::runtime::Runtime;
use tracing::debug;
// Use the shared PyO3 helper that submits Python callables to a Python
// ThreadPoolExecutor and waits without holding the GIL for the whole duration.
use etcd_client as etcd;
use fluxon_mq::lease_manager::{LeaseBackendUid, LeaseRegisterKind};
use fluxon_util::pyo3::run_longtime_py_function;

// ---------------- Python Wrapper: expose fluxon_mq leases in fluxon_pyo3 ----------------

#[pyclass(name = "LeaseBackendUid")]
pub struct PyLeaseBackendUid {
    inner: LeaseBackendUid,
}

#[pymethods]
impl PyLeaseBackendUid {
    /// Construct a kvclient backend uid that carries kvclient callbacks.
    ///
    /// MQ 层在使用 kvclient 作为 payload lease backend 时，应通过
    /// 此方法将 allocate/keepalive 能力注入到底层统一的
    /// LeaseManager 抽象中。
    #[staticmethod]
    fn kv_client_with_callbacks(
        py: Python<'_>,
        cluster: String,
        allocate_cb: PyObject,
        keepalive_cb: PyObject,
    ) -> PyResult<Self> {
        let alloc_py: Py<PyAny> = allocate_cb.into_py(py);
        let keep_py: Py<PyAny> = keepalive_cb.into_py(py);

        // Bridge Python callbacks into Rust closures stored in LeaseBackendUid
        let alloc_cb: Arc<dyn Fn(i64) -> AnyResult<u64> + Send + Sync + 'static> = {
            let cb = alloc_py;
            let cluster_for_alloc = cluster.clone();
            Arc::new(move |ttl_seconds: i64| {
                // Use fluxon_util::pyo3::run_longtime_py_function to avoid
                // holding the GIL while the Python callback runs.
                let arg_lease_ttl = Python::with_gil(|py| ttl_seconds.into_py(py));
                let py_ret =
                    run_longtime_py_function(&cb, vec![arg_lease_ttl], None).map_err(|e| {
                        anyhow::anyhow!(
                            "kvclient allocate callback error for cluster={}: {:?}",
                            cluster_for_alloc,
                            e
                        )
                    })?;
                Python::with_gil(|py| {
                    let id: i64 = py_ret.extract(py).map_err(|e| {
                        anyhow::anyhow!(
                            "kvclient allocate callback returned non-int for cluster={}: {:?}",
                            cluster_for_alloc,
                            e
                        )
                    })?;
                    if id <= 0 {
                        anyhow::bail!(
                            "kvclient allocate callback returned non-positive id {} for cluster={}",
                            id,
                            cluster_for_alloc
                        );
                    }
                    Ok(id as u64)
                })
            })
        };

        let keep_cb: Arc<dyn Fn(u64) -> AnyResult<()> + Send + Sync + 'static> = {
            let cb = keep_py;
            let cluster_for_keep = cluster.clone();
            Arc::new(move |lease_id: u64| {
                // Run the Python keepalive callback via the shared long-time runner.
                let arg_lease_id = Python::with_gil(|py| (lease_id as i64).into_py(py));
                let py_ret =
                    run_longtime_py_function(&cb, vec![arg_lease_id], None).map_err(|e| {
                        anyhow::anyhow!(
                            "kvclient keepalive callback error for cluster={} lease_id={}: {:?}",
                            cluster_for_keep,
                            lease_id,
                            e
                        )
                    })?;

                // Accept either None (success) or an object exposing error(): Any.
                Python::with_gil(|py| {
                    if let Ok(err_method) = py_ret.getattr(py, "error") {
                        if let Ok(err_val) = err_method.call0(py) {
                            if !err_val.is_none(py) {
                                return Err(anyhow::anyhow!(
                                    "kvclient keepalive returned error for cluster={} lease_id={}: {:?}",
                                    cluster_for_keep,
                                    lease_id,
                                    err_val
                                ));
                            }
                        }
                    }
                    Ok(())
                })
            })
        };

        Ok(PyLeaseBackendUid {
            inner: LeaseBackendUid::kv_client_with_callbacks(cluster, alloc_cb, keep_cb),
        })
    }

    fn __repr__(&self) -> String {
        match self.inner.kind() {
            fluxon_util::lease_manager::LeaseType::Etcd => match self.inner.endpoints() {
                Some(v) if !v.is_empty() => {
                    format!("<LeaseBackendUid etcd endpoints={}>", v.join(","))
                }
                _ => "<LeaseBackendUid etcd>".to_string(),
            },
            fluxon_util::lease_manager::LeaseType::KvClient => match self.inner.cluster() {
                Some(c) => format!("<LeaseBackendUid kvclient cluster={}>", c),
                None => "<LeaseBackendUid kvclient>".to_string(),
            },
        }
    }
}

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
        // Use the singleton runtime provided by the PyO3 mpsc module
        // as the only runtime source, then pass it down to fluxon_mq.
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
                            LeaseRegisterKind::EtcdNewlyGranted,
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

    /// Register a kvclient lease via constructed backend uid carrying callbacks.
    ///
    /// 统一风格：由 Python 先构造 `LeaseBackendUid.kv_client_with_callbacks(...)`，
    /// 之后所有 kvclient lease 操作都通过该 uid 进行。
    #[pyo3(signature = (kv_backend_uid, lease_id, ttl_seconds, *, register_by))]
    fn register_kvclient_lease_via_backend(
        &self,
        py: Python<'_>,
        kv_backend_uid: Py<PyLeaseBackendUid>,
        lease_id: u64,
        ttl_seconds: i64,
        register_by: String,
    ) -> PyResult<PyGeneralLease> {
        // Borrow backend uid while holding GIL, then release GIL during async bridge to avoid deadlock:
        // The registration path may immediately invoke the provided kvclient keepalive callback, which
        // calls back into Python via PyO3. If we keep holding the GIL here while waiting for the async
        // completion, that callback will block on GIL and cause a deadlock. Wrapping the blocking bridge
        // in allow_threads ensures the GIL is released.
        let backend_uid = kv_backend_uid.borrow(py).backend_uid().clone();
        let t0 = Instant::now();
        debug!(
            target: "fluxon_pyo3::lease",
            "begin register_kvclient_lease_via_backend: lease_id={}, ttl_seconds={}, register_by={}",
            lease_id, ttl_seconds, register_by
        );
        let rth = self.rt.handle().clone();
        let outer = py
            .allow_threads(|| {
                self.rt.run_async_from_sync(async move {
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
            .map_err(|e| {
                anyhow::anyhow!(
                    "runtime bridge failed in register_kvclient_lease_via_backend: {}",
                    e
                )
            })
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
        let lease =
            outer.map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
        debug!(
            target: "fluxon_kv::lease",
            "end register_kvclient_lease_via_backend: id={}, elapsed_ms={}",
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

impl PyLeaseBackendUid {
    pub(crate) fn backend_uid(&self) -> &LeaseBackendUid {
        &self.inner
    }
}
