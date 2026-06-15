use etcd_client as etcd;
use fluxon_util::run_async_from_sync::SyncAsyncBridge;
use pyo3::prelude::*;
use pyo3::{PyErr, PyObject};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::runtime::Runtime;
use tracing::debug;

#[pyclass(name = "EtcdLock")]
pub struct PyEtcdLock {
    rt: Arc<Runtime>,
    endpoints: Vec<String>,
    name: String,
    ttl_seconds: i64,
    timeout_seconds: f64,
    lease_id: Option<i64>,
    lock_key: Option<Vec<u8>>,
}

#[pymethods]
impl PyEtcdLock {
    #[new]
    #[pyo3(signature = (endpoints, name, ttl_seconds, timeout_seconds=None))]
    fn new(
        endpoints: Vec<String>,
        name: String,
        ttl_seconds: i64,
        timeout_seconds: Option<f64>,
    ) -> PyResult<Self> {
        if endpoints.is_empty() {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "EtcdLock requires at least one endpoint",
            ));
        }
        if ttl_seconds <= 0 {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                "EtcdLock ttl_seconds must be > 0, got {}",
                ttl_seconds
            )));
        }
        let timeout_seconds = timeout_seconds.unwrap_or(10.0);
        if !(timeout_seconds.is_finite() && timeout_seconds > 0.0) {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                "EtcdLock timeout_seconds must be finite and > 0, got {}",
                timeout_seconds
            )));
        }

        Ok(Self {
            rt: crate::mpsc::get_global_runtime(),
            endpoints,
            name,
            ttl_seconds,
            timeout_seconds,
            lease_id: None,
            lock_key: None,
        })
    }

    #[getter]
    fn held(&self) -> bool {
        self.lock_key.is_some()
    }

    #[getter]
    fn lease_id(&self) -> Option<i64> {
        self.lease_id
    }

    #[pyo3(signature = (timeout_seconds=None))]
    fn acquire(&mut self, py: Python<'_>, timeout_seconds: Option<f64>) -> PyResult<bool> {
        if self.lock_key.is_some() {
            return Ok(true);
        }

        let timeout_seconds = timeout_seconds.unwrap_or(self.timeout_seconds);
        if !(timeout_seconds.is_finite() && timeout_seconds > 0.0) {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                "EtcdLock timeout_seconds must be finite and > 0, got {}",
                timeout_seconds
            )));
        }

        let endpoints = self.endpoints.clone();
        let name = self.name.clone();
        let ttl_seconds = self.ttl_seconds;
        let timeout_duration = Duration::from_secs_f64(timeout_seconds);
        let t0 = Instant::now();

        debug!(
            target: "fluxon_pyo3::etcd",
            "begin etcd lock acquire: name={}, ttl_seconds={}, timeout_seconds={}",
            name,
            ttl_seconds,
            timeout_seconds
        );

        let outer = py
            .allow_threads(|| {
                self.rt.run_async_from_sync(async move {
                    let mut client = etcd::Client::connect(endpoints, None).await.map_err(|e| {
                        anyhow::anyhow!("failed to connect etcd for lock {}: {:?}", name, e)
                    })?;

                    let lease_resp = client.lease_grant(ttl_seconds, None).await.map_err(|e| {
                        anyhow::anyhow!("failed to grant etcd lease for lock {}: {:?}", name, e)
                    })?;
                    let lease_id = lease_resp.id();

                    match tokio::time::timeout(
                        timeout_duration,
                        client.lock(
                            name.clone(),
                            Some(etcd::LockOptions::new().with_lease(lease_id)),
                        ),
                    )
                    .await
                    {
                        Ok(Ok(resp)) => Ok(Some((lease_id, resp.key().to_vec()))),
                        Ok(Err(err)) => {
                            let _ = client.lease_revoke(lease_id).await;
                            Err(anyhow::anyhow!(
                                "failed to acquire etcd lock {}: {:?}",
                                name,
                                err
                            ))
                        }
                        Err(_) => {
                            let _ = client.lease_revoke(lease_id).await;
                            Ok(None)
                        }
                    }
                })
            })
            .map_err(|e| anyhow::anyhow!("runtime bridge failed in EtcdLock.acquire: {}", e))
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

        let acquire_outcome =
            outer.map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

        match acquire_outcome {
            Some((lease_id, lock_key)) => {
                self.lease_id = Some(lease_id);
                self.lock_key = Some(lock_key);
                debug!(
                    target: "fluxon_pyo3::etcd",
                    "end etcd lock acquire: name={}, lease_id={}, elapsed_ms={}",
                    self.name,
                    lease_id,
                    t0.elapsed().as_millis()
                );
                Ok(true)
            }
            None => {
                debug!(
                    target: "fluxon_pyo3::etcd",
                    "end etcd lock acquire timeout: name={}, elapsed_ms={}",
                    self.name,
                    t0.elapsed().as_millis()
                );
                Ok(false)
            }
        }
    }

    fn release(&mut self, py: Python<'_>) -> PyResult<bool> {
        let Some(lock_key) = self.lock_key.clone() else {
            return Ok(false);
        };
        let Some(lease_id) = self.lease_id else {
            self.lock_key = None;
            return Ok(false);
        };

        let endpoints = self.endpoints.clone();
        let name = self.name.clone();
        let t0 = Instant::now();

        debug!(
            target: "fluxon_pyo3::etcd",
            "begin etcd lock release: name={}, lease_id={}",
            name,
            lease_id
        );

        let outer = py
            .allow_threads(|| {
                self.rt.run_async_from_sync(async move {
                    let mut client = etcd::Client::connect(endpoints, None).await.map_err(|e| {
                        anyhow::anyhow!("failed to connect etcd for unlock {}: {:?}", name, e)
                    })?;

                    let unlock_result = client.unlock(lock_key).await.map(|_| true).map_err(|e| {
                        anyhow::anyhow!("failed to unlock etcd lock {}: {:?}", name, e)
                    });

                    let revoke_result = client.lease_revoke(lease_id).await;
                    match (unlock_result, revoke_result) {
                        (Ok(unlocked), Ok(_)) => Ok(unlocked),
                        (Ok(_), Err(err)) => Err(anyhow::anyhow!(
                            "failed to revoke etcd lease {} for lock {}: {:?}",
                            lease_id,
                            name,
                            err
                        )),
                        (Err(err), Ok(_)) => Err(err),
                        (Err(unlock_err), Err(revoke_err)) => Err(anyhow::anyhow!(
                            "failed to unlock etcd lock {}: {}; failed to revoke lease {}: {:?}",
                            name,
                            unlock_err,
                            lease_id,
                            revoke_err
                        )),
                    }
                })
            })
            .map_err(|e| anyhow::anyhow!("runtime bridge failed in EtcdLock.release: {}", e))
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

        self.lock_key = None;
        self.lease_id = None;
        let released =
            outer.map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
        debug!(
            target: "fluxon_pyo3::etcd",
            "end etcd lock release: name={}, released={}, elapsed_ms={}",
            self.name,
            released,
            t0.elapsed().as_millis()
        );
        Ok(released)
    }

    fn __enter__<'py>(
        mut slf: PyRefMut<'py, Self>,
        py: Python<'py>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        if !slf.acquire(py, None)? {
            return Err(PyErr::new::<pyo3::exceptions::PyTimeoutError, _>(format!(
                "timed out acquiring EtcdLock name={} timeout_seconds={}",
                slf.name, slf.timeout_seconds
            )));
        }
        Ok(slf)
    }

    #[pyo3(signature = (_exc_type=None, _exc=None, _traceback=None))]
    fn __exit__(
        &mut self,
        py: Python<'_>,
        _exc_type: Option<PyObject>,
        _exc: Option<PyObject>,
        _traceback: Option<PyObject>,
    ) -> PyResult<()> {
        let _ = self.release(py)?;
        Ok(())
    }

    fn __repr__(&self) -> String {
        format!(
            "<EtcdLock name={} held={} lease_id={:?}>",
            self.name,
            self.lock_key.is_some(),
            self.lease_id
        )
    }
}
