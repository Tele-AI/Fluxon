use etcd_client as etcd;
use fluxon_util::etcd::{PooledEtcdClient, etcd_clients_pool};
use fluxon_util::run_async_from_sync::SyncAsyncBridge;
use pyo3::prelude::*;
use pyo3::pybacked::PyBackedBytes;
use pyo3::types::{PyBytes, PyList, PyTuple};
use pyo3::{PyErr, PyObject};
use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::runtime::Runtime;
use tracing::debug;

fn is_lease_not_found_error(err: &etcd::Error) -> bool {
    let msg = format!("{:?}", err).to_ascii_lowercase();
    msg.contains("requested lease not found")
        || msg.contains("lease not found")
        || msg.contains("code: notfound")
}

fn normalize_raw_endpoint(endpoint: &str) -> PyResult<String> {
    let endpoint = endpoint.trim();
    if endpoint.is_empty() {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "etcd endpoint must be non-empty raw host:port",
        ));
    }
    if endpoint.contains("://") {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
            "etcd endpoint must be raw host:port without scheme, got: {}",
            endpoint
        )));
    }
    Ok(format!("http://{}", endpoint))
}

fn normalize_raw_endpoints(endpoints: Vec<String>, component: &str) -> PyResult<Vec<String>> {
    if endpoints.is_empty() {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
            "{} requires at least one endpoint",
            component
        )));
    }
    let mut normalized = Vec::with_capacity(endpoints.len());
    for endpoint in endpoints {
        normalized.push(normalize_raw_endpoint(&endpoint)?);
    }
    Ok(normalized)
}

fn is_reconnectable_etcd_error(err: &etcd::Error) -> bool {
    is_reconnectable_etcd_error_text(&format!("{:?}", err))
}

fn is_reconnectable_etcd_error_text(msg: &str) -> bool {
    let msg = msg.to_ascii_lowercase();
    msg.contains("unavailable")
        || msg.contains("connection")
        || msg.contains("transport")
        || msg.contains("timed out")
        || msg.contains("timeout")
        || msg.contains("broken pipe")
        || msg.contains("closed")
}

async fn run_etcd_op<T, F, Fut>(
    pool_entry: PooledEtcdClient,
    context: String,
    mut op: F,
) -> anyhow::Result<T>
where
    F: FnMut(etcd::Client) -> Fut,
    Fut: Future<Output = Result<T, etcd::Error>>,
{
    let mut last_err = None;
    for attempt in 1..=2 {
        let snapshot = pool_entry.snapshot().await?;
        match op(snapshot.client()).await {
            Ok(value) => return Ok(value),
            Err(err) => {
                let should_retry = attempt == 1 && is_reconnectable_etcd_error(&err);
                last_err = Some(err);
                if should_retry {
                    snapshot.invalidate().await;
                    continue;
                }
                let err = last_err.take().expect("etcd error must be recorded");
                return Err(anyhow::anyhow!("{}: {:?}", context, err));
            }
        }
    }

    let err = last_err.expect("etcd retry loop must record the last error");
    Err(anyhow::anyhow!("{}: {:?}", context, err))
}

#[pyclass(name = "EtcdKvClient")]
pub struct PyEtcdKvClient {
    rt: Arc<Runtime>,
    endpoints: Vec<String>,
    pool_entry: PooledEtcdClient,
}

#[pymethods]
impl PyEtcdKvClient {
    #[new]
    fn new(endpoints: Vec<String>) -> PyResult<Self> {
        let endpoints = normalize_raw_endpoints(endpoints, "EtcdKvClient")?;
        let pool_entry = etcd_clients_pool().acquire(endpoints.clone());
        Ok(Self {
            rt: crate::mpsc::get_global_runtime(),
            endpoints,
            pool_entry,
        })
    }

    fn get(&self, py: Python<'_>, key: String) -> PyResult<Option<Py<PyBytes>>> {
        if key.is_empty() {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "etcd get key must not be empty",
            ));
        }

        let pool_entry = self.pool_entry.clone();
        let key_for_op = key.clone();
        let value = py
            .allow_threads(|| {
                self.rt.run_async_from_sync(async move {
                    let resp = run_etcd_op(
                        pool_entry,
                        format!("etcd get failed for key={}", key),
                        move |mut client| {
                            let key = key_for_op.clone();
                            async move { client.get(key, None).await }
                        },
                    )
                    .await?;
                    Ok::<Option<Vec<u8>>, anyhow::Error>(
                        resp.kvs().first().map(|kv| kv.value().to_vec()),
                    )
                })
            })
            .map_err(|e| anyhow::anyhow!("runtime bridge failed in EtcdKvClient.get: {}", e))
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

        Ok(value.map(|raw| PyBytes::new_bound(py, &raw).into()))
    }

    fn get_prefix(&self, py: Python<'_>, prefix: String) -> PyResult<PyObject> {
        if prefix.is_empty() {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "etcd get_prefix prefix must not be empty",
            ));
        }

        let pool_entry = self.pool_entry.clone();
        let prefix_for_op = prefix.clone();
        let rows = py
            .allow_threads(|| {
                self.rt.run_async_from_sync(async move {
                    let resp = run_etcd_op(
                        pool_entry,
                        format!("etcd get_prefix failed for prefix={}", prefix),
                        move |mut client| {
                            let prefix = prefix_for_op.clone();
                            async move {
                                client
                                    .get(prefix, Some(etcd::GetOptions::new().with_prefix()))
                                    .await
                            }
                        },
                    )
                    .await?;
                    Ok::<Vec<(Vec<u8>, Vec<u8>)>, anyhow::Error>(
                        resp.kvs()
                            .iter()
                            .map(|kv| (kv.key().to_vec(), kv.value().to_vec()))
                            .collect(),
                    )
                })
            })
            .map_err(|e| anyhow::anyhow!("runtime bridge failed in EtcdKvClient.get_prefix: {}", e))
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

        let out = PyList::empty_bound(py);
        for (key, value) in rows {
            let item = PyTuple::new_bound(
                py,
                [
                    PyBytes::new_bound(py, &key).into_py(py),
                    PyBytes::new_bound(py, &value).into_py(py),
                ],
            );
            out.append(item)?;
        }
        Ok(out.into_any().into_py(py))
    }

    #[pyo3(signature = (key, value, lease_id=None))]
    fn put(
        &self,
        py: Python<'_>,
        key: String,
        value: PyBackedBytes,
        lease_id: Option<i64>,
    ) -> PyResult<()> {
        if key.is_empty() {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "etcd put key must not be empty",
            ));
        }
        if let Some(lease_id) = lease_id {
            if lease_id <= 0 {
                return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                    "lease_id must be positive, got {}",
                    lease_id
                )));
            }
        }

        let pool_entry = self.pool_entry.clone();
        let key_for_op = key.clone();
        let value = value.as_ref().to_vec();
        py.allow_threads(|| {
            self.rt.run_async_from_sync(async move {
                run_etcd_op(
                    pool_entry,
                    format!("etcd put failed for key={}", key),
                    move |mut client| {
                        let key = key_for_op.clone();
                        let value = value.clone();
                        async move {
                            let opts = lease_id.map(|id| etcd::PutOptions::new().with_lease(id));
                            client.put(key, value, opts).await.map(|_| ())
                        }
                    },
                )
                .await?;
                Ok::<(), anyhow::Error>(())
            })
        })
        .map_err(|e| anyhow::anyhow!("runtime bridge failed in EtcdKvClient.put: {}", e))
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))
    }

    fn delete(&self, py: Python<'_>, key: String) -> PyResult<bool> {
        if key.is_empty() {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "etcd delete key must not be empty",
            ));
        }

        let pool_entry = self.pool_entry.clone();
        let key_for_op = key.clone();
        py.allow_threads(|| {
            self.rt.run_async_from_sync(async move {
                run_etcd_op(
                    pool_entry,
                    format!("etcd delete failed for key={}", key),
                    move |mut client| {
                        let key = key_for_op.clone();
                        async move {
                            client
                                .delete(key, None)
                                .await
                                .map(|resp| resp.deleted() > 0)
                        }
                    },
                )
                .await
            })
        })
        .map_err(|e| anyhow::anyhow!("runtime bridge failed in EtcdKvClient.delete: {}", e))
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))
    }

    fn delete_prefix(&self, py: Python<'_>, prefix: String) -> PyResult<i64> {
        if prefix.is_empty() {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "etcd delete_prefix prefix must not be empty",
            ));
        }

        let pool_entry = self.pool_entry.clone();
        let prefix_for_op = prefix.clone();
        py.allow_threads(|| {
            self.rt.run_async_from_sync(async move {
                run_etcd_op(
                    pool_entry,
                    format!("etcd delete_prefix failed for prefix={}", prefix),
                    move |mut client| {
                        let prefix = prefix_for_op.clone();
                        async move {
                            client
                                .delete(prefix, Some(etcd::DeleteOptions::new().with_prefix()))
                                .await
                                .map(|resp| resp.deleted())
                        }
                    },
                )
                .await
            })
        })
        .map_err(|e| anyhow::anyhow!("runtime bridge failed in EtcdKvClient.delete_prefix: {}", e))
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))
    }

    fn lease_ttl(&self, py: Python<'_>, lease_id: i64) -> PyResult<i64> {
        if lease_id <= 0 {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                "lease_id must be positive, got {}",
                lease_id
            )));
        }

        let pool_entry = self.pool_entry.clone();
        py.allow_threads(|| {
            self.rt.run_async_from_sync(async move {
                run_etcd_op(
                    pool_entry,
                    format!("etcd lease_ttl failed for lease_id={}", lease_id),
                    move |mut client| async move {
                        client
                            .lease_time_to_live(lease_id, None)
                            .await
                            .map(|resp| resp.ttl())
                    },
                )
                .await
            })
        })
        .map_err(|e| anyhow::anyhow!("runtime bridge failed in EtcdKvClient.lease_ttl: {}", e))
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))
    }

    fn revoke_lease(&self, py: Python<'_>, lease_id: i64) -> PyResult<()> {
        if lease_id <= 0 {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                "lease_id must be positive, got {}",
                lease_id
            )));
        }

        let pool_entry = self.pool_entry.clone();
        py.allow_threads(|| {
            self.rt.run_async_from_sync(async move {
                run_etcd_op(
                    pool_entry,
                    format!("etcd revoke_lease failed for lease_id={}", lease_id),
                    move |mut client| async move { client.lease_revoke(lease_id).await.map(|_| ()) },
                )
                .await
            })
        })
        .map_err(|e| anyhow::anyhow!("runtime bridge failed in EtcdKvClient.revoke_lease: {}", e))
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))
    }

    fn __repr__(&self) -> String {
        format!("<EtcdKvClient endpoints={:?}>", self.endpoints)
    }
}

#[pyclass(name = "EtcdLock")]
pub struct PyEtcdLock {
    rt: Arc<Runtime>,
    pool_entry: PooledEtcdClient,
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
        let endpoints = normalize_raw_endpoints(endpoints, "EtcdLock")?;
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

        let pool_entry = etcd_clients_pool().acquire(endpoints.clone());
        Ok(Self {
            rt: crate::mpsc::get_global_runtime(),
            pool_entry,
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

        let pool_entry = self.pool_entry.clone();
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
                    let mut client = pool_entry.client().await.map_err(|e| {
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

        let pool_entry = self.pool_entry.clone();
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
                    let mut client = pool_entry.client().await.map_err(|e| {
                        anyhow::anyhow!("failed to connect etcd for unlock {}: {:?}", name, e)
                    })?;

                    let unlock_result = client.unlock(lock_key).await.map(|_| true).map_err(|e| {
                        anyhow::anyhow!("failed to unlock etcd lock {}: {:?}", name, e)
                    });

                    let revoke_result = match client.lease_revoke(lease_id).await {
                        Ok(_) => Ok(()),
                        Err(err) if is_lease_not_found_error(&err) => {
                            debug!(
                                target: "fluxon_pyo3::etcd",
                                "etcd lock release lease already gone: name={}, lease_id={}",
                                name,
                                lease_id
                            );
                            Ok(())
                        }
                        Err(err) => Err(err),
                    };
                    match (unlock_result, revoke_result) {
                        (Ok(unlocked), Ok(())) => Ok(unlocked),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_raw_endpoint_accepts_raw_host_port() {
        assert_eq!(
            normalize_raw_endpoint(" 127.0.0.1:2379 ").unwrap(),
            "http://127.0.0.1:2379"
        );
    }

    #[test]
    fn normalize_raw_endpoint_rejects_empty_or_schemed_endpoint() {
        assert!(normalize_raw_endpoint("").is_err());
        assert!(normalize_raw_endpoint("   ").is_err());
        assert!(normalize_raw_endpoint("http://127.0.0.1:2379").is_err());
        assert!(normalize_raw_endpoint("https://127.0.0.1:2379").is_err());
    }

    #[test]
    fn normalize_raw_endpoints_requires_at_least_one_endpoint() {
        assert!(normalize_raw_endpoints(Vec::new(), "EtcdKvClient").is_err());
        assert_eq!(
            normalize_raw_endpoints(
                vec!["127.0.0.1:2379".to_string(), "localhost:2380".to_string()],
                "EtcdKvClient",
            )
            .unwrap(),
            vec![
                "http://127.0.0.1:2379".to_string(),
                "http://localhost:2380".to_string()
            ]
        );
    }

    #[test]
    fn etcd_kv_client_constructor_normalizes_raw_endpoints() {
        let client = PyEtcdKvClient::new(vec!["127.0.0.1:2379".to_string()]).unwrap();
        assert_eq!(client.endpoints, vec!["http://127.0.0.1:2379"]);
    }

    #[test]
    fn etcd_lock_constructor_normalizes_raw_endpoints() {
        let lock = PyEtcdLock::new(
            vec!["127.0.0.1:2379".to_string()],
            "/unit-test/lock".to_string(),
            10,
            Some(1.0),
        )
        .unwrap();
        assert_eq!(
            lock.pool_entry.endpoints(),
            ["http://127.0.0.1:2379".to_string()]
        );
    }

    #[test]
    fn etcd_lock_constructor_rejects_schemed_endpoints() {
        assert!(
            PyEtcdLock::new(
                vec!["http://127.0.0.1:2379".to_string()],
                "/unit-test/lock".to_string(),
                10,
                Some(1.0),
            )
            .is_err()
        );
    }

    #[test]
    fn reconnectable_error_text_matches_transient_transport_failures() {
        assert!(is_reconnectable_etcd_error_text("StatusCode::UNAVAILABLE"));
        assert!(is_reconnectable_etcd_error_text(
            "etcdserver: request timed out"
        ));
        assert!(is_reconnectable_etcd_error_text("transport error"));
        assert!(is_reconnectable_etcd_error_text("connection closed"));
        assert!(is_reconnectable_etcd_error_text("broken pipe"));

        assert!(!is_reconnectable_etcd_error_text(
            "requested lease not found"
        ));
        assert!(!is_reconnectable_etcd_error_text("permission denied"));
    }
}
