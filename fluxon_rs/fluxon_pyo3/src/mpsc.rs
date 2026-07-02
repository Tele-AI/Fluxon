use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use crossbeam_channel as cbchan;
use fluxon_mq::DeleteResult as CoreDeleteResult;
use fluxon_mq::consumer::{
    ConsumedPayload as CoreConsumedPayload, MqPayload as CoreMqPayload,
    PayloadResult as CorePayloadResult,
};
use fluxon_mq::{
    BrokerChannelConfig, BrokerHandle, ChanManager, MpscConsumer as CoreMpscConsumer,
    MpscError as CoreMpscError, MpscProducer as CoreMpscProducer, ShutdownCtl,
    create::{ChanCreateConfig, create_mpsc_channel},
};
use pyo3::Py;
use pyo3::PyErr;
use pyo3::prelude::*;
use pyo3::types::{PyAny, PyBytes, PyString};
use tokio::runtime::Handle;
use tokio::runtime::Runtime;
// (no local payload buffering)

use crate::flatdict_zerocopy::{
    FlatDictDataOwner, attach_cleanup_to_flatdict_pyobject, decode_flat_dict_to_wrapped_py_object,
};
use crate::lease_manager::PyLeaseBackendUid;
use fluxon_kv::{Framework as KvFramework, KvClientTrait};
use fluxon_mq::lease_manager::LeaseBackendUid;
use fluxon_util::lease_manager::{GLOBAL_LM, LeaseManager};
use fluxon_util::run_async_from_sync::SyncAsyncBridge;
use tracing::{debug, warn};

// Type-erased payload bridging: define a local wrapper for Python objects
// that implements the core MqPayload trait so we can downcast later.
struct PyPayload {
    inner: PyObject,
    cleanup_runtime: Handle,
}

impl CoreMqPayload for PyPayload {
    fn attach_cleanup(
        &mut self,
        cleanup: fluxon_mq::consumer::PayloadCleanup,
    ) -> Result<(), fluxon_mq::consumer::PayloadCleanup> {
        let runtime = self.cleanup_runtime.clone();
        let mut cleanup = Some(cleanup);
        let attached = Python::with_gil(|py| {
            attach_cleanup_to_flatdict_pyobject(py, &self.inner, || {
                let cleanup = cleanup
                    .take()
                    .expect("cleanup must be present when DLPack attach is selected");
                Box::new(move || {
                    runtime.spawn(async move {
                        cleanup().await;
                    });
                })
            })
        });
        if attached {
            Ok(())
        } else {
            Err(cleanup.expect("cleanup must remain present when attach fails"))
        }
    }
}

// Shared runtime for PyO3 helpers that are not lifecycle-governed by a KV Framework.
// MQ producer/consumer operations should prefer the KV client's runtime/framework to
// ensure background tasks are registered and joined during `framework.shutdown()`.
static GLOBAL_RUNTIME: OnceLock<Arc<Runtime>> = OnceLock::new();

static CONSUMED_MESSAGE_CLASS: OnceLock<Py<PyAny>> = OnceLock::new();

const SUB_CLUSTER_SYNC_INTERVAL: Duration = Duration::from_secs(30);
const RUST_KV_GET_TIMEOUT: Duration = Duration::from_secs(10);
const RUST_KV_DELETE_TIMEOUT: Duration = Duration::from_secs(10);
const RUST_KV_DELETE_JOIN_WARN_INTERVAL: Duration = Duration::from_secs(1);
const PAYLOAD_STAGE_WATCHDOG_INTERVAL: Duration = Duration::from_secs(2);
const PAYLOAD_STAGE_SLOW_WARN_THRESHOLD: Duration = Duration::from_secs(1);
const BROKER_EMPTY_POLL_INTERVAL: Duration = Duration::from_millis(20);
const GET_ONE_PENDING_WARN_INTERVAL: Duration = Duration::from_secs(2);

/// Global runtime for standalone PyO3 helpers (e.g., lease_manager.rs).
/// MQ operations should use the KV client's runtime/framework instead.
pub(crate) fn get_global_runtime() -> Arc<Runtime> {
    GLOBAL_RUNTIME
        .get_or_init(|| Arc::new(Runtime::new().expect("failed to create MPSC runtime")))
        .clone()
}

fn connect_distributed_broker(kv_framework: &Arc<KvFramework>) -> BrokerHandle {
    BrokerHandle::new_distributed(
        kv_framework.cluster_manager_view().clone(),
        kv_framework.p2p_view().clone(),
    )
}

fn init_broker_for_channel(
    runtime: &Handle,
    broker: &BrokerHandle,
    channel_id: i64,
    capacity: i64,
) -> PyResult<()> {
    use pyo3::exceptions::PyRuntimeError;

    runtime
        .run_async_from_sync(async move {
            broker
                .upsert_channel(BrokerChannelConfig {
                    channel_id,
                    capacity,
                })
                .await
        })
        .map_err(|e| PyRuntimeError::new_err(format!("broker runtime bridge failed: {}", e)))?
        .map_err(|e| {
            PyRuntimeError::new_err(format!(
                "failed to upsert broker channel config: chan_id={} capacity={} err={}",
                channel_id, capacity, e
            ))
        })?;
    Ok(())
}

fn delete_broker_channel(
    runtime: &Handle,
    broker: &BrokerHandle,
    channel_id: i64,
) -> PyResult<Vec<String>> {
    use pyo3::exceptions::PyRuntimeError;

    let payload_keys = runtime
        .run_async_from_sync(async move { broker.delete_channel(channel_id).await })
        .map_err(|e| PyRuntimeError::new_err(format!("broker runtime bridge failed: {}", e)))?
        .map_err(|e| {
            PyRuntimeError::new_err(format!(
                "failed to delete broker channel: chan_id={} err={}",
                channel_id, e
            ))
        })?;
    Ok(payload_keys)
}

fn get_consumed_message_class(py: Python<'_>) -> PyResult<Py<PyAny>> {
    if let Some(c) = CONSUMED_MESSAGE_CLASS.get() {
        return Ok(c.clone_ref(py));
    }
    let module = py.import_bound("fluxon_py._api_ext_chan.mpsc")?;
    let cls = module.getattr("ConsumedMessage")?.into_py(py);
    let _ = CONSUMED_MESSAGE_CLASS.set(cls);
    Ok(CONSUMED_MESSAGE_CLASS.get().unwrap().clone_ref(py))
}

fn payload_stage_name(stage: u8) -> &'static str {
    match stage {
        0 => "init",
        1 => "kv_get",
        2 => "prepare_payload",
        3 => "wait_gil",
        4 => "py_decode_payload",
        5 => "py_consumed_message",
        6 => "reserved",
        7 => "reserved",
        8 => "finished",
        _ => "unknown",
    }
}

fn payload_result_kind(result: &CorePayloadResult) -> &'static str {
    match result {
        CorePayloadResult::Ok(_) => "ok",
        CorePayloadResult::Retryable(_) => "retryable",
        CorePayloadResult::NonRetryable(_) => "non_retryable",
    }
}

fn ns_to_ms(ns: Option<u128>) -> Option<u128> {
    ns.map(|v| v / 1_000_000)
}

fn finalize_payload_result(
    result: CorePayloadResult,
    stage: &Arc<AtomicU8>,
    done: &Arc<AtomicBool>,
    payload_begin: Instant,
    producer_id: &str,
    key: &str,
    kv_get_ns: Option<u128>,
    decode_ns: Option<u128>,
    py_wrap_ns: Option<u128>,
) -> CorePayloadResult {
    done.store(true, Ordering::Relaxed);
    let elapsed = payload_begin.elapsed();
    if elapsed >= PAYLOAD_STAGE_SLOW_WARN_THRESHOLD || !matches!(result, CorePayloadResult::Ok(_)) {
        warn!(
            "[MpscConsumer payload] finished: producer_id={} key={} stage={} result={} elapsed_ms={} kv_get_ms={:?} decode_ms={:?} py_wrap_ms={:?}",
            producer_id,
            key,
            payload_stage_name(stage.load(Ordering::Relaxed)),
            payload_result_kind(&result),
            elapsed.as_millis(),
            ns_to_ms(kv_get_ns),
            ns_to_ms(decode_ns),
            ns_to_ms(py_wrap_ns),
        );
    }
    result
}

fn map_producer_result(
    py: Python<'_>,
    result: Result<(), CoreMpscError>,
    producer: &CoreMpscProducer,
) -> PyResult<()> {
    use crate::error::CoreMpscErrorReExport as CoreErr;

    match result {
        Ok(()) => Ok(()),
        Err(e) => match e {
            CoreErr::MessageBufferFull {
                channel_id,
                capacity,
                ..
            } => Err(crate::error::pyerr_message_buffer_full(
                py,
                &e.to_string(),
                channel_id,
                capacity,
            )),
            CoreErr::PutPayloadNonRetryable | CoreErr::PutPayloadUnknownCode { .. } => {
                Err(crate::error::pyerr_chan_message_produce(
                    py,
                    &e.to_string(),
                    producer.chan_id(),
                    Some(&producer.producer_idx().to_string()),
                    None,
                ))
            }
            CoreErr::Etcd(_) => Err(crate::error::pyerr_etcd(py, &e.to_string(), "mpsc_rust")),
            CoreErr::JoinError(_) => Err(crate::error::pyerr_join_error(
                py,
                &e.to_string(),
                "mpsc_rust",
            )),
            CoreErr::Closed => Err(crate::error::pyerr_producer_closed(
                py,
                &e.to_string(),
                producer.chan_id(),
                Some(producer.producer_idx()),
            )),
            CoreErr::Internal(_) | CoreErr::NoMessage => Err(crate::error::pyerr_internal(
                py,
                &e.to_string(),
                "mpsc_rust",
            )),
            _ => Err(crate::error::pyerr_internal(
                py,
                &e.to_string(),
                "mpsc_rust",
            )),
        },
    }
}

fn map_consumer_error(py: Python<'_>, err: CoreMpscError, chan_id: i64) -> PyErr {
    use crate::error::CoreMpscErrorReExport as CoreErr;
    let message = err.to_string();

    match err {
        CoreErr::NoMessage => crate::error::pyerr_message_consumption_no_new_message(
            py, &message, chan_id, None, None,
        ),
        CoreErr::MessageBufferFull { capacity, .. } => {
            crate::error::pyerr_message_buffer_full(py, &message, chan_id, capacity)
        }
        CoreErr::GetPayloadNonRetryable { .. }
        | CoreErr::GetPayloadUnknownCode { .. }
        | CoreErr::ConsumeOffsetUpdate { .. }
        | CoreErr::DeletePayloadNonRetryable { .. }
        | CoreErr::DeletePayloadUnknownCode { .. } => {
            crate::error::pyerr_message_consumption(py, &message, chan_id, None, None)
        }
        CoreErr::PutPayloadNonRetryable | CoreErr::PutPayloadUnknownCode { .. } => {
            crate::error::pyerr_chan_message_produce(py, &message, chan_id, None, None)
        }
        CoreErr::Etcd(_) => crate::error::pyerr_etcd(py, &message, "mpsc_rust"),
        CoreErr::JoinError(_) => crate::error::pyerr_join_error(py, &message, "mpsc_rust"),
        CoreErr::Internal(_) | CoreErr::Closed => {
            crate::error::pyerr_internal(py, &message, "mpsc_rust")
        }
    }
}

fn consumed_payload_to_pyobject(
    py: Python<'_>,
    consumed: CoreConsumedPayload,
) -> PyResult<(PyObject, u64)> {
    use pyo3::exceptions::PyRuntimeError;

    let CoreConsumedPayload { payload, .. } = consumed;
    let pyobj = match payload.downcast::<PyPayload>() {
        Ok(v) => v.inner,
        Err(_) => {
            return Err(PyRuntimeError::new_err(
                "payload type mismatch: expected PyPayload",
            ));
        }
    };

    let payload_len: u64 = {
        let any = pyobj.bind(py);
        if any.is_instance_of::<PyBytes>() {
            let b = any
                .downcast::<PyBytes>()
                .expect("PyBytes downcast failed after is_instance_of");
            b.as_bytes().len() as u64
        } else {
            0
        }
    };
    Ok((pyobj, payload_len))
}

// (LeaseManagerHandle and PyLease moved to lease_manager.rs)

/// Shared MPSC context bound to a specific etcd endpoint set.
/// Holds only the endpoints; runtime and lease managers are
/// singletons under the hood.
#[pyclass]
pub struct MpscContext {
    endpoints: Vec<String>,
    kv_backend_uid: LeaseBackendUid,
    kv_framework: Arc<KvFramework>,
    kv_runtime: Handle,
    mq_framework: Option<fluxon_mq::Framework>,
}

#[pymethods]
impl MpscContext {
    /// Create a new MPSC context from etcd endpoints.
    ///
    /// The runtime is created lazily on first use and shared
    /// globally; `LeaseManager::for_endpoints` handles per-endpoint
    /// singletons internally.
    #[new]
    fn new(
        py: Python<'_>,
        etcd_endpoints: Vec<String>,
        kv_backend_uid: Py<PyLeaseBackendUid>,
        kv_client: Py<crate::KvClient>,
    ) -> PyResult<Self> {
        let uid = kv_backend_uid.borrow(py).backend_uid().clone();
        let kv_client_ref = kv_client.borrow(py);
        let mq_context = crate::new_fluxon_mq_context(&kv_client_ref)?;
        Ok(Self {
            endpoints: etcd_endpoints,
            kv_backend_uid: uid,
            kv_framework: mq_context.kv_framework.clone(),
            kv_runtime: mq_context.runtime.clone(),
            mq_framework: Some(mq_context.mq_framework.clone()),
        })
    }

    /// Create a producer bound to a channel. If `chan_id` is None,
    /// a new channel will be created using the provided capacity
    /// and ttl.
    ///
    /// `ttl_seconds` controls the member lease TTL, `weight`
    /// configures the smooth weighted RR weight for this producer.
    ///
    /// 为满足 Python 语法约束（可选参数不能在必选参数之前），
    /// 这里将 `ttl_seconds` 也声明为可选，并在实现中主动校验
    /// 其必填性。
    ///
    /// `override_global_lease_id` / `override_member_lease_id`
    /// 允许上层（例如 MPMC）覆写 channel 的 global / member
    /// lease。若提供，则 `create_mpsc_channel` 将复用该 lease
    /// 而不是新建；生命周期仍由上层控制，本层只负责
    /// keepalive，并在 drop 时不 revoke。
    #[pyo3(signature = (chan_id=None, ttl_seconds=None, weight=None, capacity=None, override_global_lease_id=None, override_member_lease_id=None, override_payload_lease_id=None, parent_mpmc_id_opt=None, parent_mpmc_member_id_opt=None))]
    fn new_producer(
        &self,
        chan_id: Option<i64>,
        ttl_seconds: Option<i64>,
        weight: Option<i64>,
        capacity: Option<i64>,
        override_global_lease_id: Option<i64>,
        override_member_lease_id: Option<i64>,
        override_payload_lease_id: Option<i64>,
        parent_mpmc_id_opt: Option<i64>,
        parent_mpmc_member_id_opt: Option<i64>,
        py: Python<'_>,
    ) -> PyResult<MpscProducerHandle> {
        let ttl_seconds = match ttl_seconds {
            Some(v) => v,
            None => {
                return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                    "ttl_seconds is required for new_producer",
                ));
            }
        };

        let endpoints = self.endpoints.clone();
        let kv_backend_uid = self.kv_backend_uid.clone();
        let self_info = self
            .kv_framework
            .cluster_manager_view()
            .cluster_manager()
            .get_self_info();
        let external_client_id = Some(self_info.id.to_string());
        let observe_node_id = self_info.id.to_string();
        let observe_node_role = self_info.node_role().to_string();
        let observe = self
            .kv_framework
            .metric_reporter_view()
            .metric_reporter()
            .metrics_handle();
        let lifecycle = self.mq_framework.as_ref().cloned().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("MpscContext is closed")
        })?;
        let shutdown = ShutdownCtl::new();
        let shutdown_for_core = shutdown.clone();
        let runtime = self.kv_runtime.clone();
        let rth = runtime.clone();
        let outer = py.allow_threads(|| runtime
            .run_async_from_sync(async move {
                let lease_manager = GLOBAL_LM.clone();

                // Construct channel manager either by creating a new
                // channel or by binding to an existing one.
                let chan_mgr: anyhow::Result<ChanManager> = async {
                    match chan_id {
                        Some(id) => ChanManager::new_with_chan_id(
                            lease_manager.clone(),
                            endpoints.clone(),
                            kv_backend_uid.clone(),
                            id,
                            rth.clone(),
                        )
                        .await
                        .map_err(|e| anyhow::anyhow!(e.to_string())),
                        None => {
                            let cap = capacity.ok_or_else(|| {
                                anyhow::anyhow!(
                                    "capacity is required when chan_id is None for new_producer"
                                )
                            })?;
                            let cfg = ChanCreateConfig {
                                capacity: cap,
                                ttl_seconds,
                                weight,
                                override_global_lease_id,
                                override_member_lease_id,
                                override_payload_lease_id,
                            };
                            create_mpsc_channel(
                                &GLOBAL_LM,
                                endpoints.clone(),
                                kv_backend_uid.clone(),
                                cfg,
                                rth.clone(),
                            )
                            .await
                            .map_err(|e| anyhow::anyhow!(e.to_string()))
                        }
                    }
                }
                .await;
                let chan_mgr = chan_mgr?;

                let category = match (parent_mpmc_id_opt, parent_mpmc_member_id_opt) {
                    (Some(pid), Some(_mid)) => fluxon_mq::keys::MqCategory::MpmcSub { parent_mpmc_id: pid },
                    (None, None) => fluxon_mq::keys::MqCategory::Mpsc,
                    _ => return Err(anyhow::anyhow!("parent_mpmc_id_opt and parent_mpmc_member_id_opt must be both provided or both None")),
                };

                CoreMpscProducer::bind_mpsc(
                    chan_mgr,
                    ttl_seconds,
                    weight,
                    lifecycle,
                    shutdown_for_core,
                    external_client_id,
                    category,
                    parent_mpmc_member_id_opt,
                    observe_node_id,
                    observe_node_role,
                    observe,
                )
                .await
            }))
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "runtime bridge failed in new_producer: {}",
                e
            )))?;
        let producer = outer.map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "failed to bind MPSC producer: {}",
                e
            ))
        })?;

        Ok(MpscProducerHandle {
            inner: Some(producer),
            broker: None,
            shutdown,
            kv_framework: self.kv_framework.clone(),
            kv_runtime: self.kv_runtime.clone(),
            put_profile_next_log_at: Instant::now() + Duration::from_secs(30),
            put_profile_window_calls: 0,
            put_profile_window_bytes: 0,
        })
    }

    /// Create a consumer bound to a channel. If `chan_id` is None,
    /// a new channel will be created using the provided capacity
    /// and ttl.
    ///
    /// `ttl_seconds` controls the member lease TTL.
    ///
    /// 同样地，将 `ttl_seconds` 声明为可选并在实现中主动校验。
    ///
    /// 覆写 lease 语义与 `new_producer` 相同。
    #[pyo3(signature = (chan_id=None, ttl_seconds=None, capacity=None, override_global_lease_id=None, override_member_lease_id=None, override_payload_lease_id=None, parent_mpmc_id_opt=None, parent_mpmc_member_id_opt=None))]
    fn new_consumer(
        &self,
        chan_id: Option<i64>,
        ttl_seconds: Option<i64>,
        capacity: Option<i64>,
        override_global_lease_id: Option<i64>,
        override_member_lease_id: Option<i64>,
        override_payload_lease_id: Option<i64>,
        parent_mpmc_id_opt: Option<i64>,
        parent_mpmc_member_id_opt: Option<i64>,
        py: Python<'_>,
    ) -> PyResult<MpscConsumerHandle> {
        let ttl_seconds = match ttl_seconds {
            Some(v) => v,
            None => {
                return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                    "ttl_seconds is required for new_consumer",
                ));
            }
        };

        let endpoints = self.endpoints.clone();
        let kv_backend_uid = self.kv_backend_uid.clone();
        let shutdown = ShutdownCtl::new();
        let shutdown_for_core = shutdown.clone();
        let self_info = self
            .kv_framework
            .cluster_manager_view()
            .cluster_manager()
            .get_self_info();
        let external_client_id = Some(self_info.id.to_string());
        let observe_node_id = self_info.id.to_string();
        let observe_node_role = self_info.node_role().to_string();
        let kvclient_sub_cluster: Option<String> = self_info.sub_cluster.clone();
        let observe = self
            .kv_framework
            .metric_reporter_view()
            .metric_reporter()
            .metrics_handle();
        let lifecycle = self.mq_framework.as_ref().cloned().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("MpscContext is closed")
        })?;
        let runtime = self.kv_runtime.clone();
        let rth = runtime.clone();
        let outer = py.allow_threads(|| runtime
            .run_async_from_sync(async move {
                let lease_manager = GLOBAL_LM.clone();

                let chan_mgr: anyhow::Result<ChanManager> = async {
                    match chan_id {
                        Some(id) => ChanManager::new_with_chan_id(
                            lease_manager.clone(),
                            endpoints.clone(),
                            kv_backend_uid.clone(),
                            id,
                            rth.clone(),
                        )
                        .await
                        .map_err(|e| anyhow::anyhow!(e.to_string())),
                        None => {
                            let cap = capacity.ok_or_else(|| {
                                anyhow::anyhow!(
                                    "capacity is required when chan_id is None for new_consumer"
                                )
                            })?;
                            let cfg = ChanCreateConfig {
                                capacity: cap,
                                ttl_seconds,
                                // weight is producer-only; consumer path
                                // does not configure or use it.
                                weight: None,
                                override_global_lease_id,
                                override_member_lease_id,
                                override_payload_lease_id,
                            };
                            create_mpsc_channel(
                                &GLOBAL_LM,
                                endpoints.clone(),
                                kv_backend_uid.clone(),
                                cfg,
                                rth.clone(),
                            )
                            .await
                            .map_err(|e| anyhow::anyhow!(e.to_string()))
                        }
                    }
                }
                .await;
                let chan_mgr = chan_mgr?;

                let category = match (parent_mpmc_id_opt, parent_mpmc_member_id_opt) {
                    (Some(pid), Some(_mid)) => fluxon_mq::keys::MqCategory::MpmcSub { parent_mpmc_id: pid },
                    (None, None) => fluxon_mq::keys::MqCategory::Mpsc,
                    _ => return Err(anyhow::anyhow!("parent_mpmc_id_opt and parent_mpmc_member_id_opt must be both provided or both None")),
                };

                CoreMpscConsumer::bind_mpsc(
                    chan_mgr,
                    ttl_seconds,
                    lifecycle,
                    shutdown_for_core,
                    external_client_id,
                    category,
                    kvclient_sub_cluster,
                    observe_node_id,
                    observe_node_role,
                    observe,
                )
                .await
            }))
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "runtime bridge failed in new_consumer: {}",
                e
            )))?;
        let consumer = outer.map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "failed to bind MPSC consumer: {}",
                e
            ))
        })?;

        Ok(MpscConsumerHandle {
            inner: Some(consumer),
            broker: None,
            shutdown,
            parent_mpmc_id_opt,
            kv_framework: self.kv_framework.clone(),
            kv_runtime: self.kv_runtime.clone(),
            next_sub_cluster_sync_at: Instant::now(),
            get_one_profile_next_log_at: Instant::now() + Duration::from_secs(30),
            get_one_profile_cnt: 0,
            get_one_profile_total_sum_ns: 0,
            get_one_profile_total_max_ns: 0,
            get_one_profile_wait_rx_sum_ns: 0,
            get_one_profile_wait_rx_max_ns: 0,
            get_one_profile_signal_sum_ns: 0,
            get_one_profile_signal_max_ns: 0,
            get_one_profile_post_sum_ns: 0,
            get_one_profile_post_max_ns: 0,
            get_one_profile_recv_timeouts: 0,
            get_one_profile_recv_calls: 0,
            get_one_profile_window_bytes: 0,
            get_one_profile_last_prefetch_target: 0,
            get_one_profile_last_timeout_ms: None,
        })
    }

    fn close(&mut self, py: Python<'_>) -> PyResult<()> {
        let mq_framework = match self.mq_framework.take() {
            Some(v) => v,
            None => return Ok(()),
        };
        let runtime = self.kv_runtime.clone();
        let shutdown_res = py.allow_threads(|| {
            runtime.run_async_from_sync(async move { mq_framework.shutdown().await })
        });
        match shutdown_res {
            Ok(Ok(())) => Ok(()),
            Ok(Err(e)) => Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "failed to shutdown mq framework: {}",
                e
            ))),
            Err(e) => Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "runtime bridge failed while shutting down mq framework: {}",
                e
            ))),
        }
    }

    fn delete_broker_channel(&self, chan_id: i64) -> PyResult<Vec<String>> {
        let broker = connect_distributed_broker(&self.kv_framework);
        delete_broker_channel(&self.kv_runtime, &broker, chan_id)
    }
}

/// PyO3 handle for MPSC producer. Currently this focuses on
/// identity/lease management; data path (put/get) will be
/// layered on top in follow-up work.
#[pyclass]
pub struct MpscProducerHandle {
    pub(crate) inner: Option<CoreMpscProducer>,
    broker: Option<BrokerHandle>,
    shutdown: ShutdownCtl,
    kv_framework: Arc<KvFramework>,
    kv_runtime: Handle,
    put_profile_next_log_at: Instant,
    put_profile_window_calls: u64,
    put_profile_window_bytes: u64,
}

enum ProducerPutMode {
    Etcd,
    Broker { broker: BrokerHandle },
}

impl MpscProducerHandle {
    fn put_flat_dict_ptrs_impl(
        &mut self,
        ptrs: Vec<(u8, u64, u32, u64, u32, Option<u32>)>,
        mode: ProducerPutMode,
    ) -> PyResult<()> {
        use pyo3::exceptions::PyRuntimeError;

        if self.shutdown.is_closed() {
            return Err(PyRuntimeError::new_err("MpscProducerHandle is closed"));
        }

        let kv_framework = self.kv_framework.clone();
        let kv_runtime = self.kv_runtime.clone();
        let runtime = kv_runtime.clone();

        let inner = self
            .inner
            .take()
            .ok_or_else(|| PyRuntimeError::new_err("MpscProducerHandle is already in use"))?;

        let mut ptrs_owned: Vec<(u8, usize, u32, u64, u32, Option<u32>)> =
            Vec::with_capacity(ptrs.len());
        for (type_id, key_ptr, key_len, val_u64, val_len, extra) in ptrs.into_iter() {
            let key_ptr_usize: usize = match usize::try_from(key_ptr) {
                Ok(v) => v,
                Err(_) => {
                    self.inner = Some(inner);
                    return Err(PyRuntimeError::new_err("dict_key_ptr out of range"));
                }
            };
            ptrs_owned.push((type_id, key_ptr_usize, key_len, val_u64, val_len, extra));
        }
        let payload_len =
            match fluxon_kv::memholder::kvclient_encode::calc_flat_dict_encoded_len(&ptrs_owned) {
                Ok(v) => v,
                Err(e) => {
                    self.inner = Some(inner);
                    return Err(PyRuntimeError::new_err(format!(
                        "calc_flat_dict_encoded_len failed: {}",
                        e
                    )));
                }
            };
        let ptrs_arc = Arc::new(ptrs_owned);

        let err_cell: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
        let err_for_closure = err_cell.clone();

        let (tx, rx) = cbchan::bounded::<(Result<(), CoreMpscError>, CoreMpscProducer)>(1);

        runtime.spawn(async move {
            let mut guard = ProducerGuard::new(inner, tx, ShutdownCtl::new());
            let payload_lease_id = guard.inner_mut().payload_lease_id() as u64;
            let put_payload: Arc<
                dyn Fn(String, i64, Option<String>) -> i32 + Send + Sync + 'static,
            > = Arc::new(move |key: String, _msg_id: i64, preferred_sub_cluster| {
                let mut o = fluxon_kv::client_kv_api::PutOptionalArgs::new();
                o.0.push(fluxon_kv::client_kv_api::PutOptionalArg::LeaseId(
                    payload_lease_id,
                ));
                if let Some(sc) = preferred_sub_cluster {
                    o.0.push(fluxon_kv::client_kv_api::PutOptionalArg::PreferredSubCluster(
                        sc,
                    ));
                }

                let ptrs_for_call: Vec<(u8, usize, u32, u64, u32, Option<u32>)> =
                    (*ptrs_arc).clone();
                let kv_framework_for_call = kv_framework.clone();
                let kv_runtime_for_call = kv_runtime.clone();
                let put_res = kv_runtime_for_call.run_async_from_sync(async move {
                    unsafe { kv_framework_for_call.kv_put_ptrs(&key, ptrs_for_call, o).await }
                });

                match put_res {
                    Ok(Ok(())) => 0,
                    Ok(Err(e)) => {
                        if matches!(
                            &e,
                            fluxon_kv::rpcresp_kvresult_convert::msg_and_error::KvError::Api(
                                fluxon_kv::rpcresp_kvresult_convert::msg_and_error::ApiError::NoSpace { .. }
                            )
                        ) {
                            1
                        } else {
                            if let Ok(mut g) = err_for_closure.lock() {
                                *g = Some(e.to_string());
                            }
                            2
                        }
                    }
                    Err(e) => {
                        if let Ok(mut g) = err_for_closure.lock() {
                            *g = Some(format!("runtime bridge failed: {}", e));
                        }
                        2
                    }
                }
            });

            let res = match mode {
                ProducerPutMode::Etcd => {
                    let put_payload = put_payload.clone();
                    guard
                        .inner_mut()
                        .put_with_payload(move |key, msg_id, preferred_sub_cluster| {
                            (put_payload)(key, msg_id, preferred_sub_cluster)
                        })
                        .await
                }
                ProducerPutMode::Broker { broker } => {
                    let put_payload = put_payload.clone();
                    guard
                        .inner_mut()
                        .put_with_payload_via_broker(
                            &broker,
                            payload_len,
                            move |key, msg_id, preferred_sub_cluster| {
                                (put_payload)(key, msg_id, preferred_sub_cluster)
                            },
                        )
                        .await
                }
            };

            guard.finish(res);
        });

        let (mapped, maybe_back) = Python::with_gil(|py| {
            let (result, producer_back) = loop {
                match py.allow_threads(|| rx.recv_timeout(Duration::from_millis(50))) {
                    Ok(v) => break v,
                    Err(cbchan::RecvTimeoutError::Timeout) => {}
                    Err(cbchan::RecvTimeoutError::Disconnected) => {
                        self.shutdown.close();
                        return (
                            Err(crate::error::pyerr_chan_message_produce(
                                py,
                                "producer is closed",
                                self.chan_id(),
                                Some(&self.producer_idx().to_string()),
                                None,
                            )),
                            None,
                        );
                    }
                }
                if let Err(e) = py.check_signals() {
                    self.shutdown.close();
                    return (Err(e), None);
                }
            };

            if let Ok(mut guard) = err_cell.lock() {
                if let Some(msg) = guard.take() {
                    return (
                        Err(crate::error::pyerr_chan_message_produce(
                            py,
                            &msg,
                            producer_back.chan_id(),
                            Some(&producer_back.producer_idx().to_string()),
                            None,
                        )),
                        Some(producer_back),
                    );
                }
            }

            (
                map_producer_result(py, result, &producer_back),
                Some(producer_back),
            )
        });

        if let Some(back) = maybe_back {
            if mapped.is_ok() {
                self.put_profile_window_calls += 1;
                self.put_profile_window_bytes += payload_len;
                let now = Instant::now();
                if now >= self.put_profile_next_log_at && self.put_profile_window_calls > 0 {
                    back.observe_put_window(
                        self.put_profile_window_calls,
                        self.put_profile_window_bytes,
                    );
                    self.put_profile_next_log_at = now + Duration::from_secs(30);
                    self.put_profile_window_calls = 0;
                    self.put_profile_window_bytes = 0;
                }
            }
            self.inner = Some(back);
        }

        mapped
    }
}

#[pymethods]
impl MpscProducerHandle {
    fn chan_id(&self) -> i64 {
        self.inner
            .as_ref()
            .expect("MpscProducerHandle inner not initialized or already taken by an in-flight put")
            .chan_id()
    }

    fn producer_idx(&self) -> String {
        self.inner
            .as_ref()
            .expect("MpscProducerHandle inner not initialized or already taken by an in-flight put")
            .producer_idx()
            .to_string()
    }

    fn payload_lease_id(&self) -> i64 {
        self.inner
            .as_ref()
            .expect("MpscProducerHandle inner not initialized or already taken by an in-flight put")
            .payload_lease_id()
    }

    #[pyo3(signature = (ptrs))]
    fn put_flat_dict_ptrs(
        &mut self,
        ptrs: Vec<(u8, u64, u32, u64, u32, Option<u32>)>,
    ) -> PyResult<()> {
        use pyo3::exceptions::PyRuntimeError;

        let broker = self
            .broker
            .clone()
            .ok_or_else(|| PyRuntimeError::new_err("broker is not initialized"))?;
        self.put_flat_dict_ptrs_impl(ptrs, ProducerPutMode::Broker { broker })
    }

    #[pyo3(signature = (ptrs))]
    fn put_flat_dict_ptrs_legacy_for_internal_check(
        &mut self,
        ptrs: Vec<(u8, u64, u32, u64, u32, Option<u32>)>,
    ) -> PyResult<()> {
        self.put_flat_dict_ptrs_impl(ptrs, ProducerPutMode::Etcd)
    }

    fn init_broker(&mut self) -> PyResult<()> {
        use pyo3::exceptions::PyRuntimeError;

        let inner = self
            .inner
            .as_ref()
            .ok_or_else(|| PyRuntimeError::new_err("MpscProducerHandle inner not initialized"))?;
        let broker = connect_distributed_broker(&self.kv_framework);
        init_broker_for_channel(
            &self.kv_runtime,
            &broker,
            inner.chan_id(),
            inner.channel_capacity(),
        )?;
        self.broker = Some(broker);
        Ok(())
    }

    // Removed: the legacy `put_with_payload(callback)` API was intentionally deleted to
    // force a single supported data path (put_flat_dict_ptrs) and avoid Python callbacks
    // in the hot put loop.
    fn shutdown_clone(&mut self) -> PyShutdownCtl {
        PyShutdownCtl {
            shutdown: self.shutdown.clone(),
        }
        // self.shutdown.clone()
    }

    fn record_nonblocking_put_success(&mut self, unix_ms: i64) -> PyResult<()> {
        if self.shutdown.is_closed() {
            return Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                "MpscProducerHandle is closed",
            ));
        }
        self.inner
            .as_ref()
            .expect("MpscProducerHandle inner not initialized")
            .record_nonblocking_put_success(unix_ms);
        Ok(())
    }

    fn record_blocking_put_observed(&mut self, unix_ms: i64) -> PyResult<()> {
        if self.shutdown.is_closed() {
            return Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                "MpscProducerHandle is closed",
            ));
        }
        self.inner
            .as_ref()
            .expect("MpscProducerHandle inner not initialized")
            .record_blocking_put_observed(unix_ms);
        Ok(())
    }
}

#[pyclass]
pub struct PyShutdownCtl {
    shutdown: ShutdownCtl,
}

#[pymethods]
impl PyShutdownCtl {
    fn close(&self) {
        self.shutdown.close();
    }
}

/// PyO3 handle for MPSC consumer.
#[pyclass]
pub struct MpscConsumerHandle {
    pub(crate) inner: Option<CoreMpscConsumer>,
    broker: Option<BrokerHandle>,
    shutdown: ShutdownCtl,
    /// Optional parent MPMC id when this MPSC acts as a submodule of a MPMC channel.
    /// Only used for diagnostics (rate-limited retry logging) and not for behavior.
    parent_mpmc_id_opt: Option<i64>,
    kv_framework: Arc<KvFramework>,
    kv_runtime: Handle,
    next_sub_cluster_sync_at: Instant,
    // PyO3 get_one call-site profiler (Rust-side). This complements:
    // - fluxon_mq prefetch latency logs (actor/internal view)
    // - Python call-site profiler in fluxon_py/_api_ext_chan/mpsc.py (Python view)
    //
    // The intent is to break down PyO3 get_one wall time into:
    // - time blocked in crossbeam recv loop (waiting for the async task result)
    // - time spent in Python signal checks
    // - post-processing time (downcast/extract/return)
    get_one_profile_next_log_at: Instant,
    get_one_profile_cnt: u64,
    get_one_profile_total_sum_ns: u64,
    get_one_profile_total_max_ns: u64,
    get_one_profile_wait_rx_sum_ns: u64,
    get_one_profile_wait_rx_max_ns: u64,
    get_one_profile_signal_sum_ns: u64,
    get_one_profile_signal_max_ns: u64,
    get_one_profile_post_sum_ns: u64,
    get_one_profile_post_max_ns: u64,
    get_one_profile_recv_timeouts: u64,
    get_one_profile_recv_calls: u64,
    get_one_profile_window_bytes: u64,
    get_one_profile_last_prefetch_target: usize,
    get_one_profile_last_timeout_ms: Option<i64>,
}

enum ConsumerGetMode {
    Prefetch {
        prefetch_target: usize,
        timeout_ms: Option<i64>,
        maybe_sync_sub_cluster: Option<Option<String>>,
    },
    Broker {
        broker: BrokerHandle,
        timeout_ms: Option<i64>,
    },
}

impl MpscConsumerHandle {
    fn get_one_impl(
        &mut self,
        py: Python<'_>,
        mode: ConsumerGetMode,
        profile_prefetch_target: usize,
        profile_timeout_ms: Option<i64>,
    ) -> PyResult<PyObject> {
        use pyo3::exceptions::PyRuntimeError;
        use std::time::Duration;
        let get_one_begin = std::time::Instant::now();
        let chan_id_for_profile = self.chan_id();
        let consumer_idx_for_profile = self.consumer_idx();
        self.get_one_profile_last_prefetch_target = profile_prefetch_target;
        self.get_one_profile_last_timeout_ms = profile_timeout_ms;
        if self.shutdown.is_closed() {
            return Err(crate::error::pyerr_channel_closed(
                py,
                "consumer is closed",
                self.chan_id(),
            ));
        }

        let runtime = self.kv_runtime.clone();
        let shutdown_for_task = self.shutdown.clone();

        let inner = self
            .inner
            .take()
            .ok_or_else(|| PyRuntimeError::new_err("MpscConsumerHandle is already in use"))?;

        let (tx, rx) =
            cbchan::bounded::<(Result<CoreConsumedPayload, CoreMpscError>, CoreMpscConsumer)>(1);

        runtime.spawn(async move {
            let mut guard = ConsumerGuard::new(inner, tx, ShutdownCtl::new());
            let (chan_id_for_log, consumer_idx_for_log) = {
                let inner_ref = guard.inner_mut();
                (inner_ref.chan_id(), inner_ref.consumer_idx().to_string())
            };
            let res = match mode {
                ConsumerGetMode::Prefetch {
                    prefetch_target,
                    timeout_ms,
                    maybe_sync_sub_cluster,
                } => {
                    if let Some(sc) = maybe_sync_sub_cluster {
                        if let Err(e) = guard.inner_mut().sync_kvclient_sub_cluster(sc.clone()).await
                        {
                            warn!(
                                "[MpscConsumer chan_id={} consumer_idx={}] failed to sync kvclient_sub_cluster={:?}: {}; continuing consumption",
                                chan_id_for_log, consumer_idx_for_log, sc, e
                            );
                        }
                    }
                    if let Some(ms) = timeout_ms {
                        guard
                            .inner_mut()
                            .get_with_payload_retry_wait_timeout(
                                prefetch_target,
                                Duration::from_millis(ms as u64),
                            )
                            .await
                    } else {
                        guard
                            .inner_mut()
                            .get_with_payload_retry(prefetch_target)
                            .await
                    }
                }
                ConsumerGetMode::Broker { broker, timeout_ms } => {
                    if let Some(ms) = timeout_ms {
                        let timeout = Duration::from_millis(ms.max(0) as u64);
                        let deadline = Instant::now() + timeout;
                        loop {
                            match guard.inner_mut().get_with_payload_via_broker(&broker).await {
                                Err(CoreMpscError::NoMessage)
                                    if !shutdown_for_task.is_closed()
                                        && Instant::now() < deadline =>
                                {
                                    let remaining =
                                        deadline.saturating_duration_since(Instant::now());
                                    tokio::time::sleep(std::cmp::min(
                                        BROKER_EMPTY_POLL_INTERVAL,
                                        remaining,
                                    ))
                                    .await;
                                }
                                result => break result,
                            }
                        }
                    } else {
                        guard.inner_mut().get_with_payload_via_broker(&broker).await
                    }
                }
            };
            match &res {
                Ok(payload) => {
                    debug!(
                        "[MpscConsumerHandle chan_id={} consumer_idx={}] async get finished: producer_id={} nonblocking_hit={}",
                        chan_id_for_log,
                        consumer_idx_for_log,
                        payload.producer_id,
                        payload.nonblocking_hit,
                    );
                }
                Err(err) => {
                    debug!(
                        "[MpscConsumerHandle chan_id={} consumer_idx={}] async get finished with error: {:?}",
                        chan_id_for_log,
                        consumer_idx_for_log,
                        err,
                    );
                }
            }
            guard.finish(res);
        });

        let mut wait_rx_ns: u64 = 0;
        let mut wait_rx_max_ns: u64 = 0;
        let mut signal_ns: u64 = 0;
        let mut signal_max_ns: u64 = 0;
        let mut recv_timeouts: u64 = 0;
        let mut recv_calls: u64 = 0;
        let wait_begin = Instant::now();
        let mut next_pending_warn_at = wait_begin + GET_ONE_PENDING_WARN_INTERVAL;

        let (result, consumer_back) = loop {
            recv_calls += 1;
            let recv_begin = Instant::now();
            let recv_res = py.allow_threads(|| rx.recv_timeout(Duration::from_millis(50)));
            let recv_elapsed_ns = recv_begin.elapsed().as_nanos() as u64;
            wait_rx_ns += recv_elapsed_ns;
            if recv_elapsed_ns > wait_rx_max_ns {
                wait_rx_max_ns = recv_elapsed_ns;
            }

            match recv_res {
                Ok(v) => break v,
                Err(cbchan::RecvTimeoutError::Timeout) => {
                    recv_timeouts += 1;
                    let now = Instant::now();
                    if now >= next_pending_warn_at {
                        warn!(
                            "[MpscConsumerHandle chan_id={} consumer_idx={}] get_one still pending: elapsed_ms={} recv_calls={} recv_timeouts={} prefetch_target={} timeout_ms={:?}",
                            chan_id_for_profile,
                            consumer_idx_for_profile,
                            wait_begin.elapsed().as_millis(),
                            recv_calls,
                            recv_timeouts,
                            profile_prefetch_target,
                            profile_timeout_ms,
                        );
                        next_pending_warn_at = now + GET_ONE_PENDING_WARN_INTERVAL;
                    }
                }
                Err(cbchan::RecvTimeoutError::Disconnected) => {
                    return Err(PyRuntimeError::new_err("get_one task cancelled"));
                }
            }

            let signal_begin = Instant::now();
            let signal_res = py.check_signals();
            let signal_elapsed_ns = signal_begin.elapsed().as_nanos() as u64;
            signal_ns += signal_elapsed_ns;
            if signal_elapsed_ns > signal_max_ns {
                signal_max_ns = signal_elapsed_ns;
            }

            if let Err(e) = signal_res {
                self.shutdown.close();
                return Err(e);
            }
        };

        let post_begin = Instant::now();
        self.inner = Some(consumer_back);

        let consumed = match result {
            Ok(v) => v,
            Err(e) => return Err(map_consumer_error(py, e, self.chan_id())),
        };
        let (pyobj, payload_len) = consumed_payload_to_pyobject(py, consumed)?;

        let get_one_total = get_one_begin.elapsed();
        let total_ns = get_one_total.as_nanos() as u64;
        let post_ns = post_begin.elapsed().as_nanos() as u64;

        self.get_one_profile_cnt += 1;
        self.get_one_profile_window_bytes += payload_len;
        self.get_one_profile_total_sum_ns += total_ns;
        if total_ns > self.get_one_profile_total_max_ns {
            self.get_one_profile_total_max_ns = total_ns;
        }
        self.get_one_profile_wait_rx_sum_ns += wait_rx_ns;
        if wait_rx_max_ns > self.get_one_profile_wait_rx_max_ns {
            self.get_one_profile_wait_rx_max_ns = wait_rx_max_ns;
        }
        self.get_one_profile_signal_sum_ns += signal_ns;
        if signal_max_ns > self.get_one_profile_signal_max_ns {
            self.get_one_profile_signal_max_ns = signal_max_ns;
        }
        self.get_one_profile_post_sum_ns += post_ns;
        if post_ns > self.get_one_profile_post_max_ns {
            self.get_one_profile_post_max_ns = post_ns;
        }
        self.get_one_profile_recv_timeouts += recv_timeouts;
        self.get_one_profile_recv_calls += recv_calls;

        let now = Instant::now();
        if now >= self.get_one_profile_next_log_at && self.get_one_profile_cnt > 0 {
            let cnt = self.get_one_profile_cnt;
            let avg_total_ms =
                (self.get_one_profile_total_sum_ns as f64) / (cnt as f64) / 1_000_000.0;
            let avg_wait_rx_ms =
                (self.get_one_profile_wait_rx_sum_ns as f64) / (cnt as f64) / 1_000_000.0;
            let avg_signal_ms =
                (self.get_one_profile_signal_sum_ns as f64) / (cnt as f64) / 1_000_000.0;
            let avg_post_ms =
                (self.get_one_profile_post_sum_ns as f64) / (cnt as f64) / 1_000_000.0;
            let max_total_ms = (self.get_one_profile_total_max_ns as f64) / 1_000_000.0;
            let max_wait_rx_ms = (self.get_one_profile_wait_rx_max_ns as f64) / 1_000_000.0;
            let max_signal_ms = (self.get_one_profile_signal_max_ns as f64) / 1_000_000.0;
            let max_post_ms = (self.get_one_profile_post_max_ns as f64) / 1_000_000.0;

            tracing::info!(
                "[MpscConsumerHandle chan_id={} consumer_idx={}] get_one breakdown: avg_total_ms={:.3} max_total_ms={:.3} avg_wait_rx_ms={:.3} max_wait_rx_ms={:.3} avg_signal_ms={:.3} max_signal_ms={:.3} avg_post_ms={:.3} max_post_ms={:.3} cnt={} recv_calls={} recv_timeouts={} last_prefetch_target={} last_timeout_ms={:?}",
                chan_id_for_profile,
                consumer_idx_for_profile,
                avg_total_ms,
                max_total_ms,
                avg_wait_rx_ms,
                max_wait_rx_ms,
                avg_signal_ms,
                max_signal_ms,
                avg_post_ms,
                max_post_ms,
                cnt,
                self.get_one_profile_recv_calls,
                self.get_one_profile_recv_timeouts,
                self.get_one_profile_last_prefetch_target,
                self.get_one_profile_last_timeout_ms,
            );

            self.inner
                .as_ref()
                .expect("MpscConsumerHandle inner not initialized")
                .observe_get_one_breakdown_window_ms(
                    avg_total_ms,
                    max_total_ms,
                    avg_wait_rx_ms,
                    max_wait_rx_ms,
                    avg_signal_ms,
                    max_signal_ms,
                    avg_post_ms,
                    max_post_ms,
                    cnt,
                    self.get_one_profile_recv_timeouts,
                    self.get_one_profile_window_bytes,
                );

            self.get_one_profile_next_log_at = now + Duration::from_secs(30);
            self.get_one_profile_cnt = 0;
            self.get_one_profile_total_sum_ns = 0;
            self.get_one_profile_total_max_ns = 0;
            self.get_one_profile_wait_rx_sum_ns = 0;
            self.get_one_profile_wait_rx_max_ns = 0;
            self.get_one_profile_signal_sum_ns = 0;
            self.get_one_profile_signal_max_ns = 0;
            self.get_one_profile_post_sum_ns = 0;
            self.get_one_profile_post_max_ns = 0;
            self.get_one_profile_recv_timeouts = 0;
            self.get_one_profile_recv_calls = 0;
            self.get_one_profile_window_bytes = 0;
        }
        Ok(pyobj)
    }

    fn get_batch_via_broker_impl(
        &mut self,
        py: Python<'_>,
        broker: BrokerHandle,
        batch_size: usize,
        timeout_ms: Option<i64>,
    ) -> PyResult<Vec<PyObject>> {
        use pyo3::exceptions::PyRuntimeError;

        if self.shutdown.is_closed() {
            return Err(crate::error::pyerr_channel_closed(
                py,
                "consumer is closed",
                self.chan_id(),
            ));
        }

        let chan_id_for_log = self.chan_id();
        let consumer_idx_for_log = self.consumer_idx();
        let runtime = self.kv_runtime.clone();
        let shutdown_for_task = self.shutdown.clone();
        let inner = self
            .inner
            .take()
            .ok_or_else(|| PyRuntimeError::new_err("MpscConsumerHandle is already in use"))?;
        let (tx, rx) = cbchan::bounded::<(
            Result<Vec<CoreConsumedPayload>, CoreMpscError>,
            CoreMpscConsumer,
        )>(1);

        runtime.spawn(async move {
            let mut guard = BatchConsumerGuard::new(inner, tx, ShutdownCtl::new());
            let res = {
                if let Some(ms) = timeout_ms {
                    let timeout = Duration::from_millis(ms.max(0) as u64);
                    let deadline = Instant::now() + timeout;
                    loop {
                        match guard
                            .inner_mut()
                            .get_batch_with_payload_via_broker(&broker, batch_size)
                            .await
                        {
                            Err(CoreMpscError::NoMessage)
                                if !shutdown_for_task.is_closed() && Instant::now() < deadline =>
                            {
                                let remaining = deadline.saturating_duration_since(Instant::now());
                                tokio::time::sleep(std::cmp::min(
                                    BROKER_EMPTY_POLL_INTERVAL,
                                    remaining,
                                ))
                                .await;
                            }
                            result => break result,
                        }
                    }
                } else {
                    guard
                        .inner_mut()
                        .get_batch_with_payload_via_broker(&broker, batch_size)
                        .await
                }
            };
            guard.finish(res);
        });

        let wait_begin = Instant::now();
        let mut next_pending_warn_at = wait_begin + GET_ONE_PENDING_WARN_INTERVAL;
        let mut recv_calls: u64 = 0;
        let mut recv_timeouts: u64 = 0;

        let (result, consumer_back) = loop {
            recv_calls += 1;
            let recv_res = py.allow_threads(|| rx.recv_timeout(Duration::from_millis(50)));
            match recv_res {
                Ok(v) => break v,
                Err(cbchan::RecvTimeoutError::Timeout) => {
                    recv_timeouts += 1;
                    let now = Instant::now();
                    if now >= next_pending_warn_at {
                        warn!(
                            "[MpscConsumerHandle chan_id={} consumer_idx={}] get_batch still pending: elapsed_ms={} recv_calls={} recv_timeouts={} batch_size={} timeout_ms={:?}",
                            chan_id_for_log,
                            consumer_idx_for_log,
                            wait_begin.elapsed().as_millis(),
                            recv_calls,
                            recv_timeouts,
                            batch_size,
                            timeout_ms,
                        );
                        next_pending_warn_at = now + GET_ONE_PENDING_WARN_INTERVAL;
                    }
                }
                Err(cbchan::RecvTimeoutError::Disconnected) => {
                    return Err(PyRuntimeError::new_err("get_batch task cancelled"));
                }
            }

            if let Err(e) = py.check_signals() {
                self.shutdown.close();
                return Err(e);
            }
        };

        self.inner = Some(consumer_back);
        let payloads = match result {
            Ok(v) => v,
            Err(e) => return Err(map_consumer_error(py, e, chan_id_for_log)),
        };

        let mut objects = Vec::with_capacity(payloads.len());
        for consumed in payloads {
            let (pyobj, payload_len) = consumed_payload_to_pyobject(py, consumed)?;
            self.get_one_profile_window_bytes += payload_len;
            objects.push(pyobj);
        }
        Ok(objects)
    }
}

#[pymethods]
impl MpscConsumerHandle {
    fn chan_id(&self) -> i64 {
        self.inner
            .as_ref()
            .expect("MpscConsumerHandle inner not initialized")
            .chan_id()
    }

    fn consumer_idx(&self) -> String {
        self.inner
            .as_ref()
            .expect("MpscConsumerHandle inner not initialized")
            .consumer_idx()
            .to_string()
    }

    /// Initialize the global payload callback for this consumer.
    ///
    /// 回调在 consumer 生命周期内复用；后续 `get_one` /
    /// `get_with_payload` 调用都不会再传入回调参数。
    #[pyo3(signature = (callback))]
    fn init_payload_callback(&mut self, callback: PyObject) -> PyResult<()> {
        use pyo3::exceptions::PyRuntimeError;
        use std::sync::Arc;

        let cb: Arc<PyObject> = Arc::new(callback);
        let kv_runtime = self.kv_runtime.clone();

        // Capture identifiers for rate-limited retry logging (diagnostic only).
        let mpsc_id_for_log = self.chan_id();
        let parent_mpmc_id_opt = self.parent_mpmc_id_opt;

        // Rate limit helper lives in fluxon_util::limitrate

        let bridge_cb: fluxon_mq::consumer::PayloadCallback = Arc::new(
            move |producer_id: String, key: String| {
                let cb_for_call = cb.clone();
                let kv_runtime_for_call = kv_runtime.clone();
                Box::pin(async move {
                    let producer_id_for_call = producer_id.clone();
                    let key_for_call = key.clone();

                    let join = limit_thirdparty::tokio::task::spawn_blocking(move || {
                        // Run the Python callback via a global Python executor.
                        // This avoids blocking the Tokio scheduler thread.
                        let (pid_obj, key_obj) = Python::with_gil(|py| {
                            (
                                PyString::new_bound(py, &producer_id_for_call)
                                    .unbind()
                                    .into(),
                                PyString::new_bound(py, &key_for_call).unbind().into(),
                            )
                        });

                        match fluxon_util::pyo3::run_longtime_py_function(
                            cb_for_call.as_ref(),
                            vec![pid_obj, key_obj],
                            None,
                        ) {
                            Ok(obj) => {
                                // Normalize error reporting to (code:int, msg:str). Otherwise treat as payload object.
                                Python::with_gil(|py| {
                                    if let Ok((code, msg)) = obj.extract::<(i32, String)>(py) {
                                        if code == 1 {
                                            // Rate-limited warn when starting a retry for get_payload.
                                            // Only log if parent MPMC id is provided to form a unique key (mpmc+mpsc).
                                            if let Some(mpmc_id) = parent_mpmc_id_opt {
                                                let uniq = format!(
                                                    "mpmc:{}-mpsc:{}",
                                                    mpmc_id, mpsc_id_for_log
                                                );
                                                if fluxon_util::limitrate::allow(
                                                    &uniq,
                                                    Duration::from_secs(30),
                                                    false,
                                                ) {
                                                    tracing::warn!(
                                                        "[mpsc-get] retryable get_payload; will retry. mpmc_id={}, mpsc_id={}, producer_id={}, key={}, msg={}",
                                                        mpmc_id,
                                                        mpsc_id_for_log,
                                                        producer_id,
                                                        key,
                                                        msg
                                                    );
                                                }
                                            }
                                            CorePayloadResult::Retryable(msg)
                                        } else {
                                            CorePayloadResult::NonRetryable(msg)
                                        }
                                    } else {
                                        CorePayloadResult::Ok(Box::new(PyPayload {
                                            inner: obj.clone_ref(py),
                                            cleanup_runtime: kv_runtime_for_call.clone(),
                                        }))
                                    }
                                })
                            }
                            Err(e) => {
                                // Treat Python exceptions as non-retryable and print for debugging.
                                Python::with_gil(|py| e.print(py));
                                CorePayloadResult::NonRetryable(format!(
                                    "python callback raised: {}",
                                    e
                                ))
                            }
                        }
                    });

                    match join.await {
                        Ok(v) => v,
                        Err(e) => CorePayloadResult::NonRetryable(format!(
                            "python callback join error: {}",
                            e
                        )),
                    }
                })
            },
        );

        match self.inner.as_mut() {
            Some(inner) => {
                // 同步设置回调：内部仅使用 try_send 推送命令到
                // actor，不依赖当前线程上的 Tokio runtime。
                inner.set_payload_callback(bridge_cb);
                Ok(())
            }
            None => Err(PyRuntimeError::new_err(
                "MpscConsumerHandle inner not initialized",
            )),
        }
    }

    /// Initialize a Rust-KV-backed payload callback for this consumer.
    ///
    /// This path bypasses the legacy Python callback + threadpool execution and
    /// directly calls `fluxon_kv` via the injected `Framework`. The callback
    /// reuses the same optimized flat-dict decode helper as the RPC and KV
    /// holder paths, so `bytes -> final Python payload object` stays under a
    /// single Rust-side authority.
    ///
    /// Behavior is selected by Python-side config (payload_backend); the default
    /// is Rust-KV as explicitly requested by business for benchmarking.
    fn init_payload_callback_rust_kv(&mut self) -> PyResult<()> {
        use pyo3::exceptions::PyRuntimeError;
        use std::sync::Arc;

        let acting_as_submodule = self.parent_mpmc_id_opt.is_some();
        let chan_id_for_msg_str = self.chan_id().to_string();

        let kv_framework = self.kv_framework.clone();
        let kv_runtime = self.kv_runtime.clone();

        let bridge_cb: fluxon_mq::consumer::PayloadCallback = Arc::new(
            move |producer_id: String, key: String| {
                let kv_framework_for_call = kv_framework.clone();
                let kv_runtime_for_call = kv_runtime.clone();
                let chan_id_for_msg_str_for_call = chan_id_for_msg_str.clone();
                Box::pin(async move {
                    #[derive(Clone)]
                    enum KvHolder {
                        Owner(Arc<fluxon_kv::memholder::UserMemHolder>),
                        External(Arc<fluxon_kv::memholder::ExternalMemHolder>),
                    }

                    let payload_begin = Instant::now();
                    let stage = Arc::new(AtomicU8::new(0));
                    let done = Arc::new(AtomicBool::new(false));
                    let mut kv_get_ns: Option<u128> = None;
                    let decode_ns: Option<u128> = None;
                    let mut py_wrap_ns: Option<u128> = None;

                    let stage_for_watchdog = stage.clone();
                    let done_for_watchdog = done.clone();
                    let key_for_watchdog = key.clone();
                    let producer_id_for_watchdog = producer_id.clone();
                    kv_runtime_for_call.spawn(async move {
                        let watchdog_begin = Instant::now();
                        loop {
                            tokio::time::sleep(PAYLOAD_STAGE_WATCHDOG_INTERVAL).await;
                            if done_for_watchdog.load(Ordering::Relaxed) {
                                return;
                            }
                            warn!(
                                "[MpscConsumer payload] still running: producer_id={} key={} stage={} elapsed_ms={}",
                                producer_id_for_watchdog,
                                key_for_watchdog,
                                payload_stage_name(stage_for_watchdog.load(Ordering::Relaxed)),
                                watchdog_begin.elapsed().as_millis(),
                            );
                        }
                    });

                    let key_for_call = key.clone();
                    stage.store(1, Ordering::Relaxed);
                    let kv_get_begin = Instant::now();
                    let join = kv_runtime_for_call.spawn(async move {
                        tokio::time::timeout(
                            RUST_KV_GET_TIMEOUT,
                            kv_framework_for_call.kv_get(&key_for_call),
                        )
                        .await
                    });

                    let kv_get_res = match join.await {
                        Ok(Ok(v)) => {
                            kv_get_ns = Some(kv_get_begin.elapsed().as_nanos());
                            v
                        }
                        Ok(Err(_elapsed)) => {
                            kv_get_ns = Some(kv_get_begin.elapsed().as_nanos());
                            return finalize_payload_result(
                                CorePayloadResult::Retryable(format!(
                                    "kv_get timed out after {}ms for key={}",
                                    RUST_KV_GET_TIMEOUT.as_millis(),
                                    key
                                )),
                                &stage,
                                &done,
                                payload_begin,
                                &producer_id,
                                &key,
                                kv_get_ns,
                                decode_ns,
                                py_wrap_ns,
                            );
                        }
                        Err(e) => {
                            kv_get_ns = Some(kv_get_begin.elapsed().as_nanos());
                            return finalize_payload_result(
                                CorePayloadResult::NonRetryable(format!(
                                    "kv_get join error: {}",
                                    e
                                )),
                                &stage,
                                &done,
                                payload_begin,
                                &producer_id,
                                &key,
                                kv_get_ns,
                                decode_ns,
                                py_wrap_ns,
                            );
                        }
                    };

                    let holder = match kv_get_res {
                        Ok(fluxon_kv::KvGetResult::Owner(Some(holder))) => KvHolder::Owner(holder),
                        Ok(fluxon_kv::KvGetResult::External(Some(holder))) => {
                            KvHolder::External(holder)
                        }
                        Ok(fluxon_kv::KvGetResult::Owner(None))
                        | Ok(fluxon_kv::KvGetResult::External(None)) => {
                            return finalize_payload_result(
                                CorePayloadResult::NonRetryable(format!(
                                    "kv_get returned None holder for key={}",
                                    key
                                )),
                                &stage,
                                &done,
                                payload_begin,
                                &producer_id,
                                &key,
                                kv_get_ns,
                                decode_ns,
                                py_wrap_ns,
                            );
                        }
                        Err(e) => {
                            use fluxon_kv::rpcresp_kvresult_convert::msg_and_error::{
                                ApiError, KvError,
                            };
                            let result = match &e {
                                KvError::Api(ApiError::KeyNotFound { .. })
                                | KvError::Api(ApiError::InvalidArgument { .. })
                                | KvError::Api(ApiError::UserRpcMissingPayload { .. })
                                | KvError::Api(ApiError::FileWriteError { .. }) => {
                                    CorePayloadResult::NonRetryable(format!("kv_get failed: {}", e))
                                }
                                _ => {
                                    CorePayloadResult::Retryable(format!("kv_get retryable: {}", e))
                                }
                            };
                            return finalize_payload_result(
                                result,
                                &stage,
                                &done,
                                payload_begin,
                                &producer_id,
                                &key,
                                kv_get_ns,
                                decode_ns,
                                py_wrap_ns,
                            );
                        }
                    };

                    stage.store(2, Ordering::Relaxed);
                    let stage_for_py = stage.clone();
                    let py_wrap_begin = Instant::now();
                    stage.store(3, Ordering::Relaxed);
                    let payload_owner = match &holder {
                        KvHolder::Owner(h) => FlatDictDataOwner::from_user_memholder(h.clone()),
                        KvHolder::External(h) => {
                            FlatDictDataOwner::from_external_memholder(h.clone())
                        }
                    };
                    let pyobj_res: Result<PyObject, String> = Python::with_gil(|py| {
                        stage_for_py.store(4, Ordering::Relaxed);
                        let payload_obj = decode_flat_dict_to_wrapped_py_object(py, payload_owner)
                            .map_err(|e| {
                                format!("flat dict decode failed for key={}: {}", key, e)
                            })?;
                        if acting_as_submodule {
                            stage_for_py.store(5, Ordering::Relaxed);
                            let cls = get_consumed_message_class(py)
                                .map_err(|e| format!("load ConsumedMessage failed: {}", e))?;
                            let pid_obj: PyObject = PyString::new_bound(py, &producer_id).into();
                            let chan_obj: PyObject =
                                PyString::new_bound(py, &chan_id_for_msg_str_for_call).into();
                            let msg = cls
                                .bind(py)
                                .call1((payload_obj.clone_ref(py), pid_obj, chan_obj))
                                .map_err(|e| format!("ConsumedMessage(...) failed: {}", e))?;
                            Ok(msg.into())
                        } else {
                            Ok(payload_obj)
                        }
                    });
                    py_wrap_ns = Some(py_wrap_begin.elapsed().as_nanos());
                    stage.store(8, Ordering::Relaxed);

                    match pyobj_res {
                        Ok(obj) => finalize_payload_result(
                            CorePayloadResult::Ok(Box::new(PyPayload {
                                inner: obj,
                                cleanup_runtime: kv_runtime_for_call.clone(),
                            })),
                            &stage,
                            &done,
                            payload_begin,
                            &producer_id,
                            &key,
                            kv_get_ns,
                            decode_ns,
                            py_wrap_ns,
                        ),
                        Err(msg) => finalize_payload_result(
                            CorePayloadResult::NonRetryable(msg),
                            &stage,
                            &done,
                            payload_begin,
                            &producer_id,
                            &key,
                            kv_get_ns,
                            decode_ns,
                            py_wrap_ns,
                        ),
                    }
                })
            },
        );

        match self.inner.as_mut() {
            Some(inner) => {
                inner.set_payload_callback(bridge_cb);
                Ok(())
            }
            None => Err(PyRuntimeError::new_err(
                "MpscConsumerHandle inner not initialized",
            )),
        }
    }

    /// New get API that relies on the previously initialized
    /// payload callback and returns the Python payload object.
    ///
    /// `prefetch_target` 用于驱动 Rust 侧预取窗口大小，通常
    /// 由 Python `get_data(batch_size, prefetch_num)` 计算得出。
    ///
    /// `timeout_ms` is an optional timeout (milliseconds) for waiting on an
    /// available inflight slot. If it fires, the call returns `NoMessage`.
    ///
    /// Important: once a message is reserved (i.e. an inflight JoinHandle is
    /// popped), the call will await it to completion to avoid dropping in-flight
    /// fetches and stranding offsets.
    #[pyo3(signature = (prefetch_target, timeout_ms=None))]
    fn get_one(
        &mut self,
        py: Python<'_>,
        prefetch_target: usize,
        timeout_ms: Option<i64>,
    ) -> PyResult<PyObject> {
        use pyo3::exceptions::PyRuntimeError;

        let _ = prefetch_target;
        let broker = self
            .broker
            .clone()
            .ok_or_else(|| PyRuntimeError::new_err("broker is not initialized"))?;
        self.get_one_impl(
            py,
            ConsumerGetMode::Broker { broker, timeout_ms },
            0,
            timeout_ms,
        )
    }

    #[pyo3(signature = (batch_size, prefetch_target, timeout_ms=None))]
    fn get_batch(
        &mut self,
        py: Python<'_>,
        batch_size: usize,
        prefetch_target: usize,
        timeout_ms: Option<i64>,
    ) -> PyResult<Vec<PyObject>> {
        use pyo3::exceptions::PyRuntimeError;

        let _ = prefetch_target;
        let broker = self
            .broker
            .clone()
            .ok_or_else(|| PyRuntimeError::new_err("broker is not initialized"))?;
        self.get_batch_via_broker_impl(py, broker, batch_size, timeout_ms)
    }

    #[pyo3(signature = (prefetch_target, timeout_ms=None))]
    fn get_one_legacy_for_internal_check(
        &mut self,
        py: Python<'_>,
        prefetch_target: usize,
        timeout_ms: Option<i64>,
    ) -> PyResult<PyObject> {
        let maybe_sync_sub_cluster = {
            let now = Instant::now();
            if now >= self.next_sub_cluster_sync_at {
                self.next_sub_cluster_sync_at = now + SUB_CLUSTER_SYNC_INTERVAL;
                Some(
                    self.kv_framework
                        .cluster_manager_view()
                        .cluster_manager()
                        .get_self_info()
                        .sub_cluster
                        .clone(),
                )
            } else {
                None
            }
        };
        self.get_one_impl(
            py,
            ConsumerGetMode::Prefetch {
                prefetch_target,
                timeout_ms,
                maybe_sync_sub_cluster,
            },
            prefetch_target,
            timeout_ms,
        )
    }

    fn init_broker(&mut self) -> PyResult<()> {
        use pyo3::exceptions::PyRuntimeError;

        let inner = self
            .inner
            .as_ref()
            .ok_or_else(|| PyRuntimeError::new_err("MpscConsumerHandle inner not initialized"))?;
        let broker = connect_distributed_broker(&self.kv_framework);
        init_broker_for_channel(
            &self.kv_runtime,
            &broker,
            inner.chan_id(),
            inner.channel_capacity(),
        )?;
        self.broker = Some(broker);
        Ok(())
    }

    /// Initialize a delete callback which will be invoked by Rust after
    /// a successful consume-offset commit, to remove the payload key
    /// from the backend. The Python callback signature is
    /// `callback(key: str) -> int | (int, str)` where the code
    /// semantics are:
    ///   - 0: success
    ///   - 1: retryable (e.g. transient backend/network error)
    ///   - otherwise: non-retryable, with optional message
    #[pyo3(signature = (callback))]
    fn init_delete_callback(&mut self, callback: PyObject) -> PyResult<()> {
        use pyo3::exceptions::PyRuntimeError;
        use std::sync::Arc;

        // Capture identifiers for rate-limited retry logging (diagnostic only).
        let mpsc_id_for_log = self.chan_id();
        let parent_mpmc_id_opt = self.parent_mpmc_id_opt;

        let cb: Arc<PyObject> = Arc::new(callback);

        let bridge_cb: fluxon_mq::consumer::DeleteCallback = Arc::new(move |key: String| {
            let cb_for_call = cb.clone();
            Box::pin(async move {
                let key_for_call = key.clone();
                let join = limit_thirdparty::tokio::task::spawn_blocking(move || {
                    // Run the Python delete callback via a global Python executor.
                    let key_obj = Python::with_gil(|py| {
                        PyString::new_bound(py, &key_for_call).unbind().into()
                    });

                    let obj = match fluxon_util::pyo3::run_longtime_py_function(
                        cb_for_call.as_ref(),
                        vec![key_obj],
                        None,
                    ) {
                        Ok(v) => v,
                        Err(e) => {
                            Python::with_gil(|py| e.print(py));
                            return CoreDeleteResult::NonRetryable(format!(
                                "python delete callback raised: {}",
                                e
                            ));
                        }
                    };

                    Python::with_gil(|py| {
                        // Keep a copy for logging.
                        let key_for_log = key.clone();

                        if let Ok((code, msg)) = obj.extract::<(i32, String)>(py) {
                            return if code == 0 {
                                CoreDeleteResult::Ok
                            } else if code == 1 {
                                if let Some(mpmc_id) = parent_mpmc_id_opt {
                                    let uniq = format!("mpmc:{}-mpsc:{}", mpmc_id, mpsc_id_for_log);
                                    if fluxon_util::limitrate::allow(
                                        &uniq,
                                        Duration::from_secs(30),
                                        false,
                                    ) {
                                        tracing::warn!(
                                            "[mpsc-del] retryable delete_payload; will retry. mpmc_id={}, mpsc_id={}, key={}, msg={}",
                                            mpmc_id,
                                            mpsc_id_for_log,
                                            key_for_log,
                                            msg
                                        );
                                    }
                                }
                                CoreDeleteResult::Retryable(msg)
                            } else {
                                CoreDeleteResult::NonRetryable(msg)
                            };
                        }

                        if let Ok(code) = obj.extract::<i32>(py) {
                            return if code == 0 {
                                CoreDeleteResult::Ok
                            } else if code == 1 {
                                if let Some(mpmc_id) = parent_mpmc_id_opt {
                                    let uniq = format!("mpmc:{}-mpsc:{}", mpmc_id, mpsc_id_for_log);
                                    if fluxon_util::limitrate::allow(
                                        &uniq,
                                        Duration::from_secs(30),
                                        false,
                                    ) {
                                        tracing::warn!(
                                            "[mpsc-del] retryable delete_payload; will retry. mpmc_id={}, mpsc_id={}, key={}, msg=python code=1",
                                            mpmc_id,
                                            mpsc_id_for_log,
                                            key_for_log,
                                        );
                                    }
                                }
                                CoreDeleteResult::Retryable(
                                    "retryable by python callback code=1".to_string(),
                                )
                            } else {
                                CoreDeleteResult::NonRetryable(format!(
                                    "python delete callback returned code={}",
                                    code
                                ))
                            };
                        }

                        // Default: treat as success when callback returns non-int.
                        CoreDeleteResult::Ok
                    })
                });

                match join.await {
                    Ok(v) => v,
                    Err(e) => CoreDeleteResult::NonRetryable(format!(
                        "python delete callback join error: {}",
                        e
                    )),
                }
            })
        });

        match self.inner.as_mut() {
            Some(inner) => {
                inner.set_delete_callback(bridge_cb);
                Ok(())
            }
            None => Err(PyRuntimeError::new_err(
                "MpscConsumerHandle inner not initialized",
            )),
        }
    }

    /// Call Rust-side get API using the callback that was
    /// previously initialized via `init_payload_callback`.
    ///
    /// 为向后兼容，仍保留旧接口签名；内部会先初始化回调，
    /// 然后调用 `get_one` 返回 payload。
    #[pyo3(signature = (callback))]
    fn get_with_payload(&mut self, py: Python<'_>, callback: PyObject) -> PyResult<PyObject> {
        self.init_payload_callback(callback)?;
        // Backward-compatible entry: use prefetch_target = 1.
        self.get_one(py, 1, None)
    }

    /// Mark this consumer as closed. Prefetch actor and retry loops
    /// will observe the flag and abort.
    fn shutdown_clone(&mut self) -> PyShutdownCtl {
        PyShutdownCtl {
            shutdown: self.shutdown.clone(),
        }
        // self.shutdown.clone()
    }

    /// Initialize a Rust-KV-backed delete callback for this consumer.
    ///
    /// Semantics match the legacy Python path:
    /// - KeyNotFound is treated as idempotent success.
    /// - Other network-like errors are treated as retryable.
    fn init_delete_callback_rust_kv(&mut self) -> PyResult<()> {
        use pyo3::exceptions::PyRuntimeError;

        let kv_framework = self.kv_framework.clone();
        let kv_runtime = self.kv_runtime.clone();

        let bridge_cb: fluxon_mq::consumer::DeleteCallback = std::sync::Arc::new(
            move |key: String| {
                let kv_framework_for_call = kv_framework.clone();
                let kv_runtime_for_call = kv_runtime.clone();
                Box::pin(async move {
                    let key_for_call = key.clone();
                    let child_key_for_log = key.clone();
                    let join_begin = Instant::now();
                    let mut join = kv_runtime_for_call.spawn(async move {
                        debug!(
                            "[MpscConsumer delete rust-kv] child entered kv_delete: key={}",
                            child_key_for_log,
                        );
                        tokio::time::timeout(
                            RUST_KV_DELETE_TIMEOUT,
                            kv_framework_for_call.kv_delete(&key_for_call),
                        )
                        .await
                    });

                    let kv_del_res = loop {
                        tokio::select! {
                            biased;
                            res = &mut join => {
                                break match res {
                                    Ok(Ok(v)) => v,
                                    Ok(Err(_elapsed)) => {
                                        return CoreDeleteResult::Retryable(format!(
                                            "kv_delete timed out after {}ms for key={}",
                                            RUST_KV_DELETE_TIMEOUT.as_millis(),
                                            key
                                        ));
                                    }
                                    Err(e) => {
                                        return CoreDeleteResult::NonRetryable(format!(
                                            "kv_delete join error: {}",
                                            e
                                        ));
                                    }
                                };
                            }
                            _ = tokio::time::sleep(RUST_KV_DELETE_JOIN_WARN_INTERVAL) => {
                                warn!(
                                    "[MpscConsumer delete rust-kv] join still pending: key={} waited_ms={}",
                                    key,
                                    join_begin.elapsed().as_millis(),
                                );
                            }
                        }
                    };
                    debug!(
                        "[MpscConsumer delete rust-kv] child resolved kv_delete: key={} waited_ms={}",
                        key,
                        join_begin.elapsed().as_millis(),
                    );

                    match kv_del_res {
                        Ok(()) => CoreDeleteResult::Ok,
                        Err(e) => {
                            use fluxon_kv::rpcresp_kvresult_convert::msg_and_error::{
                                ApiError, KvError,
                            };
                            match &e {
                                KvError::Api(ApiError::KeyNotFound { .. }) => CoreDeleteResult::Ok,
                                KvError::Api(ApiError::InvalidArgument { .. })
                                | KvError::Api(ApiError::UserRpcMissingPayload { .. })
                                | KvError::Api(ApiError::FileWriteError { .. }) => {
                                    CoreDeleteResult::NonRetryable(format!(
                                        "kv_delete failed: {}",
                                        e
                                    ))
                                }
                                _ => CoreDeleteResult::Retryable(format!(
                                    "kv_delete retryable: {}",
                                    e
                                )),
                            }
                        }
                    }
                })
            },
        );

        match self.inner.as_mut() {
            Some(inner) => {
                inner.set_delete_callback(bridge_cb);
                Ok(())
            }
            None => Err(PyRuntimeError::new_err(
                "MpscConsumerHandle inner not initialized",
            )),
        }
    }
}

/// Guard 类型：在异步任务中持有 CoreMpscProducer，负责在
/// 正常/异常路径下通过 crossbeam_channel 将结果和 inner 一并发回。
struct ProducerGuard {
    inner: Option<CoreMpscProducer>,
    tx: Option<cbchan::Sender<(Result<(), CoreMpscError>, CoreMpscProducer)>>,
    shutdown: ShutdownCtl,
}

impl ProducerGuard {
    fn new(
        inner: CoreMpscProducer,
        tx: cbchan::Sender<(Result<(), CoreMpscError>, CoreMpscProducer)>,
        shutdown: ShutdownCtl,
    ) -> Self {
        Self {
            inner: Some(inner),
            tx: Some(tx),
            shutdown,
        }
    }

    fn inner_mut(&mut self) -> &mut CoreMpscProducer {
        self.inner
            .as_mut()
            .expect("ProducerGuard inner already taken")
    }

    fn finish(mut self, res: Result<(), CoreMpscError>) {
        if let (Some(inner), Some(tx)) = (self.inner.take(), self.tx.take()) {
            let _ = tx.send((res, inner));
        }
    }
}

impl Drop for ProducerGuard {
    fn drop(&mut self) {
        if let (Some(inner), Some(tx)) = (self.inner.take(), self.tx.take()) {
            let err = if self.shutdown.is_closed() {
                CoreMpscError::Closed
            } else {
                CoreMpscError::Internal("producer guard dropped unexpectedly".to_string())
            };
            let _ = tx.send((Err(err), inner));
        }
    }
}

/// Guard 类型：在异步任务中持有 CoreMpscConsumer，负责在
/// 正常/异常路径下通过 crossbeam_channel 将结果和 inner 一并发回。
struct ConsumerGuard {
    inner: Option<CoreMpscConsumer>,
    tx: Option<cbchan::Sender<(Result<CoreConsumedPayload, CoreMpscError>, CoreMpscConsumer)>>,
    shutdown: ShutdownCtl,
}

impl ConsumerGuard {
    fn new(
        inner: CoreMpscConsumer,
        tx: cbchan::Sender<(Result<CoreConsumedPayload, CoreMpscError>, CoreMpscConsumer)>,
        shutdown: ShutdownCtl,
    ) -> Self {
        Self {
            inner: Some(inner),
            tx: Some(tx),
            shutdown,
        }
    }

    fn inner_mut(&mut self) -> &mut CoreMpscConsumer {
        self.inner
            .as_mut()
            .expect("ConsumerGuard inner already taken")
    }

    fn finish(mut self, res: Result<CoreConsumedPayload, CoreMpscError>) {
        if let (Some(inner), Some(tx)) = (self.inner.take(), self.tx.take()) {
            match &res {
                Ok(payload) => {
                    debug!(
                        "[ConsumerGuard] sending async get result back: chan_id={} consumer_idx={} producer_id={} nonblocking_hit={}",
                        inner.chan_id(),
                        inner.consumer_idx(),
                        payload.producer_id,
                        payload.nonblocking_hit,
                    );
                }
                Err(err) => {
                    debug!(
                        "[ConsumerGuard] sending async get error back: chan_id={} consumer_idx={} err={:?}",
                        inner.chan_id(),
                        inner.consumer_idx(),
                        err,
                    );
                }
            }
            let _ = tx.send((res, inner));
        }
    }
}

impl Drop for ConsumerGuard {
    fn drop(&mut self) {
        if let (Some(inner), Some(tx)) = (self.inner.take(), self.tx.take()) {
            let err = if self.shutdown.is_closed() {
                CoreMpscError::Closed
            } else {
                CoreMpscError::Internal("consumer guard dropped unexpectedly".to_string())
            };
            let _ = tx.send((Err(err), inner));
        }
    }
}

struct BatchConsumerGuard {
    inner: Option<CoreMpscConsumer>,
    tx: Option<
        cbchan::Sender<(
            Result<Vec<CoreConsumedPayload>, CoreMpscError>,
            CoreMpscConsumer,
        )>,
    >,
    shutdown: ShutdownCtl,
}

impl BatchConsumerGuard {
    fn new(
        inner: CoreMpscConsumer,
        tx: cbchan::Sender<(
            Result<Vec<CoreConsumedPayload>, CoreMpscError>,
            CoreMpscConsumer,
        )>,
        shutdown: ShutdownCtl,
    ) -> Self {
        Self {
            inner: Some(inner),
            tx: Some(tx),
            shutdown,
        }
    }

    fn inner_mut(&mut self) -> &mut CoreMpscConsumer {
        self.inner
            .as_mut()
            .expect("BatchConsumerGuard inner already taken")
    }

    fn finish(mut self, res: Result<Vec<CoreConsumedPayload>, CoreMpscError>) {
        if let (Some(inner), Some(tx)) = (self.inner.take(), self.tx.take()) {
            let _ = tx.send((res, inner));
        }
    }
}

impl Drop for BatchConsumerGuard {
    fn drop(&mut self) {
        if let (Some(inner), Some(tx)) = (self.inner.take(), self.tx.take()) {
            let err = if self.shutdown.is_closed() {
                CoreMpscError::Closed
            } else {
                CoreMpscError::Internal("batch consumer guard dropped unexpectedly".to_string())
            };
            let _ = tx.send((Err(err), inner));
        }
    }
}
