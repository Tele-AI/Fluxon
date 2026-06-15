pub const KEY_CLUSTER_NAME: &str = "fluxon_cluster_name";
pub const KEY_MEMBER_KIND: &str = "fluxon_member_kind";
pub const KEY_ROLE: &str = "fluxon_role";
pub const KEY_MEMBER_ID: &str = "fluxon_member_id";

pub const GREPTIME_LOG_EXTRACT_KEYS_HEADER_VALUE: &str =
    "fluxon_cluster_name,fluxon_member_kind,fluxon_role,fluxon_member_id";

// ---------------- Prometheus/remote-write label keys (generic) ----------------
//
// We intentionally centralize label names to avoid schema divergence between:
// - emitters (business crates)
// - query side (fluxon_cli promql)
//
// Keep these keys stable once published.
pub const PROM_LABEL_NODE: &str = "node";
pub const PROM_LABEL_ROLE: &str = "role";
pub const PROM_LABEL_METRIC: &str = "metric";
pub const PROM_LABEL_STAT: &str = "stat";
pub const PROM_LABEL_COMPONENT: &str = "component";
pub const PROM_LABEL_PEER: &str = "peer";
pub const PROM_LABEL_RDMA_DEVICE: &str = "rdma_device";
pub const PROM_LABEL_RDMA_PORT: &str = "rdma_port";
pub const PROM_LABEL_RDMA_NETDEV: &str = "rdma_netdev";
pub const PROM_LABEL_RDMA_PCI_BDF: &str = "rdma_pci_bdf";
pub const PROM_LABEL_RDMA_TRANSFER_STATE: &str = "rdma_transfer_state";

// ---------------- KV peer network observe schema ----------------
//
// This metric is intentionally "per-peer" (higher cardinality) because it is consumed by the
// topology UI to attribute traffic to owner/external relationships and to machine/sub_cluster
// aggregates. Emitters must use best-effort / non-blocking paths only.
pub const PROM_METRIC_KV_PEER_NETWORK_BYTES_TOTAL: &str = "kv_peer_network_bytes_total";

pub const PROM_VALUE_KV_COMPONENT_RPC_TRANSPORT: &str = "rpc_transport";
pub const PROM_VALUE_KV_COMPONENT_TRANSFER_ENGINE: &str = "transfer_engine";
// Local IPC payload attribution.
//
// English note:
// - This is not a "network" byte counter; it is payload bytes moved via local IPC tiers, including:
//   - iceoryx2 P2P transport (external <-> external)
//   - owner shared-memory fast path (external <-> owner; direct mmap + memcpy)
// - We emit it under KV peer network metric so topology can aggregate it consistently with other components.
pub const PROM_VALUE_KV_COMPONENT_LOCAL_IPC: &str = "local_ipc";

// ---------------- Process-level resource metrics (generic) ----------------
//
// These gauges are emitted by long-running Fluxon processes via the observe actor.
// Keep names stable once published.
pub const PROM_METRIC_PROCESS_CPU_USAGE_PERCENT: &str = "process_cpu_usage_percent";
pub const PROM_METRIC_CONTAINER_MEMORY_USAGE_BYTES: &str = "container_memory_usage_bytes";
pub const PROM_METRIC_CONTAINER_MEMORY_LIMIT_BYTES: &str = "container_memory_limit_bytes";

// ---------------- Tokio runtime observe schema (stable runtime metrics) ----------------
//
// These gauges intentionally use only stable Tokio runtime metrics APIs.
// They are primarily consumed by the KV monitor page to inspect owner runtime health
// without enabling `tokio_unstable`.
pub const PROM_METRIC_TOKIO_NUM_WORKERS: &str = "tokio_num_workers";
pub const PROM_METRIC_TOKIO_ALIVE_TASKS: &str = "tokio_alive_tasks";
pub const PROM_METRIC_TOKIO_GLOBAL_QUEUE_DEPTH: &str = "tokio_global_queue_depth";
pub const PROM_METRIC_TOKIO_BUSY_PERCENT: &str = "tokio_busy_percent";
pub const PROM_METRIC_TOKIO_MAX_WORKER_BUSY_PERCENT: &str = "tokio_max_worker_busy_percent";
pub const PROM_METRIC_TOKIO_PARK_UNPARK_RATE_HZ: &str = "tokio_park_unpark_rate_hz";

// ---------------- RDMA observe schema ----------------
//
// RDMA metrics are emitted by the commu layer from the same periodic self-probe that publishes
// etcd `rdma_runtime`. The schema is intentionally split into:
// - node-scoped health/count gauges
// - per-port gauges keyed by stable hardware/network labels
pub const PROM_METRIC_RDMA_PROBE_PORT_COUNT: &str = "rdma_probe_port_count";
pub const PROM_METRIC_RDMA_PROBE_USABLE_PORT_COUNT: &str = "rdma_probe_usable_port_count";
pub const PROM_METRIC_RDMA_PROBE_ERROR: &str = "rdma_probe_error";
pub const PROM_METRIC_RDMA_PORT_USABLE: &str = "rdma_port_usable";
pub const PROM_METRIC_RDMA_PORT_SPEED_GBPS: &str = "rdma_port_speed_gbps";
pub const PROM_METRIC_RDMA_PORT_ACTIVE_MTU_BYTES: &str = "rdma_port_active_mtu_bytes";
pub const PROM_METRIC_RDMA_PORT_GID_COUNT: &str = "rdma_port_gid_count";
pub const PROM_METRIC_RDMA_PORT_NUMA_NODE: &str = "rdma_port_numa_node";
pub const PROM_METRIC_RDMA_TRANSFER_ENGINE_STATE: &str = "rdma_transfer_engine_state";
pub const PROM_METRIC_RDMA_TRANSFER_ENGINE_START_FAILURES: &str =
    "rdma_transfer_engine_start_failures";

// ---------------- tcp_thread observe schema ----------------
//
// Windowed latency gauges emitted by the observe actor from best-effort transport samples.
pub const PROM_LABEL_TCP_THREAD_LANE: &str = "lane";
pub const PROM_METRIC_TCP_THREAD_LATENCY_STAT_US: &str = "tcp_thread_latency_stat_microseconds";
pub const PROM_METRIC_TCP_THREAD_LATENCY_SAMPLE_COUNT: &str = "tcp_thread_latency_sample_count";
pub const PROM_METRIC_TCP_THREAD_TRANSPORT_BYTES_TOTAL: &str = "tcp_thread_transport_bytes_total";
pub const PROM_METRIC_TCP_THREAD_TRANSPORT_MESSAGES_TOTAL: &str =
    "tcp_thread_transport_messages_total";
pub const PROM_METRIC_P2P_RECV_TRANSPORT_BYTES_TOTAL: &str = "p2p_recv_transport_bytes_total";
pub const PROM_METRIC_P2P_RECV_TRANSPORT_MESSAGES_TOTAL: &str = "p2p_recv_transport_messages_total";
pub const PROM_METRIC_P2P_RPC_COMPLETION_LATENCY_STAT_US: &str =
    "p2p_rpc_completion_latency_stat_microseconds";
pub const PROM_METRIC_P2P_RPC_COMPLETION_LATENCY_SAMPLE_COUNT: &str =
    "p2p_rpc_completion_latency_sample_count";
pub const PROM_METRIC_P2P_RPC_COMPLETION_BYTES_TOTAL: &str = "p2p_rpc_completion_bytes_total";
pub const PROM_METRIC_P2P_RPC_COMPLETION_MESSAGES_TOTAL: &str = "p2p_rpc_completion_messages_total";
pub const PROM_VALUE_P2P_RPC_COMPLETION_METRIC_RESPONSE_SUBMITTED: &str = "response_submitted";
pub const PROM_VALUE_P2P_RPC_COMPLETION_METRIC_RESPONSE_SUBMIT_FAILED: &str =
    "response_submit_failed";
pub const PROM_VALUE_P2P_RPC_COMPLETION_METRIC_USER_RPC_REQUEST_FAST_PATH_USED: &str =
    "user_rpc_request_fast_path_used";
pub const PROM_VALUE_P2P_RPC_COMPLETION_METRIC_USER_RPC_REQUEST_SLOW_PATH_USED: &str =
    "user_rpc_request_slow_path_used";
pub const PROM_VALUE_P2P_RPC_COMPLETION_METRIC_USER_RPC_REQUEST_FAST_PATH_BYPASS_TRANSPORT_POLICY:
    &str = "user_rpc_request_fast_path_bypass_transport_policy";
pub const PROM_VALUE_P2P_RPC_COMPLETION_METRIC_USER_RPC_REQUEST_FAST_PATH_BYPASS_LANE_NOT_DIRECT:
    &str = "user_rpc_request_fast_path_bypass_lane_not_direct";
pub const PROM_VALUE_P2P_RPC_COMPLETION_METRIC_USER_RPC_REQUEST_FAST_PATH_BYPASS_REMAINING_HOPS:
    &str = "user_rpc_request_fast_path_bypass_remaining_hops";
pub const PROM_VALUE_P2P_RPC_COMPLETION_METRIC_USER_RPC_REQUEST_FAST_PATH_BYPASS_PEER_NOT_READY:
    &str = "user_rpc_request_fast_path_bypass_peer_not_ready";
pub const PROM_VALUE_P2P_RPC_COMPLETION_METRIC_USER_RPC_REQUEST_FAST_PATH_BYPASS_BACKEND_EPOCH_MISSING:
    &str = "user_rpc_request_fast_path_bypass_backend_epoch_missing";
pub const PROM_VALUE_P2P_RPC_COMPLETION_METRIC_USER_RPC_REQUEST_FAST_PATH_FALLBACK_SEND_NOT_READY:
    &str = "user_rpc_request_fast_path_fallback_send_not_ready";
pub const PROM_VALUE_P2P_RPC_COMPLETION_METRIC_USER_RPC_REQUEST_FAST_PATH_FALLBACK_SEND_ERROR:
    &str = "user_rpc_request_fast_path_fallback_send_error";
pub const PROM_VALUE_P2P_RPC_COMPLETION_METRIC_USER_RPC_RESPONSE_FAST_PATH_USED: &str =
    "user_rpc_response_fast_path_used";
pub const PROM_VALUE_P2P_RPC_COMPLETION_METRIC_USER_RPC_RESPONSE_SLOW_PATH_USED: &str =
    "user_rpc_response_slow_path_used";
pub const PROM_VALUE_P2P_RPC_COMPLETION_METRIC_USER_RPC_RESPONSE_FAST_PATH_BYPASS_TRANSPORT_POLICY:
    &str = "user_rpc_response_fast_path_bypass_transport_policy";
pub const PROM_VALUE_P2P_RPC_COMPLETION_METRIC_USER_RPC_RESPONSE_FAST_PATH_BYPASS_LANE_NOT_DIRECT:
    &str = "user_rpc_response_fast_path_bypass_lane_not_direct";
pub const PROM_VALUE_P2P_RPC_COMPLETION_METRIC_USER_RPC_RESPONSE_FAST_PATH_BYPASS_REMAINING_HOPS:
    &str = "user_rpc_response_fast_path_bypass_remaining_hops";
pub const PROM_VALUE_P2P_RPC_COMPLETION_METRIC_USER_RPC_RESPONSE_FAST_PATH_BYPASS_PEER_NOT_READY:
    &str = "user_rpc_response_fast_path_bypass_peer_not_ready";
pub const PROM_VALUE_P2P_RPC_COMPLETION_METRIC_USER_RPC_RESPONSE_FAST_PATH_BYPASS_BACKEND_EPOCH_MISSING:
    &str = "user_rpc_response_fast_path_bypass_backend_epoch_missing";
pub const PROM_VALUE_P2P_RPC_COMPLETION_METRIC_USER_RPC_RESPONSE_FAST_PATH_FALLBACK_SEND_NOT_READY:
    &str = "user_rpc_response_fast_path_fallback_send_not_ready";
pub const PROM_VALUE_P2P_RPC_COMPLETION_METRIC_USER_RPC_RESPONSE_FAST_PATH_FALLBACK_SEND_ERROR:
    &str = "user_rpc_response_fast_path_fallback_send_error";
pub const PROM_VALUE_P2P_RPC_COMPLETION_LATENCY_USER_RPC_REQUEST_FAST_PATH_TOTAL: &str =
    "user_rpc_request_fast_path_total";
pub const PROM_VALUE_P2P_RPC_COMPLETION_LATENCY_USER_RPC_REQUEST_SLOW_PATH_TOTAL: &str =
    "user_rpc_request_slow_path_total";
pub const PROM_VALUE_P2P_RPC_COMPLETION_LATENCY_USER_RPC_RESPONSE_FAST_PATH_TOTAL: &str =
    "user_rpc_response_fast_path_total";
pub const PROM_VALUE_P2P_RPC_COMPLETION_LATENCY_USER_RPC_RESPONSE_SLOW_PATH_TOTAL: &str =
    "user_rpc_response_slow_path_total";

// ---------------- Fluxon FS observe schema (filesystem usage) ----------------
//
// Fluxon reports filesystem usage for user-facing mount points that the system actually touches:
// - export roots (served by fluxon_fs agents)
// - shared memory directories (KV membership)
// - /tmp
//
// Each node reports its own mount points via statvfs and emits used/total as gauges.
pub const PROM_LABEL_FS_MOUNT_KIND: &str = "fs_mount_kind";
pub const PROM_LABEL_FS_TARGET_DIR_ABS: &str = "fs_target_dir_abs";
pub const PROM_LABEL_FS_MOUNTPOINT_DIR_ABS: &str = "fs_mountpoint_dir_abs";
pub const PROM_METRIC_FS_MOUNT_FS_USED_BYTES: &str = "fs_mount_fs_used_bytes";
pub const PROM_METRIC_FS_MOUNT_FS_TOTAL_BYTES: &str = "fs_mount_fs_total_bytes";
pub const PROM_METRIC_SHM_FILE_SIZE_BYTES: &str = "shm_file_size_bytes";
pub const PROM_METRIC_SHM_FILE_ALLOCATED_BYTES: &str = "shm_file_allocated_bytes";

// ---------------- Fluxon FS observe schema (I/O frequency, path-agnostic) ----------------
//
// Count user-visible FS I/O operations without breaking down by file path (as requested).
// Use `rate(...[30s])` to get ops/s in the topology UI.
pub const PROM_LABEL_FS_IO_OP: &str = "fs_io_op";
pub const PROM_METRIC_FS_IO_OPS_TOTAL: &str = "fs_io_ops_total";

// ---------------- MQ observe schema (labels + metric names) ----------------
//
// These constants define the "MQ performance observe" schema exported via Prom remote-write.
// They are used by:
// - fluxon_mq / fluxon_pyo3 emitters
// - fluxon_cli promql queries and UI join logic
pub const PROM_LABEL_MQ_CATEGORY: &str = "mq_category";
pub const PROM_LABEL_MQ_CHAN_ID: &str = "mq_chan_id";
pub const PROM_LABEL_MQ_CONSUMER_IDX: &str = "mq_consumer_idx";
pub const PROM_LABEL_MQ_PRODUCER_IDX: &str = "mq_producer_idx";
pub const PROM_LABEL_MQ_METRIC: &str = "metric";
pub const PROM_LABEL_MQ_STAT: &str = "stat";

pub const PROM_METRIC_MQ_PREFETCH_LATENCY_US: &str = "mq_prefetch_latency_microseconds";
pub const PROM_METRIC_MQ_PREFETCH_INFLIGHT_QUEUE_SIZE: &str = "mq_prefetch_inflight_queue_size";
pub const PROM_METRIC_MQ_PREFETCH_TARGET_INFLIGHT: &str = "mq_prefetch_target_inflight";

pub const PROM_METRIC_MQ_GET_ONE_LATENCY_US: &str = "mq_get_one_latency_microseconds";
pub const PROM_METRIC_MQ_GET_ONE_WINDOW_CALLS: &str = "mq_get_one_window_calls";
pub const PROM_METRIC_MQ_GET_ONE_WINDOW_TIMEOUTS: &str = "mq_get_one_window_timeouts";
pub const PROM_METRIC_MQ_GET_ONE_WINDOW_BYTES: &str = "mq_get_one_window_bytes";

pub const PROM_METRIC_MQ_PUT_WINDOW_CALLS: &str = "mq_put_window_calls";
pub const PROM_METRIC_MQ_PUT_WINDOW_BYTES: &str = "mq_put_window_bytes";
pub const PROM_METRIC_MQ_PRODUCER_NONBLOCKING_WINDOW_CALLS: &str =
    "mq_producer_nonblocking_window_calls";
pub const PROM_METRIC_MQ_PRODUCER_NONBLOCKING_LATEST_PHASE_CALLS: &str =
    "mq_producer_nonblocking_latest_phase_calls";
pub const PROM_METRIC_MQ_PRODUCER_NONBLOCKING_LATEST_PHASE_RPS: &str =
    "mq_producer_nonblocking_latest_phase_rps";
pub const PROM_METRIC_MQ_PRODUCER_NONBLOCKING_LATEST_INTERVAL_UNIX_MS: &str =
    "mq_producer_nonblocking_latest_interval_unix_ms";
pub const PROM_METRIC_MQ_CONSUMER_NONBLOCKING_WINDOW_CALLS: &str =
    "mq_consumer_nonblocking_window_calls";
pub const PROM_METRIC_MQ_CONSUMER_NONBLOCKING_LATEST_PHASE_CALLS: &str =
    "mq_consumer_nonblocking_latest_phase_calls";
pub const PROM_METRIC_MQ_CONSUMER_NONBLOCKING_LATEST_PHASE_RPS: &str =
    "mq_consumer_nonblocking_latest_phase_rps";
pub const PROM_METRIC_MQ_CONSUMER_NONBLOCKING_LATEST_INTERVAL_UNIX_MS: &str =
    "mq_consumer_nonblocking_latest_interval_unix_ms";

// ---------------- MQ observe schema values (label values) ----------------
//
// Centralize value strings to avoid divergence between emitters and query side.
pub const PROM_VALUE_MQ_CATEGORY_MPSC: &str = "mpsc";
pub const PROM_VALUE_MQ_CATEGORY_MPMC_SUB: &str = "mpmc_sub";

pub const PROM_VALUE_MQ_STAT_AVG: &str = "avg";
pub const PROM_VALUE_MQ_STAT_LATEST: &str = "latest";
pub const PROM_VALUE_MQ_STAT_MAX: &str = "max";

// Prefetch latency metrics
pub const PROM_VALUE_MQ_PREFETCH_METRIC_GET_HANDLE: &str = "get_handle";
pub const PROM_VALUE_MQ_PREFETCH_METRIC_HANDLE_AWAIT: &str = "handle_await";
pub const PROM_VALUE_MQ_PREFETCH_METRIC_ETCD_PUT: &str = "etcd_put";

// get_one breakdown metrics
pub const PROM_VALUE_MQ_GET_ONE_METRIC_TOTAL: &str = "total";
pub const PROM_VALUE_MQ_GET_ONE_METRIC_WAIT_RX: &str = "wait_rx";
pub const PROM_VALUE_MQ_GET_ONE_METRIC_SIGNAL: &str = "signal";
pub const PROM_VALUE_MQ_GET_ONE_METRIC_POST: &str = "post";
pub const PROM_VALUE_MQ_INTERVAL_BEGIN: &str = "begin";
pub const PROM_VALUE_MQ_INTERVAL_END: &str = "end";

// ---------------- Fluxon FS transfer observe schema ----------------
//
// These constants define the transfer job history schema exported via Prom remote-write.
// They are used by:
// - fluxon_fs / fluxon_fs_s3_gateway emitters
// - transfer detail history queries in the embedded panel
pub const PROM_LABEL_TRANSFER_JOB_ID: &str = "transfer_job_id";
pub const PROM_LABEL_TRANSFER_SRC_EXPORT: &str = "transfer_src_export";
pub const PROM_LABEL_TRANSFER_DST_EXPORT: &str = "transfer_dst_export";

pub const PROM_METRIC_TRANSFER_JOB_BANDWIDTH_BYTES_PER_SEC: &str =
    "fluxon_transfer_job_bandwidth_bytes_per_sec";
pub const PROM_METRIC_TRANSFER_JOB_RUNNING_WORKER_COUNT: &str =
    "fluxon_transfer_job_running_worker_count";
pub const PROM_METRIC_TRANSFER_JOB_WRITING_BATCH_COUNT: &str =
    "fluxon_transfer_job_writing_batch_count";
pub const PROM_METRIC_TRANSFER_JOB_TOTAL_WRITTEN_BYTES: &str =
    "fluxon_transfer_job_total_written_bytes";
