use crate::config::MemberKind;
use fluxon_commu::MemberRdmaTransferEngineRuntime;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TransferEngineKind {
    Closed,
    P2p,
    Unknown,
}

impl TransferEngineKind {
    pub fn parse_etcd_value(s: &str) -> Self {
        match s {
            "closed" => TransferEngineKind::Closed,
            "p2p" => TransferEngineKind::P2p,
            _ => TransferEngineKind::Unknown,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            TransferEngineKind::Closed => "closed",
            TransferEngineKind::P2p => "p2p",
            TransferEngineKind::Unknown => "unknown",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransferEngineEdge {
    pub from: String,
    pub to: String,
    pub route: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KvPeerNetworkRateSnapshot {
    pub node: String,
    pub peer: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tx_mbps: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rx_mbps: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RdmaNetdevRateSnapshot {
    pub node: String,
    pub netdev: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tx_mbps: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rx_mbps: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KvTopologyOwnerExternalMaxSnapshot {
    pub owner_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tx_mbps_max: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rx_mbps_max: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KvTopologyMachineExternalMaxSnapshot {
    pub sub_cluster: String,
    pub machine_key: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tx_mbps_max: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rx_mbps_max: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KvTopologySubClusterOwnerOwnerMaxSnapshot {
    pub sub_cluster: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tx_mbps_max: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rx_mbps_max: Option<f64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FsMountKindWire {
    Export,
    Shm,
    Tmp,
    Unknown,
}

impl FsMountKindWire {
    pub fn parse_label(s: &str) -> Self {
        match s.trim() {
            "export" => FsMountKindWire::Export,
            "shm" => FsMountKindWire::Shm,
            "tmp" => FsMountKindWire::Tmp,
            _ => FsMountKindWire::Unknown,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            FsMountKindWire::Export => "export",
            FsMountKindWire::Shm => "shm",
            FsMountKindWire::Tmp => "tmp",
            FsMountKindWire::Unknown => "unknown",
        }
    }
}

impl From<fluxon_observability::types::FsMountKind> for FsMountKindWire {
    fn from(v: fluxon_observability::types::FsMountKind) -> Self {
        match v {
            fluxon_observability::types::FsMountKind::Export => FsMountKindWire::Export,
            fluxon_observability::types::FsMountKind::Shm => FsMountKindWire::Shm,
            fluxon_observability::types::FsMountKind::Tmp => FsMountKindWire::Tmp,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FsMountFsSnapshot {
    pub node: String,
    pub mount_kind: FsMountKindWire,
    pub target_dir_abs: String,
    pub mountpoint_dir_abs: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub used_bytes: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub total_bytes: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShmFileSnapshot {
    pub node: String,
    pub shm_dir_abs: String,
    pub file_path_abs: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub logical_size_bytes: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub allocated_bytes: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FsExportRegistryRecordSnapshot {
    pub export_name: String,
    pub agent_instance_key: String,
    pub remote_root_dir_abs: String,
    pub updated_unix_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FsMountRegistryRecordSnapshot {
    pub external_instance_key: String,
    pub local_mount_dir_abs: String,
    pub remote_root_dir_abs: String,
    pub updated_unix_ms: i64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RoutePixelState {
    Off,
    Direct,
    DirectP2pMode,
    Alt,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum P2pTransportKind {
    Ice,
    Tcp,
    Websocket,
    Quic,
    Tquic,
    Unknown,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RoutePixels {
    pub p2p: RoutePixelState,
    pub p2p_transport: P2pTransportKind,
    pub te: RoutePixelState,
}

fn tokenize_route_value(s: &str) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    let mut cur = String::new();
    for ch in s.chars() {
        let ch = ch.to_ascii_lowercase();
        if ch.is_ascii_alphanumeric() || ch == '_' {
            cur.push(ch);
            continue;
        }
        if !cur.is_empty() {
            out.push(std::mem::take(&mut cur));
        }
    }
    if !cur.is_empty() {
        out.push(cur);
    }
    out
}

#[derive(Clone, Debug)]
pub struct ParseRoutePixelsResult {
    pub pixels: RoutePixels,
    pub unknown: bool,
}

pub fn parse_route_pixels(route: &str) -> ParseRoutePixelsResult {
    let tokens = tokenize_route_value(route);
    if tokens.is_empty() {
        return ParseRoutePixelsResult {
            pixels: RoutePixels {
                p2p: RoutePixelState::Off,
                p2p_transport: P2pTransportKind::Unknown,
                te: RoutePixelState::Off,
            },
            unknown: false,
        };
    }
    let mut has_p2p = false;
    let mut has_te = false;
    let mut p2p_transport = P2pTransportKind::Unknown;
    let mut te_is_p2p_mode = false;
    let mut p2p_alt = false;
    let mut te_alt = false;
    for t in tokens {
        match t.as_str() {
            "p2p" => has_p2p = true,
            "ice" => {
                has_p2p = true;
                p2p_transport = P2pTransportKind::Ice;
            }
            "tcp" => {
                has_p2p = true;
                p2p_transport = P2pTransportKind::Tcp;
            }
            "websocket" | "ws" => {
                has_p2p = true;
                p2p_transport = P2pTransportKind::Websocket;
            }
            "quic" => {
                has_p2p = true;
                p2p_transport = P2pTransportKind::Quic;
            }
            "tquic" => {
                has_p2p = true;
                p2p_transport = P2pTransportKind::Tquic;
            }
            "closed" | "te" => has_te = true,
            "transfer" | "engine" => has_te = true,
            "p2p_mode" | "p2pmode" | "p2pmod" => {
                has_te = true;
                te_is_p2p_mode = true;
            }
            "relay" => p2p_alt = true,
            "fallback" => te_alt = true,
            _ => {}
        }
    }
    if !has_p2p && !has_te && (p2p_alt || te_alt) {
        return ParseRoutePixelsResult {
            pixels: RoutePixels {
                p2p: RoutePixelState::Off,
                p2p_transport: P2pTransportKind::Unknown,
                te: RoutePixelState::Off,
            },
            unknown: true,
        };
    }
    if !has_p2p && !has_te {
        return ParseRoutePixelsResult {
            pixels: RoutePixels {
                p2p: RoutePixelState::Off,
                p2p_transport: P2pTransportKind::Unknown,
                te: RoutePixelState::Off,
            },
            unknown: true,
        };
    }
    let p2p_state = if !has_p2p {
        RoutePixelState::Off
    } else if p2p_alt {
        RoutePixelState::Alt
    } else {
        RoutePixelState::Direct
    };
    let p2p_transport = if p2p_state == RoutePixelState::Direct {
        p2p_transport
    } else {
        P2pTransportKind::Unknown
    };
    let te_state = if !has_te {
        RoutePixelState::Off
    } else if te_alt {
        RoutePixelState::Alt
    } else if te_is_p2p_mode {
        RoutePixelState::DirectP2pMode
    } else {
        RoutePixelState::Direct
    };
    ParseRoutePixelsResult {
        pixels: RoutePixels {
            p2p: p2p_state,
            p2p_transport,
            te: te_state,
        },
        unknown: false,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UiPillStatus {
    Ok,
    Na,
    Warn,
}

#[derive(Debug, Clone)]
pub struct UiPill {
    pub label: &'static str,
    pub value_text: String,
    pub status: UiPillStatus,
}

impl UiPill {
    pub fn render_text(&self) -> String {
        format!("({}: {})", self.label, self.value_text)
    }
}

fn fmt_opt_with_unit(v: Option<f64>, digits: usize, unit: &'static str) -> (String, UiPillStatus) {
    match v {
        Some(v) => (format!("{:.*}{}", digits, v, unit), UiPillStatus::Ok),
        None => ("N/A".to_string(), UiPillStatus::Na),
    }
}

fn fmt_ms_from_us(v: Option<f64>) -> (String, UiPillStatus) {
    match v {
        Some(us) => (format!("{:.3}ms", us / 1000.0), UiPillStatus::Ok),
        None => ("N/A".to_string(), UiPillStatus::Na),
    }
}

fn fmt_bytes_auto(v: Option<f64>, per_sec: bool) -> (String, UiPillStatus) {
    let Some(bytes) = v else {
        return ("N/A".to_string(), UiPillStatus::Na);
    };

    let suffix = if per_sec { "/s" } else { "" };
    let mut value = bytes;
    let mut unit = "B";
    for u in ["KB", "MB", "GB", "TB", "PB"] {
        if value.abs() < 1000.0 {
            break;
        }
        value /= 1000.0;
        unit = u;
    }
    if unit == "B" {
        (format!("{:.0}{}{}", value, unit, suffix), UiPillStatus::Ok)
    } else {
        (format!("{:.1}{}{}", value, unit, suffix), UiPillStatus::Ok)
    }
}

fn fmt_bytes_per_sec_from_mbps(v_mbps: Option<f64>) -> (String, UiPillStatus) {
    let Some(mbps) = v_mbps else {
        return ("N/A".to_string(), UiPillStatus::Na);
    };
    let bytes_per_sec = mbps * 1_000_000.0 / 8.0;
    fmt_bytes_auto(Some(bytes_per_sec), true)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NetworkConfigSnapshot {
    pub subnet_whitelist: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterMember {
    pub id: String,
    pub addresses: Vec<String>,
    pub port: Option<u16>,
    pub node_start_time: i64,
    pub metadata: BTreeMap<String, String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sub_cluster: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub network: Option<NetworkConfigSnapshot>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MemberRole {
    Master,
    OwnerClient,
    ExternalClient,
    SideTransferWorker,
    Unknown,
}

pub const AVAILABLE_MEMBER_ROLES: &[MemberRole] = &[
    MemberRole::Master,
    MemberRole::OwnerClient,
    MemberRole::ExternalClient,
    MemberRole::SideTransferWorker,
];

impl MemberRole {
    pub fn as_str(self) -> &'static str {
        match self {
            MemberRole::Master => "master",
            MemberRole::OwnerClient => "owner_client",
            MemberRole::ExternalClient => "external_client",
            MemberRole::SideTransferWorker => "side_transfer_worker",
            MemberRole::Unknown => "unknown",
        }
    }

    pub fn parse_query_str(s: &str) -> Option<Self> {
        match s {
            "master" => Some(MemberRole::Master),
            "owner_client" => Some(MemberRole::OwnerClient),
            "external_client" => Some(MemberRole::ExternalClient),
            "side_transfer_worker" => Some(MemberRole::SideTransferWorker),
            "unknown" => Some(MemberRole::Unknown),
            _ => None,
        }
    }

    pub fn is_external_like(self) -> bool {
        matches!(
            self,
            MemberRole::ExternalClient | MemberRole::SideTransferWorker
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FsComponentKind {
    Controller,
    Agent,
}

impl FsComponentKind {
    pub fn as_str(self) -> &'static str {
        match self {
            FsComponentKind::Controller => "controller",
            FsComponentKind::Agent => "agent",
        }
    }
}

const FLUXON_FS_AGENT_MARKERS: &[&str] = &[
    // English note: match both "packaged CLI" and "examples" entrypoints to avoid hard-coding a single run mode.
    "fluxon_fs_agent",
    "start_fluxon_fs_agent.py",
    "fluxon_fs/agent_cli.py",
];

const FLUXON_FS_CONTROLLER_MARKERS: &[&str] = &[
    // English note: master_cli is the config controller entrypoint (see fluxon_py/fluxon_fs/master_cli.py).
    "fluxon_fs_master",
    "start_fluxon_fs_master.py",
    "fluxon_fs/master_cli.py",
    "fluxon_fs_controller",
];

pub fn classify_fluxon_fs_component(member_id: &str, cmd: Option<&str>) -> Option<FsComponentKind> {
    fn contains_any(haystack: &str, needles: &[&str]) -> bool {
        needles.iter().any(|n| haystack.contains(n))
    }

    // English note: Fluxon FS nodes are external clients; we classify them by process cmdline / instance_key.
    // This keeps the monitor side config-free (no extra CLI args / env vars).
    if contains_any(member_id, FLUXON_FS_CONTROLLER_MARKERS) {
        return Some(FsComponentKind::Controller);
    }
    if contains_any(member_id, FLUXON_FS_AGENT_MARKERS) {
        return Some(FsComponentKind::Agent);
    }
    if let Some(cmd) = cmd {
        if contains_any(cmd, FLUXON_FS_CONTROLLER_MARKERS) {
            return Some(FsComponentKind::Controller);
        }
        if contains_any(cmd, FLUXON_FS_AGENT_MARKERS) {
            return Some(FsComponentKind::Agent);
        }
    }
    None
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemberSnapshot {
    pub member_id: String,
    pub role: MemberRole,
    pub is_p2p_relay: bool,
    pub is_side_transfer_worker: bool,
    pub node_start_time: i64,

    pub hostname: Option<String>,
    pub accessible_ip: Option<String>,
    pub shared_mem_dir: Option<String>,
    pub p2p_listen_port: Option<u16>,

    pub rdma_runtime_reported: bool,
    pub rdma_probe_error: Option<String>,
    pub rdma_devices: Vec<MemberRdmaDeviceSnapshot>,
    pub rdma_ports: Vec<MemberRdmaPortSnapshot>,
    pub rdma_transfer_engine: Option<MemberRdmaTransferEngineRuntime>,

    pub pid: Option<u32>,
    pub cmd: Option<String>,
    pub sub_cluster: Option<String>,
    pub product_uuid: Option<String>,

    pub node_cpu_usage_percent: Option<f64>,
    pub node_cpu_logical_cores: Option<f64>,
    pub node_memory_usage_bytes: Option<f64>,
    pub node_memory_total_bytes: Option<f64>,
    pub container_memory_usage_bytes: Option<f64>,
    pub container_memory_limit_bytes: Option<f64>,
    pub process_resident_memory_bytes: Option<f64>,
    pub process_cpu_usage_percent: Option<f64>,
    pub tokio_num_workers: Option<f64>,
    pub tokio_alive_tasks: Option<f64>,
    pub tokio_global_queue_depth: Option<f64>,
    pub tokio_busy_percent: Option<f64>,
    pub tokio_max_worker_busy_percent: Option<f64>,
    pub tokio_park_unpark_rate_hz: Option<f64>,
    pub process_net_tx_mbps: Option<f64>,
    pub process_net_rx_mbps: Option<f64>,

    pub kv_put_rps: Option<f64>,
    pub kv_get_rps: Option<f64>,
    pub kv_put_bps: Option<f64>,
    pub kv_get_bps: Option<f64>,

    pub kv_put_latency_mean_us: Option<f64>,
    pub kv_put_latency_p95_us: Option<f64>,
    pub kv_put_latency_p99_us: Option<f64>,
    pub kv_get_latency_mean_us: Option<f64>,
    pub kv_get_latency_p95_us: Option<f64>,
    pub kv_get_latency_p99_us: Option<f64>,

    pub seg_capacity_bytes: Option<f64>,
    pub seg_used_bytes: Option<f64>,

    pub fs_read_rps: Option<f64>,
    pub fs_write_rps: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemberRdmaDeviceSnapshot {
    pub device: String,
    pub detected: bool,
    pub desired_enabled: bool,
    pub effective_enabled: bool,
    pub total_ports: usize,
    pub usable_ports: usize,
    pub blocked_ports: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemberRdmaPortSnapshot {
    pub device: String,
    pub port: u8,
    pub port_key: String,
    pub detected: bool,
    pub desired_enabled: bool,
    pub effective_enabled: bool,
    pub usable: bool,
    pub link_layer_text: String,
    pub port_state_text: String,
    pub phys_state_text: String,
    pub active_mtu_text: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub netdev: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pci_bdf: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pcie_max_bandwidth_mbps: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub numa_node: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub speed_gbps: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub driver: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub firmware: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
}

impl MemberRdmaPortSnapshot {
    pub fn topology_identity_token(&self) -> Option<String> {
        if !self.detected {
            return None;
        }
        let base = self
            .pci_bdf
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(|v| format!("pci={v}"))
            .or_else(|| {
                self.netdev
                    .as_deref()
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .map(|v| format!("net={v}"))
            })
            .or_else(|| {
                let device = self.device.trim();
                (!device.is_empty()).then(|| format!("dev={device}"))
            })
            .unwrap_or_else(|| format!("port_key={}", self.port_key));
        Some(format!("{base}#port={}", self.port))
    }
}

impl MemberSnapshot {
    pub fn topology_nic_group_key(&self) -> String {
        let mut tokens = BTreeSet::new();
        for port in &self.rdma_ports {
            if let Some(token) = port.topology_identity_token() {
                tokens.insert(token);
            }
        }
        if !tokens.is_empty() {
            return format!("rdma:{}", tokens.into_iter().collect::<Vec<_>>().join("|"));
        }
        if self.role == MemberRole::OwnerClient {
            return format!("no-rdma-owner:{}", self.member_id);
        }
        if let Some(ip) = self
            .accessible_ip
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
        {
            return format!("no-rdma:ip={ip}");
        }
        if let Some(hostname) = self
            .hostname
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
        {
            return format!("no-rdma:host={hostname}");
        }
        format!("no-rdma:member={}", self.member_id)
    }

    pub fn topology_nic_group_meta(&self) -> Option<String> {
        let detected_ports: Vec<&MemberRdmaPortSnapshot> = self
            .rdma_ports
            .iter()
            .filter(|port| port.detected)
            .collect();
        if detected_ports.is_empty() {
            if self.role == MemberRole::OwnerClient {
                return Some("match=no_rdma_owner_unique".to_string());
            }
            return Some(format!("fallback_key={}", self.topology_nic_group_key()));
        }

        let mut devices = BTreeSet::new();
        let mut usable_ports = 0usize;
        for port in &detected_ports {
            if let Some(pci_bdf) = port
                .pci_bdf
                .as_deref()
                .map(str::trim)
                .filter(|s| !s.is_empty())
            {
                devices.insert(format!("pci={pci_bdf}"));
            } else if let Some(netdev) = port
                .netdev
                .as_deref()
                .map(str::trim)
                .filter(|s| !s.is_empty())
            {
                devices.insert(format!("net={netdev}"));
            } else if !port.device.trim().is_empty() {
                devices.insert(format!("dev={}", port.device.trim()));
            }
            if port.usable {
                usable_ports += 1;
            }
        }

        Some(format!(
            "devices={} ports={} usable={}",
            devices.len(),
            detected_ports.len(),
            usable_ports
        ))
    }

    pub fn topology_rdma_meta(&self) -> Option<String> {
        #[derive(Default)]
        struct DeviceAgg {
            device: String,
            pci_bdf: Option<String>,
            pcie_max_bandwidth_mbps: Option<u64>,
            ports: BTreeSet<u8>,
            netdevs: BTreeSet<String>,
            speeds_gbps: BTreeSet<u32>,
            numa_nodes: BTreeSet<i32>,
            drivers: BTreeSet<String>,
            firmwares: BTreeSet<String>,
            usable_ports: usize,
            total_ports: usize,
        }

        fn trimmed_owned(value: Option<&str>) -> Option<String> {
            value
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(ToOwned::to_owned)
        }

        fn join_strings(values: &BTreeSet<String>) -> Option<String> {
            (!values.is_empty()).then(|| values.iter().cloned().collect::<Vec<_>>().join(","))
        }

        fn join_u8(values: &BTreeSet<u8>) -> Option<String> {
            (!values.is_empty()).then(|| {
                values
                    .iter()
                    .map(|value| value.to_string())
                    .collect::<Vec<_>>()
                    .join(",")
            })
        }

        fn join_u32(values: &BTreeSet<u32>) -> Option<String> {
            (!values.is_empty()).then(|| {
                values
                    .iter()
                    .map(|value| format!("{value}G"))
                    .collect::<Vec<_>>()
                    .join(",")
            })
        }

        fn join_i32(values: &BTreeSet<i32>) -> Option<String> {
            (!values.is_empty()).then(|| {
                values
                    .iter()
                    .map(|value| value.to_string())
                    .collect::<Vec<_>>()
                    .join(",")
            })
        }

        fn format_rate_mbps(value_mbps: u64) -> String {
            let value_gbps = value_mbps as f64 / 1000.0;
            if value_gbps >= 100.0 {
                format!("{value_gbps:.0}G")
            } else if value_gbps >= 10.0 {
                format!("{value_gbps:.1}G")
            } else if value_gbps >= 1.0 {
                format!("{value_gbps:.2}G")
            } else {
                format!("{value_mbps}M")
            }
        }

        let detected_ports: Vec<&MemberRdmaPortSnapshot> = self
            .rdma_ports
            .iter()
            .filter(|port| port.detected)
            .collect();
        if detected_ports.is_empty() {
            let mut lines = Vec::new();
            lines.push("rdma ports=0".to_string());
            if let Some(err) = self.rdma_probe_error.as_deref() {
                lines.push(format!("probe_error={err}"));
            }
            if let Some(runtime) = self.rdma_transfer_engine.as_ref() {
                lines.push(format!(
                    "te={} start_failures={}",
                    runtime.state.as_str(),
                    runtime.consecutive_start_failures
                ));
            }
            return Some(lines.join("\n"));
        }

        let mut agg_by_key: BTreeMap<String, DeviceAgg> = BTreeMap::new();
        let mut usable_ports = 0usize;
        for port in detected_ports {
            let key = port
                .pci_bdf
                .as_deref()
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(ToOwned::to_owned)
                .unwrap_or_else(|| {
                    let device = port.device.trim();
                    if device.is_empty() {
                        port.port_key.clone()
                    } else {
                        device.to_string()
                    }
                });
            let entry = agg_by_key.entry(key).or_default();
            if entry.device.is_empty() {
                entry.device = {
                    let device = port.device.trim();
                    if device.is_empty() {
                        port.port_key.clone()
                    } else {
                        device.to_string()
                    }
                };
            }
            if entry.pci_bdf.is_none() {
                entry.pci_bdf = trimmed_owned(port.pci_bdf.as_deref());
            }
            if entry.pcie_max_bandwidth_mbps.is_none() {
                entry.pcie_max_bandwidth_mbps = port.pcie_max_bandwidth_mbps;
            }
            entry.ports.insert(port.port);
            if let Some(netdev) = trimmed_owned(port.netdev.as_deref()) {
                entry.netdevs.insert(netdev);
            }
            if let Some(speed_gbps) = port.speed_gbps {
                entry.speeds_gbps.insert(speed_gbps);
            }
            if let Some(numa_node) = port.numa_node {
                entry.numa_nodes.insert(numa_node);
            }
            if let Some(driver) = trimmed_owned(port.driver.as_deref()) {
                entry.drivers.insert(driver);
            }
            if let Some(firmware) = trimmed_owned(port.firmware.as_deref()) {
                entry.firmwares.insert(firmware);
            }
            entry.total_ports += 1;
            if port.usable {
                entry.usable_ports += 1;
                usable_ports += 1;
            }
        }

        let total_ports = agg_by_key
            .values()
            .map(|entry| entry.total_ports)
            .sum::<usize>();
        let mut lines = vec![format!(
            "rdma devices={} ports={} usable={}",
            agg_by_key.len(),
            total_ports,
            usable_ports
        )];
        for entry in agg_by_key.values() {
            let mut parts = vec![entry.device.clone()];
            if let Some(ports) = join_u8(&entry.ports) {
                parts.push(format!("ports={ports}"));
            }
            if let Some(netdevs) = join_strings(&entry.netdevs) {
                parts.push(format!("net={netdevs}"));
            }
            if let Some(pci_bdf) = entry.pci_bdf.as_ref() {
                parts.push(format!("pci={pci_bdf}"));
            }
            if let Some(speeds) = join_u32(&entry.speeds_gbps) {
                parts.push(format!("speed={speeds}"));
            }
            if let Some(pcie_max_bandwidth_mbps) = entry.pcie_max_bandwidth_mbps {
                parts.push(format!(
                    "pcie={}",
                    format_rate_mbps(pcie_max_bandwidth_mbps)
                ));
            }
            if let Some(numa_nodes) = join_i32(&entry.numa_nodes) {
                parts.push(format!("numa={numa_nodes}"));
            }
            parts.push(format!(
                "usable={}/{}",
                entry.usable_ports, entry.total_ports
            ));
            if let Some(drivers) = join_strings(&entry.drivers) {
                parts.push(format!("drv={drivers}"));
            }
            if let Some(firmwares) = join_strings(&entry.firmwares) {
                parts.push(format!("fw={firmwares}"));
            }
            lines.push(parts.join(" "));
        }
        if let Some(err) = self.rdma_probe_error.as_deref() {
            lines.push(format!("probe_error={err}"));
        }
        if let Some(runtime) = self.rdma_transfer_engine.as_ref() {
            lines.push(format!(
                "te={} start_failures={}",
                runtime.state.as_str(),
                runtime.consecutive_start_failures
            ));
        }
        Some(lines.join("\n"))
    }

    pub fn topology_rdma_usage_meta(&self) -> Option<String> {
        #[derive(Default)]
        struct DeviceAgg {
            device: String,
            pci_bdf: Option<String>,
            ports: BTreeSet<u8>,
            netdevs: BTreeSet<String>,
        }

        fn trimmed_owned(value: Option<&str>) -> Option<String> {
            value
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(ToOwned::to_owned)
        }

        fn join_strings(values: &BTreeSet<String>) -> Option<String> {
            (!values.is_empty()).then(|| values.iter().cloned().collect::<Vec<_>>().join(","))
        }

        fn join_u8(values: &BTreeSet<u8>) -> Option<String> {
            (!values.is_empty()).then(|| {
                values
                    .iter()
                    .map(|value| value.to_string())
                    .collect::<Vec<_>>()
                    .join(",")
            })
        }

        let enabled_ports: Vec<&MemberRdmaPortSnapshot> = self
            .rdma_ports
            .iter()
            .filter(|port| port.effective_enabled)
            .collect();
        if enabled_ports.is_empty() {
            return Some("rdma use=none\nports=none".to_string());
        }

        let mut agg_by_key: BTreeMap<String, DeviceAgg> = BTreeMap::new();
        for port in enabled_ports {
            let key = port
                .pci_bdf
                .as_deref()
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(ToOwned::to_owned)
                .unwrap_or_else(|| {
                    let device = port.device.trim();
                    if device.is_empty() {
                        port.port_key.clone()
                    } else {
                        device.to_string()
                    }
                });
            let entry = agg_by_key.entry(key).or_default();
            if entry.device.is_empty() {
                entry.device = {
                    let device = port.device.trim();
                    if device.is_empty() {
                        port.port_key.clone()
                    } else {
                        device.to_string()
                    }
                };
            }
            if entry.pci_bdf.is_none() {
                entry.pci_bdf = trimmed_owned(port.pci_bdf.as_deref());
            }
            entry.ports.insert(port.port);
            if let Some(netdev) = trimmed_owned(port.netdev.as_deref()) {
                entry.netdevs.insert(netdev);
            }
        }

        let total_ports = agg_by_key
            .values()
            .map(|entry| entry.ports.len())
            .sum::<usize>();
        let mut lines = vec![format!(
            "rdma use devices={} ports={}",
            agg_by_key.len(),
            total_ports
        )];
        for entry in agg_by_key.values() {
            let mut parts = vec![entry.device.clone()];
            if let Some(ports) = join_u8(&entry.ports) {
                parts.push(format!("ports={ports}"));
            }
            if let Some(netdevs) = join_strings(&entry.netdevs) {
                parts.push(format!("net={netdevs}"));
            }
            if let Some(pci_bdf) = entry.pci_bdf.as_ref() {
                parts.push(format!("pci={pci_bdf}"));
            }
            lines.push(parts.join(" "));
        }
        Some(lines.join("\n"))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SegmentDeviceSnapshot {
    pub device: String,
    pub seg_capacity_bytes: Option<f64>,
    pub seg_used_bytes: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeSnapshot {
    pub node_key: String,
    pub hostname: Option<String>,
    pub accessible_ip: Option<String>,
    pub shared_mem_dir: Option<String>,
    pub is_p2p_relay: bool,
    pub node_cpu_usage_percent: Option<f64>,
    pub node_cpu_logical_cores: Option<f64>,
    pub node_memory_usage_bytes: Option<f64>,
    pub node_memory_total_bytes: Option<f64>,
    pub container_memory_usage_bytes: Option<f64>,
    pub container_memory_limit_bytes: Option<f64>,
    pub members: Vec<MemberSnapshot>,
    pub segment_devices: Vec<SegmentDeviceSnapshot>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MqMemberStatus {
    Alive,
    Stale,
    Invalid,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MqChanMetaSnapshot {
    pub capacity: i64,
    pub ttl_seconds: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub payload_lease_id: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MqProducerSnapshot {
    pub producer_idx: String,
    pub status: MqMemberStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub external_client_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub owner_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub produce_offset: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub consume_offset: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub put_window_calls: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub put_window_bytes: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub nonblocking_latest_phase_calls: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub nonblocking_latest_phase_rps: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub nonblocking_latest_begin_unix_ms: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub nonblocking_latest_end_unix_ms: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MqConsumerSnapshot {
    pub consumer_idx: String,
    pub status: MqMemberStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub external_client_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub owner_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub kvclient_sub_cluster: Option<String>,
    // MQ performance metrics are best-effort joined from Prometheus by (chan_id, consumer_idx).
    // Missing values are expected when:
    // - the emitter has not reported yet (30s window),
    // - Prometheus/TSDB is unavailable,
    // - the metric series is absent for this consumer.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prefetch_avg_get_handle_us: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prefetch_latest_get_handle_us: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prefetch_avg_handle_await_us: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prefetch_latest_handle_await_us: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prefetch_avg_etcd_put_us: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prefetch_latest_etcd_put_us: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prefetch_inflight_queue_size: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prefetch_target_inflight: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub get_one_avg_total_us: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub get_one_max_total_us: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub get_one_avg_wait_rx_us: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub get_one_max_wait_rx_us: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub get_one_avg_signal_us: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub get_one_max_signal_us: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub get_one_avg_post_us: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub get_one_max_post_us: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub get_one_window_calls: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub get_one_window_timeouts: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub get_one_window_bytes: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub nonblocking_latest_phase_calls: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub nonblocking_latest_phase_rps: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub nonblocking_latest_begin_unix_ms: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub nonblocking_latest_end_unix_ms: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MqChannelSnapshot {
    pub chan_id: i64,
    pub unique_keys: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub meta: Option<MqChanMetaSnapshot>,
    pub producers: Vec<MqProducerSnapshot>,
    pub consumers: Vec<MqConsumerSnapshot>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MqSnapshot {
    pub channels: Vec<MqChannelSnapshot>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterSnapshot {
    pub cluster_name: String,
    pub member_kind: MemberKind,
    pub etcd_endpoints: Vec<String>,
    pub prometheus_base_url: String,
    pub warnings: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub visible_member_roles: Option<Vec<MemberRole>>,
    pub master_id: Option<String>,
    pub master_network: Option<NetworkConfigSnapshot>,
    pub transfer_engine_edges: Vec<TransferEngineEdge>,
    pub kv_peer_network: Vec<KvPeerNetworkRateSnapshot>,
    #[serde(default)]
    pub rdma_netdev_network: Vec<RdmaNetdevRateSnapshot>,
    #[serde(default)]
    pub fs_mount_fs: Vec<FsMountFsSnapshot>,
    #[serde(default)]
    pub shm_files: Vec<ShmFileSnapshot>,
    #[serde(default)]
    pub fs_export_registry: Vec<FsExportRegistryRecordSnapshot>,
    #[serde(default)]
    pub fs_mount_registry: Vec<FsMountRegistryRecordSnapshot>,
    #[serde(default)]
    pub kv_topology_owner_external_max: Vec<KvTopologyOwnerExternalMaxSnapshot>,
    #[serde(default)]
    pub kv_topology_machine_external_max: Vec<KvTopologyMachineExternalMaxSnapshot>,
    #[serde(default)]
    pub kv_topology_sub_cluster_owner_owner_max: Vec<KvTopologySubClusterOwnerOwnerMaxSnapshot>,
    pub nodes: Vec<NodeSnapshot>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mq: Option<MqSnapshot>,
    pub total_put_rps: Option<f64>,
    pub total_get_rps: Option<f64>,
    pub total_put_bps: Option<f64>,
    pub total_get_bps: Option<f64>,
    pub total_put_latency_mean_us: Option<f64>,
    pub total_put_latency_p95_us: Option<f64>,
    pub total_put_latency_p99_us: Option<f64>,
    pub total_get_latency_mean_us: Option<f64>,
    pub total_get_latency_p95_us: Option<f64>,
    pub total_get_latency_p99_us: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClustersResponse {
    pub clusters: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct ClusterViewModel {
    pub header: ClusterViewHeader,
    pub totals: ClusterViewTotals,
    pub warnings: Vec<String>,
    pub owner_segment_usage: Option<Vec<OwnerSegmentUsageViewModel>>,
    pub nodes: Vec<NodeViewModel>,
}

#[derive(Debug, Clone)]
pub struct OwnerSegmentUsageViewModel {
    pub owner_id: String,
    pub total_used: String,
    pub total_cap: String,
    pub total_util: String,
    pub devices: Vec<OwnerSegmentDeviceUsageViewModel>,
}

#[derive(Debug, Clone)]
pub struct OwnerSegmentDeviceUsageViewModel {
    pub device: String,
    pub used: String,
    pub cap: String,
    pub util: String,
}

pub fn pills_for_cluster_totals(t: &ClusterViewTotals) -> Vec<UiPill> {
    if let Some(users) = t.fs_users {
        let mut pills: Vec<UiPill> = Vec::new();
        if let Some(agents) = t.fs_agents {
            pills.push(UiPill {
                label: "agents",
                value_text: agents.to_string(),
                status: UiPillStatus::Ok,
            });
        }
        if let Some(ctrls) = t.fs_controllers {
            pills.push(UiPill {
                label: "controllers",
                value_text: ctrls.to_string(),
                status: UiPillStatus::Ok,
            });
        }
        pills.push(UiPill {
            label: "users",
            value_text: users.to_string(),
            status: UiPillStatus::Ok,
        });
        return pills;
    }

    let (put_rps, put_rps_status) = fmt_opt_with_unit(t.total_put_rps, 3, "rps");
    let (get_rps, get_rps_status) = fmt_opt_with_unit(t.total_get_rps, 3, "rps");
    let (put_bps, put_bps_status) = fmt_bytes_auto(t.total_put_bps, true);
    let (get_bps, get_bps_status) = fmt_bytes_auto(t.total_get_bps, true);

    let (put_avg, put_avg_status) = fmt_ms_from_us(t.total_put_latency_mean_us);
    let (put_p95, put_p95_status) = fmt_ms_from_us(t.total_put_latency_p95_us);
    let (put_p99, put_p99_status) = fmt_ms_from_us(t.total_put_latency_p99_us);
    let (get_avg, get_avg_status) = fmt_ms_from_us(t.total_get_latency_mean_us);
    let (get_p95, get_p95_status) = fmt_ms_from_us(t.total_get_latency_p95_us);
    let (get_p99, get_p99_status) = fmt_ms_from_us(t.total_get_latency_p99_us);

    vec![
        UiPill {
            label: "put_rps",
            value_text: put_rps,
            status: put_rps_status,
        },
        UiPill {
            label: "get_rps",
            value_text: get_rps,
            status: get_rps_status,
        },
        UiPill {
            label: "put_B/s",
            value_text: put_bps,
            status: put_bps_status,
        },
        UiPill {
            label: "get_B/s",
            value_text: get_bps,
            status: get_bps_status,
        },
        UiPill {
            label: "put_avg",
            value_text: put_avg,
            status: put_avg_status,
        },
        UiPill {
            label: "put_p95",
            value_text: put_p95,
            status: put_p95_status,
        },
        UiPill {
            label: "put_p99",
            value_text: put_p99,
            status: put_p99_status,
        },
        UiPill {
            label: "get_avg",
            value_text: get_avg,
            status: get_avg_status,
        },
        UiPill {
            label: "get_p95",
            value_text: get_p95,
            status: get_p95_status,
        },
        UiPill {
            label: "get_p99",
            value_text: get_p99,
            status: get_p99_status,
        },
    ]
}

pub fn pills_for_node_resource(n: &NodeSnapshot) -> Vec<UiPill> {
    let (cpu, cpu_status) = fmt_opt_with_unit(n.node_cpu_usage_percent, 2, "%");
    let (mem_used, mem_used_status) = fmt_bytes_auto(n.node_memory_usage_bytes, false);
    let (mem_total, mem_total_status) = fmt_bytes_auto(n.node_memory_total_bytes, false);
    let (ct_mem_used, ct_mem_used_status) = fmt_bytes_auto(n.container_memory_usage_bytes, false);
    let (ct_mem_limit, ct_mem_limit_status) = fmt_bytes_auto(n.container_memory_limit_bytes, false);
    vec![
        UiPill {
            label: "cpu",
            value_text: cpu,
            status: cpu_status,
        },
        UiPill {
            label: "mem_used",
            value_text: mem_used,
            status: mem_used_status,
        },
        UiPill {
            label: "mem_total",
            value_text: mem_total,
            status: mem_total_status,
        },
        UiPill {
            label: "ct_mem_used",
            value_text: ct_mem_used,
            status: ct_mem_used_status,
        },
        UiPill {
            label: "ct_mem_limit",
            value_text: ct_mem_limit,
            status: ct_mem_limit_status,
        },
    ]
}

pub fn pills_for_process_resource(p: &ProcessViewModel) -> Vec<UiPill> {
    let (rss, rss_status) = fmt_bytes_auto(p.resident_memory_bytes_sum, false);
    let (tx, tx_status) = fmt_bytes_per_sec_from_mbps(p.net_tx_mbps_sum);
    let (rx, rx_status) = fmt_bytes_per_sec_from_mbps(p.net_rx_mbps_sum);
    vec![
        UiPill {
            label: "rss",
            value_text: rss,
            status: rss_status,
        },
        UiPill {
            label: "tx",
            value_text: tx,
            status: tx_status,
        },
        UiPill {
            label: "rx",
            value_text: rx,
            status: rx_status,
        },
    ]
}

pub fn pills_for_instance(m: &MemberSnapshot) -> Vec<UiPill> {
    let (put_rps, put_rps_status) = fmt_opt_with_unit(m.kv_put_rps, 3, "rps");
    let (get_rps, get_rps_status) = fmt_opt_with_unit(m.kv_get_rps, 3, "rps");
    let (put_bps, put_bps_status) = fmt_bytes_auto(m.kv_put_bps, true);
    let (get_bps, get_bps_status) = fmt_bytes_auto(m.kv_get_bps, true);

    let (put_avg, put_avg_status) = fmt_ms_from_us(m.kv_put_latency_mean_us);
    let (put_p95, put_p95_status) = fmt_ms_from_us(m.kv_put_latency_p95_us);
    let (put_p99, put_p99_status) = fmt_ms_from_us(m.kv_put_latency_p99_us);
    let (get_avg, get_avg_status) = fmt_ms_from_us(m.kv_get_latency_mean_us);
    let (get_p95, get_p95_status) = fmt_ms_from_us(m.kv_get_latency_p95_us);
    let (get_p99, get_p99_status) = fmt_ms_from_us(m.kv_get_latency_p99_us);

    let mut pills = vec![
        UiPill {
            label: "put_rps",
            value_text: put_rps,
            status: put_rps_status,
        },
        UiPill {
            label: "get_rps",
            value_text: get_rps,
            status: get_rps_status,
        },
        UiPill {
            label: "put_B/s",
            value_text: put_bps,
            status: put_bps_status,
        },
        UiPill {
            label: "get_B/s",
            value_text: get_bps,
            status: get_bps_status,
        },
        UiPill {
            label: "put_avg",
            value_text: put_avg,
            status: put_avg_status,
        },
        UiPill {
            label: "put_p95",
            value_text: put_p95,
            status: put_p95_status,
        },
        UiPill {
            label: "put_p99",
            value_text: put_p99,
            status: put_p99_status,
        },
        UiPill {
            label: "get_avg",
            value_text: get_avg,
            status: get_avg_status,
        },
        UiPill {
            label: "get_p95",
            value_text: get_p95,
            status: get_p95_status,
        },
        UiPill {
            label: "get_p99",
            value_text: get_p99,
            status: get_p99_status,
        },
    ];

    if m.role == MemberRole::OwnerClient {
        let (seg_used, seg_used_status) = fmt_bytes_auto(m.seg_used_bytes, false);
        let (seg_cap, seg_cap_status) = fmt_bytes_auto(m.seg_capacity_bytes, false);
        pills.push(UiPill {
            label: "seg_used",
            value_text: seg_used,
            status: seg_used_status,
        });
        pills.push(UiPill {
            label: "seg_cap",
            value_text: seg_cap,
            status: seg_cap_status,
        });
    }
    pills
}

#[derive(Debug, Clone)]
pub struct ClusterViewHeader {
    pub cluster_name: String,
    pub member_kind: MemberKind,
    pub etcd_endpoints: Vec<String>,
    pub prometheus_base_url: String,
    pub master_network_subnet_whitelist: Option<String>,
    pub visible_member_roles: Option<Vec<MemberRole>>,
}

#[derive(Debug, Clone)]
pub struct ClusterViewTotals {
    pub total_put_rps: Option<f64>,
    pub total_get_rps: Option<f64>,
    pub total_put_bps: Option<f64>,
    pub total_get_bps: Option<f64>,
    pub total_put_latency_mean_us: Option<f64>,
    pub total_put_latency_p95_us: Option<f64>,
    pub total_put_latency_p99_us: Option<f64>,
    pub total_get_latency_mean_us: Option<f64>,
    pub total_get_latency_p95_us: Option<f64>,
    pub total_get_latency_p99_us: Option<f64>,
    pub fs_agents: Option<i64>,
    pub fs_controllers: Option<i64>,
    pub fs_users: Option<i64>,
}

#[derive(Debug, Clone)]
pub struct NodeViewModel {
    pub node: NodeSnapshot,
    pub processes: Vec<ProcessViewModel>,
}

#[derive(Debug, Clone)]
pub struct ProcessViewModel {
    pub pid: Option<u32>,
    pub cmd: Option<String>,
    pub resident_memory_bytes_sum: Option<f64>,
    pub net_tx_mbps_sum: Option<f64>,
    pub net_rx_mbps_sum: Option<f64>,
    pub instances: Vec<MemberSnapshot>,
}

#[derive(Debug, Clone)]
pub struct MemberTableRowView {
    pub node_key: String,
    pub member_id: String,
    pub logs_href: String,
    pub role: MemberRole,
    pub role_text: String,
    pub role_rank: i32,
    pub pid: Option<u32>,
    pub cmd: Option<String>,
    pub pid_text: String,
    pub cmd_text: String,
    pub node_start_time: i64,
    pub hostname: Option<String>,
    pub accessible_ip: Option<String>,
    pub shared_mem_dir: Option<String>,
    pub p2p_listen_port: Option<u16>,
    pub rdma_ports: Vec<MemberRdmaPortSnapshot>,
    pub rdma_transfer_engine: Option<MemberRdmaTransferEngineRuntime>,
    pub hostname_text: String,
    pub accessible_ip_text: String,
    pub shared_mem_dir_text: String,
    pub p2p_listen_port_text: String,
    pub rdma_text: String,
    pub search_text: String,
    pub cpu_text: String,
    pub cpu_sort: String,
    pub mem_used_text: String,
    pub mem_used_sort: String,
    pub rss_text: String,
    pub rss_sort: String,
    pub tx_text: String,
    pub tx_sort: String,
    pub rx_text: String,
    pub rx_sort: String,
    pub put_rps_text: String,
    pub put_rps_sort: String,
    pub get_rps_text: String,
    pub get_rps_sort: String,
    pub put_avg_text: String,
    pub put_avg_sort: String,
    pub get_avg_text: String,
    pub get_avg_sort: String,
    pub seg_used_text: String,
    pub seg_used_sort: String,
    pub seg_cap_text: String,
    pub seg_cap_sort: String,
}

#[derive(Debug, Clone)]
pub struct OwnerSegmentOwnerRowView {
    pub owner_id: String,
    pub total_used: String,
    pub total_cap: String,
    pub total_util: String,
}

#[derive(Debug, Clone)]
pub struct OwnerSegmentDeviceRowView {
    pub owner_id: String,
    pub device: String,
    pub used: String,
    pub cap: String,
    pub util: String,
}

#[derive(Debug, Clone)]
pub struct OwnerSegmentTablesView {
    pub owner_rows: Vec<OwnerSegmentOwnerRowView>,
    pub device_rows: Vec<OwnerSegmentDeviceRowView>,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum ProcessKey {
    Pid(u32),
    NoPid(String),
}

pub fn url_encode_component(s: &str) -> String {
    url::form_urlencoded::byte_serialize(s.as_bytes()).collect::<String>()
}

fn sum_opt_f64(values: impl Iterator<Item = Option<f64>>) -> Option<f64> {
    let mut sum = 0f64;
    let mut seen = false;
    for v in values {
        if let Some(v) = v {
            sum += v;
            seen = true;
        }
    }
    if seen { Some(sum) } else { None }
}

pub fn build_cluster_view_model(snapshot: &ClusterSnapshot) -> ClusterViewModel {
    use std::collections::BTreeMap;

    fn is_role_visible(visible_roles: Option<&Vec<MemberRole>>, role: MemberRole) -> bool {
        // Why `None => show all`:
        // - Cause: Role filter is only driven by the UI/query (no startup config).
        // - Behavior: When the UI does not specify roles, keep the original "show all" output.
        // - Effect: Avoid accidental data loss across upgrades and links.
        visible_roles.map(|v| v.contains(&role)).unwrap_or(true)
    }

    fn fmt_util_percent(used: Option<f64>, cap: Option<f64>) -> String {
        let (Some(used), Some(cap)) = (used, cap) else {
            return "N/A".to_string();
        };
        if cap <= 0.0 {
            return "N/A".to_string();
        }
        format!("{:.1}%", used * 100.0 / cap)
    }

    fn sum_opt_f64_complete(values: impl Iterator<Item = Option<f64>>) -> Option<f64> {
        let mut sum = 0f64;
        let mut seen = false;
        for v in values {
            let Some(v) = v else {
                return None;
            };
            sum += v;
            seen = true;
        }
        if seen { Some(sum) } else { None }
    }

    let mut nodes: Vec<NodeViewModel> = Vec::with_capacity(snapshot.nodes.len());
    let visible_roles = snapshot.visible_member_roles.as_ref();
    for node in &snapshot.nodes {
        let filtered_members: Vec<MemberSnapshot> = node
            .members
            .iter()
            .filter(|m| is_role_visible(visible_roles, m.role))
            .cloned()
            .collect();
        if filtered_members.is_empty() {
            continue;
        }

        let mut node_filtered = node.clone();
        node_filtered.members = filtered_members;
        node_filtered.node_cpu_usage_percent = node_filtered
            .members
            .iter()
            .filter_map(|m| m.node_cpu_usage_percent)
            .fold(None, |acc, v| Some(acc.map(|a| a.max(v)).unwrap_or(v)));
        node_filtered.node_memory_usage_bytes = node_filtered
            .members
            .iter()
            .filter_map(|m| m.node_memory_usage_bytes)
            .fold(None, |acc, v| Some(acc.map(|a| a.max(v)).unwrap_or(v)));
        node_filtered.node_memory_total_bytes = node_filtered
            .members
            .iter()
            .filter_map(|m| m.node_memory_total_bytes)
            .fold(None, |acc, v| Some(acc.map(|a| a.max(v)).unwrap_or(v)));
        node_filtered.container_memory_usage_bytes = node_filtered
            .members
            .iter()
            .filter_map(|m| m.container_memory_usage_bytes)
            .fold(None, |acc, v| Some(acc.map(|a| a.max(v)).unwrap_or(v)));
        node_filtered.container_memory_limit_bytes = node_filtered
            .members
            .iter()
            .filter_map(|m| m.container_memory_limit_bytes)
            .fold(None, |acc, v| Some(acc.map(|a| a.max(v)).unwrap_or(v)));

        let mut process_map: BTreeMap<ProcessKey, Vec<MemberSnapshot>> = BTreeMap::new();
        for m in &node_filtered.members {
            let key = match m.pid {
                Some(pid) => ProcessKey::Pid(pid),
                None => ProcessKey::NoPid(m.member_id.clone()),
            };
            process_map.entry(key).or_default().push(m.clone());
        }

        let mut processes: Vec<ProcessViewModel> = Vec::with_capacity(process_map.len());
        for (_k, mut instances) in process_map {
            instances.sort_by(|a, b| a.member_id.cmp(&b.member_id));
            let pid = instances.iter().find_map(|m| m.pid);
            let cmd = instances.iter().find_map(|m| m.cmd.clone());
            let resident_memory_bytes_sum =
                sum_opt_f64(instances.iter().map(|m| m.process_resident_memory_bytes));
            let net_tx_mbps_sum = sum_opt_f64(instances.iter().map(|m| m.process_net_tx_mbps));
            let net_rx_mbps_sum = sum_opt_f64(instances.iter().map(|m| m.process_net_rx_mbps));
            processes.push(ProcessViewModel {
                pid,
                cmd,
                resident_memory_bytes_sum,
                net_tx_mbps_sum,
                net_rx_mbps_sum,
                instances,
            });
        }

        processes.sort_by(|a, b| match (a.pid, b.pid) {
            (Some(a), Some(b)) => a.cmp(&b),
            (Some(_), None) => std::cmp::Ordering::Less,
            (None, Some(_)) => std::cmp::Ordering::Greater,
            (None, None) => std::cmp::Ordering::Equal,
        });

        nodes.push(NodeViewModel {
            node: node_filtered,
            processes,
        });
    }

    let owner_segment_usage = if snapshot.member_kind == MemberKind::Fs {
        None
    } else if is_role_visible(visible_roles, MemberRole::OwnerClient) {
        let mut owners: Vec<OwnerSegmentUsageViewModel> = Vec::new();
        for n in &snapshot.nodes {
            let Some(owner_id) = n
                .members
                .iter()
                .find(|m| m.role == MemberRole::OwnerClient)
                .map(|m| m.member_id.clone())
            else {
                continue;
            };

            let total_used_bytes =
                sum_opt_f64_complete(n.segment_devices.iter().map(|d| d.seg_used_bytes));
            let total_cap_bytes =
                sum_opt_f64_complete(n.segment_devices.iter().map(|d| d.seg_capacity_bytes));
            let (total_used, _total_used_status) = fmt_bytes_auto(total_used_bytes, false);
            let (total_cap, _total_cap_status) = fmt_bytes_auto(total_cap_bytes, false);
            let total_util = fmt_util_percent(total_used_bytes, total_cap_bytes);

            let mut devices: Vec<OwnerSegmentDeviceUsageViewModel> =
                Vec::with_capacity(n.segment_devices.len());
            for d in &n.segment_devices {
                let (used, _used_status) = fmt_bytes_auto(d.seg_used_bytes, false);
                let (cap, _cap_status) = fmt_bytes_auto(d.seg_capacity_bytes, false);
                devices.push(OwnerSegmentDeviceUsageViewModel {
                    device: d.device.clone(),
                    used,
                    cap,
                    util: fmt_util_percent(d.seg_used_bytes, d.seg_capacity_bytes),
                });
            }
            devices.sort_by(|a, b| a.device.cmp(&b.device));

            owners.push(OwnerSegmentUsageViewModel {
                owner_id,
                total_used,
                total_cap,
                total_util,
                devices,
            });
        }
        owners.sort_by(|a, b| a.owner_id.cmp(&b.owner_id));
        Some(owners)
    } else {
        None
    };

    let (fs_agents, fs_controllers, fs_users) = if snapshot.member_kind == MemberKind::Fs {
        let mut agents = 0i64;
        let mut ctrls = 0i64;
        let mut users = 0i64;
        for n in &snapshot.nodes {
            users += n.members.len() as i64;
            for m in &n.members {
                match classify_fluxon_fs_component(&m.member_id, m.cmd.as_deref()) {
                    Some(FsComponentKind::Agent) => agents += 1,
                    Some(FsComponentKind::Controller) => ctrls += 1,
                    None => {}
                }
            }
        }
        (Some(agents), Some(ctrls), Some(users))
    } else {
        (None, None, None)
    };

    ClusterViewModel {
        header: ClusterViewHeader {
            cluster_name: snapshot.cluster_name.clone(),
            member_kind: snapshot.member_kind,
            etcd_endpoints: snapshot.etcd_endpoints.clone(),
            prometheus_base_url: snapshot.prometheus_base_url.clone(),
            master_network_subnet_whitelist: snapshot.master_id.as_ref().map(|_| {
                match snapshot.master_network.as_ref() {
                    None => "<unset>".to_string(),
                    Some(cfg) => {
                        if cfg.subnet_whitelist.is_empty() {
                            "[]".to_string()
                        } else {
                            cfg.subnet_whitelist.join(",")
                        }
                    }
                }
            }),
            visible_member_roles: snapshot.visible_member_roles.clone(),
        },
        totals: ClusterViewTotals {
            total_put_rps: snapshot.total_put_rps,
            total_get_rps: snapshot.total_get_rps,
            total_put_bps: snapshot.total_put_bps,
            total_get_bps: snapshot.total_get_bps,
            total_put_latency_mean_us: snapshot.total_put_latency_mean_us,
            total_put_latency_p95_us: snapshot.total_put_latency_p95_us,
            total_put_latency_p99_us: snapshot.total_put_latency_p99_us,
            total_get_latency_mean_us: snapshot.total_get_latency_mean_us,
            total_get_latency_p95_us: snapshot.total_get_latency_p95_us,
            total_get_latency_p99_us: snapshot.total_get_latency_p99_us,
            fs_agents,
            fs_controllers,
            fs_users,
        },
        warnings: snapshot.warnings.clone(),
        owner_segment_usage,
        nodes,
    }
}

pub fn build_member_table_rows(snapshot: &ClusterSnapshot) -> Vec<MemberTableRowView> {
    fn is_role_visible(visible_roles: Option<&Vec<MemberRole>>, role: MemberRole) -> bool {
        visible_roles.map(|v| v.contains(&role)).unwrap_or(true)
    }

    fn role_rank(role: MemberRole) -> i32 {
        match role {
            MemberRole::Master => 0,
            MemberRole::OwnerClient => 1,
            MemberRole::ExternalClient => 2,
            MemberRole::SideTransferWorker => 3,
            MemberRole::Unknown => 4,
        }
    }

    fn sort_value_opt(v: Option<f64>) -> String {
        v.map(|v| v.to_string())
            .unwrap_or_else(|| "NaN".to_string())
    }

    let visible_roles = snapshot.visible_member_roles.as_ref();
    let mut rows: Vec<MemberTableRowView> = Vec::new();
    for node in &snapshot.nodes {
        for m in &node.members {
            if !is_role_visible(visible_roles, m.role) {
                continue;
            }

            let (role_text, role_rank_value) = if snapshot.member_kind == MemberKind::Fs {
                match classify_fluxon_fs_component(&m.member_id, m.cmd.as_deref()) {
                    Some(kind) => {
                        let rank = match kind {
                            FsComponentKind::Controller => 0,
                            FsComponentKind::Agent => 1,
                        };
                        (kind.as_str().to_string(), rank)
                    }
                    None => (m.role.as_str().to_string(), 2),
                }
            } else {
                (m.role.as_str().to_string(), role_rank(m.role))
            };

            let (cpu_text, _cpu_status) = fmt_opt_with_unit(m.node_cpu_usage_percent, 2, "%");
            let (mem_used_text, _mem_used_status) =
                fmt_bytes_auto(m.node_memory_usage_bytes, false);
            let (rss_text, _rss_status) = fmt_bytes_auto(m.process_resident_memory_bytes, false);
            let (tx_text, _tx_status) = fmt_bytes_per_sec_from_mbps(m.process_net_tx_mbps);
            let (rx_text, _rx_status) = fmt_bytes_per_sec_from_mbps(m.process_net_rx_mbps);
            let (put_rps_text, _put_rps_status) = fmt_opt_with_unit(m.kv_put_rps, 3, "rps");
            let (get_rps_text, _get_rps_status) = fmt_opt_with_unit(m.kv_get_rps, 3, "rps");
            let (put_avg_text, _put_avg_status) = fmt_ms_from_us(m.kv_put_latency_mean_us);
            let (get_avg_text, _get_avg_status) = fmt_ms_from_us(m.kv_get_latency_mean_us);
            let (seg_used_text, _seg_used_status) = fmt_bytes_auto(m.seg_used_bytes, false);
            let (seg_cap_text, _seg_cap_status) = fmt_bytes_auto(m.seg_capacity_bytes, false);
            let pid_text = m
                .pid
                .map(|v| v.to_string())
                .unwrap_or_else(|| "".to_string());
            let cmd_text = m.cmd.clone().unwrap_or_else(|| "".to_string());
            let hostname_text = m.hostname.clone().unwrap_or_else(|| "".to_string());
            let accessible_ip_text = m.accessible_ip.clone().unwrap_or_else(|| "".to_string());
            let shared_mem_dir_text = m.shared_mem_dir.clone().unwrap_or_else(|| "".to_string());
            let p2p_listen_port_text = m
                .p2p_listen_port
                .map(|v| v.to_string())
                .unwrap_or_else(|| "".to_string());
            let rdma_text = {
                let mut parts: Vec<String> = Vec::new();
                if let Some(transfer_engine) = m.rdma_transfer_engine.as_ref() {
                    if transfer_engine.consecutive_start_failures == 0 {
                        parts.push(transfer_engine.state.as_str().to_string());
                    } else {
                        parts.push(format!(
                            "{} fail={}",
                            transfer_engine.state.as_str(),
                            transfer_engine.consecutive_start_failures
                        ));
                    }
                }
                if !m.rdma_runtime_reported {
                    parts.push("not_reported".to_string());
                } else if m.rdma_probe_error.is_some() {
                    parts.push("probe_error".to_string());
                } else if m.rdma_devices.is_empty() {
                    parts.push("no_rdma_nic".to_string());
                } else {
                    parts.push(
                        m.rdma_devices
                            .iter()
                            .map(|device| {
                                format!(
                                    "{}:{}:{}:{}:{}",
                                    device.device,
                                    if device.detected { "det" } else { "miss" },
                                    if device.desired_enabled { "on" } else { "off" },
                                    if device.effective_enabled {
                                        "eff"
                                    } else {
                                        "noeff"
                                    },
                                    device.usable_ports
                                )
                            })
                            .collect::<Vec<_>>()
                            .join(" "),
                    );
                }
                parts.join(" ").trim().to_string()
            };
            let search_text = [
                node.node_key.as_str(),
                m.member_id.as_str(),
                role_text.as_str(),
                cmd_text.as_str(),
                hostname_text.as_str(),
                accessible_ip_text.as_str(),
                shared_mem_dir_text.as_str(),
                rdma_text.as_str(),
            ]
            .join(" ");

            let logs_href = format!(
                "/logs?cluster_name={}&member_kind={}&role={}&member_id={}",
                url_encode_component(&snapshot.cluster_name),
                snapshot.member_kind.as_query_str(),
                m.role.as_str(),
                url_encode_component(&m.member_id),
            );

            rows.push(MemberTableRowView {
                node_key: node.node_key.clone(),
                member_id: m.member_id.clone(),
                logs_href,
                role: m.role,
                role_text,
                role_rank: role_rank_value,
                pid: m.pid,
                cmd: m.cmd.clone(),
                pid_text,
                cmd_text,
                node_start_time: m.node_start_time,
                hostname: m.hostname.clone(),
                accessible_ip: m.accessible_ip.clone(),
                shared_mem_dir: m.shared_mem_dir.clone(),
                p2p_listen_port: m.p2p_listen_port,
                rdma_ports: m.rdma_ports.clone(),
                rdma_transfer_engine: m.rdma_transfer_engine.clone(),
                hostname_text,
                accessible_ip_text,
                shared_mem_dir_text,
                p2p_listen_port_text,
                rdma_text,
                search_text,
                cpu_text,
                cpu_sort: sort_value_opt(m.node_cpu_usage_percent),
                mem_used_text,
                mem_used_sort: sort_value_opt(m.node_memory_usage_bytes),
                rss_text,
                rss_sort: sort_value_opt(m.process_resident_memory_bytes),
                tx_text,
                tx_sort: sort_value_opt(m.process_net_tx_mbps),
                rx_text,
                rx_sort: sort_value_opt(m.process_net_rx_mbps),
                put_rps_text,
                put_rps_sort: sort_value_opt(m.kv_put_rps),
                get_rps_text,
                get_rps_sort: sort_value_opt(m.kv_get_rps),
                put_avg_text,
                put_avg_sort: sort_value_opt(m.kv_put_latency_mean_us),
                get_avg_text,
                get_avg_sort: sort_value_opt(m.kv_get_latency_mean_us),
                seg_used_text,
                seg_used_sort: sort_value_opt(m.seg_used_bytes),
                seg_cap_text,
                seg_cap_sort: sort_value_opt(m.seg_capacity_bytes),
            });
        }
    }

    rows.sort_by(|a, b| {
        let ak = (&a.node_key, a.role_rank, &a.member_id);
        let bk = (&b.node_key, b.role_rank, &b.member_id);
        ak.cmp(&bk)
    });

    rows
}

pub fn build_owner_segment_tables(snapshot: &ClusterSnapshot) -> OwnerSegmentTablesView {
    fn fmt_util_percent(used: Option<f64>, cap: Option<f64>) -> String {
        let (Some(used), Some(cap)) = (used, cap) else {
            return "N/A".to_string();
        };
        if cap <= 0.0 {
            return "N/A".to_string();
        }
        format!("{:.1}%", used * 100.0 / cap)
    }

    fn sum_opt_f64_complete(values: impl Iterator<Item = Option<f64>>) -> Option<f64> {
        let mut sum = 0f64;
        let mut seen = false;
        for v in values {
            let Some(v) = v else {
                return None;
            };
            sum += v;
            seen = true;
        }
        if seen { Some(sum) } else { None }
    }

    let mut owner_rows: Vec<OwnerSegmentOwnerRowView> = Vec::new();
    let mut device_rows: Vec<OwnerSegmentDeviceRowView> = Vec::new();

    for n in &snapshot.nodes {
        let Some(owner_id) = n
            .members
            .iter()
            .find(|m| m.role == MemberRole::OwnerClient)
            .map(|m| m.member_id.clone())
        else {
            continue;
        };

        let total_used_bytes =
            sum_opt_f64_complete(n.segment_devices.iter().map(|d| d.seg_used_bytes));
        let total_cap_bytes =
            sum_opt_f64_complete(n.segment_devices.iter().map(|d| d.seg_capacity_bytes));
        let (total_used, _total_used_status) = fmt_bytes_auto(total_used_bytes, false);
        let (total_cap, _total_cap_status) = fmt_bytes_auto(total_cap_bytes, false);
        let total_util = fmt_util_percent(total_used_bytes, total_cap_bytes);

        owner_rows.push(OwnerSegmentOwnerRowView {
            owner_id: owner_id.clone(),
            total_used,
            total_cap,
            total_util,
        });

        for d in &n.segment_devices {
            let (used, _used_status) = fmt_bytes_auto(d.seg_used_bytes, false);
            let (cap, _cap_status) = fmt_bytes_auto(d.seg_capacity_bytes, false);
            device_rows.push(OwnerSegmentDeviceRowView {
                owner_id: owner_id.clone(),
                device: d.device.clone(),
                used,
                cap,
                util: fmt_util_percent(d.seg_used_bytes, d.seg_capacity_bytes),
            });
        }
    }

    owner_rows.sort_by(|a, b| a.owner_id.cmp(&b.owner_id));
    device_rows.sort_by(|a, b| match a.owner_id.cmp(&b.owner_id) {
        std::cmp::Ordering::Equal => a.device.cmp(&b.device),
        o => o,
    });

    OwnerSegmentTablesView {
        owner_rows,
        device_rows,
    }
}
