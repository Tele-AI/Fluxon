use bitcode::{Decode, Encode};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Encode, Decode)]
#[serde(rename_all = "snake_case")]
pub enum RdmaLinkLayer {
    Infiniband,
    Ethernet,
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Encode, Decode)]
#[serde(rename_all = "snake_case")]
pub enum RdmaPortState {
    Nop,
    Down,
    Init,
    Armed,
    Active,
    ActiveDefer,
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Encode, Decode)]
#[serde(rename_all = "snake_case")]
pub enum RdmaPhysState {
    NoStateChange,
    Sleep,
    Polling,
    Disabled,
    PortConfigurationTraining,
    LinkUp,
    LinkErrorRecovery,
    PhyTest,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Encode, Decode)]
pub struct RdmaPortSnapshot {
    pub device: String,
    pub port: u8,
    pub port_key: String,
    pub netdev: Option<String>,
    pub pci_bdf: Option<String>,
    pub pcie_max_bandwidth_mbps: Option<u64>,
    pub numa_node: Option<i32>,
    pub speed_gbps: Option<u32>,
    pub driver: Option<String>,
    pub firmware: Option<String>,
    pub link_layer: RdmaLinkLayer,
    pub port_state: RdmaPortState,
    pub phys_state: RdmaPhysState,
    pub active_mtu_bytes: u32,
    pub lid: u16,
    pub gid_count: u32,
    pub open_ok: bool,
    pub alloc_pd_ok: bool,
    pub usable: bool,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RdmaRuntimeSnapshot {
    pub pid: u32,
    pub ppid: u32,
    pub exe: Option<String>,
    pub cwd: Option<String>,
    pub root: Option<String>,
    pub cmdline: Vec<String>,
    pub namespace_links: Vec<String>,
    pub env_fluxon_pyo3_libs_dir: Option<String>,
    pub env_rdmav_drivers: Option<String>,
    pub env_ibv_drivers: Option<String>,
    pub env_ld_library_path: Option<String>,
    pub relevant_loaded_libraries: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RdmaProbeSnapshot {
    pub ports: Vec<RdmaPortSnapshot>,
    pub probe_error: Option<String>,
    pub verbs_device_count: usize,
    pub ibv_get_device_list_device_count_raw: i32,
    pub ibv_get_device_list_returned_null: bool,
    pub ibv_get_device_list_errno: Option<i32>,
    pub verbs_device_names: Vec<String>,
    pub sysfs_infiniband_entries: Vec<String>,
    pub dev_infiniband_entries: Vec<String>,
    pub env_rdmav_drivers: Option<String>,
    pub env_ibv_drivers: Option<String>,
    pub env_ld_library_path: Option<String>,
    pub runtime_snapshot: RdmaRuntimeSnapshot,
}
