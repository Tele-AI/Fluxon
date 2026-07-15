pub use fluxon_commu::cluster_manager::*;
pub use fluxon_commu::{
    META_KEY_ACCESSIBLE_IP, META_KEY_CMD, META_KEY_HOSTNAME, META_KEY_LOCAL_IPC_ROOT, META_KEY_PID,
    META_KEY_PRODUCT_UUID, META_KEY_RDMA_CONTROL, META_KEY_RDMA_RUNTIME,
    META_KEY_SHARED_STORAGE_NODE_ID, META_KEY_SHARED_STORAGE_NODE_START_TIME,
};

pub(crate) const META_KEY_KV_SSD_STORAGE: &str = "kv_ssd_storage";

pub(crate) fn member_has_kv_ssd_storage(member: &ClusterMember) -> bool {
    member
        .metadata
        .get(META_KEY_KV_SSD_STORAGE)
        .is_some_and(|value| value == "true")
}

pub mod app_logic_ext;

#[cfg(test)]
mod cluster_manager_test;
