pub mod cli_renderer;
pub mod config;
pub mod build_info {
    use std::sync::OnceLock;

    pub const GIT_COMMIT_ID: &str = fluxon_util::build_info::GIT_COMMIT_ID;
    pub const SOURCE_SHA256: &str = fluxon_util::build_info::SOURCE_SHA256;

    pub fn print_startup_info() {
        eprintln!("{} {}", env!("CARGO_PKG_NAME"), env!("CARGO_PKG_VERSION"));
        eprintln!("commit: {}", GIT_COMMIT_ID);
        eprintln!("source-sha256: {}", SOURCE_SHA256);
    }

    pub fn long_version() -> &'static str {
        static LONG_VERSION: OnceLock<String> = OnceLock::new();
        LONG_VERSION
            .get_or_init(|| fluxon_util::build_info::format_long_version(env!("CARGO_PKG_VERSION")))
            .as_str()
    }
}
pub mod model;
pub mod prom;
pub mod server;
pub mod web_renderer;

// English note: keep this service name stable because it is part of the public URL surface
// (/r/<service_name>/<cluster_name>/...) and etcd-published proxy descriptor key format.
pub const OPS_PANEL_SERVICE_NAME: &str = "ops";

use crate::config::{AVAILABLE_MEMBER_KINDS, MemberKind, MonitorConfig};
use crate::model::{
    ClusterMember, ClusterSnapshot, ClustersResponse, MemberRdmaDeviceSnapshot,
    MemberRdmaPortSnapshot, MemberSnapshot, NodeSnapshot, RdmaNetdevRateSnapshot,
    TransferEngineEdge,
};
use crate::model::{
    MqChanMetaSnapshot, MqChannelSnapshot, MqConsumerSnapshot, MqMemberStatus, MqProducerSnapshot,
    MqSnapshot,
};
use crate::prom::{PromClient, role_from_member_metadata};
use anyhow::Context;
use etcd_client::Client as EtcdClient;
use fluxon_commu::{
    EtcdPrefixScanAction, META_KEY_CMD, META_KEY_HOSTNAME, META_KEY_PID, META_KEY_PRODUCT_UUID,
    META_KEY_RDMA_CONTROL, META_KEY_RDMA_RUNTIME, MemberRdmaControl, MemberRdmaRuntime,
    RdmaLinkLayer, RdmaPhysState, RdmaPortSnapshot, RdmaPortState, cluster_member_base_prefix,
    cluster_member_ext_prefix, cluster_owner_rdma_control_prefix, scan_etcd_prefix_paginated,
};
use serde::Deserialize;
use std::collections::{BTreeMap, HashMap};
use std::io::{IsTerminal, Read, Write};

const MQ_META_KEY_PREFIX: &str = "/channels/meta/";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
enum MqChanRoleWire {
    Producer,
    Consumer,
}

#[derive(Debug, Clone, Deserialize)]
struct MqChanMetaWire {
    capacity: i64,
    ttl_seconds: i64,
    #[serde(default)]
    payload_lease_id: Option<i64>,
}

#[derive(Debug, Clone, Deserialize)]
struct MqConsumerMemberMetaWire {
    role: MqChanRoleWire,
    #[serde(default)]
    kvclient_sub_cluster: Option<String>,
    #[serde(default)]
    external_client_id: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct MqProducerMemberMetaWire {
    #[serde(default)]
    external_client_id: Option<String>,
}

fn parse_mq_chan_id_from_meta_key(key: &str) -> Option<i64> {
    let rest = key.strip_prefix(MQ_META_KEY_PREFIX)?;
    if rest.is_empty() || rest.contains('/') {
        return None;
    }
    rest.parse::<i64>().ok()
}

fn parse_mq_producer_idx_from_member_key(key: &str, chan_id: i64) -> Option<String> {
    let prefix = format!("/channels/{}/producer/", chan_id);
    let rest = key.strip_prefix(&prefix)?;
    let last = rest.split('/').next()?;
    let suffix = last.strip_prefix("producer_")?;
    if suffix.is_empty() {
        return None;
    }
    Some(suffix.to_string())
}

fn parse_mq_consumer_idx_from_member_key(key: &str, chan_id: i64) -> Option<String> {
    let prefix = format!("/channels/{}/consumer/", chan_id);
    let rest = key.strip_prefix(&prefix)?;
    let last = rest.split('/').next()?;
    let suffix = last.strip_prefix("consumer_")?;
    if suffix.is_empty() {
        return None;
    }
    Some(suffix.to_string())
}

fn parse_mq_offset_value_i64(warnings: &mut Vec<String>, ctx: &str, value: &[u8]) -> Option<i64> {
    let s = match std::str::from_utf8(value) {
        Ok(v) => v,
        Err(e) => {
            warnings.push(format!("invalid utf-8 mq offset value ({ctx}): err={e}"));
            return None;
        }
    };
    match s.parse::<i64>() {
        Ok(v) => Some(v),
        Err(e) => {
            warnings.push(format!(
                "invalid mq offset value ({ctx}): value={s:?} err={e}"
            ));
            None
        }
    }
}

fn normalize_external_client_id(
    warnings: &mut Vec<String>,
    ctx: &str,
    v: Option<String>,
) -> Option<String> {
    let Some(s) = v else {
        return None;
    };
    if s.trim().is_empty() {
        warnings.push(format!("empty external_client_id ({ctx})"));
        return None;
    }
    if s != s.trim() {
        warnings.push(format!(
            "external_client_id has leading/trailing whitespace ({ctx}): {s:?}"
        ));
        return None;
    }
    Some(s)
}

fn transfer_link_p2p_prefix(cluster_name: &str) -> String {
    format!("/{}/transfer_link/p2p/", cluster_name)
}

fn transfer_link_te_prefix(cluster_name: &str) -> String {
    format!("/{}/transfer_link/te/", cluster_name)
}

async fn scan_transfer_link_part_for_cluster(
    etcd: &mut EtcdClient,
    prefix: &str,
) -> anyhow::Result<Vec<(String, String, String)>> {
    let mut edges: Vec<(String, String, String)> = Vec::new();
    scan_etcd_prefix_paginated(etcd, prefix, |key, value| {
        let key = String::from_utf8_lossy(key);
        if !key.starts_with(prefix) {
            return Ok::<EtcdPrefixScanAction, anyhow::Error>(EtcdPrefixScanAction::Continue);
        }
        let rest = &key[prefix.len()..];
        let Some((from, to)) = rest.split_once('/') else {
            return Ok::<EtcdPrefixScanAction, anyhow::Error>(EtcdPrefixScanAction::Continue);
        };
        if from.is_empty() || to.is_empty() {
            return Ok::<EtcdPrefixScanAction, anyhow::Error>(EtcdPrefixScanAction::Continue);
        }
        let value = String::from_utf8_lossy(value);
        let value = value.trim();
        if value.is_empty() {
            return Ok::<EtcdPrefixScanAction, anyhow::Error>(EtcdPrefixScanAction::Continue);
        }
        edges.push((from.to_string(), to.to_string(), value.to_string()));
        Ok::<EtcdPrefixScanAction, anyhow::Error>(EtcdPrefixScanAction::Continue)
    })
    .await
    .map_err(anyhow::Error::from)?;
    Ok(edges)
}

fn merge_transfer_link_parts(
    p2p_parts: Vec<(String, String, String)>,
    te_parts: Vec<(String, String, String)>,
) -> Vec<TransferEngineEdge> {
    let mut by_edge: std::collections::HashMap<(String, String), (Option<String>, Option<String>)> =
        std::collections::HashMap::new();
    for (from, to, value) in p2p_parts {
        if from == to {
            continue;
        }
        by_edge.entry((from, to)).or_insert((None, None)).0 = Some(value);
    }
    for (from, to, value) in te_parts {
        if from == to {
            continue;
        }
        by_edge.entry((from, to)).or_insert((None, None)).1 = Some(value);
    }

    let mut edges: Vec<TransferEngineEdge> = Vec::with_capacity(by_edge.len());
    for ((from, to), (p2p, te)) in by_edge {
        let route = match (p2p, te) {
            (Some(p2p), Some(te)) => format!("{}+{}", p2p, te),
            (Some(p2p), None) => p2p,
            (None, Some(te)) => te,
            (None, None) => continue,
        };
        edges.push(TransferEngineEdge { from, to, route });
    }
    edges.sort_by(|a, b| match a.from.cmp(&b.from) {
        std::cmp::Ordering::Equal => a.to.cmp(&b.to),
        order => order,
    });
    edges
}

pub async fn load_transfer_engine_edges_for_cluster(
    etcd_endpoints: &[String],
    cluster_name: &str,
) -> anyhow::Result<Vec<TransferEngineEdge>> {
    let mut etcd = EtcdClient::connect(etcd_endpoints.to_vec(), None)
        .await
        .with_context(|| "connect etcd (transfer_link scan)".to_string())?;
    let p2p_prefix = transfer_link_p2p_prefix(cluster_name);
    let te_prefix = transfer_link_te_prefix(cluster_name);
    let p2p_parts = scan_transfer_link_part_for_cluster(&mut etcd, &p2p_prefix).await?;
    let te_parts = scan_transfer_link_part_for_cluster(&mut etcd, &te_prefix).await?;
    Ok(merge_transfer_link_parts(p2p_parts, te_parts))
}

#[cfg(test)]
mod mq_parse_tests {
    use super::{
        merge_transfer_link_parts, parse_mq_chan_id_from_meta_key,
        parse_mq_consumer_idx_from_member_key, parse_mq_producer_idx_from_member_key,
    };

    #[test]
    fn test_parse_mq_chan_id_from_meta_key() {
        assert_eq!(parse_mq_chan_id_from_meta_key("/channels/meta/1"), Some(1));
        assert_eq!(
            parse_mq_chan_id_from_meta_key("/channels/meta/999"),
            Some(999)
        );
        assert_eq!(parse_mq_chan_id_from_meta_key("/channels/meta/"), None);
        assert_eq!(parse_mq_chan_id_from_meta_key("/channels/meta/1/2"), None);
        assert_eq!(parse_mq_chan_id_from_meta_key("/other/meta/1"), None);
    }

    #[test]
    fn test_parse_mq_producer_idx_from_member_key() {
        assert_eq!(
            parse_mq_producer_idx_from_member_key("/channels/7/producer/producer_42", 7),
            Some("42".to_string())
        );
        assert_eq!(
            parse_mq_producer_idx_from_member_key("/channels/7/producer/producer_", 7),
            None
        );
        assert_eq!(
            parse_mq_producer_idx_from_member_key("/channels/8/producer/producer_42", 7),
            None
        );
    }

    #[test]
    fn test_parse_mq_consumer_idx_from_member_key() {
        assert_eq!(
            parse_mq_consumer_idx_from_member_key("/channels/7/consumer/consumer_1", 7),
            Some("1".to_string())
        );
        assert_eq!(
            parse_mq_consumer_idx_from_member_key("/channels/7/consumer/consumer_", 7),
            None
        );
        assert_eq!(
            parse_mq_consumer_idx_from_member_key("/channels/8/consumer/consumer_1", 7),
            None
        );
    }

    #[test]
    fn test_merge_transfer_link_parts_skips_self_edges() {
        let edges = merge_transfer_link_parts(
            vec![
                (
                    "node-a".to_string(),
                    "node-a".to_string(),
                    "p2p+relay".to_string(),
                ),
                (
                    "node-a".to_string(),
                    "node-b".to_string(),
                    "p2p+ice".to_string(),
                ),
            ],
            vec![
                (
                    "node-b".to_string(),
                    "node-b".to_string(),
                    "closed".to_string(),
                ),
                (
                    "node-a".to_string(),
                    "node-b".to_string(),
                    "closed".to_string(),
                ),
            ],
        );

        assert_eq!(edges.len(), 1);
        assert_eq!(edges[0].from, "node-a");
        assert_eq!(edges[0].to, "node-b");
        assert_eq!(edges[0].route, "p2p+ice+closed");
    }
}

#[cfg(test)]
mod rdma_control_parse_tests {
    use super::parse_member_rdma_control;
    use fluxon_commu::{META_KEY_PRODUCT_UUID, META_KEY_RDMA_CONTROL};
    use std::collections::BTreeMap;

    #[test]
    fn test_parse_member_rdma_control_keeps_persistent_config_across_restart() {
        let mut warnings = Vec::new();
        let mut ext = BTreeMap::new();
        let metadata = BTreeMap::new();
        ext.insert(
            META_KEY_RDMA_CONTROL.to_string(),
            r#"{"node_start_time":1,"enabled_devices":["mlx5_1","mlx5_0"]}"#.to_string(),
        );

        let control =
            parse_member_rdma_control(&mut warnings, "owner_node-1", 2, &metadata, Some(&ext))
                .unwrap();

        assert!(warnings.is_empty());
        assert_eq!(control.enabled_devices, vec!["mlx5_1", "mlx5_0"]);
    }

    #[test]
    fn test_parse_member_rdma_control_invalidates_machine_mismatch() {
        let mut warnings = Vec::new();
        let mut ext = BTreeMap::new();
        let mut metadata = BTreeMap::new();
        metadata.insert(META_KEY_PRODUCT_UUID.to_string(), "uuid-b".to_string());
        ext.insert(
            META_KEY_RDMA_CONTROL.to_string(),
            r#"{"node_start_time":1,"machine_product_uuid":"uuid-a","enabled_devices":["mlx5_0"]}"#
                .to_string(),
        );

        let control =
            parse_member_rdma_control(&mut warnings, "owner_node-1", 2, &metadata, Some(&ext));

        assert!(control.is_none());
        assert_eq!(warnings.len(), 1);
        assert!(warnings[0].contains("machine identity changed"));
    }
}

async fn build_mq_snapshot(
    cfg: &MonitorConfig,
    warnings: &mut Vec<String>,
    etcd: &mut EtcdClient,
) -> anyhow::Result<MqSnapshot> {
    // 1) Enumerate chan_id via /channels/meta/
    let mut meta_by_chan_id: BTreeMap<i64, Option<MqChanMetaSnapshot>> = BTreeMap::new();
    scan_etcd_prefix_paginated(etcd, MQ_META_KEY_PREFIX, |key, value| {
        let key = String::from_utf8_lossy(key).to_string();
        let Some(chan_id) = parse_mq_chan_id_from_meta_key(&key) else {
            warnings.push(format!(
                "unexpected mq meta key under {MQ_META_KEY_PREFIX}: {key}"
            ));
            return Ok::<EtcdPrefixScanAction, anyhow::Error>(EtcdPrefixScanAction::Continue);
        };
        let meta_opt = match serde_json::from_slice::<MqChanMetaWire>(value) {
            Ok(m) => Some(MqChanMetaSnapshot {
                capacity: m.capacity,
                ttl_seconds: m.ttl_seconds,
                payload_lease_id: m.payload_lease_id,
            }),
            Err(e) => {
                warnings.push(format!(
                    "invalid mq channel meta json: chan_id={} err={} value={}",
                    chan_id,
                    e,
                    String::from_utf8_lossy(value)
                ));
                None
            }
        };
        meta_by_chan_id.insert(chan_id, meta_opt);
        Ok::<EtcdPrefixScanAction, anyhow::Error>(EtcdPrefixScanAction::Continue)
    })
    .await
    .with_context(|| format!("etcd scan prefix: {MQ_META_KEY_PREFIX}"))?;

    // 2) Optional unique_key -> chan_id mappings, discovered via configured prefixes.
    let mut unique_keys_by_chan_id: BTreeMap<i64, Vec<String>> = BTreeMap::new();
    if let Some(prefixes) = cfg.mq_unique_key_prefixes.as_ref() {
        for p in prefixes {
            let mut ignored_parse_fail = 0usize;
            let mut ignored_unknown_chan = 0usize;
            let mut parse_fail_samples: Vec<String> = Vec::new();
            let mut unknown_chan_samples: Vec<String> = Vec::new();

            scan_etcd_prefix_paginated(etcd, p, |key, value| {
                let key = String::from_utf8_lossy(key).to_string();
                let value_str = String::from_utf8_lossy(value).to_string();
                let chan_id = match value_str.parse::<i64>() {
                    Ok(v) => v,
                    Err(e) => {
                        ignored_parse_fail += 1;
                        if parse_fail_samples.len() < 3 {
                            parse_fail_samples
                                .push(format!("key={key:?} value={value_str:?} err={e}"));
                        }
                        return Ok::<EtcdPrefixScanAction, anyhow::Error>(
                            EtcdPrefixScanAction::Continue,
                        );
                    }
                };

                // Match by chan_id: only attach unique_key to channels that exist under
                // `/channels/meta/`. This filters out lock keys and other unrelated keys
                // under the scanned prefixes.
                if !meta_by_chan_id.contains_key(&chan_id) {
                    ignored_unknown_chan += 1;
                    if unknown_chan_samples.len() < 3 {
                        unknown_chan_samples.push(format!(
                            "key={key:?} chan_id={chan_id} (meta missing under {MQ_META_KEY_PREFIX})"
                        ));
                    }
                    return Ok::<EtcdPrefixScanAction, anyhow::Error>(
                        EtcdPrefixScanAction::Continue,
                    );
                }

                unique_keys_by_chan_id.entry(chan_id).or_default().push(key);
                Ok::<EtcdPrefixScanAction, anyhow::Error>(EtcdPrefixScanAction::Continue)
            })
            .await
            .with_context(|| format!("etcd scan prefix (mq_unique_key_prefixes): {p}"))?;

            if ignored_parse_fail > 0 {
                warnings.push(format!(
                    "mq unique_key scan ignored {} keys with non-numeric values under prefix={:?} samples={}",
                    ignored_parse_fail,
                    p,
                    parse_fail_samples.join(" | ")
                ));
            }
            if ignored_unknown_chan > 0 {
                warnings.push(format!(
                    "mq unique_key scan ignored {} keys whose chan_id has no /channels/meta entry under prefix={:?} samples={}",
                    ignored_unknown_chan,
                    p,
                    unknown_chan_samples.join(" | ")
                ));
            }
        }
        for (_chan_id, keys) in unique_keys_by_chan_id.iter_mut() {
            keys.sort();
            keys.dedup();
        }
    }

    // 3) Collect per-channel member state + offsets.
    let mut channels: Vec<MqChannelSnapshot> = Vec::new();
    let mut external_ids_seen: BTreeMap<String, ()> = BTreeMap::new();
    for (chan_id, meta_opt) in meta_by_chan_id.iter() {
        let unique_keys = unique_keys_by_chan_id
            .get(chan_id)
            .cloned()
            .unwrap_or_default();

        // Producer membership: alive set + external_client_id (best-effort).
        let mut producer_external_by_idx: BTreeMap<String, Option<String>> = BTreeMap::new();
        let mut producer_alive: BTreeMap<String, ()> = BTreeMap::new();
        let producer_prefix = format!("/channels/{}/producer/producer_", chan_id);
        match scan_etcd_prefix_paginated(etcd, &producer_prefix, |key, value| {
            let key = String::from_utf8_lossy(key).to_string();
            let Some(pid) = parse_mq_producer_idx_from_member_key(&key, *chan_id) else {
                warnings.push(format!(
                    "unexpected mq producer membership key: chan_id={} key={key}",
                    chan_id
                ));
                return Ok::<EtcdPrefixScanAction, anyhow::Error>(EtcdPrefixScanAction::Continue);
            };
            producer_alive.insert(pid.clone(), ());

            let ext = match serde_json::from_slice::<MqProducerMemberMetaWire>(value) {
                Ok(m) => normalize_external_client_id(
                    warnings,
                    &format!(
                        "mq producer membership: chan_id={} producer_idx={}",
                        chan_id, pid
                    ),
                    m.external_client_id,
                ),
                Err(_) => None,
            };
            if let Some(ref e) = ext {
                external_ids_seen.insert(e.clone(), ());
            }
            producer_external_by_idx.insert(pid, ext);
            Ok::<EtcdPrefixScanAction, anyhow::Error>(EtcdPrefixScanAction::Continue)
        })
        .await
        {
            Ok(()) => {}
            Err(e) => {
                warnings.push(format!(
                    "etcd scan prefix failed (mq producer membership): {} (err={})",
                    producer_prefix, e
                ));
            }
        }

        // Consumer membership.
        let mut consumers: Vec<MqConsumerSnapshot> = Vec::new();
        let consumer_prefix = format!("/channels/{}/consumer/consumer_", chan_id);
        match scan_etcd_prefix_paginated(etcd, &consumer_prefix, |key, value| {
            let key = String::from_utf8_lossy(key).to_string();
            let Some(cid) = parse_mq_consumer_idx_from_member_key(&key, *chan_id) else {
                warnings.push(format!(
                    "unexpected mq consumer membership key: chan_id={} key={key}",
                    chan_id
                ));
                return Ok::<EtcdPrefixScanAction, anyhow::Error>(EtcdPrefixScanAction::Continue);
            };

            let (status, external_client_id, kvclient_sub_cluster) =
                match serde_json::from_slice::<MqConsumerMemberMetaWire>(value) {
                    Ok(m) => {
                        if m.role != MqChanRoleWire::Consumer {
                            warnings.push(format!(
                                "unexpected mq consumer membership role: chan_id={} consumer_idx={} role={:?}",
                                chan_id, cid, m.role
                            ));
                        }
                        let ext = normalize_external_client_id(
                            warnings,
                            &format!(
                                "mq consumer membership: chan_id={} consumer_idx={}",
                                chan_id, cid
                            ),
                            m.external_client_id,
                        );
                        (MqMemberStatus::Alive, ext, m.kvclient_sub_cluster)
                    }
                    Err(e) => {
                        warnings.push(format!(
                            "invalid mq consumer membership json: chan_id={} consumer_idx={} err={} value={}",
                            chan_id,
                            cid,
                            e,
                            String::from_utf8_lossy(value)
                        ));
                        (MqMemberStatus::Invalid, None, None)
                    }
                };
            if let Some(ref e) = external_client_id {
                external_ids_seen.insert(e.clone(), ());
            }
            consumers.push(MqConsumerSnapshot {
                consumer_idx: cid,
                status,
                external_client_id,
                owner_id: None,
                kvclient_sub_cluster,
                prefetch_avg_get_handle_us: None,
                prefetch_latest_get_handle_us: None,
                prefetch_avg_handle_await_us: None,
                prefetch_latest_handle_await_us: None,
                prefetch_avg_etcd_put_us: None,
                prefetch_latest_etcd_put_us: None,
                prefetch_inflight_queue_size: None,
                prefetch_target_inflight: None,
                get_one_avg_total_us: None,
                get_one_max_total_us: None,
                get_one_avg_wait_rx_us: None,
                get_one_max_wait_rx_us: None,
                get_one_avg_signal_us: None,
                get_one_max_signal_us: None,
                get_one_avg_post_us: None,
                get_one_max_post_us: None,
                get_one_window_calls: None,
                get_one_window_timeouts: None,
                get_one_window_bytes: None,
                nonblocking_latest_phase_calls: None,
                nonblocking_latest_phase_rps: None,
                nonblocking_latest_begin_unix_ms: None,
                nonblocking_latest_end_unix_ms: None,
            });
            Ok::<EtcdPrefixScanAction, anyhow::Error>(EtcdPrefixScanAction::Continue)
        })
        .await
        {
            Ok(()) => {}
            Err(e) => {
                warnings.push(format!(
                    "etcd scan prefix failed (mq consumer membership): {} (err={})",
                    consumer_prefix, e
                ));
            }
        }

        // Offsets.
        let mut produce_offset_by_pid: BTreeMap<String, i64> = BTreeMap::new();
        let produce_offset_prefix =
            format!("/channels/{}/producer_offset_of_all_producer/", chan_id);
        match scan_etcd_prefix_paginated(etcd, &produce_offset_prefix, |key, value| {
            let key = String::from_utf8_lossy(key).to_string();
            let Some(pid) = key.strip_prefix(&produce_offset_prefix) else {
                return Ok::<EtcdPrefixScanAction, anyhow::Error>(EtcdPrefixScanAction::Continue);
            };
            if pid.is_empty() || pid.contains('/') {
                warnings.push(format!(
                    "unexpected mq produce_offset key: chan_id={} key={key}",
                    chan_id
                ));
                return Ok::<EtcdPrefixScanAction, anyhow::Error>(EtcdPrefixScanAction::Continue);
            }
            let ctx = format!(
                "mq produce_offset: chan_id={} producer_idx={}",
                chan_id, pid
            );
            if let Some(v) = parse_mq_offset_value_i64(warnings, &ctx, value) {
                produce_offset_by_pid.insert(pid.to_string(), v);
            }
            Ok::<EtcdPrefixScanAction, anyhow::Error>(EtcdPrefixScanAction::Continue)
        })
        .await
        {
            Ok(()) => {}
            Err(e) => {
                warnings.push(format!(
                    "etcd scan prefix failed (mq produce_offset): {} (err={})",
                    produce_offset_prefix, e
                ));
            }
        }

        let mut consume_offset_by_pid: BTreeMap<String, i64> = BTreeMap::new();
        let consume_offset_prefix =
            format!("/channels/{}/consumer_offset_of_all_producer/", chan_id);
        match scan_etcd_prefix_paginated(etcd, &consume_offset_prefix, |key, value| {
            let key = String::from_utf8_lossy(key).to_string();
            let Some(pid) = key.strip_prefix(&consume_offset_prefix) else {
                return Ok::<EtcdPrefixScanAction, anyhow::Error>(EtcdPrefixScanAction::Continue);
            };
            if pid.is_empty() || pid.contains('/') {
                warnings.push(format!(
                    "unexpected mq consume_offset key: chan_id={} key={key}",
                    chan_id
                ));
                return Ok::<EtcdPrefixScanAction, anyhow::Error>(EtcdPrefixScanAction::Continue);
            }
            let ctx = format!(
                "mq consume_offset: chan_id={} producer_idx={}",
                chan_id, pid
            );
            if let Some(v) = parse_mq_offset_value_i64(warnings, &ctx, value) {
                consume_offset_by_pid.insert(pid.to_string(), v);
            }
            Ok::<EtcdPrefixScanAction, anyhow::Error>(EtcdPrefixScanAction::Continue)
        })
        .await
        {
            Ok(()) => {}
            Err(e) => {
                warnings.push(format!(
                    "etcd scan prefix failed (mq consume_offset): {} (err={})",
                    consume_offset_prefix, e
                ));
            }
        }

        // Join producer ids from membership + offsets.
        let mut all_pids: BTreeMap<String, ()> = BTreeMap::new();
        for pid in producer_alive.keys() {
            all_pids.insert(pid.clone(), ());
        }
        for pid in produce_offset_by_pid.keys() {
            all_pids.insert(pid.clone(), ());
        }
        for pid in consume_offset_by_pid.keys() {
            all_pids.insert(pid.clone(), ());
        }

        let mut producers: Vec<MqProducerSnapshot> = Vec::new();
        for pid in all_pids.keys() {
            let status = if producer_alive.contains_key(pid) {
                MqMemberStatus::Alive
            } else {
                MqMemberStatus::Stale
            };
            let external_client_id = producer_external_by_idx.get(pid).cloned().unwrap_or(None);
            producers.push(MqProducerSnapshot {
                producer_idx: pid.clone(),
                status,
                external_client_id,
                owner_id: None,
                produce_offset: produce_offset_by_pid.get(pid).copied(),
                consume_offset: consume_offset_by_pid.get(pid).copied(),
                put_window_calls: None,
                put_window_bytes: None,
                nonblocking_latest_phase_calls: None,
                nonblocking_latest_phase_rps: None,
                nonblocking_latest_begin_unix_ms: None,
                nonblocking_latest_end_unix_ms: None,
            });
        }

        channels.push(MqChannelSnapshot {
            chan_id: *chan_id,
            unique_keys,
            meta: meta_opt.clone(),
            producers,
            consumers,
        });
    }

    // 4) Best-effort external_client_id -> owner_id mapping via share-group index.
    let mut owner_by_member_id: HashMap<String, String> = HashMap::new();
    let mut owner_start_time_by_member_id: HashMap<String, i64> = HashMap::new();
    if !external_ids_seen.is_empty() {
        let share_group_owner_prefix =
            format!("/fluxon_commu_share_group/{}/owner/", cfg.cluster_name);
        match scan_etcd_prefix_paginated(etcd, &share_group_owner_prefix, |key, _value| {
            let key = String::from_utf8_lossy(key).to_string();
            if let Some((member_id, owner_id, owner_start_time)) =
                try_parse_share_group_owner_by_member_id(&share_group_owner_prefix, &key)
            {
                let replace = owner_start_time_by_member_id
                    .get(&member_id)
                    .is_none_or(|current| owner_start_time >= *current);
                if replace {
                    owner_start_time_by_member_id.insert(member_id.clone(), owner_start_time);
                    owner_by_member_id.insert(member_id, owner_id);
                }
            }
            Ok::<EtcdPrefixScanAction, anyhow::Error>(EtcdPrefixScanAction::Continue)
        })
        .await
        {
            Ok(()) => {}
            Err(e) => {
                warnings.push(format!(
                    "etcd scan prefix failed (mq share-group index): {} (err={})",
                    share_group_owner_prefix, e
                ));
            }
        }
    }

    for ch in &mut channels {
        for p in &mut ch.producers {
            if let Some(ext) = p.external_client_id.as_ref() {
                p.owner_id = owner_by_member_id.get(ext).cloned();
            }
        }
        for c in &mut ch.consumers {
            if let Some(ext) = c.external_client_id.as_ref() {
                c.owner_id = owner_by_member_id.get(ext).cloned();
            }
        }
    }

    Ok(MqSnapshot { channels })
}

#[derive(Debug, Clone, Deserialize)]
struct AccessibleIpInfo {
    ip: String,
    node_start_time: i64,
}

fn parse_member_rdma_control(
    warnings: &mut Vec<String>,
    member_id: &str,
    _member_start_time: i64,
    base_metadata: &BTreeMap<String, String>,
    ext: Option<&BTreeMap<String, String>>,
) -> Option<MemberRdmaControl> {
    fn metadata_value(metadata: &BTreeMap<String, String>, key: &str) -> Option<String> {
        let value = metadata.get(key)?.trim();
        if value.is_empty() {
            None
        } else {
            Some(value.to_string())
        }
    }

    fn rdma_control_machine_identity_conflicts_with_base_metadata(
        control: &MemberRdmaControl,
        base_metadata: &BTreeMap<String, String>,
    ) -> bool {
        if let (Some(saved), Some(current)) = (
            control.machine_product_uuid.as_deref(),
            metadata_value(base_metadata, META_KEY_PRODUCT_UUID).as_deref(),
        ) {
            return saved != current;
        }
        if let (Some(saved), Some(current)) = (
            control.machine_hostname.as_deref(),
            metadata_value(base_metadata, META_KEY_HOSTNAME).as_deref(),
        ) {
            return saved != current;
        }
        false
    }

    let raw = ext.and_then(|m| m.get(META_KEY_RDMA_CONTROL))?;
    match serde_json::from_str::<MemberRdmaControl>(raw) {
        Ok(control) => {
            if rdma_control_machine_identity_conflicts_with_base_metadata(&control, base_metadata) {
                warnings.push(format!(
                    "stale persistent rdma_control for member_id={} (machine identity changed)",
                    member_id
                ));
                None
            } else {
                Some(control)
            }
        }
        Err(err) => {
            warnings.push(format!(
                "invalid rdma_control for member_id={} (err={})",
                member_id, err
            ));
            None
        }
    }
}

async fn overlay_persistent_owner_rdma_control(
    etcd: &mut EtcdClient,
    cluster_name: &str,
    warnings: &mut Vec<String>,
    member_ext: &mut HashMap<String, BTreeMap<String, String>>,
) {
    let control_prefix = format!("{}/", cluster_owner_rdma_control_prefix(cluster_name));
    match scan_etcd_prefix_paginated(etcd, &control_prefix, |key, value| {
        let key = String::from_utf8_lossy(key).to_string();
        if !key.starts_with(&control_prefix) {
            return Ok::<EtcdPrefixScanAction, anyhow::Error>(EtcdPrefixScanAction::Continue);
        }
        let member_id = &key[control_prefix.len()..];
        if member_id.is_empty() || member_id.contains('/') {
            warnings.push(format!(
                "unexpected persistent rdma_control key under {}: {}",
                control_prefix, key
            ));
            return Ok::<EtcdPrefixScanAction, anyhow::Error>(EtcdPrefixScanAction::Continue);
        }
        member_ext.entry(member_id.to_string()).or_default().insert(
            META_KEY_RDMA_CONTROL.to_string(),
            String::from_utf8_lossy(value).to_string(),
        );
        Ok::<EtcdPrefixScanAction, anyhow::Error>(EtcdPrefixScanAction::Continue)
    })
    .await
    {
        Ok(()) => {}
        Err(e) => {
            warnings.push(format!(
                "etcd scan prefix failed (persistent owner rdma control): {} (err={})",
                control_prefix, e
            ));
        }
    }
}

fn parse_member_rdma_runtime(
    warnings: &mut Vec<String>,
    member_id: &str,
    member_start_time: i64,
    ext: Option<&BTreeMap<String, String>>,
) -> Option<MemberRdmaRuntime> {
    let raw = ext.and_then(|m| m.get(META_KEY_RDMA_RUNTIME))?;
    match serde_json::from_str::<MemberRdmaRuntime>(raw) {
        Ok(runtime) => {
            if runtime.node_start_time != member_start_time {
                warnings.push(format!(
                    "stale member ext rdma_runtime for member_id={} (ext.node_start_time={}, base.node_start_time={})",
                    member_id, runtime.node_start_time, member_start_time
                ));
                None
            } else {
                Some(runtime)
            }
        }
        Err(err) => {
            warnings.push(format!(
                "invalid member ext rdma_runtime for member_id={} (err={})",
                member_id, err
            ));
            None
        }
    }
}

fn rdma_link_layer_text(value: RdmaLinkLayer) -> &'static str {
    match value {
        RdmaLinkLayer::Infiniband => "ib",
        RdmaLinkLayer::Ethernet => "eth",
        RdmaLinkLayer::Unknown => "unknown",
    }
}

fn rdma_port_state_text(value: RdmaPortState) -> &'static str {
    match value {
        RdmaPortState::Nop => "nop",
        RdmaPortState::Down => "down",
        RdmaPortState::Init => "init",
        RdmaPortState::Armed => "armed",
        RdmaPortState::Active => "active",
        RdmaPortState::ActiveDefer => "active_defer",
        RdmaPortState::Unknown => "unknown",
    }
}

fn rdma_phys_state_text(value: RdmaPhysState) -> &'static str {
    match value {
        RdmaPhysState::NoStateChange => "no_state_change",
        RdmaPhysState::Sleep => "sleep",
        RdmaPhysState::Polling => "polling",
        RdmaPhysState::Disabled => "disabled",
        RdmaPhysState::PortConfigurationTraining => "port_cfg_training",
        RdmaPhysState::LinkUp => "link_up",
        RdmaPhysState::LinkErrorRecovery => "link_error_recovery",
        RdmaPhysState::PhyTest => "phy_test",
        RdmaPhysState::Unknown => "unknown",
    }
}

fn build_member_rdma_ports(
    _control: Option<&MemberRdmaControl>,
    runtime: Option<&MemberRdmaRuntime>,
) -> Vec<MemberRdmaPortSnapshot> {
    let mut port_keys: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
    if let Some(runtime) = runtime {
        for port in &runtime.ports {
            port_keys.insert(port.port_key.clone());
        }
        for port_key in &runtime.desired_enabled_ports {
            port_keys.insert(port_key.clone());
        }
        for port_key in &runtime.effective_enabled_ports {
            port_keys.insert(port_key.clone());
        }
    }

    let desired_enabled = runtime
        .map(|runtime| {
            runtime
                .desired_enabled_ports
                .iter()
                .cloned()
                .collect::<std::collections::BTreeSet<String>>()
        })
        .unwrap_or_default();
    let effective_enabled = runtime
        .map(|runtime| {
            runtime
                .effective_enabled_ports
                .iter()
                .cloned()
                .collect::<std::collections::BTreeSet<String>>()
        })
        .unwrap_or_default();
    let runtime_by_port_key = runtime
        .map(|runtime| {
            runtime
                .ports
                .iter()
                .map(|port| (port.port_key.clone(), port))
                .collect::<std::collections::BTreeMap<String, _>>()
        })
        .unwrap_or_default();

    port_keys
        .into_iter()
        .map(|port_key| {
            if let Some(port) = runtime_by_port_key.get(&port_key) {
                MemberRdmaPortSnapshot {
                    device: port.device.clone(),
                    port: port.port,
                    port_key: port_key.clone(),
                    desired_enabled: desired_enabled.contains(&port_key),
                    detected: true,
                    effective_enabled: effective_enabled.contains(&port_key),
                    usable: port.usable,
                    link_layer_text: rdma_link_layer_text(port.link_layer).to_string(),
                    port_state_text: rdma_port_state_text(port.port_state).to_string(),
                    phys_state_text: rdma_phys_state_text(port.phys_state).to_string(),
                    active_mtu_text: if port.active_mtu_bytes == 0 {
                        "-".to_string()
                    } else {
                        format!("{}B", port.active_mtu_bytes)
                    },
                    netdev: port.netdev.clone(),
                    pci_bdf: port.pci_bdf.clone(),
                    pcie_max_bandwidth_mbps: port.pcie_max_bandwidth_mbps,
                    numa_node: port.numa_node,
                    speed_gbps: port.speed_gbps,
                    driver: port.driver.clone(),
                    firmware: port.firmware.clone(),
                    last_error: port.last_error.clone(),
                }
            } else {
                MemberRdmaPortSnapshot {
                    device: String::new(),
                    port: 0,
                    port_key: port_key.clone(),
                    desired_enabled: desired_enabled.contains(&port_key),
                    detected: false,
                    effective_enabled: effective_enabled.contains(&port_key),
                    usable: false,
                    link_layer_text: "-".to_string(),
                    port_state_text: "missing".to_string(),
                    phys_state_text: "-".to_string(),
                    active_mtu_text: "-".to_string(),
                    netdev: None,
                    pci_bdf: None,
                    pcie_max_bandwidth_mbps: None,
                    numa_node: None,
                    speed_gbps: None,
                    driver: None,
                    firmware: None,
                    last_error: None,
                }
            }
        })
        .collect()
}

fn build_member_rdma_devices(
    control: Option<&MemberRdmaControl>,
    runtime: Option<&MemberRdmaRuntime>,
) -> Vec<MemberRdmaDeviceSnapshot> {
    let desired_enabled_devices = control
        .map(|value| {
            value
                .enabled_devices
                .iter()
                .cloned()
                .collect::<std::collections::BTreeSet<String>>()
        })
        .unwrap_or_default();
    let effective_enabled_ports = runtime
        .map(|value| {
            value
                .effective_enabled_ports
                .iter()
                .cloned()
                .collect::<std::collections::BTreeSet<String>>()
        })
        .unwrap_or_default();
    let ports_by_device = runtime
        .map(|value| {
            let mut grouped = std::collections::BTreeMap::<String, Vec<&RdmaPortSnapshot>>::new();
            for port in &value.ports {
                grouped.entry(port.device.clone()).or_default().push(port);
            }
            grouped
        })
        .unwrap_or_default();
    let mut device_names = desired_enabled_devices.clone();
    for device in ports_by_device.keys() {
        device_names.insert(device.clone());
    }

    device_names
        .into_iter()
        .map(|device| {
            let ports = ports_by_device.get(&device);
            let total_ports = ports.map(|value| value.len()).unwrap_or(0);
            let usable_ports = ports
                .map(|value| value.iter().filter(|port| port.usable).count())
                .unwrap_or(0);
            let blocked_ports = total_ports.saturating_sub(usable_ports);
            let effective_enabled = ports
                .map(|value| {
                    value
                        .iter()
                        .any(|port| effective_enabled_ports.contains(&port.port_key))
                })
                .unwrap_or(false);
            MemberRdmaDeviceSnapshot {
                device: device.clone(),
                detected: ports.is_some(),
                desired_enabled: desired_enabled_devices.contains(&device),
                effective_enabled,
                total_ports,
                usable_ports,
                blocked_ports,
            }
        })
        .collect()
}

fn node_key_for_member(
    warnings: &mut Vec<String>,
    owner_by_member_id: &HashMap<String, String>,
    role: crate::model::MemberRole,
    member_id: &str,
    shared_mem_dir: Option<&str>,
) -> String {
    // Fluxon CLI "node" is a virtual node determined by the owner client.
    // - owner_client: defines the node identity.
    // - external-like members (external_client / side_transfer_worker): attach to the owner's
    //   node because the shared memory is still owned by owner_client.
    // shared_mem_dir is collected for display/diagnostics only.
    if role == crate::model::MemberRole::OwnerClient {
        return member_id.to_string();
    }
    if !role.is_external_like() {
        return member_id.to_string();
    }

    match owner_by_member_id.get(member_id).cloned() {
        Some(owner_id) => owner_id,
        None => {
            match shared_mem_dir.filter(|s| !s.trim().is_empty()) {
                Some(dir) => warnings.push(format!(
                    "missing share-group owner_id mapping for external-like member; expected etcd key under /fluxon_commu_share_group/.../members/: member_id={} shared_mem_dir={}",
                    member_id, dir
                )),
                None => warnings.push(format!(
                    "missing share-group owner_id mapping for external-like member; expected etcd key under /fluxon_commu_share_group/.../members/: member_id={}",
                    member_id
                )),
            }
            member_id.to_string()
        }
    }
}

fn try_parse_share_group_owner_by_member_id(
    share_group_owner_prefix: &str,
    etcd_key: &str,
) -> Option<(String, String, i64)> {
    // Key format:
    // `/fluxon_commu_share_group/{cluster}/owner/{owner_id}/start_time/{owner_start_time}/members/{member_id}` => value "1"
    if !etcd_key.starts_with(share_group_owner_prefix) {
        return None;
    }
    let rest = &etcd_key[share_group_owner_prefix.len()..];
    let (owner_id, rest) = rest.split_once("/start_time/")?;
    let (owner_start_time_text, member_id) = rest.split_once("/members/")?;
    if owner_id.trim().is_empty()
        || owner_start_time_text.trim().is_empty()
        || member_id.trim().is_empty()
        || member_id.contains('/')
    {
        return None;
    }
    let owner_start_time = owner_start_time_text.parse::<i64>().ok()?;
    Some((
        member_id.to_string(),
        owner_id.to_string(),
        owner_start_time,
    ))
}

async fn build_fs_cluster_snapshot(
    cfg: &MonitorConfig,
    warnings: &mut Vec<String>,
) -> anyhow::Result<ClusterSnapshot> {
    let mut etcd = EtcdClient::connect(cfg.etcd_endpoints.clone(), None)
        .await
        .with_context(|| "connect etcd".to_string())?;

    let cluster_prefix = format!("{}/", cluster_member_base_prefix(&cfg.cluster_name));
    let meta_prefix = format!("{}/", cluster_member_ext_prefix(&cfg.cluster_name));

    let mut members_all: Vec<ClusterMember> = Vec::new();
    scan_etcd_prefix_paginated(&mut etcd, &cluster_prefix, |_key, value| {
        let m: ClusterMember = serde_json::from_slice(value).with_context(|| {
            format!(
                "parse cluster member json: {}",
                String::from_utf8_lossy(value)
            )
        })?;
        members_all.push(m);
        Ok::<EtcdPrefixScanAction, anyhow::Error>(EtcdPrefixScanAction::Continue)
    })
    .await
    .with_context(|| format!("etcd scan prefix: {}", cluster_prefix))?;
    if members_all.is_empty() {
        warnings.push(format!(
            "no members found under etcd prefix: {}",
            cluster_prefix
        ));
    }

    let mut member_ext: HashMap<String, BTreeMap<String, String>> = HashMap::new();
    match scan_etcd_prefix_paginated(&mut etcd, &meta_prefix, |key, value| {
        let key = String::from_utf8_lossy(key).to_string();
        let value = String::from_utf8_lossy(value).to_string();
        if !key.starts_with(&meta_prefix) {
            return Ok::<EtcdPrefixScanAction, anyhow::Error>(EtcdPrefixScanAction::Continue);
        }
        let rest = &key[meta_prefix.len()..];
        let Some((member_id, k)) = rest.split_once('/') else {
            return Ok::<EtcdPrefixScanAction, anyhow::Error>(EtcdPrefixScanAction::Continue);
        };
        member_ext
            .entry(member_id.to_string())
            .or_default()
            .insert(k.to_string(), value);
        Ok::<EtcdPrefixScanAction, anyhow::Error>(EtcdPrefixScanAction::Continue)
    })
    .await
    {
        Ok(()) => {}
        Err(e) => {
            warnings.push(format!(
                "etcd scan prefix failed (member ext metadata): {} (err={})",
                meta_prefix, e
            ));
        }
    }
    overlay_persistent_owner_rdma_control(&mut etcd, &cfg.cluster_name, warnings, &mut member_ext)
        .await;

    let mut members: Vec<ClusterMember> = Vec::new();
    for m in members_all {
        let ext = member_ext.get(&m.id);
        let cmd = ext
            .and_then(|m| m.get("cmd").map(|s| s.as_str()))
            .or_else(|| m.metadata.get("cmd").map(|s| s.as_str()));
        if crate::model::classify_fluxon_fs_component(&m.id, cmd).is_some() {
            members.push(m);
        }
    }
    if members.is_empty() {
        warnings.push(
            "no fluxon_fs agent/controller members found (matched by cmdline/instance_key markers)"
                .to_string(),
        );
    }

    let mut nodes_map: BTreeMap<String, NodeSnapshot> = BTreeMap::new();
    for m in members {
        let role = role_from_member_metadata(&m.metadata);
        let is_p2p_relay = matches!(
            m.metadata.get("p2p_relay").map(|v| v.as_str()),
            Some("true")
        );
        let is_side_transfer_worker = matches!(
            m.metadata.get("side_transfer_worker").map(|v| v.as_str()),
            Some("true")
        );
        let ext = member_ext.get(&m.id);

        let hostname = ext
            .and_then(|m| m.get(META_KEY_HOSTNAME).cloned())
            .or_else(|| m.metadata.get(META_KEY_HOSTNAME).cloned());
        let accessible_ip = match ext.and_then(|m| m.get("accessible_ip")) {
            Some(v) => match serde_json::from_str::<AccessibleIpInfo>(v) {
                Ok(info) => {
                    if info.node_start_time != m.node_start_time {
                        warnings.push(format!(
                            "stale member ext accessible_ip for member_id={} (ext.node_start_time={}, base.node_start_time={})",
                            m.id, info.node_start_time, m.node_start_time
                        ));
                        None
                    } else if info.ip.is_empty() {
                        warnings.push(format!(
                            "empty member ext accessible_ip for member_id={} (ext.node_start_time={})",
                            m.id, info.node_start_time
                        ));
                        None
                    } else {
                        Some(info.ip)
                    }
                }
                Err(e) => {
                    warnings.push(format!(
                        "invalid member ext accessible_ip for member_id={} (expected JSON {{ip,node_start_time}}): {} (err={})",
                        m.id, v, e
                    ));
                    None
                }
            },
            None => None,
        };
        let shared_mem_dir = ext
            .and_then(|m| m.get("shared_mem_dir").cloned())
            .or_else(|| m.metadata.get("shared_mem_dir").cloned());
        let rdma_control =
            parse_member_rdma_control(warnings, &m.id, m.node_start_time, &m.metadata, ext);
        let rdma_runtime = parse_member_rdma_runtime(warnings, &m.id, m.node_start_time, ext);
        let pid = ext
            .and_then(|m| m.get(META_KEY_PID))
            .and_then(|s| s.parse::<u32>().ok())
            .or_else(|| {
                m.metadata
                    .get(META_KEY_PID)
                    .and_then(|s| s.parse::<u32>().ok())
            });
        let cmd = ext
            .and_then(|m| m.get(META_KEY_CMD).cloned())
            .or_else(|| m.metadata.get(META_KEY_CMD).cloned());
        let p2p_listen_port = ext
            .and_then(|m| m.get("p2p_listen_port"))
            .and_then(|s| s.parse::<u16>().ok())
            .or_else(|| {
                m.metadata
                    .get("p2p_listen_port")
                    .and_then(|s| s.parse::<u16>().ok())
            });

        let member_snapshot = MemberSnapshot {
            member_id: m.id.clone(),
            role,
            is_p2p_relay,
            is_side_transfer_worker,
            node_start_time: m.node_start_time,
            hostname: hostname.clone(),
            accessible_ip: accessible_ip.clone(),
            shared_mem_dir: shared_mem_dir.clone(),
            p2p_listen_port,
            rdma_runtime_reported: rdma_runtime.is_some(),
            rdma_probe_error: rdma_runtime
                .as_ref()
                .and_then(|runtime| runtime.probe_error.clone()),
            rdma_devices: build_member_rdma_devices(rdma_control.as_ref(), rdma_runtime.as_ref()),
            rdma_ports: build_member_rdma_ports(rdma_control.as_ref(), rdma_runtime.as_ref()),
            rdma_transfer_engine: rdma_runtime
                .as_ref()
                .map(|runtime| runtime.transfer_engine.clone()),
            pid,
            cmd: cmd.clone(),
            sub_cluster: m.sub_cluster.clone(),
            product_uuid: m.metadata.get(META_KEY_PRODUCT_UUID).cloned(),
            node_cpu_usage_percent: None,
            node_cpu_logical_cores: None,
            node_memory_usage_bytes: None,
            node_memory_total_bytes: None,
            container_memory_usage_bytes: None,
            container_memory_limit_bytes: None,
            process_resident_memory_bytes: None,
            process_cpu_usage_percent: None,
            tokio_num_workers: None,
            tokio_alive_tasks: None,
            tokio_global_queue_depth: None,
            tokio_busy_percent: None,
            tokio_max_worker_busy_percent: None,
            tokio_park_unpark_rate_hz: None,
            process_net_tx_mbps: None,
            process_net_rx_mbps: None,
            kv_put_rps: None,
            kv_get_rps: None,
            kv_put_bps: None,
            kv_get_bps: None,
            kv_put_latency_mean_us: None,
            kv_put_latency_p95_us: None,
            kv_put_latency_p99_us: None,
            kv_get_latency_mean_us: None,
            kv_get_latency_p95_us: None,
            kv_get_latency_p99_us: None,
            seg_capacity_bytes: None,
            seg_used_bytes: None,
            fs_read_rps: None,
            fs_write_rps: None,
        };

        let node_key = m.id.clone();
        let node_entry = nodes_map
            .entry(node_key.clone())
            .or_insert_with(|| NodeSnapshot {
                node_key,
                hostname: hostname.clone(),
                accessible_ip: accessible_ip.clone(),
                shared_mem_dir: shared_mem_dir.clone(),
                is_p2p_relay,
                node_cpu_usage_percent: None,
                node_cpu_logical_cores: None,
                node_memory_usage_bytes: None,
                node_memory_total_bytes: None,
                container_memory_usage_bytes: None,
                container_memory_limit_bytes: None,
                members: Vec::new(),
                segment_devices: Vec::new(),
            });

        if is_p2p_relay {
            node_entry.is_p2p_relay = true;
        }
        if node_entry.hostname.is_none() {
            node_entry.hostname = hostname.clone();
        }
        if node_entry.accessible_ip.is_none() {
            node_entry.accessible_ip = accessible_ip.clone();
        }
        if node_entry.shared_mem_dir.is_none() {
            node_entry.shared_mem_dir = shared_mem_dir.clone();
        }
        node_entry.members.push(member_snapshot);
    }

    let mut nodes: Vec<NodeSnapshot> = nodes_map.into_values().collect();
    for n in &mut nodes {
        n.members.sort_by(|a, b| a.member_id.cmp(&b.member_id));
    }

    let warnings_out = std::mem::take(warnings);
    Ok(ClusterSnapshot {
        cluster_name: cfg.cluster_name.clone(),
        member_kind: cfg.member_kind,
        etcd_endpoints: cfg.etcd_endpoints.clone(),
        prometheus_base_url: cfg.prometheus_base_url.clone(),
        warnings: warnings_out,
        visible_member_roles: None,
        master_id: None,
        master_network: None,
        transfer_engine_edges: Vec::new(),
        kv_peer_network: Vec::new(),
        rdma_netdev_network: Vec::new(),
        fs_mount_fs: Vec::new(),
        shm_files: Vec::new(),
        fs_export_registry: Vec::new(),
        fs_mount_registry: Vec::new(),
        kv_topology_owner_external_max: Vec::new(),
        kv_topology_machine_external_max: Vec::new(),
        kv_topology_sub_cluster_owner_owner_max: Vec::new(),
        nodes,
        mq: None,
        total_put_rps: None,
        total_get_rps: None,
        total_put_bps: None,
        total_get_bps: None,
        total_put_latency_mean_us: None,
        total_put_latency_p95_us: None,
        total_put_latency_p99_us: None,
        total_get_latency_mean_us: None,
        total_get_latency_p95_us: None,
        total_get_latency_p99_us: None,
    })
}

pub async fn build_cluster_snapshot(cfg: &MonitorConfig) -> anyhow::Result<ClusterSnapshot> {
    build_cluster_snapshot_with_prom_query_time(cfg, None).await
}

pub async fn build_cluster_snapshot_with_prom_query_time(
    cfg: &MonitorConfig,
    prom_query_time_s: Option<f64>,
) -> anyhow::Result<ClusterSnapshot> {
    let mut warnings: Vec<String> = Vec::new();
    if cfg.member_kind == MemberKind::Mq {
        let mut etcd = EtcdClient::connect(cfg.etcd_endpoints.clone(), None)
            .await
            .with_context(|| "connect etcd".to_string())?;
        let mut mq = build_mq_snapshot(cfg, &mut warnings, &mut etcd).await?;

        let prom =
            PromClient::new_with_query_time(cfg.prometheus_base_url.clone(), prom_query_time_s);
        let mq_prom_maps = crate::prom::collect_mq_prom_snapshot(&prom, &mut warnings).await;
        for ch in &mut mq.channels {
            for p in &mut ch.producers {
                let key = (ch.chan_id, p.producer_idx.clone());
                p.put_window_calls = mq_prom_maps.put_window_calls.get(&key).copied();
                p.put_window_bytes = mq_prom_maps.put_window_bytes.get(&key).copied();
                p.nonblocking_latest_phase_calls = mq_prom_maps
                    .producer_nonblocking_latest_phase_calls
                    .get(&key)
                    .copied();
                p.nonblocking_latest_phase_rps = mq_prom_maps
                    .producer_nonblocking_latest_phase_rps
                    .get(&key)
                    .copied();
                p.nonblocking_latest_begin_unix_ms = mq_prom_maps
                    .producer_nonblocking_latest_begin_unix_ms
                    .get(&key)
                    .copied();
                p.nonblocking_latest_end_unix_ms = mq_prom_maps
                    .producer_nonblocking_latest_end_unix_ms
                    .get(&key)
                    .copied();
            }
            for c in &mut ch.consumers {
                let key = (ch.chan_id, c.consumer_idx.clone());
                c.prefetch_avg_get_handle_us =
                    mq_prom_maps.prefetch_avg_get_handle_us.get(&key).copied();
                c.prefetch_latest_get_handle_us = mq_prom_maps
                    .prefetch_latest_get_handle_us
                    .get(&key)
                    .copied();
                c.prefetch_avg_handle_await_us =
                    mq_prom_maps.prefetch_avg_handle_await_us.get(&key).copied();
                c.prefetch_latest_handle_await_us = mq_prom_maps
                    .prefetch_latest_handle_await_us
                    .get(&key)
                    .copied();
                c.prefetch_avg_etcd_put_us =
                    mq_prom_maps.prefetch_avg_etcd_put_us.get(&key).copied();
                c.prefetch_latest_etcd_put_us =
                    mq_prom_maps.prefetch_latest_etcd_put_us.get(&key).copied();
                c.prefetch_inflight_queue_size =
                    mq_prom_maps.prefetch_inflight_queue_size.get(&key).copied();
                c.prefetch_target_inflight =
                    mq_prom_maps.prefetch_target_inflight.get(&key).copied();

                c.get_one_avg_total_us = mq_prom_maps.get_one_avg_total_us.get(&key).copied();
                c.get_one_max_total_us = mq_prom_maps.get_one_max_total_us.get(&key).copied();
                c.get_one_avg_wait_rx_us = mq_prom_maps.get_one_avg_wait_rx_us.get(&key).copied();
                c.get_one_max_wait_rx_us = mq_prom_maps.get_one_max_wait_rx_us.get(&key).copied();
                c.get_one_avg_signal_us = mq_prom_maps.get_one_avg_signal_us.get(&key).copied();
                c.get_one_max_signal_us = mq_prom_maps.get_one_max_signal_us.get(&key).copied();
                c.get_one_avg_post_us = mq_prom_maps.get_one_avg_post_us.get(&key).copied();
                c.get_one_max_post_us = mq_prom_maps.get_one_max_post_us.get(&key).copied();
                c.get_one_window_calls = mq_prom_maps.get_one_window_calls.get(&key).copied();
                c.get_one_window_timeouts = mq_prom_maps.get_one_window_timeouts.get(&key).copied();
                c.get_one_window_bytes = mq_prom_maps.get_one_window_bytes.get(&key).copied();
                c.nonblocking_latest_phase_calls = mq_prom_maps
                    .consumer_nonblocking_latest_phase_calls
                    .get(&key)
                    .copied();
                c.nonblocking_latest_phase_rps = mq_prom_maps
                    .consumer_nonblocking_latest_phase_rps
                    .get(&key)
                    .copied();
                c.nonblocking_latest_begin_unix_ms = mq_prom_maps
                    .consumer_nonblocking_latest_begin_unix_ms
                    .get(&key)
                    .copied();
                c.nonblocking_latest_end_unix_ms = mq_prom_maps
                    .consumer_nonblocking_latest_end_unix_ms
                    .get(&key)
                    .copied();
            }
        }
        return Ok(ClusterSnapshot {
            cluster_name: cfg.cluster_name.clone(),
            member_kind: cfg.member_kind,
            etcd_endpoints: cfg.etcd_endpoints.clone(),
            prometheus_base_url: cfg.prometheus_base_url.clone(),
            warnings,
            visible_member_roles: None,
            master_id: None,
            master_network: None,
            transfer_engine_edges: Vec::new(),
            kv_peer_network: Vec::new(),
            rdma_netdev_network: Vec::new(),
            fs_mount_fs: Vec::new(),
            shm_files: Vec::new(),
            fs_export_registry: Vec::new(),
            fs_mount_registry: Vec::new(),
            kv_topology_owner_external_max: Vec::new(),
            kv_topology_machine_external_max: Vec::new(),
            kv_topology_sub_cluster_owner_owner_max: Vec::new(),
            nodes: Vec::new(),
            mq: Some(mq),
            total_put_rps: None,
            total_get_rps: None,
            total_put_bps: None,
            total_get_bps: None,
            total_put_latency_mean_us: None,
            total_put_latency_p95_us: None,
            total_put_latency_p99_us: None,
            total_get_latency_mean_us: None,
            total_get_latency_p95_us: None,
            total_get_latency_p99_us: None,
        });
    }
    if cfg.member_kind == MemberKind::Fs {
        return build_fs_cluster_snapshot(cfg, &mut warnings).await;
    }
    let mut etcd = EtcdClient::connect(cfg.etcd_endpoints.clone(), None)
        .await
        .with_context(|| "connect etcd".to_string())?;

    let cluster_prefix = format!("{}/", cluster_member_base_prefix(&cfg.cluster_name));
    let meta_prefix = format!("{}/", cluster_member_ext_prefix(&cfg.cluster_name));

    let mut members: Vec<ClusterMember> = Vec::new();
    let mut master_id: Option<String> = None;
    let mut master_network = None;
    let mut has_external_like_members = false;
    scan_etcd_prefix_paginated(&mut etcd, &cluster_prefix, |_key, value| {
        let m: ClusterMember = serde_json::from_slice(value).with_context(|| {
            format!(
                "parse cluster member json: {}",
                String::from_utf8_lossy(value)
            )
        })?;
        let role = role_from_member_metadata(&m.metadata);
        if role.is_external_like() {
            has_external_like_members = true;
        }
        if role == crate::model::MemberRole::Master {
            if let Some(prev) = master_id.as_ref() {
                warnings.push(format!("multiple masters found: {}, {}", prev, m.id));
            } else {
                master_id = Some(m.id.clone());
                master_network = m.network.clone();
            }
        }
        members.push(m);
        Ok::<EtcdPrefixScanAction, anyhow::Error>(EtcdPrefixScanAction::Continue)
    })
    .await
    .with_context(|| format!("etcd scan prefix: {}", cluster_prefix))?;
    if members.is_empty() {
        warnings.push(format!(
            "no members found under etcd prefix: {}",
            cluster_prefix
        ));
    }

    let mut member_ext: HashMap<String, BTreeMap<String, String>> = HashMap::new();
    match scan_etcd_prefix_paginated(&mut etcd, &meta_prefix, |key, value| {
        let key = String::from_utf8_lossy(key).to_string();
        let value = String::from_utf8_lossy(value).to_string();
        if !key.starts_with(&meta_prefix) {
            return Ok::<EtcdPrefixScanAction, anyhow::Error>(EtcdPrefixScanAction::Continue);
        }
        let rest = &key[meta_prefix.len()..];
        let Some((member_id, k)) = rest.split_once('/') else {
            return Ok::<EtcdPrefixScanAction, anyhow::Error>(EtcdPrefixScanAction::Continue);
        };
        member_ext
            .entry(member_id.to_string())
            .or_default()
            .insert(k.to_string(), value);
        Ok::<EtcdPrefixScanAction, anyhow::Error>(EtcdPrefixScanAction::Continue)
    })
    .await
    {
        Ok(()) => {}
        Err(e) => {
            warnings.push(format!(
                "etcd scan prefix failed (member ext metadata): {} (err={})",
                meta_prefix, e
            ));
        }
    }
    overlay_persistent_owner_rdma_control(
        &mut etcd,
        &cfg.cluster_name,
        &mut warnings,
        &mut member_ext,
    )
    .await;

    // Share-group index published by fluxon_commu (ClusterManager::set_self_share_group_binding):
    // - Allows prefix query by owner generation to list all members (owner/external-like) that
    //   attach to it.
    // - Fluxon CLI uses it to map external-like member_id -> owner_id for virtual-node grouping.
    let share_group_owner_prefix = format!("/fluxon_commu_share_group/{}/owner/", cfg.cluster_name);
    let mut owner_by_member_id: HashMap<String, String> = HashMap::new();
    let mut owner_start_time_by_member_id: HashMap<String, i64> = HashMap::new();
    if has_external_like_members {
        match scan_etcd_prefix_paginated(&mut etcd, &share_group_owner_prefix, |key, _value| {
            let key = String::from_utf8_lossy(key).to_string();
            if let Some((member_id, owner_id, owner_start_time)) =
                try_parse_share_group_owner_by_member_id(&share_group_owner_prefix, &key)
            {
                let replace = owner_start_time_by_member_id
                    .get(&member_id)
                    .is_none_or(|current| owner_start_time >= *current);
                if replace {
                    owner_start_time_by_member_id.insert(member_id.clone(), owner_start_time);
                    owner_by_member_id.insert(member_id, owner_id);
                }
            }
            Ok::<EtcdPrefixScanAction, anyhow::Error>(EtcdPrefixScanAction::Continue)
        })
        .await
        {
            Ok(()) => {}
            Err(e) => {
                warnings.push(format!(
                    "etcd scan prefix failed (share-group index): {} (err={})",
                    share_group_owner_prefix, e
                ));
            }
        }
    }

    let prom = PromClient::new_with_query_time(cfg.prometheus_base_url.clone(), prom_query_time_s);
    let prom_maps = crate::prom::collect_prom_snapshot(&prom, &mut warnings).await;

    let mut peer_pairs: std::collections::BTreeSet<(String, String)> =
        std::collections::BTreeSet::new();
    peer_pairs.extend(
        prom_maps
            .kv_peer_network_tx_mbps_by_node_peer
            .keys()
            .cloned(),
    );
    peer_pairs.extend(
        prom_maps
            .kv_peer_network_rx_mbps_by_node_peer
            .keys()
            .cloned(),
    );
    let mut kv_peer_network: Vec<crate::model::KvPeerNetworkRateSnapshot> =
        Vec::with_capacity(peer_pairs.len());
    for (node, peer) in peer_pairs {
        let tx_mbps = prom_maps
            .kv_peer_network_tx_mbps_by_node_peer
            .get(&(node.clone(), peer.clone()))
            .copied();
        let rx_mbps = prom_maps
            .kv_peer_network_rx_mbps_by_node_peer
            .get(&(node.clone(), peer.clone()))
            .copied();
        kv_peer_network.push(crate::model::KvPeerNetworkRateSnapshot {
            node,
            peer,
            tx_mbps,
            rx_mbps,
        });
    }
    let mut rdma_netdev_pairs: std::collections::BTreeSet<(String, String)> =
        std::collections::BTreeSet::new();
    rdma_netdev_pairs.extend(
        prom_maps
            .node_network_tx_mbps_by_node_device
            .keys()
            .cloned(),
    );
    rdma_netdev_pairs.extend(
        prom_maps
            .node_network_rx_mbps_by_node_device
            .keys()
            .cloned(),
    );
    let mut rdma_netdev_network: Vec<RdmaNetdevRateSnapshot> =
        Vec::with_capacity(rdma_netdev_pairs.len());
    for (node, netdev) in rdma_netdev_pairs {
        let tx_mbps = prom_maps
            .node_network_tx_mbps_by_node_device
            .get(&(node.clone(), netdev.clone()))
            .copied();
        let rx_mbps = prom_maps
            .node_network_rx_mbps_by_node_device
            .get(&(node.clone(), netdev.clone()))
            .copied();
        rdma_netdev_network.push(RdmaNetdevRateSnapshot {
            node,
            netdev,
            tx_mbps,
            rx_mbps,
        });
    }

    let mut mount_triples: std::collections::BTreeSet<(
        String,
        fluxon_observability::types::FsMountKind,
        String,
        String,
    )> = std::collections::BTreeSet::new();
    mount_triples.extend(
        prom_maps
            .fs_mount_fs_used_bytes_by_node_kind_mountpoint_target
            .keys()
            .cloned(),
    );
    mount_triples.extend(
        prom_maps
            .fs_mount_fs_total_bytes_by_node_kind_mountpoint_target
            .keys()
            .cloned(),
    );
    let mut fs_mount_fs: Vec<crate::model::FsMountFsSnapshot> =
        Vec::with_capacity(mount_triples.len());
    for (node, kind, mountpoint_dir_abs, target_dir_abs) in mount_triples {
        let used_bytes = prom_maps
            .fs_mount_fs_used_bytes_by_node_kind_mountpoint_target
            .get(&(
                node.clone(),
                kind,
                mountpoint_dir_abs.clone(),
                target_dir_abs.clone(),
            ))
            .copied();
        let total_bytes = prom_maps
            .fs_mount_fs_total_bytes_by_node_kind_mountpoint_target
            .get(&(
                node.clone(),
                kind,
                mountpoint_dir_abs.clone(),
                target_dir_abs.clone(),
            ))
            .copied();
        fs_mount_fs.push(crate::model::FsMountFsSnapshot {
            node,
            mount_kind: kind.into(),
            target_dir_abs,
            mountpoint_dir_abs,
            used_bytes,
            total_bytes,
        });
    }
    let mut shm_file_triples: std::collections::BTreeSet<(String, String, String)> =
        std::collections::BTreeSet::new();
    shm_file_triples.extend(
        prom_maps
            .shm_file_size_bytes_by_node_dir_file
            .keys()
            .cloned(),
    );
    shm_file_triples.extend(
        prom_maps
            .shm_file_allocated_bytes_by_node_dir_file
            .keys()
            .cloned(),
    );
    let mut shm_files: Vec<crate::model::ShmFileSnapshot> =
        Vec::with_capacity(shm_file_triples.len());
    for (node, shm_dir_abs, file_path_abs) in shm_file_triples {
        let logical_size_bytes = prom_maps
            .shm_file_size_bytes_by_node_dir_file
            .get(&(node.clone(), shm_dir_abs.clone(), file_path_abs.clone()))
            .copied();
        let allocated_bytes = prom_maps
            .shm_file_allocated_bytes_by_node_dir_file
            .get(&(node.clone(), shm_dir_abs.clone(), file_path_abs.clone()))
            .copied();
        shm_files.push(crate::model::ShmFileSnapshot {
            node,
            shm_dir_abs,
            file_path_abs,
            logical_size_bytes,
            allocated_bytes,
        });
    }

    fn sum_segment_bytes_by_node(
        by_node_device: &HashMap<(String, String), f64>,
    ) -> HashMap<String, f64> {
        let mut out: HashMap<String, f64> = HashMap::new();
        for ((node, _device), v) in by_node_device {
            *out.entry(node.clone()).or_insert(0.0) += *v;
        }
        out
    }

    let seg_capacity_bytes_by_node =
        sum_segment_bytes_by_node(&prom_maps.seg_capacity_bytes_by_node_device);
    let seg_used_bytes_by_node =
        sum_segment_bytes_by_node(&prom_maps.seg_used_bytes_by_node_device);

    async fn prom_scalar_best_effort(
        warnings: &mut Vec<String>,
        prom: &PromClient,
        label: &str,
        promql: &str,
    ) -> Option<f64> {
        match query_scalar_f64(prom, promql).await {
            Ok(v) => v,
            Err(e) => {
                warnings.push(format!("prometheus query failed ({label}): {e}"));
                None
            }
        }
    }

    let total_put_rps = prom_scalar_best_effort(
        &mut warnings,
        &prom,
        "total_put_rps",
        "sum(sum_over_time(kv_op_end_event{op=\"put\",status=\"success\"}[1s]))",
    )
    .await;
    let total_get_rps = prom_scalar_best_effort(
        &mut warnings,
        &prom,
        "total_get_rps",
        "sum(sum_over_time(kv_op_end_event{op=\"get\",status=~\"hit|success\"}[1s]))",
    )
    .await;
    let total_put_bps = prom_scalar_best_effort(
        &mut warnings,
        &prom,
        "total_put_bps",
        "sum(sum_over_time(kv_op_end_bytes{op=\"put\",status=\"success\"}[1s]))",
    )
    .await;
    let total_get_bps = prom_scalar_best_effort(
        &mut warnings,
        &prom,
        "total_get_bps",
        "sum(sum_over_time(kv_op_end_bytes{op=\"get\",status=~\"hit|success\"}[1s]))",
    )
    .await;

    let total_put_latency_mean_us = prom_scalar_best_effort(
        &mut warnings,
        &prom,
        "total_put_latency_mean_us",
        "avg(kv_operation_latency_stat_microseconds{metric=\"put_whole\",stat=\"mean\"})",
    )
    .await;
    let total_put_latency_p95_us = prom_scalar_best_effort(
        &mut warnings,
        &prom,
        "total_put_latency_p95_us",
        "max(kv_operation_latency_stat_microseconds{metric=\"put_whole\",stat=\"p95\"})",
    )
    .await;
    let total_put_latency_p99_us = prom_scalar_best_effort(
        &mut warnings,
        &prom,
        "total_put_latency_p99_us",
        "max(kv_operation_latency_stat_microseconds{metric=\"put_whole\",stat=\"p99\"})",
    )
    .await;

    let total_get_latency_mean_us = prom_scalar_best_effort(
        &mut warnings,
        &prom,
        "total_get_latency_mean_us",
        "avg(kv_operation_latency_stat_microseconds{metric=\"get_whole\",stat=\"mean\"})",
    )
    .await;
    let total_get_latency_p95_us = prom_scalar_best_effort(
        &mut warnings,
        &prom,
        "total_get_latency_p95_us",
        "max(kv_operation_latency_stat_microseconds{metric=\"get_whole\",stat=\"p95\"})",
    )
    .await;
    let total_get_latency_p99_us = prom_scalar_best_effort(
        &mut warnings,
        &prom,
        "total_get_latency_p99_us",
        "max(kv_operation_latency_stat_microseconds{metric=\"get_whole\",stat=\"p99\"})",
    )
    .await;

    let mut nodes_map: BTreeMap<String, NodeSnapshot> = BTreeMap::new();
    for m in members {
        let role = role_from_member_metadata(&m.metadata);
        let is_p2p_relay = matches!(
            m.metadata.get("p2p_relay").map(|v| v.as_str()),
            Some("true")
        );
        let is_side_transfer_worker = matches!(
            m.metadata.get("side_transfer_worker").map(|v| v.as_str()),
            Some("true")
        );
        let ext = member_ext.get(&m.id);
        let hostname = ext
            .and_then(|m| m.get(META_KEY_HOSTNAME).cloned())
            .or_else(|| m.metadata.get(META_KEY_HOSTNAME).cloned());
        let accessible_ip = match ext.and_then(|m| m.get("accessible_ip")) {
            Some(v) => match serde_json::from_str::<AccessibleIpInfo>(v) {
                Ok(info) => {
                    if info.node_start_time != m.node_start_time {
                        warnings.push(format!(
                            "stale member ext accessible_ip for member_id={} (ext.node_start_time={}, base.node_start_time={})",
                            m.id, info.node_start_time, m.node_start_time
                        ));
                        None
                    } else if info.ip.is_empty() {
                        warnings.push(format!(
                            "empty member ext accessible_ip for member_id={} (ext.node_start_time={})",
                            m.id, info.node_start_time
                        ));
                        None
                    } else {
                        Some(info.ip)
                    }
                }
                Err(e) => {
                    warnings.push(format!(
                        "invalid member ext accessible_ip for member_id={} (expected JSON {{ip,node_start_time}}): {} (err={})",
                        m.id, v, e
                    ));
                    None
                }
            },
            None => None,
        };
        let shared_mem_dir = ext
            .and_then(|m| m.get("shared_mem_dir").cloned())
            .or_else(|| m.metadata.get("shared_mem_dir").cloned());
        let pid = ext
            .and_then(|m| m.get(META_KEY_PID))
            .and_then(|s| s.parse::<u32>().ok())
            .or_else(|| {
                m.metadata
                    .get(META_KEY_PID)
                    .and_then(|s| s.parse::<u32>().ok())
            });
        let cmd = ext
            .and_then(|m| m.get(META_KEY_CMD).cloned())
            .or_else(|| m.metadata.get(META_KEY_CMD).cloned());
        let rdma_control =
            parse_member_rdma_control(&mut warnings, &m.id, m.node_start_time, &m.metadata, ext);
        let rdma_runtime = parse_member_rdma_runtime(&mut warnings, &m.id, m.node_start_time, ext);

        let node_key = match role {
            role if role == crate::model::MemberRole::OwnerClient || role.is_external_like() => {
                node_key_for_member(
                    &mut warnings,
                    &owner_by_member_id,
                    role,
                    &m.id,
                    shared_mem_dir.as_deref(),
                )
            }
            _ => shared_mem_dir
                .clone()
                .or_else(|| accessible_ip.clone())
                .or_else(|| m.addresses.get(0).cloned())
                .unwrap_or_else(|| m.id.clone()),
        };

        let member_id = m.id.clone();
        let member_snapshot = MemberSnapshot {
            member_id: member_id.clone(),
            role,
            is_p2p_relay,
            is_side_transfer_worker,
            node_start_time: m.node_start_time,
            hostname: hostname.clone(),
            accessible_ip: accessible_ip.clone(),
            shared_mem_dir: shared_mem_dir.clone(),
            p2p_listen_port: m.port,
            rdma_runtime_reported: rdma_runtime.is_some(),
            rdma_probe_error: rdma_runtime
                .as_ref()
                .and_then(|runtime| runtime.probe_error.clone()),
            rdma_devices: build_member_rdma_devices(rdma_control.as_ref(), rdma_runtime.as_ref()),
            rdma_ports: build_member_rdma_ports(rdma_control.as_ref(), rdma_runtime.as_ref()),
            rdma_transfer_engine: rdma_runtime
                .as_ref()
                .map(|runtime| runtime.transfer_engine.clone()),
            pid,
            cmd,
            sub_cluster: m.sub_cluster.clone(),
            product_uuid: m.metadata.get(META_KEY_PRODUCT_UUID).cloned(),

            node_cpu_usage_percent: prom_maps.node_cpu_usage_percent.get(&member_id).copied(),
            node_cpu_logical_cores: prom_maps.node_cpu_logical_cores.get(&member_id).copied(),
            node_memory_usage_bytes: prom_maps.node_memory_usage_bytes.get(&member_id).copied(),
            node_memory_total_bytes: prom_maps.node_memory_total_bytes.get(&member_id).copied(),
            container_memory_usage_bytes: prom_maps
                .container_memory_usage_bytes
                .get(&member_id)
                .copied(),
            container_memory_limit_bytes: prom_maps
                .container_memory_limit_bytes
                .get(&member_id)
                .copied(),
            process_resident_memory_bytes: prom_maps
                .process_resident_memory_bytes
                .get(&member_id)
                .copied(),
            process_cpu_usage_percent: prom_maps.process_cpu_usage_percent.get(&member_id).copied(),
            tokio_num_workers: prom_maps.tokio_num_workers.get(&member_id).copied(),
            tokio_alive_tasks: prom_maps.tokio_alive_tasks.get(&member_id).copied(),
            tokio_global_queue_depth: prom_maps.tokio_global_queue_depth.get(&member_id).copied(),
            tokio_busy_percent: prom_maps.tokio_busy_percent.get(&member_id).copied(),
            tokio_max_worker_busy_percent: prom_maps
                .tokio_max_worker_busy_percent
                .get(&member_id)
                .copied(),
            tokio_park_unpark_rate_hz: prom_maps.tokio_park_unpark_rate_hz.get(&member_id).copied(),
            process_net_tx_mbps: prom_maps.process_network_tx_mbps.get(&member_id).copied(),
            process_net_rx_mbps: prom_maps.process_network_rx_mbps.get(&member_id).copied(),

            kv_put_rps: prom_maps.put_rps.get(&member_id).copied(),
            kv_get_rps: prom_maps.get_rps.get(&member_id).copied(),
            kv_put_bps: prom_maps.put_bps.get(&member_id).copied(),
            kv_get_bps: prom_maps.get_bps.get(&member_id).copied(),

            kv_put_latency_mean_us: prom_maps.put_latency_mean_us.get(&member_id).copied(),
            kv_put_latency_p95_us: prom_maps.put_latency_p95_us.get(&member_id).copied(),
            kv_put_latency_p99_us: prom_maps.put_latency_p99_us.get(&member_id).copied(),
            kv_get_latency_mean_us: prom_maps.get_latency_mean_us.get(&member_id).copied(),
            kv_get_latency_p95_us: prom_maps.get_latency_p95_us.get(&member_id).copied(),
            kv_get_latency_p99_us: prom_maps.get_latency_p99_us.get(&member_id).copied(),

            seg_capacity_bytes: if role == crate::model::MemberRole::OwnerClient {
                seg_capacity_bytes_by_node.get(&member_id).copied()
            } else {
                None
            },
            seg_used_bytes: if role == crate::model::MemberRole::OwnerClient {
                seg_used_bytes_by_node.get(&member_id).copied()
            } else {
                None
            },

            fs_read_rps: prom_maps.fs_read_rps.get(&member_id).copied(),
            fs_write_rps: prom_maps.fs_write_rps.get(&member_id).copied(),
        };

        let node_entry = nodes_map
            .entry(node_key.clone())
            .or_insert_with(|| NodeSnapshot {
                node_key: node_key.clone(),
                hostname: hostname.clone(),
                accessible_ip: accessible_ip.clone(),
                shared_mem_dir: shared_mem_dir.clone(),
                is_p2p_relay: false,
                node_cpu_usage_percent: None,
                node_cpu_logical_cores: None,
                node_memory_usage_bytes: None,
                node_memory_total_bytes: None,
                container_memory_usage_bytes: None,
                container_memory_limit_bytes: None,
                members: Vec::new(),
                segment_devices: Vec::new(),
            });
        if is_p2p_relay {
            node_entry.is_p2p_relay = true;
        }
        if node_entry.hostname.is_none() {
            node_entry.hostname = hostname.clone();
        }
        if node_entry.accessible_ip.is_none() {
            node_entry.accessible_ip = accessible_ip.clone();
        }
        if node_entry.shared_mem_dir.is_none() {
            node_entry.shared_mem_dir = shared_mem_dir.clone();
        }
        node_entry.members.push(member_snapshot);
    }

    let mut nodes: Vec<NodeSnapshot> = nodes_map.into_values().collect();
    for n in &mut nodes {
        n.members.sort_by(|a, b| a.member_id.cmp(&b.member_id));
        n.node_cpu_usage_percent = n
            .members
            .iter()
            .filter_map(|m| m.node_cpu_usage_percent)
            .fold(None, |acc, v| Some(acc.map(|a| a.max(v)).unwrap_or(v)));
        n.node_cpu_logical_cores = n
            .members
            .iter()
            .filter_map(|m| m.node_cpu_logical_cores)
            .fold(None, |acc, v| Some(acc.map(|a| a.max(v)).unwrap_or(v)));
        n.node_memory_usage_bytes = n
            .members
            .iter()
            .filter_map(|m| m.node_memory_usage_bytes)
            .fold(None, |acc, v| Some(acc.map(|a| a.max(v)).unwrap_or(v)));
        n.node_memory_total_bytes = n
            .members
            .iter()
            .filter_map(|m| m.node_memory_total_bytes)
            .fold(None, |acc, v| Some(acc.map(|a| a.max(v)).unwrap_or(v)));
        n.container_memory_usage_bytes = n
            .members
            .iter()
            .filter_map(|m| m.container_memory_usage_bytes)
            .fold(None, |acc, v| Some(acc.map(|a| a.max(v)).unwrap_or(v)));
        n.container_memory_limit_bytes = n
            .members
            .iter()
            .filter_map(|m| m.container_memory_limit_bytes)
            .fold(None, |acc, v| Some(acc.map(|a| a.max(v)).unwrap_or(v)));

        let mut owner_ids: Vec<String> = n
            .members
            .iter()
            .filter(|m| m.role == crate::model::MemberRole::OwnerClient)
            .map(|m| m.member_id.clone())
            .collect();
        owner_ids.sort();
        owner_ids.dedup();
        if owner_ids.len() > 1 {
            warnings.push(format!(
                "multiple owner_client instances under one node_key (expected 0/1): node_key={} owners={}",
                n.node_key,
                owner_ids.join(",")
            ));
        }
        let Some(owner_id) = owner_ids.into_iter().next() else {
            continue;
        };

        let mut devices: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
        for ((node, device), _v) in &prom_maps.seg_capacity_bytes_by_node_device {
            if node == &owner_id {
                devices.insert(device.clone());
            }
        }
        for ((node, device), _v) in &prom_maps.seg_used_bytes_by_node_device {
            if node == &owner_id {
                devices.insert(device.clone());
            }
        }

        if devices.is_empty() {
            warnings.push(format!(
                "missing segment metrics for owner_client: owner_id={} (expected Prom series: kvcache_segment_*_bytes{{node=\"{}\",device}})",
                owner_id, owner_id
            ));
            continue;
        }

        let mut missing_cap: Vec<String> = Vec::new();
        let mut missing_used: Vec<String> = Vec::new();
        let mut cap_zero: Vec<String> = Vec::new();
        let mut used_gt_cap: Vec<String> = Vec::new();
        for device in devices {
            let cap = prom_maps
                .seg_capacity_bytes_by_node_device
                .get(&(owner_id.clone(), device.clone()))
                .copied();
            let used = prom_maps
                .seg_used_bytes_by_node_device
                .get(&(owner_id.clone(), device.clone()))
                .copied();
            if cap.is_none() {
                missing_cap.push(device.clone());
            }
            if used.is_none() {
                missing_used.push(device.clone());
            }
            if let Some(cap) = cap {
                if cap == 0.0 {
                    cap_zero.push(device.clone());
                }
            }
            if let (Some(used), Some(cap)) = (used, cap) {
                if used > cap {
                    used_gt_cap.push(device.clone());
                }
            }
            n.segment_devices.push(crate::model::SegmentDeviceSnapshot {
                device,
                seg_capacity_bytes: cap,
                seg_used_bytes: used,
            });
        }
        n.segment_devices.sort_by(|a, b| a.device.cmp(&b.device));

        if !missing_cap.is_empty() {
            warnings.push(format!(
                "segment metrics missing capacity series for owner_id={} devices={}",
                owner_id,
                missing_cap.join(",")
            ));
        }
        if !missing_used.is_empty() {
            warnings.push(format!(
                "segment metrics missing used series for owner_id={} devices={}",
                owner_id,
                missing_used.join(",")
            ));
        }
        if !cap_zero.is_empty() {
            warnings.push(format!(
                "segment metrics has cap=0 for owner_id={} devices={}",
                owner_id,
                cap_zero.join(",")
            ));
        }
        if !used_gt_cap.is_empty() {
            warnings.push(format!(
                "segment metrics has used>cap for owner_id={} devices={}",
                owner_id,
                used_gt_cap.join(",")
            ));
        }
    }

    let (
        kv_topology_owner_external_max,
        kv_topology_machine_external_max,
        kv_topology_sub_cluster_owner_owner_max,
    ) = if cfg.member_kind == MemberKind::Kv {
        collect_kv_topology_history_max(&prom, &nodes, &mut warnings).await
    } else {
        (Vec::new(), Vec::new(), Vec::new())
    };
    let transfer_engine_edges = if cfg.member_kind == MemberKind::Kv {
        load_transfer_engine_edges_for_cluster(&cfg.etcd_endpoints, &cfg.cluster_name).await?
    } else {
        Vec::new()
    };

    Ok(ClusterSnapshot {
        cluster_name: cfg.cluster_name.clone(),
        member_kind: cfg.member_kind,
        etcd_endpoints: cfg.etcd_endpoints.clone(),
        prometheus_base_url: cfg.prometheus_base_url.clone(),
        warnings,
        visible_member_roles: None,
        master_id,
        master_network,
        transfer_engine_edges,
        kv_peer_network,
        rdma_netdev_network,
        fs_mount_fs,
        shm_files,
        fs_export_registry: Vec::new(),
        fs_mount_registry: Vec::new(),
        kv_topology_owner_external_max,
        kv_topology_machine_external_max,
        kv_topology_sub_cluster_owner_owner_max,
        nodes,
        mq: None,
        total_put_rps,
        total_get_rps,
        total_put_bps,
        total_get_bps,
        total_put_latency_mean_us,
        total_put_latency_p95_us,
        total_put_latency_p99_us,
        total_get_latency_mean_us,
        total_get_latency_p95_us,
        total_get_latency_p99_us,
    })
}

async fn collect_kv_topology_history_max(
    prom: &PromClient,
    nodes: &[NodeSnapshot],
    warnings: &mut Vec<String>,
) -> (
    Vec<crate::model::KvTopologyOwnerExternalMaxSnapshot>,
    Vec<crate::model::KvTopologyMachineExternalMaxSnapshot>,
    Vec<crate::model::KvTopologySubClusterOwnerOwnerMaxSnapshot>,
) {
    use fluxon_observability::keys::{
        PROM_LABEL_NODE, PROM_LABEL_PEER, PROM_LABEL_ROLE, PROM_METRIC_KV_PEER_NETWORK_BYTES_TOTAL,
    };

    const SUB_CLUSTER_MISSING_LABEL: &str = "(missing)";
    // English note:
    // - "history max" is the maximum value of the target Mbps expression over a recent window.
    // - We intentionally compute the max client-side via `query_range` instead of using PromQL
    //   subquery `max_over_time((expr)[window:step])`, because GreptimeDB's PromQL compatibility
    //   does not reliably support subquery semantics for derived expressions.
    // - KV counters are flushed on a coarse interval, so a too-short `rate()` window can produce an
    //   empty vector (not enough samples). Keep `RATE_WINDOW` >= the flush interval.
    // 30d "1 month" history window (user-specified) for topology max.
    const HISTORY_WINDOW_S: f64 = 30.0 * 24.0 * 3600.0;
    const HISTORY_STEP: &str = "30s";
    const RATE_WINDOW: &str = "2m";
    const OWNER_ROLE_LABEL_VALUE: &str = "client";

    fn normalized_sub_cluster(sc: Option<&str>) -> String {
        match sc.map(|s| s.trim()).filter(|s| !s.is_empty()) {
            Some(s) => s.to_string(),
            None => SUB_CLUSTER_MISSING_LABEL.to_string(),
        }
    }

    fn prom_str_literal(s: &str) -> String {
        // PromQL uses double-quoted strings; escape a minimal set of characters.
        let mut out = String::with_capacity(s.len() + 8);
        for ch in s.chars() {
            match ch {
                '\\' => out.push_str("\\\\"),
                '"' => out.push_str("\\\""),
                '\n' => out.push_str("\\n"),
                '\r' => out.push_str("\\r"),
                '\t' => out.push_str("\\t"),
                _ => out.push(ch),
            }
        }
        out
    }

    fn prom_regex_escape_literal(s: &str) -> String {
        // RE2 syntax (Prometheus regex). We build a "union of exact literals", so escape meta chars.
        let mut out = String::with_capacity(s.len() + 8);
        for ch in s.chars() {
            match ch {
                '\\' | '.' | '+' | '*' | '?' | '|' | '{' | '}' | '(' | ')' | '[' | ']' | '^'
                | '$' => {
                    out.push('\\');
                    out.push(ch);
                }
                _ => out.push(ch),
            }
        }
        out
    }

    fn prom_regex_union_exact(ids: &[String]) -> Option<String> {
        let mut parts: Vec<String> = Vec::new();
        for id in ids {
            let t = id.trim();
            if t.is_empty() {
                continue;
            }
            parts.push(prom_regex_escape_literal(t));
        }
        parts.sort();
        parts.dedup();
        if parts.is_empty() {
            return None;
        }
        if parts.len() == 1 {
            return Some(format!("^{}$", parts[0]));
        }
        Some(format!("^(?:{})$", parts.join("|")))
    }

    #[derive(Clone)]
    struct OwnerRec {
        owner_id: String,
        sub_cluster: String,
        group_key: String,
        externals: Vec<String>,
    }

    // Extract owner + attached external-like mapping from the virtual node membership snapshot.
    // Keep this consistent with the topology builder (web_renderer::build_kv_topology_view).
    let mut owners: Vec<OwnerRec> = Vec::new();
    for n in nodes {
        let mut owner_ms: Vec<&MemberSnapshot> = n
            .members
            .iter()
            .filter(|m| m.role == crate::model::MemberRole::OwnerClient)
            .collect();
        owner_ms.sort_by(|a, b| a.member_id.cmp(&b.member_id));

        let mut external_ms: Vec<&MemberSnapshot> = n
            .members
            .iter()
            .filter(|m| m.role.is_external_like())
            .collect();
        external_ms.sort_by(|a, b| a.member_id.cmp(&b.member_id));

        if owner_ms.len() == 1 {
            let owner = owner_ms[0];
            let sc = normalized_sub_cluster(owner.sub_cluster.as_deref());
            let group_key = owner.topology_nic_group_key();
            let mut external_ids: Vec<String> =
                external_ms.iter().map(|m| m.member_id.clone()).collect();
            external_ids.sort();
            external_ids.dedup();
            owners.push(OwnerRec {
                owner_id: owner.member_id.clone(),
                sub_cluster: sc,
                group_key,
                externals: external_ids,
            });
            continue;
        }

        // 0 or >1 owners: keep owners, but do not attach externals to avoid incorrect inference.
        for owner in owner_ms {
            let sc = normalized_sub_cluster(owner.sub_cluster.as_deref());
            let group_key = owner.topology_nic_group_key();
            owners.push(OwnerRec {
                owner_id: owner.member_id.clone(),
                sub_cluster: sc,
                group_key,
                externals: Vec::new(),
            });
        }
    }

    if owners.is_empty() {
        return (Vec::new(), Vec::new(), Vec::new());
    }

    let mut owners_by_sc_group: HashMap<(String, String), Vec<String>> = HashMap::new();
    for o in &owners {
        owners_by_sc_group
            .entry((o.sub_cluster.clone(), o.group_key.clone()))
            .or_default()
            .push(o.owner_id.clone());
    }
    for (_k, v) in owners_by_sc_group.iter_mut() {
        v.sort();
        v.dedup();
    }

    let end_s = match prom.effective_query_time_s() {
        Ok(v) => v,
        Err(e) => {
            warnings.push(format!(
                "topology history max: resolve query time failed: err={e}"
            ));
            return (Vec::new(), Vec::new(), Vec::new());
        }
    };
    let start_s = (end_s - HISTORY_WINDOW_S).max(0.0);

    async fn query_range_max_mbps(
        prom: &PromClient,
        warnings: &mut Vec<String>,
        label: &str,
        promql: &str,
        start_s: f64,
        end_s: f64,
        step: &str,
    ) -> Option<f64> {
        let series = match prom.query_range(promql, start_s, end_s, step).await {
            Ok(v) => v,
            Err(e) => {
                warnings.push(format!(
                    "topology history max: prom query_range failed: label={label} err={e} promql={promql}"
                ));
                return None;
            }
        };
        let mut out: Option<f64> = None;
        for s in series {
            for (_ts, v_s) in s.values {
                let Ok(v) = v_s.parse::<f64>() else {
                    continue;
                };
                if !v.is_finite() {
                    continue;
                }
                out = Some(match out {
                    Some(m) => m.max(v),
                    None => v,
                });
            }
        }
        out
    }

    // Owner history max: owner -> (sum(owner->externals)) max over the window.
    let mut out_owner: Vec<crate::model::KvTopologyOwnerExternalMaxSnapshot> = Vec::new();
    for o in &owners {
        let Some(ext_re) = prom_regex_union_exact(&o.externals) else {
            continue;
        };
        let node = prom_str_literal(o.owner_id.as_str());
        let peer_re = prom_str_literal(ext_re.as_str());

        let sel_common = format!(
            "{PROM_LABEL_ROLE}=\"{OWNER_ROLE_LABEL_VALUE}\",{PROM_LABEL_NODE}=\"{node}\",{PROM_LABEL_PEER}=~\"{peer_re}\""
        );

        let tx_inner = format!(
            "sum(rate({PROM_METRIC_KV_PEER_NETWORK_BYTES_TOTAL}{{{sel_common},direction=\"tx\"}}[{RATE_WINDOW}])) * 8 / 1000000"
        );
        let rx_inner = format!(
            "sum(rate({PROM_METRIC_KV_PEER_NETWORK_BYTES_TOTAL}{{{sel_common},direction=\"rx\"}}[{RATE_WINDOW}])) * 8 / 1000000"
        );

        let tx_mbps_max = query_range_max_mbps(
            prom,
            warnings,
            "owner_external_tx",
            &tx_inner,
            start_s,
            end_s,
            HISTORY_STEP,
        )
        .await;
        let rx_mbps_max = query_range_max_mbps(
            prom,
            warnings,
            "owner_external_rx",
            &rx_inner,
            start_s,
            end_s,
            HISTORY_STEP,
        )
        .await;
        if tx_mbps_max.is_none() && rx_mbps_max.is_none() {
            continue;
        }
        out_owner.push(crate::model::KvTopologyOwnerExternalMaxSnapshot {
            owner_id: o.owner_id.clone(),
            tx_mbps_max,
            rx_mbps_max,
        });
    }
    out_owner.sort_by(|a, b| a.owner_id.cmp(&b.owner_id));

    // NIC-group history max: sum(owner->external) max over the window.
    let mut out_machine: Vec<crate::model::KvTopologyMachineExternalMaxSnapshot> = Vec::new();
    for ((sc, group_key), owner_ids) in &owners_by_sc_group {
        let Some(owner_re) = prom_regex_union_exact(owner_ids) else {
            continue;
        };
        let mut exts: Vec<String> = Vec::new();
        for o in &owners {
            if &o.sub_cluster == sc && &o.group_key == group_key {
                exts.extend(o.externals.iter().cloned());
            }
        }
        let Some(ext_re) = prom_regex_union_exact(&exts) else {
            continue;
        };

        let node_re = prom_str_literal(owner_re.as_str());
        let peer_re = prom_str_literal(ext_re.as_str());

        let sel_common = format!(
            "{PROM_LABEL_ROLE}=\"{OWNER_ROLE_LABEL_VALUE}\",{PROM_LABEL_NODE}=~\"{node_re}\",{PROM_LABEL_PEER}=~\"{peer_re}\""
        );

        let tx_inner = format!(
            "sum(rate({PROM_METRIC_KV_PEER_NETWORK_BYTES_TOTAL}{{{sel_common},direction=\"tx\"}}[{RATE_WINDOW}])) * 8 / 1000000"
        );
        let rx_inner = format!(
            "sum(rate({PROM_METRIC_KV_PEER_NETWORK_BYTES_TOTAL}{{{sel_common},direction=\"rx\"}}[{RATE_WINDOW}])) * 8 / 1000000"
        );

        let tx_mbps_max = query_range_max_mbps(
            prom,
            warnings,
            "machine_external_tx",
            &tx_inner,
            start_s,
            end_s,
            HISTORY_STEP,
        )
        .await;
        let rx_mbps_max = query_range_max_mbps(
            prom,
            warnings,
            "machine_external_rx",
            &rx_inner,
            start_s,
            end_s,
            HISTORY_STEP,
        )
        .await;

        if tx_mbps_max.is_none() && rx_mbps_max.is_none() {
            continue;
        }

        out_machine.push(crate::model::KvTopologyMachineExternalMaxSnapshot {
            sub_cluster: sc.clone(),
            machine_key: group_key.clone(),
            tx_mbps_max,
            rx_mbps_max,
        });
    }
    out_machine.sort_by(|a, b| {
        let c = a.sub_cluster.cmp(&b.sub_cluster);
        if c != std::cmp::Ordering::Equal {
            return c;
        }
        a.machine_key.cmp(&b.machine_key)
    });

    // sub_cluster history max: sum(owner<->owner cross-group) max over the window.
    let mut out_sc: Vec<crate::model::KvTopologySubClusterOwnerOwnerMaxSnapshot> = Vec::new();
    let mut scs: Vec<String> = owners.iter().map(|o| o.sub_cluster.clone()).collect();
    scs.sort();
    scs.dedup();
    for sc in scs {
        let mut owner_ids_sc: Vec<String> = owners
            .iter()
            .filter(|o| o.sub_cluster == sc)
            .map(|o| o.owner_id.clone())
            .collect();
        owner_ids_sc.sort();
        owner_ids_sc.dedup();
        let Some(owner_re) = prom_regex_union_exact(&owner_ids_sc) else {
            continue;
        };
        let owner_re_lit = prom_str_literal(owner_re.as_str());

        let total_tx_bytes_s = format!(
            "sum(rate({PROM_METRIC_KV_PEER_NETWORK_BYTES_TOTAL}{{{PROM_LABEL_ROLE}=\"{OWNER_ROLE_LABEL_VALUE}\",direction=\"tx\",{PROM_LABEL_NODE}=~\"{owner_re_lit}\",{PROM_LABEL_PEER}=~\"{owner_re_lit}\"}}[{RATE_WINDOW}]))"
        );
        let total_rx_bytes_s = format!(
            "sum(rate({PROM_METRIC_KV_PEER_NETWORK_BYTES_TOTAL}{{{PROM_LABEL_ROLE}=\"{OWNER_ROLE_LABEL_VALUE}\",direction=\"rx\",{PROM_LABEL_NODE}=~\"{owner_re_lit}\",{PROM_LABEL_PEER}=~\"{owner_re_lit}\"}}[{RATE_WINDOW}]))"
        );

        // English note (PromQL empty-vector semantics):
        // - We compute cross-group owner<->owner traffic as: total(sc) - sum(same_group(sc)).
        // - `sum(rate(...))` returns an empty vector when the selector has no series at a given time.
        //   In PromQL, `A + B` becomes empty if either side is empty, which would incorrectly erase
        //   other machines' contributions.
        // - Wrap each per-machine term with `... or vector(0)` so "no internal traffic" == 0,
        //   while keeping `total_*` unwrapped so the whole query still returns empty when the
        //   sub_cluster has no data at all (so CLI can keep it as None instead of forcing 0).
        let mut same_tx_parts: Vec<String> = Vec::new();
        let mut same_rx_parts: Vec<String> = Vec::new();
        for ((sc2, _mk), owner_ids) in &owners_by_sc_group {
            if sc2 != &sc {
                continue;
            }
            let Some(re_m) = prom_regex_union_exact(owner_ids) else {
                continue;
            };
            let re_m_lit = prom_str_literal(re_m.as_str());
            same_tx_parts.push(format!(
                "(sum(rate({PROM_METRIC_KV_PEER_NETWORK_BYTES_TOTAL}{{{PROM_LABEL_ROLE}=\"{OWNER_ROLE_LABEL_VALUE}\",direction=\"tx\",{PROM_LABEL_NODE}=~\"{re_m_lit}\",{PROM_LABEL_PEER}=~\"{re_m_lit}\"}}[{RATE_WINDOW}])) or vector(0))"
            ));
            same_rx_parts.push(format!(
                "(sum(rate({PROM_METRIC_KV_PEER_NETWORK_BYTES_TOTAL}{{{PROM_LABEL_ROLE}=\"{OWNER_ROLE_LABEL_VALUE}\",direction=\"rx\",{PROM_LABEL_NODE}=~\"{re_m_lit}\",{PROM_LABEL_PEER}=~\"{re_m_lit}\"}}[{RATE_WINDOW}])) or vector(0))"
            ));
        }

        let same_tx_bytes_s = if same_tx_parts.is_empty() {
            "0".to_string()
        } else {
            same_tx_parts.join(" + ")
        };
        let same_rx_bytes_s = if same_rx_parts.is_empty() {
            "0".to_string()
        } else {
            same_rx_parts.join(" + ")
        };

        let cross_tx_mbps = format!("(({total_tx_bytes_s}) - ({same_tx_bytes_s})) * 8 / 1000000");
        let cross_rx_mbps = format!("(({total_rx_bytes_s}) - ({same_rx_bytes_s})) * 8 / 1000000");

        let tx_mbps_max = query_range_max_mbps(
            prom,
            warnings,
            "sub_cluster_owner_owner_tx",
            &cross_tx_mbps,
            start_s,
            end_s,
            HISTORY_STEP,
        )
        .await;
        let rx_mbps_max = query_range_max_mbps(
            prom,
            warnings,
            "sub_cluster_owner_owner_rx",
            &cross_rx_mbps,
            start_s,
            end_s,
            HISTORY_STEP,
        )
        .await;

        if tx_mbps_max.is_none() && rx_mbps_max.is_none() {
            continue;
        }

        out_sc.push(crate::model::KvTopologySubClusterOwnerOwnerMaxSnapshot {
            sub_cluster: sc,
            tx_mbps_max,
            rx_mbps_max,
        });
    }

    (out_owner, out_machine, out_sc)
}

async fn query_scalar_f64(prom: &PromClient, promql: &str) -> anyhow::Result<Option<f64>> {
    let v = prom.query_instant(promql).await?;
    if v.is_empty() {
        return Ok(None);
    }
    Ok(v[0].value_f64())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TuiKey {
    Up,
    Down,
    Left,
    Right,
    Enter,
    Tab,
    Refresh,
    FocusCluster,
    FocusKind,
    FocusView,
    Quit,
    Other,
}

struct RawModeGuard {
    stdin_fd: i32,
    orig: libc::termios,
}

impl Drop for RawModeGuard {
    fn drop(&mut self) {
        unsafe {
            let _ = libc::tcsetattr(self.stdin_fd, libc::TCSANOW, &self.orig);
        }
        let mut out = std::io::stdout().lock();
        let _ = write!(out, "\x1b[0m\x1b[?25h\x1b[?1049l");
        let _ = out.flush();
    }
}

fn enable_raw_mode() -> anyhow::Result<RawModeGuard> {
    if !std::io::stdin().is_terminal() || !std::io::stdout().is_terminal() {
        anyhow::bail!("interactive TUI requires a TTY (stdin/stdout)");
    }

    let stdin_fd = libc::STDIN_FILENO;
    let mut orig: libc::termios = unsafe { std::mem::zeroed() };
    let rc = unsafe { libc::tcgetattr(stdin_fd, &mut orig) };
    if rc != 0 {
        anyhow::bail!("tcgetattr failed (rc={})", rc);
    }

    let mut raw = orig;
    raw.c_lflag &= !(libc::ECHO | libc::ICANON);
    raw.c_cc[libc::VMIN] = 1;
    raw.c_cc[libc::VTIME] = 0;

    let rc = unsafe { libc::tcsetattr(stdin_fd, libc::TCSANOW, &raw) };
    if rc != 0 {
        anyhow::bail!("tcsetattr failed (rc={})", rc);
    }

    let mut out = std::io::stdout().lock();
    write!(out, "\x1b[?1049h\x1b[?25l")?;
    out.flush()?;

    Ok(RawModeGuard { stdin_fd, orig })
}

fn terminal_rows() -> anyhow::Result<usize> {
    let mut ws: libc::winsize = unsafe { std::mem::zeroed() };
    let rc = unsafe { libc::ioctl(libc::STDOUT_FILENO, libc::TIOCGWINSZ, &mut ws) };
    if rc != 0 {
        anyhow::bail!("ioctl(TIOCGWINSZ) failed (rc={})", rc);
    }
    if ws.ws_row == 0 {
        anyhow::bail!("ioctl(TIOCGWINSZ) returned 0 rows");
    }
    Ok(ws.ws_row as usize)
}

fn read_key(stdin: &mut impl Read) -> anyhow::Result<TuiKey> {
    let mut b = [0u8; 1];
    stdin.read_exact(&mut b).context("read key")?;
    match b[0] {
        0x03 => Ok(TuiKey::Quit),
        b'\n' | b'\r' => Ok(TuiKey::Enter),
        b'\t' => Ok(TuiKey::Tab),
        b'q' | b'Q' => Ok(TuiKey::Quit),
        b'r' | b'R' => Ok(TuiKey::Refresh),
        b'C' => Ok(TuiKey::FocusCluster),
        b'K' => Ok(TuiKey::FocusKind),
        b'V' => Ok(TuiKey::FocusView),
        b'k' => Ok(TuiKey::Up),
        b'j' => Ok(TuiKey::Down),
        0x1b => {
            // Arrow keys are typically: ESC [ A/B/C/D
            if stdin.read_exact(&mut b).is_err() {
                return Ok(TuiKey::Quit);
            }
            if b[0] != b'[' {
                return Ok(TuiKey::Other);
            }
            stdin.read_exact(&mut b).context("read key seq")?;
            match b[0] {
                b'A' => Ok(TuiKey::Up),
                b'B' => Ok(TuiKey::Down),
                b'C' => Ok(TuiKey::Right),
                b'D' => Ok(TuiKey::Left),
                _ => Ok(TuiKey::Other),
            }
        }
        _ => Ok(TuiKey::Other),
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Focus {
    Cluster,
    Kind,
    View,
}

fn focus_next(f: Focus) -> Focus {
    match f {
        Focus::Cluster => Focus::Kind,
        Focus::Kind => Focus::View,
        Focus::View => Focus::Cluster,
    }
}

fn focus_prev(f: Focus) -> Focus {
    match f {
        Focus::Cluster => Focus::View,
        Focus::Kind => Focus::Cluster,
        Focus::View => Focus::Kind,
    }
}

fn render_tabs_line(
    label: &str,
    items: &[String],
    cursor: usize,
    selected: Option<usize>,
    focused: bool,
) -> anyhow::Result<String> {
    use std::fmt::Write as _;
    let mut out = String::new();
    write!(&mut out, "{}: ", label)?;
    for (i, it) in items.iter().enumerate() {
        if i > 0 {
            write!(&mut out, " ")?;
        }
        let is_selected = selected == Some(i);
        let display = if is_selected {
            format!("*{}*", it)
        } else {
            it.clone()
        };
        if focused && i == cursor {
            write!(&mut out, "\x1b[7m[{}]\x1b[0m", display)?;
        } else {
            write!(&mut out, "[{}]", display)?;
        }
    }
    Ok(out)
}

fn render_kind_tabs_line(
    kinds: &[MemberKind],
    cursor: usize,
    selected: Option<usize>,
    focused: bool,
) -> anyhow::Result<String> {
    let items: Vec<String> = kinds
        .iter()
        .map(|k| k.as_display_str().to_string())
        .collect();
    render_tabs_line("Kind", &items, cursor, selected, focused)
}

fn url_encode_component(s: &str) -> String {
    url::form_urlencoded::byte_serialize(s.as_bytes()).collect::<String>()
}

fn render_screen(
    http_server_addr: &str,
    clusters: &[String],
    kinds: &[MemberKind],
    focus: Focus,
    cluster_cursor: usize,
    cluster_selected: Option<usize>,
    kind_cursor: usize,
    kind_selected: Option<usize>,
    last_update_desc: &str,
    next_refresh_in_secs: Option<u64>,
    last_error: Option<&str>,
    last_body: &str,
    view_scroll: usize,
    term_rows: usize,
) -> anyhow::Result<usize> {
    let mut lines: Vec<String> = Vec::new();
    lines.push(
        "Fluxon TUI  (Ctrl+C quit, Left/Right focus, Up/Down move, Enter select, r refresh)"
            .to_string(),
    );
    lines.push(format!("Server: {}", http_server_addr));
    lines.push(render_tabs_line(
        "Cluster",
        clusters,
        cluster_cursor,
        cluster_selected,
        focus == Focus::Cluster,
    )?);
    lines.push(render_kind_tabs_line(
        kinds,
        kind_cursor,
        kind_selected,
        focus == Focus::Kind,
    )?);

    let cluster_name = cluster_selected
        .and_then(|i| clusters.get(i))
        .map(|s| s.as_str());
    let kind = kind_selected.and_then(|i| kinds.get(i)).copied();
    match (cluster_name, kind) {
        (Some(c), Some(k)) => {
            lines.push(format!(
                "Selected: cluster={} kind={}",
                c,
                k.as_display_str()
            ));
            lines.push(format!(
                "Web: {}/view?cluster_name={}&member_kind={}",
                http_server_addr.trim_end_matches('/'),
                url_encode_component(c),
                url_encode_component(k.as_query_str())
            ));
            lines.push(format!(
                "Landing: {}/",
                http_server_addr.trim_end_matches('/')
            ));
        }
        _ => {
            lines.push("Selected: (press Enter on Cluster and Kind)".to_string());
            lines.push(format!(
                "Landing: {}/",
                http_server_addr.trim_end_matches('/')
            ));
        }
    }

    lines.push(String::new());
    lines.push(format!("Last update: {}", last_update_desc));
    match next_refresh_in_secs {
        Some(v) => lines.push(format!("Next refresh in: {}s", v)),
        None => lines.push("Next refresh: N/A (select Cluster + Kind)".to_string()),
    }

    let mut view_label = "View".to_string();
    if focus == Focus::View {
        view_label = "\x1b[7mView\x1b[0m".to_string();
    }

    let content = if let Some(e) = last_error {
        format!("Error:\n{}", e)
    } else {
        last_body.to_string()
    };
    let content_lines: Vec<&str> = if content.is_empty() {
        Vec::new()
    } else {
        content.lines().collect()
    };

    let header_rows = lines.len() + 1;
    let view_height = term_rows.saturating_sub(header_rows);
    if view_height == 0 || content_lines.is_empty() {
        lines.push(format!("{}  (empty)", view_label));
    } else {
        let max_scroll = content_lines.len().saturating_sub(view_height);
        let scroll = std::cmp::min(view_scroll, max_scroll);
        let end = std::cmp::min(scroll + view_height, content_lines.len());
        lines.push(format!(
            "{}  (lines {}-{}/{}, scroll {}/{})",
            view_label,
            scroll + 1,
            end,
            content_lines.len(),
            scroll,
            max_scroll
        ));
        for l in &content_lines[scroll..end] {
            lines.push((*l).to_string());
        }
        let mut stdout = std::io::stdout();
        let mut out = String::new();
        out.push_str("\x1b[2J\x1b[H");
        for l in &lines {
            out.push_str(l);
            out.push('\n');
        }
        stdout.write_all(out.as_bytes())?;
        stdout.flush()?;
        return Ok(scroll);
    }

    let mut stdout = std::io::stdout();
    let mut out = String::new();
    out.push_str("\x1b[2J\x1b[H");
    for l in &lines {
        out.push_str(l);
        out.push('\n');
    }
    stdout.write_all(out.as_bytes())?;
    stdout.flush()?;
    Ok(0)
}

async fn fetch_cli_text(
    client: &reqwest::Client,
    base: &url::Url,
    cluster_name: &str,
    kind: MemberKind,
) -> anyhow::Result<(reqwest::StatusCode, String)> {
    let cli_url = {
        let mut u = base.join("/cli").context("build /cli url")?;
        u.query_pairs_mut()
            .append_pair("cluster_name", cluster_name)
            .append_pair("member_kind", kind.as_query_str());
        u
    };
    let resp = client.get(cli_url).send().await.context("GET /cli")?;
    let status = resp.status();
    let body = resp.text().await.context("read /cli body")?;
    Ok((status, body))
}

pub async fn render_cluster_over_http_interactive(
    http_server_addr: &str,
) -> anyhow::Result<String> {
    let base = url::Url::parse(http_server_addr)
        .with_context(|| format!("invalid http_server_addr: {}", http_server_addr))?;
    if base.scheme() != "http" && base.scheme() != "https" {
        anyhow::bail!("http_server_addr must start with http:// or https://");
    }

    let client = reqwest::Client::new();

    let clusters_url = base
        .join("/api/clusters")
        .context("build /api/clusters url")?;
    let clusters_resp = {
        let resp = client
            .get(clusters_url)
            .send()
            .await
            .context("GET /api/clusters")?;
        let status = resp.status();
        let body = resp.text().await.context("read /api/clusters body")?;
        if !status.is_success() {
            anyhow::bail!(
                "GET /api/clusters failed: HTTP {}\n{}",
                status.as_u16(),
                body
            );
        }
        serde_json::from_str::<ClustersResponse>(&body).context("parse /api/clusters json")?
    };
    if clusters_resp.clusters.is_empty() {
        anyhow::bail!("no clusters discovered from /api/clusters");
    }

    let _raw = enable_raw_mode()?;
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<TuiKey>();
    std::thread::spawn(move || {
        let mut stdin = std::io::stdin().lock();
        loop {
            let k = match read_key(&mut stdin) {
                Ok(k) => k,
                Err(_) => TuiKey::Quit,
            };
            if tx.send(k).is_err() {
                break;
            }
            if matches!(k, TuiKey::Quit) {
                break;
            }
        }
    });

    let clusters = clusters_resp.clusters;
    let kinds: Vec<MemberKind> = AVAILABLE_MEMBER_KINDS.to_vec();

    let mut focus = Focus::Cluster;
    let mut cluster_cursor = 0usize;
    let mut cluster_selected: Option<usize> = None;
    let mut kind_cursor = 0usize;
    let mut kind_selected: Option<usize> = None;

    let mut last_body = String::new();
    let mut last_error: Option<String> = None;
    let mut last_update_desc = "never".to_string();
    let mut view_scroll: usize = 0;
    let refresh_interval_secs = crate::config::AUTO_REFRESH_INTERVAL.as_secs();
    let mut next_refresh_in_secs: u64 = refresh_interval_secs;

    let term_rows = terminal_rows()?;
    view_scroll = render_screen(
        http_server_addr,
        &clusters,
        &kinds,
        focus,
        cluster_cursor,
        cluster_selected,
        kind_cursor,
        kind_selected,
        &last_update_desc,
        None,
        last_error.as_deref(),
        &last_body,
        view_scroll,
        term_rows,
    )?;

    let ctrl_c = tokio::signal::ctrl_c();
    tokio::pin!(ctrl_c);

    let mut ticker = tokio::time::interval(std::time::Duration::from_millis(1000));
    loop {
        tokio::select! {
            _ = &mut ctrl_c => { break; }
            _ = ticker.tick() => {
                let mut requested_fetch = false;
                if cluster_selected.is_some() && kind_selected.is_some() {
                    if next_refresh_in_secs > 0 {
                        next_refresh_in_secs -= 1;
                    }
                    if next_refresh_in_secs == 0 {
                        requested_fetch = true;
                    }
                }

                if requested_fetch {
                    if let (Some(ci), Some(ki)) = (cluster_selected, kind_selected) {
                        if let (Some(cluster_name), Some(kind)) = (clusters.get(ci), kinds.get(ki).copied()) {
                            match fetch_cli_text(&client, &base, cluster_name, kind).await {
                                Ok((status, body)) => {
                                    if status.is_success() {
                                        last_body = body;
                                        last_error = None;
                                        last_update_desc = format!("ok (HTTP {})", status.as_u16());
                                    } else {
                                        last_error = Some(format!("GET /cli failed: HTTP {}\n{}", status.as_u16(), body));
                                        last_body.clear();
                                        last_update_desc = format!("error (HTTP {})", status.as_u16());
                                    }
                                }
                                Err(e) => {
                                    last_error = Some(format!("GET /cli failed: {}", e));
                                    last_body.clear();
                                    last_update_desc = "error (transport)".to_string();
                                }
                            }
                            next_refresh_in_secs = refresh_interval_secs;
                        }
                    }
                }

                let term_rows = terminal_rows()?;
                view_scroll = render_screen(
                    http_server_addr,
                    &clusters,
                    &kinds,
                    focus,
                    cluster_cursor,
                    cluster_selected,
                    kind_cursor,
                    kind_selected,
                    &last_update_desc,
                    if cluster_selected.is_some() && kind_selected.is_some() { Some(next_refresh_in_secs) } else { None },
                    last_error.as_deref(),
                    &last_body,
                    view_scroll,
                    term_rows,
                )?;
            }
            maybe_key = rx.recv() => {
                let Some(key) = maybe_key else { break; };
                let mut requested_fetch = false;
                match key {
                    TuiKey::Quit => break,
                    TuiKey::Tab => focus = focus_next(focus),
                    TuiKey::FocusCluster => focus = Focus::Cluster,
                    TuiKey::FocusKind => focus = Focus::Kind,
                    TuiKey::FocusView => focus = Focus::View,
                    TuiKey::Left => {
                        focus = focus_prev(focus);
                    }
                    TuiKey::Right => {
                        focus = focus_next(focus);
                    }
                    TuiKey::Up => {
                        match focus {
                            Focus::Cluster => {
                                if !clusters.is_empty() {
                                    if cluster_cursor == 0 { cluster_cursor = clusters.len() - 1; } else { cluster_cursor -= 1; }
                                }
                            }
                            Focus::Kind => {
                                if !kinds.is_empty() {
                                    if kind_cursor == 0 { kind_cursor = kinds.len() - 1; } else { kind_cursor -= 1; }
                                }
                            }
                            Focus::View => {
                                view_scroll = view_scroll.saturating_sub(1);
                            }
                        }
                    }
                    TuiKey::Down => {
                        match focus {
                            Focus::Cluster => { if !clusters.is_empty() { cluster_cursor = (cluster_cursor + 1) % clusters.len(); } }
                            Focus::Kind => { if !kinds.is_empty() { kind_cursor = (kind_cursor + 1) % kinds.len(); } }
                            Focus::View => {
                                view_scroll = view_scroll.saturating_add(1);
                            }
                        }
                    }
                    TuiKey::Enter => {
                        match focus {
                            Focus::Cluster => {
                                cluster_selected = Some(cluster_cursor);
                                last_body.clear();
                                last_error = None;
                                last_update_desc = "cluster selected".to_string();
                                requested_fetch = kind_selected.is_some();
                            }
                            Focus::Kind => {
                                kind_selected = Some(kind_cursor);
                                last_body.clear();
                                last_error = None;
                                last_update_desc = "kind selected".to_string();
                                requested_fetch = cluster_selected.is_some();
                            }
                            Focus::View => {
                                last_update_desc = "refresh requested".to_string();
                                requested_fetch = cluster_selected.is_some() && kind_selected.is_some();
                            }
                        }
                    }
                    TuiKey::Refresh => {
                        last_update_desc = "refresh requested".to_string();
                        requested_fetch = cluster_selected.is_some() && kind_selected.is_some();
                    }
                    TuiKey::Other => {}
                }

                if requested_fetch {
                    if let (Some(ci), Some(ki)) = (cluster_selected, kind_selected) {
                        if let (Some(cluster_name), Some(kind)) = (clusters.get(ci), kinds.get(ki).copied()) {
                            match fetch_cli_text(&client, &base, cluster_name, kind).await {
                                Ok((status, body)) => {
                                    if status.is_success() {
                                        last_body = body;
                                        last_error = None;
                                        last_update_desc = format!("ok (HTTP {})", status.as_u16());
                                    } else {
                                        last_error = Some(format!("GET /cli failed: HTTP {}\n{}", status.as_u16(), body));
                                        last_body.clear();
                                        last_update_desc = format!("error (HTTP {})", status.as_u16());
                                    }
                                }
                                Err(e) => {
                                    last_error = Some(format!("GET /cli failed: {}", e));
                                    last_body.clear();
                                    last_update_desc = "error (transport)".to_string();
                                }
                            }
                            next_refresh_in_secs = refresh_interval_secs;
                        }
                    }
                }
                let term_rows = terminal_rows()?;
                view_scroll = render_screen(
                    http_server_addr,
                    &clusters,
                    &kinds,
                    focus,
                    cluster_cursor,
                    cluster_selected,
                    kind_cursor,
                    kind_selected,
                    &last_update_desc,
                    if cluster_selected.is_some() && kind_selected.is_some() { Some(next_refresh_in_secs) } else { None },
                    last_error.as_deref(),
                    &last_body,
                    view_scroll,
                    term_rows,
                )?;
            }
        }
    }

    Ok(String::new())
}
