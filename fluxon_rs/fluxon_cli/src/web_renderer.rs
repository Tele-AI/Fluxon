use crate::config::{AUTO_REFRESH_INTERVAL, AVAILABLE_MEMBER_KINDS, MemberKind};
use crate::model::{
    AVAILABLE_MEMBER_ROLES, ClusterSnapshot, MemberRole, P2pTransportKind, RoutePixelState, UiPill,
    UiPillStatus, build_cluster_view_model, build_member_table_rows, parse_route_pixels,
    pills_for_cluster_totals,
};
use askama::Template;
use fluxon_util::fs_statvfs::normalize_abs_dir_label;
use serde::Serialize;
use std::collections::{BTreeSet, HashMap};

fn mq_status_str(s: crate::model::MqMemberStatus) -> &'static str {
    match s {
        crate::model::MqMemberStatus::Alive => "alive",
        crate::model::MqMemberStatus::Stale => "stale",
        crate::model::MqMemberStatus::Invalid => "invalid",
    }
}

#[derive(Clone)]
struct BuildInfoView {
    version: String,
    commit: String,
    source_sha256: String,
}

#[derive(Clone)]
struct MemberKindView {
    query: String,
    display: String,
    selected: bool,
}

#[derive(Clone)]
struct MemberRoleView {
    query: String,
    display: String,
    checked: bool,
}

#[derive(Clone)]
struct LogRoleView {
    query: String,
    display: String,
}

#[derive(Clone)]
struct PillView {
    text: String,
    class_name: String,
}

#[derive(Clone)]
struct HeaderView {
    cluster_name: String,
    cluster_name_encoded: String,
    member_kind: String,
    member_kind_query: String,
    etcd_endpoints: String,
    prometheus_base_url: String,
    master_network_subnet_whitelist: Option<String>,
}

#[derive(Clone)]
struct ClusterLandingView {
    name: String,
    encoded: String,
}

#[derive(Clone, Copy)]
enum RegisteredPanelKind {
    Ops,
}

impl RegisteredPanelKind {
    fn as_service_name(&self) -> &'static str {
        match self {
            RegisteredPanelKind::Ops => crate::OPS_PANEL_SERVICE_NAME,
        }
    }

    fn as_display_str(&self) -> &'static str {
        match self {
            RegisteredPanelKind::Ops => crate::OPS_PANEL_SERVICE_NAME,
        }
    }
}

const AVAILABLE_REGISTERED_PANELS: &[RegisteredPanelKind] = &[RegisteredPanelKind::Ops];

#[derive(Clone)]
struct RegisteredPanelView {
    service_name: String,
    display: String,
}

#[derive(Template)]
#[template(path = "landing.html")]
struct LandingTemplate {
    clusters: Vec<ClusterLandingView>,
    member_kinds: Vec<MemberKindView>,
    panels: Vec<RegisteredPanelView>,
}

#[derive(Template)]
#[template(path = "monitor_table.html")]
struct MonitorTableTemplate {
    header: HeaderView,
    build: BuildInfoView,
    member_kinds: Vec<MemberKindView>,
    member_roles: Vec<MemberRoleView>,
    log_roles: Vec<LogRoleView>,
    total_pills: Vec<PillView>,
    owner_rdma_controls: Vec<RdmaControlOwnerView>,
    owner_tokio_rows: Vec<OwnerTokioHealthRowView>,
    rows: Vec<crate::model::MemberTableRowView>,
    owner_seg: crate::model::OwnerSegmentTablesView,
    matrix: MatrixView,
    warnings: Vec<String>,
    refresh_secs: u64,
}

#[derive(Clone)]
struct RdmaControlDeviceView {
    device: String,
    desired_enabled: bool,
    effective_enabled: bool,
    health_text: String,
    health_class: String,
    disable_checkbox: bool,
    detail_text: String,
    ports: Vec<RdmaControlPortView>,
}

#[derive(Clone)]
struct RdmaControlPortView {
    port_key: String,
    state_text: String,
    state_class: String,
    title_text: String,
}

#[derive(Clone)]
struct RdmaControlOwnerView {
    member_id: String,
    node_start_time: i64,
    node_key: String,
    hostname_text: String,
    accessible_ip_text: String,
    transfer_engine_text: String,
    transfer_engine_class: String,
    transfer_engine_detail: Option<String>,
    runtime_state_text: String,
    probe_error: Option<String>,
    devices: Vec<RdmaControlDeviceView>,
}

#[derive(Clone)]
struct OwnerTokioHealthRowView {
    member_id: String,
    hostname_text: String,
    accessible_ip_text: String,
    workers_text: String,
    alive_tasks_text: String,
    queue_depth_text: String,
    busy_text: String,
    max_worker_busy_text: String,
    park_unpark_rate_text: String,
}

#[derive(Clone)]
struct MatrixNodeView {
    key: String,
    cls: String,
}

#[derive(Clone)]
struct MatrixCellView {
    td_cls: String,
    p2p_cls: String,
    te_cls: String,
}

#[derive(Clone)]
struct MatrixRowView {
    key: String,
    cls: String,
    cells: Vec<MatrixCellView>,
}

#[derive(Clone)]
struct MatrixView {
    nodes: Vec<MatrixNodeView>,
    rows: Vec<MatrixRowView>,
    node_count: usize,
    edge_count: usize,
    unknown_routes: Vec<String>,
}

#[derive(Clone)]
struct TopologyView {
    json_escaped: String,
    node_count: usize,
    edge_count: usize,
}

#[derive(Template)]
#[template(path = "topology.html")]
struct TopologyTemplate {
    header: HeaderView,
    build: BuildInfoView,
    topology: Option<TopologyView>,
    warnings: Vec<String>,
    refresh_secs: u64,
}

#[derive(Clone)]
struct MqChannelView {
    chan_id: i64,
    capacity: String,
    ttl_seconds: String,
    payload_lease_id: String,
    unique_keys: String,
    producer_count: String,
    consumer_count: String,
    producer_offsets: String,
    current_inflight: NumCellView,
    prefetch_avg_get_handle_ms: NumCellView,
    prefetch_latest_get_handle_ms: NumCellView,
    prefetch_avg_handle_await_ms: NumCellView,
    prefetch_latest_handle_await_ms: NumCellView,
    prefetch_avg_etcd_put_ms: NumCellView,
    prefetch_latest_etcd_put_ms: NumCellView,
    prefetch_inflight_queue_size: NumCellView,
    prefetch_target_inflight: NumCellView,
    get_one_avg_total_ms: NumCellView,
    get_one_max_total_ms: NumCellView,
    get_one_avg_wait_rx_ms: NumCellView,
    get_one_max_wait_rx_ms: NumCellView,
    get_one_avg_signal_ms: NumCellView,
    get_one_max_signal_ms: NumCellView,
    get_one_avg_post_ms: NumCellView,
    get_one_max_post_ms: NumCellView,
    get_one_window_calls_sum: NumCellView,
    get_one_window_timeouts_sum: NumCellView,
    producer_nonblocking_latest_phase_calls: NumCellView,
    producer_nonblocking_latest_phase_rps: NumCellView,
    producer_nonblocking_latest_interval: String,
    consumer_nonblocking_latest_phase_calls: NumCellView,
    consumer_nonblocking_latest_phase_rps: NumCellView,
    consumer_nonblocking_latest_interval: String,
}

#[derive(Clone)]
struct NumCellView {
    text: String,
    sort: String,
}

impl NumCellView {
    fn na() -> Self {
        Self {
            text: "N/A".to_string(),
            sort: "NaN".to_string(),
        }
    }
}

fn ms_cell_from_us(v: Option<f64>) -> NumCellView {
    match v {
        Some(us) => {
            let ms = us / 1000.0;
            NumCellView {
                text: format!("{:.3}", ms),
                sort: ms.to_string(),
            }
        }
        None => NumCellView::na(),
    }
}

fn i64_cell_from_opt_f64(v: Option<f64>) -> NumCellView {
    match v {
        Some(x) => {
            let n = x.round() as i64;
            NumCellView {
                text: n.to_string(),
                sort: n.to_string(),
            }
        }
        None => NumCellView::na(),
    }
}

fn hz_cell_from_opt_f64(v: Option<f64>) -> NumCellView {
    match v {
        Some(x) => NumCellView {
            text: format!("{:.3}", x),
            sort: x.to_string(),
        },
        None => NumCellView::na(),
    }
}

fn max_opt_f64(values: impl Iterator<Item = Option<f64>>) -> Option<f64> {
    values.flatten().fold(None, |acc, v| match acc {
        None => Some(v),
        Some(cur) => Some(cur.max(v)),
    })
}

fn sum_opt_f64_any(values: impl Iterator<Item = Option<f64>>) -> Option<f64> {
    // English note: "any" semantics:
    // - if at least one member reports, return sum of reported values;
    // - if all are missing, return None.
    let mut any = false;
    let mut sum = 0.0;
    for v in values {
        if let Some(x) = v {
            any = true;
            sum += x;
        }
    }
    if any { Some(sum) } else { None }
}

fn i64_cell_from_opt_i64(v: Option<i64>) -> NumCellView {
    match v {
        Some(x) => NumCellView {
            text: x.to_string(),
            sort: x.to_string(),
        },
        None => NumCellView::na(),
    }
}

fn latest_interval_text(begin_unix_ms: Option<f64>, end_unix_ms: Option<f64>) -> String {
    match (begin_unix_ms, end_unix_ms) {
        (Some(begin), Some(end)) => {
            let begin_ms = begin.round() as i64;
            let end_ms = end.round() as i64;
            format!("{begin_ms} ~ {end_ms}")
        }
        _ => "N/A".to_string(),
    }
}

#[derive(Clone)]
struct MqMemberView {
    channel_unique_keys: String,
    kind: String,
    idx: String,
    status: String,
    external_client_id: String,
    owner_id: String,
    produce_offset: String,
    consume_offset: String,
    kvclient_sub_cluster: String,
    prefetch_avg_get_handle_ms: NumCellView,
    prefetch_latest_get_handle_ms: NumCellView,
    prefetch_avg_handle_await_ms: NumCellView,
    prefetch_latest_handle_await_ms: NumCellView,
    prefetch_avg_etcd_put_ms: NumCellView,
    prefetch_latest_etcd_put_ms: NumCellView,
    prefetch_inflight_queue_size: NumCellView,
    prefetch_target_inflight: NumCellView,
    get_one_avg_total_ms: NumCellView,
    get_one_max_total_ms: NumCellView,
    get_one_avg_wait_rx_ms: NumCellView,
    get_one_max_wait_rx_ms: NumCellView,
    get_one_avg_signal_ms: NumCellView,
    get_one_max_signal_ms: NumCellView,
    get_one_avg_post_ms: NumCellView,
    get_one_max_post_ms: NumCellView,
    get_one_window_calls: NumCellView,
    get_one_window_timeouts: NumCellView,
    nonblocking_latest_phase_calls: NumCellView,
    nonblocking_latest_phase_rps: NumCellView,
    nonblocking_latest_interval: String,
}

#[derive(Clone)]
struct MqChannelGroupView {
    channel: MqChannelView,
    members: Vec<MqMemberView>,
}

#[derive(Template)]
#[template(path = "mq.html")]
struct MqTemplate {
    header: HeaderView,
    build: BuildInfoView,
    warnings: Vec<String>,
    channel_groups: Vec<MqChannelGroupView>,
    refresh_secs: u64,
}

fn build_info_view() -> BuildInfoView {
    BuildInfoView {
        version: env!("CARGO_PKG_VERSION").to_string(),
        commit: crate::build_info::GIT_COMMIT_ID.to_string(),
        source_sha256: crate::build_info::SOURCE_SHA256.to_string(),
    }
}

fn member_kind_views(selected: MemberKind) -> Vec<MemberKindView> {
    AVAILABLE_MEMBER_KINDS
        .iter()
        .copied()
        .map(|k| MemberKindView {
            query: k.as_query_str().to_string(),
            display: k.as_display_str().to_string(),
            selected: k == selected,
        })
        .collect()
}

fn registered_panel_views() -> Vec<RegisteredPanelView> {
    AVAILABLE_REGISTERED_PANELS
        .iter()
        .copied()
        .map(|p| RegisteredPanelView {
            service_name: p.as_service_name().to_string(),
            display: p.as_display_str().to_string(),
        })
        .collect()
}

fn member_role_views(visible_roles: Option<&Vec<MemberRole>>) -> Vec<MemberRoleView> {
    AVAILABLE_MEMBER_ROLES
        .iter()
        .copied()
        .map(|r| MemberRoleView {
            query: r.as_str().to_string(),
            display: r.as_str().to_string(),
            checked: visible_roles.map(|v| v.contains(&r)).unwrap_or(true),
        })
        .collect()
}

fn log_role_views() -> Vec<LogRoleView> {
    [
        MemberRole::Master,
        MemberRole::OwnerClient,
        MemberRole::ExternalClient,
        MemberRole::SideTransferWorker,
    ]
    .iter()
    .copied()
    .map(|r| LogRoleView {
        query: r.as_str().to_string(),
        display: r.as_str().to_string(),
    })
    .collect()
}

fn pill_views(pills: &[UiPill]) -> Vec<PillView> {
    pills
        .iter()
        .map(|p| {
            let class_name = match p.status {
                UiPillStatus::Ok => "".to_string(),
                UiPillStatus::Na => "pill_na".to_string(),
                UiPillStatus::Warn => "pill_warn".to_string(),
            };
            PillView {
                text: p.render_text(),
                class_name,
            }
        })
        .collect()
}

fn header_view(snapshot: &ClusterSnapshot) -> HeaderView {
    let vm = build_cluster_view_model(snapshot);
    HeaderView {
        cluster_name: vm.header.cluster_name.clone(),
        cluster_name_encoded: crate::model::url_encode_component(&vm.header.cluster_name),
        member_kind: vm.header.member_kind.as_display_str().to_string(),
        member_kind_query: vm.header.member_kind.as_query_str().to_string(),
        etcd_endpoints: vm.header.etcd_endpoints.join(","),
        prometheus_base_url: vm.header.prometheus_base_url.clone(),
        master_network_subnet_whitelist: vm.header.master_network_subnet_whitelist.clone(),
    }
}

pub fn render_landing_page(clusters: &[String]) -> String {
    let clusters_view = clusters
        .iter()
        .map(|c| ClusterLandingView {
            name: c.clone(),
            encoded: crate::model::url_encode_component(c),
        })
        .collect::<Vec<_>>();
    let template = LandingTemplate {
        clusters: clusters_view,
        member_kinds: member_kind_views(MemberKind::Kv),
        panels: registered_panel_views(),
    };
    template.render().unwrap()
}

fn render_table_cluster(snapshot: &ClusterSnapshot) -> String {
    let vm = build_cluster_view_model(snapshot);
    let header = header_view(snapshot);
    let total_pills = pill_views(&pills_for_cluster_totals(&vm.totals));
    let owner_rdma_controls = build_owner_rdma_controls(snapshot);
    let owner_tokio_rows = build_owner_tokio_health_rows(snapshot);
    let rows = build_member_table_rows(snapshot);
    let owner_seg = crate::model::build_owner_segment_tables(snapshot);
    let mut warnings = snapshot.warnings.clone();
    let matrix = build_transfer_link_matrix(snapshot, &mut warnings);
    let template = MonitorTableTemplate {
        header,
        build: build_info_view(),
        member_kinds: member_kind_views(vm.header.member_kind),
        member_roles: member_role_views(snapshot.visible_member_roles.as_ref()),
        log_roles: log_role_views(),
        total_pills,
        owner_rdma_controls,
        owner_tokio_rows,
        rows,
        owner_seg,
        matrix,
        warnings,
        refresh_secs: AUTO_REFRESH_INTERVAL.as_secs(),
    };
    template.render().unwrap()
}

fn build_owner_rdma_controls(snapshot: &ClusterSnapshot) -> Vec<RdmaControlOwnerView> {
    let mut owners: Vec<RdmaControlOwnerView> = Vec::new();
    for node in &snapshot.nodes {
        for member in &node.members {
            if member.role != MemberRole::OwnerClient {
                continue;
            }
            let (transfer_engine_text, transfer_engine_class, transfer_engine_detail) = match member
                .rdma_transfer_engine
                .as_ref()
            {
                Some(transfer_engine) => {
                    let class = match transfer_engine.state {
                        fluxon_commu::MemberRdmaTransferEngineState::Running => "rdma_health_ok",
                        fluxon_commu::MemberRdmaTransferEngineState::Starting
                        | fluxon_commu::MemberRdmaTransferEngineState::Restarting => {
                            if transfer_engine.consecutive_start_failures > 0 {
                                "rdma_health_bad"
                            } else {
                                "rdma_health_warn"
                            }
                        }
                        fluxon_commu::MemberRdmaTransferEngineState::Disabled
                        | fluxon_commu::MemberRdmaTransferEngineState::Stopped => {
                            if transfer_engine.consecutive_start_failures > 0 {
                                "rdma_health_bad"
                            } else {
                                "muted"
                            }
                        }
                    };
                    (
                        transfer_engine.state.as_str().to_string(),
                        class.to_string(),
                        (transfer_engine.consecutive_start_failures > 0).then(|| {
                            format!(
                                "consecutive_start_failures={}",
                                transfer_engine.consecutive_start_failures
                            )
                        }),
                    )
                }
                None => ("n/a".to_string(), "muted".to_string(), None),
            };
            let runtime_state_text = if !member.rdma_runtime_reported {
                "not reported".to_string()
            } else if member.rdma_probe_error.is_some() {
                "probe error".to_string()
            } else if member.rdma_ports.is_empty() {
                "no RDMA NIC".to_string()
            } else {
                "reported".to_string()
            };

            let mut ports_by_device: HashMap<String, Vec<(u8, RdmaControlPortView)>> =
                HashMap::new();
            for port in &member.rdma_ports {
                let (state_text, state_class) = if !port.usable {
                    ("blocked", "rdma_health_bad")
                } else if port.effective_enabled {
                    ("effective", "rdma_health_ok")
                } else {
                    ("inactive", "muted")
                };
                let device_name = if port.device.is_empty() {
                    "-".to_string()
                } else {
                    port.device.clone()
                };
                ports_by_device.entry(device_name).or_default().push((
                    port.port,
                    RdmaControlPortView {
                        port_key: port.port.to_string(),
                        state_text: state_text.to_string(),
                        state_class: state_class.to_string(),
                        title_text: format!(
                            "{}:{} | {} / {} / {} / {} | phys={} mtu={}{}",
                            if port.device.is_empty() {
                                "-"
                            } else {
                                &port.device
                            },
                            port.port,
                            port.port_state_text,
                            port.link_layer_text,
                            if port.usable { "usable" } else { "blocked" },
                            if port.effective_enabled {
                                "effective"
                            } else {
                                "inactive"
                            },
                            port.phys_state_text,
                            port.active_mtu_text,
                            match port.last_error.as_ref() {
                                Some(err) => format!(" err={}", err),
                                None => String::new(),
                            }
                        ),
                    },
                ));
            }

            let mut devices = member
                .rdma_devices
                .iter()
                .map(|device| {
                    let (health_text, health_class) =
                        if !device.detected || device.usable_ports == 0 {
                            ("unhealthy".to_string(), "rdma_health_bad".to_string())
                        } else if device.blocked_ports > 0 {
                            ("degraded".to_string(), "rdma_health_warn".to_string())
                        } else {
                            ("healthy".to_string(), "rdma_health_ok".to_string())
                        };
                    let disable_checkbox =
                        !device.desired_enabled && (!device.detected || device.usable_ports == 0);
                    RdmaControlDeviceView {
                        device: device.device.clone(),
                        desired_enabled: device.desired_enabled,
                        effective_enabled: device.effective_enabled,
                        health_text,
                        health_class,
                        disable_checkbox,
                        ports: {
                            let mut ports =
                                ports_by_device.remove(&device.device).unwrap_or_default();
                            ports.sort_by(|a, b| {
                                a.0.cmp(&b.0).then_with(|| a.1.port_key.cmp(&b.1.port_key))
                            });
                            ports.into_iter().map(|(_, port)| port).collect()
                        },
                        detail_text: format!(
                            "{} ports, {} usable, {} blocked, {}",
                            device.total_ports,
                            device.usable_ports,
                            device.blocked_ports,
                            if device.detected {
                                "detected"
                            } else {
                                "missing"
                            }
                        ),
                    }
                })
                .collect::<Vec<_>>();
            let mut extra_devices = ports_by_device.keys().cloned().collect::<Vec<_>>();
            extra_devices.sort();
            for device_name in extra_devices {
                let mut ports = ports_by_device.remove(&device_name).unwrap_or_default();
                ports.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.port_key.cmp(&b.1.port_key)));
                let ports = ports.into_iter().map(|(_, port)| port).collect::<Vec<_>>();
                let effective_enabled = ports.iter().any(|port| port.state_text == "effective");
                devices.push(RdmaControlDeviceView {
                    device: device_name,
                    desired_enabled: false,
                    effective_enabled,
                    health_text: "unknown".to_string(),
                    health_class: "rdma_health_warn".to_string(),
                    disable_checkbox: true,
                    detail_text: "device summary unavailable; derived from port probe".to_string(),
                    ports,
                });
            }
            devices.sort_by(|a, b| a.device.cmp(&b.device));

            owners.push(RdmaControlOwnerView {
                member_id: member.member_id.clone(),
                node_start_time: member.node_start_time,
                node_key: node.node_key.clone(),
                hostname_text: member.hostname.clone().unwrap_or_default(),
                accessible_ip_text: member.accessible_ip.clone().unwrap_or_default(),
                transfer_engine_text,
                transfer_engine_class,
                transfer_engine_detail,
                runtime_state_text,
                probe_error: member.rdma_probe_error.clone(),
                devices,
            });
        }
    }
    owners.sort_by(|a, b| a.member_id.cmp(&b.member_id));
    owners
}

fn build_owner_tokio_health_rows(snapshot: &ClusterSnapshot) -> Vec<OwnerTokioHealthRowView> {
    fn fmt_count(v: Option<f64>) -> String {
        v.map(|v| format!("{:.0}", v))
            .unwrap_or_else(|| "N/A".to_string())
    }

    fn fmt_percent(v: Option<f64>) -> String {
        v.map(|v| format!("{:.1}%", v))
            .unwrap_or_else(|| "N/A".to_string())
    }

    fn fmt_rate(v: Option<f64>) -> String {
        v.map(|v| format!("{:.2}/s", v))
            .unwrap_or_else(|| "N/A".to_string())
    }

    let mut rows: Vec<OwnerTokioHealthRowView> = Vec::new();
    for node in &snapshot.nodes {
        for member in &node.members {
            if member.role != MemberRole::OwnerClient {
                continue;
            }
            rows.push(OwnerTokioHealthRowView {
                member_id: member.member_id.clone(),
                hostname_text: member.hostname.clone().unwrap_or_else(|| "".to_string()),
                accessible_ip_text: member
                    .accessible_ip
                    .clone()
                    .unwrap_or_else(|| "".to_string()),
                workers_text: fmt_count(member.tokio_num_workers),
                alive_tasks_text: fmt_count(member.tokio_alive_tasks),
                queue_depth_text: fmt_count(member.tokio_global_queue_depth),
                busy_text: fmt_percent(member.tokio_busy_percent),
                max_worker_busy_text: fmt_percent(member.tokio_max_worker_busy_percent),
                park_unpark_rate_text: fmt_rate(member.tokio_park_unpark_rate_hz),
            });
        }
    }
    rows.sort_by(|a, b| a.member_id.cmp(&b.member_id));
    rows
}

pub fn render_topology_page(snapshot: &ClusterSnapshot) -> String {
    let header = header_view(snapshot);
    let topology = build_kv_topology_view(snapshot);
    let template = TopologyTemplate {
        header,
        build: build_info_view(),
        topology,
        warnings: snapshot.warnings.clone(),
        refresh_secs: AUTO_REFRESH_INTERVAL.as_secs(),
    };
    template.render().unwrap()
}

fn render_mq_cluster(snapshot: &ClusterSnapshot) -> String {
    let header = header_view(snapshot);
    let mut channel_groups: Vec<MqChannelGroupView> = Vec::new();

    let mq = snapshot.mq.as_ref();
    if let Some(mq) = mq {
        for ch in &mq.channels {
            let meta = ch.meta.as_ref();
            let consumers = &ch.consumers;
            let channel_unique_keys = if ch.unique_keys.is_empty() {
                "N/A (no unique key mapping discovered)".to_string()
            } else {
                ch.unique_keys.join(",")
            };
            let mut current_inflight_sum: i64 = 0;
            let mut current_inflight_ready = false;
            let producer_offsets = if ch.producers.is_empty() {
                "N/A".to_string()
            } else {
                ch.producers
                    .iter()
                    .map(|p| {
                        let produce_offset = p
                            .produce_offset
                            .map(|v| v.to_string())
                            .unwrap_or_else(|| "N/A".to_string());
                        let consume_offset = p
                            .consume_offset
                            .map(|v| v.to_string())
                            .unwrap_or_else(|| "N/A".to_string());
                        if let (Some(prod), Some(cons)) = (p.produce_offset, p.consume_offset) {
                            current_inflight_ready = true;
                            current_inflight_sum += (prod - cons).max(0);
                        }
                        format!("{}: {}/{}", p.producer_idx, produce_offset, consume_offset)
                    })
                    .collect::<Vec<String>>()
                    .join(", ")
            };

            let prefetch_avg_get_handle_us_max =
                max_opt_f64(consumers.iter().map(|c| c.prefetch_avg_get_handle_us));
            let prefetch_latest_get_handle_us_max =
                max_opt_f64(consumers.iter().map(|c| c.prefetch_latest_get_handle_us));
            let prefetch_avg_handle_await_us_max =
                max_opt_f64(consumers.iter().map(|c| c.prefetch_avg_handle_await_us));
            let prefetch_latest_handle_await_us_max =
                max_opt_f64(consumers.iter().map(|c| c.prefetch_latest_handle_await_us));
            let prefetch_avg_etcd_put_us_max =
                max_opt_f64(consumers.iter().map(|c| c.prefetch_avg_etcd_put_us));
            let prefetch_latest_etcd_put_us_max =
                max_opt_f64(consumers.iter().map(|c| c.prefetch_latest_etcd_put_us));
            let prefetch_inflight_queue_size_max =
                max_opt_f64(consumers.iter().map(|c| c.prefetch_inflight_queue_size));
            let prefetch_target_inflight_max =
                max_opt_f64(consumers.iter().map(|c| c.prefetch_target_inflight));

            let get_one_avg_total_us_max =
                max_opt_f64(consumers.iter().map(|c| c.get_one_avg_total_us));
            let get_one_max_total_us_max =
                max_opt_f64(consumers.iter().map(|c| c.get_one_max_total_us));
            let get_one_avg_wait_rx_us_max =
                max_opt_f64(consumers.iter().map(|c| c.get_one_avg_wait_rx_us));
            let get_one_max_wait_rx_us_max =
                max_opt_f64(consumers.iter().map(|c| c.get_one_max_wait_rx_us));
            let get_one_avg_signal_us_max =
                max_opt_f64(consumers.iter().map(|c| c.get_one_avg_signal_us));
            let get_one_max_signal_us_max =
                max_opt_f64(consumers.iter().map(|c| c.get_one_max_signal_us));
            let get_one_avg_post_us_max =
                max_opt_f64(consumers.iter().map(|c| c.get_one_avg_post_us));
            let get_one_max_post_us_max =
                max_opt_f64(consumers.iter().map(|c| c.get_one_max_post_us));
            let get_one_window_calls_sum =
                sum_opt_f64_any(consumers.iter().map(|c| c.get_one_window_calls));
            let get_one_window_timeouts_sum =
                sum_opt_f64_any(consumers.iter().map(|c| c.get_one_window_timeouts));
            let mut latest_producer_nonblocking_end_unix_ms: Option<f64> = None;
            let mut latest_producer_nonblocking_begin_unix_ms: Option<f64> = None;
            let mut latest_producer_nonblocking_phase_calls: Option<f64> = None;
            let mut latest_producer_nonblocking_phase_rps: Option<f64> = None;
            for producer in &ch.producers {
                let Some(end_unix_ms) = producer.nonblocking_latest_end_unix_ms else {
                    continue;
                };
                let replace = latest_producer_nonblocking_end_unix_ms
                    .map(|current| end_unix_ms > current)
                    .unwrap_or(true);
                if replace {
                    latest_producer_nonblocking_end_unix_ms = Some(end_unix_ms);
                    latest_producer_nonblocking_begin_unix_ms =
                        producer.nonblocking_latest_begin_unix_ms;
                    latest_producer_nonblocking_phase_calls =
                        producer.nonblocking_latest_phase_calls;
                    latest_producer_nonblocking_phase_rps = producer.nonblocking_latest_phase_rps;
                }
            }
            let mut latest_consumer_nonblocking_end_unix_ms: Option<f64> = None;
            let mut latest_consumer_nonblocking_begin_unix_ms: Option<f64> = None;
            let mut latest_consumer_nonblocking_phase_calls: Option<f64> = None;
            let mut latest_consumer_nonblocking_phase_rps: Option<f64> = None;
            for consumer in consumers {
                let Some(end_unix_ms) = consumer.nonblocking_latest_end_unix_ms else {
                    continue;
                };
                let replace = latest_consumer_nonblocking_end_unix_ms
                    .map(|current| end_unix_ms > current)
                    .unwrap_or(true);
                if replace {
                    latest_consumer_nonblocking_end_unix_ms = Some(end_unix_ms);
                    latest_consumer_nonblocking_begin_unix_ms =
                        consumer.nonblocking_latest_begin_unix_ms;
                    latest_consumer_nonblocking_phase_calls =
                        consumer.nonblocking_latest_phase_calls;
                    latest_consumer_nonblocking_phase_rps = consumer.nonblocking_latest_phase_rps;
                }
            }

            let channel_view = MqChannelView {
                chan_id: ch.chan_id,
                capacity: meta
                    .map(|m| m.capacity.to_string())
                    .unwrap_or_else(|| "N/A".to_string()),
                ttl_seconds: meta
                    .map(|m| m.ttl_seconds.to_string())
                    .unwrap_or_else(|| "N/A".to_string()),
                payload_lease_id: meta
                    .and_then(|m| m.payload_lease_id.map(|v| v.to_string()))
                    .unwrap_or_else(|| "N/A".to_string()),
                unique_keys: channel_unique_keys.clone(),
                producer_count: ch.producers.len().to_string(),
                consumer_count: ch.consumers.len().to_string(),
                producer_offsets,
                current_inflight: i64_cell_from_opt_i64(if current_inflight_ready {
                    Some(current_inflight_sum)
                } else {
                    None
                }),
                prefetch_avg_get_handle_ms: ms_cell_from_us(prefetch_avg_get_handle_us_max),
                prefetch_latest_get_handle_ms: ms_cell_from_us(prefetch_latest_get_handle_us_max),
                prefetch_avg_handle_await_ms: ms_cell_from_us(prefetch_avg_handle_await_us_max),
                prefetch_latest_handle_await_ms: ms_cell_from_us(
                    prefetch_latest_handle_await_us_max,
                ),
                prefetch_avg_etcd_put_ms: ms_cell_from_us(prefetch_avg_etcd_put_us_max),
                prefetch_latest_etcd_put_ms: ms_cell_from_us(prefetch_latest_etcd_put_us_max),
                prefetch_inflight_queue_size: i64_cell_from_opt_f64(
                    prefetch_inflight_queue_size_max,
                ),
                prefetch_target_inflight: i64_cell_from_opt_f64(prefetch_target_inflight_max),
                get_one_avg_total_ms: ms_cell_from_us(get_one_avg_total_us_max),
                get_one_max_total_ms: ms_cell_from_us(get_one_max_total_us_max),
                get_one_avg_wait_rx_ms: ms_cell_from_us(get_one_avg_wait_rx_us_max),
                get_one_max_wait_rx_ms: ms_cell_from_us(get_one_max_wait_rx_us_max),
                get_one_avg_signal_ms: ms_cell_from_us(get_one_avg_signal_us_max),
                get_one_max_signal_ms: ms_cell_from_us(get_one_max_signal_us_max),
                get_one_avg_post_ms: ms_cell_from_us(get_one_avg_post_us_max),
                get_one_max_post_ms: ms_cell_from_us(get_one_max_post_us_max),
                get_one_window_calls_sum: i64_cell_from_opt_f64(get_one_window_calls_sum),
                get_one_window_timeouts_sum: i64_cell_from_opt_f64(get_one_window_timeouts_sum),
                producer_nonblocking_latest_phase_calls: i64_cell_from_opt_f64(
                    latest_producer_nonblocking_phase_calls,
                ),
                producer_nonblocking_latest_phase_rps: hz_cell_from_opt_f64(
                    latest_producer_nonblocking_phase_rps,
                ),
                producer_nonblocking_latest_interval: latest_interval_text(
                    latest_producer_nonblocking_begin_unix_ms,
                    latest_producer_nonblocking_end_unix_ms,
                ),
                consumer_nonblocking_latest_phase_calls: i64_cell_from_opt_f64(
                    latest_consumer_nonblocking_phase_calls,
                ),
                consumer_nonblocking_latest_phase_rps: hz_cell_from_opt_f64(
                    latest_consumer_nonblocking_phase_rps,
                ),
                consumer_nonblocking_latest_interval: latest_interval_text(
                    latest_consumer_nonblocking_begin_unix_ms,
                    latest_consumer_nonblocking_end_unix_ms,
                ),
            };

            let mut channel_members: Vec<MqMemberView> = Vec::new();

            for p in &ch.producers {
                channel_members.push(MqMemberView {
                    channel_unique_keys: channel_unique_keys.clone(),
                    kind: "producer".to_string(),
                    idx: p.producer_idx.clone(),
                    status: mq_status_str(p.status).to_string(),
                    external_client_id: p
                        .external_client_id
                        .clone()
                        .unwrap_or_else(|| "N/A".to_string()),
                    owner_id: p.owner_id.clone().unwrap_or_else(|| "N/A".to_string()),
                    produce_offset: p
                        .produce_offset
                        .map(|v| v.to_string())
                        .unwrap_or_else(|| "N/A".to_string()),
                    consume_offset: p
                        .consume_offset
                        .map(|v| v.to_string())
                        .unwrap_or_else(|| "N/A".to_string()),
                    kvclient_sub_cluster: "N/A".to_string(),
                    prefetch_avg_get_handle_ms: NumCellView::na(),
                    prefetch_latest_get_handle_ms: NumCellView::na(),
                    prefetch_avg_handle_await_ms: NumCellView::na(),
                    prefetch_latest_handle_await_ms: NumCellView::na(),
                    prefetch_avg_etcd_put_ms: NumCellView::na(),
                    prefetch_latest_etcd_put_ms: NumCellView::na(),
                    prefetch_inflight_queue_size: NumCellView::na(),
                    prefetch_target_inflight: NumCellView::na(),
                    get_one_avg_total_ms: NumCellView::na(),
                    get_one_max_total_ms: NumCellView::na(),
                    get_one_avg_wait_rx_ms: NumCellView::na(),
                    get_one_max_wait_rx_ms: NumCellView::na(),
                    get_one_avg_signal_ms: NumCellView::na(),
                    get_one_max_signal_ms: NumCellView::na(),
                    get_one_avg_post_ms: NumCellView::na(),
                    get_one_max_post_ms: NumCellView::na(),
                    get_one_window_calls: NumCellView::na(),
                    get_one_window_timeouts: NumCellView::na(),
                    nonblocking_latest_phase_calls: i64_cell_from_opt_f64(
                        p.nonblocking_latest_phase_calls,
                    ),
                    nonblocking_latest_phase_rps: hz_cell_from_opt_f64(
                        p.nonblocking_latest_phase_rps,
                    ),
                    nonblocking_latest_interval: latest_interval_text(
                        p.nonblocking_latest_begin_unix_ms,
                        p.nonblocking_latest_end_unix_ms,
                    ),
                });
            }
            for c in &ch.consumers {
                channel_members.push(MqMemberView {
                    channel_unique_keys: channel_unique_keys.clone(),
                    kind: "consumer".to_string(),
                    idx: c.consumer_idx.clone(),
                    status: mq_status_str(c.status).to_string(),
                    external_client_id: c
                        .external_client_id
                        .clone()
                        .unwrap_or_else(|| "N/A".to_string()),
                    owner_id: c.owner_id.clone().unwrap_or_else(|| "N/A".to_string()),
                    produce_offset: "N/A".to_string(),
                    consume_offset: "N/A".to_string(),
                    kvclient_sub_cluster: c
                        .kvclient_sub_cluster
                        .clone()
                        .unwrap_or_else(|| "N/A".to_string()),
                    prefetch_avg_get_handle_ms: ms_cell_from_us(c.prefetch_avg_get_handle_us),
                    prefetch_latest_get_handle_ms: ms_cell_from_us(c.prefetch_latest_get_handle_us),
                    prefetch_avg_handle_await_ms: ms_cell_from_us(c.prefetch_avg_handle_await_us),
                    prefetch_latest_handle_await_ms: ms_cell_from_us(
                        c.prefetch_latest_handle_await_us,
                    ),
                    prefetch_avg_etcd_put_ms: ms_cell_from_us(c.prefetch_avg_etcd_put_us),
                    prefetch_latest_etcd_put_ms: ms_cell_from_us(c.prefetch_latest_etcd_put_us),
                    prefetch_inflight_queue_size: i64_cell_from_opt_f64(
                        c.prefetch_inflight_queue_size,
                    ),
                    prefetch_target_inflight: i64_cell_from_opt_f64(c.prefetch_target_inflight),
                    get_one_avg_total_ms: ms_cell_from_us(c.get_one_avg_total_us),
                    get_one_max_total_ms: ms_cell_from_us(c.get_one_max_total_us),
                    get_one_avg_wait_rx_ms: ms_cell_from_us(c.get_one_avg_wait_rx_us),
                    get_one_max_wait_rx_ms: ms_cell_from_us(c.get_one_max_wait_rx_us),
                    get_one_avg_signal_ms: ms_cell_from_us(c.get_one_avg_signal_us),
                    get_one_max_signal_ms: ms_cell_from_us(c.get_one_max_signal_us),
                    get_one_avg_post_ms: ms_cell_from_us(c.get_one_avg_post_us),
                    get_one_max_post_ms: ms_cell_from_us(c.get_one_max_post_us),
                    get_one_window_calls: i64_cell_from_opt_f64(c.get_one_window_calls),
                    get_one_window_timeouts: i64_cell_from_opt_f64(c.get_one_window_timeouts),
                    nonblocking_latest_phase_calls: i64_cell_from_opt_f64(
                        c.nonblocking_latest_phase_calls,
                    ),
                    nonblocking_latest_phase_rps: hz_cell_from_opt_f64(
                        c.nonblocking_latest_phase_rps,
                    ),
                    nonblocking_latest_interval: latest_interval_text(
                        c.nonblocking_latest_begin_unix_ms,
                        c.nonblocking_latest_end_unix_ms,
                    ),
                });
            }

            channel_groups.push(MqChannelGroupView {
                channel: channel_view,
                members: channel_members,
            });
        }
    }

    channel_groups.sort_by_key(|group| group.channel.chan_id);

    let template = MqTemplate {
        header,
        build: build_info_view(),
        warnings: snapshot.warnings.clone(),
        channel_groups,
        refresh_secs: AUTO_REFRESH_INTERVAL.as_secs(),
    };
    template.render().unwrap()
}

pub fn render_cluster(snapshot: &ClusterSnapshot) -> String {
    if snapshot.member_kind == MemberKind::Mq {
        return render_mq_cluster(snapshot);
    }
    render_table_cluster(snapshot)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
enum TopologyNodeKindWire {
    SubCluster,
    Machine,
    FsMountpoint,
    FsShm,
    FsExport,
    FsTmp,
    Owner,
    External,
    MqProducer,
    MqConsumer,
    OrphanMachine,
    OrphanExternal,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
enum TopologyLinkKindWire {
    Solid,
    Dashed,
}

#[derive(Debug, Clone, Serialize)]
struct TopologyNodeWire {
    id: String,
    kind: TopologyNodeKindWire,
    label: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    meta: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    detail: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    fs_used_bytes: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    fs_total_bytes: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    fs_read_rps: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    fs_write_rps: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tx_mbps_max: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    rx_mbps_max: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    process_cpu_usage_percent: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    process_resident_memory_bytes: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    container_memory_usage_bytes: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    container_memory_limit_bytes: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    machine_process_resident_memory_sum_bytes: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    machine_cpu_used_cores: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    machine_cpu_total_cores: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    machine_memory_used_bytes: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    machine_memory_total_bytes: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    machine_container_memory_usage_bytes: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    machine_container_memory_limit_bytes: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    machine_rdma_tx_mbps_cur: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    machine_rdma_tx_mbps_max: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    machine_rdma_enabled_mbps_max: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    machine_rdma_pcie_mbps_max: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    machine_rdma_rx_mbps_cur: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    machine_rdma_rx_mbps_max: Option<f64>,
}

#[derive(Debug, Clone, Serialize)]
struct TopologyLinkWire {
    source: String,
    target: String,
    kind: TopologyLinkKindWire,
    #[serde(skip_serializing_if = "Option::is_none")]
    label: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tx_mbps: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    rx_mbps: Option<f64>,
}

#[derive(Debug, Clone, Serialize)]
struct TopologyWire {
    nodes: Vec<TopologyNodeWire>,
    links: Vec<TopologyLinkWire>,
}

fn json_escape_for_html_script_tag(json: &str) -> String {
    // English note:
    // - We embed topology JSON in a `<script type="application/json">...</script>` element.
    // - If member ids contain `<`/`&`, the HTML parser could treat them as markup.
    // - Escape a minimal set to keep the JSON safe in HTML while remaining valid JSON.
    json.replace('&', "\\u0026")
        .replace('<', "\\u003c")
        .replace('>', "\\u003e")
}

fn build_kv_topology_view(snapshot: &ClusterSnapshot) -> Option<TopologyView> {
    const SUB_CLUSTER_MISSING_LABEL: &str = "(missing)";

    if snapshot.member_kind != MemberKind::Kv {
        return None;
    }

    fn normalized_sub_cluster(sc: Option<&str>) -> String {
        match sc.map(|s| s.trim()).filter(|s| !s.is_empty()) {
            Some(s) => s.to_string(),
            None => SUB_CLUSTER_MISSING_LABEL.to_string(),
        }
    }

    fn nic_group_label_and_meta(group_key: &str) -> (String, Option<String>) {
        if group_key.starts_with("rdma:") {
            return (
                "machine".to_string(),
                Some("match=rdma_nic_set".to_string()),
            );
        }
        if group_key.starts_with("no-rdma-owner:") {
            return (
                "machine".to_string(),
                Some("match=no_rdma_owner_unique".to_string()),
            );
        }
        if let Some(rest) = group_key.strip_prefix("no-rdma:") {
            return ("machine".to_string(), Some(rest.to_string()));
        }
        ("machine".to_string(), Some(group_key.to_string()))
    }

    #[derive(Clone)]
    struct OwnerRec {
        owner_id: String,
        externals: Vec<String>,
    }

    #[derive(Clone)]
    struct MemberMetaRec {
        role: MemberRole,
        group_key: String,
        rdma_usage_meta: Option<String>,
        sub_cluster: String,
        process_cpu_usage_percent: Option<f64>,
        process_resident_memory_bytes: Option<f64>,
        container_memory_usage_bytes: Option<f64>,
        container_memory_limit_bytes: Option<f64>,
        node_cpu_usage_percent: Option<f64>,
        node_cpu_logical_cores: Option<f64>,
        node_memory_usage_bytes: Option<f64>,
        node_memory_total_bytes: Option<f64>,
        fs_read_rps: Option<f64>,
        fs_write_rps: Option<f64>,
    }

    let owner_ext_max_by_owner: HashMap<String, (Option<f64>, Option<f64>)> = snapshot
        .kv_topology_owner_external_max
        .iter()
        .map(|r| (r.owner_id.clone(), (r.tx_mbps_max, r.rx_mbps_max)))
        .collect();
    let mut machine_ext_max_by_machine: HashMap<String, (Option<f64>, Option<f64>)> =
        HashMap::new();
    for r in &snapshot.kv_topology_machine_external_max {
        let entry = machine_ext_max_by_machine
            .entry(r.machine_key.clone())
            .or_insert((None, None));
        if let Some(tx) = r.tx_mbps_max {
            if tx.is_finite() && tx > 0.0 {
                entry.0 = Some(entry.0.map_or(tx, |p| p.max(tx)));
            }
        }
        if let Some(rx) = r.rx_mbps_max {
            if rx.is_finite() && rx > 0.0 {
                entry.1 = Some(entry.1.map_or(rx, |p| p.max(rx)));
            }
        }
    }

    let mut member_meta_by_id: HashMap<String, MemberMetaRec> = HashMap::new();
    let mut member_snapshot_by_id: HashMap<String, &crate::model::MemberSnapshot> = HashMap::new();
    for node in &snapshot.nodes {
        for m in &node.members {
            let group_key = m.topology_nic_group_key();
            let sc = normalized_sub_cluster(m.sub_cluster.as_deref());
            member_snapshot_by_id.insert(m.member_id.clone(), m);
            member_meta_by_id.insert(
                m.member_id.clone(),
                MemberMetaRec {
                    role: m.role,
                    group_key,
                    rdma_usage_meta: m.topology_rdma_usage_meta(),
                    sub_cluster: sc,
                    process_cpu_usage_percent: m.process_cpu_usage_percent,
                    process_resident_memory_bytes: m.process_resident_memory_bytes,
                    container_memory_usage_bytes: m.container_memory_usage_bytes,
                    container_memory_limit_bytes: m.container_memory_limit_bytes,
                    node_cpu_usage_percent: m.node_cpu_usage_percent,
                    node_cpu_logical_cores: m.node_cpu_logical_cores,
                    node_memory_usage_bytes: m.node_memory_usage_bytes,
                    node_memory_total_bytes: m.node_memory_total_bytes,
                    fs_read_rps: m.fs_read_rps,
                    fs_write_rps: m.fs_write_rps,
                },
            );
        }
    }

    let mut peer_rates_by_node: HashMap<String, Vec<(String, f64, f64)>> = HashMap::new();
    let mut rate_by_node_peer: HashMap<(String, String), (f64, f64)> = HashMap::new();
    for r in &snapshot.kv_peer_network {
        let tx = r.tx_mbps.unwrap_or(0.0);
        let rx = r.rx_mbps.unwrap_or(0.0);
        peer_rates_by_node
            .entry(r.node.clone())
            .or_default()
            .push((r.peer.clone(), tx, rx));
        rate_by_node_peer.insert((r.node.clone(), r.peer.clone()), (tx, rx));
    }

    #[derive(Clone, Copy)]
    struct MachineResourceAgg {
        rss_sum_bytes: f64,
        cpu_usage_percent: Option<f64>,
        cpu_logical_cores: Option<f64>,
        mem_used_bytes: Option<f64>,
        mem_total_bytes: Option<f64>,
        container_mem_used_bytes: Option<f64>,
        container_mem_limit_bytes: Option<f64>,
    }

    #[derive(Clone, Copy, Default)]
    struct MachineRdmaAgg {
        tx_mbps_cur: f64,
        tx_mbps_max: f64,
        enabled_mbps_max: f64,
        pcie_mbps_max: f64,
        rx_mbps_cur: f64,
        rx_mbps_max: f64,
    }

    #[derive(Clone, Default)]
    struct HostRdmaRec {
        representative_rank: i32,
        representative_node_id: Option<String>,
        netdev_speed_mbps: BTreeMap<String, f64>,
        enabled_netdev_speed_mbps: BTreeMap<String, f64>,
        pcie_device_mbps: BTreeMap<String, f64>,
    }

    fn member_host_key(member: &crate::model::MemberSnapshot) -> String {
        if let Some(ip) = member
            .accessible_ip
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
        {
            return format!("ip:{ip}");
        }
        if let Some(hostname) = member
            .hostname
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
        {
            return format!("host:{hostname}");
        }
        format!("member:{}", member.member_id)
    }

    fn member_host_display(
        member: &crate::model::MemberSnapshot,
        fallback_host_key: &str,
    ) -> String {
        let hostname = member
            .hostname
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty());
        let ip = member
            .accessible_ip
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty());
        match (hostname, ip) {
            (Some(hostname), Some(ip)) => format!("host={hostname} ip={ip}"),
            (Some(hostname), None) => format!("host={hostname}"),
            (None, Some(ip)) => format!("ip={ip}"),
            (None, None) => fallback_host_key.to_string(),
        }
    }

    fn member_routing_rank(role: MemberRole) -> i32 {
        match role {
            MemberRole::OwnerClient => 0,
            MemberRole::ExternalClient | MemberRole::SideTransferWorker => 1,
            _ => 2,
        }
    }

    fn fmt_topology_rate_mbps(value_mbps: f64) -> String {
        if !value_mbps.is_finite() || value_mbps <= 0.0 {
            return "0".to_string();
        }
        let value_gbps = value_mbps / 1000.0;
        if value_gbps >= 100.0 {
            format!("{value_gbps:.0}G")
        } else if value_gbps >= 10.0 {
            format!("{value_gbps:.1}G")
        } else if value_gbps >= 1.0 {
            format!("{value_gbps:.2}G")
        } else {
            format!("{value_mbps:.0}M")
        }
    }

    fn fmt_topology_bytes(value_bytes: f64) -> String {
        if !value_bytes.is_finite() || value_bytes < 0.0 {
            return "N/A".to_string();
        }
        const KIB: f64 = 1024.0;
        const MIB: f64 = KIB * 1024.0;
        const GIB: f64 = MIB * 1024.0;
        const TIB: f64 = GIB * 1024.0;
        if value_bytes >= TIB {
            format!("{:.2}TiB", value_bytes / TIB)
        } else if value_bytes >= GIB {
            format!("{:.2}GiB", value_bytes / GIB)
        } else if value_bytes >= MIB {
            format!("{:.2}MiB", value_bytes / MIB)
        } else if value_bytes >= KIB {
            format!("{:.2}KiB", value_bytes / KIB)
        } else {
            format!("{value_bytes:.0}B")
        }
    }

    // FS view model inputs:
    // - `fs_mount_fs_*` gauges provide `df -h <target_dir>`-equivalent used/total (statvfs) and the resolved mount point.
    // - `shared_mem_dir` from KV membership is only used to annotate shm user counts.
    // - `fs_export_registry` is only used to annotate export names / agent ids.

    #[derive(Clone)]
    struct FsExportAgg {
        export_name: String,
        agent_instance_key: String,
        remote_root_dir_abs: String,
        updated_unix_ms: i64,
    }

    // Step 1) Extract:
    // - (machine_key, owner_id, externals[]) per observed "share-group node" (KV membership).
    // - orphan external-like clients (missing owner attachment).
    //
    // Note: A "node" here is a Fluxon CLI virtual node keyed by owner_id. In normal operation it
    // should contain exactly one owner_client. If it contains 0 owners, all external-like members
    // are orphaned.
    // If it contains multiple owners (unexpected), we still show each owner, but we do NOT attach
    // externals to any owner to avoid incorrect ownership inference.
    //
    // The topology is clustered by RDMA NIC-set identity. Sub-cluster labels are treated
    // as per-instance metadata and are rendered on a single "sub_cluster" node per machine.
    use std::collections::{BTreeMap, BTreeSet, HashMap};
    let mut member_machine_by_id: HashMap<String, String> = HashMap::new();
    let mut owners_by_machine: BTreeMap<String, Vec<OwnerRec>> = BTreeMap::new();
    let mut orphan_externals_by_machine: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for node in &snapshot.nodes {
        let mut owners: Vec<&crate::model::MemberSnapshot> = node
            .members
            .iter()
            .filter(|m| m.role == MemberRole::OwnerClient)
            .collect();
        owners.sort_by(|a, b| a.member_id.cmp(&b.member_id));

        let mut externals: Vec<&crate::model::MemberSnapshot> = node
            .members
            .iter()
            .filter(|m| m.role.is_external_like())
            .collect();
        externals.sort_by(|a, b| a.member_id.cmp(&b.member_id));

        if owners.len() == 1 {
            let owner = owners[0];
            let Some(owner_meta) = member_meta_by_id.get(owner.member_id.as_str()) else {
                continue;
            };
            let machine_key = owner_meta.group_key.clone();

            let mut external_ids: Vec<String> =
                externals.iter().map(|m| m.member_id.clone()).collect();
            external_ids.sort();
            external_ids.dedup();

            member_machine_by_id.insert(owner.member_id.clone(), machine_key.clone());
            for ext_id in &external_ids {
                member_machine_by_id.insert(ext_id.clone(), machine_key.clone());
            }

            owners_by_machine
                .entry(machine_key)
                .or_default()
                .push(OwnerRec {
                    owner_id: owner.member_id.clone(),
                    externals: external_ids,
                });
            continue;
        }

        // 0 or >1 owners.
        if !owners.is_empty() {
            for owner in owners {
                let Some(owner_meta) = member_meta_by_id.get(owner.member_id.as_str()) else {
                    continue;
                };
                let machine_key = owner_meta.group_key.clone();
                member_machine_by_id.insert(owner.member_id.clone(), machine_key.clone());
                owners_by_machine
                    .entry(machine_key)
                    .or_default()
                    .push(OwnerRec {
                        owner_id: owner.member_id.clone(),
                        externals: Vec::new(),
                    });
            }
        }

        // Orphan external-like member: visible in membership, but missing share-group owner_id
        // mapping, or
        // "node" contains multiple owners and we refuse to guess the attachment.
        for m in externals {
            let Some(meta) = member_meta_by_id.get(m.member_id.as_str()) else {
                continue;
            };
            let machine_key = meta.group_key.clone();
            member_machine_by_id.insert(m.member_id.clone(), machine_key.clone());
            orphan_externals_by_machine
                .entry(machine_key)
                .or_default()
                .push(m.member_id.clone());
        }
    }

    let mut rdma_rate_by_node_netdev: HashMap<(String, String), (f64, f64)> = HashMap::new();
    for r in &snapshot.rdma_netdev_network {
        let netdev = r.netdev.trim();
        if netdev.is_empty() {
            continue;
        }
        let tx = r
            .tx_mbps
            .filter(|value| value.is_finite() && *value > 0.0)
            .unwrap_or(0.0);
        let rx = r
            .rx_mbps
            .filter(|value| value.is_finite() && *value > 0.0)
            .unwrap_or(0.0);
        rdma_rate_by_node_netdev.insert((r.node.clone(), netdev.to_string()), (tx, rx));
    }

    let mut hosts_by_machine: BTreeMap<String, BTreeMap<String, HostRdmaRec>> = BTreeMap::new();
    for node in &snapshot.nodes {
        for member in &node.members {
            let Some(meta) = member_meta_by_id.get(&member.member_id) else {
                continue;
            };
            let machine_key = member_machine_by_id
                .get(&member.member_id)
                .cloned()
                .unwrap_or_else(|| meta.group_key.clone());
            let host_key = member_host_key(member);
            let host_entry = hosts_by_machine
                .entry(machine_key)
                .or_default()
                .entry(host_key)
                .or_default();
            let rank = member_routing_rank(member.role);
            let should_replace_repr = host_entry.representative_node_id.is_none()
                || rank < host_entry.representative_rank
                || (rank == host_entry.representative_rank
                    && host_entry
                        .representative_node_id
                        .as_ref()
                        .is_some_and(|current| member.member_id < *current));
            if should_replace_repr {
                host_entry.representative_rank = rank;
                host_entry.representative_node_id = Some(member.member_id.clone());
            }
            for port in &member.rdma_ports {
                if !port.detected {
                    continue;
                }
                if let Some(netdev) = port
                    .netdev
                    .as_deref()
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                {
                    if let Some(speed_gbps) = port.speed_gbps.filter(|value| *value > 0) {
                        let speed_mbps = speed_gbps as f64 * 1000.0;
                        host_entry
                            .netdev_speed_mbps
                            .entry(netdev.to_string())
                            .and_modify(|value| *value = value.max(speed_mbps))
                            .or_insert(speed_mbps);
                        if port.effective_enabled {
                            host_entry
                                .enabled_netdev_speed_mbps
                                .entry(netdev.to_string())
                                .and_modify(|value| *value = value.max(speed_mbps))
                                .or_insert(speed_mbps);
                        }
                    }
                }
                if let (Some(pci_bdf), Some(pcie_max_bandwidth_mbps)) = (
                    port.pci_bdf
                        .as_deref()
                        .map(str::trim)
                        .filter(|s| !s.is_empty()),
                    port.pcie_max_bandwidth_mbps.filter(|value| *value > 0),
                ) {
                    let pcie_max_mbps = pcie_max_bandwidth_mbps as f64;
                    host_entry
                        .pcie_device_mbps
                        .entry(pci_bdf.to_string())
                        .and_modify(|value| *value = value.max(pcie_max_mbps))
                        .or_insert(pcie_max_mbps);
                }
            }
        }
    }

    let mut machine_rdma_detail_lines_by_machine: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for (machine_key, hosts) in &hosts_by_machine {
        let mut lines: Vec<String> = Vec::new();
        for (host_key, host) in hosts {
            let Some(representative_node_id) = host.representative_node_id.as_ref() else {
                continue;
            };
            let Some(member) = member_snapshot_by_id.get(representative_node_id) else {
                continue;
            };
            lines.push(member_host_display(member, host_key.as_str()));
            if let Some(rdma_meta) = member.topology_rdma_meta() {
                for line in rdma_meta.lines() {
                    let trimmed = line.trim();
                    if trimmed.is_empty() {
                        continue;
                    }
                    lines.push(format!("  {trimmed}"));
                }
            }
        }
        if !lines.is_empty() {
            machine_rdma_detail_lines_by_machine.insert(machine_key.clone(), lines);
        }
    }

    let mut machine_rdma_by_machine: HashMap<String, MachineRdmaAgg> = HashMap::new();
    for (machine_key, hosts) in hosts_by_machine {
        let mut agg = MachineRdmaAgg::default();
        for host in hosts.into_values() {
            let Some(representative_node_id) = host.representative_node_id else {
                continue;
            };
            for (netdev, speed_mbps) in host.netdev_speed_mbps {
                agg.tx_mbps_max += speed_mbps;
                agg.rx_mbps_max += speed_mbps;
                if let Some((tx, rx)) = rdma_rate_by_node_netdev
                    .get(&(representative_node_id.clone(), netdev.clone()))
                    .copied()
                {
                    if tx.is_finite() && tx > 0.0 {
                        agg.tx_mbps_cur += tx;
                    }
                    if rx.is_finite() && rx > 0.0 {
                        agg.rx_mbps_cur += rx;
                    }
                }
            }
            for speed_mbps in host.enabled_netdev_speed_mbps.into_values() {
                agg.enabled_mbps_max += speed_mbps;
            }
            for pcie_max_mbps in host.pcie_device_mbps.into_values() {
                agg.pcie_mbps_max += pcie_max_mbps;
            }
        }
        if agg.tx_mbps_max > 0.0
            || agg.rx_mbps_max > 0.0
            || agg.enabled_mbps_max > 0.0
            || agg.pcie_mbps_max > 0.0
        {
            machine_rdma_by_machine.insert(machine_key, agg);
        }
    }

    if owners_by_machine.is_empty()
        && orphan_externals_by_machine.is_empty()
        && snapshot.fs_mount_fs.is_empty()
        && snapshot.fs_export_registry.is_empty()
        && snapshot.fs_mount_registry.is_empty()
    {
        return None;
    }

    // Group-scoped sub-cluster label mapping (for the `sub_cluster` node meta).
    let mut sub_clusters_by_machine: BTreeMap<String, BTreeMap<String, BTreeSet<String>>> =
        BTreeMap::new();
    for (member_id, meta) in &member_meta_by_id {
        let mk = member_machine_by_id
            .get(member_id)
            .cloned()
            .unwrap_or_else(|| meta.group_key.clone());
        sub_clusters_by_machine
            .entry(mk)
            .or_default()
            .entry(meta.sub_cluster.clone())
            .or_default()
            .insert(member_id.clone());
    }

    // Group-scoped resource aggregation:
    // - For external processes attached to an owner, the group id is derived from the owner
    //   (not the external's own metadata) to avoid `uniq:*` splits.
    let mut machine_resource_by_machine: HashMap<String, MachineResourceAgg> = HashMap::new();
    for (member_id, meta) in &member_meta_by_id {
        let mk = member_machine_by_id
            .get(member_id)
            .cloned()
            .unwrap_or_else(|| meta.group_key.clone());
        let entry = machine_resource_by_machine
            .entry(mk)
            .or_insert(MachineResourceAgg {
                rss_sum_bytes: 0.0,
                cpu_usage_percent: None,
                cpu_logical_cores: None,
                mem_used_bytes: None,
                mem_total_bytes: None,
                container_mem_used_bytes: None,
                container_mem_limit_bytes: None,
            });
        if let Some(rss) = meta.process_resident_memory_bytes {
            if rss.is_finite() && rss > 0.0 {
                entry.rss_sum_bytes += rss;
            }
        }
        if let Some(used) = meta.node_memory_usage_bytes {
            if used.is_finite() && used > 0.0 {
                entry.mem_used_bytes = Some(entry.mem_used_bytes.map_or(used, |p| p.max(used)));
            }
        }
        if let Some(total) = meta.node_memory_total_bytes {
            if total.is_finite() && total > 0.0 {
                entry.mem_total_bytes = Some(entry.mem_total_bytes.map_or(total, |p| p.max(total)));
            }
        }
        if let Some(used) = meta.container_memory_usage_bytes {
            if used.is_finite() && used > 0.0 {
                entry.container_mem_used_bytes =
                    Some(entry.container_mem_used_bytes.map_or(used, |p| p.max(used)));
            }
        }
        if let Some(limit) = meta.container_memory_limit_bytes {
            if limit.is_finite() && limit > 0.0 {
                entry.container_mem_limit_bytes = Some(
                    entry
                        .container_mem_limit_bytes
                        .map_or(limit, |p| p.max(limit)),
                );
            }
        }
        if let Some(pct) = meta.node_cpu_usage_percent {
            if pct.is_finite() {
                let v = pct.clamp(0.0, 100.0);
                entry.cpu_usage_percent = Some(entry.cpu_usage_percent.map_or(v, |p| p.max(v)));
            }
        }
        if let Some(cores) = meta.node_cpu_logical_cores {
            if cores.is_finite() && cores > 0.0 {
                entry.cpu_logical_cores =
                    Some(entry.cpu_logical_cores.map_or(cores, |p| p.max(cores)));
            }
        }
    }

    // FS view model:
    // - Fluxon reports `df -h <target_dir>` results via statvfs.
    // - Each sample includes the target directory and the resolved mount point (from /proc/self/mountinfo).
    // - Topology groups targets by mount point and displays capacity on the mount point node.
    #[derive(Clone, Default)]
    struct FsMountpointAgg {
        used_bytes: Option<f64>,
        total_bytes: Option<f64>,
        export_targets: BTreeSet<String>,
        shm_targets: BTreeSet<String>,
        tmp_targets: BTreeSet<String>,
    }

    #[derive(Clone, Default)]
    struct ShmFileAgg {
        logical_sum_bytes: f64,
        allocated_sum_bytes: f64,
        files: Vec<(String, Option<f64>, Option<f64>)>,
    }

    let mut fs_mountpoints_by_machine: BTreeMap<String, BTreeMap<String, FsMountpointAgg>> =
        BTreeMap::new();
    let mut fs_exports_by_machine: BTreeMap<String, Vec<FsExportAgg>> = BTreeMap::new();
    let mut shm_paths_by_machine: BTreeMap<String, BTreeMap<String, usize>> = BTreeMap::new();
    let mut shm_files_by_machine: BTreeMap<String, BTreeMap<String, ShmFileAgg>> = BTreeMap::new();

    // Mount filesystem stats (used/total) from Prometheus.
    for s in &snapshot.fs_mount_fs {
        let target_dir_abs = normalize_abs_dir_label(s.target_dir_abs.as_str());
        let mountpoint_dir_abs = normalize_abs_dir_label(s.mountpoint_dir_abs.as_str());
        if target_dir_abs.is_empty() || mountpoint_dir_abs.is_empty() {
            continue;
        }
        let Some(meta) = member_meta_by_id.get(&s.node) else {
            continue;
        };
        let mk = member_machine_by_id
            .get(&s.node)
            .cloned()
            .unwrap_or_else(|| meta.group_key.clone());
        let agg = fs_mountpoints_by_machine
            .entry(mk)
            .or_default()
            .entry(mountpoint_dir_abs.clone())
            .or_default();
        if let Some(used) = s.used_bytes {
            if used.is_finite() && used >= 0.0 {
                agg.used_bytes = Some(agg.used_bytes.map_or(used, |p| p.max(used)));
            }
        }
        if let Some(total) = s.total_bytes {
            if total.is_finite() && total > 0.0 {
                agg.total_bytes = Some(agg.total_bytes.map_or(total, |p| p.max(total)));
            }
        }

        match s.mount_kind {
            crate::model::FsMountKindWire::Export => {
                agg.export_targets.insert(target_dir_abs);
            }
            crate::model::FsMountKindWire::Shm => {
                agg.shm_targets.insert(target_dir_abs);
            }
            crate::model::FsMountKindWire::Tmp => {
                agg.tmp_targets.insert(target_dir_abs);
            }
            crate::model::FsMountKindWire::Unknown => {}
        }
    }

    // FS export roots from etcd registry.
    for r in &snapshot.fs_export_registry {
        let agent = r.agent_instance_key.trim();
        if agent.is_empty() {
            continue;
        }
        let Some(meta) = member_meta_by_id.get(agent) else {
            continue;
        };
        let mk = member_machine_by_id
            .get(agent)
            .cloned()
            .unwrap_or_else(|| meta.group_key.clone());
        fs_exports_by_machine
            .entry(mk)
            .or_default()
            .push(FsExportAgg {
                export_name: r.export_name.clone(),
                agent_instance_key: r.agent_instance_key.clone(),
                remote_root_dir_abs: normalize_abs_dir_label(r.remote_root_dir_abs.as_str()),
                updated_unix_ms: r.updated_unix_ms,
            });
    }
    for (_mk, exports) in fs_exports_by_machine.iter_mut() {
        exports.sort_by(|a, b| {
            let c = a.export_name.cmp(&b.export_name);
            if c != std::cmp::Ordering::Equal {
                return c;
            }
            a.agent_instance_key.cmp(&b.agent_instance_key)
        });
        exports.dedup_by(|a, b| {
            a.export_name == b.export_name
                && a.agent_instance_key == b.agent_instance_key
                && a.remote_root_dir_abs == b.remote_root_dir_abs
        });
    }

    // shm directories from KV membership.
    for node in &snapshot.nodes {
        for m in &node.members {
            let Some(shm) = m
                .shared_mem_dir
                .as_deref()
                .map(|s| s.trim())
                .filter(|s| !s.is_empty())
            else {
                continue;
            };
            let shm_dir_abs = normalize_abs_dir_label(shm);
            if shm_dir_abs.is_empty() {
                continue;
            }
            let Some(meta) = member_meta_by_id.get(&m.member_id) else {
                continue;
            };
            let mk = member_machine_by_id
                .get(&m.member_id)
                .cloned()
                .unwrap_or_else(|| meta.group_key.clone());
            *shm_paths_by_machine
                .entry(mk)
                .or_default()
                .entry(shm_dir_abs)
                .or_insert(0) += 1;
        }
    }
    for s in &snapshot.shm_files {
        let shm_dir_abs = normalize_abs_dir_label(s.shm_dir_abs.as_str());
        if shm_dir_abs.is_empty() {
            continue;
        }
        let Some(meta) = member_meta_by_id.get(&s.node) else {
            continue;
        };
        let mk = member_machine_by_id
            .get(&s.node)
            .cloned()
            .unwrap_or_else(|| meta.group_key.clone());
        let agg = shm_files_by_machine
            .entry(mk)
            .or_default()
            .entry(shm_dir_abs)
            .or_default();
        if let Some(v) = s.logical_size_bytes.filter(|v| v.is_finite() && *v >= 0.0) {
            agg.logical_sum_bytes += v;
        }
        if let Some(v) = s.allocated_bytes.filter(|v| v.is_finite() && *v >= 0.0) {
            agg.allocated_sum_bytes += v;
        }
        agg.files.push((
            s.file_path_abs.clone(),
            s.logical_size_bytes,
            s.allocated_bytes,
        ));
    }
    for per_machine in shm_files_by_machine.values_mut() {
        for agg in per_machine.values_mut() {
            agg.files.sort_by(|a, b| {
                let a_alloc = a.2.unwrap_or(0.0);
                let b_alloc = b.2.unwrap_or(0.0);
                b_alloc
                    .partial_cmp(&a_alloc)
                    .unwrap_or(std::cmp::Ordering::Equal)
                    .then_with(|| a.0.cmp(&b.0))
            });
        }
    }

    // Step 2) Build a hierarchical graph (machine clustered):
    // sub_cluster(machine-local metadata) -> machine -> owner -> external.
    let mut nodes_out: Vec<TopologyNodeWire> = Vec::new();
    let mut links_out: Vec<TopologyLinkWire> = Vec::new();
    let mut external_node_id_by_member_id: HashMap<String, String> = HashMap::new();
    let mut export_dashed_links: Vec<(String, String)> = Vec::new();

    fn node_id_sub_cluster(machine_key: &str) -> String {
        format!("group:{machine_key}/sub_cluster")
    }

    fn node_id_machine(machine_key: &str) -> String {
        format!("group:{machine_key}")
    }

    fn node_id_owner(machine_key: &str, owner_id: &str) -> String {
        format!("group:{machine_key}/owner:{owner_id}")
    }

    fn node_id_external(machine_key: &str, owner_id: &str, external_id: &str) -> String {
        format!("group:{machine_key}/owner:{owner_id}/external:{external_id}")
    }

    fn node_id_orphan_bucket(machine_key: &str) -> String {
        format!("group:{machine_key}/orphan_bucket")
    }

    fn node_id_orphan_external(machine_key: &str, external_id: &str) -> String {
        format!("group:{machine_key}/orphan_bucket/external:{external_id}")
    }

    fn node_id_fs_shm(machine_key: &str, shm_dir_abs: &str) -> String {
        format!("group:{machine_key}/fs_shm:{shm_dir_abs}")
    }

    fn node_id_fs_mountpoint(machine_key: &str, mountpoint_dir_abs: &str) -> String {
        format!("group:{machine_key}/fs_mountpoint:{mountpoint_dir_abs}")
    }

    fn node_id_fs_export(machine_key: &str, remote_root_dir_abs: &str) -> String {
        format!("group:{machine_key}/fs_export:{remote_root_dir_abs}")
    }

    fn node_id_fs_tmp(machine_key: &str, tmp_dir_abs: &str) -> String {
        format!("group:{machine_key}/fs_tmp:{tmp_dir_abs}")
    }

    let mut all_machines: BTreeSet<String> = BTreeSet::new();
    all_machines.extend(owners_by_machine.keys().cloned());
    all_machines.extend(orphan_externals_by_machine.keys().cloned());
    all_machines.extend(machine_resource_by_machine.keys().cloned());
    all_machines.extend(sub_clusters_by_machine.keys().cloned());
    all_machines.extend(fs_mountpoints_by_machine.keys().cloned());
    all_machines.extend(fs_exports_by_machine.keys().cloned());
    all_machines.extend(shm_paths_by_machine.keys().cloned());

    for machine_key in all_machines {
        let sc_id = node_id_sub_cluster(machine_key.as_str());
        let (sc_label, sc_meta) = match sub_clusters_by_machine.get(&machine_key) {
            Some(by_sc) => {
                let label = if by_sc.len() > 1 {
                    "sub_cluster (!)".to_string()
                } else {
                    "sub_cluster".to_string()
                };
                let mut parts: Vec<String> = Vec::new();
                for (sc, ids) in by_sc {
                    let list = ids.iter().cloned().collect::<Vec<_>>().join(",");
                    parts.push(format!("{sc}=[{list}]"));
                }
                (label, Some(parts.join(" ; ")))
            }
            None => (
                "sub_cluster".to_string(),
                Some("sub_cluster missing".to_string()),
            ),
        };
        nodes_out.push(TopologyNodeWire {
            id: sc_id.clone(),
            kind: TopologyNodeKindWire::SubCluster,
            label: sc_label,
            meta: sc_meta,
            detail: None,
            fs_used_bytes: None,
            fs_total_bytes: None,
            fs_read_rps: None,
            fs_write_rps: None,
            tx_mbps_max: None,
            rx_mbps_max: None,
            process_cpu_usage_percent: None,
            process_resident_memory_bytes: None,
            container_memory_usage_bytes: None,
            container_memory_limit_bytes: None,
            machine_process_resident_memory_sum_bytes: None,
            machine_cpu_used_cores: None,
            machine_cpu_total_cores: None,
            machine_memory_used_bytes: None,
            machine_memory_total_bytes: None,
            machine_container_memory_usage_bytes: None,
            machine_container_memory_limit_bytes: None,
            machine_rdma_tx_mbps_cur: None,
            machine_rdma_tx_mbps_max: None,
            machine_rdma_enabled_mbps_max: None,
            machine_rdma_pcie_mbps_max: None,
            machine_rdma_rx_mbps_cur: None,
            machine_rdma_rx_mbps_max: None,
        });

        let mut machine_owners: Vec<OwnerRec> = owners_by_machine
            .get(&machine_key)
            .cloned()
            .unwrap_or_default();
        machine_owners.sort_by(|a, b| a.owner_id.cmp(&b.owner_id));
        let mut machine_orphans: Vec<String> = orphan_externals_by_machine
            .get(&machine_key)
            .cloned()
            .unwrap_or_default();
        machine_orphans.sort();
        machine_orphans.dedup();

        let owners_cnt = machine_owners.len();
        let externals_cnt: usize = machine_owners.iter().map(|o| o.externals.len()).sum();
        let orphans_cnt = machine_orphans.len();
        let (machine_label, machine_meta_base) = nic_group_label_and_meta(machine_key.as_str());
        let machine_id = node_id_machine(machine_key.as_str());
        let (m_tx_max, m_rx_max) = machine_ext_max_by_machine
            .get(&machine_key)
            .copied()
            .unwrap_or((None, None));
        let machine_rdma_agg = machine_rdma_by_machine.get(&machine_key).copied();
        let resource_agg = machine_resource_by_machine.get(&machine_key).copied();
        let (
            machine_rss_sum_bytes,
            machine_cpu_usage_percent,
            machine_cpu_logical_cores,
            machine_mem_used_bytes,
            machine_mem_total_bytes,
            machine_container_mem_used_bytes,
            machine_container_mem_limit_bytes,
        ) = resource_agg
            .map(|a| {
                (
                    a.rss_sum_bytes,
                    a.cpu_usage_percent,
                    a.cpu_logical_cores,
                    a.mem_used_bytes,
                    a.mem_total_bytes,
                    a.container_mem_used_bytes,
                    a.container_mem_limit_bytes,
                )
            })
            .unwrap_or((0.0, None, None, None, None, None, None));
        let (machine_cpu_used_cores, machine_cpu_total_cores) =
            match (machine_cpu_usage_percent, machine_cpu_logical_cores) {
                (Some(pct), Some(cores)) if pct.is_finite() && cores.is_finite() && cores > 0.0 => {
                    (Some(cores * pct / 100.0), Some(cores))
                }
                _ => (None, None),
            };
        let mut machine_meta_lines: Vec<String> = Vec::new();
        if let Some(base) = machine_meta_base {
            machine_meta_lines.push(base);
        }
        machine_meta_lines.push(format!("owners={owners_cnt} externals={externals_cnt}"));
        if orphans_cnt > 0 {
            machine_meta_lines.push(format!("orphan_externals={orphans_cnt}"));
        }
        if let Some(agg) = machine_rdma_agg.filter(|agg| {
            agg.tx_mbps_max > 0.0 || agg.enabled_mbps_max > 0.0 || agg.pcie_mbps_max > 0.0
        }) {
            let mut rdma_summary_parts =
                vec![format!("all={}", fmt_topology_rate_mbps(agg.tx_mbps_max))];
            if agg.enabled_mbps_max > 0.0 {
                rdma_summary_parts.push(format!(
                    "enabled={}",
                    fmt_topology_rate_mbps(agg.enabled_mbps_max)
                ));
            }
            if agg.pcie_mbps_max > 0.0 {
                rdma_summary_parts.push(format!(
                    "pcie={}",
                    fmt_topology_rate_mbps(agg.pcie_mbps_max)
                ));
            }
            machine_meta_lines.push(format!("rdma agg {}", rdma_summary_parts.join(" ")));
        }
        if let Some(rdma_lines) = machine_rdma_detail_lines_by_machine.get(&machine_key) {
            machine_meta_lines.extend(rdma_lines.iter().cloned());
        }
        nodes_out.push(TopologyNodeWire {
            id: machine_id.clone(),
            kind: TopologyNodeKindWire::Machine,
            label: machine_label,
            meta: Some(machine_meta_lines.join("\n")),
            detail: None,
            fs_used_bytes: None,
            fs_total_bytes: None,
            fs_read_rps: None,
            fs_write_rps: None,
            tx_mbps_max: m_tx_max,
            rx_mbps_max: m_rx_max,
            process_cpu_usage_percent: None,
            process_resident_memory_bytes: None,
            container_memory_usage_bytes: None,
            container_memory_limit_bytes: None,
            machine_process_resident_memory_sum_bytes: (machine_rss_sum_bytes > 0.0)
                .then_some(machine_rss_sum_bytes),
            machine_cpu_used_cores,
            machine_cpu_total_cores,
            machine_memory_used_bytes: machine_mem_used_bytes,
            machine_memory_total_bytes: machine_mem_total_bytes,
            machine_container_memory_usage_bytes: machine_container_mem_used_bytes,
            machine_container_memory_limit_bytes: machine_container_mem_limit_bytes,
            machine_rdma_tx_mbps_cur: machine_rdma_agg
                .filter(|agg| agg.tx_mbps_max > 0.0)
                .map(|agg| agg.tx_mbps_cur.max(0.0)),
            machine_rdma_tx_mbps_max: machine_rdma_agg
                .filter(|agg| agg.tx_mbps_max > 0.0)
                .map(|agg| agg.tx_mbps_max),
            machine_rdma_enabled_mbps_max: machine_rdma_agg
                .filter(|agg| agg.enabled_mbps_max > 0.0)
                .map(|agg| agg.enabled_mbps_max),
            machine_rdma_pcie_mbps_max: machine_rdma_agg
                .filter(|agg| agg.pcie_mbps_max > 0.0)
                .map(|agg| agg.pcie_mbps_max),
            machine_rdma_rx_mbps_cur: machine_rdma_agg
                .filter(|agg| agg.rx_mbps_max > 0.0)
                .map(|agg| agg.rx_mbps_cur.max(0.0)),
            machine_rdma_rx_mbps_max: machine_rdma_agg
                .filter(|agg| agg.rx_mbps_max > 0.0)
                .map(|agg| agg.rx_mbps_max),
        });
        let link_label = if orphans_cnt == 0 {
            Some(format!("owners={owners_cnt} externals={externals_cnt}"))
        } else {
            Some(format!(
                "owners={owners_cnt} externals={externals_cnt} orphan_externals={orphans_cnt}"
            ))
        };

        let mut sc_machine_tx_mbps = 0.0;
        let mut sc_machine_rx_mbps = 0.0;
        for o in &machine_owners {
            let Some(owner_meta) = member_meta_by_id.get(&o.owner_id) else {
                continue;
            };
            if owner_meta.role != MemberRole::OwnerClient {
                continue;
            }
            let Some(peers) = peer_rates_by_node.get(&o.owner_id) else {
                continue;
            };
            for (peer_id, tx, rx) in peers {
                if peer_id == &o.owner_id {
                    continue;
                }
                let Some(peer_meta) = member_meta_by_id.get(peer_id) else {
                    continue;
                };
                if peer_meta.role != MemberRole::OwnerClient {
                    continue;
                }
                if peer_meta.group_key == owner_meta.group_key {
                    continue;
                }
                sc_machine_tx_mbps += *tx;
                sc_machine_rx_mbps += *rx;
            }
        }
        links_out.push(TopologyLinkWire {
            source: sc_id.clone(),
            target: machine_id.clone(),
            kind: TopologyLinkKindWire::Solid,
            label: link_label,
            tx_mbps: (sc_machine_tx_mbps > 0.0).then_some(sc_machine_tx_mbps),
            rx_mbps: (sc_machine_rx_mbps > 0.0).then_some(sc_machine_rx_mbps),
        });

        // FS mount view (df -h clusters):
        // machine -> fs_mountpoint (capacity) -> fs_{export,shm,tmp} (paths)
        if let Some(mps) = fs_mountpoints_by_machine.get(&machine_key) {
            let mut export_names_by_target: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
            let mut export_agents_by_target: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
            if let Some(exports) = fs_exports_by_machine.get(&machine_key) {
                for e in exports {
                    let root = normalize_abs_dir_label(e.remote_root_dir_abs.as_str());
                    if root.is_empty() {
                        continue;
                    }
                    export_names_by_target
                        .entry(root.clone())
                        .or_default()
                        .insert(e.export_name.clone());
                    export_agents_by_target
                        .entry(root)
                        .or_default()
                        .insert(e.agent_instance_key.clone());
                }
            }
            let shms = shm_paths_by_machine.get(&machine_key);

            for (mountpoint_dir_abs, mp) in mps {
                let mp_id =
                    node_id_fs_mountpoint(machine_key.as_str(), mountpoint_dir_abs.as_str());
                let meta = Some(format!(
                    "mountpoint exports={} shm={} tmp={}",
                    mp.export_targets.len(),
                    mp.shm_targets.len(),
                    mp.tmp_targets.len(),
                ));
                nodes_out.push(TopologyNodeWire {
                    id: mp_id.clone(),
                    kind: TopologyNodeKindWire::FsMountpoint,
                    label: mountpoint_dir_abs.clone(),
                    meta,
                    detail: None,
                    fs_used_bytes: mp.used_bytes,
                    fs_total_bytes: mp.total_bytes,
                    fs_read_rps: None,
                    fs_write_rps: None,
                    tx_mbps_max: None,
                    rx_mbps_max: None,
                    process_cpu_usage_percent: None,
                    process_resident_memory_bytes: None,
                    container_memory_usage_bytes: None,
                    container_memory_limit_bytes: None,
                    machine_process_resident_memory_sum_bytes: None,
                    machine_cpu_used_cores: None,
                    machine_cpu_total_cores: None,
                    machine_memory_used_bytes: None,
                    machine_memory_total_bytes: None,
                    machine_container_memory_usage_bytes: None,
                    machine_container_memory_limit_bytes: None,
                    machine_rdma_tx_mbps_cur: None,
                    machine_rdma_tx_mbps_max: None,
                    machine_rdma_enabled_mbps_max: None,
                    machine_rdma_pcie_mbps_max: None,
                    machine_rdma_rx_mbps_cur: None,
                    machine_rdma_rx_mbps_max: None,
                });
                links_out.push(TopologyLinkWire {
                    source: machine_id.clone(),
                    target: mp_id.clone(),
                    kind: TopologyLinkKindWire::Solid,
                    label: None,
                    tx_mbps: None,
                    rx_mbps: None,
                });

                for target_dir_abs in &mp.export_targets {
                    let node_id = node_id_fs_export(machine_key.as_str(), target_dir_abs.as_str());
                    let meta = export_names_by_target.get(target_dir_abs).map(|names| {
                        format!(
                            "exports={}",
                            names.iter().cloned().collect::<Vec<_>>().join(",")
                        )
                    });
                    nodes_out.push(TopologyNodeWire {
                        id: node_id.clone(),
                        kind: TopologyNodeKindWire::FsExport,
                        label: target_dir_abs.clone(),
                        meta,
                        detail: None,
                        fs_used_bytes: None,
                        fs_total_bytes: None,
                        fs_read_rps: None,
                        fs_write_rps: None,
                        tx_mbps_max: None,
                        rx_mbps_max: None,
                        process_cpu_usage_percent: None,
                        process_resident_memory_bytes: None,
                        container_memory_usage_bytes: None,
                        container_memory_limit_bytes: None,
                        machine_process_resident_memory_sum_bytes: None,
                        machine_cpu_used_cores: None,
                        machine_cpu_total_cores: None,
                        machine_memory_used_bytes: None,
                        machine_memory_total_bytes: None,
                        machine_container_memory_usage_bytes: None,
                        machine_container_memory_limit_bytes: None,
                        machine_rdma_tx_mbps_cur: None,
                        machine_rdma_tx_mbps_max: None,
                        machine_rdma_enabled_mbps_max: None,
                        machine_rdma_pcie_mbps_max: None,
                        machine_rdma_rx_mbps_cur: None,
                        machine_rdma_rx_mbps_max: None,
                    });
                    links_out.push(TopologyLinkWire {
                        source: mp_id.clone(),
                        target: node_id.clone(),
                        kind: TopologyLinkKindWire::Solid,
                        label: None,
                        tx_mbps: None,
                        rx_mbps: None,
                    });
                    if let Some(agent_ids) = export_agents_by_target.get(target_dir_abs) {
                        for agent in agent_ids {
                            export_dashed_links.push((node_id.clone(), agent.clone()));
                        }
                    }
                }

                for target_dir_abs in &mp.shm_targets {
                    let meta = {
                        let users = shms
                            .and_then(|m| m.get(target_dir_abs))
                            .map(|cnt| format!("shm users={cnt}"));
                        let files = shm_files_by_machine
                            .get(&machine_key)
                            .and_then(|m| m.get(target_dir_abs))
                            .map(|agg| {
                                format!(
                                    "shm_files={} alloc_sum={} logical_sum={}",
                                    agg.files.len(),
                                    fmt_topology_bytes(agg.allocated_sum_bytes),
                                    fmt_topology_bytes(agg.logical_sum_bytes),
                                )
                            });
                        match (users, files) {
                            (Some(a), Some(b)) => Some(format!("{a}\n{b}")),
                            (Some(a), None) => Some(a),
                            (None, Some(b)) => Some(b),
                            (None, None) => None,
                        }
                    };
                    let detail = shm_files_by_machine
                        .get(&machine_key)
                        .and_then(|m| m.get(target_dir_abs))
                        .and_then(|agg| {
                            if agg.files.is_empty() {
                                return None;
                            }
                            let mut lines: Vec<String> = Vec::new();
                            for (path, logical, allocated) in &agg.files {
                                let logical_s = logical
                                    .map(fmt_topology_bytes)
                                    .unwrap_or_else(|| "N/A".to_string());
                                let allocated_s = allocated
                                    .map(fmt_topology_bytes)
                                    .unwrap_or_else(|| "N/A".to_string());
                                lines.push(format!(
                                    "{} | alloc={} | logical={}",
                                    path, allocated_s, logical_s
                                ));
                            }
                            Some(lines.join("\n"))
                        });
                    let node_id = node_id_fs_shm(machine_key.as_str(), target_dir_abs.as_str());
                    nodes_out.push(TopologyNodeWire {
                        id: node_id.clone(),
                        kind: TopologyNodeKindWire::FsShm,
                        label: target_dir_abs.clone(),
                        meta,
                        detail,
                        fs_used_bytes: None,
                        fs_total_bytes: None,
                        fs_read_rps: None,
                        fs_write_rps: None,
                        tx_mbps_max: None,
                        rx_mbps_max: None,
                        process_cpu_usage_percent: None,
                        process_resident_memory_bytes: None,
                        container_memory_usage_bytes: None,
                        container_memory_limit_bytes: None,
                        machine_process_resident_memory_sum_bytes: None,
                        machine_cpu_used_cores: None,
                        machine_cpu_total_cores: None,
                        machine_memory_used_bytes: None,
                        machine_memory_total_bytes: None,
                        machine_container_memory_usage_bytes: None,
                        machine_container_memory_limit_bytes: None,
                        machine_rdma_tx_mbps_cur: None,
                        machine_rdma_tx_mbps_max: None,
                        machine_rdma_enabled_mbps_max: None,
                        machine_rdma_pcie_mbps_max: None,
                        machine_rdma_rx_mbps_cur: None,
                        machine_rdma_rx_mbps_max: None,
                    });
                    links_out.push(TopologyLinkWire {
                        source: mp_id.clone(),
                        target: node_id,
                        kind: TopologyLinkKindWire::Solid,
                        label: None,
                        tx_mbps: None,
                        rx_mbps: None,
                    });
                }

                for target_dir_abs in &mp.tmp_targets {
                    let node_id = node_id_fs_tmp(machine_key.as_str(), target_dir_abs.as_str());
                    nodes_out.push(TopologyNodeWire {
                        id: node_id.clone(),
                        kind: TopologyNodeKindWire::FsTmp,
                        label: target_dir_abs.clone(),
                        meta: Some("tmp".to_string()),
                        detail: None,
                        fs_used_bytes: None,
                        fs_total_bytes: None,
                        fs_read_rps: None,
                        fs_write_rps: None,
                        tx_mbps_max: None,
                        rx_mbps_max: None,
                        process_cpu_usage_percent: None,
                        process_resident_memory_bytes: None,
                        container_memory_usage_bytes: None,
                        container_memory_limit_bytes: None,
                        machine_process_resident_memory_sum_bytes: None,
                        machine_cpu_used_cores: None,
                        machine_cpu_total_cores: None,
                        machine_memory_used_bytes: None,
                        machine_memory_total_bytes: None,
                        machine_container_memory_usage_bytes: None,
                        machine_container_memory_limit_bytes: None,
                        machine_rdma_tx_mbps_cur: None,
                        machine_rdma_tx_mbps_max: None,
                        machine_rdma_enabled_mbps_max: None,
                        machine_rdma_pcie_mbps_max: None,
                        machine_rdma_rx_mbps_cur: None,
                        machine_rdma_rx_mbps_max: None,
                    });
                    links_out.push(TopologyLinkWire {
                        source: mp_id.clone(),
                        target: node_id,
                        kind: TopologyLinkKindWire::Solid,
                        label: None,
                        tx_mbps: None,
                        rx_mbps: None,
                    });
                }
            }
        }

        for o in machine_owners {
            let owner_node_id = node_id_owner(machine_key.as_str(), &o.owner_id);
            let (o_tx_max, o_rx_max) = owner_ext_max_by_owner
                .get(&o.owner_id)
                .copied()
                .unwrap_or((None, None));
            let (o_cpu, o_rss, o_ct_mem, o_ct_limit, o_meta) = member_meta_by_id
                .get(&o.owner_id)
                .map(|m| {
                    (
                        m.process_cpu_usage_percent,
                        m.process_resident_memory_bytes,
                        m.container_memory_usage_bytes,
                        m.container_memory_limit_bytes,
                        m.rdma_usage_meta.clone(),
                    )
                })
                .unwrap_or((None, None, None, None, None));
            nodes_out.push(TopologyNodeWire {
                id: owner_node_id.clone(),
                kind: TopologyNodeKindWire::Owner,
                label: o.owner_id.clone(),
                meta: o_meta.or(Some("owner".to_string())),
                detail: None,
                fs_used_bytes: None,
                fs_total_bytes: None,
                fs_read_rps: None,
                fs_write_rps: None,
                tx_mbps_max: o_tx_max,
                rx_mbps_max: o_rx_max,
                process_cpu_usage_percent: o_cpu,
                process_resident_memory_bytes: o_rss,
                container_memory_usage_bytes: o_ct_mem,
                container_memory_limit_bytes: o_ct_limit,
                machine_process_resident_memory_sum_bytes: None,
                machine_cpu_used_cores: None,
                machine_cpu_total_cores: None,
                machine_memory_used_bytes: None,
                machine_memory_total_bytes: None,
                machine_container_memory_usage_bytes: None,
                machine_container_memory_limit_bytes: None,
                machine_rdma_tx_mbps_cur: None,
                machine_rdma_tx_mbps_max: None,
                machine_rdma_enabled_mbps_max: None,
                machine_rdma_pcie_mbps_max: None,
                machine_rdma_rx_mbps_cur: None,
                machine_rdma_rx_mbps_max: None,
            });
            links_out.push(TopologyLinkWire {
                source: machine_id.clone(),
                target: owner_node_id.clone(),
                kind: TopologyLinkKindWire::Solid,
                label: Some(format!("externals={}", o.externals.len())),
                tx_mbps: None,
                rx_mbps: None,
            });

            for ext in o.externals {
                let ext_member_id = ext.clone();
                let ext_id = node_id_external(machine_key.as_str(), &o.owner_id, &ext);
                let (e_cpu, e_rss, e_ct_mem, e_ct_limit, e_fs_r, e_fs_w) = member_meta_by_id
                    .get(&ext)
                    .map(|m| {
                        (
                            m.process_cpu_usage_percent,
                            m.process_resident_memory_bytes,
                            m.container_memory_usage_bytes,
                            m.container_memory_limit_bytes,
                            m.fs_read_rps,
                            m.fs_write_rps,
                        )
                    })
                    .unwrap_or((None, None, None, None, None, None));
                nodes_out.push(TopologyNodeWire {
                    id: ext_id.clone(),
                    kind: TopologyNodeKindWire::External,
                    label: ext.clone(),
                    meta: Some("external".to_string()),
                    detail: None,
                    fs_used_bytes: None,
                    fs_total_bytes: None,
                    fs_read_rps: e_fs_r,
                    fs_write_rps: e_fs_w,
                    tx_mbps_max: None,
                    rx_mbps_max: None,
                    process_cpu_usage_percent: e_cpu,
                    process_resident_memory_bytes: e_rss,
                    container_memory_usage_bytes: e_ct_mem,
                    container_memory_limit_bytes: e_ct_limit,
                    machine_process_resident_memory_sum_bytes: None,
                    machine_cpu_used_cores: None,
                    machine_cpu_total_cores: None,
                    machine_memory_used_bytes: None,
                    machine_memory_total_bytes: None,
                    machine_container_memory_usage_bytes: None,
                    machine_container_memory_limit_bytes: None,
                    machine_rdma_tx_mbps_cur: None,
                    machine_rdma_tx_mbps_max: None,
                    machine_rdma_enabled_mbps_max: None,
                    machine_rdma_pcie_mbps_max: None,
                    machine_rdma_rx_mbps_cur: None,
                    machine_rdma_rx_mbps_max: None,
                });
                external_node_id_by_member_id.insert(ext_member_id, ext_id.clone());

                let mut tx_mbps: Option<f64> = None;
                let mut rx_mbps: Option<f64> = None;
                if let Some((tx, rx)) = rate_by_node_peer.get(&(o.owner_id.clone(), ext.clone())) {
                    if *tx > 0.0 {
                        tx_mbps = Some(*tx);
                    }
                    if *rx > 0.0 {
                        rx_mbps = Some(*rx);
                    }
                }
                if let Some((tx2, rx2)) = rate_by_node_peer.get(&(ext.clone(), o.owner_id.clone()))
                {
                    if *rx2 > 0.0 {
                        tx_mbps = Some(tx_mbps.map_or(*rx2, |v| v.max(*rx2)));
                    }
                    if *tx2 > 0.0 {
                        rx_mbps = Some(rx_mbps.map_or(*tx2, |v| v.max(*tx2)));
                    }
                }
                links_out.push(TopologyLinkWire {
                    source: owner_node_id.clone(),
                    target: ext_id,
                    kind: TopologyLinkKindWire::Solid,
                    label: None,
                    tx_mbps,
                    rx_mbps,
                });
            }
        }

        if !machine_orphans.is_empty() {
            let orphan_bucket_id = node_id_orphan_bucket(machine_key.as_str());
            nodes_out.push(TopologyNodeWire {
                id: orphan_bucket_id.clone(),
                kind: TopologyNodeKindWire::OrphanMachine,
                label: "(orphan)".to_string(),
                meta: Some("missing owner attachment".to_string()),
                detail: None,
                fs_used_bytes: None,
                fs_total_bytes: None,
                fs_read_rps: None,
                fs_write_rps: None,
                tx_mbps_max: None,
                rx_mbps_max: None,
                process_cpu_usage_percent: None,
                process_resident_memory_bytes: None,
                container_memory_usage_bytes: None,
                container_memory_limit_bytes: None,
                machine_process_resident_memory_sum_bytes: None,
                machine_cpu_used_cores: None,
                machine_cpu_total_cores: None,
                machine_memory_used_bytes: None,
                machine_memory_total_bytes: None,
                machine_container_memory_usage_bytes: None,
                machine_container_memory_limit_bytes: None,
                machine_rdma_tx_mbps_cur: None,
                machine_rdma_tx_mbps_max: None,
                machine_rdma_enabled_mbps_max: None,
                machine_rdma_pcie_mbps_max: None,
                machine_rdma_rx_mbps_cur: None,
                machine_rdma_rx_mbps_max: None,
            });
            links_out.push(TopologyLinkWire {
                source: machine_id.clone(),
                target: orphan_bucket_id.clone(),
                kind: TopologyLinkKindWire::Solid,
                label: Some(format!("orphan_externals={}", machine_orphans.len())),
                tx_mbps: None,
                rx_mbps: None,
            });

            for ext in machine_orphans {
                let ext_member_id = ext.clone();
                let ext_id = node_id_orphan_external(machine_key.as_str(), &ext);
                let (e_cpu, e_rss, e_ct_mem, e_ct_limit, e_fs_r, e_fs_w) = member_meta_by_id
                    .get(&ext)
                    .map(|m| {
                        (
                            m.process_cpu_usage_percent,
                            m.process_resident_memory_bytes,
                            m.container_memory_usage_bytes,
                            m.container_memory_limit_bytes,
                            m.fs_read_rps,
                            m.fs_write_rps,
                        )
                    })
                    .unwrap_or((None, None, None, None, None, None));
                nodes_out.push(TopologyNodeWire {
                    id: ext_id.clone(),
                    kind: TopologyNodeKindWire::OrphanExternal,
                    label: ext,
                    meta: Some("orphan external".to_string()),
                    detail: None,
                    fs_used_bytes: None,
                    fs_total_bytes: None,
                    fs_read_rps: e_fs_r,
                    fs_write_rps: e_fs_w,
                    tx_mbps_max: None,
                    rx_mbps_max: None,
                    process_cpu_usage_percent: e_cpu,
                    process_resident_memory_bytes: e_rss,
                    container_memory_usage_bytes: e_ct_mem,
                    container_memory_limit_bytes: e_ct_limit,
                    machine_process_resident_memory_sum_bytes: None,
                    machine_cpu_used_cores: None,
                    machine_cpu_total_cores: None,
                    machine_memory_used_bytes: None,
                    machine_memory_total_bytes: None,
                    machine_container_memory_usage_bytes: None,
                    machine_container_memory_limit_bytes: None,
                    machine_rdma_tx_mbps_cur: None,
                    machine_rdma_tx_mbps_max: None,
                    machine_rdma_enabled_mbps_max: None,
                    machine_rdma_pcie_mbps_max: None,
                    machine_rdma_rx_mbps_cur: None,
                    machine_rdma_rx_mbps_max: None,
                });
                external_node_id_by_member_id.insert(ext_member_id, ext_id.clone());
                links_out.push(TopologyLinkWire {
                    source: orphan_bucket_id.clone(),
                    target: ext_id,
                    kind: TopologyLinkKindWire::Solid,
                    label: None,
                    tx_mbps: None,
                    rx_mbps: None,
                });
            }
        }
    }
    // Step 2.5) Attach export nodes to the exporting instance (visual-only, dashed link).
    for (export_node_id, agent_instance_key) in export_dashed_links {
        let Some(ext_node_id) = external_node_id_by_member_id.get(&agent_instance_key) else {
            continue;
        };
        links_out.push(TopologyLinkWire {
            source: export_node_id,
            target: ext_node_id.clone(),
            kind: TopologyLinkKindWire::Dashed,
            label: None,
            tx_mbps: None,
            rx_mbps: None,
        });
    }

    // Step 3) Optional: attach MQ producer/consumer nodes under their external client process.
    //
    // English note:
    // - MQ member state is stored in etcd (chan_id/producers/consumers) and is independent of KV bandwidth.
    // - We show producer/consumer as child nodes of the external process that hosts them.
    // - We do not infer attachment when `external_client_id` is missing or unknown.
    if let Some(mq) = snapshot.mq.as_ref() {
        const MQ_RATE_WINDOW_SECS: f64 = 30.0;

        fn safe_fragment(s: &str) -> String {
            s.replace('/', "_")
        }

        for ch in &mq.channels {
            for p in &ch.producers {
                let Some(ext_member_id) = p.external_client_id.as_deref() else {
                    continue;
                };
                let Some(parent_id) = external_node_id_by_member_id.get(ext_member_id) else {
                    continue;
                };

                let pid = safe_fragment(&p.producer_idx);
                let node_id = format!("{}/mq_producer:chan={}/idx={}", parent_id, ch.chan_id, pid);

                let calls = p.put_window_calls.unwrap_or(0.0);
                let bytes = p.put_window_bytes.unwrap_or(0.0);
                let rps = calls / MQ_RATE_WINDOW_SECS;
                let avg = if calls > 0.0 { bytes / calls } else { 0.0 };

                nodes_out.push(TopologyNodeWire {
                    id: node_id.clone(),
                    kind: TopologyNodeKindWire::MqProducer,
                    label: format!("mq producer {}", p.producer_idx),
                    meta: Some(format!(
                        "chan={} calls_30s={} produce_rate_30s_avg_msgs_s={:.3} avg_bytes={:.1}",
                        ch.chan_id, calls as i64, rps, avg
                    )),
                    detail: None,
                    fs_used_bytes: None,
                    fs_total_bytes: None,
                    fs_read_rps: None,
                    fs_write_rps: None,
                    tx_mbps_max: None,
                    rx_mbps_max: None,
                    process_cpu_usage_percent: None,
                    process_resident_memory_bytes: None,
                    container_memory_usage_bytes: None,
                    container_memory_limit_bytes: None,
                    machine_process_resident_memory_sum_bytes: None,
                    machine_cpu_used_cores: None,
                    machine_cpu_total_cores: None,
                    machine_memory_used_bytes: None,
                    machine_memory_total_bytes: None,
                    machine_container_memory_usage_bytes: None,
                    machine_container_memory_limit_bytes: None,
                    machine_rdma_tx_mbps_cur: None,
                    machine_rdma_tx_mbps_max: None,
                    machine_rdma_enabled_mbps_max: None,
                    machine_rdma_pcie_mbps_max: None,
                    machine_rdma_rx_mbps_cur: None,
                    machine_rdma_rx_mbps_max: None,
                });
                links_out.push(TopologyLinkWire {
                    source: parent_id.clone(),
                    target: node_id,
                    kind: TopologyLinkKindWire::Solid,
                    label: None,
                    tx_mbps: None,
                    rx_mbps: None,
                });
            }

            for c in &ch.consumers {
                let Some(ext_member_id) = c.external_client_id.as_deref() else {
                    continue;
                };
                let Some(parent_id) = external_node_id_by_member_id.get(ext_member_id) else {
                    continue;
                };

                let cid = safe_fragment(&c.consumer_idx);
                let node_id = format!("{}/mq_consumer:chan={}/idx={}", parent_id, ch.chan_id, cid);

                let calls = c.get_one_window_calls.unwrap_or(0.0);
                let bytes = c.get_one_window_bytes.unwrap_or(0.0);
                let rps = calls / MQ_RATE_WINDOW_SECS;
                let avg = if calls > 0.0 { bytes / calls } else { 0.0 };

                nodes_out.push(TopologyNodeWire {
                    id: node_id.clone(),
                    kind: TopologyNodeKindWire::MqConsumer,
                    label: format!("mq consumer {}", c.consumer_idx),
                    meta: Some(format!(
                        "chan={} calls_30s={} consume_rate_30s_avg_msgs_s={:.3} avg_bytes={:.1} timeouts_30s={}",
                        ch.chan_id,
                        calls as i64,
                        rps,
                        avg,
                        c.get_one_window_timeouts.unwrap_or(0.0) as i64
                    )),
                    detail: None,
                    fs_used_bytes: None,
                    fs_total_bytes: None,
                    fs_read_rps: None,
                    fs_write_rps: None,
                    tx_mbps_max: None,
                    rx_mbps_max: None,
                    process_cpu_usage_percent: None,
                    process_resident_memory_bytes: None,
                    container_memory_usage_bytes: None,
                    container_memory_limit_bytes: None,
                    machine_process_resident_memory_sum_bytes: None,
                    machine_cpu_used_cores: None,
                    machine_cpu_total_cores: None,
                    machine_memory_used_bytes: None,
                    machine_memory_total_bytes: None,
                    machine_container_memory_usage_bytes: None,
                    machine_container_memory_limit_bytes: None,
                    machine_rdma_tx_mbps_cur: None,
                    machine_rdma_tx_mbps_max: None,
                    machine_rdma_enabled_mbps_max: None,
                    machine_rdma_pcie_mbps_max: None,
                    machine_rdma_rx_mbps_cur: None,
                    machine_rdma_rx_mbps_max: None,
                });
                links_out.push(TopologyLinkWire {
                    source: parent_id.clone(),
                    target: node_id,
                    kind: TopologyLinkKindWire::Solid,
                    label: None,
                    tx_mbps: None,
                    rx_mbps: None,
                });
            }
        }
    }

    let wire = TopologyWire {
        nodes: nodes_out,
        links: links_out,
    };
    let json = serde_json::to_string(&wire).unwrap();
    let json_escaped = json_escape_for_html_script_tag(&json);
    Some(TopologyView {
        json_escaped,
        node_count: wire.nodes.len(),
        edge_count: wire.links.len(),
    })
}

fn build_transfer_link_matrix(
    snapshot: &ClusterSnapshot,
    warnings: &mut Vec<String>,
) -> MatrixView {
    let mut keys: BTreeSet<String> = BTreeSet::new();
    let mut hidden_keys: BTreeSet<String> = BTreeSet::new();
    let mut node_cls_by_key: HashMap<String, &'static str> = HashMap::new();
    for n in &snapshot.nodes {
        // Matrix endpoints must be member_id because link keys are written as:
        // - `/{cluster}/transfer_link/p2p/{from}/{to}`
        // - `/{cluster}/transfer_link/te/{from}/{to}`
        // where {from}/{to} are member_id. The monitor scanner joins p2p+te into a single edge route.
        // node_key is presentation-only (can be accessible_ip, shared_mem_dir, etc.) and must not
        // be mixed into matrix endpoints; otherwise it creates "orphan" rows/cols without edges.
        for m in &n.members {
            // Side-transfer workers are internal transport helpers. Keep them out of the web
            // transfer-link matrix so the page reflects user-facing cluster members only.
            if m.is_side_transfer_worker {
                hidden_keys.insert(m.member_id.clone());
                continue;
            }
            keys.insert(m.member_id.clone());
            let member_cls = if m.is_p2p_relay {
                "nrelay"
            } else if m.role == MemberRole::Master {
                "nmaster"
            } else if m.role == MemberRole::OwnerClient {
                "nowner"
            } else {
                ""
            };
            if !member_cls.is_empty() {
                node_cls_by_key.insert(m.member_id.clone(), member_cls);
            }
        }
    }

    let nodes: Vec<String> = keys.into_iter().collect();
    let mut idx_by_key: HashMap<&str, usize> = HashMap::new();
    for (i, k) in nodes.iter().enumerate() {
        idx_by_key.insert(k.as_str(), i);
    }

    let mut pixels_by_edge: HashMap<(usize, usize), crate::model::RoutePixels> = HashMap::new();
    let mut unknown_routes: BTreeSet<String> = BTreeSet::new();
    let mut orphan_endpoints: BTreeSet<String> = BTreeSet::new();
    let mut visible_edge_count = 0usize;
    for e in &snapshot.transfer_engine_edges {
        if hidden_keys.contains(&e.from) || hidden_keys.contains(&e.to) {
            continue;
        }
        visible_edge_count += 1;
        let from_i = idx_by_key.get(e.from.as_str()).copied();
        let to_i = idx_by_key.get(e.to.as_str()).copied();
        let (Some(from_i), Some(to_i)) = (from_i, to_i) else {
            if !idx_by_key.contains_key(e.from.as_str()) {
                orphan_endpoints.insert(e.from.clone());
            }
            if !idx_by_key.contains_key(e.to.as_str()) {
                orphan_endpoints.insert(e.to.clone());
            }
            continue;
        };
        let parsed = parse_route_pixels(&e.route);
        if parsed.unknown {
            unknown_routes.insert(e.route.clone());
            continue;
        }
        pixels_by_edge.insert((from_i, to_i), parsed.pixels);
    }

    if !orphan_endpoints.is_empty() {
        let preview = orphan_endpoints
            .iter()
            .take(12)
            .cloned()
            .collect::<Vec<_>>()
            .join(", ");
        warnings.push(format!(
            "transfer_engine route references endpoints not in member list; ignored by matrix: count={} preview=[{}]",
            orphan_endpoints.len(),
            preview
        ));
    }

    let nodes_view = nodes
        .iter()
        .map(|k| MatrixNodeView {
            key: k.clone(),
            cls: node_cls_by_key.get(k).copied().unwrap_or("").to_string(),
        })
        .collect::<Vec<_>>();

    let mut rows_view: Vec<MatrixRowView> = Vec::with_capacity(nodes.len());
    for row in 0..nodes.len() {
        let row_key = nodes[row].clone();
        let row_cls = node_cls_by_key
            .get(&row_key)
            .copied()
            .unwrap_or("")
            .to_string();
        let mut cells: Vec<MatrixCellView> = Vec::with_capacity(nodes.len());
        for col in 0..nodes.len() {
            if row == col {
                cells.push(MatrixCellView {
                    td_cls: "mxdia".to_string(),
                    p2p_cls: "mxpx".to_string(),
                    te_cls: "mxpx".to_string(),
                });
                continue;
            }
            let pixels =
                pixels_by_edge
                    .get(&(row, col))
                    .copied()
                    .unwrap_or(crate::model::RoutePixels {
                        p2p: RoutePixelState::Off,
                        p2p_transport: P2pTransportKind::Unknown,
                        te: RoutePixelState::Off,
                    });
            let p2p_cls = match pixels.p2p {
                RoutePixelState::Off => "mxpx".to_string(),
                RoutePixelState::Direct | RoutePixelState::DirectP2pMode => {
                    let suffix = match pixels.p2p_transport {
                        P2pTransportKind::Ice => "mxp2p_ice",
                        P2pTransportKind::Tcp => "mxp2p_tcp",
                        P2pTransportKind::Websocket => "mxp2p_websocket",
                        P2pTransportKind::Quic => "mxp2p_quic",
                        P2pTransportKind::Tquic => "mxp2p_tquic",
                        P2pTransportKind::Unknown => "mxp2p_unknown",
                    };
                    format!("mxpx {}", suffix)
                }
                RoutePixelState::Alt => "mxpx mxalt".to_string(),
            };
            let te_cls = match pixels.te {
                RoutePixelState::Off => "mxpx".to_string(),
                RoutePixelState::Direct => "mxpx mxte".to_string(),
                RoutePixelState::DirectP2pMode => "mxpx mxtequic".to_string(),
                RoutePixelState::Alt => "mxpx mxalt".to_string(),
            };
            cells.push(MatrixCellView {
                td_cls: String::new(),
                p2p_cls,
                te_cls,
            });
        }
        rows_view.push(MatrixRowView {
            key: row_key,
            cls: row_cls,
            cells,
        });
    }

    MatrixView {
        node_count: nodes.len(),
        edge_count: visible_edge_count,
        unknown_routes: unknown_routes.into_iter().collect(),
        nodes: nodes_view,
        rows: rows_view,
    }
}

#[cfg(test)]
mod tests {
    use super::build_transfer_link_matrix;
    use crate::config::MemberKind;
    use crate::model::{
        ClusterSnapshot, MemberRole, MemberSnapshot, NodeSnapshot, TransferEngineEdge,
    };

    fn test_member(
        member_id: &str,
        role: MemberRole,
        is_side_transfer_worker: bool,
    ) -> MemberSnapshot {
        MemberSnapshot {
            member_id: member_id.to_string(),
            role,
            is_p2p_relay: false,
            is_side_transfer_worker,
            node_start_time: 1,
            hostname: None,
            accessible_ip: None,
            shared_mem_dir: None,
            p2p_listen_port: None,
            rdma_runtime_reported: false,
            rdma_probe_error: None,
            rdma_devices: Vec::new(),
            rdma_ports: Vec::new(),
            rdma_transfer_engine: None,
            pid: None,
            cmd: None,
            sub_cluster: None,
            product_uuid: None,
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
        }
    }

    fn test_snapshot() -> ClusterSnapshot {
        ClusterSnapshot {
            cluster_name: "test-cluster".to_string(),
            member_kind: MemberKind::Kv,
            etcd_endpoints: Vec::new(),
            prometheus_base_url: String::new(),
            warnings: Vec::new(),
            visible_member_roles: None,
            master_id: None,
            master_network: None,
            transfer_engine_edges: vec![
                TransferEngineEdge {
                    from: "owner-a".to_string(),
                    to: "ext-a".to_string(),
                    route: "tcp te".to_string(),
                },
                TransferEngineEdge {
                    from: "owner-a__side_0".to_string(),
                    to: "ext-a".to_string(),
                    route: "tcp te".to_string(),
                },
                TransferEngineEdge {
                    from: "owner-a__side_0".to_string(),
                    to: "missing-owner".to_string(),
                    route: "tcp te".to_string(),
                },
            ],
            kv_peer_network: Vec::new(),
            rdma_netdev_network: Vec::new(),
            fs_mount_fs: Vec::new(),
            shm_files: Vec::new(),
            fs_export_registry: Vec::new(),
            fs_mount_registry: Vec::new(),
            kv_topology_owner_external_max: Vec::new(),
            kv_topology_machine_external_max: Vec::new(),
            kv_topology_sub_cluster_owner_owner_max: Vec::new(),
            nodes: vec![NodeSnapshot {
                node_key: "node-a".to_string(),
                hostname: None,
                accessible_ip: None,
                shared_mem_dir: None,
                is_p2p_relay: false,
                node_cpu_usage_percent: None,
                node_cpu_logical_cores: None,
                node_memory_usage_bytes: None,
                node_memory_total_bytes: None,
                container_memory_usage_bytes: None,
                container_memory_limit_bytes: None,
                members: vec![
                    test_member("owner-a", MemberRole::OwnerClient, false),
                    test_member("owner-a__side_0", MemberRole::SideTransferWorker, true),
                    test_member("ext-a", MemberRole::ExternalClient, false),
                ],
                segment_devices: Vec::new(),
            }],
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
        }
    }

    #[test]
    fn transfer_link_matrix_hides_side_transfer_workers_and_side_only_edges() {
        let snapshot = test_snapshot();
        let mut warnings = Vec::new();

        let matrix = build_transfer_link_matrix(&snapshot, &mut warnings);

        assert_eq!(matrix.node_count, 2);
        assert_eq!(matrix.edge_count, 1);
        assert!(warnings.is_empty());
        assert_eq!(
            matrix
                .nodes
                .iter()
                .map(|node| node.key.as_str())
                .collect::<Vec<_>>(),
            vec!["ext-a", "owner-a"]
        );
        assert!(matrix.rows.iter().all(|row| row.key != "owner-a__side_0"));
    }
}
