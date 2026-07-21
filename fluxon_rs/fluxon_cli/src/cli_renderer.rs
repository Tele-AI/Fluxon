use crate::config::MemberKind;
use crate::model::{
    ClusterSnapshot, MemberRole, MemberSnapshot, NodeSnapshot, P2pTransportKind,
    ParseRoutePixelsResult, ProcessViewModel, RoutePixelState, RoutePixels, UiPill, UiPillStatus,
    build_cluster_view_model, parse_route_pixels, pills_for_cluster_totals, pills_for_instance,
    pills_for_node_resource, pills_for_process_resource,
};
use std::collections::{BTreeSet, HashMap};

fn mq_status_str(s: crate::model::MqMemberStatus) -> &'static str {
    match s {
        crate::model::MqMemberStatus::Alive => "alive",
        crate::model::MqMemberStatus::Stale => "stale",
        crate::model::MqMemberStatus::Invalid => "invalid",
    }
}

fn fmt_ms_from_opt_us(v_us: Option<f64>) -> String {
    match v_us {
        Some(v) => format!("{:.3}", v / 1000.0),
        None => "N/A".to_string(),
    }
}

fn fmt_i64_from_opt_f64(v: Option<f64>) -> String {
    match v {
        Some(v) => format!("{}", v.round() as i64),
        None => "N/A".to_string(),
    }
}

fn fmt_hz_from_opt_f64(v: Option<f64>) -> String {
    match v {
        Some(v) => format!("{:.3}", v),
        None => "N/A".to_string(),
    }
}

fn fmt_interval_from_opt_f64(begin: Option<f64>, end: Option<f64>) -> String {
    match (begin, end) {
        (Some(begin), Some(end)) => {
            format!("{}~{}", begin.round() as i64, end.round() as i64)
        }
        _ => "N/A".to_string(),
    }
}

fn render_mq_cluster(snapshot: &ClusterSnapshot) -> String {
    let mut out = String::new();

    let build_lines = vec![
        format!("version: {}", env!("CARGO_PKG_VERSION")),
        format!("commit: {}", crate::build_info::GIT_COMMIT_ID),
        format!("source-sha256: {}", crate::build_info::SOURCE_SHA256),
    ];
    push_box(&mut out, "Build", &build_lines);
    out.push('\n');

    let vm = build_cluster_view_model(snapshot);
    let mut header_lines = vec![
        format!("cluster_name: {}", vm.header.cluster_name),
        format!("member_kind: {}", vm.header.member_kind.as_display_str()),
        format!("etcd_endpoints: {}", vm.header.etcd_endpoints.join(",")),
        format!("prometheus_base_url: {}", vm.header.prometheus_base_url),
    ];
    if let Some(v) = vm.header.master_network_subnet_whitelist.as_ref() {
        header_lines.push(format!("master.network.subnet_whitelist: {}", v));
    }
    push_box(&mut out, "Cluster", &header_lines);
    out.push('\n');

    if !snapshot.warnings.is_empty() {
        let lines: Vec<String> = snapshot
            .warnings
            .iter()
            .map(|w| format!("- {}", w))
            .collect();
        push_box(&mut out, "Warnings", &lines);
        out.push('\n');
    }

    let Some(mq) = snapshot.mq.as_ref() else {
        push_box(&mut out, "MQ", &["N/A (mq snapshot missing)".to_string()]);
        return out;
    };

    let mut lines: Vec<String> = Vec::new();
    if mq.channels.is_empty() {
        lines.push("N/A (no channels)".to_string());
        push_box(&mut out, "MQ", &lines);
        return out;
    }

    for ch in &mq.channels {
        let meta_line = match ch.meta.as_ref() {
            Some(m) => format!(
                "capacity={} ttl_seconds={} payload_lease_id={}",
                m.capacity,
                m.ttl_seconds,
                m.payload_lease_id
                    .map(|v| v.to_string())
                    .unwrap_or_else(|| "N/A".to_string())
            ),
            None => "meta=N/A".to_string(),
        };
        lines.push(format!("mq: chan_id={} {}", ch.chan_id, meta_line));

        if ch.unique_keys.is_empty() {
            lines.push("|-- unique_key: N/A".to_string());
        } else {
            for k in &ch.unique_keys {
                lines.push(format!("|-- unique_key: {} -> {}", k, ch.chan_id));
            }
        }

        // Group members by external_client_id first, then by owner_id.
        let mut groups: std::collections::BTreeMap<
            String,
            std::collections::BTreeMap<
                String,
                (
                    Vec<&crate::model::MqProducerSnapshot>,
                    Vec<&crate::model::MqConsumerSnapshot>,
                ),
            >,
        > = std::collections::BTreeMap::new();
        for p in &ch.producers {
            let ext = p.external_client_id.as_deref().unwrap_or("N/A").to_string();
            let owner = p.owner_id.as_deref().unwrap_or("N/A").to_string();
            let entry = groups.entry(ext).or_default().entry(owner).or_default();
            entry.0.push(p);
        }
        for c in &ch.consumers {
            let ext = c.external_client_id.as_deref().unwrap_or("N/A").to_string();
            let owner = c.owner_id.as_deref().unwrap_or("N/A").to_string();
            let entry = groups.entry(ext).or_default().entry(owner).or_default();
            entry.1.push(c);
        }
        if groups.is_empty() {
            lines.push("|-- members: N/A".to_string());
            continue;
        }
        for (ext, owners) in groups {
            lines.push(format!("|-- external_client_id={}", ext));
            for (owner, (producers, consumers)) in owners {
                lines.push(format!("|   |-- owner_id={}", owner));

                lines.push("|   |   |-- producers:".to_string());
                if producers.is_empty() {
                    lines.push("|   |   |   |-- N/A".to_string());
                } else {
                    for p in producers {
                        lines.push(format!(
                            "|   |   |   |-- idx={} status={} produce_offset={} consume_offset={} nonblocking_latest_calls_cap30s={} nonblocking_latest_rps_cap30s={} nonblocking_latest_interval={}",
                            p.producer_idx,
                            mq_status_str(p.status),
                            p.produce_offset
                                .map(|v| v.to_string())
                                .unwrap_or_else(|| "N/A".to_string()),
                            p.consume_offset
                                .map(|v| v.to_string())
                                .unwrap_or_else(|| "N/A".to_string()),
                            fmt_i64_from_opt_f64(p.nonblocking_latest_phase_calls),
                            fmt_hz_from_opt_f64(p.nonblocking_latest_phase_rps),
                            fmt_interval_from_opt_f64(
                                p.nonblocking_latest_begin_unix_ms,
                                p.nonblocking_latest_end_unix_ms,
                            ),
                        ));
                    }
                }

                lines.push("|   |   |-- consumers:".to_string());
                if consumers.is_empty() {
                    lines.push("|   |   |   |-- N/A".to_string());
                } else {
                    for c in consumers {
                        let perf = format!(
                            "get_one_avg_total_ms={} get_one_max_total_ms={} timeouts={} calls={} prefetch_avg_etcd_put_ms={} inflight={} target={} nonblocking_latest_calls_cap30s={} nonblocking_latest_rps_cap30s={} nonblocking_latest_interval={}",
                            fmt_ms_from_opt_us(c.get_one_avg_total_us),
                            fmt_ms_from_opt_us(c.get_one_max_total_us),
                            fmt_i64_from_opt_f64(c.get_one_window_timeouts),
                            fmt_i64_from_opt_f64(c.get_one_window_calls),
                            fmt_ms_from_opt_us(c.prefetch_avg_etcd_put_us),
                            fmt_i64_from_opt_f64(c.prefetch_inflight_queue_size),
                            fmt_i64_from_opt_f64(c.prefetch_target_inflight),
                            fmt_i64_from_opt_f64(c.nonblocking_latest_phase_calls),
                            fmt_hz_from_opt_f64(c.nonblocking_latest_phase_rps),
                            fmt_interval_from_opt_f64(
                                c.nonblocking_latest_begin_unix_ms,
                                c.nonblocking_latest_end_unix_ms,
                            ),
                        );
                        lines.push(format!(
                            "|   |   |   |-- idx={} status={} kvclient_sub_cluster={} perf=[{}]",
                            c.consumer_idx,
                            mq_status_str(c.status),
                            c.kvclient_sub_cluster.as_deref().unwrap_or("N/A"),
                            perf,
                        ));
                    }
                }
            }
        }

        lines.push(String::new());
    }

    if lines.last().map(|s| s.is_empty()).unwrap_or(false) {
        lines.pop();
    }
    push_box(&mut out, "MQ", &lines);
    out
}

const PIXEL_W: usize = 6;
const PIXEL_GAP_W: usize = 1;
const PIXEL_HALF_W: usize = PIXEL_W / 2;

fn push_half_pixel_te(out: &mut String, state: RoutePixelState) {
    match state {
        RoutePixelState::Off => out.push_str(&" ".repeat(PIXEL_HALF_W)),
        RoutePixelState::Direct => {
            out.push_str("\x1b[42m");
            out.push_str(&" ".repeat(PIXEL_HALF_W));
            out.push_str("\x1b[0m");
        }
        RoutePixelState::DirectP2pMode => {
            out.push_str("\x1b[46m");
            out.push_str(&" ".repeat(PIXEL_HALF_W));
            out.push_str("\x1b[0m");
        }
        RoutePixelState::Alt => {
            out.push_str("\x1b[43m");
            out.push_str(&" ".repeat(PIXEL_HALF_W));
            out.push_str("\x1b[0m");
        }
    }
}

fn p2p_bg(p2p_transport: P2pTransportKind) -> &'static str {
    match p2p_transport {
        P2pTransportKind::Ice => "\x1b[44m",
        P2pTransportKind::Tcp => "\x1b[41m",
        P2pTransportKind::Websocket => "\x1b[45m",
        P2pTransportKind::Quic => "\x1b[46m",
        P2pTransportKind::Tquic => "\x1b[102m",
        P2pTransportKind::Unknown => "\x1b[100m",
    }
}

fn push_half_pixel_p2p(out: &mut String, state: RoutePixelState, p2p_transport: P2pTransportKind) {
    match state {
        RoutePixelState::Off => out.push_str(&" ".repeat(PIXEL_HALF_W)),
        RoutePixelState::Direct => {
            out.push_str(p2p_bg(p2p_transport));
            out.push_str(&" ".repeat(PIXEL_HALF_W));
            out.push_str("\x1b[0m");
        }
        RoutePixelState::DirectP2pMode => {
            out.push_str(p2p_bg(p2p_transport));
            out.push_str(&" ".repeat(PIXEL_HALF_W));
            out.push_str("\x1b[0m");
        }
        RoutePixelState::Alt => {
            out.push_str("\x1b[43m");
            out.push_str(&" ".repeat(PIXEL_HALF_W));
            out.push_str("\x1b[0m");
        }
    }
}

fn render_transfer_link_matrix(snapshot: &ClusterSnapshot) -> (Vec<String>, Vec<String>) {
    let mut keys: BTreeSet<String> = BTreeSet::new();
    let mut node_cls_by_key: HashMap<String, &'static str> = HashMap::new();
    for n in &snapshot.nodes {
        for m in &n.members {
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

    let mut lines: Vec<String> = Vec::new();
    let mut notes: Vec<String> = Vec::new();

    if nodes.is_empty() {
        lines.push("N/A (no nodes)".to_string());
        return (lines, notes);
    }

    lines.push("Legend: cell=[p2p][te]".to_string());
    lines.push("  p2p direct (outgoing connect_to alive): ice=blue tcp=red websocket=magenta quic=cyan tquic=bright_green unknown=gray".to_string());
    lines.push("  p2p relay / te fallback = yellow".to_string());
    lines.push("  te direct = green, tequic = cyan".to_string());
    lines.push(String::new());

    if !node_cls_by_key.is_empty() {
        lines.push("Node labels: master=cyan relay=green owner=blue".to_string());
        lines.push(String::new());
    }

    let mut idx_by_key: HashMap<&str, usize> = HashMap::new();
    for (i, k) in nodes.iter().enumerate() {
        idx_by_key.insert(k.as_str(), i);
    }
    let mut pixels_by_edge: HashMap<(usize, usize), RoutePixels> = HashMap::new();
    let mut unknown_routes: BTreeSet<String> = BTreeSet::new();
    let mut orphan_endpoints: BTreeSet<String> = BTreeSet::new();
    for e in &snapshot.transfer_engine_edges {
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
        let ParseRoutePixelsResult { pixels, unknown } = parse_route_pixels(&e.route);
        if unknown {
            unknown_routes.insert(e.route.clone());
            continue;
        }
        pixels_by_edge.insert((from_i, to_i), pixels);
    }

    if !unknown_routes.is_empty() {
        notes.push(
            "Unknown route values detected in etcd; those cells are rendered as empty:".to_string(),
        );
        for v in unknown_routes {
            notes.push(format!("- {}", v));
        }
    }
    if !orphan_endpoints.is_empty() {
        notes.push(
            "Transfer-link endpoints not found in current member list are ignored:".to_string(),
        );
        for v in orphan_endpoints {
            notes.push(format!("- {}", v));
        }
    }

    let row_label_w = nodes.iter().map(|s| s.len()).max().unwrap_or(0);
    let cell_w = (PIXEL_HALF_W * 2) + PIXEL_GAP_W;
    let header_h = nodes.iter().map(|s| s.chars().count()).max().unwrap_or(0);
    fn ansi_for_node_cls(cls: &str) -> &'static str {
        match cls {
            "nmaster" => "\x1b[36m",
            "nrelay" => "\x1b[32m",
            "nowner" => "\x1b[34m",
            _ => "",
        }
    }

    for i in 0..header_h {
        let mut s = " ".repeat(row_label_w + 1);
        for col in 0..nodes.len() {
            let label = &nodes[col];
            let label_len = label.chars().count();
            let pad = header_h.saturating_sub(label_len);
            let ch = if i < pad {
                ' '
            } else {
                label.chars().nth(i - pad).unwrap_or(' ')
            };
            let cls = node_cls_by_key.get(label).copied().unwrap_or("");
            let ansi = ansi_for_node_cls(cls);
            if !ansi.is_empty() {
                s.push_str(ansi);
            }
            s.push(ch);
            if !ansi.is_empty() {
                s.push_str("\x1b[0m");
            }
            s.push_str(&" ".repeat(cell_w.saturating_sub(1)));
        }
        lines.push(s);
    }

    for row in 0..nodes.len() {
        let row_key = &nodes[row];
        let row_cls = node_cls_by_key.get(row_key).copied().unwrap_or("");
        let row_ansi = ansi_for_node_cls(row_cls);
        let row_pad = row_label_w.saturating_sub(row_key.len());
        let mut s = String::new();
        if !row_ansi.is_empty() {
            s.push_str(row_ansi);
        }
        s.push_str(row_key);
        if !row_ansi.is_empty() {
            s.push_str("\x1b[0m");
        }
        s.push_str(&" ".repeat(row_pad));
        s.push(' ');
        for col in 0..nodes.len() {
            if row == col {
                s.push_str("\x1b[100m");
                s.push_str(&" ".repeat(PIXEL_HALF_W));
                s.push_str("\x1b[0m");
                s.push_str(&" ".repeat(PIXEL_GAP_W));
                s.push_str("\x1b[100m");
                s.push_str(&" ".repeat(PIXEL_HALF_W));
                s.push_str("\x1b[0m");
                continue;
            }
            let pixels = pixels_by_edge
                .get(&(row, col))
                .copied()
                .unwrap_or(RoutePixels {
                    p2p: RoutePixelState::Off,
                    p2p_transport: P2pTransportKind::Unknown,
                    te: RoutePixelState::Off,
                });
            push_half_pixel_p2p(&mut s, pixels.p2p, pixels.p2p_transport);
            s.push_str(&" ".repeat(PIXEL_GAP_W));
            push_half_pixel_te(&mut s, pixels.te);
        }
        lines.push(s);
    }

    (lines, notes)
}

fn visible_len_ansi(s: &str) -> usize {
    let mut n = 0usize;
    let b = s.as_bytes();
    let mut i = 0usize;
    while i < b.len() {
        if b[i] == 0x1b {
            if i + 1 < b.len() && b[i + 1] == b'[' {
                i += 2;
                while i < b.len() && b[i] != b'm' {
                    i += 1;
                }
                if i < b.len() {
                    i += 1;
                }
                continue;
            }
        }
        n += 1;
        i += 1;
    }
    n
}

fn push_box(out: &mut String, title: &str, lines: &[String]) {
    let title_len = visible_len_ansi(title);
    let max_line_len = lines.iter().map(|l| visible_len_ansi(l)).max().unwrap_or(0);
    let inner_width = std::cmp::max(title_len, max_line_len);
    let border = format!("+{}+\n", "-".repeat(inner_width + 2));

    out.push_str(&border);
    out.push_str("| ");
    out.push_str(title);
    out.push_str(&" ".repeat(inner_width - title_len));
    out.push_str(" |\n");
    out.push_str(&border);
    for line in lines {
        let pad = inner_width.saturating_sub(visible_len_ansi(line));
        out.push_str("| ");
        out.push_str(line);
        out.push_str(&" ".repeat(pad));
        out.push_str(" |\n");
    }
    out.push_str(&border);
}

fn render_pills_inline(pills: &[UiPill]) -> String {
    let mut out = String::new();
    for (i, p) in pills.iter().enumerate() {
        if i > 0 {
            out.push_str(" | ");
        }
        let text = p.render_text();
        match p.status {
            UiPillStatus::Ok => out.push_str(&text),
            UiPillStatus::Na => {
                out.push_str("\x1b[2m");
                out.push_str(&text);
                out.push_str("\x1b[0m");
            }
            UiPillStatus::Warn => {
                out.push_str("\x1b[33m");
                out.push_str(&text);
                out.push_str("\x1b[0m");
            }
        }
    }
    out
}

fn render_member_lines(m: &MemberSnapshot, out: &mut Vec<String>, prefix: &str) {
    if m.role == MemberRole::OwnerClient {
        out.push(format!(
            "{prefix}|-- instance: id={} role={} accessible_ip={} p2p_listen_port={}",
            m.member_id,
            m.role.as_str(),
            m.accessible_ip.as_deref().unwrap_or("N/A"),
            m.p2p_listen_port
                .map(|v| v.to_string())
                .unwrap_or_else(|| "N/A".to_string()),
        ));
    } else {
        out.push(format!(
            "{prefix}|-- instance: id={} role={} p2p_listen_port={}",
            m.member_id,
            m.role.as_str(),
            m.p2p_listen_port
                .map(|v| v.to_string())
                .unwrap_or_else(|| "N/A".to_string()),
        ));
    }
    let pills = pills_for_instance(m);
    out.push(format!("{prefix}|   {}", render_pills_inline(&pills)));
}

fn render_process_lines(p: &ProcessViewModel, out: &mut Vec<String>, prefix: &str) {
    let pid = p
        .pid
        .map(|v| v.to_string())
        .unwrap_or_else(|| "N/A".to_string());
    let cmd = p.cmd.as_deref().unwrap_or("N/A");
    out.push(format!("{prefix}|-- process: pid={pid} cmd={cmd}"));
    let pills = pills_for_process_resource(p);
    out.push(format!("{prefix}|   {}", render_pills_inline(&pills)));
    for m in &p.instances {
        render_member_lines(m, out, &format!("{prefix}|   "));
    }
}

fn render_node_lines(n: &NodeSnapshot, processes: &[ProcessViewModel], out: &mut Vec<String>) {
    out.push(format!(
        "node: key={} hostname={} accessible_ip={} shared_mem_dir={}",
        n.node_key,
        n.hostname.as_deref().unwrap_or("N/A"),
        n.accessible_ip.as_deref().unwrap_or("N/A"),
        n.shared_mem_dir.as_deref().unwrap_or("N/A"),
    ));
    let pills = pills_for_node_resource(n);
    out.push(format!("|  {}", render_pills_inline(&pills)));
    if processes.is_empty() {
        out.push("|-- process: N/A (no members)".to_string());
    } else {
        for p in processes {
            render_process_lines(p, out, "");
        }
    }
    out.push(String::new());
}

pub fn render_cluster(snapshot: &ClusterSnapshot) -> String {
    if snapshot.member_kind == MemberKind::Mq {
        return render_mq_cluster(snapshot);
    }
    let vm = build_cluster_view_model(snapshot);
    let mut out = String::new();

    let build_lines = vec![
        format!("version: {}", env!("CARGO_PKG_VERSION")),
        format!("commit: {}", crate::build_info::GIT_COMMIT_ID),
        format!("source-sha256: {}", crate::build_info::SOURCE_SHA256),
    ];
    push_box(&mut out, "Build", &build_lines);
    out.push('\n');

    let mut header_lines = vec![
        format!("cluster_name: {}", vm.header.cluster_name),
        format!("member_kind: {}", vm.header.member_kind.as_display_str()),
        format!("etcd_endpoints: {}", vm.header.etcd_endpoints.join(",")),
        format!("prometheus_base_url: {}", vm.header.prometheus_base_url),
    ];
    if let Some(v) = vm.header.master_network_subnet_whitelist.as_ref() {
        header_lines.push(format!("master.network.subnet_whitelist: {}", v));
    }
    push_box(&mut out, "Cluster", &header_lines);
    out.push('\n');

    if !snapshot.warnings.is_empty() {
        let lines: Vec<String> = snapshot
            .warnings
            .iter()
            .map(|w| format!("- {}", w))
            .collect();
        push_box(&mut out, "Warnings", &lines);
        out.push('\n');
    }

    let total_pills = pills_for_cluster_totals(&vm.totals);
    push_box(&mut out, "Totals", &[render_pills_inline(&total_pills)]);
    out.push('\n');

    if let Some(owners) = vm.owner_segment_usage.as_ref() {
        let mut lines: Vec<String> = Vec::new();
        if owners.is_empty() {
            lines.push("N/A (no owner_client nodes)".to_string());
        } else {
            for o in owners {
                lines.push(format!(
                    "owner: {} | total used={} cap={} util={}",
                    o.owner_id, o.total_used, o.total_cap, o.total_util
                ));
                for d in &o.devices {
                    lines.push(format!(
                        "|-- resource={} device={} used={} cap={} util={}",
                        d.resource_kind, d.device, d.used, d.cap, d.util
                    ));
                }
            }
        }
        push_box(&mut out, "Owner storage usage", &lines);
        out.push('\n');
    }

    let mut member_lines: Vec<String> = Vec::new();
    for n in &vm.nodes {
        render_node_lines(&n.node, &n.processes, &mut member_lines);
    }
    if member_lines.is_empty() {
        member_lines.push("N/A (no nodes)".to_string());
    } else if member_lines.last().map(|s| s.is_empty()).unwrap_or(false) {
        member_lines.pop();
    }
    push_box(&mut out, "Cluster members", &member_lines);
    out.push('\n');

    if snapshot.member_kind != MemberKind::Fs {
        let (matrix_lines, notes) = render_transfer_link_matrix(snapshot);
        push_box(&mut out, "Transfer link matrix", &matrix_lines);
        if !notes.is_empty() {
            out.push('\n');
            push_box(&mut out, "Transfer link matrix notes", &notes);
        }
    }
    out
}
