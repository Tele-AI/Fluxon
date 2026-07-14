use crate::{ClusterError, ClusterResult, ETCD_PREFIX_SCAN_PAGE_LIMIT, NodeID, NodeIDString};
use bitcode::{Decode, Encode};
use etcd_client::{Client, EventType, GetOptions, WatchOptions, WatchStream, Watcher};
use fluxon_util::prefix_scan::{prefix_scan_key_after, prefix_scan_range_end_exclusive};
use limit_thirdparty::tokio::sync::ampsc;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, RwLock, watch};
use tracing::warn;

pub const META_KEY_TRANSFER_READY: &str = "transfer_ready";
pub const META_KEY_TRANSFER_BACKEND_EPOCH: &str = "transfer_backend_epoch";

pub fn transfer_backend_epoch_from_metadata(metadata: &HashMap<String, String>) -> Option<u64> {
    let raw = metadata.get(META_KEY_TRANSFER_BACKEND_EPOCH)?;
    match raw.parse::<u64>() {
        Ok(value) => Some(value),
        Err(err) => {
            warn!(
                key = META_KEY_TRANSFER_BACKEND_EPOCH,
                value = raw,
                err = %err,
                "invalid transfer backend epoch in member metadata"
            );
            None
        }
    }
}

/// Transfer readiness info published after a member's transfer segment is registered.
/// `node_start_time` is the member version key.
/// `backend_epoch` is the transfer backend generation within the member process lifetime.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Encode, Decode)]
pub struct TransferReadyInfo {
    pub node_start_time: i64,
    pub backend_epoch: u64,
    pub ready_ts_micros: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Encode, Decode)]
pub enum TransferLinkP2pState {
    Unknown,
    Direct,
    Relay,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Encode, Decode)]
pub enum P2pTransportKind {
    Ice,
    Tcp,
    Websocket,
    Quic,
    Tquic,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Encode, Decode)]
pub enum TransferLinkTeState {
    None,
    ClosedDirect,
    P2pModeDirect,
    ClosedFallback,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Encode, Decode)]
pub struct TransferLinkRecord {
    pub p2p: TransferLinkP2pState,
    pub p2p_transport: Option<P2pTransportKind>,
    pub te: TransferLinkTeState,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Encode, Decode)]
pub enum TransferLinkKeyKind {
    P2p,
    Te,
}

impl TransferLinkRecord {
    pub fn to_etcd_p2p_value(self) -> String {
        let mut tokens: Vec<&'static str> = Vec::new();
        match self.p2p {
            TransferLinkP2pState::Unknown => {}
            TransferLinkP2pState::Direct => tokens.push("p2p"),
            TransferLinkP2pState::Relay => {
                tokens.push("p2p");
                tokens.push("relay");
            }
        }
        if matches!(self.p2p, TransferLinkP2pState::Direct) {
            if let Some(k) = self.p2p_transport {
                tokens.push(match k {
                    P2pTransportKind::Ice => "ice",
                    P2pTransportKind::Tcp => "tcp",
                    P2pTransportKind::Websocket => "websocket",
                    P2pTransportKind::Quic => "quic",
                    P2pTransportKind::Tquic => "tquic",
                });
            }
        }
        tokens.join("+")
    }

    pub fn to_etcd_te_value(self) -> String {
        let mut tokens: Vec<&'static str> = Vec::new();
        match self.te {
            TransferLinkTeState::None => {}
            TransferLinkTeState::ClosedDirect => tokens.push("closed"),
            TransferLinkTeState::P2pModeDirect => tokens.push("p2p_mode"),
            TransferLinkTeState::ClosedFallback => {
                tokens.push("closed");
                tokens.push("fallback");
            }
        }
        tokens.join("+")
    }

    pub fn to_etcd_value(self) -> String {
        let p2p = self.to_etcd_p2p_value();
        let te = self.to_etcd_te_value();
        if p2p.is_empty() {
            return te;
        }
        if te.is_empty() {
            return p2p;
        }
        format!("{}+{}", p2p, te)
    }

    pub fn parse_etcd_p2p_value(
        raw: &str,
    ) -> Result<(TransferLinkP2pState, Option<P2pTransportKind>), String> {
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            return Ok((TransferLinkP2pState::Unknown, None));
        }

        let mut has_p2p = false;
        let mut is_relay = false;
        let mut transport: Option<P2pTransportKind> = None;
        for token in trimmed.split('+') {
            match token.trim() {
                "" => {}
                "p2p" => has_p2p = true,
                "relay" => is_relay = true,
                "ice" => transport = Some(P2pTransportKind::Ice),
                "tcp" => transport = Some(P2pTransportKind::Tcp),
                "websocket" => transport = Some(P2pTransportKind::Websocket),
                "quic" => transport = Some(P2pTransportKind::Quic),
                "tquic" => transport = Some(P2pTransportKind::Tquic),
                other => {
                    return Err(format!("unknown transfer_link p2p token: {}", other));
                }
            }
        }

        if !has_p2p {
            return Err(format!(
                "invalid transfer_link p2p value without 'p2p' marker: {}",
                raw
            ));
        }
        if is_relay && transport.is_some() {
            return Err(format!(
                "invalid transfer_link p2p value with both relay and transport markers: {}",
                raw
            ));
        }
        if is_relay {
            return Ok((TransferLinkP2pState::Relay, None));
        }
        Ok((TransferLinkP2pState::Direct, transport))
    }
}

const TRANSFER_LINK_P2P_WATCH_RESTART_MIN: Duration = Duration::from_millis(250);
const TRANSFER_LINK_P2P_WATCH_RESTART_MAX: Duration = Duration::from_secs(30);
const TRANSFER_LINK_P2P_WATCH_PROGRESS_INTERVAL: Duration = Duration::from_secs(60);

type DirectEdgeSet = HashMap<NodeIDString, BTreeSet<NodeIDString>>;

#[derive(Default)]
struct TransferLinkP2pSnapshotState {
    direct_edges: DirectEdgeSet,
    revision: i64,
    initialized: bool,
}

struct TransferLinkP2pSnapshotInner {
    client: Client,
    prefix: String,
    state: Arc<RwLock<TransferLinkP2pSnapshotState>>,
    initialize_lock: Mutex<()>,
    stop_tx: watch::Sender<bool>,
}

impl Drop for TransferLinkP2pSnapshotInner {
    fn drop(&mut self) {
        let _ = self.stop_tx.send(true);
    }
}

#[derive(Clone)]
pub struct TransferLinkP2pSnapshotSource {
    inner: Arc<TransferLinkP2pSnapshotInner>,
}

impl TransferLinkP2pSnapshotSource {
    pub fn new(client: Client, prefix: String) -> Self {
        let (stop_tx, _stop_rx) = watch::channel(false);
        Self {
            inner: Arc::new(TransferLinkP2pSnapshotInner {
                client,
                prefix,
                state: Arc::new(RwLock::new(TransferLinkP2pSnapshotState::default())),
                initialize_lock: Mutex::new(()),
                stop_tx,
            }),
        }
    }

    /// Return the latest observed direct-edge snapshot.
    ///
    /// The first call reads one revision-consistent snapshot and establishes a watch from the
    /// following revision. Later calls reuse the incrementally maintained cache.
    pub async fn fetch_direct_edges(&self) -> ClusterResult<HashMap<NodeID, Vec<NodeID>>> {
        self.ensure_initialized().await?;
        let state = self.inner.state.read().await;
        Ok(state
            .direct_edges
            .iter()
            .map(|(from, tos)| {
                (
                    from.clone().into(),
                    tos.iter().cloned().map(Into::into).collect(),
                )
            })
            .collect())
    }

    async fn ensure_initialized(&self) -> ClusterResult<()> {
        if self.inner.state.read().await.initialized {
            return Ok(());
        }

        let _initialize_guard = self.inner.initialize_lock.lock().await;
        if self.inner.state.read().await.initialized {
            return Ok(());
        }

        let mut client = self.inner.client.clone();
        let (direct_edges, revision) =
            load_transfer_link_p2p_snapshot(&mut client, &self.inner.prefix).await?;
        let (watcher, stream) = start_transfer_link_p2p_watch(
            &mut client,
            &self.inner.prefix,
            revision.saturating_add(1),
        )
        .await?;

        {
            let mut state = self.inner.state.write().await;
            state.direct_edges = direct_edges;
            state.revision = revision;
            state.initialized = true;
        }

        tokio::spawn(run_transfer_link_p2p_watch(
            client,
            self.inner.prefix.clone(),
            Arc::clone(&self.inner.state),
            self.inner.stop_tx.subscribe(),
            watcher,
            stream,
        ));
        Ok(())
    }
}

async fn load_transfer_link_p2p_snapshot(
    client: &mut Client,
    prefix: &str,
) -> ClusterResult<(DirectEdgeSet, i64)> {
    let range_end = prefix_scan_range_end_exclusive(prefix.as_bytes()).unwrap_or_else(|| vec![0]);
    let mut start_key = prefix.as_bytes().to_vec();
    let mut snapshot_revision = None;
    let mut direct_edges = DirectEdgeSet::new();

    loop {
        let mut options = GetOptions::new()
            .with_range(range_end.clone())
            .with_limit(ETCD_PREFIX_SCAN_PAGE_LIMIT);
        if let Some(revision) = snapshot_revision {
            options = options.with_revision(revision);
        }
        let response = client
            .get(start_key.clone(), Some(options))
            .await
            .map_err(|err| {
                ClusterError::MemberSync(format!(
                    "Get transfer_link p2p prefix {prefix} failed at start key {start_key:?}: {err}"
                ))
            })?;
        if snapshot_revision.is_none() {
            snapshot_revision = Some(
                response
                    .header()
                    .ok_or_else(|| {
                        ClusterError::MemberSync(format!(
                            "Get transfer_link p2p prefix {prefix} returned no response header"
                        ))
                    })?
                    .revision(),
            );
        }

        for kv in response.kvs() {
            apply_transfer_link_p2p_put(&mut direct_edges, prefix, kv.key(), kv.value());
        }
        if !response.more() {
            break;
        }
        let last_key = response
            .kvs()
            .last()
            .expect("non-empty transfer_link page with more=true must have a last key")
            .key();
        start_key = prefix_scan_key_after(last_key);
    }

    Ok((direct_edges, snapshot_revision.unwrap_or(0)))
}

async fn start_transfer_link_p2p_watch(
    client: &mut Client,
    prefix: &str,
    start_revision: i64,
) -> ClusterResult<(Watcher, WatchStream)> {
    let (watcher, stream) = client
        .watch(
            prefix,
            Some(
                WatchOptions::new()
                    .with_prefix()
                    .with_start_revision(start_revision)
                    .with_progress_notify(),
            ),
        )
        .await
        .map_err(|err| {
            ClusterError::MemberSync(format!(
                "Start transfer_link p2p watch for prefix {prefix} at revision {start_revision} failed: {err}"
            ))
        })?;
    if watcher.watch_id() < 0 {
        return Err(ClusterError::MemberSync(format!(
            "Start transfer_link p2p watch for prefix {prefix} at revision {start_revision} was rejected by etcd"
        )));
    }
    Ok((watcher, stream))
}

async fn run_transfer_link_p2p_watch(
    mut client: Client,
    prefix: String,
    state: Arc<RwLock<TransferLinkP2pSnapshotState>>,
    mut stop_rx: watch::Receiver<bool>,
    mut watcher: Watcher,
    mut stream: WatchStream,
) {
    let mut restart_delay = TRANSFER_LINK_P2P_WATCH_RESTART_MIN;
    loop {
        let mut progress_interval =
            tokio::time::interval(TRANSFER_LINK_P2P_WATCH_PROGRESS_INTERVAL);
        progress_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let needs_resync = loop {
            let message = tokio::select! {
                message = stream.message() => message,
                _ = wait_transfer_link_watch_stop(&mut stop_rx) => return,
                _ = progress_interval.tick() => {
                    if let Err(err) = watcher.request_progress().await {
                        warn!(prefix = %prefix, err = %err, "transfer_link p2p watch progress request failed");
                        break false;
                    }
                    continue;
                }
            };
            match message {
                Ok(Some(response)) => {
                    if response.canceled() {
                        let compact_revision = response.compact_revision();
                        warn!(
                            prefix = %prefix,
                            compact_revision,
                            reason = response.cancel_reason(),
                            "transfer_link p2p watch canceled"
                        );
                        break compact_revision > 0;
                    }
                    apply_transfer_link_p2p_watch_response(&state, &prefix, &response).await;
                    restart_delay = TRANSFER_LINK_P2P_WATCH_RESTART_MIN;
                }
                Ok(None) => {
                    warn!(prefix = %prefix, "transfer_link p2p watch stream closed");
                    break false;
                }
                Err(err) => {
                    warn!(prefix = %prefix, err = %err, "transfer_link p2p watch stream failed");
                    break false;
                }
            }
        };
        drop(watcher);

        if wait_transfer_link_watch_restart(
            &mut stop_rx,
            jittered_transfer_link_watch_restart_delay(restart_delay),
        )
        .await
        {
            return;
        }
        restart_delay = next_transfer_link_watch_restart_delay(restart_delay);

        if needs_resync {
            loop {
                match load_transfer_link_p2p_snapshot(&mut client, &prefix).await {
                    Ok((direct_edges, revision)) => {
                        let mut current = state.write().await;
                        current.direct_edges = direct_edges;
                        current.revision = revision;
                        current.initialized = true;
                        break;
                    }
                    Err(err) => {
                        warn!(prefix = %prefix, err = %err, "transfer_link p2p watch resync failed");
                        if wait_transfer_link_watch_restart(
                            &mut stop_rx,
                            jittered_transfer_link_watch_restart_delay(restart_delay),
                        )
                        .await
                        {
                            return;
                        }
                        restart_delay = next_transfer_link_watch_restart_delay(restart_delay);
                    }
                }
            }
        }

        loop {
            let start_revision = state.read().await.revision.saturating_add(1);
            let watch_result = tokio::select! {
                result = start_transfer_link_p2p_watch(&mut client, &prefix, start_revision) => result,
                _ = wait_transfer_link_watch_stop(&mut stop_rx) => return,
            };
            match watch_result {
                Ok((next_watcher, next_stream)) => {
                    watcher = next_watcher;
                    stream = next_stream;
                    break;
                }
                Err(err) => {
                    warn!(
                        prefix = %prefix,
                        start_revision,
                        err = %err,
                        "transfer_link p2p watch restart failed"
                    );
                    if wait_transfer_link_watch_restart(
                        &mut stop_rx,
                        jittered_transfer_link_watch_restart_delay(restart_delay),
                    )
                    .await
                    {
                        return;
                    }
                    restart_delay = next_transfer_link_watch_restart_delay(restart_delay);
                }
            }
        }
    }
}

async fn apply_transfer_link_p2p_watch_response(
    state: &RwLock<TransferLinkP2pSnapshotState>,
    prefix: &str,
    response: &etcd_client::WatchResponse,
) {
    let mut current = state.write().await;
    let previous_revision = current.revision;
    let mut observed_revision = response
        .header()
        .map(|header| header.revision())
        .unwrap_or(previous_revision);

    for event in response.events() {
        let Some(kv) = event.kv() else {
            continue;
        };
        observed_revision = observed_revision.max(kv.mod_revision());
        if kv.mod_revision() <= previous_revision {
            continue;
        }
        match event.event_type() {
            EventType::Put => {
                apply_transfer_link_p2p_put(&mut current.direct_edges, prefix, kv.key(), kv.value())
            }
            EventType::Delete => {
                remove_transfer_link_p2p_edge(&mut current.direct_edges, prefix, kv.key())
            }
        }
    }
    current.revision = current.revision.max(observed_revision);
}

fn apply_transfer_link_p2p_put(
    direct_edges: &mut DirectEdgeSet,
    prefix: &str,
    key: &[u8],
    value: &[u8],
) {
    let (from, to) = match parse_transfer_link_p2p_key(prefix, key) {
        Ok(edge) => edge,
        Err(err) => {
            warn!(key = ?key, prefix = %prefix, err = %err, "skipping malformed transfer_link p2p key");
            return;
        }
    };
    let raw = match std::str::from_utf8(value) {
        Ok(raw) => raw,
        Err(err) => {
            remove_direct_edge(direct_edges, &from, &to);
            warn!(key = ?key, err = %err, "removing transfer_link p2p edge with malformed value bytes");
            return;
        }
    };
    match TransferLinkRecord::parse_etcd_p2p_value(raw) {
        Ok((TransferLinkP2pState::Direct, _transport)) => {
            direct_edges.entry(from).or_default().insert(to);
        }
        Ok((_state, _transport)) => remove_direct_edge(direct_edges, &from, &to),
        Err(err) => {
            remove_direct_edge(direct_edges, &from, &to);
            warn!(key = ?key, value = raw, err = %err, "removing malformed transfer_link p2p record");
        }
    }
}

fn remove_transfer_link_p2p_edge(direct_edges: &mut DirectEdgeSet, prefix: &str, key: &[u8]) {
    match parse_transfer_link_p2p_key(prefix, key) {
        Ok((from, to)) => remove_direct_edge(direct_edges, &from, &to),
        Err(err) => {
            warn!(key = ?key, prefix = %prefix, err = %err, "skipping malformed deleted transfer_link p2p key");
        }
    }
}

fn remove_direct_edge(direct_edges: &mut DirectEdgeSet, from: &str, to: &str) {
    let remove_from = if let Some(targets) = direct_edges.get_mut(from) {
        targets.remove(to);
        targets.is_empty()
    } else {
        false
    };
    if remove_from {
        direct_edges.remove(from);
    }
}

fn parse_transfer_link_p2p_key(
    prefix: &str,
    key: &[u8],
) -> Result<(NodeIDString, NodeIDString), String> {
    let key = std::str::from_utf8(key).map_err(|err| err.to_string())?;
    let key_prefix = format!("{prefix}/");
    let suffix = key
        .strip_prefix(&key_prefix)
        .ok_or_else(|| format!("key is outside prefix {key_prefix}"))?;
    let mut parts = suffix.split('/');
    let from = parts.next().unwrap_or_default();
    let to = parts.next().unwrap_or_default();
    if from.is_empty() || to.is_empty() || parts.next().is_some() {
        return Err(format!("invalid transfer_link p2p key shape: {key}"));
    }
    Ok((from.to_string(), to.to_string()))
}

async fn wait_transfer_link_watch_stop(stop_rx: &mut watch::Receiver<bool>) {
    loop {
        if *stop_rx.borrow() {
            return;
        }
        if stop_rx.changed().await.is_err() {
            return;
        }
    }
}

async fn wait_transfer_link_watch_restart(
    stop_rx: &mut watch::Receiver<bool>,
    delay: Duration,
) -> bool {
    tokio::select! {
        _ = wait_transfer_link_watch_stop(stop_rx) => true,
        _ = tokio::time::sleep(delay) => false,
    }
}

fn next_transfer_link_watch_restart_delay(current: Duration) -> Duration {
    current
        .checked_mul(2)
        .unwrap_or(TRANSFER_LINK_P2P_WATCH_RESTART_MAX)
        .min(TRANSFER_LINK_P2P_WATCH_RESTART_MAX)
}

fn jittered_transfer_link_watch_restart_delay(current: Duration) -> Duration {
    transfer_link_watch_restart_delay_from_sample(current, rand::random())
}

fn transfer_link_watch_restart_delay_from_sample(current: Duration, sample: u64) -> Duration {
    let max_millis = u64::try_from(current.as_millis()).unwrap_or(u64::MAX);
    let min_millis = (max_millis / 2).max(1);
    let spread = max_millis.saturating_sub(min_millis);
    let offset = if spread == 0 {
        0
    } else {
        ((u128::from(sample) * u128::from(spread)) / u128::from(u64::MAX)) as u64
    };
    Duration::from_millis(min_millis.saturating_add(offset))
}

#[derive(Debug, Clone)]
pub struct TransferLinkEtcdWrite {
    pub kind: TransferLinkKeyKind,
    pub from: NodeIDString,
    pub to: NodeIDString,
    pub value: String,
}

#[derive(Clone)]
pub struct TransferLinkEtcdWriterHandle {
    pub tx: ampsc::Sender<TransferLinkEtcdWrite>,
}

impl TransferLinkEtcdWriterHandle {
    pub fn new(tx: ampsc::Sender<TransferLinkEtcdWrite>) -> Self {
        Self { tx }
    }

    pub fn try_report_p2p(
        &self,
        from: NodeIDString,
        to: NodeIDString,
        record: TransferLinkRecord,
    ) -> ClusterResult<()> {
        if from == to {
            return Ok(());
        }
        let msg = TransferLinkEtcdWrite {
            kind: TransferLinkKeyKind::P2p,
            from,
            to,
            value: record.to_etcd_p2p_value(),
        };
        self.tx.try_send(msg).map_err(|e| {
            ClusterError::Unreachable(format!("transfer_link writer queue send failed: {}", e))
        })
    }

    pub fn try_report_te(
        &self,
        from: NodeIDString,
        to: NodeIDString,
        record: TransferLinkRecord,
    ) -> ClusterResult<()> {
        if from == to {
            return Ok(());
        }
        let msg = TransferLinkEtcdWrite {
            kind: TransferLinkKeyKind::Te,
            from,
            to,
            value: record.to_etcd_te_value(),
        };
        self.tx.try_send(msg).map_err(|e| {
            ClusterError::Unreachable(format!("transfer_link writer queue send failed: {}", e))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{
        DirectEdgeSet, P2pTransportKind, TRANSFER_LINK_P2P_WATCH_RESTART_MAX,
        TRANSFER_LINK_P2P_WATCH_RESTART_MIN, TransferLinkP2pState, TransferLinkRecord,
        apply_transfer_link_p2p_put, next_transfer_link_watch_restart_delay,
        parse_transfer_link_p2p_key, remove_transfer_link_p2p_edge,
        transfer_link_watch_restart_delay_from_sample,
    };

    #[test]
    fn parse_transfer_link_p2p_value_supports_direct_and_relay() {
        assert_eq!(
            TransferLinkRecord::parse_etcd_p2p_value("p2p+quic").unwrap(),
            (TransferLinkP2pState::Direct, Some(P2pTransportKind::Quic))
        );
        assert_eq!(
            TransferLinkRecord::parse_etcd_p2p_value("p2p+relay").unwrap(),
            (TransferLinkP2pState::Relay, None)
        );
        assert_eq!(
            TransferLinkRecord::parse_etcd_p2p_value("").unwrap(),
            (TransferLinkP2pState::Unknown, None)
        );
    }

    #[test]
    fn incremental_transfer_link_updates_add_replace_and_delete_edges() {
        let prefix = "/cluster/transfer_link/p2p";
        let key = b"/cluster/transfer_link/p2p/a/b";
        let mut edges = DirectEdgeSet::new();

        apply_transfer_link_p2p_put(&mut edges, prefix, key, b"p2p+tcp");
        assert!(edges.get("a").is_some_and(|targets| targets.contains("b")));

        apply_transfer_link_p2p_put(&mut edges, prefix, key, b"p2p+relay");
        assert!(!edges.contains_key("a"));

        apply_transfer_link_p2p_put(&mut edges, prefix, key, b"p2p+quic");
        remove_transfer_link_p2p_edge(&mut edges, prefix, key);
        assert!(!edges.contains_key("a"));
    }

    #[test]
    fn malformed_incremental_value_removes_stale_direct_edge() {
        let prefix = "/cluster/transfer_link/p2p";
        let key = b"/cluster/transfer_link/p2p/a/b";
        let mut edges = DirectEdgeSet::new();

        apply_transfer_link_p2p_put(&mut edges, prefix, key, b"p2p+tcp");
        apply_transfer_link_p2p_put(&mut edges, prefix, key, b"unknown-token");

        assert!(!edges.contains_key("a"));
    }

    #[test]
    fn transfer_link_key_parser_requires_exact_edge_shape() {
        let prefix = "/cluster/transfer_link/p2p";
        assert_eq!(
            parse_transfer_link_p2p_key(prefix, b"/cluster/transfer_link/p2p/a/b").unwrap(),
            ("a".to_string(), "b".to_string())
        );
        assert!(
            parse_transfer_link_p2p_key(prefix, b"/cluster/transfer_link/p2p/a/b/extra").is_err()
        );
        assert!(parse_transfer_link_p2p_key(prefix, b"/other/a/b").is_err());
    }

    #[test]
    fn transfer_link_watch_restart_delay_is_bounded() {
        let mut delay = TRANSFER_LINK_P2P_WATCH_RESTART_MIN;
        for _ in 0..32 {
            delay = next_transfer_link_watch_restart_delay(delay);
        }
        assert_eq!(delay, TRANSFER_LINK_P2P_WATCH_RESTART_MAX);
        assert_eq!(
            transfer_link_watch_restart_delay_from_sample(TRANSFER_LINK_P2P_WATCH_RESTART_MAX, 0),
            TRANSFER_LINK_P2P_WATCH_RESTART_MAX / 2
        );
        assert_eq!(
            transfer_link_watch_restart_delay_from_sample(
                TRANSFER_LINK_P2P_WATCH_RESTART_MAX,
                u64::MAX,
            ),
            TRANSFER_LINK_P2P_WATCH_RESTART_MAX
        );
    }
}
