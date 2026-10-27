use crate::{
    ClusterError, ClusterResult, EtcdPrefixScanAction, NodeID, NodeIDString,
    scan_etcd_prefix_paginated,
};
use bitcode::{Decode, Encode};
use etcd_client::Client;
use limit_thirdparty::tokio::sync::ampsc;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeSet, HashMap};
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

#[derive(Clone)]
pub struct TransferLinkP2pSnapshotSource {
    client: Client,
    prefix: String,
}

impl TransferLinkP2pSnapshotSource {
    pub fn new(client: Client, prefix: String) -> Self {
        Self { client, prefix }
    }

    pub async fn fetch_direct_edges(&self) -> ClusterResult<HashMap<NodeID, Vec<NodeID>>> {
        let mut client = self.client.clone();
        let key_prefix = format!("{}/", self.prefix);
        let mut direct_edges: HashMap<NodeID, BTreeSet<NodeID>> = HashMap::new();
        scan_etcd_prefix_paginated(&mut client, &self.prefix, |key, value| {
            let key = match std::str::from_utf8(key) {
                Ok(value) => value,
                Err(err) => {
                    warn!(err = %err, "skipping malformed transfer_link p2p key bytes");
                    return Ok::<EtcdPrefixScanAction, std::convert::Infallible>(
                        EtcdPrefixScanAction::Continue,
                    );
                }
            };
            let value = match std::str::from_utf8(value) {
                Ok(value) => value,
                Err(err) => {
                    warn!(key = %key, err = %err, "skipping malformed transfer_link p2p value bytes");
                    return Ok::<EtcdPrefixScanAction, std::convert::Infallible>(
                        EtcdPrefixScanAction::Continue,
                    );
                }
            };
            let Some(suffix) = key.strip_prefix(&key_prefix) else {
                warn!(key = %key, prefix = %self.prefix, "skipping transfer_link p2p key outside prefix");
                return Ok::<EtcdPrefixScanAction, std::convert::Infallible>(
                    EtcdPrefixScanAction::Continue,
                );
            };
            let mut parts = suffix.split('/');
            let Some(from) = parts.next() else {
                return Ok::<EtcdPrefixScanAction, std::convert::Infallible>(
                    EtcdPrefixScanAction::Continue,
                );
            };
            let Some(to) = parts.next() else {
                warn!(key = %key, "skipping malformed transfer_link p2p key without target");
                return Ok::<EtcdPrefixScanAction, std::convert::Infallible>(
                    EtcdPrefixScanAction::Continue,
                );
            };
            if parts.next().is_some() || from.is_empty() || to.is_empty() {
                warn!(key = %key, "skipping malformed transfer_link p2p key shape");
                return Ok::<EtcdPrefixScanAction, std::convert::Infallible>(
                    EtcdPrefixScanAction::Continue,
                );
            }

            let (p2p_state, _transport) = match TransferLinkRecord::parse_etcd_p2p_value(value) {
                Ok(parsed) => parsed,
                Err(err) => {
                    warn!(key = %key, value = %value, err = %err, "skipping malformed transfer_link p2p record");
                    return Ok::<EtcdPrefixScanAction, std::convert::Infallible>(
                        EtcdPrefixScanAction::Continue,
                    );
                }
            };
            if p2p_state != TransferLinkP2pState::Direct {
                return Ok::<EtcdPrefixScanAction, std::convert::Infallible>(
                    EtcdPrefixScanAction::Continue,
                );
            }

            direct_edges
                .entry(from.to_string().into())
                .or_default()
                .insert(to.to_string().into());
            Ok::<EtcdPrefixScanAction, std::convert::Infallible>(
                EtcdPrefixScanAction::Continue,
            )
        })
        .await
        .map_err(|err| {
            ClusterError::MemberSync(format!(
                "Get transfer_link p2p prefix {} failed: {}",
                self.prefix, err
            ))
        })?;

        Ok(direct_edges
            .into_iter()
            .map(|(from, tos)| (from, tos.into_iter().collect()))
            .collect())
    }
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
    use super::{P2pTransportKind, TransferLinkP2pState, TransferLinkRecord};

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
}
