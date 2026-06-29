use std::collections::{HashMap, HashSet, VecDeque};
use std::env;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bitcode::{Decode, Encode};
use fluxon_commu::cluster_manager::ClusterManagerView;
use fluxon_commu::p2p::rpc::{MsgPack, MsgPackSerializePart, RPCCaller, RPCHandler, RPCReq};
use fluxon_commu::p2p::P2pModuleView;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::sync::{mpsc, oneshot};

use crate::keys::{self, MqCategory};
use crate::manager::PRODUCE_OFFSET_BEGIN;

const BROKER_RPC_REQ_MSG_ID: u32 = 8101;
const BROKER_RPC_RESP_MSG_ID: u32 = 8102;
pub const FLUXON_MQ_COMPONENT_METADATA_KEY: &str = "fluxon_mq_component";
pub const FLUXON_MQ_COMPONENT_BROKER_METADATA_VALUE: &str = "broker";
const BROKER_PAYLOAD_BYTES_CAP_ENV: &str = "FLUXON_MQ_BROKER_PAYLOAD_BYTES_CAP";
const BROKER_PAYLOAD_BYTES_CAP_PERCENT_ENV: &str = "FLUXON_MQ_BROKER_PAYLOAD_BYTES_CAP_PERCENT";
const BROKER_CLEANUP_RELEASE_DELAY_MS_ENV: &str = "FLUXON_MQ_BROKER_CLEANUP_RELEASE_DELAY_MS";
const OWNER_POOL_DRAM_BYTES_ENV: &str = "FLUXON_OWNER_POOL_DRAM_BYTES";
const DEFAULT_BROKER_PAYLOAD_BYTES_CAP: u64 = 64 * 1024 * 1024 * 1024;
const DEFAULT_BROKER_PAYLOAD_BYTES_CAP_PERCENT: u64 = 60;
const DEFAULT_BROKER_CLEANUP_RELEASE_DELAY_MS: u64 = 0;
const BROKER_DISCOVERY_TIMEOUT: Duration = Duration::from_secs(15);

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, Encode, Decode)]
pub struct BrokerChannelConfig {
    pub channel_id: i64,
    pub capacity: i64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, Encode, Decode)]
pub struct BrokerReserveRequest {
    pub channel_id: i64,
    pub producer_id: String,
    pub category: MqCategory,
    pub payload_bytes: u64,
    pub now_ms: i64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, Encode, Decode)]
pub struct BrokerFetchRequest {
    pub channel_id: i64,
    pub consumer_id: String,
    pub now_ms: i64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, Encode, Decode)]
pub struct BrokerEnvelope {
    pub channel_id: i64,
    pub producer_id: String,
    pub msg_id: i64,
    pub reservation_id: u64,
    pub payload_key: String,
    pub payload_bytes: u64,
    pub reserved_at_ms: i64,
    pub published_at_ms: Option<i64>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, Encode, Decode)]
pub struct BrokerReservation {
    pub envelope: BrokerEnvelope,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, Encode, Decode)]
pub struct BrokerFetchedMessage {
    pub envelope: BrokerEnvelope,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, Encode, Decode)]
pub struct BrokerFetchBatch {
    pub messages: Vec<BrokerFetchedMessage>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, Encode, Decode)]
pub struct BrokerCommitOutcome {
    pub first_commit: bool,
    pub cleanup: Option<BrokerEnvelope>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, Encode, Decode)]
pub struct BrokerCommitBatchOutcome {
    pub first_commit_count: usize,
    pub cleanup: Vec<BrokerEnvelope>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum BrokerLogRecord {
    UpsertChannel {
        config: BrokerChannelConfig,
    },
    DeleteChannel {
        channel_id: i64,
    },
    Reserve {
        channel_id: i64,
        producer_id: String,
        msg_id: i64,
        reservation_id: u64,
        payload_key: String,
        payload_bytes: u64,
        reserved_at_ms: i64,
    },
    Publish {
        channel_id: i64,
        reservation_id: u64,
        published_at_ms: i64,
    },
    Abort {
        channel_id: i64,
        reservation_id: u64,
    },
    Fetch {
        channel_id: i64,
        reservation_id: u64,
        consumer_id: String,
        fetched_at_ms: i64,
    },
    Commit {
        channel_id: i64,
        reservation_id: u64,
        committed_at_ms: i64,
    },
    CleanupAck {
        channel_id: i64,
        reservation_id: u64,
    },
    RequeueInflight {
        channel_id: i64,
        reservation_id: u64,
    },
}

impl BrokerLogRecord {
    fn reservation_id(&self) -> Option<u64> {
        match self {
            BrokerLogRecord::Reserve { reservation_id, .. }
            | BrokerLogRecord::Publish { reservation_id, .. }
            | BrokerLogRecord::Abort { reservation_id, .. }
            | BrokerLogRecord::Fetch { reservation_id, .. }
            | BrokerLogRecord::Commit { reservation_id, .. }
            | BrokerLogRecord::CleanupAck { reservation_id, .. }
            | BrokerLogRecord::RequeueInflight { reservation_id, .. } => Some(*reservation_id),
            BrokerLogRecord::UpsertChannel { .. } | BrokerLogRecord::DeleteChannel { .. } => None,
        }
    }
}

#[derive(Debug, Error, PartialEq, Eq, Clone, Serialize, Deserialize, Encode, Decode)]
pub enum BrokerError {
    #[error("broker channel not found: channel_id={0}")]
    ChannelNotFound(i64),

    #[error(
        "broker channel capacity must be positive: channel_id={channel_id} capacity={capacity}"
    )]
    InvalidCapacity { channel_id: i64, capacity: i64 },

    #[error(
        "broker channel is full: channel_id={channel_id} capacity={capacity} used_slots={used_slots}"
    )]
    ChannelFull {
        channel_id: i64,
        capacity: i64,
        used_slots: i64,
    },

    #[error(
        "broker payload byte budget is full: requested_bytes={requested_bytes} capacity_bytes={capacity_bytes} used_bytes={used_bytes}"
    )]
    PayloadBytesFull {
        requested_bytes: u64,
        capacity_bytes: u64,
        used_bytes: u64,
    },

    #[error(
        "broker payload is larger than byte budget: requested_bytes={requested_bytes} capacity_bytes={capacity_bytes}"
    )]
    PayloadTooLarge {
        requested_bytes: u64,
        capacity_bytes: u64,
    },

    #[error(
        "broker reservation not found: channel_id={channel_id} reservation_id={reservation_id}"
    )]
    ReservationNotFound {
        channel_id: i64,
        reservation_id: u64,
    },

    #[error(
        "broker delivery not in-flight: channel_id={channel_id} reservation_id={reservation_id}"
    )]
    DeliveryNotFound {
        channel_id: i64,
        reservation_id: u64,
    },

    #[error("invalid broker log record: {0}")]
    InvalidRecord(String),

    #[error("broker master unavailable: {0}")]
    BrokerUnavailable(String),

    #[error("broker rpc error: {0}")]
    Rpc(String),

    #[error("broker actor closed")]
    ActorClosed,
}

#[derive(Debug, Default)]
pub struct LocalBroker {
    state: BrokerState,
    log: Vec<BrokerLogRecord>,
}

#[derive(Debug)]
struct BrokerState {
    channels: HashMap<i64, ChannelState>,
    payload_byte_capacity: u64,
    used_payload_bytes: u64,
}

impl Default for BrokerState {
    fn default() -> Self {
        Self {
            channels: HashMap::new(),
            payload_byte_capacity: default_payload_byte_capacity(),
            used_payload_bytes: 0,
        }
    }
}

#[derive(Debug)]
struct ChannelState {
    config: BrokerChannelConfig,
    next_reservation_id: u64,
    next_msg_by_producer: HashMap<String, i64>,
    pending: HashMap<u64, BrokerEnvelope>,
    visible: VecDeque<BrokerEnvelope>,
    inflight: HashMap<u64, BrokerEnvelope>,
    inflight_order: VecDeque<u64>,
    cleanup: VecDeque<BrokerEnvelope>,
    cleanup_inflight: HashMap<u64, BrokerEnvelope>,
    committed: HashSet<u64>,
    used_slots: i64,
    reserve_waiters: VecDeque<ReserveWaiter>,
    fetch_waiters: VecDeque<FetchWaiter>,
}

impl ChannelState {
    fn new(config: BrokerChannelConfig) -> Self {
        Self {
            config,
            next_reservation_id: 1,
            next_msg_by_producer: HashMap::new(),
            pending: HashMap::new(),
            visible: VecDeque::new(),
            inflight: HashMap::new(),
            inflight_order: VecDeque::new(),
            cleanup: VecDeque::new(),
            cleanup_inflight: HashMap::new(),
            committed: HashSet::new(),
            used_slots: 0,
            reserve_waiters: VecDeque::new(),
            fetch_waiters: VecDeque::new(),
        }
    }
}

#[derive(Debug)]
struct ReserveWaiter {
    req: BrokerReserveRequest,
    reply: oneshot::Sender<Result<BrokerReservation, BrokerError>>,
}

#[derive(Debug)]
struct FetchWaiter {
    req: BrokerFetchRequest,
    reply: oneshot::Sender<Result<Option<BrokerFetchedMessage>, BrokerError>>,
}

impl LocalBroker {
    pub fn new() -> Self {
        Self::default()
    }

    #[cfg(test)]
    fn with_payload_byte_capacity(payload_byte_capacity: u64) -> Self {
        Self {
            state: BrokerState {
                channels: HashMap::new(),
                payload_byte_capacity: payload_byte_capacity.max(1),
                used_payload_bytes: 0,
            },
            log: Vec::new(),
        }
    }

    pub fn from_log(records: &[BrokerLogRecord]) -> Result<Self, BrokerError> {
        let mut broker = Self::new();
        for record in records {
            broker.apply_log_record(record.clone())?;
            broker.log.push(record.clone());
        }
        Ok(broker)
    }

    pub fn log_records(&self) -> &[BrokerLogRecord] {
        &self.log
    }

    pub fn upsert_channel(&mut self, config: BrokerChannelConfig) -> Result<(), BrokerError> {
        let record = self.upsert_channel_record(config)?;
        self.apply_and_record(record)
    }

    pub fn delete_channel(&mut self, channel_id: i64) -> Result<Vec<String>, BrokerError> {
        let payload_keys = self.delete_channel_state(channel_id);
        self.log.push(BrokerLogRecord::DeleteChannel { channel_id });
        Ok(payload_keys)
    }

    pub fn reserve(&mut self, req: BrokerReserveRequest) -> Result<BrokerReservation, BrokerError> {
        let channel_id = req.channel_id;
        let record = self.reserve_record(req)?;
        let reservation_id = record.reservation_id().ok_or_else(|| {
            BrokerError::InvalidRecord("reserve record missing reservation_id".to_string())
        })?;
        self.apply_and_record(record)?;
        let envelope = self
            .channel(channel_id)?
            .pending
            .get(&reservation_id)
            .cloned()
            .ok_or(BrokerError::ReservationNotFound {
                channel_id,
                reservation_id,
            })?;
        Ok(BrokerReservation { envelope })
    }

    pub fn publish(
        &mut self,
        channel_id: i64,
        reservation_id: u64,
        now_ms: i64,
    ) -> Result<BrokerEnvelope, BrokerError> {
        let record = self.publish_record(channel_id, reservation_id, now_ms)?;
        self.apply_and_record(record)?;
        self.channel(channel_id)?
            .visible
            .iter()
            .find(|env| env.reservation_id == reservation_id)
            .cloned()
            .ok_or(BrokerError::ReservationNotFound {
                channel_id,
                reservation_id,
            })
    }

    pub fn abort(&mut self, channel_id: i64, reservation_id: u64) -> Result<(), BrokerError> {
        let record = self.abort_record(channel_id, reservation_id)?;
        self.apply_and_record(record)
    }

    pub fn fetch_next(
        &mut self,
        req: BrokerFetchRequest,
    ) -> Result<Option<BrokerFetchedMessage>, BrokerError> {
        let channel_id = req.channel_id;
        let Some(record) = self.fetch_record(req)? else {
            return Ok(None);
        };
        let reservation_id = record.reservation_id().ok_or_else(|| {
            BrokerError::InvalidRecord("fetch record missing reservation_id".to_string())
        })?;
        self.apply_and_record(record)?;
        let envelope = self
            .channel(channel_id)?
            .inflight
            .get(&reservation_id)
            .cloned()
            .ok_or(BrokerError::DeliveryNotFound {
                channel_id,
                reservation_id,
            })?;
        Ok(Some(BrokerFetchedMessage { envelope }))
    }

    pub fn fetch_batch_available(
        &mut self,
        req: BrokerFetchRequest,
        max_items: usize,
    ) -> Result<BrokerFetchBatch, BrokerError> {
        let mut messages = Vec::new();
        for _ in 0..max_items {
            let Some(message) = self.fetch_next(req.clone())? else {
                break;
            };
            messages.push(message);
        }
        Ok(BrokerFetchBatch { messages })
    }

    pub fn commit(
        &mut self,
        channel_id: i64,
        reservation_id: u64,
        now_ms: i64,
    ) -> Result<BrokerCommitOutcome, BrokerError> {
        let Some(record) = self.commit_record(channel_id, reservation_id, now_ms)? else {
            return Ok(BrokerCommitOutcome {
                first_commit: false,
                cleanup: None,
            });
        };
        self.apply_and_record(record)?;
        let cleanup = self.channel(channel_id)?.cleanup.back().cloned();
        Ok(BrokerCommitOutcome {
            first_commit: true,
            cleanup,
        })
    }

    pub fn commit_batch(
        &mut self,
        channel_id: i64,
        reservation_ids: Vec<u64>,
        now_ms: i64,
    ) -> Result<BrokerCommitBatchOutcome, BrokerError> {
        let mut cleanup = Vec::new();
        let mut first_commit_count = 0usize;
        for reservation_id in reservation_ids {
            let outcome = self.commit(channel_id, reservation_id, now_ms)?;
            if outcome.first_commit {
                first_commit_count += 1;
                if let Some(envelope) = outcome.cleanup {
                    cleanup.push(envelope);
                }
            }
        }
        Ok(BrokerCommitBatchOutcome {
            first_commit_count,
            cleanup,
        })
    }

    pub fn requeue_inflight(
        &mut self,
        channel_id: i64,
        reservation_id: u64,
    ) -> Result<(), BrokerError> {
        let record = self.requeue_inflight_record(channel_id, reservation_id)?;
        self.apply_and_record(record)
    }

    pub fn requeue_all_inflight(&mut self, channel_id: i64) -> Result<(), BrokerError> {
        let reservation_ids: Vec<u64> = self
            .channel(channel_id)?
            .inflight_order
            .iter()
            .rev()
            .copied()
            .collect();
        for reservation_id in reservation_ids {
            self.requeue_inflight(channel_id, reservation_id)?;
        }
        Ok(())
    }

    pub fn take_cleanup_batch(
        &mut self,
        channel_id: i64,
        max_items: usize,
    ) -> Result<Vec<BrokerEnvelope>, BrokerError> {
        let channel = self.channel_mut(channel_id)?;
        let mut batch = Vec::new();
        for _ in 0..max_items {
            let Some(envelope) = channel.cleanup.pop_front() else {
                break;
            };
            channel
                .cleanup_inflight
                .insert(envelope.reservation_id, envelope.clone());
            batch.push(envelope);
        }
        Ok(batch)
    }

    pub fn cleanup_ack(&mut self, channel_id: i64, reservation_id: u64) -> Result<(), BrokerError> {
        let record = self.cleanup_ack_record(channel_id, reservation_id)?;
        let _ = self.apply_cleanup_ack_and_record(record, true)?;
        Ok(())
    }

    pub fn cleanup_ack_for_delayed_release(
        &mut self,
        channel_id: i64,
        reservation_id: u64,
    ) -> Result<u64, BrokerError> {
        let record = self.cleanup_ack_record(channel_id, reservation_id)?;
        self.apply_cleanup_ack_and_record(record, false)
    }

    pub fn cleanup_nack(
        &mut self,
        channel_id: i64,
        reservation_id: u64,
    ) -> Result<(), BrokerError> {
        let channel = self.channel_mut(channel_id)?;
        if let Some(envelope) = channel.cleanup_inflight.remove(&reservation_id) {
            channel.cleanup.push_front(envelope);
        }
        Ok(())
    }

    pub fn apply_log_record(&mut self, record: BrokerLogRecord) -> Result<(), BrokerError> {
        match record {
            BrokerLogRecord::UpsertChannel { config } => {
                validate_capacity(&config)?;
                match self.state.channels.get_mut(&config.channel_id) {
                    Some(channel) => {
                        if config.capacity < channel.used_slots {
                            return Err(BrokerError::InvalidRecord(format!(
                                "channel_id={} capacity={} below used_slots={}",
                                config.channel_id, config.capacity, channel.used_slots
                            )));
                        }
                        channel.config = config;
                    }
                    None => {
                        self.state
                            .channels
                            .insert(config.channel_id, ChannelState::new(config));
                    }
                }
            }
            BrokerLogRecord::DeleteChannel { channel_id } => {
                let _ = self.delete_channel_state(channel_id);
            }
            BrokerLogRecord::Reserve {
                channel_id,
                producer_id,
                msg_id,
                reservation_id,
                payload_key,
                payload_bytes,
                reserved_at_ms,
            } => {
                let channel = self.channel(channel_id)?;
                if channel.pending.contains_key(&reservation_id)
                    || channel
                        .visible
                        .iter()
                        .any(|env| env.reservation_id == reservation_id)
                    || channel.inflight.contains_key(&reservation_id)
                    || channel.committed.contains(&reservation_id)
                {
                    return Err(BrokerError::InvalidRecord(format!(
                        "duplicate reservation_id={} for channel_id={}",
                        reservation_id, channel_id
                    )));
                }
                if payload_bytes > self.state.payload_byte_capacity {
                    return Err(BrokerError::PayloadTooLarge {
                        requested_bytes: payload_bytes,
                        capacity_bytes: self.state.payload_byte_capacity,
                    });
                }
                if self.state.used_payload_bytes.saturating_add(payload_bytes)
                    > self.state.payload_byte_capacity
                {
                    return Err(BrokerError::PayloadBytesFull {
                        requested_bytes: payload_bytes,
                        capacity_bytes: self.state.payload_byte_capacity,
                        used_bytes: self.state.used_payload_bytes,
                    });
                }

                let channel = self.channel_mut(channel_id)?;
                channel.next_reservation_id = channel.next_reservation_id.max(reservation_id + 1);
                let next_msg = channel
                    .next_msg_by_producer
                    .entry(producer_id.clone())
                    .or_insert(PRODUCE_OFFSET_BEGIN + 1);
                *next_msg = (*next_msg).max(msg_id + 1);

                let envelope = BrokerEnvelope {
                    channel_id,
                    producer_id,
                    msg_id,
                    reservation_id,
                    payload_key,
                    payload_bytes,
                    reserved_at_ms,
                    published_at_ms: None,
                };
                channel.pending.insert(reservation_id, envelope);
                channel.used_slots += 1;
                self.state.used_payload_bytes += payload_bytes;
            }
            BrokerLogRecord::Publish {
                channel_id,
                reservation_id,
                published_at_ms,
            } => {
                let channel = self.channel_mut(channel_id)?;
                let mut envelope = channel.pending.remove(&reservation_id).ok_or(
                    BrokerError::ReservationNotFound {
                        channel_id,
                        reservation_id,
                    },
                )?;
                envelope.published_at_ms = Some(published_at_ms);
                channel.visible.push_back(envelope);
            }
            BrokerLogRecord::Abort {
                channel_id,
                reservation_id,
            } => {
                let channel = self.channel_mut(channel_id)?;
                let envelope = channel.pending.remove(&reservation_id).ok_or(
                    BrokerError::ReservationNotFound {
                        channel_id,
                        reservation_id,
                    },
                )?;
                channel.used_slots -= 1;
                self.release_payload_bytes(envelope.payload_bytes);
            }
            BrokerLogRecord::Fetch {
                channel_id,
                reservation_id,
                consumer_id: _,
                fetched_at_ms: _,
            } => {
                let channel = self.channel_mut(channel_id)?;
                let Some(front) = channel.visible.front() else {
                    return Err(BrokerError::ReservationNotFound {
                        channel_id,
                        reservation_id,
                    });
                };
                if front.reservation_id != reservation_id {
                    return Err(BrokerError::InvalidRecord(format!(
                        "fetch order mismatch for channel_id={}: expected_reservation_id={} actual_reservation_id={}",
                        channel_id, front.reservation_id, reservation_id
                    )));
                }
                let envelope = channel
                    .visible
                    .pop_front()
                    .expect("visible front checked above");
                channel.inflight.insert(reservation_id, envelope);
                channel.inflight_order.push_back(reservation_id);
            }
            BrokerLogRecord::Commit {
                channel_id,
                reservation_id,
                committed_at_ms: _,
            } => {
                let channel = self.channel_mut(channel_id)?;
                if channel.committed.contains(&reservation_id) {
                    return Ok(());
                }
                let envelope = channel.inflight.remove(&reservation_id).ok_or(
                    BrokerError::DeliveryNotFound {
                        channel_id,
                        reservation_id,
                    },
                )?;
                remove_from_deque(&mut channel.inflight_order, reservation_id);
                channel.committed.insert(reservation_id);
                channel.cleanup.push_back(envelope);
                channel.used_slots -= 1;
            }
            BrokerLogRecord::CleanupAck {
                channel_id,
                reservation_id,
            } => {
                self.apply_cleanup_ack(channel_id, reservation_id, true)?;
            }
            BrokerLogRecord::RequeueInflight {
                channel_id,
                reservation_id,
            } => {
                let channel = self.channel_mut(channel_id)?;
                let envelope = channel.inflight.remove(&reservation_id).ok_or(
                    BrokerError::DeliveryNotFound {
                        channel_id,
                        reservation_id,
                    },
                )?;
                remove_from_deque(&mut channel.inflight_order, reservation_id);
                channel.visible.push_front(envelope);
            }
        }
        Ok(())
    }

    fn release_payload_bytes(&mut self, payload_bytes: u64) {
        self.state.used_payload_bytes = self.state.used_payload_bytes.saturating_sub(payload_bytes);
    }

    fn delete_channel_state(&mut self, channel_id: i64) -> Vec<String> {
        let Some(mut channel) = self.state.channels.remove(&channel_id) else {
            return Vec::new();
        };

        let mut payload_bytes = 0u64;
        let mut payload_keys = Vec::new();
        collect_deleted_payloads(
            channel.pending.drain().map(|(_, envelope)| envelope),
            &mut payload_keys,
            &mut payload_bytes,
        );
        collect_deleted_payloads(
            channel.visible.drain(..),
            &mut payload_keys,
            &mut payload_bytes,
        );
        collect_deleted_payloads(
            channel.inflight.drain().map(|(_, envelope)| envelope),
            &mut payload_keys,
            &mut payload_bytes,
        );
        collect_deleted_payloads(
            channel.cleanup.drain(..),
            &mut payload_keys,
            &mut payload_bytes,
        );
        collect_deleted_payloads(
            channel
                .cleanup_inflight
                .drain()
                .map(|(_, envelope)| envelope),
            &mut payload_keys,
            &mut payload_bytes,
        );

        while let Some(waiter) = channel.reserve_waiters.pop_front() {
            let _ = waiter
                .reply
                .send(Err(BrokerError::ChannelNotFound(channel_id)));
        }
        while let Some(waiter) = channel.fetch_waiters.pop_front() {
            let _ = waiter
                .reply
                .send(Err(BrokerError::ChannelNotFound(channel_id)));
        }

        self.release_payload_bytes(payload_bytes);
        payload_keys
    }

    fn apply_cleanup_ack(
        &mut self,
        channel_id: i64,
        reservation_id: u64,
        release_payload_now: bool,
    ) -> Result<u64, BrokerError> {
        let channel = self.channel_mut(channel_id)?;
        let envelope = if let Some(envelope) = channel.cleanup_inflight.remove(&reservation_id) {
            envelope
        } else if let Some(pos) = channel
            .cleanup
            .iter()
            .position(|env| env.reservation_id == reservation_id)
        {
            channel
                .cleanup
                .remove(pos)
                .expect("cleanup envelope position checked above")
        } else {
            return Err(BrokerError::ReservationNotFound {
                channel_id,
                reservation_id,
            });
        };
        channel.committed.remove(&reservation_id);
        let payload_bytes = envelope.payload_bytes;
        if release_payload_now {
            self.release_payload_bytes(payload_bytes);
        }
        Ok(payload_bytes)
    }

    fn apply_and_record(&mut self, record: BrokerLogRecord) -> Result<(), BrokerError> {
        self.apply_log_record(record.clone())?;
        self.log.push(record);
        Ok(())
    }

    fn apply_cleanup_ack_and_record(
        &mut self,
        record: BrokerLogRecord,
        release_payload_now: bool,
    ) -> Result<u64, BrokerError> {
        let BrokerLogRecord::CleanupAck {
            channel_id,
            reservation_id,
        } = record.clone()
        else {
            return Err(BrokerError::InvalidRecord(
                "apply_cleanup_ack_and_record requires CleanupAck".to_string(),
            ));
        };
        let payload_bytes =
            self.apply_cleanup_ack(channel_id, reservation_id, release_payload_now)?;
        self.log.push(record);
        Ok(payload_bytes)
    }

    fn upsert_channel_record(
        &self,
        config: BrokerChannelConfig,
    ) -> Result<BrokerLogRecord, BrokerError> {
        validate_capacity(&config)?;
        if let Some(channel) = self.state.channels.get(&config.channel_id) {
            if config.capacity < channel.used_slots {
                return Err(BrokerError::InvalidRecord(format!(
                    "channel_id={} capacity={} below used_slots={}",
                    config.channel_id, config.capacity, channel.used_slots
                )));
            }
        }
        Ok(BrokerLogRecord::UpsertChannel { config })
    }

    fn reserve_record(&self, req: BrokerReserveRequest) -> Result<BrokerLogRecord, BrokerError> {
        let channel = self.channel(req.channel_id)?;
        if broker_category_enforces_capacity(req.category)
            && channel.used_slots >= channel.config.capacity
        {
            return Err(BrokerError::ChannelFull {
                channel_id: req.channel_id,
                capacity: channel.config.capacity,
                used_slots: channel.used_slots,
            });
        }

        let msg_id = channel
            .next_msg_by_producer
            .get(&req.producer_id)
            .copied()
            .unwrap_or(PRODUCE_OFFSET_BEGIN + 1);
        let reservation_id = channel.next_reservation_id;
        let payload_key = keys::backend_message_key_with_category(
            req.channel_id,
            &req.producer_id,
            msg_id,
            &req.category,
        );
        let payload_bytes = req.payload_bytes.max(1);
        if payload_bytes > self.state.payload_byte_capacity {
            return Err(BrokerError::PayloadTooLarge {
                requested_bytes: payload_bytes,
                capacity_bytes: self.state.payload_byte_capacity,
            });
        }
        if self.state.used_payload_bytes.saturating_add(payload_bytes)
            > self.state.payload_byte_capacity
        {
            return Err(BrokerError::PayloadBytesFull {
                requested_bytes: payload_bytes,
                capacity_bytes: self.state.payload_byte_capacity,
                used_bytes: self.state.used_payload_bytes,
            });
        }
        Ok(BrokerLogRecord::Reserve {
            channel_id: req.channel_id,
            producer_id: req.producer_id,
            msg_id,
            reservation_id,
            payload_key,
            payload_bytes,
            reserved_at_ms: req.now_ms,
        })
    }

    fn publish_record(
        &self,
        channel_id: i64,
        reservation_id: u64,
        now_ms: i64,
    ) -> Result<BrokerLogRecord, BrokerError> {
        let channel = self.channel(channel_id)?;
        if !channel.pending.contains_key(&reservation_id) {
            return Err(BrokerError::ReservationNotFound {
                channel_id,
                reservation_id,
            });
        }
        Ok(BrokerLogRecord::Publish {
            channel_id,
            reservation_id,
            published_at_ms: now_ms,
        })
    }

    fn abort_record(
        &self,
        channel_id: i64,
        reservation_id: u64,
    ) -> Result<BrokerLogRecord, BrokerError> {
        let channel = self.channel(channel_id)?;
        if !channel.pending.contains_key(&reservation_id) {
            return Err(BrokerError::ReservationNotFound {
                channel_id,
                reservation_id,
            });
        }
        Ok(BrokerLogRecord::Abort {
            channel_id,
            reservation_id,
        })
    }

    fn fetch_record(
        &self,
        req: BrokerFetchRequest,
    ) -> Result<Option<BrokerLogRecord>, BrokerError> {
        let reservation_id = match self.channel(req.channel_id)?.visible.front() {
            Some(env) => env.reservation_id,
            None => return Ok(None),
        };
        Ok(Some(BrokerLogRecord::Fetch {
            channel_id: req.channel_id,
            reservation_id,
            consumer_id: req.consumer_id,
            fetched_at_ms: req.now_ms,
        }))
    }

    fn commit_record(
        &self,
        channel_id: i64,
        reservation_id: u64,
        now_ms: i64,
    ) -> Result<Option<BrokerLogRecord>, BrokerError> {
        let channel = self.channel(channel_id)?;
        if channel.committed.contains(&reservation_id) {
            return Ok(None);
        }
        if !channel.inflight.contains_key(&reservation_id) {
            return Err(BrokerError::DeliveryNotFound {
                channel_id,
                reservation_id,
            });
        }
        Ok(Some(BrokerLogRecord::Commit {
            channel_id,
            reservation_id,
            committed_at_ms: now_ms,
        }))
    }

    fn cleanup_ack_record(
        &self,
        channel_id: i64,
        reservation_id: u64,
    ) -> Result<BrokerLogRecord, BrokerError> {
        let channel = self.channel(channel_id)?;
        let exists = channel.cleanup_inflight.contains_key(&reservation_id)
            || channel
                .cleanup
                .iter()
                .any(|env| env.reservation_id == reservation_id);
        if !exists {
            return Err(BrokerError::ReservationNotFound {
                channel_id,
                reservation_id,
            });
        }
        Ok(BrokerLogRecord::CleanupAck {
            channel_id,
            reservation_id,
        })
    }

    fn requeue_inflight_record(
        &self,
        channel_id: i64,
        reservation_id: u64,
    ) -> Result<BrokerLogRecord, BrokerError> {
        let channel = self.channel(channel_id)?;
        if !channel.inflight.contains_key(&reservation_id) {
            return Err(BrokerError::DeliveryNotFound {
                channel_id,
                reservation_id,
            });
        }
        Ok(BrokerLogRecord::RequeueInflight {
            channel_id,
            reservation_id,
        })
    }

    fn channel(&self, channel_id: i64) -> Result<&ChannelState, BrokerError> {
        self.state
            .channels
            .get(&channel_id)
            .ok_or(BrokerError::ChannelNotFound(channel_id))
    }

    fn channel_mut(&mut self, channel_id: i64) -> Result<&mut ChannelState, BrokerError> {
        self.state
            .channels
            .get_mut(&channel_id)
            .ok_or(BrokerError::ChannelNotFound(channel_id))
    }
}

fn drain_reserve_waiters(broker: &mut LocalBroker) {
    loop {
        let channel_ids: Vec<i64> = broker.state.channels.keys().copied().collect();
        let mut progressed = false;
        for channel_id in channel_ids {
            progressed |= drain_reserve_waiters_for_channel(broker, channel_id);
        }
        if !progressed {
            return;
        }
    }
}

fn drain_reserve_waiters_for_channel(broker: &mut LocalBroker, channel_id: i64) -> bool {
    let mut progressed = false;
    loop {
        let waiter = match broker.channel_mut(channel_id) {
            Ok(channel) => channel.reserve_waiters.pop_front(),
            Err(_) => return progressed,
        };
        let Some(waiter) = waiter else {
            return progressed;
        };

        match broker.reserve(waiter.req.clone()) {
            Ok(reservation) => {
                if let Err(Ok(reservation)) = waiter.reply.send(Ok(reservation)) {
                    let _ = broker.abort(channel_id, reservation.envelope.reservation_id);
                }
                progressed = true;
            }
            Err(BrokerError::ChannelFull { .. }) | Err(BrokerError::PayloadBytesFull { .. }) => {
                if let Ok(channel) = broker.channel_mut(channel_id) {
                    channel.reserve_waiters.push_front(waiter);
                }
                return progressed;
            }
            Err(err) => {
                let _ = waiter.reply.send(Err(err));
                progressed = true;
            }
        }
    }
}

fn drain_fetch_waiters_for_channel(broker: &mut LocalBroker, channel_id: i64) {
    loop {
        let waiter = match broker.channel_mut(channel_id) {
            Ok(channel) => channel.fetch_waiters.pop_front(),
            Err(_) => return,
        };
        let Some(waiter) = waiter else {
            return;
        };

        match broker.fetch_next(waiter.req.clone()) {
            Ok(Some(fetched)) => {
                if let Err(Ok(Some(fetched))) = waiter.reply.send(Ok(Some(fetched))) {
                    let _ = broker.requeue_inflight(
                        fetched.envelope.channel_id,
                        fetched.envelope.reservation_id,
                    );
                }
            }
            Ok(None) => {
                if let Ok(channel) = broker.channel_mut(channel_id) {
                    channel.fetch_waiters.push_front(waiter);
                }
                return;
            }
            Err(err) => {
                let _ = waiter.reply.send(Err(err));
            }
        }
    }
}

fn fail_all_waiters_with_actor_closed(broker: &mut LocalBroker) {
    for channel in broker.state.channels.values_mut() {
        while let Some(waiter) = channel.reserve_waiters.pop_front() {
            let _ = waiter.reply.send(Err(BrokerError::ActorClosed));
        }
        while let Some(waiter) = channel.fetch_waiters.pop_front() {
            let _ = waiter.reply.send(Err(BrokerError::ActorClosed));
        }
    }
}

fn collect_deleted_payloads(
    envelopes: impl Iterator<Item = BrokerEnvelope>,
    payload_keys: &mut Vec<String>,
    payload_bytes: &mut u64,
) {
    for envelope in envelopes {
        *payload_bytes = payload_bytes.saturating_add(envelope.payload_bytes);
        payload_keys.push(envelope.payload_key);
    }
}

enum BrokerCommand {
    UpsertChannel {
        config: BrokerChannelConfig,
        reply: oneshot::Sender<Result<(), BrokerError>>,
    },
    DeleteChannel {
        channel_id: i64,
        reply: oneshot::Sender<Result<Vec<String>, BrokerError>>,
    },
    Reserve {
        req: BrokerReserveRequest,
        reply: oneshot::Sender<Result<BrokerReservation, BrokerError>>,
    },
    Publish {
        channel_id: i64,
        reservation_id: u64,
        now_ms: i64,
        reply: oneshot::Sender<Result<BrokerEnvelope, BrokerError>>,
    },
    Abort {
        channel_id: i64,
        reservation_id: u64,
        reply: oneshot::Sender<Result<(), BrokerError>>,
    },
    FetchNext {
        req: BrokerFetchRequest,
        reply: oneshot::Sender<Result<Option<BrokerFetchedMessage>, BrokerError>>,
    },
    FetchBatchAvailable {
        req: BrokerFetchRequest,
        max_items: usize,
        reply: oneshot::Sender<Result<BrokerFetchBatch, BrokerError>>,
    },
    Commit {
        channel_id: i64,
        reservation_id: u64,
        now_ms: i64,
        reply: oneshot::Sender<Result<BrokerCommitOutcome, BrokerError>>,
    },
    CommitBatch {
        channel_id: i64,
        reservation_ids: Vec<u64>,
        now_ms: i64,
        reply: oneshot::Sender<Result<BrokerCommitBatchOutcome, BrokerError>>,
    },
    RequeueInflight {
        channel_id: i64,
        reservation_id: u64,
        reply: oneshot::Sender<Result<(), BrokerError>>,
    },
    RequeueAllInflight {
        channel_id: i64,
        reply: oneshot::Sender<Result<(), BrokerError>>,
    },
    TakeCleanupBatch {
        channel_id: i64,
        max_items: usize,
        reply: oneshot::Sender<Result<Vec<BrokerEnvelope>, BrokerError>>,
    },
    CleanupAck {
        channel_id: i64,
        reservation_id: u64,
        reply: oneshot::Sender<Result<(), BrokerError>>,
    },
    CleanupNack {
        channel_id: i64,
        reservation_id: u64,
        reply: oneshot::Sender<Result<(), BrokerError>>,
    },
    ReleasePayloadBytes {
        payload_bytes: u64,
    },
    Shutdown {
        reply: oneshot::Sender<Result<(), BrokerError>>,
    },
}

#[derive(Clone, Debug)]
struct LocalBrokerHandle {
    tx: mpsc::Sender<BrokerCommand>,
}

impl LocalBrokerHandle {
    fn spawn_actor(broker: LocalBroker, queue_capacity: usize) -> Self {
        Self::spawn_actor_with_cleanup_release_delay(
            broker,
            queue_capacity,
            default_cleanup_release_delay(),
        )
    }

    fn spawn_actor_with_cleanup_release_delay(
        broker: LocalBroker,
        queue_capacity: usize,
        cleanup_release_delay: Duration,
    ) -> Self {
        let (tx, mut rx) = mpsc::channel(queue_capacity.max(1));
        let tx_for_actor = tx.clone();
        tokio::spawn(async move {
            let mut broker = broker;
            while let Some(cmd) = rx.recv().await {
                match cmd {
                    BrokerCommand::UpsertChannel { config, reply } => {
                        let channel_id = config.channel_id;
                        let result = broker.upsert_channel(config);
                        if result.is_ok() {
                            let _ = channel_id;
                            drain_reserve_waiters(&mut broker);
                        }
                        let _ = reply.send(result);
                    }
                    BrokerCommand::DeleteChannel { channel_id, reply } => {
                        let result = broker.delete_channel(channel_id);
                        if result.is_ok() {
                            drain_reserve_waiters(&mut broker);
                        }
                        let _ = reply.send(result);
                    }
                    BrokerCommand::Reserve { req, reply } => {
                        let req_clone = req.clone();
                        match broker.reserve(req_clone) {
                            Ok(reservation) => {
                                let _ = reply.send(Ok(reservation));
                            }
                            Err(err) => {
                                let _ = reply.send(Err(err));
                            }
                        }
                    }
                    BrokerCommand::Publish {
                        channel_id,
                        reservation_id,
                        now_ms,
                        reply,
                    } => {
                        let result = broker.publish(channel_id, reservation_id, now_ms);
                        if result.is_ok() {
                            drain_fetch_waiters_for_channel(&mut broker, channel_id);
                        }
                        let _ = reply.send(result);
                    }
                    BrokerCommand::Abort {
                        channel_id,
                        reservation_id,
                        reply,
                    } => {
                        let result = broker.abort(channel_id, reservation_id);
                        if result.is_ok() {
                            drain_reserve_waiters(&mut broker);
                        }
                        let _ = reply.send(result);
                    }
                    BrokerCommand::FetchNext { req, reply } => {
                        let req_clone = req.clone();
                        match broker.fetch_next(req_clone) {
                            Ok(Some(message)) => {
                                let _ = reply.send(Ok(Some(message)));
                            }
                            Ok(None) => match broker.channel_mut(req.channel_id) {
                                Ok(channel) => {
                                    channel.fetch_waiters.push_back(FetchWaiter { req, reply })
                                }
                                Err(err) => {
                                    let _ = reply.send(Err(err));
                                }
                            },
                            Err(err) => {
                                let _ = reply.send(Err(err));
                            }
                        }
                    }
                    BrokerCommand::FetchBatchAvailable {
                        req,
                        max_items,
                        reply,
                    } => {
                        let _ = reply.send(broker.fetch_batch_available(req, max_items));
                    }
                    BrokerCommand::Commit {
                        channel_id,
                        reservation_id,
                        now_ms,
                        reply,
                    } => {
                        let result = broker.commit(channel_id, reservation_id, now_ms);
                        if result.is_ok() {
                            drain_reserve_waiters(&mut broker);
                        }
                        let _ = reply.send(result);
                    }
                    BrokerCommand::CommitBatch {
                        channel_id,
                        reservation_ids,
                        now_ms,
                        reply,
                    } => {
                        let result = broker.commit_batch(channel_id, reservation_ids, now_ms);
                        if result.is_ok() {
                            drain_reserve_waiters(&mut broker);
                        }
                        let _ = reply.send(result);
                    }
                    BrokerCommand::RequeueInflight {
                        channel_id,
                        reservation_id,
                        reply,
                    } => {
                        let result = broker.requeue_inflight(channel_id, reservation_id);
                        if result.is_ok() {
                            drain_fetch_waiters_for_channel(&mut broker, channel_id);
                        }
                        let _ = reply.send(result);
                    }
                    BrokerCommand::RequeueAllInflight { channel_id, reply } => {
                        let result = broker.requeue_all_inflight(channel_id);
                        if result.is_ok() {
                            drain_fetch_waiters_for_channel(&mut broker, channel_id);
                        }
                        let _ = reply.send(result);
                    }
                    BrokerCommand::TakeCleanupBatch {
                        channel_id,
                        max_items,
                        reply,
                    } => {
                        let _ = reply.send(broker.take_cleanup_batch(channel_id, max_items));
                    }
                    BrokerCommand::CleanupAck {
                        channel_id,
                        reservation_id,
                        reply,
                    } => {
                        let result =
                            broker.cleanup_ack_for_delayed_release(channel_id, reservation_id);
                        match result {
                            Ok(payload_bytes) if cleanup_release_delay.is_zero() => {
                                broker.release_payload_bytes(payload_bytes);
                                drain_reserve_waiters(&mut broker);
                                let _ = reply.send(Ok(()));
                            }
                            Ok(payload_bytes) => {
                                let tx_release = tx_for_actor.clone();
                                tokio::spawn(async move {
                                    tokio::time::sleep(cleanup_release_delay).await;
                                    let _ = tx_release
                                        .send(BrokerCommand::ReleasePayloadBytes { payload_bytes })
                                        .await;
                                });
                                let _ = reply.send(Ok(()));
                            }
                            Err(err) => {
                                let _ = reply.send(Err(err));
                            }
                        }
                    }
                    BrokerCommand::ReleasePayloadBytes { payload_bytes } => {
                        broker.release_payload_bytes(payload_bytes);
                        if payload_bytes > 0 {
                            drain_reserve_waiters(&mut broker);
                        }
                    }
                    BrokerCommand::CleanupNack {
                        channel_id,
                        reservation_id,
                        reply,
                    } => {
                        let _ = reply.send(broker.cleanup_nack(channel_id, reservation_id));
                    }
                    BrokerCommand::Shutdown { reply } => {
                        fail_all_waiters_with_actor_closed(&mut broker);
                        let _ = reply.send(Ok(()));
                        break;
                    }
                }
            }
        });
        Self { tx }
    }

    async fn upsert_channel(&self, config: BrokerChannelConfig) -> Result<(), BrokerError> {
        self.request(|reply| BrokerCommand::UpsertChannel { config, reply })
            .await
    }

    async fn delete_channel(&self, channel_id: i64) -> Result<Vec<String>, BrokerError> {
        self.request(|reply| BrokerCommand::DeleteChannel { channel_id, reply })
            .await
    }

    async fn reserve(&self, req: BrokerReserveRequest) -> Result<BrokerReservation, BrokerError> {
        self.request(|reply| BrokerCommand::Reserve { req, reply })
            .await
    }

    async fn publish(
        &self,
        channel_id: i64,
        reservation_id: u64,
        now_ms: i64,
    ) -> Result<BrokerEnvelope, BrokerError> {
        self.request(|reply| BrokerCommand::Publish {
            channel_id,
            reservation_id,
            now_ms,
            reply,
        })
        .await
    }

    async fn abort(&self, channel_id: i64, reservation_id: u64) -> Result<(), BrokerError> {
        self.request(|reply| BrokerCommand::Abort {
            channel_id,
            reservation_id,
            reply,
        })
        .await
    }

    async fn fetch_next(
        &self,
        req: BrokerFetchRequest,
    ) -> Result<Option<BrokerFetchedMessage>, BrokerError> {
        self.request(|reply| BrokerCommand::FetchNext { req, reply })
            .await
    }

    async fn fetch_batch_available(
        &self,
        req: BrokerFetchRequest,
        max_items: usize,
    ) -> Result<BrokerFetchBatch, BrokerError> {
        self.request(|reply| BrokerCommand::FetchBatchAvailable {
            req,
            max_items,
            reply,
        })
        .await
    }

    async fn commit(
        &self,
        channel_id: i64,
        reservation_id: u64,
        now_ms: i64,
    ) -> Result<BrokerCommitOutcome, BrokerError> {
        self.request(|reply| BrokerCommand::Commit {
            channel_id,
            reservation_id,
            now_ms,
            reply,
        })
        .await
    }

    async fn commit_batch(
        &self,
        channel_id: i64,
        reservation_ids: Vec<u64>,
        now_ms: i64,
    ) -> Result<BrokerCommitBatchOutcome, BrokerError> {
        self.request(|reply| BrokerCommand::CommitBatch {
            channel_id,
            reservation_ids,
            now_ms,
            reply,
        })
        .await
    }

    async fn requeue_inflight(
        &self,
        channel_id: i64,
        reservation_id: u64,
    ) -> Result<(), BrokerError> {
        self.request(|reply| BrokerCommand::RequeueInflight {
            channel_id,
            reservation_id,
            reply,
        })
        .await
    }

    async fn requeue_all_inflight(&self, channel_id: i64) -> Result<(), BrokerError> {
        self.request(|reply| BrokerCommand::RequeueAllInflight { channel_id, reply })
            .await
    }

    async fn take_cleanup_batch(
        &self,
        channel_id: i64,
        max_items: usize,
    ) -> Result<Vec<BrokerEnvelope>, BrokerError> {
        self.request(|reply| BrokerCommand::TakeCleanupBatch {
            channel_id,
            max_items,
            reply,
        })
        .await
    }

    async fn cleanup_ack(&self, channel_id: i64, reservation_id: u64) -> Result<(), BrokerError> {
        self.request(|reply| BrokerCommand::CleanupAck {
            channel_id,
            reservation_id,
            reply,
        })
        .await
    }

    async fn cleanup_nack(&self, channel_id: i64, reservation_id: u64) -> Result<(), BrokerError> {
        self.request(|reply| BrokerCommand::CleanupNack {
            channel_id,
            reservation_id,
            reply,
        })
        .await
    }

    async fn shutdown(&self) -> Result<(), BrokerError> {
        self.request(|reply| BrokerCommand::Shutdown { reply })
            .await
    }

    async fn request<T>(
        &self,
        make_cmd: impl FnOnce(oneshot::Sender<Result<T, BrokerError>>) -> BrokerCommand,
    ) -> Result<T, BrokerError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(make_cmd(reply_tx))
            .await
            .map_err(|_| BrokerError::ActorClosed)?;
        reply_rx.await.map_err(|_| BrokerError::ActorClosed)?
    }
}

#[derive(Debug, Clone, Default, Encode, Decode)]
enum BrokerRpcOperation {
    #[default]
    Noop,
    UpsertChannel {
        config: BrokerChannelConfig,
    },
    DeleteChannel {
        channel_id: i64,
    },
    Reserve {
        req: BrokerReserveRequest,
    },
    Publish {
        channel_id: i64,
        reservation_id: u64,
        now_ms: i64,
    },
    Abort {
        channel_id: i64,
        reservation_id: u64,
    },
    FetchNext {
        req: BrokerFetchRequest,
    },
    FetchBatchAvailable {
        req: BrokerFetchRequest,
        max_items: usize,
    },
    Commit {
        channel_id: i64,
        reservation_id: u64,
        now_ms: i64,
    },
    CommitBatch {
        channel_id: i64,
        reservation_ids: Vec<u64>,
        now_ms: i64,
    },
    RequeueInflight {
        channel_id: i64,
        reservation_id: u64,
    },
    RequeueAllInflight {
        channel_id: i64,
    },
    TakeCleanupBatch {
        channel_id: i64,
        max_items: usize,
    },
    CleanupAck {
        channel_id: i64,
        reservation_id: u64,
    },
    CleanupNack {
        channel_id: i64,
        reservation_id: u64,
    },
}

#[derive(Debug, Clone, Default, Encode, Decode)]
struct BrokerRpcRequest {
    op: BrokerRpcOperation,
}

impl MsgPackSerializePart for BrokerRpcRequest {
    fn msg_id(&self) -> u32 {
        BROKER_RPC_REQ_MSG_ID
    }
}

impl RPCReq for BrokerRpcRequest {
    type Resp = BrokerRpcResponse;
}

#[derive(Debug, Clone, Encode, Decode)]
enum BrokerRpcReply {
    Unit(Result<(), BrokerError>),
    PayloadKeys(Result<Vec<String>, BrokerError>),
    Reservation(Result<BrokerReservation, BrokerError>),
    Envelope(Result<BrokerEnvelope, BrokerError>),
    Fetch(Result<Option<BrokerFetchedMessage>, BrokerError>),
    FetchBatch(Result<BrokerFetchBatch, BrokerError>),
    Commit(Result<BrokerCommitOutcome, BrokerError>),
    CommitBatch(Result<BrokerCommitBatchOutcome, BrokerError>),
    CleanupBatch(Result<Vec<BrokerEnvelope>, BrokerError>),
}

impl Default for BrokerRpcReply {
    fn default() -> Self {
        Self::Unit(Ok(()))
    }
}

#[derive(Debug, Clone, Default, Encode, Decode)]
struct BrokerRpcResponse {
    reply: BrokerRpcReply,
}

impl MsgPackSerializePart for BrokerRpcResponse {
    fn msg_id(&self) -> u32 {
        BROKER_RPC_RESP_MSG_ID
    }
}

async fn execute_rpc_request(
    broker: &LocalBrokerHandle,
    request: BrokerRpcRequest,
) -> BrokerRpcResponse {
    let reply = match request.op {
        BrokerRpcOperation::Noop => BrokerRpcReply::Unit(Err(BrokerError::Rpc(
            "broker noop request is invalid".to_string(),
        ))),
        BrokerRpcOperation::UpsertChannel { config } => {
            BrokerRpcReply::Unit(broker.upsert_channel(config).await)
        }
        BrokerRpcOperation::DeleteChannel { channel_id } => {
            BrokerRpcReply::PayloadKeys(broker.delete_channel(channel_id).await)
        }
        BrokerRpcOperation::Reserve { req } => {
            BrokerRpcReply::Reservation(broker.reserve(req).await)
        }
        BrokerRpcOperation::Publish {
            channel_id,
            reservation_id,
            now_ms,
        } => BrokerRpcReply::Envelope(broker.publish(channel_id, reservation_id, now_ms).await),
        BrokerRpcOperation::Abort {
            channel_id,
            reservation_id,
        } => BrokerRpcReply::Unit(broker.abort(channel_id, reservation_id).await),
        BrokerRpcOperation::FetchNext { req } => {
            BrokerRpcReply::Fetch(broker.fetch_next(req).await)
        }
        BrokerRpcOperation::FetchBatchAvailable { req, max_items } => {
            BrokerRpcReply::FetchBatch(broker.fetch_batch_available(req, max_items).await)
        }
        BrokerRpcOperation::Commit {
            channel_id,
            reservation_id,
            now_ms,
        } => BrokerRpcReply::Commit(broker.commit(channel_id, reservation_id, now_ms).await),
        BrokerRpcOperation::CommitBatch {
            channel_id,
            reservation_ids,
            now_ms,
        } => BrokerRpcReply::CommitBatch(
            broker
                .commit_batch(channel_id, reservation_ids, now_ms)
                .await,
        ),
        BrokerRpcOperation::RequeueInflight {
            channel_id,
            reservation_id,
        } => BrokerRpcReply::Unit(broker.requeue_inflight(channel_id, reservation_id).await),
        BrokerRpcOperation::RequeueAllInflight { channel_id } => {
            BrokerRpcReply::Unit(broker.requeue_all_inflight(channel_id).await)
        }
        BrokerRpcOperation::TakeCleanupBatch {
            channel_id,
            max_items,
        } => BrokerRpcReply::CleanupBatch(broker.take_cleanup_batch(channel_id, max_items).await),
        BrokerRpcOperation::CleanupAck {
            channel_id,
            reservation_id,
        } => BrokerRpcReply::Unit(broker.cleanup_ack(channel_id, reservation_id).await),
        BrokerRpcOperation::CleanupNack {
            channel_id,
            reservation_id,
        } => BrokerRpcReply::Unit(broker.cleanup_nack(channel_id, reservation_id).await),
    };
    BrokerRpcResponse { reply }
}

pub fn register_broker_service(p2p_view: P2pModuleView, queue_capacity: usize) {
    let broker = LocalBrokerHandle::spawn_actor(LocalBroker::new(), queue_capacity);
    let handler_view = p2p_view.clone();
    RPCHandler::<BrokerRpcRequest>::new().regist(p2p_view.p2p_module(), move |resp, msg| {
        let broker = broker.clone();
        let handler_view = handler_view.clone();
        let _ = handler_view.spawn("fluxon_mq.broker.rpc", async move {
            let response = execute_rpc_request(&broker, msg.serialize_part).await;
            let _ = resp
                .send_resp(MsgPack {
                    serialize_part: response,
                    raw_bytes: Vec::new(),
                })
                .await;
        });
        Ok(())
    });
}

#[derive(Clone)]
struct RemoteBrokerHandle {
    cluster_manager_view: ClusterManagerView,
    p2p_view: P2pModuleView,
}

#[derive(Clone)]
enum BrokerHandleInner {
    Local(LocalBrokerHandle),
    Remote(RemoteBrokerHandle),
}

pub struct BrokerHandle {
    inner: BrokerHandleInner,
}

impl Clone for BrokerHandle {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl std::fmt::Debug for BrokerHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.inner {
            BrokerHandleInner::Local(_) => f
                .debug_struct("BrokerHandle")
                .field("kind", &"local")
                .finish(),
            BrokerHandleInner::Remote(_) => f
                .debug_struct("BrokerHandle")
                .field("kind", &"remote")
                .finish(),
        }
    }
}

impl BrokerHandle {
    pub fn new_distributed(
        cluster_manager_view: ClusterManagerView,
        p2p_view: P2pModuleView,
    ) -> Self {
        Self {
            inner: BrokerHandleInner::Remote(RemoteBrokerHandle {
                cluster_manager_view,
                p2p_view,
            }),
        }
    }

    #[cfg(test)]
    pub fn new_local_for_test(queue_capacity: usize) -> Self {
        Self {
            inner: BrokerHandleInner::Local(
                LocalBrokerHandle::spawn_actor_with_cleanup_release_delay(
                    LocalBroker::new(),
                    queue_capacity,
                    Duration::ZERO,
                ),
            ),
        }
    }

    #[cfg(test)]
    pub fn new_local_with_payload_byte_capacity_for_test(
        payload_byte_capacity: u64,
        queue_capacity: usize,
    ) -> Self {
        Self {
            inner: BrokerHandleInner::Local(
                LocalBrokerHandle::spawn_actor_with_cleanup_release_delay(
                    LocalBroker::with_payload_byte_capacity(payload_byte_capacity),
                    queue_capacity,
                    Duration::ZERO,
                ),
            ),
        }
    }

    pub async fn upsert_channel(&self, config: BrokerChannelConfig) -> Result<(), BrokerError> {
        match self
            .request(BrokerRpcRequest {
                op: BrokerRpcOperation::UpsertChannel { config },
            })
            .await?
            .reply
        {
            BrokerRpcReply::Unit(result) => result,
            other => Err(BrokerError::Rpc(format!(
                "unexpected response for upsert_channel: {:?}",
                other
            ))),
        }
    }

    pub async fn delete_channel(&self, channel_id: i64) -> Result<Vec<String>, BrokerError> {
        match self
            .request(BrokerRpcRequest {
                op: BrokerRpcOperation::DeleteChannel { channel_id },
            })
            .await?
            .reply
        {
            BrokerRpcReply::PayloadKeys(result) => result,
            other => Err(BrokerError::Rpc(format!(
                "unexpected response for delete_channel: {:?}",
                other
            ))),
        }
    }

    pub async fn reserve(
        &self,
        req: BrokerReserveRequest,
    ) -> Result<BrokerReservation, BrokerError> {
        match self
            .request(BrokerRpcRequest {
                op: BrokerRpcOperation::Reserve { req },
            })
            .await?
            .reply
        {
            BrokerRpcReply::Reservation(result) => result,
            other => Err(BrokerError::Rpc(format!(
                "unexpected response for reserve: {:?}",
                other
            ))),
        }
    }

    pub async fn publish(
        &self,
        channel_id: i64,
        reservation_id: u64,
        now_ms: i64,
    ) -> Result<BrokerEnvelope, BrokerError> {
        match self
            .request(BrokerRpcRequest {
                op: BrokerRpcOperation::Publish {
                    channel_id,
                    reservation_id,
                    now_ms,
                },
            })
            .await?
            .reply
        {
            BrokerRpcReply::Envelope(result) => result,
            other => Err(BrokerError::Rpc(format!(
                "unexpected response for publish: {:?}",
                other
            ))),
        }
    }

    pub async fn abort(&self, channel_id: i64, reservation_id: u64) -> Result<(), BrokerError> {
        match self
            .request(BrokerRpcRequest {
                op: BrokerRpcOperation::Abort {
                    channel_id,
                    reservation_id,
                },
            })
            .await?
            .reply
        {
            BrokerRpcReply::Unit(result) => result,
            other => Err(BrokerError::Rpc(format!(
                "unexpected response for abort: {:?}",
                other
            ))),
        }
    }

    pub async fn fetch_next(
        &self,
        req: BrokerFetchRequest,
    ) -> Result<Option<BrokerFetchedMessage>, BrokerError> {
        match self
            .request(BrokerRpcRequest {
                op: BrokerRpcOperation::FetchNext { req },
            })
            .await?
            .reply
        {
            BrokerRpcReply::Fetch(result) => result,
            other => Err(BrokerError::Rpc(format!(
                "unexpected response for fetch_next: {:?}",
                other
            ))),
        }
    }

    pub async fn fetch_batch_available(
        &self,
        req: BrokerFetchRequest,
        max_items: usize,
    ) -> Result<BrokerFetchBatch, BrokerError> {
        match self
            .request(BrokerRpcRequest {
                op: BrokerRpcOperation::FetchBatchAvailable { req, max_items },
            })
            .await?
            .reply
        {
            BrokerRpcReply::FetchBatch(result) => result,
            other => Err(BrokerError::Rpc(format!(
                "unexpected response for fetch_batch_available: {:?}",
                other
            ))),
        }
    }

    pub async fn commit(
        &self,
        channel_id: i64,
        reservation_id: u64,
        now_ms: i64,
    ) -> Result<BrokerCommitOutcome, BrokerError> {
        match self
            .request(BrokerRpcRequest {
                op: BrokerRpcOperation::Commit {
                    channel_id,
                    reservation_id,
                    now_ms,
                },
            })
            .await?
            .reply
        {
            BrokerRpcReply::Commit(result) => result,
            other => Err(BrokerError::Rpc(format!(
                "unexpected response for commit: {:?}",
                other
            ))),
        }
    }

    pub async fn commit_batch(
        &self,
        channel_id: i64,
        reservation_ids: Vec<u64>,
        now_ms: i64,
    ) -> Result<BrokerCommitBatchOutcome, BrokerError> {
        match self
            .request(BrokerRpcRequest {
                op: BrokerRpcOperation::CommitBatch {
                    channel_id,
                    reservation_ids,
                    now_ms,
                },
            })
            .await?
            .reply
        {
            BrokerRpcReply::CommitBatch(result) => result,
            other => Err(BrokerError::Rpc(format!(
                "unexpected response for commit_batch: {:?}",
                other
            ))),
        }
    }

    pub async fn requeue_inflight(
        &self,
        channel_id: i64,
        reservation_id: u64,
    ) -> Result<(), BrokerError> {
        match self
            .request(BrokerRpcRequest {
                op: BrokerRpcOperation::RequeueInflight {
                    channel_id,
                    reservation_id,
                },
            })
            .await?
            .reply
        {
            BrokerRpcReply::Unit(result) => result,
            other => Err(BrokerError::Rpc(format!(
                "unexpected response for requeue_inflight: {:?}",
                other
            ))),
        }
    }

    pub async fn requeue_all_inflight(&self, channel_id: i64) -> Result<(), BrokerError> {
        match self
            .request(BrokerRpcRequest {
                op: BrokerRpcOperation::RequeueAllInflight { channel_id },
            })
            .await?
            .reply
        {
            BrokerRpcReply::Unit(result) => result,
            other => Err(BrokerError::Rpc(format!(
                "unexpected response for requeue_all_inflight: {:?}",
                other
            ))),
        }
    }

    pub async fn take_cleanup_batch(
        &self,
        channel_id: i64,
        max_items: usize,
    ) -> Result<Vec<BrokerEnvelope>, BrokerError> {
        match self
            .request(BrokerRpcRequest {
                op: BrokerRpcOperation::TakeCleanupBatch {
                    channel_id,
                    max_items,
                },
            })
            .await?
            .reply
        {
            BrokerRpcReply::CleanupBatch(result) => result,
            other => Err(BrokerError::Rpc(format!(
                "unexpected response for take_cleanup_batch: {:?}",
                other
            ))),
        }
    }

    pub async fn cleanup_ack(
        &self,
        channel_id: i64,
        reservation_id: u64,
    ) -> Result<(), BrokerError> {
        match self
            .request(BrokerRpcRequest {
                op: BrokerRpcOperation::CleanupAck {
                    channel_id,
                    reservation_id,
                },
            })
            .await?
            .reply
        {
            BrokerRpcReply::Unit(result) => result,
            other => Err(BrokerError::Rpc(format!(
                "unexpected response for cleanup_ack: {:?}",
                other
            ))),
        }
    }

    pub async fn cleanup_nack(
        &self,
        channel_id: i64,
        reservation_id: u64,
    ) -> Result<(), BrokerError> {
        match self
            .request(BrokerRpcRequest {
                op: BrokerRpcOperation::CleanupNack {
                    channel_id,
                    reservation_id,
                },
            })
            .await?
            .reply
        {
            BrokerRpcReply::Unit(result) => result,
            other => Err(BrokerError::Rpc(format!(
                "unexpected response for cleanup_nack: {:?}",
                other
            ))),
        }
    }

    pub async fn shutdown(&self) -> Result<(), BrokerError> {
        match &self.inner {
            BrokerHandleInner::Local(local) => local.shutdown().await,
            BrokerHandleInner::Remote(_) => Err(BrokerError::Rpc(
                "shutdown is unsupported for distributed broker handles".to_string(),
            )),
        }
    }

    async fn request(&self, request: BrokerRpcRequest) -> Result<BrokerRpcResponse, BrokerError> {
        match &self.inner {
            BrokerHandleInner::Local(local) => Ok(execute_rpc_request(local, request).await),
            BrokerHandleInner::Remote(remote) => remote.request(request).await,
        }
    }
}

impl RemoteBrokerHandle {
    async fn request(&self, request: BrokerRpcRequest) -> Result<BrokerRpcResponse, BrokerError> {
        let broker_node =
            find_or_wait_broker_node(self.cluster_manager_view.cluster_manager()).await?;
        let response = RPCCaller::<BrokerRpcRequest>::new()
            .call(
                self.p2p_view.p2p_module(),
                broker_node.into(),
                MsgPack {
                    serialize_part: request,
                    raw_bytes: Vec::new(),
                },
                None,
                6,
            )
            .await
            .map_err(|e| BrokerError::Rpc(format!("broker rpc call failed: {}", e)))?;
        Ok(response.serialize_part)
    }
}

async fn find_or_wait_broker_node(
    cluster_manager: &fluxon_commu::ClusterManager,
) -> Result<String, BrokerError> {
    let members = cluster_manager.get_members();
    let broker_nodes: Vec<_> = members
        .iter()
        .filter(|member| is_broker_member(member))
        .collect();
    if broker_nodes.len() == 1 {
        return Ok(broker_nodes[0].id.to_string());
    }
    if broker_nodes.len() > 1 {
        return Err(BrokerError::BrokerUnavailable(format!(
            "multiple brokers found: {:?}",
            broker_nodes
                .into_iter()
                .map(|member| member.id.to_string())
                .collect::<Vec<_>>()
        )));
    }

    let mut rx = cluster_manager.listen();
    tokio::time::timeout(BROKER_DISCOVERY_TIMEOUT, async move {
        while let Ok(event) = rx.recv().await {
            match event {
                fluxon_commu::ClusterEvent::MemberJoined(member)
                | fluxon_commu::ClusterEvent::MemberUpdated(member)
                    if is_broker_member(&member) =>
                {
                    return Ok(member.id.to_string());
                }
                _ => {}
            }
        }
        Err(BrokerError::BrokerUnavailable(
            "broker node not found from cluster manager".to_string(),
        ))
    })
    .await
    .unwrap_or_else(|_| {
        Err(BrokerError::BrokerUnavailable(format!(
            "timed out waiting {}s for broker node registration; start fluxon_py.runtime.start_broker first",
            BROKER_DISCOVERY_TIMEOUT.as_secs()
        )))
    })
}

fn is_broker_member(member: &fluxon_commu::ClusterMember) -> bool {
    member
        .metadata
        .get(FLUXON_MQ_COMPONENT_METADATA_KEY)
        .is_some_and(|value| value == FLUXON_MQ_COMPONENT_BROKER_METADATA_VALUE)
}

fn broker_category_enforces_capacity(category: MqCategory) -> bool {
    matches!(category, MqCategory::MpmcSub { .. })
}

pub fn now_unix_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock is before UNIX_EPOCH")
        .as_millis() as i64
}

fn validate_capacity(config: &BrokerChannelConfig) -> Result<(), BrokerError> {
    if config.capacity <= 0 {
        return Err(BrokerError::InvalidCapacity {
            channel_id: config.channel_id,
            capacity: config.capacity,
        });
    }
    Ok(())
}

fn default_payload_byte_capacity() -> u64 {
    if let Ok(raw) = env::var(BROKER_PAYLOAD_BYTES_CAP_ENV) {
        if let Ok(value) = raw.trim().parse::<u64>() {
            if value > 0 {
                return value;
            }
        }
    }

    if let Ok(raw) = env::var(OWNER_POOL_DRAM_BYTES_ENV) {
        if let Ok(value) = raw.trim().parse::<u64>() {
            if value > 0 {
                let percent = payload_byte_capacity_percent();
                return ((value as u128) * (percent as u128) / 100).max(1) as u64;
            }
        }
    }

    DEFAULT_BROKER_PAYLOAD_BYTES_CAP
}

fn payload_byte_capacity_percent() -> u64 {
    env::var(BROKER_PAYLOAD_BYTES_CAP_PERCENT_ENV)
        .ok()
        .and_then(|raw| raw.trim().parse::<u64>().ok())
        .filter(|value| (1..=100).contains(value))
        .unwrap_or(DEFAULT_BROKER_PAYLOAD_BYTES_CAP_PERCENT)
}

fn default_cleanup_release_delay() -> Duration {
    Duration::from_millis(
        env::var(BROKER_CLEANUP_RELEASE_DELAY_MS_ENV)
            .ok()
            .and_then(|raw| raw.trim().parse::<u64>().ok())
            .unwrap_or(DEFAULT_BROKER_CLEANUP_RELEASE_DELAY_MS),
    )
}

fn remove_from_deque(queue: &mut VecDeque<u64>, reservation_id: u64) {
    if let Some(pos) = queue.iter().position(|id| *id == reservation_id) {
        queue.remove(pos);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn reserve_req(channel_id: i64, producer_id: &str, now_ms: i64) -> BrokerReserveRequest {
        reserve_req_with_category(channel_id, producer_id, MqCategory::Mpsc, 1, now_ms)
    }

    fn reserve_req_with_category(
        channel_id: i64,
        producer_id: &str,
        category: MqCategory,
        payload_bytes: u64,
        now_ms: i64,
    ) -> BrokerReserveRequest {
        BrokerReserveRequest {
            channel_id,
            producer_id: producer_id.to_string(),
            category,
            payload_bytes,
            now_ms,
        }
    }

    fn reserve_req_bytes(
        channel_id: i64,
        producer_id: &str,
        payload_bytes: u64,
        now_ms: i64,
    ) -> BrokerReserveRequest {
        BrokerReserveRequest {
            channel_id,
            producer_id: producer_id.to_string(),
            category: MqCategory::Mpsc,
            payload_bytes,
            now_ms,
        }
    }

    fn fetch_req(channel_id: i64, consumer_id: &str, now_ms: i64) -> BrokerFetchRequest {
        BrokerFetchRequest {
            channel_id,
            consumer_id: consumer_id.to_string(),
            now_ms,
        }
    }

    #[test]
    fn reserve_publish_fetch_commit_frees_capacity_for_mpmc_sub() {
        let mut broker = LocalBroker::new();
        broker
            .upsert_channel(BrokerChannelConfig {
                channel_id: 7,
                capacity: 2,
            })
            .unwrap();

        let first = broker
            .reserve(reserve_req_with_category(
                7,
                "p0",
                MqCategory::MpmcSub { parent_mpmc_id: 70 },
                1,
                10,
            ))
            .unwrap();
        let second = broker
            .reserve(reserve_req_with_category(
                7,
                "p0",
                MqCategory::MpmcSub { parent_mpmc_id: 70 },
                1,
                11,
            ))
            .unwrap();
        assert_eq!(first.envelope.msg_id, 0);
        assert_eq!(second.envelope.msg_id, 1);
        assert_eq!(
            broker
                .reserve(reserve_req_with_category(
                    7,
                    "p0",
                    MqCategory::MpmcSub { parent_mpmc_id: 70 },
                    1,
                    12,
                ))
                .unwrap_err(),
            BrokerError::ChannelFull {
                channel_id: 7,
                capacity: 2,
                used_slots: 2,
            }
        );

        broker
            .publish(7, first.envelope.reservation_id, 20)
            .unwrap();
        let fetched = broker.fetch_next(fetch_req(7, "c0", 30)).unwrap().unwrap();
        assert_eq!(
            fetched.envelope.reservation_id,
            first.envelope.reservation_id
        );

        let committed = broker
            .commit(7, fetched.envelope.reservation_id, 40)
            .unwrap();
        assert!(committed.first_commit);
        assert_eq!(
            committed
                .cleanup
                .as_ref()
                .map(|env| env.payload_key.as_str()),
            Some(
                keys::backend_message_key_with_category(
                    7,
                    "p0",
                    0,
                    &MqCategory::MpmcSub { parent_mpmc_id: 70 },
                )
                .as_str()
            )
        );

        let third = broker
            .reserve(reserve_req_with_category(
                7,
                "p0",
                MqCategory::MpmcSub { parent_mpmc_id: 70 },
                1,
                50,
            ))
            .unwrap();
        assert_eq!(third.envelope.msg_id, 2);
    }

    #[test]
    fn abort_releases_pending_slot_for_mpmc_sub() {
        let mut broker = LocalBroker::new();
        broker
            .upsert_channel(BrokerChannelConfig {
                channel_id: 8,
                capacity: 1,
            })
            .unwrap();

        let reservation = broker
            .reserve(reserve_req_with_category(
                8,
                "p0",
                MqCategory::MpmcSub { parent_mpmc_id: 80 },
                1,
                10,
            ))
            .unwrap();
        assert!(matches!(
            broker.reserve(reserve_req_with_category(
                8,
                "p0",
                MqCategory::MpmcSub { parent_mpmc_id: 80 },
                1,
                11,
            )),
            Err(BrokerError::ChannelFull { .. })
        ));

        broker
            .abort(8, reservation.envelope.reservation_id)
            .unwrap();
        let next = broker
            .reserve(reserve_req_with_category(
                8,
                "p0",
                MqCategory::MpmcSub { parent_mpmc_id: 80 },
                1,
                12,
            ))
            .unwrap();
        assert_eq!(next.envelope.msg_id, 1);
    }

    #[test]
    fn replay_log_restores_visible_and_inflight_state() {
        let mut broker = LocalBroker::new();
        broker
            .upsert_channel(BrokerChannelConfig {
                channel_id: 9,
                capacity: 4,
            })
            .unwrap();
        let first = broker.reserve(reserve_req(9, "p0", 10)).unwrap();
        let second = broker.reserve(reserve_req(9, "p1", 11)).unwrap();
        broker
            .publish(9, first.envelope.reservation_id, 20)
            .unwrap();
        broker
            .publish(9, second.envelope.reservation_id, 21)
            .unwrap();
        let fetched = broker.fetch_next(fetch_req(9, "c0", 30)).unwrap().unwrap();
        assert_eq!(fetched.envelope.producer_id, "p0");

        let log = broker.log_records().to_vec();
        let mut restored = LocalBroker::from_log(&log).unwrap();
        restored.requeue_all_inflight(9).unwrap();

        let redelivered = restored
            .fetch_next(fetch_req(9, "c0", 40))
            .unwrap()
            .unwrap();
        assert_eq!(
            redelivered.envelope.reservation_id,
            first.envelope.reservation_id
        );
    }

    #[test]
    fn requeue_all_inflight_preserves_fetch_order() {
        let mut broker = LocalBroker::new();
        broker
            .upsert_channel(BrokerChannelConfig {
                channel_id: 10,
                capacity: 4,
            })
            .unwrap();
        let first = broker.reserve(reserve_req(10, "p0", 10)).unwrap();
        let second = broker.reserve(reserve_req(10, "p0", 11)).unwrap();
        broker
            .publish(10, first.envelope.reservation_id, 20)
            .unwrap();
        broker
            .publish(10, second.envelope.reservation_id, 21)
            .unwrap();

        let _ = broker.fetch_next(fetch_req(10, "c0", 30)).unwrap().unwrap();
        let _ = broker.fetch_next(fetch_req(10, "c0", 31)).unwrap().unwrap();
        broker.requeue_all_inflight(10).unwrap();

        let redelivered_first = broker.fetch_next(fetch_req(10, "c0", 40)).unwrap().unwrap();
        let redelivered_second = broker.fetch_next(fetch_req(10, "c0", 41)).unwrap().unwrap();
        assert_eq!(
            redelivered_first.envelope.reservation_id,
            first.envelope.reservation_id
        );
        assert_eq!(
            redelivered_second.envelope.reservation_id,
            second.envelope.reservation_id
        );
    }

    #[test]
    fn batch_fetch_and_commit_preserves_order_and_frees_capacity() {
        let mut broker = LocalBroker::new();
        broker
            .upsert_channel(BrokerChannelConfig {
                channel_id: 11,
                capacity: 3,
            })
            .unwrap();

        let first = broker.reserve(reserve_req(11, "p0", 10)).unwrap();
        let second = broker.reserve(reserve_req(11, "p0", 11)).unwrap();
        let third = broker.reserve(reserve_req(11, "p1", 12)).unwrap();
        for reservation in [&first, &second, &third] {
            broker
                .publish(11, reservation.envelope.reservation_id, 20)
                .unwrap();
        }

        let batch = broker
            .fetch_batch_available(fetch_req(11, "c0", 30), 2)
            .unwrap();
        assert_eq!(batch.messages.len(), 2);
        assert_eq!(batch.messages[0].envelope.msg_id, 0);
        assert_eq!(batch.messages[1].envelope.msg_id, 1);

        let outcome = broker
            .commit_batch(
                11,
                batch
                    .messages
                    .iter()
                    .map(|message| message.envelope.reservation_id)
                    .collect(),
                40,
            )
            .unwrap();
        assert_eq!(outcome.first_commit_count, 2);
        assert_eq!(outcome.cleanup.len(), 2);

        let next = broker.reserve(reserve_req(11, "p0", 50)).unwrap();
        assert_eq!(next.envelope.msg_id, 2);
    }

    #[test]
    fn payload_byte_budget_is_global_and_released_on_cleanup_ack_or_abort() {
        let mut broker = LocalBroker::with_payload_byte_capacity(10);
        broker
            .upsert_channel(BrokerChannelConfig {
                channel_id: 21,
                capacity: 8,
            })
            .unwrap();
        broker
            .upsert_channel(BrokerChannelConfig {
                channel_id: 22,
                capacity: 8,
            })
            .unwrap();

        let first = broker.reserve(reserve_req_bytes(21, "p0", 6, 10)).unwrap();
        assert_eq!(first.envelope.payload_bytes, 6);
        assert!(matches!(
            broker.reserve(reserve_req_bytes(22, "p1", 5, 11)),
            Err(BrokerError::PayloadBytesFull { .. })
        ));

        broker
            .publish(21, first.envelope.reservation_id, 20)
            .unwrap();
        let fetched = broker.fetch_next(fetch_req(21, "c0", 30)).unwrap().unwrap();
        broker
            .commit(21, fetched.envelope.reservation_id, 40)
            .unwrap();
        assert!(matches!(
            broker.reserve(reserve_req_bytes(22, "p1", 5, 41)),
            Err(BrokerError::PayloadBytesFull { .. })
        ));
        broker
            .cleanup_ack(21, fetched.envelope.reservation_id)
            .unwrap();
        let second = broker.reserve(reserve_req_bytes(22, "p1", 5, 50)).unwrap();
        broker.abort(22, second.envelope.reservation_id).unwrap();
        let third = broker.reserve(reserve_req_bytes(22, "p1", 10, 60)).unwrap();
        assert_eq!(third.envelope.payload_bytes, 10);
    }

    #[test]
    fn mpsc_reserve_does_not_gate_on_channel_capacity() {
        let mut broker = LocalBroker::new();
        broker
            .upsert_channel(BrokerChannelConfig {
                channel_id: 201,
                capacity: 1,
            })
            .unwrap();

        let first = broker.reserve(reserve_req(201, "p0", 10)).unwrap();
        let second = broker.reserve(reserve_req(201, "p0", 11)).unwrap();

        assert_eq!(first.envelope.msg_id, 0);
        assert_eq!(second.envelope.msg_id, 1);
    }

    #[test]
    fn mpmc_sub_reserve_still_gates_on_channel_capacity() {
        let mut broker = LocalBroker::new();
        broker
            .upsert_channel(BrokerChannelConfig {
                channel_id: 202,
                capacity: 1,
            })
            .unwrap();

        let _ = broker
            .reserve(reserve_req_with_category(
                202,
                "p0",
                MqCategory::MpmcSub { parent_mpmc_id: 9 },
                1,
                10,
            ))
            .unwrap();

        assert!(matches!(
            broker.reserve(reserve_req_with_category(
                202,
                "p0",
                MqCategory::MpmcSub { parent_mpmc_id: 9 },
                1,
                11,
            )),
            Err(BrokerError::ChannelFull { .. })
        ));
    }

    #[test]
    fn cleanup_ack_releases_payload_after_cleanup_batch_take() {
        let mut broker = LocalBroker::with_payload_byte_capacity(10);
        broker
            .upsert_channel(BrokerChannelConfig {
                channel_id: 23,
                capacity: 8,
            })
            .unwrap();

        let first = broker.reserve(reserve_req_bytes(23, "p0", 6, 10)).unwrap();
        broker
            .publish(23, first.envelope.reservation_id, 20)
            .unwrap();
        let fetched = broker.fetch_next(fetch_req(23, "c0", 30)).unwrap().unwrap();
        broker
            .commit(23, fetched.envelope.reservation_id, 40)
            .unwrap();
        assert_eq!(broker.take_cleanup_batch(23, 8).unwrap().len(), 1);
        assert!(matches!(
            broker.reserve(reserve_req_bytes(23, "p1", 5, 41)),
            Err(BrokerError::PayloadBytesFull { .. })
        ));

        broker
            .cleanup_ack(23, fetched.envelope.reservation_id)
            .unwrap();
        let second = broker.reserve(reserve_req_bytes(23, "p1", 5, 50)).unwrap();
        assert_eq!(second.envelope.payload_bytes, 5);
    }

    #[test]
    fn delete_channel_releases_payload_budget_for_all_queues() {
        let mut broker = LocalBroker::with_payload_byte_capacity(100);
        broker
            .upsert_channel(BrokerChannelConfig {
                channel_id: 31,
                capacity: 16,
            })
            .unwrap();
        broker
            .upsert_channel(BrokerChannelConfig {
                channel_id: 32,
                capacity: 16,
            })
            .unwrap();

        let pending = broker.reserve(reserve_req_bytes(31, "p0", 10, 10)).unwrap();

        let inflight = broker.reserve(reserve_req_bytes(31, "p0", 12, 12)).unwrap();
        broker
            .publish(31, inflight.envelope.reservation_id, 21)
            .unwrap();
        let _ = broker.fetch_next(fetch_req(31, "c0", 30)).unwrap().unwrap();

        let cleanup_inflight = broker.reserve(reserve_req_bytes(31, "p0", 13, 13)).unwrap();
        broker
            .publish(31, cleanup_inflight.envelope.reservation_id, 22)
            .unwrap();
        let fetched = broker.fetch_next(fetch_req(31, "c0", 31)).unwrap().unwrap();
        broker
            .commit(31, fetched.envelope.reservation_id, 40)
            .unwrap();
        assert_eq!(broker.take_cleanup_batch(31, 1).unwrap().len(), 1);

        let cleanup = broker.reserve(reserve_req_bytes(31, "p0", 14, 14)).unwrap();
        broker
            .publish(31, cleanup.envelope.reservation_id, 23)
            .unwrap();
        let fetched = broker.fetch_next(fetch_req(31, "c0", 32)).unwrap().unwrap();
        broker
            .commit(31, fetched.envelope.reservation_id, 41)
            .unwrap();

        let visible = broker.reserve(reserve_req_bytes(31, "p0", 11, 15)).unwrap();
        broker
            .publish(31, visible.envelope.reservation_id, 24)
            .unwrap();

        assert_eq!(broker.state.used_payload_bytes, 60);
        assert!(matches!(
            broker.reserve(reserve_req_bytes(32, "p1", 41, 50)),
            Err(BrokerError::PayloadBytesFull { .. })
        ));

        let mut payload_keys = broker.delete_channel(31).unwrap();
        payload_keys.sort();
        let mut expected_payload_keys = vec![
            pending.envelope.payload_key,
            inflight.envelope.payload_key,
            cleanup_inflight.envelope.payload_key,
            cleanup.envelope.payload_key,
            visible.envelope.payload_key,
        ];
        expected_payload_keys.sort();
        assert_eq!(payload_keys, expected_payload_keys);
        assert_eq!(broker.state.used_payload_bytes, 0);
        assert_eq!(broker.delete_channel(31), Ok(Vec::new()));
        assert_eq!(
            broker.fetch_next(fetch_req(31, "c0", 60)).unwrap_err(),
            BrokerError::ChannelNotFound(31)
        );

        let next = broker
            .reserve(reserve_req_bytes(32, "p1", 100, 70))
            .unwrap();
        assert_eq!(next.envelope.payload_bytes, 100);
    }

    #[tokio::test]
    async fn broker_handle_roundtrip_uses_local_actor() {
        let handle = BrokerHandle::new_local_for_test(32);
        handle
            .upsert_channel(BrokerChannelConfig {
                channel_id: 12,
                capacity: 2,
            })
            .await
            .unwrap();
        let reserved = handle.reserve(reserve_req(12, "p0", 10)).await.unwrap();
        handle
            .publish(12, reserved.envelope.reservation_id, 20)
            .await
            .unwrap();
        let fetched = handle
            .fetch_next(fetch_req(12, "c0", 30))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(fetched.envelope.msg_id, 0);
        handle
            .commit(12, fetched.envelope.reservation_id, 40)
            .await
            .unwrap();
        assert_eq!(handle.take_cleanup_batch(12, 8).await.unwrap().len(), 1);
        handle
            .cleanup_ack(12, fetched.envelope.reservation_id)
            .await
            .unwrap();
        handle.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn broker_handle_delete_channel_releases_payload_budget() {
        let handle = BrokerHandle::new_local_with_payload_byte_capacity_for_test(10, 8);
        handle
            .upsert_channel(BrokerChannelConfig {
                channel_id: 24,
                capacity: 4,
            })
            .await
            .unwrap();

        let first = handle
            .reserve(reserve_req_bytes(24, "p0", 6, 10))
            .await
            .unwrap();
        assert!(matches!(
            handle.reserve(reserve_req_bytes(24, "p1", 5, 11)).await,
            Err(BrokerError::PayloadBytesFull { .. })
        ));

        assert_eq!(
            handle.delete_channel(24).await.unwrap(),
            vec![first.envelope.payload_key]
        );
        assert_eq!(
            handle.delete_channel(24).await.unwrap(),
            Vec::<String>::new()
        );
        handle
            .upsert_channel(BrokerChannelConfig {
                channel_id: 25,
                capacity: 4,
            })
            .await
            .unwrap();
        let next = handle
            .reserve(reserve_req_bytes(25, "p1", 10, 20))
            .await
            .unwrap();
        assert_eq!(next.envelope.payload_bytes, 10);

        handle.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn broker_handle_returns_actor_closed_after_shutdown() {
        let handle = BrokerHandle::new_local_for_test(8);
        handle
            .upsert_channel(BrokerChannelConfig {
                channel_id: 13,
                capacity: 1,
            })
            .await
            .unwrap();
        handle.shutdown().await.unwrap();
        assert_eq!(
            handle.reserve(reserve_req(13, "p0", 10)).await.unwrap_err(),
            BrokerError::ActorClosed
        );
    }

    #[tokio::test]
    async fn broker_handle_returns_channel_full_without_waiting_for_mpmc_sub() {
        let handle = BrokerHandle::new_local_for_test(8);
        handle
            .upsert_channel(BrokerChannelConfig {
                channel_id: 14,
                capacity: 1,
            })
            .await
            .unwrap();

        let first = handle
            .reserve(reserve_req_with_category(
                14,
                "p0",
                MqCategory::MpmcSub {
                    parent_mpmc_id: 140,
                },
                1,
                10,
            ))
            .await
            .unwrap();
        assert!(matches!(
            handle
                .reserve(reserve_req_with_category(
                    14,
                    "p0",
                    MqCategory::MpmcSub {
                        parent_mpmc_id: 140
                    },
                    1,
                    11,
                ))
                .await,
            Err(BrokerError::ChannelFull { .. })
        ));

        handle
            .abort(14, first.envelope.reservation_id)
            .await
            .unwrap();
        let second = handle
            .reserve(reserve_req_with_category(
                14,
                "p0",
                MqCategory::MpmcSub {
                    parent_mpmc_id: 140,
                },
                1,
                12,
            ))
            .await
            .unwrap();
        assert_eq!(second.envelope.msg_id, 1);

        handle.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn broker_handle_returns_payload_bytes_full_without_waiting() {
        let handle = BrokerHandle::new_local_with_payload_byte_capacity_for_test(10, 8);
        handle
            .upsert_channel(BrokerChannelConfig {
                channel_id: 16,
                capacity: 8,
            })
            .await
            .unwrap();

        let first = handle
            .reserve(reserve_req_bytes(16, "p0", 6, 10))
            .await
            .unwrap();
        assert!(matches!(
            handle.reserve(reserve_req_bytes(16, "p1", 5, 11)).await,
            Err(BrokerError::PayloadBytesFull { .. })
        ));

        handle
            .publish(16, first.envelope.reservation_id, 20)
            .await
            .unwrap();
        let fetched = handle
            .fetch_next(fetch_req(16, "c0", 30))
            .await
            .unwrap()
            .unwrap();
        handle
            .commit(16, fetched.envelope.reservation_id, 40)
            .await
            .unwrap();

        handle
            .cleanup_ack(16, fetched.envelope.reservation_id)
            .await
            .unwrap();

        let second = handle
            .reserve(reserve_req_bytes(16, "p1", 5, 50))
            .await
            .unwrap();
        assert_eq!(second.envelope.producer_id, "p1");
        assert_eq!(second.envelope.payload_bytes, 5);

        handle.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn broker_handle_waits_for_message_then_resumes() {
        use std::time::Duration;
        use tokio::time::sleep;

        let handle = BrokerHandle::new_local_for_test(8);
        handle
            .upsert_channel(BrokerChannelConfig {
                channel_id: 15,
                capacity: 2,
            })
            .await
            .unwrap();

        let waiter_handle = handle.clone();
        let pending =
            tokio::spawn(async move { waiter_handle.fetch_next(fetch_req(15, "c0", 10)).await });

        sleep(Duration::from_millis(50)).await;
        assert!(!pending.is_finished());

        let reservation = handle.reserve(reserve_req(15, "p0", 11)).await.unwrap();
        handle
            .publish(15, reservation.envelope.reservation_id, 12)
            .await
            .unwrap();

        let fetched = pending.await.unwrap().unwrap().unwrap();
        assert_eq!(fetched.envelope.msg_id, 0);

        handle.shutdown().await.unwrap();
    }
}
