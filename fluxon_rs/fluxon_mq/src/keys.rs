use std::fmt::Write as _;

use bitcode::{Decode, Encode};
use serde::{Deserialize, Serialize};

/// MQ category for key generation.
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, Encode, Decode)]
pub enum MqCategory {
    /// Standalone MPSC usage
    #[default]
    Mpsc,
    /// MPSC acts as a submodule under an MPMC producer; carries parent mpmc id only.
    /// The producer member id is the same as `producer_idx` passed alongside and
    /// does not need to be duplicated here.
    MpmcSub { parent_mpmc_id: i64 },
}

/// Key prefix for channel meta information.
pub fn etcd_meta_key_prefix() -> String {
    "/channels/meta/".to_string()
}

/// Meta key for a specific channel id.
pub fn etcd_meta_key(chan_id: i64) -> String {
    let mut s = String::with_capacity(32);
    s.push_str("/channels/meta/");
    let _ = write!(&mut s, "{}", chan_id);
    s
}

/// Etcd key for a producer registration under a channel.
pub fn etcd_producer_key(chan_id: i64, producer_idx: &str) -> String {
    let mut s = String::with_capacity(64);
    s.push_str("/channels/");
    let _ = write!(&mut s, "{}", chan_id);
    s.push_str("/producer/producer_");
    s.push_str(producer_idx);
    s
}

/// Extract producer index from a full etcd producer key.
///
/// Returns `None` if the key does not match the expected pattern.
pub fn parse_etcd_producer_key(key: &str) -> Option<String> {
    let parts: Vec<&str> = key.split('/').collect();
    if parts.len() != 5 {
        return None;
    }
    if parts[3] != "producer" {
        return None;
    }
    let last = parts[4];
    let idx = last.strip_prefix("producer_")?;
    if idx.is_empty() {
        return None;
    }
    Some(idx.to_string())
}

/// Etcd key for a consumer registration under a channel.
pub fn etcd_consumer_key(chan_id: i64, consumer_idx: &str) -> String {
    let mut s = String::with_capacity(64);
    s.push_str("/channels/");
    let _ = write!(&mut s, "{}", chan_id);
    s.push_str("/consumer/consumer_");
    s.push_str(consumer_idx);
    s
}

/// Prefix for all producer keys of a channel.
pub fn etcd_producer_key_prefix(chan_id: i64) -> String {
    let mut s = String::with_capacity(48);
    s.push_str("/channels/");
    let _ = write!(&mut s, "{}", chan_id);
    s.push_str("/producer/producer_");
    s
}

/// Prefix for all consumer keys of a channel.
pub fn etcd_consumer_key_prefix(chan_id: i64) -> String {
    let mut s = String::with_capacity(48);
    s.push_str("/channels/");
    let _ = write!(&mut s, "{}", chan_id);
    s.push_str("/consumer/consumer_");
    s
}

/// Prefix for all consume offsets of all producers for a channel.
/// Matches Python `_new_consume_offset_of_all_producer_key`:
/// `/channels/{chan}/consumer_offset_of_all_producer/`.
pub fn etcd_consume_offset_all_producer_prefix(chan_id: i64) -> String {
    let mut s = String::with_capacity(64);
    s.push_str("/channels/");
    let _ = write!(&mut s, "{}", chan_id);
    s.push_str("/consumer_offset_of_all_producer/");
    s
}

/// Prefix for all produce offsets of all producers for a channel.
/// Matches Python `_new_produce_offset_of_all_producer_key`:
/// `/channels/{chan}/producer_offset_of_all_producer/`.
pub fn etcd_produce_offset_all_producer_prefix(chan_id: i64) -> String {
    let mut s = String::with_capacity(64);
    s.push_str("/channels/");
    let _ = write!(&mut s, "{}", chan_id);
    s.push_str("/producer_offset_of_all_producer/");
    s
}

/// Key for a single producer's consume offset under a channel.
pub fn etcd_consume_offset_one_producer_key(chan_id: i64, producer_idx: &str) -> String {
    let mut s = etcd_consume_offset_all_producer_prefix(chan_id);
    s.push_str(producer_idx);
    s
}

/// Key for a single producer's produce offset under a channel.
pub fn etcd_produce_offset_one_producer_key(chan_id: i64, producer_idx: &str) -> String {
    let mut s = etcd_produce_offset_all_producer_prefix(chan_id);
    s.push_str(producer_idx);
    s
}

/// Extract producer index from a full etcd produce-offset key.
///
/// Returns `None` if the key does not match the expected pattern.
pub fn parse_etcd_produce_offset_key(key: &str) -> Option<String> {
    let parts: Vec<&str> = key.split('/').collect();
    if parts.len() != 5 {
        return None;
    }
    if parts[3] != "producer_offset_of_all_producer" {
        return None;
    }
    let producer_idx = parts[4];
    if producer_idx.is_empty() {
        return None;
    }
    Some(producer_idx.to_string())
}

/// Key for a message stored in the KV backend (not etcd).
///
/// Purpose:
/// - In MpmcSub mode we intentionally choose the path layout as
///   `/mpmc/{mpmc_id}/mpsc_{chan_id}/producer/{member_id}/msg_{msg_id}`
///   so that a single `count_prefix("/mpmc/{mpmc_id}/mpsc_{chan_id}/")`
///   aggregates ALL messages for the same underlying MPSC channel
///   across ALL MPMC producers. This enables capacity gating by
///   `(mpsc_id)` in 1 RTT without any extra index table.
///   The count comes from the master-side derived prefix index, so it is
///   intended for aggregate backpressure rather than immediate visibility
///   confirmation for a just-written message.
/// - In standalone MPSC mode we keep the historical layout for
///   backward compatibility.
///
/// Use this function in preference to the legacy `backend_message_key`
/// to avoid ambiguity and keep the rate-limit semantics explicit.
pub fn backend_message_key_with_category(
    chan_id: i64,
    producer_idx: &str,
    msg_id: i64,
    category: &MqCategory,
) -> String {
    match category {
        MqCategory::Mpsc => {
            format!(
                "/mpscchan_{}_producer_{}_msg_{}",
                chan_id, producer_idx, msg_id
            )
        }
        MqCategory::MpmcSub { parent_mpmc_id } => {
            // Layout rationale:
            // - We put `mpmc_id` and `mpsc_{chan_id}` first so that a single prefix
            //   `/mpmc/{mpmc_id}/mpsc_{chan_id}/` can be used with `count_prefix`
            //   to gate capacity (1 RTT) for the underlying MPSC channel across
            //   all MPMC producers.
            // - This is a backpressure-oriented aggregate count, not an immediate
            //   visibility probe for a just-committed KV write.
            // Example: /mpmc/{mpmc_id}/mpsc_{chan_id}/producer/{member_id}/msg_{msg_id}
            format!(
                "/mpmc/{}/mpsc_{}/producer/{}/msg_{}",
                parent_mpmc_id, chan_id, producer_idx, msg_id
            )
        }
    }
}

/// Backward-compatible wrapper when category is not available.
pub fn backend_message_key(chan_id: i64, producer_idx: &str, msg_id: i64) -> String {
    backend_message_key_with_category(chan_id, producer_idx, msg_id, &MqCategory::Mpsc)
}

/// Key for producer weight used by smooth weighted round-robin.
pub fn etcd_producer_weight_key(chan_id: i64, producer_idx: &str) -> String {
    let mut s = String::with_capacity(64);
    s.push_str("/channels/");
    let _ = write!(&mut s, "{}", chan_id);
    s.push_str("/producer_weight/");
    s.push_str(producer_idx);
    s
}

/// Prefix for all producer weight keys under a channel.
pub fn etcd_producer_weight_prefix(chan_id: i64) -> String {
    let mut s = String::with_capacity(48);
    s.push_str("/channels/");
    let _ = write!(&mut s, "{}", chan_id);
    s.push_str("/producer_weight/");
    s
}

/// Key for registering a consumer index for a given channel.
/// Matches Python `_new_register_consumer_idx`:
/// `/channels/{chan}/consumer_{i}`.
pub fn etcd_register_consumer_idx_key(chan_id: i64, idx: i64) -> String {
    let mut s = String::with_capacity(48);
    s.push_str("/channels/");
    let _ = write!(&mut s, "{}", chan_id);
    s.push_str("/consumer_");
    let _ = write!(&mut s, "{}", idx);
    s
}
