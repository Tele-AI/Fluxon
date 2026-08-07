use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use etcd_client::{
    Client, Compare, CompareOp, GetResponse, PutOptions, Txn, TxnOp, TxnOpResponse, TxnResponse,
};
use tracing::{debug, warn};

use super::{is_transient_etcd_error, retry_etcd_rpc};

const MAX_CAS_CONFLICTS: u32 = 100;
const UNCERTAIN_RPC_RETRY_DELAY: Duration = Duration::from_millis(50);

#[derive(Clone, Debug, PartialEq, Eq)]
struct CounterGeneration {
    value: Vec<u8>,
    lease_id: i64,
    mod_revision: i64,
}

impl CounterGeneration {
    fn from_kv(kv: &etcd_client::KeyValue, expected_key: &str) -> Result<Self> {
        if kv.key() != expected_key.as_bytes() {
            return Err(anyhow!(
                "dist_id readback returned unexpected key for {}",
                expected_key
            ));
        }
        Ok(Self {
            value: kv.value().to_vec(),
            lease_id: kv.lease(),
            mod_revision: kv.mod_revision(),
        })
    }
}

/// Distributed ID allocator backed by etcd.
///
/// Port of Python `DistributeIdAllocator` in `fluxon_py.etcd`.
///
/// Global counter key: `dist_id_allocator/{prefix}`.
///
/// The counter key may either:
/// - reuse a caller-provided lease for channel-scoped allocators, or
/// - stay intentionally unleased for process-global monotonic counters.
pub struct DistributeIdAllocator {
    client: Client,
    prefix: String,
    lease_id: Option<i64>,
    rpc_max_retries: u32,
}

impl DistributeIdAllocator {
    /// Create an allocator without automatic unary RPC retries.
    pub fn new(client: Client, prefix: impl Into<String>, lease_id: i64) -> Self {
        Self::new_with_retry(client, prefix, lease_id, 0)
    }

    /// Create an allocator with transient unary RPC retries.
    pub fn new_with_retry(
        client: Client,
        prefix: impl Into<String>,
        lease_id: i64,
        rpc_max_retries: u32,
    ) -> Self {
        Self {
            client,
            prefix: prefix.into(),
            lease_id: Some(lease_id),
            rpc_max_retries,
        }
    }

    /// Create an unleased allocator without automatic unary RPC retries.
    ///
    /// This mode is required for process-global monotonic counters such as the
    /// top-level MPSC `chan_id` allocator. Binding that counter to a short-lived
    /// lease would let the key disappear during an idle window and later restart
    /// from `1`, which can collide with still-existing historical metadata.
    pub fn new_without_lease(client: Client, prefix: impl Into<String>) -> Self {
        Self::new_without_lease_with_retry(client, prefix, 0)
    }

    /// Create an unleased allocator with transient unary RPC retries.
    pub fn new_without_lease_with_retry(
        client: Client,
        prefix: impl Into<String>,
        rpc_max_retries: u32,
    ) -> Self {
        Self {
            client,
            prefix: prefix.into(),
            lease_id: None,
            rpc_max_retries,
        }
    }

    /// Allocate the next ID (starting from 1) using generation-fenced CAS.
    ///
    /// The 100-attempt bound applies only to ordinary CAS competition. A
    /// transient response makes that candidate ambiguous, so it is permanently
    /// skipped and the configured RPC retry budget is used to read a fresh
    /// generation before attempting a larger candidate.
    pub async fn allocate_id(&self) -> Result<i64> {
        let key = format!("dist_id_allocator/{}", self.prefix);
        let client = self.client.clone();
        let mut generation = read_counter_generation(&client, &key, self.rpc_max_retries).await?;
        let mut minimum_counter_value = 0_i64;
        let mut cas_conflicts = 0_u32;
        let mut rpc_retries_used = 0_u32;

        while cas_conflicts < MAX_CAS_CONFLICTS {
            let candidate = next_candidate(generation.as_ref(), minimum_counter_value)?;
            let txn = allocator_cas_txn(&key, generation.as_ref(), candidate, self.lease_id);
            let mut attempt_client = client.clone();

            match attempt_client.txn(txn).await {
                Ok(txn_res) if txn_res.succeeded() => {
                    debug!("updated dist_id key {} to value {}", key, candidate);
                    return Ok(candidate);
                }
                Ok(txn_res) => {
                    // A false compare means this candidate was not allocated by
                    // this call. Keep it below the next candidate even if the
                    // competing generation disappears before readback.
                    minimum_counter_value = minimum_counter_value.max(candidate);
                    generation = allocator_cas_readback(&txn_res, &key)?;
                    cas_conflicts += 1;
                }
                Err(error) if is_transient_etcd_error(&error) => {
                    if rpc_retries_used >= self.rpc_max_retries {
                        return Err(anyhow!(
                            "dist_id CAS for key {} remained uncertain after {} retries: {}",
                            key,
                            rpc_retries_used,
                            error
                        ));
                    }

                    // The server may have committed `candidate`. Never return it
                    // after losing the response: another allocator may have won
                    // the same value. Recover an authoritative generation and
                    // require the next CAS to publish a strictly larger value.
                    minimum_counter_value = minimum_counter_value.max(candidate);
                    rpc_retries_used += 1;
                    warn!(
                        dist_id_key = %key,
                        candidate,
                        retry = rpc_retries_used,
                        max_retries = self.rpc_max_retries,
                        error = %error,
                        "dist_id CAS response was uncertain; abandoning candidate"
                    );
                    tokio::time::sleep(UNCERTAIN_RPC_RETRY_DELAY).await;
                    generation = loop {
                        match read_counter_generation(&client, &key, 0).await {
                            Ok(generation) => break generation,
                            Err(read_error)
                                if is_transient_counter_read_error(&read_error)
                                    && rpc_retries_used < self.rpc_max_retries =>
                            {
                                rpc_retries_used += 1;
                                warn!(
                                    dist_id_key = %key,
                                    retry = rpc_retries_used,
                                    max_retries = self.rpc_max_retries,
                                    error = %read_error,
                                    "dist_id uncertainty recovery Get failed; retrying"
                                );
                                tokio::time::sleep(UNCERTAIN_RPC_RETRY_DELAY).await;
                            }
                            Err(read_error) => return Err(read_error),
                        }
                    };
                }
                Err(error) => {
                    return Err(anyhow!(
                        "transaction failed when updating dist_id key {}: {}",
                        key,
                        error
                    ));
                }
            }
        }

        Err(anyhow!(
            "DistributeIdAllocator with prefix {} failed after {} CAS conflicts",
            self.prefix,
            MAX_CAS_CONFLICTS
        ))
    }
}

async fn read_counter_generation(
    client: &Client,
    key: &str,
    max_retries: u32,
) -> Result<Option<CounterGeneration>> {
    let resp = retry_etcd_rpc(max_retries, "get_dist_id_counter", || {
        let mut attempt_client = client.clone();
        let attempt_key = key.to_string();
        async move { attempt_client.get(attempt_key, None).await }
    })
    .await
    .with_context(|| format!("failed to get dist_id key {key}"))?;
    counter_generation_from_get(&resp, key)
}

fn counter_generation_from_get(resp: &GetResponse, key: &str) -> Result<Option<CounterGeneration>> {
    if resp.kvs().len() > 1 {
        return Err(anyhow!(
            "dist_id Get returned duplicate exact keys for {}",
            key
        ));
    }
    resp.kvs()
        .first()
        .map(|kv| CounterGeneration::from_kv(kv, key))
        .transpose()
}

fn allocator_cas_txn(
    key: &str,
    generation: Option<&CounterGeneration>,
    candidate: i64,
    lease_id: Option<i64>,
) -> Txn {
    let compares = match generation {
        Some(generation) => vec![
            Compare::mod_revision(key, CompareOp::Equal, generation.mod_revision),
            Compare::lease(key, CompareOp::Equal, generation.lease_id),
            Compare::value(key, CompareOp::Equal, generation.value.clone()),
        ],
        None => vec![Compare::create_revision(key, CompareOp::Equal, 0)],
    };
    let put = match lease_id {
        Some(lease_id) => TxnOp::put(
            key,
            candidate.to_string(),
            Some(PutOptions::new().with_lease(lease_id)),
        ),
        None => TxnOp::put(key, candidate.to_string(), None),
    };
    Txn::new()
        .when(compares)
        .and_then(vec![put])
        .or_else(vec![TxnOp::get(key, None)])
}

fn allocator_cas_readback(txn_res: &TxnResponse, key: &str) -> Result<Option<CounterGeneration>> {
    let responses = txn_res.op_responses();
    let [TxnOpResponse::Get(get)] = responses.as_slice() else {
        return Err(anyhow!(
            "dist_id CAS readback returned invalid response shape for key {}: operations={}",
            key,
            responses.len()
        ));
    };
    if get.kvs().len() > 1 {
        return Err(anyhow!(
            "dist_id CAS readback returned duplicate exact keys for {}",
            key
        ));
    }
    get.kvs()
        .first()
        .map(|kv| CounterGeneration::from_kv(kv, key))
        .transpose()
}

fn next_candidate(
    generation: Option<&CounterGeneration>,
    minimum_counter_value: i64,
) -> Result<i64> {
    // Preserve the historical behavior of repairing a malformed counter from
    // zero, while the exact generation fence still prevents overwriting a
    // concurrently changed value.
    let observed = generation
        .and_then(|generation| std::str::from_utf8(&generation.value).ok())
        .and_then(|value| value.parse::<i64>().ok())
        .unwrap_or(0);
    let base = observed.max(minimum_counter_value);
    base.checked_add(1)
        .ok_or_else(|| anyhow!("dist_id counter overflow at {}", base))
}

fn is_transient_counter_read_error(error: &anyhow::Error) -> bool {
    error.chain().any(|source| {
        source
            .downcast_ref::<etcd_client::Error>()
            .is_some_and(is_transient_etcd_error)
    })
}

#[cfg(test)]
mod tests {
    use anyhow::Context;
    use tonic::Status;

    use super::{CounterGeneration, is_transient_counter_read_error, next_candidate};

    fn generation(value: i64, mod_revision: i64) -> CounterGeneration {
        CounterGeneration {
            value: value.to_string().into_bytes(),
            lease_id: 11,
            mod_revision,
        }
    }

    #[test]
    fn ordinary_candidate_advances_observed_counter() {
        assert_eq!(next_candidate(Some(&generation(41, 7)), 0).unwrap(), 42);
        assert_eq!(next_candidate(None, 0).unwrap(), 1);
    }

    #[test]
    fn uncertain_candidate_is_always_left_as_a_gap() {
        let uncertain_candidate = 42;
        assert_eq!(next_candidate(None, uncertain_candidate).unwrap(), 43);
        assert_eq!(
            next_candidate(Some(&generation(41, 7)), uncertain_candidate).unwrap(),
            43
        );
        assert_eq!(
            next_candidate(Some(&generation(42, 8)), uncertain_candidate).unwrap(),
            43
        );
        assert_eq!(
            next_candidate(Some(&generation(50, 9)), uncertain_candidate).unwrap(),
            51
        );
    }

    #[test]
    fn malformed_counter_is_repaired_without_bypassing_generation_fence() {
        let malformed = CounterGeneration {
            value: b"invalid".to_vec(),
            lease_id: 11,
            mod_revision: 9,
        };
        assert_eq!(next_candidate(Some(&malformed), 0).unwrap(), 1);
    }

    #[test]
    fn recovery_detects_transient_etcd_error_through_context() {
        let result: Result<(), etcd_client::Error> = Err(etcd_client::Error::GRpcStatus(
            Status::unavailable("temporary"),
        ));
        let error = result.context("recovery Get failed").unwrap_err();
        assert!(is_transient_counter_read_error(&error));
    }
}
