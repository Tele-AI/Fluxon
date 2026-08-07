use anyhow::{Context, Result, anyhow};
use etcd_client::{Client, Compare, CompareOp, PutOptions, Txn, TxnOp, TxnOpResponse};
use tracing::debug;

use super::retry_etcd_rpc;

/// Get or create a shared cluster lease id for a given logical key.
///
/// Port of Python `get_cluster_lease` in `fluxon_py.etcd`.
///
/// The lease id is stored in etcd under `cluster_lease/{lease_key}`.
/// All callers using the same `lease_key` will share the same lease id.
pub async fn get_cluster_lease_id(
    client: &mut Client,
    lease_key: &str,
    ttl_seconds: i64,
) -> Result<i64> {
    get_cluster_lease_id_with_retry(client, lease_key, ttl_seconds, 0).await
}

/// Get or create a shared cluster lease with transient unary RPC retries.
///
/// LeaseGrant remains a single call because blindly replaying it creates a
/// different lease. The safe Get and conditional publish use `max_retries`.
pub async fn get_cluster_lease_id_with_retry(
    client: &mut Client,
    lease_key: &str,
    ttl_seconds: i64,
    max_retries: u32,
) -> Result<i64> {
    let key = format!("cluster_lease/{}", lease_key);

    // Fast path: read existing lease id
    let resp = retry_etcd_rpc(max_retries, "get_cluster_lease", || {
        let mut attempt_client = client.clone();
        let attempt_key = key.clone();
        async move { attempt_client.get(attempt_key, None).await }
    })
    .await
    .with_context(|| format!("failed to get cluster lease key {key}"))?;
    if let Some(kv) = resp.kvs().first() {
        let lease_id = parse_cluster_lease_id(kv.value(), &key, "")?;
        debug!(
            "reused existing cluster lease id {} for key {}",
            lease_id, key
        );
        return Ok(lease_id);
    }

    // Create a new lease and try to publish it atomically
    let lease_resp = client
        .lease_grant(ttl_seconds, None)
        .await
        .with_context(|| format!("failed to grant lease for key {}", key))?;
    let lease_id = lease_resp.id();

    let txn_res = retry_etcd_rpc(max_retries, "publish_cluster_lease", || {
        let mut attempt_client = client.clone();
        let compare = Compare::create_revision(key.clone(), CompareOp::Equal, 0);
        let put_op = TxnOp::put(
            key.clone(),
            lease_id.to_string(),
            Some(PutOptions::new().with_lease(lease_id)),
        );
        let txn = Txn::new()
            .when(vec![compare])
            .and_then(vec![put_op])
            .or_else(vec![TxnOp::get(key.clone(), None)]);
        async move { attempt_client.txn(txn).await }
    })
    .await
    .with_context(|| format!("transaction failed when publishing cluster lease key {key}"))?;
    if txn_res.succeeded() {
        debug!(
            "published new cluster lease id {} for key {}",
            lease_id, key
        );
        return Ok(lease_id);
    }

    // A retry after an uncertain response lands here if the first publish
    // committed. A concurrent winner has the same shape. In either case the
    // atomic else-Get is the authoritative shared lease.
    let observed = cluster_lease_publish_readback(&txn_res, &key)?;
    let selected = reconcile_cluster_lease_publish(lease_id, observed, lease_key)?;
    debug!(
        "observed existing cluster lease id {} for key {} after txn",
        selected, key
    );
    Ok(selected)
}

fn parse_cluster_lease_id(value: &[u8], key: &str, suffix: &str) -> Result<i64> {
    let txt = String::from_utf8(value.to_vec())
        .with_context(|| format!("invalid lease id bytes for key {key}{suffix}"))?;
    txt.parse()
        .with_context(|| format!("invalid lease id '{}' for key {key}{suffix}", txt))
}

fn cluster_lease_publish_readback(
    txn_res: &etcd_client::TxnResponse,
    key: &str,
) -> Result<Option<i64>> {
    let responses = txn_res.op_responses();
    let [TxnOpResponse::Get(get)] = responses.as_slice() else {
        return Err(anyhow!(
            "cluster lease publish readback returned invalid response shape for key {}: operations={}",
            key,
            responses.len()
        ));
    };
    if get.kvs().len() > 1 {
        return Err(anyhow!(
            "cluster lease publish readback returned duplicate exact keys for {}",
            key
        ));
    }
    let Some(kv) = get.kvs().first() else {
        return Ok(None);
    };
    if kv.key() != key.as_bytes() {
        return Err(anyhow!(
            "cluster lease publish readback returned unexpected key for {}",
            key
        ));
    }
    parse_cluster_lease_id(kv.value(), key, " after txn").map(Some)
}

fn reconcile_cluster_lease_publish(
    candidate: i64,
    observed: Option<i64>,
    lease_key: &str,
) -> Result<i64> {
    match observed {
        // `candidate` may be our first uncertain publish; another value is the
        // concurrent winner. Both are the authoritative shared identity.
        Some(selected) => Ok(selected),
        None => Err(anyhow!(
            "failed to acquire cluster lease for key {}: key disappeared after publishing candidate {}",
            lease_key,
            candidate
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::{parse_cluster_lease_id, reconcile_cluster_lease_publish};

    #[test]
    fn parses_atomic_cluster_lease_readback() {
        assert_eq!(
            parse_cluster_lease_id(b"42", "cluster_lease/test", " after txn").unwrap(),
            42
        );
    }

    #[test]
    fn rejects_invalid_cluster_lease_readback() {
        assert!(parse_cluster_lease_id(b"not-an-id", "cluster_lease/test", "").is_err());
    }

    #[test]
    fn publish_reconciliation_accepts_own_or_competing_winner() {
        assert_eq!(
            reconcile_cluster_lease_publish(41, Some(41), "test").unwrap(),
            41
        );
        assert_eq!(
            reconcile_cluster_lease_publish(41, Some(52), "test").unwrap(),
            52
        );
        assert!(reconcile_cluster_lease_publish(41, None, "test").is_err());
    }
}
