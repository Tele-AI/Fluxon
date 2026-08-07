use etcd_client as etcd;

use crate::error::MpscError;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct OffsetGeneration {
    value: Vec<u8>,
    offset: i64,
    lease_id: i64,
    mod_revision: i64,
}

impl OffsetGeneration {
    fn from_kv(kv: &etcd::KeyValue, expected_key: &str) -> Result<Self, MpscError> {
        if kv.key() != expected_key.as_bytes() {
            return Err(MpscError::Internal(format!(
                "offset read returned an unexpected key for {}",
                expected_key
            )));
        }
        let value = kv.value().to_vec();
        let text = std::str::from_utf8(&value).map_err(|error| {
            MpscError::Internal(format!(
                "offset key {} contains invalid UTF-8: {}",
                expected_key, error
            ))
        })?;
        let offset = text.parse::<i64>().map_err(|error| {
            MpscError::Internal(format!(
                "offset key {} contains invalid value {:?}: {}",
                expected_key, text, error
            ))
        })?;
        Ok(Self {
            value,
            offset,
            lease_id: kv.lease(),
            mod_revision: kv.mod_revision(),
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum OffsetCommitProgress {
    Complete,
    Retry(Option<OffsetGeneration>),
}

fn reconcile_offset_observation(
    target: i64,
    expected_lease_id: i64,
    observed: Option<OffsetGeneration>,
) -> Result<OffsetCommitProgress, MpscError> {
    match observed {
        Some(generation) if generation.lease_id != expected_lease_id => {
            Err(MpscError::Internal(format!(
                "offset generation lease mismatch: expected={} actual={} offset={}",
                expected_lease_id, generation.lease_id, generation.offset
            )))
        }
        Some(generation) if generation.offset >= target => Ok(OffsetCommitProgress::Complete),
        generation => Ok(OffsetCommitProgress::Retry(generation)),
    }
}

#[derive(Clone)]
struct FencedOffsetTxn {
    generation: Option<OffsetGeneration>,
    txn: etcd::Txn,
}

impl FencedOffsetTxn {
    fn new(key: &str, target: i64, lease_id: i64, generation: Option<OffsetGeneration>) -> Self {
        let compares = match generation.as_ref() {
            Some(generation) => vec![
                etcd::Compare::mod_revision(key, etcd::CompareOp::Equal, generation.mod_revision),
                etcd::Compare::lease(key, etcd::CompareOp::Equal, generation.lease_id),
                etcd::Compare::value(key, etcd::CompareOp::Equal, generation.value.clone()),
            ],
            None => vec![etcd::Compare::create_revision(
                key,
                etcd::CompareOp::Equal,
                0,
            )],
        };
        let put = etcd::TxnOp::put(
            key,
            target.to_string(),
            Some(etcd::PutOptions::new().with_lease(lease_id)),
        );
        let txn = etcd::Txn::new()
            .when(compares)
            .and_then(vec![put])
            .or_else(vec![etcd::TxnOp::get(key, None)]);
        Self { generation, txn }
    }

    fn still_matches(&self, observed: Option<&OffsetGeneration>) -> bool {
        self.generation.as_ref() == observed
    }
}

/// Generation-fenced monotonic offset commit state.
///
/// A failed or timed-out mutation attempt leaves `fenced_txn` unchanged, so the
/// caller can replay the same transaction. Once another generation is observed,
/// the next attempt is fenced against that exact generation instead.
pub(crate) struct MonotonicOffsetCommit {
    key: String,
    target: i64,
    lease_id: i64,
    fenced_txn: Option<FencedOffsetTxn>,
}

impl MonotonicOffsetCommit {
    pub(crate) fn new(key: String, target: i64, lease_id: i64) -> Self {
        Self {
            key,
            target,
            lease_id,
            fenced_txn: None,
        }
    }

    /// Performs one bounded convergence attempt.
    ///
    /// The first call reads the exact generation before issuing the mutation.
    /// Later calls replay the retained fenced transaction unless a successful
    /// else-Get proves that a different, lower generation must be advanced.
    pub(crate) async fn attempt(
        &mut self,
        client: &mut etcd::Client,
    ) -> Result<OffsetCommitProgress, MpscError> {
        if self.fenced_txn.is_none() {
            let response = client.get(self.key.clone(), None).await?;
            let observed = exact_generation_from_get(&response, &self.key)?;
            match reconcile_offset_observation(self.target, self.lease_id, observed)? {
                OffsetCommitProgress::Complete => return Ok(OffsetCommitProgress::Complete),
                OffsetCommitProgress::Retry(generation) => {
                    self.fenced_txn = Some(FencedOffsetTxn::new(
                        &self.key,
                        self.target,
                        self.lease_id,
                        generation,
                    ));
                }
            }
        }

        let fenced = self
            .fenced_txn
            .as_ref()
            .expect("offset commit must have a fenced transaction")
            .clone();
        let response = client.txn(fenced.txn.clone()).await?;
        if response.succeeded() {
            return Ok(OffsetCommitProgress::Complete);
        }

        let observed = exact_generation_from_txn_readback(&response, &self.key)?;
        if fenced.still_matches(observed.as_ref()) {
            return Err(MpscError::Internal(format!(
                "offset transaction compare was false although generation still matches key {}",
                self.key
            )));
        }

        let progress = reconcile_offset_observation(self.target, self.lease_id, observed)?;
        if let OffsetCommitProgress::Retry(generation) = &progress {
            self.fenced_txn = Some(FencedOffsetTxn::new(
                &self.key,
                self.target,
                self.lease_id,
                generation.clone(),
            ));
        }
        Ok(progress)
    }
}

fn exact_generation_from_get(
    response: &etcd::GetResponse,
    key: &str,
) -> Result<Option<OffsetGeneration>, MpscError> {
    if response.kvs().len() > 1 {
        return Err(MpscError::Internal(format!(
            "exact offset read returned duplicate keys for {}",
            key
        )));
    }
    response
        .kvs()
        .first()
        .map(|kv| OffsetGeneration::from_kv(kv, key))
        .transpose()
}

fn exact_generation_from_txn_readback(
    response: &etcd::TxnResponse,
    key: &str,
) -> Result<Option<OffsetGeneration>, MpscError> {
    let responses = response.op_responses();
    let [etcd::TxnOpResponse::Get(get)] = responses.as_slice() else {
        return Err(MpscError::Internal(format!(
            "offset transaction readback returned an invalid response shape for {}: operations={}",
            key,
            responses.len()
        )));
    };
    exact_generation_from_get(get, key)
}

#[cfg(test)]
mod tests {
    use super::{
        reconcile_offset_observation, FencedOffsetTxn, OffsetCommitProgress, OffsetGeneration,
    };

    fn generation(offset: i64, mod_revision: i64) -> OffsetGeneration {
        OffsetGeneration {
            value: offset.to_string().into_bytes(),
            offset,
            lease_id: 11,
            mod_revision,
        }
    }

    #[test]
    fn lower_offset_never_counts_as_converged() {
        let current = generation(40, 7);
        assert_eq!(
            reconcile_offset_observation(41, 11, Some(current.clone())).unwrap(),
            OffsetCommitProgress::Retry(Some(current))
        );
    }

    #[test]
    fn equal_or_higher_offset_is_already_converged() {
        assert_eq!(
            reconcile_offset_observation(41, 11, Some(generation(41, 7))).unwrap(),
            OffsetCommitProgress::Complete
        );
        assert_eq!(
            reconcile_offset_observation(41, 11, Some(generation(42, 8))).unwrap(),
            OffsetCommitProgress::Complete
        );
    }

    #[test]
    fn lower_offset_with_foreign_lease_fails_closed() {
        let mut foreign = generation(40, 7);
        foreign.lease_id = 12;
        let error = reconcile_offset_observation(41, 11, Some(foreign)).unwrap_err();
        assert!(error.to_string().contains("lease mismatch"));
    }

    #[test]
    fn higher_offset_with_foreign_lease_fails_closed() {
        let mut foreign = generation(42, 8);
        foreign.lease_id = 12;
        let error = reconcile_offset_observation(41, 11, Some(foreign)).unwrap_err();
        assert!(error.to_string().contains("lease mismatch"));
    }

    #[test]
    fn old_transaction_fence_does_not_match_a_new_generation() {
        let old = generation(40, 7);
        let fenced = FencedOffsetTxn::new("offset", 41, 11, Some(old.clone()));
        assert!(fenced.still_matches(Some(&old)));

        let newer = generation(41, 8);
        assert!(!fenced.still_matches(Some(&newer)));
        assert!(!fenced.still_matches(None));
    }
}
