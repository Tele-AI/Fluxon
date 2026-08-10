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

    pub(crate) fn committed(
        offset: i64,
        lease_id: i64,
        mod_revision: i64,
    ) -> Result<Self, MpscError> {
        if mod_revision <= 0 {
            return Err(MpscError::Internal(format!(
                "offset commit returned invalid revision {}",
                mod_revision
            )));
        }
        Ok(Self {
            value: offset.to_string().into_bytes(),
            offset,
            lease_id,
            mod_revision,
        })
    }

    fn from_successful_txn(
        response: &etcd::TxnResponse,
        offset: i64,
        lease_id: i64,
    ) -> Result<Self, MpscError> {
        let mod_revision = response
            .header()
            .ok_or_else(|| MpscError::Internal("offset commit response has no header".to_string()))?
            .revision();
        Self::committed(offset, lease_id, mod_revision)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum OffsetObservation {
    Absent,
    Present(OffsetGeneration),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum OffsetCommitProgress {
    Complete(OffsetGeneration),
    Retry(OffsetObservation),
}

fn reconcile_offset_observation(
    target: i64,
    expected_lease_id: i64,
    observed: OffsetObservation,
) -> Result<OffsetCommitProgress, MpscError> {
    match observed {
        OffsetObservation::Present(generation) if generation.lease_id != expected_lease_id => {
            Err(MpscError::Internal(format!(
                "offset generation lease mismatch: expected={} actual={} offset={}",
                expected_lease_id, generation.lease_id, generation.offset
            )))
        }
        OffsetObservation::Present(generation) if generation.offset >= target => {
            Ok(OffsetCommitProgress::Complete(generation))
        }
        observation => Ok(OffsetCommitProgress::Retry(observation)),
    }
}

#[derive(Clone)]
struct FencedOffsetTxn {
    observation: OffsetObservation,
    txn: etcd::Txn,
}

impl FencedOffsetTxn {
    fn new(key: &str, target: i64, lease_id: i64, observation: OffsetObservation) -> Self {
        let compares = match &observation {
            OffsetObservation::Present(generation) => vec![
                etcd::Compare::mod_revision(key, etcd::CompareOp::Equal, generation.mod_revision),
                etcd::Compare::lease(key, etcd::CompareOp::Equal, generation.lease_id),
                etcd::Compare::value(key, etcd::CompareOp::Equal, generation.value.clone()),
            ],
            OffsetObservation::Absent => vec![etcd::Compare::create_revision(
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
        Self { observation, txn }
    }

    fn still_matches(&self, observed: &OffsetObservation) -> bool {
        &self.observation == observed
    }
}

enum OffsetCommitState {
    NeedObservation,
    Ready(FencedOffsetTxn),
    Complete(OffsetGeneration),
}

/// Generation-fenced monotonic offset commit state.
///
/// A failed or timed-out mutation attempt leaves the ready transaction unchanged,
/// so the caller can replay the same fence. Once another generation is observed,
/// the next attempt is fenced against that exact generation instead.
pub(crate) struct MonotonicOffsetCommit {
    key: String,
    target: i64,
    lease_id: i64,
    state: OffsetCommitState,
}

impl MonotonicOffsetCommit {
    pub(crate) fn new(
        key: String,
        target: i64,
        lease_id: i64,
        initial_observation: Option<OffsetObservation>,
    ) -> Result<Self, MpscError> {
        let mut commit = Self {
            key,
            target,
            lease_id,
            state: OffsetCommitState::NeedObservation,
        };
        if let Some(observation) = initial_observation {
            commit.seed_cached_observation(observation)?;
        }
        Ok(commit)
    }

    fn seed_cached_observation(&mut self, observation: OffsetObservation) -> Result<(), MpscError> {
        match reconcile_offset_observation(self.target, self.lease_id, observation)? {
            OffsetCommitProgress::Complete(_) => {
                // A cache can seed a compare, but cannot prove that an already
                // satisfied generation still exists at the time of this call.
                self.state = OffsetCommitState::NeedObservation;
            }
            OffsetCommitProgress::Retry(observation) => {
                self.state = OffsetCommitState::Ready(FencedOffsetTxn::new(
                    &self.key,
                    self.target,
                    self.lease_id,
                    observation,
                ));
            }
        }
        Ok(())
    }

    fn install_observation(
        &mut self,
        observation: OffsetObservation,
    ) -> Result<OffsetCommitProgress, MpscError> {
        let progress = reconcile_offset_observation(self.target, self.lease_id, observation)?;
        self.state = match &progress {
            OffsetCommitProgress::Complete(generation) => {
                OffsetCommitState::Complete(generation.clone())
            }
            OffsetCommitProgress::Retry(observation) => OffsetCommitState::Ready(
                FencedOffsetTxn::new(&self.key, self.target, self.lease_id, observation.clone()),
            ),
        };
        Ok(progress)
    }

    /// Performs one bounded convergence attempt.
    ///
    /// Without an initial observation, the first call reads the exact generation
    /// before issuing the mutation. Later calls replay the retained fenced
    /// transaction unless a successful else-Get proves that the state changed.
    pub(crate) async fn attempt(
        &mut self,
        client: &mut etcd::Client,
    ) -> Result<OffsetCommitProgress, MpscError> {
        if matches!(self.state, OffsetCommitState::NeedObservation) {
            let response = client.get(self.key.clone(), None).await?;
            let observed = exact_observation_from_get(&response, &self.key)?;
            if let progress @ OffsetCommitProgress::Complete(_) =
                self.install_observation(observed)?
            {
                return Ok(progress);
            }
        }

        let fenced = match &self.state {
            OffsetCommitState::Ready(fenced) => fenced.clone(),
            OffsetCommitState::Complete(generation) => {
                return Ok(OffsetCommitProgress::Complete(generation.clone()));
            }
            OffsetCommitState::NeedObservation => {
                unreachable!("offset commit must have an observation after its initial read")
            }
        };
        let response = client.txn(fenced.txn.clone()).await?;
        if response.succeeded() {
            let generation =
                OffsetGeneration::from_successful_txn(&response, self.target, self.lease_id)?;
            self.state = OffsetCommitState::Complete(generation.clone());
            return Ok(OffsetCommitProgress::Complete(generation));
        }

        let observed = exact_observation_from_txn_readback(&response, &self.key)?;
        if fenced.still_matches(&observed) {
            return Err(MpscError::Internal(format!(
                "offset transaction compare was false although generation still matches key {}",
                self.key
            )));
        }

        self.install_observation(observed)
    }
}

fn exact_observation_from_get(
    response: &etcd::GetResponse,
    key: &str,
) -> Result<OffsetObservation, MpscError> {
    if response.kvs().len() > 1 {
        return Err(MpscError::Internal(format!(
            "exact offset read returned duplicate keys for {}",
            key
        )));
    }
    Ok(
        match response
            .kvs()
            .first()
            .map(|kv| OffsetGeneration::from_kv(kv, key))
            .transpose()?
        {
            Some(generation) => OffsetObservation::Present(generation),
            None => OffsetObservation::Absent,
        },
    )
}

fn exact_observation_from_txn_readback(
    response: &etcd::TxnResponse,
    key: &str,
) -> Result<OffsetObservation, MpscError> {
    let responses = response.op_responses();
    let [etcd::TxnOpResponse::Get(get)] = responses.as_slice() else {
        return Err(MpscError::Internal(format!(
            "offset transaction readback returned an invalid response shape for {}: operations={}",
            key,
            responses.len()
        )));
    };
    exact_observation_from_get(get, key)
}

#[cfg(test)]
mod tests {
    use super::{
        reconcile_offset_observation, FencedOffsetTxn, MonotonicOffsetCommit, OffsetCommitProgress,
        OffsetCommitState, OffsetGeneration, OffsetObservation,
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
            reconcile_offset_observation(41, 11, OffsetObservation::Present(current.clone()))
                .unwrap(),
            OffsetCommitProgress::Retry(OffsetObservation::Present(current))
        );
    }

    #[test]
    fn equal_or_higher_offset_is_already_converged() {
        assert_eq!(
            reconcile_offset_observation(41, 11, OffsetObservation::Present(generation(41, 7)))
                .unwrap(),
            OffsetCommitProgress::Complete(generation(41, 7))
        );
        assert_eq!(
            reconcile_offset_observation(41, 11, OffsetObservation::Present(generation(42, 8)))
                .unwrap(),
            OffsetCommitProgress::Complete(generation(42, 8))
        );
    }

    #[test]
    fn absent_offset_requires_a_create_fenced_commit() {
        assert_eq!(
            reconcile_offset_observation(41, 11, OffsetObservation::Absent).unwrap(),
            OffsetCommitProgress::Retry(OffsetObservation::Absent)
        );
    }

    #[test]
    fn lower_offset_with_foreign_lease_fails_closed() {
        let mut foreign = generation(40, 7);
        foreign.lease_id = 12;
        let error =
            reconcile_offset_observation(41, 11, OffsetObservation::Present(foreign)).unwrap_err();
        assert!(error.to_string().contains("lease mismatch"));
    }

    #[test]
    fn higher_offset_with_foreign_lease_fails_closed() {
        let mut foreign = generation(42, 8);
        foreign.lease_id = 12;
        let error =
            reconcile_offset_observation(41, 11, OffsetObservation::Present(foreign)).unwrap_err();
        assert!(error.to_string().contains("lease mismatch"));
    }

    #[test]
    fn old_transaction_fence_does_not_match_a_new_generation() {
        let old = generation(40, 7);
        let fenced =
            FencedOffsetTxn::new("offset", 41, 11, OffsetObservation::Present(old.clone()));
        assert!(fenced.still_matches(&OffsetObservation::Present(old)));

        let newer = generation(41, 8);
        assert!(!fenced.still_matches(&OffsetObservation::Present(newer)));
        assert!(!fenced.still_matches(&OffsetObservation::Absent));
    }

    #[test]
    fn known_absence_seeds_a_fenced_txn_without_an_initial_read() {
        let commit = MonotonicOffsetCommit::new(
            "offset".to_string(),
            41,
            11,
            Some(OffsetObservation::Absent),
        )
        .unwrap();
        assert!(matches!(commit.state, OffsetCommitState::Ready(_)));
    }

    #[test]
    fn cached_current_generation_requires_current_readback() {
        let current = generation(41, 7);
        let commit = MonotonicOffsetCommit::new(
            "offset".to_string(),
            41,
            11,
            Some(OffsetObservation::Present(current.clone())),
        )
        .unwrap();
        assert!(matches!(commit.state, OffsetCommitState::NeedObservation));
    }
}
