use anyhow::{Context, Result};
use etcd_client as etcd;
use fluxon_commu::{scan_etcd_prefix_paginated, EtcdPrefixScanAction, EtcdPrefixScanError};

use std::collections::{HashMap, HashSet, VecDeque};
use std::future::{poll_fn, Future};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use downcast_rs::{impl_downcast, Downcast};
use tokio::sync::{mpsc, oneshot, Notify};
use tokio::task::JoinSet;

use crate::keys::MqCategory;
use fluxon_observability::keys::{
    PROM_LABEL_MQ_CATEGORY, PROM_LABEL_MQ_CHAN_ID, PROM_LABEL_MQ_CONSUMER_IDX,
    PROM_LABEL_MQ_METRIC, PROM_LABEL_MQ_STAT, PROM_LABEL_NODE, PROM_LABEL_ROLE,
    PROM_METRIC_MQ_GET_ONE_LATENCY_US, PROM_METRIC_MQ_GET_ONE_WINDOW_BYTES,
    PROM_METRIC_MQ_GET_ONE_WINDOW_CALLS, PROM_METRIC_MQ_GET_ONE_WINDOW_TIMEOUTS,
    PROM_METRIC_MQ_PREFETCH_INFLIGHT_QUEUE_SIZE, PROM_METRIC_MQ_PREFETCH_LATENCY_US,
    PROM_METRIC_MQ_PREFETCH_TARGET_INFLIGHT, PROM_VALUE_MQ_CATEGORY_MPMC_SUB,
    PROM_VALUE_MQ_CATEGORY_MPSC, PROM_VALUE_MQ_GET_ONE_METRIC_POST,
    PROM_VALUE_MQ_GET_ONE_METRIC_SIGNAL, PROM_VALUE_MQ_GET_ONE_METRIC_TOTAL,
    PROM_VALUE_MQ_GET_ONE_METRIC_WAIT_RX, PROM_VALUE_MQ_PREFETCH_METRIC_ETCD_PUT,
    PROM_VALUE_MQ_PREFETCH_METRIC_GET_HANDLE, PROM_VALUE_MQ_PREFETCH_METRIC_HANDLE_AWAIT,
    PROM_VALUE_MQ_STAT_AVG, PROM_VALUE_MQ_STAT_LATEST, PROM_VALUE_MQ_STAT_MAX,
};
use fluxon_observability::metrics_actor::MetricsHandle as ObserveMetricsHandle;
use fluxon_util::etcd::{
    run_prefix_watch_loop, EtcdPrefixWatchLoopControl, OwnedEtcdWatchEvent,
    OwnedEtcdWatchEventKind, ETCD_PREFIX_WATCH_RESTART_SLEEP,
};
use fluxon_util::lease_manager::LeaseManager;
use fluxon_util::prom_remote_write::{Label, Sample, TimeSeries, LABEL_NAME as RW_LABEL_NAME};

use crate::error::MpscError;
use crate::keys;
use crate::lifecycle::spawn_named;
use crate::manager::{
    get_chan_meta, ChanManager, ChanMemberMeta, ChanRole, CONSUME_OFFSET_BEGIN,
    PRODUCE_OFFSET_BEGIN,
};
use crate::nonblocking_monitor::{
    spawn_nonblocking_monitor, NonblockingMonitorHandle, NonblockingMonitorKind,
};
use crate::shutdown::ShutdownCtl;
use crate::LifecycleView;
use crate::{BrokerEnvelope, BrokerFetchRequest, BrokerFetchedMessage, BrokerHandle};
use tracing::{debug, info, warn};

const NO_MESSAGE_WARN_INTERVAL: Duration = Duration::from_secs(30);
const PREFETCH_LATENCY_LOG_INTERVAL: Duration = NO_MESSAGE_WARN_INTERVAL;
const PREFETCH_LATENCY_WINDOW_SIZE: usize = 16;
const NONBLOCKING_QUEUE_WAIT_THRESHOLD: Duration = Duration::from_millis(500);
const DELETE_CALLBACK_WARN_INTERVAL: Duration = Duration::from_secs(1);
const BROKER_CLEANUP_DELETE_RETRY_INITIAL_SLEEP: Duration = Duration::from_millis(50);
const BROKER_CLEANUP_DELETE_RETRY_MAX_SLEEP: Duration = Duration::from_secs(5);
const BROKER_CLEANUP_ACK_RETRY_INITIAL_SLEEP: Duration = Duration::from_millis(50);
const BROKER_CLEANUP_ACK_RETRY_MAX_SLEEP: Duration = Duration::from_secs(5);
const COMMIT_WAIT_WARN_INTERVAL: Duration = Duration::from_secs(10);
const COMMIT_WAIT_BREAKDOWN_SUMMARY_THRESHOLD: Duration = Duration::from_millis(50);
const COMMIT_OFFSET_PUT_TIMEOUT: Duration = Duration::from_secs(10);
const COMMIT_OFFSET_RETRY_SLEEP: Duration = Duration::from_millis(50);
const COMMIT_OFFSET_SLOW_WARN_THRESHOLD: Duration = Duration::from_secs(1);
const PREFETCH_JOB_WARN_INTERVAL: Duration = Duration::from_secs(2);
const PREFETCH_HANDLE_AWAIT_WARN_INTERVAL: Duration = Duration::from_secs(2);
const COMMIT_PROGRESS_RETENTION: usize = 1024;
const STALE_PRODUCER_PROBE_TOMB_TTL: Duration = Duration::from_secs(10);
const READY_TRACE_HISTORY_PER_PRODUCER: usize = 64;
const PREFETCH_REFILL_BURST_MAX: usize = 128;
const PREFETCH_NO_MESSAGE_RETRY_EMPTY_SLEEP: Duration = Duration::from_millis(1);
const PREFETCH_NO_MESSAGE_RETRY_PARTIAL_SLEEP: Duration = Duration::from_millis(5);
static NEXT_CONSUMER_INSTANCE_ID: AtomicUsize = AtomicUsize::new(1);

fn map_prefix_scan_error(err: EtcdPrefixScanError<MpscError>) -> MpscError {
    match err {
        EtcdPrefixScanError::Get { source, .. } => MpscError::Etcd(source),
        EtcdPrefixScanError::Callback(source) => source,
    }
}

fn merge_monotonic_offset(cached: i64, probed: Option<i64>) -> i64 {
    match probed {
        Some(value) => value.max(cached),
        None => cached,
    }
}

fn merge_offset_cache_monotonic(current: &mut HashMap<String, i64>, fetched: HashMap<String, i64>) {
    if current.is_empty() {
        *current = fetched;
        return;
    }

    for (producer_id, fetched_offset) in fetched {
        current
            .entry(producer_id)
            .and_modify(|current_offset| {
                *current_offset = (*current_offset).max(fetched_offset);
            })
            .or_insert(fetched_offset);
    }
}

fn prefetch_refill_launch_budget(target: usize, current: usize) -> usize {
    target
        .saturating_sub(current)
        .min(PREFETCH_REFILL_BURST_MAX)
        .max(1)
}

fn prefetch_no_message_retry_sleep(current: usize) -> Duration {
    if current == 0 {
        PREFETCH_NO_MESSAGE_RETRY_EMPTY_SLEEP
    } else {
        PREFETCH_NO_MESSAGE_RETRY_PARTIAL_SLEEP
    }
}

fn prefetch_job_stage_name(stage: u8) -> &'static str {
    match stage {
        0 => "init",
        1 => "payload",
        2 => "wait_turn",
        3 => "commit",
        4 => "ready_to_advance",
        5 => "popped_to_advance",
        6 => "advanced",
        _ => "unknown",
    }
}

#[derive(Clone, Copy, Default)]
struct CommitWaitBreakdownNs {
    blocked_on_payload_ns: u128,
    blocked_on_wait_turn_ns: u128,
    blocked_on_commit_ns: u128,
    blocked_on_ready_queue_ns: u128,
    blocked_on_popped_to_advance_ns: u128,
    blocked_on_notify_gap_ns: u128,
}

impl CommitWaitBreakdownNs {
    fn total_ns(&self) -> u128 {
        self.blocked_on_payload_ns
            + self.blocked_on_wait_turn_ns
            + self.blocked_on_commit_ns
            + self.blocked_on_ready_queue_ns
            + self.blocked_on_popped_to_advance_ns
            + self.blocked_on_notify_gap_ns
    }

    fn add_assign(&mut self, other: &CommitWaitBreakdownNs) {
        self.blocked_on_payload_ns += other.blocked_on_payload_ns;
        self.blocked_on_wait_turn_ns += other.blocked_on_wait_turn_ns;
        self.blocked_on_commit_ns += other.blocked_on_commit_ns;
        self.blocked_on_ready_queue_ns += other.blocked_on_ready_queue_ns;
        self.blocked_on_popped_to_advance_ns += other.blocked_on_popped_to_advance_ns;
        self.blocked_on_notify_gap_ns += other.blocked_on_notify_gap_ns;
    }
}

struct CommitWaitOutcome {
    latency_ns: u128,
    blocker_count: usize,
    breakdown: CommitWaitBreakdownNs,
    summary: Option<String>,
}

struct CommitWaitSegment {
    blocker_seq: usize,
    begin_at: Instant,
    end_at: Instant,
}

struct CommitWaitDominantBlocker {
    seq: usize,
    producer_id: String,
    consume_offset: i64,
    stage_at_wait_begin: u8,
    segment_ns: u128,
    breakdown: CommitWaitBreakdownNs,
}

#[derive(Clone)]
struct CommitSeqProgress {
    producer_id: String,
    consume_offset: i64,
    payload_begin_at: Instant,
    wait_turn_begin_at: Option<Instant>,
    commit_begin_at: Option<Instant>,
    ready_to_advance_at: Option<Instant>,
    popped_at: Option<Instant>,
    advanced_at: Option<Instant>,
}

#[derive(Clone)]
struct CommitSequencer {
    instance_id: usize,
    next_seq: Arc<AtomicUsize>,
    notify: Arc<Notify>,
    progress: Arc<Mutex<HashMap<usize, CommitSeqProgress>>>,
}

impl CommitSequencer {
    fn new(instance_id: usize) -> Self {
        Self {
            instance_id,
            next_seq: Arc::new(AtomicUsize::new(0)),
            notify: Arc::new(Notify::new()),
            progress: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    fn begin_payload(&self, seq: usize, producer_id: &str, consume_offset: i64) {
        let mut progress = self.progress.lock().unwrap();
        let prev = progress.insert(
            seq,
            CommitSeqProgress {
                producer_id: producer_id.to_string(),
                consume_offset,
                payload_begin_at: Instant::now(),
                wait_turn_begin_at: None,
                commit_begin_at: None,
                ready_to_advance_at: None,
                popped_at: None,
                advanced_at: None,
            },
        );
        assert!(
            prev.is_none(),
            "duplicate commit progress registration for seq={}",
            seq
        );
    }

    fn mark_wait_turn_begin(&self, seq: usize) {
        let mut progress = self.progress.lock().unwrap();
        let entry = progress
            .get_mut(&seq)
            .unwrap_or_else(|| panic!("missing commit progress for wait_turn seq={}", seq));
        entry.wait_turn_begin_at = Some(Instant::now());
    }

    fn mark_commit_begin(&self, seq: usize) {
        let mut progress = self.progress.lock().unwrap();
        let entry = progress
            .get_mut(&seq)
            .unwrap_or_else(|| panic!("missing commit progress for commit seq={}", seq));
        entry.commit_begin_at = Some(Instant::now());
    }

    fn mark_ready_to_advance(&self, seq: usize) {
        let mut progress = self.progress.lock().unwrap();
        let entry = progress
            .get_mut(&seq)
            .unwrap_or_else(|| panic!("missing commit progress for ready seq={}", seq));
        entry.ready_to_advance_at = Some(Instant::now());
    }

    fn mark_popped(&self, seq: usize) {
        let mut progress = self.progress.lock().unwrap();
        let entry = progress
            .get_mut(&seq)
            .unwrap_or_else(|| panic!("missing commit progress for popped seq={}", seq));
        entry.popped_at = Some(Instant::now());
    }

    fn describe_progress_at(&self, seq: usize, at: Instant) -> String {
        let progress = self.progress.lock().unwrap();
        let entry = progress
            .get(&seq)
            .unwrap_or_else(|| panic!("missing commit progress for describe seq={}", seq));
        format!(
            "producer_id={} consume_offset={} stage={}",
            entry.producer_id,
            entry.consume_offset,
            prefetch_job_stage_name(entry.stage_at(at))
        )
    }

    fn advance(&self, seq: usize) {
        let now = Instant::now();
        let prev = self.next_seq.fetch_add(1, Ordering::SeqCst);
        assert_eq!(
            prev, seq,
            "commit sequencer advanced unexpected seq: expected={} actual={}",
            seq, prev
        );
        let new_next_seq = prev + 1;
        {
            let mut progress = self.progress.lock().unwrap();
            let entry = progress
                .get_mut(&prev)
                .unwrap_or_else(|| panic!("missing commit progress for advance seq={}", prev));
            entry.advanced_at = Some(now);
            let prune_before = new_next_seq.saturating_sub(COMMIT_PROGRESS_RETENTION);
            progress.retain(|progress_seq, progress_entry| {
                *progress_seq >= prune_before || progress_entry.popped_at.is_none()
            });
        }
        debug!(
            "[CommitSequencer instance_id={}] advance: prev_next_seq={} new_next_seq={}",
            self.instance_id, prev, new_next_seq,
        );
        self.notify.notify_waiters();
    }

    async fn wait_turn(
        &self,
        seq: usize,
        shutdown: &ShutdownCtl,
    ) -> Result<CommitWaitOutcome, MpscError> {
        let wait_begin = Instant::now();
        let mut blocker_segments: Vec<CommitWaitSegment> = Vec::new();
        let mut current_blocker_seq: Option<usize> = None;
        let mut current_blocker_begin_at = wait_begin;
        loop {
            if shutdown.is_closed() {
                return Err(MpscError::Closed);
            }
            let observed_next_seq = self.next_seq.load(Ordering::SeqCst);
            if observed_next_seq == seq {
                let wait_end = Instant::now();
                if let Some(blocker_seq) = current_blocker_seq.take() {
                    blocker_segments.push(CommitWaitSegment {
                        blocker_seq,
                        begin_at: current_blocker_begin_at,
                        end_at: wait_end,
                    });
                }
                return Ok(self.build_wait_outcome(wait_begin, wait_end, blocker_segments));
            }
            if current_blocker_seq != Some(observed_next_seq) {
                let now = Instant::now();
                if let Some(blocker_seq) = current_blocker_seq.replace(observed_next_seq) {
                    blocker_segments.push(CommitWaitSegment {
                        blocker_seq,
                        begin_at: current_blocker_begin_at,
                        end_at: now,
                    });
                }
                current_blocker_begin_at = now;
            }

            let notified = self.notify.notified();
            tokio::pin!(notified);
            let wait_warn_sleep = tokio::time::sleep(COMMIT_WAIT_WARN_INTERVAL);
            tokio::pin!(wait_warn_sleep);

            // NOTE: `Notify::notify_waiters()` does not store a permit, and the waiter is only
            // registered when the `Notified` future is polled. Poll once here to avoid missing
            // a wake-up between the seq check and the actual await.
            tokio::select! {
                _ = &mut notified => {}
                else => {}
            }

            let observed_next_seq = self.next_seq.load(Ordering::SeqCst);
            if observed_next_seq == seq {
                let wait_end = Instant::now();
                if let Some(blocker_seq) = current_blocker_seq.take() {
                    blocker_segments.push(CommitWaitSegment {
                        blocker_seq,
                        begin_at: current_blocker_begin_at,
                        end_at: wait_end,
                    });
                }
                return Ok(self.build_wait_outcome(wait_begin, wait_end, blocker_segments));
            }

            tokio::select! {
                biased;
                _ = &mut notified => {}
                _ = &mut wait_warn_sleep => {
                    let blocker_seq = self.next_seq.load(Ordering::SeqCst);
                    warn!(
                        "[CommitSequencer instance_id={}] still waiting for commit turn: seq={} next_seq={} waited_ms={} blocker_seq={} blocker={}",
                        self.instance_id,
                        seq,
                        blocker_seq,
                        wait_begin.elapsed().as_millis(),
                        blocker_seq,
                        self.describe_progress_at(blocker_seq, Instant::now()),
                    );
                }
                _ = shutdown.wait_closed() => {
                    return Err(MpscError::Closed);
                }
            }
        }
    }

    fn build_wait_outcome(
        &self,
        wait_begin: Instant,
        wait_end: Instant,
        blocker_segments: Vec<CommitWaitSegment>,
    ) -> CommitWaitOutcome {
        let latency_ns = wait_end.duration_since(wait_begin).as_nanos();
        let mut breakdown = CommitWaitBreakdownNs::default();
        let mut dominant_blocker: Option<CommitWaitDominantBlocker> = None;
        {
            let progress = self.progress.lock().unwrap();
            for segment in blocker_segments.iter() {
                let entry = progress.get(&segment.blocker_seq).unwrap_or_else(|| {
                    panic!(
                        "missing commit progress for blocked seq={} blocker_seq={}",
                        self.next_seq.load(Ordering::SeqCst),
                        segment.blocker_seq
                    )
                });
                let segment_breakdown = entry.segment_breakdown(segment.begin_at, segment.end_at);
                breakdown.add_assign(&segment_breakdown);
                let segment_ns = segment.end_at.duration_since(segment.begin_at).as_nanos();
                if dominant_blocker
                    .as_ref()
                    .map(|current| current.segment_ns < segment_ns)
                    .unwrap_or(true)
                {
                    dominant_blocker = Some(CommitWaitDominantBlocker {
                        seq: segment.blocker_seq,
                        producer_id: entry.producer_id.clone(),
                        consume_offset: entry.consume_offset,
                        stage_at_wait_begin: entry.stage_at(segment.begin_at),
                        segment_ns,
                        breakdown: segment_breakdown,
                    });
                }
            }
        }

        let summary = if latency_ns >= COMMIT_WAIT_BREAKDOWN_SUMMARY_THRESHOLD.as_nanos() {
            dominant_blocker.map(|dominant| {
                format!(
                    "blockers={} dominant_blocker_seq={} dominant_producer_id={} dominant_consume_offset={} dominant_stage_at_wait_begin={} dominant_segment_ms={} dominant_payload_ms={} dominant_wait_turn_ms={} dominant_commit_ms={} dominant_ready_queue_ms={} dominant_popped_to_advance_ms={} total_payload_ms={} total_wait_turn_ms={} total_commit_ms={} total_ready_queue_ms={} total_popped_to_advance_ms={} total_notify_gap_ms={} total_accounted_ms={}",
                    blocker_segments.len(),
                    dominant.seq,
                    dominant.producer_id,
                    dominant.consume_offset,
                    prefetch_job_stage_name(dominant.stage_at_wait_begin),
                    dominant.segment_ns / 1_000_000,
                    dominant.breakdown.blocked_on_payload_ns / 1_000_000,
                    dominant.breakdown.blocked_on_wait_turn_ns / 1_000_000,
                    dominant.breakdown.blocked_on_commit_ns / 1_000_000,
                    dominant.breakdown.blocked_on_ready_queue_ns / 1_000_000,
                    dominant.breakdown.blocked_on_popped_to_advance_ns / 1_000_000,
                    breakdown.blocked_on_payload_ns / 1_000_000,
                    breakdown.blocked_on_wait_turn_ns / 1_000_000,
                    breakdown.blocked_on_commit_ns / 1_000_000,
                    breakdown.blocked_on_ready_queue_ns / 1_000_000,
                    breakdown.blocked_on_popped_to_advance_ns / 1_000_000,
                    breakdown.blocked_on_notify_gap_ns / 1_000_000,
                    breakdown.total_ns() / 1_000_000,
                )
            })
        } else {
            None
        };

        CommitWaitOutcome {
            latency_ns,
            blocker_count: blocker_segments.len(),
            breakdown,
            summary,
        }
    }
}

impl CommitSeqProgress {
    fn stage_at(&self, at: Instant) -> u8 {
        if let Some(advanced_at) = self.advanced_at {
            if at >= advanced_at {
                return 6;
            }
        }
        if let Some(ready_to_advance_at) = self.ready_to_advance_at {
            if at >= ready_to_advance_at {
                if let (Some(popped_at), Some(advanced_at)) = (self.popped_at, self.advanced_at) {
                    if popped_at > ready_to_advance_at && popped_at < advanced_at && at >= popped_at
                    {
                        return 5;
                    }
                }
                return 4;
            }
        }
        if let Some(commit_begin_at) = self.commit_begin_at {
            if at >= commit_begin_at {
                return 3;
            }
        }
        if let Some(wait_turn_begin_at) = self.wait_turn_begin_at {
            if at >= wait_turn_begin_at {
                return 2;
            }
        }
        1
    }

    fn segment_breakdown(&self, begin_at: Instant, end_at: Instant) -> CommitWaitBreakdownNs {
        let wait_turn_begin_at = self.wait_turn_begin_at.unwrap_or_else(|| {
            panic!(
                "missing wait_turn_begin_at for producer_id={}",
                self.producer_id
            )
        });
        let commit_begin_at = self.commit_begin_at.unwrap_or_else(|| {
            panic!(
                "missing commit_begin_at for producer_id={}",
                self.producer_id
            )
        });
        let ready_to_advance_at = self.ready_to_advance_at.unwrap_or_else(|| {
            panic!(
                "missing ready_to_advance_at for producer_id={}",
                self.producer_id
            )
        });
        let advanced_at = self
            .advanced_at
            .unwrap_or_else(|| panic!("missing advanced_at for producer_id={}", self.producer_id));
        let segment_effective_end = if advanced_at < end_at {
            advanced_at
        } else {
            end_at
        };
        let payload_ns = overlap_interval_ns(
            self.payload_begin_at,
            wait_turn_begin_at,
            begin_at,
            segment_effective_end,
        );
        let wait_turn_ns = overlap_interval_ns(
            wait_turn_begin_at,
            commit_begin_at,
            begin_at,
            segment_effective_end,
        );
        let commit_ns = overlap_interval_ns(
            commit_begin_at,
            ready_to_advance_at,
            begin_at,
            segment_effective_end,
        );
        let ready_queue_end = match self.popped_at {
            Some(popped_at) if popped_at > ready_to_advance_at && popped_at < advanced_at => {
                popped_at
            }
            _ => ready_to_advance_at,
        };
        let ready_queue_ns = overlap_interval_ns(
            ready_to_advance_at,
            ready_queue_end,
            begin_at,
            segment_effective_end,
        );
        let popped_ready_begin = ready_queue_end;
        let popped_to_advance_ns = overlap_interval_ns(
            popped_ready_begin,
            advanced_at,
            begin_at,
            segment_effective_end,
        );
        let notify_gap_ns = if end_at > advanced_at {
            end_at.duration_since(advanced_at).as_nanos()
        } else {
            0
        };
        CommitWaitBreakdownNs {
            blocked_on_payload_ns: payload_ns,
            blocked_on_wait_turn_ns: wait_turn_ns,
            blocked_on_commit_ns: commit_ns,
            blocked_on_ready_queue_ns: ready_queue_ns,
            blocked_on_popped_to_advance_ns: popped_to_advance_ns,
            blocked_on_notify_gap_ns: notify_gap_ns,
        }
    }
}

fn overlap_interval_ns(
    stage_begin_at: Instant,
    stage_end_at: Instant,
    segment_begin_at: Instant,
    segment_end_at: Instant,
) -> u128 {
    if stage_begin_at >= stage_end_at || segment_begin_at >= segment_end_at {
        return 0;
    }
    let overlap_begin_at = if stage_begin_at > segment_begin_at {
        stage_begin_at
    } else {
        segment_begin_at
    };
    let overlap_end_at = if stage_end_at < segment_end_at {
        stage_end_at
    } else {
        segment_end_at
    };
    if overlap_begin_at >= overlap_end_at {
        return 0;
    }
    overlap_end_at.duration_since(overlap_begin_at).as_nanos()
}

struct SlidingWindowAvgNs {
    buf_ns: [u128; PREFETCH_LATENCY_WINDOW_SIZE],
    next_idx: usize,
    len: usize,
    sum_ns: u128,
}

impl SlidingWindowAvgNs {
    const fn new() -> Self {
        Self {
            buf_ns: [0; PREFETCH_LATENCY_WINDOW_SIZE],
            next_idx: 0,
            len: 0,
            sum_ns: 0,
        }
    }

    fn push(&mut self, value_ns: u128) {
        if self.len < PREFETCH_LATENCY_WINDOW_SIZE {
            self.buf_ns[self.next_idx] = value_ns;
            self.sum_ns += value_ns;
            self.len += 1;
            self.next_idx = (self.next_idx + 1) % PREFETCH_LATENCY_WINDOW_SIZE;
            return;
        }

        let old = self.buf_ns[self.next_idx];
        self.sum_ns -= old;
        self.buf_ns[self.next_idx] = value_ns;
        self.sum_ns += value_ns;
        self.next_idx = (self.next_idx + 1) % PREFETCH_LATENCY_WINDOW_SIZE;
    }

    fn avg_ns(&self) -> Option<u128> {
        if self.len == 0 {
            return None;
        }
        Some(self.sum_ns / (self.len as u128))
    }

    fn len(&self) -> usize {
        self.len
    }
}

#[derive(Clone, Copy, Default)]
struct CommitOffsetPutTraceNs {
    total_latency_ns: u128,
    first_poll_delay_ns: u128,
    first_poll_to_ready_ns: u128,
}

struct SelectNextMessageTrace {
    refresh_latency_ns: u128,
    refresh_call_count: usize,
    probe_latency_ns: u128,
    probe_call_count: usize,
}

impl SelectNextMessageTrace {
    const fn new() -> Self {
        Self {
            refresh_latency_ns: 0,
            refresh_call_count: 0,
            probe_latency_ns: 0,
            probe_call_count: 0,
        }
    }
}

struct SelectNextMessageStats {
    total_latency_window: SlidingWindowAvgNs,
    refresh_latency_window: SlidingWindowAvgNs,
    probe_latency_window: SlidingWindowAvgNs,
    latest_total_ns: u128,
    latest_refresh_ns: u128,
    latest_probe_ns: u128,
    latest_refresh_call_count: usize,
    latest_probe_call_count: usize,
    total_attempts: u64,
    success_attempts: u64,
    no_message_attempts: u64,
    error_attempts: u64,
    refresh_call_count: u64,
    probe_call_count: u64,
    no_message_backoff_count: u64,
}

impl SelectNextMessageStats {
    fn new() -> Self {
        Self {
            total_latency_window: SlidingWindowAvgNs::new(),
            refresh_latency_window: SlidingWindowAvgNs::new(),
            probe_latency_window: SlidingWindowAvgNs::new(),
            latest_total_ns: 0,
            latest_refresh_ns: 0,
            latest_probe_ns: 0,
            latest_refresh_call_count: 0,
            latest_probe_call_count: 0,
            total_attempts: 0,
            success_attempts: 0,
            no_message_attempts: 0,
            error_attempts: 0,
            refresh_call_count: 0,
            probe_call_count: 0,
            no_message_backoff_count: 0,
        }
    }

    fn record_attempt(
        &mut self,
        total_latency_ns: u128,
        trace: &SelectNextMessageTrace,
        result: &Result<(String, i64), MpscError>,
    ) {
        self.latest_total_ns = total_latency_ns;
        self.latest_refresh_ns = trace.refresh_latency_ns;
        self.latest_probe_ns = trace.probe_latency_ns;
        self.latest_refresh_call_count = trace.refresh_call_count;
        self.latest_probe_call_count = trace.probe_call_count;
        self.total_latency_window.push(total_latency_ns);
        self.refresh_latency_window.push(trace.refresh_latency_ns);
        self.probe_latency_window.push(trace.probe_latency_ns);
        self.total_attempts += 1;
        self.refresh_call_count += trace.refresh_call_count as u64;
        self.probe_call_count += trace.probe_call_count as u64;
        match result {
            Ok(_) => {
                self.success_attempts += 1;
            }
            Err(MpscError::NoMessage) => {
                self.no_message_attempts += 1;
            }
            Err(_) => {
                self.error_attempts += 1;
            }
        }
    }

    fn record_no_message_backoff(&mut self) {
        self.no_message_backoff_count += 1;
    }
}

#[derive(Clone, Copy)]
struct ProducerReadyPathTrace {
    produce_offset: i64,
    watch_observed_at: Instant,
    watch_send_begin_at: Instant,
    actor_update_at: Instant,
}

#[derive(Clone, Copy)]
struct SelectedReadyPathTrace {
    traced_produce_offset: i64,
    watch_observed_at: Instant,
    watch_send_begin_at: Instant,
    actor_update_at: Instant,
    selected_at: Instant,
}

#[derive(Clone, Copy)]
struct ReadyPathLatencySample {
    traced_produce_offset: i64,
    watch_observed_to_actor_update_ns: u128,
    watch_send_to_actor_update_ns: u128,
    actor_update_to_select_ns: u128,
    select_to_pop_ns: u128,
    actor_update_to_pop_ns: u128,
    watch_observed_to_pop_ns: u128,
}

/// Application-level payload (type-erased) to avoid coupling with upper layers.
pub trait MqPayload: Downcast + Send {
    fn attach_cleanup(&mut self, cleanup: PayloadCleanup) -> Result<(), PayloadCleanup> {
        Err(cleanup)
    }
}
impl_downcast!(MqPayload);

pub type PayloadCleanupFuture = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;
pub type PayloadCleanup = Box<dyn FnOnce() -> PayloadCleanupFuture + Send + 'static>;

/// Callback result: deliver a payload or indicate retry/non-retry.
pub enum PayloadResult {
    Ok(Box<dyn MqPayload>),
    Retryable(String),
    NonRetryable(String),
}

/// Callback result for delete operations.
pub enum DeleteResult {
    Ok,
    Retryable(String),
    NonRetryable(String),
}

pub type DeleteFuture = Pin<Box<dyn Future<Output = DeleteResult> + Send + 'static>>;
pub type DeleteCallback = Arc<dyn Fn(String) -> DeleteFuture + Send + Sync + 'static>;

pub type PayloadFuture = Pin<Box<dyn Future<Output = PayloadResult> + Send + 'static>>;
pub type PayloadCallback = Arc<dyn Fn(String, String) -> PayloadFuture + Send + Sync + 'static>;

/// test
enum ConsumerCmd {
    SetCallback(PayloadCallback),
}

/// MPSC channel consumer handle 暴露给上层（包括 PyO3）。
///
/// 持有基础标识和与 actor 通信的 mpsc::Sender，actor 本身
/// 只在本模块内部可见，满足“handle / actor 分离”的约束。
pub struct MpscConsumer {
    chan_id: i64,
    consumer_idx: String,
    instance_id: usize,
    kvclient_sub_cluster: Option<String>,
    external_client_id: Option<String>,
    observe_node_id: String,
    observe_node_role: String,
    observe: ObserveMetricsHandle,
    lease_manager: LeaseManager,
    /// Hold channel-level leases and payload lease via ChanManager RAII.
    /// Actor does not own leases; handle owns and drops them on close.
    chan_mgr: ChanManager,
    /// 预取窗口目标大小（inflight 条数），由上层 get 调用更新，
    /// 由 actor 在内部维护 queue.size < target_inflight 的不变式。
    target_inflight: Arc<AtomicUsize>,
    /// 当前本地队列中的预取条数，由 actor/consumer 共享维护。
    inflight_queue_size: Arc<AtomicUsize>,
    /// 本地预取队列的 consumer 视角：actor push、consumer pop。
    ///
    /// 队列元素是一次完整 get 操作的 JoinHandle；consumer
    /// 只需 pop 并等待其完成即可，保证按提交顺序消费。
    inflight_queue: Arc<Mutex<VecDeque<InflightItem>>>,
    inflight_consume_notify: Arc<Notify>,
    /// 控制通道，仅用于下发回调设置等控制类命令。
    cmd_tx: mpsc::Sender<ConsumerCmd>,
    /// Local mirror of payload callback for non-prefetch direct paths.
    payload_cb: Option<PayloadCallback>,
    /// delete callback invoked after successful consume-offset commit.
    delete_cb: Option<DeleteCallback>,
    /// Shared shutdown controller used by higher layers to signal
    /// that this consumer should stop prefetching and abort retry
    /// loops.
    shutdown: ShutdownCtl,
    /// MQ category decides backend key layout
    category: MqCategory,
    prefetch_latency_get_handle_window: SlidingWindowAvgNs,
    prefetch_latency_handle_await_window: SlidingWindowAvgNs,
    prefetch_latency_kv_get_window: SlidingWindowAvgNs,
    prefetch_latency_commit_wait_window: SlidingWindowAvgNs,
    prefetch_latency_commit_wait_blocked_payload_window: SlidingWindowAvgNs,
    prefetch_latency_commit_wait_blocked_wait_turn_window: SlidingWindowAvgNs,
    prefetch_latency_commit_wait_blocked_commit_window: SlidingWindowAvgNs,
    prefetch_latency_commit_wait_blocked_ready_queue_window: SlidingWindowAvgNs,
    prefetch_latency_commit_wait_blocked_popped_to_advance_window: SlidingWindowAvgNs,
    prefetch_latency_commit_wait_blocked_notify_gap_window: SlidingWindowAvgNs,
    prefetch_latency_etcd_put_window: SlidingWindowAvgNs,
    prefetch_latency_etcd_put_first_poll_delay_window: SlidingWindowAvgNs,
    prefetch_latency_etcd_put_first_poll_to_ready_window: SlidingWindowAvgNs,
    prefetch_latency_delete_window: SlidingWindowAvgNs,
    ready_path_watch_observed_to_actor_update_window: SlidingWindowAvgNs,
    ready_path_watch_send_to_actor_update_window: SlidingWindowAvgNs,
    ready_path_actor_update_to_select_window: SlidingWindowAvgNs,
    ready_path_select_to_pop_window: SlidingWindowAvgNs,
    ready_path_actor_update_to_pop_window: SlidingWindowAvgNs,
    ready_path_watch_observed_to_pop_window: SlidingWindowAvgNs,
    prefetch_latency_next_log_at: Instant,
    commit_seq: CommitSequencer,
    ready_path_trace_missing_total: u64,
    nonblocking_monitor: NonblockingMonitorHandle,
}

impl MpscConsumer {
    fn now_ms() -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock is before UNIX_EPOCH")
            .as_millis() as i64
    }

    fn mq_category_str(&self) -> &'static str {
        match self.category {
            MqCategory::MpmcSub { .. } => PROM_VALUE_MQ_CATEGORY_MPMC_SUB,
            MqCategory::Mpsc => PROM_VALUE_MQ_CATEGORY_MPSC,
        }
    }

    fn ts_one(
        &self,
        name: &'static str,
        extra_labels: &[(&'static str, String)],
        value: f64,
        ts_ms: i64,
    ) -> TimeSeries {
        let mut labels: Vec<Label> = Vec::with_capacity(8 + extra_labels.len());
        labels.push(Label {
            name: RW_LABEL_NAME.to_string(),
            value: name.to_string(),
        });
        labels.push(Label {
            name: PROM_LABEL_NODE.to_string(),
            value: self.observe_node_id.clone(),
        });
        labels.push(Label {
            name: PROM_LABEL_ROLE.to_string(),
            value: self.observe_node_role.clone(),
        });
        labels.push(Label {
            name: PROM_LABEL_MQ_CATEGORY.to_string(),
            value: self.mq_category_str().to_string(),
        });
        labels.push(Label {
            name: PROM_LABEL_MQ_CHAN_ID.to_string(),
            value: self.chan_id.to_string(),
        });
        labels.push(Label {
            name: PROM_LABEL_MQ_CONSUMER_IDX.to_string(),
            value: self.consumer_idx.clone(),
        });
        for (k, v) in extra_labels {
            labels.push(Label {
                name: (*k).to_string(),
                value: v.clone(),
            });
        }
        TimeSeries {
            labels,
            samples: vec![Sample {
                value,
                timestamp: ts_ms,
            }],
        }
    }

    pub fn observe_get_one_breakdown_window_ms(
        &self,
        avg_total_ms: f64,
        max_total_ms: f64,
        avg_wait_rx_ms: f64,
        max_wait_rx_ms: f64,
        avg_signal_ms: f64,
        max_signal_ms: f64,
        avg_post_ms: f64,
        max_post_ms: f64,
        window_calls: u64,
        window_timeouts: u64,
        window_bytes: u64,
    ) {
        let ts_ms = Self::now_ms();
        let mut series: Vec<TimeSeries> = Vec::with_capacity(12);

        for (metric, avg_ms, max_ms) in [
            (
                PROM_VALUE_MQ_GET_ONE_METRIC_TOTAL,
                avg_total_ms,
                max_total_ms,
            ),
            (
                PROM_VALUE_MQ_GET_ONE_METRIC_WAIT_RX,
                avg_wait_rx_ms,
                max_wait_rx_ms,
            ),
            (
                PROM_VALUE_MQ_GET_ONE_METRIC_SIGNAL,
                avg_signal_ms,
                max_signal_ms,
            ),
            (PROM_VALUE_MQ_GET_ONE_METRIC_POST, avg_post_ms, max_post_ms),
        ] {
            series.push(self.ts_one(
                PROM_METRIC_MQ_GET_ONE_LATENCY_US,
                &[
                    (PROM_LABEL_MQ_METRIC, metric.to_string()),
                    (PROM_LABEL_MQ_STAT, PROM_VALUE_MQ_STAT_AVG.to_string()),
                ],
                avg_ms * 1000.0,
                ts_ms,
            ));
            series.push(self.ts_one(
                PROM_METRIC_MQ_GET_ONE_LATENCY_US,
                &[
                    (PROM_LABEL_MQ_METRIC, metric.to_string()),
                    (PROM_LABEL_MQ_STAT, PROM_VALUE_MQ_STAT_MAX.to_string()),
                ],
                max_ms * 1000.0,
                ts_ms,
            ));
        }

        series.push(self.ts_one(
            PROM_METRIC_MQ_GET_ONE_WINDOW_CALLS,
            &[],
            window_calls as f64,
            ts_ms,
        ));
        series.push(self.ts_one(
            PROM_METRIC_MQ_GET_ONE_WINDOW_TIMEOUTS,
            &[],
            window_timeouts as f64,
            ts_ms,
        ));
        series.push(self.ts_one(
            PROM_METRIC_MQ_GET_ONE_WINDOW_BYTES,
            &[],
            window_bytes as f64,
            ts_ms,
        ));

        self.observe.try_submit_timeseries(series);
    }

    fn maybe_log_prefetch_latency(
        &mut self,
        parent_mpmc_id: Option<i64>,
        latest_get_handle_ns: u128,
        latest_handle_await_ns: u128,
        latest_kv_get_ns: u128,
        latest_commit_wait_ns: u128,
        latest_commit_wait_breakdown: CommitWaitBreakdownNs,
        latest_commit_wait_blocker_count: usize,
        latest_commit_wait_summary: Option<&str>,
        latest_etcd_put_ns: u128,
        latest_etcd_put_first_poll_delay_ns: u128,
        latest_etcd_put_first_poll_to_ready_ns: u128,
        latest_delete_ns: u128,
        latest_ready_path_sample: Option<ReadyPathLatencySample>,
    ) {
        let inflight_queue_size = self.inflight_queue_size.load(Ordering::Relaxed);
        let target_inflight = self.target_inflight.load(Ordering::Relaxed);

        let latest_get_handle_ms = latest_get_handle_ns / 1_000_000;
        let latest_handle_await_ms = latest_handle_await_ns / 1_000_000;
        let latest_kv_get_ms = latest_kv_get_ns / 1_000_000;
        let latest_commit_wait_ms = latest_commit_wait_ns / 1_000_000;
        let latest_commit_wait_blocked_payload_ms =
            latest_commit_wait_breakdown.blocked_on_payload_ns / 1_000_000;
        let latest_commit_wait_blocked_wait_turn_ms =
            latest_commit_wait_breakdown.blocked_on_wait_turn_ns / 1_000_000;
        let latest_commit_wait_blocked_commit_ms =
            latest_commit_wait_breakdown.blocked_on_commit_ns / 1_000_000;
        let latest_commit_wait_blocked_ready_queue_ms =
            latest_commit_wait_breakdown.blocked_on_ready_queue_ns / 1_000_000;
        let latest_commit_wait_blocked_popped_to_advance_ms =
            latest_commit_wait_breakdown.blocked_on_popped_to_advance_ns / 1_000_000;
        let latest_commit_wait_blocked_notify_gap_ms =
            latest_commit_wait_breakdown.blocked_on_notify_gap_ns / 1_000_000;
        let latest_etcd_put_ms = latest_etcd_put_ns / 1_000_000;
        let latest_etcd_put_first_poll_delay_ms = latest_etcd_put_first_poll_delay_ns / 1_000_000;
        let latest_etcd_put_first_poll_to_ready_ms =
            latest_etcd_put_first_poll_to_ready_ns / 1_000_000;
        let latest_delete_ms = latest_delete_ns / 1_000_000;

        let now = Instant::now();
        let avg_get_handle_ms =
            self.prefetch_latency_get_handle_window.avg_ns().unwrap() / 1_000_000;
        let avg_handle_await_ms =
            self.prefetch_latency_handle_await_window.avg_ns().unwrap() / 1_000_000;
        let avg_kv_get_ms = self.prefetch_latency_kv_get_window.avg_ns().unwrap() / 1_000_000;
        let avg_commit_wait_ms =
            self.prefetch_latency_commit_wait_window.avg_ns().unwrap() / 1_000_000;
        let avg_commit_wait_blocked_payload_ms = self
            .prefetch_latency_commit_wait_blocked_payload_window
            .avg_ns()
            .unwrap()
            / 1_000_000;
        let avg_commit_wait_blocked_wait_turn_ms = self
            .prefetch_latency_commit_wait_blocked_wait_turn_window
            .avg_ns()
            .unwrap()
            / 1_000_000;
        let avg_commit_wait_blocked_commit_ms = self
            .prefetch_latency_commit_wait_blocked_commit_window
            .avg_ns()
            .unwrap()
            / 1_000_000;
        let avg_commit_wait_blocked_ready_queue_ms = self
            .prefetch_latency_commit_wait_blocked_ready_queue_window
            .avg_ns()
            .unwrap()
            / 1_000_000;
        let avg_commit_wait_blocked_popped_to_advance_ms = self
            .prefetch_latency_commit_wait_blocked_popped_to_advance_window
            .avg_ns()
            .unwrap()
            / 1_000_000;
        let avg_commit_wait_blocked_notify_gap_ms = self
            .prefetch_latency_commit_wait_blocked_notify_gap_window
            .avg_ns()
            .unwrap()
            / 1_000_000;
        let avg_etcd_put_ms = self.prefetch_latency_etcd_put_window.avg_ns().unwrap() / 1_000_000;
        let avg_etcd_put_first_poll_delay_ms = self
            .prefetch_latency_etcd_put_first_poll_delay_window
            .avg_ns()
            .unwrap()
            / 1_000_000;
        let avg_etcd_put_first_poll_to_ready_ms = self
            .prefetch_latency_etcd_put_first_poll_to_ready_window
            .avg_ns()
            .unwrap()
            / 1_000_000;
        let avg_delete_ms = self.prefetch_latency_delete_window.avg_ns().unwrap() / 1_000_000;
        let cnt = self.prefetch_latency_get_handle_window.len();
        let mut msg = format!(
            "[MpscConsumer prefetch parent_mpmc_id={:?} mpsc_id={}] avg_get_handle_ms={} avg_handle_await_ms={} avg_kv_get_ms={} avg_commit_wait_ms={} avg_commit_wait_blocked_payload_ms={} avg_commit_wait_blocked_wait_turn_ms={} avg_commit_wait_blocked_commit_ms={} avg_commit_wait_blocked_ready_queue_ms={} avg_commit_wait_blocked_popped_to_advance_ms={} avg_commit_wait_blocked_notify_gap_ms={} avg_etcd_put_ms={} avg_etcd_put_first_poll_delay_ms={} avg_etcd_put_first_poll_to_ready_ms={} avg_delete_ms={} latest_get_handle_ms={} latest_handle_await_ms={} latest_kv_get_ms={} latest_commit_wait_ms={} latest_commit_wait_blocked_payload_ms={} latest_commit_wait_blocked_wait_turn_ms={} latest_commit_wait_blocked_commit_ms={} latest_commit_wait_blocked_ready_queue_ms={} latest_commit_wait_blocked_popped_to_advance_ms={} latest_commit_wait_blocked_notify_gap_ms={} latest_commit_wait_blocker_count={} latest_etcd_put_ms={} latest_etcd_put_first_poll_delay_ms={} latest_etcd_put_first_poll_to_ready_ms={} latest_delete_ms={} cnt={} inflight_queue_size={} target_inflight={}",
            parent_mpmc_id,
            self.chan_id,
            avg_get_handle_ms,
            avg_handle_await_ms,
            avg_kv_get_ms,
            avg_commit_wait_ms,
            avg_commit_wait_blocked_payload_ms,
            avg_commit_wait_blocked_wait_turn_ms,
            avg_commit_wait_blocked_commit_ms,
            avg_commit_wait_blocked_ready_queue_ms,
            avg_commit_wait_blocked_popped_to_advance_ms,
            avg_commit_wait_blocked_notify_gap_ms,
            avg_etcd_put_ms,
            avg_etcd_put_first_poll_delay_ms,
            avg_etcd_put_first_poll_to_ready_ms,
            avg_delete_ms,
            latest_get_handle_ms,
            latest_handle_await_ms,
            latest_kv_get_ms,
            latest_commit_wait_ms,
            latest_commit_wait_blocked_payload_ms,
            latest_commit_wait_blocked_wait_turn_ms,
            latest_commit_wait_blocked_commit_ms,
            latest_commit_wait_blocked_ready_queue_ms,
            latest_commit_wait_blocked_popped_to_advance_ms,
            latest_commit_wait_blocked_notify_gap_ms,
            latest_commit_wait_blocker_count,
            latest_etcd_put_ms,
            latest_etcd_put_first_poll_delay_ms,
            latest_etcd_put_first_poll_to_ready_ms,
            latest_delete_ms,
            cnt,
            inflight_queue_size,
            target_inflight,
        );
        if let Some(summary) = latest_commit_wait_summary {
            msg.push_str(" latest_commit_wait_summary=");
            msg.push_str(summary);
        }
        let ready_trace_cnt = self.ready_path_watch_observed_to_pop_window.len();
        if ready_trace_cnt > 0 {
            let avg_ready_watch_observed_to_actor_update_ms = self
                .ready_path_watch_observed_to_actor_update_window
                .avg_ns()
                .unwrap()
                / 1_000_000;
            let avg_ready_watch_send_to_actor_update_ms = self
                .ready_path_watch_send_to_actor_update_window
                .avg_ns()
                .unwrap()
                / 1_000_000;
            let avg_ready_actor_update_to_select_ms = self
                .ready_path_actor_update_to_select_window
                .avg_ns()
                .unwrap()
                / 1_000_000;
            let avg_ready_select_to_pop_ms =
                self.ready_path_select_to_pop_window.avg_ns().unwrap() / 1_000_000;
            let avg_ready_actor_update_to_pop_ms =
                self.ready_path_actor_update_to_pop_window.avg_ns().unwrap() / 1_000_000;
            let avg_ready_watch_observed_to_pop_ms = self
                .ready_path_watch_observed_to_pop_window
                .avg_ns()
                .unwrap()
                / 1_000_000;
            msg.push_str(&format!(
                " avg_ready_watch_observed_to_actor_update_ms={} avg_ready_watch_send_to_actor_update_ms={} avg_ready_actor_update_to_select_ms={} avg_ready_select_to_pop_ms={} avg_ready_actor_update_to_pop_ms={} avg_ready_watch_observed_to_pop_ms={} ready_trace_cnt={} ready_trace_missing_total={}",
                avg_ready_watch_observed_to_actor_update_ms,
                avg_ready_watch_send_to_actor_update_ms,
                avg_ready_actor_update_to_select_ms,
                avg_ready_select_to_pop_ms,
                avg_ready_actor_update_to_pop_ms,
                avg_ready_watch_observed_to_pop_ms,
                ready_trace_cnt,
                self.ready_path_trace_missing_total,
            ));
            if let Some(sample) = latest_ready_path_sample {
                msg.push_str(&format!(
                    " latest_ready_traced_produce_offset={} latest_ready_watch_observed_to_actor_update_ms={} latest_ready_watch_send_to_actor_update_ms={} latest_ready_actor_update_to_select_ms={} latest_ready_select_to_pop_ms={} latest_ready_actor_update_to_pop_ms={} latest_ready_watch_observed_to_pop_ms={}",
                    sample.traced_produce_offset,
                    sample.watch_observed_to_actor_update_ns / 1_000_000,
                    sample.watch_send_to_actor_update_ns / 1_000_000,
                    sample.actor_update_to_select_ns / 1_000_000,
                    sample.select_to_pop_ns / 1_000_000,
                    sample.actor_update_to_pop_ns / 1_000_000,
                    sample.watch_observed_to_pop_ns / 1_000_000,
                ));
            } else {
                msg.push_str(" latest_ready_trace_hit=false");
            }
        } else {
            msg.push_str(&format!(
                " ready_trace_cnt=0 ready_trace_missing_total={}",
                self.ready_path_trace_missing_total,
            ));
        }

        if now >= self.prefetch_latency_next_log_at {
            info!("{}", msg);
            self.prefetch_latency_next_log_at = now + PREFETCH_LATENCY_LOG_INTERVAL;

            let ts_ms = Self::now_ms();
            let mut series: Vec<TimeSeries> = Vec::with_capacity(8);
            for (metric, avg_ms, latest_ms) in [
                (
                    PROM_VALUE_MQ_PREFETCH_METRIC_GET_HANDLE,
                    avg_get_handle_ms,
                    latest_get_handle_ms,
                ),
                (
                    PROM_VALUE_MQ_PREFETCH_METRIC_HANDLE_AWAIT,
                    avg_handle_await_ms,
                    latest_handle_await_ms,
                ),
                (
                    PROM_VALUE_MQ_PREFETCH_METRIC_ETCD_PUT,
                    avg_etcd_put_ms,
                    latest_etcd_put_ms,
                ),
            ] {
                series.push(self.ts_one(
                    PROM_METRIC_MQ_PREFETCH_LATENCY_US,
                    &[
                        (PROM_LABEL_MQ_METRIC, metric.to_string()),
                        (PROM_LABEL_MQ_STAT, PROM_VALUE_MQ_STAT_AVG.to_string()),
                    ],
                    (avg_ms as f64) * 1000.0,
                    ts_ms,
                ));
                series.push(self.ts_one(
                    PROM_METRIC_MQ_PREFETCH_LATENCY_US,
                    &[
                        (PROM_LABEL_MQ_METRIC, metric.to_string()),
                        (PROM_LABEL_MQ_STAT, PROM_VALUE_MQ_STAT_LATEST.to_string()),
                    ],
                    (latest_ms as f64) * 1000.0,
                    ts_ms,
                ));
            }
            series.push(self.ts_one(
                PROM_METRIC_MQ_PREFETCH_INFLIGHT_QUEUE_SIZE,
                &[],
                inflight_queue_size as f64,
                ts_ms,
            ));
            series.push(self.ts_one(
                PROM_METRIC_MQ_PREFETCH_TARGET_INFLIGHT,
                &[],
                target_inflight as f64,
                ts_ms,
            ));
            self.observe.try_submit_timeseries(series);
        } else {
            debug!("{}", msg);
        }
    }

    async fn recv_next_inflight_handle_with_idle_warn(&mut self) -> Option<InflightItem> {
        if let Some(handle) = self
            .inflight_queue
            .lock()
            .expect("inflight queue mutex poisoned")
            .pop_front()
        {
            return Some(handle);
        }

        let idle_warn_sleep = tokio::time::sleep(NO_MESSAGE_WARN_INTERVAL);
        tokio::pin!(idle_warn_sleep);

        loop {
            if self.shutdown.is_closed() {
                return None;
            }
            let queue_notify = self.inflight_consume_notify.notified();
            tokio::pin!(queue_notify);
            tokio::select! {
                biased;
                _ = &mut queue_notify => {
                    if let Some(handle) = self
                        .inflight_queue
                        .lock()
                        .expect("inflight queue mutex poisoned")
                        .pop_front()
                    {
                        return Some(handle);
                    }
                }
                _ = &mut idle_warn_sleep => {
                    let parent_mpmc_id = match self.category {
                        MqCategory::MpmcSub { parent_mpmc_id } => Some(parent_mpmc_id),
                        MqCategory::Mpsc => None,
                    };
                    warn!(
                        "[MpscConsumer instance_id={} parent_mpmc_id={:?} mpsc_id={}] waiting for inflight prefetch job: no new message for {}s",
                        self.instance_id,
                        parent_mpmc_id,
                        self.chan_id,
                        NO_MESSAGE_WARN_INTERVAL.as_secs(),
                    );
                    idle_warn_sleep
                        .as_mut()
                        .reset(tokio::time::Instant::now() + NO_MESSAGE_WARN_INTERVAL);
                }
                _ = self.shutdown.wait_closed() => {
                    return None;
                }
            }
        }
    }

    /// Bind a consumer for the given MPSC channel.
    ///
    /// `chan_mgr` carries channel-level information (chan_id and
    /// global leases) constructed by `create_mpsc_channel` or by an
    /// equivalent loader. This API focuses on per-consumer member
    /// lease and membership registration.
    pub async fn bind_mpsc(
        chan_mgr: ChanManager,
        _ttl_seconds: i64,
        lifecycle: LifecycleView,
        shutdown: ShutdownCtl,
        external_client_id: Option<String>,
        category: MqCategory,
        kvclient_sub_cluster: Option<String>,
        observe_node_id: String,
        observe_node_role: String,
        observe: ObserveMetricsHandle,
    ) -> Result<Self> {
        if let Some(id) = external_client_id.as_deref() {
            if id.trim().is_empty() {
                anyhow::bail!("external_client_id must be a non-empty string when provided");
            }
            if id != id.trim() {
                anyhow::bail!("external_client_id must not have leading/trailing whitespace");
            }
        }
        if let Some(sc) = kvclient_sub_cluster.as_deref() {
            if sc.trim().is_empty() {
                anyhow::bail!("kvclient_sub_cluster must be a non-empty string when provided");
            }
            if sc != sc.trim() {
                anyhow::bail!("kvclient_sub_cluster must not have leading/trailing whitespace");
            }
        }
        if observe_node_id.trim().is_empty() {
            anyhow::bail!("observe_node_id must be a non-empty string");
        }
        if observe_node_id != observe_node_id.trim() {
            anyhow::bail!("observe_node_id must not have leading/trailing whitespace");
        }
        if observe_node_role.trim().is_empty() {
            anyhow::bail!("observe_node_role must be a non-empty string");
        }
        if observe_node_role != observe_node_role.trim() {
            anyhow::bail!("observe_node_role must not have leading/trailing whitespace");
        }

        let chan_id = chan_mgr.chan_id;
        let lease_manager = chan_mgr.lease_manager.clone();
        let mut client = chan_mgr.etcd_client();

        // 1) Ensure channel meta exists
        let mut meta_client = chan_mgr.etcd_client();
        let _meta = get_chan_meta(&mut meta_client, chan_id)
            .await
            .with_context(|| format!("channel meta not found for chan_id={}", chan_id))?;

        // 2) Reuse ChanManager's member lease instead of creating a
        // new one. ChanManager 自身在创建或绑定 channel 时已经为
        // 当前实例准备了 member lease，这里只需要拿到 lease_id
        // 用于 membership key 绑定即可。
        let member_lease_id = chan_mgr.member_lease_id();

        // 3) Allocate consumer idx using the shared distributed id allocator.
        //
        // IMPORTANT: The allocator counter key `dist_id_allocator/channels/{chan_id}/consumers`
        // must NOT be tied to the per-member lease, otherwise a member lease expiration can
        // delete the counter and cause consumer_idx to restart from 1, while an old membership
        // key `/channels/{chan_id}/consumer/consumer_1` may still exist (e.g. bound to a different
        // longer-lived lease), leading to "already exists" bind failures.
        //
        // Use the channel-level long-lived lease so the allocator stays monotonic as long as
        // at least one ChanManager instance keeps the long lease alive.
        let mut idx_client = client.clone();
        let allocator_lease_id = chan_mgr.global_long_lease.id() as i64;
        let consumer_idx =
            register_consumer_idx(&mut idx_client, chan_id, allocator_lease_id).await?;

        // 4) Bind consumer membership key under member lease, storing
        // ChanMemberMeta as JSON for future introspection.
        let consumer_key = keys::etcd_consumer_key(chan_id, &consumer_idx);
        let compare =
            etcd::Compare::create_revision(consumer_key.clone(), etcd::CompareOp::Equal, 0);

        let member_meta = ChanMemberMeta {
            member_id: consumer_idx.clone(),
            role: ChanRole::Consumer,
            external_client_id: external_client_id.clone(),
            kvclient_sub_cluster: kvclient_sub_cluster.clone(),
        };
        let meta_bytes = serde_json::to_vec(&member_meta)
            .map_err(|e| MpscError::Internal(format!("serialize ChanMemberMeta failed: {}", e)))?;

        let put_op = etcd::TxnOp::put(
            consumer_key.clone(),
            meta_bytes,
            Some(etcd::PutOptions::new().with_lease(member_lease_id)),
        );
        let txn = etcd::Txn::new().when(vec![compare]).and_then(vec![put_op]);
        let txn_res = client
            .txn(txn)
            .await
            .with_context(|| format!("failed to bind consumer membership key {}", consumer_key))?;
        if !txn_res.succeeded() {
            anyhow::bail!("consumer membership key {} already exists", consumer_key);
        }

        // 5) 创建预取 actor，并通过共享队列与之协作。
        // actor 自身负责启动 producer member metadata 的 watch；
        // consumer 仅持有与 actor 通信的 channel 及共享队列视图。
        //
        // shutdown 控制器由上层（例如 PyO3 层）构造并注入，
        // 这里仅复用同一个实例以便 handle/actor 共享关闭信号。
        let global_lease_id = chan_mgr.global_lease.id() as i64;
        let (
            cmd_tx,
            inflight_queue,
            target_inflight,
            inflight_queue_size,
            inflight_consume_notify,
            commit_seq,
        ) = ConsumerActor::spawn(
            chan_id,
            client.clone(),
            lease_manager.clone(),
            consumer_idx.clone(),
            lifecycle.clone(),
            shutdown.clone(),
            category,
            global_lease_id,
        );
        let nonblocking_monitor = spawn_nonblocking_monitor(
            &lifecycle,
            shutdown.clone(),
            observe_node_id.clone(),
            observe_node_role.clone(),
            observe.clone(),
            category,
            NonblockingMonitorKind::Consumer { chan_id },
            consumer_idx.clone(),
        );

        Ok(Self {
            chan_id,
            consumer_idx,
            instance_id: commit_seq.instance_id,
            kvclient_sub_cluster,
            external_client_id,
            observe_node_id,
            observe_node_role,
            observe,
            lease_manager,
            chan_mgr,
            target_inflight,
            inflight_queue_size,
            inflight_queue,
            cmd_tx,
            inflight_consume_notify,
            payload_cb: None,
            delete_cb: None,
            shutdown,
            category,
            prefetch_latency_get_handle_window: SlidingWindowAvgNs::new(),
            prefetch_latency_handle_await_window: SlidingWindowAvgNs::new(),
            prefetch_latency_kv_get_window: SlidingWindowAvgNs::new(),
            prefetch_latency_commit_wait_window: SlidingWindowAvgNs::new(),
            prefetch_latency_commit_wait_blocked_payload_window: SlidingWindowAvgNs::new(),
            prefetch_latency_commit_wait_blocked_wait_turn_window: SlidingWindowAvgNs::new(),
            prefetch_latency_commit_wait_blocked_commit_window: SlidingWindowAvgNs::new(),
            prefetch_latency_commit_wait_blocked_ready_queue_window: SlidingWindowAvgNs::new(),
            prefetch_latency_commit_wait_blocked_popped_to_advance_window: SlidingWindowAvgNs::new(
            ),
            prefetch_latency_commit_wait_blocked_notify_gap_window: SlidingWindowAvgNs::new(),
            prefetch_latency_etcd_put_window: SlidingWindowAvgNs::new(),
            prefetch_latency_etcd_put_first_poll_delay_window: SlidingWindowAvgNs::new(),
            prefetch_latency_etcd_put_first_poll_to_ready_window: SlidingWindowAvgNs::new(),
            prefetch_latency_delete_window: SlidingWindowAvgNs::new(),
            ready_path_watch_observed_to_actor_update_window: SlidingWindowAvgNs::new(),
            ready_path_watch_send_to_actor_update_window: SlidingWindowAvgNs::new(),
            ready_path_actor_update_to_select_window: SlidingWindowAvgNs::new(),
            ready_path_select_to_pop_window: SlidingWindowAvgNs::new(),
            ready_path_actor_update_to_pop_window: SlidingWindowAvgNs::new(),
            ready_path_watch_observed_to_pop_window: SlidingWindowAvgNs::new(),
            prefetch_latency_next_log_at: Instant::now() + PREFETCH_LATENCY_LOG_INTERVAL,
            commit_seq,
            ready_path_trace_missing_total: 0,
            nonblocking_monitor,
        })
    }

    pub fn chan_id(&self) -> i64 {
        self.chan_id
    }

    pub fn consumer_idx(&self) -> &str {
        &self.consumer_idx
    }

    pub fn channel_capacity(&self) -> i64 {
        self.chan_mgr.capacity()
    }

    pub fn lease_manager(&self) -> &LeaseManager {
        &self.lease_manager
    }

    /// Shared shutdown controller for this consumer instance.
    pub fn shutdown_ctl(&self) -> ShutdownCtl {
        self.shutdown.clone()
    }

    fn record_nonblocking_get_success(&self, unix_ms: i64) {
        self.nonblocking_monitor.try_record_nonblocking(unix_ms);
    }

    fn record_blocking_get_observed(&self, unix_ms: i64) {
        self.nonblocking_monitor.try_record_blocking(unix_ms);
    }

    /// Sync the consumer membership metadata in etcd with the given kvclient
    /// sub-cluster.
    ///
    /// This method updates the consumer membership value
    /// (`/channels/{chan}/consumer/consumer_{idx}`) only when the provided
    /// sub-cluster differs from the last successfully written value.
    pub async fn sync_kvclient_sub_cluster(
        &mut self,
        kvclient_sub_cluster: Option<String>,
    ) -> Result<(), MpscError> {
        if let Some(sc) = kvclient_sub_cluster.as_deref() {
            if sc.trim().is_empty() {
                return Err(MpscError::Internal(
                    "kvclient_sub_cluster must be a non-empty string when provided".to_string(),
                ));
            }
            if sc != sc.trim() {
                return Err(MpscError::Internal(
                    "kvclient_sub_cluster must not have leading/trailing whitespace".to_string(),
                ));
            }
        }

        if self.kvclient_sub_cluster == kvclient_sub_cluster {
            return Ok(());
        }

        let member_meta = ChanMemberMeta {
            member_id: self.consumer_idx.clone(),
            role: ChanRole::Consumer,
            external_client_id: self.external_client_id.clone(),
            kvclient_sub_cluster: kvclient_sub_cluster.clone(),
        };
        let meta_bytes = serde_json::to_vec(&member_meta)
            .map_err(|e| MpscError::Internal(format!("serialize ChanMemberMeta failed: {}", e)))?;

        let member_lease_id = self.chan_mgr.member_lease_id();
        let consumer_key = keys::etcd_consumer_key(self.chan_id, &self.consumer_idx);
        let compare =
            etcd::Compare::create_revision(consumer_key.clone(), etcd::CompareOp::Greater, 0);
        let put_op = etcd::TxnOp::put(
            consumer_key.clone(),
            meta_bytes,
            Some(etcd::PutOptions::new().with_lease(member_lease_id)),
        );
        let txn = etcd::Txn::new().when(vec![compare]).and_then(vec![put_op]);
        let mut client = self.chan_mgr.etcd_client();
        let txn_res = client.txn(txn).await?;
        if !txn_res.succeeded() {
            return Err(MpscError::Internal(format!(
                "consumer membership key {} missing while syncing kvclient_sub_cluster",
                consumer_key
            )));
        }

        self.kvclient_sub_cluster = kvclient_sub_cluster;
        Ok(())
    }

    /// Set the global payload callback for this consumer.
    ///
    /// The callback is reused by the prefetch/consume path.
    ///
    /// Contract:
    /// - callback returns a future that resolves to `PayloadResult`
    ///   (`Ok(payload)` / `Retryable(msg)` / `NonRetryable(msg)`).
    /// - the future must be safe to poll on a Tokio runtime thread; if it needs
    ///   to call into Python or do other blocking work, it must offload via
    ///   `spawn_blocking` inside the callback implementation.
    ///
    /// This method is synchronous and only pushes a control command to the
    /// internal actor via `try_send`.
    pub fn set_payload_callback(&mut self, cb: PayloadCallback) {
        self.payload_cb = Some(cb.clone());
        let _ = self.cmd_tx.try_send(ConsumerCmd::SetCallback(cb));
    }

    /// Set the async delete callback used after successful consume-offset commit.
    pub fn set_delete_callback(&mut self, cb: DeleteCallback) {
        self.delete_cb = Some(cb);
    }

    /// 高层 get 接口：通过共享队列获取一条已预取的 future，
    /// 并等待其完成。
    pub async fn get_with_payload(
        &mut self,
        prefetch_target: usize,
    ) -> Result<ConsumedPayload, MpscError> {
        self.get_with_payload_wait_timeout(prefetch_target, None)
            .await
    }

    /// Same as `get_with_payload`, but allows returning `NoMessage` if no inflight
    /// job is available within `wait_timeout`.
    ///
    /// Important: the timeout only applies to waiting for an inflight slot to
    /// become available. Once a message is reserved (i.e. a JoinHandle is popped),
    /// this call will await it to completion to avoid dropping in-flight fetches
    /// and stranding offsets.
    pub async fn get_with_payload_wait_timeout(
        &mut self,
        prefetch_target: usize,
        wait_timeout: Option<Duration>,
    ) -> Result<ConsumedPayload, MpscError> {
        use tokio::time::{sleep, timeout};

        let target = prefetch_target.max(1);
        self.target_inflight.store(target, Ordering::Relaxed);

        let get_handle_begin = Instant::now();
        let get_handle_begin_unix_ms = Self::now_ms();

        let handle_opt = if let Some(dur) = wait_timeout {
            match timeout(dur, self.recv_next_inflight_handle_with_idle_warn()).await {
                Ok(v) => v,
                Err(_) => {
                    self.record_blocking_get_observed(get_handle_begin_unix_ms);
                    return Err(MpscError::NoMessage);
                }
            }
        } else {
            self.recv_next_inflight_handle_with_idle_warn().await
        };
        let inflight_item = handle_opt.ok_or(MpscError::Closed)?;
        debug!(
            "[MpscConsumer get_with_payload] instance_id={} chan_id={} seq={} producer_id={} consume_offset={} inflight_queue_size_after_pop={}",
            self.instance_id,
            self.chan_id,
            inflight_item.seq,
            inflight_item.producer_id,
            inflight_item.consume_offset,
            self.inflight_queue_size.load(Ordering::SeqCst),
        );
        let popped_at = Instant::now();
        let ready_path_sample = if let Some(ready_path_trace) = inflight_item.ready_path_trace {
            let sample = ReadyPathLatencySample {
                traced_produce_offset: ready_path_trace.traced_produce_offset,
                watch_observed_to_actor_update_ns: ready_path_trace
                    .actor_update_at
                    .duration_since(ready_path_trace.watch_observed_at)
                    .as_nanos(),
                watch_send_to_actor_update_ns: ready_path_trace
                    .actor_update_at
                    .duration_since(ready_path_trace.watch_send_begin_at)
                    .as_nanos(),
                actor_update_to_select_ns: ready_path_trace
                    .selected_at
                    .duration_since(ready_path_trace.actor_update_at)
                    .as_nanos(),
                select_to_pop_ns: popped_at
                    .duration_since(ready_path_trace.selected_at)
                    .as_nanos(),
                actor_update_to_pop_ns: popped_at
                    .duration_since(ready_path_trace.actor_update_at)
                    .as_nanos(),
                watch_observed_to_pop_ns: popped_at
                    .duration_since(ready_path_trace.watch_observed_at)
                    .as_nanos(),
            };
            self.ready_path_watch_observed_to_actor_update_window
                .push(sample.watch_observed_to_actor_update_ns);
            self.ready_path_watch_send_to_actor_update_window
                .push(sample.watch_send_to_actor_update_ns);
            self.ready_path_actor_update_to_select_window
                .push(sample.actor_update_to_select_ns);
            self.ready_path_select_to_pop_window
                .push(sample.select_to_pop_ns);
            self.ready_path_actor_update_to_pop_window
                .push(sample.actor_update_to_pop_ns);
            self.ready_path_watch_observed_to_pop_window
                .push(sample.watch_observed_to_pop_ns);
            debug!(
                "[MpscConsumer ready_path] instance_id={} chan_id={} seq={} producer_id={} consume_offset={} traced_produce_offset={} watch_observed_to_actor_update_ms={} watch_send_to_actor_update_ms={} actor_update_to_select_ms={} select_to_pop_ms={} actor_update_to_pop_ms={} watch_observed_to_pop_ms={}",
                self.instance_id,
                self.chan_id,
                inflight_item.seq,
                inflight_item.producer_id,
                inflight_item.consume_offset,
                sample.traced_produce_offset,
                sample.watch_observed_to_actor_update_ns / 1_000_000,
                sample.watch_send_to_actor_update_ns / 1_000_000,
                sample.actor_update_to_select_ns / 1_000_000,
                sample.select_to_pop_ns / 1_000_000,
                sample.actor_update_to_pop_ns / 1_000_000,
                sample.watch_observed_to_pop_ns / 1_000_000,
            );
            Some(sample)
        } else {
            self.ready_path_trace_missing_total += 1;
            None
        };

        let get_handle_duration = get_handle_begin.elapsed();
        let latest_get_handle_ns = get_handle_duration.as_nanos();
        let nonblocking_hit = get_handle_duration <= NONBLOCKING_QUEUE_WAIT_THRESHOLD;
        let get_handle_end_unix_ms =
            get_handle_begin_unix_ms + (get_handle_duration.as_millis() as i64);
        self.prefetch_latency_get_handle_window
            .push(latest_get_handle_ns);

        self.inflight_queue_size.fetch_sub(1, Ordering::SeqCst);
        self.inflight_consume_notify.notify_one();
        self.commit_seq.mark_popped(inflight_item.seq);

        let handle_await_begin = Instant::now();
        let mut handle = inflight_item.rx;
        let fetched_res = loop {
            tokio::select! {
                res = &mut handle => {
                    break res.map_err(|_| {
                        MpscError::Internal(format!(
                            "prefetch job dropped before sending result: seq={} producer_id={} consume_offset={}",
                            inflight_item.seq,
                            inflight_item.producer_id,
                            inflight_item.consume_offset,
                        ))
                    });
                }
                _ = tokio::time::sleep(PREFETCH_HANDLE_AWAIT_WARN_INTERVAL) => {
                    warn!(
                        "[MpscConsumer get_with_payload] instance_id={} still awaiting prefetched result: chan_id={} seq={} producer_id={} consume_offset={} waited_ms={}",
                        self.instance_id,
                        self.chan_id,
                        inflight_item.seq,
                        inflight_item.producer_id,
                        inflight_item.consume_offset,
                        handle_await_begin.elapsed().as_millis(),
                    );
                }
            }
        }?;
        let latest_handle_await_ns = handle_await_begin.elapsed().as_nanos();
        self.prefetch_latency_handle_await_window
            .push(latest_handle_await_ns);
        let fetched = fetched_res?;
        let latest_kv_get_ns = fetched.kv_get_latency_ns;
        self.prefetch_latency_kv_get_window.push(latest_kv_get_ns);
        let latest_commit_wait_ns = fetched.commit_wait_latency_ns;
        self.prefetch_latency_commit_wait_window
            .push(latest_commit_wait_ns);
        self.prefetch_latency_commit_wait_blocked_payload_window
            .push(fetched.commit_wait_breakdown.blocked_on_payload_ns);
        self.prefetch_latency_commit_wait_blocked_wait_turn_window
            .push(fetched.commit_wait_breakdown.blocked_on_wait_turn_ns);
        self.prefetch_latency_commit_wait_blocked_commit_window
            .push(fetched.commit_wait_breakdown.blocked_on_commit_ns);
        self.prefetch_latency_commit_wait_blocked_ready_queue_window
            .push(fetched.commit_wait_breakdown.blocked_on_ready_queue_ns);
        self.prefetch_latency_commit_wait_blocked_popped_to_advance_window
            .push(
                fetched
                    .commit_wait_breakdown
                    .blocked_on_popped_to_advance_ns,
            );
        self.prefetch_latency_commit_wait_blocked_notify_gap_window
            .push(fetched.commit_wait_breakdown.blocked_on_notify_gap_ns);
        debug!(
            "[MpscConsumer get_with_payload] instance_id={} prefetched result resolved: chan_id={} seq={} producer_id={} consume_offset={} awaited_ms={}",
            self.instance_id,
            self.chan_id,
            inflight_item.seq,
            inflight_item.producer_id,
            inflight_item.consume_offset,
            handle_await_begin.elapsed().as_millis(),
        );

        let latest_etcd_put_ns = fetched.etcd_put_latency_ns;
        self.prefetch_latency_etcd_put_window
            .push(latest_etcd_put_ns);
        let latest_etcd_put_first_poll_delay_ns = fetched.etcd_put_first_poll_delay_ns;
        self.prefetch_latency_etcd_put_first_poll_delay_window
            .push(latest_etcd_put_first_poll_delay_ns);
        let latest_etcd_put_first_poll_to_ready_ns = fetched.etcd_put_first_poll_to_ready_ns;
        self.prefetch_latency_etcd_put_first_poll_to_ready_window
            .push(latest_etcd_put_first_poll_to_ready_ns);
        if nonblocking_hit {
            self.record_nonblocking_get_success(get_handle_end_unix_ms);
        } else {
            self.record_blocking_get_observed(get_handle_begin_unix_ms);
        }

        let parent_mpmc_id = match self.category {
            MqCategory::MpmcSub { parent_mpmc_id } => Some(parent_mpmc_id),
            MqCategory::Mpsc => None,
        };

        // After successful consume-offset commit, attempt to delete the payload key if callback is provided.
        let mut latest_delete_ns: u128 = 0;
        if let Some(del_cb) = self.delete_cb.clone() {
            let delete_total_begin = Instant::now();
            let msg_key = keys::backend_message_key_with_category(
                self.chan_id,
                &fetched.producer_id,
                fetched.consume_offset,
                &self.category,
            );
            loop {
                if self.shutdown.is_closed() {
                    break;
                }
                let f = del_cb.clone();
                let key_clone = msg_key.clone();
                let delete_begin = Instant::now();
                let delete_fut = (f)(key_clone.clone());
                tokio::pin!(delete_fut);
                let res = loop {
                    tokio::select! {
                        biased;
                        _ = self.shutdown.wait_closed() => {
                            // Delete is best-effort cleanup after consume-offset commit.
                            // Once shutdown is observed, keep the committed consume state
                            // and stop waiting/retrying local delete work so close latency
                            // is bounded by the authoritative commit path instead of
                            // payload cleanup tail.
                            debug!(
                                "[MpscConsumer chan_id={}] stop delete callback on shutdown: key={}",
                                self.chan_id,
                                key_clone,
                            );
                            break DeleteResult::Ok;
                        }
                        res = &mut delete_fut => {
                            break res;
                        }
                        _ = sleep(DELETE_CALLBACK_WARN_INTERVAL) => {
                            warn!(
                                "[MpscConsumer chan_id={}] delete callback still pending: key={} waited_ms={}",
                                self.chan_id,
                                key_clone,
                                delete_begin.elapsed().as_millis(),
                            );
                        }
                    }
                };
                match res {
                    DeleteResult::Ok => break,
                    DeleteResult::Retryable(msg) => {
                        warn!(
                            "[MpscConsumer chan_id={}] delete payload retryable: {}",
                            self.chan_id, msg
                        );
                        sleep(std::time::Duration::from_millis(50)).await;
                        continue;
                    }
                    DeleteResult::NonRetryable(msg) => {
                        return Err(MpscError::DeletePayloadNonRetryable { message: msg });
                    }
                }
            }
            latest_delete_ns = delete_total_begin.elapsed().as_nanos();
        }
        self.prefetch_latency_delete_window.push(latest_delete_ns);
        self.maybe_log_prefetch_latency(
            parent_mpmc_id,
            latest_get_handle_ns,
            latest_handle_await_ns,
            latest_kv_get_ns,
            latest_commit_wait_ns,
            fetched.commit_wait_breakdown,
            fetched.commit_wait_blocker_count,
            fetched.commit_wait_summary.as_deref(),
            latest_etcd_put_ns,
            latest_etcd_put_first_poll_delay_ns,
            latest_etcd_put_first_poll_to_ready_ns,
            latest_delete_ns,
            ready_path_sample,
        );

        Ok(ConsumedPayload {
            producer_id: fetched.producer_id,
            payload: fetched.payload,
            nonblocking_hit,
        })
    }

    /// Variant of get_with_payload that treats回调返回码 `1` 为可重试
    /// 错误，并在 actor 内部进行重试。调用方只会看到成功或
    /// 不可恢复错误（包括返回码为 2 的情况）。
    pub async fn get_with_payload_retry(
        &mut self,
        prefetch_target: usize,
    ) -> Result<ConsumedPayload, MpscError> {
        // 底层 prefetch_actor 始终以带重试语义执行 get，
        // 这里直接复用统一实现。
        self.get_with_payload(prefetch_target).await
    }

    pub async fn get_with_payload_retry_wait_timeout(
        &mut self,
        prefetch_target: usize,
        wait_timeout: Duration,
    ) -> Result<ConsumedPayload, MpscError> {
        self.get_with_payload_wait_timeout(prefetch_target, Some(wait_timeout))
            .await
    }

    pub async fn get_with_payload_via_broker(
        &mut self,
        broker: &BrokerHandle,
    ) -> Result<ConsumedPayload, MpscError> {
        let cb = self
            .payload_cb
            .as_ref()
            .ok_or_else(|| MpscError::Internal("payload callback not set".to_string()))?
            .clone();
        get_payload_via_broker(
            broker,
            self.chan_id,
            self.consumer_idx.clone(),
            cb,
            self.delete_cb.clone(),
            self.shutdown.clone(),
        )
        .await
    }

    pub async fn get_batch_with_payload_via_broker(
        &mut self,
        broker: &BrokerHandle,
        batch_size: usize,
    ) -> Result<Vec<ConsumedPayload>, MpscError> {
        let cb = self
            .payload_cb
            .as_ref()
            .ok_or_else(|| MpscError::Internal("payload callback not set".to_string()))?
            .clone();
        get_payload_batch_via_broker(
            broker,
            self.chan_id,
            self.consumer_idx.clone(),
            batch_size,
            cb,
            self.delete_cb.clone(),
            self.shutdown.clone(),
        )
        .await
    }

    /// Runs the KV payload fetch stage with retry semantics.
    /// Consume-offset commit is handled by the prefetch job.
    async fn run_single_get(
        chan_id: i64,
        cb: PayloadCallback,
        producer_id: String,
        consume_offset: i64,
        shutdown: ShutdownCtl,
        category: MqCategory,
    ) -> Result<FetchedPayload, MpscError> {
        use tokio::time::sleep;

        let kv_get_begin = Instant::now();
        let mut payload_obj: Option<Box<dyn MqPayload>> = None;
        loop {
            if shutdown.is_closed() {
                return Err(MpscError::Closed);
            }
            let msg_key = keys::backend_message_key_with_category(
                chan_id,
                &producer_id,
                consume_offset,
                &category,
            );
            let f = cb.clone();
            let producer_for_closure = producer_id.clone();
            let res = (f)(producer_for_closure, msg_key).await;

            match res {
                PayloadResult::Ok(obj) => {
                    payload_obj = Some(obj);
                    break;
                }
                PayloadResult::Retryable(msg) => {
                    warn!(
                        "[MpscConsumer chan_id={}] get payload retryable: {}",
                        chan_id, msg
                    );
                    sleep(Duration::from_millis(50)).await;
                    continue;
                }
                PayloadResult::NonRetryable(msg) => {
                    return Err(MpscError::GetPayloadNonRetryable { message: msg })
                }
            }
        }

        let payload = payload_obj
            .ok_or_else(|| MpscError::Internal("payload missing after success".to_string()))?;
        Ok(FetchedPayload {
            producer_id,
            consume_offset,
            payload,
            kv_get_latency_ns: kv_get_begin.elapsed().as_nanos(),
            commit_wait_latency_ns: 0,
            commit_wait_breakdown: CommitWaitBreakdownNs::default(),
            commit_wait_blocker_count: 0,
            commit_wait_summary: None,
            etcd_put_latency_ns: 0,
            etcd_put_first_poll_delay_ns: 0,
            etcd_put_first_poll_to_ready_ns: 0,
        })
    }

    async fn commit_consume_offset(
        mut client: etcd::Client,
        chan_id: i64,
        global_lease_id: i64,
        producer_id: &str,
        consume_offset: i64,
        seq: usize,
        shutdown: ShutdownCtl,
    ) -> Result<CommitOffsetPutTraceNs, MpscError> {
        use tokio::time::sleep;

        let next_consume_offset = consume_offset + 1;
        let key = keys::etcd_consume_offset_one_producer_key(chan_id, producer_id);
        let next_consume_offset_str = next_consume_offset.to_string();
        let begin = Instant::now();
        let mut attempts: usize = 0;

        loop {
            if shutdown.is_closed() {
                return Err(MpscError::Closed);
            }

            attempts += 1;
            let attempt_begin = Instant::now();
            let put = client.put(
                key.clone(),
                next_consume_offset_str.clone(),
                Some(etcd::PutOptions::new().with_lease(global_lease_id)),
            );
            tokio::pin!(put);
            let mut first_poll_at = None;
            let put_res = tokio::select! {
                biased;
                _ = shutdown.wait_closed() => {
                    return Err(MpscError::Closed);
                }
                res = tokio::time::timeout(
                    COMMIT_OFFSET_PUT_TIMEOUT,
                    poll_fn(|cx| {
                        if first_poll_at.is_none() {
                            first_poll_at = Some(Instant::now());
                        }
                        put.as_mut().poll(cx)
                    }),
                ) => res,
            };

            match put_res {
                Ok(Ok(_)) => {
                    let total_elapsed = begin.elapsed();
                    let attempt_end = Instant::now();
                    let attempt_elapsed = attempt_end.duration_since(attempt_begin);
                    let first_poll_delay_ns = first_poll_at
                        .map(|ts| ts.duration_since(attempt_begin).as_nanos())
                        .unwrap_or_else(|| attempt_elapsed.as_nanos());
                    let first_poll_to_ready_ns = first_poll_at
                        .map(|ts| attempt_end.duration_since(ts).as_nanos())
                        .unwrap_or(0);
                    if attempts > 1 || total_elapsed >= COMMIT_OFFSET_SLOW_WARN_THRESHOLD {
                        warn!(
                            "[MpscConsumer commit] consume-offset committed: chan_id={} seq={} producer_id={} consume_offset={} next_consume_offset={} attempts={} total_ms={} latest_attempt_ms={} latest_first_poll_delay_ms={} latest_first_poll_to_ready_ms={}",
                            chan_id,
                            seq,
                            producer_id,
                            consume_offset,
                            next_consume_offset,
                            attempts,
                            total_elapsed.as_millis(),
                            attempt_elapsed.as_millis(),
                            first_poll_delay_ns / 1_000_000,
                            first_poll_to_ready_ns / 1_000_000,
                        );
                    }
                    return Ok(CommitOffsetPutTraceNs {
                        total_latency_ns: total_elapsed.as_nanos(),
                        first_poll_delay_ns,
                        first_poll_to_ready_ns,
                    });
                }
                Ok(Err(e)) => {
                    warn!(
                        "[MpscConsumer commit] consume-offset put failed: chan_id={} seq={} producer_id={} consume_offset={} next_consume_offset={} attempt={} err={}",
                        chan_id,
                        seq,
                        producer_id,
                        consume_offset,
                        next_consume_offset,
                        attempts,
                        e,
                    );
                }
                Err(_elapsed) => {
                    warn!(
                        "[MpscConsumer commit] consume-offset put timed out: chan_id={} seq={} producer_id={} consume_offset={} next_consume_offset={} attempt={} timeout_ms={}",
                        chan_id,
                        seq,
                        producer_id,
                        consume_offset,
                        next_consume_offset,
                        attempts,
                        COMMIT_OFFSET_PUT_TIMEOUT.as_millis(),
                    );
                }
            }

            tokio::select! {
                biased;
                _ = shutdown.wait_closed() => {
                    return Err(MpscError::Closed);
                }
                _ = sleep(COMMIT_OFFSET_RETRY_SLEEP) => {}
            }
        }
    }

    async fn run_prefetch_kv_then_commit(
        instance_id: usize,
        chan_id: i64,
        cb: PayloadCallback,
        producer_id: String,
        consume_offset: i64,
        shutdown: ShutdownCtl,
        category: MqCategory,
        seq: usize,
        commit_seq: CommitSequencer,
        client: etcd::Client,
        global_lease_id: i64,
    ) -> Result<FetchedPayload, MpscError> {
        let stage = Arc::new(AtomicU8::new(0));
        let done = Arc::new(AtomicBool::new(false));
        let stage_for_watchdog = stage.clone();
        let done_for_watchdog = done.clone();
        let shutdown_for_watchdog = shutdown.clone();
        let producer_id_for_watchdog = producer_id.clone();
        tokio::spawn(async move {
            let watch_begin = Instant::now();
            loop {
                tokio::select! {
                    biased;
                    _ = shutdown_for_watchdog.wait_closed() => return,
                    _ = tokio::time::sleep(PREFETCH_JOB_WARN_INTERVAL) => {}
                }
                if done_for_watchdog.load(Ordering::Relaxed) {
                    return;
                }
                warn!(
                    "[MpscConsumer prefetch_job] instance_id={} still pending: chan_id={} seq={} producer_id={} consume_offset={} stage={} elapsed_ms={}",
                    instance_id,
                    chan_id,
                    seq,
                    producer_id_for_watchdog,
                    consume_offset,
                    prefetch_job_stage_name(stage_for_watchdog.load(Ordering::Relaxed)),
                    watch_begin.elapsed().as_millis(),
                );
            }
        });

        let result = async {
            stage.store(1, Ordering::Relaxed);
            let mut fetched = MpscConsumer::run_single_get(
                chan_id,
                cb,
                producer_id,
                consume_offset,
                shutdown.clone(),
                category,
            )
            .await?;

            stage.store(2, Ordering::Relaxed);
            commit_seq.mark_wait_turn_begin(seq);
            let wait_outcome = commit_seq.wait_turn(seq, &shutdown).await?;
            fetched.commit_wait_latency_ns = wait_outcome.latency_ns;
            fetched.commit_wait_breakdown = wait_outcome.breakdown;
            fetched.commit_wait_blocker_count = wait_outcome.blocker_count;
            fetched.commit_wait_summary = wait_outcome.summary;

            stage.store(3, Ordering::Relaxed);
            commit_seq.mark_commit_begin(seq);
            let put_trace = MpscConsumer::commit_consume_offset(
                client,
                chan_id,
                global_lease_id,
                &fetched.producer_id,
                fetched.consume_offset,
                seq,
                shutdown.clone(),
            )
            .await?;
            fetched.etcd_put_latency_ns = put_trace.total_latency_ns;
            fetched.etcd_put_first_poll_delay_ns = put_trace.first_poll_delay_ns;
            fetched.etcd_put_first_poll_to_ready_ns = put_trace.first_poll_to_ready_ns;

            stage.store(4, Ordering::Relaxed);
            commit_seq.mark_ready_to_advance(seq);
            // Advance immediately after the ordered consume-offset commit so later
            // seqs do not wait for front-end dequeue / payload delivery cadence.
            commit_seq.advance(seq);
            Ok::<FetchedPayload, MpscError>(fetched)
        }
        .await;

        done.store(true, Ordering::Relaxed);
        result
    }
}

async fn get_payload_via_broker(
    broker: &BrokerHandle,
    chan_id: i64,
    consumer_id: String,
    cb: PayloadCallback,
    delete_cb: Option<DeleteCallback>,
    shutdown: ShutdownCtl,
) -> Result<ConsumedPayload, MpscError> {
    let fetched = broker
        .fetch_next(BrokerFetchRequest {
            channel_id: chan_id,
            consumer_id: consumer_id.clone(),
            now_ms: now_ms(),
        })
        .await
        .map_err(|e| {
            MpscError::Internal(format!(
                "broker fetch failed: chan_id={} consumer_id={} err={}",
                chan_id, consumer_id, e
            ))
        })?
        .ok_or(MpscError::NoMessage)?;
    let envelope = fetched.envelope;
    let reservation_id = envelope.reservation_id;
    let producer_id = envelope.producer_id.clone();
    let payload_key = envelope.payload_key.clone();
    let mut requeue_guard =
        BrokerInflightRequeueGuard::new(broker.clone(), chan_id, vec![reservation_id]);
    let payload = match run_payload_callback(
        chan_id,
        cb,
        producer_id.clone(),
        payload_key,
        shutdown.clone(),
    )
    .await
    {
        Ok((payload, _kv_get_latency_ns)) => payload,
        Err(err) => {
            requeue_guard.requeue_now().await;
            return Err(err);
        }
    };

    let commit_outcome = match broker.commit(chan_id, reservation_id, now_ms()).await {
        Ok(outcome) => outcome,
        Err(err) => {
            requeue_guard.requeue_now().await;
            return Err(MpscError::Internal(format!(
                "broker commit failed: chan_id={} consumer_id={} reservation_id={} err={}",
                chan_id, consumer_id, reservation_id, err
            )));
        }
    };
    requeue_guard.mark_completed(reservation_id);
    if !commit_outcome.first_commit {
        return Err(MpscError::Internal(format!(
            "broker commit returned duplicate first_commit=false: chan_id={} consumer_id={} reservation_id={}",
            chan_id, consumer_id, reservation_id
        )));
    }

    if let Some(envelope) = commit_outcome.cleanup {
        spawn_broker_cleanup(broker.clone(), chan_id, delete_cb.clone(), envelope);
    }

    Ok(ConsumedPayload {
        producer_id,
        payload,
        nonblocking_hit: true,
    })
}

struct BrokerBatchPayload {
    producer_id: String,
    payload: Box<dyn MqPayload>,
}

struct BrokerInflightRequeueGuard {
    broker: BrokerHandle,
    chan_id: i64,
    reservation_ids: Vec<u64>,
}

impl BrokerInflightRequeueGuard {
    fn new(broker: BrokerHandle, chan_id: i64, reservation_ids: Vec<u64>) -> Self {
        Self {
            broker,
            chan_id,
            reservation_ids,
        }
    }

    fn extend<I>(&mut self, reservation_ids: I)
    where
        I: IntoIterator<Item = u64>,
    {
        self.reservation_ids.extend(reservation_ids);
    }

    fn mark_completed(&mut self, reservation_id: u64) {
        if let Some(pos) = self
            .reservation_ids
            .iter()
            .position(|current| *current == reservation_id)
        {
            self.reservation_ids.remove(pos);
        }
    }

    async fn requeue_now(&mut self) {
        let reservation_ids = std::mem::take(&mut self.reservation_ids);
        requeue_pending_broker_inflight(&self.broker, self.chan_id, reservation_ids).await;
    }
}

impl Drop for BrokerInflightRequeueGuard {
    fn drop(&mut self) {
        let reservation_ids = std::mem::take(&mut self.reservation_ids);
        if reservation_ids.is_empty() {
            return;
        }
        let broker = self.broker.clone();
        let chan_id = self.chan_id;
        tokio::spawn(async move {
            requeue_pending_broker_inflight(&broker, chan_id, reservation_ids).await;
        });
    }
}

async fn get_payload_batch_via_broker(
    broker: &BrokerHandle,
    chan_id: i64,
    consumer_id: String,
    batch_size: usize,
    cb: PayloadCallback,
    delete_cb: Option<DeleteCallback>,
    shutdown: ShutdownCtl,
) -> Result<Vec<ConsumedPayload>, MpscError> {
    if batch_size == 0 {
        return Ok(Vec::new());
    }

    let first = broker
        .fetch_next(BrokerFetchRequest {
            channel_id: chan_id,
            consumer_id: consumer_id.clone(),
            now_ms: now_ms(),
        })
        .await
        .map_err(|e| {
            MpscError::Internal(format!(
                "broker fetch failed: chan_id={} consumer_id={} err={}",
                chan_id, consumer_id, e
            ))
        })?
        .ok_or(MpscError::NoMessage)?;

    let mut fetched = Vec::with_capacity(batch_size);
    let mut requeue_guard = BrokerInflightRequeueGuard::new(
        broker.clone(),
        chan_id,
        vec![first.envelope.reservation_id],
    );
    fetched.push(first);

    let remaining = batch_size.saturating_sub(1);
    if remaining > 0 {
        let mut more = match broker
            .fetch_batch_available(
                BrokerFetchRequest {
                    channel_id: chan_id,
                    consumer_id: consumer_id.clone(),
                    now_ms: now_ms(),
                },
                remaining,
            )
            .await
        {
            Ok(batch) => {
                requeue_guard.extend(
                    batch
                        .messages
                        .iter()
                        .map(|message| message.envelope.reservation_id),
                );
                batch.messages
            }
            Err(err) => {
                requeue_guard.requeue_now().await;
                return Err(MpscError::Internal(format!(
                    "broker batch fetch failed: chan_id={} consumer_id={} err={}",
                    chan_id, consumer_id, err
                )));
            }
        };
        fetched.append(&mut more);
    }

    match load_broker_payloads_commit_on_ready(
        broker,
        chan_id,
        &consumer_id,
        fetched,
        cb,
        delete_cb,
        shutdown.clone(),
        requeue_guard,
    )
    .await
    {
        Ok(payloads) => Ok(payloads
            .into_iter()
            .map(|item| ConsumedPayload {
                producer_id: item.producer_id,
                payload: item.payload,
                nonblocking_hit: true,
            })
            .collect()),
        Err(err) => Err(err),
    }
}

async fn load_broker_payloads_commit_on_ready(
    broker: &BrokerHandle,
    chan_id: i64,
    consumer_id: &str,
    fetched: Vec<BrokerFetchedMessage>,
    cb: PayloadCallback,
    delete_cb: Option<DeleteCallback>,
    shutdown: ShutdownCtl,
    mut requeue_guard: BrokerInflightRequeueGuard,
) -> Result<Vec<BrokerBatchPayload>, MpscError> {
    let reservation_ids: Vec<u64> = fetched
        .iter()
        .map(|message| message.envelope.reservation_id)
        .collect();
    let mut join_set = JoinSet::new();

    for message in fetched {
        let envelope = message.envelope;
        let reservation_id = envelope.reservation_id;
        let producer_id = envelope.producer_id.clone();
        let payload_key = envelope.payload_key.clone();
        let cb = cb.clone();
        let shutdown = shutdown.clone();
        join_set.spawn(async move {
            let result =
                run_payload_callback(chan_id, cb, producer_id.clone(), payload_key, shutdown)
                    .await
                    .map(|(payload, _kv_get_latency_ns)| BrokerBatchPayload {
                        producer_id,
                        payload,
                    });
            (reservation_id, result)
        });
    }

    let mut payload_results: HashMap<u64, Result<BrokerBatchPayload, MpscError>> =
        HashMap::with_capacity(reservation_ids.len());
    let mut batch_load_failure: Option<MpscError> = None;
    while let Some(join_res) = join_set.join_next().await {
        match join_res {
            Ok((reservation_id, Ok(payload))) => {
                payload_results.insert(reservation_id, Ok(payload));
            }
            Ok((reservation_id, Err(err))) => {
                payload_results.insert(reservation_id, Err(err));
                join_set.abort_all();
                break;
            }
            Err(err) => {
                join_set.abort_all();
                batch_load_failure = Some(MpscError::JoinError(err));
                break;
            }
        }
    }

    let mut committed_payloads = Vec::with_capacity(reservation_ids.len());
    let mut remaining_reservation_ids = Vec::new();
    let mut stop_error = batch_load_failure;
    let mut stop_after_current = stop_error.is_some();

    for reservation_id in reservation_ids {
        if stop_after_current {
            remaining_reservation_ids.push(reservation_id);
            continue;
        }

        let Some(payload_result) = payload_results.remove(&reservation_id) else {
            stop_error = Some(MpscError::Internal(format!(
                "broker batch payload load canceled before ordered commit: chan_id={} consumer_id={} reservation_id={}",
                chan_id, consumer_id, reservation_id
            )));
            stop_after_current = true;
            remaining_reservation_ids.push(reservation_id);
            continue;
        };

        let payload = match payload_result {
            Ok(payload) => payload,
            Err(err) => {
                stop_error = Some(err);
                stop_after_current = true;
                remaining_reservation_ids.push(reservation_id);
                continue;
            }
        };

        let commit_outcome = match broker.commit(chan_id, reservation_id, now_ms()).await {
            Ok(outcome) => outcome,
            Err(err) => {
                stop_error = Some(MpscError::Internal(format!(
                    "broker commit failed during batch consume: chan_id={} consumer_id={} reservation_id={} err={}",
                    chan_id, consumer_id, reservation_id, err
                )));
                stop_after_current = true;
                remaining_reservation_ids.push(reservation_id);
                continue;
            }
        };
        requeue_guard.mark_completed(reservation_id);
        if !commit_outcome.first_commit {
            stop_error = Some(MpscError::Internal(format!(
                "broker commit returned duplicate during batch consume: chan_id={} consumer_id={} reservation_id={}",
                chan_id, consumer_id, reservation_id
            )));
            stop_after_current = true;
            remaining_reservation_ids.push(reservation_id);
            continue;
        }
        if let Some(envelope) = commit_outcome.cleanup {
            spawn_broker_cleanup(broker.clone(), chan_id, delete_cb.clone(), envelope);
        }

        committed_payloads.push(payload);
    }

    if !remaining_reservation_ids.is_empty() {
        requeue_guard.requeue_now().await;
    }

    if !committed_payloads.is_empty() {
        return Ok(committed_payloads);
    }

    Err(stop_error.unwrap_or_else(|| {
        MpscError::Internal(format!(
            "broker batch consume stopped without committed payloads: chan_id={} consumer_id={}",
            chan_id, consumer_id
        ))
    }))
}

async fn run_payload_callback(
    chan_id: i64,
    cb: PayloadCallback,
    producer_id: String,
    payload_key: String,
    shutdown: ShutdownCtl,
) -> Result<(Box<dyn MqPayload>, u128), MpscError> {
    use tokio::time::sleep;

    let kv_get_begin = Instant::now();
    loop {
        if shutdown.is_closed() {
            return Err(MpscError::Closed);
        }
        let f = cb.clone();
        let producer_for_closure = producer_id.clone();
        let key_for_closure = payload_key.clone();
        let res = (f)(producer_for_closure, key_for_closure).await;

        match res {
            PayloadResult::Ok(payload) => {
                return Ok((payload, kv_get_begin.elapsed().as_nanos()));
            }
            PayloadResult::Retryable(msg) => {
                warn!(
                    "[MpscConsumer chan_id={}] get payload retryable: {}",
                    chan_id, msg
                );
                sleep(Duration::from_millis(50)).await;
            }
            PayloadResult::NonRetryable(msg) => {
                return Err(MpscError::GetPayloadNonRetryable { message: msg });
            }
        }
    }
}

async fn run_delete_callback_until_success(
    chan_id: i64,
    delete_cb: &DeleteCallback,
    payload_key: String,
) {
    use tokio::time::sleep;

    let mut retry_sleep = BROKER_CLEANUP_DELETE_RETRY_INITIAL_SLEEP;
    loop {
        let f = delete_cb.clone();
        let key_clone = payload_key.clone();
        let delete_begin = Instant::now();
        let delete_fut = (f)(key_clone.clone());
        tokio::pin!(delete_fut);
        let res = loop {
            tokio::select! {
                res = &mut delete_fut => {
                    break res;
                }
                _ = sleep(DELETE_CALLBACK_WARN_INTERVAL) => {
                    warn!(
                        "[MpscConsumer chan_id={}] async broker delete callback still pending: key={} waited_ms={}",
                        chan_id,
                        key_clone,
                        delete_begin.elapsed().as_millis(),
                    );
                }
            }
        };
        match res {
            DeleteResult::Ok => return,
            DeleteResult::Retryable(msg) => {
                warn!(
                    "[MpscConsumer chan_id={}] async broker delete payload retryable; retry_after_ms={}: {}",
                    chan_id,
                    retry_sleep.as_millis(),
                    msg
                );
            }
            DeleteResult::NonRetryable(msg) => {
                warn!(
                    "[MpscConsumer chan_id={}] async broker delete payload non-retryable; keep retrying to preserve broker byte budget; retry_after_ms={}: {}",
                    chan_id,
                    retry_sleep.as_millis(),
                    msg
                );
            }
        }
        sleep(retry_sleep).await;
        retry_sleep = retry_sleep
            .saturating_mul(2)
            .min(BROKER_CLEANUP_DELETE_RETRY_MAX_SLEEP);
    }
}

async fn run_broker_cleanup_ack_until_success(
    broker: BrokerHandle,
    chan_id: i64,
    reservation_id: u64,
) {
    use tokio::time::sleep;

    let mut retry_sleep = BROKER_CLEANUP_ACK_RETRY_INITIAL_SLEEP;
    loop {
        match broker.cleanup_ack(chan_id, reservation_id).await {
            Ok(()) => return,
            Err(err) => {
                if broker_cleanup_ack_error_is_terminal(&err) {
                    warn!(
                        "async broker cleanup ack stopped after terminal broker error: chan_id={} reservation_id={} err={}",
                        chan_id,
                        reservation_id,
                        err
                    );
                    return;
                }
                warn!(
                    "async broker cleanup ack failed; retry_after_ms={}: chan_id={} reservation_id={} err={}",
                    retry_sleep.as_millis(),
                    chan_id,
                    reservation_id,
                    err
                );
            }
        }
        sleep(retry_sleep).await;
        retry_sleep = retry_sleep
            .saturating_mul(2)
            .min(BROKER_CLEANUP_ACK_RETRY_MAX_SLEEP);
    }
}

fn broker_cleanup_ack_error_is_terminal(err: &crate::BrokerError) -> bool {
    match err {
        crate::BrokerError::ActorClosed | crate::BrokerError::ChannelNotFound(_) => true,
        crate::BrokerError::Rpc(message) => {
            message.contains("System shutdown")
                || message.contains("actor closed")
                || message.contains("channel not found")
        }
        _ => false,
    }
}

fn spawn_broker_cleanup(
    broker: BrokerHandle,
    chan_id: i64,
    delete_cb: Option<DeleteCallback>,
    envelope: BrokerEnvelope,
) {
    tokio::spawn(async move {
        let reservation_id = envelope.reservation_id;
        if let Some(delete_cb) = delete_cb.as_ref() {
            run_delete_callback_until_success(chan_id, delete_cb, envelope.payload_key.clone())
                .await;
        }
        run_broker_cleanup_ack_until_success(broker, chan_id, reservation_id).await;
    });
}

async fn requeue_pending_broker_inflight(
    broker: &BrokerHandle,
    chan_id: i64,
    reservation_ids: Vec<u64>,
) {
    if reservation_ids.is_empty() {
        return;
    }
    if let Err(err) = broker
        .requeue_inflight_batch(chan_id, reservation_ids)
        .await
    {
        warn!(
            "best-effort broker batch requeue failed: chan_id={} err={}",
            chan_id, err
        );
    }
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock is before UNIX_EPOCH")
        .as_millis() as i64
}

/// MPSC consumer actor，持有 selector、offset、lease 等完整状态。
/// 仅在 mpsc 模块内部可见，对上层 crate 透明。
pub struct ConsumedPayload {
    pub producer_id: String,
    pub payload: Box<dyn MqPayload>,
    pub nonblocking_hit: bool,
}

struct FetchedPayload {
    producer_id: String,
    consume_offset: i64,
    payload: Box<dyn MqPayload>,
    kv_get_latency_ns: u128,
    commit_wait_latency_ns: u128,
    commit_wait_breakdown: CommitWaitBreakdownNs,
    commit_wait_blocker_count: usize,
    commit_wait_summary: Option<String>,
    etcd_put_latency_ns: u128,
    etcd_put_first_poll_delay_ns: u128,
    etcd_put_first_poll_to_ready_ns: u128,
}

struct InflightItem {
    seq: usize,
    producer_id: String,
    consume_offset: i64,
    ready_path_trace: Option<SelectedReadyPathTrace>,
    rx: oneshot::Receiver<Result<FetchedPayload, MpscError>>,
}

struct SingleProducerOffsets {
    produce_offset: i64,
    consume_offset: i64,
}

struct ProducerOffsetUpdate {
    producer_id: String,
    produce_offset: i64,
    watch_observed_at: Instant,
    watch_send_begin_at: Instant,
}

async fn load_producer_meta_watch_snapshot(
    client: &mut etcd::Client,
    chan_id: i64,
    prefix: &str,
) -> Result<HashSet<String>, MpscError> {
    let mut meta_set = HashSet::new();
    scan_etcd_prefix_paginated(client, prefix, |key, _value| {
        match std::str::from_utf8(key) {
            Ok(key_str) => {
                if let Some(idx) = keys::parse_etcd_producer_key(key_str) {
                    meta_set.insert(idx);
                }
            }
            Err(e) => {
                warn!(
                    "[ConsumerActor chan_id={}] invalid utf-8 in producer meta key: {}",
                    chan_id, e
                );
            }
        }
        Ok::<EtcdPrefixScanAction, MpscError>(EtcdPrefixScanAction::Continue)
    })
    .await
    .map_err(map_prefix_scan_error)?;
    Ok(meta_set)
}

async fn refresh_producer_meta_watch_snapshot(
    client: &mut etcd::Client,
    chan_id: i64,
    prefix: &str,
    meta_tx: &mpsc::Sender<HashSet<String>>,
    shutdown: &ShutdownCtl,
) -> EtcdPrefixWatchLoopControl {
    let meta_set = match tokio::select! {
        biased;
        res = load_producer_meta_watch_snapshot(client, chan_id, prefix) => res,
        _ = shutdown.wait_closed() => return EtcdPrefixWatchLoopControl::Stop,
    } {
        Ok(meta_set) => meta_set,
        Err(e) => {
            warn!(
                "[ConsumerActor chan_id={}] failed to refresh producer meta via watch: {:?}",
                chan_id, e
            );
            return EtcdPrefixWatchLoopControl::Continue;
        }
    };

    let send_res = tokio::select! {
        biased;
        res = meta_tx.send(meta_set) => res,
        _ = shutdown.wait_closed() => return EtcdPrefixWatchLoopControl::Stop,
    };
    if send_res.is_err() {
        return EtcdPrefixWatchLoopControl::Stop;
    }

    EtcdPrefixWatchLoopControl::Continue
}

fn parse_produce_offset_watch_events(
    chan_id: i64,
    events: &[OwnedEtcdWatchEvent],
) -> Vec<ProducerOffsetUpdate> {
    let watch_observed_at = Instant::now();
    let mut updates = Vec::with_capacity(events.len());
    for event in events {
        if event.kind == OwnedEtcdWatchEventKind::Delete {
            continue;
        }

        let key_str = match std::str::from_utf8(&event.key) {
            Ok(v) => v,
            Err(e) => {
                warn!(
                    "[ConsumerActor chan_id={}] invalid utf-8 in produce_offset watch key: {}",
                    chan_id, e
                );
                continue;
            }
        };
        let Some(producer_id) = keys::parse_etcd_produce_offset_key(key_str) else {
            warn!(
                "[ConsumerActor chan_id={}] unexpected produce_offset watch key: {}",
                chan_id, key_str
            );
            continue;
        };
        let value_str = match std::str::from_utf8(&event.value) {
            Ok(v) => v,
            Err(e) => {
                warn!(
                    "[ConsumerActor chan_id={}] invalid utf-8 in produce_offset watch value for key {}: {}",
                    chan_id, key_str, e
                );
                continue;
            }
        };
        let produce_offset = match value_str.parse::<i64>() {
            Ok(v) => v,
            Err(e) => {
                warn!(
                    "[ConsumerActor chan_id={}] invalid produce_offset watch value '{}' for key {}: {}",
                    chan_id, value_str, key_str, e
                );
                continue;
            }
        };
        updates.push(ProducerOffsetUpdate {
            producer_id,
            produce_offset,
            watch_observed_at,
            watch_send_begin_at: watch_observed_at,
        });
    }
    updates
}

async fn load_produce_offset_watch_snapshot(
    client: &mut etcd::Client,
    chan_id: i64,
    prefix: &str,
) -> Result<Vec<ProducerOffsetUpdate>, MpscError> {
    let watch_observed_at = Instant::now();
    let mut updates = Vec::new();
    scan_etcd_prefix_paginated(client, prefix, |key, value| {
        let key_str = match std::str::from_utf8(key) {
            Ok(v) => v,
            Err(e) => {
                warn!(
                    "[ConsumerActor chan_id={}] invalid utf-8 in produce_offset watch key: {}",
                    chan_id, e
                );
                return Ok::<EtcdPrefixScanAction, MpscError>(EtcdPrefixScanAction::Continue);
            }
        };
        let Some(producer_id) = keys::parse_etcd_produce_offset_key(key_str) else {
            warn!(
                "[ConsumerActor chan_id={}] unexpected produce_offset watch key: {}",
                chan_id, key_str
            );
            return Ok::<EtcdPrefixScanAction, MpscError>(EtcdPrefixScanAction::Continue);
        };
        let value_str = match std::str::from_utf8(value) {
            Ok(v) => v,
            Err(e) => {
                warn!(
                    "[ConsumerActor chan_id={}] invalid utf-8 in produce_offset watch value for key {}: {}",
                    chan_id, key_str, e
                );
                return Ok::<EtcdPrefixScanAction, MpscError>(EtcdPrefixScanAction::Continue);
            }
        };
        let produce_offset = match value_str.parse::<i64>() {
            Ok(v) => v,
            Err(e) => {
                warn!(
                    "[ConsumerActor chan_id={}] invalid produce_offset watch value '{}' for key {}: {}",
                    chan_id, value_str, key_str, e
                );
                return Ok::<EtcdPrefixScanAction, MpscError>(EtcdPrefixScanAction::Continue);
            }
        };
        updates.push(ProducerOffsetUpdate {
            producer_id,
            produce_offset,
            watch_observed_at,
            watch_send_begin_at: watch_observed_at,
        });
        Ok::<EtcdPrefixScanAction, MpscError>(EtcdPrefixScanAction::Continue)
    })
    .await
    .map_err(map_prefix_scan_error)?;
    Ok(updates)
}

async fn send_produce_offset_watch_updates(
    mut updates: Vec<ProducerOffsetUpdate>,
    produce_offset_tx: &mpsc::Sender<Vec<ProducerOffsetUpdate>>,
    shutdown: &ShutdownCtl,
) -> EtcdPrefixWatchLoopControl {
    if updates.is_empty() {
        return EtcdPrefixWatchLoopControl::Continue;
    }
    let watch_send_begin_at = Instant::now();
    for update in updates.iter_mut() {
        update.watch_send_begin_at = watch_send_begin_at;
    }

    let send_res = tokio::select! {
        biased;
        res = produce_offset_tx.send(updates) => res,
        _ = shutdown.wait_closed() => return EtcdPrefixWatchLoopControl::Stop,
    };
    if send_res.is_err() {
        return EtcdPrefixWatchLoopControl::Stop;
    }

    EtcdPrefixWatchLoopControl::Continue
}

async fn refresh_produce_offset_watch_snapshot(
    client: &mut etcd::Client,
    chan_id: i64,
    prefix: &str,
    produce_offset_tx: &mpsc::Sender<Vec<ProducerOffsetUpdate>>,
    shutdown: &ShutdownCtl,
) -> EtcdPrefixWatchLoopControl {
    let updates = match tokio::select! {
        biased;
        res = load_produce_offset_watch_snapshot(client, chan_id, prefix) => res,
        _ = shutdown.wait_closed() => return EtcdPrefixWatchLoopControl::Stop,
    } {
        Ok(updates) => updates,
        Err(e) => {
            warn!(
                "[ConsumerActor chan_id={}] failed to refresh produce_offset via watch: {:?}",
                chan_id, e
            );
            return EtcdPrefixWatchLoopControl::Continue;
        }
    };

    send_produce_offset_watch_updates(updates, produce_offset_tx, shutdown).await
}

struct ConsumerActor {
    chan_id: i64,
    consumer_idx: String,
    instance_id: usize,
    lease_manager: LeaseManager,
    client: etcd::Client,
    producer_selector: ProducerSelectorForConsumer,
    /// payload 回调，由上层通过 ConsumerCmd::SetCallback 设置.
    payload_cb: Option<PayloadCallback>,
    /// 每个 producer 的本地 reservation cursor（下一条待预取 offset）。
    ///
    /// 这个 cursor 可能领先于 etcd consume offset，因为 actor 会在
    /// consume-offset 持久化之前先连续发起多条 prefetch。
    prefetch_offset_map: HashMap<String, i64>,
    /// 本地缓存的 produce offset（来自 etcd），仅在无消息或
    /// 初始化时 refresh；平时 select_next_message 只读该缓存。
    produce_cache: HashMap<String, i64>,
    /// 本地缓存的 consume offset（来自 etcd）。
    consume_cache: HashMap<String, i64>,
    /// 本地缓存的 producer 元数据存在性集合（来自 etcd
    /// `/channels/{chan}/producer/producer_` 前缀）。
    producer_meta_cache: HashSet<String>,
    /// Producers that currently have at least one locally visible message.
    ready_producers: HashSet<String>,
    /// Bounded per-producer ready traces used to explain wake-up latency.
    ready_trace_history: HashMap<String, VecDeque<ProducerReadyPathTrace>>,
    /// Producers that are locally empty and already disappeared from membership.
    ///
    /// These producers are probed on-demand before the actor goes idle so tail
    /// drain still keys off authoritative per-producer offsets instead of a
    /// stale local cache snapshot.
    stale_no_room_producers: HashSet<String>,
    /// 向 consumer 暴露的预取队列 sender。
    ///
    /// 队列元素为一次完整 get 操作的 JoinHandle。
    inflight_queue: Arc<Mutex<VecDeque<InflightItem>>>,
    /// inflight consume notify
    inflight_consume_notify: Arc<Notify>,
    /// 共享的预取窗口目标。
    target_inflight: Arc<AtomicUsize>,
    /// 共享的队列当前大小计数。
    inflight_queue_size: Arc<AtomicUsize>,
    /// Shared shutdown controller.
    shutdown: ShutdownCtl,
    lifecycle: LifecycleView,
    /// MQ category to decide backend key layout
    category: MqCategory,
    prefetch_no_message_next_warn_at: tokio::time::Instant,
    select_next_message_stats: SelectNextMessageStats,
    select_next_message_next_log_at: tokio::time::Instant,
    global_lease_id: i64,
    commit_seq: CommitSequencer,
    next_prefetch_seq: usize,
}

impl ConsumerActor {
    fn handle_cmd_msg(&mut self, cmd: Option<ConsumerCmd>) {
        match cmd {
            Some(ConsumerCmd::SetCallback(cb)) => {
                self.payload_cb = Some(cb);
            }
            None => {
                // Keep prefetching with the last installed callback even if the
                // control sender is dropped by the outer handle.
            }
        }
    }

    fn handle_meta_msg(&mut self, meta: Option<HashSet<String>>) {
        let Some(set) = meta else {
            return;
        };
        let old_meta = std::mem::replace(&mut self.producer_meta_cache, set);
        let mut affected = old_meta;
        affected.extend(self.producer_meta_cache.iter().cloned());
        let mut selector_dirty = false;
        for producer_id in affected {
            selector_dirty |= self.refresh_ready_state_from_local(&producer_id);
        }
        if selector_dirty {
            self.rebuild_ready_selector();
        }
    }

    fn handle_produce_offset_msg(&mut self, updates: Option<Vec<ProducerOffsetUpdate>>) {
        let Some(updates) = updates else {
            return;
        };
        let actor_update_at = Instant::now();
        let mut selector_dirty = false;
        for update in updates {
            let producer_id = update.producer_id;
            let ready_trace = ProducerReadyPathTrace {
                produce_offset: update.produce_offset,
                watch_observed_at: update.watch_observed_at,
                watch_send_begin_at: update.watch_send_begin_at,
                actor_update_at,
            };
            let history = self
                .ready_trace_history
                .entry(producer_id.clone())
                .or_insert_with(VecDeque::new);
            history.push_back(ready_trace);
            while history.len() > READY_TRACE_HISTORY_PER_PRODUCER {
                history.pop_front();
            }
            self.produce_cache
                .entry(producer_id.clone())
                .and_modify(|cached| {
                    *cached = (*cached).max(update.produce_offset);
                })
                .or_insert(update.produce_offset);
            selector_dirty |= self.refresh_ready_state_from_local(&producer_id);
        }
        if selector_dirty {
            self.rebuild_ready_selector();
        }
    }

    fn capture_selected_ready_path_trace(
        &mut self,
        producer_id: &str,
        actual_offset: i64,
    ) -> Option<SelectedReadyPathTrace> {
        let traces = self.ready_trace_history.get_mut(producer_id)?;
        while traces
            .front()
            .map(|trace| trace.produce_offset < actual_offset)
            .unwrap_or(false)
        {
            traces.pop_front();
        }
        let trace = traces
            .iter()
            .find(|trace| trace.produce_offset >= actual_offset)
            .copied()?;
        Some(SelectedReadyPathTrace {
            traced_produce_offset: trace.produce_offset,
            watch_observed_at: trace.watch_observed_at,
            watch_send_begin_at: trace.watch_send_begin_at,
            actor_update_at: trace.actor_update_at,
            selected_at: Instant::now(),
        })
    }

    fn cached_consume_offset(&self, producer_id: &str) -> i64 {
        self.consume_cache
            .get(producer_id)
            .copied()
            .unwrap_or(CONSUME_OFFSET_BEGIN)
    }

    fn cached_next_hint(&self, producer_id: &str) -> i64 {
        let committed_next = self.cached_consume_offset(producer_id);
        self.prefetch_offset_map
            .get(producer_id)
            .copied()
            .map(|hint| hint.max(committed_next))
            .unwrap_or(committed_next)
    }

    fn cached_produce_offset(&self, producer_id: &str) -> i64 {
        self.produce_cache
            .get(producer_id)
            .copied()
            .unwrap_or(PRODUCE_OFFSET_BEGIN)
    }

    fn has_local_producer_state(&self, producer_id: &str) -> bool {
        self.produce_cache.contains_key(producer_id)
            || self.consume_cache.contains_key(producer_id)
            || self.prefetch_offset_map.contains_key(producer_id)
    }

    fn producer_has_prefetch_room(&self, producer_id: &str) -> bool {
        let visible_tail = self.cached_produce_offset(producer_id);
        let next_hint = self.cached_next_hint(producer_id);
        next_hint <= visible_tail
    }

    fn refresh_ready_state_from_local(&mut self, producer_id: &str) -> bool {
        let ready_before = self.ready_producers.contains(producer_id);
        let stale_before = self.stale_no_room_producers.contains(producer_id);

        if !self.has_local_producer_state(producer_id) {
            self.ready_producers.remove(producer_id);
            self.stale_no_room_producers.remove(producer_id);
            return ready_before || stale_before;
        }

        let has_room = self.producer_has_prefetch_room(producer_id);
        if has_room {
            self.ready_producers.insert(producer_id.to_string());
            self.stale_no_room_producers.remove(producer_id);
        } else {
            self.ready_producers.remove(producer_id);
            if self.producer_meta_cache.contains(producer_id) {
                self.stale_no_room_producers.remove(producer_id);
            } else {
                self.stale_no_room_producers.insert(producer_id.to_string());
            }
        }

        ready_before != self.ready_producers.contains(producer_id)
            || stale_before != self.stale_no_room_producers.contains(producer_id)
    }

    fn rebuild_ready_selector(&mut self) {
        let active = self.ready_producers.clone();
        let inactive: HashSet<String> = HashSet::new();
        self.producer_selector.update_producers(&active, &inactive);
    }

    fn rebuild_ready_state_from_caches(&mut self) {
        self.ready_producers.clear();
        self.stale_no_room_producers.clear();

        let mut all_known_producers: HashSet<String> = HashSet::new();
        all_known_producers.extend(self.produce_cache.keys().cloned());
        all_known_producers.extend(self.consume_cache.keys().cloned());
        all_known_producers.extend(self.prefetch_offset_map.keys().cloned());
        all_known_producers.extend(self.producer_meta_cache.iter().cloned());

        for producer_id in all_known_producers {
            self.refresh_ready_state_from_local(&producer_id);
        }
        self.rebuild_ready_selector();
    }

    fn maybe_log_select_next_message_stats(&mut self, force: bool) {
        if self.select_next_message_stats.total_attempts == 0
            && self.select_next_message_stats.no_message_backoff_count == 0
        {
            return;
        }

        let now = tokio::time::Instant::now();
        if !force && now < self.select_next_message_next_log_at {
            return;
        }

        let avg_select_ns = self
            .select_next_message_stats
            .total_latency_window
            .avg_ns()
            .unwrap_or(0);
        let avg_refresh_ns = self
            .select_next_message_stats
            .refresh_latency_window
            .avg_ns()
            .unwrap_or(0);
        let avg_probe_ns = self
            .select_next_message_stats
            .probe_latency_window
            .avg_ns()
            .unwrap_or(0);
        let avg_local_ns = avg_select_ns.saturating_sub(avg_refresh_ns + avg_probe_ns);
        let latest_select_ns = self.select_next_message_stats.latest_total_ns;
        let latest_refresh_ns = self.select_next_message_stats.latest_refresh_ns;
        let latest_probe_ns = self.select_next_message_stats.latest_probe_ns;
        let latest_local_ns = latest_select_ns.saturating_sub(latest_refresh_ns + latest_probe_ns);
        let avg_select_ms = avg_select_ns / 1_000_000;
        let avg_refresh_ms = avg_refresh_ns / 1_000_000;
        let avg_probe_ms = avg_probe_ns / 1_000_000;
        let avg_local_ms = avg_local_ns / 1_000_000;
        let latest_select_ms = latest_select_ns / 1_000_000;
        let latest_refresh_ms = latest_refresh_ns / 1_000_000;
        let latest_probe_ms = latest_probe_ns / 1_000_000;
        let latest_local_ms = latest_local_ns / 1_000_000;
        let parent_mpmc_id = match self.category {
            MqCategory::MpmcSub { parent_mpmc_id } => Some(parent_mpmc_id),
            MqCategory::Mpsc => None,
        };

        info!(
            "[ConsumerActor select_next_message parent_mpmc_id={:?} mpsc_id={}] avg_select_ms={} avg_local_ms={} avg_refresh_ms={} avg_probe_ms={} latest_select_ms={} latest_local_ms={} latest_refresh_ms={} latest_probe_ms={} latest_refresh_calls={} latest_probe_calls={} attempts={} success={} no_message={} other_err={} refresh_calls={} probe_calls={} no_message_backoff_hits={} window_cnt={} inflight_queue_size={} target_inflight={} force={}",
            parent_mpmc_id,
            self.chan_id,
            avg_select_ms,
            avg_local_ms,
            avg_refresh_ms,
            avg_probe_ms,
            latest_select_ms,
            latest_local_ms,
            latest_refresh_ms,
            latest_probe_ms,
            self.select_next_message_stats.latest_refresh_call_count,
            self.select_next_message_stats.latest_probe_call_count,
            self.select_next_message_stats.total_attempts,
            self.select_next_message_stats.success_attempts,
            self.select_next_message_stats.no_message_attempts,
            self.select_next_message_stats.error_attempts,
            self.select_next_message_stats.refresh_call_count,
            self.select_next_message_stats.probe_call_count,
            self.select_next_message_stats.no_message_backoff_count,
            self.select_next_message_stats.total_latency_window.len(),
            self.inflight_queue_size.load(Ordering::Relaxed),
            self.target_inflight.load(Ordering::Relaxed),
            force,
        );

        self.select_next_message_next_log_at = now + PREFETCH_LATENCY_LOG_INTERVAL;
    }

    fn drain_pending_actor_inputs(
        &mut self,
        rx: &mut mpsc::Receiver<ConsumerCmd>,
        meta_rx: &mut mpsc::Receiver<HashSet<String>>,
        produce_offset_rx: &mut mpsc::Receiver<Vec<ProducerOffsetUpdate>>,
    ) {
        loop {
            let mut progressed = false;

            loop {
                match rx.try_recv() {
                    Ok(cmd) => {
                        self.handle_cmd_msg(Some(cmd));
                        progressed = true;
                    }
                    Err(tokio::sync::mpsc::error::TryRecvError::Empty) => break,
                    Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => break,
                }
            }

            loop {
                match meta_rx.try_recv() {
                    Ok(meta) => {
                        self.handle_meta_msg(Some(meta));
                        progressed = true;
                    }
                    Err(tokio::sync::mpsc::error::TryRecvError::Empty) => break,
                    Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => break,
                }
            }

            loop {
                match produce_offset_rx.try_recv() {
                    Ok(updates) => {
                        self.handle_produce_offset_msg(Some(updates));
                        progressed = true;
                    }
                    Err(tokio::sync::mpsc::error::TryRecvError::Empty) => break,
                    Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => break,
                }
            }

            if !progressed {
                break;
            }
        }
    }

    async fn wait_actor_inputs_or_timeout(
        &mut self,
        rx: &mut mpsc::Receiver<ConsumerCmd>,
        meta_rx: &mut mpsc::Receiver<HashSet<String>>,
        produce_offset_rx: &mut mpsc::Receiver<Vec<ProducerOffsetUpdate>>,
        duration: Duration,
    ) {
        tokio::select! {
            biased;
            _ = self.shutdown.wait_closed() => {}
            cmd = rx.recv() => {
                self.handle_cmd_msg(cmd);
            }
            meta = meta_rx.recv() => {
                self.handle_meta_msg(meta);
            }
            produce_offset = produce_offset_rx.recv() => {
                self.handle_produce_offset_msg(produce_offset);
            }
            _ = tokio::time::sleep(duration) => {}
        }
    }

    async fn wait_actor_inputs(
        &mut self,
        rx: &mut mpsc::Receiver<ConsumerCmd>,
        meta_rx: &mut mpsc::Receiver<HashSet<String>>,
        produce_offset_rx: &mut mpsc::Receiver<Vec<ProducerOffsetUpdate>>,
    ) {
        tokio::select! {
            biased;
            _ = self.shutdown.wait_closed() => {}
            cmd = rx.recv() => {
                self.handle_cmd_msg(cmd);
            }
            meta = meta_rx.recv() => {
                self.handle_meta_msg(meta);
            }
            produce_offset = produce_offset_rx.recv() => {
                self.handle_produce_offset_msg(produce_offset);
            }
        }
    }

    async fn wait_actor_inputs_or_inflight_consume(
        &mut self,
        rx: &mut mpsc::Receiver<ConsumerCmd>,
        meta_rx: &mut mpsc::Receiver<HashSet<String>>,
        produce_offset_rx: &mut mpsc::Receiver<Vec<ProducerOffsetUpdate>>,
    ) {
        let inflight_consume_notify = self.inflight_consume_notify.clone();
        let notify = inflight_consume_notify.notified();
        tokio::pin!(notify);

        tokio::select! {
            biased;
            _ = self.shutdown.wait_closed() => {}
            cmd = rx.recv() => {
                self.handle_cmd_msg(cmd);
            }
            meta = meta_rx.recv() => {
                self.handle_meta_msg(meta);
            }
            produce_offset = produce_offset_rx.recv() => {
                self.handle_produce_offset_msg(produce_offset);
            }
            _ = &mut notify => {}
        }
    }

    /// 构造并启动一个 consumer-side 预取 actor：
    /// - 启动 actor 主循环（处理 callback 设置 / prefetch tick）。
    /// - 启动 producer member metadata watch（只 watch producer
    ///   member 元数据前缀，不 watch offset）。
    /// 返回给上层：
    /// - 控制通道 sender（用于设置 callback）
    /// - 预取队列 receiver（consumer 从中 pop future 并等待）
    /// - 共享的 target_inflight / inflight_queue_size 计数。
    fn spawn(
        chan_id: i64,
        client: etcd::Client,
        lease_manager: LeaseManager,
        consumer_idx: String,
        lifecycle: LifecycleView,
        shutdown: ShutdownCtl,
        category: MqCategory,
        global_lease_id: i64,
    ) -> (
        mpsc::Sender<ConsumerCmd>,
        Arc<Mutex<VecDeque<InflightItem>>>,
        Arc<AtomicUsize>,
        Arc<AtomicUsize>,
        Arc<Notify>,
        CommitSequencer,
    ) {
        let instance_id = NEXT_CONSUMER_INSTANCE_ID.fetch_add(1, Ordering::Relaxed);
        let producer_selector = ProducerSelectorForConsumer::new(client.clone(), Some(chan_id));
        let (cmd_tx, cmd_rx) = mpsc::channel(8);
        let (meta_tx, meta_rx) = mpsc::channel(8);
        let (produce_offset_tx, produce_offset_rx) = mpsc::channel(128);
        let inflight_queue = Arc::new(Mutex::new(VecDeque::new()));
        let target_inflight = Arc::new(AtomicUsize::new(0));
        let inflight_queue_size = Arc::new(AtomicUsize::new(0));
        let inflight_consume_notify = Arc::new(Notify::new());
        let commit_seq = CommitSequencer::new(instance_id);
        let now = tokio::time::Instant::now();

        let actor = ConsumerActor {
            chan_id,
            consumer_idx,
            instance_id,
            lease_manager: lease_manager.clone(),
            client: client.clone(),
            producer_selector,
            payload_cb: None,
            prefetch_offset_map: HashMap::new(),
            produce_cache: HashMap::new(),
            consume_cache: HashMap::new(),
            producer_meta_cache: HashSet::new(),
            ready_producers: HashSet::new(),
            ready_trace_history: HashMap::new(),
            stale_no_room_producers: HashSet::new(),
            inflight_queue: inflight_queue.clone(),
            inflight_consume_notify: inflight_consume_notify.clone(),
            target_inflight: target_inflight.clone(),
            inflight_queue_size: inflight_queue_size.clone(),
            shutdown: shutdown.clone(),
            lifecycle: lifecycle.clone(),
            category,
            prefetch_no_message_next_warn_at: now + NO_MESSAGE_WARN_INTERVAL,
            select_next_message_stats: SelectNextMessageStats::new(),
            select_next_message_next_log_at: now + PREFETCH_LATENCY_LOG_INTERVAL,
            global_lease_id,
            commit_seq: commit_seq.clone(),
            next_prefetch_seq: 0,
        };

        // 启动 consumer actor 主循环。
        spawn_named(
            &lifecycle,
            format!("fluxon_mq:consumer:actor_main:chan_id={}", chan_id),
            async move {
                actor.run(cmd_rx, meta_rx, produce_offset_rx).await;
                println!(
                    "[ConsumerActor instance_id={} chan_id={}] exiting main loop",
                    instance_id, chan_id
                );
            },
        );

        // 启动 producer member metadata watch：仅 watch
        // `/channels/{chan}/producer/producer_` 前缀，用于感知
        // producer 的 active/inactive 状态。offset 仍然通过
        // refresh_offsets_from_etcd 按需拉取。
        ConsumerActor::spawn_meta_watch(
            client.clone(),
            chan_id,
            meta_tx,
            lifecycle.clone(),
            shutdown.clone(),
        );
        ConsumerActor::spawn_produce_offset_watch(
            client,
            chan_id,
            produce_offset_tx,
            lifecycle,
            shutdown,
        );

        (
            cmd_tx,
            inflight_queue,
            target_inflight,
            inflight_queue_size,
            inflight_consume_notify,
            commit_seq,
        )
    }

    fn spawn_meta_watch(
        client: etcd::Client,
        chan_id: i64,
        meta_tx: mpsc::Sender<HashSet<String>>,
        lifecycle: LifecycleView,
        shutdown: ShutdownCtl,
    ) {
        spawn_named(
            &lifecycle,
            format!("fluxon_mq:consumer:producer_meta_watch:chan_id={}", chan_id),
            async move {
                let prefix = keys::etcd_producer_key_prefix(chan_id);
                let opts = etcd::WatchOptions::new().with_prefix();
                let watch_label =
                    format!("[ConsumerActor chan_id={}] producer meta watch", chan_id);
                let stop = shutdown.clone();
                let resync_shutdown = shutdown.clone();
                let batch_shutdown = shutdown.clone();
                let resync_client = client.clone();
                let batch_client = client.clone();
                let resync_prefix = prefix.clone();
                let batch_prefix = prefix.clone();
                let resync_meta_tx = meta_tx.clone();
                let batch_meta_tx = meta_tx;

                run_prefix_watch_loop(
                    client,
                    prefix,
                    opts,
                    ETCD_PREFIX_WATCH_RESTART_SLEEP,
                    watch_label,
                    stop,
                    move || {
                        let mut refresh_client = resync_client.clone();
                        let prefix = resync_prefix.clone();
                        let meta_tx = resync_meta_tx.clone();
                        let shutdown = resync_shutdown.clone();
                        async move {
                            refresh_producer_meta_watch_snapshot(
                                &mut refresh_client,
                                chan_id,
                                &prefix,
                                &meta_tx,
                                &shutdown,
                            )
                            .await
                        }
                    },
                    move |_events| {
                        let mut refresh_client = batch_client.clone();
                        let prefix = batch_prefix.clone();
                        let meta_tx = batch_meta_tx.clone();
                        let shutdown = batch_shutdown.clone();
                        async move {
                            refresh_producer_meta_watch_snapshot(
                                &mut refresh_client,
                                chan_id,
                                &prefix,
                                &meta_tx,
                                &shutdown,
                            )
                            .await
                        }
                    },
                )
                .await;
            },
        );
    }

    fn spawn_produce_offset_watch(
        client: etcd::Client,
        chan_id: i64,
        produce_offset_tx: mpsc::Sender<Vec<ProducerOffsetUpdate>>,
        lifecycle: LifecycleView,
        shutdown: ShutdownCtl,
    ) {
        spawn_named(
            &lifecycle,
            format!(
                "fluxon_mq:consumer:produce_offset_watch:chan_id={}",
                chan_id
            ),
            async move {
                let prefix = keys::etcd_produce_offset_all_producer_prefix(chan_id);
                let opts = etcd::WatchOptions::new().with_prefix();
                let watch_label =
                    format!("[ConsumerActor chan_id={}] produce_offset watch", chan_id);
                let stop = shutdown.clone();
                let resync_shutdown = shutdown.clone();
                let batch_shutdown = shutdown.clone();
                let resync_client = client.clone();
                let resync_prefix = prefix.clone();
                let resync_tx = produce_offset_tx.clone();
                let batch_tx = produce_offset_tx;

                run_prefix_watch_loop(
                    client,
                    prefix,
                    opts,
                    ETCD_PREFIX_WATCH_RESTART_SLEEP,
                    watch_label,
                    stop,
                    move || {
                        let mut refresh_client = resync_client.clone();
                        let prefix = resync_prefix.clone();
                        let produce_offset_tx = resync_tx.clone();
                        let shutdown = resync_shutdown.clone();
                        async move {
                            refresh_produce_offset_watch_snapshot(
                                &mut refresh_client,
                                chan_id,
                                &prefix,
                                &produce_offset_tx,
                                &shutdown,
                            )
                            .await
                        }
                    },
                    move |events| {
                        let produce_offset_tx = batch_tx.clone();
                        let shutdown = batch_shutdown.clone();
                        async move {
                            let updates = parse_produce_offset_watch_events(chan_id, &events);
                            send_produce_offset_watch_updates(
                                updates,
                                &produce_offset_tx,
                                &shutdown,
                            )
                            .await
                        }
                    },
                )
                .await;
            },
        );
    }

    async fn run(
        mut self,
        mut rx: mpsc::Receiver<ConsumerCmd>,
        mut meta_rx: mpsc::Receiver<HashSet<String>>,
        mut produce_offset_rx: mpsc::Receiver<Vec<ProducerOffsetUpdate>>,
    ) {
        loop {
            if self.shutdown.is_closed() {
                break;
            }

            // Do not poll `prefetch_tick()` as a `tokio::select!` branch. If the
            // branch is canceled while queueing a new inflight item is pending, the
            // oneshot receiver inside `InflightItem` is dropped after the
            // prefetch job has already started, which strands commit ordering.
            self.drain_pending_actor_inputs(&mut rx, &mut meta_rx, &mut produce_offset_rx);
            self.prefetch_tick(&mut rx, &mut meta_rx, &mut produce_offset_rx)
                .await;
        }
        self.maybe_log_select_next_message_stats(true);
    }

    /// 单次 prefetch tick：根据 shared part 中的 target_inflight
    /// 和当前队列大小尝试发起一次预取；若无法预取则适当 sleep。
    async fn prefetch_tick(
        &mut self,
        rx: &mut mpsc::Receiver<ConsumerCmd>,
        meta_rx: &mut mpsc::Receiver<HashSet<String>>,
        produce_offset_rx: &mut mpsc::Receiver<Vec<ProducerOffsetUpdate>>,
    ) {
        if self.shutdown.is_closed() {
            return;
        }

        if self.payload_cb.is_none() {
            self.wait_actor_inputs_or_timeout(
                rx,
                meta_rx,
                produce_offset_rx,
                Duration::from_millis(10),
            )
            .await;
            return;
        }

        let target = self.target_inflight.load(Ordering::Relaxed);
        if target == 0 {
            self.wait_actor_inputs_or_timeout(
                rx,
                meta_rx,
                produce_offset_rx,
                Duration::from_millis(50),
            )
            .await;
            return;
        }

        let initial_queue_size = self.inflight_queue_size.load(Ordering::SeqCst);
        let burst_limit = prefetch_refill_launch_budget(target, initial_queue_size);
        let mut launched = 0usize;
        loop {
            let current = self.inflight_queue_size.load(Ordering::SeqCst);
            if current >= target {
                if launched == 0 {
                    self.wait_actor_inputs_or_inflight_consume(rx, meta_rx, produce_offset_rx)
                        .await;
                }
                return;
            }
            if launched >= burst_limit {
                return;
            }

            match self.try_prefetch_one().await {
                Ok(()) => {
                    launched += 1;
                    self.prefetch_no_message_next_warn_at =
                        tokio::time::Instant::now() + NO_MESSAGE_WARN_INTERVAL;
                    self.maybe_log_select_next_message_stats(false);
                }
                Err(MpscError::NoMessage) => {
                    self.select_next_message_stats.record_no_message_backoff();
                    if launched > 0 {
                        self.maybe_log_select_next_message_stats(false);
                        return;
                    }
                    let now = tokio::time::Instant::now();
                    if now >= self.prefetch_no_message_next_warn_at {
                        let parent_mpmc_id = match self.category {
                            MqCategory::MpmcSub { parent_mpmc_id } => Some(parent_mpmc_id),
                            MqCategory::Mpsc => None,
                        };
                        warn!(
                            "[ConsumerActor instance_id={} parent_mpmc_id={:?} mpsc_id={}] prefetch idle: no new message for {}s",
                            self.instance_id,
                            parent_mpmc_id,
                            self.chan_id,
                            NO_MESSAGE_WARN_INTERVAL.as_secs(),
                        );
                        self.prefetch_no_message_next_warn_at = now + NO_MESSAGE_WARN_INTERVAL;
                    }
                    self.maybe_log_select_next_message_stats(false);
                    self.wait_actor_inputs_or_timeout(
                        rx,
                        meta_rx,
                        produce_offset_rx,
                        prefetch_no_message_retry_sleep(current),
                    )
                    .await;
                    return;
                }
                Err(other) => {
                    self.prefetch_no_message_next_warn_at =
                        tokio::time::Instant::now() + NO_MESSAGE_WARN_INTERVAL;
                    warn!(
                        "[ConsumerActor instance_id={} chan_id={}] try_prefetch_one error: {:?}",
                        self.instance_id, self.chan_id, other
                    );
                    self.maybe_log_select_next_message_stats(false);
                    self.wait_actor_inputs_or_timeout(
                        rx,
                        meta_rx,
                        produce_offset_rx,
                        Duration::from_millis(100),
                    )
                    .await;
                    return;
                }
            }
        }
    }

    /// 尝试预取一条消息：在有新消息的情况下直接发起 future，
    /// 将其放入共享队列；若当前没有任何 producer 有新消息，
    /// 返回 `MpscError::NoMessage`。
    async fn try_prefetch_one(&mut self) -> Result<(), MpscError> {
        if self.shutdown.is_closed() {
            return Err(MpscError::Closed);
        }
        let cb = self
            .payload_cb
            .as_ref()
            .ok_or_else(|| MpscError::Internal("payload callback not set".to_string()))?
            .clone();

        let (producer_id, consume_offset) = self.select_next_message().await?;

        let chan_id = self.chan_id;
        let instance_id = self.instance_id;
        let shutdown = self.shutdown.clone();

        let category = self.category; // Copy (MqCategory is Copy)
        let seq = self.next_prefetch_seq;
        self.next_prefetch_seq += 1;
        let ready_path_trace = self.capture_selected_ready_path_trace(&producer_id, consume_offset);
        let commit_seq = self.commit_seq.clone();
        let client = self.client.clone();
        let global_lease_id = self.global_lease_id;
        let (tx, rx) = oneshot::channel::<Result<FetchedPayload, MpscError>>();
        let lifecycle = self.lifecycle.clone();
        let producer_id_for_name = producer_id.clone();
        let producer_id_for_queue = producer_id.clone();
        self.commit_seq
            .begin_payload(seq, &producer_id_for_queue, consume_offset);
        spawn_named(
            &lifecycle,
            format!(
                "fluxon_mq:consumer:prefetch_job:chan_id={}:seq={}:producer={}",
                chan_id, seq, producer_id_for_name
            ),
            async move {
                let res = MpscConsumer::run_prefetch_kv_then_commit(
                    instance_id,
                    chan_id,
                    cb,
                    producer_id,
                    consume_offset,
                    shutdown,
                    category,
                    seq,
                    commit_seq,
                    client,
                    global_lease_id,
                )
                .await;
                let send_ok = tx.send(res).is_ok();
                if send_ok {
                    debug!(
                        "[MpscConsumer prefetch_job] instance_id={} sent result: chan_id={} seq={} producer_id={} consume_offset={}",
                        instance_id,
                        chan_id,
                        seq,
                        producer_id_for_name,
                        consume_offset,
                    );
                } else {
                    warn!(
                        "[MpscConsumer prefetch_job] instance_id={} receiver dropped before result send: chan_id={} seq={} producer_id={} consume_offset={}",
                        instance_id,
                        chan_id,
                        seq,
                        producer_id_for_name,
                        consume_offset,
                    );
                }
            },
        );

        let queue_size_after_inc = self.inflight_queue_size.fetch_add(1, Ordering::SeqCst) + 1;
        debug!(
            "[MpscConsumer enqueue] instance_id={} chan_id={} seq={} producer_id={} consume_offset={} queue_size_after_inc={} target_inflight={}",
            self.instance_id,
            self.chan_id,
            seq,
            producer_id_for_queue,
            consume_offset,
            queue_size_after_inc,
            self.target_inflight.load(Ordering::SeqCst),
        );
        self.inflight_queue
            .lock()
            .expect("inflight queue mutex poisoned")
            .push_back(InflightItem {
                seq,
                producer_id: producer_id_for_queue,
                consume_offset,
                ready_path_trace,
                rx,
            });
        self.inflight_consume_notify.notify_one();
        debug!(
            "[MpscConsumer enqueue] instance_id={} chan_id={} seq={} queue_send_completed queue_size_now={}",
            self.instance_id,
            self.chan_id,
            seq,
            self.inflight_queue_size.load(Ordering::SeqCst),
        );

        Ok(())
    }

    async fn select_next_message(&mut self) -> Result<(String, i64), MpscError> {
        let select_begin = Instant::now();
        let mut trace = SelectNextMessageTrace::new();

        // Cold start still needs one authoritative snapshot to bootstrap the
        // local ready set before watch-driven updates take over.
        if self.produce_cache.is_empty() && self.consume_cache.is_empty() {
            self.refresh_offsets_from_etcd_timed(&mut trace).await?;
        }

        let result = self.select_next_message_from_cache(&mut trace).await;
        self.select_next_message_stats.record_attempt(
            select_begin.elapsed().as_nanos(),
            &trace,
            &result,
        );
        result
    }

    async fn probe_stale_no_room_producers_timed(
        &mut self,
        trace: &mut SelectNextMessageTrace,
    ) -> Result<(), MpscError> {
        if self.stale_no_room_producers.is_empty() {
            return Ok(());
        }

        let stale_probe_candidates: Vec<String> =
            self.stale_no_room_producers.iter().cloned().collect();
        let stale_probe_count = stale_probe_candidates.len();
        let probe_begin = Instant::now();
        let mut probe_join_set = JoinSet::new();
        for producer_id in stale_probe_candidates {
            let cached_produce_offset = self.cached_produce_offset(&producer_id);
            let cached_consume_offset = self.cached_consume_offset(&producer_id);
            let client = self.client.clone();
            let chan_id = self.chan_id;
            probe_join_set.spawn(async move {
                ConsumerActor::probe_single_producer_offsets_with_cache(
                    client,
                    chan_id,
                    producer_id,
                    cached_produce_offset,
                    cached_consume_offset,
                )
                .await
            });
        }

        let mut probed_offsets: Vec<(String, SingleProducerOffsets)> =
            Vec::with_capacity(stale_probe_count);
        while let Some(join_res) = probe_join_set.join_next().await {
            let probe_res = match join_res {
                Ok(res) => res,
                Err(err) => {
                    probe_join_set.abort_all();
                    trace.probe_latency_ns += probe_begin.elapsed().as_nanos();
                    trace.probe_call_count += stale_probe_count;
                    return Err(MpscError::Internal(format!(
                        "stale producer probe task failed: {}",
                        err
                    )));
                }
            };
            match probe_res {
                Ok(probed) => {
                    probed_offsets.push(probed);
                }
                Err(err) => {
                    probe_join_set.abort_all();
                    trace.probe_latency_ns += probe_begin.elapsed().as_nanos();
                    trace.probe_call_count += stale_probe_count;
                    return Err(err);
                }
            }
        }
        trace.probe_latency_ns += probe_begin.elapsed().as_nanos();
        trace.probe_call_count += stale_probe_count;

        let mut selector_dirty = false;
        for (producer_id, probe) in probed_offsets {
            self.produce_cache
                .insert(producer_id.clone(), probe.produce_offset);
            self.consume_cache
                .insert(producer_id.clone(), probe.consume_offset);

            if probe.produce_offset < self.cached_next_hint(&producer_id)
                && !self.producer_meta_cache.contains(&producer_id)
            {
                let _ = self
                    .producer_selector
                    .try_set_tomb(&producer_id, STALE_PRODUCER_PROBE_TOMB_TTL);
            }
            selector_dirty |= self.refresh_ready_state_from_local(&producer_id);
        }
        if selector_dirty {
            self.rebuild_ready_selector();
        }

        Ok(())
    }

    /// 在本地 ready producer 集合上执行一次选择逻辑。
    ///
    /// 对于“membership 已消失且本地判断已无消息”的 producer，
    /// 仅在 ready 集为空时按需抓取这些 producer 的权威 offsets，
    /// 避免热路径每轮扫描所有 producer。
    async fn select_next_message_from_cache(
        &mut self,
        trace: &mut SelectNextMessageTrace,
    ) -> Result<(String, i64), MpscError> {
        if self.ready_producers.is_empty() {
            self.probe_stale_no_room_producers_timed(trace).await?;
        }

        if self.ready_producers.is_empty() {
            return Err(MpscError::NoMessage);
        }

        let ready_count = self.ready_producers.len();
        for _ in 0..ready_count {
            self.producer_selector.moveon_round_robin();
            let producer_id = self
                .producer_selector
                .current_producer_idx()
                .ok_or(MpscError::NoMessage)?
                .to_string();

            let next_hint = self.cached_next_hint(&producer_id);

            if !self.producer_has_prefetch_room(&producer_id) {
                if self.refresh_ready_state_from_local(&producer_id) {
                    self.rebuild_ready_selector();
                }
                continue;
            }

            let actual_offset = next_hint;
            self.prefetch_offset_map
                .insert(producer_id.clone(), actual_offset + 1);
            if self.refresh_ready_state_from_local(&producer_id) {
                self.rebuild_ready_selector();
            }

            return Ok((producer_id, actual_offset));
        }

        if !self.stale_no_room_producers.is_empty() {
            self.probe_stale_no_room_producers_timed(trace).await?;
            if !self.ready_producers.is_empty() {
                let retry_ready_count = self.ready_producers.len();
                for _ in 0..retry_ready_count {
                    self.producer_selector.moveon_round_robin();
                    let producer_id = self
                        .producer_selector
                        .current_producer_idx()
                        .ok_or(MpscError::NoMessage)?
                        .to_string();

                    let next_hint = self.cached_next_hint(&producer_id);
                    if !self.producer_has_prefetch_room(&producer_id) {
                        if self.refresh_ready_state_from_local(&producer_id) {
                            self.rebuild_ready_selector();
                        }
                        continue;
                    }

                    let actual_offset = next_hint;
                    self.prefetch_offset_map
                        .insert(producer_id.clone(), actual_offset + 1);
                    if self.refresh_ready_state_from_local(&producer_id) {
                        self.rebuild_ready_selector();
                    }
                    return Ok((producer_id, actual_offset));
                }
            }
        }

        Err(MpscError::NoMessage)
    }

    async fn refresh_offsets_from_etcd_timed(
        &mut self,
        trace: &mut SelectNextMessageTrace,
    ) -> Result<(), MpscError> {
        let refresh_begin = Instant::now();
        self.refresh_offsets_from_etcd().await?;
        trace.refresh_latency_ns += refresh_begin.elapsed().as_nanos();
        trace.refresh_call_count += 1;
        Ok(())
    }

    async fn get_single_offset_optional_with_client(
        mut client: etcd::Client,
        key: String,
        offset_name: &'static str,
    ) -> Result<Option<i64>, MpscError> {
        let resp = client.get(key.clone(), None).await?;
        let Some(kv) = resp.kvs().first() else {
            return Ok(None);
        };
        let raw = std::str::from_utf8(kv.value()).map_err(|e| {
            MpscError::Internal(format!(
                "invalid utf-8 value in {} key {}: {}",
                offset_name, key, e
            ))
        })?;
        raw.parse::<i64>().map(Some).map_err(|e| {
            MpscError::Internal(format!(
                "invalid offset '{}' in {} key {}: {}",
                raw, offset_name, key, e
            ))
        })
    }

    async fn probe_single_producer_offsets_with_cache(
        client: etcd::Client,
        chan_id: i64,
        producer_id: String,
        cached_produce_offset: i64,
        cached_consume_offset: i64,
    ) -> Result<(String, SingleProducerOffsets), MpscError> {
        let (produce_offset_opt, consume_offset_opt) = tokio::try_join!(
            ConsumerActor::get_single_offset_optional_with_client(
                client.clone(),
                keys::etcd_produce_offset_one_producer_key(chan_id, &producer_id),
                "produce_offset_of_all_producer",
            ),
            ConsumerActor::get_single_offset_optional_with_client(
                client,
                keys::etcd_consume_offset_one_producer_key(chan_id, &producer_id),
                "consume_offset_of_all_producer",
            ),
        )?;
        let produce_offset = merge_monotonic_offset(cached_produce_offset, produce_offset_opt);
        let consume_offset = merge_monotonic_offset(cached_consume_offset, consume_offset_opt);
        Ok((
            producer_id,
            SingleProducerOffsets {
                produce_offset,
                consume_offset,
            },
        ))
    }

    async fn get_produce_offset_of_all_producer(&self) -> Result<HashMap<String, i64>, MpscError> {
        let mut client = self.client.clone();
        let prefix = keys::etcd_produce_offset_all_producer_prefix(self.chan_id);
        let mut result = HashMap::new();
        scan_etcd_prefix_paginated(&mut client, &prefix, |key, value| {
            let key = std::str::from_utf8(key).map_err(|e| {
                MpscError::Internal(format!("invalid utf-8 key in produce_offset: {}", e))
            })?;
            if let Some(idx) = key.split('/').last() {
                let val_str = std::str::from_utf8(value).map_err(|e| {
                    MpscError::Internal(format!("invalid utf-8 value in produce_offset: {}", e))
                })?;
                let offset: i64 = val_str.parse().map_err(|e| {
                    MpscError::Internal(format!(
                        "invalid offset '{}' in produce_offset: {}",
                        val_str, e
                    ))
                })?;
                result.insert(idx.to_string(), offset);
            }
            Ok::<EtcdPrefixScanAction, MpscError>(EtcdPrefixScanAction::Continue)
        })
        .await
        .map_err(map_prefix_scan_error)?;
        Ok(result)
    }

    async fn get_consume_offset_of_all_producer(&self) -> Result<HashMap<String, i64>, MpscError> {
        let mut client = self.client.clone();
        let prefix = keys::etcd_consume_offset_all_producer_prefix(self.chan_id);
        let mut result = HashMap::new();
        scan_etcd_prefix_paginated(&mut client, &prefix, |key, value| {
            let key = std::str::from_utf8(key).map_err(|e| {
                MpscError::Internal(format!("invalid utf-8 key in consume_offset: {}", e))
            })?;
            if let Some(idx) = key.split('/').last() {
                let val_str = std::str::from_utf8(value).map_err(|e| {
                    MpscError::Internal(format!("invalid utf-8 value in consume_offset: {}", e))
                })?;
                let offset: i64 = val_str.parse().map_err(|e| {
                    MpscError::Internal(format!(
                        "invalid offset '{}' in consume_offset: {}",
                        val_str, e
                    ))
                })?;
                result.insert(idx.to_string(), offset);
            }
            Ok::<EtcdPrefixScanAction, MpscError>(EtcdPrefixScanAction::Continue)
        })
        .await
        .map_err(map_prefix_scan_error)?;
        Ok(result)
    }

    /// 从 etcd 全量刷新一次 produce/consume offset 缓存。冷启动时
    /// 直接建立缓存；热路径 refresh 时只做单调合并，避免缺失 key
    /// 或过旧快照把已经观测过的 offset 回退。
    async fn refresh_offsets_from_etcd(&mut self) -> Result<(), MpscError> {
        let produce_offsets = self.get_produce_offset_of_all_producer().await?;
        let consume_offsets = self.get_consume_offset_of_all_producer().await?;
        merge_offset_cache_monotonic(&mut self.produce_cache, produce_offsets);
        merge_offset_cache_monotonic(&mut self.consume_cache, consume_offsets);

        // 同步刷新 producer 元数据存在性，用于 inactive/tomb 判断。
        let mut client = self.client.clone();
        let prefix = keys::etcd_producer_key_prefix(self.chan_id);
        let mut meta_set = HashSet::new();
        scan_etcd_prefix_paginated(&mut client, &prefix, |key, _value| {
            let key_str = std::str::from_utf8(key).map_err(|e| {
                MpscError::Internal(format!("invalid utf-8 key in producer meta: {}", e))
            })?;
            if let Some(idx) = keys::parse_etcd_producer_key(key_str) {
                meta_set.insert(idx);
            }
            Ok::<EtcdPrefixScanAction, MpscError>(EtcdPrefixScanAction::Continue)
        })
        .await
        .map_err(map_prefix_scan_error)?;
        self.producer_meta_cache = meta_set;
        self.rebuild_ready_state_from_caches();

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{
        get_payload_batch_via_broker, get_payload_via_broker, merge_monotonic_offset,
        merge_offset_cache_monotonic, MqPayload, PayloadCallback, PayloadResult,
    };
    use crate::{
        keys::MqCategory, BrokerChannelConfig, BrokerFetchRequest, BrokerHandle,
        BrokerReserveRequest,
    };
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::sync::Notify;

    struct TestPayload;

    impl MqPayload for TestPayload {}

    #[test]
    fn merge_monotonic_offset_keeps_cached_when_probe_missing() {
        assert_eq!(merge_monotonic_offset(62, None), 62);
    }

    #[test]
    fn merge_monotonic_offset_never_rewinds() {
        assert_eq!(merge_monotonic_offset(62, Some(61)), 62);
        assert_eq!(merge_monotonic_offset(62, Some(63)), 63);
    }

    #[test]
    fn merge_offset_cache_monotonic_preserves_existing_entries_on_hot_refresh() {
        let mut current = HashMap::from([
            ("producer_a".to_string(), 62),
            ("producer_b".to_string(), 41),
        ]);
        let fetched = HashMap::from([
            ("producer_a".to_string(), 61),
            ("producer_c".to_string(), 7),
        ]);

        merge_offset_cache_monotonic(&mut current, fetched);

        assert_eq!(current.get("producer_a"), Some(&62));
        assert_eq!(current.get("producer_b"), Some(&41));
        assert_eq!(current.get("producer_c"), Some(&7));
    }

    #[test]
    fn visible_tail_does_not_allow_prefetch_past_last_published_offset() {
        let visible_tail = 0;
        let next_visible = 0;
        let next_not_yet_published = 1;

        assert!(next_visible <= visible_tail);
        assert!(next_not_yet_published > visible_tail);
    }

    async fn fetch_next_for_test(
        broker: &BrokerHandle,
        channel_id: i64,
        consumer_id: &str,
        now_ms: i64,
    ) -> crate::BrokerFetchedMessage {
        tokio::time::timeout(
            Duration::from_secs(1),
            broker.fetch_next(BrokerFetchRequest {
                channel_id,
                consumer_id: consumer_id.to_string(),
                now_ms,
            }),
        )
        .await
        .expect("timed out waiting for broker redelivery")
        .unwrap()
        .unwrap()
    }

    #[tokio::test]
    async fn broker_single_consume_timeout_requeues_reserved_message() {
        let broker = BrokerHandle::new_local_for_test(32);
        broker
            .upsert_channel(BrokerChannelConfig {
                channel_id: 72,
                capacity: 2,
            })
            .await
            .unwrap();

        let reserved = broker
            .reserve(BrokerReserveRequest {
                channel_id: 72,
                producer_id: "p0".to_string(),
                category: MqCategory::Mpsc,
                payload_bytes: 1,
                now_ms: 10,
            })
            .await
            .unwrap();
        broker
            .publish(72, reserved.envelope.reservation_id, 20)
            .await
            .unwrap();

        let callback_started = Arc::new(Notify::new());
        let cb_started_for_callback = callback_started.clone();
        let cb: PayloadCallback = Arc::new(move |_producer_id: String, _key: String| {
            let cb_started_for_callback = cb_started_for_callback.clone();
            Box::pin(async move {
                cb_started_for_callback.notify_one();
                tokio::time::sleep(Duration::from_millis(50)).await;
                PayloadResult::Ok(Box::new(TestPayload))
            })
        });

        let mut consume = Box::pin(get_payload_via_broker(
            &broker,
            72,
            "c0".to_string(),
            cb,
            None,
            crate::ShutdownCtl::new(),
        ));
        tokio::select! {
            _ = callback_started.notified() => {}
            result = &mut consume => panic!("consume completed before timeout setup: {:?}", result.err()),
        }
        assert!(tokio::time::timeout(Duration::from_millis(5), &mut consume)
            .await
            .is_err());
        drop(consume);

        let redelivered = fetch_next_for_test(&broker, 72, "c1", 30).await;
        assert_eq!(
            redelivered.envelope.reservation_id,
            reserved.envelope.reservation_id
        );
    }

    #[tokio::test]
    async fn broker_batch_consume_timeout_requeues_reserved_messages_in_order() {
        let broker = BrokerHandle::new_local_for_test(32);
        broker
            .upsert_channel(BrokerChannelConfig {
                channel_id: 73,
                capacity: 2,
            })
            .await
            .unwrap();

        let first = broker
            .reserve(BrokerReserveRequest {
                channel_id: 73,
                producer_id: "p0".to_string(),
                category: MqCategory::Mpsc,
                payload_bytes: 1,
                now_ms: 10,
            })
            .await
            .unwrap();
        let second = broker
            .reserve(BrokerReserveRequest {
                channel_id: 73,
                producer_id: "p0".to_string(),
                category: MqCategory::Mpsc,
                payload_bytes: 1,
                now_ms: 11,
            })
            .await
            .unwrap();
        broker
            .publish(73, first.envelope.reservation_id, 20)
            .await
            .unwrap();
        broker
            .publish(73, second.envelope.reservation_id, 21)
            .await
            .unwrap();

        let callback_started = Arc::new(Notify::new());
        let cb_started_for_callback = callback_started.clone();
        let cb: PayloadCallback = Arc::new(move |_producer_id: String, _key: String| {
            let cb_started_for_callback = cb_started_for_callback.clone();
            Box::pin(async move {
                cb_started_for_callback.notify_one();
                tokio::time::sleep(Duration::from_millis(50)).await;
                PayloadResult::Ok(Box::new(TestPayload))
            })
        });

        let mut consume = Box::pin(get_payload_batch_via_broker(
            &broker,
            73,
            "c0".to_string(),
            2,
            cb,
            None,
            crate::ShutdownCtl::new(),
        ));
        tokio::select! {
            _ = callback_started.notified() => {}
            result = &mut consume => panic!("batch consume completed before timeout setup: {:?}", result.err()),
        }
        assert!(tokio::time::timeout(Duration::from_millis(5), &mut consume)
            .await
            .is_err());
        drop(consume);

        let redelivered_first = fetch_next_for_test(&broker, 73, "c1", 30).await;
        let redelivered_second = fetch_next_for_test(&broker, 73, "c1", 31).await;
        assert_eq!(
            redelivered_first.envelope.reservation_id,
            first.envelope.reservation_id
        );
        assert_eq!(
            redelivered_second.envelope.reservation_id,
            second.envelope.reservation_id
        );
    }

    #[tokio::test]
    async fn broker_batch_consume_requeues_without_out_of_order_commit() {
        let broker = BrokerHandle::new_local_for_test(32);
        broker
            .upsert_channel(BrokerChannelConfig {
                channel_id: 71,
                capacity: 2,
            })
            .await
            .unwrap();

        let first = broker
            .reserve(BrokerReserveRequest {
                channel_id: 71,
                producer_id: "p0".to_string(),
                category: MqCategory::Mpsc,
                payload_bytes: 1,
                now_ms: 10,
            })
            .await
            .unwrap();
        let second = broker
            .reserve(BrokerReserveRequest {
                channel_id: 71,
                producer_id: "p0".to_string(),
                category: MqCategory::Mpsc,
                payload_bytes: 1,
                now_ms: 11,
            })
            .await
            .unwrap();
        broker
            .publish(71, first.envelope.reservation_id, 20)
            .await
            .unwrap();
        broker
            .publish(71, second.envelope.reservation_id, 21)
            .await
            .unwrap();

        let first_key = first.envelope.payload_key.clone();
        let cb: PayloadCallback = Arc::new(move |_producer_id: String, key: String| {
            let first_key = first_key.clone();
            Box::pin(async move {
                if key == first_key {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    PayloadResult::NonRetryable("first payload failed".to_string())
                } else {
                    PayloadResult::Ok(Box::new(TestPayload))
                }
            })
        });

        let err = get_payload_batch_via_broker(
            &broker,
            71,
            "c0".to_string(),
            2,
            cb,
            None,
            crate::ShutdownCtl::new(),
        )
        .await
        .err()
        .expect("batch consume should fail when the first payload callback fails");
        assert!(matches!(
            err,
            crate::MpscError::GetPayloadNonRetryable { .. }
        ));

        let redelivered_first = broker
            .fetch_next(crate::BrokerFetchRequest {
                channel_id: 71,
                consumer_id: "c1".to_string(),
                now_ms: 30,
            })
            .await
            .unwrap()
            .unwrap();
        let redelivered_second = broker
            .fetch_next(crate::BrokerFetchRequest {
                channel_id: 71,
                consumer_id: "c1".to_string(),
                now_ms: 31,
            })
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            redelivered_first.envelope.reservation_id,
            first.envelope.reservation_id
        );
        assert_eq!(
            redelivered_second.envelope.reservation_id,
            second.envelope.reservation_id
        );
    }
}

/// Producer selector for consumer-side weighted round robin.
///
/// This mirrors the Python `ProducerSelectorForConsumer` logic:
/// - Maintains a producer list and their weights.
/// - Provides smooth weighted round-robin selection.
/// - Supports tombing producers with optional delayed removal.
pub struct ProducerSelectorForConsumer {
    producers: Vec<String>,
    producer_weight_map: HashMap<String, i64>,
    producer_current_weight_map: HashMap<String, i64>,
    rr_idx: usize,
    tomb_first_ts: HashMap<String, Instant>,
    client: etcd::Client,
    chan_id: Option<i64>,
}

impl ProducerSelectorForConsumer {
    pub fn new(client: etcd::Client, chan_id: Option<i64>) -> Self {
        Self {
            producers: Vec::new(),
            producer_weight_map: HashMap::new(),
            producer_current_weight_map: HashMap::new(),
            rr_idx: 0,
            tomb_first_ts: HashMap::new(),
            client,
            chan_id,
        }
    }

    fn dbg_tag(&self) -> String {
        format!("[ProducerSelector chan_id={:?}]", self.chan_id)
    }

    /// Update a single producer's weight from etcd and reset its
    /// current weight to 0 to avoid old weight affecting new rounds.
    pub async fn update_producer_weight(&mut self, producer_idx: &str) {
        let weight = self.get_producer_weight(producer_idx).await;
        assert!(weight > 0, "Producer weight must be positive");
        self.producer_weight_map
            .insert(producer_idx.to_string(), weight);
        self.producer_current_weight_map
            .insert(producer_idx.to_string(), 0);
    }

    /// Update rr list using membership maps for active/inactive producers.
    /// This keeps existing order where possible and appends new producers
    /// with inactive-first then active order.
    pub fn update_producers(&mut self, active: &HashSet<String>, inactive: &HashSet<String>) {
        let curr = self.current_producer_idx().map(|s| s.to_string());
        let mut new_list = Vec::new();
        let mut included = HashSet::new();
        let mut new_rr_idx: Option<usize> = None;

        // 1) Keep existing order for still-present producers
        for p in &self.producers {
            if inactive.contains(p) || active.contains(p) {
                if !included.contains(p) {
                    if let Some(ref curr_id) = curr {
                        if curr_id == p {
                            new_rr_idx = Some(new_list.len());
                        }
                    }
                    included.insert(p.clone());
                    new_list.push(p.clone());
                }
            }
        }

        // 2) Append remaining inactive-first then active
        for p in inactive {
            if !included.contains(p) {
                included.insert(p.clone());
                new_list.push(p.clone());
            }
        }
        for p in active {
            if !included.contains(p) {
                included.insert(p.clone());
                new_list.push(p.clone());
            }
        }

        self.producers = new_list;
        if let Some(idx) = new_rr_idx {
            self.rr_idx = idx;
        }
    }

    /// Advance to next producer in the rr list using smooth weighted
    /// round-robin.
    pub fn moveon_round_robin(&mut self) {
        if self.producers.is_empty() {
            warn!(
                "{} moveon_round_robin called with empty producer list",
                self.dbg_tag()
            );
            return;
        }

        // Ensure all producers have a weight
        for p in &self.producers {
            self.producer_weight_map.entry(p.clone()).or_insert(1);
            self.producer_current_weight_map
                .entry(p.clone())
                .or_insert(0);
        }

        let total_weight: i64 = self
            .producers
            .iter()
            .map(|p| self.producer_weight_map.get(p).copied().unwrap_or(1))
            .sum();
        if total_weight <= 0 {
            return;
        }

        for p in &self.producers {
            if let Some(w) = self.producer_weight_map.get(p).copied() {
                *self
                    .producer_current_weight_map
                    .entry(p.clone())
                    .or_insert(0) += w;
            }
        }

        // Select producer with maximum current weight
        if let Some(selected) = self.producers.iter().max_by_key(|p| {
            self.producer_current_weight_map
                .get(*p)
                .copied()
                .unwrap_or(0)
        }) {
            if let Some(curr) = self.producer_current_weight_map.get_mut(selected) {
                *curr -= total_weight;
            }
            if let Some(idx) = self.producers.iter().position(|p| p == selected) {
                self.rr_idx = idx;
            }
        }
    }

    /// Immediately tomb a producer from the current rr list; adjust
    /// rr_idx to preserve selection semantics.
    pub fn set_producer_tomb(&mut self, producer_id: &str) {
        if self.producers.is_empty() {
            return;
        }
        if let Some(idx) = self.producers.iter().position(|p| p == producer_id) {
            let n = self.producers.len();
            let curr_mod = if n > 0 { self.rr_idx % n } else { 0 };
            self.producers.remove(idx);
            self.producer_weight_map.remove(producer_id);
            self.producer_current_weight_map.remove(producer_id);
            if self.producers.is_empty() {
                self.rr_idx = 0;
                return;
            }
            if idx < curr_mod {
                self.rr_idx = self.rr_idx.saturating_sub(1);
            }
        }
    }

    /// Delayed tomb: first call records timestamp; subsequent calls
    /// within `ttl` return false; once elapsed, tomb the producer and
    /// return true.
    pub fn try_set_tomb(&mut self, producer_id: &str, ttl: Duration) -> bool {
        if !self.producers.iter().any(|p| p == producer_id) {
            return false;
        }
        let now = Instant::now();
        if let Some(first) = self.tomb_first_ts.get(producer_id).copied() {
            if now.duration_since(first) >= ttl {
                self.set_producer_tomb(producer_id);
                self.tomb_first_ts.remove(producer_id);
                return true;
            }
            false
        } else {
            self.tomb_first_ts.insert(producer_id.to_string(), now);
            false
        }
    }

    /// Get the current producer id based on rr_idx, or None if empty.
    pub fn current_producer_idx(&self) -> Option<&str> {
        if self.producers.is_empty() {
            None
        } else {
            Some(&self.producers[self.rr_idx % self.producers.len()])
        }
    }

    /// Fetch producer weight from etcd; on error or missing value
    /// returns 1.
    async fn get_producer_weight(&mut self, producer_idx: &str) -> i64 {
        let chan_id = match self.chan_id {
            Some(id) => id,
            None => return 1,
        };
        let weight_key = keys::etcd_producer_weight_key(chan_id, producer_idx);
        match self.client.get(weight_key.clone(), None).await {
            Ok(resp) => {
                if let Some(kv) = resp.kvs().first() {
                    if let Ok(txt) = std::str::from_utf8(kv.value()) {
                        if let Ok(w) = txt.parse::<i64>() {
                            debug!(
                                "{} got producer {} weight: {}",
                                self.dbg_tag(),
                                producer_idx,
                                w
                            );
                            return w;
                        }
                    }
                }
                1
            }
            Err(e) => {
                debug!(
                    "{} failed to get producer {} weight: {:?}",
                    self.dbg_tag(),
                    producer_idx,
                    e
                );
                1
            }
        }
    }
}

/// Register a new consumer idx for the given channel using
/// a simple "put if not exists" pattern.
///
/// Mirrors Python `ChanManager.register_consumer_idx`.
async fn register_consumer_idx(
    client: &mut etcd::Client,
    chan_id: i64,
    lease_id: i64,
) -> Result<String, MpscError> {
    use fluxon_util::etcd::DistributeIdAllocator;

    // Reuse the distributed ID allocator with a per-channel prefix,
    // mirroring the producer id allocation logic but scoped to
    // consumers. This avoids the fixed 0..1000 range and keeps
    // allocation monotonic per channel.
    let prefix = format!("channels/{}/consumers", chan_id);
    let allocator = DistributeIdAllocator::new(client.clone(), prefix, lease_id);

    allocator
        .allocate_id()
        .await
        .map(|id| id.to_string())
        .map_err(|e| {
            MpscError::Internal(format!(
                "failed to register consumer idx for chan_id={} via allocator: {}",
                chan_id, e
            ))
        })
}
