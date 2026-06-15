use std::collections::VecDeque;
use std::time::{SystemTime, UNIX_EPOCH};

use tokio::sync::mpsc;

use fluxon_observability::keys::{
    PROM_LABEL_MQ_CATEGORY, PROM_LABEL_MQ_CHAN_ID, PROM_LABEL_MQ_CONSUMER_IDX,
    PROM_LABEL_MQ_METRIC, PROM_LABEL_MQ_PRODUCER_IDX, PROM_LABEL_NODE, PROM_LABEL_ROLE,
    PROM_METRIC_MQ_CONSUMER_NONBLOCKING_LATEST_INTERVAL_UNIX_MS,
    PROM_METRIC_MQ_CONSUMER_NONBLOCKING_LATEST_PHASE_CALLS,
    PROM_METRIC_MQ_CONSUMER_NONBLOCKING_LATEST_PHASE_RPS,
    PROM_METRIC_MQ_PRODUCER_NONBLOCKING_LATEST_INTERVAL_UNIX_MS,
    PROM_METRIC_MQ_PRODUCER_NONBLOCKING_LATEST_PHASE_CALLS,
    PROM_METRIC_MQ_PRODUCER_NONBLOCKING_LATEST_PHASE_RPS, PROM_VALUE_MQ_CATEGORY_MPMC_SUB,
    PROM_VALUE_MQ_CATEGORY_MPSC, PROM_VALUE_MQ_INTERVAL_BEGIN, PROM_VALUE_MQ_INTERVAL_END,
};
use fluxon_observability::metrics_actor::MetricsHandle as ObserveMetricsHandle;
use fluxon_util::prom_remote_write::{Label, Sample, TimeSeries, LABEL_NAME as RW_LABEL_NAME};
use tracing::warn;

use crate::keys::MqCategory;
use crate::lifecycle::spawn_named;
use crate::shutdown::ShutdownCtl;
use crate::LifecycleView;

const EVENT_QUEUE_CAPACITY: usize = 1024;
const LATEST_RATE_WINDOW_MAX_MS: i64 = 30_000;
const LATEST_RATE_BUCKET_MS: i64 = 100;

#[derive(Clone)]
pub struct NonblockingMonitorHandle {
    tx: mpsc::Sender<NonblockingMonitorEvent>,
}

impl NonblockingMonitorHandle {
    pub fn try_record_nonblocking(&self, end_unix_ms: i64) {
        let event = NonblockingMonitorEvent::NonblockingHit { end_unix_ms };
        if let Err(err) = self.tx.try_send(event) {
            warn!("nonblocking monitor dropped nonblocking event: {}", err);
        }
    }

    pub fn try_record_blocking(&self, unix_ms: i64) {
        let event = NonblockingMonitorEvent::BlockingObserved { unix_ms };
        if let Err(err) = self.tx.try_send(event) {
            warn!("nonblocking monitor dropped blocking event: {}", err);
        }
    }
}

enum NonblockingMonitorEvent {
    NonblockingHit { end_unix_ms: i64 },
    BlockingObserved { unix_ms: i64 },
}

struct ActivePhase {
    begin_unix_ms: i64,
    latest_end_unix_ms: i64,
    recent_call_buckets: VecDeque<CallBucket>,
}

struct CallBucket {
    start_unix_ms: i64,
    calls: u64,
}

impl ActivePhase {
    fn new(end_unix_ms: i64) -> Self {
        let mut out = Self {
            begin_unix_ms: end_unix_ms,
            latest_end_unix_ms: end_unix_ms,
            recent_call_buckets: VecDeque::new(),
        };
        out.record_call(end_unix_ms);
        out
    }

    fn record_call(&mut self, end_unix_ms: i64) {
        self.latest_end_unix_ms = end_unix_ms;

        let bucket_start_unix_ms = end_unix_ms - end_unix_ms.rem_euclid(LATEST_RATE_BUCKET_MS);
        match self.recent_call_buckets.back_mut() {
            Some(bucket) if bucket.start_unix_ms == bucket_start_unix_ms => {
                bucket.calls += 1;
            }
            _ => {
                self.recent_call_buckets.push_back(CallBucket {
                    start_unix_ms: bucket_start_unix_ms,
                    calls: 1,
                });
            }
        }
        self.prune_recent_buckets(end_unix_ms);
    }

    fn prune_recent_buckets(&mut self, end_unix_ms: i64) {
        let min_bucket_start_unix_ms =
            end_unix_ms - LATEST_RATE_WINDOW_MAX_MS - LATEST_RATE_BUCKET_MS;
        while self
            .recent_call_buckets
            .front()
            .map(|bucket| bucket.start_unix_ms < min_bucket_start_unix_ms)
            .unwrap_or(false)
        {
            self.recent_call_buckets.pop_front();
        }
    }

    fn latest_window_calls(&self, end_unix_ms: i64, window_ms: i64) -> u64 {
        let window_begin_unix_ms = end_unix_ms - window_ms;
        self.recent_call_buckets
            .iter()
            .filter(|bucket| {
                let bucket_end_unix_ms = bucket.start_unix_ms + LATEST_RATE_BUCKET_MS;
                bucket_end_unix_ms > window_begin_unix_ms
            })
            .map(|bucket| bucket.calls)
            .sum()
    }
}

#[derive(Clone, Copy)]
pub enum NonblockingMonitorKind {
    Producer { chan_id: i64 },
    Consumer { chan_id: i64 },
}

pub fn spawn_nonblocking_monitor(
    lifecycle: &LifecycleView,
    shutdown: ShutdownCtl,
    observe_node_id: String,
    observe_node_role: String,
    observe: ObserveMetricsHandle,
    category: MqCategory,
    kind: NonblockingMonitorKind,
    member_idx: String,
) -> NonblockingMonitorHandle {
    let (tx, rx) = mpsc::channel(EVENT_QUEUE_CAPACITY);
    let actor = NonblockingMonitorActor {
        rx,
        shutdown,
        observe_node_id,
        observe_node_role,
        observe,
        category,
        kind,
        member_idx,
        active_phase: None,
    };
    let actor_name = match kind {
        NonblockingMonitorKind::Producer { chan_id } => format!(
            "fluxon_mq:producer:nonblocking_monitor:chan_id={}:producer_idx={}",
            chan_id, actor.member_idx
        ),
        NonblockingMonitorKind::Consumer { chan_id } => format!(
            "fluxon_mq:consumer:nonblocking_monitor:chan_id={}:consumer_idx={}",
            chan_id, actor.member_idx
        ),
    };
    spawn_named(lifecycle, actor_name, async move {
        actor.run().await;
    });
    NonblockingMonitorHandle { tx }
}

struct NonblockingMonitorActor {
    rx: mpsc::Receiver<NonblockingMonitorEvent>,
    shutdown: ShutdownCtl,
    observe_node_id: String,
    observe_node_role: String,
    observe: ObserveMetricsHandle,
    category: MqCategory,
    kind: NonblockingMonitorKind,
    member_idx: String,
    active_phase: Option<ActivePhase>,
}

impl NonblockingMonitorActor {
    async fn run(mut self) {
        loop {
            tokio::select! {
                biased;
                _ = self.shutdown.wait_closed() => {
                    self.flush_active_phase(Self::now_ms());
                    return;
                }
                maybe = self.rx.recv() => {
                    let Some(event) = maybe else {
                        self.flush_active_phase(Self::now_ms());
                        return;
                    };
                    match event {
                        NonblockingMonitorEvent::NonblockingHit { end_unix_ms } => {
                            self.record_nonblocking_hit(end_unix_ms);
                        }
                        NonblockingMonitorEvent::BlockingObserved { unix_ms } => {
                            self.flush_active_phase(unix_ms);
                        }
                    }
                }
            }
        }
    }

    fn record_nonblocking_hit(&mut self, end_unix_ms: i64) {
        match self.active_phase.as_mut() {
            Some(phase) => {
                phase.record_call(end_unix_ms);
            }
            None => {
                self.active_phase = Some(ActivePhase::new(end_unix_ms));
            }
        }
    }

    fn flush_active_phase(&mut self, blocking_unix_ms: i64) {
        let Some(phase) = self.active_phase.take() else {
            return;
        };
        let end_unix_ms = phase.latest_end_unix_ms.min(blocking_unix_ms);
        let duration_ms = (end_unix_ms - phase.begin_unix_ms).max(1);
        let latest_rate_window_ms = duration_ms.min(LATEST_RATE_WINDOW_MAX_MS);
        let latest_rate_window_calls =
            phase.latest_window_calls(end_unix_ms, latest_rate_window_ms);
        let duration_s = (latest_rate_window_ms as f64) / 1000.0;
        let rps = (latest_rate_window_calls as f64) / duration_s;
        let ts_ms = Self::now_ms();

        let series = vec![
            self.ts_one_latest_phase_calls(latest_rate_window_calls as f64, ts_ms),
            self.ts_one_latest_phase_rps(rps, ts_ms),
            self.ts_one_latest_interval(
                PROM_VALUE_MQ_INTERVAL_BEGIN,
                phase.begin_unix_ms as f64,
                ts_ms,
            ),
            self.ts_one_latest_interval(PROM_VALUE_MQ_INTERVAL_END, end_unix_ms as f64, ts_ms),
        ];
        self.observe.try_submit_timeseries(series);
    }

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

    fn labels(
        &self,
        metric_name: &'static str,
        extra_labels: &[(&'static str, String)],
    ) -> Vec<Label> {
        let mut labels: Vec<Label> = Vec::with_capacity(8 + extra_labels.len());
        labels.push(Label {
            name: RW_LABEL_NAME.to_string(),
            value: metric_name.to_string(),
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
        let chan_id = match self.kind {
            NonblockingMonitorKind::Producer { chan_id } => chan_id,
            NonblockingMonitorKind::Consumer { chan_id } => chan_id,
        };
        labels.push(Label {
            name: PROM_LABEL_MQ_CHAN_ID.to_string(),
            value: chan_id.to_string(),
        });
        match self.kind {
            NonblockingMonitorKind::Producer { .. } => labels.push(Label {
                name: PROM_LABEL_MQ_PRODUCER_IDX.to_string(),
                value: self.member_idx.clone(),
            }),
            NonblockingMonitorKind::Consumer { .. } => labels.push(Label {
                name: PROM_LABEL_MQ_CONSUMER_IDX.to_string(),
                value: self.member_idx.clone(),
            }),
        }
        for (k, v) in extra_labels {
            labels.push(Label {
                name: (*k).to_string(),
                value: v.clone(),
            });
        }
        labels
    }

    fn ts_one(
        &self,
        metric_name: &'static str,
        extra_labels: &[(&'static str, String)],
        value: f64,
        ts_ms: i64,
    ) -> TimeSeries {
        TimeSeries {
            labels: self.labels(metric_name, extra_labels),
            samples: vec![Sample {
                value,
                timestamp: ts_ms,
            }],
        }
    }

    fn ts_one_latest_phase_calls(&self, value: f64, ts_ms: i64) -> TimeSeries {
        match self.kind {
            NonblockingMonitorKind::Producer { .. } => self.ts_one(
                PROM_METRIC_MQ_PRODUCER_NONBLOCKING_LATEST_PHASE_CALLS,
                &[],
                value,
                ts_ms,
            ),
            NonblockingMonitorKind::Consumer { .. } => self.ts_one(
                PROM_METRIC_MQ_CONSUMER_NONBLOCKING_LATEST_PHASE_CALLS,
                &[],
                value,
                ts_ms,
            ),
        }
    }

    fn ts_one_latest_phase_rps(&self, value: f64, ts_ms: i64) -> TimeSeries {
        match self.kind {
            NonblockingMonitorKind::Producer { .. } => self.ts_one(
                PROM_METRIC_MQ_PRODUCER_NONBLOCKING_LATEST_PHASE_RPS,
                &[],
                value,
                ts_ms,
            ),
            NonblockingMonitorKind::Consumer { .. } => self.ts_one(
                PROM_METRIC_MQ_CONSUMER_NONBLOCKING_LATEST_PHASE_RPS,
                &[],
                value,
                ts_ms,
            ),
        }
    }

    fn ts_one_latest_interval(&self, metric: &'static str, value: f64, ts_ms: i64) -> TimeSeries {
        match self.kind {
            NonblockingMonitorKind::Producer { .. } => self.ts_one(
                PROM_METRIC_MQ_PRODUCER_NONBLOCKING_LATEST_INTERVAL_UNIX_MS,
                &[(PROM_LABEL_MQ_METRIC, metric.to_string())],
                value,
                ts_ms,
            ),
            NonblockingMonitorKind::Consumer { .. } => self.ts_one(
                PROM_METRIC_MQ_CONSUMER_NONBLOCKING_LATEST_INTERVAL_UNIX_MS,
                &[(PROM_LABEL_MQ_METRIC, metric.to_string())],
                value,
                ts_ms,
            ),
        }
    }
}
