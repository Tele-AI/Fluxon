use tokio::sync::mpsc;
use tracing::warn;

use fluxon_util::prom_remote_write::TimeSeries;

use crate::prom_remote_write_actor::PromRemoteWriteHandle;

enum MetricsActorMsg {
    SubmitTimeSeries { series: Vec<TimeSeries> },
}

#[derive(Clone)]
pub struct MetricsHandle {
    tx: Option<mpsc::Sender<MetricsActorMsg>>,
}

impl MetricsHandle {
    pub fn noop() -> Self {
        Self { tx: None }
    }

    pub fn try_submit_timeseries(&self, series: Vec<TimeSeries>) {
        if series.is_empty() {
            return;
        }
        let Some(tx) = &self.tx else {
            return;
        };
        if let Err(e) = tx.try_send(MetricsActorMsg::SubmitTimeSeries { series }) {
            warn!("metrics actor dropped SubmitTimeSeries: {}", e);
        }
    }
}

pub struct MetricsActorOwned {
    rx: mpsc::Receiver<MetricsActorMsg>,
    prom: PromRemoteWriteHandle,
}

impl MetricsActorOwned {
    pub fn new(
        max_pending_msgs: usize,
        prom: PromRemoteWriteHandle,
    ) -> (MetricsHandle, MetricsActorOwned) {
        let (tx, rx) = mpsc::channel(max_pending_msgs);
        let handle = MetricsHandle { tx: Some(tx) };
        let owned = MetricsActorOwned { rx, prom };
        (handle, owned)
    }

    pub async fn run<F>(mut self, shutdown: F)
    where
        F: std::future::Future<Output = ()> + Send,
    {
        tokio::pin!(shutdown);

        loop {
            tokio::select! {
                biased;

                _ = &mut shutdown => {
                    break;
                }
                maybe = self.rx.recv() => {
                    let Some(msg) = maybe else {
                        break;
                    };
                    match msg {
                        MetricsActorMsg::SubmitTimeSeries { series } => {
                            self.prom.try_submit_collected(Vec::new(), series);
                        }
                    }
                }
            }
        }
    }
}
