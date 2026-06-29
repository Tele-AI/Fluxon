use std::fs::File;
use std::path::PathBuf;
use std::sync::mpsc;
use std::time::Duration;
use tracing::{info, warn};

#[derive(Debug, Clone, Copy)]
pub(crate) enum PprofRole {
    Master,
    Broker,
    Client,
}

impl PprofRole {
    fn as_str(self) -> &'static str {
        match self {
            PprofRole::Master => "master",
            PprofRole::Broker => "broker",
            PprofRole::Client => "client",
        }
    }
}

pub(crate) fn spawn_pprof_flamegraph_on_timeout_or_shutdown(
    pprof_duration_seconds: Option<u64>,
    output_dir: PathBuf,
    cluster_name: String,
    role: PprofRole,
    instance_key: String,
    mut shutdown_waiter: fluxon_framework_compiled::shutdown::ShutdownWaiter,
) {
    let duration_seconds = match pprof_duration_seconds {
        Some(v) => v,
        None => return,
    };
    if duration_seconds == 0 {
        return;
    }

    if let Err(e) = std::fs::create_dir_all(&output_dir) {
        warn!(
            "pprof enabled but failed to create output_dir={:?}: {}",
            output_dir, e
        );
        return;
    }

    std::thread::spawn(move || {
        let guard = match pprof::ProfilerGuard::new(100) {
            Ok(g) => g,
            Err(e) => {
                warn!("Failed to start pprof profiler: {}", e);
                return;
            }
        };

        let role_str = role.as_str();
        info!(
            "pprof enabled for {} (cluster_name={}, instance_key={}), will dump after {}s or on shutdown (whichever first)",
            role_str, cluster_name, instance_key, duration_seconds
        );
        let (tx, rx) = mpsc::channel::<()>();
        std::thread::spawn(move || {
            shutdown_waiter.wait_sync();
            let _ = tx.send(());
        });
        let _ = rx.recv_timeout(Duration::from_secs(duration_seconds));

        let ts = chrono::Utc::now().format("%Y%m%d_%H%M%S");
        let out_path = output_dir.join(format!(
            "pprof_{}_{}_{}_{}.svg",
            cluster_name, role_str, instance_key, ts
        ));

        let report = match guard.report().build() {
            Ok(r) => r,
            Err(e) => {
                warn!("Failed to build pprof report: {}", e);
                return;
            }
        };

        let file = match File::create(&out_path) {
            Ok(f) => f,
            Err(e) => {
                warn!("Failed to create pprof output file {:?}: {}", out_path, e);
                return;
            }
        };

        if let Err(e) = report.flamegraph(file) {
            warn!("Failed to write pprof flamegraph {:?}: {}", out_path, e);
            return;
        }

        info!("Wrote pprof flamegraph to {:?}", out_path);
    });
}
