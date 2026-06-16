use crate::Framework;
use crate::config::MasterConfig;
use anyhow::Result;
use fluxon_cli::config::{
    MemberKind as MonitorMemberKind, MonitorConfig, MonitorConfigYaml, OutputFormat,
};
use std::net::SocketAddr;
use std::sync::Arc;
use tracing::{info, warn};

fn build_master_ui_monitor_config(
    config: &MasterConfig,
) -> Result<Option<(MonitorConfig, SocketAddr)>> {
    let Some(master_ui) = config.master_ui.as_ref() else {
        return Ok(None);
    };
    let monitoring = config
        .monitoring
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("master_ui requires monitoring config on master"))?;
    let listen_addr: SocketAddr = master_ui.http_listen_addr.parse().map_err(|e| {
        anyhow::anyhow!(
            "invalid master_ui.http_listen_addr (expected host:port): {} err={}",
            master_ui.http_listen_addr,
            e
        )
    })?;
    let monitor_cfg = MonitorConfigYaml {
        etcd_endpoints: config.etcd_endpoints.clone(),
        prometheus_base_url: monitoring.prometheus_base_url.clone(),
        cluster_name: config.cluster_name.clone(),
        member_kind: MonitorMemberKind::Kv,
        output: OutputFormat::Web,
        mq_unique_key_prefixes: None,
        http_listen_addr: Some(master_ui.http_listen_addr.clone()),
        greptime_sql: None,
    }
    .verify()?;
    Ok(Some((monitor_cfg, listen_addr)))
}

pub(crate) fn try_start_master_ui_monitor(
    framework: Arc<Framework>,
    config: &MasterConfig,
) -> Result<bool> {
    let Some((ui_cfg, listen_addr)) = build_master_ui_monitor_config(config)? else {
        return Ok(false);
    };

    let listener = std::net::TcpListener::bind(listen_addr)
        .map_err(|e| anyhow::anyhow!("Failed to bind master_ui at {}: {}", listen_addr, e))?;
    listener
        .set_nonblocking(true)
        .map_err(|e| anyhow::anyhow!("Failed to set master_ui listener nonblocking: {}", e))?;

    let framework_for_shutdown = framework.clone();
    let framework_for_task = framework.clone();
    let cluster_view = framework_for_task.cluster_manager_view().clone();
    let cluster_view_for_shutdown = cluster_view.clone();
    let _ = cluster_view.spawn("master_ui_http_server", async move {
        let mut shutdown_waiter = cluster_view_for_shutdown.register_shutdown_waiter();
        let shutdown = async move {
            shutdown_waiter.wait().await;
        };
        if let Err(err) =
            fluxon_cli::server::serve_http_with_shutdown_from_tcp(ui_cfg, listener, shutdown, None)
                .await
        {
            warn!(
                err = %err,
                listen_addr = %listen_addr,
                "master_ui http server exited with error; requesting framework shutdown"
            );
            framework_for_shutdown.request_shutdown();
        }
    });

    info!("master_ui http server started at {}", listen_addr);
    Ok(true)
}
