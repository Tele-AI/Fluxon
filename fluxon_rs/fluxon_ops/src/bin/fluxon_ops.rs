use clap::{Parser, Subcommand};
use serde::Deserialize;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use fluxon_proxy;
use fluxon_util::{
    FluxonCliProxyDescriptorV2, FluxonCliProxyTransportV2, fluxon_cli_proxy_desc_etcd_key_v2,
};

#[derive(Parser)]
#[command(version)]
struct Args {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    Serve {
        #[arg(short = 'c', long = "config")]
        config: PathBuf,
        #[arg(short = 'w', long = "workdir")]
        workdir: PathBuf,
    },
    Agent {
        #[arg(short = 'c', long = "config")]
        config: PathBuf,
        #[arg(short = 'w', long = "workdir")]
        workdir: PathBuf,
        #[arg(long = "python")]
        python: PathBuf,
    },
    Monitor {
        #[arg(short = 'c', long = "config")]
        config: PathBuf,
        #[arg(short = 'w', long = "workdir")]
        workdir: PathBuf,
    },
    SmokeSupervisor {
        #[arg(short = 'w', long = "workdir")]
        workdir: PathBuf,
        #[arg(long = "python")]
        python: PathBuf,
    },
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct ServeConfigYaml {
    ops_controller: fluxon_ops::ControllerConfigYaml,
    fluxon_cli: fluxon_cli::config::MonitorConfigYaml,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    match args.cmd {
        Cmd::Serve { config, workdir } => run_serve(&config, &workdir).await,
        Cmd::Agent {
            config,
            workdir,
            python,
        } => run_agent(&config, &workdir, &python).await,
        Cmd::Monitor { config, workdir } => run_monitor(&config, &workdir).await,
        Cmd::SmokeSupervisor { workdir, python } => {
            fluxon_ops::smoke_selection_supervisor(&python, &workdir)
        }
    }
}

async fn run_monitor(config: &Path, workdir: &Path) -> anyhow::Result<()> {
    fluxon_cli::build_info::print_startup_info();
    std::env::set_current_dir(workdir)?;

    let cfg_yaml = fluxon_cli::config::MonitorConfigYaml::from_file(config)?;
    let cfg = cfg_yaml.verify()?;
    let snapshot = fluxon_cli::build_cluster_snapshot(&cfg).await?;
    match cfg.output {
        fluxon_cli::config::OutputFormat::Cli => {
            print!("{}", fluxon_cli::cli_renderer::render_cluster(&snapshot));
        }
        fluxon_cli::config::OutputFormat::Web => {
            print!("{}", fluxon_cli::web_renderer::render_cluster(&snapshot));
        }
    }
    Ok(())
}

async fn run_agent(config: &Path, workdir: &Path, python: &Path) -> anyhow::Result<()> {
    let config_yaml = std::fs::read_to_string(config)
        .map_err(|e| anyhow::anyhow!("read config failed: path={} err={}", config.display(), e))?;
    fluxon_ops::run_agent_blocking(&config_yaml, workdir, python).await
}

async fn run_serve(config: &Path, workdir: &Path) -> anyhow::Result<()> {
    let unified_yaml = std::fs::read_to_string(config)
        .map_err(|e| anyhow::anyhow!("read config failed: path={} err={}", config.display(), e))?;
    let unified: ServeConfigYaml = serde_yaml::from_str(&unified_yaml)
        .map_err(|e| anyhow::anyhow!("parse config yaml failed: {}", e))?;

    let cli_cfg = unified.fluxon_cli.verify()?;
    let listen = cli_cfg
        .http_listen_addr
        .clone()
        .ok_or_else(|| anyhow::anyhow!("fluxon_cli.http_listen_addr is required"))?;
    let listen_addr: SocketAddr = listen.parse().map_err(|e| {
        anyhow::anyhow!(
            "invalid fluxon_cli.http_listen_addr (expected host:port): {listen} err={e}"
        )
    })?;

    let panel_cluster_name = unified
        .ops_controller
        .kv_client
        .fluxonkv_spec
        .cluster_name
        .clone();
    if cli_cfg.cluster_name != panel_cluster_name {
        anyhow::bail!(
            "invalid config: fluxon_cli.cluster_name must match ops_controller.kv_client.fluxonkv_spec.cluster_name. fluxon_cli.cluster_name={} ops_cluster_name={}",
            cli_cfg.cluster_name,
            panel_cluster_name
        );
    }

    std::env::set_current_dir(workdir)?;
    let workdir2 = workdir.to_path_buf();

    let ops_controller_yaml = serde_yaml::to_string(&unified.ops_controller)
        .map_err(|e| anyhow::anyhow!("serialize ops_controller config failed: {}", e))?;

    let (fw_ready_tx, fw_ready_rx) = tokio::sync::oneshot::channel::<Arc<fluxon_kv::Framework>>();
    let mut ops_task = tokio::spawn(async move {
        fluxon_ops::run_controller_blocking(&ops_controller_yaml, &workdir2, fw_ready_tx).await
    });

    let fw = tokio::select! {
        r = &mut ops_task => {
            return match r {
                Ok(v) => v,
                Err(e) => Err(anyhow::anyhow!("ops_controller task join failed before fw_ready: {}", e)),
            };
        }
        r = fw_ready_rx => {
            r.map_err(|_| anyhow::anyhow!("ops_controller did not send fw_ready handle (fw_ready_rx dropped)"))?
        }
    };

    let expected_node_id = fw
        .cluster_manager_view()
        .cluster_manager()
        .get_self_info()
        .id
        .to_string();
    if expected_node_id.trim().is_empty() {
        anyhow::bail!("invalid ops_controller self node_id (empty)");
    }

    // Register the RPC message type once so fluxon_cli's embedded proxy backend can call it without
    // requiring per-request registration.
    fluxon_proxy::ensure_panel_proxy_userrpc_client_registered(fw.p2p_view().p2p_module());

    // Provide an explicit proxy backend so fluxon_cli can execute p2p_rpc transports without
    // depending on fluxon_kv (inversion of control).
    let backend = fluxon_proxy::build_fluxon_cli_registered_panel_proxy_backend(
        fw.clone(),
        Duration::from_secs(60),
    );

    let etcd_key =
        fluxon_cli_proxy_desc_etcd_key_v2(fluxon_ops::OPS_SERVICE_NAME, &cli_cfg.cluster_name);
    let mut etcd = etcd_client::Client::connect(cli_cfg.etcd_endpoints.clone(), None)
        .await
        .map_err(|e| {
            anyhow::anyhow!(
                "etcd connect failed while waiting for ops panel: key={} err={}",
                etcd_key,
                e
            )
        })?;

    let ready_deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(30);
    loop {
        tokio::select! {
            r = &mut ops_task => {
                return match r {
                    Ok(v) => v,
                    Err(e) => Err(anyhow::anyhow!("ops_controller task join failed: {}", e)),
                };
            }
            _ = tokio::time::sleep(tokio::time::Duration::from_millis(100)) => {
                let resp = etcd.get(etcd_key.clone(), None).await
                    .map_err(|e| anyhow::anyhow!("etcd get failed while waiting for ops panel: key={} err={}", etcd_key, e))?;
                let Some(kv) = resp.kvs().first() else {
                    if tokio::time::Instant::now() >= ready_deadline {
                        ops_task.abort();
                        anyhow::bail!("ops panel descriptor is not published within 30s: key={}", etcd_key);
                    }
                    continue;
                };

                let raw = String::from_utf8_lossy(kv.value()).trim().to_string();
                if raw.is_empty() {
                    anyhow::bail!("invalid ops panel descriptor in etcd (empty): key={}", etcd_key);
                }
                let desc: FluxonCliProxyDescriptorV2 = serde_json::from_str(&raw)
                    .map_err(|e| anyhow::anyhow!("invalid ops panel descriptor json in etcd: key={} err={}", etcd_key, e))?;
                let node_id = match desc.transport {
                    FluxonCliProxyTransportV2::P2pRpc { node_id } => {
                        if node_id.trim().is_empty() {
                            anyhow::bail!("invalid ops panel descriptor transport.p2p_rpc.node_id (empty): key={}", etcd_key);
                        }
                        if node_id != expected_node_id {
                            if tokio::time::Instant::now() >= ready_deadline {
                                ops_task.abort();
                                anyhow::bail!(
                                    "ops panel descriptor node_id mismatch after 30s: key={} expected_node_id={} got_node_id={}",
                                    etcd_key,
                                    expected_node_id,
                                    node_id
                                );
                            }
                            continue;
                        }
                        node_id
                    }
                    FluxonCliProxyTransportV2::Http { base_url } => {
                        if tokio::time::Instant::now() >= ready_deadline {
                            ops_task.abort();
                            anyhow::bail!(
                                "ops panel descriptor transport mismatch after 30s: key={} expected=p2p_rpc got=http(base_url={})",
                                etcd_key,
                                base_url
                            );
                        }
                        continue;
                    }
                };

                // English note:
                // - Avoid probing /readyz via a self-RPC here. During early bootstrap, the in-process
                //   dispatch path can be back-pressured or not fully initialized, which can hang the
                //   probe and prevent the HTTP endpoint from ever binding.
                // - Descriptor publish implies ops_controller finished framework construction and
                //   registered panel-proxy RPC handlers. That is a sufficient readiness definition
                //   to avoid stale-descriptor races deterministically.
                break;
            }
        }
    }

    let mut cli_task = tokio::spawn(async move {
        let listener = std::net::TcpListener::bind(listen_addr).map_err(|e| {
            anyhow::anyhow!("fluxon_cli http bind failed at {}: {}", listen_addr, e)
        })?;
        listener.set_nonblocking(true).map_err(|e| {
            anyhow::anyhow!(
                "fluxon_cli http set_nonblocking failed at {}: {}",
                listen_addr,
                e
            )
        })?;
        fluxon_cli::server::serve_http_from_tcp(cli_cfg, listener, Some(backend)).await
    });

    tokio::select! {
        r = &mut ops_task => {
            cli_task.abort();
            match r {
                Ok(v) => v,
                Err(e) => Err(anyhow::anyhow!("ops_controller task join failed: {}", e)),
            }
        }
        r = &mut cli_task => {
            ops_task.abort();
            match r {
                Ok(v) => v,
                Err(e) => Err(anyhow::anyhow!("fluxon_cli task join failed: {}", e)),
            }
        }
    }
}
