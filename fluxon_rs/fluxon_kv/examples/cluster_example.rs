use anyhow::Result;
use async_trait::async_trait;
use clap::Parser;
use fluxon_framework::{
    AnyResult, LogicalModule, ResourceRegistry, define_framework, define_module,
};
use fluxon_kv::cluster_manager::ClusterManagerView;
use fluxon_kv::cluster_manager::{
    ClusterError, ClusterEvent, ClusterManager, ClusterManagerAccessTrait, ClusterManagerNewArg,
    ClusterManagerRdmaControlInit, ClusterManagerViewTrait,
};
use limit_thirdparty::tokio;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use thiserror::Error;
use tokio::signal;
use tracing::{info, warn};

// fluxon-init-dag: yaml=./cluster_example_init_steps.yaml

/// 集群模块专用错误类型
#[derive(Error, Debug)]
pub enum ClusterExampleError {
    #[error("Cluster operation failed: {0}")]
    Cluster(#[from] ClusterError),

    #[error("Framework operation failed: {0}")]
    Framework(String),

    #[error("Configuration error: {0}")]
    Configuration(String),

    #[error("Unknown error: {0}")]
    Unknown(String),
}

#[derive(Parser)]
#[command(name = "cluster_example")]
#[command(
    about = "A cluster example demonstrating distributed cache node management with fluxon_framework"
)]
struct Args {
    /// Instance name for this cluster node
    #[arg(help = "Unique instance name for this cluster node (e.g., cache_node_1)")]
    instance_name: String,

    /// etcd endpoint
    #[arg(long, default_value = "http://127.0.0.1:2379")]
    etcd_endpoint: String,

    /// Cluster name
    #[arg(long, default_value = "kvcache_cluster")]
    cluster_name: String,

    /// Node address
    #[arg(long, default_value = "127.0.0.1")]
    address: String,

    /// Node port
    #[arg(long, default_value_t = 8080)]
    port: u16,
}

// 集群管理包装模块
pub struct ClusterExample {
    view: std::sync::OnceLock<ClusterExampleView>,
    shutdown: Arc<AtomicBool>,
}

// 集群模块参数
#[derive(Debug, Clone)]
pub struct ClusterExampleNewArg {
    pub shutdown: Arc<AtomicBool>,
}

impl ClusterExampleNewArg {
    pub fn new(shutdown: Arc<AtomicBool>) -> Self {
        Self { shutdown }
    }
}

impl ClusterExample {
    fn view(&self) -> &ClusterExampleView {
        self.view.get().unwrap()
    }

    pub async fn construct(arg: ClusterExampleNewArg) -> Result<Self, ClusterExampleError> {
        info!("Constructing ClusterExample (PreView)");
        Ok(Self {
            view: std::sync::OnceLock::new(),
            shutdown: arg.shutdown,
        })
    }

    pub async fn init2_for_example(&self) -> Result<(), ClusterExampleError> {
        info!("ClusterExample init2_for_example");
        let manager = self.view().cluster_manager();

        // Set cluster event callback.
        let mut event_rx = manager.listen();
        let shutdown_for_events = self.shutdown.clone();
        let view_for_events = self.view().clone();
        view_for_events.spawn("cluster_example_events", async move {
            while let Ok(event) = event_rx.recv().await {
                if shutdown_for_events.load(Ordering::Relaxed) {
                    break;
                }
                match event {
                    ClusterEvent::MemberJoined(member) => {
                        info!(
                            "New member joined cluster: {} (instance: {}) at {:?}:{:?} with role: {}",
                            member.id,
                            member.metadata.get("instance_name").unwrap_or(&member.id),
                            member.addresses,
                            member.port,
                            member.metadata.get("role").unwrap_or(&"unknown".to_string())
                        );
                    }
                    ClusterEvent::MemberLeft(member_id) => {
                        warn!("Member left cluster: {}", member_id);
                    }
                    ClusterEvent::MemberUpdated(member) => {
                        info!(
                            "Member updated: {} (instance: {})",
                            member.id,
                            member.metadata.get("instance_name").unwrap_or(&member.id)
                        );
                    }
                }
            }
        });

        let view_for_watch = self.view().clone();
        let watch_view = view_for_watch.clone();
        view_for_watch.spawn("cluster_example_watch", async move {
            let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(10));
            while !watch_view
                .cluster_example()
                .shutdown
                .load(Ordering::Relaxed)
            {
                interval.tick().await;

                let manager_guard = watch_view.cluster_manager();
                let members = manager_guard.get_members();
                info!("Current cluster status:");
                info!("   - Total members: {}", members.len() + 1); // +1 for self
                info!(
                    "   - Self: {} (at {:?}",
                    manager_guard.get_self_info().id,
                    manager_guard.get_self_info().port
                );

                for member in &members {
                    info!(
                        "   - Member: {} (instance: {}) at {:?}:{:?} (role: {})",
                        member.id,
                        member.metadata.get("instance_name").unwrap_or(&member.id),
                        member.addresses,
                        member.port,
                        member
                            .metadata
                            .get("role")
                            .unwrap_or(&"unknown".to_string())
                    );
                }
                drop(manager_guard);
            }
        });

        Ok(())
    }
}

#[async_trait]
impl LogicalModule for ClusterExample {
    type View = ClusterExampleView;
    type NewArg = ClusterExampleNewArg;
    type Error = ClusterExampleError;

    fn name(&self) -> &str {
        "ClusterExample"
    }

    fn attach_view(&self, view: Self::View) {
        self.view
            .set(view)
            .unwrap_or_else(|_| panic!("ClusterExample view attached twice"));
    }

    async fn shutdown(&self) -> Result<(), Self::Error> {
        info!("Shutting down ClusterModule");

        // 设置关闭标志
        self.shutdown.store(true, Ordering::Relaxed);

        info!("ClusterModule shutdown successfully");
        Ok(())
    }
}

// 使用宏定义模块
define_module!(
    ClusterExample,
    (cluster_example, ClusterExample),
    (cluster_manager, ClusterManager)
);

// 定义框架
define_framework!(cluster_example: ClusterExample, cluster_manager: ClusterManager);

include!(concat!(
    env!("OUT_DIR"),
    "/fluxon_init_dag/cluster_example.rs"
));

#[tokio::main]
async fn main() -> Result<()> {
    // 初始化日志
    fluxon_util::init_log_test("cluster_example");

    // 解析命令行参数
    let args = Args::parse();

    info!(
        "Starting cluster example with instance name: {} (managed by fluxon_framework)",
        args.instance_name
    );

    // etcd 连接配置
    let etcd_endpoints = vec![args.etcd_endpoint];

    // 创建集群模块参数
    let shutdown = Arc::new(AtomicBool::new(false));

    // 创建框架参数
    let framework_args = FrameworkArgs {
        cluster_example_arg: ClusterExampleNewArg::new(Arc::clone(&shutdown)),
        cluster_manager_arg: ClusterManagerNewArg {
            etcd_endpoints,
            cluster_name: args.cluster_name.clone(),
            instance_name: Some(args.instance_name.clone()),
            port: Some(args.port),
            metadata: HashMap::from([
                ("role".to_string(), "cache_node".to_string()),
                ("master".to_string(), "false".to_string()),
                ("client".to_string(), "true".to_string()),
                ("external_client".to_string(), "false".to_string()),
                (
                    "version".to_string(),
                    fluxon_util::git_version_build_record::get_current_git_commitid().unwrap(),
                ),
                ("capacity".to_string(), "1000".to_string()),
                ("instance_name".to_string(), args.instance_name.clone()),
                ("managed_by".to_string(), "fluxon_framework".to_string()),
            ]),
            local_ipc_root: None,
            rdma_control_init: ClusterManagerRdmaControlInit::Disabled,
            sub_cluster: None,
            network: None,
        },
    };

    // 创建并初始化框架
    let framework = Framework::new(format!(
        "fluxon_kv.example.cluster:{}:{}",
        args.cluster_name, args.instance_name
    ));
    info!("Initializing framework...");

    if let Err(e) = init_framework(&framework, framework_args).await {
        warn!("Framework initialization failed: {}", e);
        warn!("Please ensure etcd is running and accessible");
        let _ = framework.shutdown().await;
        return Err(anyhow::anyhow!("Framework initialization failed: {}", e));
    }

    info!("Framework initialized successfully");
    framework.wait_shutdown_signal().await;

    shutdown.store(true, Ordering::SeqCst);
    info!("Shutting down framework...");
    framework.shutdown().await.map_err(|e| {
        warn!("Framework shutdown failed: {}", e);
        anyhow::anyhow!("Framework shutdown failed: {}", e)
    })?;
    info!("Framework shutdown completed");

    info!("Cluster example with fluxon_framework completed");
    Ok(())
}
