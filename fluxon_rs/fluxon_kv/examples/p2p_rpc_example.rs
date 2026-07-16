use async_trait::async_trait;
use clap::Parser;
use fluxon_framework::{
    AnyResult, LogicalModule, ResourceRegistry, define_framework, define_module,
};
use fluxon_kv::rpcresp_kvresult_convert::msg_and_error::KvError;
use fluxon_kv::{
    cluster_manager::{
        ClusterError, ClusterManager, ClusterManagerAccessTrait, ClusterManagerNewArg,
        ClusterManagerRdmaControlInit, NodeID,
    },
    p2p::{
        P2PError, P2PResult,
        msg_pack::{MsgPack, MsgPackSerializePart, RPCCaller, RPCHandler, RPCReq},
        p2p_module::{
            P2pModule, P2pModuleAccessTrait, P2pModuleNewArg, P2pModuleView, P2pModuleViewTrait,
            P2pTcpThreadTransportTuning,
        },
    },
};
use lazy_static::lazy_static;
use limit_thirdparty::tokio;
use limit_thirdparty::tokio::io::{self, AsyncBufReadExt, BufReader};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use std::time::{SystemTime, UNIX_EPOCH};
use std::{borrow::Cow, io::Write};
use thiserror::Error;
use tracing_subscriber::{EnvFilter, fmt};
// use tracing_subscriber::{fmt::format::FmtSpan, EnvFilter};
use bitcode::{Decode, Encode};
use fluxon_kv::cluster_manager::ClusterManagerView;
use fluxon_kv::cluster_manager::ClusterManagerViewTrait;
use std::collections::HashMap;

// fluxon-init-dag: yaml=./p2p_rpc_example_init_steps.yaml

// 共享状态：用于存储最新消息
lazy_static! {
    static ref LATEST_MESSAGE: Mutex<Option<String>> = Mutex::new(None);
}

// 聊天示例错误类型（示例简单处理：不把 P2PError 嵌入 source）
#[derive(Debug, Error)]
pub enum ChatExampleError {
    #[error("IO错误: {0}")]
    IoError(#[from] std::io::Error),
    #[error("Cluster错误: {0}")]
    ClusterError(#[from] ClusterError),
    #[error("KV错误: {0}")]
    KvError(#[from] KvError),
    #[error("Framework错误: {0}")]
    FrameworkError(String),
    #[error("其他错误: {0}")]
    Other(String),
}

// 聊天消息结构
#[derive(Debug, Clone, Encode, Decode, Default)]
struct ChatMessage {
    sender_id: String,
    sender_name: String,
    content: String,
    timestamp: u64,
}

impl MsgPackSerializePart for ChatMessage {
    fn msg_id(&self) -> u32 {
        1001
    }
}

#[derive(Debug, Clone, Encode, Decode, Default)]
struct ChatAck {}

impl MsgPackSerializePart for ChatAck {
    fn msg_id(&self) -> u32 {
        1002
    }
}

impl RPCReq for ChatMessage {
    type Resp = ChatAck;
}

// 聊天模块
pub struct ChatModule {
    view: std::sync::OnceLock<ChatModuleView>,
    node_name: String,
    target_id: Arc<Mutex<Option<NodeID>>>,
    caller: RPCCaller<ChatMessage>,
}

// 聊天模块参数
#[derive(Debug, Clone)]
pub struct ChatModuleNewArg {
    node_name: String,
}

impl ChatModuleNewArg {
    pub fn new(node_name: String) -> Self {
        Self { node_name }
    }
}

impl ChatModule {
    fn view(&self) -> &ChatModuleView {
        self.view.get().unwrap()
    }

    pub async fn construct(arg: ChatModuleNewArg) -> Result<Self, ChatExampleError> {
        tracing::info!("Constructing ChatModule (PreView)");
        tracing::info!("Chat node {} constructing", arg.node_name);
        Ok(Self {
            view: std::sync::OnceLock::new(),
            node_name: arg.node_name,
            target_id: Arc::new(Mutex::new(None)),
            caller: RPCCaller::default(),
        })
    }

    pub async fn init2_for_example(&self) -> Result<(), ChatExampleError> {
        tracing::info!("ChatModule init2_for_example");

        // Register RPC handler/caller.
        let p2p = self.view().p2p_module();
        self.caller.regist(p2p);
        let rpc_handler = RPCHandler::<ChatMessage>::new();
        let this_node_id = self.view().cluster_manager().get_self_info().id;
        let handler_view = self.view().clone();

        rpc_handler.regist(p2p, move |resp, msg| {
            let msg = msg.serialize_part;
            // Only show messages from other nodes.
            if msg.sender_id != this_node_id.as_ref() {
                println!("\n[{}] {}: {}", msg.sender_name, msg.timestamp, msg.content);
                print!("请输入回复: ");
                std::io::stdout().flush().unwrap();

                let mut latest = LATEST_MESSAGE.lock().unwrap();
                *latest = Some(format!("[{}] {}", msg.sender_name, msg.content));
            }

            let view = handler_view.clone();
            view.spawn("chat_ack", async move {
                let ack = MsgPack {
                    serialize_part: ChatAck::default(),
                    raw_bytes: Vec::new(),
                };
                if let Err(e) = resp.send_resp(ack).await {
                    tracing::error!("Failed to send ack: {:?}", e);
                }
            });

            Ok(())
        });

        let view = self.view().clone();
        let view_task = view.clone();
        let node_name = self.node_name.clone();
        let target_id_arc = self.target_id.clone();

        view.spawn("chat_discover_and_loop", async move {
            let node_id: Cow<'_, str> = view_task.cluster_manager().get_self_info().id.into();
            let mut target_node_id: Option<NodeID> = None;
            // Wait for discovering another member.
            loop {
                let members = view_task.cluster_manager().get_members();
                let other_member = members
                    .into_iter()
                    .find(|m| m.id.as_str() != node_id.as_ref());

                if let Some(member) = other_member {
                    let target_name = member.metadata.get("instance_name").unwrap_or(&member.id);
                    println!("\n发现聊天对象: {} (节点ID: {})", target_name, member.id);
                    target_node_id = Some(member.id.into());
                    break;
                }
                println!("等待其他聊天节点加入...");
                tokio::time::sleep(Duration::from_secs(2)).await;
            }

            if let Some(tid) = target_node_id {
                {
                    let mut target_id_guard = target_id_arc.lock().unwrap();
                    *target_id_guard = Some(tid.clone());
                }

                let target_name = "Peer"; // Simplified
                println!("\n聊天程序已启动!");
                println!("你是: {} (节点{})", node_name, node_id);
                println!("你正在与 {} (节点{}) 聊天", target_name, tid);
                println!("输入消息并按Enter发送，或输入'exit'退出");
                print!("请输入消息: ");
                std::io::stdout().flush().unwrap();

                let mut reader = BufReader::new(io::stdin());
                let mut buffer = String::new();

                loop {
                    buffer.clear();
                    match reader.read_line(&mut buffer).await {
                        Ok(0) => break, // EOF
                        Ok(_) => {
                            let input = buffer.trim().to_string();
                            if input.eq_ignore_ascii_case("exit") {
                                break;
                            }

                            if !input.is_empty() {
                                let view_clone = view_task.clone();
                                let content = input.clone();
                                let name = node_name.clone();
                                let node_id_clone = node_id.clone();
                                let tid_clone = tid.clone();

                                let view_spawn = view_clone.clone();
                                view_spawn.spawn("chat_send", async move {
                                    match send_chat(
                                        &view_clone,
                                        content,
                                        node_id_clone,
                                        &name,
                                        tid_clone,
                                    )
                                    .await
                                    {
                                        Ok(_) => {}
                                        Err(e) => tracing::error!("发送消息失败: {:?}", e),
                                    }
                                });
                            }
                            print!("请输入消息: ");
                            std::io::stdout().flush().unwrap();
                        }
                        Err(e) => {
                            tracing::error!("读取输入失败: {:?}", e);
                            break;
                        }
                    }
                }
            }
        });

        Ok(())
    }
}

#[async_trait]
impl LogicalModule for ChatModule {
    type View = ChatModuleView;
    type NewArg = ChatModuleNewArg;
    type Error = ChatExampleError;

    fn name(&self) -> &str {
        "ChatModule"
    }

    fn attach_view(&self, view: Self::View) {
        self.view
            .set(view)
            .unwrap_or_else(|_| panic!("ChatModule view attached twice"));
    }

    async fn shutdown(&self) -> Result<(), Self::Error> {
        tracing::info!("关闭 ChatModule");
        Ok(())
    }
}

// 发送聊天消息
async fn send_chat(
    view: &ChatModuleView,
    content: String,
    node_id: NodeID,
    name: &str,
    target_id: NodeID,
) -> P2PResult<()> {
    let p2p = view.p2p_module();
    let caller = &view.chat_module().caller;
    // 创建消息
    let message = ChatMessage {
        sender_id: node_id.to_string(),
        sender_name: name.to_string(),
        content,
        timestamp: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs(),
    };

    let msg = MsgPack {
        serialize_part: message,
        raw_bytes: Vec::new(),
    };

    // 发送消息
    tracing::info!("发送消息到节点 {}", target_id);
    caller
        .call(p2p, target_id.clone(), msg, None, 0)
        .await
        .map_err(|e| {
            tracing::error!("发送消息失败: {:?}", e);
            e
        })?;
    Ok(())
}

// 使用宏定义模块和框架
define_module!(
    ChatModule,
    (chat_module, ChatModule),
    (p2p, P2pModule),
    (cluster_manager, ClusterManager)
);

define_framework! {
    p2p: P2pModule,
    chat: ChatModule,
    cluster_manager: ClusterManager
}

include!(concat!(
    env!("OUT_DIR"),
    "/fluxon_init_dag/p2p_rpc_example.rs"
));

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(short, long)]
    instance_name: String,
    #[arg(short, long, default_value_t = String::from("test-cluster"))]
    cluster_name: String,
    #[arg(long, default_value_t = String::from("http://192.0.2.10:2379"))]
    etcd_endpoint: String,
}

#[tokio::main]
async fn main() -> AnyResult<()> {
    // 初始化日志
    let subscriber = fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("info".parse().unwrap()))
        // .with_span_events(FmtSpan::CLOSE)
        .finish();

    tracing::subscriber::set_global_default(subscriber).expect("设置全局日志订阅器失败");

    tracing::info!("启动聊天程序");

    // 读取命令行参数
    let args = Args::parse();
    let node_name = args.instance_name.clone();
    tracing::info!("启动节点 {}", node_name,);

    // 创建模块参数
    let p2p_module_arg =
        P2pModuleNewArg::new(None, P2pTcpThreadTransportTuning::default(), false, false);
    let chat_arg = ChatModuleNewArg::new(node_name.to_string());
    let cluster_manager_arg = ClusterManagerNewArg {
        etcd_endpoints: vec![args.etcd_endpoint],
        cluster_name: args.cluster_name,
        instance_name: Some(args.instance_name.clone()),
        port: None, // Port is now discovered automatically
        metadata: HashMap::from([
            ("instance_name".to_string(), node_name.clone()),
            ("master".to_string(), "false".to_string()),
            ("client".to_string(), "true".to_string()),
            ("external_client".to_string(), "false".to_string()),
        ]),
        local_ipc_root: None,
        rdma_control_init: ClusterManagerRdmaControlInit::Disabled,
        sub_cluster: None,
        network: None,
    };

    // 创建框架参数
    let framework_args = FrameworkArgs {
        p2p_arg: p2p_module_arg,
        chat_arg,
        cluster_manager_arg,
    };

    let framework = Framework::new(format!("fluxon_kv.example.p2p_rpc:{}", node_name));
    tracing::info!("初始化框架 (init-step DAG style)");
    init_framework(&framework, framework_args).await?;

    // 等待连接建立
    // tracing::info!("等待5秒钟让节点互联...");
    // tokio::time::sleep(Duration::from_secs(5)).await;

    // 只等待Ctrl+C信号
    // tokio::signal::ctrl_c().await.expect("无法监听Ctrl+C信号");
    framework.wait_shutdown_signal().await;
    tracing::info!("接收到Ctrl+C信号，聊天程序退出");

    framework
        .shutdown()
        .await
        .map_err(|e| ChatExampleError::FrameworkError(e.to_string()))?;

    Ok(())
}
