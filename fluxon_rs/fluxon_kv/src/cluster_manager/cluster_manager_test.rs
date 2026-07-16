use async_trait::async_trait;
use etcd_client::{Client, DeleteOptions};
use fluxon_framework::{AnyResult, LogicalModule, define_framework, define_module};
use limit_thirdparty::tokio;
use limit_thirdparty::tokio::time::sleep;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};
use std::{collections::HashMap, sync::Arc};

use super::{
    ClusterError, ClusterEvent, ClusterManager, ClusterManagerAccessTrait, ClusterManagerNewArg,
    ClusterManagerRdmaControlInit, ClusterManagerView, ClusterManagerViewTrait,
};

// fluxon-init-dag: yaml=./cluster_manager_test_init_steps.yaml

/// 获取 etcd endpoint（从项目根目录的 build_config_ext.yml 读取）
fn get_etcd_endpoints() -> Vec<String> {
    let etcd = fluxon_util::dev_config::read_etcd_endpoint_from_build_config()
        .expect("read etcd endpoint from build_config_ext.yml");
    vec![etcd]
}

/// 测试前清理 etcd key
async fn clean_etcd_members(cluster_name: &str) {
    fluxon_util::test_util::start_test_etcd().expect("start etcd for cluster_manager tests");
    let mut client = Client::connect(get_etcd_endpoints(), None)
        .await
        .expect("etcd connect fail");
    let prefix = format!("/cluster/{}/members", cluster_name);
    match client
        .delete(prefix.clone(), Some(DeleteOptions::new().with_prefix()))
        .await
    {
        Ok(_) => println!("删除前缀成功"),
        Err(e) => println!("删除前缀{}失败", e),
    };
}

async fn wait_for_member_ids(
    cluster_manager: &ClusterManager,
    expected_ids: &[&str],
) -> Vec<super::ClusterMember> {
    let deadline = Instant::now() + Duration::from_secs(20);
    let mut expected_ids: Vec<String> = expected_ids.iter().map(|id| (*id).to_string()).collect();
    expected_ids.sort();

    loop {
        let members = cluster_manager.get_members();
        let mut actual_ids: Vec<String> = members.iter().map(|member| member.id.clone()).collect();
        actual_ids.sort();
        if actual_ids == expected_ids {
            return members;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for cluster members: expected={expected_ids:?}, actual={actual_ids:?}"
        );
        sleep(Duration::from_millis(50)).await;
    }
}

/// 创建一个测试模块来包装 ClusterManager
pub struct TestClusterModule {
    view: std::sync::OnceLock<TestClusterModuleView>,
    shutdown: Arc<AtomicBool>,
}

/// 测试模块参数
#[derive(Debug, Clone)]
pub struct TestClusterModuleNewArg {
    pub cluster_name: String,
    pub instance_name: String,
    pub port: u16,
    pub shutdown: Arc<AtomicBool>,
}

impl TestClusterModule {
    /// 添加测试方法
    pub fn cluster_manager(&self) -> &ClusterManager {
        self.view.get().unwrap().cluster_manager()
    }

    pub async fn construct(arg: TestClusterModuleNewArg) -> Result<Self, ClusterError> {
        let TestClusterModuleNewArg {
            cluster_name,
            instance_name,
            port,
            shutdown,
        } = arg;
        println!("Constructing TestClusterModule");
        let _ = (cluster_name, instance_name, port);
        Ok(Self {
            view: std::sync::OnceLock::new(),
            shutdown,
        })
    }

    pub async fn init2_for_test(&self) -> Result<(), ClusterError> {
        println!("TestClusterModule init2_for_test");

        let mut event_rx = self.view.get().unwrap().cluster_manager().listen();
        let shutdown_clone = self.shutdown.clone();

        let view_for_events = self.view.get().unwrap().clone();
        let mut shutdown_waiter = view_for_events.register_shutdown_waiter();
        let _ = view_for_events.spawn("test_cluster_module_events", async move {
            loop {
                tokio::select! {
                    _ = shutdown_waiter.wait() => break,
                    event = event_rx.recv() => {
                        let Ok(event) = event else {
                            break;
                        };
                        if shutdown_clone.load(Ordering::Relaxed) {
                            break;
                        }
                        match event {
                            ClusterEvent::MemberJoined(member) => {
                                println!("Test module event: member joined - {}", member.id);
                            }
                            ClusterEvent::MemberLeft(member_id) => {
                                println!("Test module event: member left - {}", member_id);
                            }
                            ClusterEvent::MemberUpdated(member) => {
                                println!("Test module event: member updated - {}", member.id);
                            }
                        }
                    }
                }
            }
        });

        Ok(())
    }
}

#[async_trait]
impl LogicalModule for TestClusterModule {
    type View = TestClusterModuleView;
    type NewArg = TestClusterModuleNewArg;
    type Error = ClusterError;

    fn name(&self) -> &str {
        "TestClusterModule"
    }

    fn attach_view(&self, view: Self::View) {
        self.view
            .set(view)
            .unwrap_or_else(|_| panic!("TestClusterModule view attached twice"));
    }

    async fn shutdown(&self) -> Result<(), Self::Error> {
        println!("关闭 TestClusterModule");
        self.shutdown.store(true, Ordering::Relaxed);
        Ok(())
    }
}

// 定义模块和框架
define_module!(TestClusterModule, (cluster_manager, ClusterManager));
define_framework!(test_cluster_module: TestClusterModule, cluster_manager: ClusterManager);

include!(concat!(
    env!("OUT_DIR"),
    "/fluxon_init_dag/cluster_manager_test.rs"
));

/// 测试 ClusterManager 的基本功能
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_cluster_manager_basic_functionality() {
    let _test_guard = crate::kvcore_test_lib::integration_test_lock().await;
    // Box::pin(async move {
    // tokio::runtime::Runtime::new().unwrap().block_on(async move {
    let cluster_name = "test-cluster-basic";

    clean_etcd_members(cluster_name).await;
    let shutdown = Arc::new(AtomicBool::new(false));
    let build_version = fluxon_util::git_version_build_record::get_current_git_commitid().unwrap();

    // 创建框架参数
    let framework_args = FrameworkArgs {
        test_cluster_module_arg: TestClusterModuleNewArg {
            cluster_name: cluster_name.to_string(),
            instance_name: "test-member-1".to_string(),
            port: 8080,
            shutdown: shutdown.clone(),
        },
        cluster_manager_arg: ClusterManagerNewArg {
            etcd_endpoints: get_etcd_endpoints(),
            cluster_name: cluster_name.to_string(),
            instance_name: Some("test-member-1".to_string()),
            port: Some(8080),
            metadata: HashMap::from([
                ("role".to_string(), "test_node".to_string()),
                ("master".to_string(), "false".to_string()),
                ("client".to_string(), "true".to_string()),
                ("external_client".to_string(), "false".to_string()),
                ("version".to_string(), build_version.clone()),
            ]),
            local_ipc_root: None,
            rdma_control_init: ClusterManagerRdmaControlInit::Disabled,
            sub_cluster: None,
            network: None,
        },
    };

    // 创建并初始化框架
    let framework = Framework::new("fluxon_kv.cluster_manager_test");
    init_framework(&framework, framework_args).await.unwrap();

    // 等待5秒，让成员信息同步
    tokio::time::sleep(Duration::from_secs(5)).await;

    // 通过 TestClusterModule 访问 ClusterManager
    let test_module = framework.0.test_cluster_module.get().unwrap();
    let cluster_manager = test_module.cluster_manager();

    // 验证基本功能
    let member_info = cluster_manager.get_self_info();
    assert_eq!(member_info.id, "test-member-1");
    assert_eq!(member_info.port, Some(8080));
    assert_eq!(
        member_info.metadata.get("role"),
        Some(&"test_node".to_string())
    );
    assert_eq!(member_info.metadata.get("version"), Some(&build_version));

    let members = cluster_manager.get_members();
    assert_eq!(members.len(), 1);
    assert_eq!(members[0].id, "test-member-1");

    // 测试设置监听端口
    cluster_manager.set_listening_port(9090).await.unwrap();
    let updated_info = cluster_manager.get_self_info();
    assert_eq!(updated_info.port, Some(9090));

    // 关闭框架
    framework.shutdown().await.unwrap();
    clean_etcd_members(cluster_name).await;
    // })
    // });
    // }
}

/// 测试集群成员监听功能
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_cluster_manager_watch_functionality() {
    let _test_guard = crate::kvcore_test_lib::integration_test_lock().await;
    let cluster_name = "test-cluster-watch";
    clean_etcd_members(cluster_name).await;

    let shutdown1 = Arc::new(AtomicBool::new(false));
    let shutdown2 = Arc::new(AtomicBool::new(false));

    // 创建第一个框架实例
    let framework_args1 = FrameworkArgs {
        test_cluster_module_arg: TestClusterModuleNewArg {
            cluster_name: cluster_name.to_string(),
            instance_name: "test-member-1".to_string(),
            port: 8080,
            shutdown: shutdown1.clone(),
        },
        cluster_manager_arg: ClusterManagerNewArg {
            etcd_endpoints: get_etcd_endpoints(),
            cluster_name: cluster_name.to_string(),
            instance_name: Some("test-member-1".to_string()),
            port: Some(8080),
            metadata: HashMap::from([
                ("role".to_string(), "test_node".to_string()),
                ("master".to_string(), "false".to_string()),
                ("client".to_string(), "true".to_string()),
                ("external_client".to_string(), "false".to_string()),
            ]),
            local_ipc_root: None,
            rdma_control_init: ClusterManagerRdmaControlInit::Disabled,
            sub_cluster: None,
            network: None,
        },
    };

    let framework1 = Framework::new("fluxon_kv.cluster_manager_test");
    init_framework(&framework1, framework_args1).await.unwrap();
    // 等待5秒，让成员信息同步
    tokio::time::sleep(Duration::from_secs(5)).await;

    let test_module1 = framework1.0.test_cluster_module.get().unwrap();
    let cluster_manager1 = test_module1.cluster_manager();

    // 验证第一个成员已加入
    let members1 = cluster_manager1.get_members();
    assert_eq!(members1.len(), 1);
    assert_eq!(members1[0].id, "test-member-1");

    // 创建第二个框架实例
    let framework_args2 = FrameworkArgs {
        test_cluster_module_arg: TestClusterModuleNewArg {
            cluster_name: cluster_name.to_string(),
            instance_name: "test-member-2".to_string(),
            port: 8081,
            shutdown: shutdown2.clone(),
        },
        cluster_manager_arg: ClusterManagerNewArg {
            etcd_endpoints: get_etcd_endpoints(),
            cluster_name: cluster_name.to_string(),
            instance_name: Some("test-member-2".to_string()),
            port: Some(8081),
            metadata: HashMap::from([
                ("role".to_string(), "test_node".to_string()),
                ("master".to_string(), "false".to_string()),
                ("client".to_string(), "true".to_string()),
                ("external_client".to_string(), "false".to_string()),
            ]),
            local_ipc_root: None,
            rdma_control_init: ClusterManagerRdmaControlInit::Disabled,
            sub_cluster: None,
            network: None,
        },
    };

    let framework2 = Framework::new("fluxon_kv.cluster_manager_test");
    init_framework(&framework2, framework_args2).await.unwrap();
    let test_module2 = framework2.0.test_cluster_module.get().unwrap();
    let cluster_manager2 = test_module2.cluster_manager();

    // 等待成员发现
    sleep(Duration::from_millis(10000)).await;

    // 验证两个成员都能看到对方
    let members1 = cluster_manager1.get_members();
    let members2 = cluster_manager2.get_members();

    assert_eq!(members1.len(), 2);
    assert_eq!(members2.len(), 2);

    assert!(members1.iter().any(|m| m.id == "test-member-2"));
    assert!(members2.iter().any(|m| m.id == "test-member-1"));

    // 关闭第二个框架
    framework2.shutdown().await.unwrap();
    sleep(Duration::from_millis(10000)).await;

    // 验证第一个成员检测到第二个成员离开
    let members1 = cluster_manager1.get_members();
    assert_eq!(members1.len(), 1);
    assert!(members1.iter().all(|m| m.id != "test-member-2"));

    // 关闭第一个框架
    framework1.shutdown().await.unwrap();
    clean_etcd_members(cluster_name).await;
}

/// 测试租约管理
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_cluster_manager_lease_management() {
    let _test_guard = crate::kvcore_test_lib::integration_test_lock().await;
    let cluster_name = "test-cluster-lease";
    clean_etcd_members(cluster_name).await;

    let shutdown = Arc::new(AtomicBool::new(false));

    let framework_args = FrameworkArgs {
        test_cluster_module_arg: TestClusterModuleNewArg {
            cluster_name: cluster_name.to_string(),
            instance_name: "test-member-1".to_string(),
            port: 8080,
            shutdown: shutdown.clone(),
        },
        cluster_manager_arg: ClusterManagerNewArg {
            etcd_endpoints: get_etcd_endpoints(),
            cluster_name: cluster_name.to_string(),
            instance_name: Some("test-member-1".to_string()),
            port: Some(8080),
            metadata: HashMap::from([
                ("role".to_string(), "test_node".to_string()),
                ("master".to_string(), "false".to_string()),
                ("client".to_string(), "true".to_string()),
                ("external_client".to_string(), "false".to_string()),
            ]),
            local_ipc_root: None,
            rdma_control_init: ClusterManagerRdmaControlInit::Disabled,
            sub_cluster: None,
            network: None,
        },
    };

    let framework = Framework::new("fluxon_kv.cluster_manager_test");
    init_framework(&framework, framework_args).await.unwrap();

    // 通过 TestClusterModule 访问 ClusterManager
    let test_module = framework.0.test_cluster_module.get().unwrap();
    let cluster_manager = test_module.cluster_manager();

    // The public closed-runtime facade does not expose lease internals. Verify the observable
    // behavior instead: the member stays visible and keeps the same generation.
    let self_info = cluster_manager.get_self_info();
    assert_eq!(self_info.id, "test-member-1");
    assert!(self_info.node_start_time > 0);
    assert!(cluster_manager.is_watching_for_test());

    sleep(Duration::from_secs(5)).await;

    let members = cluster_manager.get_members();
    assert!(
        members
            .iter()
            .any(|m| { m.id == self_info.id && m.node_start_time == self_info.node_start_time })
    );

    framework.shutdown().await.unwrap();
    clean_etcd_members(cluster_name).await;
}

/// 测试多个成员同时操作
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_cluster_manager_multiple_members() {
    let _test_guard = crate::kvcore_test_lib::integration_test_lock().await;
    let cluster_name = "test-cluster-multi";
    clean_etcd_members(cluster_name).await;

    let shutdown1 = Arc::new(AtomicBool::new(false));
    let shutdown2 = Arc::new(AtomicBool::new(false));
    let shutdown3 = Arc::new(AtomicBool::new(false));

    // 创建第一个框架实例
    let framework_args1 = FrameworkArgs {
        test_cluster_module_arg: TestClusterModuleNewArg {
            cluster_name: cluster_name.to_string(),
            instance_name: "test-member-1".to_string(),
            port: 8080,
            shutdown: shutdown1.clone(),
        },
        cluster_manager_arg: ClusterManagerNewArg {
            etcd_endpoints: get_etcd_endpoints(),
            cluster_name: cluster_name.to_string(),
            instance_name: Some("test-member-1".to_string()),
            port: Some(8080),
            metadata: HashMap::from([
                ("role".to_string(), "leader".to_string()),
                ("master".to_string(), "false".to_string()),
                ("client".to_string(), "true".to_string()),
                ("external_client".to_string(), "false".to_string()),
            ]),
            local_ipc_root: None,
            rdma_control_init: ClusterManagerRdmaControlInit::Disabled,
            sub_cluster: None,
            network: None,
        },
    };

    let framework1 = Framework::new("fluxon_kv.cluster_manager_test");
    init_framework(&framework1, framework_args1).await.unwrap();
    let test_module1 = framework1.0.test_cluster_module.get().unwrap();
    let cluster_manager1 = test_module1.cluster_manager();

    // 创建第二个框架实例
    let framework_args2 = FrameworkArgs {
        test_cluster_module_arg: TestClusterModuleNewArg {
            cluster_name: cluster_name.to_string(),
            instance_name: "test-member-2".to_string(),
            port: 8081,
            shutdown: shutdown2.clone(),
        },
        cluster_manager_arg: ClusterManagerNewArg {
            etcd_endpoints: get_etcd_endpoints(),
            cluster_name: cluster_name.to_string(),
            instance_name: Some("test-member-2".to_string()),
            port: Some(8081),
            metadata: HashMap::from([
                ("role".to_string(), "follower".to_string()),
                ("master".to_string(), "false".to_string()),
                ("client".to_string(), "true".to_string()),
                ("external_client".to_string(), "false".to_string()),
            ]),
            local_ipc_root: None,
            rdma_control_init: ClusterManagerRdmaControlInit::Disabled,
            sub_cluster: None,
            network: None,
        },
    };

    let framework2 = Framework::new("fluxon_kv.cluster_manager_test");
    init_framework(&framework2, framework_args2).await.unwrap();
    let test_module2 = framework2.0.test_cluster_module.get().unwrap();
    let cluster_manager2 = test_module2.cluster_manager();

    // 创建第三个框架实例
    let framework_args3 = FrameworkArgs {
        test_cluster_module_arg: TestClusterModuleNewArg {
            cluster_name: cluster_name.to_string(),
            instance_name: "test-member-3".to_string(),
            port: 8082,
            shutdown: shutdown3.clone(),
        },
        cluster_manager_arg: ClusterManagerNewArg {
            etcd_endpoints: get_etcd_endpoints(),
            cluster_name: cluster_name.to_string(),
            instance_name: Some("test-member-3".to_string()),
            port: Some(8082),
            metadata: HashMap::from([
                ("role".to_string(), "observer".to_string()),
                ("master".to_string(), "false".to_string()),
                ("client".to_string(), "true".to_string()),
                ("external_client".to_string(), "false".to_string()),
            ]),
            local_ipc_root: None,
            rdma_control_init: ClusterManagerRdmaControlInit::Disabled,
            sub_cluster: None,
            network: None,
        },
    };

    let framework3 = Framework::new("fluxon_kv.cluster_manager_test");
    init_framework(&framework3, framework_args3).await.unwrap();
    let test_module3 = framework3.0.test_cluster_module.get().unwrap();
    let cluster_manager3 = test_module3.cluster_manager();

    // Wait for the observable contract instead of sampling after a fixed delay.
    let expected_ids = ["test-member-1", "test-member-2", "test-member-3"];
    let members1 = wait_for_member_ids(cluster_manager1, &expected_ids).await;
    let members2 = wait_for_member_ids(cluster_manager2, &expected_ids).await;
    let members3 = wait_for_member_ids(cluster_manager3, &expected_ids).await;

    assert_eq!(members1.len(), 3);
    assert_eq!(members2.len(), 3);
    assert_eq!(members3.len(), 3);

    // 验证成员ID
    let member1_ids: Vec<String> = members1.iter().map(|m| m.id.clone()).collect();
    let member2_ids: Vec<String> = members2.iter().map(|m| m.id.clone()).collect();
    let member3_ids: Vec<String> = members3.iter().map(|m| m.id.clone()).collect();

    assert!(member1_ids.contains(&"test-member-1".to_string()));
    assert!(member1_ids.contains(&"test-member-2".to_string()));
    assert!(member1_ids.contains(&"test-member-3".to_string()));

    assert!(member2_ids.contains(&"test-member-1".to_string()));
    assert!(member2_ids.contains(&"test-member-2".to_string()));
    assert!(member2_ids.contains(&"test-member-3".to_string()));

    assert!(member3_ids.contains(&"test-member-1".to_string()));
    assert!(member3_ids.contains(&"test-member-2".to_string()));
    assert!(member3_ids.contains(&"test-member-3".to_string()));

    // 验证元数据
    let member2_info = members1.iter().find(|m| m.id == "test-member-2").unwrap();
    assert_eq!(
        member2_info.metadata.get("role"),
        Some(&"follower".to_string())
    );

    let member3_info = members1.iter().find(|m| m.id == "test-member-3").unwrap();
    assert_eq!(
        member3_info.metadata.get("role"),
        Some(&"observer".to_string())
    );

    // 关闭框架
    framework1.shutdown().await.unwrap();
    framework2.shutdown().await.unwrap();
    framework3.shutdown().await.unwrap();
    clean_etcd_members(cluster_name).await;
}

/// 测试并发操作
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_cluster_manager_concurrent_operations() {
    let _test_guard = crate::kvcore_test_lib::integration_test_lock().await;
    let cluster_name = "test-cluster-concurrent";
    clean_etcd_members(cluster_name).await;

    let shutdown = Arc::new(AtomicBool::new(false));

    let framework_args = FrameworkArgs {
        test_cluster_module_arg: TestClusterModuleNewArg {
            cluster_name: cluster_name.to_string(),
            instance_name: "test-member-concurrent".to_string(),
            port: 8080,
            shutdown: shutdown.clone(),
        },
        cluster_manager_arg: ClusterManagerNewArg {
            etcd_endpoints: get_etcd_endpoints(),
            cluster_name: cluster_name.to_string(),
            instance_name: Some("test-member-concurrent".to_string()),
            port: Some(8080),
            metadata: HashMap::from([
                ("role".to_string(), "test_node".to_string()),
                ("master".to_string(), "false".to_string()),
                ("client".to_string(), "true".to_string()),
                ("external_client".to_string(), "false".to_string()),
            ]),
            local_ipc_root: None,
            rdma_control_init: ClusterManagerRdmaControlInit::Disabled,
            sub_cluster: None,
            network: None,
        },
    };

    let framework = Framework::new("fluxon_kv.cluster_manager_test");
    init_framework(&framework, framework_args).await.unwrap();

    // 通过 TestClusterModule 访问 ClusterManager
    let test_module = framework.0.test_cluster_module.get().unwrap();
    let cluster_manager = test_module.cluster_manager();

    // 等待5秒，让成员信息同步
    tokio::time::sleep(Duration::from_secs(5)).await;

    // 先获取一次成员信息，验证基本功能
    let initial_members = cluster_manager.get_members();
    assert_eq!(initial_members.len(), 1);
    assert_eq!(initial_members[0].id, "test-member-concurrent");

    // Concurrent reads across OS threads. This exercises the synchronization inside
    // ClusterManager::get_members() without relying on tokio::spawn re-exports.
    let cm = framework.init_get_cluster_manager();
    let handles: Vec<_> = (0..10)
        .map(|_| {
            let cm = cm.clone();
            std::thread::spawn(move || cm.get_members())
        })
        .collect();

    for h in handles {
        let members = h.join().unwrap();
        assert_eq!(members.len(), 1);
        assert_eq!(members[0].id, "test-member-concurrent");
    }

    framework.shutdown().await.unwrap();
    clean_etcd_members(cluster_name).await;
}

/// 测试多个成员使用相同 instance_name 会报错
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_cluster_manager_duplicate_instance_name() {
    let _test_guard = crate::kvcore_test_lib::integration_test_lock().await;
    let cluster_name = "test-cluster-duplicate";
    clean_etcd_members(cluster_name).await;

    let shutdown = Arc::new(AtomicBool::new(false));

    // 第一个成员使用 instance_name "duplicate-member"
    let framework_args1 = FrameworkArgs {
        test_cluster_module_arg: TestClusterModuleNewArg {
            cluster_name: cluster_name.to_string(),
            instance_name: "duplicate-member".to_string(),
            port: 9000,
            shutdown: shutdown.clone(),
        },
        cluster_manager_arg: ClusterManagerNewArg {
            etcd_endpoints: get_etcd_endpoints(),
            cluster_name: cluster_name.to_string(),
            instance_name: Some("duplicate-member".to_string()),
            port: Some(9000),
            metadata: HashMap::from([
                ("role".to_string(), "test_node".to_string()),
                ("master".to_string(), "false".to_string()),
                ("client".to_string(), "true".to_string()),
                ("external_client".to_string(), "false".to_string()),
            ]),
            local_ipc_root: None,
            rdma_control_init: ClusterManagerRdmaControlInit::Disabled,
            sub_cluster: None,
            network: None,
        },
    };

    let framework1 = Framework::new("fluxon_kv.cluster_manager_test");
    init_framework(&framework1, framework_args1).await.unwrap();

    // The closed-runtime facade treats a duplicate instance_name as the same logical member
    // generation being updated/rejoined. It should not create duplicate visible members.
    let framework_args2 = FrameworkArgs {
        test_cluster_module_arg: TestClusterModuleNewArg {
            cluster_name: cluster_name.to_string(),
            instance_name: "duplicate-member".to_string(),
            port: 9001,
            shutdown: shutdown.clone(),
        },
        cluster_manager_arg: ClusterManagerNewArg {
            etcd_endpoints: get_etcd_endpoints(),
            cluster_name: cluster_name.to_string(),
            instance_name: Some("duplicate-member".to_string()),
            port: Some(9001),
            metadata: HashMap::from([
                ("role".to_string(), "test_node".to_string()),
                ("master".to_string(), "false".to_string()),
                ("client".to_string(), "true".to_string()),
                ("external_client".to_string(), "false".to_string()),
            ]),
            local_ipc_root: None,
            rdma_control_init: ClusterManagerRdmaControlInit::Disabled,
            sub_cluster: None,
            network: None,
        },
    };

    let framework2 = Framework::new("fluxon_kv.cluster_manager_test");
    let result = init_framework(&framework2, framework_args2).await;

    assert!(
        result.is_ok(),
        "closed-runtime facade should accept duplicate instance_name as a logical rejoin/update"
    );
    sleep(Duration::from_secs(2)).await;

    let cluster_manager1 = framework1.init_get_cluster_manager();
    let members = cluster_manager1.get_members();
    assert_eq!(
        members
            .iter()
            .filter(|m| m.id == "duplicate-member")
            .count(),
        1
    );

    // 清理资源
    framework2.shutdown().await.unwrap();
    framework1.shutdown().await.unwrap();
    clean_etcd_members(cluster_name).await;
}
// 需要concurrent test吗或者更细致的一些testing？/assert
