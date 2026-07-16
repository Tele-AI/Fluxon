use async_trait::async_trait;
use fluxon_util::notify_state;
use std::sync::atomic::{AtomicBool, Ordering};

/// A runtime init resource latch managed by the framework.
///
/// Semantics:
/// - One-way: NotReady -> Ready (idempotent).
/// - No versioning / epochs.
/// - Used as an init-time scheduling gate (and optionally for runtime checks).
pub struct ResourceLatch {
    ready: AtomicBool,
    notify: ::tokio::sync::Notify,
}

impl ResourceLatch {
    fn new() -> Self {
        Self {
            ready: AtomicBool::new(false),
            notify: ::tokio::sync::Notify::new(),
        }
    }

    pub fn is_ready(&self) -> bool {
        self.ready.load(Ordering::Acquire)
    }

    pub fn mark_ready(&self) {
        // Notify only on the first transition to keep the operation cheap.
        if !self.ready.swap(true, Ordering::Release) {
            self.notify.notify_waiters();
        }
    }

    pub async fn wait_ready(&self) {
        notify_state::wait_until(&self.notify, || self.is_ready()).await;
    }
}

/// Registry of init resources for a single framework instance.
///
/// Indexing is intentionally usize-based to keep the framework independent
/// from per-crate generated ResourceId enums.
pub struct ResourceRegistry {
    latches: Vec<ResourceLatch>,
}

impl ResourceRegistry {
    pub fn new(resource_count: usize) -> Self {
        let mut latches = Vec::with_capacity(resource_count);
        for _ in 0..resource_count {
            latches.push(ResourceLatch::new());
        }
        Self { latches }
    }

    pub fn len(&self) -> usize {
        self.latches.len()
    }

    pub fn is_ready(&self, idx: usize) -> bool {
        self.latches
            .get(idx)
            .unwrap_or_else(|| panic!("resource idx out of range: idx={}, len={}", idx, self.len()))
            .is_ready()
    }

    pub fn mark_ready(&self, idx: usize) {
        self.latches
            .get(idx)
            .unwrap_or_else(|| panic!("resource idx out of range: idx={}, len={}", idx, self.len()))
            .mark_ready();
    }

    pub async fn wait_ready(&self, idx: usize) {
        self.latches
            .get(idx)
            .unwrap_or_else(|| panic!("resource idx out of range: idx={}, len={}", idx, self.len()))
            .wait_ready()
            .await;
    }
}

#[async_trait]
pub trait ResourceRegistryAccessTrait: Send + Sync {
    fn resource_registry(&self) -> &ResourceRegistry;
}
pub type AnyResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[async_trait]
pub trait LogicalModule: Send + Sync {
    type View: Send + Sync;
    type NewArg: Send + Sync;
    type Error: std::error::Error + Send + 'static;

    fn name(&self) -> &str;

    /// Attach the runtime view to a constructed module instance.
    ///
    /// The default implementation is a no-op for modules that don't store the view.
    fn attach_view(&self, _view: Self::View) {}

    /// Hook that runs before modules are finally closed.
    /// The framework broadcasts shutdown signals first, then calls this.
    /// Default no-op implementation for modules that don't need it.
    async fn before_shutdown(&self) -> Result<(), Self::Error> {
        Ok(())
    }
    async fn shutdown(&self) -> Result<(), Self::Error>;
}

#[macro_export]
macro_rules! define_module {
    ($module:ident $(, ($field:ident, $dep_type:ty))*) => {
        paste::paste! {

            // 定义模块的AccessTrait
            #[async_trait::async_trait]
            pub trait [<$module AccessTrait>]: Send + Sync {
                fn [<$module:snake>](&self) -> &$module;
            }

            // 创建一个新trait，将所有依赖的AccessTrait作为supertrait
            pub trait [<$module ViewTrait>]: Send + Sync $(+ [<$dep_type AccessTrait>])* + $crate::ResourceRegistryAccessTrait + fluxon_framework_compiled::shutdown::ViewShutdownExt + fluxon_framework_compiled::async_panic::AsyncPanicSendExt + fluxon_framework_compiled::spawn::ViewSpawnExt {}
            // // 创建一个新trait，将所有依赖的AccessTrait作为supertrait
            // pub trait [<$module ViewTrait>]: Send + Sync $(+ [<$dep_type AccessTrait>])* + fluxon_framework_compiled::shutdown::ViewShutdownExt {}




            // 为所有实现了必要AccessTrait的类型自动实现ViewTrait
            impl<T> [<$module ViewTrait>] for T
            where
                T: Send + Sync $(+ [<$dep_type AccessTrait>])* + $crate::ResourceRegistryAccessTrait + fluxon_framework_compiled::shutdown::ViewShutdownExt + fluxon_framework_compiled::async_panic::AsyncPanicSendExt + fluxon_framework_compiled::spawn::ViewSpawnExt
            {}
            // // 为所有实现了必要AccessTrait的类型自动实现ViewTrait
            // impl<T> [<$module ViewTrait>] for T
            // where
            //     T: Send + Sync $(+ [<$dep_type AccessTrait>])* + fluxon_framework_compiled::shutdown::ViewShutdownExt
            // {}

            // 定义View结构体
            #[derive(Clone)]
            pub struct [<$module View>] {
                pub view: std::sync::Weak<dyn [<$module ViewTrait>]>
            }

            // View实现
            impl [<$module View>] {
                pub fn new(view: &std::sync::Arc<dyn [<$module ViewTrait>]>) -> Self {
                    //println!("new view of {}", stringify!($module));
                    Self {
                        view: std::sync::Arc::downgrade(view)
                    }
                }

                pub fn try_upgrade(&self) -> Option<fluxon_framework_compiled::upgrade_view_guard::UpgradeViewGuard<dyn [<$module ViewTrait>]>> {
                    self.view
                        .upgrade()
                        .map(|arc| fluxon_framework_compiled::upgrade_view_guard::UpgradeViewGuard::new(arc))
                }

                pub fn resource_registry(&self) -> &$crate::ResourceRegistry {
                    let arc_view = self.view.upgrade().expect(&format!(
                        "view of module {} has been dropped when accessing resource registry",
                        stringify!($module)
                    ));
                    unsafe {
                        // Internal-only access: FrameworkInner implements ResourceRegistryAccessTrait.
                        let ptr = std::ptr::NonNull::new(std::sync::Arc::as_ptr(&arc_view) as *const _ as *mut _)
                            .unwrap();
                        let view_ref: &dyn [<$module ViewTrait>] = ptr.as_ref();
                        let reg_ptr = std::ptr::NonNull::new(
                            view_ref.resource_registry() as *const _ as *mut _,
                        )
                        .unwrap();
                        reg_ptr.as_ref()
                    }
                }

                // 获取每个依赖模块
                $(
                    pub fn [<$dep_type:snake>](&self) -> &$dep_type {
                        //println!("accessing {}", stringify!($dep_type));
                        let arc_view = self.view.upgrade().expect(&format!("view of module {} has been dropped when accessing dependency {}", stringify!($module), stringify!($dep_type)));
                        let ret=unsafe {
                            // 直接访问，不需要安全检查，因为这是在框架内部使用
                            // Use associated function form for broad compiler compatibility
                            let ptr = std::ptr::NonNull::new(std::sync::Arc::as_ptr(&arc_view) as *const _ as *mut _).unwrap();
                            let view_ref: &dyn [<$module ViewTrait>] = ptr.as_ref();
                            let module_ptr=std::ptr::NonNull::new(view_ref.[<$dep_type:snake>]() as *const _ as *mut _).unwrap();
                            module_ptr.as_ref()
                        };
                        //println!("accessed {}", stringify!($dep_type));
                        ret
                    }
                )*

                pub fn register_shutdown_poller(&self) -> fluxon_framework_compiled::shutdown::ShutdownPoller {
                    use fluxon_framework_compiled::shutdown::ViewShutdownExt;

                    let view_ref: std::sync::Arc<dyn [<$module ViewTrait>]> = self.view.upgrade().unwrap();
                    view_ref.register_shutdown_poller()
                }

                pub fn register_shutdown_waiter(&self) -> fluxon_framework_compiled::shutdown::ShutdownWaiter {
                    use fluxon_framework_compiled::shutdown::ViewShutdownExt;
                    let view_ref: std::sync::Arc<dyn [<$module ViewTrait>]> = self.view.upgrade().unwrap();
                    view_ref.register_shutdown_waiter()
                }

                pub fn async_panic(&self, msg: String) {
                    use fluxon_framework_compiled::async_panic::AsyncPanicSendExt;
                    let view_ref: std::sync::Arc<dyn [<$module ViewTrait>]> = self.view.upgrade().unwrap();
                    view_ref.async_panic(msg);
                }

                pub fn runtime_num_workers(&self) -> usize {
                    let view_ref: std::sync::Arc<dyn [<$module ViewTrait>]> = self
                        .view
                        .upgrade()
                        .expect("view of module has been dropped before runtime_num_workers");
                    fluxon_framework_compiled::spawn::ViewSpawnExt::runtime_num_workers(&*view_ref)
                }

                pub fn spawn<F, N>(&self, name: N, fut: F) -> fluxon_framework_compiled::util::ViewSpawnHandle<dyn [<$module ViewTrait>]>
                where
                    F: std::future::Future<Output = ()> + Send + 'static,
                    N: Into<String>,
                {
                    let view_ref: std::sync::Arc<dyn [<$module ViewTrait>]> = self
                        .view
                        .upgrade()
                        .expect("view of module has been dropped before spawn");
                    let boxed: ::std::pin::Pin<
                        Box<dyn ::std::future::Future<Output = ()> + Send>,
                    > = Box::pin(fut);
                    let handle = fluxon_framework_compiled::spawn::ViewSpawnExt::spawn_boxed(
                        &*view_ref,
                        boxed,
                    );
                    fluxon_framework_compiled::util::ViewSpawnHandle::new(name, handle, view_ref)
                }
            }
        }
    };
}

#[macro_export]
macro_rules! define_framework {
    ($first:ident: $first_type:ty $(, $rest:ident: $rest_type:ty)*) => {
        paste::paste! {
            // expand the modules
            pub struct FrameworkInner {
                name: String,
                shutdown_notifier: fluxon_framework_compiled::shutdown::ShutdownNotifier,
                shutdown_poller: fluxon_framework_compiled::shutdown::ShutdownPoller,
                init0_stage_done: std::sync::atomic::AtomicBool,
                async_panic_sender: fluxon_framework_compiled::async_panic::AsyncPanicSend,
                async_panic_receiver: parking_lot::Mutex<Option<fluxon_framework_compiled::async_panic::AsyncPanicRecv>>,
                // Task registry handle (RwLock for fast read on push)
                task_registry_handle: parking_lot::RwLock<Option<fluxon_framework_compiled::task_registry::TaskRegistryHandle>>,
                // Handle to the Tokio runtime that owns all framework tasks.
                // It is captured when the framework is created and reused
                // for all subsequent View::spawn calls, so that even
                // non-runtime threads (e.g. FFI Drop handlers) can spawn
                // background tasks without panicking on missing reactor.
                runtime_handle: ::tokio::runtime::Handle,

                resource_registry: std::sync::OnceLock<$crate::ResourceRegistry>,

                [<$first_type:snake>]: std::sync::OnceLock<std::sync::Arc<$first_type>>,
                $(
                    [<$rest_type:snake>]: std::sync::OnceLock<std::sync::Arc<$rest_type>>,
                )*
            }

            #[derive(Clone)]
            pub struct Framework(std::sync::Arc<FrameworkInner>);

            impl Framework {
                pub fn new(name: impl Into<String>) -> Self {
                    let name = name.into();
                    let (async_panic_sender, async_panic_receiver) = fluxon_framework_compiled::async_panic::new_async_panic();
                    // Capture the current Tokio runtime handle. Framework
                    // depends on a Tokio runtime. If we cannot get the current
                    // runtime handle here, treat it as a programming error
                    // (consistent with later spawn behavior).
                    let runtime_handle = ::tokio::runtime::Handle::try_current()
                        .expect("Framework::new() must be called from within a Tokio runtime");
                    let inner = FrameworkInner{
                        name,
                        shutdown_notifier: fluxon_framework_compiled::shutdown::ShutdownNotifier::new(),
                        shutdown_poller: fluxon_framework_compiled::shutdown::ShutdownPoller::new(),
                        init0_stage_done: std::sync::atomic::AtomicBool::new(false),
                        async_panic_sender,
                        async_panic_receiver: parking_lot::Mutex::new(Some(async_panic_receiver)),
                        task_registry_handle: parking_lot::RwLock::new(None),
                        runtime_handle,
                        resource_registry: std::sync::OnceLock::new(),
                        [<$first_type:snake>]: std::sync::OnceLock::new(),
                        $(
                            [<$rest_type:snake>]: std::sync::OnceLock::new(),
                        )*
                    };
                    let this = Self(std::sync::Arc::new(inner));
                    // Start background task reaper for auto-cleanup of finished tasks.
                    let hdl = fluxon_framework_compiled::task_registry::TaskRegistry::start_background(30000);
                    *this.0.task_registry_handle.write() = Some(hdl);
                    this
                }

                pub fn name(&self) -> &str {
                    &self.0.name
                }

                pub async fn wait_shutdown_signal(&self) {
                    use limit_thirdparty::tokio;
                    let mut async_panic_receiver = self.0.async_panic_receiver.lock().take().expect(
                        "wait_shutdown_signal is deigned to call by only main thread or monitor thread, and should be called only once");
                    let mut shutdown_waiter = self.0.shutdown_notifier.listen();
                    let mut term_receiver=tokio::signal::unix_signal_only_allow_use_by_framework(tokio::signal::unix::SignalKind::terminate()).unwrap();
                    tokio::select!{
                        _ = shutdown_waiter.wait() => {
                            tracing::info!(framework=%self.name(), "received shutdown notifier");
                        }
                        // ctrl+c
                        _ = tokio::signal::ctrl_c_only_allow_use_by_framework() => {
                            tracing::info!(framework=%self.name(), "received ctrl+c");
                        }
                        // // sigterm
                        _ = term_receiver.recv() => {
                            tracing::info!(framework=%self.name(), "received sigterm");
                        }
                        // async panic
                        _ = async_panic_receiver.recv_and_panic() => {
                            tracing::error!(framework=%self.name(), "received async panic; need to fix some bugs");
                        }
                    };
                }
            }

            // 为Framework实现各个模块的AccessTrait
            #[async_trait::async_trait]
            impl [<$first_type AccessTrait>] for FrameworkInner {
                fn [<$first_type:snake>](&self) -> &$first_type {
                    if !self.init0_stage_done.load(std::sync::atomic::Ordering::Acquire) {
                        panic!("module {} not initialized after all modules init() done", stringify!($first_type));
                    }
                    self.[<$first_type:snake>]
                        .get()
                        .expect(&format!("module {} not initialized when view access", stringify!($first_type)))
                        .as_ref()
                }
            }

            $(
                #[async_trait::async_trait]
                impl [<$rest_type AccessTrait>] for FrameworkInner {
                    fn [<$rest_type:snake>](&self) -> &$rest_type {
                        if !self.init0_stage_done.load(std::sync::atomic::Ordering::Acquire) {
                            panic!("module {} not initialized after all modules init() done", stringify!($rest_type));
                        }
                        self.[<$rest_type:snake>]
                            .get()
                            .expect(&format!("module {} not initialized when view access", stringify!($rest_type)))
                            .as_ref()
                    }
                }
            )*

            impl fluxon_framework_compiled::shutdown::ViewShutdownExt for FrameworkInner {
                fn register_shutdown_waiter(&self) -> fluxon_framework_compiled::shutdown::ShutdownWaiter {
                    self.shutdown_notifier.listen()
                }

                fn register_shutdown_poller(&self) -> fluxon_framework_compiled::shutdown::ShutdownPoller {
                    self.shutdown_poller.clone()
                }
            }

            impl fluxon_framework_compiled::async_panic::AsyncPanicSendExt for FrameworkInner {
                fn async_panic(&self, msg: String) {
                    let handle = self.async_panic_sender.spawn_on(&self.runtime_handle, msg);
                    fluxon_framework_compiled::spawn::ViewSpawnExt::push_join_handle(
                        self,
                        "async_panic".to_string(),
                        handle,
                    );
                }
            }

            impl fluxon_framework_compiled::spawn::ViewSpawnExt for FrameworkInner {
                fn push_join_handle(&self, name: String, handle: ::tokio::task::JoinHandle<()>) {
                    let hdl_opt = self.task_registry_handle.read();
                    if let Some(hdl)=hdl_opt.as_ref(){
                        hdl.register(name, handle);
                    }
                    // let hdl = hdl_opt.as_ref().expect("task_registry_handle not initialized");
                }

                fn runtime_num_workers(&self) -> usize {
                    self.runtime_handle.metrics().num_workers()
                }

                fn spawn_boxed(
                    &self,
                    fut: ::std::pin::Pin<
                        Box<dyn ::std::future::Future<Output = ()> + Send>,
                    >,
                ) -> ::tokio::task::JoinHandle<()> {
                    self.runtime_handle.spawn(fut)
                }
            }

            impl $crate::ResourceRegistryAccessTrait for FrameworkInner {
                fn resource_registry(&self) -> &$crate::ResourceRegistry {
                    self.resource_registry.get().expect("resource registry not initialized")
                }
            }

            impl $crate::ResourceRegistryAccessTrait for Framework {
                fn resource_registry(&self) -> &$crate::ResourceRegistry {
                    $crate::ResourceRegistryAccessTrait::resource_registry(&*self.0)
                }
            }


            // Init-step DAG contexts use `Framework` directly as the spawn/shutdown core.
            // This keeps generated code simple (no need to expose FrameworkInner).
            impl fluxon_framework_compiled::shutdown::ViewShutdownExt for Framework {
                fn register_shutdown_waiter(&self) -> fluxon_framework_compiled::shutdown::ShutdownWaiter {
                    fluxon_framework_compiled::shutdown::ViewShutdownExt::register_shutdown_waiter(&*self.0)
                }

                fn register_shutdown_poller(&self) -> fluxon_framework_compiled::shutdown::ShutdownPoller {
                    fluxon_framework_compiled::shutdown::ViewShutdownExt::register_shutdown_poller(&*self.0)
                }
            }

            impl fluxon_framework_compiled::async_panic::AsyncPanicSendExt for Framework {
                fn async_panic(&self, msg: String) {
                    fluxon_framework_compiled::async_panic::AsyncPanicSendExt::async_panic(&*self.0, msg);
                }
            }

            impl fluxon_framework_compiled::spawn::ViewSpawnExt for Framework {
                fn push_join_handle(&self, name: String, handle: ::tokio::task::JoinHandle<()>) {
                    fluxon_framework_compiled::spawn::ViewSpawnExt::push_join_handle(&*self.0, name, handle);
                }

                fn runtime_num_workers(&self) -> usize {
                    fluxon_framework_compiled::spawn::ViewSpawnExt::runtime_num_workers(&*self.0)
                }

                fn spawn_boxed(
                    &self,
                    fut: ::std::pin::Pin<
                        Box<dyn ::std::future::Future<Output = ()> + Send>,
                    >,
                ) -> ::tokio::task::JoinHandle<()> {
                    fluxon_framework_compiled::spawn::ViewSpawnExt::spawn_boxed(&*self.0, fut)
                }
            }

            // Framework已经实现了所有AccessTrait，它自动实现了各ViewTrait

            // 定义框架参数结构体
            pub struct FrameworkArgs {
                pub [<$first _arg>]: [<$first_type NewArg>],
                $(
                    pub [<$rest _arg>]: [<$rest_type NewArg>],
                )*
            }


            // // 实现ViewTrait需要的方法（需要安全地访问其他模块）
            // impl [<$first_type ViewTrait>] for Framework {
            //     fn $first(&self) -> &$first_type {
            //         unsafe {
            //             let ptr = self.modules.as_ptr() as *const $first_type;
            //             &*ptr
            //         }
            //     }
            // }

            // $(
            //     impl [<$rest_type ViewTrait>] for Framework {
            //         fn $rest(&self) -> &$rest_type {
            //             unsafe {
            //                 let ptr = (self.modules.as_ptr().add(::std::mem::size_of::<$first_type>())) as *const $rest_type;
            //                 &*ptr
            //             }
            //         }
            //     }
            // )*

            // 实现FrameworkTrait
            impl Framework {
                pub(crate) fn init_set_resource_registry(&self, reg: $crate::ResourceRegistry) {
                    self.0
                        .resource_registry
                        .set(reg)
                        .unwrap_or_else(|_| panic!("resource registry already initialized"));
                }

                pub(crate) fn init_mark_views_ready(&self) {
                    // Views are available only after this barrier.
                    self.0
                        .init0_stage_done
                        .store(true, std::sync::atomic::Ordering::Release);
                }

                pub(crate) fn init_attach_views(&self) {
                    // Views are available only after this barrier.
                    self.0
                        .init0_stage_done
                        .store(true, std::sync::atomic::Ordering::Release);

                    // Bind each module's runtime view. Most modules override LogicalModule::attach_view
                    // to store it in an OnceLock; modules that don't need a view keep the default no-op.
                    $crate::LogicalModule::attach_view(
                        self.0
                            .[<$first_type:snake>]
                            .get()
                            .expect(&format!(
                                "module {} not initialized at attach_views barrier",
                                stringify!($first_type)
                            ))
                            .as_ref(),
                        self.[<$first _view>](),
                    );
                    $(
                        $crate::LogicalModule::attach_view(
                            self.0
                                .[<$rest_type:snake>]
                                .get()
                                .expect(&format!(
                                    "module {} not initialized at attach_views barrier",
                                    stringify!($rest_type)
                                ))
                                .as_ref(),
                            self.[<$rest _view>](),
                        );
                    )*
                }

                pub(crate) fn [<init_set_ $first_type:snake>](&self, m: std::sync::Arc<$first_type>) {
                    self.0
                        .[<$first_type:snake>]
                        .set(m)
                        .unwrap_or_else(|_| {
                            panic!("module {} already initialized", stringify!($first_type))
                        });
                }

                pub(crate) fn [<init_get_ $first_type:snake>](&self) -> std::sync::Arc<$first_type> {
                    self.0
                        .[<$first_type:snake>]
                        .get()
                        .expect(&format!(
                            "module {} not initialized when required by init dag",
                            stringify!($first_type)
                        ))
                        .clone()
                }

                $(
                    pub(crate) fn [<init_set_ $rest_type:snake>](&self, m: std::sync::Arc<$rest_type>) {
                        self.0
                            .[<$rest_type:snake>]
                            .set(m)
                            .unwrap_or_else(|_| {
                                panic!("module {} already initialized", stringify!($rest_type))
                            });
                    }

	                    pub(crate) fn [<init_get_ $rest_type:snake>](&self) -> std::sync::Arc<$rest_type> {
	                        self.0
	                            .[<$rest_type:snake>]
	                            .get()
	                            .expect(&format!(
	                                "module {} not initialized when required by init dag",
	                                stringify!($rest_type)
	                            ))
	                            .clone()
	                    }
	                )*

	                /// Broadcast shutdown to background tasks without waiting for joins.
	                ///
	                /// English note:
	                /// - This is designed for "best-effort" cleanup in FFI / Drop paths where blocking is unsafe.
	                /// - For a graceful shutdown with module joins, use `shutdown().await`.
	                pub fn request_shutdown(&self) {
	                    self.0.shutdown_notifier.shutdown();
	                    self.0.shutdown_poller.shutdown();
	                }

	                pub async fn shutdown(&self) -> AnyResult<()> {
	                    tracing::info!(framework=%self.name(), "shutdown begin");
	                    // Broadcast shutdown to background tasks and pollers first.
	                    self.0.shutdown_notifier.shutdown();
                    self.0.shutdown_poller.shutdown();

                    // Only shut down modules that were actually constructed in the selected init-DAG variant.
                    //
                    // Rationale: with tag/variant-based init DAG, a Framework type may contain modules that are
                    // intentionally absent (OnceLock unset) for a given runtime role.
                    if let Some(m) = self.0.[<$first_type:snake>].get() {
                        m.before_shutdown()
                            .await
                            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
                    }
                    $(
                        if let Some(m) = self.0.[<$rest_type:snake>].get() {
                            m.before_shutdown()
                                .await
                                .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
                        }
                    )*


                    // Then run final shutdown for each module.
                    if let Some(m) = self.0.[<$first_type:snake>].get() {
                        m.shutdown()
                            .await
                            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
                    }
                    $(
                        if let Some(m) = self.0.[<$rest_type:snake>].get() {
                            m.shutdown()
                                .await
                                .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
                        }
                    )*

                    // Task registry completed joining; no per-task join needed here.
                    // Ask task registry to stop and join tasks internally (actor-style ack).
                    let task_registry_handle = { self.0.task_registry_handle.write().take() };
                    if let Some(h) = task_registry_handle {
                        h.stop_and_join().await;
                    }

                    let banner = format!(
                        "\n===============================================\n\
||                                           ||\n\
||     🚀  Shutdown Complete ({})  🚀     ||\n\
||                                           ||\n\
||               👋  Goodbye!  👋            ||\n\
||                                           ||\n\
===============================================\n",
                        self.name(),
                    );
                    tracing::info!("{banner}");


                    Ok(())
                }

                pub fn [<$first _view>](&self) -> [<$first_type View>] {
                    //println!("getting view of {}", stringify!($first_type));
                    // 先克隆self得到Framework实例，再装箱为Arc
                    let framework = self.0.clone();
                    //println!("got arc of framework");
                    let framework_arc: std::sync::Arc<dyn [<$first_type ViewTrait>]> = framework;
                    //println!("got dyn view trait of {}", stringify!($first_type));
                    [<$first_type View>]::new(&framework_arc)
                }

                $(
                    pub fn [<$rest _view>](&self) -> [<$rest_type View>] {
                        //println!("getting view of {}", stringify!($rest_type));
                        // 先克隆self得到Framework实例，再装箱为Arc
                        let framework = self.0.clone();
                        //println!("got arc of framework");
                        let framework_arc: std::sync::Arc<dyn [<$rest_type ViewTrait>]> = framework;
                        //println!("got dyn view trait of {}", stringify!($rest_type));
                        [<$rest_type View>]::new(&framework_arc)
                    }
                )*
            }
        }
    };
}

#[cfg(test)]
mod tests {
    use super::*;
    use limit_thirdparty::tokio;
    use std::sync::Mutex;
    use thiserror::Error;

    // 定义测试模块的错误类型
    #[derive(Error, Debug)]
    pub enum TestModuleError {
        #[error("模块初始化失败: {reason}")]
        InitializationFailed { reason: String },
        #[error("模块关闭失败: {reason}")]
        ShutdownFailed { reason: String },
        #[error("模块操作错误: {0}")]
        OperationError(String),
    }

    // 先定义 TestModuleB
    pub struct TestModuleB {
        _phantom: std::marker::PhantomData<()>,
        pub initialized: Mutex<bool>,
        pub shutdown: Mutex<bool>,
    }

    #[async_trait]
    impl LogicalModule for TestModuleB {
        type View = TestModuleBView;
        type NewArg = TestModuleBNewArg;
        type Error = TestModuleError;

        fn name(&self) -> &str {
            "TestModuleB"
        }

        async fn shutdown(&self) -> Result<(), Self::Error> {
            *self.shutdown.lock().unwrap() = true;
            Ok(())
        }
    }

    define_module!(TestModuleB);

    // 然后定义 TestModuleA
    pub struct TestModuleA {
        _phantom: std::marker::PhantomData<()>,
        pub initialized: Mutex<bool>,
        pub shutdown: Mutex<bool>,
    }

    #[async_trait]
    impl LogicalModule for TestModuleA {
        type View = TestModuleAView;
        type NewArg = TestModuleANewArg;
        type Error = TestModuleError;

        fn name(&self) -> &str {
            "TestModuleA"
        }

        async fn shutdown(&self) -> Result<(), Self::Error> {
            *self.shutdown.lock().unwrap() = true;
            Ok(())
        }
    }

    define_module!(TestModuleA, (a, TestModuleA), (b, TestModuleB));

    // 定义测试模块的NewArg
    pub struct TestModuleANewArg;
    pub struct TestModuleBNewArg;

    define_framework! {
        a: TestModuleA,
        b: TestModuleB
    }

    #[test]
    fn test_module_size() {
        //println!("Size of TestModuleA: {}", std::mem::size_of::<TestModuleA>());
        //println!("Size of TestModuleB: {}", std::mem::size_of::<TestModuleB>());
        //println!("Size of TestModuleAView: {}", std::mem::size_of::<TestModuleAView>());
        //println!("Size of TestModuleBView: {}", std::mem::size_of::<TestModuleBView>());
    }

    #[tokio::test]
    async fn test_framework() {
        // init tracing
        let _ = tracing_subscriber::fmt::try_init();

        //println!("Starting test_framework");

        let fw = Framework::new("fluxon_framework.test");
        //println!("Created new framework");

        // Initialize via the init-step DAG style:
        // - construct modules first
        // - then attach views once at the barrier
        fw.init_set_resource_registry(ResourceRegistry::new(0));
        fw.init_set_test_module_a(std::sync::Arc::new(TestModuleA {
            _phantom: std::marker::PhantomData,
            initialized: Mutex::new(true),
            shutdown: Mutex::new(false),
        }));
        fw.init_set_test_module_b(std::sync::Arc::new(TestModuleB {
            _phantom: std::marker::PhantomData,
            initialized: Mutex::new(true),
            shutdown: Mutex::new(false),
        }));
        fw.init_attach_views();
        //println!("Initialized framework");

        // 通过 framework 获取 TestModuleA 的 view
        let view = fw.a_view();

        // 验证 TestModuleA 已初始化
        assert!(*view.test_module_a().initialized.lock().unwrap());
        assert!(!*view.test_module_a().shutdown.lock().unwrap());

        // 验证 TestModuleB 已初始化
        assert!(*view.test_module_b().initialized.lock().unwrap());
        assert!(!*view.test_module_b().shutdown.lock().unwrap());

        //println!("TestModuleB name: {}", view.test_module_b().name());
        assert_eq!(view.test_module_b().name(), "TestModuleB");

        // 使用trait方法关闭
        fw.shutdown().await.unwrap();
        //println!("Shutdown framework");

        // 验证模块已关闭
        let view = fw.a_view();
        assert!(*view.test_module_a().shutdown.lock().unwrap());
        assert!(*view.test_module_b().shutdown.lock().unwrap());
    }
}
