use std::sync::Arc;

use async_trait::async_trait;
use fluxon_framework::{LogicalModule, ResourceRegistry, define_framework, define_module};

// `define_framework!` expands `Framework::shutdown()` with an unqualified `AnyResult`.
// Define it locally to keep the macro call site self-contained.
pub type AnyResult<T> = fluxon_framework::AnyResult<T>;

/// FS root module used to define an independent framework boundary for FS.
///
/// Design notes (causal chain):
/// - FS owns long-running background tasks (pollers/samplers, config fetch loops, etc).
/// - These tasks must be registered into a task registry so shutdown can join them deterministically.
/// - FS must not rely on "someone else's framework" as its lifecycle boundary, otherwise the
///   lifecycle semantics drift across service layers (Rust service vs PyO3 embedding).
pub struct FsRootModule;

pub struct FsRootModuleNewArg;

#[derive(thiserror::Error, Debug)]
#[error("FsRootModuleError: {msg}")]
pub struct FsRootModuleError {
    msg: &'static str,
}

#[async_trait]
impl LogicalModule for FsRootModule {
    type View = FsRootModuleView;
    type NewArg = FsRootModuleNewArg;
    type Error = FsRootModuleError;

    fn name(&self) -> &str {
        "FsRootModule"
    }

    async fn shutdown(&self) -> Result<(), Self::Error> {
        Ok(())
    }
}

define_module!(FsRootModule);

define_framework!(fs_root: FsRootModule);

/// Construct a fully initialized FS framework within the current Tokio runtime.
///
/// This must be called from within a Tokio runtime context because the underlying framework
/// captures `tokio::runtime::Handle::try_current()` to implement spawn semantics.
pub fn new_fs_framework(name: impl Into<String>) -> Framework {
    let fw = Framework::new(name);
    fw.init_set_resource_registry(ResourceRegistry::new(0));
    fw.init_set_fs_root_module(Arc::new(FsRootModule));
    fw.init_attach_views();
    fw
}

/// Spawn one FS-owned actor and register its completion with the FS framework.
pub(crate) fn spawn_fs_task<F>(framework: &Framework, name: impl Into<String>, future: F) -> bool
where
    F: std::future::Future<Output = ()> + Send + 'static,
{
    framework.spawn_registered_boxed(name, Box::pin(future))
}
