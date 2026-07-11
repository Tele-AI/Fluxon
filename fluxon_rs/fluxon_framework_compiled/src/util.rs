use std::fmt::Debug;
use std::future::Future;
use std::ops::{Deref, DerefMut};
use std::pin::Pin;
use std::ptr::NonNull;
use std::task::{Context, Poll};

use parking_lot::MutexGuard;
use tokio::task::JoinHandle;

#[cfg(test)]
use tracing_subscriber::{Layer, layer::SubscriberExt, util::SubscriberInitExt};

#[cfg(test)]
pub fn test_tracing_start() {
    let my_filter = tracing_subscriber::filter::filter_fn(|v| {
        if let Some(mp) = v.module_path() {
            if mp.contains("async_raft") {
                return false;
            }
            if mp.contains("hyper") {
                return false;
            }
        }
        v.level() != &tracing::Level::TRACE
    });
    let my_layer = tracing_subscriber::fmt::layer();
    let _ = tracing_subscriber::registry()
        .with(my_layer.with_filter(my_filter))
        .try_init();
}

pub struct JoinHandleWrapper(pub JoinHandle<()>);

impl Future for JoinHandleWrapper {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match Pin::new(&mut self.0).poll(cx) {
            Poll::Ready(Ok(_)) => Poll::Ready(()),
            Poll::Ready(Err(e)) => {
                eprintln!("Task error: {}", e);
                Poll::Ready(())
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

pub enum WithBind<'a, T> {
    MutexGuard(MutexGuard<'a, T>),
    MutexGuardOpt(MutexGuard<'a, Option<T>>),
}

impl<T> WithBind<'_, T> {
    pub fn option_mut(&mut self) -> &mut Option<T> {
        match self {
            Self::MutexGuard(_) => unreachable!(),
            Self::MutexGuardOpt(g) => &mut *g,
        }
    }
}

impl<T> Deref for WithBind<'_, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        match self {
            Self::MutexGuard(g) => g,
            Self::MutexGuardOpt(g) => g.as_ref().unwrap(),
        }
    }
}

impl<T> DerefMut for WithBind<'_, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        match self {
            Self::MutexGuard(g) => g,
            Self::MutexGuardOpt(g) => g.as_mut().unwrap(),
        }
    }
}

pub struct StrUnsafeRef(usize, usize);

impl StrUnsafeRef {
    pub fn new(str: &str) -> StrUnsafeRef {
        StrUnsafeRef(str.as_ptr() as usize, str.len())
    }
    pub fn str<'a>(&self) -> &'a str {
        std::str::from_utf8(unsafe { std::slice::from_raw_parts(self.0 as *const u8, self.1) })
            .unwrap()
    }
}

pub struct TryUtf8VecU8(pub Vec<u8>);

impl Debug for TryUtf8VecU8 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let res = std::str::from_utf8(&self.0);
        match res {
            Ok(str) => write!(f, "{}", str),
            Err(_) => write!(f, "{:?}", &self.0),
        }
    }
}

pub struct SendNonNull<T>(pub NonNull<T>);
unsafe impl<T> Send for SendNonNull<T> {}

pub unsafe fn non_null<T>(v: &T) -> std::ptr::NonNull<T> {
    let ptr = v as *const T as *mut T;
    // SAFETY: caller upholds that `v` lives long enough and is non-null;
    // we simply cast it to a NonNull pointer for use in FFI/buffer views.
    unsafe { std::ptr::NonNull::new_unchecked(ptr) }
}

// A join handle returned by View::spawn that, when dropped without being awaited,
// enqueues the underlying JoinHandle into the framework's queue for unified shutdown join.
pub struct ViewSpawnHandle<T: crate::spawn::ViewSpawnExt + ?Sized> {
    name: String,
    handle: Option<JoinHandle<()>>,
    view: Option<std::sync::Arc<T>>,
}

impl<T> ViewSpawnHandle<T>
where
    T: crate::spawn::ViewSpawnExt + ?Sized,
{
    pub fn new<N: Into<String>>(name: N, handle: JoinHandle<()>, view: std::sync::Arc<T>) -> Self {
        Self {
            name: name.into(),
            handle: Some(handle),
            view: Some(view),
        }
    }
}

impl<T> std::future::Future for ViewSpawnHandle<T>
where
    T: crate::spawn::ViewSpawnExt + ?Sized,
{
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if let Some(ref mut handle) = self.handle {
            match Pin::new(handle).poll(cx) {
                Poll::Ready(Ok(_)) => {
                    self.handle.take();
                    Poll::Ready(())
                }
                Poll::Ready(Err(_e)) => {
                    self.handle.take();
                    Poll::Ready(())
                }
                Poll::Pending => Poll::Pending,
            }
        } else {
            Poll::Ready(())
        }
    }
}

impl<T> ViewSpawnHandle<T>
where
    T: crate::spawn::ViewSpawnExt + ?Sized,
{
    /// Abort the underlying task. After calling, the handle will not be enqueued on drop.
    pub fn abort(mut self) {
        if let Some(handle) = &self.handle {
            handle.abort();
        }
        self.handle = None;
    }
}

impl<T> Drop for ViewSpawnHandle<T>
where
    T: crate::spawn::ViewSpawnExt + ?Sized,
{
    fn drop(&mut self) {
        if let (Some(handle), Some(view)) = (self.handle.take(), self.view.take()) {
            let name = std::mem::take(&mut self.name);
            crate::spawn::ViewSpawnExt::push_join_handle(&*view, name, handle);
        }
    }
}
