// Copyright 2026 AsterSQL.

//! Scoped interception of actual TiKV requests and their transport responses.
use crate::Result;
use std::any::Any;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, RwLock};
use std::time::Duration;

pub trait RpcInterceptor: Send + Sync + 'static {
    fn before(&self, _request: &dyn Any, _timeout: &mut Duration) -> Result<()> {
        Ok(())
    }
    fn after(&self, _request: &dyn Any, _response: &mut Result<Box<dyn Any>>) {}
    fn delay(&self, _request: &dyn Any) -> Duration {
        Duration::ZERO
    }
    fn prewrite_batch_size(&self, _start_ts: u64) -> Option<usize> {
        None
    }
    fn lock_ttl(&self, _start_ts: u64) -> Option<u64> {
        None
    }
    fn disable_heartbeat(&self, _start_ts: u64) -> bool {
        false
    }
}
static NEXT_ID: AtomicU64 = AtomicU64::new(1);
static INTERCEPTORS: LazyLock<RwLock<Vec<(u64, Arc<dyn RpcInterceptor>)>>> =
    LazyLock::new(|| RwLock::new(Vec::new()));

pub struct InterceptorGuard(u64);
impl Drop for InterceptorGuard {
    fn drop(&mut self) {
        INTERCEPTORS
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .retain(|(id, _)| *id != self.0);
    }
}
/// The guard unregisters only its own hook, even if guards are dropped out of order.
pub fn register(interceptor: Arc<dyn RpcInterceptor>) -> InterceptorGuard {
    let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
    INTERCEPTORS
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .push((id, interceptor));
    InterceptorGuard(id)
}
pub(crate) fn snapshot() -> Vec<Arc<dyn RpcInterceptor>> {
    INTERCEPTORS
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .iter()
        .map(|(_, hook)| Arc::clone(hook))
        .collect()
}
pub(crate) fn prewrite_batch_size(start_ts: u64, default: u64) -> u64 {
    snapshot()
        .iter()
        .filter_map(|hook| hook.prewrite_batch_size(start_ts).map(|size| size as u64))
        .min()
        .unwrap_or(default)
        .max(1)
}
pub(crate) fn lock_ttl(start_ts: u64, default: u64) -> u64 {
    snapshot()
        .iter()
        .filter_map(|hook| hook.lock_ttl(start_ts))
        .min()
        .unwrap_or(default)
}
pub(crate) fn disable_heartbeat(start_ts: u64) -> bool {
    snapshot()
        .iter()
        .any(|hook| hook.disable_heartbeat(start_ts))
}
