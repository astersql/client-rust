// Copyright 2026 AsterSQL.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use crate::rpc_interceptor::{self, RpcInterceptor};
use crate::{Error, Result};

struct PdGuard {
    calls: Arc<AtomicUsize>,
}

impl RpcInterceptor for PdGuard {
    fn before_pd(&self, method: &str) -> Result<()> {
        if !method.starts_with("/aster-test/") {
            return Ok(());
        }
        self.calls.fetch_add(1, Ordering::SeqCst);
        if method == "/aster-test/blocked" {
            return Err(Error::GrpcAPI(tonic::Status::permission_denied(
                "blocked by test guard",
            )));
        }
        Ok(())
    }
}

#[test]
fn pd_hooks_run_before_rpc_and_unregister_with_their_guard() {
    let calls = Arc::new(AtomicUsize::new(0));
    let registration = rpc_interceptor::register(Arc::new(PdGuard {
        calls: Arc::clone(&calls),
    }));

    rpc_interceptor::before_pd("/aster-test/allowed").unwrap();
    let error = rpc_interceptor::before_pd("/aster-test/blocked").unwrap_err();
    assert!(matches!(
        error,
        Error::GrpcAPI(ref status) if status.code() == tonic::Code::PermissionDenied
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 2);

    drop(registration);
    rpc_interceptor::before_pd("/aster-test/blocked").unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 2);
}
