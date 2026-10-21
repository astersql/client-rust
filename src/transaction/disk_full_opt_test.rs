// Copyright 2026 AsterSQL.

use std::any::Any;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use crate::mock::{MockKvClient, MockPdClient};
use crate::proto::kvrpcpb;
use crate::proto::pdpb::Timestamp;
use crate::request::Keyspace;
use crate::transaction::HeartbeatOption;
use crate::{DiskFullOpt, Transaction, TransactionOptions};

#[tokio::test]
async fn disk_full_option_is_attached_to_prewrite_and_commit() {
    let prewrites = Arc::new(AtomicUsize::new(0));
    let commits = Arc::new(AtomicUsize::new(0));
    let observed_prewrites = Arc::clone(&prewrites);
    let observed_commits = Arc::clone(&commits);
    let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
        move |request: &dyn Any| {
            if let Some(request) = request.downcast_ref::<kvrpcpb::PrewriteRequest>() {
                assert_eq!(
                    request
                        .context
                        .as_ref()
                        .map(|context| context.disk_full_opt),
                    Some(DiskFullOpt::AllowedOnAlmostFull as i32)
                );
                observed_prewrites.fetch_add(1, Ordering::SeqCst);
                return Ok(Box::<kvrpcpb::PrewriteResponse>::default() as Box<dyn Any>);
            }
            if let Some(request) = request.downcast_ref::<kvrpcpb::CommitRequest>() {
                assert_eq!(
                    request
                        .context
                        .as_ref()
                        .map(|context| context.disk_full_opt),
                    Some(DiskFullOpt::AllowedOnAlmostFull as i32)
                );
                observed_commits.fetch_add(1, Ordering::SeqCst);
                return Ok(Box::<kvrpcpb::CommitResponse>::default() as Box<dyn Any>);
            }
            panic!("unexpected request")
        },
    )));
    let mut transaction = Transaction::new(
        Timestamp::default(),
        pd_client,
        TransactionOptions::new_optimistic().heartbeat_option(HeartbeatOption::NoHeartbeat),
        Keyspace::Disable,
    );
    transaction.set_disk_full_opt(DiskFullOpt::AllowedOnAlmostFull);
    transaction
        .put("key".to_owned(), "value".to_owned())
        .await
        .expect("buffer write");
    transaction.commit().await.expect("commit transaction");

    assert_eq!(prewrites.load(Ordering::SeqCst), 1);
    assert_eq!(commits.load(Ordering::SeqCst), 1);
}
