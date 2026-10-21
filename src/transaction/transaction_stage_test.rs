// Copyright 2026 AsterSQL.

use std::sync::Arc;

use crate::mock::{MockKvClient, MockPdClient};
use crate::request::Keyspace;
use crate::TimestampExt;
use crate::{Key, Timestamp, TransactionOptions};

#[tokio::test]
async fn statement_stage_cleanup_restores_prior_writes_and_disposes_new_writes() {
    let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(|_| {
        panic!("staged mutation checkpoint must not require a TiKV RPC")
    })));
    let mut txn = super::Transaction::new(
        Timestamp::from_version(100),
        pd_client,
        TransactionOptions::new_optimistic(),
        Keyspace::Disable,
    );
    txn.stage_put("prior".to_owned(), "10").await.unwrap();
    let stage = txn.stage_statement();
    txn.stage_put("new".to_owned(), "20").await.unwrap();
    assert_eq!(
        txn.buffer.get(&Key::from(b"new".to_vec())),
        Some(b"20".to_vec())
    );
    txn.cleanup_statement(stage).await.unwrap();
    assert_eq!(
        txn.buffer.get(&Key::from(b"prior".to_vec())),
        Some(b"10".to_vec())
    );
    assert_eq!(txn.buffer.get(&Key::from(b"new".to_vec())), None);
    txn.status.store(
        super::TransactionStatus::Rolledback as u8,
        std::sync::atomic::Ordering::SeqCst,
    );
}
