// Copyright 2026 AsterSQL.

use crate::mock::{MockKvClient, MockPdClient};
use crate::proto::{kvrpcpb, pdpb::Timestamp};
use crate::request::Keyspace;
use crate::transaction::HeartbeatOption;
use crate::{Error, SchemaLeaseChecker, Transaction, TransactionOptions};
use std::any::Any;
use std::sync::{Arc, Mutex};

#[tokio::test]
async fn schema_checker_rejects_2pc_before_primary_commit() {
    let events = Arc::new(Mutex::new(Vec::new()));
    let rpc_events = events.clone();
    let pd = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
        move |request: &dyn Any| {
            if request.is::<kvrpcpb::PrewriteRequest>() {
                rpc_events.lock().unwrap().push("prewrite");
                return Ok(Box::<kvrpcpb::PrewriteResponse>::default() as Box<dyn Any>);
            }
            if request.is::<kvrpcpb::CommitRequest>() {
                rpc_events.lock().unwrap().push("commit");
                return Ok(Box::<kvrpcpb::CommitResponse>::default() as Box<dyn Any>);
            }
            panic!("unexpected RPC")
        },
    )));
    let mut txn = Transaction::new(
        Timestamp::default(),
        pd,
        TransactionOptions::new_optimistic().heartbeat_option(HeartbeatOption::NoHeartbeat),
        Keyspace::Disable,
    );
    let check_events = events.clone();
    txn.set_schema_lease_checker(Some(SchemaLeaseChecker::new(move |ts| {
        assert_eq!(ts, 0); // Mock PD's commit timestamp.
        check_events.lock().unwrap().push("check");
        Err(Error::StringError("schema expired".into()))
    })));
    txn.put("key".to_owned(), "value".to_owned()).await.unwrap();
    assert!(txn
        .commit()
        .await
        .unwrap_err()
        .to_string()
        .contains("schema expired"));
    assert_eq!(*events.lock().unwrap(), vec!["prewrite", "check"]);
}

#[tokio::test]
async fn schema_checker_rejects_async_and_one_pc_before_prewrite() {
    for one_pc in [false, true] {
        let pd = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(|_| {
            panic!("schema rejection must prevent the commit-point prewrite")
        })));
        let mut txn = Transaction::new(
            Timestamp::default(),
            pd,
            TransactionOptions::new_optimistic().heartbeat_option(HeartbeatOption::NoHeartbeat),
            Keyspace::Disable,
        );
        txn.set_one_pc(one_pc);
        txn.set_async_commit(!one_pc);
        txn.set_schema_lease_checker(Some(SchemaLeaseChecker::new(|_| {
            Err(Error::StringError("schema changed".into()))
        })));
        txn.put("key".to_owned(), "value".to_owned()).await.unwrap();
        assert!(txn
            .commit()
            .await
            .unwrap_err()
            .to_string()
            .contains("schema changed"));
    }
}

#[tokio::test]
async fn schema_checker_bounds_async_and_one_pc_prewrite() {
    for one_pc in [false, true] {
        let checks = Arc::new(Mutex::new(Vec::new()));
        let observed = checks.clone();
        let pd = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |request: &dyn Any| {
                if let Some(request) = request.downcast_ref::<kvrpcpb::PrewriteRequest>() {
                    let current = *observed.lock().unwrap().last().unwrap();
                    assert_eq!(request.max_commit_ts, current + (2000 << 18));
                    assert_eq!(request.try_one_pc, one_pc);
                    assert_eq!(request.use_async_commit, !one_pc);
                    return Ok(Box::new(kvrpcpb::PrewriteResponse {
                        one_pc_commit_ts: if one_pc { 1 } else { 0 },
                        min_commit_ts: if one_pc { 0 } else { 1 },
                        ..Default::default()
                    }) as Box<dyn Any>);
                }
                if request.is::<kvrpcpb::CommitRequest>() {
                    return Ok(Box::<kvrpcpb::CommitResponse>::default() as Box<dyn Any>);
                }
                panic!("unexpected RPC")
            },
        )));
        let mut txn = Transaction::new(
            Timestamp::default(),
            pd,
            TransactionOptions::new_optimistic().heartbeat_option(HeartbeatOption::NoHeartbeat),
            Keyspace::Disable,
        );
        txn.set_one_pc(one_pc);
        txn.set_async_commit(!one_pc);
        let checks_callback = checks.clone();
        txn.set_schema_lease_checker(Some(SchemaLeaseChecker::new(move |ts| {
            checks_callback.lock().unwrap().push(ts);
            Ok(())
        })));
        txn.put("key".to_owned(), "value".to_owned()).await.unwrap();
        txn.commit().await.unwrap();
        assert_eq!(checks.lock().unwrap().len(), 1);
    }
}

#[tokio::test]
async fn schema_checker_rechecks_async_fallback_at_commit_ts() {
    let checks = Arc::new(Mutex::new(Vec::new()));
    let pd = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
        |request: &dyn Any| {
            if request.is::<kvrpcpb::PrewriteRequest>() {
                // A zero min_commit_ts means TiKV rejected async commit.
                return Ok(Box::<kvrpcpb::PrewriteResponse>::default() as Box<dyn Any>);
            }
            panic!("second schema check must prevent the primary commit")
        },
    )));
    let mut txn = Transaction::new(
        Timestamp::default(),
        pd,
        TransactionOptions::new_optimistic().heartbeat_option(HeartbeatOption::NoHeartbeat),
        Keyspace::Disable,
    );
    txn.set_async_commit(true);
    let observed = checks.clone();
    txn.set_schema_lease_checker(Some(SchemaLeaseChecker::new(move |ts| {
        let mut checks = observed.lock().unwrap();
        checks.push(ts);
        if checks.len() == 1 {
            Ok(())
        } else {
            Err(Error::StringError("schema expired after fallback".into()))
        }
    })));
    txn.put("key".to_owned(), "value".to_owned()).await.unwrap();
    assert!(txn
        .commit()
        .await
        .unwrap_err()
        .to_string()
        .contains("after fallback"));
    assert_eq!(checks.lock().unwrap().len(), 2);
    assert_eq!(checks.lock().unwrap()[1], 0);
}

#[tokio::test]
async fn schema_checker_does_not_check_empty_transactions() {
    let pd = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(|_| {
        panic!("no writes")
    })));
    let mut txn = Transaction::new(
        Timestamp::default(),
        pd,
        TransactionOptions::new_optimistic().heartbeat_option(HeartbeatOption::NoHeartbeat),
        Keyspace::Disable,
    );
    txn.set_schema_lease_checker(Some(SchemaLeaseChecker::new(|_| panic!("no writes"))));
    assert!(txn.commit().await.unwrap().is_none());
}
