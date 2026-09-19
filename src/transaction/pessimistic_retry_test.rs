// Copyright 2026 AsterSQL.
use super::{
    is_pessimistic_retry, pessimistic_lock_resolve_backoff, remaining_lock_wait_timeout_ms,
    shared_lock_conflict_info, should_retry_pessimistic_lock,
};
use crate::backoff::PESSIMISTIC_BACKOFF;
use crate::proto::kvrpcpb::{self, write_conflict::Reason, KeyError, WriteConflict};
use crate::Error;
use std::time::Duration;
fn conflict(reason: Reason) -> Error {
    Error::KeyError(Box::new(KeyError {
        conflict: Some(WriteConflict {
            reason: reason as i32,
            ..Default::default()
        }),
        ..Default::default()
    }))
}
#[test]
fn only_pessimistic_wakeup_conflicts_are_retried() {
    assert!(is_pessimistic_retry(&Error::PessimisticLockError {
        inner: Box::new(Error::MultipleKeyErrors(vec![conflict(
            Reason::PessimisticRetry
        )])),
        success_keys: vec![]
    }));
    assert!(!is_pessimistic_retry(&conflict(Reason::Optimistic)));
    assert!(!is_pessimistic_retry(&Error::MultipleKeyErrors(vec![
        conflict(Reason::PessimisticRetry),
        conflict(Reason::SelfRolledBack)
    ])));
    assert!(!is_pessimistic_retry(&Error::ExtractedErrors(vec![])));
}

#[test]
fn explicit_wait_budgets_do_not_restart_client_side() {
    let wakeup = conflict(Reason::PessimisticRetry);
    assert!(should_retry_pessimistic_lock(0, &wakeup));
    assert!(!should_retry_pessimistic_lock(-1, &wakeup));
    assert!(!should_retry_pessimistic_lock(300, &wakeup));
}

#[test]
fn explicit_wait_budgets_skip_generic_live_lock_resolution() {
    assert!(!pessimistic_lock_resolve_backoff(0, PESSIMISTIC_BACKOFF).is_none());
    assert!(pessimistic_lock_resolve_backoff(-1, PESSIMISTIC_BACKOFF).is_none());
    assert!(pessimistic_lock_resolve_backoff(300, PESSIMISTIC_BACKOFF).is_none());
}

#[test]
fn finite_wait_retries_use_only_the_remaining_budget() {
    assert_eq!(
        remaining_lock_wait_timeout_ms(-1, Duration::from_secs(2)),
        -1
    );
    assert_eq!(remaining_lock_wait_timeout_ms(0, Duration::from_secs(2)), 0);
    assert_eq!(
        remaining_lock_wait_timeout_ms(300, Duration::from_millis(125)),
        175
    );
    assert_eq!(
        remaining_lock_wait_timeout_ms(300, Duration::from_secs(1)),
        1
    );
}

#[test]
fn shared_lock_conflict_is_found_inside_pessimistic_error() {
    let locks = vec![kvrpcpb::LockInfo {
        shared_lock_infos: vec![kvrpcpb::LockInfo {
            lock_version: 42,
            ..Default::default()
        }],
        ..Default::default()
    }];
    let error = Error::PessimisticLockError {
        inner: Box::new(Error::ResolveLockError(locks)),
        success_keys: vec![],
    };
    assert_eq!(shared_lock_conflict_info(&error, 42), (true, true));
    assert_eq!(shared_lock_conflict_info(&error, 7), (true, false));
}
