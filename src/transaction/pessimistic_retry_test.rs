// Copyright 2026 AsterSQL.
use super::is_pessimistic_retry;
use crate::proto::kvrpcpb::{write_conflict::Reason, KeyError, WriteConflict};
use crate::Error;
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
