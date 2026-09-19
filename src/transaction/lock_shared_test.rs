// Copyright 2026 AsterSQL.

use super::{contains_shared_locks, reject_shared_locks, shared_locks_contain_transaction};
use crate::proto::kvrpcpb;

#[test]
fn shared_lock_wrappers_are_detected_without_becoming_cleanup_targets() {
    let wrapper = kvrpcpb::LockInfo {
        shared_lock_infos: vec![kvrpcpb::LockInfo {
            lock_version: 42,
            ..Default::default()
        }],
        ..Default::default()
    };
    assert!(contains_shared_locks(std::slice::from_ref(&wrapper)));
    assert!(reject_shared_locks(&[wrapper]).is_err());
    assert!(!contains_shared_locks(&[kvrpcpb::LockInfo::default()]));
}

#[test]
fn shared_lock_holder_is_identified_from_nested_lock_info() {
    let wrapper = kvrpcpb::LockInfo {
        shared_lock_infos: vec![kvrpcpb::LockInfo {
            lock_version: 42,
            ..Default::default()
        }],
        ..Default::default()
    };
    assert!(shared_locks_contain_transaction(&[wrapper.clone()], 42));
    assert!(!shared_locks_contain_transaction(&[wrapper], 7));
}
