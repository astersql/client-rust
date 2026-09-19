// Copyright 2026 AsterSQL.

use crate::proto::kvrpcpb::Op;
use crate::timestamp::{Timestamp, TimestampExt};
use crate::Key;

#[test]
fn foreign_key_shared_lock_selects_shared_pessimistic_mutation() {
    let make = |shared| {
        super::new_pessimistic_lock_request(
            vec![Key::from(b"parent-row".to_vec())].into_iter(),
            Key::from(b"parent-row".to_vec()),
            Timestamp::from_version(100),
            3000,
            Timestamp::from_version(101),
            false,
            50,
            shared,
        )
    };
    let shared = make(true);
    assert_eq!(shared.mutations[0].op, Op::SharedPessimisticLock as i32);
    assert_eq!(shared.wait_timeout, 50);
    assert_eq!(make(false).mutations[0].op, Op::PessimisticLock as i32);
}
