// Copyright 2026 AsterSQL.

use crate::proto::kvrpcpb::Mutation;

#[test]
fn pessimistic_lock_request_carries_nowait_and_millisecond_budget() {
    let make = |wait_ms| {
        super::new_pessimistic_lock_request(
            vec![Mutation {
                key: b"row".to_vec(),
                ..Default::default()
            }],
            b"row".to_vec(),
            100,
            3000,
            101,
            false,
            wait_ms,
        )
    };
    assert_eq!(make(-1).wait_timeout, -1);
    assert_eq!(make(50).wait_timeout, 50);
}
