// Copyright 2026 AsterSQL.

use crate::proto::kvrpcpb::prewrite_request::PessimisticAction;
use crate::proto::kvrpcpb::{Mutation, PrewriteRequest};
use crate::request::Shardable;

#[test]
fn prewrite_shards_preserve_key_checks_and_primary_secondaries() {
    let primary = Mutation {
        key: b"primary".to_vec(),
        ..Default::default()
    };
    let secondary = Mutation {
        key: b"secondary".to_vec(),
        ..Default::default()
    };
    let request = PrewriteRequest {
        mutations: vec![primary.clone(), secondary.clone()],
        primary_lock: primary.key.clone(),
        secondaries: vec![secondary.key.clone()],
        use_async_commit: true,
        try_one_pc: true,
        pessimistic_actions: vec![
            PessimisticAction::DoPessimisticCheck as i32,
            PessimisticAction::DoConstraintCheck as i32,
        ],
        ..Default::default()
    };
    let mut primary_request = request.clone();
    primary_request.apply_shard(vec![primary]);
    assert_eq!(primary_request.secondaries, vec![secondary.key.clone()]);
    assert_eq!(
        primary_request.pessimistic_actions,
        vec![PessimisticAction::DoPessimisticCheck as i32]
    );
    assert!(!primary_request.try_one_pc);
    let mut secondary_request = request;
    secondary_request.apply_shard(vec![secondary]);
    assert!(secondary_request.secondaries.is_empty());
    assert_eq!(
        secondary_request.pessimistic_actions,
        vec![PessimisticAction::DoConstraintCheck as i32]
    );
    assert!(!secondary_request.try_one_pc);
}
