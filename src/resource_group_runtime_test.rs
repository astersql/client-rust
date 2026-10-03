// Copyright 2026 AsterSQL.
// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.
use super::*;
use crate::proto::resource_manager::{
    GrantedRuTokenBucket, GroupRequestUnitSettings, TokenBucket, TokenLimitSettings,
};

fn group() -> ResourceGroup {
    ResourceGroup {
        name: "test-group".into(),
        r_u_settings: Some(GroupRequestUnitSettings {
            r_u: Some(TokenBucket {
                settings: Some(TokenLimitSettings {
                    burst_limit: -1,
                    fill_rate: 1000,
                    ..Default::default()
                }),
                ..Default::default()
            }),
        }),
        ..Default::default()
    }
}
fn response(burst: i64) -> TokenBucketResponse {
    TokenBucketResponse {
        resource_group_name: "test-group".into(),
        granted_r_u_tokens: vec![GrantedRuTokenBucket {
            granted_tokens: Some(TokenBucket {
                settings: Some(TokenLimitSettings {
                    burst_limit: burst,
                    fill_rate: 1000,
                    ..Default::default()
                }),
                tokens: 1000.0,
            }),
            ..Default::default()
        }],
        ..Default::default()
    }
}

#[test]
fn runtime_state_requires_first_response_and_tracks_granted_burst() {
    let states = ResourceGroupRuntimeStates::default();
    states.register_group(&group());
    assert_eq!(states.get_resource_group_runtime_state("test-group"), None);
    for burst in [-1, 100, 0, -1] {
        states.handle_token_bucket_responses(&[response(burst)]);
        assert_eq!(
            states.get_resource_group_runtime_state("test-group"),
            Some(ResourceGroupRuntimeState {
                has_limited_burst: burst >= 0
            })
        );
        assert_eq!(states.get_resource_group_runtime_state("unknown"), None);
    }
}

#[test]
fn tombstone_is_not_revived_by_late_response_and_registration_resets_state() {
    let states = ResourceGroupRuntimeStates::default();
    states.handle_token_bucket_responses(&[response(100)]);
    assert_eq!(states.get_resource_group_runtime_state("test-group"), None);
    states.register_group(&group());
    states.handle_token_bucket_responses(&[response(100)]);
    states.tombstone_group("test-group");
    states.handle_token_bucket_responses(&[response(0)]);
    assert_eq!(states.get_resource_group_runtime_state("test-group"), None);
    states.register_group(&group());
    assert_eq!(states.get_resource_group_runtime_state("test-group"), None);
}

#[test]
fn empty_response_completes_initial_request_and_last_grant_wins() {
    let states = ResourceGroupRuntimeStates::default();
    states.register_group(&group());
    states.handle_token_bucket_responses(&[TokenBucketResponse {
        resource_group_name: "test-group".into(),
        ..Default::default()
    }]);
    assert_eq!(
        states.get_resource_group_runtime_state("test-group"),
        Some(ResourceGroupRuntimeState {
            has_limited_burst: false
        })
    );
    let mut grants = response(100);
    grants
        .granted_r_u_tokens
        .extend(response(-1).granted_r_u_tokens);
    states.handle_token_bucket_responses(&[grants]);
    assert_eq!(
        states.get_resource_group_runtime_state("test-group"),
        Some(ResourceGroupRuntimeState {
            has_limited_burst: false
        })
    );
}
