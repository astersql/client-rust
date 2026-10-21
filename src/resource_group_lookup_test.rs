// Copyright 2026 AsterSQL.
// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};

struct Provider {
    config: Vec<u8>,
    result: Mutex<Result<Option<rm::ResourceGroup>, LookupError>>,
    calls: AtomicUsize,
}
impl ResourceGroupProvider for Provider {
    fn get(&self, key: &[u8]) -> Result<meta::GetResponse, LookupError> {
        assert_eq!(key, CONTROLLER_CONFIG_PATH);
        Ok(meta::GetResponse {
            kvs: vec![meta::KeyValue {
                key: key.into(),
                value: self.config.clone(),
                ..Default::default()
            }],
            ..Default::default()
        })
    }
    fn put(&self, _: &[u8], _: &[u8]) -> Result<meta::PutResponse, LookupError> {
        Ok(meta::PutResponse::default())
    }
    fn get_resource_group(&self, _: &str) -> Result<Option<rm::ResourceGroup>, LookupError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.result.lock().unwrap().clone()
    }
}
fn settings(fill_rate: u64, burst_limit: i64) -> rm::GroupRequestUnitSettings {
    rm::GroupRequestUnitSettings {
        r_u: Some(rm::TokenBucket {
            settings: Some(rm::TokenLimitSettings {
                fill_rate,
                burst_limit,
                ..Default::default()
            }),
            ..Default::default()
        }),
    }
}
fn provider(error: LookupError) -> Arc<Provider> {
    Arc::new(Provider {
        config: serde_json::to_vec(&ServerConfig::default()).unwrap(),
        result: Mutex::new(Err(error)),
        calls: AtomicUsize::new(0),
    })
}
fn wrapped(error: LookupError) -> LookupError {
    LookupError::GetResourceGroup {
        name: "test-group".into(),
        cause: Box::new(error),
    }
}
#[test]
fn transient_lookup_synthesizes_ru_group_and_recovers_without_caching() {
    let provider = provider(wrapped(
        Status::unavailable("resource manager unavailable").into(),
    ));
    let controller = ResourceGroupLookupController::new(
        provider.clone(),
        0,
        &[CreateOption::DegradedRuSettings(settings(
            2_000_000,
            50_000_000_000,
        ))],
    )
    .unwrap();
    let group = controller.get_resource_group("test-group").unwrap();
    assert_eq!(group.name, "test-group");
    assert_eq!(group.mode, rm::GroupMode::RuMode as i32);
    assert_eq!(
        group.r_u_settings,
        Some(settings(2_000_000, 50_000_000_000))
    );
    let real = rm::ResourceGroup {
        name: "test-group".into(),
        mode: rm::GroupMode::RuMode as i32,
        r_u_settings: Some(settings(1, 0)),
        ..Default::default()
    };
    *provider.result.lock().unwrap() = Ok(Some(real.clone()));
    assert_eq!(controller.get_resource_group("test-group").unwrap(), real);
    assert_eq!(controller.get_resource_group("test-group").unwrap(), real);
    assert_eq!(provider.calls.load(Ordering::SeqCst), 2);
}
#[test]
fn errors_preserve_caller_and_not_found_identity() {
    for error in [
        LookupError::CallerCanceled,
        LookupError::CallerDeadlineExceeded,
        Status::cancelled("server canceled").into(),
        Status::unknown(
            "[PD:resourcemanager:ErrGroupNotExists]the test-group resource group does not exist",
        )
        .into(),
        Status::permission_denied("forbidden").into(),
    ] {
        let original = wrapped(error.clone());
        let provider = provider(original.clone());
        let controller = ResourceGroupLookupController::new(
            provider,
            0,
            &[CreateOption::DegradedRuSettings(settings(50, 100))],
        )
        .unwrap();
        let err = controller.get_resource_group("test-group").unwrap_err();
        assert_eq!(
            err.to_string(),
            match error {
                LookupError::CallerCanceled | LookupError::CallerDeadlineExceeded =>
                    error.to_string(),
                _ => original.to_string(),
            }
        );
    }
}
#[test]
fn disabled_fallback_preserves_server_status_and_nil_group_is_not_found() {
    for status in [
        Status::unavailable("unavailable"),
        Status::deadline_exceeded("server timeout"),
    ] {
        let original = wrapped(status.into());
        let controller =
            ResourceGroupLookupController::new(provider(original.clone()), 0, &[]).unwrap();
        assert_eq!(
            controller
                .get_resource_group("test-group")
                .unwrap_err()
                .to_string(),
            original.to_string()
        );
    }
    let provider = provider(LookupError::Other("unused".into()));
    *provider.result.lock().unwrap() = Ok(None);
    let controller = ResourceGroupLookupController::new(
        provider,
        0,
        &[CreateOption::DegradedRuSettings(settings(50, 100))],
    )
    .unwrap();
    assert!(matches!(
        controller.get_resource_group("test-group"),
        Err(LookupError::NotFound(_))
    ));
}
#[test]
fn provider_config_is_loaded_before_explicit_options() {
    let config = ServerConfig {
        token_rpc_params: TokenRpcParams {
            wait_retry_interval: "250ms".into(),
            wait_retry_times: 4,
        },
        ru_version_policy: Some(RuVersionPolicy {
            default: 2,
            overrides: HashMap::from([(1, 1)]),
        }),
        ..Default::default()
    };
    let provider = Arc::new(Provider {
        config: serde_json::to_vec(&config).unwrap(),
        result: Mutex::new(Ok(None)),
        calls: AtomicUsize::new(0),
    });
    let base = ResourceGroupLookupController::new(provider.clone(), 0, &[]).unwrap();
    assert_eq!(
        base.config().wait_retry_interval,
        Duration::from_millis(250)
    );
    assert_eq!(base.config().wait_retry_times, 4);
    assert_eq!(base.ru_version(), 2);
    let enabled = ResourceGroupLookupController::new(
        provider.clone(),
        1,
        &[
            CreateOption::WaitRetryInterval(Duration::from_millis(100)),
            CreateOption::WaitRetryTimes(20),
            CreateOption::DegradedModeWaitDuration(Duration::from_millis(1500)),
        ],
    )
    .unwrap();
    assert_eq!(
        enabled.config().wait_retry_interval,
        Duration::from_millis(100)
    );
    assert_eq!(enabled.config().wait_retry_times, 20);
    assert_eq!(
        enabled.config().degraded_mode_wait_duration,
        Duration::from_millis(1500)
    );
    assert_eq!(enabled.ru_version(), 1);
}

#[test]
fn fallback_error_classification_matches_pd_controller() {
    for error in [
        Status::deadline_exceeded("server timeout").into(),
        LookupError::Other("no leader".into()),
        LookupError::Other("not primary".into()),
        LookupError::Other("keyspace is not served".into()),
    ] {
        let controller = ResourceGroupLookupController::new(
            provider(wrapped(error)),
            0,
            &[CreateOption::DegradedRuSettings(settings(50, 100))],
        )
        .unwrap();
        assert_eq!(
            controller
                .get_resource_group("test-group")
                .unwrap()
                .r_u_settings,
            Some(settings(50, 100))
        );
    }
}
#[test]
fn invalid_config_and_invalid_group_are_errors_not_cached() {
    let invalid_provider = Arc::new(Provider {
        config: b"invalid json".to_vec(),
        result: Mutex::new(Ok(None)),
        calls: AtomicUsize::new(0),
    });
    assert!(ResourceGroupLookupController::new(invalid_provider, 0, &[]).is_err());
    let provider = provider(LookupError::Other("unused".into()));
    *provider.result.lock().unwrap() = Ok(Some(rm::ResourceGroup {
        name: "test-group".into(),
        ..Default::default()
    }));
    let controller = ResourceGroupLookupController::new(provider.clone(), 0, &[]).unwrap();
    assert!(controller.get_resource_group("test-group").is_err());
    assert!(controller.get_resource_group("test-group").is_err());
    assert_eq!(provider.calls.load(Ordering::SeqCst), 2);
}
