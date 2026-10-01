// Copyright 2026 AsterSQL.
use super::*;
#[test]
fn go_merge_187_runtime_evidence_bridge() {
    let stats = ReadStats::default();
    assert!(!stats.point_response_stats().payload_complete());
    let response = kvrpcpb::GetResponse {
        value: vec![1, 2, 3],
        exec_details_v2: Some(kvrpcpb::ExecDetailsV2 {
            scan_detail_v2: Some(kvrpcpb::ScanDetailV2 {
                total_versions: 6,
                processed_versions: 2,
                processed_versions_size: 8,
                ..Default::default()
            }),
            ..Default::default()
        }),
        ..Default::default()
    };
    stats.record_response(&response);
    let frozen = stats.point_response_stats();
    assert!(frozen.payload_complete() && frozen.scan_detail_complete());
    assert_eq!(
        (
            frozen.payload_bytes,
            frozen.total_keys,
            frozen.processed_keys,
            frozen.processed_bytes
        ),
        (3, 6, 2, 8)
    );
    stats.record_response(&kvrpcpb::GetResponse {
        not_found: true,
        ..Default::default()
    });
    assert!(!stats.point_response_stats().scan_detail_complete());
    assert_eq!(frozen.payload_bytes, 3);
}

#[tokio::test]
async fn go_merge_187_runtime_evidence_bridge_dispatch() {
    use crate::request::{Keyspace, Plan, PlanBuilder};
    let pd = Arc::new(crate::mock::MockPdClient::new(
        crate::mock::MockKvClient::with_dispatch_hook(|_| {
            Ok(Box::new(kvrpcpb::GetResponse {
                value: vec![7, 8],
                exec_details_v2: Some(kvrpcpb::ExecDetailsV2 {
                    scan_detail_v2: Some(kvrpcpb::ScanDetailV2::default()),
                    ..Default::default()
                }),
                ..Default::default()
            }) as Box<dyn Any>)
        }),
    ));
    let options = ReadOptions::new(Duration::ZERO);
    let stats = options.stats.clone();
    let plan = PlanBuilder::new(
        pd,
        Keyspace::Disable,
        kvrpcpb::GetRequest {
            key: b"key".to_vec(),
            ..Default::default()
        },
    )
    .with_read_options(Some(options))
    .retry_multi_region(crate::Backoff::no_backoff())
    .plan();
    assert!(plan.execute().await.is_ok());
    assert_eq!(stats.point_response_stats().payload_bytes, 2);
    assert!(stats.point_response_stats().scan_detail_complete());
}

#[test]
fn point_response_payload_and_coverage_match_go() {
    let stats = ReadStats::default();
    stats.record_response(&kvrpcpb::GetResponse {
        value: vec![1; 99],
        error: Some(kvrpcpb::KeyError::default()),
        ..Default::default()
    });
    assert_eq!(stats.point_response_stats().payload_bytes, 0);
    assert!(stats.point_response_stats().payload_complete());
    assert!(!stats.point_response_stats().scan_detail_complete());
    stats.record_response(&kvrpcpb::BatchGetResponse {
        pairs: vec![
            kvrpcpb::KvPair {
                key: vec![1, 2],
                value: vec![3, 4, 5],
                ..Default::default()
            },
            kvrpcpb::KvPair {
                error: Some(kvrpcpb::KeyError::default()),
                value: vec![1; 99],
                ..Default::default()
            },
        ],
        ..Default::default()
    });
    assert_eq!(stats.point_response_stats().payload_bytes, 5);
    stats.record_response(&kvrpcpb::GetResponse {
        region_error: Some(crate::proto::errorpb::Error::default()),
        value: vec![1; 99],
        ..Default::default()
    });
    assert_eq!(stats.point_response_stats().payload_bytes, 5);
    let mut invalid = stats.point_response_stats();
    invalid.invalidate();
    let before = invalid.payload_bytes;
    invalid.record_response(None, 99);
    assert!(!invalid.is_valid());
    assert_eq!(invalid.payload_bytes, before);
}
