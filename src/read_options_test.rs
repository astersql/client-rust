// Copyright 2026 AsterSQL.

use crate::pd::PdClient;
use crate::proto::{keyspacepb, kvrpcpb, metapb};
use crate::region::{RegionVerId, RegionWithLeader};
use crate::request::{Keyspace, Plan, PlanBuilder};
use crate::store::{KvClient, RegionStore, Request, Store};
use crate::{Backoff, Error, Key, ReadOptions, Result, Timestamp};
use async_trait::async_trait;
use std::any::Any;
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[derive(Clone, Default)]
struct Client {
    calls: Arc<Mutex<Vec<(u64, bool, Option<Duration>)>>>,
    ordinary_error: bool,
}
#[async_trait]
impl KvClient for Client {
    async fn dispatch(&self, request: &dyn Request) -> Result<Box<dyn Any>> {
        self.dispatch_with_timeout(request, None).await
    }
    async fn dispatch_with_timeout(
        &self,
        request: &dyn Request,
        timeout: Option<Duration>,
    ) -> Result<Box<dyn Any>> {
        let request = request
            .as_any()
            .downcast_ref::<kvrpcpb::GetRequest>()
            .unwrap();
        let context = request.context.as_ref().unwrap();
        self.calls.lock().unwrap().push((
            context.peer.as_ref().unwrap().store_id,
            context.replica_read,
            timeout,
        ));
        if self.ordinary_error {
            return Err(Error::InternalError {
                message: "ordinary error".into(),
            });
        }
        if timeout.is_some() {
            return Err(tonic::Status::deadline_exceeded("injected read deadline").into());
        }
        Ok(Box::new(kvrpcpb::GetResponse {
            value: vec![42],
            ..Default::default()
        }))
    }
}
struct Pd {
    client: Client,
}
impl Pd {
    fn region() -> RegionWithLeader {
        let peers = (1..=3)
            .map(|id| metapb::Peer {
                id,
                store_id: id + 10,
                ..Default::default()
            })
            .collect::<Vec<_>>();
        RegionWithLeader {
            region: metapb::Region {
                id: 1,
                peers: peers.clone(),
                region_epoch: Some(metapb::RegionEpoch::default()),
                ..Default::default()
            },
            leader: Some(peers[0].clone()),
        }
    }
}
#[async_trait]
impl PdClient for Pd {
    type KvClient = Client;
    async fn map_region_to_store(self: Arc<Self>, region: RegionWithLeader) -> Result<RegionStore> {
        Ok(RegionStore::new(region, Arc::new(self.client.clone())))
    }
    async fn region_for_key(&self, _: &Key) -> Result<RegionWithLeader> {
        Ok(Self::region())
    }
    async fn region_for_id(&self, _: u64) -> Result<RegionWithLeader> {
        Ok(Self::region())
    }
    async fn all_stores(&self) -> Result<Vec<Store>> {
        Ok(vec![Store::new(Arc::new(self.client.clone()))])
    }
    async fn get_timestamp(self: Arc<Self>) -> Result<Timestamp> {
        Ok(Timestamp::default())
    }
    async fn update_safepoint(self: Arc<Self>, _: u64) -> Result<bool> {
        unreachable!()
    }
    async fn load_keyspace(&self, _: &str) -> Result<keyspacepb::KeyspaceMeta> {
        unreachable!()
    }
    async fn update_leader(&self, _: RegionVerId, _: metapb::Peer) -> Result<()> {
        Ok(())
    }
    async fn invalidate_region_cache(&self, _: RegionVerId) {
        panic!("read deadline must not invalidate healthy Region")
    }
    async fn invalidate_store_cache(&self, _: u64) {
        panic!("read deadline must not invalidate healthy store")
    }
}

#[tokio::test]
async fn read_deadlines_try_each_peer_then_restore_default_timeout() {
    let client = Client::default();
    let options = ReadOptions::new(Duration::from_millis(1));
    let stats = options.stats.clone();
    let request = kvrpcpb::GetRequest {
        key: vec![1],
        version: 42,
        ..Default::default()
    };
    let plan = PlanBuilder::new(
        Arc::new(Pd {
            client: client.clone(),
        }),
        Keyspace::Disable,
        request,
    )
    .with_read_options(Some(options))
    .resolve_lock(
        Timestamp::default(),
        Backoff::no_backoff(),
        Keyspace::Disable,
    )
    .retry_multi_region(Backoff::no_backoff())
    .plan();
    let response = plan.execute().await.unwrap();
    assert_eq!(response.len(), 1);
    assert_eq!(response[0].as_ref().unwrap().value, vec![42]);
    let timeout = Some(Duration::from_millis(1));
    assert_eq!(
        *client.calls.lock().unwrap(),
        vec![
            (11, false, timeout),
            (12, true, timeout),
            (13, true, timeout),
            (11, false, None)
        ]
    );
    let attempts = stats.snapshot();
    assert_eq!(attempts.len(), 4);
    assert_eq!(
        attempts
            .iter()
            .map(|attempt| attempt.store_id)
            .collect::<Vec<_>>(),
        vec![11, 12, 13, 11]
    );
    assert!(attempts[..3].iter().all(|attempt| attempt.timed_out));
    assert!(!attempts[3].timed_out);
}

#[tokio::test]
async fn ordinary_read_error_is_not_retried_as_a_timeout() {
    let client = Client {
        ordinary_error: true,
        ..Client::default()
    };
    let plan = PlanBuilder::new(
        Arc::new(Pd {
            client: client.clone(),
        }),
        Keyspace::Disable,
        kvrpcpb::GetRequest {
            key: vec![1],
            version: 42,
            ..Default::default()
        },
    )
    .with_read_options(Some(ReadOptions::new(Duration::from_millis(1))))
    .retry_multi_region(Backoff::no_backoff())
    .plan();
    assert!(plan.execute().await.is_err());
    assert_eq!(client.calls.lock().unwrap().len(), 1);
}

#[test]
fn read_pool_response_diagnostics_collect_get_batch_and_buffer_batch() {
    let stats = crate::ReadStats::default();
    let sample = kvrpcpb::PoolTaskDetails {
        poll_count: 4,
        dispatch_count: 2,
        total_wall_nanos: 20_000_000,
        total_queue_wait_nanos: 6_000_000,
        max_queue_wait_nanos: 4_000_000,
        min_queue_wait_nanos: 2_000_000,
        total_wake_wait_nanos: 4_000_000,
        max_wake_wait_nanos: 4_000_000,
        min_wake_wait_nanos: 4_000_000,
        fair_queue_enabled: true,
        total_fair_queue_waited_task_slices: 6,
        max_fair_queue_waited_task_slices: 4,
        min_fair_queue_waited_task_slices: 2,
        poll_cpu_nanos: 8_000_000,
        max_poll_cpu_nanos: 3_000_000,
        min_poll_cpu_nanos: 1_000_000,
        poll_wall_nanos: 12_000_000,
        max_poll_wall_nanos: 5_000_000,
        min_poll_wall_nanos: 2_000_000,
    };
    let exec = Some(kvrpcpb::ExecDetailsV2 {
        read_pool_task_details: Some(sample),
        ..Default::default()
    });
    for response in [
        Box::new(kvrpcpb::GetResponse {
            exec_details_v2: exec.clone(),
            ..Default::default()
        }) as Box<dyn Any>,
        Box::new(kvrpcpb::BatchGetResponse {
            exec_details_v2: exec.clone(),
            ..Default::default()
        }),
        Box::new(kvrpcpb::BufferBatchGetResponse {
            exec_details_v2: exec.clone(),
            ..Default::default()
        }),
    ] {
        stats.record_response(response.as_ref());
    }
    let pool = stats.read_pool_task_details();
    assert_eq!(
        (
            pool.task_count,
            pool.poll_count,
            pool.max_poll_count,
            pool.min_poll_count
        ),
        (3, 12, 4, 4)
    );
    assert_eq!(
        (
            pool.dispatch_count,
            pool.max_dispatch_count,
            pool.min_dispatch_count
        ),
        (6, 2, 2)
    );
    assert_eq!(
        (
            pool.total_wall_time,
            pool.max_task_wall_time,
            pool.min_task_wall_time
        ),
        (
            Duration::from_millis(60),
            Duration::from_millis(20),
            Duration::from_millis(20)
        )
    );
    assert_eq!(pool.task_wall_time_sample_count, 3);
    assert_eq!(
        (
            pool.total_queue_wait_time,
            pool.max_queue_wait_time,
            pool.min_queue_wait_time
        ),
        (
            Duration::from_millis(18),
            Duration::from_millis(4),
            Duration::from_millis(2)
        )
    );
    assert_eq!(
        (
            pool.total_wake_wait_time,
            pool.max_wake_wait_time,
            pool.min_wake_wait_time
        ),
        (
            Duration::from_millis(12),
            Duration::from_millis(4),
            Duration::from_millis(4)
        )
    );
    assert_eq!(
        (
            pool.fair_queue_sample_count,
            pool.total_fair_queue_waited_task_slices,
            pool.max_fair_queue_waited_task_slices,
            pool.min_fair_queue_waited_task_slices
        ),
        (6, 18, 4, 2)
    );
    assert_eq!(
        (
            pool.poll_cpu_time,
            pool.max_poll_cpu_time,
            pool.min_poll_cpu_time
        ),
        (
            Duration::from_millis(24),
            Duration::from_millis(3),
            Duration::from_millis(1)
        )
    );
    assert_eq!(
        (
            pool.poll_wall_time,
            pool.max_poll_wall_time,
            pool.min_poll_wall_time
        ),
        (
            Duration::from_millis(36),
            Duration::from_millis(5),
            Duration::from_millis(2)
        )
    );
    stats.record_response(&kvrpcpb::GetResponse::default());
    stats.record_response(&kvrpcpb::GetResponse {
        region_error: Some(Default::default()),
        exec_details_v2: exec,
        ..Default::default()
    });
    assert_eq!(stats.read_pool_task_details(), pool);
}

#[test]
fn read_pool_zero_samples_preserve_observed_minima() {
    let mut aggregate = crate::PoolTaskDetails::default();
    aggregate.merge_from_pb(&kvrpcpb::PoolTaskDetails {
        poll_count: 2,
        dispatch_count: 3,
        total_wall_nanos: 20,
        total_queue_wait_nanos: 10,
        min_queue_wait_nanos: 2,
        max_queue_wait_nanos: 8,
        total_wake_wait_nanos: 6,
        min_wake_wait_nanos: 2,
        max_wake_wait_nanos: 4,
        fair_queue_enabled: true,
        total_fair_queue_waited_task_slices: 6,
        min_fair_queue_waited_task_slices: 2,
        max_fair_queue_waited_task_slices: 4,
        poll_cpu_nanos: 5,
        min_poll_cpu_nanos: 2,
        max_poll_cpu_nanos: 3,
        poll_wall_nanos: 9,
        min_poll_wall_nanos: 4,
        max_poll_wall_nanos: 5,
    });
    aggregate.merge_from_pb(&Default::default());
    assert_eq!(
        (
            aggregate.task_count,
            aggregate.min_poll_count,
            aggregate.min_dispatch_count
        ),
        (2, 0, 0)
    );
    assert_eq!(aggregate.min_task_wall_time, Duration::from_nanos(20));
    assert_eq!(aggregate.min_queue_wait_time, Duration::from_nanos(2));
    assert_eq!(aggregate.min_wake_wait_time, Duration::from_nanos(2));
    assert_eq!(aggregate.min_poll_cpu_time, Duration::from_nanos(2));
    assert_eq!(aggregate.min_poll_wall_time, Duration::from_nanos(4));
    assert_eq!(aggregate.min_fair_queue_waited_task_slices, 2);
}

#[test]
fn read_pool_parallel_response_samples_and_snapshots_are_independent() {
    let stats = Arc::new(crate::ReadStats::default());
    let workers = (0..16)
        .map(|_| {
            let stats = stats.clone();
            std::thread::spawn(move || {
                stats.record_response(&kvrpcpb::GetResponse {
                    exec_details_v2: Some(kvrpcpb::ExecDetailsV2 {
                        read_pool_task_details: Some(kvrpcpb::PoolTaskDetails {
                            poll_count: 2,
                            ..Default::default()
                        }),
                        ..Default::default()
                    }),
                    ..Default::default()
                })
            })
        })
        .collect::<Vec<_>>();
    for worker in workers {
        worker.join().unwrap();
    }
    let snapshot = stats.read_pool_task_details();
    assert_eq!((snapshot.task_count, snapshot.poll_count), (16, 32));
    stats.record_response(&kvrpcpb::GetResponse {
        exec_details_v2: Some(kvrpcpb::ExecDetailsV2 {
            read_pool_task_details: Some(kvrpcpb::PoolTaskDetails::default()),
            ..Default::default()
        }),
        ..Default::default()
    });
    assert_eq!(snapshot.task_count, 16);
    assert_eq!(stats.read_pool_task_details().task_count, 17);
}
