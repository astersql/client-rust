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
