// Copyright 2026 AsterSQL.
// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::any::Any;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use derive_new::new;
use tonic::codec::CompressionEncoding;
use tonic::transport::Channel;

use super::Request;
use crate::proto::tikvpb::tikv_client::TikvClient;
use crate::Result;
use crate::SecurityManager;

/// A trait for connecting to TiKV stores.
#[async_trait]
pub trait KvConnect: Sized + Send + Sync + 'static {
    type KvClient: KvClient + Clone + Send + Sync + 'static;

    async fn connect(&self, address: &str) -> Result<Self::KvClient>;
}

#[derive(new, Clone)]
pub struct TikvConnect {
    security_mgr: Arc<SecurityManager>,
    timeout: Duration,
    grpc_max_decoding_message_size: usize,
}

#[async_trait]
impl KvConnect for TikvConnect {
    type KvClient = KvRpcClient;

    async fn connect(&self, address: &str) -> Result<KvRpcClient> {
        self.security_mgr
            .connect_with_timeout(address, self.timeout, move |channel| {
                TikvClient::new(channel)
                    .max_decoding_message_size(self.grpc_max_decoding_message_size)
                    .accept_compressed(CompressionEncoding::Gzip)
            })
            .await
            .map(|c| KvRpcClient::new(c, self.timeout))
    }
}

#[async_trait]
pub trait KvClient {
    async fn dispatch(&self, req: &dyn Request) -> Result<Box<dyn Any>>;
    async fn dispatch_with_timeout(
        &self,
        req: &dyn Request,
        _timeout: Option<Duration>,
    ) -> Result<Box<dyn Any>> {
        self.dispatch(req).await
    }
}

/// This client handles requests for a single TiKV node. It converts the data
/// types and abstractions of the client program into the grpc data types.
#[derive(new, Clone)]
pub struct KvRpcClient {
    rpc_client: TikvClient<Channel>,
    timeout: Duration,
}

#[async_trait]
impl KvClient for KvRpcClient {
    async fn dispatch(&self, request: &dyn Request) -> Result<Box<dyn Any>> {
        self.dispatch_with_timeout(request, None).await
    }
    async fn dispatch_with_timeout(
        &self,
        request: &dyn Request,
        timeout: Option<Duration>,
    ) -> Result<Box<dyn Any>> {
        let hooks = crate::rpc_interceptor::snapshot();
        let mut timeout = timeout.unwrap_or(self.timeout);
        for hook in &hooks {
            hook.before(request.as_any(), &mut timeout)?;
        }
        let delay = hooks.iter().fold(Duration::ZERO, |delay, hook| {
            delay.saturating_add(hook.delay(request.as_any()))
        });
        let mut response = tokio::time::timeout(timeout, async {
            if !delay.is_zero() {
                tokio::time::sleep(delay).await;
            }
            request.dispatch(&self.rpc_client, timeout).await
        })
        .await
        .unwrap_or_else(|_| {
            Err(crate::Error::GrpcAPI(tonic::Status::deadline_exceeded(
                "TiKV RPC deadline elapsed",
            )))
        });
        for hook in &hooks {
            hook.after(request.as_any(), &mut response);
        }
        response
    }
}
