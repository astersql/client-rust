// Copyright 2026 AsterSQL.
// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! PD metadata discovery without initializing a keyspace-scoped KV client.
use super::{RetryClient, RetryClientTrait};
use crate::{
    proto::{keyspacepb, pdpb},
    Result, SecurityManager,
};
use std::{sync::Arc, time::Duration};

/// Reuses PD leader discovery, TLS and reconnection while leaving keyspace lookup explicit.
pub struct MetadataClient {
    inner: RetryClient,
}
impl MetadataClient {
    pub async fn connect(
        endpoints: &[String],
        security: Arc<SecurityManager>,
        timeout: Duration,
    ) -> Result<Self> {
        Ok(Self {
            inner: RetryClient::connect(endpoints, security, timeout).await?,
        })
    }
    pub async fn get_members(&self) -> Result<pdpb::GetMembersResponse> {
        self.inner.get_members().await
    }
    pub async fn load_keyspace(&self, name: &str) -> Result<keyspacepb::KeyspaceMeta> {
        self.inner.load_keyspace(name).await
    }
}
#[cfg(test)]
#[path = "metadata_test.rs"]
mod tests;
