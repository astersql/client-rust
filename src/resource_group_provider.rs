// Copyright 2026 AsterSQL.
// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! Synchronous embedding boundary backed by a dedicated gRPC runtime thread.
//! The worker owns its runtime so callers may run inside or outside Tokio.
use crate::{
    proto::{meta_storagepb as meta, resource_manager as rm},
    resource_group_lookup::{LookupError, ResourceGroupProvider},
    SecurityManager,
};
use std::{
    sync::{mpsc, Arc, Mutex},
    thread::JoinHandle,
    time::Duration,
};
use tonic::{Request, Status};

enum Work {
    Get(
        Vec<u8>,
        mpsc::Sender<Result<meta::GetResponse, LookupError>>,
    ),
    Put(
        Vec<u8>,
        Vec<u8>,
        mpsc::Sender<Result<meta::PutResponse, LookupError>>,
    ),
    Group(
        String,
        mpsc::Sender<Result<Option<rm::ResourceGroup>, LookupError>>,
    ),
    Stop,
}
pub struct GrpcResourceGroupProvider {
    sender: mpsc::Sender<Work>,
    worker: Mutex<Option<JoinHandle<()>>>,
}
impl GrpcResourceGroupProvider {
    /// Resolve the PD leader and cluster identity using the existing discovery client.
    pub fn connect_pd(
        endpoints: Vec<String>,
        keyspace_id: u32,
        security: Arc<SecurityManager>,
        timeout: Duration,
    ) -> Result<Self, LookupError> {
        let discovery_security = security.clone();
        let members = std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|e| LookupError::Other(e.to_string()))?;
            runtime.block_on(async {
                let client =
                    crate::pd::MetadataClient::connect(&endpoints, discovery_security, timeout)
                        .await
                        .map_err(|e| LookupError::Other(e.to_string()))?;
                client
                    .get_members()
                    .await
                    .map_err(|e| LookupError::Other(e.to_string()))
            })
        })
        .join()
        .map_err(|_| LookupError::Other("PD discovery worker panicked".into()))??;
        let cluster_id = members
            .header
            .ok_or_else(|| LookupError::Other("PD discovery response has no header".into()))?
            .cluster_id;
        let endpoint = members
            .leader
            .and_then(|leader| leader.client_urls.into_iter().next())
            .ok_or_else(|| {
                LookupError::Other("PD discovery response has no leader endpoint".into())
            })?;
        Self::connect(endpoint, cluster_id, keyspace_id, security, timeout)
    }

    /// Connect to the resource-manager service endpoint selected by the embedder.
    /// PD leader discovery or MCS service discovery remains owned by the embedder.
    pub fn connect(
        endpoint: String,
        cluster_id: u64,
        keyspace_id: u32,
        security: Arc<SecurityManager>,
        timeout: Duration,
    ) -> Result<Self, LookupError> {
        let (sender, receiver) = mpsc::channel();
        let (ready_tx, ready_rx) = mpsc::channel();
        let worker = std::thread::spawn(move || {
            let runtime = match tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            {
                Ok(runtime) => runtime,
                Err(error) => {
                    let _ = ready_tx.send(Err(LookupError::Other(error.to_string())));
                    return;
                }
            };
            let clients = runtime.block_on(async {
                let group = security
                    .connect_with_timeout(
                        &endpoint,
                        timeout,
                        rm::resource_manager_client::ResourceManagerClient::new,
                    )
                    .await
                    .map_err(|e| LookupError::Other(e.to_string()))?;
                let metadata = security
                    .connect_with_timeout(
                        &endpoint,
                        timeout,
                        meta::meta_storage_client::MetaStorageClient::new,
                    )
                    .await
                    .map_err(|e| LookupError::Other(e.to_string()))?;
                Ok::<_, LookupError>((group, metadata))
            });
            let (mut group, mut metadata) = match clients {
                Ok(clients) => {
                    let _ = ready_tx.send(Ok(()));
                    clients
                }
                Err(error) => {
                    let _ = ready_tx.send(Err(error));
                    return;
                }
            };
            let header = Some(meta::RequestHeader {
                cluster_id,
                ..Default::default()
            });
            while let Ok(work) = receiver.recv() {
                match work {
                    Work::Stop => break,
                    Work::Get(key, reply) => {
                        let request = Request::new(meta::GetRequest {
                            header: header.clone(),
                            key,
                            ..Default::default()
                        });
                        let result = runtime
                            .block_on(async {
                                tokio::time::timeout(timeout, metadata.get(request))
                                    .await
                                    .map_err(|_| LookupError::CallerDeadlineExceeded)?
                                    .map_err(LookupError::from)
                            })
                            .and_then(|response| {
                                let response = response.into_inner();
                                if let Some(error) =
                                    response.header.as_ref().and_then(|h| h.error.as_ref())
                                {
                                    return Err(LookupError::Other(error.message.clone()));
                                }
                                Ok(response)
                            });
                        let _ = reply.send(result);
                    }
                    Work::Put(key, value, reply) => {
                        let request = Request::new(meta::PutRequest {
                            header: header.clone(),
                            key,
                            value,
                            ..Default::default()
                        });
                        let result = runtime
                            .block_on(async {
                                tokio::time::timeout(timeout, metadata.put(request))
                                    .await
                                    .map_err(|_| LookupError::CallerDeadlineExceeded)?
                                    .map_err(LookupError::from)
                            })
                            .and_then(|response| {
                                let response = response.into_inner();
                                if let Some(error) =
                                    response.header.as_ref().and_then(|h| h.error.as_ref())
                                {
                                    return Err(LookupError::Other(error.message.clone()));
                                }
                                Ok(response)
                            });
                        let _ = reply.send(result);
                    }
                    Work::Group(name, reply) => {
                        let request = Request::new(rm::GetResourceGroupRequest {
                            resource_group_name: name.clone(),
                            keyspace_id: Some(rm::KeyspaceIdValue { value: keyspace_id }),
                            ..Default::default()
                        });
                        let result = runtime
                            .block_on(async {
                                tokio::time::timeout(timeout, group.get_resource_group(request))
                                    .await
                                    .map_err(|_| LookupError::CallerDeadlineExceeded)?
                                    .map_err(LookupError::from)
                            })
                            .and_then(|response| {
                                let response = response.into_inner();
                                if let Some(error) = response.error {
                                    return Err(LookupError::Other(error.message));
                                }
                                Ok(response.group)
                            })
                            .map_err(|cause| LookupError::GetResourceGroup {
                                name,
                                cause: Box::new(cause),
                            });
                        let _ = reply.send(result);
                    }
                }
            }
        });
        match ready_rx.recv().map_err(|_| {
            LookupError::from(Status::unavailable("resource manager worker stopped"))
        })? {
            Ok(()) => Ok(Self {
                sender,
                worker: Mutex::new(Some(worker)),
            }),
            Err(error) => {
                let _ = worker.join();
                Err(error)
            }
        }
    }
    fn receive<T>(
        &self,
        work: impl FnOnce(mpsc::Sender<Result<T, LookupError>>) -> Work,
    ) -> Result<T, LookupError> {
        let (sender, receiver) = mpsc::channel();
        self.sender.send(work(sender)).map_err(|_| {
            LookupError::from(Status::unavailable("resource manager worker stopped"))
        })?;
        receiver.recv().map_err(|_| {
            LookupError::from(Status::unavailable("resource manager worker stopped"))
        })?
    }
}
impl ResourceGroupProvider for GrpcResourceGroupProvider {
    fn get(&self, key: &[u8]) -> Result<meta::GetResponse, LookupError> {
        self.receive(|reply| Work::Get(key.into(), reply))
    }
    fn put(&self, key: &[u8], value: &[u8]) -> Result<meta::PutResponse, LookupError> {
        self.receive(|reply| Work::Put(key.into(), value.into(), reply))
    }
    fn get_resource_group(&self, name: &str) -> Result<Option<rm::ResourceGroup>, LookupError> {
        self.receive(|reply| Work::Group(name.into(), reply))
    }
}
impl Drop for GrpcResourceGroupProvider {
    fn drop(&mut self) {
        let _ = self.sender.send(Work::Stop);
        if let Some(worker) = self.worker.get_mut().unwrap().take() {
            let _ = worker.join();
        }
    }
}

#[cfg(test)]
#[path = "resource_group_provider_test.rs"]
mod tests;
