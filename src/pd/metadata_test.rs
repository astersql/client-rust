// Copyright 2026 AsterSQL.
// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.
use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};
use tonic::codegen::{http, Body, BoxFuture, StdError};

#[derive(Clone)]
struct Boundary {
    endpoint: String,
    loads: Arc<AtomicUsize>,
}
struct Members(Boundary);
impl tonic::server::UnaryService<pdpb::GetMembersRequest> for Members {
    type Response = pdpb::GetMembersResponse;
    type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
    fn call(&mut self, _: tonic::Request<pdpb::GetMembersRequest>) -> Self::Future {
        let endpoint = self.0.endpoint.clone();
        Box::pin(async move {
            let member = pdpb::Member {
                member_id: 1,
                client_urls: vec![endpoint],
                ..Default::default()
            };
            Ok(tonic::Response::new(pdpb::GetMembersResponse {
                header: Some(pdpb::ResponseHeader {
                    cluster_id: 17,
                    ..Default::default()
                }),
                members: vec![member.clone()],
                leader: Some(member),
                ..Default::default()
            }))
        })
    }
}
struct Keyspace(Boundary);
impl tonic::server::UnaryService<keyspacepb::LoadKeyspaceRequest> for Keyspace {
    type Response = keyspacepb::LoadKeyspaceResponse;
    type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
    fn call(&mut self, request: tonic::Request<keyspacepb::LoadKeyspaceRequest>) -> Self::Future {
        self.0.loads.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move {
            let request = request.into_inner();
            assert_eq!(request.header.unwrap().cluster_id, 17);
            let keyspace = (request.name != "missing").then(|| keyspacepb::KeyspaceMeta {
                id: 42,
                name: request.name,
                config: [
                    ("meta_service_group_id".into(), "group1".into()),
                    ("meta_service_group_addrs".into(), "meta:2379".into()),
                ]
                .into(),
                ..Default::default()
            });
            Ok(tonic::Response::new(keyspacepb::LoadKeyspaceResponse {
                header: Some(pdpb::ResponseHeader {
                    cluster_id: 17,
                    ..Default::default()
                }),
                keyspace,
            }))
        })
    }
}
macro_rules! service {
    ($name:ident, $grpc_name:literal, $request:ty, $response:ty, $handler:ident) => {
        #[derive(Clone)]
        struct $name(Boundary);
        impl tonic::server::NamedService for $name {
            const NAME: &'static str = $grpc_name;
        }
        impl<B> tonic::codegen::Service<http::Request<B>> for $name
        where
            B: Body + Send + 'static,
            B::Error: Into<StdError> + Send + 'static,
        {
            type Response = http::Response<tonic::body::BoxBody>;
            type Error = std::convert::Infallible;
            type Future = BoxFuture<Self::Response, Self::Error>;
            fn poll_ready(
                &mut self,
                _: &mut std::task::Context<'_>,
            ) -> std::task::Poll<std::result::Result<(), Self::Error>> {
                std::task::Poll::Ready(Ok(()))
            }
            fn call(&mut self, request: http::Request<B>) -> Self::Future {
                let expected_method = if $grpc_name == "pdpb.PD" {
                    "GetMembers"
                } else {
                    "LoadKeyspace"
                };
                if request.uri().path() != format!("/{}/{expected_method}", $grpc_name) {
                    return Box::pin(async {
                        Ok(http::Response::builder()
                            .status(200)
                            .header("grpc-status", "12")
                            .header("content-type", "application/grpc")
                            .body(tonic::body::empty_body())
                            .unwrap())
                    });
                }
                let boundary = self.0.clone();
                Box::pin(async move {
                    let codec = tonic::codec::ProstCodec::<$response, $request>::default();
                    Ok(tonic::server::Grpc::new(codec)
                        .unary($handler(boundary), request)
                        .await)
                })
            }
        }
    };
}
service!(
    PdService,
    "pdpb.PD",
    pdpb::GetMembersRequest,
    pdpb::GetMembersResponse,
    Members
);
service!(
    KeyspaceService,
    "keyspacepb.Keyspace",
    keyspacepb::LoadKeyspaceRequest,
    keyspacepb::LoadKeyspaceResponse,
    Keyspace
);

#[tokio::test]
async fn discovery_requires_pd_endpoints() {
    assert!(MetadataClient::connect(
        &[],
        Arc::new(SecurityManager::default()),
        Duration::from_millis(50)
    )
    .await
    .is_err());
}

#[tokio::test]
async fn discovery_keeps_keyspace_lookup_explicit_and_returns_full_metadata() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let loads = Arc::new(AtomicUsize::new(0));
    let boundary = Boundary {
        endpoint: endpoint.clone(),
        loads: loads.clone(),
    };
    let incoming = futures::stream::unfold(listener, |listener| async {
        let connection = listener.accept().await.map(|(stream, _)| stream);
        Some((connection, listener))
    });
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(PdService(boundary.clone()))
            .add_service(KeyspaceService(boundary))
            .serve_with_incoming_shutdown(incoming, async {
                let _ = stopped.await;
            }),
    );
    let client = MetadataClient::connect(
        &[endpoint.clone()],
        Arc::new(SecurityManager::default()),
        Duration::from_secs(1),
    )
    .await
    .unwrap();
    assert_eq!(loads.load(Ordering::SeqCst), 0);
    let members = client.get_members().await.unwrap();
    assert_eq!(members.members[0].client_urls, [endpoint]);
    let meta = client.load_keyspace("ks1").await.unwrap();
    assert_eq!(meta.id, 42);
    assert_eq!(meta.name, "ks1");
    assert_eq!(meta.config["meta_service_group_addrs"], "meta:2379");
    assert_eq!(loads.load(Ordering::SeqCst), 1);
    assert!(client.load_keyspace("missing").await.is_err());
    drop(client);
    stop.send(()).unwrap();
    server.await.unwrap().unwrap();
}
