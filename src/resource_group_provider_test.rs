// Copyright 2026 AsterSQL.
// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.
use super::*;
use crate::resource_group_lookup::{CreateOption, ResourceGroupLookupController, ServerConfig};
use std::sync::atomic::{AtomicUsize, Ordering};
use tonic::codegen::{http, Body, BoxFuture, StdError};

#[derive(Clone)]
struct Boundary(Arc<AtomicUsize>, String);
struct Members(Boundary);
impl tonic::server::UnaryService<crate::proto::pdpb::GetMembersRequest> for Members {
    type Response = crate::proto::pdpb::GetMembersResponse;
    type Future = BoxFuture<tonic::Response<Self::Response>, Status>;
    fn call(&mut self, _: Request<crate::proto::pdpb::GetMembersRequest>) -> Self::Future {
        let endpoint = self.0 .1.clone();
        Box::pin(async move {
            let member = crate::proto::pdpb::Member {
                member_id: 1,
                client_urls: vec![endpoint],
                ..Default::default()
            };
            Ok(tonic::Response::new(
                crate::proto::pdpb::GetMembersResponse {
                    header: Some(crate::proto::pdpb::ResponseHeader {
                        cluster_id: 17,
                        ..Default::default()
                    }),
                    members: vec![member.clone()],
                    leader: Some(member),
                    ..Default::default()
                },
            ))
        })
    }
}
struct Get(#[allow(dead_code)] Boundary);
impl tonic::server::UnaryService<meta::GetRequest> for Get {
    type Response = meta::GetResponse;
    type Future = BoxFuture<tonic::Response<Self::Response>, Status>;
    fn call(&mut self, request: Request<meta::GetRequest>) -> Self::Future {
        let request = request.into_inner();
        Box::pin(async move {
            assert_eq!(request.header.unwrap().cluster_id, 17);
            assert_eq!(
                request.key,
                crate::resource_group_lookup::CONTROLLER_CONFIG_PATH
            );
            Ok(tonic::Response::new(meta::GetResponse {
                header: Some(meta::ResponseHeader::default()),
                kvs: vec![meta::KeyValue {
                    key: request.key,
                    value: serde_json::to_vec(&ServerConfig::default()).unwrap(),
                    ..Default::default()
                }],
                ..Default::default()
            }))
        })
    }
}
struct Put(#[allow(dead_code)] Boundary);
impl tonic::server::UnaryService<meta::PutRequest> for Put {
    type Response = meta::PutResponse;
    type Future = BoxFuture<tonic::Response<Self::Response>, Status>;
    fn call(&mut self, request: Request<meta::PutRequest>) -> Self::Future {
        Box::pin(async move {
            let request = request.into_inner();
            assert_eq!(request.header.unwrap().cluster_id, 17);
            assert_eq!(request.key, b"test-key");
            assert_eq!(request.value, b"test-value");
            Ok(tonic::Response::new(meta::PutResponse {
                header: Some(meta::ResponseHeader::default()),
                ..Default::default()
            }))
        })
    }
}
struct Group(Boundary);
impl tonic::server::UnaryService<rm::GetResourceGroupRequest> for Group {
    type Response = rm::GetResourceGroupResponse;
    type Future = BoxFuture<tonic::Response<Self::Response>, Status>;
    fn call(&mut self, request: Request<rm::GetResourceGroupRequest>) -> Self::Future {
        let count = self.0 .0.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move {
            let request = request.into_inner();
            assert_eq!(request.resource_group_name, "test-group");
            assert_eq!(request.keyspace_id.unwrap().value, 42);
            if count == 0 {
                return Err(Status::unavailable("resource manager unavailable"));
            }
            Ok(tonic::Response::new(rm::GetResourceGroupResponse {
                group: Some(rm::ResourceGroup {
                    name: request.resource_group_name,
                    mode: rm::GroupMode::RuMode as i32,
                    r_u_settings: Some(ru(1)),
                    ..Default::default()
                }),
                ..Default::default()
            }))
        })
    }
}
fn ru(fill_rate: u64) -> rm::GroupRequestUnitSettings {
    rm::GroupRequestUnitSettings {
        r_u: Some(rm::TokenBucket {
            settings: Some(rm::TokenLimitSettings {
                fill_rate,
                burst_limit: 50_000_000_000,
                ..Default::default()
            }),
            ..Default::default()
        }),
    }
}
macro_rules! service {
    ($name:ident,$grpc:literal, $($method:literal => ($req:ty,$resp:ty,$handler:ident)),+) => {
        #[derive(Clone)]struct $name(Boundary);
        impl tonic::server::NamedService for $name{const NAME:&'static str=$grpc;}
        impl<B> tonic::codegen::Service<http::Request<B>> for $name where B:Body+Send+'static,B::Error:Into<StdError>+Send+'static {
            type Response=http::Response<tonic::body::BoxBody>;type Error=std::convert::Infallible;type Future=BoxFuture<Self::Response,Self::Error>;
            fn poll_ready(&mut self,_:&mut std::task::Context<'_>)->std::task::Poll<Result<(),Self::Error>>{std::task::Poll::Ready(Ok(()))}
            fn call(&mut self,request:http::Request<B>)->Self::Future {
                let boundary=self.0.clone();
                match request.uri().path(){
                    $(concat!("/",$grpc,"/",$method)=>Box::pin(async move{Ok(tonic::server::Grpc::new(tonic::codec::ProstCodec::<$resp,$req>::default()).unary($handler(boundary),request).await)}),)+
                    _=>Box::pin(async{Ok(http::Response::builder().status(200).header("grpc-status","12").header("content-type","application/grpc").body(tonic::body::empty_body()).unwrap())})
                }
            }
        }
    }
}
service!(PdService,"pdpb.PD","GetMembers"=>(crate::proto::pdpb::GetMembersRequest,crate::proto::pdpb::GetMembersResponse,Members));
service!(MetadataService,"meta_storagepb.MetaStorage","Get"=>(meta::GetRequest,meta::GetResponse,Get),"Put"=>(meta::PutRequest,meta::PutResponse,Put));
service!(GroupService,"resource_manager.ResourceManager","GetResourceGroup"=>(rm::GetResourceGroupRequest,rm::GetResourceGroupResponse,Group));
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn grpc_provider_passes_headers_and_recovers_from_real_status() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let count = Arc::new(AtomicUsize::new(0));
    let boundary = Boundary(count.clone(), endpoint.clone());
    let incoming = futures::stream::unfold(listener, |listener| async {
        Some((listener.accept().await.map(|(stream, _)| stream), listener))
    });
    let (stop_tx, stop_rx) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(PdService(boundary.clone()))
            .add_service(MetadataService(boundary.clone()))
            .add_service(GroupService(boundary))
            .serve_with_incoming_shutdown(incoming, async {
                let _ = stop_rx.await;
            }),
    );
    let provider = Arc::new(
        GrpcResourceGroupProvider::connect_pd(
            vec![endpoint],
            42,
            Arc::new(SecurityManager::default()),
            Duration::from_secs(2),
        )
        .unwrap(),
    );
    let response = provider.put(b"test-key", b"test-value").unwrap();
    assert!(response.header.is_some());
    let controller = ResourceGroupLookupController::new(
        provider.clone(),
        42,
        &[CreateOption::DegradedRuSettings(ru(2_000_000))],
    )
    .unwrap();
    assert_eq!(
        controller
            .get_resource_group("test-group")
            .unwrap()
            .r_u_settings,
        Some(ru(2_000_000))
    );
    assert_eq!(
        controller
            .get_resource_group("test-group")
            .unwrap()
            .r_u_settings,
        Some(ru(1))
    );
    assert_eq!(
        controller
            .get_resource_group("test-group")
            .unwrap()
            .r_u_settings,
        Some(ru(1))
    );
    assert_eq!(count.load(Ordering::SeqCst), 2);
    drop(controller);
    drop(provider);
    let _ = stop_tx.send(());
    server.await.unwrap().unwrap();
}
