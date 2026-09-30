pub mod grpc;
pub mod rest;
pub mod validator;

use std::net::SocketAddr;
use tracing::info;

use crate::auth::AuthManager;
use crate::feed::FeedCoordinator;
use grpc::{GrpcOracleService, OracleServiceServer};

pub async fn run_servers(
    feed: FeedCoordinator,
    auth: AuthManager,
    http_addr: SocketAddr,
    grpc_addr: SocketAddr,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let rest_app = rest::create_rest_router(feed.clone(), auth.clone());
    let grpc_service = GrpcOracleService::new(feed, auth);

    info!("Starting Axum REST Gateway on http://{}", http_addr);
    info!("Starting Tonic gRPC Server on {}", grpc_addr);

    let rest_listener = tokio::net::TcpListener::bind(http_addr).await?;
    let rest_server = axum::serve(rest_listener, rest_app);

    let grpc_server = tonic::transport::Server::builder()
        .accept_http1(true)
        .add_service(tonic_web::enable(OracleServiceServer::new(grpc_service)))
        .serve(grpc_addr);

    tokio::select! {
        res = rest_server => {
            if let Err(e) = res {
                tracing::error!("REST server error: {}", e);
            }
        }
        res = grpc_server => {
            if let Err(e) = res {
                tracing::error!("gRPC server error: {}", e);
            }
        }
    }

    Ok(())
}
