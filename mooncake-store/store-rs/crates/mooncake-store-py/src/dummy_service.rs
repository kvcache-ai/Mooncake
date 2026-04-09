use std::net::SocketAddr;
use std::os::fd::OwnedFd;
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::{self, JoinHandle};

use mooncake_store_core::StoreError;
use tokio::runtime::Runtime;
use tokio::sync::oneshot;
use tonic::transport::Server;
use tonic::{Request, Response, Status};

pub use crate::dummy_loop::{SharedStoreClient, SharedStoreClientLoop};
use crate::shm::{
    DummyClientId, ShmRegisterRequest, bind_shm_listener, dummy_ipc_socket_path,
    map_registered_region, recv_shm_register_request,
};

pub mod pb {
    tonic::include_proto!("mooncake.store.dummy");
}

pub struct DummyStoreServerHandle {
    address: String,
    socket_path: PathBuf,
    shutdown_flag: Arc<AtomicBool>,
    grpc_shutdown: Option<oneshot::Sender<()>>,
    grpc_thread: Option<JoinHandle<()>>,
    shm_thread: Option<JoinHandle<()>>,
}

impl DummyStoreServerHandle {
    pub fn address(&self) -> &str {
        &self.address
    }

    pub fn socket_path(&self) -> &Path {
        &self.socket_path
    }

    pub fn shutdown(mut self) -> Result<(), StoreError> {
        self.shutdown_flag.store(true, Ordering::SeqCst);
        if let Some(tx) = self.grpc_shutdown.take() {
            let _ = tx.send(());
        }
        let _ = UnixStream::connect(&self.socket_path);
        if let Some(thread) = self.shm_thread.take() {
            let _ = thread.join();
        }
        if let Some(thread) = self.grpc_thread.take() {
            let _ = thread.join();
        }
        if self.socket_path.exists() {
            let _ = std::fs::remove_file(&self.socket_path);
        }
        Ok(())
    }
}

impl Drop for DummyStoreServerHandle {
    fn drop(&mut self) {
        self.shutdown_flag.store(true, Ordering::SeqCst);
        if let Some(tx) = self.grpc_shutdown.take() {
            let _ = tx.send(());
        }
        let _ = UnixStream::connect(&self.socket_path);
        if let Some(thread) = self.shm_thread.take() {
            let _ = thread.join();
        }
        if let Some(thread) = self.grpc_thread.take() {
            let _ = thread.join();
        }
        if self.socket_path.exists() {
            let _ = std::fs::remove_file(&self.socket_path);
        }
    }
}

pub fn start_dummy_store_server(
    client: Arc<SharedStoreClient>,
    bind_addr: &str,
) -> Result<DummyStoreServerHandle, StoreError> {
    let socket_path = dummy_ipc_socket_path(bind_addr);
    let shutdown_flag = Arc::new(AtomicBool::new(false));
    let context = Arc::new(DummyStoreContext { client });

    let shm_listener = bind_shm_listener(&socket_path)?;
    let shm_shutdown = shutdown_flag.clone();
    let shm_context = context.clone();
    let shm_thread = thread::Builder::new()
        .name("mooncake-store-dummy-shm".to_string())
        .spawn(move || {
            while !shm_shutdown.load(Ordering::SeqCst) {
                match recv_shm_register_request(&shm_listener) {
                    Ok((request, fd)) => {
                        if let Err(error) = shm_context.register_region(request, fd) {
                            tracing::warn!(error = %error, "dummy shm registration failed");
                        }
                    }
                    Err(error) => {
                        if shm_shutdown.load(Ordering::SeqCst) {
                            break;
                        }
                        tracing::warn!(error = %error, "dummy shm accept loop failed");
                    }
                }
            }
        })
        .map_err(|error| StoreError::Transport(format!("failed to spawn shm thread: {error}")))?;

    let service = GrpcDummyStoreService {
        context: context.clone(),
    };
    let address: SocketAddr = bind_addr
        .parse()
        .map_err(|error| StoreError::Transport(format!("invalid dummy server address: {error}")))?;
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let grpc_thread = thread::Builder::new()
        .name("mooncake-store-dummy-grpc".to_string())
        .spawn(move || {
            let runtime = Runtime::new().expect("dummy gRPC runtime should build");
            runtime.block_on(async move {
                let result = Server::builder()
                    .add_service(pb::dummy_store_service_server::DummyStoreServiceServer::new(service))
                    .serve_with_shutdown(address, async move {
                        let _ = shutdown_rx.await;
                    })
                    .await;
                if let Err(error) = result {
                    tracing::warn!(error = %error, "dummy gRPC server stopped with error");
                }
            });
        })
        .map_err(|error| StoreError::Transport(format!("failed to spawn gRPC thread: {error}")))?;

    Ok(DummyStoreServerHandle {
        address: bind_addr.to_string(),
        socket_path,
        shutdown_flag,
        grpc_shutdown: Some(shutdown_tx),
        grpc_thread: Some(grpc_thread),
        shm_thread: Some(shm_thread),
    })
}

struct DummyStoreContext {
    client: Arc<SharedStoreClient>,
}

impl DummyStoreContext {
    fn register_region(
        &self,
        request: ShmRegisterRequest,
        fd: OwnedFd,
    ) -> Result<(), StoreError> {
        let mapped = map_registered_region(fd, request.size as usize)?;
        self.client.register_buffer(mapped.base(), mapped.len())?;
        let client_id = request_client_id(request.client_id_hi, request.client_id_lo);
        if let Some(previous) = self.client.install_region(client_id, request.region_id, mapped) {
            if let Err(error) = self
                .client
                .unregister_buffer(previous.base(), previous.len())
            {
                tracing::warn!(error = %error, "dummy shm stale region unregister failed");
            }
        }
        Ok(())
    }
}

#[derive(Clone)]
struct GrpcDummyStoreService {
    context: Arc<DummyStoreContext>,
}

#[tonic::async_trait]
impl pb::dummy_store_service_server::DummyStoreService for GrpcDummyStoreService {
    async fn health(
        &self,
        _request: Request<pb::HealthRequest>,
    ) -> Result<Response<pb::HealthReply>, Status> {
        Ok(Response::new(pb::HealthReply { status: 0 }))
    }

    async fn put(
        &self,
        request: Request<pb::PutRequest>,
    ) -> Result<Response<pb::StatusReply>, Status> {
        let status = self
            .context
            .client
            .put(request.into_inner())
            .await
            .map_err(executor_status)?;
        Ok(Response::new(pb::StatusReply { status }))
    }

    async fn get(
        &self,
        request: Request<pb::GetRequest>,
    ) -> Result<Response<pb::GetReply>, Status> {
        let reply = self
            .context
            .client
            .get(request.into_inner())
            .await
            .map_err(executor_status)?;
        Ok(Response::new(reply))
    }

    async fn batch_is_exist(
        &self,
        request: Request<pb::BatchIsExistRequest>,
    ) -> Result<Response<pb::BatchIsExistReply>, Status> {
        let statuses = self
            .context
            .client
            .batch_is_exist(request.into_inner())
            .await
            .map_err(executor_status)?;
        Ok(Response::new(pb::BatchIsExistReply { statuses }))
    }

    async fn batch_put_from(
        &self,
        request: Request<pb::BatchPutFromRequest>,
    ) -> Result<Response<pb::BatchStatusReply>, Status> {
        let statuses = self
            .context
            .client
            .batch_put_from(request.into_inner())
            .await
            .map_err(executor_status)?;
        Ok(Response::new(pb::BatchStatusReply { statuses }))
    }

    async fn batch_put_from_multi_buffers(
        &self,
        request: Request<pb::BatchPutFromMultiBuffersRequest>,
    ) -> Result<Response<pb::BatchStatusReply>, Status> {
        let statuses = self
            .context
            .client
            .batch_put_from_multi_buffers(request.into_inner())
            .await
            .map_err(executor_status)?;
        Ok(Response::new(pb::BatchStatusReply { statuses }))
    }

    async fn batch_get_into(
        &self,
        request: Request<pb::BatchGetIntoRequest>,
    ) -> Result<Response<pb::BatchGetIntoReply>, Status> {
        let lengths = self
            .context
            .client
            .batch_get_into(request.into_inner())
            .await
            .map_err(executor_status)?;
        Ok(Response::new(pb::BatchGetIntoReply { lengths }))
    }

    async fn batch_get_into_multi_buffers(
        &self,
        request: Request<pb::BatchGetIntoMultiBuffersRequest>,
    ) -> Result<Response<pb::BatchGetIntoReply>, Status> {
        let lengths = self
            .context
            .client
            .batch_get_into_multi_buffers(request.into_inner())
            .await
            .map_err(executor_status)?;
        Ok(Response::new(pb::BatchGetIntoReply { lengths }))
    }

    async fn remove_all(
        &self,
        request: Request<pb::RemoveAllRequest>,
    ) -> Result<Response<pb::RemoveAllReply>, Status> {
        let request = request.into_inner();
        let (status, removed) = self
            .context
            .client
            .remove_all(request.force)
            .await
            .map_err(executor_status)?;
        Ok(Response::new(pb::RemoveAllReply { status, removed }))
    }

    async fn unregister_region(
        &self,
        request: Request<pb::UnregisterRegionRequest>,
    ) -> Result<Response<pb::StatusReply>, Status> {
        let request = request.into_inner();
        let client_id = request_client_id(request.client_id_hi, request.client_id_lo);
        let status = match self.context.client.take_region(client_id, request.region_id) {
            Some(region) => match self
                .context
                .client
                .unregister_buffer_async(region.base(), region.len())
                .await
            {
                Ok(()) => 0,
                Err(error) => {
                    tracing::warn!(error = %error, "dummy unregister region failed");
                    -1
                }
            },
            None => -1,
        };
        Ok(Response::new(pb::StatusReply { status }))
    }
}

fn request_client_id(high: u64, low: u64) -> DummyClientId {
    DummyClientId { high, low }
}

fn executor_status(error: StoreError) -> Status {
    Status::internal(format!("dummy executor failed: {error}"))
}
