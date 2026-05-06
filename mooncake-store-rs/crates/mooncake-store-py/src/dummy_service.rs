use std::net::SocketAddr;
use std::os::fd::OwnedFd;
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};

use mooncake_store_core::StoreError;
use tokio::runtime::Runtime;
use tokio::sync::oneshot;
use tonic::transport::Server;
use tonic::{Request, Response, Status};

use crate::dispatcher::StoreDispatcher;
use crate::shm::{
    bind_shm_listener, dummy_ipc_socket_path, hot_cache_ipc_socket_path, map_registered_region,
    recv_hot_cache_fd_request, recv_shm_register_request, send_hot_cache_fd_response,
    DummyClientId, ShmRegisterRequest,
};
use crate::DEFAULT_COMPAT_WORKER_SCOPE;

pub mod pb {
    tonic::include_proto!("mooncake.store.dummy");
}

pub struct DummyStoreServerHandle {
    address: String,
    socket_paths: Vec<PathBuf>,
    hot_cache_socket_paths: Vec<PathBuf>,
    shutdown_flag: Arc<AtomicBool>,
    grpc_shutdown: Option<oneshot::Sender<()>>,
    grpc_thread: Option<JoinHandle<()>>,
    shm_threads: Vec<JoinHandle<()>>,
    hot_cache_threads: Vec<JoinHandle<()>>,
}

impl DummyStoreServerHandle {
    pub fn address(&self) -> &str {
        &self.address
    }

    pub fn socket_path(&self) -> &Path {
        &self.socket_paths[0]
    }

    pub fn shutdown(mut self) -> Result<(), StoreError> {
        self.shutdown_flag.store(true, Ordering::SeqCst);
        if let Some(tx) = self.grpc_shutdown.take() {
            let _ = tx.send(());
        }
        for path in &self.socket_paths {
            let _ = UnixStream::connect(path);
        }
        for path in &self.hot_cache_socket_paths {
            let _ = UnixStream::connect(path);
        }
        for thread in self.shm_threads.drain(..) {
            let _ = thread.join();
        }
        for thread in self.hot_cache_threads.drain(..) {
            let _ = thread.join();
        }
        if let Some(thread) = self.grpc_thread.take() {
            let _ = thread.join();
        }
        for path in &self.socket_paths {
            if path.exists() {
                let _ = std::fs::remove_file(path);
            }
        }
        for path in &self.hot_cache_socket_paths {
            if path.exists() {
                let _ = std::fs::remove_file(path);
            }
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
        for path in &self.socket_paths {
            let _ = UnixStream::connect(path);
        }
        for path in &self.hot_cache_socket_paths {
            let _ = UnixStream::connect(path);
        }
        for thread in self.shm_threads.drain(..) {
            let _ = thread.join();
        }
        for thread in self.hot_cache_threads.drain(..) {
            let _ = thread.join();
        }
        if let Some(thread) = self.grpc_thread.take() {
            let _ = thread.join();
        }
        for path in &self.socket_paths {
            if path.exists() {
                let _ = std::fs::remove_file(path);
            }
        }
        for path in &self.hot_cache_socket_paths {
            if path.exists() {
                let _ = std::fs::remove_file(path);
            }
        }
    }
}

fn spawn_shm_listener_thread(
    listener: UnixListener,
    context: Arc<DummyStoreContext>,
    shutdown_flag: Arc<AtomicBool>,
    thread_name: String,
) -> Result<JoinHandle<()>, StoreError> {
    thread::Builder::new()
        .name(thread_name)
        .spawn(move || {
            while !shutdown_flag.load(Ordering::SeqCst) {
                match recv_shm_register_request(&listener) {
                    Ok((request, fd)) => {
                        if let Err(error) = context.register_region(request, fd) {
                            tracing::warn!(error = %error, "dummy shm registration failed");
                        }
                    }
                    Err(error) => {
                        if shutdown_flag.load(Ordering::SeqCst) {
                            break;
                        }
                        tracing::warn!(error = %error, "dummy shm accept loop failed");
                    }
                }
            }
        })
        .map_err(|error| StoreError::Transport(format!("failed to spawn shm thread: {error}")))
}

fn spawn_hot_cache_listener_thread(
    listener: UnixListener,
    context: Arc<DummyStoreContext>,
    shutdown_flag: Arc<AtomicBool>,
    thread_name: String,
) -> Result<JoinHandle<()>, StoreError> {
    thread::Builder::new()
        .name(thread_name)
        .spawn(move || {
            while !shutdown_flag.load(Ordering::SeqCst) {
                match recv_hot_cache_fd_request(&listener) {
                    Ok((stream, _request)) => match context.client.hot_cache_fd() {
                        Ok(Some((fd, size))) => {
                            if let Err(error) = send_hot_cache_fd_response(&stream, &fd, size) {
                                tracing::warn!(
                                    error = %error,
                                    "dummy hot cache fd reply failed"
                                );
                            }
                        }
                        Ok(None) => {}
                        Err(error) => {
                            tracing::warn!(error = %error, "dummy hot cache fd lookup failed");
                        }
                    },
                    Err(error) => {
                        if shutdown_flag.load(Ordering::SeqCst) {
                            break;
                        }
                        tracing::warn!(error = %error, "dummy hot cache accept loop failed");
                    }
                }
            }
        })
        .map_err(|error| {
            StoreError::Transport(format!("failed to spawn hot cache thread: {error}"))
        })
}

pub fn start_dummy_store_server(
    client: Arc<StoreDispatcher>,
    bind_addr: &str,
    worker_scope: &str,
) -> Result<DummyStoreServerHandle, StoreError> {
    let client = Arc::new(client.fork_with_scope(worker_scope.to_string())?);
    let shutdown_flag = Arc::new(AtomicBool::new(false));
    let context = Arc::new(DummyStoreContext { client });

    let mut socket_paths = vec![dummy_ipc_socket_path(bind_addr, worker_scope)];
    if worker_scope != DEFAULT_COMPAT_WORKER_SCOPE {
        let legacy_path = dummy_ipc_socket_path(bind_addr, DEFAULT_COMPAT_WORKER_SCOPE);
        if legacy_path != socket_paths[0] {
            socket_paths.push(legacy_path);
        }
    }
    let mut shm_threads = Vec::with_capacity(socket_paths.len());
    for (index, path) in socket_paths.iter().enumerate() {
        let listener = bind_shm_listener(path)?;
        shm_threads.push(spawn_shm_listener_thread(
            listener,
            context.clone(),
            shutdown_flag.clone(),
            format!("mooncake-store-dummy-shm-{index}"),
        )?);
    }

    let mut hot_cache_socket_paths = Vec::new();
    let mut hot_cache_threads = Vec::new();
    if context.client.hot_cache_fd()?.is_some() {
        hot_cache_socket_paths.push(hot_cache_ipc_socket_path(bind_addr, worker_scope));
        if worker_scope != DEFAULT_COMPAT_WORKER_SCOPE {
            let legacy_path =
                hot_cache_ipc_socket_path(bind_addr, DEFAULT_COMPAT_WORKER_SCOPE);
            if legacy_path != hot_cache_socket_paths[0] {
                hot_cache_socket_paths.push(legacy_path);
            }
        }
        for (index, path) in hot_cache_socket_paths.iter().enumerate() {
            let listener = bind_shm_listener(path)?;
            hot_cache_threads.push(spawn_hot_cache_listener_thread(
                listener,
                context.clone(),
                shutdown_flag.clone(),
                format!("mooncake-store-dummy-hot-cache-{index}"),
            )?);
        }
    }

    let grpc_context = context.clone();
    let address: SocketAddr = bind_addr
        .parse()
        .map_err(|error| StoreError::Transport(format!("invalid dummy server address: {error}")))?;
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let grpc_thread = thread::Builder::new()
        .name("mooncake-store-dummy-grpc".to_string())
        .spawn(move || {
            let runtime = Runtime::new().expect("dummy gRPC runtime should build");
            let service = GrpcDummyStoreService {
                context: grpc_context.clone(),
            };
            runtime.block_on(async move {
                let result = Server::builder()
                    .add_service(
                        pb::dummy_store_service_server::DummyStoreServiceServer::new(service),
                    )
                    .serve_with_shutdown(address, async move {
                        let _ = shutdown_rx.await;
                    })
                    .await;
                if let Err(error) = result {
                    tracing::warn!(error = %error, "dummy gRPC server stopped with error");
                }
            });
            drop(grpc_context);
        })
        .map_err(|error| StoreError::Transport(format!("failed to spawn gRPC thread: {error}")))?;

    Ok(DummyStoreServerHandle {
        address: bind_addr.to_string(),
        socket_paths,
        hot_cache_socket_paths,
        shutdown_flag,
        grpc_shutdown: Some(shutdown_tx),
        grpc_thread: Some(grpc_thread),
        shm_threads,
        hot_cache_threads,
    })
}

struct DummyStoreContext {
    client: Arc<StoreDispatcher>,
}

impl DummyStoreContext {
    fn register_region(&self, request: ShmRegisterRequest, fd: OwnedFd) -> Result<(), StoreError> {
        let mapped = map_registered_region(fd, request.size as usize)?;
        self.client.register_buffer(mapped.base(), mapped.len())?;
        let client_id = request_client_id(request.client_id_hi, request.client_id_lo);
        if let Some(previous) = self
            .client
            .install_region(client_id, request.region_id, mapped)
        {
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

    async fn acquire_hot_cache(
        &self,
        request: Request<pb::HotCacheAcquireRequest>,
    ) -> Result<Response<pb::HotCacheAcquireReply>, Status> {
        Ok(Response::new(
            self.context.client.acquire_hot_cache(request.into_inner()),
        ))
    }

    async fn release_hot_cache(
        &self,
        request: Request<pb::HotCacheReleaseRequest>,
    ) -> Result<Response<pb::StatusReply>, Status> {
        Ok(Response::new(pb::StatusReply {
            status: self.context.client.release_hot_cache(request.into_inner()),
        }))
    }

    async fn batch_acquire_hot_cache(
        &self,
        request: Request<pb::BatchHotCacheAcquireRequest>,
    ) -> Result<Response<pb::BatchHotCacheAcquireReply>, Status> {
        Ok(Response::new(
            self.context
                .client
                .batch_acquire_hot_cache(request.into_inner()),
        ))
    }

    async fn batch_release_hot_cache(
        &self,
        request: Request<pb::BatchHotCacheReleaseRequest>,
    ) -> Result<Response<pb::StatusReply>, Status> {
        Ok(Response::new(pb::StatusReply {
            status: self
                .context
                .client
                .batch_release_hot_cache(request.into_inner()),
        }))
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
        let status = match self
            .context
            .client
            .take_region(client_id, request.region_id)
        {
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
    match error {
        StoreError::Transport(message) if message.contains("timed out") => {
            Status::deadline_exceeded(format!("dummy executor timed out: {message}"))
        }
        other => Status::internal(format!("dummy executor failed: {other}")),
    }
}
