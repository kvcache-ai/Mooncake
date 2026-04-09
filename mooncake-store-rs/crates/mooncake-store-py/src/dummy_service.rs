use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddr;
use std::os::fd::OwnedFd;
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::{self, JoinHandle};

use mooncake_store_client::{
    MooncakeCompatibilityFacade, ObjectRef, ReplicationPolicy, StoreClient,
};
use mooncake_store_core::StoreError;
use parking_lot::{Mutex, MutexGuard};
use tokio::runtime::Runtime;
use tokio::sync::oneshot;
use tonic::{Request, Response, Status};
use tonic::transport::Server;

use crate::shm::{
    DummyClientId, OwnedMappedRegion, ShmRegisterRequest, bind_shm_listener,
    dummy_ipc_socket_path, map_registered_region, recv_shm_register_request,
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
    let tracked_keys = Arc::new(Mutex::new(BTreeSet::new()));
    let regions = Arc::new(Mutex::new(BTreeMap::new()));
    let context = Arc::new(DummyStoreContext {
        client,
        regions,
        tracked_keys,
    });

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
    regions: Arc<Mutex<BTreeMap<(u64, u64, u64), ServerRegion>>>,
    tracked_keys: Arc<Mutex<BTreeSet<(String, String)>>>,
}

impl DummyStoreContext {
    fn register_region(
        &self,
        request: ShmRegisterRequest,
        fd: OwnedFd,
    ) -> Result<(), StoreError> {
        let mapped = map_registered_region(fd, request.size as usize)?;
        let key = (request.client_id_hi, request.client_id_lo, request.region_id);
        let client = self.client.lock();
        client.register_buffer(mapped.ptr.cast(), mapped.len)?;
        if let Some(previous) = self.regions.lock().insert(key, ServerRegion { mapped }) {
            let _ = client.unregister_buffer(previous.mapped.ptr.cast(), previous.mapped.len);
        }
        Ok(())
    }

    fn unregister_region(&self, client_id: DummyClientId, region_id: u64) -> i32 {
        let key = (client_id.high, client_id.low, region_id);
        let Some(region) = self.regions.lock().remove(&key) else {
            return -1;
        };
        match self
            .client
            .lock()
            .unregister_buffer(region.mapped.ptr.cast(), region.mapped.len)
        {
            Ok(()) => 0,
            Err(error) => {
                tracing::warn!(error = %error, "dummy unregister region failed");
                -1
            }
        }
    }

    fn region_slice(&self, client_id: DummyClientId, buffer: &pb::SharedBufferRef) -> Option<(*mut u8, usize)> {
        let regions = self.regions.lock();
        let region = regions.get(&(client_id.high, client_id.low, buffer.region_id))?;
        let offset = usize::try_from(buffer.offset).ok()?;
        let length = usize::try_from(buffer.length).ok()?;
        let end = offset.checked_add(length)?;
        if end > region.mapped.len {
            return None;
        }
        Some((unsafe { region.mapped.ptr.add(offset) }, length))
    }

    fn track_put_key(&self, tenant: &str, key: &str) {
        self.tracked_keys
            .lock()
            .insert((tenant.to_string(), key.to_string()));
    }

    fn forget_key(&self, tenant: &str, key: &str) {
        self.tracked_keys
            .lock()
            .remove(&(tenant.to_string(), key.to_string()));
    }

    fn remove_all(&self, force: bool) -> (i32, i64) {
        let keys = self.tracked_keys.lock().iter().cloned().collect::<Vec<_>>();
        let mut removed = 0i64;
        for (tenant, key) in keys {
            let result = self
                .client
                .lock()
                .remove_in_tenant(&tenant, &key, force);
            if result.is_ok() {
                self.forget_key(&tenant, &key);
                removed += 1;
            }
        }
        (0, removed)
    }
}

struct ServerRegion {
    mapped: OwnedMappedRegion,
}

pub struct SharedStoreClient {
    inner: Mutex<StoreClient>,
}

impl SharedStoreClient {
    pub fn new(client: StoreClient) -> Self {
        Self {
            inner: Mutex::new(client),
        }
    }

    pub fn lock(&self) -> MutexGuard<'_, StoreClient> {
        self.inner.lock()
    }
}

unsafe impl Send for SharedStoreClient {}
unsafe impl Sync for SharedStoreClient {}

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
        let request = request.into_inner();
        let tenant = normalized_tenant(&self.context.client.lock(), &request.tenant);
        let policy = proto_policy(&request.replication);
        let result = {
            let client = self.context.client.lock();
            match policy.as_ref() {
                Some(policy) => client.put_in_tenant_with_policy(&tenant, &request.key, &request.value, policy),
                None => client.put_in_tenant(&tenant, &request.key, &request.value),
            }
        };
        let status = if result.is_ok() { 0 } else { -1 };
        if status == 0 {
            self.context.track_put_key(&tenant, &request.key);
        }
        Ok(Response::new(pb::StatusReply { status }))
    }

    async fn get(
        &self,
        request: Request<pb::GetRequest>,
    ) -> Result<Response<pb::GetReply>, Status> {
        let request = request.into_inner();
        let tenant = normalized_tenant(&self.context.client.lock(), &request.tenant);
        let result = self.context.client.lock().get_in_tenant(&tenant, &request.key);
        let reply = match result {
            Ok(value) => pb::GetReply { status: 0, value },
            Err(_) => pb::GetReply {
                status: -1,
                value: Vec::new(),
            },
        };
        Ok(Response::new(reply))
    }

    async fn batch_is_exist(
        &self,
        request: Request<pb::BatchIsExistRequest>,
    ) -> Result<Response<pb::BatchIsExistReply>, Status> {
        let request = request.into_inner();
        let client = self.context.client.lock();
        let tenants = request
            .objects
            .iter()
            .map(|object| normalized_tenant(&client, &object.tenant))
            .collect::<Vec<_>>();
        let refs = request
            .objects
            .iter()
            .zip(tenants.iter())
            .map(|(object, tenant)| ObjectRef::new(object.key.as_str()).tenant(tenant.as_str()))
            .collect::<Vec<_>>();
        let statuses = client
            .batch_is_exist(&refs)
            .map(|items| items.into_iter().map(i32::from).collect())
            .unwrap_or_else(|_| vec![-1; refs.len()]);
        Ok(Response::new(pb::BatchIsExistReply { statuses }))
    }

    async fn batch_put_from(
        &self,
        request: Request<pb::BatchPutFromRequest>,
    ) -> Result<Response<pb::BatchStatusReply>, Status> {
        let request = request.into_inner();
        let client_id = DummyClientId {
            high: request.client_id_hi,
            low: request.client_id_lo,
        };
        let tenant = normalized_tenant(&self.context.client.lock(), &request.tenant);
        let policy = proto_policy(&request.replication);
        let mut statuses = Vec::with_capacity(request.items.len());
        for item in &request.items {
            let Some(buffer) = item.buffer.as_ref() else {
                statuses.push(-1);
                continue;
            };
            let Some((ptr, len)) = self.context.region_slice(client_id, buffer) else {
                statuses.push(-1);
                continue;
            };
            let result = {
                let client = self.context.client.lock();
                match policy.as_ref() {
                    Some(policy) => client.put_from_in_tenant_with_policy(
                        &tenant,
                        &item.key,
                        ptr.cast(),
                        len,
                        policy,
                    ),
                    None => client.put_from_in_tenant(&tenant, &item.key, ptr.cast(), len),
                }
            };
            if result.is_ok() {
                self.context.track_put_key(&tenant, &item.key);
                statuses.push(0);
            } else {
                statuses.push(-1);
            }
        }
        Ok(Response::new(pb::BatchStatusReply { statuses }))
    }

    async fn batch_get_into(
        &self,
        request: Request<pb::BatchGetIntoRequest>,
    ) -> Result<Response<pb::BatchGetIntoReply>, Status> {
        let request = request.into_inner();
        let client_id = DummyClientId {
            high: request.client_id_hi,
            low: request.client_id_lo,
        };
        let tenant = normalized_tenant(&self.context.client.lock(), &request.tenant);
        let mut lengths = Vec::with_capacity(request.items.len());
        for item in &request.items {
            let Some(buffer) = item.buffer.as_ref() else {
                lengths.push(-1);
                continue;
            };
            let Some((ptr, len)) = self.context.region_slice(client_id, buffer) else {
                lengths.push(-1);
                continue;
            };
            let buffer = unsafe { std::slice::from_raw_parts_mut(ptr, len) };
            let result = self
                .context
                .client
                .lock()
                .get_into_in_tenant(&tenant, &item.key, buffer);
            match result {
                Ok(size) => lengths.push(size as i64),
                Err(_) => lengths.push(-1),
            }
        }
        Ok(Response::new(pb::BatchGetIntoReply { lengths }))
    }

    async fn remove_all(
        &self,
        request: Request<pb::RemoveAllRequest>,
    ) -> Result<Response<pb::RemoveAllReply>, Status> {
        let request = request.into_inner();
        let (status, removed) = self.context.remove_all(request.force);
        Ok(Response::new(pb::RemoveAllReply { status, removed }))
    }

    async fn unregister_region(
        &self,
        request: Request<pb::UnregisterRegionRequest>,
    ) -> Result<Response<pb::StatusReply>, Status> {
        let request = request.into_inner();
        let status = self.context.unregister_region(
            DummyClientId {
                high: request.client_id_hi,
                low: request.client_id_lo,
            },
            request.region_id,
        );
        Ok(Response::new(pb::StatusReply { status }))
    }
}

fn normalized_tenant(client: &StoreClient, tenant: &str) -> String {
    if tenant.is_empty() {
        client.default_tenant().to_string()
    } else {
        tenant.to_string()
    }
}

fn proto_policy(policy: &Option<pb::ReplicationPolicy>) -> Option<ReplicationPolicy> {
    let policy = policy.as_ref()?;
    let mut resolved = ReplicationPolicy::new()
        .prefer_local(policy.prefer_local)
        .prefer_alloc_in_same_node(policy.prefer_alloc_in_same_node)
        .with_soft_pin(policy.with_soft_pin)
        .replica_count(policy.replica_count.max(1) as usize);
    if !policy.preferred_segments.is_empty() {
        resolved = resolved.preferred_segments(policy.preferred_segments.clone());
    }
    if !policy.preferred_storage_owners.is_empty() {
        resolved = resolved.preferred_storage_owners(policy.preferred_storage_owners.clone());
    }
    Some(resolved)
}
