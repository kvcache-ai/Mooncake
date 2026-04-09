use std::collections::{BTreeMap, BTreeSet};
use std::ffi::c_void;
use std::sync::Arc;
use std::sync::mpsc;
use std::time::Duration;

use mooncake_store_client::{
    MooncakeCompatibilityFacade, MultiBufferGetRequest, MultiBufferPutRequest, ObjectRef,
    ReplicationPolicy, StoreClient,
};
use mooncake_store_core::StoreError;
use parking_lot::Mutex;
use tokio::sync::oneshot;

use crate::dummy_service::pb;
use crate::shm::{DummyClientId, OwnedMappedRegion};

type RegionKey = (u64, u64, u64);
type RegionMap = BTreeMap<RegionKey, OwnedMappedRegion>;
type TrackedKey = (String, String);

pub struct SharedStoreClient {
    regions: Arc<Mutex<RegionMap>>,
    sender: mpsc::Sender<ClientCommand>,
}

pub struct SharedStoreClientLoop {
    regions: Arc<Mutex<RegionMap>>,
    receiver: mpsc::Receiver<ClientCommand>,
    tracked_keys: BTreeSet<TrackedKey>,
}

enum ClientCommand {
    RegisterBuffer {
        base_ptr: usize,
        len: usize,
        reply: oneshot::Sender<Result<(), StoreError>>,
    },
    UnregisterBuffer {
        base_ptr: usize,
        len: usize,
        reply: oneshot::Sender<Result<(), StoreError>>,
    },
    Put {
        request: pb::PutRequest,
        reply: oneshot::Sender<i32>,
    },
    Get {
        request: pb::GetRequest,
        reply: oneshot::Sender<pb::GetReply>,
    },
    BatchIsExist {
        request: pb::BatchIsExistRequest,
        reply: oneshot::Sender<Vec<i32>>,
    },
    BatchPutFrom {
        request: pb::BatchPutFromRequest,
        reply: oneshot::Sender<Vec<i32>>,
    },
    BatchPutFromMultiBuffers {
        request: pb::BatchPutFromMultiBuffersRequest,
        reply: oneshot::Sender<Vec<i32>>,
    },
    BatchGetInto {
        request: pb::BatchGetIntoRequest,
        reply: oneshot::Sender<Vec<i64>>,
    },
    BatchGetIntoMultiBuffers {
        request: pb::BatchGetIntoMultiBuffersRequest,
        reply: oneshot::Sender<Vec<i64>>,
    },
    RemoveAll {
        force: bool,
        reply: oneshot::Sender<(i32, i64)>,
    },
}

impl SharedStoreClient {
    pub fn new() -> (Self, SharedStoreClientLoop) {
        let regions = Arc::new(Mutex::new(BTreeMap::new()));
        let (sender, receiver) = mpsc::channel();
        (
            Self {
                regions: regions.clone(),
                sender,
            },
            SharedStoreClientLoop {
                regions,
                receiver,
                tracked_keys: BTreeSet::new(),
            },
        )
    }

    pub(crate) fn install_region(
        &self,
        client_id: DummyClientId,
        region_id: u64,
        mapped: OwnedMappedRegion,
    ) -> Option<OwnedMappedRegion> {
        self.regions
            .lock()
            .insert(region_key(client_id, region_id), mapped)
    }

    pub(crate) fn take_region(
        &self,
        client_id: DummyClientId,
        region_id: u64,
    ) -> Option<OwnedMappedRegion> {
        self.regions.lock().remove(&region_key(client_id, region_id))
    }

    pub fn register_buffer(&self, base_ptr: usize, len: usize) -> Result<(), StoreError> {
        self.request_blocking(|reply| ClientCommand::RegisterBuffer {
            base_ptr,
            len,
            reply,
        })?
    }

    pub fn unregister_buffer(&self, base_ptr: usize, len: usize) -> Result<(), StoreError> {
        self.request_blocking(|reply| ClientCommand::UnregisterBuffer {
            base_ptr,
            len,
            reply,
        })?
    }

    pub async fn unregister_buffer_async(
        &self,
        base_ptr: usize,
        len: usize,
    ) -> Result<(), StoreError> {
        self.request_async(|reply| ClientCommand::UnregisterBuffer {
            base_ptr,
            len,
            reply,
        })
        .await?
    }

    pub async fn put(&self, request: pb::PutRequest) -> Result<i32, StoreError> {
        self.request_async(|reply| ClientCommand::Put { request, reply })
            .await
    }

    pub async fn get(&self, request: pb::GetRequest) -> Result<pb::GetReply, StoreError> {
        self.request_async(|reply| ClientCommand::Get { request, reply })
            .await
    }

    pub async fn batch_is_exist(
        &self,
        request: pb::BatchIsExistRequest,
    ) -> Result<Vec<i32>, StoreError> {
        self.request_async(|reply| ClientCommand::BatchIsExist { request, reply })
            .await
    }

    pub async fn batch_put_from(
        &self,
        request: pb::BatchPutFromRequest,
    ) -> Result<Vec<i32>, StoreError> {
        self.request_async(|reply| ClientCommand::BatchPutFrom { request, reply })
            .await
    }

    pub async fn batch_put_from_multi_buffers(
        &self,
        request: pb::BatchPutFromMultiBuffersRequest,
    ) -> Result<Vec<i32>, StoreError> {
        self.request_async(|reply| ClientCommand::BatchPutFromMultiBuffers { request, reply })
            .await
    }

    pub async fn batch_get_into(
        &self,
        request: pb::BatchGetIntoRequest,
    ) -> Result<Vec<i64>, StoreError> {
        self.request_async(|reply| ClientCommand::BatchGetInto { request, reply })
            .await
    }

    pub async fn batch_get_into_multi_buffers(
        &self,
        request: pb::BatchGetIntoMultiBuffersRequest,
    ) -> Result<Vec<i64>, StoreError> {
        self.request_async(|reply| ClientCommand::BatchGetIntoMultiBuffers { request, reply })
            .await
    }

    pub async fn remove_all(&self, force: bool) -> Result<(i32, i64), StoreError> {
        self.request_async(|reply| ClientCommand::RemoveAll { force, reply })
            .await
    }

    async fn request_async<T>(
        &self,
        build: impl FnOnce(oneshot::Sender<T>) -> ClientCommand,
    ) -> Result<T, StoreError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.send(build(reply_tx))?;
        reply_rx.await.map_err(|_| executor_dropped())
    }

    fn request_blocking<T>(
        &self,
        build: impl FnOnce(oneshot::Sender<T>) -> ClientCommand,
    ) -> Result<T, StoreError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.send(build(reply_tx))?;
        reply_rx.blocking_recv().map_err(|_| executor_dropped())
    }

    fn send(&self, command: ClientCommand) -> Result<(), StoreError> {
        self.sender.send(command).map_err(|_| executor_dropped())
    }
}

impl SharedStoreClientLoop {
    pub fn pump(&mut self, client: &mut StoreClient, max_wait: Duration) {
        match self.receiver.recv_timeout(max_wait) {
            Ok(command) => {
                self.execute(client, command);
                while let Ok(command) = self.receiver.try_recv() {
                    self.execute(client, command);
                }
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {}
            Err(mpsc::RecvTimeoutError::Disconnected) => {}
        }
    }

    pub fn drain(&mut self, client: &mut StoreClient) {
        while let Ok(command) = self.receiver.try_recv() {
            self.execute(client, command);
        }
    }

    fn execute(&mut self, client: &mut StoreClient, command: ClientCommand) {
        match command {
            ClientCommand::RegisterBuffer {
                base_ptr,
                len,
                reply,
            } => {
                let buffer = base_ptr as *mut c_void;
                let _ = reply.send(client.register_buffer(buffer, len));
            }
            ClientCommand::UnregisterBuffer {
                base_ptr,
                len,
                reply,
            } => {
                let buffer = base_ptr as *mut c_void;
                let _ = reply.send(client.unregister_buffer(buffer, len));
            }
            ClientCommand::Put { request, reply } => {
                let _ = reply.send(execute_put(client, &mut self.tracked_keys, request));
            }
            ClientCommand::Get { request, reply } => {
                let _ = reply.send(execute_get(client, request));
            }
            ClientCommand::BatchIsExist { request, reply } => {
                let _ = reply.send(execute_batch_is_exist(client, request));
            }
            ClientCommand::BatchPutFrom { request, reply } => {
                let _ = reply.send(execute_batch_put_from(
                    client,
                    &self.regions,
                    &mut self.tracked_keys,
                    request,
                ));
            }
            ClientCommand::BatchPutFromMultiBuffers { request, reply } => {
                let _ = reply.send(execute_batch_put_from_multi_buffers(
                    client,
                    &self.regions,
                    &mut self.tracked_keys,
                    request,
                ));
            }
            ClientCommand::BatchGetInto { request, reply } => {
                let _ = reply.send(execute_batch_get_into(client, &self.regions, request));
            }
            ClientCommand::BatchGetIntoMultiBuffers { request, reply } => {
                let _ = reply.send(execute_batch_get_into_multi_buffers(
                    client,
                    &self.regions,
                    request,
                ));
            }
            ClientCommand::RemoveAll { force, reply } => {
                let _ = reply.send(execute_remove_all(client, &mut self.tracked_keys, force));
            }
        }
    }
}

fn execute_put(
    client: &StoreClient,
    tracked_keys: &mut BTreeSet<TrackedKey>,
    request: pb::PutRequest,
) -> i32 {
    let tenant = normalized_tenant(client, &request.tenant);
    let policy = proto_policy(&request.replication);
    let result = match policy.as_ref() {
        Some(policy) => client.put_in_tenant_with_policy(&tenant, &request.key, &request.value, policy),
        None => client.put_in_tenant(&tenant, &request.key, &request.value),
    };
    if result.is_ok() {
        tracked_keys.insert((tenant, request.key));
        0
    } else {
        -1
    }
}

fn execute_get(client: &StoreClient, request: pb::GetRequest) -> pb::GetReply {
    let tenant = normalized_tenant(client, &request.tenant);
    match client.get_in_tenant(&tenant, &request.key) {
        Ok(value) => pb::GetReply { status: 0, value },
        Err(_) => pb::GetReply {
            status: -1,
            value: Vec::new(),
        },
    }
}

fn execute_batch_is_exist(client: &StoreClient, request: pb::BatchIsExistRequest) -> Vec<i32> {
    let tenants = request
        .objects
        .iter()
        .map(|object| normalized_tenant(client, &object.tenant))
        .collect::<Vec<_>>();
    let refs = request
        .objects
        .iter()
        .zip(tenants.iter())
        .map(|(object, tenant)| ObjectRef::new(object.key.as_str()).tenant(tenant.as_str()))
        .collect::<Vec<_>>();
    client
        .batch_is_exist(&refs)
        .map(|items| items.into_iter().map(i32::from).collect())
        .unwrap_or_else(|_| vec![-1; refs.len()])
}

fn execute_batch_put_from(
    client: &StoreClient,
    regions: &Arc<Mutex<RegionMap>>,
    tracked_keys: &mut BTreeSet<TrackedKey>,
    request: pb::BatchPutFromRequest,
) -> Vec<i32> {
    let client_id = request_client_id(request.client_id_hi, request.client_id_lo);
    let tenant = normalized_tenant(client, &request.tenant);
    let policy = proto_policy(&request.replication);
    let mut statuses = Vec::with_capacity(request.items.len());
    for item in &request.items {
        let status = match item.buffer.as_ref() {
            Some(buffer) => {
                let regions = regions.lock();
                match resolve_shared_slice(&regions, client_id, buffer) {
                    Ok(payload) => {
                        let result = match policy.as_ref() {
                            Some(policy) => {
                                client.put_in_tenant_with_policy(&tenant, &item.key, payload, policy)
                            }
                            None => client.put_in_tenant(&tenant, &item.key, payload),
                        };
                        if result.is_ok() {
                            0
                        } else {
                            -1
                        }
                    }
                    Err(_) => -1,
                }
            }
            None => -1,
        };
        if status == 0 {
            tracked_keys.insert((tenant.clone(), item.key.clone()));
        }
        statuses.push(status);
    }
    statuses
}

fn execute_batch_put_from_multi_buffers(
    client: &StoreClient,
    regions: &Arc<Mutex<RegionMap>>,
    tracked_keys: &mut BTreeSet<TrackedKey>,
    request: pb::BatchPutFromMultiBuffersRequest,
) -> Vec<i32> {
    let client_id = request_client_id(request.client_id_hi, request.client_id_lo);
    let tenant = normalized_tenant(client, &request.tenant);
    let policy = proto_policy(&request.replication);
    let mut statuses = Vec::with_capacity(request.items.len());
    for item in &request.items {
        let status = match item.buffers.as_ref() {
            Some(group) => {
                let regions = regions.lock();
                let mut buffers = Vec::with_capacity(group.buffers.len());
                let mut invalid = false;
                for buffer in &group.buffers {
                    match resolve_shared_slice(&regions, client_id, buffer) {
                        Ok(slice) => buffers.push(slice),
                        Err(_) => {
                            invalid = true;
                            break;
                        }
                    }
                }
                if invalid {
                    -1
                } else {
                    let mut request = MultiBufferPutRequest::new(item.key.as_str(), buffers.as_slice());
                    request = request.tenant(tenant.as_str());
                    if let Some(policy) = policy.clone() {
                        request = request.replication(policy);
                    }
                    match client.batch_put_from_multi_buffers(std::slice::from_ref(&request)) {
                        Ok(_) => 0,
                        Err(_) => -1,
                    }
                }
            }
            None => -1,
        };
        if status == 0 {
            tracked_keys.insert((tenant.clone(), item.key.clone()));
        }
        statuses.push(status);
    }
    statuses
}

fn execute_batch_get_into(
    client: &StoreClient,
    regions: &Arc<Mutex<RegionMap>>,
    request: pb::BatchGetIntoRequest,
) -> Vec<i64> {
    let client_id = request_client_id(request.client_id_hi, request.client_id_lo);
    let tenant = normalized_tenant(client, &request.tenant);
    let mut lengths = Vec::with_capacity(request.items.len());
    for item in &request.items {
        let length = match item.buffer.as_ref() {
            Some(buffer) => {
                let regions = regions.lock();
                match resolve_shared_slice_mut(&regions, client_id, buffer) {
                    Ok(target) => match client.get_into_in_tenant(&tenant, &item.key, target) {
                        Ok(size) => size as i64,
                        Err(_) => -1,
                    },
                    Err(_) => -1,
                }
            }
            None => -1,
        };
        lengths.push(length);
    }
    lengths
}

fn execute_batch_get_into_multi_buffers(
    client: &StoreClient,
    regions: &Arc<Mutex<RegionMap>>,
    request: pb::BatchGetIntoMultiBuffersRequest,
) -> Vec<i64> {
    let client_id = request_client_id(request.client_id_hi, request.client_id_lo);
    let tenant = normalized_tenant(client, &request.tenant);
    let mut lengths = Vec::with_capacity(request.items.len());
    for item in &request.items {
        let length = match item.buffers.as_ref() {
            Some(group) => {
                let regions = regions.lock();
                let mut buffers = Vec::with_capacity(group.buffers.len());
                let mut invalid = false;
                for buffer in &group.buffers {
                    match resolve_shared_slice_mut(&regions, client_id, buffer) {
                        Ok(slice) => buffers.push(slice),
                        Err(_) => {
                            invalid = true;
                            break;
                        }
                    }
                }
                if invalid {
                    -1
                } else {
                    let mut request = MultiBufferGetRequest::new(item.key.as_str(), buffers.as_mut_slice());
                    request = request.tenant(tenant.as_str());
                    match client.batch_get_into_multi_buffers(std::slice::from_mut(&mut request)) {
                        Ok(mut sizes) => sizes.pop().map(|size| size as i64).unwrap_or(-1),
                        Err(_) => -1,
                    }
                }
            }
            None => -1,
        };
        lengths.push(length);
    }
    lengths
}

fn execute_remove_all(
    client: &StoreClient,
    tracked_keys: &mut BTreeSet<TrackedKey>,
    force: bool,
) -> (i32, i64) {
    let keys = tracked_keys.iter().cloned().collect::<Vec<_>>();
    let mut removed = 0i64;
    for (tenant, key) in keys {
        if client.remove_in_tenant(&tenant, &key, force).is_ok() {
            tracked_keys.remove(&(tenant, key));
            removed += 1;
        }
    }
    (0, removed)
}

fn resolve_shared_slice<'a>(
    regions: &'a RegionMap,
    client_id: DummyClientId,
    buffer: &pb::SharedBufferRef,
) -> Result<&'a [u8], StoreError> {
    let offset = usize::try_from(buffer.offset)
        .map_err(|_| StoreError::Allocator("shared buffer offset overflow".to_string()))?;
    let len = usize::try_from(buffer.length)
        .map_err(|_| StoreError::Allocator("shared buffer length overflow".to_string()))?;
    let region = regions
        .get(&region_key(client_id, buffer.region_id))
        .ok_or_else(|| StoreError::NotFound("shared region not found".to_string()))?;
    region.slice(offset, len)
}

fn resolve_shared_slice_mut<'a>(
    regions: &'a RegionMap,
    client_id: DummyClientId,
    buffer: &pb::SharedBufferRef,
) -> Result<&'a mut [u8], StoreError> {
    let offset = usize::try_from(buffer.offset)
        .map_err(|_| StoreError::Allocator("shared buffer offset overflow".to_string()))?;
    let len = usize::try_from(buffer.length)
        .map_err(|_| StoreError::Allocator("shared buffer length overflow".to_string()))?;
    let region = regions
        .get(&region_key(client_id, buffer.region_id))
        .ok_or_else(|| StoreError::NotFound("shared region not found".to_string()))?;
    region.slice_mut(offset, len)
}

fn request_client_id(high: u64, low: u64) -> DummyClientId {
    DummyClientId { high, low }
}

fn region_key(client_id: DummyClientId, region_id: u64) -> RegionKey {
    (client_id.high, client_id.low, region_id)
}

fn executor_dropped() -> StoreError {
    StoreError::Transport("dummy client loop stopped".to_string())
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
