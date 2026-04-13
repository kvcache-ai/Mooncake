use std::collections::{BTreeMap, BTreeSet};
use std::ffi::c_void;
use std::sync::mpsc;
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::Duration;

use mooncake_store_client::{
    MooncakeCompatibilityFacade, MultiBufferGetRequest, MultiBufferPutRequest, ObjectRef,
    ReplicationPolicy, StoreClient,
};
use mooncake_store_core::{
    ClientEpoch, ClientLease, ClientLifecycleState, ClientRuntimeId, HandoffKind, HandoffPlan,
    StoreError,
};
use parking_lot::Mutex;

use crate::dummy_service::pb;
use crate::shm::{DummyClientId, OwnedMappedRegion};

type RegionKey = (u64, u64, u64);
type RegionMap = BTreeMap<RegionKey, OwnedMappedRegion>;
type TrackedKey = (String, String);
type Task = Box<dyn FnOnce(&mut StoreClient, &mut DispatcherState) + Send + 'static>;
const DISPATCHER_REQUEST_TIMEOUT: Duration = Duration::from_secs(5);

pub(crate) struct DispatcherState {
    tracked_keys: BTreeSet<TrackedKey>,
}

enum Message {
    Task(Task),
    Shutdown,
}

pub struct StoreDispatcher {
    regions: Arc<Mutex<RegionMap>>,
    sender: Mutex<Option<mpsc::Sender<Message>>>,
    thread: Mutex<Option<JoinHandle<()>>>,
}

impl StoreDispatcher {
    pub fn spawn(client: StoreClient, thread_name: impl Into<String>) -> Result<Self, StoreError> {
        let regions = Arc::new(Mutex::new(BTreeMap::new()));
        let (sender, receiver) = mpsc::channel();
        let thread = thread::Builder::new()
            .name(thread_name.into())
            .spawn(move || worker_loop(client, receiver))
            .map_err(|error| {
                StoreError::Transport(format!("failed to spawn store dispatcher: {error}"))
            })?;
        Ok(Self {
            regions,
            sender: Mutex::new(Some(sender)),
            thread: Mutex::new(Some(thread)),
        })
    }

    pub fn register_local_memory(&self) -> Result<(), StoreError> {
        self.run(|client| client.register_local_memory())
    }

    pub fn heartbeat(&self, expires_at_ms: u64) -> Result<(), StoreError> {
        self.run(move |client| client.heartbeat(expires_at_ms))
    }

    pub fn activate(&self) -> Result<(), StoreError> {
        self.run(|client| client.activate())
    }

    pub fn enter_standby(&self) -> Result<(), StoreError> {
        self.run(|client| client.enter_standby())
    }

    pub fn enter_draining(&self) -> Result<(), StoreError> {
        self.run(|client| client.enter_draining())
    }

    pub fn plan_handoff(
        &self,
        successor_epoch: ClientEpoch,
        kind: HandoffKind,
        barrier_version: u64,
        created_at_ms: u64,
        deadline_ms: Option<u64>,
    ) -> Result<HandoffPlan, StoreError> {
        self.run(move |client| {
            client.plan_handoff(
                successor_epoch,
                kind,
                barrier_version,
                created_at_ms,
                deadline_ms,
            )
        })
    }

    pub fn activate_if_targeted_handoff(&self) -> Result<Option<HandoffPlan>, StoreError> {
        self.run(|client| client.activate_if_targeted_handoff())
    }

    pub fn find_hot_upgrade_successor(&self) -> Result<Option<ClientLease>, StoreError> {
        self.run(|client| client.find_hot_upgrade_successor())
    }

    pub fn runtime_state(
        &self,
        runtime: ClientRuntimeId,
    ) -> Result<Option<ClientLifecycleState>, StoreError> {
        self.run(move |client| client.runtime_state(&runtime))
    }

    pub fn evacuate_owned_replicas(&self) -> Result<usize, StoreError> {
        self.run(|client| client.evacuate_owned_replicas())
    }

    pub fn evacuate_owned_replicas_to_runtime(
        &self,
        runtime: ClientRuntimeId,
    ) -> Result<usize, StoreError> {
        self.run(move |client| client.evacuate_owned_replicas_to_runtime(&runtime))
    }

    pub fn register_buffer(&self, base_ptr: usize, len: usize) -> Result<(), StoreError> {
        self.run(move |client| client.register_buffer(base_ptr as *mut c_void, len))
    }

    pub fn unregister_buffer(&self, base_ptr: usize, len: usize) -> Result<(), StoreError> {
        self.run(move |client| client.unregister_buffer(base_ptr as *mut c_void, len))
    }

    pub async fn unregister_buffer_async(
        &self,
        base_ptr: usize,
        len: usize,
    ) -> Result<(), StoreError> {
        self.run_async(move |client| client.unregister_buffer(base_ptr as *mut c_void, len))
            .await
    }

    pub(crate) fn run<T, F>(&self, f: F) -> Result<T, StoreError>
    where
        T: Send + 'static,
        F: FnOnce(&mut StoreClient) -> Result<T, StoreError> + Send + 'static,
    {
        self.run_with_state(move |client, _state| f(client))
    }

    pub(crate) async fn run_async<T, F>(&self, f: F) -> Result<T, StoreError>
    where
        T: Send + 'static,
        F: FnOnce(&mut StoreClient) -> Result<T, StoreError> + Send + 'static,
    {
        self.run_async_with_state(move |client, _state| f(client))
            .await
    }

    pub(crate) fn run_with_state<T, F>(&self, f: F) -> Result<T, StoreError>
    where
        T: Send + 'static,
        F: FnOnce(&mut StoreClient, &mut DispatcherState) -> Result<T, StoreError> + Send + 'static,
    {
        let (reply_tx, reply_rx) = std::sync::mpsc::sync_channel(1);
        self.send(Box::new(move |client, state| {
            let _ = reply_tx.send(f(client, state));
        }))?;
        match reply_rx.recv_timeout(DISPATCHER_REQUEST_TIMEOUT) {
            Ok(result) => result,
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => Err(dispatcher_timeout()),
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => Err(dispatcher_stopped()),
        }
    }

    pub(crate) async fn run_async_with_state<T, F>(&self, f: F) -> Result<T, StoreError>
    where
        T: Send + 'static,
        F: FnOnce(&mut StoreClient, &mut DispatcherState) -> Result<T, StoreError> + Send + 'static,
    {
        let (reply_tx, reply_rx) = std::sync::mpsc::sync_channel(1);
        self.send(Box::new(move |client, state| {
            let _ = reply_tx.send(f(client, state));
        }))?;
        match tokio::task::spawn_blocking(move || reply_rx.recv_timeout(DISPATCHER_REQUEST_TIMEOUT))
            .await
        {
            Ok(Ok(result)) => result,
            Ok(Err(std::sync::mpsc::RecvTimeoutError::Timeout)) => Err(dispatcher_timeout()),
            Ok(Err(std::sync::mpsc::RecvTimeoutError::Disconnected)) => Err(dispatcher_stopped()),
            Err(error) => Err(StoreError::Transport(format!(
                "store dispatcher wait worker failed: {error}"
            ))),
        }
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
        self.regions
            .lock()
            .remove(&region_key(client_id, region_id))
    }

    pub async fn put(&self, request: pb::PutRequest) -> Result<i32, StoreError> {
        self.run_async_with_state(move |client, state| Ok(execute_put(client, state, request)))
            .await
    }

    pub async fn get(&self, request: pb::GetRequest) -> Result<pb::GetReply, StoreError> {
        self.run_async(move |client| Ok(execute_get(client, request)))
            .await
    }

    pub async fn batch_is_exist(
        &self,
        request: pb::BatchIsExistRequest,
    ) -> Result<Vec<i32>, StoreError> {
        self.run_async(move |client| Ok(execute_batch_is_exist(client, request)))
            .await
    }

    pub async fn batch_put_from(
        &self,
        request: pb::BatchPutFromRequest,
    ) -> Result<Vec<i32>, StoreError> {
        let regions = self.regions.clone();
        self.run_async_with_state(move |client, state| {
            Ok(execute_batch_put_from(client, &regions, state, request))
        })
        .await
    }

    pub async fn batch_put_from_multi_buffers(
        &self,
        request: pb::BatchPutFromMultiBuffersRequest,
    ) -> Result<Vec<i32>, StoreError> {
        let regions = self.regions.clone();
        self.run_async_with_state(move |client, state| {
            Ok(execute_batch_put_from_multi_buffers(
                client, &regions, state, request,
            ))
        })
        .await
    }

    pub async fn batch_get_into(
        &self,
        request: pb::BatchGetIntoRequest,
    ) -> Result<Vec<i64>, StoreError> {
        let regions = self.regions.clone();
        self.run_async(move |client| Ok(execute_batch_get_into(client, &regions, request)))
            .await
    }

    pub async fn batch_get_into_multi_buffers(
        &self,
        request: pb::BatchGetIntoMultiBuffersRequest,
    ) -> Result<Vec<i64>, StoreError> {
        let regions = self.regions.clone();
        self.run_async(move |client| {
            Ok(execute_batch_get_into_multi_buffers(
                client, &regions, request,
            ))
        })
        .await
    }

    pub async fn remove_all(&self, force: bool) -> Result<(i32, i64), StoreError> {
        self.run_async_with_state(move |client, state| Ok(execute_remove_all(client, state, force)))
            .await
    }

    pub fn shutdown(&self) {
        let sender = self.sender.lock().take();
        if let Some(sender) = sender {
            let _ = sender.send(Message::Shutdown);
        }
        if let Some(thread) = self.thread.lock().take() {
            let _ = thread.join();
        }
    }

    fn send(&self, task: Task) -> Result<(), StoreError> {
        let sender = self.sender.lock();
        let Some(sender) = sender.as_ref() else {
            return Err(dispatcher_stopped());
        };
        sender
            .send(Message::Task(task))
            .map_err(|_| dispatcher_stopped())
    }
}

impl Drop for StoreDispatcher {
    fn drop(&mut self) {
        self.shutdown();
    }
}

fn worker_loop(mut client: StoreClient, receiver: mpsc::Receiver<Message>) {
    let mut state = DispatcherState {
        tracked_keys: BTreeSet::new(),
    };
    while let Ok(message) = receiver.recv() {
        match message {
            Message::Task(task) => task(&mut client, &mut state),
            Message::Shutdown => break,
        }
    }
}

fn execute_put(client: &StoreClient, state: &mut DispatcherState, request: pb::PutRequest) -> i32 {
    let tenant = normalized_tenant(client, &request.tenant);
    let policy = proto_policy(&request.replication);
    let result = match policy.as_ref() {
        Some(policy) => {
            client.put_in_tenant_with_policy(&tenant, &request.key, &request.value, policy)
        }
        None => client.put_in_tenant(&tenant, &request.key, &request.value),
    };
    if result.is_ok() {
        state.tracked_keys.insert((tenant, request.key));
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
    state: &mut DispatcherState,
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
                            Some(policy) => client
                                .put_in_tenant_with_policy(&tenant, &item.key, payload, policy),
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
            state
                .tracked_keys
                .insert((tenant.clone(), item.key.clone()));
        }
        statuses.push(status);
    }
    statuses
}

fn execute_batch_put_from_multi_buffers(
    client: &StoreClient,
    regions: &Arc<Mutex<RegionMap>>,
    state: &mut DispatcherState,
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
                    let mut request =
                        MultiBufferPutRequest::new(item.key.as_str(), buffers.as_slice());
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
            state
                .tracked_keys
                .insert((tenant.clone(), item.key.clone()));
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
                    let mut request =
                        MultiBufferGetRequest::new(item.key.as_str(), buffers.as_mut_slice());
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
    state: &mut DispatcherState,
    force: bool,
) -> (i32, i64) {
    let keys = state.tracked_keys.iter().cloned().collect::<Vec<_>>();
    let mut removed = 0i64;
    for (tenant, key) in keys {
        if client.remove_in_tenant(&tenant, &key, force).is_ok() {
            state.tracked_keys.remove(&(tenant, key));
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

fn dispatcher_stopped() -> StoreError {
    StoreError::Transport("store dispatcher stopped".to_string())
}

fn dispatcher_timeout() -> StoreError {
    StoreError::Transport(format!(
        "store dispatcher request timed out after {}ms",
        DISPATCHER_REQUEST_TIMEOUT.as_millis()
    ))
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

#[cfg(test)]
mod tests {
    use mooncake_store_client::StoreClient;

    use super::StoreDispatcher;

    #[test]
    fn dispatcher_handle_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<StoreDispatcher>();
    }

    #[test]
    fn store_client_is_send() {
        fn assert_send<T: Send>() {}
        assert_send::<StoreClient>();
    }
}
