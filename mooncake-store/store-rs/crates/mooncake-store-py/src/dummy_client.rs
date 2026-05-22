use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::thread::sleep;
use std::time::{Duration, Instant};

use mooncake_store_client::ReplicationPolicy;
use mooncake_store_core::{Result, StoreError};
use parking_lot::Mutex;
use tokio::runtime::Runtime;
use tonic::transport::{Channel, Endpoint};

use crate::config::CompatTimeoutConfig;
use crate::dummy_service::pb;
use crate::shm::{
    dummy_ipc_socket_path, hot_cache_ipc_socket_path, request_hot_cache_region,
    resolve_shared_region, send_shm_register_request, shared_region_for_registration,
    DummyClientId, OwnedMappedRegion, ShmRegisterRequest,
};

pub struct DummySession {
    runtime: Arc<Runtime>,
    channel: Channel,
    server_addr: String,
    socket_path: PathBuf,
    hot_cache_socket_path: PathBuf,
    client_id: DummyClientId,
    rpc_timeout: Duration,
    registered_regions: Mutex<BTreeMap<usize, RegisteredRegion>>,
    hot_cache_region: Mutex<Option<OwnedMappedRegion>>,
}

#[derive(Clone, Copy)]
struct RegisteredRegion {
    region_id: u64,
    requested_len: usize,
}

impl DummySession {
    pub fn connect(server_addr: &str, worker_scope: impl Into<String>) -> Result<Self> {
        let timeouts = CompatTimeoutConfig::from_env();
        Self::connect_with_rpc_timeout(server_addr, worker_scope, timeouts.dummy_rpc_timeout)
    }

    pub fn connect_with_rpc_timeout(
        server_addr: &str,
        worker_scope: impl Into<String>,
        rpc_timeout: Duration,
    ) -> Result<Self> {
        let worker_scope = worker_scope.into();
        let endpoint_uri = format!("http://{server_addr}");
        static NEXT_DUMMY_RUNTIME_ID: AtomicU64 = AtomicU64::new(1);
        let runtime_id = NEXT_DUMMY_RUNTIME_ID.fetch_add(1, Ordering::Relaxed);
        let runtime = Arc::new(
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(1)
                .thread_name(format!("dummy-session-{runtime_id}"))
                .enable_all()
                .build()
                .map_err(|error| {
                    StoreError::Transport(format!("dummy runtime should initialize: {error}"))
                })?,
        );
        let deadline = Instant::now() + Duration::from_secs(2);
        let channel = 'connect: loop {
            let endpoint = Endpoint::from_shared(endpoint_uri.clone()).map_err(|error| {
                StoreError::Transport(format!("invalid dummy server endpoint: {error}"))
            })?;
            match runtime.block_on(endpoint.connect()) {
                Ok(channel) => break 'connect channel,
                Err(_) if Instant::now() < deadline => {
                    sleep(Duration::from_millis(25));
                }
                Err(error) => {
                    return Err(StoreError::Transport(format!(
                        "failed to connect to dummy server {server_addr}: {error}",
                    )));
                }
            }
        };
        let session = Self {
            runtime,
            channel,
            server_addr: server_addr.to_string(),
            socket_path: dummy_ipc_socket_path(server_addr, &worker_scope),
            hot_cache_socket_path: hot_cache_ipc_socket_path(server_addr, &worker_scope),
            client_id: DummyClientId::new(),
            rpc_timeout,
            registered_regions: Mutex::new(BTreeMap::new()),
            hot_cache_region: Mutex::new(None),
        };
        session.try_map_hot_cache();
        Ok(session)
    }

    pub fn server_addr(&self) -> &str {
        &self.server_addr
    }

    pub fn close(&self) {
        let pointers = self
            .registered_regions
            .lock()
            .keys()
            .copied()
            .collect::<Vec<_>>();
        for pointer in pointers {
            let _ = self.unregister_buffer(pointer, None);
        }
        let _ = self.hot_cache_region.lock().take();
    }

    pub fn health_check(&self) -> i32 {
        let request = pb::HealthRequest {};
        match self.rpc(|mut client| async move { client.health(request).await }) {
            Ok(reply) => reply.into_inner().status,
            Err(_) => 2,
        }
    }

    pub fn put(
        &self,
        key: &str,
        value: &[u8],
        tenant: Option<&str>,
        policy: Option<&ReplicationPolicy>,
    ) -> Result<i32> {
        let request = pb::PutRequest {
            key: key.to_string(),
            tenant: tenant.unwrap_or_default().to_string(),
            value: value.to_vec(),
            replication: policy.map(replication_to_proto),
        };
        Ok(self
            .rpc(|mut client| async move { client.put(request).await })?
            .into_inner()
            .status)
    }

    pub fn get(&self, key: &str, tenant: Option<&str>) -> Result<(i32, Vec<u8>)> {
        if let Some(value) = self.try_get_hot_cache_bytes(key, tenant)? {
            return Ok((0, value));
        }
        let request = pb::GetRequest {
            key: key.to_string(),
            tenant: tenant.unwrap_or_default().to_string(),
        };
        let reply = self
            .rpc(|mut client| async move { client.get(request).await })?
            .into_inner();
        Ok((reply.status, reply.value))
    }

    pub fn remove(&self, key: &str, tenant: Option<&str>, force: bool) -> Result<i32> {
        let request = pb::RemoveRequest {
            key: key.to_string(),
            tenant: tenant.unwrap_or_default().to_string(),
            force,
        };
        Ok(self
            .rpc(|mut client| async move { client.remove(request).await })?
            .into_inner()
            .status)
    }

    pub fn batch_remove(
        &self,
        keys: &[String],
        tenant: Option<&str>,
        force: bool,
    ) -> Result<Vec<i32>> {
        let tenant_str = tenant.unwrap_or_default().to_string();
        let request = pb::BatchRemoveRequest {
            objects: keys
                .iter()
                .map(|key| pb::ObjectRef {
                    key: key.clone(),
                    tenant: tenant_str.clone(),
                })
                .collect(),
            force,
        };
        Ok(self
            .rpc(|mut client| async move { client.batch_remove(request).await })?
            .into_inner()
            .statuses)
    }

    pub fn batch_is_exist(&self, keys: &[String], tenant: Option<&str>) -> Result<Vec<i32>> {
        let request = pb::BatchIsExistRequest {
            objects: keys
                .iter()
                .map(|key| pb::ObjectRef {
                    key: key.clone(),
                    tenant: tenant.unwrap_or_default().to_string(),
                })
                .collect(),
        };
        Ok(self
            .rpc(|mut client| async move { client.batch_is_exist(request).await })?
            .into_inner()
            .statuses)
    }

    pub fn register_buffer(&self, buffer_ptr: usize, size: usize) -> Result<i32> {
        let registration = shared_region_for_registration(buffer_ptr, size)?;
        let request = ShmRegisterRequest::new(
            self.client_id,
            registration.region_id,
            registration.registered_len,
        );
        send_shm_register_request(&self.socket_path, &request, &registration.fd)?;

        let mut regions = self.registered_regions.lock();
        use std::collections::btree_map::Entry;
        match regions.entry(buffer_ptr) {
            Entry::Occupied(_) => Err(StoreError::Allocator(format!(
                "buffer {buffer_ptr:#x} is already registered"
            ))),
            Entry::Vacant(vacant) => {
                vacant.insert(RegisteredRegion {
                    region_id: registration.region_id,
                    requested_len: registration.requested_len,
                });
                Ok(0)
            }
        }
    }

    pub fn unregister_buffer(&self, buffer_ptr: usize, size: Option<usize>) -> Result<i32> {
        let region = self
            .registered_regions
            .lock()
            .remove(&buffer_ptr)
            .ok_or_else(|| {
                StoreError::NotFound(format!("buffer {buffer_ptr:#x} is not registered"))
            })?;
        if let Some(size) = size {
            if size != region.requested_len {
                return Err(StoreError::Allocator(format!(
                    "registered buffer size mismatch: requested={size} actual={}",
                    region.requested_len
                )));
            }
        }
        let request = pb::UnregisterRegionRequest {
            client_id_hi: self.client_id.high,
            client_id_lo: self.client_id.low,
            region_id: region.region_id,
        };
        Ok(self
            .rpc(|mut client| async move { client.unregister_region(request).await })?
            .into_inner()
            .status)
    }

    pub fn batch_put_from(
        &self,
        items: &[(String, usize, usize)],
        tenant: Option<&str>,
        policy: Option<&ReplicationPolicy>,
    ) -> Result<Vec<i32>> {
        let request = pb::BatchPutFromRequest {
            client_id_hi: self.client_id.high,
            client_id_lo: self.client_id.low,
            tenant: tenant.unwrap_or_default().to_string(),
            replication: policy.map(replication_to_proto),
            items: items
                .iter()
                .map(|(key, ptr, size)| {
                    let region = resolve_shared_region(*ptr, *size)?;
                    self.ensure_region_registered(region.base)?;
                    Ok(pb::SharedPutItem {
                        key: key.clone(),
                        buffer: Some(pb::SharedBufferRef {
                            region_id: region.region_id,
                            offset: region.offset as u64,
                            length: region.len as u64,
                        }),
                    })
                })
                .collect::<Result<Vec<_>>>()?,
        };
        Ok(self
            .rpc(|mut client| async move { client.batch_put_from(request).await })?
            .into_inner()
            .statuses)
    }

    pub fn batch_put_from_multi_buffers(
        &self,
        items: &[(String, Vec<(usize, usize)>)],
        tenant: Option<&str>,
        policy: Option<&ReplicationPolicy>,
    ) -> Result<Vec<i32>> {
        let request = pb::BatchPutFromMultiBuffersRequest {
            client_id_hi: self.client_id.high,
            client_id_lo: self.client_id.low,
            tenant: tenant.unwrap_or_default().to_string(),
            replication: policy.map(replication_to_proto),
            items: items
                .iter()
                .map(|(key, buffers)| {
                    Ok(pb::SharedMultiPutItem {
                        key: key.clone(),
                        buffers: Some(pb::SharedBufferGroup {
                            buffers: buffers
                                .iter()
                                .map(|(ptr, size)| self.shared_buffer_ref(*ptr, *size))
                                .collect::<Result<Vec<_>>>()?,
                        }),
                    })
                })
                .collect::<Result<Vec<_>>>()?,
        };
        Ok(self
            .rpc(|mut client| async move { client.batch_put_from_multi_buffers(request).await })?
            .into_inner()
            .statuses)
    }

    pub fn put_from(
        &self,
        key: &str,
        buffer_ptr: usize,
        size: usize,
        tenant: Option<&str>,
        policy: Option<&ReplicationPolicy>,
    ) -> Result<i32> {
        Ok(self
            .batch_put_from(&[(key.to_string(), buffer_ptr, size)], tenant, policy)?
            .into_iter()
            .next()
            .unwrap_or(-1))
    }

    pub fn batch_get_into(
        &self,
        items: &[(String, usize, usize)],
        tenant: Option<&str>,
    ) -> Result<Vec<i64>> {
        let mut lengths = vec![-1; items.len()];
        let hot_replies = self.batch_acquire_hot_cache(items, tenant)?;
        let mut hit_handles = Vec::new();
        let mut misses = Vec::new();

        for (index, ((key, ptr, size), reply)) in items.iter().zip(hot_replies.iter()).enumerate() {
            if reply.status == 0 {
                let region = resolve_shared_region(*ptr, *size)?;
                self.ensure_region_registered(region.base)?;
                let target = unsafe { std::slice::from_raw_parts_mut(*ptr as *mut u8, *size) };
                lengths[index] = self.copy_hot_cache_reply_to_slice(reply, target)? as i64;
                hit_handles.push(pb::HotCacheReleaseRequest {
                    block_id: reply.block_id,
                    generation: reply.generation,
                });
            } else {
                misses.push((index, key.clone(), *ptr, *size));
            }
        }
        self.batch_release_hot_cache_handles(hit_handles)?;

        if misses.is_empty() {
            return Ok(lengths);
        }

        let request = pb::BatchGetIntoRequest {
            client_id_hi: self.client_id.high,
            client_id_lo: self.client_id.low,
            tenant: tenant.unwrap_or_default().to_string(),
            items: misses
                .iter()
                .map(|(_, key, ptr, size)| {
                    let region = resolve_shared_region(*ptr, *size)?;
                    self.ensure_region_registered(region.base)?;
                    Ok(pb::SharedGetItem {
                        key: key.clone(),
                        buffer: Some(pb::SharedBufferRef {
                            region_id: region.region_id,
                            offset: region.offset as u64,
                            length: region.len as u64,
                        }),
                    })
                })
                .collect::<Result<Vec<_>>>()?,
        };
        let miss_lengths = self
            .rpc(|mut client| async move { client.batch_get_into(request).await })?
            .into_inner()
            .lengths;
        for ((index, _, _, _), length) in misses.into_iter().zip(miss_lengths) {
            lengths[index] = length;
        }
        Ok(lengths)
    }

    pub fn batch_get_into_multi_buffers(
        &self,
        items: &[(String, Vec<(usize, usize)>)],
        tenant: Option<&str>,
    ) -> Result<Vec<i64>> {
        let mut lengths = vec![-1; items.len()];
        let hot_replies = self.batch_acquire_hot_cache_multi(items, tenant)?;
        let mut hit_handles = Vec::new();
        let mut misses = Vec::new();

        for (index, ((key, buffers), reply)) in items.iter().zip(hot_replies.iter()).enumerate() {
            if reply.status == 0 {
                let mut targets = buffers
                    .iter()
                    .map(|(ptr, size)| {
                        let region = resolve_shared_region(*ptr, *size)?;
                        self.ensure_region_registered(region.base)?;
                        Ok(unsafe { std::slice::from_raw_parts_mut(*ptr as *mut u8, *size) })
                    })
                    .collect::<Result<Vec<_>>>()?;
                lengths[index] =
                    self.copy_hot_cache_reply_to_slices(reply, targets.as_mut_slice())? as i64;
                hit_handles.push(pb::HotCacheReleaseRequest {
                    block_id: reply.block_id,
                    generation: reply.generation,
                });
            } else {
                misses.push((index, key.clone(), buffers.clone()));
            }
        }
        self.batch_release_hot_cache_handles(hit_handles)?;

        if misses.is_empty() {
            return Ok(lengths);
        }

        let request = pb::BatchGetIntoMultiBuffersRequest {
            client_id_hi: self.client_id.high,
            client_id_lo: self.client_id.low,
            tenant: tenant.unwrap_or_default().to_string(),
            items: misses
                .iter()
                .map(|(_, key, buffers)| {
                    Ok(pb::SharedMultiGetItem {
                        key: key.clone(),
                        buffers: Some(pb::SharedBufferGroup {
                            buffers: buffers
                                .iter()
                                .map(|(ptr, size)| self.shared_buffer_ref(*ptr, *size))
                                .collect::<Result<Vec<_>>>()?,
                        }),
                    })
                })
                .collect::<Result<Vec<_>>>()?,
        };
        let miss_lengths = self
            .rpc(|mut client| async move { client.batch_get_into_multi_buffers(request).await })?
            .into_inner()
            .lengths;
        for ((index, _, _), length) in misses.into_iter().zip(miss_lengths) {
            lengths[index] = length;
        }
        Ok(lengths)
    }

    pub fn get_into(
        &self,
        key: &str,
        buffer_ptr: usize,
        size: usize,
        tenant: Option<&str>,
    ) -> Result<i64> {
        Ok(self
            .batch_get_into(&[(key.to_string(), buffer_ptr, size)], tenant)?
            .into_iter()
            .next()
            .unwrap_or(-1))
    }

    pub fn remove_all(&self, force: bool) -> Result<(i32, i64)> {
        let reply = self
            .rpc(
                |mut client| async move { client.remove_all(pb::RemoveAllRequest { force }).await },
            )?
            .into_inner();
        Ok((reply.status, reply.removed))
    }

    #[cfg(test)]
    pub(crate) fn has_hot_cache_mapping(&self) -> bool {
        self.hot_cache_region.lock().is_some()
    }

    fn try_map_hot_cache(&self) {
        if let Ok(region) = request_hot_cache_region(&self.hot_cache_socket_path, self.client_id) {
            *self.hot_cache_region.lock() = Some(region);
        }
    }

    fn try_get_hot_cache_bytes(&self, key: &str, tenant: Option<&str>) -> Result<Option<Vec<u8>>> {
        let Some(reply) = self.acquire_hot_cache(key, tenant)? else {
            return Ok(None);
        };
        let value = self.copy_hot_cache_reply_to_vec(&reply)?;
        self.release_hot_cache_handle(&reply)?;
        Ok(Some(value))
    }

    fn acquire_hot_cache(
        &self,
        key: &str,
        tenant: Option<&str>,
    ) -> Result<Option<pb::HotCacheAcquireReply>> {
        if self.hot_cache_region.lock().is_none() {
            return Ok(None);
        }
        let request = pb::HotCacheAcquireRequest {
            key: key.to_string(),
            tenant: tenant.unwrap_or_default().to_string(),
        };
        let reply = self
            .rpc(|mut client| async move { client.acquire_hot_cache(request).await })?
            .into_inner();
        if reply.status == 0 {
            Ok(Some(reply))
        } else {
            Ok(None)
        }
    }

    fn batch_acquire_hot_cache(
        &self,
        items: &[(String, usize, usize)],
        tenant: Option<&str>,
    ) -> Result<Vec<pb::HotCacheAcquireReply>> {
        if self.hot_cache_region.lock().is_none() {
            return Ok(vec![
                pb::HotCacheAcquireReply {
                    status: -1,
                    ..Default::default()
                };
                items.len()
            ]);
        }
        let request = pb::BatchHotCacheAcquireRequest {
            objects: items
                .iter()
                .map(|(key, _, _)| pb::ObjectRef {
                    key: key.clone(),
                    tenant: tenant.unwrap_or_default().to_string(),
                })
                .collect(),
        };
        Ok(self
            .rpc(|mut client| async move { client.batch_acquire_hot_cache(request).await })?
            .into_inner()
            .items)
    }

    fn batch_acquire_hot_cache_multi(
        &self,
        items: &[(String, Vec<(usize, usize)>)],
        tenant: Option<&str>,
    ) -> Result<Vec<pb::HotCacheAcquireReply>> {
        if self.hot_cache_region.lock().is_none() {
            return Ok(vec![
                pb::HotCacheAcquireReply {
                    status: -1,
                    ..Default::default()
                };
                items.len()
            ]);
        }
        let request = pb::BatchHotCacheAcquireRequest {
            objects: items
                .iter()
                .map(|(key, _)| pb::ObjectRef {
                    key: key.clone(),
                    tenant: tenant.unwrap_or_default().to_string(),
                })
                .collect(),
        };
        Ok(self
            .rpc(|mut client| async move { client.batch_acquire_hot_cache(request).await })?
            .into_inner()
            .items)
    }

    fn release_hot_cache_handle(&self, reply: &pb::HotCacheAcquireReply) -> Result<()> {
        let request = pb::HotCacheReleaseRequest {
            block_id: reply.block_id,
            generation: reply.generation,
        };
        let _ = self.rpc(|mut client| async move { client.release_hot_cache(request).await })?;
        Ok(())
    }

    fn batch_release_hot_cache_handles(
        &self,
        handles: Vec<pb::HotCacheReleaseRequest>,
    ) -> Result<()> {
        if handles.is_empty() {
            return Ok(());
        }
        let request = pb::BatchHotCacheReleaseRequest { handles };
        let _ =
            self.rpc(|mut client| async move { client.batch_release_hot_cache(request).await })?;
        Ok(())
    }

    fn copy_hot_cache_reply_to_vec(&self, reply: &pb::HotCacheAcquireReply) -> Result<Vec<u8>> {
        let region = self.hot_cache_region.lock();
        let Some(region) = region.as_ref() else {
            return Err(StoreError::NotFound(
                "dummy hot cache shm is not mapped".to_string(),
            ));
        };
        let offset = usize::try_from(reply.offset)
            .map_err(|_| StoreError::Allocator("hot cache offset overflow".to_string()))?;
        let len = usize::try_from(reply.length)
            .map_err(|_| StoreError::Allocator("hot cache length overflow".to_string()))?;
        Ok(region.slice(offset, len)?.to_vec())
    }

    fn copy_hot_cache_reply_to_slice(
        &self,
        reply: &pb::HotCacheAcquireReply,
        target: &mut [u8],
    ) -> Result<usize> {
        let region = self.hot_cache_region.lock();
        let Some(region) = region.as_ref() else {
            return Err(StoreError::NotFound(
                "dummy hot cache shm is not mapped".to_string(),
            ));
        };
        let offset = usize::try_from(reply.offset)
            .map_err(|_| StoreError::Allocator("hot cache offset overflow".to_string()))?;
        let len = usize::try_from(reply.length)
            .map_err(|_| StoreError::Allocator("hot cache length overflow".to_string()))?;
        if len > target.len() {
            return Err(StoreError::Allocator(format!(
                "dummy hot cache target too small: value={len} target={}",
                target.len()
            )));
        }
        target[..len].copy_from_slice(region.slice(offset, len)?);
        Ok(len)
    }

    fn copy_hot_cache_reply_to_slices(
        &self,
        reply: &pb::HotCacheAcquireReply,
        targets: &mut [&mut [u8]],
    ) -> Result<usize> {
        let region = self.hot_cache_region.lock();
        let Some(region) = region.as_ref() else {
            return Err(StoreError::NotFound(
                "dummy hot cache shm is not mapped".to_string(),
            ));
        };
        let offset = usize::try_from(reply.offset)
            .map_err(|_| StoreError::Allocator("hot cache offset overflow".to_string()))?;
        let len = usize::try_from(reply.length)
            .map_err(|_| StoreError::Allocator("hot cache length overflow".to_string()))?;
        let source = region.slice(offset, len)?;
        let capacity = targets.iter().map(|target| target.len()).sum::<usize>();
        if len > capacity {
            return Err(StoreError::Allocator(format!(
                "dummy hot cache targets too small: value={len} target={capacity}"
            )));
        }
        let mut copied = 0usize;
        for target in targets {
            if copied == len {
                break;
            }
            let chunk = target.len().min(len - copied);
            target[..chunk].copy_from_slice(&source[copied..copied + chunk]);
            copied += chunk;
        }
        Ok(len)
    }

    fn ensure_region_registered(&self, base_ptr: usize) -> Result<()> {
        if self.registered_regions.lock().contains_key(&base_ptr) {
            return Ok(());
        }
        Err(StoreError::Allocator(format!(
            "shared region {base_ptr:#x} is not registered with dummy server"
        )))
    }

    fn shared_buffer_ref(&self, ptr: usize, size: usize) -> Result<pb::SharedBufferRef> {
        let region = resolve_shared_region(ptr, size)?;
        self.ensure_region_registered(region.base)?;
        Ok(pb::SharedBufferRef {
            region_id: region.region_id,
            offset: region.offset as u64,
            length: region.len as u64,
        })
    }

    fn rpc<F, Fut, T>(&self, f: F) -> Result<T>
    where
        F: FnOnce(pb::dummy_store_service_client::DummyStoreServiceClient<Channel>) -> Fut,
        Fut: std::future::Future<Output = std::result::Result<T, tonic::Status>>,
    {
        self.runtime
            .block_on(async {
                tokio::time::timeout(
                    self.rpc_timeout,
                    f(
                        pb::dummy_store_service_client::DummyStoreServiceClient::new(
                            self.channel.clone(),
                        ),
                    ),
                )
                .await
            })
            .map_err(|_| {
                StoreError::Transport(format!(
                    "dummy rpc timed out after {}ms",
                    self.rpc_timeout.as_millis()
                ))
            })?
            .map_err(|error| StoreError::Transport(format!("dummy rpc failed: {error}")))
    }
}

fn replication_to_proto(policy: &ReplicationPolicy) -> pb::ReplicationPolicy {
    pb::ReplicationPolicy {
        replica_count: policy.replica_count.unwrap_or(1) as u32,
        preferred_segments: policy
            .preferred_segments
            .iter()
            .map(|segment| segment.0.clone())
            .collect(),
        preferred_storage_owners: policy.preferred_storage_owners.clone(),
        prefer_local: policy.prefer_local,
        prefer_alloc_in_same_node: policy.prefer_alloc_in_same_node,
        with_soft_pin: policy.with_soft_pin,
    }
}
