use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::LazyLock;
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
    dummy_ipc_socket_path, resolve_shared_region, send_shm_register_request,
    shared_region_for_registration, DummyClientId, ShmRegisterRequest,
};

static DUMMY_RUNTIME: LazyLock<Runtime> =
    LazyLock::new(|| Runtime::new().expect("dummy runtime should initialize"));

pub struct DummySession {
    channel: Channel,
    server_addr: String,
    socket_path: PathBuf,
    client_id: DummyClientId,
    rpc_timeout: Duration,
    registered_regions: Mutex<BTreeMap<usize, RegisteredRegion>>,
}

#[derive(Clone, Copy)]
struct RegisteredRegion {
    region_id: u64,
    requested_len: usize,
}

impl DummySession {
    pub fn connect(server_addr: &str) -> Result<Self> {
        let timeouts = CompatTimeoutConfig::from_env();
        Self::connect_with_rpc_timeout(server_addr, timeouts.dummy_rpc_timeout)
    }

    pub fn connect_with_rpc_timeout(server_addr: &str, rpc_timeout: Duration) -> Result<Self> {
        let endpoint_uri = format!("http://{server_addr}");
        let deadline = Instant::now() + Duration::from_secs(2);
        let channel = 'connect: loop {
            let endpoint = Endpoint::from_shared(endpoint_uri.clone()).map_err(|error| {
                StoreError::Transport(format!("invalid dummy server endpoint: {error}"))
            })?;
            match DUMMY_RUNTIME.block_on(endpoint.connect()) {
                Ok(channel) => break 'connect channel,
                Err(_) if Instant::now() < deadline => {
                    sleep(Duration::from_millis(25));
                }
                Err(error) => {
                    return Err(StoreError::Transport(format!(
                        "failed to connect to dummy server: {}",
                        error,
                    )));
                }
            }
        };
        Ok(Self {
            channel,
            server_addr: server_addr.to_string(),
            socket_path: dummy_ipc_socket_path(server_addr),
            client_id: DummyClientId::new(),
            rpc_timeout: rpc_timeout.max(Duration::from_millis(1)),
            registered_regions: Mutex::new(BTreeMap::new()),
        })
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
        let request = pb::GetRequest {
            key: key.to_string(),
            tenant: tenant.unwrap_or_default().to_string(),
        };
        let reply = self
            .rpc(|mut client| async move { client.get(request).await })?
            .into_inner();
        Ok((reply.status, reply.value))
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
        self.registered_regions.lock().insert(
            buffer_ptr,
            RegisteredRegion {
                region_id: registration.region_id,
                requested_len: registration.requested_len,
            },
        );
        Ok(0)
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
        let request = pb::BatchGetIntoRequest {
            client_id_hi: self.client_id.high,
            client_id_lo: self.client_id.low,
            tenant: tenant.unwrap_or_default().to_string(),
            items: items
                .iter()
                .map(|(key, ptr, size)| {
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
        Ok(self
            .rpc(|mut client| async move { client.batch_get_into(request).await })?
            .into_inner()
            .lengths)
    }

    pub fn batch_get_into_multi_buffers(
        &self,
        items: &[(String, Vec<(usize, usize)>)],
        tenant: Option<&str>,
    ) -> Result<Vec<i64>> {
        let request = pb::BatchGetIntoMultiBuffersRequest {
            client_id_hi: self.client_id.high,
            client_id_lo: self.client_id.low,
            tenant: tenant.unwrap_or_default().to_string(),
            items: items
                .iter()
                .map(|(key, buffers)| {
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
        Ok(self
            .rpc(|mut client| async move { client.batch_get_into_multi_buffers(request).await })?
            .into_inner()
            .lengths)
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
        DUMMY_RUNTIME
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
