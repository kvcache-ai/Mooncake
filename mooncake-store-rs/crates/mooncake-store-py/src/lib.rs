pub mod admin;
pub mod config;
pub mod dispatcher;
mod dummy_client;
pub mod dummy_service;
mod hot_cache;
pub mod runtime;
mod shm;
#[cfg(test)]
mod test_support;

use std::collections::BTreeMap;
use std::ffi::c_void;
use std::slice;

use dispatcher::StoreDispatcher;
use dummy_client::DummySession;
use mooncake_store_client::{
    init_tracing as init_store_tracing, metrics_http_server_addr, render_prometheus_metrics,
    start_metrics_http_server, stop_metrics_http_server, MooncakeCompatibilityFacade,
    MultiBufferPutRequest, ObjectRef, PutFromRequest, PutRequest, ReplicationPolicy,
    RouteControlMode,
};
use mooncake_store_core::{
    ClientEpoch, ClientLifecycleState, ObjectRoute, SegmentAnnouncement, SegmentName, StoreError,
};
use pyo3::exceptions::{PyKeyError, PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList};
use runtime::CompatRuntimeArgs;

enum StoreBackend {
    Real(StoreDispatcher),
    Dummy(DummySession),
}

impl StoreBackend {
    fn close(&self) {
        match self {
            Self::Real(dispatcher) => {
                dispatcher.stop_heartbeat_loop();
                let _ = dispatcher.enter_offline();
                dispatcher.shutdown();
            }
            Self::Dummy(dummy) => dummy.close(),
        }
    }
}

#[pyclass(name = "MooncakeDistributedStore")]
struct PyMooncakeDistributedStore {
    backend: Option<StoreBackend>,
}

#[pyclass(name = "MooncakeHostMemAllocator")]
struct PyMooncakeHostMemAllocator {
    use_hugepage: Option<bool>,
    hugepage_size: Option<usize>,
}

#[pymethods]
impl PyMooncakeDistributedStore {
    #[new]
    fn new() -> Self {
        Self { backend: None }
    }

    /// Starts the real Store-RS runtime behind the Python compatibility facade.
    ///
    /// Tenant-scoped routing and resource policy should be authored through
    /// `mooncake-store-admin policy ...` and durable metadata. `route_topk` and
    /// `route_control` are accepted here as compatibility/bootstrap fallbacks so
    /// existing Python integrations continue to work.
    #[pyo3(signature = (
        local_hostname,
        metadata_url,
        global_segment_size,
        local_buffer_size,
        protocol = "tcp",
        rdma_devices = "",
        master_server = "",
        *,
        stable_id = None,
        epoch = 1,
        initial_state = "active",
        tenant = "default",
        labels = None,
        routed_writes = false,
        replica_count = 1,
        route_topk = 2,
        keyspace = None,
        transport_metadata_url = None,
        transport_rpc_port = None,
        transport_backend = None,
        local_segment_name = None,
        expires_at_ms = None,
        use_hugepage = None,
        hugepage_size = None,
        route_control = "embedded_wrh"
    ), text_signature = "(local_hostname, metadata_url, global_segment_size, local_buffer_size, protocol='tcp', rdma_devices='', master_server='', *, stable_id=None, epoch=1, initial_state='active', tenant='default', labels=None, routed_writes=False, replica_count=1, route_topk=2, keyspace=None, transport_metadata_url=None, transport_rpc_port=None, transport_backend=None, local_segment_name=None, expires_at_ms=None, use_hugepage=None, hugepage_size=None, route_control='embedded_wrh')")]
    #[allow(clippy::too_many_arguments)]
    fn setup(
        &mut self,
        local_hostname: &str,
        metadata_url: &str,
        global_segment_size: usize,
        local_buffer_size: usize,
        protocol: &str,
        rdma_devices: &str,
        master_server: &str,
        stable_id: Option<String>,
        epoch: u64,
        initial_state: &str,
        tenant: &str,
        labels: Option<BTreeMap<String, String>>,
        routed_writes: bool,
        replica_count: usize,
        route_topk: usize,
        keyspace: Option<String>,
        transport_metadata_url: Option<String>,
        transport_rpc_port: Option<u16>,
        transport_backend: Option<String>,
        local_segment_name: Option<String>,
        expires_at_ms: Option<u64>,
        use_hugepage: Option<bool>,
        hugepage_size: Option<usize>,
        route_control: &str,
    ) -> PyResult<i32> {
        let epoch = parse_client_epoch_arg(epoch)?;
        let initial_state = parse_initial_state_arg(initial_state)?;
        let route_control = parse_route_control_arg(route_control)?;
        let runtime = CompatRuntimeArgs {
            setup: config::CompatSetupArgs {
                local_hostname: local_hostname.to_string(),
                metadata_url: metadata_url.to_string(),
                transport_metadata_url,
                global_segment_size,
                local_buffer_size,
                protocol: protocol.to_string(),
                _rdma_devices: rdma_devices.to_string(),
                transport_rpc_port,
                transport_backend,
                stable_id,
                tenant: tenant.to_string(),
                labels: labels.unwrap_or_default(),
                routed_writes,
                replica_count,
                route_topk,
                keyspace,
                expires_at_ms,
                use_hugepage,
                hugepage_size_bytes: hugepage_size,
                timeouts: None,
            },
            local_segment_name,
            epoch,
            initial_state,
            route_control,
        }
        .build()
        .map_err(store_error_to_py)?;
        let _ = master_server;
        let stable_id = runtime.stable_id.clone();
        let dispatcher = StoreDispatcher::spawn(
            runtime.client,
            format!("mooncake-py-dispatcher-{stable_id}"),
        )
        .map_err(store_error_to_py)?;
        dispatcher
            .register_local_memory()
            .map_err(store_error_to_py)?;
        dispatcher
            .start_heartbeat_loop(runtime.lease_ttl_ms, None)
            .map_err(store_error_to_py)?;
        self.replace_backend(StoreBackend::Real(dispatcher));
        Ok(0)
    }

    #[pyo3(signature = (mem_pool_size, local_buffer_size, server_address))]
    fn setup_dummy(
        &mut self,
        mem_pool_size: usize,
        local_buffer_size: usize,
        server_address: &str,
    ) -> PyResult<i32> {
        let _ = (mem_pool_size, local_buffer_size);
        let session = DummySession::connect(server_address).map_err(store_error_to_py)?;
        self.replace_backend(StoreBackend::Dummy(session));
        Ok(0)
    }

    fn close(&mut self) {
        if let Some(backend) = self.backend.take() {
            backend.close();
        }
    }

    #[pyo3(signature = (
        key,
        value,
        *,
        tenant = None,
        replica_count = None,
        preferred_segment = None,
        preferred_segments = None,
        preferred_storage_owner = None,
        preferred_storage_owners = None,
        prefer_local = true,
        prefer_alloc_in_same_node = false,
        with_soft_pin = false
    ))]
    #[allow(clippy::too_many_arguments)]
    fn put(
        &self,
        key: &str,
        value: Vec<u8>,
        tenant: Option<&str>,
        replica_count: Option<usize>,
        preferred_segment: Option<String>,
        preferred_segments: Option<Vec<String>>,
        preferred_storage_owner: Option<String>,
        preferred_storage_owners: Option<Vec<String>>,
        prefer_local: bool,
        prefer_alloc_in_same_node: bool,
        with_soft_pin: bool,
    ) -> PyResult<i32> {
        let policy = replication_policy(
            replica_count,
            preferred_segment,
            preferred_segments,
            preferred_storage_owner,
            preferred_storage_owners,
            prefer_local,
            prefer_alloc_in_same_node,
            with_soft_pin,
        );
        match self.backend_ref()? {
            StoreBackend::Dummy(dummy) => dummy
                .put(key, &value, tenant, policy.as_ref())
                .map_err(store_error_to_py),
            StoreBackend::Real(dispatcher) => {
                let key = key.to_string();
                let tenant = tenant.map(str::to_string);
                let cache_key = key.clone();
                let cache_tenant = tenant.clone();
                dispatcher
                    .run(move |client| match (tenant.as_deref(), policy.as_ref()) {
                        (Some(tenant), Some(policy)) => {
                            client.put_in_tenant_with_policy(tenant, &key, &value, policy)
                        }
                        (None, Some(policy)) => client.put_with_policy(&key, &value, policy),
                        (Some(tenant), None) => client.put_in_tenant(tenant, &key, &value),
                        (None, None) => client.put(&key, &value),
                    })
                    .map_err(store_error_to_py)?;
                dispatcher.invalidate_key(cache_key, cache_tenant);
                Ok(0)
            }
        }
    }

    #[pyo3(signature = (key, *, tenant = None))]
    fn get<'py>(
        &self,
        py: Python<'py>,
        key: &str,
        tenant: Option<&str>,
    ) -> PyResult<Bound<'py, PyBytes>> {
        let value = match self.backend_ref()? {
            StoreBackend::Dummy(dummy) => {
                let (status, value) = dummy.get(key, tenant).map_err(store_error_to_py)?;
                if status != 0 {
                    return Err(PyKeyError::new_err(format!(
                        "dummy store get failed for key={key}"
                    )));
                }
                value
            }
            StoreBackend::Real(dispatcher) => dispatcher
                .get_value(key.to_string(), tenant.map(str::to_string))
                .map_err(store_error_to_py)?,
        };
        Ok(PyBytes::new(py, &value))
    }

    #[pyo3(signature = (key, *, tenant = None))]
    fn is_exist(&self, key: &str, tenant: Option<&str>) -> PyResult<bool> {
        match self.backend_ref()? {
            StoreBackend::Dummy(dummy) => {
                let results = dummy
                    .batch_is_exist(&[key.to_string()], tenant)
                    .map_err(store_error_to_py)?;
                Ok(results.first().copied().unwrap_or_default() == 1)
            }
            StoreBackend::Real(dispatcher) => {
                let key = key.to_string();
                let tenant = tenant.map(str::to_string);
                dispatcher
                    .run(move |client| match tenant.as_deref() {
                        Some(tenant) => client.is_exist_in_tenant(tenant, &key),
                        None => client.is_exist(&key),
                    })
                    .map_err(store_error_to_py)
            }
        }
    }

    #[pyo3(signature = (keys, *, tenant = None))]
    fn batch_is_exist(&self, keys: Vec<String>, tenant: Option<&str>) -> PyResult<Vec<i32>> {
        match self.backend_ref()? {
            StoreBackend::Dummy(dummy) => dummy
                .batch_is_exist(&keys, tenant)
                .map_err(store_error_to_py),
            StoreBackend::Real(dispatcher) => {
                let item_count = keys.len();
                let tenant = tenant.map(str::to_string);
                dispatcher
                    .run(move |client| {
                        let objects = keys
                            .iter()
                            .map(|key| {
                                let mut object = ObjectRef::new(key.as_str());
                                if let Some(tenant) = tenant.as_deref() {
                                    object = object.tenant(tenant);
                                }
                                object
                            })
                            .collect::<Vec<_>>();
                        client
                            .batch_is_exist(&objects)
                            .map(|items| items.into_iter().map(i32::from).collect())
                    })
                    .or_else(|error| {
                        if should_soft_miss_error(&error) {
                            Ok(soft_miss_exists_result(item_count, &error))
                        } else {
                            Err(store_error_to_py(error))
                        }
                    })
            }
        }
    }

    fn get_hostname(&self) -> PyResult<String> {
        match self.backend_ref()? {
            StoreBackend::Dummy(dummy) => Ok(dummy.server_addr().to_string()),
            StoreBackend::Real(dispatcher) => dispatcher
                .run(|client| client.get_hostname())
                .map_err(store_error_to_py),
        }
    }

    #[pyo3(signature = (key, *, tenant = None))]
    fn get_size(&self, key: &str, tenant: Option<&str>) -> PyResult<usize> {
        match self.backend_ref()? {
            StoreBackend::Dummy(dummy) => {
                let (status, value) = dummy.get(key, tenant).map_err(store_error_to_py)?;
                if status != 0 {
                    return Ok(0);
                }
                Ok(value.len())
            }
            StoreBackend::Real(dispatcher) => {
                let key = key.to_string();
                let tenant = tenant.map(str::to_string);
                dispatcher
                    .run(move |client| match tenant.as_deref() {
                        Some(tenant) => client.get_size_in_tenant(tenant, &key),
                        None => client.get_size(&key),
                    })
                    .map_err(store_error_to_py)
            }
        }
    }

    fn register_buffer(&self, buffer_ptr: usize, size: usize) -> PyResult<i32> {
        match self.backend_ref()? {
            StoreBackend::Dummy(dummy) => dummy
                .register_buffer(buffer_ptr, size)
                .map_err(store_error_to_py),
            StoreBackend::Real(dispatcher) => {
                let _ = pointer_from_usize(buffer_ptr)?;
                dispatcher
                    .register_buffer(buffer_ptr, size)
                    .map_err(store_error_to_py)?;
                Ok(0)
            }
        }
    }

    fn unregister_buffer(&self, buffer_ptr: usize, size: usize) -> PyResult<i32> {
        match self.backend_ref()? {
            StoreBackend::Dummy(dummy) => dummy
                .unregister_buffer(buffer_ptr, Some(size))
                .map_err(store_error_to_py),
            StoreBackend::Real(dispatcher) => {
                let _ = pointer_from_usize(buffer_ptr)?;
                dispatcher
                    .unregister_buffer(buffer_ptr, size)
                    .map_err(store_error_to_py)?;
                Ok(0)
            }
        }
    }

    #[pyo3(signature = (
        key,
        buffer_ptr,
        size,
        *,
        tenant = None,
        replica_count = None,
        preferred_segment = None,
        preferred_segments = None,
        preferred_storage_owner = None,
        preferred_storage_owners = None,
        prefer_local = true,
        prefer_alloc_in_same_node = false,
        with_soft_pin = false
    ))]
    #[allow(clippy::too_many_arguments)]
    fn put_from(
        &self,
        key: &str,
        buffer_ptr: usize,
        size: usize,
        tenant: Option<&str>,
        replica_count: Option<usize>,
        preferred_segment: Option<String>,
        preferred_segments: Option<Vec<String>>,
        preferred_storage_owner: Option<String>,
        preferred_storage_owners: Option<Vec<String>>,
        prefer_local: bool,
        prefer_alloc_in_same_node: bool,
        with_soft_pin: bool,
    ) -> PyResult<i32> {
        let policy = replication_policy(
            replica_count,
            preferred_segment,
            preferred_segments,
            preferred_storage_owner,
            preferred_storage_owners,
            prefer_local,
            prefer_alloc_in_same_node,
            with_soft_pin,
        );
        match self.backend_ref()? {
            StoreBackend::Dummy(dummy) => dummy
                .put_from(key, buffer_ptr, size, tenant, policy.as_ref())
                .map_err(store_error_to_py),
            StoreBackend::Real(dispatcher) => {
                let _ = pointer_from_usize(buffer_ptr)?;
                let key = key.to_string();
                let tenant = tenant.map(str::to_string);
                let cache_key = key.clone();
                let cache_tenant = tenant.clone();
                dispatcher
                    .run(move |client| {
                        let buffer = buffer_ptr as *const c_void;
                        match (tenant.as_deref(), policy.as_ref()) {
                            (Some(tenant), Some(policy)) => client
                                .put_from_in_tenant_with_policy(tenant, &key, buffer, size, policy),
                            (None, Some(policy)) => {
                                client.put_from_with_policy(&key, buffer, size, policy)
                            }
                            (Some(tenant), None) => {
                                client.put_from_in_tenant(tenant, &key, buffer, size)
                            }
                            (None, None) => client.put_from(&key, buffer, size),
                        }
                    })
                    .map_err(store_error_to_py)?;
                dispatcher.invalidate_key(cache_key, cache_tenant);
                Ok(0)
            }
        }
    }

    #[pyo3(signature = (key, buffer_ptr, size, *, tenant = None))]
    fn get_into(
        &self,
        key: &str,
        buffer_ptr: usize,
        size: usize,
        tenant: Option<&str>,
    ) -> PyResult<usize> {
        match self.backend_ref()? {
            StoreBackend::Dummy(dummy) => {
                let size = dummy
                    .get_into(key, buffer_ptr, size, tenant)
                    .map_err(store_error_to_py)?;
                if size < 0 {
                    return Err(PyKeyError::new_err(format!(
                        "dummy store get_into failed for key={key}"
                    )));
                }
                Ok(size as usize)
            }
            StoreBackend::Real(dispatcher) => {
                let _ = pointer_from_usize(buffer_ptr)?;
                dispatcher
                    .get_into_buffer(
                        key.to_string(),
                        tenant.map(str::to_string),
                        buffer_ptr,
                        size,
                    )
                    .map_err(store_error_to_py)
            }
        }
    }

    #[pyo3(signature = (
        items,
        *,
        tenant = None,
        replica_count = None,
        preferred_segment = None,
        preferred_segments = None,
        preferred_storage_owner = None,
        preferred_storage_owners = None,
        prefer_local = true,
        prefer_alloc_in_same_node = false,
        with_soft_pin = false
    ))]
    #[allow(clippy::too_many_arguments)]
    fn batch_put(
        &self,
        items: Vec<(String, Vec<u8>)>,
        tenant: Option<&str>,
        replica_count: Option<usize>,
        preferred_segment: Option<String>,
        preferred_segments: Option<Vec<String>>,
        preferred_storage_owner: Option<String>,
        preferred_storage_owners: Option<Vec<String>>,
        prefer_local: bool,
        prefer_alloc_in_same_node: bool,
        with_soft_pin: bool,
    ) -> PyResult<i32> {
        let policy = replication_policy(
            replica_count,
            preferred_segment,
            preferred_segments,
            preferred_storage_owner,
            preferred_storage_owners,
            prefer_local,
            prefer_alloc_in_same_node,
            with_soft_pin,
        );
        match self.backend_ref()? {
            StoreBackend::Dummy(dummy) => {
                for (key, value) in &items {
                    let status = dummy
                        .put(key, value, tenant, policy.as_ref())
                        .map_err(store_error_to_py)?;
                    if status != 0 {
                        return Ok(status);
                    }
                }
                Ok(0)
            }
            StoreBackend::Real(dispatcher) => {
                let tenant = tenant.map(str::to_string);
                let cache_keys = items.iter().map(|(key, _)| key.clone()).collect::<Vec<_>>();
                let cache_tenant = tenant.clone();
                dispatcher
                    .run(move |client| {
                        let requests = items
                            .iter()
                            .map(|(key, value)| {
                                let mut request = PutRequest::new(key, value);
                                if let Some(tenant) = tenant.as_deref() {
                                    request = request.tenant(tenant);
                                }
                                if let Some(policy) = policy.clone() {
                                    request = request.replication(policy);
                                }
                                request
                            })
                            .collect::<Vec<_>>();
                        client.batch_put(&requests).map(|_| ())
                    })
                    .map_err(store_error_to_py)?;
                dispatcher.invalidate_keys(cache_keys, cache_tenant);
                Ok(0)
            }
        }
    }

    #[pyo3(signature = (
        items,
        *,
        tenant = None,
        replica_count = None,
        preferred_segment = None,
        preferred_segments = None,
        preferred_storage_owner = None,
        preferred_storage_owners = None,
        prefer_local = true,
        prefer_alloc_in_same_node = false,
        with_soft_pin = false
    ))]
    #[allow(clippy::too_many_arguments)]
    fn batch_put_from(
        &self,
        py: Python<'_>,
        items: Vec<(String, usize, usize)>,
        tenant: Option<&str>,
        replica_count: Option<usize>,
        preferred_segment: Option<String>,
        preferred_segments: Option<Vec<String>>,
        preferred_storage_owner: Option<String>,
        preferred_storage_owners: Option<Vec<String>>,
        prefer_local: bool,
        prefer_alloc_in_same_node: bool,
        with_soft_pin: bool,
    ) -> PyResult<Py<PyAny>> {
        let policy = replication_policy(
            replica_count,
            preferred_segment,
            preferred_segments,
            preferred_storage_owner,
            preferred_storage_owners,
            prefer_local,
            prefer_alloc_in_same_node,
            with_soft_pin,
        );
        match self.backend_ref()? {
            StoreBackend::Dummy(dummy) => {
                let statuses = dummy
                    .batch_put_from(&items, tenant, policy.as_ref())
                    .map_err(store_error_to_py)?;
                Ok(PyList::new(py, statuses)?.into_any().unbind())
            }
            StoreBackend::Real(dispatcher) => {
                for (_, buffer_ptr, _) in &items {
                    let _ = pointer_from_usize(*buffer_ptr)?;
                }
                let item_count = items.len();
                let tenant = tenant.map(str::to_string);
                let cache_keys = items
                    .iter()
                    .map(|(key, _, _)| key.clone())
                    .collect::<Vec<_>>();
                let cache_tenant = tenant.clone();
                dispatcher
                    .run(move |client| {
                        let requests = items
                            .iter()
                            .map(|(key, buffer_ptr, size)| {
                                let mut request = PutFromRequest::new(
                                    key,
                                    (*buffer_ptr as *mut c_void).cast_const(),
                                    *size,
                                );
                                if let Some(tenant) = tenant.as_deref() {
                                    request = request.tenant(tenant);
                                }
                                if let Some(policy) = policy.clone() {
                                    request = request.replication(policy);
                                }
                                request
                            })
                            .collect::<Vec<_>>();
                        client.batch_put_from(&requests).map(|_| requests.len())
                    })
                    .map_err(store_error_to_py)?;
                dispatcher.invalidate_keys(cache_keys, cache_tenant);
                Ok(PyList::new(py, vec![0; item_count])?.into_any().unbind())
            }
        }
    }

    #[pyo3(signature = (
        keys,
        buffer_ptrs,
        sizes,
        *,
        tenant = None,
        replica_count = None,
        preferred_segment = None,
        preferred_segments = None,
        preferred_storage_owner = None,
        preferred_storage_owners = None,
        prefer_local = true,
        prefer_alloc_in_same_node = false,
        with_soft_pin = false
    ))]
    #[allow(clippy::too_many_arguments)]
    fn batch_put_from_raw(
        &self,
        py: Python<'_>,
        keys: Vec<String>,
        buffer_ptrs: Vec<usize>,
        sizes: Vec<usize>,
        tenant: Option<&str>,
        replica_count: Option<usize>,
        preferred_segment: Option<String>,
        preferred_segments: Option<Vec<String>>,
        preferred_storage_owner: Option<String>,
        preferred_storage_owners: Option<Vec<String>>,
        prefer_local: bool,
        prefer_alloc_in_same_node: bool,
        with_soft_pin: bool,
    ) -> PyResult<Py<PyAny>> {
        if keys.len() != buffer_ptrs.len() || keys.len() != sizes.len() {
            return Err(PyValueError::new_err(
                "keys, buffer_ptrs, and sizes must have the same length",
            ));
        }
        let items = keys
            .into_iter()
            .zip(buffer_ptrs)
            .zip(sizes)
            .map(|((key, buffer_ptr), size)| (key, buffer_ptr, size))
            .collect::<Vec<_>>();
        self.batch_put_from(
            py,
            items,
            tenant,
            replica_count,
            preferred_segment,
            preferred_segments,
            preferred_storage_owner,
            preferred_storage_owners,
            prefer_local,
            prefer_alloc_in_same_node,
            with_soft_pin,
        )
    }

    #[pyo3(signature = (
        items,
        *,
        tenant = None,
        replica_count = None,
        preferred_segment = None,
        preferred_segments = None,
        preferred_storage_owner = None,
        preferred_storage_owners = None,
        prefer_local = true,
        prefer_alloc_in_same_node = false,
        with_soft_pin = false
    ))]
    #[allow(clippy::too_many_arguments)]
    fn batch_put_from_multi_buffers(
        &self,
        items: Vec<(String, Vec<Vec<u8>>)>,
        tenant: Option<&str>,
        replica_count: Option<usize>,
        preferred_segment: Option<String>,
        preferred_segments: Option<Vec<String>>,
        preferred_storage_owner: Option<String>,
        preferred_storage_owners: Option<Vec<String>>,
        prefer_local: bool,
        prefer_alloc_in_same_node: bool,
        with_soft_pin: bool,
    ) -> PyResult<i32> {
        let policy = replication_policy(
            replica_count,
            preferred_segment,
            preferred_segments,
            preferred_storage_owner,
            preferred_storage_owners,
            prefer_local,
            prefer_alloc_in_same_node,
            with_soft_pin,
        );
        match self.backend_ref()? {
            StoreBackend::Dummy(dummy) => {
                for (key, buffers) in &items {
                    let total = buffers.iter().map(Vec::len).sum();
                    let mut payload = Vec::with_capacity(total);
                    for buffer in buffers {
                        payload.extend_from_slice(buffer);
                    }
                    let status = dummy
                        .put(key, &payload, tenant, policy.as_ref())
                        .map_err(store_error_to_py)?;
                    if status != 0 {
                        return Ok(status);
                    }
                }
                Ok(0)
            }
            StoreBackend::Real(dispatcher) => {
                let tenant = tenant.map(str::to_string);
                let cache_keys = items.iter().map(|(key, _)| key.clone()).collect::<Vec<_>>();
                let cache_tenant = tenant.clone();
                dispatcher
                    .run(move |client| {
                        let borrowed = items
                            .iter()
                            .map(|(_, buffers)| {
                                buffers
                                    .iter()
                                    .map(|buffer| buffer.as_slice())
                                    .collect::<Vec<_>>()
                            })
                            .collect::<Vec<_>>();
                        let requests = items
                            .iter()
                            .zip(borrowed.iter())
                            .map(|((key, _), slices)| {
                                let mut request =
                                    MultiBufferPutRequest::new(key, slices.as_slice());
                                if let Some(tenant) = tenant.as_deref() {
                                    request = request.tenant(tenant);
                                }
                                if let Some(policy) = policy.clone() {
                                    request = request.replication(policy);
                                }
                                request
                            })
                            .collect::<Vec<_>>();
                        client.batch_put_from_multi_buffers(&requests).map(|_| ())
                    })
                    .map_err(store_error_to_py)?;
                dispatcher.invalidate_keys(cache_keys, cache_tenant);
                Ok(0)
            }
        }
    }

    #[pyo3(signature = (
        keys,
        all_buffer_ptrs,
        all_sizes,
        *,
        tenant = None,
        replica_count = None,
        preferred_segment = None,
        preferred_segments = None,
        preferred_storage_owner = None,
        preferred_storage_owners = None,
        prefer_local = true,
        prefer_alloc_in_same_node = false,
        with_soft_pin = false
    ))]
    #[allow(clippy::too_many_arguments)]
    fn batch_put_from_multi_buffers_raw(
        &self,
        keys: Vec<String>,
        all_buffer_ptrs: Vec<Vec<usize>>,
        all_sizes: Vec<Vec<usize>>,
        tenant: Option<&str>,
        replica_count: Option<usize>,
        preferred_segment: Option<String>,
        preferred_segments: Option<Vec<String>>,
        preferred_storage_owner: Option<String>,
        preferred_storage_owners: Option<Vec<String>>,
        prefer_local: bool,
        prefer_alloc_in_same_node: bool,
        with_soft_pin: bool,
    ) -> PyResult<Vec<i32>> {
        if keys.len() != all_buffer_ptrs.len() || keys.len() != all_sizes.len() {
            return Err(PyValueError::new_err(
                "keys, all_buffer_ptrs, and all_sizes must have the same length",
            ));
        }
        let policy = replication_policy(
            replica_count,
            preferred_segment,
            preferred_segments,
            preferred_storage_owner,
            preferred_storage_owners,
            prefer_local,
            prefer_alloc_in_same_node,
            with_soft_pin,
        );
        match self.backend_ref()? {
            StoreBackend::Dummy(dummy) => {
                let items = keys
                    .iter()
                    .zip(all_buffer_ptrs.iter())
                    .zip(all_sizes.iter())
                    .map(|((key, buffer_ptrs), sizes)| {
                        if buffer_ptrs.len() != sizes.len() {
                            return Err(PyValueError::new_err(
                                "each multi-buffer pointer group must match its sizes group",
                            ));
                        }
                        Ok((
                            key.clone(),
                            buffer_ptrs
                                .iter()
                                .copied()
                                .zip(sizes.iter().copied())
                                .collect::<Vec<_>>(),
                        ))
                    })
                    .collect::<PyResult<Vec<_>>>()?;
                dummy
                    .batch_put_from_multi_buffers(&items, tenant, policy.as_ref())
                    .map_err(store_error_to_py)
            }
            StoreBackend::Real(dispatcher) => {
                for (buffer_ptrs, sizes) in all_buffer_ptrs.iter().zip(all_sizes.iter()) {
                    if buffer_ptrs.len() != sizes.len() {
                        return Err(PyValueError::new_err(
                            "each multi-buffer pointer group must match its sizes group",
                        ));
                    }
                    for buffer_ptr in buffer_ptrs {
                        let _ = pointer_from_usize(*buffer_ptr)?;
                    }
                }
                let key_count = keys.len();
                let tenant = tenant.map(str::to_string);
                let cache_keys = keys.clone();
                let cache_tenant = tenant.clone();
                dispatcher
                    .run(move |client| {
                        let borrowed = all_buffer_ptrs
                            .iter()
                            .zip(all_sizes.iter())
                            .map(|(buffer_ptrs, sizes)| {
                                buffer_ptrs
                                    .iter()
                                    .zip(sizes.iter())
                                    .map(|(buffer_ptr, size)| unsafe {
                                        slice::from_raw_parts(*buffer_ptr as *const u8, *size)
                                    })
                                    .collect::<Vec<_>>()
                            })
                            .collect::<Vec<_>>();
                        let requests = keys
                            .iter()
                            .zip(borrowed.iter())
                            .map(|(key, buffers)| {
                                let mut request =
                                    MultiBufferPutRequest::new(key, buffers.as_slice());
                                if let Some(tenant) = tenant.as_deref() {
                                    request = request.tenant(tenant);
                                }
                                if let Some(policy) = policy.clone() {
                                    request = request.replication(policy);
                                }
                                request
                            })
                            .collect::<Vec<_>>();
                        client.batch_put_from_multi_buffers(&requests).map(|_| ())
                    })
                    .map_err(store_error_to_py)?;
                dispatcher.invalidate_keys(cache_keys, cache_tenant);
                Ok(vec![0; key_count])
            }
        }
    }

    #[pyo3(signature = (key, force = false, *, tenant = None))]
    fn remove(&self, key: &str, force: bool, tenant: Option<&str>) -> PyResult<i32> {
        let dispatcher = self.real_dispatcher()?;
        let key = key.to_string();
        let tenant = tenant.map(str::to_string);
        let cache_key = key.clone();
        let cache_tenant = tenant.clone();
        dispatcher
            .run(move |client| match tenant.as_deref() {
                Some(tenant) => client.remove_in_tenant(tenant, &key, force),
                None => client.remove(&key, force),
            })
            .map_err(store_error_to_py)?;
        dispatcher.invalidate_key(cache_key, cache_tenant);
        Ok(0)
    }

    #[pyo3(signature = (keys, force = false, *, tenant = None))]
    fn batch_remove(
        &self,
        keys: Vec<String>,
        force: bool,
        tenant: Option<&str>,
    ) -> PyResult<Vec<i32>> {
        let dispatcher = self.real_dispatcher()?;
        let key_count = keys.len();
        let tenant = tenant.map(str::to_string);
        let cache_keys = keys.clone();
        let cache_tenant = tenant.clone();
        dispatcher
            .run(move |client| {
                let objects = keys
                    .iter()
                    .map(|key| {
                        let mut object = ObjectRef::new(key.as_str());
                        if let Some(tenant) = tenant.as_deref() {
                            object = object.tenant(tenant);
                        }
                        object
                    })
                    .collect::<Vec<_>>();
                client.batch_remove(&objects, force).map(|_| ())
            })
            .map_err(store_error_to_py)?;
        dispatcher.invalidate_keys(cache_keys, cache_tenant);
        Ok(vec![0; key_count])
    }

    #[pyo3(signature = (keys, *, tenant = None))]
    fn batch_get<'py>(
        &self,
        py: Python<'py>,
        keys: Vec<String>,
        tenant: Option<&str>,
    ) -> PyResult<Vec<Py<PyBytes>>> {
        let dispatcher = self.real_dispatcher()?;
        let values = dispatcher
            .batch_get_values(keys, tenant.map(str::to_string))
            .map_err(store_error_to_py)?;
        Ok(values
            .into_iter()
            .map(|value| PyBytes::new(py, &value).unbind())
            .collect())
    }

    #[pyo3(signature = (keys, *, tenant = None))]
    fn batch_get_buffer<'py>(
        &self,
        py: Python<'py>,
        keys: Vec<String>,
        tenant: Option<&str>,
    ) -> PyResult<Vec<Py<PyBytes>>> {
        self.batch_get(py, keys, tenant)
    }

    #[pyo3(signature = (items, *, tenant = None))]
    fn batch_get_into(
        &self,
        items: Vec<(String, usize, usize)>,
        tenant: Option<&str>,
    ) -> PyResult<Vec<i64>> {
        match self.backend_ref()? {
            StoreBackend::Dummy(dummy) => dummy
                .batch_get_into(&items, tenant)
                .map_err(store_error_to_py),
            StoreBackend::Real(dispatcher) => {
                for (_, buffer_ptr, _) in &items {
                    let _ = pointer_from_usize(*buffer_ptr)?;
                }
                let item_count = items.len();
                dispatcher
                    .batch_get_into_buffers(items, tenant.map(str::to_string))
                    .or_else(|error| {
                        if should_soft_miss_error(&error) {
                            Ok(soft_miss_length_result(item_count, &error))
                        } else {
                            Err(store_error_to_py(error))
                        }
                    })
            }
        }
    }

    #[pyo3(signature = (keys, buffer_ptrs, sizes, *, tenant = None))]
    fn batch_get_into_raw(
        &self,
        keys: Vec<String>,
        buffer_ptrs: Vec<usize>,
        sizes: Vec<usize>,
        tenant: Option<&str>,
    ) -> PyResult<Vec<i64>> {
        if keys.len() != buffer_ptrs.len() || keys.len() != sizes.len() {
            return Err(PyValueError::new_err(
                "keys, buffer_ptrs, and sizes must have the same length",
            ));
        }
        let items = keys
            .into_iter()
            .zip(buffer_ptrs)
            .zip(sizes)
            .map(|((key, buffer_ptr), size)| (key, buffer_ptr, size))
            .collect::<Vec<_>>();
        self.batch_get_into(items, tenant)
    }

    #[pyo3(signature = (keys, all_buffer_ptrs, all_sizes, prefer_alloc_in_same_node = false, *, tenant = None))]
    fn batch_get_into_multi_buffers(
        &self,
        keys: Vec<String>,
        all_buffer_ptrs: Vec<Vec<usize>>,
        all_sizes: Vec<Vec<usize>>,
        prefer_alloc_in_same_node: bool,
        tenant: Option<&str>,
    ) -> PyResult<Vec<i64>> {
        let _ = prefer_alloc_in_same_node;
        if keys.len() != all_buffer_ptrs.len() || keys.len() != all_sizes.len() {
            return Err(PyValueError::new_err(
                "keys, all_buffer_ptrs, and all_sizes must have the same length",
            ));
        }
        match self.backend_ref()? {
            StoreBackend::Dummy(dummy) => {
                let items = keys
                    .iter()
                    .zip(all_buffer_ptrs.iter())
                    .zip(all_sizes.iter())
                    .map(|((key, buffer_ptrs), sizes)| {
                        if buffer_ptrs.len() != sizes.len() {
                            return Err(PyValueError::new_err(
                                "each multi-buffer pointer group must match its sizes group",
                            ));
                        }
                        Ok((
                            key.clone(),
                            buffer_ptrs
                                .iter()
                                .copied()
                                .zip(sizes.iter().copied())
                                .collect::<Vec<_>>(),
                        ))
                    })
                    .collect::<PyResult<Vec<_>>>()?;
                dummy
                    .batch_get_into_multi_buffers(&items, tenant)
                    .map_err(store_error_to_py)
            }
            StoreBackend::Real(dispatcher) => {
                for (buffer_ptrs, sizes) in all_buffer_ptrs.iter().zip(all_sizes.iter()) {
                    if buffer_ptrs.len() != sizes.len() {
                        return Err(PyValueError::new_err(
                            "each multi-buffer pointer group must match its sizes group",
                        ));
                    }
                    for buffer_ptr in buffer_ptrs {
                        let _ = pointer_from_usize(*buffer_ptr)?;
                    }
                }
                let item_count = keys.len();
                dispatcher
                    .batch_get_into_multi_buffers_raw(
                        keys,
                        all_buffer_ptrs,
                        all_sizes,
                        tenant.map(str::to_string),
                    )
                    .or_else(|error| {
                        if should_soft_miss_error(&error) {
                            Ok(soft_miss_length_result(item_count, &error))
                        } else {
                            Err(store_error_to_py(error))
                        }
                    })
            }
        }
    }

    #[pyo3(signature = (storage_bytes))]
    fn expand_local_memory<'py>(
        &self,
        py: Python<'py>,
        storage_bytes: usize,
    ) -> PyResult<Py<PyAny>> {
        let announcement = self
            .real_dispatcher()?
            .run(move |client| client.expand_local_memory(storage_bytes))
            .map_err(store_error_to_py)?;
        segment_to_py(py, &announcement)
    }

    fn drain_segment(&self, segment_name: &str) -> PyResult<i32> {
        let segment_name = SegmentName::new(segment_name);
        self.real_dispatcher()?
            .run(move |client| client.drain_segment(&segment_name))
            .map_err(store_error_to_py)?;
        Ok(0)
    }

    fn retire_segment(&self, segment_name: &str) -> PyResult<bool> {
        let segment_name = SegmentName::new(segment_name);
        self.real_dispatcher()?
            .run(move |client| client.retire_segment(&segment_name))
            .map_err(store_error_to_py)
    }

    fn evacuate_owned_replicas(&mut self) -> PyResult<usize> {
        self.real_dispatcher()?
            .evacuate_owned_replicas()
            .map_err(store_error_to_py)
    }

    fn list_segments<'py>(&self, py: Python<'py>) -> PyResult<Vec<Py<PyAny>>> {
        self.real_dispatcher()?
            .run(|client| client.list_segments())
            .map_err(store_error_to_py)?
            .iter()
            .map(|segment| segment_to_py(py, segment))
            .collect()
    }

    #[pyo3(signature = (key, *, tenant = None))]
    fn query_route<'py>(
        &self,
        py: Python<'py>,
        key: &str,
        tenant: Option<&str>,
    ) -> PyResult<Option<Py<PyAny>>> {
        let key = key.to_string();
        let tenant = tenant.map(str::to_string);
        let route = self
            .real_dispatcher()?
            .run(move |client| match tenant.as_deref() {
                Some(tenant) => client.query_route_in_tenant(tenant, &key),
                None => client.query_route(&key),
            })
            .map_err(store_error_to_py)?;
        route.map(|route| route_to_py(py, &route)).transpose()
    }

    fn heartbeat(&mut self, expires_at_ms: u64) -> PyResult<i32> {
        self.real_dispatcher()?
            .heartbeat(expires_at_ms)
            .map_err(store_error_to_py)?;
        Ok(0)
    }

    fn activate(&mut self) -> PyResult<i32> {
        self.real_dispatcher()?
            .activate()
            .map_err(store_error_to_py)?;
        Ok(0)
    }

    fn enter_standby(&mut self) -> PyResult<i32> {
        self.real_dispatcher()?
            .enter_standby()
            .map_err(store_error_to_py)?;
        Ok(0)
    }

    fn enter_draining(&mut self) -> PyResult<i32> {
        self.real_dispatcher()?
            .enter_draining()
            .map_err(store_error_to_py)?;
        Ok(0)
    }

    #[pyo3(signature = (force = false))]
    fn remove_all(&self, force: bool) -> PyResult<i64> {
        match self.backend_ref()? {
            StoreBackend::Dummy(dummy) => {
                let (status, removed) = dummy.remove_all(force).map_err(store_error_to_py)?;
                if status != 0 {
                    return Err(PyRuntimeError::new_err("dummy remove_all failed"));
                }
                Ok(removed)
            }
            StoreBackend::Real(_) => Err(PyRuntimeError::new_err(
                "remove_all is not available for real-mode native store",
            )),
        }
    }

    fn health_check(&self) -> PyResult<i32> {
        match self.backend.as_ref() {
            Some(StoreBackend::Dummy(dummy)) => Ok(dummy.health_check()),
            Some(StoreBackend::Real(_)) => Ok(0),
            None => Ok(1),
        }
    }

    fn metrics_text(&self) -> String {
        render_prometheus_metrics()
    }

    #[pyo3(signature = (bind_addr = "127.0.0.1:0"))]
    fn start_metrics_server(&self, bind_addr: &str) -> PyResult<String> {
        start_metrics_http_server(bind_addr).map_err(store_error_to_py)
    }

    fn stop_metrics_server(&self) -> PyResult<()> {
        stop_metrics_http_server().map_err(store_error_to_py)
    }

    fn metrics_server_address(&self) -> Option<String> {
        metrics_http_server_addr()
    }
}

#[pyfunction]
#[pyo3(signature = (filter = None))]
fn init_tracing(filter: Option<&str>) -> PyResult<()> {
    init_store_tracing(filter).map_err(store_error_to_py)
}

#[pyfunction]
fn metrics_text() -> String {
    render_prometheus_metrics()
}

#[pyfunction]
#[pyo3(signature = (bind_addr = "127.0.0.1:0"))]
fn start_metrics_server(bind_addr: &str) -> PyResult<String> {
    start_metrics_http_server(bind_addr).map_err(store_error_to_py)
}

#[pyfunction]
fn stop_metrics_server() -> PyResult<()> {
    stop_metrics_http_server().map_err(store_error_to_py)
}

#[pyfunction]
fn metrics_server_address() -> Option<String> {
    metrics_http_server_addr()
}

#[pymodule]
fn _store_rs(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PyMooncakeDistributedStore>()?;
    module.add_class::<PyMooncakeHostMemAllocator>()?;
    module.add_function(wrap_pyfunction!(init_tracing, module)?)?;
    module.add_function(wrap_pyfunction!(metrics_text, module)?)?;
    module.add_function(wrap_pyfunction!(start_metrics_server, module)?)?;
    module.add_function(wrap_pyfunction!(stop_metrics_server, module)?)?;
    module.add_function(wrap_pyfunction!(metrics_server_address, module)?)?;
    Ok(())
}

impl PyMooncakeDistributedStore {
    fn replace_backend(&mut self, backend: StoreBackend) {
        if let Some(previous) = self.backend.replace(backend) {
            previous.close();
        }
    }

    fn backend_ref(&self) -> PyResult<&StoreBackend> {
        self.backend
            .as_ref()
            .ok_or_else(|| PyRuntimeError::new_err("store is not set up"))
    }

    fn real_dispatcher(&self) -> PyResult<&StoreDispatcher> {
        match self.backend_ref()? {
            StoreBackend::Real(dispatcher) => Ok(dispatcher),
            StoreBackend::Dummy(_) => Err(PyRuntimeError::new_err(
                "operation is not available in dummy mode",
            )),
        }
    }
}

#[pymethods]
impl PyMooncakeHostMemAllocator {
    #[new]
    #[pyo3(signature = (use_hugepage = None, hugepage_size = None))]
    fn new(use_hugepage: Option<bool>, hugepage_size: Option<usize>) -> Self {
        Self {
            use_hugepage,
            hugepage_size,
        }
    }

    fn alloc(&self, size: usize) -> PyResult<usize> {
        let result = if self.use_hugepage.is_none() && self.hugepage_size.is_none() {
            shm::allocate_shared_region(size)
        } else {
            shm::allocate_shared_region_with_options(size, self.use_hugepage, self.hugepage_size)
        };
        result.map_err(store_error_to_py)
    }

    fn free(&self, ptr: usize) -> PyResult<i32> {
        shm::free_shared_region(ptr).map_err(store_error_to_py)?;
        Ok(0)
    }
}

fn segment_to_py(py: Python<'_>, segment: &SegmentAnnouncement) -> PyResult<Py<PyAny>> {
    let dict = PyDict::new(py);
    dict.set_item("owner", segment.owner.storage_key())?;
    dict.set_item("segment_name", segment.segment_name.0.clone())?;
    dict.set_item("capacity_bytes", segment.capacity_bytes)?;
    dict.set_item("used_bytes", segment.used_bytes)?;
    dict.set_item("state", format!("{:?}", segment.state))?;
    dict.set_item("alignment_bytes", segment.alignment_bytes)?;
    dict.set_item("tags", segment.tags.clone())?;
    Ok(dict.into_any().unbind())
}

fn route_to_py(py: Python<'_>, route: &ObjectRoute) -> PyResult<Py<PyAny>> {
    let dict = PyDict::new(py);
    dict.set_item("key", route.key.0.clone())?;
    dict.set_item("version", route.version.0)?;
    dict.set_item("state", format!("{:?}", route.state))?;
    let replicas = PyList::empty(py);
    for replica in &route.replicas {
        let entry = PyDict::new(py);
        entry.set_item("owner", replica.owner.storage_key())?;
        entry.set_item("segment_name", replica.segment_name.0.clone())?;
        entry.set_item("offset", replica.offset)?;
        entry.set_item("segment_offset", replica.segment_offset)?;
        entry.set_item("length", replica.length)?;
        entry.set_item("priority", replica.priority)?;
        entry.set_item("tier", format!("{:?}", replica.tier))?;
        replicas.append(entry)?;
    }
    dict.set_item("replicas", replicas)?;
    Ok(dict.into_any().unbind())
}

#[allow(clippy::too_many_arguments)]
fn replication_policy(
    replica_count: Option<usize>,
    preferred_segment: Option<String>,
    preferred_segments: Option<Vec<String>>,
    preferred_storage_owner: Option<String>,
    preferred_storage_owners: Option<Vec<String>>,
    prefer_local: bool,
    prefer_alloc_in_same_node: bool,
    with_soft_pin: bool,
) -> Option<ReplicationPolicy> {
    let mut policy = ReplicationPolicy::new()
        .prefer_local(prefer_local)
        .prefer_alloc_in_same_node(prefer_alloc_in_same_node)
        .with_soft_pin(with_soft_pin);
    let mut has_policy = prefer_alloc_in_same_node || with_soft_pin || !prefer_local;
    if let Some(replica_count) = replica_count {
        policy = policy.replica_count(replica_count);
        has_policy = true;
    }
    let mut combined_segments = Vec::new();
    if let Some(segment) = preferred_segment.filter(|segment| !segment.is_empty()) {
        combined_segments.push(segment);
    }
    if let Some(mut segments) = preferred_segments.filter(|segments| !segments.is_empty()) {
        combined_segments.append(&mut segments);
    }
    if !combined_segments.is_empty() {
        policy = policy.preferred_segments(combined_segments);
        has_policy = true;
    }
    let mut combined_owners = Vec::new();
    if let Some(owner) = preferred_storage_owner.filter(|owner| !owner.is_empty()) {
        combined_owners.push(owner);
    }
    if let Some(mut owners) = preferred_storage_owners.filter(|owners| !owners.is_empty()) {
        combined_owners.append(&mut owners);
    }
    if !combined_owners.is_empty() {
        policy = policy.preferred_storage_owners(combined_owners);
        has_policy = true;
    }
    has_policy.then_some(policy)
}

fn pointer_from_usize(pointer: usize) -> PyResult<*mut c_void> {
    if pointer == 0 {
        return Err(PyValueError::new_err("buffer pointer must not be null"));
    }
    Ok(pointer as *mut c_void)
}

fn store_error_to_py(error: StoreError) -> PyErr {
    match error {
        StoreError::NotFound(message) => PyKeyError::new_err(message),
        StoreError::Conflict(message)
        | StoreError::InvalidState(message)
        | StoreError::StaleEpoch(message)
        | StoreError::Unsupported(message)
        | StoreError::Allocator(message)
        | StoreError::Metadata(message)
        | StoreError::Transport(message) => PyRuntimeError::new_err(message),
    }
}

fn should_soft_miss_error(error: &StoreError) -> bool {
    matches!(
        error,
        StoreError::NotFound(_)
            | StoreError::InvalidState(_)
            | StoreError::Metadata(_)
            | StoreError::Transport(_)
    )
}

fn soft_miss_status(error: &StoreError) -> i64 {
    match error {
        StoreError::NotFound(_) => -1,
        StoreError::InvalidState(_) => -2,
        StoreError::Metadata(_) => -3,
        StoreError::Transport(_) => -4,
        StoreError::Conflict(_) => -5,
        StoreError::StaleEpoch(_) => -6,
        StoreError::Unsupported(_) => -7,
        StoreError::Allocator(_) => -8,
    }
}

fn soft_miss_length_result(items: usize, error: &StoreError) -> Vec<i64> {
    vec![soft_miss_status(error); items]
}

fn soft_miss_exists_result(items: usize, error: &StoreError) -> Vec<i32> {
    vec![soft_miss_status(error) as i32; items]
}

fn parse_route_control_arg(value: &str) -> PyResult<RouteControlMode> {
    match value.trim().to_ascii_lowercase().as_str() {
        "embedded_wrh" | "embedded-wrh" | "wrh" => Ok(RouteControlMode::EmbeddedWrh),
        "metadata_only" | "metadata-only" | "metadata" => Ok(RouteControlMode::MetadataOnly),
        _ => Err(PyValueError::new_err(format!(
            "unsupported route_control {value:?}; expected embedded_wrh or metadata_only"
        ))),
    }
}

fn parse_client_epoch_arg(value: u64) -> PyResult<ClientEpoch> {
    if value == 0 {
        return Err(PyValueError::new_err("epoch must be greater than zero"));
    }
    Ok(ClientEpoch(value))
}

fn parse_initial_state_arg(value: &str) -> PyResult<ClientLifecycleState> {
    match value.trim().to_ascii_lowercase().as_str() {
        "standby" => Ok(ClientLifecycleState::Standby),
        "active" => Ok(ClientLifecycleState::Active),
        "draining" => Ok(ClientLifecycleState::Draining),
        "sealed" => Ok(ClientLifecycleState::Sealed),
        "offline" => Ok(ClientLifecycleState::Offline),
        _ => Err(PyValueError::new_err(format!(
            "unsupported initial_state {value:?}; expected standby, active, draining, sealed, or offline"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};
    use std::ffi::c_void;
    use std::net::TcpListener;
    use std::ptr;
    use std::slice;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::thread::sleep;
    use std::time::Duration;

    use mooncake_metadata::InMemoryMetadataBackend;
    use mooncake_store_client::{
        snapshot_metrics, LocalMemoryConfig, MooncakeCompatibilityFacade, PlacementPlanner,
        RouteControlMode, StoreClient, StoreClientBuilder, StoreTransport,
    };
    use mooncake_store_core::{
        CasResult, ClientEpoch, ClientLease, ClientLifecycleState, ClientRuntimeId, ClientStableId,
        CompatibilityDescriptor, HandoffKind, HandoffPlan, MetadataBackend, ObjectKey, ObjectRoute,
        ReplicaRoute, ReplicaTier, RoutePolicy, RoutePolicyDomain, RouteState, RouteVersion,
        SegmentAnnouncement, SegmentLifecycleState, SegmentName, SegmentReservation, StoreError,
        TenantObjectAccounting, TenantPolicy, TenantPolicyScope, TenantQuotaAbortOutcome,
        TenantQuotaFinalizeOutcome, TenantQuotaFinalizeRequest, TenantQuotaReservation,
        TenantQuotaReservationOutcome, TenantQuotaReservationRequest, TenantQuotaState,
    };
    use mooncake_transport::{
        Opcode, SegmentBuffer, SegmentInfo, SegmentKind, TransferProgress, TransferRequest,
        TransferStatus,
    };
    use parking_lot::Mutex;
    use pyo3::exceptions::{PyKeyError, PyRuntimeError, PyValueError};
    use pyo3::types::{PyAnyMethods, PyBytesMethods, PyModule};
    use pyo3::{prepare_freethreaded_python, Python};

    use super::{
        _store_rs, init_tracing, metrics_server_address, metrics_text, parse_client_epoch_arg,
        parse_initial_state_arg, pointer_from_usize, replication_policy, route_to_py,
        segment_to_py, start_metrics_server, stop_metrics_server, store_error_to_py, DummySession,
        PyMooncakeDistributedStore, PyMooncakeHostMemAllocator, StoreBackend,
    };
    use crate::dispatcher::StoreDispatcher;
    use crate::dummy_service::pb;
    use crate::dummy_service::{start_dummy_store_server, DummyStoreServerHandle};
    use crate::test_support::env_test_lock;

    struct TestTransport {
        local_segment: String,
        state: Arc<Mutex<TestTransportState>>,
    }

    struct TestTransportState {
        next_handle: u64,
        next_batch: u64,
        allocations: BTreeMap<usize, Box<[u8]>>,
        segments_by_name: BTreeMap<String, u64>,
        segments_by_handle: BTreeMap<u64, TestSegment>,
        live_batches: BTreeSet<u64>,
        registered_memory: BTreeMap<usize, usize>,
    }

    #[derive(Clone, Copy)]
    struct TestSegment {
        base: usize,
        len: usize,
    }

    impl TestTransport {
        fn new(local_segment: &str) -> Self {
            Self {
                local_segment: local_segment.to_string(),
                state: Arc::new(Mutex::new(TestTransportState {
                    next_handle: 1,
                    next_batch: 1,
                    allocations: BTreeMap::new(),
                    segments_by_name: BTreeMap::new(),
                    segments_by_handle: BTreeMap::new(),
                    live_batches: BTreeSet::new(),
                    registered_memory: BTreeMap::new(),
                })),
            }
        }
    }

    impl StoreTransport for TestTransport {
        fn segment_name(&self) -> mooncake_store_core::Result<String> {
            Ok(self.local_segment.clone())
        }

        fn rpc_server_address(&self) -> mooncake_store_core::Result<(String, u16)> {
            Ok(("127.0.0.1".to_string(), 0))
        }

        fn open_segment(&self, segment_name: &str) -> mooncake_store_core::Result<u64> {
            self.state
                .lock()
                .segments_by_name
                .get(segment_name)
                .copied()
                .ok_or_else(|| StoreError::NotFound(format!("segment {segment_name} not found")))
        }

        fn close_segment(&self, handle: u64) -> mooncake_store_core::Result<()> {
            if self.state.lock().segments_by_handle.contains_key(&handle) {
                return Ok(());
            }
            Err(StoreError::NotFound(format!(
                "segment handle {handle} not found"
            )))
        }

        fn get_segment_info(&self, handle: u64) -> mooncake_store_core::Result<SegmentInfo> {
            let state = self.state.lock();
            let segment = state
                .segments_by_handle
                .get(&handle)
                .copied()
                .ok_or_else(|| {
                    StoreError::NotFound(format!("segment handle {handle} not found"))
                })?;
            Ok(SegmentInfo {
                kind: SegmentKind::Memory,
                buffers: vec![SegmentBuffer {
                    base: segment.base as u64,
                    length: segment.len as u64,
                    location: "cpu:0".to_string(),
                }],
            })
        }

        fn adopt_local_memory(
            &self,
            addr: *mut c_void,
            size: usize,
            _location: &str,
        ) -> mooncake_store_core::Result<()> {
            let mut state = self.state.lock();
            if state.segments_by_name.contains_key(&self.local_segment) {
                return Ok(());
            }
            register_segment(&mut state, self.local_segment.clone(), addr as usize, size);
            Ok(())
        }

        fn allocate_memory(
            &self,
            size: usize,
            _location: &str,
        ) -> mooncake_store_core::Result<*mut c_void> {
            let mut state = self.state.lock();
            let base = allocate_boxed_region(&mut state, size);
            Ok(base as *mut c_void)
        }

        fn free_memory(&self, addr: *mut c_void) -> mooncake_store_core::Result<()> {
            let base = addr as usize;
            let mut state = self.state.lock();
            state
                .allocations
                .remove(&base)
                .ok_or_else(|| StoreError::NotFound(format!("allocation {base:#x} not found")))?;
            let orphaned = state
                .segments_by_handle
                .iter()
                .filter_map(|(handle, segment)| (segment.base == base).then_some(*handle))
                .collect::<Vec<_>>();
            for handle in orphaned {
                state.segments_by_handle.remove(&handle);
                state
                    .segments_by_name
                    .retain(|_, current_handle| *current_handle != handle);
            }
            state.registered_memory.remove(&base);
            Ok(())
        }

        fn register_memory(
            &self,
            addr: *mut c_void,
            size: usize,
        ) -> mooncake_store_core::Result<()> {
            self.state
                .lock()
                .registered_memory
                .insert(addr as usize, size);
            Ok(())
        }

        fn unregister_memory(
            &self,
            addr: *mut c_void,
            size: usize,
        ) -> mooncake_store_core::Result<()> {
            let mut state = self.state.lock();
            match state.registered_memory.remove(&(addr as usize)) {
                Some(recorded) if recorded == size => Ok(()),
                Some(recorded) => Err(StoreError::Allocator(format!(
                    "registered size mismatch: expected={recorded} actual={size}"
                ))),
                None => Err(StoreError::NotFound(format!(
                    "registered allocation {:p} not found",
                    addr
                ))),
            }
        }

        fn allocate_batch(&self, batch_size: usize) -> mooncake_store_core::Result<u64> {
            if batch_size == 0 {
                return Err(StoreError::Transport(
                    "batch_size must be greater than zero".to_string(),
                ));
            }
            let mut state = self.state.lock();
            let batch_id = state.next_batch;
            state.next_batch += 1;
            state.live_batches.insert(batch_id);
            Ok(batch_id)
        }

        fn free_batch(&self, batch_id: u64) -> mooncake_store_core::Result<()> {
            if self.state.lock().live_batches.remove(&batch_id) {
                return Ok(());
            }
            Err(StoreError::NotFound(format!("batch {batch_id} not found")))
        }

        fn submit(
            &self,
            batch_id: u64,
            requests: &[TransferRequest],
        ) -> mooncake_store_core::Result<()> {
            let state = self.state.lock();
            if !state.live_batches.contains(&batch_id) {
                return Err(StoreError::NotFound(format!("batch {batch_id} not found")));
            }
            for request in requests {
                let segment = state
                    .segments_by_handle
                    .get(&request.target_id)
                    .copied()
                    .ok_or_else(|| {
                        StoreError::NotFound(format!(
                            "segment handle {} not found",
                            request.target_id
                        ))
                    })?;
                validate_request_bounds(segment, request)?;
                unsafe {
                    match request.opcode {
                        Opcode::Write => ptr::copy_nonoverlapping(
                            request.source.cast::<u8>(),
                            request.target_offset as *mut u8,
                            request.length as usize,
                        ),
                        Opcode::Read => ptr::copy_nonoverlapping(
                            request.target_offset as *const u8,
                            request.source.cast::<u8>(),
                            request.length as usize,
                        ),
                    }
                }
            }
            Ok(())
        }

        fn task_status(
            &self,
            batch_id: u64,
            _task_id: usize,
        ) -> mooncake_store_core::Result<TransferProgress> {
            self.overall_status(batch_id)
        }

        fn overall_status(&self, batch_id: u64) -> mooncake_store_core::Result<TransferProgress> {
            if self.state.lock().live_batches.contains(&batch_id) {
                return Ok(TransferProgress {
                    status: TransferStatus::Completed,
                    transferred_bytes: 0,
                });
            }
            Err(StoreError::NotFound(format!("batch {batch_id} not found")))
        }
    }

    fn allocate_boxed_region(state: &mut TestTransportState, size: usize) -> usize {
        let mut memory = vec![0u8; size].into_boxed_slice();
        let base = memory.as_mut_ptr() as usize;
        state.allocations.insert(base, memory);
        base
    }

    fn register_segment(
        state: &mut TestTransportState,
        segment_name: String,
        base: usize,
        size: usize,
    ) -> u64 {
        let handle = state.next_handle;
        state.next_handle += 1;
        state.segments_by_name.insert(segment_name, handle);
        state
            .segments_by_handle
            .insert(handle, TestSegment { base, len: size });
        handle
    }

    fn validate_request_bounds(
        segment: TestSegment,
        request: &TransferRequest,
    ) -> mooncake_store_core::Result<()> {
        let start = usize::try_from(request.target_offset)
            .map_err(|_| StoreError::Transport("target offset does not fit usize".to_string()))?;
        let end = start
            .checked_add(request.length as usize)
            .ok_or_else(|| StoreError::Transport("request length overflow".to_string()))?;
        let segment_end = segment
            .base
            .checked_add(segment.len)
            .ok_or_else(|| StoreError::Transport("segment length overflow".to_string()))?;
        if start < segment.base || end > segment_end {
            return Err(StoreError::Transport(format!(
                "request out of segment bounds: start={start} end={end} segment={}..{}",
                segment.base, segment_end
            )));
        }
        Ok(())
    }

    fn build_client(name: &str) -> StoreClient {
        build_client_with_metadata(name, Arc::new(InMemoryMetadataBackend::new()))
    }

    fn build_client_with_metadata(name: &str, metadata: Arc<dyn MetadataBackend>) -> StoreClient {
        build_client_with_metadata_and_state(name, metadata, ClientLifecycleState::Active)
    }

    fn build_client_with_metadata_and_state(
        name: &str,
        metadata: Arc<dyn MetadataBackend>,
        state: ClientLifecycleState,
    ) -> StoreClient {
        let transport = Arc::new(TestTransport::new(&format!("{name}-segment")));
        build_client_with_metadata_and_transport(name, metadata, transport, state)
    }

    fn build_client_with_metadata_and_transport(
        name: &str,
        metadata: Arc<dyn MetadataBackend>,
        transport: Arc<TestTransport>,
        state: ClientLifecycleState,
    ) -> StoreClient {
        let planner = PlacementPlanner::new(metadata.clone()).require_label("storage", "true");
        StoreClientBuilder::new(metadata, name)
            .epoch(ClientEpoch(1))
            .state(state)
            .label("storage", "true")
            .live_client_sync_interval(Duration::from_millis(25))
            .compatibility(CompatibilityDescriptor::default())
            .route_control(RouteControlMode::MetadataOnly)
            .transport(transport)
            .local_memory(
                LocalMemoryConfig::new()
                    .numa_aware(false)
                    .storage_bytes(4 * 1024)
                    .scratch_bytes(4 * 1024)
                    .alignment(1)
                    .reclaim_grace_ms(0),
            )
            .routed_writes(planner, 1)
            .build(60_000)
            .expect("test store client should build")
    }

    struct BlockingHealthMetadata {
        inner: InMemoryMetadataBackend,
        block_lease: Arc<AtomicBool>,
        block_state_update: Arc<AtomicBool>,
        release: Arc<AtomicBool>,
        entered: std::sync::Mutex<Option<std::sync::mpsc::Sender<()>>>,
    }

    struct RecoveryCountingMetadata {
        inner: InMemoryMetadataBackend,
        lease_failures_remaining: Mutex<usize>,
        publish_segment_calls: AtomicUsize,
    }

    impl BlockingHealthMetadata {
        fn new(
            block_lease: Arc<AtomicBool>,
            block_state_update: Arc<AtomicBool>,
            release: Arc<AtomicBool>,
            entered: std::sync::mpsc::Sender<()>,
        ) -> Self {
            Self {
                inner: InMemoryMetadataBackend::new(),
                block_lease,
                block_state_update,
                release,
                entered: std::sync::Mutex::new(Some(entered)),
            }
        }

        fn maybe_block(&self, enabled: &AtomicBool) {
            if enabled.load(Ordering::SeqCst) {
                if let Some(sender) = self.entered.lock().expect("mutex poisoned").take() {
                    let _ = sender.send(());
                }
                while !self.release.load(Ordering::SeqCst) {
                    sleep(Duration::from_millis(10));
                }
            }
        }
    }

    impl RecoveryCountingMetadata {
        fn new() -> Self {
            Self {
                inner: InMemoryMetadataBackend::new(),
                lease_failures_remaining: Mutex::new(0),
                publish_segment_calls: AtomicUsize::new(0),
            }
        }

        fn fail_next_lease_upserts(&self, count: usize) {
            *self.lease_failures_remaining.lock() = count;
        }

        fn publish_segment_calls(&self) -> usize {
            self.publish_segment_calls.load(Ordering::SeqCst)
        }
    }

    impl MetadataBackend for BlockingHealthMetadata {
        fn route_namespace(&self) -> String {
            self.inner.route_namespace()
        }

        fn upsert_client_lease(&self, lease: &ClientLease) -> mooncake_store_core::Result<()> {
            self.maybe_block(&self.block_lease);
            self.inner.upsert_client_lease(lease)
        }

        fn update_client_state(
            &self,
            runtime: &ClientRuntimeId,
            next: ClientLifecycleState,
        ) -> mooncake_store_core::Result<()> {
            self.maybe_block(&self.block_state_update);
            self.inner.update_client_state(runtime, next)
        }

        fn list_live_clients(&self) -> mooncake_store_core::Result<Vec<ClientLease>> {
            self.inner.list_live_clients()
        }

        fn publish_segment(
            &self,
            segment: &SegmentAnnouncement,
        ) -> mooncake_store_core::Result<()> {
            self.inner.publish_segment(segment)
        }

        fn unpublish_segment(
            &self,
            owner: &ClientRuntimeId,
            segment: &SegmentName,
        ) -> mooncake_store_core::Result<()> {
            self.inner.unpublish_segment(owner, segment)
        }

        fn list_segments(
            &self,
            owner: Option<&ClientRuntimeId>,
        ) -> mooncake_store_core::Result<Vec<SegmentAnnouncement>> {
            self.inner.list_segments(owner)
        }

        fn update_segment_state(
            &self,
            owner: &ClientRuntimeId,
            segment: &SegmentName,
            next: SegmentLifecycleState,
        ) -> mooncake_store_core::Result<()> {
            self.inner.update_segment_state(owner, segment, next)
        }

        fn reserve_segment(
            &self,
            owner: &ClientRuntimeId,
            segment: &SegmentName,
            length_bytes: u64,
        ) -> mooncake_store_core::Result<SegmentReservation> {
            self.inner.reserve_segment(owner, segment, length_bytes)
        }

        fn release_segment(
            &self,
            owner: &ClientRuntimeId,
            segment: &SegmentName,
            offset_bytes: u64,
            length_bytes: u64,
        ) -> mooncake_store_core::Result<()> {
            self.inner
                .release_segment(owner, segment, offset_bytes, length_bytes)
        }

        fn get_object_route(
            &self,
            key: &ObjectKey,
        ) -> mooncake_store_core::Result<Option<ObjectRoute>> {
            self.inner.get_object_route(key)
        }

        fn list_object_routes(&self) -> mooncake_store_core::Result<Vec<ObjectRoute>> {
            self.inner.list_object_routes()
        }

        fn compare_and_swap_object_route(
            &self,
            key: &ObjectKey,
            expected: Option<RouteVersion>,
            next: Option<&ObjectRoute>,
        ) -> mooncake_store_core::Result<CasResult> {
            self.inner
                .compare_and_swap_object_route(key, expected, next)
        }

        fn get_route_policy(
            &self,
            domain: &RoutePolicyDomain,
        ) -> mooncake_store_core::Result<Option<RoutePolicy>> {
            self.inner.get_route_policy(domain)
        }

        fn put_route_policy_if_absent(
            &self,
            domain: &RoutePolicyDomain,
            policy: &RoutePolicy,
        ) -> mooncake_store_core::Result<bool> {
            self.inner.put_route_policy_if_absent(domain, policy)
        }

        fn put_route_policy(
            &self,
            domain: &RoutePolicyDomain,
            policy: &RoutePolicy,
        ) -> mooncake_store_core::Result<()> {
            self.inner.put_route_policy(domain, policy)
        }

        fn delete_route_policy(
            &self,
            domain: &RoutePolicyDomain,
        ) -> mooncake_store_core::Result<bool> {
            self.inner.delete_route_policy(domain)
        }

        fn list_route_policies(
            &self,
        ) -> mooncake_store_core::Result<Vec<(RoutePolicyDomain, RoutePolicy)>> {
            self.inner.list_route_policies()
        }

        fn get_tenant_policy(
            &self,
            scope: &TenantPolicyScope,
        ) -> mooncake_store_core::Result<Option<TenantPolicy>> {
            self.inner.get_tenant_policy(scope)
        }

        fn list_tenant_policies(&self) -> mooncake_store_core::Result<Vec<TenantPolicy>> {
            self.inner.list_tenant_policies()
        }

        fn put_tenant_policy(
            &self,
            policy: &TenantPolicy,
            expected_version: Option<u64>,
        ) -> mooncake_store_core::Result<TenantPolicy> {
            self.inner.put_tenant_policy(policy, expected_version)
        }

        fn delete_tenant_policy(
            &self,
            scope: &TenantPolicyScope,
            expected_version: Option<u64>,
        ) -> mooncake_store_core::Result<bool> {
            self.inner.delete_tenant_policy(scope, expected_version)
        }

        fn get_tenant_quota_state(
            &self,
            scope: &TenantPolicyScope,
        ) -> mooncake_store_core::Result<Option<TenantQuotaState>> {
            self.inner.get_tenant_quota_state(scope)
        }

        fn get_tenant_object_accounting(
            &self,
            key: &ObjectKey,
        ) -> mooncake_store_core::Result<Option<TenantObjectAccounting>> {
            self.inner.get_tenant_object_accounting(key)
        }

        fn list_tenant_quota_reservations(
            &self,
            scope: &TenantPolicyScope,
        ) -> mooncake_store_core::Result<Vec<TenantQuotaReservation>> {
            self.inner.list_tenant_quota_reservations(scope)
        }

        fn reserve_tenant_quota(
            &self,
            request: &TenantQuotaReservationRequest,
        ) -> mooncake_store_core::Result<TenantQuotaReservationOutcome> {
            self.inner.reserve_tenant_quota(request)
        }

        fn finalize_tenant_quota(
            &self,
            request: &TenantQuotaFinalizeRequest,
        ) -> mooncake_store_core::Result<TenantQuotaFinalizeOutcome> {
            self.inner.finalize_tenant_quota(request)
        }

        fn abort_tenant_quota(
            &self,
            reservation_id: &str,
        ) -> mooncake_store_core::Result<TenantQuotaAbortOutcome> {
            self.inner.abort_tenant_quota(reservation_id)
        }

        fn put_handoff(&self, handoff: &HandoffPlan) -> mooncake_store_core::Result<()> {
            self.inner.put_handoff(handoff)
        }

        fn get_handoff(
            &self,
            stable_id: &ClientStableId,
        ) -> mooncake_store_core::Result<Option<HandoffPlan>> {
            self.inner.get_handoff(stable_id)
        }
    }

    impl MetadataBackend for RecoveryCountingMetadata {
        fn route_namespace(&self) -> String {
            self.inner.route_namespace()
        }

        fn upsert_client_lease(&self, lease: &ClientLease) -> mooncake_store_core::Result<()> {
            let mut remaining = self.lease_failures_remaining.lock();
            if *remaining > 0 {
                *remaining -= 1;
                return Err(StoreError::Metadata("simulated redis outage".to_string()));
            }
            drop(remaining);
            self.inner.upsert_client_lease(lease)
        }

        fn update_client_state(
            &self,
            runtime: &ClientRuntimeId,
            next: ClientLifecycleState,
        ) -> mooncake_store_core::Result<()> {
            self.inner.update_client_state(runtime, next)
        }

        fn list_live_clients(&self) -> mooncake_store_core::Result<Vec<ClientLease>> {
            self.inner.list_live_clients()
        }

        fn publish_segment(
            &self,
            segment: &SegmentAnnouncement,
        ) -> mooncake_store_core::Result<()> {
            self.publish_segment_calls.fetch_add(1, Ordering::SeqCst);
            self.inner.publish_segment(segment)
        }

        fn unpublish_segment(
            &self,
            owner: &ClientRuntimeId,
            segment: &SegmentName,
        ) -> mooncake_store_core::Result<()> {
            self.inner.unpublish_segment(owner, segment)
        }

        fn list_segments(
            &self,
            owner: Option<&ClientRuntimeId>,
        ) -> mooncake_store_core::Result<Vec<SegmentAnnouncement>> {
            self.inner.list_segments(owner)
        }

        fn update_segment_state(
            &self,
            owner: &ClientRuntimeId,
            segment: &SegmentName,
            next: SegmentLifecycleState,
        ) -> mooncake_store_core::Result<()> {
            self.inner.update_segment_state(owner, segment, next)
        }

        fn reserve_segment(
            &self,
            owner: &ClientRuntimeId,
            segment: &SegmentName,
            length_bytes: u64,
        ) -> mooncake_store_core::Result<SegmentReservation> {
            self.inner.reserve_segment(owner, segment, length_bytes)
        }

        fn release_segment(
            &self,
            owner: &ClientRuntimeId,
            segment: &SegmentName,
            offset_bytes: u64,
            length_bytes: u64,
        ) -> mooncake_store_core::Result<()> {
            self.inner
                .release_segment(owner, segment, offset_bytes, length_bytes)
        }

        fn get_object_route(
            &self,
            key: &ObjectKey,
        ) -> mooncake_store_core::Result<Option<ObjectRoute>> {
            self.inner.get_object_route(key)
        }

        fn list_object_routes(&self) -> mooncake_store_core::Result<Vec<ObjectRoute>> {
            self.inner.list_object_routes()
        }

        fn compare_and_swap_object_route(
            &self,
            key: &ObjectKey,
            expected: Option<RouteVersion>,
            next: Option<&ObjectRoute>,
        ) -> mooncake_store_core::Result<CasResult> {
            self.inner
                .compare_and_swap_object_route(key, expected, next)
        }

        fn get_route_policy(
            &self,
            domain: &RoutePolicyDomain,
        ) -> mooncake_store_core::Result<Option<RoutePolicy>> {
            self.inner.get_route_policy(domain)
        }

        fn put_route_policy_if_absent(
            &self,
            domain: &RoutePolicyDomain,
            policy: &RoutePolicy,
        ) -> mooncake_store_core::Result<bool> {
            self.inner.put_route_policy_if_absent(domain, policy)
        }

        fn put_route_policy(
            &self,
            domain: &RoutePolicyDomain,
            policy: &RoutePolicy,
        ) -> mooncake_store_core::Result<()> {
            self.inner.put_route_policy(domain, policy)
        }

        fn delete_route_policy(
            &self,
            domain: &RoutePolicyDomain,
        ) -> mooncake_store_core::Result<bool> {
            self.inner.delete_route_policy(domain)
        }

        fn list_route_policies(
            &self,
        ) -> mooncake_store_core::Result<Vec<(RoutePolicyDomain, RoutePolicy)>> {
            self.inner.list_route_policies()
        }

        fn get_tenant_policy(
            &self,
            scope: &TenantPolicyScope,
        ) -> mooncake_store_core::Result<Option<TenantPolicy>> {
            self.inner.get_tenant_policy(scope)
        }

        fn list_tenant_policies(&self) -> mooncake_store_core::Result<Vec<TenantPolicy>> {
            self.inner.list_tenant_policies()
        }

        fn put_tenant_policy(
            &self,
            policy: &TenantPolicy,
            expected_version: Option<u64>,
        ) -> mooncake_store_core::Result<TenantPolicy> {
            self.inner.put_tenant_policy(policy, expected_version)
        }

        fn delete_tenant_policy(
            &self,
            scope: &TenantPolicyScope,
            expected_version: Option<u64>,
        ) -> mooncake_store_core::Result<bool> {
            self.inner.delete_tenant_policy(scope, expected_version)
        }

        fn get_tenant_quota_state(
            &self,
            scope: &TenantPolicyScope,
        ) -> mooncake_store_core::Result<Option<TenantQuotaState>> {
            self.inner.get_tenant_quota_state(scope)
        }

        fn get_tenant_object_accounting(
            &self,
            key: &ObjectKey,
        ) -> mooncake_store_core::Result<Option<TenantObjectAccounting>> {
            self.inner.get_tenant_object_accounting(key)
        }

        fn list_tenant_quota_reservations(
            &self,
            scope: &TenantPolicyScope,
        ) -> mooncake_store_core::Result<Vec<TenantQuotaReservation>> {
            self.inner.list_tenant_quota_reservations(scope)
        }

        fn reserve_tenant_quota(
            &self,
            request: &TenantQuotaReservationRequest,
        ) -> mooncake_store_core::Result<TenantQuotaReservationOutcome> {
            self.inner.reserve_tenant_quota(request)
        }

        fn finalize_tenant_quota(
            &self,
            request: &TenantQuotaFinalizeRequest,
        ) -> mooncake_store_core::Result<TenantQuotaFinalizeOutcome> {
            self.inner.finalize_tenant_quota(request)
        }

        fn abort_tenant_quota(
            &self,
            reservation_id: &str,
        ) -> mooncake_store_core::Result<TenantQuotaAbortOutcome> {
            self.inner.abort_tenant_quota(reservation_id)
        }

        fn put_handoff(&self, handoff: &HandoffPlan) -> mooncake_store_core::Result<()> {
            self.inner.put_handoff(handoff)
        }

        fn get_handoff(
            &self,
            stable_id: &ClientStableId,
        ) -> mooncake_store_core::Result<Option<HandoffPlan>> {
            self.inner.get_handoff(stable_id)
        }
    }

    fn build_real_store(name: &str) -> PyMooncakeDistributedStore {
        let dispatcher = StoreDispatcher::spawn(build_client(name), format!("dispatcher-{name}"))
            .expect("dispatcher should spawn");
        dispatcher
            .register_local_memory()
            .expect("local memory should register");
        let mut store = PyMooncakeDistributedStore::new();
        store.replace_backend(StoreBackend::Real(dispatcher));
        store
    }

    fn lease_expires_at_ms(metadata: &InMemoryMetadataBackend, runtime: &ClientRuntimeId) -> u64 {
        metadata
            .list_live_clients()
            .expect("leases should list")
            .into_iter()
            .find(|lease| lease.runtime == *runtime)
            .expect("target lease should exist")
            .expires_at_ms
    }

    fn wait_until_lease_after(
        metadata: &InMemoryMetadataBackend,
        runtime: &ClientRuntimeId,
        previous_expires_at_ms: u64,
    ) -> bool {
        for _ in 0..80 {
            if lease_expires_at_ms(metadata, runtime) > previous_expires_at_ms {
                return true;
            }
            sleep(Duration::from_millis(25));
        }
        false
    }

    fn bind_addr() -> String {
        let listener = TcpListener::bind("127.0.0.1:0").expect("listener should bind");
        let address = listener
            .local_addr()
            .expect("listener addr should resolve")
            .to_string();
        drop(listener);
        address
    }

    fn build_dummy_store(name: &str) -> (PyMooncakeDistributedStore, DummyStoreServerHandle) {
        let dispatcher = Arc::new(
            StoreDispatcher::spawn(build_client(name), format!("dummy-dispatcher-{name}"))
                .expect("dummy dispatcher should spawn"),
        );
        dispatcher
            .register_local_memory()
            .expect("dummy local memory should register");
        let server =
            start_dummy_store_server(dispatcher, &bind_addr()).expect("dummy server should start");
        let mut store = PyMooncakeDistributedStore::new();
        store
            .setup_dummy(0, 0, server.address())
            .expect("dummy store should connect");
        for _ in 0..40 {
            if store.health_check().expect("health check should succeed") == 0 {
                break;
            }
            sleep(Duration::from_millis(25));
        }
        (store, server)
    }

    struct EnvVarGuard {
        key: &'static str,
        previous: Option<String>,
    }

    impl EnvVarGuard {
        fn set(key: &'static str, value: &str) -> Self {
            let previous = std::env::var(key).ok();
            std::env::set_var(key, value);
            Self { key, previous }
        }

        fn unset(key: &'static str) -> Self {
            let previous = std::env::var(key).ok();
            std::env::remove_var(key);
            Self { key, previous }
        }
    }

    impl Drop for EnvVarGuard {
        fn drop(&mut self) {
            if let Some(previous) = self.previous.as_deref() {
                std::env::set_var(self.key, previous);
            } else {
                std::env::remove_var(self.key);
            }
        }
    }

    #[derive(Clone)]
    struct BlockingDummyStoreService {
        release: Arc<AtomicBool>,
    }

    impl BlockingDummyStoreService {
        async fn wait(&self) {
            while !self.release.load(Ordering::SeqCst) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }
    }

    #[tonic::async_trait]
    impl pb::dummy_store_service_server::DummyStoreService for BlockingDummyStoreService {
        async fn health(
            &self,
            _request: tonic::Request<pb::HealthRequest>,
        ) -> std::result::Result<tonic::Response<pb::HealthReply>, tonic::Status> {
            Ok(tonic::Response::new(pb::HealthReply { status: 0 }))
        }

        async fn put(
            &self,
            _request: tonic::Request<pb::PutRequest>,
        ) -> std::result::Result<tonic::Response<pb::StatusReply>, tonic::Status> {
            self.wait().await;
            Ok(tonic::Response::new(pb::StatusReply { status: 0 }))
        }

        async fn get(
            &self,
            _request: tonic::Request<pb::GetRequest>,
        ) -> std::result::Result<tonic::Response<pb::GetReply>, tonic::Status> {
            self.wait().await;
            Ok(tonic::Response::new(pb::GetReply {
                status: 0,
                value: b"released".to_vec(),
            }))
        }

        async fn acquire_hot_cache(
            &self,
            _request: tonic::Request<pb::HotCacheAcquireRequest>,
        ) -> std::result::Result<tonic::Response<pb::HotCacheAcquireReply>, tonic::Status> {
            self.wait().await;
            Ok(tonic::Response::new(pb::HotCacheAcquireReply {
                status: -1,
                ..Default::default()
            }))
        }

        async fn release_hot_cache(
            &self,
            _request: tonic::Request<pb::HotCacheReleaseRequest>,
        ) -> std::result::Result<tonic::Response<pb::StatusReply>, tonic::Status> {
            self.wait().await;
            Ok(tonic::Response::new(pb::StatusReply { status: 0 }))
        }

        async fn batch_acquire_hot_cache(
            &self,
            _request: tonic::Request<pb::BatchHotCacheAcquireRequest>,
        ) -> std::result::Result<tonic::Response<pb::BatchHotCacheAcquireReply>, tonic::Status>
        {
            self.wait().await;
            Ok(tonic::Response::new(pb::BatchHotCacheAcquireReply {
                items: vec![],
            }))
        }

        async fn batch_release_hot_cache(
            &self,
            _request: tonic::Request<pb::BatchHotCacheReleaseRequest>,
        ) -> std::result::Result<tonic::Response<pb::StatusReply>, tonic::Status> {
            self.wait().await;
            Ok(tonic::Response::new(pb::StatusReply { status: 0 }))
        }

        async fn batch_is_exist(
            &self,
            _request: tonic::Request<pb::BatchIsExistRequest>,
        ) -> std::result::Result<tonic::Response<pb::BatchIsExistReply>, tonic::Status> {
            self.wait().await;
            Ok(tonic::Response::new(pb::BatchIsExistReply {
                statuses: vec![],
            }))
        }

        async fn batch_put_from(
            &self,
            _request: tonic::Request<pb::BatchPutFromRequest>,
        ) -> std::result::Result<tonic::Response<pb::BatchStatusReply>, tonic::Status> {
            self.wait().await;
            Ok(tonic::Response::new(pb::BatchStatusReply {
                statuses: vec![],
            }))
        }

        async fn batch_put_from_multi_buffers(
            &self,
            _request: tonic::Request<pb::BatchPutFromMultiBuffersRequest>,
        ) -> std::result::Result<tonic::Response<pb::BatchStatusReply>, tonic::Status> {
            self.wait().await;
            Ok(tonic::Response::new(pb::BatchStatusReply {
                statuses: vec![],
            }))
        }

        async fn batch_get_into(
            &self,
            _request: tonic::Request<pb::BatchGetIntoRequest>,
        ) -> std::result::Result<tonic::Response<pb::BatchGetIntoReply>, tonic::Status> {
            self.wait().await;
            Ok(tonic::Response::new(pb::BatchGetIntoReply {
                lengths: vec![],
            }))
        }

        async fn batch_get_into_multi_buffers(
            &self,
            _request: tonic::Request<pb::BatchGetIntoMultiBuffersRequest>,
        ) -> std::result::Result<tonic::Response<pb::BatchGetIntoReply>, tonic::Status> {
            self.wait().await;
            Ok(tonic::Response::new(pb::BatchGetIntoReply {
                lengths: vec![],
            }))
        }

        async fn remove_all(
            &self,
            _request: tonic::Request<pb::RemoveAllRequest>,
        ) -> std::result::Result<tonic::Response<pb::RemoveAllReply>, tonic::Status> {
            self.wait().await;
            Ok(tonic::Response::new(pb::RemoveAllReply {
                status: 0,
                removed: 0,
            }))
        }

        async fn unregister_region(
            &self,
            _request: tonic::Request<pb::UnregisterRegionRequest>,
        ) -> std::result::Result<tonic::Response<pb::StatusReply>, tonic::Status> {
            self.wait().await;
            Ok(tonic::Response::new(pb::StatusReply { status: 0 }))
        }
    }

    struct BlockingDummyServerHandle {
        address: String,
        release: Arc<AtomicBool>,
        shutdown: Option<tokio::sync::oneshot::Sender<()>>,
        thread: Option<std::thread::JoinHandle<()>>,
    }

    impl BlockingDummyServerHandle {
        fn address(&self) -> &str {
            &self.address
        }

        fn release(&self) {
            self.release.store(true, Ordering::SeqCst);
        }

        fn shutdown(mut self) {
            self.release();
            if let Some(shutdown) = self.shutdown.take() {
                let _ = shutdown.send(());
            }
            if let Some(thread) = self.thread.take() {
                let _ = thread.join();
            }
        }
    }

    fn start_blocking_dummy_server() -> BlockingDummyServerHandle {
        let address = bind_addr();
        let socket_addr = address
            .parse()
            .expect("blocking dummy server address should parse");
        let release = Arc::new(AtomicBool::new(false));
        let service = BlockingDummyStoreService {
            release: release.clone(),
        };
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let thread = std::thread::Builder::new()
            .name("blocking-dummy-server".to_string())
            .spawn(move || {
                let runtime =
                    tokio::runtime::Runtime::new().expect("blocking dummy runtime should build");
                runtime.block_on(async move {
                    tonic::transport::Server::builder()
                        .add_service(
                            pb::dummy_store_service_server::DummyStoreServiceServer::new(service),
                        )
                        .serve_with_shutdown(socket_addr, async move {
                            let _ = shutdown_rx.await;
                        })
                        .await
                        .expect("blocking dummy server should run");
                });
            })
            .expect("blocking dummy server thread should spawn");
        DummySession::connect(&address).expect("blocking dummy server should accept connections");
        BlockingDummyServerHandle {
            address,
            release,
            shutdown: Some(shutdown_tx),
            thread: Some(thread),
        }
    }

    fn sample_segment() -> SegmentAnnouncement {
        SegmentAnnouncement {
            owner: ClientRuntimeId::new("owner", ClientEpoch(2)),
            segment_name: SegmentName::new("segment-z"),
            capacity_bytes: 256,
            used_bytes: 32,
            state: SegmentLifecycleState::Active,
            alignment_bytes: 8,
            tags: vec!["dram".to_string()],
        }
    }

    fn sample_route() -> ObjectRoute {
        ObjectRoute {
            key: ObjectKey::new("key-z"),
            namespace: None,
            logical_key: None,
            canonical_key: None,
            sharing_scope: None,
            qos_tier: None,
            version: RouteVersion(4),
            state: RouteState::Active,
            compatibility: CompatibilityDescriptor::default(),
            replicas: vec![ReplicaRoute {
                owner: ClientRuntimeId::new("owner", ClientEpoch(2)),
                segment_name: SegmentName::new("segment-z"),
                offset: 64,
                segment_offset: 64,
                length: 5,
                checksum: Some(3),
                tier: ReplicaTier::Dram,
                priority: 1,
            }],
        }
    }

    fn init_python() {
        prepare_freethreaded_python();
    }

    #[test]
    fn helper_functions_and_module_registration_work() {
        init_python();
        let store = PyMooncakeDistributedStore::new();
        assert_eq!(
            store
                .health_check()
                .expect("default health check should succeed"),
            1
        );
        assert!(pointer_from_usize(0).is_err());
        assert!(replication_policy(None, None, None, None, None, true, false, false).is_none());

        let policy = replication_policy(
            Some(2),
            Some("segment-a".to_string()),
            Some(vec!["segment-b".to_string()]),
            Some("writer-a".to_string()),
            Some(vec!["writer-b".to_string()]),
            false,
            true,
            true,
        )
        .expect("non-default replication hints should create a policy");
        assert_eq!(policy.replica_count, Some(2));
        assert_eq!(policy.preferred_segments.len(), 2);
        assert_eq!(policy.preferred_storage_owners.len(), 2);
        assert!(!policy.prefer_local);
        assert!(policy.prefer_alloc_in_same_node);
        assert!(policy.with_soft_pin);

        Python::with_gil(|py| {
            let not_found = store_error_to_py(StoreError::NotFound("missing".to_string()));
            assert!(not_found.is_instance_of::<PyKeyError>(py));
            let runtime = store_error_to_py(StoreError::Metadata("oops".to_string()));
            assert!(runtime.is_instance_of::<PyRuntimeError>(py));
        });
    }

    #[test]
    fn python_setup_parsers_accept_hot_upgrade_identity_flags() {
        assert_eq!(
            parse_client_epoch_arg(7).expect("epoch parser should accept positive values"),
            ClientEpoch(7)
        );
        assert_eq!(
            parse_initial_state_arg("standby").expect("state parser should accept standby"),
            ClientLifecycleState::Standby
        );
        assert_eq!(
            parse_initial_state_arg(" Draining ").expect("state parser should trim input"),
            ClientLifecycleState::Draining
        );
    }

    #[test]
    fn python_setup_parsers_reject_invalid_hot_upgrade_identity_flags() {
        init_python();
        Python::with_gil(|py| {
            assert!(parse_client_epoch_arg(0)
                .expect_err("epoch zero must be rejected")
                .is_instance_of::<PyValueError>(py));
            assert!(parse_initial_state_arg("promoting")
                .expect_err("unknown lifecycle state must be rejected")
                .is_instance_of::<PyValueError>(py));
        });
    }

    #[test]
    fn helpers_convert_python_dicts_and_top_level_module_exports() {
        init_python();
        Python::with_gil(|py| {
            let segment = sample_segment();
            let route = sample_route();
            let segment_dict = segment_to_py(py, &segment).expect("segment should convert");
            let route_dict = route_to_py(py, &route).expect("route should convert");
            let segment_any = segment_dict.bind(py);
            let route_any = route_dict.bind(py);
            assert_eq!(
                segment_any
                    .get_item("segment_name")
                    .expect("segment field should exist")
                    .extract::<String>()
                    .expect("segment name should extract"),
                "segment-z"
            );
            assert_eq!(
                route_any
                    .get_item("key")
                    .expect("route field should exist")
                    .extract::<String>()
                    .expect("route key should extract"),
                "key-z"
            );

            let module = PyModule::new(py, "_store_rs").expect("module should create");
            _store_rs(&module).expect("module init should succeed");
            assert!(module.getattr("MooncakeDistributedStore").is_ok());
            assert!(module.getattr("MooncakeHostMemAllocator").is_ok());
            assert!(module.getattr("metrics_text").is_ok());
        });

        stop_metrics_server().expect("metrics server cleanup should succeed");
        init_tracing(Some("info")).expect("tracing init should be idempotent");
        let address = start_metrics_server("127.0.0.1:0").expect("metrics server should start");
        assert!(address.contains(':'));
        assert!(metrics_server_address().is_some());
        assert!(!metrics_text().is_empty());
        stop_metrics_server().expect("metrics server should stop");
        assert!(metrics_server_address().is_none());

        let allocator = PyMooncakeHostMemAllocator::new(None, None);
        let ptr = allocator
            .alloc(128)
            .expect("allocator should return shared memory");
        allocator
            .free(ptr)
            .expect("allocator should free shared memory");
    }

    #[test]
    fn real_store_supports_core_python_compat_api() {
        init_python();
        let mut store = PyMooncakeDistributedStore::new();
        store.replace_backend(StoreBackend::Real(
            StoreDispatcher::spawn(build_client("py-real"), "dispatcher-py-real")
                .expect("dispatcher should spawn"),
        ));
        store
            .real_dispatcher()
            .expect("real dispatcher should be installed")
            .register_local_memory()
            .expect("local memory should register");
        assert_eq!(
            store
                .health_check()
                .expect("real health check should succeed"),
            0
        );
        assert_eq!(
            store.get_hostname().expect("hostname should resolve"),
            "127.0.0.1"
        );

        store
            .put(
                "alpha",
                b"one".to_vec(),
                None,
                None,
                None,
                None,
                None,
                None,
                true,
                false,
                false,
            )
            .expect("put should succeed");
        store
            .batch_put(
                vec![
                    ("beta".to_string(), b"two".to_vec()),
                    ("gamma".to_string(), b"three".to_vec()),
                ],
                None,
                None,
                None,
                None,
                None,
                None,
                true,
                false,
                false,
            )
            .expect("batch_put should succeed");

        Python::with_gil(|py| {
            assert_eq!(
                store
                    .get(py, "alpha", None)
                    .expect("get should succeed")
                    .as_bytes(),
                b"one"
            );
            let batch = store
                .batch_get(py, vec!["alpha".to_string(), "beta".to_string()], None)
                .expect("batch_get should succeed");
            assert_eq!(batch[0].bind(py).as_bytes(), b"one");
            assert_eq!(batch[1].bind(py).as_bytes(), b"two");
            let mirrored = store
                .batch_get_buffer(py, vec!["gamma".to_string()], None)
                .expect("batch_get_buffer should delegate to batch_get");
            assert_eq!(mirrored[0].bind(py).as_bytes(), b"three");
        });

        assert!(store
            .is_exist("alpha", None)
            .expect("is_exist should succeed"));
        assert_eq!(
            store
                .batch_is_exist(vec!["alpha".to_string(), "missing".to_string()], None)
                .expect("batch_is_exist should succeed"),
            vec![1, 0]
        );
        assert_eq!(
            store.get_size("alpha", None).expect("size should resolve"),
            3
        );

        let mut registered = vec![0u8; 32];
        store
            .register_buffer(registered.as_mut_ptr() as usize, registered.len())
            .expect("register_buffer should succeed");
        store
            .unregister_buffer(registered.as_mut_ptr() as usize, registered.len())
            .expect("unregister_buffer should succeed");

        let source = b"from-buffer".to_vec();
        store
            .register_buffer(source.as_ptr() as usize, source.len())
            .expect("source buffer registration should succeed");
        store
            .put_from(
                "delta",
                source.as_ptr() as usize,
                source.len(),
                None,
                Some(1),
                None,
                None,
                None,
                None,
                true,
                false,
                false,
            )
            .expect("put_from should succeed");
        let mut read_buffer = vec![0u8; 32];
        let copied = store
            .get_into(
                "delta",
                read_buffer.as_mut_ptr() as usize,
                read_buffer.len(),
                None,
            )
            .expect("get_into should succeed");
        assert_eq!(copied, source.len());
        assert_eq!(&read_buffer[..copied], source.as_slice());

        Python::with_gil(|py| {
            let statuses = store
                .batch_put_from(
                    py,
                    vec![
                        (
                            "epsilon".to_string(),
                            source.as_ptr() as usize,
                            source.len(),
                        ),
                        ("zeta".to_string(), source.as_ptr() as usize, source.len()),
                    ],
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    true,
                    false,
                    false,
                )
                .expect("batch_put_from should succeed");
            assert_eq!(
                statuses
                    .bind(py)
                    .extract::<Vec<i32>>()
                    .expect("batch_put_from should return status list"),
                vec![0, 0]
            );
        });
        store
            .unregister_buffer(source.as_ptr() as usize, source.len())
            .expect("source buffer unregister should succeed");

        let mut target_a = vec![0u8; 16];
        let mut target_b = vec![0u8; 16];
        let lengths = store
            .batch_get_into(
                vec![
                    (
                        "epsilon".to_string(),
                        target_a.as_mut_ptr() as usize,
                        target_a.len(),
                    ),
                    (
                        "zeta".to_string(),
                        target_b.as_mut_ptr() as usize,
                        target_b.len(),
                    ),
                ],
                None,
            )
            .expect("batch_get_into should succeed");
        assert_eq!(lengths, vec![source.len() as i64, source.len() as i64]);
        assert_eq!(&target_a[..source.len()], source.as_slice());
        assert_eq!(&target_b[..source.len()], source.as_slice());

        let raw_lengths = store
            .batch_get_into_raw(
                vec!["alpha".to_string()],
                vec![read_buffer.as_mut_ptr() as usize],
                vec![read_buffer.len()],
                None,
            )
            .expect("batch_get_into_raw should succeed");
        assert_eq!(raw_lengths, vec![3]);
        assert_eq!(
            store
                .batch_get_into(
                    vec![(
                        "missing".to_string(),
                        read_buffer.as_mut_ptr() as usize,
                        read_buffer.len(),
                    )],
                    None,
                )
                .expect("missing batch_get_into should degrade to soft miss"),
            vec![-1]
        );
        assert!(store
            .batch_get_into_raw(vec!["alpha".to_string()], vec![], vec![], None)
            .is_err());

        store
            .batch_put_from_multi_buffers(
                vec![(
                    "multi".to_string(),
                    vec![b"ab".to_vec(), b"cd".to_vec(), b"ef".to_vec()],
                )],
                None,
                None,
                None,
                None,
                None,
                None,
                true,
                false,
                false,
            )
            .expect("multi-buffer put should succeed");
        let mut part1 = vec![0u8; 2];
        let mut part2 = vec![0u8; 4];
        let multi_lengths = store
            .batch_get_into_multi_buffers(
                vec!["multi".to_string()],
                vec![vec![
                    part1.as_mut_ptr() as usize,
                    part2.as_mut_ptr() as usize,
                ]],
                vec![vec![part1.len(), part2.len()]],
                false,
                None,
            )
            .expect("multi-buffer get should succeed");
        assert_eq!(multi_lengths, vec![6]);
        assert_eq!(&part1, b"ab");
        assert_eq!(&part2, b"cdef");
        assert_eq!(
            store
                .batch_get_into_multi_buffers(
                    vec!["missing-multi".to_string()],
                    vec![vec![
                        part1.as_mut_ptr() as usize,
                        part2.as_mut_ptr() as usize,
                    ]],
                    vec![vec![part1.len(), part2.len()]],
                    false,
                    None,
                )
                .expect("missing multi-buffer get should degrade to soft miss"),
            vec![-1]
        );

        Python::with_gil(|py| {
            assert!(store
                .batch_put_from_raw(
                    py,
                    vec!["oops".to_string()],
                    vec![],
                    vec![],
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    true,
                    false,
                    false,
                )
                .is_err());
        });

        Python::with_gil(|py| {
            let route = store
                .query_route(py, "alpha", None)
                .expect("query_route should succeed")
                .expect("route should exist");
            assert_eq!(
                route
                    .bind(py)
                    .get_item("key")
                    .expect("route key should exist")
                    .extract::<String>()
                    .expect("route key should extract"),
                "default::alpha"
            );
            let segments = store
                .list_segments(py)
                .expect("list_segments should succeed");
            assert!(!segments.is_empty());
            let segment_name = segments[0]
                .bind(py)
                .get_item("segment_name")
                .expect("segment_name should exist")
                .extract::<String>()
                .expect("segment_name should extract");
            assert!(store.drain_segment(&segment_name).is_err());
            match store.retire_segment(&segment_name) {
                Ok(retired) => assert!(!retired),
                Err(_) => {}
            }
        });

        store.heartbeat(90_000).expect("heartbeat should succeed");
        store.enter_standby().expect("standby should succeed");
        store.activate().expect("activate should succeed");

        store
            .remove("alpha", false, None)
            .expect("remove should succeed");
        assert_eq!(
            store
                .batch_remove(vec!["beta".to_string(), "gamma".to_string()], false, None)
                .expect("batch_remove should succeed"),
            vec![0, 0]
        );
        assert!(store.remove_all(false).is_err());
        assert!(!store.metrics_text().is_empty());

        store.enter_draining().expect("draining should succeed");
        let _ = store.evacuate_owned_replicas();
        match store.evacuate_owned_replicas() {
            Ok(_) => {}
            Err(error) => {
                assert!(
                    error
                        .to_string()
                        .contains("not enough writable placement targets"),
                    "unexpected shrink result: {error:?}"
                );
            }
        }

        store.close();
        assert!(store.get_hostname().is_err());
    }

    #[test]
    fn dummy_store_exercises_dummy_rpc_and_shared_memory_paths() {
        init_python();
        let (mut store, server) = build_dummy_store("py-dummy");
        assert_eq!(
            store.health_check().expect("health check should succeed"),
            0
        );
        assert_eq!(
            store.get_hostname().expect("dummy hostname should resolve"),
            server.address()
        );

        store
            .put(
                "alpha",
                b"one".to_vec(),
                None,
                None,
                None,
                None,
                None,
                None,
                true,
                false,
                false,
            )
            .expect("dummy put should succeed");
        store
            .batch_put(
                vec![
                    ("beta".to_string(), b"two".to_vec()),
                    ("gamma".to_string(), b"three".to_vec()),
                ],
                None,
                None,
                None,
                None,
                None,
                None,
                true,
                false,
                false,
            )
            .expect("dummy batch_put should succeed");

        Python::with_gil(|py| {
            assert_eq!(
                store
                    .get(py, "alpha", None)
                    .expect("dummy get should succeed")
                    .as_bytes(),
                b"one"
            );
            assert!(store
                .batch_get(py, vec!["alpha".to_string()], None)
                .is_err());
            assert!(store.query_route(py, "alpha", None).is_err());
        });
        assert!(store
            .is_exist("alpha", None)
            .expect("dummy is_exist should succeed"));
        assert_eq!(
            store
                .batch_is_exist(vec!["alpha".to_string(), "missing".to_string()], None)
                .expect("dummy batch_is_exist should succeed"),
            vec![1, 0]
        );
        assert_eq!(
            store
                .get_size("gamma", None)
                .expect("dummy size should resolve"),
            5
        );

        let allocator = PyMooncakeHostMemAllocator::new(None, None);
        let write_ptr = allocator
            .alloc(64)
            .expect("dummy write buffer should allocate");
        let read_ptr = allocator
            .alloc(64)
            .expect("dummy read buffer should allocate");
        unsafe {
            slice::from_raw_parts_mut(write_ptr as *mut u8, 64)[..5].copy_from_slice(b"hello");
        }

        store
            .register_buffer(write_ptr, 64)
            .expect("dummy register_buffer should succeed");
        store
            .register_buffer(read_ptr, 64)
            .expect("dummy register_buffer should succeed");
        store
            .put_from(
                "buffered", write_ptr, 5, None, None, None, None, None, None, true, false, false,
            )
            .expect("dummy put_from should succeed");
        let copied = store
            .get_into("buffered", read_ptr, 64, None)
            .expect("dummy get_into should succeed");
        assert_eq!(copied, 5);
        unsafe {
            assert_eq!(slice::from_raw_parts(read_ptr as *const u8, 5), b"hello");
        }

        Python::with_gil(|py| {
            let statuses = store
                .batch_put_from(
                    py,
                    vec![("from-batch".to_string(), write_ptr, 5)],
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    true,
                    false,
                    false,
                )
                .expect("dummy batch_put_from should succeed");
            assert_eq!(
                statuses
                    .bind(py)
                    .extract::<Vec<i32>>()
                    .expect("dummy batch_put_from should return status list"),
                vec![0]
            );
        });

        let raw_statuses = store
            .batch_put_from_multi_buffers_raw(
                vec!["raw-multi".to_string()],
                vec![vec![write_ptr, write_ptr + 2]],
                vec![vec![2, 3]],
                None,
                None,
                None,
                None,
                None,
                None,
                true,
                false,
                false,
            )
            .expect("dummy raw multi-buffer put should succeed");
        assert_eq!(raw_statuses, vec![0]);

        store
            .batch_put_from_multi_buffers(
                vec![(
                    "vec-multi".to_string(),
                    vec![b"ab".to_vec(), b"cd".to_vec(), b"ef".to_vec()],
                )],
                None,
                None,
                None,
                None,
                None,
                None,
                true,
                false,
                false,
            )
            .expect("dummy vec multi-buffer put should succeed");

        let lengths = store
            .batch_get_into(vec![("from-batch".to_string(), read_ptr, 64)], None)
            .expect("dummy batch_get_into should succeed");
        assert_eq!(lengths, vec![5]);
        let multi_lengths = store
            .batch_get_into_multi_buffers(
                vec!["vec-multi".to_string()],
                vec![vec![read_ptr, read_ptr + 2]],
                vec![vec![2, 4]],
                false,
                None,
            )
            .expect("dummy batch_get_into_multi_buffers should succeed");
        assert_eq!(multi_lengths, vec![6]);

        assert!(store
            .batch_get_into_raw(vec!["alpha".to_string()], vec![], vec![], None)
            .is_err());

        store
            .unregister_buffer(read_ptr, 64)
            .expect("dummy unregister_buffer should succeed");
        assert!(store.get_into("buffered", read_ptr, 64, None).is_err());

        let removed = store
            .remove_all(false)
            .expect("dummy remove_all should succeed");
        assert!(removed >= 1);
        assert!(store.remove("alpha", false, None).is_err());
        Python::with_gil(|py| {
            assert!(store.list_segments(py).is_err());
        });

        store.close();
        allocator
            .free(write_ptr)
            .expect("dummy write buffer should free");
        allocator
            .free(read_ptr)
            .expect("dummy read buffer should free");
        server.shutdown().expect("dummy server should stop");
    }

    #[test]
    fn real_get_reuses_local_hot_cache_after_remote_delete() {
        let _guard = env_test_lock().lock();
        let _cache_size = EnvVarGuard::set("MC_STORE_LOCAL_HOT_CACHE_SIZE", "4096");
        let _block_size = EnvVarGuard::set("MC_STORE_LOCAL_HOT_BLOCK_SIZE", "1024");
        let _use_shm = EnvVarGuard::unset("MC_STORE_LOCAL_HOT_CACHE_USE_SHM");
        init_python();
        let mut store = build_real_store("py-real-hot-cache");
        store
            .put(
                "alpha",
                b"one".to_vec(),
                None,
                None,
                None,
                None,
                None,
                None,
                true,
                false,
                false,
            )
            .expect("put should succeed");

        Python::with_gil(|py| {
            assert_eq!(
                store
                    .get(py, "alpha", None)
                    .expect("first get should succeed")
                    .as_bytes(),
                b"one"
            );
        });
        assert!(store
            .real_dispatcher()
            .expect("real dispatcher should exist")
            .hot_cache_contains("default", "alpha"));
        store
            .real_dispatcher()
            .expect("real dispatcher should exist")
            .run(|client| client.remove("alpha", true))
            .expect("raw remove should succeed");
        Python::with_gil(|py| {
            assert_eq!(
                store
                    .get(py, "alpha", None)
                    .expect("cache-backed get should succeed")
                    .as_bytes(),
                b"one"
            );
        });
        store.close();
    }

    #[test]
    fn dummy_clients_share_hot_cache_shm_hits() {
        let _guard = env_test_lock().lock();
        let _cache_size = EnvVarGuard::set("MC_STORE_LOCAL_HOT_CACHE_SIZE", "4096");
        let _block_size = EnvVarGuard::set("MC_STORE_LOCAL_HOT_BLOCK_SIZE", "1024");
        let _use_shm = EnvVarGuard::set("MC_STORE_LOCAL_HOT_CACHE_USE_SHM", "1");

        let dispatcher = Arc::new(
            StoreDispatcher::spawn(build_client("dummy-hot-cache"), "dummy-hot-cache")
                .expect("dispatcher should spawn"),
        );
        dispatcher
            .register_local_memory()
            .expect("local memory should register");
        let server = start_dummy_store_server(dispatcher.clone(), &bind_addr())
            .expect("server should start");
        let dummy_one = DummySession::connect(server.address()).expect("dummy one should connect");
        let dummy_two = DummySession::connect(server.address()).expect("dummy two should connect");
        assert!(dummy_one.has_hot_cache_mapping());
        assert!(dummy_two.has_hot_cache_mapping());

        dispatcher
            .run(|client| client.put("alpha", b"one"))
            .expect("put should succeed");
        let (status, value) = dummy_one
            .get("alpha", None)
            .expect("first dummy get should work");
        assert_eq!(status, 0);
        assert_eq!(value, b"one");
        assert!(dispatcher.hot_cache_contains("default", "alpha"));

        dispatcher
            .run(|client| client.remove("alpha", true))
            .expect("raw remove should succeed");
        let (status, value) = dummy_two
            .get("alpha", None)
            .expect("second dummy should hit shared hot cache");
        assert_eq!(status, 0);
        assert_eq!(value, b"one");

        dummy_one.close();
        dummy_two.close();
        server.shutdown().expect("server should stop");
    }

    #[test]
    fn dummy_rpc_returns_timeout_instead_of_hanging() {
        let server = start_blocking_dummy_server();
        let session =
            DummySession::connect_with_rpc_timeout(server.address(), Duration::from_millis(200))
                .expect("dummy client should connect to blocking server");
        let started = std::time::Instant::now();
        let error = session
            .get("blocked", None)
            .expect_err("blocked dummy rpc should time out");
        assert!(
            matches!(error, StoreError::Transport(ref message) if message.contains("timed out")),
            "unexpected error: {error}"
        );
        assert!(
            started.elapsed() < Duration::from_secs(8),
            "dummy rpc timeout should fail fast"
        );
        server.shutdown();
    }

    #[test]
    fn dispatcher_async_bridge_wakes_foreign_runtime() {
        let dispatcher = Arc::new(
            StoreDispatcher::spawn(build_client("dispatcher-foreign-runtime"), "dispatcher")
                .expect("dispatcher should spawn"),
        );
        let worker = std::thread::Builder::new()
            .name("dispatcher-foreign-runtime-test".to_string())
            .spawn({
                let dispatcher = dispatcher.clone();
                move || {
                    let runtime =
                        tokio::runtime::Runtime::new().expect("foreign runtime should build");
                    runtime.block_on(async move {
                        for expected in 0..16u32 {
                            let value = dispatcher
                                .run_async(move |_client| Ok::<_, StoreError>(expected))
                                .await
                                .expect("foreign runtime call should complete");
                            assert_eq!(value, expected);
                        }
                    });
                }
            })
            .expect("foreign runtime worker should spawn");
        worker.join().expect("foreign runtime worker should join");
    }

    #[test]
    fn dispatcher_async_wait_returns_timeout_instead_of_hanging() {
        let dispatcher = StoreDispatcher::spawn_with_timeouts(
            build_client("dispatcher-timeout"),
            "dispatcher-timeout".to_string(),
            Duration::from_millis(200),
            Duration::from_secs(15),
        )
        .expect("dispatcher should spawn");
        let (entered_tx, entered_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let started = std::time::Instant::now();
        let worker = std::thread::Builder::new()
            .name("dispatcher-timeout-test".to_string())
            .spawn(move || {
                dispatcher.run(move |_client| {
                    let _ = entered_tx.send(());
                    let _ = release_rx.recv();
                    Ok::<_, StoreError>(())
                })
            })
            .expect("dispatcher timeout worker should spawn");
        entered_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("dispatcher task should enter");
        let error = worker
            .join()
            .expect("dispatcher timeout worker should join")
            .expect_err("blocked dispatcher request should time out");
        assert!(
            matches!(error, StoreError::Transport(ref message) if message.contains("timed out")),
            "unexpected error: {error}"
        );
        assert!(
            started.elapsed() < Duration::from_secs(8),
            "dispatcher timeout should fail fast"
        );
        let _ = release_tx.send(());
    }

    #[test]
    fn dispatcher_shared_requests_do_not_head_of_line_block() {
        let dispatcher = Arc::new(
            StoreDispatcher::spawn(
                build_client("dispatcher-shared"),
                "dispatcher-shared".to_string(),
            )
            .expect("dispatcher should spawn"),
        );
        let (entered_tx, entered_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let worker = std::thread::Builder::new()
            .name("dispatcher-shared-read-holder".to_string())
            .spawn({
                let dispatcher = dispatcher.clone();
                move || {
                    dispatcher.run(move |_client| {
                        let _ = entered_tx.send(());
                        let _ = release_rx.recv();
                        Ok::<_, StoreError>(())
                    })
                }
            })
            .expect("dispatcher shared test worker should spawn");

        entered_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("first shared request should enter");
        let started = std::time::Instant::now();
        let second = dispatcher
            .run(|_client| Ok::<_, StoreError>(7usize))
            .expect("second shared request should not block behind first");
        assert_eq!(second, 7);
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "second shared request should complete quickly"
        );
        let _ = release_tx.send(());
        worker
            .join()
            .expect("dispatcher shared worker should join")
            .expect("first shared request should finish after release");
    }

    #[test]
    fn dispatcher_heartbeat_uses_independent_health_channel() {
        let dispatcher = Arc::new(
            StoreDispatcher::spawn(
                build_client("dispatcher-health-read-lock"),
                "dispatcher-health-read-lock".to_string(),
            )
            .expect("dispatcher should spawn"),
        );
        let (entered_tx, entered_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let worker = std::thread::Builder::new()
            .name("dispatcher-health-read-holder".to_string())
            .spawn({
                let dispatcher = dispatcher.clone();
                move || {
                    dispatcher.run(move |_client| {
                        let _ = entered_tx.send(());
                        let _ = release_rx.recv();
                        Ok::<_, StoreError>(())
                    })
                }
            })
            .expect("dispatcher health read-holder should spawn");

        entered_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("read-holder should enter");

        let started = std::time::Instant::now();
        dispatcher
            .heartbeat(60_000)
            .expect("heartbeat should not wait for read lock");
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "heartbeat should use independent health channel"
        );

        let _ = release_tx.send(());
        worker
            .join()
            .expect("dispatcher health read-holder should join")
            .expect("read-holder should finish after release");
    }

    #[test]
    fn dispatcher_state_updates_use_independent_health_channel() {
        let dispatcher = Arc::new(
            StoreDispatcher::spawn(
                build_client("dispatcher-state-read-lock"),
                "dispatcher-state-read-lock".to_string(),
            )
            .expect("dispatcher should spawn"),
        );
        let (entered_tx, entered_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let worker = std::thread::Builder::new()
            .name("dispatcher-state-read-holder".to_string())
            .spawn({
                let dispatcher = dispatcher.clone();
                move || {
                    dispatcher.run(move |_client| {
                        let _ = entered_tx.send(());
                        let _ = release_rx.recv();
                        Ok::<_, StoreError>(())
                    })
                }
            })
            .expect("dispatcher state read-holder should spawn");

        entered_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("read-holder should enter");

        let started = std::time::Instant::now();
        dispatcher
            .enter_draining()
            .expect("state update should not wait for read lock");
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "state update should use independent health channel"
        );

        let _ = release_tx.send(());
        worker
            .join()
            .expect("dispatcher state read-holder should join")
            .expect("read-holder should finish after release");
    }

    #[test]
    fn dispatcher_state_update_changes_inner_lifecycle() {
        let dispatcher = StoreDispatcher::spawn(
            build_client_with_metadata_and_state(
                "dispatcher-standby-activate",
                Arc::new(InMemoryMetadataBackend::new()),
                ClientLifecycleState::Standby,
            ),
            "dispatcher-standby-activate".to_string(),
        )
        .expect("dispatcher should spawn");
        dispatcher
            .register_local_memory()
            .expect("local memory should register");

        assert_eq!(
            dispatcher
                .run(|client| Ok::<_, StoreError>(client.lifecycle_state()))
                .expect("lifecycle read should succeed"),
            ClientLifecycleState::Standby
        );
        dispatcher.activate().expect("activate should succeed");
        assert_eq!(
            dispatcher
                .run(|client| Ok::<_, StoreError>(client.lifecycle_state()))
                .expect("lifecycle read should succeed"),
            ClientLifecycleState::Active
        );
        dispatcher
            .run(|client| client.put("activated-write", b"ok"))
            .expect("activated storage should accept writes");
        dispatcher
            .enter_draining()
            .expect("draining should succeed");
        assert_eq!(
            dispatcher
                .run(|client| Ok::<_, StoreError>(client.lifecycle_state()))
                .expect("lifecycle read should succeed"),
            ClientLifecycleState::Draining
        );
    }

    #[test]
    fn real_store_close_publishes_offline_state() {
        let metadata = Arc::new(InMemoryMetadataBackend::new());
        let backend = StoreBackend::Real(
            StoreDispatcher::spawn(
                build_client_with_metadata("dispatcher-close-offline", metadata.clone()),
                "dispatcher-close-offline".to_string(),
            )
            .expect("dispatcher should spawn"),
        );
        backend.close();

        let runtime = ClientRuntimeId::new("dispatcher-close-offline", ClientEpoch(1));
        let state = metadata
            .list_live_clients()
            .expect("leases should list")
            .into_iter()
            .find(|lease| lease.runtime == runtime)
            .map(|lease| lease.state);
        assert_eq!(state, Some(ClientLifecycleState::Offline));
    }

    #[test]
    fn dispatcher_auto_heartbeat_extends_lease_until_stopped() {
        let metadata = Arc::new(InMemoryMetadataBackend::new());
        let dispatcher = StoreDispatcher::spawn(
            build_client_with_metadata("dispatcher-auto-heartbeat", metadata.clone()),
            "dispatcher-auto-heartbeat".to_string(),
        )
        .expect("dispatcher should spawn");
        let runtime = ClientRuntimeId::new("dispatcher-auto-heartbeat", ClientEpoch(1));
        let initial_expires_at = lease_expires_at_ms(&metadata, &runtime);

        dispatcher
            .start_heartbeat_loop(90_000, Some(25))
            .expect("heartbeat loop should start");
        let refreshed = wait_until_lease_after(&metadata, &runtime, initial_expires_at);
        dispatcher.stop_heartbeat_loop();

        assert!(
            refreshed,
            "background heartbeat should refresh the lease expiry"
        );
        let stopped_expires_at = lease_expires_at_ms(&metadata, &runtime);
        sleep(Duration::from_millis(100));
        assert_eq!(
            lease_expires_at_ms(&metadata, &runtime),
            stopped_expires_at,
            "stopped heartbeat loop must not keep mutating the lease"
        );
        dispatcher.shutdown();
    }

    #[test]
    fn real_store_close_stops_auto_heartbeat_before_offline() {
        let metadata = Arc::new(InMemoryMetadataBackend::new());
        let dispatcher = StoreDispatcher::spawn(
            build_client_with_metadata("dispatcher-auto-heartbeat-close", metadata.clone()),
            "dispatcher-auto-heartbeat-close".to_string(),
        )
        .expect("dispatcher should spawn");
        dispatcher
            .start_heartbeat_loop(90_000, Some(25))
            .expect("heartbeat loop should start");
        let backend = StoreBackend::Real(dispatcher);

        backend.close();
        sleep(Duration::from_millis(100));

        let runtime = ClientRuntimeId::new("dispatcher-auto-heartbeat-close", ClientEpoch(1));
        let state = metadata
            .list_live_clients()
            .expect("leases should list")
            .into_iter()
            .find(|lease| lease.runtime == runtime)
            .map(|lease| lease.state);
        assert_eq!(state, Some(ClientLifecycleState::Offline));
    }

    #[test]
    fn dispatcher_plan_handoff_does_not_wait_for_read_lock() {
        let dispatcher = Arc::new(
            StoreDispatcher::spawn(
                build_client("dispatcher-plan-handoff"),
                "dispatcher-plan-handoff".to_string(),
            )
            .expect("dispatcher should spawn"),
        );
        let (entered_tx, entered_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let worker = std::thread::Builder::new()
            .name("dispatcher-plan-handoff-read-holder".to_string())
            .spawn({
                let dispatcher = dispatcher.clone();
                move || {
                    dispatcher.run(move |_client| {
                        let _ = entered_tx.send(());
                        let _ = release_rx.recv();
                        Ok::<_, StoreError>(())
                    })
                }
            })
            .expect("dispatcher plan-handoff read-holder should spawn");

        entered_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("read-holder should enter");

        let started = std::time::Instant::now();
        let plan = dispatcher
            .plan_handoff(ClientEpoch(2), HandoffKind::HotUpgrade, 7, 1_000, None)
            .expect("plan_handoff should not wait for read lock");
        assert_eq!(plan.to.epoch, ClientEpoch(2));
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "plan_handoff should use shared dispatcher path"
        );

        let _ = release_tx.send(());
        worker
            .join()
            .expect("dispatcher plan-handoff read-holder should join")
            .expect("read-holder should finish after release");
    }

    #[test]
    fn dispatcher_targeted_handoff_activation_does_not_wait_for_read_lock() {
        let metadata = Arc::new(InMemoryMetadataBackend::new());
        let dispatcher = Arc::new(
            StoreDispatcher::spawn(
                build_client_with_metadata("dispatcher-targeted-handoff", metadata.clone()),
                "dispatcher-targeted-handoff".to_string(),
            )
            .expect("dispatcher should spawn"),
        );
        dispatcher
            .enter_standby()
            .expect("standby transition should succeed");
        let runtime = dispatcher
            .run(|client| Ok::<_, StoreError>(client.runtime_id().clone()))
            .expect("runtime id should load");
        metadata
            .put_handoff(&HandoffPlan {
                stable_id: runtime.stable_id.clone(),
                from: runtime.clone(),
                to: runtime.clone(),
                kind: HandoffKind::HotUpgrade,
                barrier_version: 9,
                created_at_ms: 2_000,
                deadline_ms: None,
            })
            .expect("targeted handoff should publish");

        let (entered_tx, entered_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let worker = std::thread::Builder::new()
            .name("dispatcher-targeted-handoff-read-holder".to_string())
            .spawn({
                let dispatcher = dispatcher.clone();
                move || {
                    dispatcher.run(move |_client| {
                        let _ = entered_tx.send(());
                        let _ = release_rx.recv();
                        Ok::<_, StoreError>(())
                    })
                }
            })
            .expect("dispatcher targeted-handoff read-holder should spawn");

        entered_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("read-holder should enter");

        let started = std::time::Instant::now();
        let plan = dispatcher
            .activate_if_targeted_handoff()
            .expect("activate_if_targeted_handoff should not wait for read lock")
            .expect("targeted handoff should activate");
        assert_eq!(plan.to, runtime);
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "activate_if_targeted_handoff should not wait for read lock"
        );

        let _ = release_tx.send(());
        worker
            .join()
            .expect("dispatcher targeted-handoff read-holder should join")
            .expect("read-holder should finish after release");
    }

    #[test]
    fn dispatcher_heartbeat_timeout_does_not_block_shared_requests() {
        let (entered_tx, entered_rx) = std::sync::mpsc::channel();
        let block_enabled = Arc::new(AtomicBool::new(false));
        let release = Arc::new(AtomicBool::new(false));
        let metadata = Arc::new(BlockingHealthMetadata::new(
            block_enabled.clone(),
            Arc::new(AtomicBool::new(false)),
            release.clone(),
            entered_tx,
        ));
        let dispatcher = Arc::new(
            StoreDispatcher::spawn_with_heartbeat_timeout(
                build_client_with_metadata("dispatcher-heartbeat", metadata),
                "dispatcher-heartbeat".to_string(),
                Duration::from_millis(200),
            )
            .expect("dispatcher should spawn"),
        );
        block_enabled.store(true, Ordering::SeqCst);

        let worker = std::thread::Builder::new()
            .name("dispatcher-heartbeat-timeout".to_string())
            .spawn({
                let dispatcher = dispatcher.clone();
                move || dispatcher.heartbeat(60_000)
            })
            .expect("heartbeat timeout worker should spawn");

        entered_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("heartbeat publish should enter metadata");

        let started = std::time::Instant::now();
        let shared = dispatcher
            .run(|_client| Ok::<_, StoreError>(11usize))
            .expect("shared request should proceed while heartbeat publish is stuck");
        assert_eq!(shared, 11);
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "shared request should not wait for stuck heartbeat publish"
        );

        let error = worker
            .join()
            .expect("heartbeat timeout worker should join")
            .expect_err("stuck heartbeat should time out");
        assert!(
            matches!(error, StoreError::Transport(ref message) if message.contains("heartbeat publish timed out")),
            "unexpected error: {error}"
        );

        let failed_metrics = snapshot_metrics();
        let runtime = "dispatcher-heartbeat:1".to_string();
        let consecutive_failures = failed_metrics
            .heartbeat_consecutive_failures
            .iter()
            .find(|sample| sample.key.runtime == runtime)
            .map(|sample| sample.value as u64)
            .unwrap_or_default();
        let last_success_ms = failed_metrics
            .heartbeat_last_success_ms
            .iter()
            .find(|sample| sample.key.runtime == runtime)
            .map(|sample| sample.value as u64)
            .unwrap_or_default();
        assert_eq!(consecutive_failures, 1);
        assert_eq!(last_success_ms, 0);

        let inflight = dispatcher
            .heartbeat(60_000)
            .expect_err("second heartbeat should see the first publish still in flight");
        assert!(
            matches!(inflight, StoreError::Transport(ref message) if message.contains("still in flight")),
            "unexpected in-flight error: {inflight}"
        );

        release.store(true, Ordering::SeqCst);
        for _ in 0..50 {
            if dispatcher.heartbeat(60_000).is_ok() {
                let recovered_metrics = snapshot_metrics();
                let consecutive_failures = recovered_metrics
                    .heartbeat_consecutive_failures
                    .iter()
                    .find(|sample| sample.key.runtime == runtime)
                    .map(|sample| sample.value as u64)
                    .unwrap_or_default();
                let last_success_ms = recovered_metrics
                    .heartbeat_last_success_ms
                    .iter()
                    .find(|sample| sample.key.runtime == runtime)
                    .map(|sample| sample.value as u64)
                    .unwrap_or_default();
                assert_eq!(consecutive_failures, 0);
                assert!(last_success_ms > 0);
                return;
            }
            sleep(Duration::from_millis(20));
        }
        panic!("heartbeat should recover after the blocked publish is released");
    }

    #[test]
    fn dispatcher_heartbeat_recovery_republishes_local_segments() {
        let metadata = Arc::new(RecoveryCountingMetadata::new());
        let transport = Arc::new(TestTransport::new("dispatcher-recovery-segment"));
        let client = match StoreClientBuilder::new(metadata.clone(), "dispatcher-recovery")
            .epoch(ClientEpoch(1))
            .state(ClientLifecycleState::Active)
            .compatibility(CompatibilityDescriptor::default())
            .route_control(RouteControlMode::MetadataOnly)
            .transport(transport.clone())
            .local_memory(
                LocalMemoryConfig::new()
                    .numa_aware(false)
                    .storage_bytes(4 * 1024)
                    .scratch_bytes(4 * 1024)
                    .alignment(1)
                    .reclaim_grace_ms(0),
            )
            .build(60_000)
        {
            Ok(client) => client,
            Err(StoreError::Transport(message)) if message.contains("Operation not permitted") => {
                return;
            }
            Err(error) => panic!("dispatcher recovery client should build: {error}"),
        };
        let dispatcher = StoreDispatcher::spawn(client, "dispatcher-recovery".to_string())
            .expect("dispatcher should spawn");
        dispatcher
            .register_local_memory()
            .expect("local memory should register");
        let initial_publish_calls = metadata.publish_segment_calls();
        assert!(initial_publish_calls >= 1);

        metadata.fail_next_lease_upserts(2);
        assert!(dispatcher.heartbeat(60_000).is_err());
        assert!(dispatcher.heartbeat(60_000).is_err());
        let before_repair = metadata.publish_segment_calls();

        dispatcher
            .heartbeat(60_000)
            .expect("recovered heartbeat should repair local segment metadata");
        assert!(
            metadata.publish_segment_calls() > before_repair,
            "recovered dispatcher heartbeat should republish local segments"
        );
    }

    #[test]
    fn dispatcher_state_update_timeout_does_not_block_shared_requests() {
        let (entered_tx, entered_rx) = std::sync::mpsc::channel();
        let release = Arc::new(AtomicBool::new(false));
        let metadata = Arc::new(BlockingHealthMetadata::new(
            Arc::new(AtomicBool::new(false)),
            Arc::new(AtomicBool::new(true)),
            release.clone(),
            entered_tx,
        ));
        let dispatcher = Arc::new(
            StoreDispatcher::spawn_with_heartbeat_timeout(
                build_client_with_metadata("dispatcher-activate", metadata),
                "dispatcher-activate".to_string(),
                Duration::from_millis(200),
            )
            .expect("dispatcher should spawn"),
        );

        let worker = std::thread::Builder::new()
            .name("dispatcher-state-update-timeout".to_string())
            .spawn({
                let dispatcher = dispatcher.clone();
                move || dispatcher.activate()
            })
            .expect("state update timeout worker should spawn");

        entered_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("state update publish should enter metadata");

        let started = std::time::Instant::now();
        let shared = dispatcher
            .run(|_client| Ok::<_, StoreError>(13usize))
            .expect("shared request should proceed while state update publish is stuck");
        assert_eq!(shared, 13);
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "shared request should not wait for stuck state update"
        );

        let error = worker
            .join()
            .expect("state update timeout worker should join")
            .expect_err("stuck state update should time out");
        assert!(
            matches!(error, StoreError::Transport(ref message) if message.contains("state update publish timed out")),
            "unexpected error: {error}"
        );

        let inflight = dispatcher
            .enter_draining()
            .expect_err("second health update should see the first publish still in flight");
        assert!(
            matches!(inflight, StoreError::Transport(ref message) if message.contains("still in flight")),
            "unexpected in-flight error: {inflight}"
        );

        release.store(true, Ordering::SeqCst);
        for _ in 0..50 {
            if dispatcher.enter_draining().is_ok() {
                return;
            }
            sleep(Duration::from_millis(20));
        }
        panic!("state update should recover after the blocked publish is released");
    }
}
