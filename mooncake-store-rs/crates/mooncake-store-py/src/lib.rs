mod config;
pub mod dispatcher;
mod dummy_client;
pub mod dummy_service;
pub mod runtime;
mod shm;

use std::collections::BTreeMap;
use std::ffi::c_void;
use std::slice;

use dispatcher::StoreDispatcher;
use dummy_client::DummySession;
use mooncake_store_client::{
    init_tracing as init_store_tracing, metrics_http_server_addr, render_prometheus_metrics,
    start_metrics_http_server, stop_metrics_http_server, GetRequest, MooncakeCompatibilityFacade,
    MultiBufferGetRequest, MultiBufferPutRequest, ObjectRef, PutFromRequest, PutRequest,
    ReplicationPolicy, RouteControlMode,
};
use mooncake_store_core::{ObjectRoute, SegmentAnnouncement, SegmentName, StoreError};
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
            Self::Real(dispatcher) => dispatcher.shutdown(),
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
        tenant = "default",
        labels = None,
        routed_writes = false,
        replica_count = 1,
        keyspace = None,
        transport_metadata_url = None,
        local_segment_name = None,
        expires_at_ms = None,
        use_hugepage = None,
        hugepage_size = None
    ))]
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
        tenant: &str,
        labels: Option<BTreeMap<String, String>>,
        routed_writes: bool,
        replica_count: usize,
        keyspace: Option<String>,
        transport_metadata_url: Option<String>,
        local_segment_name: Option<String>,
        expires_at_ms: Option<u64>,
        use_hugepage: Option<bool>,
        hugepage_size: Option<usize>,
    ) -> PyResult<i32> {
        let runtime = CompatRuntimeArgs {
            setup: config::CompatSetupArgs {
                local_hostname: local_hostname.to_string(),
                metadata_url: metadata_url.to_string(),
                transport_metadata_url,
                global_segment_size,
                local_buffer_size,
                protocol: protocol.to_string(),
                _rdma_devices: rdma_devices.to_string(),
                stable_id,
                tenant: tenant.to_string(),
                labels: labels.unwrap_or_default(),
                routed_writes,
                replica_count,
                keyspace,
                expires_at_ms,
                use_hugepage,
                hugepage_size_bytes: hugepage_size,
            },
            local_segment_name,
            route_control: RouteControlMode::EmbeddedWrh,
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
            StoreBackend::Real(dispatcher) => {
                let key = key.to_string();
                let tenant = tenant.map(str::to_string);
                dispatcher
                    .run(move |client| match tenant.as_deref() {
                        Some(tenant) => client.get_in_tenant(tenant, &key),
                        None => client.get(&key),
                    })
                    .map_err(store_error_to_py)?
            }
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
                    .map_err(store_error_to_py)
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
                let key = key.to_string();
                let tenant = tenant.map(str::to_string);
                dispatcher
                    .run(move |client| {
                        let buffer =
                            unsafe { slice::from_raw_parts_mut(buffer_ptr as *mut u8, size) };
                        match tenant.as_deref() {
                            Some(tenant) => client.get_into_in_tenant(tenant, &key, buffer),
                            None => client.get_into(&key, buffer),
                        }
                    })
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
                Ok(vec![0; key_count])
            }
        }
    }

    #[pyo3(signature = (key, force = false, *, tenant = None))]
    fn remove(&self, key: &str, force: bool, tenant: Option<&str>) -> PyResult<i32> {
        match self.real_dispatcher()? {
            dispatcher => {
                let key = key.to_string();
                let tenant = tenant.map(str::to_string);
                dispatcher
                    .run(move |client| match tenant.as_deref() {
                        Some(tenant) => client.remove_in_tenant(tenant, &key, force),
                        None => client.remove(&key, force),
                    })
                    .map_err(store_error_to_py)?;
                Ok(0)
            }
        }
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
        let tenant = tenant.map(str::to_string);
        let values = dispatcher
            .run(move |client| {
                let objects = keys
                    .iter()
                    .map(|key| {
                        let mut object = ObjectRef::new(key);
                        if let Some(tenant) = tenant.as_deref() {
                            object = object.tenant(tenant);
                        }
                        object
                    })
                    .collect::<Vec<_>>();
                client.batch_get(&objects)
            })
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
                let tenant = tenant.map(str::to_string);
                dispatcher
                    .run(move |client| {
                        let mut buffers = items
                            .iter()
                            .map(|(_, buffer_ptr, size)| unsafe {
                                slice::from_raw_parts_mut(*buffer_ptr as *mut u8, *size)
                            })
                            .collect::<Vec<_>>();
                        let mut requests = items
                            .iter()
                            .zip(buffers.iter_mut())
                            .map(|((key, _, _), buffer)| {
                                let mut request = GetRequest::new(key, buffer);
                                if let Some(tenant) = tenant.as_deref() {
                                    request = request.tenant(tenant);
                                }
                                request
                            })
                            .collect::<Vec<_>>();
                        client
                            .batch_get_into(&mut requests)
                            .map(|sizes| sizes.into_iter().map(|size| size as i64).collect())
                    })
                    .map_err(store_error_to_py)
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
    ) -> PyResult<Vec<usize>> {
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
                let tenant = tenant.map(str::to_string);
                dispatcher
                    .run(move |client| {
                        let mut borrowed = all_buffer_ptrs
                            .iter()
                            .zip(all_sizes.iter())
                            .map(|(buffer_ptrs, sizes)| {
                                buffer_ptrs
                                    .iter()
                                    .zip(sizes.iter())
                                    .map(|(buffer_ptr, size)| unsafe {
                                        slice::from_raw_parts_mut(*buffer_ptr as *mut u8, *size)
                                    })
                                    .collect::<Vec<_>>()
                            })
                            .collect::<Vec<_>>();

                        let mut requests = keys
                            .iter()
                            .zip(borrowed.iter_mut())
                            .map(|(key, buffers)| {
                                let mut request =
                                    MultiBufferGetRequest::new(key, buffers.as_mut_slice());
                                if let Some(tenant) = tenant.as_deref() {
                                    request = request.tenant(tenant);
                                }
                                request
                            })
                            .collect::<Vec<_>>();
                        client.batch_get_into_multi_buffers(&mut requests)
                    })
                    .map_err(store_error_to_py)
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
