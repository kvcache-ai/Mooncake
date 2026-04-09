mod config;

use std::collections::BTreeMap;
use std::ffi::c_void;
use std::slice;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use config::CompatSetupArgs;
use mooncake_store_client::{
    init_tracing as init_store_tracing, metrics_http_server_addr, render_prometheus_metrics,
    start_metrics_http_server, stop_metrics_http_server, GetRequest, LocalMemoryConfig,
    MooncakeCompatibilityFacade, MultiBufferGetRequest, MultiBufferPutRequest, ObjectRef,
    PlacementPlanner, PutFromRequest, PutRequest, ReplicationPolicy, StoreClient,
    StoreClientBuilder, TentTransportFactory,
};
use mooncake_store_core::{
    ClientEpoch, ClientLifecycleState, CompatibilityDescriptor, ObjectRoute, SegmentAnnouncement,
    SegmentName, StoreError,
};
use mooncake_transport::TentEngine;
use pyo3::exceptions::{PyKeyError, PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList};

#[pyclass(name = "MooncakeDistributedStore", unsendable)]
struct PyMooncakeDistributedStore {
    client: Option<StoreClient>,
}

#[pymethods]
impl PyMooncakeDistributedStore {
    #[new]
    fn new() -> Self {
        Self { client: None }
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
        expires_at_ms = None
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
    ) -> PyResult<i32> {
        let plan = CompatSetupArgs {
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
        }
        .build()
        .map_err(store_error_to_py)?;
        let _ = master_server;

        let metadata = plan.metadata.clone();
        let segment_name = local_segment_name.unwrap_or_else(|| {
            format!("{}-segment-{}", plan.stable_id.replace(':', "-"), now_ms())
        });
        #[allow(clippy::arc_with_non_send_sync)]
        let engine = Arc::new(
            TentEngine::new(
                &plan
                    .tent_config
                    .clone()
                    .set("local_segment_name", &segment_name),
            )
            .map_err(store_error_to_py)?,
        );
        let factory = Arc::new(TentTransportFactory::new(plan.tent_config));
        let mut builder = StoreClientBuilder::new(plan.metadata, plan.stable_id)
            .epoch(ClientEpoch(1))
            .state(ClientLifecycleState::Active)
            .compatibility(CompatibilityDescriptor::default())
            .tenant(plan.tenant)
            .local_memory(
                LocalMemoryConfig::new()
                    .storage_bytes(plan.storage_bytes)
                    .scratch_bytes(plan.scratch_bytes)
                    .location("cpu:0"),
            )
            .with_tent(engine)
            .transport_factory(factory);

        for (key, value) in plan.labels {
            builder = builder.label(key, value);
        }
        if plan.routed_writes {
            builder = builder.routed_writes(
                PlacementPlanner::new(metadata).require_label("storage", "true"),
                plan.replica_count,
            );
        }

        let client = builder
            .build(plan.expires_at_ms)
            .map_err(store_error_to_py)?;
        client.register_local_memory().map_err(store_error_to_py)?;
        self.client = Some(client);
        Ok(0)
    }

    fn close(&mut self) {
        self.client = None;
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
        let client = self.client_ref()?;
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
        match (tenant, policy.as_ref()) {
            (Some(tenant), Some(policy)) => {
                client.put_in_tenant_with_policy(tenant, key, &value, policy)
            }
            (None, Some(policy)) => client.put_with_policy(key, &value, policy),
            (Some(tenant), None) => client.put_in_tenant(tenant, key, &value),
            (None, None) => client.put(key, &value),
        }
        .map_err(store_error_to_py)?;
        Ok(0)
    }

    #[pyo3(signature = (key, *, tenant = None))]
    fn get<'py>(
        &self,
        py: Python<'py>,
        key: &str,
        tenant: Option<&str>,
    ) -> PyResult<Bound<'py, PyBytes>> {
        let client = self.client_ref()?;
        let value = match tenant {
            Some(tenant) => client.get_in_tenant(tenant, key),
            None => client.get(key),
        }
        .map_err(store_error_to_py)?;
        Ok(PyBytes::new(py, &value))
    }

    #[pyo3(signature = (key, *, tenant = None))]
    fn is_exist(&self, key: &str, tenant: Option<&str>) -> PyResult<bool> {
        let client = self.client_ref()?;
        match tenant {
            Some(tenant) => client.is_exist_in_tenant(tenant, key),
            None => client.is_exist(key),
        }
        .map_err(store_error_to_py)
    }

    #[pyo3(signature = (keys, *, tenant = None))]
    fn batch_is_exist(&self, keys: Vec<String>, tenant: Option<&str>) -> PyResult<Vec<i32>> {
        let client = self.client_ref()?;
        let objects = keys
            .iter()
            .map(|key| {
                let mut object = ObjectRef::new(key.as_str());
                if let Some(tenant) = tenant {
                    object = object.tenant(tenant);
                }
                object
            })
            .collect::<Vec<_>>();
        client
            .batch_is_exist(&objects)
            .map(|items| items.into_iter().map(i32::from).collect())
            .map_err(store_error_to_py)
    }

    fn get_hostname(&self) -> PyResult<String> {
        self.client_ref()?.get_hostname().map_err(store_error_to_py)
    }

    #[pyo3(signature = (key, *, tenant = None))]
    fn get_size(&self, key: &str, tenant: Option<&str>) -> PyResult<usize> {
        let client = self.client_ref()?;
        match tenant {
            Some(tenant) => client.get_size_in_tenant(tenant, key),
            None => client.get_size(key),
        }
        .map_err(store_error_to_py)
    }

    fn register_buffer(&self, buffer_ptr: usize, size: usize) -> PyResult<i32> {
        let client = self.client_ref()?;
        client
            .register_buffer(pointer_from_usize(buffer_ptr)?, size)
            .map_err(store_error_to_py)?;
        Ok(0)
    }

    fn unregister_buffer(&self, buffer_ptr: usize, size: usize) -> PyResult<i32> {
        let client = self.client_ref()?;
        client
            .unregister_buffer(pointer_from_usize(buffer_ptr)?, size)
            .map_err(store_error_to_py)?;
        Ok(0)
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
        let client = self.client_ref()?;
        let buffer = pointer_from_usize(buffer_ptr)? as *const c_void;
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
        match (tenant, policy.as_ref()) {
            (Some(tenant), Some(policy)) => {
                client.put_from_in_tenant_with_policy(tenant, key, buffer, size, policy)
            }
            (None, Some(policy)) => client.put_from_with_policy(key, buffer, size, policy),
            (Some(tenant), None) => client.put_from_in_tenant(tenant, key, buffer, size),
            (None, None) => client.put_from(key, buffer, size),
        }
        .map_err(store_error_to_py)?;
        Ok(0)
    }

    #[pyo3(signature = (key, buffer_ptr, size, *, tenant = None))]
    fn get_into(
        &self,
        key: &str,
        buffer_ptr: usize,
        size: usize,
        tenant: Option<&str>,
    ) -> PyResult<usize> {
        let client = self.client_ref()?;
        let buffer = unsafe {
            slice::from_raw_parts_mut(pointer_from_usize(buffer_ptr)?.cast::<u8>(), size)
        };
        match tenant {
            Some(tenant) => client.get_into_in_tenant(tenant, key, buffer),
            None => client.get_into(key, buffer),
        }
        .map_err(store_error_to_py)
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
        let client = self.client_ref()?;
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
        let requests = items
            .iter()
            .map(|(key, value)| {
                let mut request = PutRequest::new(key, value);
                if let Some(tenant) = tenant {
                    request = request.tenant(tenant);
                }
                if let Some(policy) = policy.clone() {
                    request = request.replication(policy);
                }
                request
            })
            .collect::<Vec<_>>();
        client.batch_put(&requests).map_err(store_error_to_py)?;
        Ok(0)
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
    ) -> PyResult<i32> {
        let client = self.client_ref()?;
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
        let requests = items
            .iter()
            .map(|(key, buffer_ptr, size)| {
                let mut request =
                    PutFromRequest::new(key, (*buffer_ptr as *mut c_void).cast_const(), *size);
                if let Some(tenant) = tenant {
                    request = request.tenant(tenant);
                }
                if let Some(policy) = policy.clone() {
                    request = request.replication(policy);
                }
                request
            })
            .collect::<Vec<_>>();
        client
            .batch_put_from(&requests)
            .map_err(store_error_to_py)?;
        Ok(0)
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
    ) -> PyResult<i32> {
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
        let client = self.client_ref()?;
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
                let mut request = MultiBufferPutRequest::new(key, slices.as_slice());
                if let Some(tenant) = tenant {
                    request = request.tenant(tenant);
                }
                if let Some(policy) = policy.clone() {
                    request = request.replication(policy);
                }
                request
            })
            .collect::<Vec<_>>();
        client
            .batch_put_from_multi_buffers(&requests)
            .map_err(store_error_to_py)?;
        Ok(0)
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
        let client = self.client_ref()?;
        if keys.len() != all_buffer_ptrs.len() || keys.len() != all_sizes.len() {
            return Err(PyValueError::new_err(
                "keys, all_buffer_ptrs, and all_sizes must have the same length",
            ));
        }

        let borrowed = all_buffer_ptrs
            .iter()
            .zip(all_sizes.iter())
            .map(|(buffer_ptrs, sizes)| {
                if buffer_ptrs.len() != sizes.len() {
                    return Err(PyValueError::new_err(
                        "each multi-buffer pointer group must match its sizes group",
                    ));
                }
                buffer_ptrs
                    .iter()
                    .zip(sizes.iter())
                    .map(|(buffer_ptr, size)| unsafe {
                        Ok(slice::from_raw_parts(
                            pointer_from_usize(*buffer_ptr)?.cast::<u8>(),
                            *size,
                        ))
                    })
                    .collect::<PyResult<Vec<_>>>()
            })
            .collect::<PyResult<Vec<_>>>()?;
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

        let requests = keys
            .iter()
            .zip(borrowed.iter())
            .map(|(key, buffers)| {
                let mut request = MultiBufferPutRequest::new(key, buffers.as_slice());
                if let Some(tenant) = tenant {
                    request = request.tenant(tenant);
                }
                if let Some(policy) = policy.clone() {
                    request = request.replication(policy);
                }
                request
            })
            .collect::<Vec<_>>();
        client
            .batch_put_from_multi_buffers(&requests)
            .map_err(store_error_to_py)?;
        Ok(vec![0; keys.len()])
    }

    #[pyo3(signature = (key, force = false, *, tenant = None))]
    fn remove(&self, key: &str, force: bool, tenant: Option<&str>) -> PyResult<i32> {
        let client = self.client_ref()?;
        match tenant {
            Some(tenant) => client.remove_in_tenant(tenant, key, force),
            None => client.remove(key, force),
        }
        .map_err(store_error_to_py)?;
        Ok(0)
    }

    #[pyo3(signature = (keys, force = false, *, tenant = None))]
    fn batch_remove(
        &self,
        keys: Vec<String>,
        force: bool,
        tenant: Option<&str>,
    ) -> PyResult<Vec<i32>> {
        let client = self.client_ref()?;
        let objects = keys
            .iter()
            .map(|key| {
                let mut object = ObjectRef::new(key.as_str());
                if let Some(tenant) = tenant {
                    object = object.tenant(tenant);
                }
                object
            })
            .collect::<Vec<_>>();
        client
            .batch_remove(&objects, force)
            .map_err(store_error_to_py)?;
        Ok(vec![0; keys.len()])
    }

    #[pyo3(signature = (keys, *, tenant = None))]
    fn batch_get<'py>(
        &self,
        py: Python<'py>,
        keys: Vec<String>,
        tenant: Option<&str>,
    ) -> PyResult<Vec<Py<PyBytes>>> {
        let client = self.client_ref()?;
        let objects = keys
            .iter()
            .map(|key| {
                let mut object = ObjectRef::new(key);
                if let Some(tenant) = tenant {
                    object = object.tenant(tenant);
                }
                object
            })
            .collect::<Vec<_>>();
        let values = client.batch_get(&objects).map_err(store_error_to_py)?;
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
    ) -> PyResult<Vec<usize>> {
        let client = self.client_ref()?;
        let mut buffers = items
            .iter()
            .map(|(_, buffer_ptr, size)| unsafe {
                slice::from_raw_parts_mut(
                    pointer_from_usize(*buffer_ptr)
                        .expect("pointer must be valid")
                        .cast::<u8>(),
                    *size,
                )
            })
            .collect::<Vec<_>>();
        let mut requests = items
            .iter()
            .zip(buffers.iter_mut())
            .map(|((key, _, _), buffer)| {
                let mut request = GetRequest::new(key, buffer);
                if let Some(tenant) = tenant {
                    request = request.tenant(tenant);
                }
                request
            })
            .collect::<Vec<_>>();
        client
            .batch_get_into(&mut requests)
            .map_err(store_error_to_py)
    }

    #[pyo3(signature = (keys, buffer_ptrs, sizes, *, tenant = None))]
    fn batch_get_into_raw(
        &self,
        keys: Vec<String>,
        buffer_ptrs: Vec<usize>,
        sizes: Vec<usize>,
        tenant: Option<&str>,
    ) -> PyResult<Vec<usize>> {
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
        let client = self.client_ref()?;
        if keys.len() != all_buffer_ptrs.len() || keys.len() != all_sizes.len() {
            return Err(PyValueError::new_err(
                "keys, all_buffer_ptrs, and all_sizes must have the same length",
            ));
        }

        let mut borrowed = all_buffer_ptrs
            .iter()
            .zip(all_sizes.iter())
            .map(|(buffer_ptrs, sizes)| {
                if buffer_ptrs.len() != sizes.len() {
                    return Err(PyValueError::new_err(
                        "each multi-buffer pointer group must match its sizes group",
                    ));
                }
                buffer_ptrs
                    .iter()
                    .zip(sizes.iter())
                    .map(|(buffer_ptr, size)| unsafe {
                        Ok(slice::from_raw_parts_mut(
                            pointer_from_usize(*buffer_ptr)?.cast::<u8>(),
                            *size,
                        ))
                    })
                    .collect::<PyResult<Vec<_>>>()
            })
            .collect::<PyResult<Vec<_>>>()?;

        let mut requests = keys
            .iter()
            .zip(borrowed.iter_mut())
            .map(|(key, buffers)| {
                let mut request = MultiBufferGetRequest::new(key, buffers.as_mut_slice());
                if let Some(tenant) = tenant {
                    request = request.tenant(tenant);
                }
                request
            })
            .collect::<Vec<_>>();
        client
            .batch_get_into_multi_buffers(&mut requests)
            .map_err(store_error_to_py)
    }

    #[pyo3(signature = (storage_bytes))]
    fn expand_local_memory<'py>(
        &self,
        py: Python<'py>,
        storage_bytes: usize,
    ) -> PyResult<Py<PyAny>> {
        let client = self.client_ref()?;
        let announcement = client
            .expand_local_memory(storage_bytes)
            .map_err(store_error_to_py)?;
        segment_to_py(py, &announcement)
    }

    fn drain_segment(&self, segment_name: &str) -> PyResult<i32> {
        let client = self.client_ref()?;
        client
            .drain_segment(&SegmentName::new(segment_name))
            .map_err(store_error_to_py)?;
        Ok(0)
    }

    fn retire_segment(&self, segment_name: &str) -> PyResult<bool> {
        let client = self.client_ref()?;
        client
            .retire_segment(&SegmentName::new(segment_name))
            .map_err(store_error_to_py)
    }

    fn list_segments<'py>(&self, py: Python<'py>) -> PyResult<Vec<Py<PyAny>>> {
        let client = self.client_ref()?;
        client
            .list_segments()
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
        let client = self.client_ref()?;
        let route = match tenant {
            Some(tenant) => client.query_route_in_tenant(tenant, key),
            None => client.query_route(key),
        }
        .map_err(store_error_to_py)?;
        route.map(|route| route_to_py(py, &route)).transpose()
    }

    fn heartbeat(&mut self, expires_at_ms: u64) -> PyResult<i32> {
        let client = self.client_mut()?;
        client.heartbeat(expires_at_ms).map_err(store_error_to_py)?;
        Ok(0)
    }

    fn activate(&mut self) -> PyResult<i32> {
        let client = self.client_mut()?;
        client.activate().map_err(store_error_to_py)?;
        Ok(0)
    }

    fn enter_standby(&mut self) -> PyResult<i32> {
        let client = self.client_mut()?;
        client.enter_standby().map_err(store_error_to_py)?;
        Ok(0)
    }

    fn enter_draining(&mut self) -> PyResult<i32> {
        let client = self.client_mut()?;
        client.enter_draining().map_err(store_error_to_py)?;
        Ok(0)
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
    module.add_function(wrap_pyfunction!(init_tracing, module)?)?;
    module.add_function(wrap_pyfunction!(metrics_text, module)?)?;
    module.add_function(wrap_pyfunction!(start_metrics_server, module)?)?;
    module.add_function(wrap_pyfunction!(stop_metrics_server, module)?)?;
    module.add_function(wrap_pyfunction!(metrics_server_address, module)?)?;
    Ok(())
}

impl PyMooncakeDistributedStore {
    fn client_ref(&self) -> PyResult<&StoreClient> {
        self.client
            .as_ref()
            .ok_or_else(|| PyRuntimeError::new_err("store is not set up"))
    }

    fn client_mut(&mut self) -> PyResult<&mut StoreClient> {
        self.client
            .as_mut()
            .ok_or_else(|| PyRuntimeError::new_err("store is not set up"))
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

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time should be monotonic")
        .as_millis() as u64
}
