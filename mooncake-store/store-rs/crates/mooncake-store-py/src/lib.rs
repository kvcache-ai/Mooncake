pub mod admin;
pub mod buffer_pool;
pub mod build_info;
pub mod config;
pub mod dispatcher;
mod dummy_client;
pub mod dummy_service;
mod hot_cache;
pub mod runtime;
mod shm;
pub mod tensor;
pub mod tensor_parallel;
#[cfg(test)]
mod test_support;

use std::collections::BTreeMap;
use std::ffi::c_void;
use std::ops::Range;
use std::slice;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::Instant;

use dispatcher::{CompatNamespaceScope, CompatObjectScope, StoreDispatcher};
use dummy_client::DummySession;
use mooncake_store_client::{
    init_tracing as init_store_tracing, metrics_http_server_addr, render_prometheus_metrics,
    start_metrics_http_server, stop_metrics_http_server, MooncakeCompatibilityFacade,
    MultiBufferPutRequest, ObjectRef, OperationTracker, PutFromRequest, PutRequest,
    ReadQueryResultCache, ReplicationPolicy, RouteControlMode,
};
use mooncake_store_core::{
    ClientLifecycleState, NamespaceScope, ObjectRoute, SegmentAnnouncement, SegmentName, StoreError,
};
use pyo3::exceptions::{PyKeyError, PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList};
use runtime::{CompatRuntimeArgs, CompatTimeoutConfig};

enum StoreBackend {
    Real(StoreDispatcher),
    Dummy(DummySession),
}

fn finalize_real_dispatcher_setup(
    dispatcher: &StoreDispatcher,
    initial_state: ClientLifecycleState,
    lease_ttl_ms: u64,
) -> Result<(), StoreError> {
    dispatcher.register_local_memory()?;
    if initial_state == ClientLifecycleState::Active {
        dispatcher.activate()?;
    }
    dispatcher.start_heartbeat_loop(lease_ttl_ms, None)?;
    Ok(())
}

pub const DEFAULT_COMPAT_WORKER_SCOPE: &str = "worker-1";
const BATCH_PUT_FROM_FANOUT_ENV: &str = "MC_STORE_RS_PY_BATCH_PUT_FROM_FANOUT";
const DEFAULT_BATCH_PUT_FROM_FANOUT_WIDTH: usize = 1;

fn resolve_real_worker_scope(keyspace: Option<&str>, worker_scope: Option<&str>) -> String {
    if let Some(scope) = worker_scope
        .map(str::trim)
        .filter(|scope| !scope.is_empty())
    {
        return scope.to_string();
    }
    if let Some(keyspace) = keyspace
        .map(str::trim)
        .filter(|keyspace| !keyspace.is_empty())
    {
        return keyspace.to_string();
    }
    static NEXT_WORKER_SCOPE_ID: AtomicU64 = AtomicU64::new(1);
    let id = NEXT_WORKER_SCOPE_ID.fetch_add(1, Ordering::Relaxed);
    if id == 1 {
        return DEFAULT_COMPAT_WORKER_SCOPE.to_string();
    }
    format!("worker-{id}")
}

fn resolve_dummy_worker_scope(keyspace: Option<&str>, worker_scope: Option<&str>) -> String {
    if let Some(scope) = worker_scope
        .map(str::trim)
        .filter(|scope| !scope.is_empty())
    {
        return scope.to_string();
    }
    if let Some(keyspace) = keyspace
        .map(str::trim)
        .filter(|keyspace| !keyspace.is_empty())
    {
        return keyspace.to_string();
    }
    DEFAULT_COMPAT_WORKER_SCOPE.to_string()
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
    /// Positional layout:
    ///   `setup(local_hostname, transport_metadata_url, global_segment_size,
    ///          local_buffer_size, protocol, rdma_devices, metadata_url)`
    ///
    /// - `transport_metadata_url` is forwarded to the Transfer Engine only. Accepts
    ///   `redis://...` or `P2PHANDSHAKE`.
    /// - `metadata_url` is the Store-RS metadata URL. Accepts `redis://...` or
    ///   `etcd://...` and is required.
    ///
    /// Tenant-scoped routing and resource policy should be authored through
    /// `mooncake-store-admin policy ...` and durable metadata. `route_topk` is
    /// accepted here as a compatibility/bootstrap fallback (tenant policy overrides it).
    /// `route_control` is a cluster-level deployment setting (not per-tenant).
    #[pyo3(signature = (
        local_hostname,
        transport_metadata_url,
        global_segment_size,
        local_buffer_size,
        protocol = "tcp",
        rdma_devices = "",
        metadata_url = "",
        *,
        stable_id = None,
        initial_state = "active",
        tenant = "default",
        domain = None,
        object_set = None,
        labels = None,
        routed_writes = false,
        replica_count = 1,
        route_topk = 2,
        keyspace = None,
        worker_scope = None,
        transport_rpc_port = None,
        transport_backend = None,
        local_segment_name = None,
        expires_at_ms = None,
        use_hugepage = None,
        hugepage_size = None,
        eviction_high_watermark_percent = None,
        eviction_low_watermark_percent = None,
        route_control = "embedded_wrh"
    ), text_signature = "(local_hostname, transport_metadata_url, global_segment_size, local_buffer_size, protocol='tcp', rdma_devices='', metadata_url='', *, stable_id=None, initial_state='active', tenant='default', domain=None, object_set=None, labels=None, routed_writes=False, replica_count=1, route_topk=2, keyspace=None, worker_scope=None, transport_rpc_port=None, transport_backend=None, local_segment_name=None, expires_at_ms=None, use_hugepage=None, hugepage_size=None, eviction_high_watermark_percent=None, eviction_low_watermark_percent=None, route_control='embedded_wrh')")]
    #[allow(clippy::too_many_arguments)]
    fn setup(
        &mut self,
        local_hostname: &str,
        transport_metadata_url: &str,
        global_segment_size: usize,
        local_buffer_size: usize,
        protocol: &str,
        rdma_devices: &str,
        metadata_url: &str,
        stable_id: Option<String>,
        initial_state: &str,
        tenant: &str,
        domain: Option<String>,
        object_set: Option<String>,
        labels: Option<BTreeMap<String, String>>,
        routed_writes: bool,
        replica_count: usize,
        route_topk: usize,
        keyspace: Option<String>,
        worker_scope: Option<String>,
        transport_rpc_port: Option<u16>,
        transport_backend: Option<String>,
        local_segment_name: Option<String>,
        expires_at_ms: Option<u64>,
        use_hugepage: Option<bool>,
        hugepage_size: Option<usize>,
        eviction_high_watermark_percent: Option<u8>,
        eviction_low_watermark_percent: Option<u8>,
        route_control: &str,
    ) -> PyResult<i32> {
        let initial_state = parse_initial_state_arg(initial_state)?;
        let route_control = parse_route_control_arg(route_control)?;
        let worker_scope = resolve_real_worker_scope(keyspace.as_deref(), worker_scope.as_deref());
        let compat_scope = keyspace.clone().unwrap_or_else(|| "default".to_string());
        let runtime = CompatRuntimeArgs {
            setup: config::CompatSetupArgs {
                local_hostname: local_hostname.to_string(),
                transport_metadata_url: transport_metadata_url.to_string(),
                metadata_url: metadata_url.to_string(),
                global_segment_size,
                local_buffer_size,
                eviction_high_watermark_percent,
                eviction_low_watermark_percent,
                protocol: protocol.to_string(),
                _rdma_devices: rdma_devices.to_string(),
                transport_rpc_port,
                transport_backend,
                stable_id,
                tenant: tenant.to_string(),
                domain: domain.clone(),
                object_set: object_set.clone(),
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
            initial_state,
            route_control,
        }
        .build()
        .map_err(store_error_to_py)?;
        let stable_id = runtime.stable_id.clone();
        let lease_ttl_ms = runtime.lease_ttl_ms;
        let default_scope = CompatNamespaceScope::new(tenant.to_string(), domain, object_set);
        let dispatcher = StoreDispatcher::spawn_with_timeout_config_and_namespace_scope(
            runtime.client,
            format!("mooncake-py-dispatcher-{stable_id}-{worker_scope}"),
            CompatTimeoutConfig::from_env(),
            compat_scope,
            default_scope,
        )
        .map_err(store_error_to_py)?;
        finalize_real_dispatcher_setup(&dispatcher, initial_state, lease_ttl_ms)
            .map_err(store_error_to_py)?;
        self.replace_backend(StoreBackend::Real(dispatcher));
        Ok(0)
    }

    #[pyo3(signature = (mem_pool_size, local_buffer_size, server_address, *, keyspace = None, worker_scope = None))]
    fn setup_dummy(
        &mut self,
        mem_pool_size: usize,
        local_buffer_size: usize,
        server_address: &str,
        keyspace: Option<String>,
        worker_scope: Option<String>,
    ) -> PyResult<i32> {
        let _ = (mem_pool_size, local_buffer_size);
        let worker_scope = resolve_dummy_worker_scope(keyspace.as_deref(), worker_scope.as_deref());
        let session =
            DummySession::connect(server_address, worker_scope).map_err(store_error_to_py)?;
        self.replace_backend(StoreBackend::Dummy(session));
        Ok(0)
    }

    fn close(&mut self) {
        if let Some(backend) = self.backend.take() {
            allow_threads_ungil(move || backend.close());
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
            StoreBackend::Dummy(dummy) => {
                run_without_gil(move || dummy.put(key, &value, tenant, policy.as_ref()))
                    .map_err(store_error_to_py)
            }
            StoreBackend::Real(dispatcher) => {
                let key = key.to_string();
                let tenant = tenant.map(str::to_string);
                let cache_key = key.clone();
                let cache_tenant = tenant.clone();
                let scope = dispatcher.object_scope(tenant);
                run_without_gil(move || {
                    dispatcher.run(move |client| {
                        let mut request =
                            PutRequest::new(&key, &value).tenant(scope.tenant.as_str());
                        if scope.domain != mooncake_store_core::DEFAULT_DOMAIN {
                            request = request.domain(scope.domain.as_str());
                        }
                        if scope.object_set != mooncake_store_core::DEFAULT_OBJECT_SET {
                            request = request.object_set(scope.object_set.as_str());
                        }
                        if let Some(policy) = policy {
                            request = request.replication(policy);
                        }
                        client.batch_put(&[request]).map(|_| ())
                    })
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
                let (status, value) =
                    run_without_gil(move || dummy.get(key, tenant)).map_err(store_error_to_py)?;
                if status != 0 {
                    return Err(PyKeyError::new_err(format!(
                        "dummy store get failed for key={key}"
                    )));
                }
                value
            }
            StoreBackend::Real(dispatcher) => run_without_gil(move || {
                dispatcher.get_value(key.to_string(), tenant.map(str::to_string))
            })
            .map_err(store_error_to_py)?,
        };
        Ok(PyBytes::new(py, &value))
    }

    #[pyo3(signature = (key, *, tenant = None))]
    fn is_exist(&self, key: &str, tenant: Option<&str>) -> PyResult<bool> {
        match self.backend_ref()? {
            StoreBackend::Dummy(dummy) => {
                let key = key.to_string();
                let results = run_without_gil(move || dummy.batch_is_exist(&[key], tenant))
                    .map_err(store_error_to_py)?;
                Ok(results.first().copied().unwrap_or_default() == 1)
            }
            StoreBackend::Real(dispatcher) => {
                let key = key.to_string();
                let tenant = tenant.map(str::to_string);
                let scope = dispatcher.object_scope(tenant);
                run_without_gil(move || {
                    dispatcher.run(move |client| {
                        let mut object = ObjectRef::new(key.as_str()).tenant(scope.tenant.as_str());
                        if scope.domain != mooncake_store_core::DEFAULT_DOMAIN {
                            object = object.domain(scope.domain.as_str());
                        }
                        if scope.object_set != mooncake_store_core::DEFAULT_OBJECT_SET {
                            object = object.object_set(scope.object_set.as_str());
                        }
                        client
                            .batch_is_exist(&[object])
                            .map(|items| items.first().copied().unwrap_or(false))
                    })
                })
                .map_err(store_error_to_py)
            }
        }
    }

    #[pyo3(signature = (keys, *, tenant = None))]
    fn batch_is_exist(&self, keys: Vec<String>, tenant: Option<&str>) -> PyResult<Vec<i32>> {
        match self.backend_ref()? {
            StoreBackend::Dummy(dummy) => {
                run_without_gil(move || dummy.batch_is_exist(&keys, tenant))
                    .map_err(store_error_to_py)
            }
            StoreBackend::Real(dispatcher) => {
                let item_count = keys.len();
                let tenant = tenant.map(str::to_string);
                let scope = dispatcher.object_scope(tenant);
                run_without_gil(move || {
                    dispatcher.run(move |client| {
                        let objects = keys
                            .iter()
                            .map(|key| {
                                let mut object =
                                    ObjectRef::new(key.as_str()).tenant(scope.tenant.as_str());
                                if scope.domain != mooncake_store_core::DEFAULT_DOMAIN {
                                    object = object.domain(scope.domain.as_str());
                                }
                                if scope.object_set != mooncake_store_core::DEFAULT_OBJECT_SET {
                                    object = object.object_set(scope.object_set.as_str());
                                }
                                object
                            })
                            .collect::<Vec<_>>();
                        client
                            .batch_is_exist(&objects)
                            .map(|items| items.into_iter().map(i32::from).collect())
                    })
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
            StoreBackend::Real(dispatcher) => {
                run_without_gil(move || dispatcher.run(|client| client.get_hostname()))
                    .map_err(store_error_to_py)
            }
        }
    }

    #[pyo3(signature = (key, *, tenant = None))]
    fn get_size(&self, key: &str, tenant: Option<&str>) -> PyResult<usize> {
        match self.backend_ref()? {
            StoreBackend::Dummy(dummy) => {
                let (status, value) =
                    run_without_gil(move || dummy.get(key, tenant)).map_err(store_error_to_py)?;
                if status != 0 {
                    return Ok(0);
                }
                Ok(value.len())
            }
            StoreBackend::Real(dispatcher) => {
                let key = key.to_string();
                let tenant = tenant.map(str::to_string);
                let scope = dispatcher.object_scope(tenant);
                run_without_gil(move || {
                    dispatcher.run(move |client| {
                        let mut object = ObjectRef::new(key.as_str()).tenant(scope.tenant.as_str());
                        if scope.domain != mooncake_store_core::DEFAULT_DOMAIN {
                            object = object.domain(scope.domain.as_str());
                        }
                        if scope.object_set != mooncake_store_core::DEFAULT_OBJECT_SET {
                            object = object.object_set(scope.object_set.as_str());
                        }
                        client
                            .batch_get(&[object])
                            .map(|values| values.first().map(Vec::len).unwrap_or_default())
                    })
                })
                .or_else(|error| {
                    if should_soft_miss_error(&error) {
                        Ok(0)
                    } else {
                        Err(store_error_to_py(error))
                    }
                })
            }
        }
    }

    fn register_buffer(&self, buffer_ptr: usize, size: usize) -> PyResult<i32> {
        match self.backend_ref()? {
            StoreBackend::Dummy(dummy) => {
                run_without_gil(move || dummy.register_buffer(buffer_ptr, size))
                    .map_err(store_error_to_py)
            }
            StoreBackend::Real(dispatcher) => {
                let _ = pointer_from_usize(buffer_ptr)?;
                run_without_gil(move || dispatcher.register_buffer(buffer_ptr, size))
                    .map_err(store_error_to_py)?;
                Ok(0)
            }
        }
    }

    #[pyo3(signature = (buffer_ptr, size = None))]
    fn unregister_buffer(&self, buffer_ptr: usize, size: Option<usize>) -> PyResult<i32> {
        match self.backend_ref()? {
            StoreBackend::Dummy(dummy) => {
                run_without_gil(move || dummy.unregister_buffer(buffer_ptr, size))
                    .map_err(store_error_to_py)
            }
            StoreBackend::Real(dispatcher) => {
                let _ = pointer_from_usize(buffer_ptr)?;
                run_without_gil(move || dispatcher.unregister_buffer(buffer_ptr, size))
                    .map_err(store_error_to_py)?;
                Ok(0)
            }
        }
    }

    fn local_buffer_pool_capacity(&self) -> PyResult<usize> {
        match self.backend_ref()? {
            StoreBackend::Dummy(_) => Ok(0),
            StoreBackend::Real(dispatcher) => {
                run_without_gil(move || dispatcher.local_scratch_capacity_bytes())
                    .map_err(store_error_to_py)
            }
        }
    }

    fn local_buffer_pool_try_acquire(
        &self,
        size: usize,
    ) -> PyResult<Option<buffer_pool::LocalBufferLease>> {
        match self.backend_ref()? {
            StoreBackend::Dummy(_) => Err(PyRuntimeError::new_err(
                "BufferPool requires a real store configured with a local buffer",
            )),
            StoreBackend::Real(dispatcher) => {
                let reservation = match run_without_gil(move || dispatcher.plan_local_scratch(size))
                {
                    Ok(reservation) => reservation,
                    Err(StoreError::Allocator(message)) if message.contains("exhausted") => {
                        return Ok(None);
                    }
                    Err(error) => return Err(store_error_to_py(error)),
                };
                buffer_pool::LocalBufferLease::new(reservation, size).map(Some)
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
            StoreBackend::Dummy(dummy) => run_without_gil(move || {
                dummy.put_from(key, buffer_ptr, size, tenant, policy.as_ref())
            })
            .map_err(store_error_to_py),
            StoreBackend::Real(dispatcher) => {
                let _ = pointer_from_usize(buffer_ptr)?;
                let key = key.to_string();
                let tenant = tenant.map(str::to_string);
                let cache_key = key.clone();
                let cache_tenant = tenant.clone();
                let scope = dispatcher.object_scope(tenant);
                run_without_gil(move || {
                    dispatcher.run(move |client| {
                        let buffer = buffer_ptr as *const c_void;
                        let mut request =
                            PutFromRequest::new(&key, buffer, size).tenant(scope.tenant.as_str());
                        if scope.domain != mooncake_store_core::DEFAULT_DOMAIN {
                            request = request.domain(scope.domain.as_str());
                        }
                        if scope.object_set != mooncake_store_core::DEFAULT_OBJECT_SET {
                            request = request.object_set(scope.object_set.as_str());
                        }
                        if let Some(policy) = policy {
                            request = request.replication(policy);
                        }
                        client.batch_put_from(&[request]).map(|_| ())
                    })
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
                let size = run_without_gil(move || dummy.get_into(key, buffer_ptr, size, tenant))
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
                run_without_gil(move || {
                    dispatcher.get_into_buffer(
                        key.to_string(),
                        tenant.map(str::to_string),
                        buffer_ptr,
                        size,
                    )
                })
                .map_err(store_error_to_py)
            }
        }
    }

    #[pyo3(signature = (
        buffer_ptrs,
        all_keys,
        all_dst_offsets,
        all_src_offsets,
        all_sizes,
        *,
        buffer_sizes = None,
        tenant = None
    ))]
    #[allow(clippy::too_many_arguments)]
    fn get_into_ranges(
        &self,
        buffer_ptrs: Vec<usize>,
        all_keys: Vec<Vec<String>>,
        all_dst_offsets: Vec<Vec<Vec<usize>>>,
        all_src_offsets: Vec<Vec<Vec<usize>>>,
        all_sizes: Vec<Vec<Vec<usize>>>,
        buffer_sizes: Option<Vec<usize>>,
        tenant: Option<&str>,
    ) -> PyResult<Vec<Vec<Vec<i64>>>> {
        self.get_into_ranges_internal(
            buffer_ptrs,
            all_keys,
            all_dst_offsets,
            all_src_offsets,
            all_sizes,
            buffer_sizes,
            tenant,
            None,
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
            StoreBackend::Dummy(dummy) => run_without_gil(move || {
                for (key, value) in &items {
                    let status = dummy.put(key, value, tenant, policy.as_ref())?;
                    if status != 0 {
                        return Ok(status);
                    }
                }
                Ok(0)
            })
            .map_err(store_error_to_py),
            StoreBackend::Real(dispatcher) => {
                let tenant = tenant.map(str::to_string);
                let cache_keys = items.iter().map(|(key, _)| key.clone()).collect::<Vec<_>>();
                let cache_tenant = tenant.clone();
                let scope = dispatcher.object_scope(tenant);
                run_without_gil(move || {
                    dispatcher.run(move |client| {
                        let requests = items
                            .iter()
                            .map(|(key, value)| {
                                let mut request =
                                    PutRequest::new(key, value).tenant(scope.tenant.as_str());
                                if scope.domain != mooncake_store_core::DEFAULT_DOMAIN {
                                    request = request.domain(scope.domain.as_str());
                                }
                                if scope.object_set != mooncake_store_core::DEFAULT_OBJECT_SET {
                                    request = request.object_set(scope.object_set.as_str());
                                }
                                if let Some(policy) = policy.clone() {
                                    request = request.replication(policy);
                                }
                                request
                            })
                            .collect::<Vec<_>>();
                        client.batch_put(&requests).map(|_| ())
                    })
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
                let statuses =
                    run_without_gil(move || dummy.batch_put_from(&items, tenant, policy.as_ref()))
                        .map_err(store_error_to_py)?;
                Ok(PyList::new(py, statuses)?.into_any().unbind())
            }
            StoreBackend::Real(dispatcher) => {
                for (_, buffer_ptr, _) in &items {
                    let _ = pointer_from_usize(*buffer_ptr)?;
                }
                let tenant = tenant.map(str::to_string);
                let cache_keys = items
                    .iter()
                    .map(|(key, _, _)| key.clone())
                    .collect::<Vec<_>>();
                let cache_tenant = tenant.clone();
                let scope = dispatcher.object_scope(tenant);
                let statuses = run_without_gil(move || {
                    execute_real_batch_put_from(dispatcher, items, scope, policy)
                })
                .map_err(store_error_to_py)?;
                let successful_keys = cache_keys
                    .into_iter()
                    .zip(statuses.iter())
                    .filter_map(|(key, status)| (*status == 0).then_some(key))
                    .collect::<Vec<_>>();
                if !successful_keys.is_empty() {
                    dispatcher.invalidate_keys(successful_keys, cache_tenant);
                }
                Ok(PyList::new(py, statuses)?.into_any().unbind())
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
            StoreBackend::Dummy(dummy) => run_without_gil(move || {
                for (key, buffers) in &items {
                    let total = buffers.iter().map(Vec::len).sum();
                    let mut payload = Vec::with_capacity(total);
                    for buffer in buffers {
                        payload.extend_from_slice(buffer);
                    }
                    let status = dummy.put(key, &payload, tenant, policy.as_ref())?;
                    if status != 0 {
                        return Ok(status);
                    }
                }
                Ok(0)
            })
            .map_err(store_error_to_py),
            StoreBackend::Real(dispatcher) => {
                let tenant = tenant.map(str::to_string);
                let cache_keys = items.iter().map(|(key, _)| key.clone()).collect::<Vec<_>>();
                let cache_tenant = tenant.clone();
                let scope = dispatcher.object_scope(tenant);
                run_without_gil(move || {
                    dispatcher.run(move |client| {
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
                                    MultiBufferPutRequest::new(key, slices.as_slice())
                                        .tenant(scope.tenant.as_str());
                                if scope.domain != mooncake_store_core::DEFAULT_DOMAIN {
                                    request = request.domain(scope.domain.as_str());
                                }
                                if scope.object_set != mooncake_store_core::DEFAULT_OBJECT_SET {
                                    request = request.object_set(scope.object_set.as_str());
                                }
                                if let Some(policy) = policy.clone() {
                                    request = request.replication(policy);
                                }
                                request
                            })
                            .collect::<Vec<_>>();
                        client.batch_put_from_multi_buffers(&requests).map(|_| ())
                    })
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
                run_without_gil(move || {
                    dummy.batch_put_from_multi_buffers(&items, tenant, policy.as_ref())
                })
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
                let scope = dispatcher.object_scope(tenant);
                run_without_gil(move || {
                    dispatcher.run(move |client| {
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
                                    MultiBufferPutRequest::new(key, buffers.as_slice())
                                        .tenant(scope.tenant.as_str());
                                if scope.domain != mooncake_store_core::DEFAULT_DOMAIN {
                                    request = request.domain(scope.domain.as_str());
                                }
                                if scope.object_set != mooncake_store_core::DEFAULT_OBJECT_SET {
                                    request = request.object_set(scope.object_set.as_str());
                                }
                                if let Some(policy) = policy.clone() {
                                    request = request.replication(policy);
                                }
                                request
                            })
                            .collect::<Vec<_>>();
                        client.batch_put_from_multi_buffers(&requests).map(|_| ())
                    })
                })
                .map_err(store_error_to_py)?;
                dispatcher.invalidate_keys(cache_keys, cache_tenant);
                Ok(vec![0; key_count])
            }
        }
    }

    #[pyo3(signature = (key, force = false, *, tenant = None))]
    fn remove(&self, key: &str, force: bool, tenant: Option<&str>) -> PyResult<i32> {
        match self.backend_ref()? {
            StoreBackend::Dummy(dummy) => {
                run_without_gil(move || dummy.remove(key, tenant, force)).map_err(store_error_to_py)
            }
            StoreBackend::Real(dispatcher) => {
                let key = key.to_string();
                let tenant = tenant.map(str::to_string);
                let cache_key = key.clone();
                let cache_tenant = tenant.clone();
                let scope = dispatcher.object_scope(tenant);
                run_without_gil(move || {
                    dispatcher.run(move |client| {
                        let mut object = ObjectRef::new(key.as_str()).tenant(scope.tenant.as_str());
                        if scope.domain != mooncake_store_core::DEFAULT_DOMAIN {
                            object = object.domain(scope.domain.as_str());
                        }
                        if scope.object_set != mooncake_store_core::DEFAULT_OBJECT_SET {
                            object = object.object_set(scope.object_set.as_str());
                        }
                        client.batch_remove(&[object], force).map(|_| ())
                    })
                })
                .map_err(store_error_to_py)?;
                dispatcher.untrack_key(&cache_key, cache_tenant.as_deref());
                dispatcher.invalidate_key(cache_key, cache_tenant);
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
        match self.backend_ref()? {
            StoreBackend::Dummy(dummy) => {
                run_without_gil(move || dummy.batch_remove(&keys, tenant, force))
                    .map_err(store_error_to_py)
            }
            StoreBackend::Real(dispatcher) => {
                let key_count = keys.len();
                let tenant = tenant.map(str::to_string);
                let cache_keys = keys.clone();
                let cache_tenant = tenant.clone();
                let scope = dispatcher.object_scope(tenant);
                run_without_gil(move || {
                    dispatcher.run(move |client| {
                        let objects = keys
                            .iter()
                            .map(|key| {
                                let mut object =
                                    ObjectRef::new(key.as_str()).tenant(scope.tenant.as_str());
                                if scope.domain != mooncake_store_core::DEFAULT_DOMAIN {
                                    object = object.domain(scope.domain.as_str());
                                }
                                if scope.object_set != mooncake_store_core::DEFAULT_OBJECT_SET {
                                    object = object.object_set(scope.object_set.as_str());
                                }
                                object
                            })
                            .collect::<Vec<_>>();
                        client.batch_remove(&objects, force).map(|_| ())
                    })
                })
                .map_err(store_error_to_py)?;
                dispatcher.untrack_keys(&cache_keys, cache_tenant.as_deref());
                dispatcher.invalidate_keys(cache_keys, cache_tenant);
                Ok(vec![0; key_count])
            }
        }
    }

    #[pyo3(signature = (keys, *, tenant = None))]
    fn batch_get(
        &self,
        py: Python<'_>,
        keys: Vec<String>,
        tenant: Option<&str>,
    ) -> PyResult<Vec<Py<PyBytes>>> {
        let values = match self.backend_ref()? {
            StoreBackend::Dummy(dummy) => run_without_gil(move || {
                let mut values = Vec::with_capacity(keys.len());
                for key in keys {
                    let (status, value) = dummy.get(&key, tenant)?;
                    if status != 0 {
                        return Err(StoreError::NotFound(format!(
                            "dummy store batch_get failed for key={key}"
                        )));
                    }
                    values.push(value);
                }
                Ok(values)
            })
            .map_err(store_error_to_py)?,
            StoreBackend::Real(dispatcher) => run_without_gil(move || {
                dispatcher.batch_get_values(keys, tenant.map(str::to_string))
            })
            .map_err(store_error_to_py)?,
        };
        Ok(values
            .into_iter()
            .map(|value| PyBytes::new(py, &value).unbind())
            .collect())
    }

    #[pyo3(signature = (keys, *, tenant = None))]
    fn batch_get_buffer(
        &self,
        py: Python<'_>,
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
            StoreBackend::Dummy(dummy) => {
                run_without_gil(move || dummy.batch_get_into(&items, tenant))
                    .map_err(store_error_to_py)
            }
            StoreBackend::Real(dispatcher) => {
                for (_, buffer_ptr, _) in &items {
                    let _ = pointer_from_usize(*buffer_ptr)?;
                }
                let item_count = items.len();
                run_without_gil(move || {
                    dispatcher.batch_get_into_buffers(items, tenant.map(str::to_string))
                })
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
                run_without_gil(move || dummy.batch_get_into_multi_buffers(&items, tenant))
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
                run_without_gil(move || {
                    dispatcher.batch_get_into_multi_buffers_raw(
                        keys,
                        all_buffer_ptrs,
                        all_sizes,
                        tenant.map(str::to_string),
                    )
                })
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
    fn expand_local_memory(&self, py: Python<'_>, storage_bytes: usize) -> PyResult<Py<PyAny>> {
        let dispatcher = self.real_dispatcher()?;
        let announcement = run_without_gil(move || {
            dispatcher.run(move |client| client.expand_local_memory(storage_bytes))
        })
        .map_err(store_error_to_py)?;
        segment_to_py(py, &announcement)
    }

    fn drain_segment(&self, segment_name: &str) -> PyResult<i32> {
        let segment_name = SegmentName::new(segment_name);
        let dispatcher = self.real_dispatcher()?;
        run_without_gil(move || dispatcher.run(move |client| client.drain_segment(&segment_name)))
            .map_err(store_error_to_py)?;
        Ok(0)
    }

    fn retire_segment(&self, segment_name: &str) -> PyResult<bool> {
        let segment_name = SegmentName::new(segment_name);
        let dispatcher = self.real_dispatcher()?;
        run_without_gil(move || dispatcher.run(move |client| client.retire_segment(&segment_name)))
            .map_err(store_error_to_py)
    }

    fn evacuate_owned_replicas(&mut self) -> PyResult<usize> {
        let dispatcher = self.real_dispatcher()?;
        run_without_gil(move || dispatcher.evacuate_owned_replicas()).map_err(store_error_to_py)
    }

    fn list_segments(&self, py: Python<'_>) -> PyResult<Vec<Py<PyAny>>> {
        let dispatcher = self.real_dispatcher()?;
        run_without_gil(move || dispatcher.run(|client| client.list_segments()))
            .map_err(store_error_to_py)?
            .iter()
            .map(|segment| segment_to_py(py, segment))
            .collect()
    }

    #[pyo3(signature = (key, *, tenant = None))]
    fn query_route(
        &self,
        py: Python<'_>,
        key: &str,
        tenant: Option<&str>,
    ) -> PyResult<Option<Py<PyAny>>> {
        let key = key.to_string();
        let tenant = tenant.map(str::to_string);
        let dispatcher = self.real_dispatcher()?;
        let scope = dispatcher.object_scope(tenant);
        let route = run_without_gil(move || {
            dispatcher.run(move |client| {
                let scope = NamespaceScope::with_defaults(
                    Some(scope.tenant.as_str()),
                    Some(scope.domain.as_str()),
                    Some(scope.object_set.as_str()),
                );
                client.query_route_in_scope(&scope, &key)
            })
        })
        .map_err(store_error_to_py)?;
        route.map(|route| route_to_py(py, &route)).transpose()
    }

    fn heartbeat(&mut self, expires_at_ms: u64) -> PyResult<i32> {
        let dispatcher = self.real_dispatcher()?;
        run_without_gil(move || dispatcher.heartbeat(expires_at_ms)).map_err(store_error_to_py)?;
        Ok(0)
    }

    fn activate(&mut self) -> PyResult<i32> {
        let dispatcher = self.real_dispatcher()?;
        run_without_gil(move || dispatcher.activate()).map_err(store_error_to_py)?;
        Ok(0)
    }

    fn enter_standby(&mut self) -> PyResult<i32> {
        let dispatcher = self.real_dispatcher()?;
        run_without_gil(move || dispatcher.enter_standby()).map_err(store_error_to_py)?;
        Ok(0)
    }

    fn enter_draining(&mut self) -> PyResult<i32> {
        let dispatcher = self.real_dispatcher()?;
        run_without_gil(move || dispatcher.enter_draining()).map_err(store_error_to_py)?;
        Ok(0)
    }

    #[pyo3(signature = (force = false))]
    fn remove_all(&self, force: bool) -> PyResult<i64> {
        match self.backend_ref()? {
            StoreBackend::Dummy(dummy) => {
                let (status, removed) =
                    run_without_gil(move || dummy.remove_all(force)).map_err(store_error_to_py)?;
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
            Some(StoreBackend::Dummy(dummy)) => Ok(allow_threads_ungil(|| dummy.health_check())),
            Some(StoreBackend::Real(_)) => Ok(0),
            None => Ok(1),
        }
    }

    fn metrics_text(&self) -> String {
        render_prometheus_metrics()
    }

    #[pyo3(signature = (bind_addr = "127.0.0.1:0"))]
    fn start_metrics_server(&self, bind_addr: &str) -> PyResult<String> {
        let bind_addr = bind_addr.to_string();
        run_without_gil(move || start_metrics_http_server(&bind_addr)).map_err(store_error_to_py)
    }

    fn stop_metrics_server(&self) -> PyResult<()> {
        run_without_gil(stop_metrics_http_server).map_err(store_error_to_py)
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
    let bind_addr = bind_addr.to_string();
    run_without_gil(move || start_metrics_http_server(&bind_addr)).map_err(store_error_to_py)
}

#[pyfunction]
fn stop_metrics_server() -> PyResult<()> {
    run_without_gil(stop_metrics_http_server).map_err(store_error_to_py)
}

#[pyfunction]
fn metrics_server_address() -> Option<String> {
    metrics_http_server_addr()
}

#[pymodule]
fn _store_rs(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PyMooncakeDistributedStore>()?;
    module.add_class::<PyMooncakeHostMemAllocator>()?;
    module.add_class::<tensor::TensorReadResult>()?;
    module.add_class::<tensor_parallel::PyParallelAxis>()?;
    module.add_class::<tensor_parallel::PyTensorParallelism>()?;
    module.add_class::<tensor_parallel::PyReadTarget>()?;
    module.add_function(wrap_pyfunction!(tensor_parallel::axis_dp, module)?)?;
    module.add_function(wrap_pyfunction!(tensor_parallel::axis_tp, module)?)?;
    module.add_function(wrap_pyfunction!(tensor_parallel::axis_ep, module)?)?;
    module.add_function(wrap_pyfunction!(tensor_parallel::axis_pp, module)?)?;
    module.add_function(wrap_pyfunction!(
        tensor_parallel::read_mode_as_stored,
        module
    )?)?;
    module.add_function(wrap_pyfunction!(tensor_parallel::read_mode_shard, module)?)?;
    module.add_function(wrap_pyfunction!(tensor_parallel::read_mode_full, module)?)?;
    buffer_pool::register_module(module)?;
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
            allow_threads_ungil(move || previous.close());
        }
    }

    fn backend_ref(&self) -> PyResult<&StoreBackend> {
        self.backend
            .as_ref()
            .ok_or_else(|| PyRuntimeError::new_err("store is not set up"))
    }

    pub(crate) fn get_bytes_internal(&self, key: &str, tenant: Option<&str>) -> PyResult<Vec<u8>> {
        match self.backend_ref()? {
            StoreBackend::Dummy(dummy) => {
                let (status, value) =
                    run_without_gil(move || dummy.get(key, tenant)).map_err(store_error_to_py)?;
                if status != 0 {
                    return Err(PyKeyError::new_err(format!(
                        "get_bytes_internal failed for key={key}"
                    )));
                }
                Ok(value)
            }
            StoreBackend::Real(dispatcher) => run_without_gil(move || {
                dispatcher.get_value(key.to_string(), tenant.map(str::to_string))
            })
            .map_err(store_error_to_py),
        }
    }

    pub(crate) fn get_into_buffer_internal(
        &self,
        key: &str,
        buffer_ptr: usize,
        size: usize,
        tenant: Option<&str>,
    ) -> PyResult<usize> {
        match self.backend_ref()? {
            StoreBackend::Dummy(dummy) => {
                let result = run_without_gil(move || dummy.get_into(key, buffer_ptr, size, tenant))
                    .map_err(store_error_to_py)?;
                if result < 0 {
                    return Err(PyKeyError::new_err(format!(
                        "get_into_buffer_internal failed for key={key}"
                    )));
                }
                Ok(result as usize)
            }
            StoreBackend::Real(dispatcher) => run_without_gil(move || {
                dispatcher.get_into_buffer(
                    key.to_string(),
                    tenant.map(str::to_string),
                    buffer_ptr,
                    size,
                )
            })
            .map_err(store_error_to_py),
        }
    }

    pub(crate) fn batch_query_read_cache_internal(
        &self,
        keys: &[String],
        tenant: Option<&str>,
    ) -> PyResult<Option<ReadQueryResultCache>> {
        if keys.is_empty() {
            return Ok(Some(ReadQueryResultCache::default()));
        }
        match self.backend_ref()? {
            StoreBackend::Real(dispatcher) => run_without_gil({
                let keys = keys.to_vec();
                let tenant = tenant.map(str::to_string);
                move || dispatcher.batch_query_read_cache(keys, tenant)
            })
            .map(Some)
            .map_err(store_error_to_py),
            StoreBackend::Dummy(_) => Ok(None),
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn get_into_ranges_internal(
        &self,
        buffer_ptrs: Vec<usize>,
        all_keys: Vec<Vec<String>>,
        all_dst_offsets: Vec<Vec<Vec<usize>>>,
        all_src_offsets: Vec<Vec<Vec<usize>>>,
        all_sizes: Vec<Vec<Vec<usize>>>,
        buffer_sizes: Option<Vec<usize>>,
        tenant: Option<&str>,
        query_cache: Option<ReadQueryResultCache>,
    ) -> PyResult<Vec<Vec<Vec<i64>>>> {
        let resolved_buffer_sizes =
            buffer_sizes.unwrap_or_else(|| vec![usize::MAX; buffer_ptrs.len()]);
        if resolved_buffer_sizes.len() != buffer_ptrs.len() {
            return Err(PyValueError::new_err(format!(
                "buffer_sizes length {} does not match buffer_ptrs length {}",
                resolved_buffer_sizes.len(),
                buffer_ptrs.len()
            )));
        }
        match self.backend_ref()? {
            StoreBackend::Dummy(dummy) => {
                for &ptr in &buffer_ptrs {
                    pointer_from_usize(ptr)?;
                }
                run_without_gil(move || {
                    dummy.get_into_ranges(
                        &buffer_ptrs,
                        &resolved_buffer_sizes,
                        &all_keys,
                        &all_dst_offsets,
                        &all_src_offsets,
                        &all_sizes,
                        tenant,
                    )
                })
                .map_err(store_error_to_py)
            }
            StoreBackend::Real(dispatcher) => {
                let validated: Vec<(usize, usize)> = buffer_ptrs
                    .iter()
                    .zip(resolved_buffer_sizes.iter())
                    .map(|(&ptr, &size)| {
                        pointer_from_usize(ptr)?;
                        Ok((ptr, size))
                    })
                    .collect::<PyResult<Vec<_>>>()?;
                let ptrs = validated.iter().map(|(p, _)| *p).collect();
                let sizes = validated.iter().map(|(_, s)| *s).collect();
                run_without_gil(move || {
                    dispatcher.get_into_ranges_with_query_cache(
                        ptrs,
                        sizes,
                        all_keys,
                        all_dst_offsets,
                        all_src_offsets,
                        all_sizes,
                        tenant.map(str::to_string),
                        query_cache,
                    )
                })
                .map_err(store_error_to_py)
            }
        }
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
    dict.set_item("transport_endpoint", segment.transport_endpoint.clone())?;
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

#[inline]
pub(crate) fn pointer_from_usize(pointer: usize) -> PyResult<*mut c_void> {
    if pointer == 0 {
        return Err(PyValueError::new_err("buffer pointer must not be null"));
    }
    Ok(pointer as *mut c_void)
}

fn batch_put_from_fanout_width(item_count: usize) -> usize {
    if item_count <= 1 {
        return 1;
    }
    let fanout = std::env::var(BATCH_PUT_FROM_FANOUT_ENV)
        .ok()
        .and_then(|value| value.trim().parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_BATCH_PUT_FROM_FANOUT_WIDTH);
    item_count.min(fanout)
}

fn batch_put_from_fanout_ranges(item_count: usize, width: usize) -> Vec<Range<usize>> {
    if item_count == 0 {
        return Vec::new();
    }
    let width = width.max(1).min(item_count);
    let chunk_size = item_count.div_ceil(width);
    (0..item_count)
        .step_by(chunk_size)
        .map(|start| start..(start + chunk_size).min(item_count))
        .collect()
}

fn execute_real_batch_put_from_shard(
    dispatcher: &StoreDispatcher,
    items: Vec<(String, usize, usize)>,
    scope: CompatObjectScope,
    policy: Option<ReplicationPolicy>,
) -> Result<Vec<i32>, StoreError> {
    let item_count = items.len();
    dispatcher.run(move |client| {
        let total_bytes = items.iter().map(|(_, _, size)| *size).sum::<usize>();
        let requests = items
            .iter()
            .map(|(key, buffer_ptr, size)| {
                let mut request = PutFromRequest::new(
                    key,
                    (*buffer_ptr as *mut c_void).cast_const(),
                    *size,
                )
                .tenant(scope.tenant.as_str());
                if scope.domain != mooncake_store_core::DEFAULT_DOMAIN {
                    request = request.domain(scope.domain.as_str());
                }
                if scope.object_set != mooncake_store_core::DEFAULT_OBJECT_SET {
                    request = request.object_set(scope.object_set.as_str());
                }
                if let Some(policy) = policy.clone() {
                    request = request.replication(policy);
                }
                request
            })
            .collect::<Vec<_>>();
        let batch_started = Instant::now();
        let results = client.batch_put_from_statuses(&requests);
        let elapsed_ms = batch_started.elapsed().as_millis();
        let mut failed = 0usize;
        let statuses = items
            .iter()
            .zip(results)
            .map(|((key, _, size), result)| match result {
                Ok(_) => 0,
                Err(error) => {
                    failed += 1;
                    eprintln!(
                        "[mooncake-store] batch_put_from per-key failed runtime={} key={} bytes={} batch_items={} batch_bytes={} elapsed_ms={} error={}",
                        client.runtime_id(),
                        key,
                        size,
                        item_count,
                        total_bytes,
                        elapsed_ms,
                        error
                    );
                    tracing::debug!(
                        key = %key,
                        error = %error,
                        "batch_put_from per-key write failed"
                    );
                    -1
                }
            })
            .collect::<Vec<_>>();
        if failed > 0 {
            tracing::debug!(
                failed,
                items = item_count,
                bytes = total_bytes,
                elapsed_ms,
                "batch_put_from completed with per-key failures"
            );
        }
        Ok(statuses)
    })
}

fn execute_real_batch_put_from(
    dispatcher: &StoreDispatcher,
    items: Vec<(String, usize, usize)>,
    scope: CompatObjectScope,
    policy: Option<ReplicationPolicy>,
) -> Result<Vec<i32>, StoreError> {
    let item_count = items.len();
    let total_bytes = items.iter().map(|(_, _, size)| *size as u64).sum::<u64>();
    let fanout_width = batch_put_from_fanout_width(item_count);
    let tracker = OperationTracker::new("py_batch_put_from_fanout")
        .scope("python_bridge")
        .input_bytes(total_bytes)
        .attribute_u64("mooncake.item_count", item_count as u64)
        .attribute_u64("mooncake.fanout_width", fanout_width as u64);
    if fanout_width <= 1 {
        let result = execute_real_batch_put_from_shard(dispatcher, items, scope, policy);
        tracker.finish(
            &result,
            result.as_ref().map(|items| items.len()).unwrap_or(0) as u64,
        );
        return result;
    }
    let result = (|| {
        let ranges = batch_put_from_fanout_ranges(item_count, fanout_width);
        let mut statuses = vec![-1; item_count];
        let results = thread::scope(|thread_scope| {
            let mut handles = Vec::with_capacity(ranges.len());
            for range in ranges {
                let shard_items = items[range.clone()].to_vec();
                let shard_scope = scope.clone();
                let shard_policy = policy.clone();
                handles.push((
                    range.start,
                    thread_scope.spawn(move || {
                        execute_real_batch_put_from_shard(
                            dispatcher,
                            shard_items,
                            shard_scope,
                            shard_policy,
                        )
                    }),
                ));
            }
            handles
                .into_iter()
                .map(|(start, handle)| {
                    handle.join().map(|result| (start, result)).map_err(|_| {
                        StoreError::Transport("batch_put_from fanout worker panicked".to_string())
                    })
                })
                .collect::<Result<Vec<_>, StoreError>>()
        })?;
        for (start, shard_result) in results {
            let shard_statuses = shard_result?;
            let end = start + shard_statuses.len();
            statuses[start..end].copy_from_slice(&shard_statuses);
        }
        Ok(statuses)
    })();
    tracker.finish(
        &result,
        result.as_ref().map(|items| items.len()).unwrap_or(0) as u64,
    );
    result
}

#[inline]
pub(crate) fn run_without_gil<T, F>(f: F) -> Result<T, StoreError>
where
    T: pyo3::marker::Ungil + Send,
    F: pyo3::marker::Ungil + FnOnce() -> Result<T, StoreError>,
{
    allow_threads_ungil(f)
}

#[inline]
fn allow_threads_ungil<T, F>(f: F) -> T
where
    T: pyo3::marker::Ungil,
    F: pyo3::marker::Ungil + FnOnce() -> T,
{
    Python::with_gil(|py| py.allow_threads(f))
}

#[inline]
pub(crate) fn store_error_to_py(error: StoreError) -> PyErr {
    match error {
        StoreError::NotFound(message) => PyKeyError::new_err(message),
        StoreError::Conflict(message)
        | StoreError::QuotaExceeded { message, .. }
        | StoreError::InvalidState(message)
        | StoreError::StaleEpoch(message)
        | StoreError::Unsupported(message)
        | StoreError::Allocator(message)
        | StoreError::Backpressure(message)
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
            | StoreError::Backpressure(_)
    )
}

fn soft_miss_status(error: &StoreError) -> i64 {
    match error {
        StoreError::NotFound(_) => -1,
        StoreError::InvalidState(_) => -2,
        StoreError::Metadata(_) => -3,
        StoreError::Transport(_) => -4,
        StoreError::Conflict(_) | StoreError::QuotaExceeded { .. } => -5,
        StoreError::StaleEpoch(_) => -6,
        StoreError::Unsupported(_) => -7,
        StoreError::Allocator(_) => -8,
        StoreError::Backpressure(_) => -9,
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
    use crate::{finalize_real_dispatcher_setup, DEFAULT_COMPAT_WORKER_SCOPE};
    use std::collections::{BTreeMap, BTreeSet};
    use std::ffi::c_void;
    use std::net::TcpListener;
    use std::ptr;
    use std::slice;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Arc, Barrier};
    use std::thread::sleep;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

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
        _store_rs, batch_put_from_fanout_ranges, batch_put_from_fanout_width, init_tracing,
        metrics_server_address, metrics_text, parse_initial_state_arg, pointer_from_usize,
        replication_policy, route_to_py, segment_to_py, start_metrics_server, stop_metrics_server,
        store_error_to_py, DummySession, PyMooncakeDistributedStore, PyMooncakeHostMemAllocator,
        StoreBackend, BATCH_PUT_FROM_FANOUT_ENV,
    };
    use crate::dispatcher::{CompatNamespaceScope, StoreDispatcher};
    use crate::dummy_service::pb;
    use crate::dummy_service::{start_dummy_store_server, DummyStoreServerHandle};
    use crate::runtime::CompatTimeoutConfig;
    use crate::test_support::env_test_lock;

    struct TestTransport {
        local_segment: String,
        rpc_host: String,
        rpc_port: u16,
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
            Self::new_with_rpc_address(local_segment, "127.0.0.1", 0)
        }

        fn new_with_rpc_address(local_segment: &str, rpc_host: &str, rpc_port: u16) -> Self {
            Self {
                local_segment: local_segment.to_string(),
                rpc_host: rpc_host.to_string(),
                rpc_port,
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
            Ok((self.rpc_host.clone(), self.rpc_port))
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

    fn build_client_with_metadata_and_expiry(
        name: &str,
        metadata: Arc<dyn MetadataBackend>,
        state: ClientLifecycleState,
        expires_at_ms: u64,
    ) -> StoreClient {
        let transport = Arc::new(TestTransport::new(&format!("{name}-segment")));
        let planner = PlacementPlanner::new(metadata.clone()).require_label("storage", "true");
        StoreClientBuilder::new(metadata, name)
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
            .build(expires_at_ms)
            .expect("test store client should build")
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
        transport: Arc<dyn StoreTransport>,
        state: ClientLifecycleState,
    ) -> StoreClient {
        let planner = PlacementPlanner::new(metadata.clone()).require_label("storage", "true");
        StoreClientBuilder::new(metadata, name)
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
        block_route_cas: Arc<AtomicBool>,
        block_route_lookup: Arc<AtomicBool>,
        block_publish_segment: Arc<AtomicBool>,
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
            block_route_cas: Arc<AtomicBool>,
            release: Arc<AtomicBool>,
            entered: std::sync::mpsc::Sender<()>,
        ) -> Self {
            Self::new_with_route_lookup(
                block_lease,
                block_state_update,
                block_route_cas,
                Arc::new(AtomicBool::new(false)),
                Arc::new(AtomicBool::new(false)),
                release,
                entered,
            )
        }

        fn new_with_route_lookup(
            block_lease: Arc<AtomicBool>,
            block_state_update: Arc<AtomicBool>,
            block_route_cas: Arc<AtomicBool>,
            block_route_lookup: Arc<AtomicBool>,
            block_publish_segment: Arc<AtomicBool>,
            release: Arc<AtomicBool>,
            entered: std::sync::mpsc::Sender<()>,
        ) -> Self {
            Self {
                inner: InMemoryMetadataBackend::new(),
                block_lease,
                block_state_update,
                block_route_cas,
                block_route_lookup,
                block_publish_segment,
                release,
                entered: std::sync::Mutex::new(Some(entered)),
            }
        }

        fn new_with_publish_segment(
            block_publish_segment: Arc<AtomicBool>,
            release: Arc<AtomicBool>,
            entered: std::sync::mpsc::Sender<()>,
        ) -> Self {
            Self::new_with_route_lookup(
                Arc::new(AtomicBool::new(false)),
                Arc::new(AtomicBool::new(false)),
                Arc::new(AtomicBool::new(false)),
                Arc::new(AtomicBool::new(false)),
                block_publish_segment,
                release,
                entered,
            )
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

        fn allocate_client_lease(
            &self,
            template: &ClientLease,
        ) -> mooncake_store_core::Result<ClientRuntimeId> {
            self.inner.allocate_client_lease(template)
        }

        fn update_client_state(
            &self,
            runtime: &ClientRuntimeId,
            next: ClientLifecycleState,
        ) -> mooncake_store_core::Result<()> {
            self.maybe_block(&self.block_state_update);
            self.inner.update_client_state(runtime, next)
        }

        fn get_client_lease(
            &self,
            runtime: &ClientRuntimeId,
        ) -> mooncake_store_core::Result<Option<ClientLease>> {
            self.inner.get_client_lease(runtime)
        }

        fn get_live_runtime_by_stable_id(
            &self,
            stable_id: &ClientStableId,
        ) -> mooncake_store_core::Result<Option<ClientLease>> {
            self.inner.get_live_runtime_by_stable_id(stable_id)
        }

        fn list_live_clients(&self) -> mooncake_store_core::Result<Vec<ClientLease>> {
            self.inner.list_live_clients()
        }

        fn publish_segment(
            &self,
            segment: &SegmentAnnouncement,
        ) -> mooncake_store_core::Result<()> {
            self.maybe_block(&self.block_publish_segment);
            self.inner.publish_segment(segment)
        }

        fn unpublish_segment(
            &self,
            owner: &ClientRuntimeId,
            segment: &SegmentName,
        ) -> mooncake_store_core::Result<()> {
            self.inner.unpublish_segment(owner, segment)
        }

        fn get_segment(
            &self,
            owner: &ClientRuntimeId,
            segment: &SegmentName,
        ) -> mooncake_store_core::Result<Option<SegmentAnnouncement>> {
            self.inner.get_segment(owner, segment)
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
            self.maybe_block(&self.block_route_lookup);
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
            self.maybe_block(&self.block_route_cas);
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

        fn list_tenant_policies(
            &self,
            tenant: Option<&str>,
        ) -> mooncake_store_core::Result<Vec<TenantPolicy>> {
            self.inner.list_tenant_policies(tenant)
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

        fn get_tenant_quota_reservation(
            &self,
            reservation_id: &str,
        ) -> mooncake_store_core::Result<Option<TenantQuotaReservation>> {
            self.inner.get_tenant_quota_reservation(reservation_id)
        }

        fn list_tenant_eviction_candidates(
            &self,
            scope: &TenantPolicyScope,
            limit: usize,
        ) -> mooncake_store_core::Result<Vec<TenantObjectAccounting>> {
            self.inner.list_tenant_eviction_candidates(scope, limit)
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

        fn list_cold_tier_devices(
            &self,
            filter: &mooncake_store_core::ColdTierDeviceFilter,
        ) -> mooncake_store_core::Result<Vec<mooncake_store_core::ColdTierDeviceRecord>> {
            self.inner.list_cold_tier_devices(filter)
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

        fn allocate_client_lease(
            &self,
            template: &ClientLease,
        ) -> mooncake_store_core::Result<ClientRuntimeId> {
            self.inner.allocate_client_lease(template)
        }

        fn update_client_state(
            &self,
            runtime: &ClientRuntimeId,
            next: ClientLifecycleState,
        ) -> mooncake_store_core::Result<()> {
            self.inner.update_client_state(runtime, next)
        }

        fn get_client_lease(
            &self,
            runtime: &ClientRuntimeId,
        ) -> mooncake_store_core::Result<Option<ClientLease>> {
            self.inner.get_client_lease(runtime)
        }

        fn get_live_runtime_by_stable_id(
            &self,
            stable_id: &ClientStableId,
        ) -> mooncake_store_core::Result<Option<ClientLease>> {
            self.inner.get_live_runtime_by_stable_id(stable_id)
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

        fn get_segment(
            &self,
            owner: &ClientRuntimeId,
            segment: &SegmentName,
        ) -> mooncake_store_core::Result<Option<SegmentAnnouncement>> {
            self.inner.get_segment(owner, segment)
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

        fn list_tenant_policies(
            &self,
            tenant: Option<&str>,
        ) -> mooncake_store_core::Result<Vec<TenantPolicy>> {
            self.inner.list_tenant_policies(tenant)
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

        fn get_tenant_quota_reservation(
            &self,
            reservation_id: &str,
        ) -> mooncake_store_core::Result<Option<TenantQuotaReservation>> {
            self.inner.get_tenant_quota_reservation(reservation_id)
        }

        fn list_tenant_eviction_candidates(
            &self,
            scope: &TenantPolicyScope,
            limit: usize,
        ) -> mooncake_store_core::Result<Vec<TenantObjectAccounting>> {
            self.inner.list_tenant_eviction_candidates(scope, limit)
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

        fn list_cold_tier_devices(
            &self,
            filter: &mooncake_store_core::ColdTierDeviceFilter,
        ) -> mooncake_store_core::Result<Vec<mooncake_store_core::ColdTierDeviceRecord>> {
            self.inner.list_cold_tier_devices(filter)
        }
    }

    fn build_real_store(name: &str) -> PyMooncakeDistributedStore {
        build_real_store_with_dispatcher_scope_and_metadata(
            name,
            "default",
            Arc::new(InMemoryMetadataBackend::new()),
        )
    }

    fn build_real_store_with_dispatcher_scope_and_metadata(
        name: &str,
        compat_scope: &str,
        metadata: Arc<dyn MetadataBackend>,
    ) -> PyMooncakeDistributedStore {
        build_real_store_with_default_scope(name, compat_scope, metadata, None, None)
    }

    fn build_real_store_with_default_scope(
        name: &str,
        compat_scope: &str,
        metadata: Arc<dyn MetadataBackend>,
        domain: Option<&str>,
        object_set: Option<&str>,
    ) -> PyMooncakeDistributedStore {
        let client = build_client_with_metadata(name, metadata);
        let default_scope = CompatNamespaceScope::new(
            client.default_tenant().to_string(),
            domain.map(str::to_string),
            object_set.map(str::to_string),
        );
        let dispatcher = StoreDispatcher::spawn_with_timeout_config_and_namespace_scope(
            client,
            format!("dispatcher-{name}"),
            CompatTimeoutConfig::from_env(),
            compat_scope,
            default_scope,
        )
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
        for _ in 0..160 {
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

    fn wildcard_bind_addr(address: &str) -> String {
        let port = address
            .rsplit_once(':')
            .expect("address should contain port")
            .1;
        format!("0.0.0.0:{port}")
    }

    fn build_dummy_store(name: &str) -> (PyMooncakeDistributedStore, DummyStoreServerHandle) {
        let dispatcher = Arc::new(
            StoreDispatcher::spawn(build_client(name), format!("dummy-dispatcher-{name}"))
                .expect("dummy dispatcher should spawn"),
        );
        dispatcher
            .register_local_memory()
            .expect("dummy local memory should register");
        let worker_scope = format!("dummy-worker-{name}");
        let server = start_dummy_store_server(dispatcher, &bind_addr(), &worker_scope)
            .expect("dummy server should start");
        let mut store = PyMooncakeDistributedStore::new();
        store
            .setup_dummy(0, 0, server.address(), None, Some(worker_scope))
            .expect("dummy store should connect");
        for _ in 0..40 {
            if store.health_check().expect("health check should succeed") == 0 {
                break;
            }
            sleep(Duration::from_millis(25));
        }
        (store, server)
    }

    #[test]
    fn setup_dummy_without_worker_scope_connects_to_default_scope_server() {
        let _guard = env_test_lock().lock();
        prepare_freethreaded_python();
        let dispatcher = Arc::new(
            StoreDispatcher::spawn(
                build_client("dummy-default-worker-scope"),
                "dummy-default-worker-scope",
            )
            .expect("dummy dispatcher should spawn"),
        );
        dispatcher
            .register_local_memory()
            .expect("dummy local memory should register");
        let server =
            start_dummy_store_server(dispatcher, &bind_addr(), DEFAULT_COMPAT_WORKER_SCOPE)
                .expect("dummy server should start");
        let mut store = PyMooncakeDistributedStore::new();
        store
            .setup_dummy(0, 0, server.address(), None, None)
            .expect("dummy store should connect without explicit worker scope");
        for _ in 0..40 {
            if store.health_check().expect("health check should succeed") == 0 {
                break;
            }
            sleep(Duration::from_millis(25));
        }
        store.close();
        server.shutdown().expect("dummy server should stop");
    }

    #[test]
    fn setup_dummy_without_worker_scope_can_use_buffer_api_against_scoped_server() {
        let _guard = env_test_lock().lock();
        prepare_freethreaded_python();
        let dispatcher = Arc::new(
            StoreDispatcher::spawn(
                build_client("dummy-buffer-legacy-scope"),
                "dummy-buffer-legacy-scope",
            )
            .expect("dummy dispatcher should spawn"),
        );
        dispatcher
            .register_local_memory()
            .expect("dummy local memory should register");
        let server = start_dummy_store_server(
            dispatcher,
            &bind_addr(),
            "tenant-a/dummy-buffer-legacy-scope",
        )
        .expect("dummy server should start");
        let mut store = PyMooncakeDistributedStore::new();
        store
            .setup_dummy(0, 0, server.address(), None, None)
            .expect("dummy store should connect without explicit worker scope");
        for _ in 0..40 {
            if store.health_check().expect("health check should succeed") == 0 {
                break;
            }
            sleep(Duration::from_millis(25));
        }

        let allocator = PyMooncakeHostMemAllocator::new(None, None);
        let write_ptr = allocator
            .alloc(64)
            .expect("first legacy write buffer should allocate");
        let write_ptr_two = allocator
            .alloc(64)
            .expect("second legacy write buffer should allocate");
        let read_ptr = allocator
            .alloc(64)
            .expect("first legacy read buffer should allocate");
        let read_ptr_two = allocator
            .alloc(64)
            .expect("second legacy read buffer should allocate");
        unsafe {
            slice::from_raw_parts_mut(write_ptr as *mut u8, 64)[..5].copy_from_slice(b"hello");
            slice::from_raw_parts_mut(write_ptr_two as *mut u8, 64)[..5].copy_from_slice(b"world");
        }

        assert_eq!(
            store
                .register_buffer(write_ptr, 64)
                .expect("first legacy write buffer should register"),
            0
        );
        assert_eq!(
            store
                .register_buffer(write_ptr_two, 64)
                .expect("second legacy write buffer should register"),
            0
        );
        assert_eq!(
            store
                .register_buffer(read_ptr, 64)
                .expect("first legacy read buffer should register"),
            0
        );
        assert_eq!(
            store
                .register_buffer(read_ptr_two, 64)
                .expect("second legacy read buffer should register"),
            0
        );

        Python::with_gil(|py| {
            let statuses = store
                .batch_put_from(
                    py,
                    vec![
                        ("legacy-a".to_string(), write_ptr, 5),
                        ("legacy-b".to_string(), write_ptr_two, 5),
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
                .expect("legacy dummy batch_put_from should succeed");
            assert_eq!(
                statuses
                    .bind(py)
                    .extract::<Vec<i32>>()
                    .expect("legacy dummy batch_put_from should return status list"),
                vec![0, 0]
            );
        });

        let lengths = store
            .batch_get_into(
                vec![
                    ("legacy-a".to_string(), read_ptr, 64),
                    ("legacy-b".to_string(), read_ptr_two, 64),
                ],
                None,
            )
            .expect("legacy dummy batch_get_into should succeed");
        assert_eq!(lengths, vec![5, 5]);
        unsafe {
            assert_eq!(slice::from_raw_parts(read_ptr as *const u8, 5), b"hello");
            assert_eq!(
                slice::from_raw_parts(read_ptr_two as *const u8, 5),
                b"world"
            );
        }

        assert_eq!(
            store
                .unregister_buffer(write_ptr, Some(64))
                .expect("first legacy write buffer should unregister"),
            0
        );
        assert_eq!(
            store
                .unregister_buffer(write_ptr_two, Some(64))
                .expect("second legacy write buffer should unregister"),
            0
        );
        assert_eq!(
            store
                .unregister_buffer(read_ptr, Some(64))
                .expect("first legacy read buffer should unregister"),
            0
        );
        assert_eq!(
            store
                .unregister_buffer(read_ptr_two, Some(64))
                .expect("second legacy read buffer should unregister"),
            0
        );

        store.close();
        allocator
            .free(write_ptr)
            .expect("first legacy write buffer should free");
        allocator
            .free(write_ptr_two)
            .expect("second legacy write buffer should free");
        allocator
            .free(read_ptr)
            .expect("first legacy read buffer should free");
        allocator
            .free(read_ptr_two)
            .expect("second legacy read buffer should free");
        server.shutdown().expect("dummy server should stop");
    }

    #[test]
    fn setup_dummy_buffer_api_accepts_wildcard_bind_with_concrete_client_address() {
        let _guard = env_test_lock().lock();
        prepare_freethreaded_python();
        let metadata = Arc::new(InMemoryMetadataBackend::new());
        let transport = Arc::new(TestTransport::new_with_rpc_address(
            "dummy-buffer-wildcard-bind-segment",
            "127.0.0.1",
            17300,
        ));
        let dispatcher = Arc::new(
            StoreDispatcher::spawn(
                build_client_with_metadata_and_transport(
                    "dummy-buffer-wildcard-bind",
                    metadata,
                    transport,
                    ClientLifecycleState::Active,
                ),
                "dummy-buffer-wildcard-bind",
            )
            .expect("dummy dispatcher should spawn"),
        );
        dispatcher
            .register_local_memory()
            .expect("dummy local memory should register");
        let client_addr = bind_addr();
        let server = start_dummy_store_server(
            dispatcher,
            &wildcard_bind_addr(&client_addr),
            DEFAULT_COMPAT_WORKER_SCOPE,
        )
        .expect("dummy server should start");
        let mut store = PyMooncakeDistributedStore::new();
        store
            .setup_dummy(0, 0, &client_addr, None, None)
            .expect("dummy store should connect through concrete client address");
        for _ in 0..40 {
            if store.health_check().expect("health check should succeed") == 0 {
                break;
            }
            sleep(Duration::from_millis(25));
        }

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

        assert_eq!(
            store
                .register_buffer(write_ptr, 64)
                .expect("dummy write buffer should register"),
            0
        );
        assert_eq!(
            store
                .register_buffer(read_ptr, 64)
                .expect("dummy read buffer should register"),
            0
        );

        Python::with_gil(|py| {
            let statuses = store
                .batch_put_from(
                    py,
                    vec![("wildcard".to_string(), write_ptr, 5)],
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
                .expect("wildcard dummy batch_put_from should succeed");
            assert_eq!(
                statuses
                    .bind(py)
                    .extract::<Vec<i32>>()
                    .expect("wildcard dummy batch_put_from should return status list"),
                vec![0]
            );
        });

        let lengths = store
            .batch_get_into(vec![("wildcard".to_string(), read_ptr, 64)], None)
            .expect("wildcard dummy batch_get_into should succeed");
        assert_eq!(lengths, vec![5]);
        unsafe {
            assert_eq!(slice::from_raw_parts(read_ptr as *const u8, 5), b"hello");
        }

        assert_eq!(
            store
                .unregister_buffer(write_ptr, Some(64))
                .expect("dummy write buffer should unregister"),
            0
        );
        assert_eq!(
            store
                .unregister_buffer(read_ptr, Some(64))
                .expect("dummy read buffer should unregister"),
            0
        );

        store.close();
        server.shutdown().expect("dummy server should stop");
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

    #[test]
    fn batch_put_from_default_fanout_keeps_single_rust_batch() {
        let _guard = env_test_lock().lock();
        let _env = EnvVarGuard::unset(BATCH_PUT_FROM_FANOUT_ENV);
        assert_eq!(batch_put_from_fanout_width(32), 1);
        assert_eq!(batch_put_from_fanout_ranges(32, 1), vec![0..32]);
    }

    #[test]
    fn batch_put_from_fanout_env_can_explicitly_shard_large_batches() {
        let _guard = env_test_lock().lock();
        let _env = EnvVarGuard::set(BATCH_PUT_FROM_FANOUT_ENV, "4");
        assert_eq!(batch_put_from_fanout_width(32), 4);
        assert_eq!(
            batch_put_from_fanout_ranges(32, 4),
            vec![0..8, 8..16, 16..24, 24..32]
        );
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

        async fn remove(
            &self,
            _request: tonic::Request<pb::RemoveRequest>,
        ) -> std::result::Result<tonic::Response<pb::StatusReply>, tonic::Status> {
            self.wait().await;
            Ok(tonic::Response::new(pb::StatusReply { status: 0 }))
        }

        async fn batch_remove(
            &self,
            _request: tonic::Request<pb::BatchRemoveRequest>,
        ) -> std::result::Result<tonic::Response<pb::BatchStatusReply>, tonic::Status> {
            self.wait().await;
            Ok(tonic::Response::new(pb::BatchStatusReply {
                statuses: vec![],
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

        async fn get_into_ranges(
            &self,
            _request: tonic::Request<pb::GetIntoRangesRequest>,
        ) -> std::result::Result<tonic::Response<pb::GetIntoRangesReply>, tonic::Status> {
            self.wait().await;
            Ok(tonic::Response::new(pb::GetIntoRangesReply {
                buffer_results: vec![],
            }))
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
        DummySession::connect(&address, "blocking-worker")
            .expect("blocking dummy server should accept connections");
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
            transport_endpoint: Some("10.0.0.8:12001".to_string()),
            transport_segment_descriptor: None,
            capacity_bytes: 256,
            used_bytes: 32,
            target_chunks: Vec::new(),
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
                offset: Some(64),
                segment_offset: 64,
                length: 5,
                checksum: Some(3),
                tier: ReplicaTier::Dram,
                priority: 1,
            }],
            cold_backing: None,
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
                segment_any
                    .get_item("transport_endpoint")
                    .expect("segment field should exist")
                    .extract::<String>()
                    .expect("transport endpoint should extract"),
                "10.0.0.8:12001"
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
    fn real_store_default_namespace_scope_isolates_python_compat_api() {
        init_python();
        let metadata = Arc::new(InMemoryMetadataBackend::new());
        let object_set = "CHECKPOINT=oss://example-bucket/example-path/checkpoint-500/";
        let store_a = build_real_store_with_default_scope(
            "py-default-scope-a",
            "default",
            metadata.clone(),
            Some("sglang-chat"),
            Some(object_set),
        );
        let store_b = build_real_store_with_default_scope(
            "py-default-scope-b",
            "default",
            metadata,
            Some("sglang-chat"),
            Some("checkpoint-501"),
        );

        store_a
            .put(
                "same-key",
                b"version-a".to_vec(),
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
            .expect("put under object_set A should succeed");
        store_b
            .put(
                "same-key",
                b"version-b".to_vec(),
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
            .expect("put under object_set B should succeed");

        Python::with_gil(|py| {
            assert_eq!(
                store_a
                    .get(py, "same-key", None)
                    .expect("get from object_set A should succeed")
                    .as_bytes(),
                b"version-a"
            );
            assert_eq!(
                store_b
                    .get(py, "same-key", None)
                    .expect("get from object_set B should succeed")
                    .as_bytes(),
                b"version-b"
            );
            let route = store_a
                .query_route(py, "same-key", None)
                .expect("query_route should succeed")
                .expect("route should exist");
            assert_eq!(
                route
                    .bind(py)
                    .get_item("key")
                    .expect("route key should exist")
                    .extract::<String>()
                    .expect("route key should extract"),
                "default::ns/sglang-chat/CHECKPOINT%3Doss%3A%2F%2Fexample-bucket%2Fexample-path%2Fcheckpoint-500%2F/same-key"
            );
        });

        assert!(store_a
            .is_exist("same-key", None)
            .expect("is_exist should use default object_set"));
        assert_eq!(
            store_a
                .batch_is_exist(vec!["same-key".to_string()], None)
                .expect("batch_is_exist should use default object_set"),
            vec![1]
        );
        assert_eq!(
            store_a
                .get_size("same-key", None)
                .expect("get_size should use default object_set"),
            b"version-a".len()
        );

        store_a
            .remove("same-key", false, None)
            .expect("remove should use default object_set");
        assert!(
            store_a
                .get_size("same-key", None)
                .expect("removed object should be absent")
                == 0
        );
        Python::with_gil(|py| {
            assert_eq!(
                store_b
                    .get(py, "same-key", None)
                    .expect("other object_set should remain readable")
                    .as_bytes(),
                b"version-b"
            );
        });
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
            .unregister_buffer(registered.as_mut_ptr() as usize, Some(registered.len()))
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
            .unregister_buffer(source.as_ptr() as usize, Some(source.len()))
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
        let mut missing_target = vec![0u8; 16];
        let mut live_target = vec![0u8; 16];
        let mixed_lengths = store
            .batch_get_into(
                vec![
                    (
                        "missing".to_string(),
                        missing_target.as_mut_ptr() as usize,
                        missing_target.len(),
                    ),
                    (
                        "epsilon".to_string(),
                        live_target.as_mut_ptr() as usize,
                        live_target.len(),
                    ),
                ],
                None,
            )
            .expect("mixed batch_get_into should isolate soft misses");
        assert_eq!(mixed_lengths, vec![-1, source.len() as i64]);
        assert_eq!(&live_target[..source.len()], source.as_slice());
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
    fn real_batch_put_from_releases_gil_while_dispatcher_waits() {
        init_python();
        let (entered_tx, entered_rx) = std::sync::mpsc::channel();
        let release = Arc::new(AtomicBool::new(false));
        let metadata = Arc::new(BlockingHealthMetadata::new(
            Arc::new(AtomicBool::new(false)),
            Arc::new(AtomicBool::new(false)),
            Arc::new(AtomicBool::new(true)),
            release.clone(),
            entered_tx,
        ));
        let dispatcher = StoreDispatcher::spawn(
            build_client_with_metadata("py-batch-put-from-gil", metadata),
            "dispatcher-py-batch-put-from-gil",
        )
        .expect("dispatcher should spawn");
        dispatcher
            .register_local_memory()
            .expect("local memory should register");
        let mut store = PyMooncakeDistributedStore::new();
        store.replace_backend(StoreBackend::Real(dispatcher));

        let source = b"gil-safe-batch-put-from".to_vec();
        store
            .register_buffer(source.as_ptr() as usize, source.len())
            .expect("source buffer registration should succeed");

        let put_thread = std::thread::spawn(move || {
            Python::with_gil(|py| {
                let statuses = store
                    .batch_put_from(
                        py,
                        vec![(
                            "gil-batch-put-from".to_string(),
                            source.as_ptr() as usize,
                            source.len(),
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
                    .expect("batch_put_from should succeed");
                assert_eq!(
                    statuses
                        .bind(py)
                        .extract::<Vec<i32>>()
                        .expect("batch_put_from should return status list"),
                    vec![0]
                );
            });
        });

        entered_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("batch_put_from should block in metadata CAS");

        let release_gate = release.clone();
        let release_thread = std::thread::spawn(move || {
            sleep(Duration::from_millis(300));
            release_gate.store(true, Ordering::SeqCst);
        });

        let (elapsed_tx, elapsed_rx) = std::sync::mpsc::channel();
        let probe_thread = std::thread::spawn(move || {
            let start = std::time::Instant::now();
            Python::with_gil(|_| {});
            elapsed_tx
                .send(start.elapsed())
                .expect("probe elapsed should send");
        });

        let elapsed = elapsed_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("probe thread should observe GIL acquisition");
        assert!(
            elapsed < Duration::from_millis(150),
            "batch_put_from should release the GIL while waiting, observed {:?}",
            elapsed
        );

        probe_thread
            .join()
            .expect("probe thread should complete cleanly");
        release_thread
            .join()
            .expect("release thread should complete cleanly");
        put_thread
            .join()
            .expect("batch_put_from thread should complete cleanly");
    }

    #[test]
    fn real_batch_put_from_treats_existing_cache_key_conflict_as_success() {
        init_python();
        let (entered_tx, entered_rx) = std::sync::mpsc::channel();
        let release = Arc::new(AtomicBool::new(false));
        let metadata = Arc::new(BlockingHealthMetadata::new(
            Arc::new(AtomicBool::new(false)),
            Arc::new(AtomicBool::new(false)),
            Arc::new(AtomicBool::new(true)),
            release.clone(),
            entered_tx,
        ));
        let metadata: Arc<dyn MetadataBackend> = metadata;
        let store_a = build_real_store_with_dispatcher_scope_and_metadata(
            "py-cache-race-a",
            "default",
            metadata.clone(),
        );
        let store_b = build_real_store_with_dispatcher_scope_and_metadata(
            "py-cache-race-b",
            "default",
            metadata,
        );

        let payload_a = b"shared-cache-page".to_vec();
        let payload_b = b"shared-cache-page".to_vec();
        store_a
            .register_buffer(payload_a.as_ptr() as usize, payload_a.len())
            .expect("store-a source buffer registration should succeed");
        store_b
            .register_buffer(payload_b.as_ptr() as usize, payload_b.len())
            .expect("store-b source buffer registration should succeed");

        let barrier = Arc::new(Barrier::new(3));
        let thread_a = {
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                barrier.wait();
                Python::with_gil(|py| {
                    store_a
                        .batch_put_from(
                            py,
                            vec![(
                                "shared-cache-key".to_string(),
                                payload_a.as_ptr() as usize,
                                payload_a.len(),
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
                        .expect("store-a batch_put_from should succeed")
                        .bind(py)
                        .extract::<Vec<i32>>()
                        .expect("store-a statuses should decode")
                })
            })
        };
        let thread_b = {
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                barrier.wait();
                Python::with_gil(|py| {
                    store_b
                        .batch_put_from(
                            py,
                            vec![(
                                "shared-cache-key".to_string(),
                                payload_b.as_ptr() as usize,
                                payload_b.len(),
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
                        .expect("store-b batch_put_from should succeed")
                        .bind(py)
                        .extract::<Vec<i32>>()
                        .expect("store-b statuses should decode")
                })
            })
        };

        barrier.wait();
        entered_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("one writer should block in metadata CAS");
        sleep(Duration::from_millis(100));
        release.store(true, Ordering::SeqCst);

        let statuses_a = thread_a.join().expect("store-a thread should join");
        let statuses_b = thread_b.join().expect("store-b thread should join");
        assert_eq!(statuses_a, vec![0]);
        assert_eq!(statuses_b, vec![0]);
    }

    #[test]
    fn real_batch_put_from_multi_buffers_releases_gil_while_dispatcher_waits() {
        init_python();
        let (entered_tx, entered_rx) = std::sync::mpsc::channel();
        let release = Arc::new(AtomicBool::new(false));
        let metadata = Arc::new(BlockingHealthMetadata::new(
            Arc::new(AtomicBool::new(false)),
            Arc::new(AtomicBool::new(false)),
            Arc::new(AtomicBool::new(true)),
            release.clone(),
            entered_tx,
        ));
        let dispatcher = StoreDispatcher::spawn(
            build_client_with_metadata("py-batch-put-from-multi-gil", metadata),
            "dispatcher-py-batch-put-from-multi-gil",
        )
        .expect("dispatcher should spawn");
        dispatcher
            .register_local_memory()
            .expect("local memory should register");
        let mut store = PyMooncakeDistributedStore::new();
        store.replace_backend(StoreBackend::Real(dispatcher));

        let put_thread = std::thread::spawn(move || {
            Python::with_gil(|_| {
                assert_eq!(
                    store
                        .batch_put_from_multi_buffers(
                            vec![(
                                "gil-batch-put-from-multi".to_string(),
                                vec![b"gil".to_vec(), b"-safe".to_vec()],
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
                        .expect("batch_put_from_multi_buffers should succeed"),
                    0
                );
            });
        });

        entered_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("batch_put_from_multi_buffers should block in metadata CAS");

        let release_gate = release.clone();
        let release_thread = std::thread::spawn(move || {
            sleep(Duration::from_millis(300));
            release_gate.store(true, Ordering::SeqCst);
        });

        let (elapsed_tx, elapsed_rx) = std::sync::mpsc::channel();
        let probe_thread = std::thread::spawn(move || {
            let start = std::time::Instant::now();
            Python::with_gil(|_| {});
            elapsed_tx
                .send(start.elapsed())
                .expect("probe elapsed should send");
        });

        let elapsed = elapsed_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("probe thread should observe GIL acquisition");
        assert!(
            elapsed < Duration::from_millis(150),
            "batch_put_from_multi_buffers should release the GIL while waiting, observed {:?}",
            elapsed
        );

        probe_thread
            .join()
            .expect("probe thread should complete cleanly");
        release_thread
            .join()
            .expect("release thread should complete cleanly");
        put_thread
            .join()
            .expect("batch_put_from_multi_buffers thread should complete cleanly");
    }

    #[test]
    fn real_batch_get_into_releases_gil_while_dispatcher_waits() {
        init_python();
        let (entered_tx, entered_rx) = std::sync::mpsc::channel();
        let release = Arc::new(AtomicBool::new(false));
        let block_route_lookup = Arc::new(AtomicBool::new(true));
        let metadata = Arc::new(BlockingHealthMetadata::new_with_route_lookup(
            Arc::new(AtomicBool::new(false)),
            Arc::new(AtomicBool::new(false)),
            Arc::new(AtomicBool::new(false)),
            block_route_lookup,
            Arc::new(AtomicBool::new(false)),
            release.clone(),
            entered_tx,
        ));
        let dispatcher = StoreDispatcher::spawn(
            build_client_with_metadata("py-batch-get-into-gil", metadata),
            "dispatcher-py-batch-get-into-gil",
        )
        .expect("dispatcher should spawn");
        dispatcher
            .register_local_memory()
            .expect("local memory should register");
        let mut store = PyMooncakeDistributedStore::new();
        store.replace_backend(StoreBackend::Real(dispatcher));

        let get_thread = std::thread::spawn(move || {
            let mut target = vec![0u8; 32];
            Python::with_gil(|_| {
                assert_eq!(
                    store
                        .batch_get_into(
                            vec![(
                                "gil-batch-get-into-missing".to_string(),
                                target.as_mut_ptr() as usize,
                                target.len(),
                            )],
                            None,
                        )
                        .expect("batch_get_into should return a soft miss"),
                    vec![-1]
                );
            });
        });

        entered_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("batch_get_into should block in route lookup");

        let release_gate = release.clone();
        let release_thread = std::thread::spawn(move || {
            sleep(Duration::from_millis(300));
            release_gate.store(true, Ordering::SeqCst);
        });

        let (elapsed_tx, elapsed_rx) = std::sync::mpsc::channel();
        let probe_thread = std::thread::spawn(move || {
            let start = std::time::Instant::now();
            Python::with_gil(|_| {});
            elapsed_tx
                .send(start.elapsed())
                .expect("probe elapsed should send");
        });

        let elapsed = elapsed_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("probe thread should observe GIL acquisition");
        assert!(
            elapsed < Duration::from_millis(150),
            "batch_get_into should release the GIL while waiting, observed {:?}",
            elapsed
        );

        probe_thread
            .join()
            .expect("probe thread should complete cleanly");
        release_thread
            .join()
            .expect("release thread should complete cleanly");
        get_thread
            .join()
            .expect("batch_get_into thread should complete cleanly");
    }

    #[test]
    fn dispatcher_register_local_memory_uses_startup_timeout_not_request_timeout() {
        let (entered_tx, entered_rx) = std::sync::mpsc::channel();
        let release = Arc::new(AtomicBool::new(false));
        let metadata = Arc::new(BlockingHealthMetadata::new_with_publish_segment(
            Arc::new(AtomicBool::new(true)),
            release.clone(),
            entered_tx,
        ));
        let timeouts = CompatTimeoutConfig {
            request_timeout: Duration::from_millis(50),
            startup_timeout_override: Some(Duration::from_millis(400)),
            heartbeat_timeout: Duration::from_secs(1),
            transfer_stall_timeout: Duration::from_secs(1),
            dummy_rpc_timeout: Duration::from_millis(50),
        };
        let dispatcher = Arc::new(
            StoreDispatcher::spawn_with_timeout_config(
                build_client_with_metadata("dispatcher-startup-timeout-success", metadata),
                "dispatcher-startup-timeout-success".to_string(),
                timeouts,
            )
            .expect("dispatcher should spawn"),
        );

        let worker = std::thread::Builder::new()
            .name("dispatcher-startup-timeout-success".to_string())
            .spawn({
                let dispatcher = dispatcher.clone();
                move || dispatcher.register_local_memory()
            })
            .expect("startup registration worker should spawn");

        entered_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("startup registration should enter metadata publish");
        sleep(Duration::from_millis(200));
        release.store(true, Ordering::SeqCst);

        worker
            .join()
            .expect("startup registration worker should join")
            .expect("startup registration should outlive request timeout");
    }

    #[test]
    fn dispatcher_register_local_memory_times_out_with_startup_budget() {
        let (entered_tx, entered_rx) = std::sync::mpsc::channel();
        let release = Arc::new(AtomicBool::new(false));
        let metadata = Arc::new(BlockingHealthMetadata::new_with_publish_segment(
            Arc::new(AtomicBool::new(true)),
            release.clone(),
            entered_tx,
        ));
        let timeouts = CompatTimeoutConfig {
            request_timeout: Duration::from_secs(5),
            startup_timeout_override: Some(Duration::from_millis(120)),
            heartbeat_timeout: Duration::from_secs(1),
            transfer_stall_timeout: Duration::from_secs(1),
            dummy_rpc_timeout: Duration::from_secs(5),
        };
        let dispatcher = Arc::new(
            StoreDispatcher::spawn_with_timeout_config(
                build_client_with_metadata("dispatcher-startup-timeout-fail", metadata),
                "dispatcher-startup-timeout-fail".to_string(),
                timeouts,
            )
            .expect("dispatcher should spawn"),
        );

        let worker = std::thread::Builder::new()
            .name("dispatcher-startup-timeout-fail".to_string())
            .spawn({
                let dispatcher = dispatcher.clone();
                move || dispatcher.register_local_memory()
            })
            .expect("startup registration worker should spawn");

        entered_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("startup registration should enter metadata publish");
        let error = worker
            .join()
            .expect("startup registration worker should join")
            .expect_err("startup registration should time out");
        assert!(
            matches!(error, StoreError::Transport(ref message) if message.contains("startup registration timed out after 120ms")),
            "unexpected error: {error}"
        );
        release.store(true, Ordering::SeqCst);
    }

    #[test]
    fn dispatcher_buffer_registration_timeout_uses_explicit_override() {
        let timeouts = CompatTimeoutConfig {
            request_timeout: Duration::from_millis(50),
            startup_timeout_override: Some(Duration::from_millis(400)),
            heartbeat_timeout: Duration::from_secs(1),
            transfer_stall_timeout: Duration::from_secs(1),
            dummy_rpc_timeout: Duration::from_millis(50),
        };
        let dispatcher = StoreDispatcher::spawn_with_timeout_config(
            build_client("dispatcher-buffer-timeout-override"),
            "dispatcher-buffer-timeout-override".to_string(),
            timeouts,
        )
        .expect("dispatcher should spawn");
        assert_eq!(
            dispatcher.registration_timeout_for_bytes(64),
            Duration::from_millis(400)
        );
    }

    #[test]
    fn dispatcher_buffer_registration_timeout_uses_adaptive_floor_without_override() {
        let timeouts = CompatTimeoutConfig {
            request_timeout: Duration::from_millis(50),
            startup_timeout_override: None,
            heartbeat_timeout: Duration::from_secs(1),
            transfer_stall_timeout: Duration::from_secs(1),
            dummy_rpc_timeout: Duration::from_millis(50),
        };
        let dispatcher = StoreDispatcher::spawn_with_timeout_config(
            build_client("dispatcher-buffer-timeout-floor"),
            "dispatcher-buffer-timeout-floor".to_string(),
            timeouts,
        )
        .expect("dispatcher should spawn");
        assert_eq!(
            dispatcher.registration_timeout_for_bytes(64),
            Duration::from_secs(20)
        );
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

        for (key, value) in [
            ("alpha", b"one".to_vec()),
            ("delta", b"four".to_vec()),
            ("epsilon", b"five".to_vec()),
        ] {
            store
                .put(
                    key, value, None, None, None, None, None, None, true, false, false,
                )
                .expect("dummy put should succeed");
        }
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
            let batch = store
                .batch_get(
                    py,
                    vec!["alpha".to_string(), "beta".to_string(), "delta".to_string()],
                    None,
                )
                .expect("dummy batch_get should succeed");
            assert_eq!(batch[0].bind(py).as_bytes(), b"one");
            assert_eq!(batch[1].bind(py).as_bytes(), b"two");
            assert_eq!(batch[2].bind(py).as_bytes(), b"four");
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
            .unregister_buffer(read_ptr, Some(64))
            .expect("dummy unregister_buffer should succeed");
        assert!(store.get_into("buffered", read_ptr, 64, None).is_err());

        let removed = store
            .remove_all(false)
            .expect("dummy remove_all should succeed");
        assert!(removed >= 1);
        // After remove_all, the key is gone. force=false returns -1 status.
        let status = store
            .remove("alpha", false, None)
            .expect("dummy remove should succeed");
        assert_eq!(status, -1);
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
        let server = start_dummy_store_server(dispatcher.clone(), &bind_addr(), "shared-hot-cache")
            .expect("server should start");
        let dummy_one = DummySession::connect(server.address(), "shared-hot-cache")
            .expect("dummy one should connect");
        let dummy_two = DummySession::connect(server.address(), "shared-hot-cache")
            .expect("dummy two should connect");
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

        let dispatcher_for_remove = dispatcher
            .fork_with_scope("shared-hot-cache")
            .expect("scoped dispatcher should fork");
        dispatcher_for_remove
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
    fn dummy_worker_scope_isolates_hot_cache_shm_hits() {
        let _guard = env_test_lock().lock();
        let _cache_size = EnvVarGuard::set("MC_STORE_LOCAL_HOT_CACHE_SIZE", "4096");
        let _block_size = EnvVarGuard::set("MC_STORE_LOCAL_HOT_BLOCK_SIZE", "1024");
        let _use_shm = EnvVarGuard::set("MC_STORE_LOCAL_HOT_CACHE_USE_SHM", "1");

        let dispatcher = Arc::new(
            StoreDispatcher::spawn(
                build_client("dummy-hot-cache-isolated"),
                "dummy-hot-cache-isolated",
            )
            .expect("dispatcher should spawn"),
        );
        dispatcher
            .register_local_memory()
            .expect("local memory should register");
        let server = start_dummy_store_server(dispatcher.clone(), &bind_addr(), "scope-a")
            .expect("server should start");
        let dummy_a = DummySession::connect(server.address(), "scope-a")
            .expect("scope a client should connect");
        let dummy_b = DummySession::connect(server.address(), "scope-b")
            .expect("scope b client should connect");
        assert!(dummy_a.has_hot_cache_mapping());
        assert!(!dummy_b.has_hot_cache_mapping());

        let address = bind_addr();
        let server_b = start_dummy_store_server(dispatcher.clone(), &address, "scope-b")
            .expect("scope b server should start");
        let dummy_b_scoped = DummySession::connect(server_b.address(), "scope-b")
            .expect("scope b scoped client should connect");
        assert!(dummy_b_scoped.has_hot_cache_mapping());

        dispatcher
            .run(|client| client.put("alpha", b"one"))
            .expect("put should succeed");
        let (status, value) = dummy_a.get("alpha", None).expect("scope a get should work");
        assert_eq!(status, 0);
        assert_eq!(value, b"one");

        let dispatcher_for_remove = dispatcher
            .fork_with_scope("scope-a")
            .expect("scoped dispatcher should fork");
        dispatcher_for_remove
            .run(|client| client.remove("alpha", true))
            .expect("raw remove should succeed");
        let (status, value) = dummy_b_scoped
            .get("alpha", None)
            .expect("scope b scoped get should complete");
        assert_eq!(status, -1);
        assert!(value.is_empty());

        dummy_a.close();
        dummy_b.close();
        dummy_b_scoped.close();
        server.shutdown().expect("server should stop");
        server_b.shutdown().expect("scope b server should stop");
    }

    #[test]
    fn real_hot_cache_isolated_by_compat_scope() {
        let _guard = env_test_lock().lock();
        let _cache_size = EnvVarGuard::set("MC_STORE_LOCAL_HOT_CACHE_SIZE", "4096");
        let _block_size = EnvVarGuard::set("MC_STORE_LOCAL_HOT_BLOCK_SIZE", "1024");
        let _use_shm = EnvVarGuard::unset("MC_STORE_LOCAL_HOT_CACHE_USE_SHM");
        init_python();

        let metadata = Arc::new(InMemoryMetadataBackend::new());
        let mut store_a = build_real_store_with_dispatcher_scope_and_metadata(
            "py-real-hot-cache-scope-a",
            "scope/a",
            metadata.clone(),
        );
        let mut store_b = build_real_store_with_dispatcher_scope_and_metadata(
            "py-real-hot-cache-scope-b",
            "scope/b",
            metadata,
        );

        store_a
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
            .expect("scoped put should succeed");

        Python::with_gil(|py| {
            assert_eq!(
                store_a
                    .get(py, "alpha", None)
                    .expect("scope a get should succeed")
                    .as_bytes(),
                b"one"
            );
        });

        let dispatcher_a = store_a
            .real_dispatcher()
            .expect("scope a dispatcher should exist");
        let dispatcher_b = store_b
            .real_dispatcher()
            .expect("scope b dispatcher should exist");
        assert!(dispatcher_a.hot_cache_contains_in_scope("scope/a", "default", "alpha"));
        assert!(!dispatcher_a.hot_cache_contains_in_scope("scope/b", "default", "alpha"));
        assert!(!dispatcher_b.hot_cache_contains_in_scope("scope/b", "default", "alpha"));
        assert!(!dispatcher_b.hot_cache_contains_in_scope("scope/a", "default", "alpha"));

        store_a.close();
        store_b.close();
    }

    #[test]
    fn dummy_rpc_returns_timeout_instead_of_hanging() {
        let server = start_blocking_dummy_server();
        let session = DummySession::connect_with_rpc_timeout(
            server.address(),
            "blocking-worker",
            Duration::from_millis(200),
        )
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
        let dispatcher = Arc::new(
            StoreDispatcher::spawn_with_timeouts(
                build_client("dispatcher-timeout"),
                "dispatcher-timeout".to_string(),
                Duration::from_millis(200),
                Duration::from_secs(15),
            )
            .expect("dispatcher should spawn"),
        );
        let (entered_tx, entered_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let started = std::time::Instant::now();
        let worker = std::thread::Builder::new()
            .name("dispatcher-timeout-test".to_string())
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
    fn dispatcher_async_shared_requests_do_not_head_of_line_block() {
        let dispatcher = Arc::new(
            StoreDispatcher::spawn(
                build_client("dispatcher-async-shared"),
                "dispatcher-async-shared".to_string(),
            )
            .expect("dispatcher should spawn"),
        );
        let (entered_tx, entered_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let worker = std::thread::Builder::new()
            .name("dispatcher-async-shared-holder".to_string())
            .spawn({
                let dispatcher = dispatcher.clone();
                move || {
                    let runtime =
                        tokio::runtime::Runtime::new().expect("foreign runtime should build");
                    runtime.block_on(async move {
                        dispatcher
                            .run_async(move |_client| {
                                let _ = entered_tx.send(());
                                let _ = release_rx.recv();
                                Ok::<_, StoreError>(())
                            })
                            .await
                    })
                }
            })
            .expect("dispatcher async shared test worker should spawn");

        entered_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("first async shared request should enter");

        let started = std::time::Instant::now();
        let runtime = tokio::runtime::Runtime::new().expect("foreign runtime should build");
        let second = runtime
            .block_on(async {
                dispatcher
                    .run_async(|_client| Ok::<_, StoreError>(17usize))
                    .await
            })
            .expect("second async shared request should not block behind first");
        assert_eq!(second, 17);
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "second async shared request should complete quickly"
        );

        let _ = release_tx.send(());
        worker
            .join()
            .expect("dispatcher async shared worker should join")
            .expect("first async shared request should finish after release");
    }

    #[test]
    fn dispatcher_worker_runtimes_are_isolated() {
        let dispatcher_a = StoreDispatcher::spawn(
            build_client("dispatcher-runtime-a"),
            "dispatcher-runtime-a".to_string(),
        )
        .expect("dispatcher a should spawn");
        let dispatcher_b = StoreDispatcher::spawn(
            build_client("dispatcher-runtime-b"),
            "dispatcher-runtime-b".to_string(),
        )
        .expect("dispatcher b should spawn");

        let (entered_tx, entered_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let worker = std::thread::Builder::new()
            .name("dispatcher-runtime-a-holder".to_string())
            .spawn(move || {
                dispatcher_a.run(move |_client| {
                    let _ = entered_tx.send(());
                    let _ = release_rx.recv();
                    Ok::<_, StoreError>(())
                })
            })
            .expect("dispatcher runtime holder should spawn");

        entered_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("dispatcher a request should enter");
        let started = std::time::Instant::now();
        let second = dispatcher_b
            .run(|_client| Ok::<_, StoreError>(11usize))
            .expect("dispatcher b should stay responsive");
        assert_eq!(second, 11);
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "dispatcher b should not block behind dispatcher a"
        );
        let _ = release_tx.send(());
        worker
            .join()
            .expect("dispatcher runtime holder should join")
            .expect("dispatcher a request should complete");
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
    fn finalize_real_dispatcher_setup_starts_auto_heartbeat_loop() {
        let metadata = Arc::new(InMemoryMetadataBackend::new());
        let lease_ttl_ms = 1_500;
        let expires_at_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time should move forward")
            .as_millis() as u64
            + lease_ttl_ms;
        let dispatcher = StoreDispatcher::spawn(
            build_client_with_metadata_and_expiry(
                "dispatcher-setup-auto-heartbeat",
                metadata.clone(),
                ClientLifecycleState::Active,
                expires_at_ms,
            ),
            "dispatcher-setup-auto-heartbeat".to_string(),
        )
        .expect("dispatcher should spawn");
        finalize_real_dispatcher_setup(&dispatcher, ClientLifecycleState::Active, lease_ttl_ms)
            .expect("setup finalization should succeed");

        let runtime = ClientRuntimeId::new("dispatcher-setup-auto-heartbeat", ClientEpoch(1));
        let expires_after_setup = lease_expires_at_ms(&metadata, &runtime);
        let refreshed = wait_until_lease_after(&metadata, &runtime, expires_after_setup);
        dispatcher.stop_heartbeat_loop();

        assert!(
            refreshed,
            "setup finalization should start the background heartbeat loop"
        );
        dispatcher.shutdown();
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
        let block_enabled = Arc::new(AtomicBool::new(false));
        let release = Arc::new(AtomicBool::new(false));
        let metadata = Arc::new(BlockingHealthMetadata::new(
            block_enabled.clone(),
            Arc::new(AtomicBool::new(false)),
            Arc::new(AtomicBool::new(false)),
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
        block_enabled.store(true, Ordering::SeqCst);

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

    #[test]
    fn dummy_get_into_ranges_basic() {
        init_python();
        let (store, _server) = build_dummy_store("py-ranges");

        let data1 = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";
        let data2 = b"abcdefghijklmnopqrstuvwxyz0123456789";
        store
            .put(
                "key1",
                data1.to_vec(),
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
            .expect("put key1 should succeed");
        store
            .put(
                "key2",
                data2.to_vec(),
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
            .expect("put key2 should succeed");

        let allocator = PyMooncakeHostMemAllocator::new(None, None);
        let buf0 = allocator.alloc(32).expect("alloc buf0");
        let buf1 = allocator.alloc(32).expect("alloc buf1");
        store.register_buffer(buf0, 32).expect("register buf0");
        store.register_buffer(buf1, 32).expect("register buf1");

        unsafe {
            std::ptr::write_bytes(buf0 as *mut u8, b'_', 32);
            std::ptr::write_bytes(buf1 as *mut u8, b'_', 32);
        }

        let results = store
            .get_into_ranges(
                vec![buf0, buf1],
                vec![
                    vec!["key1".to_string(), "key2".to_string()],
                    vec!["key2".to_string(), "key1".to_string()],
                ],
                vec![vec![vec![0, 20], vec![8]], vec![vec![4], vec![16]]],
                vec![vec![vec![2, 30], vec![10]], vec![vec![0], vec![12]]],
                vec![vec![vec![4, 3], vec![6]], vec![vec![6], vec![4]]],
                Some(vec![32, 32]),
                None,
            )
            .expect("get_into_ranges should succeed");

        assert_eq!(
            results,
            vec![vec![vec![4, 3], vec![6]], vec![vec![6], vec![4]]]
        );

        unsafe {
            assert_eq!(slice::from_raw_parts(buf0 as *const u8, 4), &data1[2..6]);
            assert_eq!(
                slice::from_raw_parts((buf0 + 8) as *const u8, 6),
                &data2[10..16]
            );
            assert_eq!(
                slice::from_raw_parts((buf0 + 20) as *const u8, 3),
                &data1[30..33]
            );
            assert_eq!(
                slice::from_raw_parts((buf1 + 4) as *const u8, 6),
                &data2[0..6]
            );
            assert_eq!(
                slice::from_raw_parts((buf1 + 16) as *const u8, 4),
                &data1[12..16]
            );
        }

        store
            .unregister_buffer(buf0, Some(32))
            .expect("unregister buf0");
        store
            .unregister_buffer(buf1, Some(32))
            .expect("unregister buf1");
    }

    #[test]
    fn dummy_get_into_ranges_shape_mismatch() {
        init_python();
        let (store, _server) = build_dummy_store("py-ranges-mismatch");

        store
            .put(
                "key1",
                b"hello world".to_vec(),
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

        let allocator = PyMooncakeHostMemAllocator::new(None, None);
        let buf0 = allocator.alloc(32).expect("alloc buf0");
        store.register_buffer(buf0, 32).expect("register buf0");

        // Fragment count mismatch: dst_offsets has 1 but src_offsets has 2
        let results = store
            .get_into_ranges(
                vec![buf0],
                vec![vec!["key1".to_string()]],
                vec![vec![vec![0]]],
                vec![vec![vec![0, 1]]],
                vec![vec![vec![4, 4]]],
                Some(vec![32]),
                None,
            )
            .expect("get_into_ranges should succeed with partial errors");
        assert!(results[0][0][0] < 0);

        // Source overflow: src_offset + size > object_length
        let results = store
            .get_into_ranges(
                vec![buf0],
                vec![vec!["key1".to_string()]],
                vec![vec![vec![0]]],
                vec![vec![vec![10]]],
                vec![vec![vec![4]]],
                Some(vec![32]),
                None,
            )
            .expect("get_into_ranges should return error for source overflow");
        assert!(results[0][0][0] < 0);

        // Destination overflow: dst_offset + size > buffer_size
        let results = store
            .get_into_ranges(
                vec![buf0],
                vec![vec!["key1".to_string()]],
                vec![vec![vec![30]]],
                vec![vec![vec![0]]],
                vec![vec![vec![4]]],
                Some(vec![32]),
                None,
            )
            .expect("get_into_ranges should return error for dest overflow");
        assert!(results[0][0][0] < 0);

        // Missing key partial failure
        let results = store
            .get_into_ranges(
                vec![buf0],
                vec![vec!["missing-key".to_string(), "key1".to_string()]],
                vec![vec![vec![0], vec![8]]],
                vec![vec![vec![0], vec![0]]],
                vec![vec![vec![4], vec![4]]],
                Some(vec![32]),
                None,
            )
            .expect("missing key should return partial failure");
        assert!(results[0][0][0] < 0);
        assert_eq!(results[0][1][0], 4);

        // Zero-size read should succeed with 0 bytes
        let results = store
            .get_into_ranges(
                vec![buf0],
                vec![vec!["key1".to_string()]],
                vec![vec![vec![0]]],
                vec![vec![vec![0]]],
                vec![vec![vec![0]]],
                Some(vec![32]),
                None,
            )
            .expect("zero-size read should succeed");
        assert_eq!(results[0][0][0], 0);

        store
            .unregister_buffer(buf0, Some(32))
            .expect("unregister buf0");
    }

    /// TP 4→8 split: Trainer produces 4 shards (256 bytes each), Rollouter has 8 ranks
    /// each reading 128 bytes from one Trainer shard with src_offset = (rank % 2) * 128.
    #[test]
    fn dummy_get_into_ranges_tp_split() {
        init_python();
        let (store, _server) = build_dummy_store("py-ranges-tp-split");

        const SHARD_SIZE: usize = 256;
        const TRAINER_TP: usize = 4;
        const ROLLOUTER_TP: usize = 8;
        const RANK_SIZE: usize = SHARD_SIZE * TRAINER_TP / ROLLOUTER_TP; // 128

        // Trainer writes 4 shards with recognizable patterns
        for shard_id in 0..TRAINER_TP {
            let data: Vec<u8> = (0..SHARD_SIZE)
                .map(|i| ((shard_id + i) & 0xFF) as u8)
                .collect();
            let key = format!("layer.0.weight.tp{}", shard_id);
            store
                .put(
                    &key, data, None, None, None, None, None, None, true, false, false,
                )
                .expect("put trainer shard");
        }

        // Allocate and register one buffer per Rollouter rank
        let allocator = PyMooncakeHostMemAllocator::new(None, None);
        let mut buffer_ptrs: Vec<usize> = Vec::new();
        for _ in 0..ROLLOUTER_TP {
            let buf = allocator.alloc(RANK_SIZE).expect("alloc buffer");
            store
                .register_buffer(buf, RANK_SIZE)
                .expect("register buffer");
            unsafe {
                std::ptr::write_bytes(buf as *mut u8, 0xFF, RANK_SIZE);
            }
            buffer_ptrs.push(buf);
        }

        // Build get_into_ranges parameters
        // Each Rollouter rank reads from exactly 1 Trainer shard
        let mut all_keys: Vec<Vec<String>> = Vec::new();
        let mut all_dst_offsets: Vec<Vec<Vec<usize>>> = Vec::new();
        let mut all_src_offsets: Vec<Vec<Vec<usize>>> = Vec::new();
        let mut all_sizes: Vec<Vec<Vec<usize>>> = Vec::new();

        for rank in 0..ROLLOUTER_TP {
            let trainer_shard = rank / 2;
            let src_offset = (rank % 2) * RANK_SIZE;
            let key = format!("layer.0.weight.tp{}", trainer_shard);

            all_keys.push(vec![key]);
            all_dst_offsets.push(vec![vec![0]]);
            all_src_offsets.push(vec![vec![src_offset]]);
            all_sizes.push(vec![vec![RANK_SIZE]]);
        }

        let buffer_sizes: Vec<usize> = vec![RANK_SIZE; ROLLOUTER_TP];

        let results = store
            .get_into_ranges(
                buffer_ptrs.clone(),
                all_keys,
                all_dst_offsets,
                all_src_offsets,
                all_sizes,
                Some(buffer_sizes),
                None,
            )
            .expect("get_into_ranges should succeed");

        // Verify: all results should be positive (success = bytes read)
        for (rank, buf_results) in results.iter().enumerate() {
            for (key_idx, key_results) in buf_results.iter().enumerate() {
                for (frag_idx, &val) in key_results.iter().enumerate() {
                    assert!(
                        val > 0,
                        "rank {} key {} frag {} failed with {}",
                        rank,
                        key_idx,
                        frag_idx,
                        val
                    );
                }
            }
        }

        // Verify buffer contents match expected data
        for rank in 0..ROLLOUTER_TP {
            let trainer_shard = rank / 2;
            let src_offset = (rank % 2) * RANK_SIZE;
            let expected: Vec<u8> = (0..RANK_SIZE)
                .map(|i| ((trainer_shard + src_offset + i) & 0xFF) as u8)
                .collect();
            let actual =
                unsafe { slice::from_raw_parts(buffer_ptrs[rank] as *const u8, RANK_SIZE) };
            assert_eq!(
                actual,
                expected.as_slice(),
                "rank {} buffer content mismatch",
                rank
            );
        }

        for &buf in &buffer_ptrs {
            store
                .unregister_buffer(buf, Some(RANK_SIZE))
                .expect("unregister buffer");
        }
    }

    /// TP 8→4 merge: Trainer produces 8 shards (128 bytes each), Rollouter has 4 ranks
    /// each reading 2 full Trainer shards concatenated into one 256-byte buffer.
    #[test]
    fn dummy_get_into_ranges_tp_merge() {
        init_python();
        let (store, _server) = build_dummy_store("py-ranges-tp-merge");

        const SHARD_SIZE: usize = 128;
        const TRAINER_TP: usize = 8;
        const ROLLOUTER_TP: usize = 4;
        const RANK_SIZE: usize = SHARD_SIZE * TRAINER_TP / ROLLOUTER_TP; // 256

        // Trainer writes 8 shards with recognizable patterns
        for shard_id in 0..TRAINER_TP {
            let data: Vec<u8> = (0..SHARD_SIZE)
                .map(|i| ((shard_id + i) & 0xFF) as u8)
                .collect();
            let key = format!("layer.0.weight.tp{}", shard_id);
            store
                .put(
                    &key, data, None, None, None, None, None, None, true, false, false,
                )
                .expect("put trainer shard");
        }

        // Allocate and register one buffer per Rollouter rank
        let allocator = PyMooncakeHostMemAllocator::new(None, None);
        let mut buffer_ptrs: Vec<usize> = Vec::new();
        for _ in 0..ROLLOUTER_TP {
            let buf = allocator.alloc(RANK_SIZE).expect("alloc buffer");
            store
                .register_buffer(buf, RANK_SIZE)
                .expect("register buffer");
            unsafe {
                std::ptr::write_bytes(buf as *mut u8, 0xFF, RANK_SIZE);
            }
            buffer_ptrs.push(buf);
        }

        // Build get_into_ranges parameters
        // Each Rollouter rank reads from 2 Trainer shards, concatenating them
        let mut all_keys: Vec<Vec<String>> = Vec::new();
        let mut all_dst_offsets: Vec<Vec<Vec<usize>>> = Vec::new();
        let mut all_src_offsets: Vec<Vec<Vec<usize>>> = Vec::new();
        let mut all_sizes: Vec<Vec<Vec<usize>>> = Vec::new();

        for rank in 0..ROLLOUTER_TP {
            let shard_a = 2 * rank;
            let shard_b = 2 * rank + 1;
            let key_a = format!("layer.0.weight.tp{}", shard_a);
            let key_b = format!("layer.0.weight.tp{}", shard_b);

            all_keys.push(vec![key_a, key_b]);
            all_dst_offsets.push(vec![vec![0], vec![SHARD_SIZE]]);
            all_src_offsets.push(vec![vec![0], vec![0]]);
            all_sizes.push(vec![vec![SHARD_SIZE], vec![SHARD_SIZE]]);
        }

        let buffer_sizes: Vec<usize> = vec![RANK_SIZE; ROLLOUTER_TP];

        let results = store
            .get_into_ranges(
                buffer_ptrs.clone(),
                all_keys,
                all_dst_offsets,
                all_src_offsets,
                all_sizes,
                Some(buffer_sizes),
                None,
            )
            .expect("get_into_ranges should succeed");

        // Verify: all results positive
        for (rank, buf_results) in results.iter().enumerate() {
            for (key_idx, key_results) in buf_results.iter().enumerate() {
                for (frag_idx, &val) in key_results.iter().enumerate() {
                    assert!(
                        val > 0,
                        "rank {} key {} frag {} failed with {}",
                        rank,
                        key_idx,
                        frag_idx,
                        val
                    );
                }
            }
        }

        // Verify buffer contents: rank buffer = [shard_a data | shard_b data]
        for rank in 0..ROLLOUTER_TP {
            let shard_a = 2 * rank;
            let shard_b = 2 * rank + 1;

            let expected_a: Vec<u8> = (0..SHARD_SIZE)
                .map(|i| ((shard_a + i) & 0xFF) as u8)
                .collect();
            let expected_b: Vec<u8> = (0..SHARD_SIZE)
                .map(|i| ((shard_b + i) & 0xFF) as u8)
                .collect();

            let actual =
                unsafe { slice::from_raw_parts(buffer_ptrs[rank] as *const u8, RANK_SIZE) };
            assert_eq!(
                &actual[..SHARD_SIZE],
                expected_a.as_slice(),
                "rank {} first half mismatch",
                rank
            );
            assert_eq!(
                &actual[SHARD_SIZE..],
                expected_b.as_slice(),
                "rank {} second half mismatch",
                rank
            );
        }

        for &buf in &buffer_ptrs {
            store
                .unregister_buffer(buf, Some(RANK_SIZE))
                .expect("unregister buffer");
        }
    }
}
