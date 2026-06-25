//! PyO3 parallel tensor API for `MooncakeDistributedStore`.
//!
//! Adds parallelism-aware put/get operations that handle shard key naming,
//! metadata tagging, and cross-TP scatter-gather reconstruction.

use mooncake_store_client::ReadQueryResultCache;
use mooncake_tensor::{
    build_raw_shard_write_plan, calculate_shard_range, calculate_strided_shard_ranges,
    get_parallelism_key_name, get_parallelism_manifest_key, get_writer_partition_key_name,
    parallelism_matches_metadata, validate_uniform_shard, ParallelAxisKind, ParallelAxisSpec,
    ParsedTensorMetadata, RawShardWritePlan, ReadTargetMode, ReadTargetSpec, TensorDtype,
    TensorMetadata, TensorParallelismSpec, WriterPartitionSpec, WriterShardManifest,
};
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;

use crate::tensor::{parse_dtype_str, TensorReadResult};
use crate::{pointer_from_usize, PyMooncakeDistributedStore};

// ─── Python-exposed parallelism types ──────────────────────────────────────

#[pyclass(name = "ParallelAxis")]
#[derive(Debug, Clone)]
pub struct PyParallelAxis {
    #[pyo3(get)]
    pub kind: i32,
    #[pyo3(get)]
    pub rank: i32,
    #[pyo3(get)]
    pub size: i32,
    #[pyo3(get)]
    pub split_dim: i32,
    #[pyo3(get)]
    pub expert_id: i32,
    #[pyo3(get)]
    pub stage_id: i32,
}

#[pymethods]
impl PyParallelAxis {
    #[new]
    #[pyo3(signature = (kind, rank, size, *, split_dim = 0, expert_id = 0, stage_id = 0))]
    fn new(kind: i32, rank: i32, size: i32, split_dim: i32, expert_id: i32, stage_id: i32) -> Self {
        Self {
            kind,
            rank,
            size,
            split_dim,
            expert_id,
            stage_id,
        }
    }

    fn __repr__(&self) -> String {
        let kind_str = match ParallelAxisKind::from_i32(self.kind) {
            Some(k) => format!("{k}"),
            None => format!("?{}", self.kind),
        };
        format!(
            "ParallelAxis(kind={kind_str}, rank={}, size={})",
            self.rank, self.size
        )
    }
}

impl PyParallelAxis {
    fn to_spec(&self) -> PyResult<ParallelAxisSpec> {
        let kind = ParallelAxisKind::from_i32(self.kind)
            .ok_or_else(|| PyValueError::new_err(format!("invalid axis kind: {}", self.kind)))?;
        let spec = ParallelAxisSpec {
            kind,
            rank: self.rank,
            size: self.size,
            split_dim: self.split_dim,
            expert_id: self.expert_id,
            stage_id: self.stage_id,
        };
        spec.validate()
            .map_err(|e| PyValueError::new_err(e.to_string()))?;
        Ok(spec)
    }
}

#[pyclass(name = "TensorParallelism")]
#[derive(Debug, Clone)]
pub struct PyTensorParallelism {
    #[pyo3(get)]
    pub axes: Vec<PyParallelAxis>,
}

#[pymethods]
impl PyTensorParallelism {
    #[new]
    fn new(axes: Vec<PyParallelAxis>) -> Self {
        Self { axes }
    }

    fn __repr__(&self) -> String {
        let axes_str: Vec<String> = self.axes.iter().map(|a| a.__repr__()).collect();
        format!("TensorParallelism([{}])", axes_str.join(", "))
    }
}

impl PyTensorParallelism {
    fn to_spec(&self) -> PyResult<TensorParallelismSpec> {
        let axes: Vec<ParallelAxisSpec> = self
            .axes
            .iter()
            .map(|a| a.to_spec())
            .collect::<PyResult<Vec<_>>>()?;
        let spec = TensorParallelismSpec::new(axes);
        spec.validate()
            .map_err(|e| PyValueError::new_err(e.to_string()))?;
        Ok(spec)
    }
}

#[pyclass(name = "ReadTarget")]
#[derive(Debug, Clone)]
pub struct PyReadTarget {
    #[pyo3(get)]
    pub mode: i32,
    #[pyo3(get)]
    pub parallelism: Option<PyTensorParallelism>,
}

#[pymethods]
impl PyReadTarget {
    #[new]
    #[pyo3(signature = (mode, *, parallelism = None))]
    fn new(mode: i32, parallelism: Option<PyTensorParallelism>) -> Self {
        Self { mode, parallelism }
    }
}

impl PyReadTarget {
    fn to_spec(&self) -> PyResult<ReadTargetSpec> {
        let mode = ReadTargetMode::from_i32(self.mode).ok_or_else(|| {
            PyValueError::new_err(format!("invalid read target mode: {}", self.mode))
        })?;
        let parallelism = self.parallelism.as_ref().map(|p| p.to_spec()).transpose()?;
        Ok(ReadTargetSpec { mode, parallelism })
    }
}

// ─── Constants exposed to Python ───────────────────────────────────────────

#[pyfunction(name = "AXIS_DP")]
pub fn axis_dp() -> i32 {
    ParallelAxisKind::DP as i32
}

#[pyfunction(name = "AXIS_TP")]
pub fn axis_tp() -> i32 {
    ParallelAxisKind::TP as i32
}

#[pyfunction(name = "AXIS_EP")]
pub fn axis_ep() -> i32 {
    ParallelAxisKind::EP as i32
}

#[pyfunction(name = "AXIS_PP")]
pub fn axis_pp() -> i32 {
    ParallelAxisKind::PP as i32
}

#[pyfunction(name = "READ_MODE_AS_STORED")]
pub fn read_mode_as_stored() -> i32 {
    ReadTargetMode::AsStored as i32
}

#[pyfunction(name = "READ_MODE_SHARD")]
pub fn read_mode_shard() -> i32 {
    ReadTargetMode::Shard as i32
}

#[pyfunction(name = "READ_MODE_FULL")]
pub fn read_mode_full() -> i32 {
    ReadTargetMode::Full as i32
}

// ─── Tensor info extraction (shared with tensor.rs) ────────────────────────

struct TensorInfo {
    data_ptr: usize,
    data_bytes: usize,
    shape: Vec<i64>,
    dtype: mooncake_tensor::TensorDtype,
}

fn extract_tensor_info(tensor: &Bound<'_, PyAny>) -> PyResult<TensorInfo> {
    let data_ptr: usize = tensor.call_method0("data_ptr")?.extract()?;
    let numel: usize = tensor.call_method0("numel")?.extract()?;
    let element_size: usize = tensor.call_method0("element_size")?.extract()?;
    let data_bytes = numel
        .checked_mul(element_size)
        .ok_or_else(|| PyValueError::new_err("tensor data size overflows usize"))?;

    let shape_tuple = tensor.getattr("shape")?;
    let shape_len: usize = shape_tuple.len()?;
    let mut shape = Vec::with_capacity(shape_len);
    for i in 0..shape_len {
        shape.push(shape_tuple.get_item(i)?.extract::<i64>()?);
    }

    let dtype_obj = tensor.getattr("dtype")?;
    let dtype_str = dtype_obj.str()?.to_string();
    let dtype = parse_dtype_str(&dtype_str)
        .ok_or_else(|| PyValueError::new_err(format!("unsupported tensor dtype: {dtype_str}")))?;

    let is_contiguous: bool = tensor.call_method0("is_contiguous")?.extract()?;
    if !is_contiguous {
        return Err(PyValueError::new_err(
            "tensor must be contiguous; call .contiguous() first",
        ));
    }

    Ok(TensorInfo {
        data_ptr,
        data_bytes,
        shape,
        dtype,
    })
}

// ─── Write route ───────────────────────────────────────────────────────────

enum WriteRoute {
    DirectFull,
    LegacySingleTp,
    MultiAxisParallelism,
    WriterPartition,
}

fn resolve_write_route(
    parallelism: &Option<TensorParallelismSpec>,
    writer_partition: &Option<WriterPartitionSpec>,
) -> WriteRoute {
    if writer_partition.is_some() {
        return WriteRoute::WriterPartition;
    }
    match parallelism {
        None => WriteRoute::DirectFull,
        Some(spec) => {
            if spec.is_single_tp() {
                WriteRoute::LegacySingleTp
            } else {
                WriteRoute::MultiAxisParallelism
            }
        }
    }
}

// ─── Parallel tensor pymethods ─────────────────────────────────────────────

#[pymethods]
impl PyMooncakeDistributedStore {
    /// Store a tensor with parallelism metadata.
    ///
    /// Generates the correct storage key from the base key + parallelism spec,
    /// prepends shard-aware TensorMetadata, and writes via the existing `put`.
    #[pyo3(signature = (
        key, tensor, *,
        parallelism = None,
        writer_partition = None,
        tenant = None,
        replica_count = None
    ))]
    #[allow(clippy::too_many_arguments)]
    fn put_tensor_with_parallelism(
        &self,
        key: &str,
        tensor: &Bound<'_, PyAny>,
        parallelism: Option<PyTensorParallelism>,
        writer_partition: Option<(i32, i32, i32)>,
        tenant: Option<&str>,
        replica_count: Option<usize>,
    ) -> PyResult<i32> {
        let info = extract_tensor_info(tensor)?;
        let par_spec = parallelism.as_ref().map(|p| p.to_spec()).transpose()?;
        let wp_spec = writer_partition
            .map(|(rank, size, split_dim)| {
                let spec = WriterPartitionSpec::new(rank, size, split_dim);
                spec.validate()
                    .map_err(|e| PyValueError::new_err(e.to_string()))?;
                Ok::<_, PyErr>(spec)
            })
            .transpose()?;

        let route = resolve_write_route(&par_spec, &wp_spec);

        match route {
            WriteRoute::DirectFull => {
                let metadata = TensorMetadata::build_full(
                    info.dtype as i32,
                    &info.shape,
                    info.data_bytes as u64,
                );
                self.put_tensor_object(key, &metadata, &info, tenant, replica_count)
            }
            WriteRoute::LegacySingleTp | WriteRoute::MultiAxisParallelism => {
                let spec = par_spec
                    .as_ref()
                    .expect("parallelism routes must have par_spec");

                // Auto-slice: if TP axis present, narrow the full tensor to the TP shard.
                // Keep the sliced tensor alive until put_tensor_object has copied from data_ptr.
                if let Some(tp) = spec.tp_axis() {
                    let dim = tp.split_dim as usize;
                    if dim >= info.shape.len() {
                        return Err(PyValueError::new_err(
                            "split_dim out of range for tensor shape",
                        ));
                    }
                    validate_uniform_shard(info.shape[dim], tp.size)
                        .map_err(|e| PyValueError::new_err(e.to_string()))?;
                    let chunk = info.shape[dim] / tp.size as i64;
                    let start = tp.rank as i64 * chunk;
                    let shard_tensor = tensor
                        .call_method1("narrow", (dim as i64, start, chunk))?
                        .call_method0("contiguous")?;
                    let shard_info = extract_tensor_info(&shard_tensor)?;
                    let global_shape = info.shape.clone();
                    let storage_key = get_parallelism_key_name(key, spec);
                    let metadata = TensorMetadata::build_shard(
                        shard_info.dtype as i32,
                        &global_shape,
                        &shard_info.shape,
                        &spec.canonicalize().axes,
                        shard_info.data_bytes as u64,
                    );
                    let status = self.put_tensor_object(
                        &storage_key,
                        &metadata,
                        &shard_info,
                        tenant,
                        replica_count,
                    )?;
                    if status != 0 {
                        return Ok(status);
                    }

                    let manifest = WriterShardManifest::new(
                        shard_info.dtype as i32,
                        global_shape.len() as i32,
                        tp.split_dim,
                        tp.size,
                        &global_shape,
                    );
                    let manifest_key = get_parallelism_manifest_key(key);
                    self.put(
                        &manifest_key,
                        manifest.as_bytes().to_vec(),
                        tenant,
                        replica_count,
                        None,
                        None,
                        None,
                        None,
                        true,
                        false,
                        false,
                    )?;

                    Ok(status)
                } else {
                    // No TP axis (pure DP/PP/EP): store as-is
                    let global = info.shape.clone();
                    let storage_key = get_parallelism_key_name(key, spec);
                    let metadata = TensorMetadata::build_shard(
                        info.dtype as i32,
                        &global,
                        &info.shape,
                        &spec.canonicalize().axes,
                        info.data_bytes as u64,
                    );
                    self.put_tensor_object(&storage_key, &metadata, &info, tenant, replica_count)
                }
            }
            WriteRoute::WriterPartition => {
                let wp = wp_spec.expect("WriterPartition route must have wp_spec");
                let dim = wp.split_dim as usize;
                if dim >= info.shape.len() {
                    return Err(PyValueError::new_err(
                        "writer_partition split_dim out of range for tensor shape",
                    ));
                }
                validate_uniform_shard(info.shape[dim], wp.size)
                    .map_err(|e| PyValueError::new_err(e.to_string()))?;

                // Auto-slice: narrow the full tensor to the writer's shard
                let global_shape = info.shape.clone();
                let chunk = info.shape[dim] / wp.size as i64;
                let start = wp.rank as i64 * chunk;
                let shard_tensor = tensor
                    .call_method1("narrow", (dim as i64, start, chunk))?
                    .call_method0("contiguous")?;
                let shard_info = extract_tensor_info(&shard_tensor)?;

                // Keep shard_tensor in scope until put_tensor_object has copied the shard bytes.
                let storage_key =
                    get_writer_partition_key_name(key, wp.rank, wp.size, wp.split_dim);
                let metadata = TensorMetadata::build_shard(
                    shard_info.dtype as i32,
                    &global_shape,
                    &shard_info.shape,
                    &[],
                    shard_info.data_bytes as u64,
                );
                let status = self.put_tensor_object(
                    &storage_key,
                    &metadata,
                    &shard_info,
                    tenant,
                    replica_count,
                )?;
                if status != 0 {
                    return Ok(status);
                }

                let manifest = WriterShardManifest::new(
                    shard_info.dtype as i32,
                    global_shape.len() as i32,
                    wp.split_dim,
                    wp.size,
                    &global_shape,
                );
                let manifest_key = WriterShardManifest::manifest_key(key);
                self.put(
                    &manifest_key,
                    manifest.as_bytes().to_vec(),
                    tenant,
                    replica_count,
                    None,
                    None,
                    None,
                    None,
                    true,
                    false,
                    false,
                )
            }
        }
    }

    /// Upsert a tensor with parallelism: remove existing, then put.
    #[pyo3(signature = (
        key, tensor, *,
        parallelism = None,
        writer_partition = None,
        tenant = None,
        replica_count = None
    ))]
    #[allow(clippy::too_many_arguments)]
    fn upsert_tensor_with_parallelism(
        &self,
        key: &str,
        tensor: &Bound<'_, PyAny>,
        parallelism: Option<PyTensorParallelism>,
        writer_partition: Option<(i32, i32, i32)>,
        tenant: Option<&str>,
        replica_count: Option<usize>,
    ) -> PyResult<i32> {
        let par_spec = parallelism.as_ref().map(|p| p.to_spec()).transpose()?;
        let wp_spec = writer_partition
            .map(|(rank, size, split_dim)| {
                let spec = WriterPartitionSpec::new(rank, size, split_dim);
                spec.validate()
                    .map_err(|e| PyValueError::new_err(e.to_string()))?;
                Ok::<_, PyErr>(spec)
            })
            .transpose()?;

        // Clean up both manifest types to prevent stale cross-route reads
        let _ = self.remove(&get_parallelism_manifest_key(key), false, tenant);
        let _ = self.remove(&WriterShardManifest::manifest_key(key), false, tenant);

        let storage_key = match (&par_spec, &wp_spec) {
            (Some(spec), _) => get_parallelism_key_name(key, spec),
            (_, Some(wp)) => get_writer_partition_key_name(key, wp.rank, wp.size, wp.split_dim),
            _ => key.to_string(),
        };

        let _ = self.remove(&storage_key, false, tenant);

        self.put_tensor_with_parallelism(
            key,
            tensor,
            parallelism,
            writer_partition,
            tenant,
            replica_count,
        )
    }

    /// Put a tensor from a raw buffer `[TensorMetadata | data]` with parallelism.
    #[pyo3(signature = (
        key, buffer_ptr, size, *,
        parallelism = None,
        writer_partition = None,
        tenant = None,
        replica_count = None
    ))]
    #[allow(clippy::too_many_arguments)]
    fn put_tensor_with_parallelism_from(
        &self,
        key: &str,
        buffer_ptr: usize,
        size: usize,
        parallelism: Option<PyTensorParallelism>,
        writer_partition: Option<(i32, i32, i32)>,
        tenant: Option<&str>,
        replica_count: Option<usize>,
    ) -> PyResult<i32> {
        self.put_tensor_from_internal(
            key,
            buffer_ptr,
            size,
            parallelism,
            writer_partition,
            tenant,
            replica_count,
            false,
        )
    }

    /// Upsert a tensor from a raw buffer with parallelism.
    #[pyo3(signature = (
        key, buffer_ptr, size, *,
        parallelism = None,
        writer_partition = None,
        tenant = None,
        replica_count = None
    ))]
    #[allow(clippy::too_many_arguments)]
    fn upsert_tensor_with_parallelism_from(
        &self,
        key: &str,
        buffer_ptr: usize,
        size: usize,
        parallelism: Option<PyTensorParallelism>,
        writer_partition: Option<(i32, i32, i32)>,
        tenant: Option<&str>,
        replica_count: Option<usize>,
    ) -> PyResult<i32> {
        self.put_tensor_from_internal(
            key,
            buffer_ptr,
            size,
            parallelism,
            writer_partition,
            tenant,
            replica_count,
            true,
        )
    }

    /// Read a tensor with parallelism into a pre-registered buffer.
    ///
    /// Supports three modes:
    /// - AsStored: read the exact object as-is
    /// - Shard: read a specific shard (with TP remapping if needed)
    /// - Full: reconstruct the full tensor from all shards
    #[pyo3(signature = (
        key, buffer_ptr, size, *,
        target = None,
        tenant = None
    ))]
    fn get_tensor_with_parallelism_into(
        &self,
        key: &str,
        buffer_ptr: usize,
        size: usize,
        target: Option<PyReadTarget>,
        tenant: Option<&str>,
    ) -> PyResult<TensorReadResult> {
        let _ = pointer_from_usize(buffer_ptr)?;
        if size < TensorMetadata::WIRE_SIZE {
            return Err(PyValueError::new_err(format!(
                "buffer too small: need at least {} bytes, got {size}",
                TensorMetadata::WIRE_SIZE
            )));
        }

        let target_spec = match target {
            Some(t) => t.to_spec()?,
            None => ReadTargetSpec::as_stored(),
        };

        match target_spec.mode {
            ReadTargetMode::AsStored => {
                self.get_tensor_into_internal(key, buffer_ptr, size, tenant)
            }
            ReadTargetMode::Shard => {
                self.read_shard_into(key, buffer_ptr, size, &target_spec, tenant)
            }
            ReadTargetMode::Full => self.read_full_into(key, buffer_ptr, size, tenant),
        }
    }

    /// Read a tensor with parallelism, returning raw bytes.
    ///
    /// For AsStored/Shard: reads from the appropriate key(s).
    /// For Full: reconstructs the complete tensor from all shards.
    #[pyo3(signature = (key, *, target = None, tensor = None, tenant = None))]
    fn get_tensor_with_parallelism<'py>(
        &self,
        py: Python<'py>,
        key: &str,
        target: Option<PyReadTarget>,
        tensor: Option<&Bound<'_, PyAny>>,
        tenant: Option<&str>,
    ) -> PyResult<PyObject> {
        let target_spec = match target {
            Some(t) => t.to_spec()?,
            None => ReadTargetSpec::as_stored(),
        };

        match target_spec.mode {
            ReadTargetMode::AsStored => {
                let data = self.get_raw_bytes(key, tenant)?;
                self.return_tensor_data(py, &data, tensor)
            }
            ReadTargetMode::Shard => {
                let data = self.read_shard_bytes(key, &target_spec, tenant)?;
                self.return_tensor_data(py, &data, tensor)
            }
            ReadTargetMode::Full => {
                let data = self.read_full_bytes(key, tenant)?;
                self.return_tensor_data(py, &data, tensor)
            }
        }
    }

    // ─── Batch methods ────────────────────────────────────────────────────

    /// Batch put tensors with parallelism.
    #[pyo3(signature = (
        keys, tensors, *,
        parallelisms = None,
        writer_partitions = None,
        tenant = None,
        replica_count = None
    ))]
    #[allow(clippy::too_many_arguments)]
    fn batch_put_tensor_with_parallelism(
        &self,
        py: Python<'_>,
        keys: Vec<String>,
        tensors: Vec<Bound<'_, PyAny>>,
        parallelisms: Option<Vec<Option<PyTensorParallelism>>>,
        writer_partitions: Option<Vec<Option<(i32, i32, i32)>>>,
        tenant: Option<&str>,
        replica_count: Option<usize>,
    ) -> PyResult<Py<PyAny>> {
        validate_batch_write_args(&keys, tensors.len(), &parallelisms, &writer_partitions)?;
        let mut results = Vec::with_capacity(keys.len());
        for i in 0..keys.len() {
            let par = parallelisms.as_ref().and_then(|v| v[i].clone());
            let wp = writer_partitions.as_ref().and_then(|v| v[i]);
            let status = self.put_tensor_with_parallelism(
                &keys[i],
                &tensors[i],
                par,
                wp,
                tenant,
                replica_count,
            )?;
            results.push(status);
        }
        Ok(pyo3::types::PyList::new(py, results)?.into_any().unbind())
    }

    /// Batch upsert tensors with parallelism.
    #[pyo3(signature = (
        keys, tensors, *,
        parallelisms = None,
        writer_partitions = None,
        tenant = None,
        replica_count = None
    ))]
    #[allow(clippy::too_many_arguments)]
    fn batch_upsert_tensor_with_parallelism(
        &self,
        py: Python<'_>,
        keys: Vec<String>,
        tensors: Vec<Bound<'_, PyAny>>,
        parallelisms: Option<Vec<Option<PyTensorParallelism>>>,
        writer_partitions: Option<Vec<Option<(i32, i32, i32)>>>,
        tenant: Option<&str>,
        replica_count: Option<usize>,
    ) -> PyResult<Py<PyAny>> {
        validate_batch_write_args(&keys, tensors.len(), &parallelisms, &writer_partitions)?;
        let mut results = Vec::with_capacity(keys.len());
        for i in 0..keys.len() {
            let par = parallelisms.as_ref().and_then(|v| v[i].clone());
            let wp = writer_partitions.as_ref().and_then(|v| v[i]);
            let status = self.upsert_tensor_with_parallelism(
                &keys[i],
                &tensors[i],
                par,
                wp,
                tenant,
                replica_count,
            )?;
            results.push(status);
        }
        Ok(pyo3::types::PyList::new(py, results)?.into_any().unbind())
    }

    /// Batch put tensors from raw buffers with parallelism.
    #[pyo3(signature = (
        keys, buffer_ptrs, sizes, *,
        parallelisms = None,
        writer_partitions = None,
        tenant = None,
        replica_count = None
    ))]
    #[allow(clippy::too_many_arguments)]
    fn batch_put_tensor_with_parallelism_from(
        &self,
        py: Python<'_>,
        keys: Vec<String>,
        buffer_ptrs: Vec<usize>,
        sizes: Vec<usize>,
        parallelisms: Option<Vec<Option<PyTensorParallelism>>>,
        writer_partitions: Option<Vec<Option<(i32, i32, i32)>>>,
        tenant: Option<&str>,
        replica_count: Option<usize>,
    ) -> PyResult<Py<PyAny>> {
        validate_batch_from_args(
            &keys,
            &buffer_ptrs,
            &sizes,
            &parallelisms,
            &writer_partitions,
        )?;
        let mut results = Vec::with_capacity(keys.len());
        for i in 0..keys.len() {
            let par = parallelisms.as_ref().and_then(|v| v[i].clone());
            let wp = writer_partitions.as_ref().and_then(|v| v[i]);
            let status = self.put_tensor_from_internal(
                &keys[i],
                buffer_ptrs[i],
                sizes[i],
                par,
                wp,
                tenant,
                replica_count,
                false,
            )?;
            results.push(status);
        }
        Ok(pyo3::types::PyList::new(py, results)?.into_any().unbind())
    }

    /// Batch upsert tensors from raw buffers with parallelism.
    #[pyo3(signature = (
        keys, buffer_ptrs, sizes, *,
        parallelisms = None,
        writer_partitions = None,
        tenant = None,
        replica_count = None
    ))]
    #[allow(clippy::too_many_arguments)]
    fn batch_upsert_tensor_with_parallelism_from(
        &self,
        py: Python<'_>,
        keys: Vec<String>,
        buffer_ptrs: Vec<usize>,
        sizes: Vec<usize>,
        parallelisms: Option<Vec<Option<PyTensorParallelism>>>,
        writer_partitions: Option<Vec<Option<(i32, i32, i32)>>>,
        tenant: Option<&str>,
        replica_count: Option<usize>,
    ) -> PyResult<Py<PyAny>> {
        validate_batch_from_args(
            &keys,
            &buffer_ptrs,
            &sizes,
            &parallelisms,
            &writer_partitions,
        )?;
        let mut results = Vec::with_capacity(keys.len());
        for i in 0..keys.len() {
            let par = parallelisms.as_ref().and_then(|v| v[i].clone());
            let wp = writer_partitions.as_ref().and_then(|v| v[i]);
            let status = self.put_tensor_from_internal(
                &keys[i],
                buffer_ptrs[i],
                sizes[i],
                par,
                wp,
                tenant,
                replica_count,
                true,
            )?;
            results.push(status);
        }
        Ok(pyo3::types::PyList::new(py, results)?.into_any().unbind())
    }

    /// Batch get tensors with parallelism, returning raw bytes or tensor.
    #[pyo3(signature = (keys, *, targets = None, tensors = None, tenant = None))]
    fn batch_get_tensor_with_parallelism<'py>(
        &self,
        py: Python<'py>,
        keys: Vec<String>,
        targets: Option<Vec<Option<PyReadTarget>>>,
        tensors: Option<Vec<Option<Bound<'py, PyAny>>>>,
        tenant: Option<&str>,
    ) -> PyResult<Py<PyAny>> {
        if let Some(ref t) = targets {
            if t.len() != keys.len() {
                return Err(PyValueError::new_err(
                    "targets must have the same length as keys",
                ));
            }
        }
        if let Some(ref t) = tensors {
            if t.len() != keys.len() {
                return Err(PyValueError::new_err(
                    "tensors must have the same length as keys",
                ));
            }
        }
        let mut results = Vec::with_capacity(keys.len());
        for i in 0..keys.len() {
            let target = targets.as_ref().and_then(|v| v[i].clone());
            let tensor = tensors.as_ref().and_then(|v| v[i].clone());
            let result =
                self.get_tensor_with_parallelism(py, &keys[i], target, tensor.as_ref(), tenant)?;
            results.push(result);
        }
        Ok(pyo3::types::PyList::new(py, results)?.into_any().unbind())
    }

    /// Batch get tensors into pre-registered buffers with parallelism.
    ///
    /// Builds read plans for all keys, then issues a single merged
    /// `get_into_ranges` call to minimize round trips.
    #[pyo3(signature = (keys, buffer_ptrs, sizes, *, targets = None, tenant = None))]
    fn batch_get_tensor_with_parallelism_into(
        &self,
        py: Python<'_>,
        keys: Vec<String>,
        buffer_ptrs: Vec<usize>,
        sizes: Vec<usize>,
        targets: Option<Vec<Option<PyReadTarget>>>,
        tenant: Option<&str>,
    ) -> PyResult<Py<PyAny>> {
        if keys.len() != buffer_ptrs.len() || keys.len() != sizes.len() {
            return Err(PyValueError::new_err(
                "keys, buffer_ptrs, and sizes must have the same length",
            ));
        }
        if let Some(ref t) = targets {
            if t.len() != keys.len() {
                return Err(PyValueError::new_err(
                    "targets must have the same length as keys",
                ));
            }
        }

        let n = keys.len();
        let mut results: Vec<Option<TensorReadResult>> = vec![None; n];

        // Phase 1: Resolve primary read keys for each item
        #[derive(Clone)]
        enum BatchReadPlan {
            Direct(String),
            Shard(String, TensorParallelismSpec),
            Full(String),
        }

        let mut plans = Vec::with_capacity(n);
        for i in 0..n {
            let target_spec = match targets.as_ref().and_then(|v| v[i].clone()) {
                Some(t) => t.to_spec()?,
                None => ReadTargetSpec::as_stored(),
            };
            match target_spec.mode {
                ReadTargetMode::AsStored => {
                    plans.push(BatchReadPlan::Direct(keys[i].clone()));
                }
                ReadTargetMode::Shard => {
                    let par = target_spec.parallelism.ok_or_else(|| {
                        PyValueError::new_err("SHARD mode requires parallelism spec")
                    })?;
                    let par_key = get_parallelism_key_name(&keys[i], &par);
                    plans.push(BatchReadPlan::Shard(par_key, par));
                }
                ReadTargetMode::Full => {
                    plans.push(BatchReadPlan::Full(keys[i].clone()));
                }
            }
        }

        // Phase 2: Batch-read all direct/shard items with one get_into_ranges call
        let mut batch_indices: Vec<usize> = Vec::new();
        let mut batch_buffer_ptrs: Vec<usize> = Vec::new();
        let mut batch_all_keys: Vec<Vec<String>> = Vec::new();
        let mut batch_dst_offsets: Vec<Vec<Vec<usize>>> = Vec::new();
        let mut batch_src_offsets: Vec<Vec<Vec<usize>>> = Vec::new();
        let mut batch_sizes: Vec<Vec<Vec<usize>>> = Vec::new();
        let mut batch_buffer_sizes: Vec<usize> = Vec::new();

        for i in 0..n {
            let read_key = match &plans[i] {
                BatchReadPlan::Direct(k) | BatchReadPlan::Shard(k, _) => k.clone(),
                BatchReadPlan::Full(_) => continue,
            };
            let _ = pointer_from_usize(buffer_ptrs[i])?;
            batch_indices.push(i);
            batch_buffer_ptrs.push(buffer_ptrs[i]);
            batch_all_keys.push(vec![read_key]);
            batch_dst_offsets.push(vec![vec![0]]);
            batch_src_offsets.push(vec![vec![0]]);
            batch_sizes.push(vec![vec![sizes[i]]]);
            batch_buffer_sizes.push(sizes[i]);
        }

        if !batch_indices.is_empty() {
            let batch_results = self.get_into_ranges(
                batch_buffer_ptrs,
                batch_all_keys,
                batch_dst_offsets,
                batch_src_offsets,
                batch_sizes,
                Some(batch_buffer_sizes),
                tenant,
            )?;

            for (batch_idx, &orig_idx) in batch_indices.iter().enumerate() {
                let bytes_read = batch_results[batch_idx][0][0];
                if bytes_read <= 0 {
                    continue;
                }
                let bytes_read = bytes_read as usize;
                let buf_ptr = buffer_ptrs[orig_idx];
                let data = unsafe { std::slice::from_raw_parts(buf_ptr as *const u8, bytes_read) };
                let parsed = match TensorMetadata::parse(data) {
                    Some(p) => p,
                    None => continue,
                };

                let valid = match &plans[orig_idx] {
                    BatchReadPlan::Direct(_) => true,
                    BatchReadPlan::Shard(_, par) => {
                        parallelism_matches_metadata(par, &parsed.metadata)
                    }
                    BatchReadPlan::Full(_) => unreachable!(),
                };

                if valid {
                    results[orig_idx] = Some(TensorReadResult {
                        data_ptr: buf_ptr + parsed.data_offset,
                        data_bytes: parsed.data_bytes,
                        shape: parsed
                            .metadata
                            .layout
                            .local_shape
                            .to_vec(parsed.metadata.header.ndim as usize),
                        dtype: parsed.metadata.header.dtype,
                        total_bytes_read: bytes_read,
                    });
                }
            }
        }

        // Phase 3: Handle items that failed in phase 2
        // Try writer-partition shortcut for Shard misses, then reconstruction.
        // Full reads also handled here.
        let mut full_plan_indices = Vec::new();
        let mut full_plans = Vec::new();
        for i in 0..n {
            if results[i].is_some() {
                continue;
            }
            match &plans[i] {
                BatchReadPlan::Shard(_, par) => {
                    if let Some(shortcut_result) = self.try_writer_partition_shortcut_into(
                        &keys[i],
                        buffer_ptrs[i],
                        sizes[i],
                        par,
                        tenant,
                    ) {
                        results[i] = Some(shortcut_result?);
                    } else {
                        results[i] = Some(self.read_shard_via_reconstruction(
                            &keys[i],
                            buffer_ptrs[i],
                            sizes[i],
                            par,
                            tenant,
                        )?);
                    }
                }
                BatchReadPlan::Full(k) => {
                    // Preserve the existing compatibility behavior: a direct
                    // full-object read is the fast path, and any miss or
                    // soft-fail falls back to shard-based reconstruction.
                    if let Ok(result) =
                        self.get_tensor_into_internal(k, buffer_ptrs[i], sizes[i], tenant)
                    {
                        results[i] = Some(result);
                    } else {
                        let plan = self.build_full_into_reconstruction_plan(
                            k,
                            buffer_ptrs[i],
                            sizes[i],
                            tenant,
                            "batch_get_tensor_with_parallelism_into",
                        )?;
                        full_plan_indices.push(i);
                        full_plans.push(plan);
                    }
                }
                BatchReadPlan::Direct(_) => {
                    return Err(PyRuntimeError::new_err(format!(
                        "batch_get_into: direct read failed for key={}",
                        keys[i]
                    )));
                }
            }
        }

        let full_results = self.execute_full_into_plans(full_plans, tenant)?;
        for (plan_index, result) in full_plan_indices.into_iter().zip(full_results) {
            results[plan_index] = Some(result.ok_or_else(|| {
                PyRuntimeError::new_err(format!(
                    "batch_get_into: full reconstruction failed for key={}",
                    keys[plan_index]
                ))
            })?);
        }

        let final_results: Vec<TensorReadResult> =
            results.into_iter().map(|r| r.unwrap()).collect();
        Ok(pyo3::types::PyList::new(py, final_results)?
            .into_any()
            .unbind())
    }
}

// ─── Internal helpers ──────────────────────────────────────────────────────

// ─── Batch validation helpers ─────────────────────────────────────────────

type OptionalParallelisms = Option<Vec<Option<PyTensorParallelism>>>;
type OptionalWriterPartitions = Option<Vec<Option<(i32, i32, i32)>>>;

fn validate_batch_write_args(
    keys: &[String],
    tensor_count: usize,
    parallelisms: &OptionalParallelisms,
    writer_partitions: &OptionalWriterPartitions,
) -> PyResult<()> {
    if keys.len() != tensor_count {
        return Err(PyValueError::new_err(
            "keys and tensors must have the same length",
        ));
    }
    if parallelisms.is_some() && writer_partitions.is_some() {
        return Err(PyValueError::new_err(
            "parallelisms and writer_partitions cannot both be provided in the same batch",
        ));
    }
    if let Some(pars) = parallelisms {
        if pars.len() != keys.len() {
            return Err(PyValueError::new_err(
                "parallelisms must have the same length as keys",
            ));
        }
    }
    if let Some(wps) = writer_partitions {
        if wps.len() != keys.len() {
            return Err(PyValueError::new_err(
                "writer_partitions must have the same length as keys",
            ));
        }
    }
    Ok(())
}

fn validate_batch_from_args(
    keys: &[String],
    buffer_ptrs: &[usize],
    sizes: &[usize],
    parallelisms: &OptionalParallelisms,
    writer_partitions: &OptionalWriterPartitions,
) -> PyResult<()> {
    if keys.len() != buffer_ptrs.len() || keys.len() != sizes.len() {
        return Err(PyValueError::new_err(
            "keys, buffer_ptrs, and sizes must have the same length",
        ));
    }
    if parallelisms.is_some() && writer_partitions.is_some() {
        return Err(PyValueError::new_err(
            "parallelisms and writer_partitions cannot both be provided in the same batch",
        ));
    }
    if let Some(pars) = parallelisms {
        if pars.len() != keys.len() {
            return Err(PyValueError::new_err(
                "parallelisms must have the same length as keys",
            ));
        }
    }
    if let Some(wps) = writer_partitions {
        if wps.len() != keys.len() {
            return Err(PyValueError::new_err(
                "writer_partitions must have the same length as keys",
            ));
        }
    }
    Ok(())
}

/// Extract TensorInfo from a raw buffer containing `[TensorMetadata | data]`.
fn extract_tensor_info_from_buffer(
    buffer_ptr: usize,
    size: usize,
) -> PyResult<(TensorInfo, TensorMetadata)> {
    if size < TensorMetadata::WIRE_SIZE {
        return Err(PyValueError::new_err(format!(
            "buffer too small for tensor metadata: need at least {} bytes, got {size}",
            TensorMetadata::WIRE_SIZE
        )));
    }
    let _ = pointer_from_usize(buffer_ptr)?;
    let data =
        unsafe { std::slice::from_raw_parts(buffer_ptr as *const u8, TensorMetadata::WIRE_SIZE) };
    let parsed = TensorMetadata::parse(data)
        .ok_or_else(|| PyValueError::new_err("invalid tensor metadata in raw buffer"))?;

    let ndim = parsed.metadata.header.ndim as usize;
    let shape = parsed.metadata.layout.local_shape.to_vec(ndim);
    let dtype_i32 = parsed.metadata.header.dtype;
    let dtype = TensorDtype::from_i32(dtype_i32).ok_or_else(|| {
        PyValueError::new_err(format!("unsupported dtype in raw buffer: {dtype_i32}"))
    })?;

    let required = TensorMetadata::WIRE_SIZE + parsed.data_bytes;
    if size < required {
        return Err(PyValueError::new_err(format!(
            "buffer too small for tensor data: need {required} bytes, got {size}",
        )));
    }

    let info = TensorInfo {
        data_ptr: buffer_ptr + TensorMetadata::WIRE_SIZE,
        data_bytes: parsed.data_bytes,
        shape,
        dtype,
    };
    Ok((info, parsed.metadata))
}

/// Extract shard bytes from a raw data buffer using a shard write plan.
/// `data_start` points to the beginning of the data section (after metadata).
fn extract_shard_data_from_buffer(data_start: usize, plan: &RawShardWritePlan) -> Vec<u8> {
    let mut shard_data = vec![0u8; plan.shard_bytes];
    let mut dst_offset = 0usize;
    for &(src_offset, range_size) in &plan.data_ranges {
        unsafe {
            std::ptr::copy_nonoverlapping(
                (data_start + src_offset) as *const u8,
                shard_data.as_mut_ptr().add(dst_offset),
                range_size,
            );
        }
        dst_offset += range_size;
    }
    shard_data
}

impl PyMooncakeDistributedStore {
    fn put_tensor_object(
        &self,
        storage_key: &str,
        metadata: &TensorMetadata,
        info: &TensorInfo,
        tenant: Option<&str>,
        replica_count: Option<usize>,
    ) -> PyResult<i32> {
        let total_size = TensorMetadata::WIRE_SIZE
            .checked_add(info.data_bytes)
            .ok_or_else(|| PyValueError::new_err("total tensor object size overflows usize"))?;

        let mut buffer = vec![0u8; total_size];
        buffer[..TensorMetadata::WIRE_SIZE].copy_from_slice(metadata.as_bytes());
        // Safety: data_ptr comes from extract_tensor_info which verified is_contiguous,
        // and data_bytes = numel * element_size is the exact allocation size.
        unsafe {
            std::ptr::copy_nonoverlapping(
                info.data_ptr as *const u8,
                buffer.as_mut_ptr().add(TensorMetadata::WIRE_SIZE),
                info.data_bytes,
            );
        }

        self.put(
            storage_key,
            buffer,
            tenant,
            replica_count,
            None,
            None,
            None,
            None,
            true,
            false,
            false,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn put_tensor_object_from_vec(
        &self,
        storage_key: &str,
        metadata: &TensorMetadata,
        shard_data: Vec<u8>,
        tenant: Option<&str>,
        replica_count: Option<usize>,
        base_key: &str,
        spec: &TensorParallelismSpec,
        global_shape: &[i64],
    ) -> PyResult<i32> {
        let total_size = TensorMetadata::WIRE_SIZE + shard_data.len();
        let mut buffer = vec![0u8; total_size];
        buffer[..TensorMetadata::WIRE_SIZE].copy_from_slice(metadata.as_bytes());
        buffer[TensorMetadata::WIRE_SIZE..].copy_from_slice(&shard_data);

        let status = self.put(
            storage_key,
            buffer,
            tenant,
            replica_count,
            None,
            None,
            None,
            None,
            true,
            false,
            false,
        )?;
        if status != 0 {
            return Ok(status);
        }

        if let Some(tp_axis) = spec.tp_axis() {
            let manifest = WriterShardManifest::new(
                metadata.header.dtype,
                global_shape.len() as i32,
                tp_axis.split_dim,
                tp_axis.size,
                global_shape,
            );
            let manifest_key = get_parallelism_manifest_key(base_key);
            self.put(
                &manifest_key,
                manifest.as_bytes().to_vec(),
                tenant,
                replica_count,
                None,
                None,
                None,
                None,
                true,
                false,
                false,
            )?;
        }
        Ok(status)
    }

    #[allow(clippy::too_many_arguments)]
    fn put_tensor_from_internal(
        &self,
        key: &str,
        buffer_ptr: usize,
        size: usize,
        parallelism: Option<PyTensorParallelism>,
        writer_partition: Option<(i32, i32, i32)>,
        tenant: Option<&str>,
        replica_count: Option<usize>,
        is_upsert: bool,
    ) -> PyResult<i32> {
        let (info, _original_metadata) = extract_tensor_info_from_buffer(buffer_ptr, size)?;
        let par_spec = parallelism.as_ref().map(|p| p.to_spec()).transpose()?;
        let wp_spec = writer_partition
            .map(|(rank, sz, split_dim)| {
                let spec = WriterPartitionSpec::new(rank, sz, split_dim);
                spec.validate()
                    .map_err(|e| PyValueError::new_err(e.to_string()))?;
                Ok::<_, PyErr>(spec)
            })
            .transpose()?;

        let route = resolve_write_route(&par_spec, &wp_spec);

        if is_upsert {
            // Clean up both manifest types to prevent stale cross-route reads
            let _ = self.remove(&get_parallelism_manifest_key(key), false, tenant);
            let _ = self.remove(&WriterShardManifest::manifest_key(key), false, tenant);

            let remove_key = match (&par_spec, &wp_spec) {
                (Some(spec), _) => get_parallelism_key_name(key, spec),
                (_, Some(wp)) => get_writer_partition_key_name(key, wp.rank, wp.size, wp.split_dim),
                _ => key.to_string(),
            };
            let _ = self.remove(&remove_key, false, tenant);
        }

        match route {
            WriteRoute::DirectFull => self.put_from(
                key,
                buffer_ptr,
                size,
                tenant,
                replica_count,
                None,
                None,
                None,
                None,
                true,
                false,
                false,
            ),
            WriteRoute::LegacySingleTp | WriteRoute::MultiAxisParallelism => {
                let spec = par_spec
                    .as_ref()
                    .expect("parallelism routes must have par_spec");

                let (shard_info, global_shape) = if let Some(tp) = spec.tp_axis() {
                    let dim = tp.split_dim as usize;
                    if dim >= info.shape.len() {
                        return Err(PyValueError::new_err(
                            "_from: split_dim out of range for tensor shape",
                        ));
                    }
                    validate_uniform_shard(info.shape[dim], tp.size)
                        .map_err(|e| PyValueError::new_err(e.to_string()))?;
                    let plan = build_raw_shard_write_plan(
                        &info.shape,
                        dim,
                        tp.rank as usize,
                        tp.size as usize,
                        info.dtype.element_size(),
                    )
                    .ok_or_else(|| {
                        PyValueError::new_err("_from: failed to build shard write plan")
                    })?;
                    let shard_data = extract_shard_data_from_buffer(
                        buffer_ptr + TensorMetadata::WIRE_SIZE,
                        &plan,
                    );
                    let global = info.shape.clone();
                    return self.put_tensor_object_from_vec(
                        &get_parallelism_key_name(key, spec),
                        &TensorMetadata::build_shard(
                            info.dtype as i32,
                            &global,
                            &plan.shard_shape,
                            &spec.canonicalize().axes,
                            plan.shard_bytes as u64,
                        ),
                        shard_data,
                        tenant,
                        replica_count,
                        key,
                        spec,
                        &global,
                    );
                } else {
                    let shape = info.shape.clone();
                    (info, shape)
                };

                let storage_key = get_parallelism_key_name(key, spec);
                let metadata = TensorMetadata::build_shard(
                    shard_info.dtype as i32,
                    &global_shape,
                    &shard_info.shape,
                    &spec.canonicalize().axes,
                    shard_info.data_bytes as u64,
                );
                let status = self.put_tensor_object(
                    &storage_key,
                    &metadata,
                    &shard_info,
                    tenant,
                    replica_count,
                )?;
                if status != 0 {
                    return Ok(status);
                }

                if let Some(tp_axis) = spec.tp_axis() {
                    let manifest = WriterShardManifest::new(
                        shard_info.dtype as i32,
                        global_shape.len() as i32,
                        tp_axis.split_dim,
                        tp_axis.size,
                        &global_shape,
                    );
                    let manifest_key = get_parallelism_manifest_key(key);
                    self.put(
                        &manifest_key,
                        manifest.as_bytes().to_vec(),
                        tenant,
                        replica_count,
                        None,
                        None,
                        None,
                        None,
                        true,
                        false,
                        false,
                    )?;
                }
                Ok(status)
            }
            WriteRoute::WriterPartition => {
                let wp = wp_spec.expect("WriterPartition route must have wp_spec");
                let dim = wp.split_dim as usize;
                if dim >= info.shape.len() {
                    return Err(PyValueError::new_err(
                        "_from: writer_partition split_dim out of range",
                    ));
                }
                validate_uniform_shard(info.shape[dim], wp.size)
                    .map_err(|e| PyValueError::new_err(e.to_string()))?;

                let global_shape = info.shape.clone();
                let plan = build_raw_shard_write_plan(
                    &global_shape,
                    dim,
                    wp.rank as usize,
                    wp.size as usize,
                    info.dtype.element_size(),
                )
                .ok_or_else(|| {
                    PyValueError::new_err("_from: failed to build writer shard write plan")
                })?;

                let shard_data =
                    extract_shard_data_from_buffer(buffer_ptr + TensorMetadata::WIRE_SIZE, &plan);

                let storage_key =
                    get_writer_partition_key_name(key, wp.rank, wp.size, wp.split_dim);
                let metadata = TensorMetadata::build_shard(
                    info.dtype as i32,
                    &global_shape,
                    &plan.shard_shape,
                    &[],
                    plan.shard_bytes as u64,
                );

                let total_size = TensorMetadata::WIRE_SIZE + plan.shard_bytes;
                let mut buffer = vec![0u8; total_size];
                buffer[..TensorMetadata::WIRE_SIZE].copy_from_slice(metadata.as_bytes());
                buffer[TensorMetadata::WIRE_SIZE..].copy_from_slice(&shard_data);

                let status = self.put(
                    &storage_key,
                    buffer,
                    tenant,
                    replica_count,
                    None,
                    None,
                    None,
                    None,
                    true,
                    false,
                    false,
                )?;
                if status != 0 {
                    return Ok(status);
                }

                let manifest = WriterShardManifest::new(
                    info.dtype as i32,
                    global_shape.len() as i32,
                    wp.split_dim,
                    wp.size,
                    &global_shape,
                );
                let manifest_key = WriterShardManifest::manifest_key(key);
                self.put(
                    &manifest_key,
                    manifest.as_bytes().to_vec(),
                    tenant,
                    replica_count,
                    None,
                    None,
                    None,
                    None,
                    true,
                    false,
                    false,
                )
            }
        }
    }

    fn get_raw_bytes(&self, key: &str, tenant: Option<&str>) -> PyResult<Vec<u8>> {
        self.get_bytes_internal(key, tenant)
    }

    fn get_tensor_into_internal(
        &self,
        key: &str,
        buffer_ptr: usize,
        size: usize,
        tenant: Option<&str>,
    ) -> PyResult<TensorReadResult> {
        let _ = pointer_from_usize(buffer_ptr)?;
        if size < TensorMetadata::WIRE_SIZE {
            return Err(PyValueError::new_err(format!(
                "buffer too small for tensor metadata: need at least {} bytes, got {size}",
                TensorMetadata::WIRE_SIZE
            )));
        }

        let bytes_read = self.get_into_buffer_internal(key, buffer_ptr, size, tenant)?;
        if bytes_read > size {
            return Err(PyRuntimeError::new_err(format!(
                "bytes_read ({bytes_read}) exceeds buffer size ({size})"
            )));
        }

        let data = unsafe { std::slice::from_raw_parts(buffer_ptr as *const u8, bytes_read) };
        let parsed = TensorMetadata::parse(data).ok_or_else(|| {
            PyRuntimeError::new_err(format!("invalid tensor metadata for key={key}"))
        })?;

        Ok(TensorReadResult {
            data_ptr: buffer_ptr + parsed.data_offset,
            data_bytes: parsed.data_bytes,
            shape: parsed
                .metadata
                .layout
                .local_shape
                .to_vec(parsed.metadata.header.ndim as usize),
            dtype: parsed.metadata.header.dtype,
            total_bytes_read: bytes_read,
        })
    }

    fn return_tensor_data<'py>(
        &self,
        py: Python<'py>,
        data: &[u8],
        tensor: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<PyObject> {
        let parsed = TensorMetadata::parse(data);

        if let Some(tensor_obj) = tensor {
            if let Some(parsed) = &parsed {
                let tensor_data = &data[parsed.data_offset..parsed.data_offset + parsed.data_bytes];
                let data_ptr: usize = tensor_obj.call_method0("data_ptr")?.extract()?;
                unsafe {
                    std::ptr::copy_nonoverlapping(
                        tensor_data.as_ptr(),
                        data_ptr as *mut u8,
                        tensor_data.len(),
                    );
                }
                return Ok(tensor_obj.clone().unbind());
            }
        }

        use pyo3::types::PyBytes;
        Ok(PyBytes::new(py, data).into_any().unbind())
    }

    // ── Shard read path ────────────────────────────────────────────────────

    fn read_shard_into(
        &self,
        base_key: &str,
        buffer_ptr: usize,
        buffer_size: usize,
        target: &ReadTargetSpec,
        tenant: Option<&str>,
    ) -> PyResult<TensorReadResult> {
        let par = target
            .parallelism
            .as_ref()
            .ok_or_else(|| PyValueError::new_err("SHARD mode requires parallelism spec"))?;

        let exact_key = get_parallelism_key_name(base_key, par);
        if let Ok(result) =
            self.get_tensor_into_internal(&exact_key, buffer_ptr, buffer_size, tenant)
        {
            let data = unsafe {
                std::slice::from_raw_parts(buffer_ptr as *const u8, result.total_bytes_read)
            };
            if let Some(parsed) = TensorMetadata::parse(data) {
                if parallelism_matches_metadata(par, &parsed.metadata) {
                    return Ok(result);
                }
            }
        }

        if let Some(result) =
            self.try_writer_partition_shortcut_into(base_key, buffer_ptr, buffer_size, par, tenant)
        {
            return result;
        }

        self.read_shard_via_reconstruction(base_key, buffer_ptr, buffer_size, par, tenant)
    }

    fn read_shard_bytes(
        &self,
        base_key: &str,
        target: &ReadTargetSpec,
        tenant: Option<&str>,
    ) -> PyResult<Vec<u8>> {
        let par = target
            .parallelism
            .as_ref()
            .ok_or_else(|| PyValueError::new_err("SHARD mode requires parallelism spec"))?;

        let exact_key = get_parallelism_key_name(base_key, par);
        if let Ok(data) = self.get_raw_bytes(&exact_key, tenant) {
            if let Some(parsed) = TensorMetadata::parse(&data) {
                if parallelism_matches_metadata(par, &parsed.metadata) {
                    return Ok(data);
                }
            }
        }

        if let Some(result) = self.try_writer_partition_shortcut_bytes(base_key, par, tenant) {
            return result;
        }

        self.reconstruct_shard_bytes(base_key, par, tenant)
    }

    fn try_writer_partition_shortcut_into(
        &self,
        base_key: &str,
        buffer_ptr: usize,
        buffer_size: usize,
        par: &TensorParallelismSpec,
        tenant: Option<&str>,
    ) -> Option<PyResult<TensorReadResult>> {
        if !par.is_single_tp() {
            return None;
        }
        let tp = par.tp_axis()?;
        let wp_key = get_writer_partition_key_name(base_key, tp.rank, tp.size, tp.split_dim);
        if let Ok(result) = self.get_tensor_into_internal(&wp_key, buffer_ptr, buffer_size, tenant)
        {
            let data = unsafe {
                std::slice::from_raw_parts(buffer_ptr as *const u8, result.total_bytes_read)
            };
            if let Some(parsed) = TensorMetadata::parse(data) {
                let local_shape = parsed
                    .metadata
                    .layout
                    .local_shape
                    .to_vec(parsed.metadata.header.ndim as usize);
                let global_shape = parsed
                    .metadata
                    .layout
                    .global_shape
                    .to_vec(parsed.metadata.header.ndim as usize);
                let dim = tp.split_dim as usize;
                if dim < global_shape.len() && dim < local_shape.len() {
                    let expected_local = global_shape[dim] / tp.size as i64;
                    if local_shape[dim] == expected_local {
                        return Some(Ok(result));
                    }
                }
            }
        }
        None
    }

    fn try_writer_partition_shortcut_bytes(
        &self,
        base_key: &str,
        par: &TensorParallelismSpec,
        tenant: Option<&str>,
    ) -> Option<PyResult<Vec<u8>>> {
        if !par.is_single_tp() {
            return None;
        }
        let tp = par.tp_axis()?;
        let wp_key = get_writer_partition_key_name(base_key, tp.rank, tp.size, tp.split_dim);
        if let Ok(data) = self.get_raw_bytes(&wp_key, tenant) {
            if let Some(parsed) = TensorMetadata::parse(&data) {
                let local_shape = parsed
                    .metadata
                    .layout
                    .local_shape
                    .to_vec(parsed.metadata.header.ndim as usize);
                let global_shape = parsed
                    .metadata
                    .layout
                    .global_shape
                    .to_vec(parsed.metadata.header.ndim as usize);
                let dim = tp.split_dim as usize;
                if dim < global_shape.len() && dim < local_shape.len() {
                    let expected_local = global_shape[dim] / tp.size as i64;
                    if local_shape[dim] == expected_local {
                        return Some(Ok(data));
                    }
                }
            }
        }
        None
    }

    fn read_shard_via_reconstruction(
        &self,
        base_key: &str,
        buffer_ptr: usize,
        buffer_size: usize,
        target_par: &TensorParallelismSpec,
        tenant: Option<&str>,
    ) -> PyResult<TensorReadResult> {
        let discovered =
            self.discover_shard_sources_cached(base_key, tenant, "read_shard_via_reconstruction")?;
        let DiscoveredShardSources {
            sources,
            query_cache,
        } = discovered;

        let first = sources.first().ok_or_else(|| {
            PyRuntimeError::new_err(format!("no shard sources found for key={base_key}"))
        })?;

        let target_tp = target_par.tp_axis();
        let (target_rank, target_tp_size) = match target_tp {
            Some(tp) => (tp.rank as usize, tp.size as usize),
            None => (0, 1),
        };

        let split_dim = match target_tp {
            Some(tp) => tp.split_dim as usize,
            None => first.split_dim as usize,
        };

        let global_shape = first.global_shape.clone();
        let mut local_shape = global_shape.clone();
        if let Some(tp) = target_tp {
            let dim = tp.split_dim as usize;
            if dim < local_shape.len() && tp.size > 0 {
                local_shape[dim] /= tp.size as i64;
            }
        }

        let target_size: usize = local_shape.iter().map(|&d| d as usize).product::<usize>()
            * TensorDtype::from_i32(first.dtype)
                .map(|d| d.element_size())
                .unwrap_or(1);

        let total_needed = TensorMetadata::WIRE_SIZE + target_size;
        if buffer_size < total_needed {
            return Err(PyValueError::new_err(format!(
                "buffer too small for shard reconstruction: need {total_needed}, got {buffer_size}"
            )));
        }

        let element_size = TensorDtype::from_i32(first.dtype)
            .map(|d| d.element_size())
            .unwrap_or(1);
        let src_count = sources.len();

        let mut keys = Vec::new();
        let mut dst_offsets = Vec::new();
        let mut src_offsets = Vec::new();
        let mut sizes = Vec::new();

        if split_dim == 0 {
            let total_data_size: usize = sources.iter().map(|s| s.data_bytes).sum();
            let (target_offset, _) =
                calculate_shard_range(total_data_size, target_rank, target_tp_size);

            let mut source_cumulative_offset = 0usize;
            for source in &sources {
                let source_start = source_cumulative_offset;
                let source_end = source_start + source.data_bytes;

                let overlap_start = target_offset.max(source_start);
                let overlap_end = (target_offset + target_size).min(source_end);

                if overlap_start < overlap_end {
                    let fragment_size = overlap_end - overlap_start;
                    let dst_off = TensorMetadata::WIRE_SIZE + (overlap_start - target_offset);
                    let src_off = TensorMetadata::WIRE_SIZE + (overlap_start - source_start);

                    keys.push(source.storage_key.clone());
                    dst_offsets.push(vec![dst_off]);
                    src_offsets.push(vec![src_off]);
                    sizes.push(vec![fragment_size]);
                }

                source_cumulative_offset = source_end;
            }
        } else {
            for source in &sources {
                let ranges = calculate_strided_shard_ranges(
                    &global_shape,
                    split_dim,
                    element_size,
                    source.src_rank,
                    src_count,
                    target_rank,
                    target_tp_size,
                );
                if let Some(transfers) = ranges {
                    if transfers.is_empty() {
                        continue;
                    }
                    let mut key_dst_offs = Vec::with_capacity(transfers.len());
                    let mut key_src_offs = Vec::with_capacity(transfers.len());
                    let mut key_sizes = Vec::with_capacity(transfers.len());
                    for t in &transfers {
                        key_dst_offs.push(TensorMetadata::WIRE_SIZE + t.dst_offset);
                        key_src_offs.push(TensorMetadata::WIRE_SIZE + t.src_offset);
                        key_sizes.push(t.size);
                    }
                    keys.push(source.storage_key.clone());
                    dst_offsets.push(key_dst_offs);
                    src_offsets.push(key_src_offs);
                    sizes.push(key_sizes);
                }
            }
        }

        let metadata = TensorMetadata::build_shard(
            first.dtype,
            &global_shape,
            &local_shape,
            &target_par.canonicalize().axes,
            target_size as u64,
        );

        unsafe {
            std::ptr::copy_nonoverlapping(
                metadata.as_bytes().as_ptr(),
                buffer_ptr as *mut u8,
                TensorMetadata::WIRE_SIZE,
            );
        }

        let results = self.get_into_ranges_internal(
            vec![buffer_ptr],
            vec![keys],
            vec![dst_offsets],
            vec![src_offsets],
            vec![sizes],
            Some(vec![buffer_size]),
            tenant,
            query_cache,
        )?;

        for key_results in &results[0] {
            for &val in key_results {
                if val <= 0 {
                    return Err(PyRuntimeError::new_err(
                        "shard reconstruction: get_into_ranges fragment failed",
                    ));
                }
            }
        }

        Ok(TensorReadResult {
            data_ptr: buffer_ptr + TensorMetadata::WIRE_SIZE,
            data_bytes: target_size,
            shape: local_shape,
            dtype: first.dtype,
            total_bytes_read: total_needed,
        })
    }

    fn reconstruct_shard_bytes(
        &self,
        base_key: &str,
        target_par: &TensorParallelismSpec,
        tenant: Option<&str>,
    ) -> PyResult<Vec<u8>> {
        let sources = self.discover_shard_sources(base_key, tenant)?;
        let target_tp = target_par.tp_axis();
        let (target_rank, target_tp_size) = match target_tp {
            Some(tp) => (tp.rank as usize, tp.size as usize),
            None => (0, 1),
        };

        let first = sources.first().ok_or_else(|| {
            PyRuntimeError::new_err(format!("no shard sources found for key={base_key}"))
        })?;

        let split_dim = match target_tp {
            Some(tp) => tp.split_dim as usize,
            None => first.split_dim as usize,
        };

        let global_shape = first.global_shape.clone();
        let mut local_shape = global_shape.clone();
        if let Some(tp) = target_tp {
            let dim = tp.split_dim as usize;
            if dim < local_shape.len() && tp.size > 0 {
                local_shape[dim] /= tp.size as i64;
            }
        }

        let element_size = TensorDtype::from_i32(first.dtype)
            .map(|d| d.element_size())
            .unwrap_or(1);

        let target_size: usize =
            local_shape.iter().map(|&d| d as usize).product::<usize>() * element_size;
        let src_count = sources.len();

        let mut result_data = vec![0u8; target_size];

        if split_dim == 0 {
            let total_data_size: usize = sources.iter().map(|s| s.data_bytes).sum();
            let (target_offset, _) =
                calculate_shard_range(total_data_size, target_rank, target_tp_size);

            let mut source_cumulative_offset = 0usize;
            for source in &sources {
                let source_start = source_cumulative_offset;
                let source_end = source_start + source.data_bytes;
                let overlap_start = target_offset.max(source_start);
                let overlap_end = (target_offset + target_size).min(source_end);

                if overlap_start < overlap_end {
                    let fragment_size = overlap_end - overlap_start;
                    let src_byte_off = TensorMetadata::WIRE_SIZE + (overlap_start - source_start);
                    let dst_byte_off = overlap_start - target_offset;

                    let raw = self.get_raw_bytes(&source.storage_key, tenant)?;
                    if src_byte_off + fragment_size > raw.len() {
                        return Err(PyRuntimeError::new_err(format!(
                            "source data truncated for key={}: expected at least {} bytes, got {}",
                            source.storage_key,
                            src_byte_off + fragment_size,
                            raw.len()
                        )));
                    }
                    result_data[dst_byte_off..dst_byte_off + fragment_size]
                        .copy_from_slice(&raw[src_byte_off..src_byte_off + fragment_size]);
                }
                source_cumulative_offset = source_end;
            }
        } else {
            for source in &sources {
                let ranges = calculate_strided_shard_ranges(
                    &global_shape,
                    split_dim,
                    element_size,
                    source.src_rank,
                    src_count,
                    target_rank,
                    target_tp_size,
                );
                if let Some(transfers) = ranges {
                    let raw = if !transfers.is_empty() {
                        self.get_raw_bytes(&source.storage_key, tenant)?
                    } else {
                        continue;
                    };
                    for t in &transfers {
                        let src_byte_off = TensorMetadata::WIRE_SIZE + t.src_offset;
                        if src_byte_off + t.size > raw.len() {
                            return Err(PyRuntimeError::new_err(format!(
                                "source data truncated for key={}: expected at least {} bytes, got {}",
                                source.storage_key,
                                src_byte_off + t.size,
                                raw.len()
                            )));
                        }
                        result_data[t.dst_offset..t.dst_offset + t.size]
                            .copy_from_slice(&raw[src_byte_off..src_byte_off + t.size]);
                    }
                }
            }
        }

        let metadata = TensorMetadata::build_shard(
            first.dtype,
            &global_shape,
            &local_shape,
            &target_par.canonicalize().axes,
            target_size as u64,
        );

        let mut output = Vec::with_capacity(TensorMetadata::WIRE_SIZE + target_size);
        output.extend_from_slice(metadata.as_bytes());
        output.extend_from_slice(&result_data);
        Ok(output)
    }

    // ── Full reconstruction path ───────────────────────────────────────────

    fn build_full_into_reconstruction_plan(
        &self,
        base_key: &str,
        buffer_ptr: usize,
        buffer_size: usize,
        tenant: Option<&str>,
        context: &str,
    ) -> PyResult<FullIntoPlan> {
        let discovered = self.discover_shard_sources_cached(base_key, tenant, context)?;
        let DiscoveredShardSources {
            sources,
            query_cache,
        } = discovered;

        let first = sources.first().ok_or_else(|| {
            PyRuntimeError::new_err(format!("no shard sources found for key={base_key}"))
        })?;

        let global_shape = first.global_shape.clone();
        let element_size = TensorDtype::from_i32(first.dtype)
            .map(|d| d.element_size())
            .unwrap_or(1);
        let total_data_size: usize =
            global_shape.iter().map(|&d| d as usize).product::<usize>() * element_size;
        let total_needed = TensorMetadata::WIRE_SIZE + total_data_size;
        let split_dim = first.split_dim as usize;
        let src_count = sources.len();

        if buffer_size < total_needed {
            return Err(PyValueError::new_err(format!(
                "buffer too small for full reconstruction: need {total_needed}, got {buffer_size}"
            )));
        }

        let metadata =
            TensorMetadata::build_full(first.dtype, &global_shape, total_data_size as u64);

        unsafe {
            std::ptr::copy_nonoverlapping(
                metadata.as_bytes().as_ptr(),
                buffer_ptr as *mut u8,
                TensorMetadata::WIRE_SIZE,
            );
        }

        let mut keys = Vec::new();
        let mut dst_offsets = Vec::new();
        let mut src_offsets = Vec::new();
        let mut sizes = Vec::new();

        if split_dim == 0 {
            let mut cumulative = 0usize;
            for source in &sources {
                keys.push(source.storage_key.clone());
                dst_offsets.push(vec![TensorMetadata::WIRE_SIZE + cumulative]);
                src_offsets.push(vec![TensorMetadata::WIRE_SIZE]);
                sizes.push(vec![source.data_bytes]);
                cumulative += source.data_bytes;
            }
        } else {
            for source in &sources {
                let ranges = calculate_strided_shard_ranges(
                    &global_shape,
                    split_dim,
                    element_size,
                    source.src_rank,
                    src_count,
                    0,
                    1,
                );
                if let Some(transfers) = ranges {
                    if transfers.is_empty() {
                        continue;
                    }
                    let mut key_dst_offs = Vec::with_capacity(transfers.len());
                    let mut key_src_offs = Vec::with_capacity(transfers.len());
                    let mut key_sizes = Vec::with_capacity(transfers.len());
                    for t in &transfers {
                        key_dst_offs.push(TensorMetadata::WIRE_SIZE + t.dst_offset);
                        key_src_offs.push(TensorMetadata::WIRE_SIZE + t.src_offset);
                        key_sizes.push(t.size);
                    }
                    keys.push(source.storage_key.clone());
                    dst_offsets.push(key_dst_offs);
                    src_offsets.push(key_src_offs);
                    sizes.push(key_sizes);
                }
            }
        }

        Ok(FullIntoPlan {
            buffer_ptr,
            buffer_size,
            keys,
            dst_offsets,
            src_offsets,
            sizes,
            result: TensorReadResult {
                data_ptr: buffer_ptr + TensorMetadata::WIRE_SIZE,
                data_bytes: total_data_size,
                shape: global_shape,
                dtype: first.dtype,
                total_bytes_read: total_needed,
            },
            query_cache,
        })
    }

    fn execute_full_into_plans(
        &self,
        plans: Vec<FullIntoPlan>,
        tenant: Option<&str>,
    ) -> PyResult<Vec<Option<TensorReadResult>>> {
        if plans.is_empty() {
            return Ok(Vec::new());
        }

        let mut merged_query_cache = ReadQueryResultCache::default();
        let mut has_query_cache = false;
        let mut buffer_ptrs = Vec::with_capacity(plans.len());
        let mut buffer_sizes = Vec::with_capacity(plans.len());
        let mut all_keys = Vec::with_capacity(plans.len());
        let mut all_dst_offsets = Vec::with_capacity(plans.len());
        let mut all_src_offsets = Vec::with_capacity(plans.len());
        let mut all_sizes = Vec::with_capacity(plans.len());
        let mut expected = Vec::with_capacity(plans.len());

        // Route-query caches are request-scoped. They are built while planning
        // this batch and consumed immediately below; they must not be retained
        // across API calls because route ownership can change.
        for mut plan in plans {
            if let Some(cache) = plan.query_cache.take() {
                merged_query_cache.merge(cache);
                has_query_cache = true;
            }
            buffer_ptrs.push(plan.buffer_ptr);
            buffer_sizes.push(plan.buffer_size);
            all_keys.push(plan.keys);
            all_dst_offsets.push(plan.dst_offsets);
            all_src_offsets.push(plan.src_offsets);
            all_sizes.push(plan.sizes);
            expected.push(plan.result);
        }
        let expected_fragment_sizes = all_sizes.clone();

        let range_results = self.get_into_ranges_internal(
            buffer_ptrs,
            all_keys,
            all_dst_offsets,
            all_src_offsets,
            all_sizes,
            Some(buffer_sizes),
            tenant,
            if has_query_cache {
                Some(merged_query_cache)
            } else {
                None
            },
        )?;

        let mut results = Vec::with_capacity(expected.len());
        for (i, result) in expected.into_iter().enumerate() {
            let success = range_results
                .get(i)
                .zip(expected_fragment_sizes.get(i))
                .map(|(key_results, expected_key_sizes)| {
                    key_results.len() == expected_key_sizes.len()
                        && key_results.iter().zip(expected_key_sizes).all(
                            |(fragments, expected_sizes)| {
                                fragments.len() == expected_sizes.len()
                                    && fragments.iter().zip(expected_sizes).all(
                                        |(&value, &expected_size)| {
                                            i64::try_from(expected_size) == Ok(value)
                                        },
                                    )
                            },
                        )
                })
                .unwrap_or(false);
            results.push(if success { Some(result) } else { None });
        }
        Ok(results)
    }

    fn read_full_into(
        &self,
        base_key: &str,
        buffer_ptr: usize,
        buffer_size: usize,
        tenant: Option<&str>,
    ) -> PyResult<TensorReadResult> {
        if let Ok(result) = self.get_tensor_into_internal(base_key, buffer_ptr, buffer_size, tenant)
        {
            return Ok(result);
        }

        let plan = self.build_full_into_reconstruction_plan(
            base_key,
            buffer_ptr,
            buffer_size,
            tenant,
            "read_full_into",
        )?;
        self.execute_full_into_plans(vec![plan], tenant)?
            .into_iter()
            .next()
            .flatten()
            .ok_or_else(|| {
                PyRuntimeError::new_err("full reconstruction: get_into_ranges fragment failed")
            })
    }

    fn read_full_bytes(&self, base_key: &str, tenant: Option<&str>) -> PyResult<Vec<u8>> {
        if let Ok(data) = self.get_raw_bytes(base_key, tenant) {
            return Ok(data);
        }

        let sources = self.discover_shard_sources(base_key, tenant)?;

        let first = sources.first().ok_or_else(|| {
            PyRuntimeError::new_err(format!("no shard sources found for key={base_key}"))
        })?;

        let global_shape = first.global_shape.clone();
        let element_size = TensorDtype::from_i32(first.dtype)
            .map(|d| d.element_size())
            .unwrap_or(1);
        let total_data_size: usize =
            global_shape.iter().map(|&d| d as usize).product::<usize>() * element_size;
        let split_dim = first.split_dim as usize;
        let src_count = sources.len();

        let metadata =
            TensorMetadata::build_full(first.dtype, &global_shape, total_data_size as u64);

        let mut output = vec![0u8; TensorMetadata::WIRE_SIZE + total_data_size];
        output[..TensorMetadata::WIRE_SIZE].copy_from_slice(metadata.as_bytes());

        if split_dim == 0 {
            let mut cumulative = 0usize;
            for source in &sources {
                let raw = self.get_raw_bytes(&source.storage_key, tenant)?;
                if raw.len() > TensorMetadata::WIRE_SIZE {
                    let data = &raw[TensorMetadata::WIRE_SIZE..];
                    let copy_len = source.data_bytes.min(data.len());
                    let dst_start = TensorMetadata::WIRE_SIZE + cumulative;
                    if dst_start + copy_len > output.len() {
                        return Err(PyRuntimeError::new_err(format!(
                            "full reconstruction overflow: dst_start={dst_start}, copy_len={copy_len}, output_len={}",
                            output.len()
                        )));
                    }
                    output[dst_start..dst_start + copy_len].copy_from_slice(&data[..copy_len]);
                    cumulative += copy_len;
                }
            }
        } else {
            for source in &sources {
                let ranges = calculate_strided_shard_ranges(
                    &global_shape,
                    split_dim,
                    element_size,
                    source.src_rank,
                    src_count,
                    0,
                    1,
                );
                if let Some(transfers) = ranges {
                    if transfers.is_empty() {
                        continue;
                    }
                    let raw = self.get_raw_bytes(&source.storage_key, tenant)?;
                    for t in &transfers {
                        let src_byte_off = TensorMetadata::WIRE_SIZE + t.src_offset;
                        let dst_byte_off = TensorMetadata::WIRE_SIZE + t.dst_offset;
                        if src_byte_off + t.size > raw.len() {
                            return Err(PyRuntimeError::new_err(format!(
                                "source data truncated for key={}: expected at least {} bytes, got {}",
                                source.storage_key,
                                src_byte_off + t.size,
                                raw.len()
                            )));
                        }
                        output[dst_byte_off..dst_byte_off + t.size]
                            .copy_from_slice(&raw[src_byte_off..src_byte_off + t.size]);
                    }
                }
            }
        }

        Ok(output)
    }

    // ── Source discovery ────────────────────────────────────────────────────

    fn load_tensor_metadata_prefixes(
        &self,
        keys: &[String],
        tenant: Option<&str>,
        context: &str,
    ) -> PyResult<Option<(Vec<ParsedTensorMetadata>, ReadQueryResultCache)>> {
        let Some(query_cache) = self.batch_query_read_cache_internal(keys, tenant)? else {
            return Ok(None);
        };
        if keys.is_empty() {
            return Ok(Some((Vec::new(), query_cache)));
        }

        let scratch_size = keys
            .len()
            .checked_mul(TensorMetadata::WIRE_SIZE)
            .ok_or_else(|| {
                PyValueError::new_err(format!("{context}: metadata scratch overflow"))
            })?;
        let mut scratch = vec![0u8; scratch_size];
        let scratch_ptr = scratch.as_mut_ptr() as usize;
        if self.register_buffer(scratch_ptr, scratch_size)? != 0 {
            return Ok(None);
        }

        let dst_offsets = (0..keys.len())
            .map(|i| vec![i * TensorMetadata::WIRE_SIZE])
            .collect::<Vec<_>>();
        let src_offsets = (0..keys.len()).map(|_| vec![0]).collect::<Vec<_>>();
        let sizes = (0..keys.len())
            .map(|_| vec![TensorMetadata::WIRE_SIZE])
            .collect::<Vec<_>>();

        let range_results = self.get_into_ranges_internal(
            vec![scratch_ptr],
            vec![keys.to_vec()],
            vec![dst_offsets],
            vec![src_offsets],
            vec![sizes],
            Some(vec![scratch_size]),
            tenant,
            Some(query_cache.clone()),
        );
        let unregister_result = self.unregister_buffer(scratch_ptr, scratch_size);
        match unregister_result {
            Ok(0) => {}
            Ok(_) => return Ok(None),
            Err(error) => return Err(error),
        }
        let range_results = match range_results {
            Ok(results) => results,
            Err(_) => return Ok(None),
        };
        if range_results.len() != 1 || range_results[0].len() != keys.len() {
            return Ok(None);
        }

        let mut parsed = Vec::with_capacity(keys.len());
        for (i, key_results) in range_results[0].iter().enumerate() {
            if key_results.len() != 1 || key_results[0] != TensorMetadata::WIRE_SIZE as i64 {
                return Ok(None);
            }
            let offset = i * TensorMetadata::WIRE_SIZE;
            let metadata =
                TensorMetadata::parse_prefix(&scratch[offset..offset + TensorMetadata::WIRE_SIZE])
                    .ok_or_else(|| {
                        PyRuntimeError::new_err(format!(
                            "{context}: invalid tensor metadata prefix for key={}",
                            keys[i]
                        ))
                    })?;
            parsed.push(metadata);
        }

        Ok(Some((parsed, query_cache)))
    }

    fn sources_from_metadata_prefixes(
        &self,
        keys: Vec<String>,
        global_shape: Vec<i64>,
        split_dim: i32,
        tenant: Option<&str>,
        context: &str,
    ) -> PyResult<Option<DiscoveredShardSources>> {
        let Some((metadata, query_cache)) =
            self.load_tensor_metadata_prefixes(&keys, tenant, context)?
        else {
            return Ok(None);
        };
        if metadata.len() != keys.len() {
            return Ok(None);
        }
        let sources = keys
            .into_iter()
            .zip(metadata)
            .enumerate()
            .map(|(rank, (storage_key, parsed))| ShardSource {
                storage_key,
                data_bytes: parsed.data_bytes,
                global_shape: global_shape.clone(),
                split_dim,
                src_rank: rank,
                dtype: parsed.metadata.header.dtype,
            })
            .collect();
        Ok(Some(DiscoveredShardSources {
            sources,
            query_cache: Some(query_cache),
        }))
    }

    fn discover_shard_sources(
        &self,
        base_key: &str,
        tenant: Option<&str>,
    ) -> PyResult<Vec<ShardSource>> {
        // 1. Try writer manifest (__writer_manifest)
        let manifest_key = WriterShardManifest::manifest_key(base_key);
        if let Ok(manifest_data) = self.get_raw_bytes(&manifest_key, tenant) {
            if let Some(manifest) = WriterShardManifest::parse(&manifest_data) {
                return self.sources_from_writer_manifest(base_key, &manifest, tenant);
            }
        }

        // 2. Try parallelism manifest (__parallelism_manifest)
        let par_manifest_key = get_parallelism_manifest_key(base_key);
        if let Ok(manifest_data) = self.get_raw_bytes(&par_manifest_key, tenant) {
            if let Some(manifest) = WriterShardManifest::parse(&manifest_data) {
                return self.sources_from_parallelism_manifest(base_key, &manifest, tenant);
            }
        }

        // 3. Fallback: legacy _tp_N enumeration
        self.sources_from_legacy_tp(base_key, tenant)
    }

    fn sources_from_writer_manifest(
        &self,
        base_key: &str,
        manifest: &WriterShardManifest,
        tenant: Option<&str>,
    ) -> PyResult<Vec<ShardSource>> {
        let global_shape = manifest.global_shape.to_vec(manifest.ndim as usize);
        let mut sources = Vec::with_capacity(manifest.shard_count as usize);

        for rank in 0..manifest.shard_count {
            let key = get_writer_partition_key_name(
                base_key,
                rank,
                manifest.shard_count,
                manifest.split_dim,
            );
            let raw = self.get_raw_bytes(&key, tenant)?;

            let parsed = TensorMetadata::parse(&raw).ok_or_else(|| {
                PyRuntimeError::new_err(format!(
                    "invalid tensor metadata for writer partition key={key}"
                ))
            })?;

            sources.push(ShardSource {
                storage_key: key,
                data_bytes: parsed.data_bytes,
                global_shape: global_shape.clone(),
                split_dim: manifest.split_dim,
                src_rank: rank as usize,
                dtype: parsed.metadata.header.dtype,
            });
        }
        Ok(sources)
    }

    fn sources_from_parallelism_manifest(
        &self,
        base_key: &str,
        manifest: &WriterShardManifest,
        tenant: Option<&str>,
    ) -> PyResult<Vec<ShardSource>> {
        let global_shape = manifest.global_shape.to_vec(manifest.ndim as usize);
        let mut sources = Vec::with_capacity(manifest.shard_count as usize);

        for rank in 0..manifest.shard_count {
            let tp_spec = TensorParallelismSpec::new(vec![ParallelAxisSpec {
                kind: ParallelAxisKind::TP,
                rank,
                size: manifest.shard_count,
                split_dim: manifest.split_dim,
                expert_id: 0,
                stage_id: 0,
            }]);
            let key = get_parallelism_key_name(base_key, &tp_spec);
            let raw = self.get_raw_bytes(&key, tenant)?;

            let parsed = TensorMetadata::parse(&raw).ok_or_else(|| {
                PyRuntimeError::new_err(format!(
                    "invalid tensor metadata for parallelism shard key={key}"
                ))
            })?;

            sources.push(ShardSource {
                storage_key: key,
                data_bytes: parsed.data_bytes,
                global_shape: global_shape.clone(),
                split_dim: manifest.split_dim,
                src_rank: rank as usize,
                dtype: parsed.metadata.header.dtype,
            });
        }
        Ok(sources)
    }

    fn sources_from_legacy_tp(
        &self,
        base_key: &str,
        tenant: Option<&str>,
    ) -> PyResult<Vec<ShardSource>> {
        let mut sources = Vec::new();

        const MAX_LEGACY_TP_RANKS: u32 = 1024;
        for rank in 0..MAX_LEGACY_TP_RANKS {
            let key = format!("{base_key}_tp_{rank}");
            let raw = match self.get_raw_bytes(&key, tenant) {
                Ok(v) => v,
                Err(_) => break,
            };

            let parsed = match TensorMetadata::parse(&raw) {
                Some(p) => p,
                None => break,
            };

            let ndim = parsed.metadata.header.ndim as usize;
            let global_shape = parsed.metadata.layout.global_shape.to_vec(ndim);
            let split_dim = if parsed.metadata.layout.axis_count > 0 {
                parsed.metadata.layout.axes[0].split_dim
            } else {
                0
            };

            sources.push(ShardSource {
                storage_key: key,
                data_bytes: parsed.data_bytes,
                global_shape,
                split_dim,
                src_rank: rank as usize,
                dtype: parsed.metadata.header.dtype,
            });
        }

        if sources.is_empty() {
            return Err(PyRuntimeError::new_err(format!(
                "no shard sources found for key={base_key} (tried writer manifest and legacy _tp_N)"
            )));
        }

        Ok(sources)
    }

    fn discover_shard_sources_cached(
        &self,
        base_key: &str,
        tenant: Option<&str>,
        context: &str,
    ) -> PyResult<DiscoveredShardSources> {
        let manifest_key = WriterShardManifest::manifest_key(base_key);
        if let Ok(manifest_data) = self.get_raw_bytes(&manifest_key, tenant) {
            if let Some(manifest) = WriterShardManifest::parse(&manifest_data) {
                let global_shape = manifest.global_shape.to_vec(manifest.ndim as usize);
                let keys = (0..manifest.shard_count)
                    .map(|rank| {
                        get_writer_partition_key_name(
                            base_key,
                            rank,
                            manifest.shard_count,
                            manifest.split_dim,
                        )
                    })
                    .collect::<Vec<_>>();
                if let Some(discovered) = self.sources_from_metadata_prefixes(
                    keys,
                    global_shape,
                    manifest.split_dim,
                    tenant,
                    context,
                )? {
                    return Ok(discovered);
                }
                return Ok(DiscoveredShardSources {
                    sources: self.sources_from_writer_manifest(base_key, &manifest, tenant)?,
                    query_cache: None,
                });
            }
        }

        let par_manifest_key = get_parallelism_manifest_key(base_key);
        if let Ok(manifest_data) = self.get_raw_bytes(&par_manifest_key, tenant) {
            if let Some(manifest) = WriterShardManifest::parse(&manifest_data) {
                let global_shape = manifest.global_shape.to_vec(manifest.ndim as usize);
                let keys = (0..manifest.shard_count)
                    .map(|rank| {
                        let tp_spec = TensorParallelismSpec::new(vec![ParallelAxisSpec {
                            kind: ParallelAxisKind::TP,
                            rank,
                            size: manifest.shard_count,
                            split_dim: manifest.split_dim,
                            expert_id: 0,
                            stage_id: 0,
                        }]);
                        get_parallelism_key_name(base_key, &tp_spec)
                    })
                    .collect::<Vec<_>>();
                if let Some(discovered) = self.sources_from_metadata_prefixes(
                    keys,
                    global_shape,
                    manifest.split_dim,
                    tenant,
                    context,
                )? {
                    return Ok(discovered);
                }
                return Ok(DiscoveredShardSources {
                    sources: self.sources_from_parallelism_manifest(base_key, &manifest, tenant)?,
                    query_cache: None,
                });
            }
        }

        Ok(DiscoveredShardSources {
            sources: self.sources_from_legacy_tp(base_key, tenant)?,
            query_cache: None,
        })
    }
}

struct ShardSource {
    storage_key: String,
    data_bytes: usize,
    global_shape: Vec<i64>,
    split_dim: i32,
    src_rank: usize,
    dtype: i32,
}

struct DiscoveredShardSources {
    sources: Vec<ShardSource>,
    query_cache: Option<ReadQueryResultCache>,
}

struct FullIntoPlan {
    buffer_ptr: usize,
    buffer_size: usize,
    keys: Vec<String>,
    dst_offsets: Vec<Vec<usize>>,
    src_offsets: Vec<Vec<usize>>,
    sizes: Vec<Vec<usize>>,
    result: TensorReadResult,
    query_cache: Option<ReadQueryResultCache>,
}
