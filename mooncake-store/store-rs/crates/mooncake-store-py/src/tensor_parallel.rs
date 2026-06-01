//! PyO3 parallel tensor API for `MooncakeDistributedStore`.
//!
//! Adds parallelism-aware put/get operations that handle shard key naming,
//! metadata tagging, and cross-TP scatter-gather reconstruction.

use mooncake_tensor::{
    calculate_shard_range, calculate_strided_shard_ranges, get_parallelism_key_name,
    get_writer_partition_key_name, parallelism_matches_metadata, ParallelAxisKind,
    ParallelAxisSpec, ReadTargetMode, ReadTargetSpec, TensorDtype, TensorMetadata,
    TensorParallelismSpec, WriterPartitionSpec, WriterShardManifest,
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

// ─── Shard calculation helpers ─────────────────────────────────────────────

fn compute_global_shape(local_shape: &[i64], spec: &TensorParallelismSpec) -> Vec<i64> {
    let mut global = local_shape.to_vec();
    if let Some(tp) = spec.tp_axis() {
        let dim = tp.split_dim as usize;
        if dim < global.len() && tp.size > 0 {
            global[dim] = global[dim] * tp.size as i64;
        }
    }
    global
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
                let storage_key = get_parallelism_key_name(key, spec);
                let global_shape = compute_global_shape(&info.shape, spec);
                let metadata = TensorMetadata::build_shard(
                    info.dtype as i32,
                    &global_shape,
                    &info.shape,
                    &spec.canonicalize().axes,
                    info.data_bytes as u64,
                );
                self.put_tensor_object(&storage_key, &metadata, &info, tenant, replica_count)
            }
            WriteRoute::WriterPartition => {
                let wp = wp_spec.expect("WriterPartition route must have wp_spec");
                let storage_key = get_writer_partition_key_name(key, wp.rank);

                let mut global_shape = info.shape.clone();
                let dim = wp.split_dim as usize;
                if dim < global_shape.len() {
                    global_shape[dim] = global_shape[dim] * wp.size as i64;
                }
                let metadata = TensorMetadata::build_shard(
                    info.dtype as i32,
                    &global_shape,
                    &info.shape,
                    &[],
                    info.data_bytes as u64,
                );
                let status =
                    self.put_tensor_object(&storage_key, &metadata, &info, tenant, replica_count)?;
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

        let storage_key = match (&par_spec, &wp_spec) {
            (Some(spec), _) => get_parallelism_key_name(key, spec),
            (_, Some(wp)) => {
                let manifest_key = WriterShardManifest::manifest_key(key);
                let _ = self.remove(&manifest_key, false, tenant);
                get_writer_partition_key_name(key, wp.rank)
            }
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
}

// ─── Internal helpers ──────────────────────────────────────────────────────

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

        self.reconstruct_shard_bytes(base_key, par, tenant)
    }

    fn read_shard_via_reconstruction(
        &self,
        base_key: &str,
        buffer_ptr: usize,
        buffer_size: usize,
        target_par: &TensorParallelismSpec,
        tenant: Option<&str>,
    ) -> PyResult<TensorReadResult> {
        let sources = self.discover_shard_sources(base_key, tenant)?;

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
                local_shape[dim] = local_shape[dim] / tp.size as i64;
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

        let results = self.get_into_ranges(
            vec![buffer_ptr],
            vec![keys],
            vec![dst_offsets],
            vec![src_offsets],
            vec![sizes],
            Some(vec![buffer_size]),
            tenant,
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
                local_shape[dim] = local_shape[dim] / tp.size as i64;
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

        let results = self.get_into_ranges(
            vec![buffer_ptr],
            vec![keys],
            vec![dst_offsets],
            vec![src_offsets],
            vec![sizes],
            Some(vec![buffer_size]),
            tenant,
        )?;

        for key_results in &results[0] {
            for &val in key_results {
                if val <= 0 {
                    return Err(PyRuntimeError::new_err(
                        "full reconstruction: get_into_ranges fragment failed",
                    ));
                }
            }
        }

        Ok(TensorReadResult {
            data_ptr: buffer_ptr + TensorMetadata::WIRE_SIZE,
            data_bytes: total_data_size,
            shape: global_shape,
            dtype: first.dtype,
            total_bytes_read: total_needed,
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
                    let dst_start = TensorMetadata::WIRE_SIZE + cumulative;
                    output[dst_start..dst_start + data.len()].copy_from_slice(data);
                    cumulative += data.len();
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

    fn discover_shard_sources(
        &self,
        base_key: &str,
        tenant: Option<&str>,
    ) -> PyResult<Vec<ShardSource>> {
        let manifest_key = WriterShardManifest::manifest_key(base_key);
        if let Ok(manifest_data) = self.get_raw_bytes(&manifest_key, tenant) {
            if let Some(manifest) = WriterShardManifest::parse(&manifest_data) {
                return self.sources_from_writer_manifest(base_key, &manifest, tenant);
            }
        }

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
            let key = get_writer_partition_key_name(base_key, rank);
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
}

struct ShardSource {
    storage_key: String,
    data_bytes: usize,
    global_shape: Vec<i64>,
    split_dim: i32,
    src_rank: usize,
    dtype: i32,
}
