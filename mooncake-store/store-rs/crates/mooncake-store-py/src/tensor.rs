//! PyO3 tensor API methods for `MooncakeDistributedStore`.
//!
//! This module adds tensor-aware put/get operations that prepend binary
//! `TensorMetadata` to stored objects, enabling typed tensor storage and
//! retrieval with shape/dtype preservation.

use mooncake_tensor::{TensorDtype, TensorMetadata};
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;

use crate::{pointer_from_usize, PyMooncakeDistributedStore};

// ─── Conversion helpers ──────────────────────────────────────────────────────

/// Map numpy/torch dtype string to TensorDtype enum.
pub fn parse_dtype_str(s: &str) -> Option<TensorDtype> {
    match s {
        "float32" | "torch.float32" => Some(TensorDtype::Float32),
        "float64" | "torch.float64" => Some(TensorDtype::Float64),
        "int8" | "torch.int8" => Some(TensorDtype::Int8),
        "uint8" | "torch.uint8" => Some(TensorDtype::Uint8),
        "int16" | "torch.int16" => Some(TensorDtype::Int16),
        "uint16" | "torch.uint16" => Some(TensorDtype::Uint16),
        "int32" | "torch.int32" => Some(TensorDtype::Int32),
        "uint32" | "torch.uint32" => Some(TensorDtype::Uint32),
        "int64" | "torch.int64" => Some(TensorDtype::Int64),
        "uint64" | "torch.uint64" => Some(TensorDtype::Uint64),
        "bool" | "torch.bool" => Some(TensorDtype::Bool),
        "float16" | "torch.float16" => Some(TensorDtype::Float16),
        "bfloat16" | "torch.bfloat16" => Some(TensorDtype::Bfloat16),
        "float8_e4m3fn" | "torch.float8_e4m3fn" => Some(TensorDtype::Float8E4m3),
        "float8_e5m2" | "torch.float8_e5m2" => Some(TensorDtype::Float8E5m2),
        _ => None,
    }
}

/// Tensor info extracted from a Python tensor object.
struct TensorInfo {
    data_ptr: usize,
    data_bytes: usize,
    shape: Vec<i64>,
    dtype: TensorDtype,
}

/// Extract tensor metadata from a Python object (torch.Tensor).
fn extract_tensor_info(tensor: &Bound<'_, PyAny>) -> PyResult<TensorInfo> {
    let data_ptr: usize = tensor.call_method0("data_ptr")?.extract()?;
    let numel: usize = tensor.call_method0("numel")?.extract()?;
    let element_size: usize = tensor.call_method0("element_size")?.extract()?;

    let data_bytes = numel.checked_mul(element_size).ok_or_else(|| {
        PyValueError::new_err("tensor data size overflows usize (numel * element_size)")
    })?;

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

// ─── Result types ───────────────────────────────────────────────────────────

/// Result returned from tensor read operations.
#[pyclass(name = "TensorReadResult")]
#[derive(Debug, Clone)]
pub struct TensorReadResult {
    #[pyo3(get)]
    pub data_ptr: usize,
    #[pyo3(get)]
    pub data_bytes: usize,
    #[pyo3(get)]
    pub shape: Vec<i64>,
    #[pyo3(get)]
    pub dtype: i32,
    #[pyo3(get)]
    pub total_bytes_read: usize,
}

// ─── Tensor PyMethods ────────────────────────────────────────────────────────

#[pymethods]
impl PyMooncakeDistributedStore {
    /// Store a tensor with binary metadata prepended.
    ///
    /// The object stored is `[TensorMetadata(304 bytes) | raw_data]`.
    #[pyo3(signature = (key, tensor, *, tenant = None, replica_count = None))]
    fn put_tensor(
        &self,
        key: &str,
        tensor: &Bound<'_, PyAny>,
        tenant: Option<&str>,
        replica_count: Option<usize>,
    ) -> PyResult<i32> {
        let info = extract_tensor_info(tensor)?;
        let total_size = TensorMetadata::WIRE_SIZE
            .checked_add(info.data_bytes)
            .ok_or_else(|| PyValueError::new_err("total tensor object size overflows usize"))?;
        let metadata =
            TensorMetadata::build_full(info.dtype as i32, &info.shape, info.data_bytes as u64);

        let mut buffer = vec![0u8; total_size];
        buffer[..TensorMetadata::WIRE_SIZE].copy_from_slice(metadata.as_bytes());
        unsafe {
            std::ptr::copy_nonoverlapping(
                info.data_ptr as *const u8,
                buffer.as_mut_ptr().add(TensorMetadata::WIRE_SIZE),
                info.data_bytes,
            );
        }

        // Delegate to existing `put` with default placement args.
        self.put(
            key,
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

    /// Store a tensor using zero-copy from the tensor's data pointer.
    ///
    /// The tensor data must reside in registered memory with at least
    /// `WIRE_SIZE` (304) bytes of writable space before `data_ptr`.
    #[pyo3(signature = (key, tensor, *, tenant = None, replica_count = None))]
    fn put_tensor_from(
        &self,
        key: &str,
        tensor: &Bound<'_, PyAny>,
        tenant: Option<&str>,
        replica_count: Option<usize>,
    ) -> PyResult<i32> {
        let info = extract_tensor_info(tensor)?;
        let total_size = TensorMetadata::WIRE_SIZE
            .checked_add(info.data_bytes)
            .ok_or_else(|| PyValueError::new_err("total tensor object size overflows usize"))?;
        let metadata =
            TensorMetadata::build_full(info.dtype as i32, &info.shape, info.data_bytes as u64);

        let buffer_ptr = info
            .data_ptr
            .checked_sub(TensorMetadata::WIRE_SIZE)
            .ok_or_else(|| {
                PyValueError::new_err(
                    "put_tensor_from: data_ptr is too low; registered buffer must have \
                 at least 304 bytes of space before tensor data",
                )
            })?;

        // Write metadata header into the space before tensor data.
        unsafe {
            std::ptr::copy_nonoverlapping(
                metadata.as_bytes().as_ptr(),
                buffer_ptr as *mut u8,
                TensorMetadata::WIRE_SIZE,
            );
        }

        // Delegate to existing `put_from` with default placement args.
        self.put_from(
            key,
            buffer_ptr,
            total_size,
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

    /// Read a tensor object into a buffer and return parsed result.
    #[pyo3(signature = (key, buffer_ptr, size, *, tenant = None))]
    fn get_tensor_into(
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

        let bytes_read = self.get_into(key, buffer_ptr, size, tenant)?;

        // Safety: buffer_ptr validated non-null above; get_into wrote bytes_read bytes into it.
        let data = unsafe { std::slice::from_raw_parts(buffer_ptr as *const u8, bytes_read) };
        let parsed = TensorMetadata::parse(data).ok_or_else(|| {
            PyRuntimeError::new_err(format!(
                "get_tensor_into: invalid tensor metadata for key={key}"
            ))
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
}
