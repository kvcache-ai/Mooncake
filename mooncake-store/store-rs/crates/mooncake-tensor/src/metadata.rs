//! TensorMetadata binary format — `#[repr(C)]` structs that are byte-compatible
//! with the C++ `integration_utils.h` definitions.
//!
//! Wire layout of a stored tensor object:
//!
//! ```text
//! ┌──────────────────────────┬─────────────────────┐
//! │  TensorMetadata (304 B)  │  raw tensor data    │
//! └──────────────────────────┴─────────────────────┘
//! ```

use std::mem;

// ─── Constants ───────────────────────────────────────────────────────────────

/// Magic number identifying a tensor object (`"MOON"` in ASCII).
pub const TENSOR_OBJECT_MAGIC: u32 = 0x4d4f4f4e;
/// Current wire-format version.
pub const TENSOR_OBJECT_VERSION: u16 = 1;
/// Maximum number of dimensions in a tensor shape.
pub const MAX_TENSOR_DIMS: usize = 8;
/// Maximum number of parallelism layout axes.
pub const MAX_LAYOUT_AXES: usize = 4;

// ─── Enums ───────────────────────────────────────────────────────────────────

/// Tensor element data type (matches C++ `TensorDtype` enum values).
#[repr(i32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TensorDtype {
    Float32 = 0,
    Float64 = 1,
    Int8 = 2,
    Uint8 = 3,
    Int16 = 4,
    Uint16 = 5,
    Int32 = 6,
    Uint32 = 7,
    Int64 = 8,
    Uint64 = 9,
    Bool = 10,
    Float16 = 11,
    Bfloat16 = 12,
    Float8E4m3 = 13,
    Float8E5m2 = 14,
}

impl TensorDtype {
    /// Convert from raw i32 discriminant. Returns `None` for unknown values.
    pub fn from_i32(v: i32) -> Option<Self> {
        match v {
            0 => Some(Self::Float32),
            1 => Some(Self::Float64),
            2 => Some(Self::Int8),
            3 => Some(Self::Uint8),
            4 => Some(Self::Int16),
            5 => Some(Self::Uint16),
            6 => Some(Self::Int32),
            7 => Some(Self::Uint32),
            8 => Some(Self::Int64),
            9 => Some(Self::Uint64),
            10 => Some(Self::Bool),
            11 => Some(Self::Float16),
            12 => Some(Self::Bfloat16),
            13 => Some(Self::Float8E4m3),
            14 => Some(Self::Float8E5m2),
            _ => None,
        }
    }

    /// Element size in bytes (for dtypes with a well-defined fixed width).
    pub fn element_size(self) -> usize {
        match self {
            Self::Float32 | Self::Int32 | Self::Uint32 => 4,
            Self::Float64 | Self::Int64 | Self::Uint64 => 8,
            Self::Int8 | Self::Uint8 | Self::Bool | Self::Float8E4m3 | Self::Float8E5m2 => 1,
            Self::Int16 | Self::Uint16 | Self::Float16 | Self::Bfloat16 => 2,
        }
    }
}

/// Whether the stored tensor is the full global tensor or a shard.
#[repr(u32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TensorLayoutKind {
    Full = 0,
    Shard = 1,
}

// ─── repr(C) wire structs ────────────────────────────────────────────────────

/// Fixed-length shape array (matches C++ `TensorShape`).
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TensorShape {
    pub dims: [i64; MAX_TENSOR_DIMS],
}

impl TensorShape {
    pub const ZERO: Self = Self {
        dims: [0; MAX_TENSOR_DIMS],
    };

    /// Create from a dimension slice, zero-padding unused dims.
    pub fn from_slice(shape: &[i64]) -> Self {
        let mut s = Self::ZERO;
        let n = shape.len().min(MAX_TENSOR_DIMS);
        s.dims[..n].copy_from_slice(&shape[..n]);
        s
    }

    /// Extract the first `ndim` dimensions as a Vec.
    pub fn to_vec(&self, ndim: usize) -> Vec<i64> {
        let n = ndim.min(MAX_TENSOR_DIMS);
        self.dims[..n].to_vec()
    }
}

/// Per-axis layout info (matches C++ `LayoutAxis`).
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LayoutAxis {
    pub kind: i32,
    pub axis_index: i32,
    pub shard_rank: i32,
    pub shard_count: i32,
    pub split_dim: i32,
    pub reserved0: i32,
    pub reserved1: i64,
}

impl LayoutAxis {
    pub const ZERO: Self = Self {
        kind: 0,
        axis_index: 0,
        shard_rank: 0,
        shard_count: 0,
        split_dim: 0,
        reserved0: 0,
        reserved1: 0,
    };
}

/// Tensor layout metadata block (matches C++ `TensorLayoutMetadata`).
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TensorLayoutMetadata {
    pub global_shape: TensorShape,
    pub local_shape: TensorShape,
    pub axis_count: u32,
    pub reserved0: u32,
    pub axes: [LayoutAxis; MAX_LAYOUT_AXES],
}

/// Tensor object header (matches C++ `TensorObjectHeader`).
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TensorObjectHeader {
    pub magic: u32,
    pub version: u16,
    pub header_size: u16,
    pub dtype: i32,
    pub ndim: i32,
    pub layout_kind: u32,
    pub reserved_flags: u32,
    pub data_offset: u64,
    pub data_bytes: u64,
}

/// Combined wire-format metadata (header + layout).
/// This struct is written/read verbatim to/from the object prefix.
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TensorMetadata {
    pub header: TensorObjectHeader,
    pub layout: TensorLayoutMetadata,
}

// ─── Parsed result ───────────────────────────────────────────────────────────

/// Convenience struct returned after successful parse/validation.
#[derive(Debug, Clone)]
pub struct ParsedTensorMetadata {
    pub metadata: TensorMetadata,
    pub data_offset: usize,
    pub data_bytes: usize,
}

// ─── Construction ────────────────────────────────────────────────────────────

impl TensorMetadata {
    /// Build metadata for a full (non-sharded) tensor.
    pub fn build_full(dtype: i32, shape: &[i64], data_bytes: u64) -> Self {
        let ndim = shape.len().min(MAX_TENSOR_DIMS) as i32;
        let data_offset = mem::size_of::<TensorMetadata>() as u64;

        Self {
            header: TensorObjectHeader {
                magic: TENSOR_OBJECT_MAGIC,
                version: TENSOR_OBJECT_VERSION,
                header_size: mem::size_of::<TensorMetadata>() as u16,
                dtype,
                ndim,
                layout_kind: TensorLayoutKind::Full as u32,
                reserved_flags: 0,
                data_offset,
                data_bytes,
            },
            layout: TensorLayoutMetadata {
                global_shape: TensorShape::from_slice(shape),
                local_shape: TensorShape::from_slice(shape),
                axis_count: 0,
                reserved0: 0,
                axes: [LayoutAxis::ZERO; MAX_LAYOUT_AXES],
            },
        }
    }

    /// Size of the wire-format metadata structure in bytes.
    pub const WIRE_SIZE: usize = mem::size_of::<TensorMetadata>();

    /// Serialize to bytes (safe because of `#[repr(C)]`).
    pub fn as_bytes(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self as *const Self as *const u8, Self::WIRE_SIZE) }
    }

    /// Validate and parse from raw bytes.
    pub fn parse(data: &[u8]) -> Option<ParsedTensorMetadata> {
        if data.len() < Self::WIRE_SIZE {
            return None;
        }

        // Safety: TensorMetadata is repr(C) with fixed layout, and we've checked size.
        let metadata: TensorMetadata =
            unsafe { std::ptr::read_unaligned(data.as_ptr() as *const TensorMetadata) };

        if !metadata.validate(data.len()) {
            return None;
        }

        Some(ParsedTensorMetadata {
            data_offset: metadata.header.data_offset as usize,
            data_bytes: metadata.header.data_bytes as usize,
            metadata,
        })
    }

    /// Validate and parse only the fixed metadata prefix.
    ///
    /// This is useful for read planners that need dtype, shape, and payload
    /// length without fetching the full tensor object. It intentionally skips
    /// the `data_offset + data_bytes <= total_length` check because the caller
    /// only supplied the prefix.
    pub fn parse_prefix(data: &[u8]) -> Option<ParsedTensorMetadata> {
        if data.len() < Self::WIRE_SIZE {
            return None;
        }

        // Safety: TensorMetadata is repr(C) with fixed layout, and we've checked size.
        let metadata: TensorMetadata =
            unsafe { std::ptr::read_unaligned(data.as_ptr() as *const TensorMetadata) };

        if !metadata.validate_prefix() {
            return None;
        }

        Some(ParsedTensorMetadata {
            data_offset: metadata.header.data_offset as usize,
            data_bytes: metadata.header.data_bytes as usize,
            metadata,
        })
    }

    /// Validate metadata invariants.
    pub fn validate(&self, total_length: usize) -> bool {
        if !self.validate_prefix() {
            return false;
        }
        if self.header.data_offset as usize > total_length {
            return false;
        }
        let end = self.header.data_offset.checked_add(self.header.data_bytes);
        match end {
            Some(e) => match usize::try_from(e) {
                Ok(end) if end <= total_length => {}
                _ => return false,
            },
            _ => return false,
        }
        true
    }

    /// Validate fixed-prefix metadata invariants that do not require the object
    /// payload length.
    pub fn validate_prefix(&self) -> bool {
        if self.header.magic != TENSOR_OBJECT_MAGIC {
            return false;
        }
        if self.header.version != TENSOR_OBJECT_VERSION {
            return false;
        }
        if self.header.header_size as usize != Self::WIRE_SIZE {
            return false;
        }
        if self.header.ndim < 0 || self.header.ndim as usize > MAX_TENSOR_DIMS {
            return false;
        }
        if TensorDtype::from_i32(self.header.dtype).is_none() {
            return false;
        }
        let Ok(data_offset) = usize::try_from(self.header.data_offset) else {
            return false;
        };
        if data_offset < Self::WIRE_SIZE {
            return false;
        }
        if usize::try_from(self.header.data_bytes).is_err()
            || self
                .header
                .data_offset
                .checked_add(self.header.data_bytes)
                .is_none()
        {
            return false;
        }
        if self.layout.axis_count as usize > MAX_LAYOUT_AXES {
            return false;
        }
        if self.header.layout_kind == TensorLayoutKind::Shard as u32 && self.layout.axis_count == 0
        {
            return false;
        }
        true
    }

    /// Total object size (metadata + data). Returns `None` on overflow.
    pub fn total_length(&self) -> Option<usize> {
        let offset = usize::try_from(self.header.data_offset).ok()?;
        let data = usize::try_from(self.header.data_bytes).ok()?;
        offset.checked_add(data)
    }
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_wire_size_matches_c() {
        // C++ sizeof(TensorMetadata) = sizeof(TensorObjectHeader) + sizeof(TensorLayoutMetadata)
        // Header: 4+2+2+4+4+4+4+8+8 = 40 bytes
        // LayoutAxis: 4+4+4+4+4+4+8 = 32 bytes, ×4 = 128
        // Layout: 64(global_shape) + 64(local_shape) + 4 + 4 + 128 = 264 bytes
        // Total = 40 + 264 = 304 bytes
        assert_eq!(mem::size_of::<TensorObjectHeader>(), 40);
        assert_eq!(mem::size_of::<LayoutAxis>(), 32);
        assert_eq!(mem::size_of::<TensorLayoutMetadata>(), 264);
        assert_eq!(TensorMetadata::WIRE_SIZE, 304);
    }

    #[test]
    fn test_build_full_roundtrip() {
        let shape = [4, 8, 16];
        let data_bytes = 4 * 8 * 16 * 4; // float32
        let meta = TensorMetadata::build_full(TensorDtype::Float32 as i32, &shape, data_bytes);

        assert_eq!(meta.header.magic, TENSOR_OBJECT_MAGIC);
        assert_eq!(meta.header.version, TENSOR_OBJECT_VERSION);
        assert_eq!(meta.header.ndim, 3);
        assert_eq!(meta.header.dtype, 0); // Float32
        assert_eq!(meta.header.layout_kind, TensorLayoutKind::Full as u32);
        assert_eq!(meta.header.data_bytes, data_bytes);
        assert_eq!(meta.header.data_offset as usize, TensorMetadata::WIRE_SIZE);
        assert_eq!(meta.layout.axis_count, 0);

        // Roundtrip via bytes
        let bytes = meta.as_bytes();
        let total_len = TensorMetadata::WIRE_SIZE + data_bytes as usize;
        let mut buf = vec![0u8; total_len];
        buf[..TensorMetadata::WIRE_SIZE].copy_from_slice(bytes);
        let parsed = TensorMetadata::parse(&buf).unwrap();
        assert_eq!(parsed.metadata, meta);
        assert_eq!(parsed.data_offset, TensorMetadata::WIRE_SIZE);
        assert_eq!(parsed.data_bytes, data_bytes as usize);
    }

    #[test]
    fn test_validate_rejects_bad_magic() {
        let meta = TensorMetadata::build_full(0, &[4], 16);
        let mut buf = vec![0u8; TensorMetadata::WIRE_SIZE + 16];
        buf[..TensorMetadata::WIRE_SIZE].copy_from_slice(meta.as_bytes());
        // corrupt magic
        buf[0] = 0xFF;
        assert!(TensorMetadata::parse(&buf).is_none());
    }

    #[test]
    fn test_validate_rejects_truncated() {
        let meta = TensorMetadata::build_full(0, &[4], 16);
        let bytes = meta.as_bytes();
        // too short to contain full header
        assert!(TensorMetadata::parse(&bytes[..10]).is_none());
        // header says data extends beyond buffer
        assert!(TensorMetadata::parse(bytes).is_none()); // only 304 bytes, need 304+16=320
    }

    #[test]
    fn test_parse_prefix_accepts_header_without_payload() {
        let shape = [8, 16];
        let data_bytes = 8 * 16 * 4;
        let meta = TensorMetadata::build_full(TensorDtype::Float32 as i32, &shape, data_bytes);
        let bytes = meta.as_bytes();

        assert!(TensorMetadata::parse(bytes).is_none());

        let parsed = TensorMetadata::parse_prefix(bytes).unwrap();
        assert_eq!(parsed.metadata, meta);
        assert_eq!(parsed.data_offset, TensorMetadata::WIRE_SIZE);
        assert_eq!(parsed.data_bytes, data_bytes as usize);
    }

    #[test]
    fn test_tensor_shape_from_slice() {
        let s = TensorShape::from_slice(&[2, 3, 4]);
        assert_eq!(s.to_vec(3), vec![2, 3, 4]);
        assert_eq!(s.dims[3..], [0; 5]);
    }
}
