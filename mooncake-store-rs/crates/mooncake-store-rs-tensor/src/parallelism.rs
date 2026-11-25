//! Parallelism-aware tensor layout types and key-naming utilities.
//!
//! These types describe how a tensor is distributed across parallel ranks
//! (DP, TP, EP, PP) and provide deterministic storage-key generation so
//! that readers and writers with different layouts can interoperate.

use std::fmt;
use std::mem;

use crate::metadata::{
    LayoutAxis, TensorLayoutKind, TensorLayoutMetadata, TensorMetadata, TensorObjectHeader,
    TensorShape, MAX_LAYOUT_AXES, MAX_TENSOR_DIMS, TENSOR_OBJECT_MAGIC, TENSOR_OBJECT_VERSION,
};

// ─── Axis kind ──────────────────────────────────────────────────────────────

/// Parallel axis kind, ordered by canonical sort priority.
#[repr(i32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum ParallelAxisKind {
    DP = 0,
    TP = 1,
    EP = 2,
    PP = 3,
}

impl ParallelAxisKind {
    pub fn from_i32(v: i32) -> Option<Self> {
        match v {
            0 => Some(Self::DP),
            1 => Some(Self::TP),
            2 => Some(Self::EP),
            3 => Some(Self::PP),
            _ => None,
        }
    }

    fn short_name(self) -> &'static str {
        match self {
            Self::DP => "dp",
            Self::TP => "tp",
            Self::EP => "ep",
            Self::PP => "pp",
        }
    }
}

impl fmt::Display for ParallelAxisKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.short_name())
    }
}

// ─── Axis spec ──────────────────────────────────────────────────────────────

/// One axis of a parallelism layout.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ParallelAxisSpec {
    pub kind: ParallelAxisKind,
    pub rank: i32,
    pub size: i32,
    pub split_dim: i32,
    pub expert_id: i32,
    pub stage_id: i32,
}

impl ParallelAxisSpec {
    pub fn new(kind: ParallelAxisKind, rank: i32, size: i32) -> Self {
        Self {
            kind,
            rank,
            size,
            split_dim: 0,
            expert_id: 0,
            stage_id: 0,
        }
    }

    pub fn with_split_dim(mut self, split_dim: i32) -> Self {
        self.split_dim = split_dim;
        self
    }

    pub fn with_expert_id(mut self, expert_id: i32) -> Self {
        self.expert_id = expert_id;
        self
    }

    pub fn with_stage_id(mut self, stage_id: i32) -> Self {
        self.stage_id = stage_id;
        self
    }

    /// Validate that rank/size are sane.
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.size <= 0 {
            return Err("axis size must be positive");
        }
        if self.rank < 0 || self.rank >= self.size {
            return Err("axis rank must be in [0, size)");
        }
        Ok(())
    }

    /// Convert to the wire-format `LayoutAxis`.
    pub fn to_layout_axis(&self) -> LayoutAxis {
        LayoutAxis {
            kind: self.kind as i32,
            axis_index: 0,
            shard_rank: self.rank,
            shard_count: self.size,
            split_dim: self.split_dim,
            reserved0: self.expert_id,
            reserved1: self.stage_id as i64,
        }
    }

    /// Parse from a wire-format `LayoutAxis`.
    pub fn from_layout_axis(axis: &LayoutAxis) -> Option<Self> {
        let kind = ParallelAxisKind::from_i32(axis.kind)?;
        if axis.reserved1 < i32::MIN as i64 || axis.reserved1 > i32::MAX as i64 {
            return None;
        }
        Some(Self {
            kind,
            rank: axis.shard_rank,
            size: axis.shard_count,
            split_dim: axis.split_dim,
            expert_id: axis.reserved0,
            stage_id: axis.reserved1 as i32,
        })
    }
}

// ─── Parallelism spec ───────────────────────────────────────────────────────

/// Complete parallelism specification — a list of axes.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TensorParallelismSpec {
    pub axes: Vec<ParallelAxisSpec>,
}

impl TensorParallelismSpec {
    pub fn new(axes: Vec<ParallelAxisSpec>) -> Self {
        Self { axes }
    }

    /// Sort axes by canonical order (DP < TP < EP < PP) and deduplicate
    /// axis kinds. Returns a new spec.
    pub fn canonicalize(&self) -> Self {
        let mut sorted = self.axes.clone();
        sorted.sort_by_key(|a| a.kind);
        Self { axes: sorted }
    }

    pub fn validate(&self) -> Result<(), &'static str> {
        if self.axes.is_empty() {
            return Err("parallelism spec must have at least one axis");
        }
        if self.axes.len() > MAX_LAYOUT_AXES {
            return Err("too many parallelism axes");
        }
        for axis in &self.axes {
            axis.validate()?;
        }
        Ok(())
    }

    /// Find the first TP axis, if any.
    pub fn tp_axis(&self) -> Option<&ParallelAxisSpec> {
        self.axes.iter().find(|a| a.kind == ParallelAxisKind::TP)
    }

    /// True if this spec is a single TP axis (legacy-compatible path).
    pub fn is_single_tp(&self) -> bool {
        self.axes.len() == 1 && self.axes[0].kind == ParallelAxisKind::TP
    }

    /// Build wire-format `LayoutAxis` array.
    pub fn to_layout_axes(&self) -> [LayoutAxis; MAX_LAYOUT_AXES] {
        let mut axes = [LayoutAxis::ZERO; MAX_LAYOUT_AXES];
        for (i, spec) in self.axes.iter().enumerate().take(MAX_LAYOUT_AXES) {
            axes[i] = spec.to_layout_axis();
        }
        axes
    }
}

// ─── Read target ────────────────────────────────────────────────────────────

/// How the reader wants to consume the stored tensor.
#[repr(i32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ReadTargetMode {
    AsStored = 0,
    Shard = 1,
    Full = 2,
}

impl ReadTargetMode {
    pub fn from_i32(v: i32) -> Option<Self> {
        match v {
            0 => Some(Self::AsStored),
            1 => Some(Self::Shard),
            2 => Some(Self::Full),
            _ => None,
        }
    }
}

/// Read target: mode + optional parallelism for SHARD mode.
#[derive(Debug, Clone)]
pub struct ReadTargetSpec {
    pub mode: ReadTargetMode,
    pub parallelism: Option<TensorParallelismSpec>,
}

impl ReadTargetSpec {
    pub fn as_stored() -> Self {
        Self {
            mode: ReadTargetMode::AsStored,
            parallelism: None,
        }
    }

    pub fn full() -> Self {
        Self {
            mode: ReadTargetMode::Full,
            parallelism: None,
        }
    }

    pub fn shard(parallelism: TensorParallelismSpec) -> Self {
        Self {
            mode: ReadTargetMode::Shard,
            parallelism: Some(parallelism),
        }
    }
}

// ─── Writer partition ───────────────────────────────────────────────────────

/// Lightweight write-side shorthand: "I am writer rank R of N, splitting
/// along dimension D."
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct WriterPartitionSpec {
    pub rank: i32,
    pub size: i32,
    pub split_dim: i32,
}

impl WriterPartitionSpec {
    pub fn new(rank: i32, size: i32, split_dim: i32) -> Self {
        Self {
            rank,
            size,
            split_dim,
        }
    }

    pub fn validate(&self) -> Result<(), &'static str> {
        if self.size <= 0 {
            return Err("writer partition size must be positive");
        }
        if self.rank < 0 || self.rank >= self.size {
            return Err("writer partition rank must be in [0, size)");
        }
        if self.split_dim < 0 {
            return Err("writer partition split_dim must be non-negative");
        }
        Ok(())
    }
}

// ─── Writer shard manifest ──────────────────────────────────────────────────

/// Manifest stored alongside writer-partition shards to enable
/// reconstruction of the full tensor.
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WriterShardManifest {
    pub magic: u32,
    pub version: u16,
    pub _pad: u16,
    pub dtype: i32,
    pub ndim: i32,
    pub split_dim: i32,
    pub shard_count: i32,
    pub global_shape: TensorShape,
}

impl WriterShardManifest {
    pub const WIRE_SIZE: usize = mem::size_of::<Self>();

    pub fn new(
        dtype: i32,
        ndim: i32,
        split_dim: i32,
        shard_count: i32,
        global_shape: &[i64],
    ) -> Self {
        Self {
            magic: TENSOR_OBJECT_MAGIC,
            version: TENSOR_OBJECT_VERSION,
            _pad: 0,
            dtype,
            ndim,
            split_dim,
            shard_count,
            global_shape: TensorShape::from_slice(global_shape),
        }
    }

    pub fn as_bytes(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self as *const Self as *const u8, Self::WIRE_SIZE) }
    }

    pub fn parse(data: &[u8]) -> Option<Self> {
        if data.len() < Self::WIRE_SIZE {
            return None;
        }
        let manifest: Self = unsafe { std::ptr::read_unaligned(data.as_ptr() as *const Self) };
        if manifest.magic != TENSOR_OBJECT_MAGIC {
            return None;
        }
        if manifest.version != TENSOR_OBJECT_VERSION {
            return None;
        }
        if manifest.ndim < 0 || manifest.ndim as usize > MAX_TENSOR_DIMS {
            return None;
        }
        if manifest.shard_count <= 0 {
            return None;
        }
        if manifest.split_dim < 0 {
            return None;
        }
        Some(manifest)
    }

    pub fn manifest_key(base_key: &str) -> String {
        format!("{base_key}__writer_manifest")
    }
}

// ─── Key naming ─────────────────────────────────────────────────────────────

/// Generate the storage key for a parallelism-tagged object.
///
/// - Single TP axis: legacy format `{base}_tp_{rank}` for compatibility.
/// - Multi-axis: `{base}__dp_{rank}of{size}__tp_{rank}of{size}_sd{split_dim}`
pub fn get_parallelism_key_name(base_key: &str, spec: &TensorParallelismSpec) -> String {
    let canonical = spec.canonicalize();

    if canonical.is_single_tp() {
        let tp = &canonical.axes[0];
        return format!("{base_key}_tp_{}", tp.rank);
    }

    let mut key = base_key.to_string();
    for axis in &canonical.axes {
        match axis.kind {
            ParallelAxisKind::TP => {
                key.push_str(&format!(
                    "__{}_{}of{}_sd{}",
                    axis.kind.short_name(),
                    axis.rank,
                    axis.size,
                    axis.split_dim
                ));
            }
            _ => {
                key.push_str(&format!(
                    "__{}_{}of{}",
                    axis.kind.short_name(),
                    axis.rank,
                    axis.size
                ));
            }
        }
    }
    key
}

/// Generate a writer-partition shard key.
pub fn get_writer_partition_key_name(
    base_key: &str,
    rank: i32,
    size: i32,
    split_dim: i32,
) -> String {
    format!("{base_key}__writer_{rank}of{size}_sd{split_dim}")
}

/// Generate a parallelism manifest key.
pub fn get_parallelism_manifest_key(base_key: &str) -> String {
    format!("{base_key}__parallelism_manifest")
}

// ─── Shard range calculation ────────────────────────────────────────────────

/// Validate that a dimension can be uniformly divided by the shard count.
pub fn validate_uniform_shard(dim_size: i64, shard_count: i32) -> Result<(), &'static str> {
    if shard_count <= 0 {
        return Err("shard count must be positive");
    }
    if dim_size <= 0 {
        return Err("dimension size must be positive");
    }
    if dim_size % shard_count as i64 != 0 {
        return Err("dimension not uniformly divisible by shard count");
    }
    Ok(())
}

/// Compute (offset, size) for a uniform shard of `total_size` elements.
///
/// Returns `(byte_offset, byte_count)` suitable for slicing into a flat buffer.
pub fn calculate_shard_range(total_size: usize, rank: usize, count: usize) -> (usize, usize) {
    if count == 0 {
        return (0, 0);
    }
    let base_size = total_size / count;
    let remainder = total_size % count;
    let offset = base_size * rank + remainder.min(rank);
    let size = base_size + if rank < remainder { 1 } else { 0 };
    (offset, size)
}

// ─── Metadata matching ──────────────────────────────────────────────────────

/// Check whether a requested parallelism spec matches the stored metadata axes.
pub fn parallelism_matches_metadata(
    spec: &TensorParallelismSpec,
    metadata: &TensorMetadata,
) -> bool {
    let canonical = spec.canonicalize();
    if canonical.axes.len() != metadata.layout.axis_count as usize {
        return false;
    }
    for (i, axis_spec) in canonical.axes.iter().enumerate() {
        let stored = &metadata.layout.axes[i];
        if stored.kind != axis_spec.kind as i32
            || stored.shard_rank != axis_spec.rank
            || stored.shard_count != axis_spec.size
        {
            return false;
        }
        if axis_spec.kind == ParallelAxisKind::TP && stored.split_dim != axis_spec.split_dim {
            return false;
        }
    }
    true
}

// ─── Shard metadata construction ────────────────────────────────────────────

impl TensorMetadata {
    /// Build metadata for a sharded tensor.
    pub fn build_shard(
        dtype: i32,
        global_shape: &[i64],
        local_shape: &[i64],
        axes: &[ParallelAxisSpec],
        data_bytes: u64,
    ) -> Self {
        debug_assert_eq!(
            local_shape.len(),
            global_shape.len(),
            "local_shape and global_shape must have same length"
        );
        debug_assert!(
            global_shape
                .iter()
                .zip(local_shape.iter())
                .all(|(g, l)| l <= g),
            "local_shape dimensions must not exceed global_shape"
        );

        let ndim = global_shape.len().min(MAX_TENSOR_DIMS) as i32;
        let data_offset = mem::size_of::<TensorMetadata>() as u64;
        let axis_count = axes.len().min(MAX_LAYOUT_AXES) as u32;

        let mut layout_axes = [LayoutAxis::ZERO; MAX_LAYOUT_AXES];
        for (i, spec) in axes.iter().enumerate().take(MAX_LAYOUT_AXES) {
            layout_axes[i] = spec.to_layout_axis();
        }

        Self {
            header: TensorObjectHeader {
                magic: TENSOR_OBJECT_MAGIC,
                version: TENSOR_OBJECT_VERSION,
                header_size: mem::size_of::<TensorMetadata>() as u16,
                dtype,
                ndim,
                layout_kind: TensorLayoutKind::Shard as u32,
                reserved_flags: 0,
                data_offset,
                data_bytes,
            },
            layout: TensorLayoutMetadata {
                global_shape: TensorShape::from_slice(global_shape),
                local_shape: TensorShape::from_slice(local_shape),
                axis_count,
                reserved0: 0,
                axes: layout_axes,
            },
        }
    }
}

// ─── Strided shard range calculation ───────────────────────────────────────

/// A single byte-range transfer: copy `size` bytes from `src_offset` to `dst_offset`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ByteRangeTransfer {
    pub src_offset: usize,
    pub dst_offset: usize,
    pub size: usize,
}

/// Compute scatter-gather byte ranges for extracting a target shard from a source
/// shard when `split_dim > 0` (non-leading dimension split).
///
/// For split_dim=0, the data is contiguous and `calculate_shard_range` suffices.
/// For split_dim>0, each source shard stores data in row-major order with a
/// different stride layout, so we need multiple non-contiguous transfers.
///
/// Parameters:
/// - `global_shape`: full tensor shape
/// - `split_dim`: dimension along which sharding occurs
/// - `element_size`: bytes per element
/// - `src_rank`, `src_count`: which source shard (rank) out of how many
/// - `tgt_rank`, `tgt_count`: which target shard (rank) out of how many
///
/// Returns `None` if no data from this source overlaps the target, or if
/// parameters are invalid. Otherwise returns the list of byte-range transfers
/// where offsets are relative to the DATA portion (after metadata header).
pub fn calculate_strided_shard_ranges(
    global_shape: &[i64],
    split_dim: usize,
    element_size: usize,
    src_rank: usize,
    src_count: usize,
    tgt_rank: usize,
    tgt_count: usize,
) -> Option<Vec<ByteRangeTransfer>> {
    if split_dim >= global_shape.len() || src_count == 0 || tgt_count == 0 {
        return None;
    }

    let global_dim_size = global_shape[split_dim] as usize;
    if global_dim_size == 0 {
        return None;
    }

    let src_dim_size = global_dim_size / src_count;
    let tgt_dim_size = global_dim_size / tgt_count;

    let src_dim_start = src_rank * src_dim_size;
    let src_dim_end = src_dim_start + src_dim_size;

    let tgt_dim_start = tgt_rank * tgt_dim_size;
    let tgt_dim_end = tgt_dim_start + tgt_dim_size;

    let overlap_start = src_dim_start.max(tgt_dim_start);
    let overlap_end = src_dim_end.min(tgt_dim_end);
    if overlap_start >= overlap_end {
        return Some(vec![]);
    }

    let overlap_count = overlap_end - overlap_start;

    // "inner size" = number of bytes per row at split_dim level
    // = element_size * product(shape[split_dim+1:])
    let inner_size: usize = global_shape[split_dim + 1..]
        .iter()
        .map(|&d| d as usize)
        .product::<usize>()
        * element_size;

    // "outer count" = number of independent "planes" above split_dim
    // = product(shape[:split_dim])
    let outer_count: usize = global_shape[..split_dim]
        .iter()
        .map(|&d| d as usize)
        .product::<usize>()
        .max(1);

    // Within the source shard, row layout: outer_count rows, each of size src_dim_size * inner_size
    let src_row_bytes = src_dim_size * inner_size;
    // Within the target shard, row layout: outer_count rows, each of size tgt_dim_size * inner_size
    let tgt_row_bytes = tgt_dim_size * inner_size;

    let local_src_start = overlap_start - src_dim_start;
    let local_tgt_start = overlap_start - tgt_dim_start;
    let transfer_bytes = overlap_count * inner_size;

    let mut transfers = Vec::with_capacity(outer_count);
    for outer_idx in 0..outer_count {
        let src_offset = outer_idx * src_row_bytes + local_src_start * inner_size;
        let dst_offset = outer_idx * tgt_row_bytes + local_tgt_start * inner_size;
        transfers.push(ByteRangeTransfer {
            src_offset,
            dst_offset,
            size: transfer_bytes,
        });
    }

    Some(transfers)
}

// ─── Raw shard write planning ─────────────────────────────────────────────

/// Plan for extracting a shard from a full-tensor raw buffer.
/// `data_ranges` are `(src_offset_from_data_start, size)` pairs.
#[derive(Debug, Clone)]
pub struct RawShardWritePlan {
    pub shard_shape: Vec<i64>,
    pub shard_bytes: usize,
    pub data_ranges: Vec<(usize, usize)>,
}

/// Compute the byte ranges within a full-tensor data section that correspond
/// to a specific shard along `split_dim`.
///
/// Returns `None` if parameters are invalid.
pub fn build_raw_shard_write_plan(
    global_shape: &[i64],
    split_dim: usize,
    rank: usize,
    count: usize,
    element_size: usize,
) -> Option<RawShardWritePlan> {
    if count == 0 || split_dim >= global_shape.len() {
        return None;
    }
    let dim_size = global_shape[split_dim] as usize;
    if dim_size == 0 || !dim_size.is_multiple_of(count) {
        return None;
    }

    let shard_extent = dim_size / count;
    let shard_start = rank * shard_extent;

    let mut shard_shape = global_shape.to_vec();
    shard_shape[split_dim] = shard_extent as i64;

    let shard_numel: usize = shard_shape.iter().map(|&d| d as usize).product();
    let shard_bytes = shard_numel * element_size;

    if split_dim == 0 {
        let inner: usize = global_shape[1..]
            .iter()
            .map(|&d| d as usize)
            .product::<usize>()
            .max(1);
        let offset = shard_start * inner * element_size;
        return Some(RawShardWritePlan {
            shard_shape,
            shard_bytes,
            data_ranges: vec![(offset, shard_bytes)],
        });
    }

    // For split_dim > 0: strided extraction
    let elements_before: usize = global_shape[..split_dim]
        .iter()
        .map(|&d| d as usize)
        .product::<usize>()
        .max(1);

    let elements_after: usize = global_shape[split_dim + 1..]
        .iter()
        .map(|&d| d as usize)
        .product::<usize>()
        .max(1);

    let row_bytes = shard_extent * elements_after * element_size;
    let full_row_bytes = dim_size * elements_after * element_size;

    let mut data_ranges = Vec::with_capacity(elements_before);
    for slice_idx in 0..elements_before {
        let src_offset = slice_idx * full_row_bytes + shard_start * elements_after * element_size;
        data_ranges.push((src_offset, row_bytes));
    }

    Some(RawShardWritePlan {
        shard_shape,
        shard_bytes,
        data_ranges,
    })
}

// ─── Tests ──────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::TensorDtype;

    #[test]
    fn test_canonicalize_sorts_axes() {
        let spec = TensorParallelismSpec::new(vec![
            ParallelAxisSpec::new(ParallelAxisKind::TP, 0, 4),
            ParallelAxisSpec::new(ParallelAxisKind::DP, 1, 2),
        ]);
        let canonical = spec.canonicalize();
        assert_eq!(canonical.axes[0].kind, ParallelAxisKind::DP);
        assert_eq!(canonical.axes[1].kind, ParallelAxisKind::TP);
    }

    #[test]
    fn test_canonicalize_preserves_values() {
        let spec = TensorParallelismSpec::new(vec![
            ParallelAxisSpec::new(ParallelAxisKind::PP, 1, 4).with_stage_id(3),
            ParallelAxisSpec::new(ParallelAxisKind::DP, 0, 2),
            ParallelAxisSpec::new(ParallelAxisKind::TP, 2, 8).with_split_dim(1),
        ]);
        let canonical = spec.canonicalize();
        assert_eq!(canonical.axes.len(), 3);
        assert_eq!(canonical.axes[0].kind, ParallelAxisKind::DP);
        assert_eq!(canonical.axes[0].rank, 0);
        assert_eq!(canonical.axes[1].kind, ParallelAxisKind::TP);
        assert_eq!(canonical.axes[1].split_dim, 1);
        assert_eq!(canonical.axes[2].kind, ParallelAxisKind::PP);
        assert_eq!(canonical.axes[2].stage_id, 3);
    }

    #[test]
    fn test_key_name_single_tp() {
        let spec =
            TensorParallelismSpec::new(vec![ParallelAxisSpec::new(ParallelAxisKind::TP, 2, 4)]);
        assert_eq!(
            get_parallelism_key_name("model.weight", &spec),
            "model.weight_tp_2"
        );
    }

    #[test]
    fn test_key_name_multi_axis() {
        let spec = TensorParallelismSpec::new(vec![
            ParallelAxisSpec::new(ParallelAxisKind::TP, 1, 4).with_split_dim(0),
            ParallelAxisSpec::new(ParallelAxisKind::DP, 0, 2),
        ]);
        assert_eq!(
            get_parallelism_key_name("model.weight", &spec),
            "model.weight__dp_0of2__tp_1of4_sd0"
        );
    }

    #[test]
    fn test_key_name_pp_tp() {
        let spec = TensorParallelismSpec::new(vec![
            ParallelAxisSpec::new(ParallelAxisKind::PP, 0, 2),
            ParallelAxisSpec::new(ParallelAxisKind::TP, 3, 8).with_split_dim(1),
        ]);
        let key = get_parallelism_key_name("layer.0.weight", &spec);
        assert!(key.contains("__pp_0of2"));
        assert!(key.contains("__tp_3of8_sd1"));
    }

    #[test]
    fn test_calculate_shard_range_even() {
        assert_eq!(calculate_shard_range(1024, 0, 4), (0, 256));
        assert_eq!(calculate_shard_range(1024, 1, 4), (256, 256));
        assert_eq!(calculate_shard_range(1024, 2, 4), (512, 256));
        assert_eq!(calculate_shard_range(1024, 3, 4), (768, 256));
    }

    #[test]
    fn test_calculate_shard_range_uneven() {
        // 10 / 3 = 3 rem 1: sizes 4, 3, 3
        assert_eq!(calculate_shard_range(10, 0, 3), (0, 4));
        assert_eq!(calculate_shard_range(10, 1, 3), (4, 3));
        assert_eq!(calculate_shard_range(10, 2, 3), (7, 3));
    }

    #[test]
    fn test_calculate_shard_range_single() {
        assert_eq!(calculate_shard_range(100, 0, 1), (0, 100));
    }

    #[test]
    fn test_calculate_shard_range_zero_count() {
        assert_eq!(calculate_shard_range(100, 0, 0), (0, 0));
    }

    #[test]
    fn test_build_shard_metadata_roundtrip() {
        let global_shape = [8, 16];
        let local_shape = [4, 16];
        let axes = [ParallelAxisSpec::new(ParallelAxisKind::TP, 0, 2).with_split_dim(0)];
        let data_bytes = 4 * 16 * 4; // float32
        let meta = TensorMetadata::build_shard(
            TensorDtype::Float32 as i32,
            &global_shape,
            &local_shape,
            &axes,
            data_bytes as u64,
        );

        assert_eq!(meta.header.magic, TENSOR_OBJECT_MAGIC);
        assert_eq!(meta.header.layout_kind, TensorLayoutKind::Shard as u32);
        assert_eq!(meta.layout.axis_count, 1);
        assert_eq!(meta.layout.axes[0].kind, ParallelAxisKind::TP as i32);
        assert_eq!(meta.layout.axes[0].shard_rank, 0);
        assert_eq!(meta.layout.axes[0].shard_count, 2);
        assert_eq!(meta.layout.axes[0].split_dim, 0);
        assert_eq!(meta.layout.global_shape.to_vec(2), vec![8, 16]);
        assert_eq!(meta.layout.local_shape.to_vec(2), vec![4, 16]);

        // Roundtrip via bytes
        let total_len = TensorMetadata::WIRE_SIZE + data_bytes;
        let mut buf = vec![0u8; total_len];
        buf[..TensorMetadata::WIRE_SIZE].copy_from_slice(meta.as_bytes());
        let parsed = TensorMetadata::parse(&buf).unwrap();
        assert_eq!(parsed.metadata, meta);
    }

    #[test]
    fn test_writer_manifest_roundtrip() {
        let manifest =
            WriterShardManifest::new(TensorDtype::Bfloat16 as i32, 2, 0, 4, &[1024, 2048]);
        let bytes = manifest.as_bytes();
        let parsed = WriterShardManifest::parse(bytes).unwrap();
        assert_eq!(parsed, manifest);
        assert_eq!(parsed.shard_count, 4);
        assert_eq!(parsed.split_dim, 0);
        assert_eq!(parsed.global_shape.to_vec(2), vec![1024, 2048]);
    }

    #[test]
    fn test_writer_manifest_rejects_bad_magic() {
        let mut manifest = WriterShardManifest::new(0, 1, 0, 2, &[8]);
        manifest.magic = 0xDEAD;
        let bytes = manifest.as_bytes();
        assert!(WriterShardManifest::parse(bytes).is_none());
    }

    #[test]
    fn test_parallelism_matches_metadata() {
        let axes = vec![
            ParallelAxisSpec::new(ParallelAxisKind::DP, 0, 2),
            ParallelAxisSpec::new(ParallelAxisKind::TP, 1, 4).with_split_dim(0),
        ];
        let meta = TensorMetadata::build_shard(0, &[8, 16], &[4, 16], &axes, 256);

        let spec = TensorParallelismSpec::new(axes.clone());
        assert!(parallelism_matches_metadata(&spec, &meta));

        // Reversed order should still match after canonicalization
        let reversed_spec = TensorParallelismSpec::new(vec![axes[1], axes[0]]);
        assert!(parallelism_matches_metadata(&reversed_spec, &meta));

        // Different rank should not match
        let different_rank = TensorParallelismSpec::new(vec![
            ParallelAxisSpec::new(ParallelAxisKind::DP, 1, 2),
            ParallelAxisSpec::new(ParallelAxisKind::TP, 1, 4).with_split_dim(0),
        ]);
        assert!(!parallelism_matches_metadata(&different_rank, &meta));
    }

    #[test]
    fn test_axis_validate() {
        assert!(ParallelAxisSpec::new(ParallelAxisKind::TP, 0, 4)
            .validate()
            .is_ok());
        assert!(ParallelAxisSpec::new(ParallelAxisKind::TP, 3, 4)
            .validate()
            .is_ok());
        assert!(ParallelAxisSpec::new(ParallelAxisKind::TP, 4, 4)
            .validate()
            .is_err());
        assert!(ParallelAxisSpec::new(ParallelAxisKind::TP, -1, 4)
            .validate()
            .is_err());
        assert!(ParallelAxisSpec::new(ParallelAxisKind::TP, 0, 0)
            .validate()
            .is_err());
        assert!(ParallelAxisSpec::new(ParallelAxisKind::TP, 0, -1)
            .validate()
            .is_err());
    }

    #[test]
    fn test_writer_partition_validate() {
        assert!(WriterPartitionSpec::new(0, 4, 0).validate().is_ok());
        assert!(WriterPartitionSpec::new(3, 4, 0).validate().is_ok());
        assert!(WriterPartitionSpec::new(4, 4, 0).validate().is_err());
        assert!(WriterPartitionSpec::new(0, 0, 0).validate().is_err());
        assert!(WriterPartitionSpec::new(0, 4, -1).validate().is_err());
    }

    #[test]
    fn test_writer_partition_key_name() {
        assert_eq!(
            get_writer_partition_key_name("model.weight", 3, 4, 0),
            "model.weight__writer_3of4_sd0"
        );
        assert_eq!(
            get_writer_partition_key_name("model.weight", 1, 8, 1),
            "model.weight__writer_1of8_sd1"
        );
    }

    #[test]
    fn test_writer_manifest_key() {
        assert_eq!(
            WriterShardManifest::manifest_key("model.weight"),
            "model.weight__writer_manifest"
        );
    }

    #[test]
    fn test_is_single_tp() {
        let single =
            TensorParallelismSpec::new(vec![ParallelAxisSpec::new(ParallelAxisKind::TP, 0, 4)]);
        assert!(single.is_single_tp());

        let multi = TensorParallelismSpec::new(vec![
            ParallelAxisSpec::new(ParallelAxisKind::DP, 0, 2),
            ParallelAxisSpec::new(ParallelAxisKind::TP, 0, 4),
        ]);
        assert!(!multi.is_single_tp());

        let dp_only =
            TensorParallelismSpec::new(vec![ParallelAxisSpec::new(ParallelAxisKind::DP, 0, 2)]);
        assert!(!dp_only.is_single_tp());
    }

    #[test]
    fn test_layout_axis_roundtrip() {
        let spec = ParallelAxisSpec::new(ParallelAxisKind::EP, 1, 8)
            .with_split_dim(2)
            .with_expert_id(5)
            .with_stage_id(3);
        let wire = spec.to_layout_axis();
        let recovered = ParallelAxisSpec::from_layout_axis(&wire).unwrap();
        assert_eq!(recovered, spec);
    }

    #[test]
    fn test_writer_manifest_wire_size() {
        // magic(4) + version(2) + pad(2) + dtype(4) + ndim(4) + split_dim(4) + shard_count(4) + global_shape(64) = 88
        assert_eq!(WriterShardManifest::WIRE_SIZE, 88);
    }

    #[test]
    fn test_strided_ranges_split_dim0() {
        // split_dim=0: each transfer is a single contiguous range
        // shape [8, 4], elem=4, src: rank=0 of 2 (rows 0..3), tgt: rank=0 of 4 (rows 0..1)
        let ranges = calculate_strided_shard_ranges(&[8, 4], 0, 4, 0, 2, 0, 4).unwrap();
        // outer_count=1, inner_size=4*4=16, src has dim_size=4, tgt has dim_size=2
        // overlap: [0, 2), transfer_bytes=2*16=32
        assert_eq!(ranges.len(), 1);
        assert_eq!(ranges[0].src_offset, 0);
        assert_eq!(ranges[0].dst_offset, 0);
        assert_eq!(ranges[0].size, 32);
    }

    #[test]
    fn test_strided_ranges_split_dim1_basic() {
        // shape [4, 8], split_dim=1, elem=4 bytes
        // src: rank=0 of 2 → cols 0..3, tgt: rank=0 of 4 → cols 0..1
        let ranges = calculate_strided_shard_ranges(&[4, 8], 1, 4, 0, 2, 0, 4).unwrap();
        // outer_count=4 (shape[0]=4), inner_size=4 bytes (shape[2:]=empty, so 1*4)
        // src_dim_size=4, tgt_dim_size=2
        // overlap [0,2), overlap_count=2, local_src_start=0, local_tgt_start=0
        // transfer_bytes = 2*4 = 8
        // src_row_bytes = 4*4=16, tgt_row_bytes = 2*4=8
        assert_eq!(ranges.len(), 4);
        for (i, r) in ranges.iter().enumerate() {
            assert_eq!(r.src_offset, i * 16);
            assert_eq!(r.dst_offset, i * 8);
            assert_eq!(r.size, 8);
        }
    }

    #[test]
    fn test_strided_ranges_split_dim1_offset() {
        // shape [4, 8], split_dim=1, elem=4
        // src: rank=0 of 2 (cols 0..3), tgt: rank=1 of 4 (cols 2..3)
        let ranges = calculate_strided_shard_ranges(&[4, 8], 1, 4, 0, 2, 1, 4).unwrap();
        // tgt_dim_start=2, overlap [2,4), local_src_start=2, local_tgt_start=0
        assert_eq!(ranges.len(), 4);
        for (i, r) in ranges.iter().enumerate() {
            assert_eq!(r.src_offset, i * 16 + 2 * 4); // skip first 2 cols
            assert_eq!(r.dst_offset, i * 8);
            assert_eq!(r.size, 8);
        }
    }

    #[test]
    fn test_strided_ranges_no_overlap() {
        // shape [8, 4], split_dim=0
        // src: rank=0 of 2 (rows 0..3), tgt: rank=1 of 2 (rows 4..7)
        let ranges = calculate_strided_shard_ranges(&[8, 4], 0, 4, 0, 2, 1, 2).unwrap();
        assert!(ranges.is_empty());
    }

    #[test]
    fn test_strided_ranges_3d_split_dim1() {
        // shape [2, 4, 3], split_dim=1, elem=4
        // src: rank=0 of 2 (dim1: 0..1), tgt: rank=0 of 4 (dim1: 0..0, i.e. 1 element)
        let ranges = calculate_strided_shard_ranges(&[2, 4, 3], 1, 4, 0, 2, 0, 4).unwrap();
        // outer_count=2, inner_size=3*4=12
        // src_dim_size=2, tgt_dim_size=1
        // overlap [0,1), transfer_bytes=1*12=12
        // src_row_bytes=2*12=24, tgt_row_bytes=1*12=12
        assert_eq!(ranges.len(), 2);
        assert_eq!(
            ranges[0],
            ByteRangeTransfer {
                src_offset: 0,
                dst_offset: 0,
                size: 12
            }
        );
        assert_eq!(
            ranges[1],
            ByteRangeTransfer {
                src_offset: 24,
                dst_offset: 12,
                size: 12
            }
        );
    }

    #[test]
    fn test_validate_uniform_shard() {
        assert!(validate_uniform_shard(256, 4).is_ok());
        assert!(validate_uniform_shard(128, 1).is_ok());
        assert!(validate_uniform_shard(30, 4).is_err());
        assert!(validate_uniform_shard(0, 4).is_err());
        assert!(validate_uniform_shard(100, 0).is_err());
        assert!(validate_uniform_shard(100, -1).is_err());
    }

    #[test]
    fn test_build_raw_shard_write_plan_dim0() {
        // shape [8, 4], split_dim=0, rank=1, count=4, elem_size=4
        // shard_extent = 8/4 = 2, shard_start = 1*2 = 2
        // shard_shape = [2, 4], shard_bytes = 2*4*4 = 32
        // inner = 4, offset = 2 * 4 * 4 = 32
        let plan = build_raw_shard_write_plan(&[8, 4], 0, 1, 4, 4).unwrap();
        assert_eq!(plan.shard_shape, vec![2, 4]);
        assert_eq!(plan.shard_bytes, 32);
        assert_eq!(plan.data_ranges, vec![(32, 32)]);
    }

    #[test]
    fn test_build_raw_shard_write_plan_dim1() {
        // shape [4, 8], split_dim=1, rank=0, count=2, elem_size=4
        // shard_extent = 8/2 = 4, shard_start = 0
        // shard_shape = [4, 4], shard_bytes = 4*4*4 = 64
        // elements_before = 4, elements_after = 1
        // row_bytes = 4*1*4 = 16, full_row_bytes = 8*1*4 = 32
        // ranges: [(0, 16), (32, 16), (64, 16), (96, 16)]
        let plan = build_raw_shard_write_plan(&[4, 8], 1, 0, 2, 4).unwrap();
        assert_eq!(plan.shard_shape, vec![4, 4]);
        assert_eq!(plan.shard_bytes, 64);
        assert_eq!(plan.data_ranges.len(), 4);
        assert_eq!(plan.data_ranges[0], (0, 16));
        assert_eq!(plan.data_ranges[1], (32, 16));
        assert_eq!(plan.data_ranges[2], (64, 16));
        assert_eq!(plan.data_ranges[3], (96, 16));
    }

    #[test]
    fn test_build_raw_shard_write_plan_1d() {
        // 1D: shape [1024], split_dim=0, rank=2, count=4, elem_size=4
        // shard_extent = 256, shard_start = 512
        // shard_bytes = 256*4 = 1024
        // offset = 512 * 1(empty product, max(1)) * 4 = 2048
        let plan = build_raw_shard_write_plan(&[1024], 0, 2, 4, 4).unwrap();
        assert_eq!(plan.shard_shape, vec![256]);
        assert_eq!(plan.shard_bytes, 1024);
        assert_eq!(plan.data_ranges, vec![(2048, 1024)]);
    }

    #[test]
    fn test_build_raw_shard_write_plan_invalid() {
        assert!(build_raw_shard_write_plan(&[8, 4], 0, 0, 0, 4).is_none());
        assert!(build_raw_shard_write_plan(&[8, 4], 3, 0, 2, 4).is_none());
        assert!(build_raw_shard_write_plan(&[7, 4], 0, 0, 2, 4).is_none()); // 7 % 2 != 0
    }
}
