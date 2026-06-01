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
pub fn get_writer_partition_key_name(base_key: &str, rank: i32) -> String {
    format!("{base_key}__writer_{rank}")
}

/// Generate a parallelism manifest key.
pub fn get_parallelism_manifest_key(base_key: &str) -> String {
    format!("{base_key}__parallelism_manifest")
}

// ─── Shard range calculation ────────────────────────────────────────────────

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
            get_writer_partition_key_name("model.weight", 3),
            "model.weight__writer_3"
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
}
