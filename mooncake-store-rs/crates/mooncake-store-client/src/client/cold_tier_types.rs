// Cold tier configuration types.
// Included via `include!()` at module level in mod.rs.
//
// These types are part of the public StoreClientBuilder API surface.
// They configure device targets, watermarks, admission rate limits,
// offload scheduling policy, and eviction priority.

/// Physical storage medium for a cold tier device.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ColdTierKind {
    /// Local SSD (NVMe / SATA).  Supports UUID-based target resolution.
    Ssd,
    /// Network-attached filesystem.  Directory targets only (no UUID).
    Nfs,
}

/// SSD backend implementation strategy.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ColdTierSsdEngine {
    /// Binary-only flat-file backend.  One file per object, O(1) read/write.
    #[default]
    LocalDir,
    /// Experimental SSD engine with restart recovery for materialized extent locators.
    ExtentStore,
}

/// Resolved target specification for a cold tier device.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ColdTierTarget {
    /// Filesystem directory path (e.g., `/mnt/ssd0/mooncake`).
    Directory(std::path::PathBuf),
    /// Block device UUID; resolved to a mount point at startup.
    Uuid(String),
}

#[derive(Clone, Debug, Eq, PartialEq)]
/// Startup bootstrap config for a Mooncake cold tier device.
///
/// The `target` field is the storage-specific spec used to identify a disk device or resolve a
/// cold root. Supplying this config at startup is equivalent to the Admin HTTP
/// create/register/enable lifecycle for the initial device state.
pub struct ColdTierTargetConfig {
    pub cold_tier_id: String,
    pub kind: ColdTierKind,
    pub target: ColdTierTarget,
    pub ssd_engine: ColdTierSsdEngine,
    pub capacity_override_bytes: Option<u64>,
    pub tags: Vec<String>,
}

impl ColdTierTargetConfig {
    pub fn directory(
        cold_tier_id: impl Into<String>,
        kind: ColdTierKind,
        directory: impl Into<std::path::PathBuf>,
    ) -> Self {
        Self {
            cold_tier_id: cold_tier_id.into(),
            kind,
            target: ColdTierTarget::Directory(directory.into()),
            ssd_engine: ColdTierSsdEngine::LocalDir,
            capacity_override_bytes: None,
            tags: Vec::new(),
        }
    }

    pub fn uuid(
        cold_tier_id: impl Into<String>,
        kind: ColdTierKind,
        uuid: impl Into<String>,
    ) -> Self {
        Self {
            cold_tier_id: cold_tier_id.into(),
            kind,
            target: ColdTierTarget::Uuid(uuid.into()),
            ssd_engine: ColdTierSsdEngine::LocalDir,
            capacity_override_bytes: None,
            tags: Vec::new(),
        }
    }

    pub fn ssd_engine(mut self, ssd_engine: ColdTierSsdEngine) -> Self {
        self.ssd_engine = ssd_engine;
        self
    }

    /// Selects the experimental ExtentStore SSD engine.
    ///
    /// ExtentStore supports restart recovery for materialized extent locators, but remains
    /// experimental while operational coverage is expanded.
    pub fn extent_store_engine(self) -> Self {
        self.ssd_engine(ColdTierSsdEngine::ExtentStore)
    }

    pub fn capacity_override_bytes(mut self, capacity_override_bytes: u64) -> Self {
        self.capacity_override_bytes = Some(capacity_override_bytes.max(1));
        self
    }

    pub fn tags<I, S>(mut self, tags: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.tags = tags.into_iter().map(Into::into).collect();
        self
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ColdTierTargetSpec {
    pub cold_tier_id: String,
    pub kind: ColdTierKind,
    #[serde(default)]
    pub directory: Option<std::path::PathBuf>,
    #[serde(default)]
    pub uuid: Option<String>,
    #[serde(default)]
    pub ssd_engine: Option<ColdTierSsdEngine>,
    #[serde(default)]
    pub capacity_override_bytes: Option<u64>,
    #[serde(default)]
    pub tags: Vec<String>,
}

impl TryFrom<ColdTierTargetSpec> for ColdTierTargetConfig {
    type Error = StoreError;

    fn try_from(value: ColdTierTargetSpec) -> std::result::Result<Self, Self::Error> {
        let cold_tier_id = value.cold_tier_id.trim().to_string();
        if cold_tier_id.is_empty() {
            return Err(StoreError::InvalidState(
                "cold tier cold_tier_id must not be empty".to_string(),
            ));
        }
        let target = match (value.directory, value.uuid) {
            (Some(directory), None) => ColdTierTarget::Directory(directory),
            (None, Some(uuid)) if !uuid.trim().is_empty() => {
                ColdTierTarget::Uuid(uuid.trim().to_string())
            }
            (Some(_), Some(_)) => {
                return Err(StoreError::InvalidState(format!(
                    "cold tier {} must configure exactly one of directory or uuid",
                    value.cold_tier_id
                )))
            }
            _ => {
                return Err(StoreError::InvalidState(format!(
                    "cold tier {} must configure one of directory or uuid",
                    value.cold_tier_id
                )))
            }
        };
        if value.kind == ColdTierKind::Nfs && matches!(target, ColdTierTarget::Uuid(_)) {
            return Err(StoreError::InvalidState(format!(
                "cold tier {} does not support uuid targets for nfs backends",
                value.cold_tier_id
            )));
        }
        let ssd_engine = value.ssd_engine.unwrap_or_default();
        if value.kind != ColdTierKind::Ssd && ssd_engine != ColdTierSsdEngine::LocalDir {
            return Err(StoreError::InvalidState(format!(
                "cold tier {} can only configure ssd_engine for ssd backends",
                value.cold_tier_id
            )));
        }
        Ok(ColdTierTargetConfig {
            cold_tier_id,
            kind: value.kind,
            target,
            ssd_engine,
            capacity_override_bytes: value.capacity_override_bytes,
            tags: value.tags,
        })
    }
}

/// Controls when cold tier backing metadata is assigned to new objects.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ColdTierOffloadMode {
    /// Assign cold backing eagerly at write time.  The background offload
    /// pipeline materializes the payload to SSD asynchronously.
    #[default]
    Passthrough,
    /// Defer cold backing assignment until eviction pressure forces the
    /// hot replica out of DRAM.
    EvictTriggered,
}

/// Ordering strategy for the pending offload queue.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ColdTierPendingOffloadPolicy {
    /// First-in, first-out.
    #[default]
    Fifo,
    /// Smallest objects first (minimize per-object SSD write latency).
    SizeSmallFirst,
    /// Largest objects first (maximize bytes-per-IO throughput).
    SizeLargeFirst,
}

/// Priority strategy for selecting which hot replicas to evict.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ColdTierEvictionPriorityPolicy {
    /// Clock algorithm: approximate LRU with a second-chance bit.
    #[default]
    Clock,
    /// Evict the coldest (least recently accessed), largest objects first.
    ColdestLargestFirst,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ColdTierOffloadPriorityConfig {
    pub pending_policy: ColdTierPendingOffloadPolicy,
    pub eviction_policy: ColdTierEvictionPriorityPolicy,
    pub pending_aging_ms: u64,
    pub pending_scan_limit: usize,
    pub eviction_scan_limit: usize,
}

impl Default for ColdTierOffloadPriorityConfig {
    fn default() -> Self {
        Self {
            pending_policy: ColdTierPendingOffloadPolicy::Fifo,
            eviction_policy: ColdTierEvictionPriorityPolicy::Clock,
            pending_aging_ms: 30_000,
            pending_scan_limit: 64,
            eviction_scan_limit: 64,
        }
    }
}

impl ColdTierOffloadPriorityConfig {
    pub fn pending_policy(mut self, policy: ColdTierPendingOffloadPolicy) -> Self {
        self.pending_policy = policy;
        self
    }

    pub fn eviction_policy(mut self, policy: ColdTierEvictionPriorityPolicy) -> Self {
        self.eviction_policy = policy;
        self
    }

    pub fn pending_aging_ms(mut self, pending_aging_ms: u64) -> Self {
        self.pending_aging_ms = pending_aging_ms.max(1);
        self
    }

    pub fn pending_scan_limit(mut self, pending_scan_limit: usize) -> Self {
        self.pending_scan_limit = pending_scan_limit.max(1);
        self
    }

    pub fn eviction_scan_limit(mut self, eviction_scan_limit: usize) -> Self {
        self.eviction_scan_limit = eviction_scan_limit.max(1);
        self
    }
}

/// Per-device byte thresholds for cold tier capacity management.
///
/// - `high_bytes`: when used+reserved exceeds this, eviction/cleanup begins.
/// - `low_bytes`: eviction/cleanup stops when used+reserved drops below this.
/// - `critical_bytes`: hard ceiling; new offloads are rejected above this.
/// - `reserve_bytes`: headroom subtracted from available capacity during
///   admission checks (accounts for in-flight writes not yet accounted).
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ColdTierWatermarkConfig {
    pub high_bytes: Option<u64>,
    pub low_bytes: Option<u64>,
    pub critical_bytes: Option<u64>,
    pub reserve_bytes: u64,
}

impl ColdTierWatermarkConfig {
    pub fn high_bytes(mut self, high_bytes: u64) -> Self {
        self.high_bytes = Some(high_bytes.max(1));
        self
    }

    pub fn critical_bytes(mut self, critical_bytes: u64) -> Self {
        self.critical_bytes = Some(critical_bytes.max(1));
        self
    }

    pub fn low_bytes(mut self, low_bytes: u64) -> Self {
        self.low_bytes = Some(low_bytes.max(1));
        self
    }

    pub fn reserve_bytes(mut self, reserve_bytes: u64) -> Self {
        self.reserve_bytes = reserve_bytes;
        self
    }
}

/// Rate limiting and concurrency control for cold tier I/O operations.
///
/// Two-tier model: runtime-level limits (shared across all devices on this
/// runtime) and per-device limits.  Both offload (write) and restore (read)
/// have independent settings.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ColdTierRateLimitConfig {
    pub offload_runtime_max_in_flight: usize,
    pub offload_device_max_in_flight: usize,
    pub offload_runtime_ops_per_sec: u32,
    pub offload_device_ops_per_sec: u32,
    pub restore_runtime_max_in_flight: usize,
    pub restore_device_max_in_flight: usize,
    pub restore_runtime_ops_per_sec: u32,
    pub restore_device_ops_per_sec: u32,
    /// Maximum number of distinct object keys with concurrent in-flight restores.
    /// Limits fan-out when many cold reads arrive simultaneously.
    pub restore_max_distinct_flights: usize,
    /// Maximum queued restore-promotion requests before shedding new arrivals.
    pub restore_promotion_queue_limit: usize,
    /// Number of restore promotions to process per batch tick.
    pub restore_promotion_batch_limit: usize,
    pub restore_promotion_max_in_flight: usize,
    /// Maximum concurrent owner-side cold restore promote operations.
    /// Limits DRAM segment slot consumption to prevent exhaustion under heavy
    /// concurrent cold-read traffic.  0 = auto (20% of actual DRAM segments).
    pub restore_owner_promote_max_in_flight: usize,
    /// Number of pending offloads to kick on each foreground write that finds
    /// its cold backing still in PendingOffload state.
    pub foreground_offload_kick_batch: usize,
    /// Number of consecutive I/O errors before pausing a device.
    pub device_pause_after_errors: u32,
    /// Duration (ms) to pause a device after consecutive errors.
    pub device_pause_ms: u64,
    /// Number of probe I/Os allowed per device during a pause period.
    pub device_probe_batch: usize,
    /// Offload queue depth below which no pressure response applies.
    pub pressure_soft_threshold: usize,
    /// Offload queue depth at which maximum pressure response activates.
    pub pressure_hard_threshold: usize,
    /// Minimum restore device concurrency even under maximum offload pressure.
    pub pressure_restore_floor: usize,
    /// Boosted offload device concurrency when under pressure.
    pub pressure_offload_boost: usize,
    /// If no offload completes within this duration (ms), treat as stalled.
    pub pressure_stall_timeout_ms: u64,
    /// Total staging buffer pool size in bytes for cold restore SSD-to-RDMA
    /// passthrough.  Variable-length allocations are carved from this region.
    /// Default: 256 MiB.  Override with `MC_STORE_RS_STAGING_POOL_BYTES`.
    pub staging_pool_bytes: usize,
}

impl Default for ColdTierRateLimitConfig {
    fn default() -> Self {
        Self {
            offload_runtime_max_in_flight: 4,
            offload_device_max_in_flight: 2,
            offload_runtime_ops_per_sec: 512,
            offload_device_ops_per_sec: 512,
            restore_runtime_max_in_flight: 64,
            restore_device_max_in_flight: 32,
            restore_runtime_ops_per_sec: 1024,
            restore_device_ops_per_sec: 512,
            restore_max_distinct_flights: 256,
            restore_promotion_queue_limit: DEFAULT_RESTORE_PROMOTION_QUEUE_LIMIT,
            restore_promotion_batch_limit: 32,
            restore_promotion_max_in_flight: 32,
            restore_owner_promote_max_in_flight: 0,
            foreground_offload_kick_batch: 16,
            device_pause_after_errors: 3,
            device_pause_ms: 1_000,
            device_probe_batch: 1,
            pressure_soft_threshold: 2,
            pressure_hard_threshold: 8,
            pressure_restore_floor: 2,
            pressure_offload_boost: 4,
            pressure_stall_timeout_ms: 200,
            staging_pool_bytes: 256 * 1024 * 1024,
        }
    }
}

impl ColdTierRateLimitConfig {
    pub fn offload_runtime_max_in_flight(mut self, limit: usize) -> Self {
        self.offload_runtime_max_in_flight = limit.max(1);
        self
    }

    pub fn offload_device_max_in_flight(mut self, limit: usize) -> Self {
        self.offload_device_max_in_flight = limit.max(1);
        self
    }

    pub fn offload_runtime_ops_per_sec(mut self, limit: u32) -> Self {
        self.offload_runtime_ops_per_sec = limit.max(1);
        self
    }

    pub fn offload_device_ops_per_sec(mut self, limit: u32) -> Self {
        self.offload_device_ops_per_sec = limit.max(1);
        self
    }

    pub fn restore_max_distinct_flights(mut self, limit: usize) -> Self {
        self.restore_max_distinct_flights = limit.max(1);
        self
    }

    pub fn foreground_offload_kick_batch(mut self, limit: usize) -> Self {
        self.foreground_offload_kick_batch = limit.max(1);
        self
    }
}
