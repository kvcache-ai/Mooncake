#[derive(Clone, Copy, Debug)]
pub struct ObjectRef<'a> {
    pub tenant: Option<&'a str>,
    pub domain: Option<&'a str>,
    pub object_set: Option<&'a str>,
    pub qos_tier: Option<&'a str>,
    pub key: &'a str,
}

impl<'a> ObjectRef<'a> {
    pub fn new(key: &'a str) -> Self {
        Self {
            tenant: None,
            domain: None,
            object_set: None,
            qos_tier: None,
            key,
        }
    }

    pub fn tenant(mut self, tenant: &'a str) -> Self {
        self.tenant = Some(tenant);
        self
    }

    pub fn domain(mut self, domain: &'a str) -> Self {
        self.domain = Some(domain);
        self
    }

    pub fn object_set(mut self, object_set: &'a str) -> Self {
        self.object_set = Some(object_set);
        self
    }

    pub fn qos_tier(mut self, qos_tier: &'a str) -> Self {
        self.qos_tier = Some(qos_tier);
        self
    }
}

#[derive(Clone, Debug, Default)]
pub struct ReadQueryResultCache {
    entries: HashMap<ObjectKey, Option<ObjectRoute>>,
}

impl ReadQueryResultCache {
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Merge another request-scoped cache into this one.
    ///
    /// Duplicate object keys use last-writer-wins semantics, matching
    /// `HashMap::extend`. This cache is only used within one high-level read
    /// request, so later entries are treated as the fresher observation.
    pub fn merge(&mut self, other: Self) {
        self.entries.extend(other.entries);
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct NamespaceQuota {
    pub max_bytes: Option<u64>,
    pub max_objects: Option<usize>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ExecutionFairness {
    pub max_remote_batch_items_per_tenant: Option<usize>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct BandwidthShaping {
    pub max_remote_batch_bytes: Option<usize>,
    pub max_remote_batch_burst_items: Option<usize>,
    pub max_inflight_bytes_per_batch: Option<u64>,
}

impl BandwidthShaping {
    pub fn new() -> Self {
        Self {
            max_remote_batch_bytes: None,
            max_remote_batch_burst_items: None,
            max_inflight_bytes_per_batch: None,
        }
    }

    pub fn max_remote_batch_bytes(mut self, max_remote_batch_bytes: usize) -> Self {
        self.max_remote_batch_bytes = Some(max_remote_batch_bytes.max(1));
        self
    }

    pub fn max_remote_batch_burst_items(mut self, max_remote_batch_burst_items: usize) -> Self {
        self.max_remote_batch_burst_items = Some(max_remote_batch_burst_items.max(1));
        self
    }

    pub fn max_inflight_bytes_per_batch(mut self, max_inflight_bytes_per_batch: u64) -> Self {
        self.max_inflight_bytes_per_batch = Some(max_inflight_bytes_per_batch.max(1));
        self
    }
}

impl ExecutionFairness {
    pub fn new() -> Self {
        Self {
            max_remote_batch_items_per_tenant: None,
        }
    }

    pub fn max_remote_batch_items_per_tenant(
        mut self,
        max_remote_batch_items_per_tenant: usize,
    ) -> Self {
        self.max_remote_batch_items_per_tenant = Some(max_remote_batch_items_per_tenant.max(1));
        self
    }
}

impl NamespaceQuota {
    pub fn new() -> Self {
        Self {
            max_bytes: None,
            max_objects: None,
        }
    }

    pub fn max_bytes(mut self, max_bytes: u64) -> Self {
        self.max_bytes = Some(max_bytes.max(1));
        self
    }

    pub fn max_objects(mut self, max_objects: usize) -> Self {
        self.max_objects = Some(max_objects.max(1));
        self
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ColdTierKind {
    Ssd,
    Nfs,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ColdTierSsdEngine {
    #[default]
    LocalDir,
    /// Experimental SSD engine with restart recovery for materialized extent locators.
    ExtentStore,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ColdTierTarget {
    Directory(std::path::PathBuf),
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

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ColdTierOffloadMode {
    #[default]
    Passthrough,
    EvictTriggered,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ColdTierPendingOffloadPolicy {
    #[default]
    Fifo,
    SizeSmallFirst,
    SizeLargeFirst,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ColdTierEvictionPriorityPolicy {
    #[default]
    Clock,
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

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ColdTierWatermarkConfig {
    pub high_bytes: Option<u64>,
    pub low_bytes: Option<u64>,
    pub critical_bytes: Option<u64>,
    pub reserve_bytes: u64,
}

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
    pub restore_max_distinct_flights: usize,
    pub restore_promotion_queue_limit: usize,
    pub restore_promotion_batch_limit: usize,
    pub restore_promotion_max_in_flight: usize,
    /// Maximum concurrent owner-side cold restore promote operations.
    /// Limits DRAM segment slot consumption to prevent exhaustion under heavy
    /// concurrent cold-read traffic.  0 = auto (20% of actual DRAM segments).
    pub restore_owner_promote_max_in_flight: usize,
    pub foreground_offload_kick_batch: usize,
    pub device_pause_after_errors: u32,
    pub device_pause_ms: u64,
    pub device_probe_batch: usize,
    /// Offload queue depth below which no restore throttle applies.
    pub pressure_soft_threshold: usize,
    /// Offload queue depth at which restore concurrency is throttled to floor.
    pub pressure_hard_threshold: usize,
    /// Minimum restore device concurrency even under maximum offload pressure.
    pub pressure_restore_floor: usize,
    /// Boosted offload device concurrency when under pressure.
    pub pressure_offload_boost: usize,
    /// If no offload completes within this duration (ms), treat as stalled.
    pub pressure_stall_timeout_ms: u64,
    /// Total staging buffer pool size in bytes for cold restore SSD→RDMA
    /// passthrough.  Variable-length allocations are carved from this region.
    /// Default: 256 MiB.  Override with `MC_STORE_RS_STAGING_POOL_BYTES`.
    pub staging_pool_bytes: usize,
}

const DEFAULT_RESTORE_PROMOTION_QUEUE_LIMIT: usize = 1024;

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

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplicationPolicy {
    pub replica_count: Option<usize>,
    pub with_soft_pin: bool,
    pub preferred_segments: Vec<SegmentName>,
    pub preferred_storage_owners: Vec<String>,
    pub prefer_alloc_in_same_node: bool,
    pub prefer_local: bool,
}

impl Default for ReplicationPolicy {
    fn default() -> Self {
        Self {
            replica_count: None,
            with_soft_pin: false,
            preferred_segments: Vec::new(),
            preferred_storage_owners: Vec::new(),
            prefer_alloc_in_same_node: false,
            prefer_local: true,
        }
    }
}

impl ReplicationPolicy {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn replica_count(mut self, replica_count: usize) -> Self {
        self.replica_count = Some(replica_count);
        self
    }

    pub fn with_soft_pin(mut self, with_soft_pin: bool) -> Self {
        self.with_soft_pin = with_soft_pin;
        self
    }

    pub fn prefer_alloc_in_same_node(mut self, prefer_alloc_in_same_node: bool) -> Self {
        self.prefer_alloc_in_same_node = prefer_alloc_in_same_node;
        self
    }

    pub fn prefer_local(mut self, prefer_local: bool) -> Self {
        self.prefer_local = prefer_local;
        self
    }

    pub fn preferred_segment(mut self, segment: impl Into<String>) -> Self {
        self.preferred_segments.push(SegmentName::new(segment));
        self
    }

    pub fn preferred_segments<I, S>(mut self, segments: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.preferred_segments = segments
            .into_iter()
            .map(SegmentName::new)
            .collect::<Vec<_>>();
        self
    }

    pub fn preferred_storage_owner(mut self, owner: impl Into<String>) -> Self {
        self.preferred_storage_owners.push(owner.into());
        self
    }

    pub fn preferred_storage_owners<I, S>(mut self, owners: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.preferred_storage_owners = owners.into_iter().map(Into::into).collect::<Vec<_>>();
        self
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum ExplicitMigrationMode {
    Copy,
    Move,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ReplicaReadSelector {
    Segment(SegmentName),
    #[allow(dead_code)]
    ColdBacking,
    // 预留给显式 owner+segment 选择；当前生产路径主要由 segment-only 调用。
    #[allow(dead_code)]
    OwnerAndSegment {
        owner: ClientRuntimeId,
        segment_name: SegmentName,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExplicitMigrationPlan {
    pub mode: ExplicitMigrationMode,
    pub source: ReplicaReadSelector,
    pub target_segments: Vec<SegmentName>,
    pub all_or_nothing: bool,
}

#[derive(Clone, Debug)]
pub struct PutRequest<'a> {
    pub tenant: Option<&'a str>,
    pub domain: Option<&'a str>,
    pub object_set: Option<&'a str>,
    pub qos_tier: Option<&'a str>,
    pub key: &'a str,
    pub value: &'a [u8],
    pub policy: Option<ReplicationPolicy>,
}

impl<'a> PutRequest<'a> {
    pub fn new(key: &'a str, value: &'a [u8]) -> Self {
        Self {
            tenant: None,
            domain: None,
            object_set: None,
            qos_tier: None,
            key,
            value,
            policy: None,
        }
    }

    pub fn tenant(mut self, tenant: &'a str) -> Self {
        self.tenant = Some(tenant);
        self
    }

    pub fn domain(mut self, domain: &'a str) -> Self {
        self.domain = Some(domain);
        self
    }

    pub fn object_set(mut self, object_set: &'a str) -> Self {
        self.object_set = Some(object_set);
        self
    }

    pub fn qos_tier(mut self, qos_tier: &'a str) -> Self {
        self.qos_tier = Some(qos_tier);
        self
    }

    pub fn replication(mut self, policy: ReplicationPolicy) -> Self {
        self.policy = Some(policy);
        self
    }
}

#[derive(Clone, Debug)]
pub struct PutFromRequest<'a> {
    pub tenant: Option<&'a str>,
    pub domain: Option<&'a str>,
    pub object_set: Option<&'a str>,
    pub qos_tier: Option<&'a str>,
    pub key: &'a str,
    pub buffer: *const c_void,
    pub size: usize,
    pub policy: Option<ReplicationPolicy>,
}

impl<'a> PutFromRequest<'a> {
    pub fn new(key: &'a str, buffer: *const c_void, size: usize) -> Self {
        Self {
            tenant: None,
            domain: None,
            object_set: None,
            qos_tier: None,
            key,
            buffer,
            size,
            policy: None,
        }
    }

    pub fn tenant(mut self, tenant: &'a str) -> Self {
        self.tenant = Some(tenant);
        self
    }

    pub fn domain(mut self, domain: &'a str) -> Self {
        self.domain = Some(domain);
        self
    }

    pub fn object_set(mut self, object_set: &'a str) -> Self {
        self.object_set = Some(object_set);
        self
    }

    pub fn qos_tier(mut self, qos_tier: &'a str) -> Self {
        self.qos_tier = Some(qos_tier);
        self
    }

    pub fn replication(mut self, policy: ReplicationPolicy) -> Self {
        self.policy = Some(policy);
        self
    }
}

#[derive(Debug)]
pub struct GetRequest<'a> {
    pub tenant: Option<&'a str>,
    pub domain: Option<&'a str>,
    pub object_set: Option<&'a str>,
    pub qos_tier: Option<&'a str>,
    pub key: &'a str,
    pub buffer: &'a mut [u8],
}

pub struct MultiBufferPutRequest<'a> {
    pub tenant: Option<&'a str>,
    pub domain: Option<&'a str>,
    pub object_set: Option<&'a str>,
    pub qos_tier: Option<&'a str>,
    pub key: &'a str,
    pub buffers: &'a [&'a [u8]],
    pub policy: Option<ReplicationPolicy>,
}

impl<'a> MultiBufferPutRequest<'a> {
    pub fn new(key: &'a str, buffers: &'a [&'a [u8]]) -> Self {
        Self {
            tenant: None,
            domain: None,
            object_set: None,
            qos_tier: None,
            key,
            buffers,
            policy: None,
        }
    }

    pub fn tenant(mut self, tenant: &'a str) -> Self {
        self.tenant = Some(tenant);
        self
    }

    pub fn domain(mut self, domain: &'a str) -> Self {
        self.domain = Some(domain);
        self
    }

    pub fn object_set(mut self, object_set: &'a str) -> Self {
        self.object_set = Some(object_set);
        self
    }

    pub fn qos_tier(mut self, qos_tier: &'a str) -> Self {
        self.qos_tier = Some(qos_tier);
        self
    }

    pub fn replication(mut self, policy: ReplicationPolicy) -> Self {
        self.policy = Some(policy);
        self
    }
}

pub struct MultiBufferGetRequest<'a> {
    pub tenant: Option<&'a str>,
    pub domain: Option<&'a str>,
    pub object_set: Option<&'a str>,
    pub qos_tier: Option<&'a str>,
    pub key: &'a str,
    pub buffers: &'a mut [&'a mut [u8]],
}

impl<'a> MultiBufferGetRequest<'a> {
    pub fn new(key: &'a str, buffers: &'a mut [&'a mut [u8]]) -> Self {
        Self {
            tenant: None,
            domain: None,
            object_set: None,
            qos_tier: None,
            key,
            buffers,
        }
    }

    pub fn tenant(mut self, tenant: &'a str) -> Self {
        self.tenant = Some(tenant);
        self
    }

    pub fn domain(mut self, domain: &'a str) -> Self {
        self.domain = Some(domain);
        self
    }

    pub fn object_set(mut self, object_set: &'a str) -> Self {
        self.object_set = Some(object_set);
        self
    }

    pub fn qos_tier(mut self, qos_tier: &'a str) -> Self {
        self.qos_tier = Some(qos_tier);
        self
    }
}

impl<'a> GetRequest<'a> {
    pub fn new(key: &'a str, buffer: &'a mut [u8]) -> Self {
        Self {
            tenant: None,
            domain: None,
            object_set: None,
            qos_tier: None,
            key,
            buffer,
        }
    }

    pub fn tenant(mut self, tenant: &'a str) -> Self {
        self.tenant = Some(tenant);
        self
    }

    pub fn domain(mut self, domain: &'a str) -> Self {
        self.domain = Some(domain);
        self
    }

    pub fn object_set(mut self, object_set: &'a str) -> Self {
        self.object_set = Some(object_set);
        self
    }

    pub fn qos_tier(mut self, qos_tier: &'a str) -> Self {
        self.qos_tier = Some(qos_tier);
        self
    }
}
