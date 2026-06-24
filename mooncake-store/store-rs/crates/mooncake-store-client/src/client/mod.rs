use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::ffi::c_void;
use std::ops::Deref;
use std::ptr;
use std::slice;
use std::sync::{
    atomic::{AtomicBool, AtomicU64, AtomicU8, AtomicUsize, Ordering},
    Arc, Condvar as StdCondvar, Mutex as StdMutex, OnceLock,
};
use std::thread::sleep;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use mooncake_store_core::{
    CasResult, ClientEndpointSet, ClientEpoch, ClientLease, ClientLifecycleState, ClientRuntimeId,
    ClientStableId, ColdTierDeviceRecord, ColdTierDeviceState, ColdTierDeviceUpdate,
    CompatibilityDescriptor, HandoffKind, HandoffPlan, LogicalObjectId, MetadataBackend,
    NamespaceScope, ObjectKey, ObjectRoute, ReplicaRoute, ReplicaTier, Result, RouteCasRequest,
    RouteControlMode, RouteDirectory, RoutePolicy, RoutePolicyDomain, RouteState, RouteVersion,
    SegmentAnnouncement, SegmentLifecycleState, SegmentName, SegmentTargetChunk, StoreError,
    TenantObjectAccountingState, TenantPlacementPolicy, TenantPolicyScope, TenantPolicySpec,
    TenantQuotaFinalizeRequest, TenantQuotaPolicy, TenantQuotaReservationRequest,
};
use mooncake_store_route::{RouteHitReporter, RouteOperations};
use mooncake_transport::{
    Opcode, SegmentBuffer, SegmentInfo, TentEngine, TransferBatchHints, TransferPacingMode,
    TransferRequest,
};
use parking_lot::Mutex;
use tracing::{debug, info, info_span, trace, warn};

use crate::control_plane::pb;
use crate::control_plane::{
    control_address_label, AllocatorService, ControlPlaneClient, ControlPlaneHandle,
    EvictionService, MigrationExecutionStatus, MigrationService, ReleaseOp, ReserveSpecificOp,
    RouteTrafficReport, UnsupportedColdTierControlService,
};
use crate::memory::{
    execute_copy_instructions, LocalMemoryConfig, LocalMemoryState, PreparedCopy, RegionAllocation,
    ScratchReservation, StorageExtentInfo, StorageSegmentSpec,
};
use crate::observability::{
    record_api_items, registry, ApiItemTrace, ApiItemsTraceRecord, OperationTracker,
};
use crate::placement::PlacementPlanner;
use crate::route_directory::{
    authority_compare_and_swap, authority_compare_and_swap_many, authority_contains_many,
    authority_get, authority_get_many, authority_list_routes_by_replica_owner, authority_replace,
    authority_replace_many, build_route_directory,
};
use crate::transport::{
    registration_chunks, wait_for_batch_completion_detailed, StoreTransport, StoreTransportFactory,
    TentStoreTransport,
};

const DEFAULT_TENANT: &str = "default";
const DEFAULT_TRANSFER_STALL_TIMEOUT: Duration = Duration::from_secs(10);
const DEFAULT_LIVE_CLIENT_SYNC_INTERVAL: Duration = Duration::from_secs(1);
const DEFAULT_SUSPECT_RUNTIME_TTL: Duration = Duration::from_secs(5);
const DEFAULT_ROUTE_TOPK: usize = 2;
const DEFAULT_REQUEST_TIMEOUT_BASE: Duration = Duration::from_secs(1);
const DEFAULT_REQUEST_TIMEOUT_CAP: Duration = Duration::from_secs(60);
const DEFAULT_REQUEST_FAILOVER_SLACK: Duration = Duration::from_millis(250);
const DEFAULT_REQUEST_THROUGHPUT_FLOOR_BYTES_PER_SEC: u64 = 32 * 1024 * 1024;
const DEFAULT_ROUTE_REFRESH_RETRY_DELAY: Duration = Duration::from_millis(25);
const DEFAULT_REMOTE_COLD_RESTORE_RECHECK_DELAY: Duration = Duration::from_millis(10);
const DEFAULT_PUT_WRITE_RETRY_LIMIT: usize = 4;
const DEFAULT_SEGMENT_PUBLISH_LEASE_TTL_MS: u64 = 30_000;
const DEFAULT_TENANT_QUOTA_RESERVATION_TTL_MS: u64 = 60_000;
const DEFAULT_TENANT_POLICY_CACHE_TTL_MS: u64 = 1_000;
const DEFAULT_TENANT_POLICY_CACHE_IDLE_TTL_MS: u64 = 30_000;
const DEBUG_PER_KEY_SAMPLE_MODULUS: u64 = 128;
const STABLE_PHASE_HASH_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
const STABLE_PHASE_HASH_PRIME: u64 = 0x0000_0001_0000_01b3;
#[allow(dead_code)]
const DEFAULT_OFFLOAD_MATERIALIZE_BATCH: usize = 32;
#[allow(dead_code)]
const DEFAULT_PENDING_OFFLOAD_RETRY_BASE_MS: u64 = 100;
#[allow(dead_code)]
const DEFAULT_PENDING_OFFLOAD_RETRY_MAX_MS: u64 = 5_000;
#[allow(dead_code)]
const DEFAULT_COLD_TIER_CLEANUP_DEVICE_BATCH: usize = 1;
#[allow(dead_code)]
const DEFAULT_COLD_TIER_CLEANUP_VICTIM_BATCH: usize = 32;
#[allow(dead_code)]
const DEFAULT_PENDING_DELETE_GC_BATCH: usize = 64;
#[allow(dead_code)]
const DEFAULT_OFFLOAD_POLL_INTERVAL: Duration = Duration::from_millis(500);
const TRANSFER_STALL_TIMEOUT_ENV: &str = "MC_STORE_RS_TRANSFER_STALL_TIMEOUT_MS";
const LEGACY_TRANSFER_TIMEOUT_ENV: &str = "MC_STORE_RS_TRANSFER_TIMEOUT_MS";
const REQUEST_TIMEOUT_ENV: &str = "MC_STORE_RS_REQUEST_TIMEOUT_MS";

type SharedLifecycleState = Arc<AtomicU8>;
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum BatchPutRouteConflictPolicy {
    Strict,
    AcceptExistingActiveRoute,
}

#[derive(Clone, Debug)]
struct TenantQuotaPolicyCacheEntry {
    version: Option<u64>,
    quota: Option<TenantQuotaPolicy>,
    refreshed_at_ms: u64,
    last_accessed_ms: u64,
}

include!("types.rs");
include!("cold_tier_types.rs");
include!("builder.rs");
include!("state_core.rs");
include!("membership_sync.rs");
include!("state_adapters.rs");
include!("state_store.rs");
include!("helpers.rs");

/// Cold tier persistent storage backend abstraction.
/// Contains PersistentStorageBackend trait, LocalDir backend, and ColdTierBackendResolver.
#[allow(dead_code)]
#[path = "cold_tier_storage_backend.rs"]
mod cold_tier_storage_backend;
use cold_tier_storage_backend::*;

/// Cold tier device management, admission control, offload pipeline, and cleanup.
#[allow(dead_code)]
mod cold_tier;
use self::cold_tier::{
    payload_checksum, ClockEntryId, ColdTierHandle, EvictionReadySignal, HotReplicaTracker,
    StorageClockState,
};

type SharedRouteWriteGate = Arc<RouteWriteGate>;

pub struct StoreClient {
    metadata: Arc<dyn MetadataBackend>,
    route_directory: Arc<dyn RouteDirectory>,
    _control_plane: ControlPlaneHandle,
    control_client: Arc<ControlPlaneClient>,
    allocator: Arc<Mutex<LocalAllocatorState>>,
    storage_owner: Arc<StorageOwnerState>,
    lease: ClientLease,
    lease_ttl_ms: u64,
    live_client_cache: SharedLiveClientCache,
    suspect_runtime_cache: SharedSuspectRuntimeCache,
    membership_sync: MembershipSyncHandle,
    _async_eviction: AsyncEvictionHandle,
    async_replica_tracking: AsyncReplicaTrackHandle,
    async_route_hit_reporting: AsyncRouteHitReportHandle,
    default_tenant: String,
    local_memory: LocalMemoryConfig,
    transport: Option<Arc<dyn StoreTransport>>,
    transport_factory: Option<Arc<dyn StoreTransportFactory>>,
    write_mode: WriteMode,
    route_control: RouteControlMode,
    route_topk: usize,
    transfer_stall_timeout: Duration,
    request_timeout_override: Option<Duration>,
    lifecycle_state: SharedLifecycleState,
    route_write_gate: SharedRouteWriteGate,
    startup_activation_pending: AtomicBool,
    heartbeat_repair_pending: AtomicUsize,
    tenant_quota_reservation_counter: Arc<AtomicU64>,
    namespace_quota: Option<NamespaceQuota>,
    execution_fairness: Option<ExecutionFairness>,
    bandwidth_shaping: Option<BandwidthShaping>,
    placement_policy: Option<TenantPlacementPolicy>,
    state: Arc<Mutex<StoreState>>,
    /// True if this client owns the cold tier device lifecycle (bootstrap,
    /// heartbeat, shutdown cleanup).  False for cloned/shared clients.
    #[allow(dead_code)] // used in Drop impl for shutdown cleanup
    owns_cold_tier_lifecycle: bool,
    /// Controls cleanup behavior during client shutdown (Restart vs Decommission).
    #[allow(dead_code)] // used in Drop impl for shutdown cleanup
    cold_tier_shutdown_mode: ColdTierShutdownMode,
    restore_promotions: SharedRestorePromotionQueue,
    cold_tier: cold_tier::ColdTierHandle,
    cold_restore_flights: SharedColdRestoreSingleflight,
    #[allow(dead_code)]
    deferred_cold_tier_reconciles: Mutex<Vec<DeferredColdTierReconcile>>,
}

enum HealthUpdateKind {
    Heartbeat,
    StateTransition { operation: &'static str },
}

pub struct HealthChannel {
    metadata: Arc<dyn MetadataBackend>,
    lease: Mutex<ClientLease>,
    lease_ttl_ms: u64,
}

pub struct HealthUpdate {
    metadata: Arc<dyn MetadataBackend>,
    lease: ClientLease,
    kind: HealthUpdateKind,
}

pub type HeartbeatLease = HealthUpdate;

fn encode_lifecycle_state(state: ClientLifecycleState) -> u8 {
    match state {
        ClientLifecycleState::Standby => 0,
        ClientLifecycleState::Active => 1,
        ClientLifecycleState::Draining => 2,
        ClientLifecycleState::Sealed => 3,
        ClientLifecycleState::Offline => 4,
    }
}

fn decode_lifecycle_state(encoded: u8) -> ClientLifecycleState {
    match encoded {
        0 => ClientLifecycleState::Standby,
        1 => ClientLifecycleState::Active,
        2 => ClientLifecycleState::Draining,
        3 => ClientLifecycleState::Sealed,
        4 => ClientLifecycleState::Offline,
        _ => ClientLifecycleState::Offline,
    }
}

#[derive(Copy, Clone, Debug)]
struct RequestDeadline {
    deadline: Instant,
}

impl RequestDeadline {
    fn after(timeout: Duration) -> Self {
        Self {
            deadline: Instant::now() + timeout,
        }
    }

    fn instant(self) -> Instant {
        self.deadline
    }

    fn has_expired(self) -> bool {
        Instant::now() >= self.deadline
    }

    fn remaining(self) -> Duration {
        self.deadline.saturating_duration_since(Instant::now())
    }
}

pub(crate) fn duration_from_env_ms(keys: &[&str]) -> Option<Duration> {
    keys.iter()
        .find_map(|key| std::env::var(key).ok())
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|millis| *millis > 0)
        .map(Duration::from_millis)
}

fn transfer_stall_timeout_from_env() -> Duration {
    duration_from_env_ms(&[TRANSFER_STALL_TIMEOUT_ENV, LEGACY_TRANSFER_TIMEOUT_ENV])
        .unwrap_or(DEFAULT_TRANSFER_STALL_TIMEOUT)
}

fn request_timeout_override_from_env() -> Option<Duration> {
    duration_from_env_ms(&[REQUEST_TIMEOUT_ENV])
}

fn estimate_request_timeout(
    bytes: u64,
    attempts: usize,
    transfer_stall_timeout: Duration,
) -> Duration {
    let attempts = attempts.max(1) as u32;
    let transfer_ms = if bytes == 0 {
        0
    } else {
        let bytes_per_sec = u128::from(DEFAULT_REQUEST_THROUGHPUT_FLOOR_BYTES_PER_SEC.max(1));
        let millis = (u128::from(bytes) * 1000).div_ceil(bytes_per_sec);
        millis.min(u128::from(u64::MAX)) as u64
    };
    let failover_slack = DEFAULT_REQUEST_FAILOVER_SLACK.saturating_mul(attempts.saturating_sub(1));
    let timeout = transfer_stall_timeout
        .saturating_mul(attempts)
        .saturating_add(Duration::from_millis(transfer_ms))
        .saturating_add(DEFAULT_REQUEST_TIMEOUT_BASE)
        .saturating_add(failover_slack);
    timeout.clamp(
        transfer_stall_timeout.saturating_add(DEFAULT_REQUEST_TIMEOUT_BASE),
        DEFAULT_REQUEST_TIMEOUT_CAP,
    )
}

fn pending_publish_grace_timeout(
    bytes: u64,
    transfer_stall_timeout: Duration,
    request_timeout_override: Option<Duration>,
) -> Duration {
    request_timeout_override
        .unwrap_or_else(|| estimate_request_timeout(bytes, 1, transfer_stall_timeout))
        .saturating_add(DEFAULT_REQUEST_FAILOVER_SLACK)
}

fn pending_publish_deadline_ms(
    bytes: u64,
    transfer_stall_timeout: Duration,
    request_timeout_override: Option<Duration>,
) -> u64 {
    let grace_ms =
        pending_publish_grace_timeout(bytes, transfer_stall_timeout, request_timeout_override)
            .as_millis()
            .min(u128::from(u64::MAX)) as u64;
    now_ms().saturating_add(grace_ms)
}

impl StoreClient {
    fn request_deadline_for_transfer(&self, bytes: u64, attempts: usize) -> RequestDeadline {
        let timeout = self.request_timeout_override.unwrap_or_else(|| {
            estimate_request_timeout(bytes, attempts, self.transfer_stall_timeout)
        });
        RequestDeadline::after(timeout)
    }
}

impl HealthChannel {
    pub fn new(metadata: Arc<dyn MetadataBackend>, lease: ClientLease, lease_ttl_ms: u64) -> Self {
        Self {
            metadata,
            lease: Mutex::new(lease),
            lease_ttl_ms: lease_ttl_ms.max(1),
        }
    }

    pub fn prepare_heartbeat(&self, expires_at_ms: u64) -> HeartbeatLease {
        let mut lease = self.lease.lock();
        lease.expires_at_ms = expires_at_ms;
        HealthUpdate::heartbeat(self.metadata.clone(), lease.clone())
    }

    pub fn prepare_state_update(
        &self,
        next_state: ClientLifecycleState,
        operation: &'static str,
    ) -> HealthUpdate {
        let mut lease = self.lease.lock();
        lease.state = next_state;
        lease.expires_at_ms = lease
            .expires_at_ms
            .max(now_ms().saturating_add(self.lease_ttl_ms));
        HealthUpdate::state_transition(self.metadata.clone(), lease.clone(), operation)
    }

    pub fn snapshot_lease(&self) -> ClientLease {
        self.lease.lock().clone()
    }

    pub fn sync_state(&self, next_state: ClientLifecycleState) {
        self.lease.lock().state = next_state;
    }
}

impl HealthUpdate {
    fn heartbeat(metadata: Arc<dyn MetadataBackend>, lease: ClientLease) -> Self {
        Self {
            metadata,
            lease,
            kind: HealthUpdateKind::Heartbeat,
        }
    }

    fn state_transition(
        metadata: Arc<dyn MetadataBackend>,
        lease: ClientLease,
        operation: &'static str,
    ) -> Self {
        Self {
            metadata,
            lease,
            kind: HealthUpdateKind::StateTransition { operation },
        }
    }

    pub fn publish(self) -> Result<()> {
        let Self {
            metadata,
            lease,
            kind,
        } = self;
        match kind {
            HealthUpdateKind::Heartbeat => {
                let _span = info_span!(
                    "store.heartbeat",
                    runtime = %lease.runtime,
                    expires_at_ms = lease.expires_at_ms
                )
                .entered();
                let tracker = OperationTracker::new("heartbeat");
                let result = metadata.upsert_client_lease(&lease);
                tracker.finish(&result, 0);
                result
            }
            HealthUpdateKind::StateTransition { operation } => {
                let _span = info_span!(
                    "store.health_state_update",
                    runtime = %lease.runtime,
                    operation,
                    state = ?lease.state
                )
                .entered();
                let tracker = OperationTracker::new(operation);
                let result = metadata.upsert_client_lease(&lease);
                tracker.finish(&result, 0);
                result
            }
        }
    }
}

include!("runtime_core.rs");
include!("runtime_alloc.rs");
include!("runtime_io.rs");
include!("runtime_write.rs");
include!("facade.rs");

pub fn stable_phase_spread_ms(identity: &str, interval_ms: u64, salt: &str) -> u64 {
    if interval_ms == 0 {
        return 0;
    }
    stable_phase_hash64(identity, salt) % interval_ms + 1
}

fn stable_phase_hash64(identity: &str, salt: &str) -> u64 {
    let mut hash = STABLE_PHASE_HASH_OFFSET;
    for byte in salt.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(STABLE_PHASE_HASH_PRIME);
    }
    hash ^= 0xff;
    hash = hash.wrapping_mul(STABLE_PHASE_HASH_PRIME);
    for byte in identity.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(STABLE_PHASE_HASH_PRIME);
    }
    hash
}

fn stable_debug_log_sample(parts: &[&str]) -> bool {
    is_debug_sample(stable_joined_hash64(parts), DEBUG_PER_KEY_SAMPLE_MODULUS)
}

fn is_debug_sample(hash: u64, modulus: u64) -> bool {
    matches!(hash.checked_rem(modulus), Some(0))
}

fn stable_joined_hash64(parts: &[&str]) -> u64 {
    let mut hash = STABLE_PHASE_HASH_OFFSET;
    for part in parts {
        for byte in part.as_bytes() {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(STABLE_PHASE_HASH_PRIME);
        }
        hash ^= u64::from(b'|');
        hash = hash.wrapping_mul(STABLE_PHASE_HASH_PRIME);
    }
    hash
}

fn log_route_publish_sample(
    runtime: &ClientRuntimeId,
    route: &ObjectRoute,
    value_bytes: usize,
    operation: &'static str,
) {
    trace!(
        runtime = %runtime,
        key = %route.key.0,
        route_version = route.version.0,
        replica_count = route.replicas.len(),
        value_bytes,
        operation,
        replicas = ?route.replicas,
        "route publish completed"
    );
    if stable_debug_log_sample(&["route_publish", operation, &route.key.0]) {
        debug!(
            runtime = %runtime,
            key = %route.key.0,
            route_version = route.version.0,
            replica_count = route.replicas.len(),
            value_bytes,
            operation,
            "sampled route publish"
        );
    }
}

#[cfg(test)]
mod tests;
