use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::ffi::c_void;
use std::ptr;
use std::slice;
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use mooncake_store_core::{
    CasResult, ClientEndpointSet, ClientEpoch, ClientLease, ClientLifecycleState, ClientRuntimeId,
    ClientStableId, CompatibilityDescriptor, HandoffKind, HandoffPlan, MetadataBackend, ObjectKey,
    ObjectRoute, ReplicaRoute, ReplicaTier, Result, RouteCasRequest, RouteControlMode,
    RouteDirectory, RoutePolicy, RoutePolicyDomain, RouteState, RouteVersion, SegmentAnnouncement,
    SegmentLifecycleState, SegmentName, StoreError,
};
use mooncake_transport::{Opcode, TentEngine, TransferRequest};
use parking_lot::Mutex;
use tracing::{debug, info, info_span, warn};

use crate::control_plane::{
    control_address_label, AllocatorService, AuthorityService, ControlPlaneClient,
    ControlPlaneHandle, EvictionService, ReleaseOp, ReserveSpecificOp,
};
use crate::memory::{
    LocalMemoryConfig, LocalMemoryState, RegionAllocation, StorageExtentInfo, StorageSegmentSpec,
};
use crate::observability::{registry, OperationTracker};
use crate::placement::PlacementPlanner;
use crate::route_directory::{
    authority_compare_and_swap, authority_compare_and_swap_many, authority_get, authority_get_many,
    authority_list_routes_by_replica_owner, authority_replace, authority_replace_many,
    build_route_directory,
};
use crate::transport::{wait_for_batch_completion, StoreTransport, StoreTransportFactory};

const DEFAULT_TENANT: &str = "default";
const DEFAULT_TRANSFER_TIMEOUT: Duration = Duration::from_secs(3);
const DEFAULT_LIVE_CLIENT_SYNC_INTERVAL: Duration = Duration::from_secs(1);
const DEFAULT_SUSPECT_RUNTIME_TTL: Duration = Duration::from_secs(5);
const DEFAULT_ROUTE_TOPK: usize = 2;

include!("types.rs");
include!("builder.rs");
include!("state_core.rs");
include!("membership_sync.rs");
include!("state_adapters.rs");
include!("state_store.rs");
include!("helpers.rs");

pub struct StoreClient {
    metadata: Arc<dyn MetadataBackend>,
    route_directory: Arc<dyn RouteDirectory>,
    _control_plane: ControlPlaneHandle,
    control_client: Arc<ControlPlaneClient>,
    allocator: Arc<Mutex<LocalAllocatorState>>,
    storage_owner: Arc<StorageOwnerState>,
    lease: ClientLease,
    live_client_cache: SharedLiveClientCache,
    suspect_runtime_cache: SharedSuspectRuntimeCache,
    membership_sync: MembershipSyncHandle,
    _async_eviction: AsyncEvictionHandle,
    default_tenant: String,
    local_memory: LocalMemoryConfig,
    transport: Option<Arc<dyn StoreTransport>>,
    transport_factory: Option<Arc<dyn StoreTransportFactory>>,
    write_mode: WriteMode,
    route_control: RouteControlMode,
    route_topk: usize,
    state: Mutex<StoreState>,
}

enum HealthUpdateKind {
    Heartbeat,
    StateTransition { operation: &'static str },
}

pub struct HealthUpdate {
    metadata: Arc<dyn MetadataBackend>,
    lease: ClientLease,
    kind: HealthUpdateKind,
}

pub type HeartbeatLease = HealthUpdate;

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
                let result = metadata.update_client_state(&lease.runtime, lease.state);
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

#[cfg(test)]
mod tests;
