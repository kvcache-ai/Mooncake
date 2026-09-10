mod client;
mod control_plane;
mod memory;
mod observability;
mod placement;
mod route_directory;
mod transport;

pub use client::cold_tier::nof::{
    derive_physical_key, NofBackend, NofBacking, NofHealth, NofManagedAllocationRequest,
    NofManagedAllocator, NofManagedLimits, NofManagedLocator, NofManagedRead,
    NofManagedReadRequest, NofManagedWrite, NofManagedWriteRequest, NofObjectDelete,
    NofObjectLimits, NofObjectQuery, NofObjectRead, NofObjectShardWrite, NofObjectState,
    NofObjectWrite, NofPhysicalDelete, NofPhysicalDeleteRequest, NofPhysicalQuery,
    NofPhysicalQueryRequest, NofPhysicalRead, NofPhysicalReadRequest, NofPhysicalWrite,
    NofPhysicalWriteRequest, NofStorageHealth, NofTargetConfig, OpaquePhysicalKey,
    PhysicalKeyInput,
};
#[cfg(feature = "kvcs-capi")]
pub use client::cold_tier::nof::{KvcsCapiExecutor, KvcsLowLevelClient, KvcsMode};
pub use client::{
    stable_phase_spread_ms, BandwidthShaping, ColdTierKind, ColdTierOffloadMode,
    ColdTierShutdownMode, ColdTierSsdEngine, ColdTierTarget, ColdTierTargetConfig,
    ColdTierTargetSpec, DebugEvictAllResult, ExecutionFairness, GetRequest, HealthChannel,
    HealthUpdate, HeartbeatLease, MooncakeCompatibilityFacade, MultiBufferGetRequest,
    MultiBufferPutRequest, NamespaceQuota, ObjectRef, PutFromRequest, PutRequest,
    ReadQueryResultCache, ReplicationPolicy, StoreClient, StoreClientBuilder,
};
pub use control_plane::{pb as control_plane_pb, MigrationControlClient};
pub use memory::{LocalMemoryConfig, ScratchReservation};
pub use mooncake_store_core::RouteControlMode;
pub use mooncake_transport::{TransferBatchHints, TransferPacingMode};
pub use observability::{
    init_tracing, init_tracing_from_env, metrics_http_server_addr, record_heartbeat_health,
    record_tenant_local_eviction, record_tenant_quota_reconcile, register_debug_evict_all,
    render_breakdown_json, render_prometheus_metrics, render_stats_json, snapshot_metrics,
    start_metrics_http_server, start_metrics_http_server_from_env, stop_metrics_http_server,
    OperationMetricSnapshot, OperationTracker,
};
pub use placement::{PlacementChoice, PlacementPlanner};
pub use transport::{
    http_transport_label, wait_for_batch_completion, ClassicTeTransportFactory,
    HttpStoreTransportFactory, HttpTransportServerHandle, StoreTransport, StoreTransportFactory,
    TentTransportFactory,
};

#[cfg(test)]
pub use observability::{metrics_test_lock, reset_metrics};
