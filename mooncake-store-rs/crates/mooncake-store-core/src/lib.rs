pub mod cold_tier;
pub mod compat;
pub mod error;
pub mod hugepage;
pub mod identity;
pub mod identity_codec;
pub mod lifecycle;
pub mod route;
pub mod traits;

pub use cold_tier::{
    ColdBackingReplica, ColdBackingRoute, ColdBackingRouteFilter, ColdBackingState,
    ColdTierDeviceFilter, ColdTierDeviceRecord, ColdTierDeviceState, ColdTierDeviceUpdate,
    ColdTierPutDeviceResult, ColdTierTarget, ColdTierTargetSpec, ColdTierUsageDelta,
};
pub use compat::CompatibilityDescriptor;
pub use error::{Result, StoreError};
pub use hugepage::{parse_hugepage_size, HugePageConfig};
pub use identity::{
    ClientEndpointSet, ClientEpoch, ClientRuntimeId, ClientStableId, LogicalObjectId,
    NamespaceScope, ReuseIdentity, CONTROL_ADDR_LABEL, DEFAULT_DOMAIN, DEFAULT_OBJECT_SET,
    DEFAULT_QOS_TIER, DEFAULT_TENANT, METRICS_PORT_LABEL,
};
pub use identity_codec::{
    apply_route_identity, parse_legacy_scoped_key, route_logical_object_id, route_reuse_identity,
    scoped_logical_object_id, scoped_object_key,
};
pub use lifecycle::{ClientLifecycleState, HandoffKind, HandoffPlan};
pub use route::{
    CasResult, ClientLease, ObjectKey, ObjectRoute, ReplicaRoute, ReplicaTier, RouteCasRequest,
    RouteControlMode, RoutePolicy, RoutePolicyDomain, RouteState, RouteVersion,
    SegmentAnnouncement, SegmentLifecycleState, SegmentName, SegmentReservation,
    SegmentTargetChunk, TenantBandwidthShapingPolicy, TenantExecutionFairnessPolicy,
    TenantObjectAccounting, TenantObjectAccountingState, TenantPlacementPolicy, TenantPolicy,
    TenantPolicyScope, TenantPolicySpec, TenantQuotaAbortOutcome, TenantQuotaFinalizeOutcome,
    TenantQuotaFinalizeRequest, TenantQuotaPolicy, TenantQuotaReservation,
    TenantQuotaReservationOutcome, TenantQuotaReservationRequest, TenantQuotaReservationState,
    TenantQuotaState, TenantRoutePolicy,
};
pub use traits::{MetadataBackend, PlacementStrategy, RouteDirectory};
