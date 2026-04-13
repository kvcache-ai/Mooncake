pub mod compat;
pub mod error;
pub mod hugepage;
pub mod identity;
pub mod lifecycle;
pub mod route;
pub mod traits;

pub use compat::CompatibilityDescriptor;
pub use error::{Result, StoreError};
pub use hugepage::{parse_hugepage_size, HugePageConfig};
pub use identity::{
    ClientEndpointSet, ClientEpoch, ClientRuntimeId, ClientStableId, LogicalObjectId,
    NamespaceScope, ReuseIdentity, DEFAULT_DOMAIN, DEFAULT_OBJECT_SET, DEFAULT_QOS_TIER,
    DEFAULT_TENANT,
};
pub use lifecycle::{ClientLifecycleState, HandoffKind, HandoffPlan};
pub use route::{
    CasResult, ClientLease, ObjectKey, ObjectRoute, ReplicaRoute, ReplicaTier, RouteCasRequest,
    RouteControlMode, RoutePolicy, RoutePolicyDomain, RouteState, RouteVersion,
    SegmentAnnouncement, SegmentLifecycleState, SegmentName, SegmentReservation,
};
pub use traits::{MetadataBackend, PlacementStrategy, RouteDirectory};
