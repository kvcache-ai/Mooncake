pub mod compat;
pub mod error;
pub mod identity;
pub mod lifecycle;
pub mod route;
pub mod traits;

pub use compat::CompatibilityDescriptor;
pub use error::{Result, StoreError};
pub use identity::{ClientEndpointSet, ClientEpoch, ClientRuntimeId, ClientStableId};
pub use lifecycle::{ClientLifecycleState, HandoffKind, HandoffPlan};
pub use route::{
    CasResult, ClientLease, ObjectKey, ObjectRoute, ReplicaRoute, ReplicaTier, RouteCasRequest,
    RouteState, RouteVersion, SegmentAnnouncement, SegmentLifecycleState, SegmentName,
    SegmentReservation,
};
pub use traits::{MetadataBackend, PlacementStrategy, RouteDirectory};
