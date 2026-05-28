//! Public shim for integrating and operating the object route table.

mod contracts;
mod factory;
mod metadata;
mod operations;

pub(crate) use contracts::RouteAuthorityClient;
pub use contracts::{RouteAuthorityService, RouteMembershipProvider};
pub use factory::build_route_directory;
pub(crate) use metadata::build_metadata_route_directory;
pub use operations::{RouteHitReporter, RouteOperations};
