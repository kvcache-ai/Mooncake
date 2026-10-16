use std::sync::Arc;

use mooncake_store_core::{ClientLease, MetadataBackend, RouteControlMode, RouteDirectory};

use crate::control::RouteControlTransport;
use crate::shim::RouteMembershipProvider;

pub fn build_route_directory(
    mode: RouteControlMode,
    route_topk: usize,
    metadata: Arc<dyn MetadataBackend>,
    lease: &ClientLease,
    control_transport: Arc<dyn RouteControlTransport>,
    membership: Arc<dyn RouteMembershipProvider>,
) -> Arc<dyn RouteDirectory> {
    match mode {
        RouteControlMode::MetadataOnly => crate::shim::build_metadata_route_directory(metadata),
        RouteControlMode::EmbeddedWrh => crate::mesh::build_embedded_wrh_route_directory(
            route_topk,
            metadata,
            lease,
            control_transport,
            membership,
        ),
    }
}
