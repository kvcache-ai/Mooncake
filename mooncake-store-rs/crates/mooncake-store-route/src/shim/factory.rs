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
    crate::table::build_route_table_directory(
        mode,
        route_topk,
        metadata,
        lease,
        control_transport,
        membership,
    )
}
