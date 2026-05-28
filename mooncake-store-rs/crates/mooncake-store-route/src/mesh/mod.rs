//! Route authority mesh internals.

mod local_authority;
mod registry;

pub use local_authority::LocalRouteAuthority;

pub(crate) use registry::{
    authority_compare_and_swap_many, authority_get_many, authority_get_version_floor,
    authority_is_local, authority_list_reuse_candidates, authority_list_routes_by_replica_owner,
    authority_list_routes_in_scope, authority_replace_many, local_authority_service,
    register_local_authority, unregister_local_authority,
};
