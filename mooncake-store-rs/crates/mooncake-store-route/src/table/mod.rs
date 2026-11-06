//! Single-authority local route table implementation.

mod delete_marker;
mod local;

pub(crate) use delete_marker::{
    DeleteMarkerSnapshot, DeleteMarkerStore, DEFAULT_DELETE_MARKER_BUDGET_BYTES,
};
pub(crate) use local::LocalRouteTable;
