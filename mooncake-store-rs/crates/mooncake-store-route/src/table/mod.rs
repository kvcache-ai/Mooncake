//! Single-authority local route table implementation.

mod delete_marker;
mod local;

#[cfg(test)]
pub(crate) use delete_marker::DEFAULT_DELETE_MARKER_BUDGET_BYTES;
pub(crate) use delete_marker::{DeleteMarkerSnapshot, DeleteMarkerStore};
pub(crate) use local::LocalRouteTable;
