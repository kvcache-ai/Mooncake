//! Single-authority local route table implementation.

mod delete_marker;
mod gauges;
mod local;

#[cfg(test)]
pub(crate) use delete_marker::DEFAULT_DELETE_MARKER_BUDGET_BYTES;
pub(crate) use delete_marker::{DeleteMarkerSnapshot, DeleteMarkerStore};
pub use gauges::RouteGaugeSnapshot;
pub(crate) use local::LocalRouteTable;
