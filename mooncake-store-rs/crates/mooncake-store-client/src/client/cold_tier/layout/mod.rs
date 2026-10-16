//! Cold Tier physical layout primitives shared by its storage executors.

mod physical_key;
mod value_chunk;

pub use physical_key::{derive_physical_key, OpaquePhysicalKey, PhysicalKeyInput};
pub(crate) use value_chunk::ValueChunkPlan;
