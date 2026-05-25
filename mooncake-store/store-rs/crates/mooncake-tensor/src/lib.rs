//! Pure-Rust tensor metadata encoding/decoding.
//!
//! Binary layout is compatible with the C++ `integration_utils.h` in the community
//! Mooncake repository.  No PyO3 dependency — this crate can be used from both the
//! Python binding layer and from plain Rust clients.

pub mod metadata;

pub use metadata::*;
