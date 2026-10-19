// Copyright 2024 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! # mooncake_conductor
//!
//! Safe Rust bindings for the Mooncake Conductor client: prefix-cache hit
//! queries, service registration, and the conductor's global view. The API
//! mirrors the Python `mooncake.conductor.ConductorClient`, with error codes
//! surfaced as [`ConductorError`] instead of integer return values.
//!
//! ## Quick start
//!
//! ```ignore
//! use mooncake_conductor::{ClientConfig, ConductorClient, QueryRequest};
//!
//! let client = ConductorClient::new()?;
//! client.setup(&ClientConfig::new("127.0.0.1:13334"))?;
//!
//! let tokens: Vec<i32> = (1..=48).collect();
//! let result = client.query(&QueryRequest::new("my-model", 16, &tokens))?;
//! for (instance_id, hit) in &result.hits {
//!     println!("{instance_id}: {} tokens matched", hit.longest_matched);
//! }
//! ```
//!
//! The crate statically links `libconductor_cpp_core` from a Mooncake build
//! tree; see `build.rs` for the environment variables that locate it.

pub mod client;
pub mod error;
mod ffi;
pub mod types;

pub use client::ConductorClient;
pub use error::ConductorError;
pub use types::{
    CacheHit, ClientConfig, ContextView, GlobalView, HashProfile, HashProfileConfig, HealthStatus,
    PublisherKind, QueryRequest, QueryResult, RankMatch, RegisterResult, ServiceConfig,
    ServiceInfo,
};
