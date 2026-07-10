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

//! # mooncake_store
//!
//! Safe Rust bindings for the [Mooncake](https://github.com/kvcache-ai/Mooncake)
//! distributed KV-cache store.
//!
//! ## Quick start
//!
//! ```ignore
//! use mooncake_store::{MooncakeStore, ReplicateConfig};
//!
//! let store = MooncakeStore::new()?;
//! store.setup(
//!     "localhost",
//!     "http://127.0.0.1:8080/metadata",
//!     512 << 20, // global_segment_size
//!     128 << 20, // local_buffer_size
//!     "tcp",
//!     "",
//!     "127.0.0.1:50051",
//! )?;
//!
//! store.put("hello", b"world", None)?;
//!
//! let value = store.get("hello")?;
//! assert_eq!(value, b"world");
//!
//! store.remove("hello", false)?;
//! ```

pub mod error;
pub mod store;

pub use error::StoreError;
pub use store::{MooncakeStore, ReplicateConfig};
