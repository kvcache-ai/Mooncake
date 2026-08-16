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

//! # transfer_engine_rust
//!
//! High-performance Rust bindings for the Mooncake Transfer Engine C API
//! (`transfer_engine_c.h`).
//!
//! Hot-path types (`TransferRequest`, `BufferEntry`, `TransferStatus`) are
//! `#[repr(C)]` and layout-checked against the C ABI, so
//! [`TransferEngine::submit_transfer`] passes slices to C with no heap
//! allocation or per-request copy on the Rust side.
//!
//! ```ignore
//! use transfer_engine_rust::{MemoryPool, Opcode, TransferEngine, TransferRequest};
//!
//! let engine = TransferEngine::initialize(
//!     "127.0.0.1:12345",
//!     "http://127.0.0.1:8080/metadata",
//!     "tcp",
//!     "",
//! )?;
//! let mut pool = MemoryPool::new(1 << 20);
//! unsafe {
//!     engine.register_local_memory(pool.as_void_ptr(), pool.len(), "cpu:0")?;
//!     engine.transfer_sync_write("peer:12345", pool.as_void_ptr(), 0, 4096)?;
//!     engine.unregister_local_memory(pool.as_void_ptr())?;
//! }
//! # Ok::<(), transfer_engine_rust::EngineError>(())
//! ```

mod engine;
mod error;
mod ffi;
mod memory;
mod types;

pub use engine::{TransferEngine, TransferEngineOptions};
pub use error::EngineError;
pub use memory::MemoryPool;
pub use types::{
    BatchId, BufferEntry, NicLoadStat, NotifyMsg, Opcode, SegmentId, TransferRequest,
    TransferStatus, TransferStatusCode, INVALID_BATCH, LOCAL_SEGMENT, WILDCARD_LOCATION,
};

// Historical module path used by the old example binary.
pub use engine as transfer_engine;
