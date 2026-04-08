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

use std::ffi::NulError;

/// Errors returned by Mooncake Store operations.
#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    /// A required pointer argument was null (e.g. the store handle has not
    /// been initialised yet, or an internal allocation failed).
    #[error("null handle or pointer")]
    NullHandle,

    /// A C string could not be constructed because the input contains an
    /// interior null byte.
    #[error("string argument contains null byte: {0}")]
    InvalidString(#[from] NulError),

    /// The C layer returned a negative status code.
    #[error("store operation failed with code {0}")]
    OperationFailed(i32),

    /// The requested key does not exist in the store.
    ///
    /// This variant is available for consumers that perform an explicit
    /// existence check (e.g. via [`crate::MooncakeStore::is_exist`]) before
    /// acting on the result. The C API itself does not expose a distinct
    /// "not found" error code, so internal methods return
    /// [`OperationFailed`](StoreError::OperationFailed) instead.
    #[error("key not found")]
    NotFound,
}
