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

/// Errors returned by Transfer Engine operations.
///
/// `#[non_exhaustive]` so adding variants is not a breaking change; downstream
/// matches must include a wildcard arm.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum EngineError {
    /// The C layer failed to allocate a Transfer Engine handle.
    #[error("failed to create Transfer Engine handle")]
    NullHandle,

    /// A C string could not be constructed because the input contains an
    /// interior null byte.
    #[error("string argument contains null byte: {0}")]
    InvalidString(#[from] NulError),

    /// The C layer returned a non-zero status code.
    #[error("transfer engine operation failed with code {0}")]
    OperationFailed(i32),

    /// One or more arguments are invalid (e.g. mismatched slice lengths).
    #[error("invalid argument: {0}")]
    InvalidArgument(&'static str),

    /// A transfer finished in a failed or timed-out state.
    #[error("transfer did not complete successfully")]
    TransferFailed,

    /// Waiting for a transfer exceeded the caller-supplied timeout.
    #[error("transfer timed out")]
    Timeout,
}
