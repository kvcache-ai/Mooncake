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

// Values of `enum mooncake_conductor_error` (= mooncake::conductor::ErrorCode),
// matching the constants python/mooncake/conductor.py re-exports.
const INTERNAL_ERROR: i32 = -1;
const INVALID_PARAMS: i32 = -600;
const RPC_FAIL: i32 = -900;
const RPC_TIMEOUT: i32 = -901;
const CONDUCTOR_UNAVAILABLE: i32 = -2000;
const SERVICE_NOT_FOUND: i32 = -2001;

/// Errors returned by Mooncake Conductor operations.
///
/// `#[non_exhaustive]` so adding variants is not a breaking change; downstream
/// matches must include a wildcard arm.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ConductorError {
    /// The C layer could not allocate a client handle.
    #[error("null handle or pointer")]
    NullHandle,

    /// A C string could not be constructed because the input contains an
    /// interior null byte.
    #[error("string argument contains null byte: {0}")]
    InvalidString(#[from] NulError),

    /// Unexpected failure inside the client (`INTERNAL_ERROR`).
    #[error("conductor internal error")]
    Internal,

    /// The request was rejected as malformed (`INVALID_PARAMS`), e.g. an
    /// address without a port or a conflicting re-registration.
    #[error("invalid parameters")]
    InvalidParams,

    /// The RPC transport failed (`RPC_FAIL`).
    #[error("conductor RPC failed")]
    RpcFail,

    /// The RPC did not complete within the request timeout (`RPC_TIMEOUT`).
    #[error("conductor RPC timed out")]
    RpcTimeout,

    /// The client is not connected; call `setup` first
    /// (`CONDUCTOR_UNAVAILABLE`).
    #[error("conductor unavailable")]
    Unavailable,

    /// Unregister found no matching service (`SERVICE_NOT_FOUND`).
    #[error("service not found")]
    ServiceNotFound,

    /// A negative status code this crate does not know about.
    #[error("conductor operation failed with code {0}")]
    Unknown(i32),
}

impl ConductorError {
    /// Map a negative C status code to an error.
    pub fn from_code(code: i32) -> Self {
        match code {
            INTERNAL_ERROR => Self::Internal,
            INVALID_PARAMS => Self::InvalidParams,
            RPC_FAIL => Self::RpcFail,
            RPC_TIMEOUT => Self::RpcTimeout,
            CONDUCTOR_UNAVAILABLE => Self::Unavailable,
            SERVICE_NOT_FOUND => Self::ServiceNotFound,
            other => Self::Unknown(other),
        }
    }

    /// The C status code this error corresponds to, if any.
    pub fn code(&self) -> Option<i32> {
        match self {
            Self::Internal => Some(INTERNAL_ERROR),
            Self::InvalidParams => Some(INVALID_PARAMS),
            Self::RpcFail => Some(RPC_FAIL),
            Self::RpcTimeout => Some(RPC_TIMEOUT),
            Self::Unavailable => Some(CONDUCTOR_UNAVAILABLE),
            Self::ServiceNotFound => Some(SERVICE_NOT_FOUND),
            Self::Unknown(code) => Some(*code),
            Self::NullHandle | Self::InvalidString(_) => None,
        }
    }
}

/// Turn a C return code into a `Result`.
pub(crate) fn check(rc: i32) -> Result<(), ConductorError> {
    if rc == 0 {
        Ok(())
    } else {
        Err(ConductorError::from_code(rc))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ffi;

    #[test]
    fn codes_match_c_header() {
        for (local, c_value) in [
            (
                INTERNAL_ERROR,
                i64::from(ffi::MOONCAKE_CONDUCTOR_INTERNAL_ERROR),
            ),
            (
                INVALID_PARAMS,
                i64::from(ffi::MOONCAKE_CONDUCTOR_INVALID_PARAMS),
            ),
            (RPC_FAIL, i64::from(ffi::MOONCAKE_CONDUCTOR_RPC_FAIL)),
            (RPC_TIMEOUT, i64::from(ffi::MOONCAKE_CONDUCTOR_RPC_TIMEOUT)),
            (
                CONDUCTOR_UNAVAILABLE,
                i64::from(ffi::MOONCAKE_CONDUCTOR_UNAVAILABLE),
            ),
            (
                SERVICE_NOT_FOUND,
                i64::from(ffi::MOONCAKE_CONDUCTOR_SERVICE_NOT_FOUND),
            ),
        ] {
            assert_eq!(i64::from(local), c_value);
        }
        assert_eq!(i64::from(ffi::MOONCAKE_CONDUCTOR_OK), 0);
    }

    #[test]
    fn code_round_trips() {
        for code in [
            INTERNAL_ERROR,
            INVALID_PARAMS,
            RPC_FAIL,
            RPC_TIMEOUT,
            CONDUCTOR_UNAVAILABLE,
            SERVICE_NOT_FOUND,
            -12345,
        ] {
            assert_eq!(ConductorError::from_code(code).code(), Some(code));
        }
        assert!(matches!(
            ConductorError::from_code(-12345),
            ConductorError::Unknown(-12345)
        ));
        assert!(check(0).is_ok());
        assert!(matches!(
            check(SERVICE_NOT_FOUND),
            Err(ConductorError::ServiceNotFound)
        ));
    }
}
