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

//! Public types that match the Transfer Engine C ABI (`transfer_engine_c.h`).
//!
//! `TransferRequest`, `BufferEntry`, and `TransferStatus` are `#[repr(C)]` and
//! layout-checked against the bindgen types so hot-path calls can pass Rust
//! slices to the C API without allocating or copying.

use std::ffi::c_void;

use crate::ffi;

/// Wildcard memory location accepted by `registerLocalMemory` (`"*"`).
pub const WILDCARD_LOCATION: &str = "*";

/// Local-segment handle (`LOCAL_SEGMENT` in the C header).
pub const LOCAL_SEGMENT: SegmentId = 0;

/// Sentinel returned by `allocateBatchID` on failure (`INVALID_BATCH`).
pub const INVALID_BATCH: u64 = u64::MAX;

/// Integer handle for an opened remote (or local) segment.
pub type SegmentId = i32;

/// Opaque batch identifier returned by [`crate::TransferEngine::allocate_batch_id`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub struct BatchId(pub u64);

impl BatchId {
    /// Returns `true` when this value is the C `INVALID_BATCH` sentinel.
    #[inline]
    pub fn is_invalid(self) -> bool {
        self.0 == INVALID_BATCH
    }
}

/// Transfer opcode. Numeric values match `OPCODE_READ` / `OPCODE_WRITE`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum Opcode {
    Read = 0,
    Write = 1,
}

/// Transfer completion state. Numeric values match `STATUS_*` in the C header.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum TransferStatusCode {
    Waiting = 0,
    Pending = 1,
    Invalid = 2,
    Canceled = 3,
    Completed = 4,
    Timeout = 5,
    Failed = 6,
}

impl TransferStatusCode {
    /// Convert a raw C status integer. Unknown values map to [`Self::Invalid`].
    #[inline]
    pub fn from_raw(raw: i32) -> Self {
        match raw {
            0 => Self::Waiting,
            1 => Self::Pending,
            2 => Self::Invalid,
            3 => Self::Canceled,
            4 => Self::Completed,
            5 => Self::Timeout,
            6 => Self::Failed,
            _ => Self::Invalid,
        }
    }

    /// `true` when the task has reached a terminal success or failure state.
    #[inline]
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Completed | Self::Failed | Self::Timeout | Self::Canceled | Self::Invalid
        )
    }

    /// `true` when the task completed successfully.
    #[inline]
    pub fn is_completed(self) -> bool {
        self == Self::Completed
    }
}

/// One transfer operation. Layout matches `transfer_request_t`.
///
/// Passing a `&[TransferRequest]` to
/// [`crate::TransferEngine::submit_transfer`] is a zero-copy FFI call.
#[derive(Debug, Clone, Copy)]
#[repr(C)]
pub struct TransferRequest {
    pub opcode: Opcode,
    pub source: *mut c_void,
    pub target_id: SegmentId,
    pub target_offset: u64,
    pub length: u64,
}

impl TransferRequest {
    /// Build a READ request (remote → local).
    #[inline]
    pub fn read(local: *mut c_void, target_id: SegmentId, target_offset: u64, length: u64) -> Self {
        Self {
            opcode: Opcode::Read,
            source: local,
            target_id,
            target_offset,
            length,
        }
    }

    /// Build a WRITE request (local → remote).
    #[inline]
    pub fn write(
        local: *mut c_void,
        target_id: SegmentId,
        target_offset: u64,
        length: u64,
    ) -> Self {
        Self {
            opcode: Opcode::Write,
            source: local,
            target_id,
            target_offset,
            length,
        }
    }
}

// SAFETY: TransferRequest is a C POD of pointers and integers. Sending the
// descriptor across threads is safe; the caller still owns the pointed-to
// memory and must keep it valid for the transfer.
unsafe impl Send for TransferRequest {}
unsafe impl Sync for TransferRequest {}

/// One registered buffer. Layout matches `buffer_entry_t`.
#[derive(Debug, Clone, Copy)]
#[repr(C)]
pub struct BufferEntry {
    pub addr: *mut c_void,
    pub length: usize,
}

impl BufferEntry {
    #[inline]
    pub fn new(addr: *mut c_void, length: usize) -> Self {
        Self { addr, length }
    }
}

unsafe impl Send for BufferEntry {}
unsafe impl Sync for BufferEntry {}

/// Per-task status. Layout matches `transfer_status_t`.
#[derive(Debug, Clone, Copy)]
#[repr(C)]
pub struct TransferStatus {
    pub status: TransferStatusCode,
    pub transferred_bytes: u64,
}

impl TransferStatus {
    #[inline]
    pub fn is_completed(&self) -> bool {
        self.status.is_completed()
    }

    #[inline]
    pub fn is_terminal(&self) -> bool {
        self.status.is_terminal()
    }
}

/// Notification payload used by `submitTransferWithNotify` / `getNotifsFromEngine`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NotifyMsg {
    pub name: String,
    pub msg: String,
}

impl NotifyMsg {
    pub fn new(name: impl Into<String>, msg: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            msg: msg.into(),
        }
    }
}

/// One NIC's load snapshot from `getNicLoadStats`.
#[derive(Debug, Clone)]
pub struct NicLoadStat {
    pub device_name: String,
    pub inflight_bytes: u64,
    pub ewma_bandwidth_bps: f64,
}

// Compile-time layout checks against bindgen so submit_transfer can pass
// slices through without converting element-by-element.
const _: () = {
    assert!(
        std::mem::size_of::<TransferRequest>() == std::mem::size_of::<ffi::transfer_request_t>()
    );
    assert!(
        std::mem::align_of::<TransferRequest>() == std::mem::align_of::<ffi::transfer_request_t>()
    );
    assert!(std::mem::size_of::<BufferEntry>() == std::mem::size_of::<ffi::buffer_entry_t>());
    assert!(std::mem::align_of::<BufferEntry>() == std::mem::align_of::<ffi::buffer_entry_t>());
    assert!(std::mem::size_of::<TransferStatus>() == std::mem::size_of::<ffi::transfer_status_t>());
    assert!(
        std::mem::align_of::<TransferStatus>() == std::mem::align_of::<ffi::transfer_status_t>()
    );
};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_field_offsets_match_c() {
        assert_eq!(
            std::mem::offset_of!(TransferRequest, opcode),
            std::mem::offset_of!(ffi::transfer_request, opcode)
        );
        assert_eq!(
            std::mem::offset_of!(TransferRequest, source),
            std::mem::offset_of!(ffi::transfer_request, source)
        );
        assert_eq!(
            std::mem::offset_of!(TransferRequest, target_id),
            std::mem::offset_of!(ffi::transfer_request, target_id)
        );
        assert_eq!(
            std::mem::offset_of!(TransferRequest, target_offset),
            std::mem::offset_of!(ffi::transfer_request, target_offset)
        );
        assert_eq!(
            std::mem::offset_of!(TransferRequest, length),
            std::mem::offset_of!(ffi::transfer_request, length)
        );
    }

    #[test]
    fn opcode_and_status_raw_values() {
        assert_eq!(Opcode::Read as i32, 0);
        assert_eq!(Opcode::Write as i32, 1);
        assert_eq!(TransferStatusCode::Completed as i32, 4);
        assert_eq!(TransferStatusCode::Failed as i32, 6);
        assert!(TransferStatusCode::from_raw(4).is_completed());
        assert!(TransferStatusCode::from_raw(6).is_terminal());
        assert!(!TransferStatusCode::Pending.is_terminal());
    }
}
