mod classic;
mod tent;

use std::ffi::c_void;

pub use classic::ClassicTransferEngine;
pub use tent::{SegmentInfo, TentEngine, TentEngineConfig};

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum TransportEngineKind {
    ClassicTe,
    Tent,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum Opcode {
    Read,
    Write,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum TransferStatus {
    Waiting,
    Pending,
    Invalid,
    Canceled,
    Completed,
    Timeout,
    Failed,
}

#[derive(Copy, Clone, Debug)]
pub struct TransferProgress {
    pub status: TransferStatus,
    pub transferred_bytes: u64,
}

#[derive(Copy, Clone, Debug)]
pub struct TransferRequest {
    pub opcode: Opcode,
    pub source: *mut c_void,
    pub target_id: u64,
    pub target_offset: u64,
    pub length: u64,
}

pub fn default_upstream_build_dir() -> &'static str {
    mooncake_transport_sys::DEFAULT_UPSTREAM_BUILD_DIR
}
