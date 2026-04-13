mod classic;
mod tent;

use std::ffi::c_void;

pub use classic::{ClassicEngineConfig, ClassicTransferEngine, ClassicTransportProtocol};
pub use tent::{SegmentBuffer, SegmentInfo, SegmentKind, TentEngine, TentEngineConfig};

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

#[cfg(test)]
mod tests {
    use super::{
        default_upstream_build_dir, Opcode, TransferProgress, TransferRequest, TransferStatus,
        TransportEngineKind,
    };

    #[test]
    fn transport_public_types_are_constructible() {
        let request = TransferRequest {
            opcode: Opcode::Write,
            source: std::ptr::null_mut(),
            target_id: 7,
            target_offset: 9,
            length: 11,
        };
        let progress = TransferProgress {
            status: TransferStatus::Completed,
            transferred_bytes: 11,
        };

        assert_eq!(request.target_id, 7);
        assert_eq!(progress.status, TransferStatus::Completed);
        assert_eq!(
            TransportEngineKind::ClassicTe,
            TransportEngineKind::ClassicTe
        );
        assert!(!default_upstream_build_dir().is_empty());
    }
}
