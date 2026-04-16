mod classic;
mod env;
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

pub fn rdma_device_max_registration_size() -> Option<usize> {
    usize::try_from(unsafe { mooncake_transport_sys::tent::mooncake_tent_probe_rdma_max_mr_size() })
        .ok()
        .filter(|value| *value > 0)
}

pub(crate) fn clamp_registration_size(
    probed_limit: Option<usize>,
    configured_cap: Option<usize>,
) -> Option<usize> {
    match (probed_limit, configured_cap) {
        (Some(probed), Some(configured)) => Some(probed.min(configured)),
        (Some(probed), None) => Some(probed),
        (None, Some(configured)) => Some(configured),
        (None, None) => None,
    }
}

#[cfg(test)]
mod tests {
    use super::{
        clamp_registration_size, default_upstream_build_dir, rdma_device_max_registration_size,
        Opcode, TransferProgress, TransferRequest, TransferStatus, TransportEngineKind,
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

    #[test]
    fn rdma_registration_limit_helpers_preserve_smallest_positive_limit() {
        assert_eq!(clamp_registration_size(Some(64), Some(32)), Some(32));
        assert_eq!(clamp_registration_size(Some(64), None), Some(64));
        assert_eq!(clamp_registration_size(None, Some(32)), Some(32));
        assert_eq!(clamp_registration_size(None, None), None);
        assert!(rdma_device_max_registration_size().is_none_or(|value| value > 0));
    }
}
