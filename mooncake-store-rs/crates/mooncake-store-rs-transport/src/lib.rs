mod classic;
mod env;
mod tent;

use std::ffi::c_void;

use serde::{Deserialize, Serialize};

pub use classic::{ClassicEngineConfig, ClassicTransferEngine, ClassicTransportProtocol};
pub use tent::{SegmentBuffer, SegmentInfo, SegmentKind, TentEngine, TentEngineConfig};

/// Ascend device-memory helpers used by the client when a put_from source or a
/// get_into destination is NPU memory: the store copies through host memory on
/// the CPU, so device ranges must be staged with ACL copies.
///
/// Returns `None` when the ACL helper is unavailable (non-ascend build or the
/// TENT shim is not loaded), `Some(false)` for host memory.
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub fn ascend_pointer_is_device(addr: *mut c_void) -> Option<bool> {
    let rc = unsafe { mooncake_store_rs_transport_sys::tent::tentAclPointerIsDevice(addr) };
    match rc {
        1 => Some(true),
        0 => Some(false),
        _ => None,
    }
}

#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub fn ascend_copy_d2h(dst: *mut c_void, src: *const c_void, len: usize) -> Result<(), String> {
    let rc = unsafe { mooncake_store_rs_transport_sys::tent::tentAclMemcpyD2H(dst, src, len) };
    if rc == 0 {
        Ok(())
    } else {
        Err(format!("ACL D2H copy failed (rc={rc})"))
    }
}

#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub fn ascend_copy_h2d(dst: *mut c_void, src: *const c_void, len: usize) -> Result<(), String> {
    let rc = unsafe { mooncake_store_rs_transport_sys::tent::tentAclMemcpyH2D(dst, src, len) };
    if rc == 0 {
        Ok(())
    } else {
        Err(format!("ACL H2D copy failed (rc={rc})"))
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum TransportEngineKind {
    ClassicTe,
    Tent,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum Opcode {
    Read,
    Write,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum TransferStatus {
    Waiting,
    Pending,
    Invalid,
    Canceled,
    Completed,
    Timeout,
    Failed,
}

#[derive(Copy, Clone, Debug, Serialize, Deserialize)]
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

#[derive(Copy, Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub enum TransferPacingMode {
    LatencySensitive,
    #[default]
    Standard,
    ThroughputOptimized,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct TransferBatchHints {
    pub pacing_group: Option<String>,
    pub mode: TransferPacingMode,
    pub max_inflight_bytes: Option<u64>,
}

pub fn rdma_device_max_registration_size() -> Option<usize> {
    usize::try_from(unsafe {
        mooncake_store_rs_transport_sys::tent::mooncake_tent_probe_rdma_max_mr_size()
    })
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
        clamp_registration_size, rdma_device_max_registration_size, Opcode, TransferBatchHints,
        TransferPacingMode, TransferProgress, TransferRequest, TransferStatus, TransportEngineKind,
    };

    #[test]
    fn transfer_batch_hints_serde_round_trip_preserves_fields() {
        let hints = TransferBatchHints {
            pacing_group: Some("tenant:default".to_string()),
            mode: TransferPacingMode::ThroughputOptimized,
            max_inflight_bytes: Some(8192),
        };

        let encoded = serde_json::to_string(&hints).expect("hints should serialize");
        let decoded: TransferBatchHints =
            serde_json::from_str(&encoded).expect("hints should deserialize");
        assert_eq!(decoded, hints);
        assert!(encoded.contains("ThroughputOptimized"));
    }

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
        let hints = TransferBatchHints {
            pacing_group: Some("tenant:default".to_string()),
            mode: TransferPacingMode::ThroughputOptimized,
            max_inflight_bytes: Some(11),
        };

        assert_eq!(request.target_id, 7);
        assert_eq!(progress.status, TransferStatus::Completed);
        assert_eq!(hints.mode, TransferPacingMode::ThroughputOptimized);
        assert_eq!(
            TransportEngineKind::ClassicTe,
            TransportEngineKind::ClassicTe
        );
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
