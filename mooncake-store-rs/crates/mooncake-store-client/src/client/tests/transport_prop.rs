use mooncake_store_core::StoreError;
use mooncake_store_test_utils::transport::TestTransport;
use mooncake_store_transport_core::StoreTransport;
use mooncake_transport::{Opcode, TransferBatchHints, TransferPacingMode, TransferRequest};
use proptest::prelude::*;

// ---------------------------------------------------------------------------
// Property: batch allocation is monotonically increasing
// ---------------------------------------------------------------------------

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    #[test]
    fn prop_batch_ids_are_strictly_increasing(n in 1usize..=20usize) {
        let transport = TestTransport::new("seg");
        let _ = transport.allocate_memory(512, "cpu:0").unwrap();
        let mut prev = 0u64;
        for _ in 0..n {
            let bid = transport.allocate_batch(1).expect("allocate_batch should succeed");
            prop_assert!(bid > prev, "batch id {bid} should be > prev {prev}");
            prev = bid;
            transport.free_batch(bid).expect("free_batch should succeed");
        }
    }

    #[test]
    fn prop_allocate_then_free_batch_size_zero_is_rejected(n in 1usize..=10usize) {
        let transport = TestTransport::new("seg");
        for _ in 0..n {
            let result = transport.allocate_batch(0);
            prop_assert!(result.is_err(), "batch_size 0 must be rejected");
            prop_assert!(
                matches!(result.unwrap_err(), StoreError::Transport(_)),
                "error must be Transport variant"
            );
        }
    }

    #[test]
    fn prop_memory_allocation_size_preserved(sizes in prop::collection::vec(16usize..=1024usize, 1..=8)) {
        let transport = TestTransport::new("seg");
        for size in &sizes {
            let addr = transport.allocate_memory(*size, "cpu:0").expect("alloc should succeed");
            prop_assert!(!addr.is_null(), "allocated address must not be null");
            transport.free_memory(addr).expect("free should succeed");
        }
    }

    #[test]
    fn prop_register_unregister_is_idempotent_per_region(size in 64usize..=512usize) {
        let transport = TestTransport::new("seg");
        let addr = transport.allocate_memory(size, "cpu:0").expect("alloc");
        transport.register_memory(addr, size).expect("register");
        transport.unregister_memory(addr, size).expect("unregister");
        // Double-register is allowed (insert overwrites)
        transport.register_memory(addr, size).expect("re-register");
        transport.unregister_memory(addr, size).expect("re-unregister");
        transport.free_memory(addr).expect("free");
    }

    #[test]
    fn prop_write_read_round_trip_arbitrary_payload(
        payload in prop::collection::vec(any::<u8>(), 1..=256usize),
    ) {
        let transport = TestTransport::new("seg");
        let size = payload.len().max(1);
        let addr = transport.allocate_memory(size, "cpu:0").expect("alloc");

        let batch = transport.allocate_batch(1).expect("alloc batch");
        let write_req = TransferRequest {
            opcode: Opcode::Write,
            source: payload.as_ptr() as *mut std::ffi::c_void,
            target_id: 1,
            target_offset: addr as u64,
            length: size as u64,
        };
        transport.submit(batch, &[write_req]).expect("write should succeed");
        transport.free_batch(batch).expect("free batch");

        let mut read_buf = vec![0u8; size];
        let read_batch = transport.allocate_batch(1).expect("alloc read batch");
        let read_req = TransferRequest {
            opcode: Opcode::Read,
            source: read_buf.as_mut_ptr() as *mut std::ffi::c_void,
            target_id: 1,
            target_offset: addr as u64,
            length: size as u64,
        };
        transport.submit(read_batch, &[read_req]).expect("read should succeed");
        transport.free_batch(read_batch).expect("free read batch");
        transport.free_memory(addr).expect("free memory");

        prop_assert_eq!(&read_buf, &payload, "read data must match written data");
    }

    #[test]
    fn prop_submitted_batch_bytes_totals_accumulate(
        batch_sizes in prop::collection::vec(1u64..=128u64, 1..=6),
    ) {
        let transport = TestTransport::new("seg");
        let total: u64 = batch_sizes.iter().sum();
        let region_size = (total as usize).max(1);
        let addr = transport.allocate_memory(region_size, "cpu:0").expect("alloc");

        let mut expected_bytes = Vec::new();
        let mut offset = 0u64;
        for &size in &batch_sizes {
            let batch = transport.allocate_batch(1).expect("alloc batch");
            let req = TransferRequest {
                opcode: Opcode::Write,
                source: [0u8; 128].as_ptr() as *mut std::ffi::c_void,
                target_id: 1,
                target_offset: addr as u64 + offset,
                length: size,
            };
            transport.submit(batch, &[req]).expect("submit");
            transport.free_batch(batch).expect("free batch");
            expected_bytes.push(size);
            offset += size;
        }

        prop_assert_eq!(
            transport.submitted_batch_bytes(),
            expected_bytes,
            "submitted bytes should track each batch independently"
        );
        transport.free_memory(addr).expect("free");
    }
}

// ---------------------------------------------------------------------------
// Deterministic scenario tests
// ---------------------------------------------------------------------------

#[test]
fn transport_free_unknown_batch_returns_not_found() {
    let transport = TestTransport::new("seg");
    let err = transport
        .free_batch(9999)
        .expect_err("freeing unknown batch must fail");
    assert!(
        matches!(err, StoreError::NotFound(_)),
        "expected NotFound, got {err:?}"
    );
}

#[test]
fn transport_submit_to_unknown_batch_returns_not_found() {
    let transport = TestTransport::new("seg");
    let addr = transport.allocate_memory(64, "cpu:0").unwrap();
    let req = TransferRequest {
        opcode: Opcode::Write,
        source: [0u8; 8].as_ptr() as *mut std::ffi::c_void,
        target_id: 1,
        target_offset: addr as u64,
        length: 8,
    };
    let err = transport
        .submit(9999, &[req])
        .expect_err("submit to unknown batch must fail");
    assert!(matches!(err, StoreError::NotFound(_)));
    transport.free_memory(addr).unwrap();
}

#[test]
fn transport_open_unknown_segment_returns_not_found() {
    let transport = TestTransport::new("seg");
    let err = transport
        .open_segment("nonexistent")
        .expect_err("opening unknown segment must fail");
    assert!(matches!(err, StoreError::NotFound(_)));
}

#[test]
fn transport_close_unknown_handle_returns_not_found() {
    let transport = TestTransport::new("seg");
    let err = transport
        .close_segment(42)
        .expect_err("closing unknown handle must fail");
    assert!(matches!(err, StoreError::NotFound(_)));
}

#[test]
fn transport_get_segment_info_returns_correct_buffer_layout() {
    let transport = TestTransport::new("seg");
    let addr = transport.allocate_memory(256, "cpu:0").unwrap();
    let handle = transport.open_segment("seg").unwrap();
    let info = transport.get_segment_info(handle).unwrap();
    assert_eq!(info.buffers.len(), 1);
    assert_eq!(info.buffers[0].base, addr as u64);
    assert_eq!(info.buffers[0].length, 256);
    transport.free_memory(addr).unwrap();
}

#[test]
fn transport_peer_shares_segment_state() {
    let transport_a = TestTransport::new("seg-a");
    let _addr = transport_a.allocate_memory(128, "cpu:0").unwrap();

    let transport_b = transport_a.peer("seg-b");
    let handle = transport_b
        .open_segment("seg-a")
        .expect("peer should see seg-a");
    transport_b.close_segment(handle).unwrap();
}

#[test]
fn transport_factory_creates_independent_local_segments() {
    let transport = TestTransport::new("local");
    let factory = transport.factory();

    let t1 = factory.create("seg-x").unwrap();
    let t2 = factory.create("seg-y").unwrap();

    assert_eq!(t1.segment_name().unwrap(), "seg-x");
    assert_eq!(t2.segment_name().unwrap(), "seg-y");
}

#[test]
fn transport_submit_with_hints_records_pacing_group() {
    let transport = TestTransport::new("seg");
    let addr = transport.allocate_memory(64, "cpu:0").unwrap();
    let batch = transport.allocate_batch(1).unwrap();
    let req = TransferRequest {
        opcode: Opcode::Write,
        source: [0u8; 8].as_ptr() as *mut std::ffi::c_void,
        target_id: 1,
        target_offset: addr as u64,
        length: 8,
    };
    let hints = TransferBatchHints {
        pacing_group: Some("tenant/default".to_string()),
        mode: TransferPacingMode::LatencySensitive,
        max_inflight_bytes: Some(4096),
    };
    transport.submit_with_hints(batch, &[req], &hints).unwrap();
    transport.free_batch(batch).unwrap();

    let recorded = transport.submitted_batch_hints();
    assert_eq!(recorded.len(), 1);
    assert_eq!(recorded[0].0.as_deref(), Some("tenant/default"));
    assert_eq!(recorded[0].1, TransferPacingMode::LatencySensitive);
    assert_eq!(recorded[0].2, Some(4096));
    transport.free_memory(addr).unwrap();
}

#[test]
fn transport_republish_local_metadata_call_count() {
    let transport = TestTransport::new("seg");
    assert_eq!(transport.republish_local_metadata_calls(), 0);
    transport.republish_local_metadata().unwrap();
    transport.republish_local_metadata().unwrap();
    assert_eq!(transport.republish_local_metadata_calls(), 2);
}

#[test]
fn transport_fail_inject_clears_after_first_submit() {
    let transport = TestTransport::new("seg");
    let addr = transport.allocate_memory(64, "cpu:0").unwrap();
    let handle = transport.open_segment("seg").unwrap();

    transport.fail_next_submit_for_segment("seg");

    let batch = transport.allocate_batch(1).unwrap();
    let req = TransferRequest {
        opcode: Opcode::Write,
        source: [0u8; 8].as_ptr() as *mut std::ffi::c_void,
        target_id: handle,
        target_offset: addr as u64,
        length: 8,
    };
    let err = transport.submit(batch, &[req]).unwrap_err();
    assert!(matches!(err, StoreError::Transport(_)));

    // Re-inject and ensure second inject also works
    transport.fail_next_submit_for_segment("seg");
    let batch2 = transport.allocate_batch(1).unwrap();
    let err2 = transport.submit(batch2, &[req]).unwrap_err();
    assert!(matches!(err2, StoreError::Transport(_)));

    // No inject: should succeed
    let batch3 = transport.allocate_batch(1).unwrap();
    transport
        .submit(batch3, &[req])
        .expect("should succeed without injection");
    transport.free_batch(batch3).unwrap();
    transport.close_segment(handle).unwrap();
    transport.free_memory(addr).unwrap();
}

#[test]
fn transport_add_external_segment_is_openable_by_peer() {
    let transport = TestTransport::new("local");
    let handle = transport.add_external_segment("peer-seg", 256);
    let (base, len) = transport
        .segment_bounds("peer-seg")
        .expect("bounds should be available");
    assert_eq!(len, 256);
    assert!(base > 0);
    // A peer transport sharing the same state can open the external segment
    let peer = transport.peer("peer-local");
    let opened = peer.open_segment("peer-seg").unwrap();
    assert_eq!(opened, handle);
    peer.close_segment(opened).unwrap();
}

#[test]
fn transport_restart_external_segment_preserves_content() {
    let transport = TestTransport::new("local");
    let handle = transport.add_external_segment("restart-seg", 64);
    let (base, _) = transport.segment_bounds("restart-seg").unwrap();

    // Write a pattern
    let payload = [0xABu8; 8];
    let batch = transport.allocate_batch(1).unwrap();
    let req = TransferRequest {
        opcode: Opcode::Write,
        source: payload.as_ptr() as *mut std::ffi::c_void,
        target_id: handle,
        target_offset: base,
        length: 8,
    };
    transport.submit(batch, &[req]).unwrap();
    transport.free_batch(batch).unwrap();

    // Restart relocates the segment memory
    let new_handle = transport.restart_external_segment("restart-seg");
    let (new_base, _) = transport.segment_bounds("restart-seg").unwrap();

    // Read back: data should be preserved after restart
    let mut read_buf = [0u8; 8];
    let rbatch = transport.allocate_batch(1).unwrap();
    let rreq = TransferRequest {
        opcode: Opcode::Read,
        source: read_buf.as_mut_ptr() as *mut std::ffi::c_void,
        target_id: new_handle,
        target_offset: new_base,
        length: 8,
    };
    transport.submit(rbatch, &[rreq]).unwrap();
    transport.free_batch(rbatch).unwrap();
    assert_eq!(read_buf, payload);
}

#[test]
fn transport_rpc_server_address_returns_loopback() {
    let transport = TestTransport::new("seg");
    let (host, _port) = transport.rpc_server_address().unwrap();
    assert_eq!(host, "127.0.0.1");
}

#[test]
fn transport_max_registration_bytes_defaults_to_none() {
    let transport = TestTransport::new("seg");
    assert_eq!(transport.max_registration_bytes(), None);
    transport.set_max_registration_bytes(Some(1024));
    assert_eq!(transport.max_registration_bytes(), Some(1024));
    transport.set_max_registration_bytes(None);
    assert_eq!(transport.max_registration_bytes(), None);
}

#[test]
fn transport_unregister_wrong_size_returns_allocator_error() {
    let transport = TestTransport::new("seg");
    let addr = transport.allocate_memory(64, "cpu:0").unwrap();
    transport.register_memory(addr, 64).unwrap();
    let err = transport
        .unregister_memory(addr, 128)
        .expect_err("mismatched size should fail");
    assert!(matches!(err, StoreError::Allocator(_)));
    // Correct size succeeds
    transport.unregister_memory(addr, 64).unwrap();
    transport.free_memory(addr).unwrap();
}

#[test]
fn transport_free_unregistered_memory_returns_not_found() {
    let transport = TestTransport::new("seg");
    let err = transport
        .free_memory(42usize as *mut std::ffi::c_void)
        .expect_err("freeing unknown addr must fail");
    assert!(matches!(err, StoreError::NotFound(_)));
}

#[test]
fn transport_adopt_local_memory_registers_external_segment() {
    let mut buf = vec![0u8; 128];
    let addr = buf.as_mut_ptr() as *mut std::ffi::c_void;
    let transport = TestTransport::new("adopted-seg");
    transport
        .adopt_local_memory(addr, 128, "cpu:0")
        .expect("adopt should succeed");
    let handle = transport
        .open_segment("adopted-seg")
        .expect("adopted segment should be discoverable");
    transport.close_segment(handle).unwrap();
}

#[test]
fn transport_submitted_batch_sizes_tracks_request_count() {
    let transport = TestTransport::new("seg");
    let addr = transport.allocate_memory(256, "cpu:0").unwrap();

    let make_req = |offset: u64| TransferRequest {
        opcode: Opcode::Write,
        source: [0u8; 8].as_ptr() as *mut std::ffi::c_void,
        target_id: 1,
        target_offset: addr as u64 + offset,
        length: 8,
    };

    let b1 = transport.allocate_batch(1).unwrap();
    transport.submit(b1, &[make_req(0)]).unwrap();
    transport.free_batch(b1).unwrap();

    let b2 = transport.allocate_batch(2).unwrap();
    transport.submit(b2, &[make_req(8), make_req(16)]).unwrap();
    transport.free_batch(b2).unwrap();

    assert_eq!(transport.submitted_batch_sizes(), vec![1, 2]);
    transport.free_memory(addr).unwrap();
}

#[test]
fn transport_overall_status_returns_completed_for_live_batch() {
    let transport = TestTransport::new("seg");
    let _ = transport.allocate_memory(64, "cpu:0").unwrap();
    let batch = transport.allocate_batch(1).unwrap();
    let status = transport.overall_status(batch).unwrap();
    assert_eq!(status.status, mooncake_transport::TransferStatus::Completed);
    transport.free_batch(batch).unwrap();
}

#[test]
fn transport_overall_status_fails_for_freed_batch() {
    let transport = TestTransport::new("seg");
    let _ = transport.allocate_memory(64, "cpu:0").unwrap();
    let batch = transport.allocate_batch(1).unwrap();
    transport.free_batch(batch).unwrap();
    let err = transport
        .overall_status(batch)
        .expect_err("status of freed batch must fail");
    assert!(matches!(err, StoreError::NotFound(_)));
}
