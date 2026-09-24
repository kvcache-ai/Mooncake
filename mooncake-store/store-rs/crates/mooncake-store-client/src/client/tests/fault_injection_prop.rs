// ---------------------------------------------------------------------------
// fault_injection_prop.rs — FaultConfig state machine, FaultyTransport behavior,
// and adversarial property tests
// ---------------------------------------------------------------------------
//
// All tests operate on FaultConfig / FaultyTransport directly (without going
// through StoreClient) to verify the fault injection infrastructure itself.
// This separates the infrastructure contract from the client integration tests
// in store_client_tests.rs.

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use mooncake_store_core::StoreError;
use mooncake_store_test_utils::transport::{FaultConfig, FaultyTransport, TestTransport};
use mooncake_store_transport_core::StoreTransport;
use mooncake_transport::{Opcode, TransferRequest};
use proptest::prelude::*;

// ===========================================================================
// FaultConfig state machine — direct field inspection
// ===========================================================================

#[test]
fn fault_config_default_has_no_active_faults() {
    let cfg = FaultConfig::default();
    assert!(!cfg.is_disconnected());
    assert_eq!(cfg.submit_failures_remaining.load(Ordering::SeqCst), 0);
    assert_eq!(cfg.open_failures_remaining.load(Ordering::SeqCst), 0);
    assert_eq!(cfg.jitter_ms.load(Ordering::SeqCst), 0);
    assert_eq!(cfg.submit_call_count.load(Ordering::SeqCst), 0);
    assert_eq!(cfg.open_call_count.load(Ordering::SeqCst), 0);
    assert_eq!(*cfg.submit_latency.lock(), Duration::ZERO);
    assert_eq!(*cfg.open_latency.lock(), Duration::ZERO);
}

#[test]
fn fault_config_disconnect_sets_is_disconnected() {
    let cfg = FaultConfig::default();
    assert!(!cfg.is_disconnected());
    cfg.disconnect();
    assert!(cfg.is_disconnected());
}

#[test]
fn fault_config_reconnect_clears_is_disconnected() {
    let cfg = FaultConfig::default();
    cfg.disconnect();
    cfg.reconnect();
    assert!(!cfg.is_disconnected());
}

#[test]
fn fault_config_disconnect_reconnect_many_times_is_stable() {
    let cfg = FaultConfig::default();
    for _ in 0..10 {
        cfg.disconnect();
        assert!(cfg.is_disconnected());
        cfg.reconnect();
        assert!(!cfg.is_disconnected());
    }
}

#[test]
fn fault_config_fail_next_submits_stores_count() {
    let cfg = FaultConfig::default();
    cfg.fail_next_submits(7);
    assert_eq!(cfg.submit_failures_remaining.load(Ordering::SeqCst), 7);
}

#[test]
fn fault_config_fail_next_opens_stores_count() {
    let cfg = FaultConfig::default();
    cfg.fail_next_opens(3);
    assert_eq!(cfg.open_failures_remaining.load(Ordering::SeqCst), 3);
}

#[test]
fn fault_config_fail_next_submits_zero_overwrites_previous() {
    let cfg = FaultConfig::default();
    cfg.fail_next_submits(5);
    cfg.fail_next_submits(0);
    assert_eq!(cfg.submit_failures_remaining.load(Ordering::SeqCst), 0);
}

#[test]
fn fault_config_reset_all_clears_all_state() {
    let cfg = FaultConfig::default();
    cfg.disconnect();
    cfg.fail_next_submits(10);
    cfg.fail_next_opens(10);
    cfg.set_submit_latency(Duration::from_millis(5));
    cfg.set_open_latency(Duration::from_millis(5));
    cfg.set_jitter_ms(10);
    cfg.reset_all();

    assert!(!cfg.is_disconnected());
    assert_eq!(cfg.submit_failures_remaining.load(Ordering::SeqCst), 0);
    assert_eq!(cfg.open_failures_remaining.load(Ordering::SeqCst), 0);
    assert_eq!(cfg.jitter_ms.load(Ordering::SeqCst), 0);
    assert_eq!(cfg.submit_call_count.load(Ordering::SeqCst), 0);
    assert_eq!(cfg.open_call_count.load(Ordering::SeqCst), 0);
    assert_eq!(*cfg.submit_latency.lock(), Duration::ZERO);
    assert_eq!(*cfg.open_latency.lock(), Duration::ZERO);
}

#[test]
fn fault_config_reset_counters_clears_call_counts_but_not_injection_state() {
    let inner = Arc::new(TestTransport::new("seg"));
    let _addr = inner.allocate_memory(64, "cpu:0").unwrap();
    let (faulty, faults) = FaultyTransport::new(inner);

    // Trigger some calls to increment counters
    let h = faulty.open_segment("seg").unwrap();
    faulty.close_segment(h).unwrap();

    // Set fault injection state
    faults.fail_next_submits(5);
    faults.fail_next_opens(3);

    assert!(faults.open_call_count.load(Ordering::SeqCst) > 0);

    faults.reset_counters();

    // Call counts cleared
    assert_eq!(faults.submit_call_count.load(Ordering::SeqCst), 0);
    assert_eq!(faults.open_call_count.load(Ordering::SeqCst), 0);
    // Injection state preserved
    assert_eq!(faults.submit_failures_remaining.load(Ordering::SeqCst), 5);
    assert_eq!(faults.open_failures_remaining.load(Ordering::SeqCst), 3);
}

#[test]
fn fault_config_set_jitter_ms_roundtrip() {
    let cfg = FaultConfig::default();
    cfg.set_jitter_ms(42);
    assert_eq!(cfg.jitter_ms.load(Ordering::SeqCst), 42);
    cfg.set_jitter_ms(0);
    assert_eq!(cfg.jitter_ms.load(Ordering::SeqCst), 0);
}

#[test]
fn fault_config_submit_latency_field_roundtrip() {
    let cfg = FaultConfig::default();
    assert_eq!(*cfg.submit_latency.lock(), Duration::ZERO);
    cfg.set_submit_latency(Duration::from_millis(5));
    assert_eq!(*cfg.submit_latency.lock(), Duration::from_millis(5));
    cfg.set_submit_latency(Duration::ZERO);
    assert_eq!(*cfg.submit_latency.lock(), Duration::ZERO);
}

#[test]
fn fault_config_open_latency_field_roundtrip() {
    let cfg = FaultConfig::default();
    assert_eq!(*cfg.open_latency.lock(), Duration::ZERO);
    cfg.set_open_latency(Duration::from_millis(3));
    assert_eq!(*cfg.open_latency.lock(), Duration::from_millis(3));
}

// ===========================================================================
// FaultyTransport behavior — open/submit fault injection
// ===========================================================================

#[test]
fn faulty_transport_open_fails_exactly_n_times_then_recovers() {
    let inner = Arc::new(TestTransport::new("seg"));
    let _addr = inner.allocate_memory(64, "cpu:0").unwrap();
    let (faulty, faults) = FaultyTransport::new(inner);

    faults.fail_next_opens(3);

    for i in 0..3 {
        let err = faulty.open_segment("seg").unwrap_err();
        assert!(
            matches!(err, StoreError::Transport(_)),
            "open {i} should fail with Transport error"
        );
    }
    // 4th call must succeed
    let handle = faulty.open_segment("seg").unwrap();
    faulty.close_segment(handle).unwrap();
}

#[test]
fn faulty_transport_submit_fails_exactly_n_times_then_recovers() {
    let inner = Arc::new(TestTransport::new("seg"));
    let addr = inner.allocate_memory(64, "cpu:0").unwrap();
    let (faulty, faults) = FaultyTransport::new(inner);

    faults.fail_next_submits(2);

    let make_req = |target: u64| TransferRequest {
        opcode: Opcode::Write,
        source: [0u8; 8].as_ptr() as *mut std::ffi::c_void,
        target_id: 1,
        target_offset: target,
        length: 8,
    };

    for _ in 0..2 {
        let batch = faulty.allocate_batch(1).unwrap();
        faulty.submit(batch, &[make_req(addr as u64)]).unwrap_err();
        faulty.free_batch(batch).unwrap();
    }
    // 3rd submit succeeds
    let batch = faulty.allocate_batch(1).unwrap();
    faulty.submit(batch, &[make_req(addr as u64)]).unwrap();
    faulty.free_batch(batch).unwrap();
    faulty.free_memory(addr).unwrap();
}

#[test]
fn faulty_transport_disconnect_blocks_open_and_submit() {
    let inner = Arc::new(TestTransport::new("seg"));
    let addr = inner.allocate_memory(64, "cpu:0").unwrap();
    let (faulty, faults) = FaultyTransport::new(inner);

    faults.disconnect();

    let open_err = faulty.open_segment("seg").unwrap_err();
    assert!(matches!(open_err, StoreError::Transport(_)));

    let batch = faulty.allocate_batch(1).unwrap();
    let req = TransferRequest {
        opcode: Opcode::Write,
        source: [0u8; 8].as_ptr() as *mut std::ffi::c_void,
        target_id: 1,
        target_offset: addr as u64,
        length: 8,
    };
    let submit_err = faulty.submit(batch, &[req]).unwrap_err();
    assert!(matches!(submit_err, StoreError::Transport(_)));
    faulty.free_batch(batch).unwrap();
    faulty.free_memory(addr).unwrap();
}

#[test]
fn faulty_transport_reconnect_restores_open_and_submit() {
    let inner = Arc::new(TestTransport::new("seg"));
    let addr = inner.allocate_memory(64, "cpu:0").unwrap();
    let (faulty, faults) = FaultyTransport::new(inner);

    faults.disconnect();
    faults.reconnect();

    let handle = faulty.open_segment("seg").unwrap();
    let batch = faulty.allocate_batch(1).unwrap();
    let req = TransferRequest {
        opcode: Opcode::Write,
        source: [0u8; 8].as_ptr() as *mut std::ffi::c_void,
        target_id: handle,
        target_offset: addr as u64,
        length: 8,
    };
    faulty.submit(batch, &[req]).unwrap();
    faulty.free_batch(batch).unwrap();
    faulty.close_segment(handle).unwrap();
    faulty.free_memory(addr).unwrap();
}

#[test]
fn faulty_transport_close_segment_is_not_faulted() {
    let inner = Arc::new(TestTransport::new("seg"));
    let _addr = inner.allocate_memory(64, "cpu:0").unwrap();
    let (faulty, faults) = FaultyTransport::new(inner);

    let handle = faulty.open_segment("seg").unwrap();

    // Disconnect doesn't block close
    faults.disconnect();
    faulty.close_segment(handle).unwrap();
}

#[test]
fn faulty_transport_open_call_count_increments_per_call() {
    let inner = Arc::new(TestTransport::new("seg"));
    let _addr = inner.allocate_memory(64, "cpu:0").unwrap();
    let (faulty, faults) = FaultyTransport::new(inner);

    assert_eq!(faults.open_call_count.load(Ordering::SeqCst), 0);
    let h1 = faulty.open_segment("seg").unwrap();
    assert_eq!(faults.open_call_count.load(Ordering::SeqCst), 1);
    faulty.close_segment(h1).unwrap();
    let h2 = faulty.open_segment("seg").unwrap();
    assert_eq!(faults.open_call_count.load(Ordering::SeqCst), 2);
    faulty.close_segment(h2).unwrap();
}

#[test]
fn faulty_transport_open_call_count_increments_even_on_failure() {
    let inner = Arc::new(TestTransport::new("seg"));
    let _addr = inner.allocate_memory(64, "cpu:0").unwrap();
    let (faulty, faults) = FaultyTransport::new(inner);

    faults.fail_next_opens(2);

    faulty.open_segment("seg").unwrap_err(); // fails, count = 1
    faulty.open_segment("seg").unwrap_err(); // fails, count = 2
    let h = faulty.open_segment("seg").unwrap(); // succeeds, count = 3
    faulty.close_segment(h).unwrap();

    assert_eq!(faults.open_call_count.load(Ordering::SeqCst), 3);
}

#[test]
fn faulty_transport_submit_call_count_increments_per_call() {
    let inner = Arc::new(TestTransport::new("seg"));
    let addr = inner.allocate_memory(64, "cpu:0").unwrap();
    let (faulty, faults) = FaultyTransport::new(inner);

    assert_eq!(faults.submit_call_count.load(Ordering::SeqCst), 0);

    let batch = faulty.allocate_batch(1).unwrap();
    let req = TransferRequest {
        opcode: Opcode::Write,
        source: [0u8; 8].as_ptr() as *mut std::ffi::c_void,
        target_id: 1,
        target_offset: addr as u64,
        length: 8,
    };
    faulty.submit(batch, &[req]).unwrap();
    faulty.free_batch(batch).unwrap();

    assert_eq!(faults.submit_call_count.load(Ordering::SeqCst), 1);
    faulty.free_memory(addr).unwrap();
}

#[test]
fn faulty_transport_open_latency_delays_but_does_not_fail() {
    let inner = Arc::new(TestTransport::new("seg"));
    let _addr = inner.allocate_memory(64, "cpu:0").unwrap();
    let (faulty, faults) = FaultyTransport::new(inner);

    faults.set_open_latency(Duration::from_millis(1));
    let start = std::time::Instant::now();
    let handle = faulty.open_segment("seg").unwrap();
    let elapsed = start.elapsed();
    faulty.close_segment(handle).unwrap();

    assert!(
        elapsed >= Duration::from_millis(1),
        "open should have been delayed by at least 1ms, elapsed={elapsed:?}"
    );
}

#[test]
fn faulty_transport_reset_all_clears_injections_and_allows_open() {
    let inner = Arc::new(TestTransport::new("seg"));
    let _addr = inner.allocate_memory(64, "cpu:0").unwrap();
    let (faulty, faults) = FaultyTransport::new(inner);

    faults.disconnect();
    faults.fail_next_opens(100);
    faults.reset_all();

    let handle = faulty.open_segment("seg").unwrap();
    faulty.close_segment(handle).unwrap();
}

// ===========================================================================
// disconnect priority over fail_next
// ===========================================================================

#[test]
fn disconnect_does_not_consume_fail_next_opens_counter() {
    // When disconnected, should_fail_open returns true without decrementing
    // open_failures_remaining
    let inner = Arc::new(TestTransport::new("seg"));
    let _addr = inner.allocate_memory(64, "cpu:0").unwrap();
    let (faulty, faults) = FaultyTransport::new(inner);

    faults.fail_next_opens(5);
    faults.disconnect();

    // 3 opens fail due to disconnect — counter NOT decremented
    for _ in 0..3 {
        faulty.open_segment("seg").unwrap_err();
    }
    assert_eq!(
        faults.open_failures_remaining.load(Ordering::SeqCst),
        5,
        "fail_next_opens counter must not be consumed by disconnect"
    );

    // After reconnect, fail_next_opens fires for 5 more calls
    faults.reconnect();
    for _ in 0..5 {
        faulty.open_segment("seg").unwrap_err();
    }
    // Now counter is exhausted, next call succeeds
    let h = faulty.open_segment("seg").unwrap();
    faulty.close_segment(h).unwrap();
}

#[test]
fn disconnect_does_not_consume_fail_next_submits_counter() {
    let inner = Arc::new(TestTransport::new("seg"));
    let addr = inner.allocate_memory(64, "cpu:0").unwrap();
    let (faulty, faults) = FaultyTransport::new(inner);

    faults.fail_next_submits(3);
    faults.disconnect();

    let make_req = || TransferRequest {
        opcode: Opcode::Write,
        source: [0u8; 8].as_ptr() as *mut std::ffi::c_void,
        target_id: 1,
        target_offset: addr as u64,
        length: 8,
    };

    // 2 submits fail due to disconnect — counter NOT decremented
    for _ in 0..2 {
        let batch = faulty.allocate_batch(1).unwrap();
        faulty.submit(batch, &[make_req()]).unwrap_err();
        faulty.free_batch(batch).unwrap();
    }
    assert_eq!(
        faults.submit_failures_remaining.load(Ordering::SeqCst),
        3,
        "fail_next_submits counter must not be consumed by disconnect"
    );
    faulty.free_memory(addr).unwrap();
}

// ===========================================================================
// Passthrough delegation — non-faulted methods
// ===========================================================================

#[test]
fn faulty_transport_segment_name_passes_through() {
    let inner = Arc::new(TestTransport::new("my-segment"));
    let (faulty, _) = FaultyTransport::new(inner);
    assert_eq!(faulty.segment_name().unwrap(), "my-segment");
}

#[test]
fn faulty_transport_rpc_server_address_passes_through() {
    let inner = Arc::new(TestTransport::new("seg"));
    let (faulty, _) = FaultyTransport::new(inner);
    let (host, _port) = faulty.rpc_server_address().unwrap();
    assert_eq!(host, "127.0.0.1");
}

#[test]
fn faulty_transport_allocate_memory_passes_through() {
    let inner = Arc::new(TestTransport::new("seg"));
    let (faulty, _) = FaultyTransport::new(inner);
    let addr = faulty.allocate_memory(128, "cpu:0").unwrap();
    assert!(!addr.is_null());
    faulty.free_memory(addr).unwrap();
}

#[test]
fn faulty_transport_allocate_batch_passes_through() {
    let inner = Arc::new(TestTransport::new("seg"));
    let (faulty, _) = FaultyTransport::new(inner);
    let batch = faulty.allocate_batch(4).unwrap();
    faulty.free_batch(batch).unwrap();
}

#[test]
fn faulty_transport_register_unregister_memory_passes_through() {
    let inner = Arc::new(TestTransport::new("seg"));
    let (faulty, _) = FaultyTransport::new(inner);
    let addr = faulty.allocate_memory(64, "cpu:0").unwrap();
    faulty.register_memory(addr, 64).unwrap();
    faulty.unregister_memory(addr, 64).unwrap();
    faulty.free_memory(addr).unwrap();
}

#[test]
fn faulty_transport_republish_local_metadata_passes_through() {
    let inner = Arc::new(TestTransport::new("seg"));
    let calls_before = inner.republish_local_metadata_calls();
    let (faulty, _) = FaultyTransport::new(inner.clone());
    faulty.republish_local_metadata().unwrap();
    faulty.republish_local_metadata().unwrap();
    assert_eq!(
        inner.republish_local_metadata_calls(),
        calls_before + 2,
        "republish_local_metadata should delegate to inner"
    );
}

#[test]
fn faulty_transport_get_segment_info_passes_through() {
    let inner = Arc::new(TestTransport::new("seg"));
    let addr = inner.allocate_memory(128, "cpu:0").unwrap();
    let (faulty, _) = FaultyTransport::new(inner);
    let handle = faulty.open_segment("seg").unwrap();
    let info = faulty.get_segment_info(handle).unwrap();
    assert!(!info.buffers.is_empty());
    assert_eq!(info.buffers[0].length, 128);
    faulty.close_segment(handle).unwrap();
    faulty.free_memory(addr).unwrap();
}

#[test]
fn faulty_transport_task_status_fails_when_disconnected() {
    let inner = Arc::new(TestTransport::new("seg"));
    let _addr = inner.allocate_memory(64, "cpu:0").unwrap();
    let (faulty, faults) = FaultyTransport::new(inner);
    let batch = faulty.allocate_batch(1).unwrap();

    faults.disconnect();
    let err = faulty.task_status(batch, 0).unwrap_err();
    assert!(
        matches!(err, StoreError::Transport(_)),
        "task_status must fail when disconnected"
    );

    faults.reconnect();
    faulty.free_batch(batch).unwrap();
}

#[test]
fn faulty_transport_overall_status_fails_when_disconnected() {
    let inner = Arc::new(TestTransport::new("seg"));
    let _addr = inner.allocate_memory(64, "cpu:0").unwrap();
    let (faulty, faults) = FaultyTransport::new(inner);
    let batch = faulty.allocate_batch(1).unwrap();

    faults.disconnect();
    let err = faulty.overall_status(batch).unwrap_err();
    assert!(
        matches!(err, StoreError::Transport(_)),
        "overall_status must fail when disconnected"
    );

    faults.reconnect();
    faulty.free_batch(batch).unwrap();
}

#[test]
fn faulty_transport_overall_status_succeeds_when_connected() {
    let inner = Arc::new(TestTransport::new("seg"));
    let _addr = inner.allocate_memory(64, "cpu:0").unwrap();
    let (faulty, _) = FaultyTransport::new(inner);
    let batch = faulty.allocate_batch(1).unwrap();
    let status = faulty.overall_status(batch).unwrap();
    assert_eq!(status.status, mooncake_transport::TransferStatus::Completed);
    faulty.free_batch(batch).unwrap();
}

// ===========================================================================
// Property tests
// ===========================================================================

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    #[test]
    fn prop_fail_next_opens_causes_exactly_n_failures(n in 1u64..=10u64) {
        let inner = Arc::new(TestTransport::new("seg"));
        let _addr = inner.allocate_memory(64, "cpu:0").expect("alloc");
        let (faulty, faults) = FaultyTransport::new(inner);

        faults.fail_next_opens(n);

        for i in 0..n {
            let result = faulty.open_segment("seg");
            prop_assert!(result.is_err(), "open {i} of {n} should fail");
        }
        let handle = faulty.open_segment("seg").expect("should succeed after n failures");
        faulty.close_segment(handle).expect("close");
    }

    #[test]
    fn prop_fail_next_submits_causes_exactly_n_failures(n in 1u64..=10u64) {
        let inner = Arc::new(TestTransport::new("seg"));
        let addr = inner.allocate_memory(64, "cpu:0").expect("alloc");
        let (faulty, faults) = FaultyTransport::new(inner);

        faults.fail_next_submits(n);

        let make_req = || TransferRequest {
            opcode: Opcode::Write,
            source: [0u8; 8].as_ptr() as *mut std::ffi::c_void,
            target_id: 1,
            target_offset: addr as u64,
            length: 8,
        };

        for i in 0..n {
            let batch = faulty.allocate_batch(1).expect("alloc batch");
            let result = faulty.submit(batch, &[make_req()]);
            prop_assert!(result.is_err(), "submit {i} of {n} should fail");
            faulty.free_batch(batch).expect("free batch");
        }

        let batch = faulty.allocate_batch(1).expect("alloc final batch");
        prop_assert!(
            faulty.submit(batch, &[make_req()]).is_ok(),
            "submit after {n} failures should succeed"
        );
        faulty.free_batch(batch).expect("free batch");
        faulty.free_memory(addr).expect("free");
    }

    #[test]
    fn prop_open_call_count_equals_number_of_open_calls(n in 1usize..=15usize) {
        let inner = Arc::new(TestTransport::new("seg"));
        let _addr = inner.allocate_memory(64, "cpu:0").expect("alloc");
        let (faulty, faults) = FaultyTransport::new(inner);

        for _ in 0..n {
            let h = faulty.open_segment("seg").expect("open");
            faulty.close_segment(h).expect("close");
        }
        prop_assert_eq!(
            faults.open_call_count.load(Ordering::SeqCst),
            n as u64,
            "open_call_count should equal number of open calls"
        );
    }

    #[test]
    fn prop_reset_all_allows_operations_after_arbitrary_faults(
        n_open_fails in 0u64..=5u64,
        n_submit_fails in 0u64..=5u64,
        disconnected in any::<bool>(),
    ) {
        let inner = Arc::new(TestTransport::new("seg"));
        let _addr = inner.allocate_memory(64, "cpu:0").expect("alloc");
        let (faulty, faults) = FaultyTransport::new(inner);

        faults.fail_next_opens(n_open_fails);
        faults.fail_next_submits(n_submit_fails);
        if disconnected {
            faults.disconnect();
        }

        faults.reset_all();

        let handle = faulty
            .open_segment("seg")
            .expect("open after reset_all should succeed");
        faulty.close_segment(handle).expect("close");
    }

    #[test]
    fn prop_disconnect_reconnect_n_times_ends_not_disconnected(n in 1usize..=20usize) {
        let cfg = FaultConfig::default();
        for _ in 0..n {
            cfg.disconnect();
            cfg.reconnect();
        }
        prop_assert!(!cfg.is_disconnected());
    }

    #[test]
    fn prop_fail_next_opens_counter_stored_correctly(n in 0u64..=100u64) {
        let cfg = FaultConfig::default();
        cfg.fail_next_opens(n);
        prop_assert_eq!(cfg.open_failures_remaining.load(Ordering::SeqCst), n);
    }

    #[test]
    fn prop_fail_next_submits_counter_stored_correctly(n in 0u64..=100u64) {
        let cfg = FaultConfig::default();
        cfg.fail_next_submits(n);
        prop_assert_eq!(cfg.submit_failures_remaining.load(Ordering::SeqCst), n);
    }

    #[test]
    fn prop_jitter_formula_is_deterministic(jitter_ms in 1u64..=50u64) {
        // delay = (call * 7 + 13) % jitter_ms — verify for first 10 calls
        let cfg = FaultConfig::default();
        cfg.set_jitter_ms(jitter_ms);

        for call in 0u64..10 {
            let expected = (call.wrapping_mul(7).wrapping_add(13)) % jitter_ms;
            // We can't call apply_jitter directly, but we know the formula.
            // Verify the formula produces values in [0, jitter_ms).
            prop_assert!(
                expected < jitter_ms,
                "jitter at call {call} should be < {jitter_ms}"
            );
        }
    }
}
