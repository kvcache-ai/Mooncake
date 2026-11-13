use super::full_lifecycle_cold_probe::{cold_probe_client, cold_probe_owner, cold_probe_prepare};
use super::*;
use crate::control_plane::{ColdReadResult, ColdTierControlService};
use mooncake_store_rs_transport::TransferRequest;

#[test]
fn late_staging_ack_preserves_the_next_readers_payload() {
    let _cold_guard = enable_cold_tier_for_test();
    let client = cold_probe_client("full-lifecycle-staging-ack");
    let mut owner = cold_probe_owner(&client, client.route_directory.clone());
    let config = Arc::get_mut(&mut owner).expect("owner is not shared before startup");
    config.staging_pool_bytes = 4096;
    config.staging_slot_ttl = Duration::from_millis(500);
    for (key, value) in [("a", [1_u8; 16]), ("b", [2_u8; 16]), ("c", [3_u8; 16])] {
        let hot = client.put(key, &value).expect("public put");
        let prepared = cold_probe_prepare(&owner, &hot);
        assert_eq!(
            owner
                .materialize_prepared_pending_offload_batch(&prepared, &mut None)
                .expect("offload"),
            1
        );
        drop(prepared);
        force_cold_only_route(&client, key);
    }
    let adapter = LocalAllocatorAdapter {
        runtime: client.runtime_id().clone(),
        allocator: client.allocator.clone(),
        storage_owner: owner.clone(),
        lifecycle_state: test_lifecycle_state(ClientLifecycleState::Active),
        transfer_stall_timeout: Duration::from_secs(1),
        request_timeout_override: None,
        allocation_lifecycle: AllocationLifecycleConfig::default(),
    };
    let read = |key: &str| -> ColdReadResult {
        let response = adapter.read_from_cold(
            &owner.metadata.route_namespace(),
            &client.runtime_id().stable_id.0,
            "default",
            key,
            "",
            "",
        );
        let reply = response
            .result
            .expect("cold read should return staging coordinates");
        response
            .deferred_promote
            .expect("promotion should run after sending reply")();
        reply
    };
    let first = read("a");
    sleep(Duration::from_millis(550));
    assert_eq!(owner.sweep_expired_staging_slots(owner.staging_slot_ttl), 1);
    let second_started = Instant::now();
    let second = read("b");
    assert_eq!(second.segment_offset, first.segment_offset);
    adapter
        .ack_cold_read_complete(&[(SegmentName::new(&first.segment_name), first.segment_offset)]);
    let (received, third) = std::thread::scope(|scope| {
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        let third_reader = scope.spawn(move || {
            let reply = read("c");
            done_tx.send(()).expect("signal third reader ready");
            reply
        });
        // A correct implementation keeps C blocked until B's own ACK. On the
        // original head the stale ACK lets C finish and overwrite B's payload.
        let _ = done_rx.recv_timeout(Duration::from_millis(50));
        assert!(
            second_started.elapsed() < owner.staging_slot_ttl,
            "the second reader must still be within its lease"
        );
        let transport = client.transport().expect("transport");
        let handle = transport
            .open_segment(second.transport_endpoint.as_deref().expect("endpoint"))
            .expect("open staging transport");
        let chunk = &second.target_chunks[0];
        let mut received = [0_u8; 16];
        let batch = transport.allocate_batch(1).expect("batch");
        let result = transport.submit(
            batch,
            &[TransferRequest {
                opcode: Opcode::Read,
                source: received.as_mut_ptr().cast(),
                target_id: handle,
                target_offset: chunk.target_offset + second.segment_offset - chunk.logical_offset,
                length: second.length,
            }],
        );
        transport.free_batch(batch).expect("free batch");
        transport
            .close_segment(handle)
            .expect("close staging transport");
        adapter.ack_cold_read_complete(&[(
            SegmentName::new(&second.segment_name),
            second.segment_offset,
        )]);
        result.expect("reader transport should complete");
        (
            received,
            third_reader
                .join()
                .expect("third reader should finish after B ACK"),
        )
    });
    adapter
        .ack_cold_read_complete(&[(SegmentName::new(&third.segment_name), third.segment_offset)]);
    assert_eq!(
        received, [2_u8; 16],
        "an expired reader's delayed ACK must not release a fresh reader's slot"
    );
}
