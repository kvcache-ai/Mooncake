use std::sync::Arc;
use std::thread::sleep;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use mooncake_store_client::{
    snapshot_metrics, LocalMemoryConfig, MooncakeCompatibilityFacade, StoreClientBuilder,
};
use mooncake_store_core::ClientLifecycleState;
use mooncake_store_rs_metadata::InMemoryMetadataBackend;
use mooncake_store_test_utils::transport::TestTransport;

fn future_expiry_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock should follow the Unix epoch")
        .as_millis() as u64
        + 60_000
}

fn tracked_replica_bytes() -> u64 {
    snapshot_metrics()
        .request_bytes
        .iter()
        .filter(|sample| sample.key.operation == "storage_owner_track_replica_routes")
        .map(|sample| sample.value)
        .sum()
}

#[test]
fn deleting_writer_drop_cannot_release_a_reused_remote_allocation() {
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let transport = Arc::new(TestTransport::new("reclaim-generation-owner-segment"));
    let owner = StoreClientBuilder::new(metadata.clone(), "reclaim-generation-owner")
        .state(ClientLifecycleState::Active)
        .label("pool", "reclaim-generation")
        .label("storage", "true")
        .segment_name("reclaim-generation-owner-segment")
        .live_client_sync_interval(Duration::from_millis(10))
        .transport(transport.clone())
        .local_memory(
            LocalMemoryConfig::new()
                .numa_aware(false)
                .storage_bytes(32)
                .scratch_bytes(4096)
                .alignment(1)
                .reclaim_grace_ms(0)
                .eviction_poll_interval(Duration::ZERO),
        )
        .build(future_expiry_ms())
        .expect("owner should build");
    owner
        .register_local_memory()
        .expect("owner memory should register");
    let writer = StoreClientBuilder::new(metadata, "reclaim-generation-writer")
        .state(ClientLifecycleState::Active)
        .label("pool", "reclaim-generation")
        .label("storage", "false")
        .live_client_sync_interval(Duration::from_millis(10))
        .transport(Arc::new(
            transport.peer("reclaim-generation-writer-segment"),
        ))
        .local_memory(
            LocalMemoryConfig::new()
                .numa_aware(false)
                .storage_bytes(0)
                .scratch_bytes(4096)
                .alignment(1)
                .reclaim_grace_ms(60_000)
                .eviction_poll_interval(Duration::ZERO),
        )
        .build(future_expiry_ms())
        .expect("writer should build");

    let tracking_before = tracked_replica_bytes();
    let old = writer
        .put("retired", b"0123456789abcdef")
        .expect("writer should fill half the owner capacity");
    let tracking_deadline = Instant::now() + Duration::from_secs(5);
    while tracked_replica_bytes() < tracking_before + 16 {
        assert!(
            Instant::now() < tracking_deadline,
            "the owner should finish tracking the remote write"
        );
        sleep(Duration::from_millis(5));
    }
    let anchor = owner
        .put("anchor", b"fedcba9876543210")
        .expect("owner should fill its remaining capacity");
    assert_eq!(old.replicas[0].owner, anchor.replicas[0].owner);

    writer
        .remove("retired", true)
        .expect("logical delete should succeed before delayed physical reclaim");
    let replacement = owner
        .put("replacement", b"abcdefghijklmnop")
        .expect("allocation pressure should reclaim or evict an owner allocation");

    // Drop flushes the old writer's delayed reclaims. It must not release a
    // different live allocation that now occupies the same physical span.
    drop(writer);
    owner
        .put("after-writer-exit", b"ABCDEFGHIJKLMNOP")
        .expect("the next write should preserve all routes that survive eviction");

    for (key, expected) in [
        ("anchor", b"fedcba9876543210".as_slice()),
        ("replacement", b"abcdefghijklmnop".as_slice()),
    ] {
        if owner
            .query_route(key)
            .expect("route query should succeed")
            .is_some()
        {
            assert_eq!(
                owner
                    .get(key)
                    .expect("a published surviving route must remain readable"),
                expected,
                "a stale delete reclaim must not overwrite {key}"
            );
        }
    }
    assert_eq!(
        owner
            .get("after-writer-exit")
            .expect("new payload should be readable"),
        b"ABCDEFGHIJKLMNOP"
    );
    assert_eq!(replacement.replicas[0].owner, old.replicas[0].owner);
}
