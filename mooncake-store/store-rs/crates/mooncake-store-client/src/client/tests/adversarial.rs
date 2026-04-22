use std::sync::Arc;

use mooncake_metadata::InMemoryMetadataBackend;
use mooncake_store_core::{
    ClientEndpointSet, ClientEpoch, ClientLease, ClientLifecycleState, ClientRuntimeId,
    CompatibilityDescriptor, MetadataBackend,
};
use mooncake_store_test_utils::fixtures::now_ms;
use proptest::prelude::*;

// align_up_u64 is a private function in the parent client module; accessible
// here because this module is a descendant of client.
use super::super::align_up_u64;

// ---------------------------------------------------------------------------
// align_up_u64 property tests
// ---------------------------------------------------------------------------

proptest! {
    #![proptest_config(ProptestConfig::with_cases(512))]

    #[test]
    fn prop_align_up_result_is_multiple_of_alignment(
        value in 0u64..=1_000_000u64,
        log_align in 0u32..=20u32,
    ) {
        let alignment = 1u64 << log_align;
        let result = align_up_u64(value, alignment);
        prop_assert_eq!(result % alignment, 0u64);
    }

    #[test]
    fn prop_align_up_result_not_less_than_input(
        value in 0u64..=1_000_000u64,
        log_align in 0u32..=20u32,
    ) {
        let alignment = 1u64 << log_align;
        prop_assert!(align_up_u64(value, alignment) >= value);
    }

    #[test]
    fn prop_align_up_overshoot_less_than_one_alignment_unit(
        value in 0u64..=1_000_000u64,
        log_align in 0u32..=20u32,
    ) {
        let alignment = 1u64 << log_align;
        let result = align_up_u64(value, alignment);
        prop_assert!(result - value < alignment);
    }

    #[test]
    fn prop_align_up_is_idempotent(
        value in 0u64..=1_000_000u64,
        log_align in 0u32..=20u32,
    ) {
        let alignment = 1u64 << log_align;
        let once = align_up_u64(value, alignment);
        prop_assert_eq!(align_up_u64(once, alignment), once);
    }
}

// ---------------------------------------------------------------------------
// Epoch allocation properties (public MetadataBackend API)
// ---------------------------------------------------------------------------

fn make_template(stable_id: &str) -> ClientLease {
    ClientLease {
        runtime: ClientRuntimeId::new(stable_id, ClientEpoch(0)),
        state: ClientLifecycleState::Active,
        compatibility: CompatibilityDescriptor::default(),
        endpoints: ClientEndpointSet::default(),
        expires_at_ms: now_ms() + 30_000,
    }
}

#[test]
fn epoch_alloc_strictly_monotone_for_same_stable_id() {
    let meta = Arc::new(InMemoryMetadataBackend::new());
    let template = make_template("adv-epoch-mono");
    let mut prev = 0u64;
    for _ in 0..10u32 {
        let rid = meta
            .allocate_client_lease(&template)
            .expect("allocate_client_lease should succeed");
        assert!(
            rid.epoch.0 > prev,
            "epoch {} not strictly greater than prev {}",
            rid.epoch.0,
            prev
        );
        prev = rid.epoch.0;
    }
}

#[test]
fn epoch_alloc_independent_per_stable_id() {
    let meta = Arc::new(InMemoryMetadataBackend::new());
    let rid_a = meta
        .allocate_client_lease(&make_template("adv-stable-a"))
        .unwrap();
    let rid_b = meta
        .allocate_client_lease(&make_template("adv-stable-b"))
        .unwrap();
    assert_eq!(rid_a.epoch.0, 1, "first alloc for stable-a should be epoch 1");
    assert_eq!(rid_b.epoch.0, 1, "first alloc for stable-b should be epoch 1");
}

#[test]
fn counting_backend_tracks_allocate_calls() {
    use mooncake_store_test_utils::metadata::CountingMetadataBackend;
    use std::sync::atomic::Ordering;

    let inner = Arc::new(InMemoryMetadataBackend::new());
    let (backend, counts) = CountingMetadataBackend::wrap(inner);
    let template = make_template("adv-counting");

    for i in 1..=5u64 {
        backend
            .allocate_client_lease(&template)
            .expect("allocate should succeed");
        assert_eq!(
            counts.allocate_client_lease.load(Ordering::Relaxed),
            i,
            "allocate counter should be {i} after {i} calls"
        );
    }
}

#[test]
fn faulty_backend_fails_after_threshold() {
    use mooncake_store_test_utils::metadata::FaultyMetadataBackend;

    let inner = Arc::new(InMemoryMetadataBackend::new());
    let faulty = FaultyMetadataBackend::wrap(inner, 3);
    let template = make_template("adv-faulty");

    // First 3 calls succeed (indices 0, 1, 2 < threshold 3)
    for _ in 0..3 {
        assert!(
            faulty.allocate_client_lease(&template).is_ok(),
            "calls before threshold should succeed"
        );
    }
    // Subsequent calls fail
    assert!(
        faulty.allocate_client_lease(&template).is_err(),
        "call at threshold should fail"
    );
    assert!(
        faulty.allocate_client_lease(&template).is_err(),
        "calls after threshold should fail"
    );
}
