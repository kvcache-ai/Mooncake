use super::*;

#[test]
fn full_lifecycle_cold_offload_physical_nof_bypasses_local_accounting() {
    let _cold_guard = enable_cold_tier_for_test();
    let (client, primary, replica) = physical_nof_client("offload-nof-accounting-boundary");
    let payload = [43u8; 150];
    client.put("physical-nof-boundary", &payload).expect("put");
    assert_eq!(
        client
            .storage_owner
            .materialize_pending_offloads_bounded(32)
            .expect("physical NoF materialization"),
        1
    );
    assert!(!primary.records.lock().is_empty());
    assert!(!replica.records.lock().is_empty());
    assert!(client
        .storage_owner
        .pending_offloads
        .unconfirmed
        .lock()
        .is_empty());
    assert!(client
        .metadata
        .list_cold_tier_devices(&mooncake_store_core::ColdTierDeviceFilter::default())
        .expect("local cold devices")
        .is_empty());
    force_cold_only_route(&client, "physical-nof-boundary");
    assert_eq!(
        client.get("physical-nof-boundary").expect("NoF read"),
        payload
    );
}

#[test]
fn full_lifecycle_cold_offload_managed_nof_bypasses_local_accounting() {
    let _cold_guard = enable_cold_tier_for_test();
    let backend = Arc::new(FakeManagedNof::default());
    let target = NofTargetConfig::new(
        "managed-nof-boundary",
        NofBackend::new(backend.clone()).expect("managed NoF backend"),
    )
    .expect("managed NoF target");
    let client = StoreClientBuilder::new(
        Arc::new(InMemoryMetadataBackend::new()),
        "offload-managed-nof-boundary",
    )
    .state(ClientLifecycleState::Active)
    .label("storage", "true")
    .transport(Arc::new(TestTransport::new("managed-nof-boundary-segment")))
    .local_memory(storage_config_with_bytes(512))
    .nof_target(target)
    .build(test_future_expiry_ms())
    .expect("managed NoF client");
    client.register_local_memory().expect("register memory");
    let payload = [17u8; 150];
    client.put("managed-nof-boundary", &payload).expect("put");
    let usage_before = client
        .metadata
        .list_cold_tier_devices(&mooncake_store_core::ColdTierDeviceFilter::default())
        .expect("registered devices before materialization")
        .into_iter()
        .map(|device| (device.device_id, (device.used_bytes, device.reserved_bytes)))
        .collect::<BTreeMap<_, _>>();
    assert_eq!(
        client
            .storage_owner
            .materialize_pending_offloads_bounded(32)
            .expect("managed NoF materialization"),
        1
    );
    let route = client
        .query_route("managed-nof-boundary")
        .expect("query")
        .expect("route");
    assert_eq!(
        route.nof_backing.as_ref().expect("NoF placement").state,
        mooncake_store_core::ColdBackingState::Materialized
    );
    assert_eq!(backend.records.lock().len(), 1);
    assert!(client
        .storage_owner
        .pending_offloads
        .unconfirmed
        .lock()
        .is_empty());
    let usage_after = client
        .metadata
        .list_cold_tier_devices(&mooncake_store_core::ColdTierDeviceFilter::default())
        .expect("registered devices after materialization")
        .into_iter()
        .map(|device| (device.device_id, (device.used_bytes, device.reserved_bytes)))
        .collect::<BTreeMap<_, _>>();
    assert_eq!(usage_after, usage_before);
    force_cold_only_route(&client, "managed-nof-boundary");
    assert_eq!(
        client.get("managed-nof-boundary").expect("NoF read"),
        payload
    );
}
