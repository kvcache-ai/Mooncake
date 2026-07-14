#include "fixture.h"

namespace mooncake::test {
TEST_F(MasterServiceTest, TenantBatchUpsertAndRevokeAreScoped) {
    const std::vector<std::string> keys = {"tenant_batch_upsert_key_a",
                                           "tenant_batch_upsert_key_b"};
    const std::vector<uint64_t> sizes = {1024, 2048};
    const std::string tenant_a = "tenant_batch_upsert_a";
    const std::string tenant_b = "tenant_batch_upsert_b";
    auto svc = std::make_unique<MasterService>(
        MakeStrictTenantConfig({"default", tenant_a, tenant_b}));
    [[maybe_unused]] const auto context = PrepareSimpleSegment(*svc);
    const UUID client_id = generate_uuid();

    ReplicateConfig config;
    config.replica_num = 1;

    auto tenant_a_results =
        svc->BatchUpsertStart(client_id, keys, tenant_a, sizes, config);
    ASSERT_EQ(tenant_a_results.size(), keys.size());
    for (const auto& result : tenant_a_results) {
        ASSERT_TRUE(result.has_value());
    }
    auto tenant_a_end = svc->BatchUpsertEnd(client_id, keys, tenant_a);
    ASSERT_EQ(tenant_a_end.size(), keys.size());
    for (const auto& result : tenant_a_end) {
        ASSERT_TRUE(result.has_value());
    }

    auto tenant_b_results =
        svc->BatchUpsertStart(client_id, keys, tenant_b, sizes, config);
    ASSERT_EQ(tenant_b_results.size(), keys.size());
    for (const auto& result : tenant_b_results) {
        ASSERT_TRUE(result.has_value());
    }
    auto tenant_b_end = svc->BatchUpsertEnd(client_id, keys, tenant_b);
    ASSERT_EQ(tenant_b_end.size(), keys.size());
    for (const auto& result : tenant_b_end) {
        ASSERT_TRUE(result.has_value());
    }

    for (const auto& key : keys) {
        EXPECT_FALSE(svc->GetReplicaList(key, "default").has_value());
        EXPECT_TRUE(svc->GetReplicaList(key, tenant_a).has_value());
        EXPECT_TRUE(svc->GetReplicaList(key, tenant_b).has_value());
    }

    const std::string revoke_key = "tenant_batch_upsert_revoke_key";
    auto revoke_start =
        svc->UpsertStart(client_id, revoke_key, tenant_a, 1024, config);
    ASSERT_TRUE(revoke_start.has_value());
    ASSERT_TRUE(
        svc->UpsertRevoke(client_id, revoke_key, tenant_a, ReplicaType::MEMORY)
            .has_value());
    EXPECT_FALSE(svc->GetReplicaList(revoke_key, tenant_a).has_value());
}

TEST_F(MasterServiceTest, UpsertNewKey) {
    // Case A: key does not exist — behaves like PutStart
    std::unique_ptr<MasterService> service_(new MasterService());
    [[maybe_unused]] const auto context = PrepareSimpleSegment(*service_);
    const UUID client_id = generate_uuid();

    std::string key = "upsert_new_key";
    uint64_t slice_length = 1024;
    ReplicateConfig config;
    config.replica_num = 1;

    auto upsert_result =
        service_->UpsertStart(client_id, key, "default", slice_length, config);
    ASSERT_TRUE(upsert_result.has_value());
    auto replicas = upsert_result.value();
    EXPECT_EQ(1, replicas.size());
    EXPECT_EQ(ReplicaStatus::PROCESSING, replicas[0].status);

    // During upsert, GetReplicaList should return not ready
    auto get_result = service_->GetReplicaList(key, "default");
    EXPECT_FALSE(get_result.has_value());
    EXPECT_EQ(ErrorCode::REPLICA_IS_NOT_READY, get_result.error());

    // UpsertEnd completes the operation
    auto end_result =
        service_->UpsertEnd(client_id, key, "default", ReplicaType::MEMORY);
    ASSERT_TRUE(end_result.has_value());

    // Verify replica is COMPLETE
    auto final_result = service_->GetReplicaList(key, "default");
    ASSERT_TRUE(final_result.has_value());
    EXPECT_EQ(1, final_result.value().replicas.size());
    EXPECT_EQ(ReplicaStatus::COMPLETE, final_result.value().replicas[0].status);
}

TEST_F(MasterServiceTest, UpsertSameSize) {
    // Case B: key exists with same size — in-place update
    std::unique_ptr<MasterService> service_(new MasterService());
    [[maybe_unused]] const auto context = PrepareSimpleSegment(*service_);
    const UUID client_id = generate_uuid();

    std::string key = "upsert_same_size";
    uint64_t slice_length = 1024;
    ReplicateConfig config;
    config.replica_num = 1;

    // First: PutStart + PutEnd to create the object
    auto put_result =
        service_->PutStart(client_id, key, "default", slice_length, config);
    ASSERT_TRUE(put_result.has_value());
    auto original_replicas = put_result.value();
    auto put_end =
        service_->PutEnd(client_id, key, "default", ReplicaType::MEMORY);
    ASSERT_TRUE(put_end.has_value());

    // UpsertStart with same size — should reuse buffers
    const UUID new_client_id = generate_uuid();
    auto upsert_result = service_->UpsertStart(new_client_id, key, "default",
                                               slice_length, config);
    ASSERT_TRUE(upsert_result.has_value());
    auto upsert_replicas = upsert_result.value();
    EXPECT_EQ(1, upsert_replicas.size());
    EXPECT_EQ(ReplicaStatus::PROCESSING, upsert_replicas[0].status);

    // Verify same buffer address (in-place reuse)
    EXPECT_EQ(original_replicas[0]
                  .get_memory_descriptor()
                  .buffer_descriptor.buffer_address_,
              upsert_replicas[0]
                  .get_memory_descriptor()
                  .buffer_descriptor.buffer_address_);

    // UpsertEnd with the new client_id
    auto end_result =
        service_->UpsertEnd(new_client_id, key, "default", ReplicaType::MEMORY);
    ASSERT_TRUE(end_result.has_value());

    // Verify replica is COMPLETE again
    auto final_result = service_->GetReplicaList(key, "default");
    ASSERT_TRUE(final_result.has_value());
    EXPECT_EQ(ReplicaStatus::COMPLETE, final_result.value().replicas[0].status);
}

TEST_F(MasterServiceTest, UpsertSameSizeRefreshesMetadata) {
    // Case B: verify client_id and put_start_time are refreshed
    std::unique_ptr<MasterService> service_(new MasterService());
    [[maybe_unused]] const auto context = PrepareSimpleSegment(*service_);
    const UUID client_id_a = generate_uuid();
    const UUID client_id_b = generate_uuid();

    std::string key = "upsert_refresh_metadata";
    uint64_t slice_length = 1024;
    ReplicateConfig config;
    config.replica_num = 1;

    // Create object with client_a
    auto put_result =
        service_->PutStart(client_id_a, key, "default", slice_length, config);
    ASSERT_TRUE(put_result.has_value());
    auto put_end =
        service_->PutEnd(client_id_a, key, "default", ReplicaType::MEMORY);
    ASSERT_TRUE(put_end.has_value());

    // UpsertStart with client_b
    auto upsert_result = service_->UpsertStart(client_id_b, key, "default",
                                               slice_length, config);
    ASSERT_TRUE(upsert_result.has_value());

    // UpsertEnd with client_a should fail (client_id was refreshed to client_b)
    auto end_fail =
        service_->UpsertEnd(client_id_a, key, "default", ReplicaType::MEMORY);
    EXPECT_FALSE(end_fail.has_value());
    EXPECT_EQ(ErrorCode::ILLEGAL_CLIENT, end_fail.error());

    // UpsertEnd with client_b should succeed
    auto end_ok =
        service_->UpsertEnd(client_id_b, key, "default", ReplicaType::MEMORY);
    ASSERT_TRUE(end_ok.has_value());
}

TEST_F(MasterServiceTest, UpsertDifferentSize) {
    // Case C: key exists with different size — delete and reallocate
    std::unique_ptr<MasterService> service_(new MasterService());
    [[maybe_unused]] const auto context = PrepareSimpleSegment(*service_);
    const UUID client_id = generate_uuid();

    std::string key = "upsert_diff_size";
    uint64_t original_size = 1024;
    uint64_t new_size = 2048;
    ReplicateConfig config;
    config.replica_num = 1;

    // Create object with original_size
    auto put_result =
        service_->PutStart(client_id, key, "default", original_size, config);
    ASSERT_TRUE(put_result.has_value());
    auto original_replicas = put_result.value();
    auto put_end =
        service_->PutEnd(client_id, key, "default", ReplicaType::MEMORY);
    ASSERT_TRUE(put_end.has_value());

    // UpsertStart with different size
    auto upsert_result =
        service_->UpsertStart(client_id, key, "default", new_size, config);
    ASSERT_TRUE(upsert_result.has_value());
    auto new_replicas = upsert_result.value();
    EXPECT_EQ(1, new_replicas.size());
    EXPECT_EQ(ReplicaStatus::PROCESSING, new_replicas[0].status);

    // Buffer address should be different (reallocated)
    EXPECT_NE(original_replicas[0]
                  .get_memory_descriptor()
                  .buffer_descriptor.buffer_address_,
              new_replicas[0]
                  .get_memory_descriptor()
                  .buffer_descriptor.buffer_address_);

    // UpsertEnd
    auto end_result =
        service_->UpsertEnd(client_id, key, "default", ReplicaType::MEMORY);
    ASSERT_TRUE(end_result.has_value());

    // Verify the object is complete
    auto final_result = service_->GetReplicaList(key, "default");
    ASSERT_TRUE(final_result.has_value());
    EXPECT_EQ(ReplicaStatus::COMPLETE, final_result.value().replicas[0].status);
}

TEST_F(MasterServiceTest, UpsertConflictReplicationTask) {
    // Upsert should fail if Copy is in progress
    const uint64_t kv_lease_ttl = 50;
    auto service_config = MasterServiceConfig::builder()
                              .set_default_kv_lease_ttl(kv_lease_ttl)
                              .build();
    std::unique_ptr<MasterService> service_(new MasterService(service_config));

    [[maybe_unused]] const auto ctx1 =
        PrepareSimpleSegment(*service_, "segment_1");
    [[maybe_unused]] const auto ctx2 =
        PrepareSimpleSegment(*service_, "segment_2");
    UUID client_id = generate_uuid();

    std::string key = "upsert_conflict_copy";
    uint64_t slice_length = 1024;
    ReplicateConfig config;
    config.replica_num = 1;
    config.preferred_segment = "segment_1";

    // Create object
    auto put_result =
        service_->PutStart(client_id, key, "default", slice_length, config);
    ASSERT_TRUE(put_result.has_value());
    auto put_end =
        service_->PutEnd(client_id, key, "default", ReplicaType::MEMORY);
    ASSERT_TRUE(put_end.has_value());

    // Start a Copy
    auto copy_result = service_->CopyStart(client_id, key, "default",
                                           "segment_1", {"segment_2"});
    ASSERT_TRUE(copy_result.has_value());

    // UpsertStart should fail with OBJECT_HAS_REPLICATION_TASK
    auto upsert_result =
        service_->UpsertStart(client_id, key, "default", slice_length, config);
    EXPECT_FALSE(upsert_result.has_value());
    EXPECT_EQ(ErrorCode::OBJECT_HAS_REPLICATION_TASK, upsert_result.error());
}

TEST_F(MasterServiceTest, UpsertPreemptsInProgressPut) {
    // Upsert should preempt an in-progress Put (no discard timeout needed
    // for preemption via Upsert — Upsert always preempts immediately)
    std::unique_ptr<MasterService> service_(new MasterService());
    [[maybe_unused]] const auto context = PrepareSimpleSegment(*service_);
    const UUID client_a = generate_uuid();
    const UUID client_b = generate_uuid();

    std::string key = "upsert_preempt";
    uint64_t slice_length = 1024;
    ReplicateConfig config;
    config.replica_num = 1;

    // Client A starts a Put but doesn't finish
    auto put_result =
        service_->PutStart(client_a, key, "default", slice_length, config);
    ASSERT_TRUE(put_result.has_value());

    // Client B upserts the same key — should preempt client A
    auto upsert_result =
        service_->UpsertStart(client_b, key, "default", slice_length, config);
    ASSERT_TRUE(upsert_result.has_value());
    auto upsert_replicas = upsert_result.value();
    EXPECT_EQ(1, upsert_replicas.size());
    EXPECT_EQ(ReplicaStatus::PROCESSING, upsert_replicas[0].status);

    // Client A's PutEnd should fail
    auto put_end_a =
        service_->PutEnd(client_a, key, "default", ReplicaType::MEMORY);
    EXPECT_FALSE(put_end_a.has_value());

    // Client B's UpsertEnd should succeed
    auto upsert_end =
        service_->UpsertEnd(client_b, key, "default", ReplicaType::MEMORY);
    ASSERT_TRUE(upsert_end.has_value());

    // Verify final state
    auto final_result = service_->GetReplicaList(key, "default");
    ASSERT_TRUE(final_result.has_value());
    EXPECT_EQ(ReplicaStatus::COMPLETE, final_result.value().replicas[0].status);
}

TEST_F(MasterServiceTest, UpsertRevoke) {
    // UpsertRevoke should clean up like PutRevoke
    std::unique_ptr<MasterService> service_(new MasterService());
    [[maybe_unused]] const auto context = PrepareSimpleSegment(*service_);
    const UUID client_id = generate_uuid();

    std::string key = "upsert_revoke";
    uint64_t slice_length = 1024;
    ReplicateConfig config;
    config.replica_num = 1;

    // UpsertStart (Case A — new key)
    auto upsert_result =
        service_->UpsertStart(client_id, key, "default", slice_length, config);
    ASSERT_TRUE(upsert_result.has_value());

    // UpsertRevoke
    auto revoke_result =
        service_->UpsertRevoke(client_id, key, "default", ReplicaType::MEMORY);
    ASSERT_TRUE(revoke_result.has_value());

    // Key should be gone
    auto exist_result = service_->ExistKey(key, "default");
    ASSERT_TRUE(exist_result.has_value());
    EXPECT_FALSE(exist_result.value());
}

TEST_F(MasterServiceTest, UpsertInPlaceThenRevoke) {
    // UpsertRevoke after in-place UpsertStart should clean up
    std::unique_ptr<MasterService> service_(new MasterService());
    [[maybe_unused]] const auto context = PrepareSimpleSegment(*service_);
    const UUID client_id = generate_uuid();

    std::string key = "upsert_inplace_revoke";
    uint64_t slice_length = 1024;
    ReplicateConfig config;
    config.replica_num = 1;

    // Create object first
    auto put_result =
        service_->PutStart(client_id, key, "default", slice_length, config);
    ASSERT_TRUE(put_result.has_value());
    auto put_end =
        service_->PutEnd(client_id, key, "default", ReplicaType::MEMORY);
    ASSERT_TRUE(put_end.has_value());

    // UpsertStart in-place (same size)
    const UUID new_client = generate_uuid();
    auto upsert_result =
        service_->UpsertStart(new_client, key, "default", slice_length, config);
    ASSERT_TRUE(upsert_result.has_value());

    // UpsertRevoke — replicas are PROCESSING, should be erased
    auto revoke_result =
        service_->UpsertRevoke(new_client, key, "default", ReplicaType::MEMORY);
    ASSERT_TRUE(revoke_result.has_value());

    // Key should be gone (no valid replicas left)
    auto exist_result = service_->ExistKey(key, "default");
    ASSERT_TRUE(exist_result.has_value());
    EXPECT_FALSE(exist_result.value());
}

TEST_F(MasterServiceTest, BatchUpsertStart) {
    // Test batch upsert with a mix of new and existing keys
    std::unique_ptr<MasterService> service_(new MasterService());
    [[maybe_unused]] const auto context = PrepareSimpleSegment(*service_);
    const UUID client_id = generate_uuid();

    ReplicateConfig config;
    config.replica_num = 1;

    // Create key_1 with size 1024
    auto put_result =
        service_->PutStart(client_id, "key_1", "default", 1024, config);
    ASSERT_TRUE(put_result.has_value());
    auto put_end =
        service_->PutEnd(client_id, "key_1", "default", ReplicaType::MEMORY);
    ASSERT_TRUE(put_end.has_value());

    // BatchUpsertStart: key_1 (same size), key_2 (new)
    std::vector<std::string> keys = {"key_1", "key_2"};
    std::vector<uint64_t> slice_lengths = {1024, 2048};

    auto results = service_->BatchUpsertStart(client_id, keys, "default",
                                              slice_lengths, config);
    ASSERT_EQ(2, results.size());
    EXPECT_TRUE(results[0].has_value());  // key_1: Case B (in-place)
    EXPECT_TRUE(results[1].has_value());  // key_2: Case A (new)

    // Complete both
    auto end_results = service_->BatchUpsertEnd(client_id, keys, "default");
    ASSERT_EQ(2, end_results.size());
    EXPECT_TRUE(end_results[0].has_value());
    EXPECT_TRUE(end_results[1].has_value());
}

TEST_F(MasterServiceTest, UpsertPreemptsInProgressUpsert) {
    // Upsert should preempt an in-progress Upsert (Case B in-place).
    // After preemption, all replicas were PROCESSING (no COMPLETE survives),
    // so metadata is erased and the new upsert falls through to Case A.
    std::unique_ptr<MasterService> service_(new MasterService());
    [[maybe_unused]] const auto context = PrepareSimpleSegment(*service_);
    const UUID client_a = generate_uuid();
    const UUID client_b = generate_uuid();
    const UUID client_c = generate_uuid();

    std::string key = "upsert_preempt_upsert";
    uint64_t slice_length = 1024;
    ReplicateConfig config;
    config.replica_num = 1;

    // Step 1: Create the object via Put
    auto put_result =
        service_->PutStart(client_a, key, "default", slice_length, config);
    ASSERT_TRUE(put_result.has_value());
    auto put_end =
        service_->PutEnd(client_a, key, "default", ReplicaType::MEMORY);
    ASSERT_TRUE(put_end.has_value());

    // Step 2: Client B starts in-place upsert (Case B) — marks COMPLETE →
    // PROCESSING
    auto upsert_b =
        service_->UpsertStart(client_b, key, "default", slice_length, config);
    ASSERT_TRUE(upsert_b.has_value());

    // Key should be unreadable now (all replicas are PROCESSING)
    auto get_mid = service_->GetReplicaList(key, "default");
    EXPECT_FALSE(get_mid.has_value());
    EXPECT_EQ(ErrorCode::REPLICA_IS_NOT_READY, get_mid.error());

    // Step 3: Client C upserts the same key — preempts Client B
    auto upsert_c =
        service_->UpsertStart(client_c, key, "default", slice_length, config);
    ASSERT_TRUE(upsert_c.has_value());
    EXPECT_EQ(1, upsert_c.value().size());

    // Step 4: Client B's UpsertEnd should fail (preempted)
    auto end_b =
        service_->UpsertEnd(client_b, key, "default", ReplicaType::MEMORY);
    EXPECT_FALSE(end_b.has_value());

    // Step 5: Client C's UpsertEnd should succeed
    auto end_c =
        service_->UpsertEnd(client_c, key, "default", ReplicaType::MEMORY);
    ASSERT_TRUE(end_c.has_value());

    // Final verification
    auto final_result = service_->GetReplicaList(key, "default");
    ASSERT_TRUE(final_result.has_value());
    EXPECT_EQ(1, final_result.value().replicas.size());
    EXPECT_EQ(ReplicaStatus::COMPLETE, final_result.value().replicas[0].status);
}

TEST_F(MasterServiceTest, UpsertDifferentSizeThenRevoke) {
    // Case C (different size) followed by UpsertRevoke.
    // Old replicas go to discarded_replicas_, new replicas are erased by
    // revoke. The key should disappear entirely.
    std::unique_ptr<MasterService> service_(new MasterService());
    [[maybe_unused]] const auto context = PrepareSimpleSegment(*service_);
    const UUID client_id = generate_uuid();

    std::string key = "upsert_diff_revoke";
    uint64_t original_size = 1024;
    uint64_t new_size = 2048;
    ReplicateConfig config;
    config.replica_num = 1;

    // Create object with original size
    auto put_result =
        service_->PutStart(client_id, key, "default", original_size, config);
    ASSERT_TRUE(put_result.has_value());
    auto put_end =
        service_->PutEnd(client_id, key, "default", ReplicaType::MEMORY);
    ASSERT_TRUE(put_end.has_value());

    // Verify the key exists
    auto exist_before = service_->ExistKey(key, "default");
    ASSERT_TRUE(exist_before.has_value());
    EXPECT_TRUE(exist_before.value());

    // UpsertStart with different size (Case C) — old replicas discarded,
    // new replicas allocated
    auto upsert_result =
        service_->UpsertStart(client_id, key, "default", new_size, config);
    ASSERT_TRUE(upsert_result.has_value());

    // Revoke — erase the newly allocated PROCESSING replicas
    auto revoke_result =
        service_->UpsertRevoke(client_id, key, "default", ReplicaType::MEMORY);
    ASSERT_TRUE(revoke_result.has_value());

    // Key should be gone (old replicas in discarded, new replicas erased)
    auto exist_after = service_->ExistKey(key, "default");
    ASSERT_TRUE(exist_after.has_value());
    EXPECT_FALSE(exist_after.value());
}

// ===================== Hard Pin Tests =====================

}  // namespace mooncake::test
