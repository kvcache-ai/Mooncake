#include "fixture.h"

namespace mooncake::test {
TEST_F(MasterServiceTest, CopyStart) {
    const uint64_t kv_lease_ttl = 50;
    auto service_config = MasterServiceConfig::builder()
                              .set_default_kv_lease_ttl(kv_lease_ttl)
                              .build();
    std::unique_ptr<MasterService> service_(new MasterService(service_config));

    // Mount 4 segments (segment_1, segment_2, segment_3, segment_4) with
    // PrepareSimpleSegment
    [[maybe_unused]] const auto context1 =
        PrepareSimpleSegment(*service_, "segment_1");
    [[maybe_unused]] const auto context2 =
        PrepareSimpleSegment(*service_, "segment_2");
    [[maybe_unused]] const auto context3 =
        PrepareSimpleSegment(*service_, "segment_3");
    [[maybe_unused]] const auto context4 =
        PrepareSimpleSegment(*service_, "segment_4");

    UUID client_id = generate_uuid();

    // Test Case 1: CopyStart a non-existent key, should fail.
    auto copy_result = service_->CopyStart(
        client_id, "non_existent_key", "default", "segment_1", {"segment_2"});
    EXPECT_FALSE(copy_result.has_value());
    EXPECT_EQ(ErrorCode::OBJECT_NOT_FOUND, copy_result.error());

    // PutStart an object with 1 replica and preferred_segment=segment_1 for
    // testing
    std::string key = "test_key";
    uint64_t slice_length = 1024;
    ReplicateConfig config;
    config.replica_num = 1;
    config.preferred_segment = "segment_1";

    auto put_start_result =
        service_->PutStart(client_id, key, "default", slice_length, config);
    ASSERT_TRUE(put_start_result.has_value());

    // Test Case 2: CopyStart to segment_2 and segment_3, should fail because
    // the only replica is not completed.
    copy_result = service_->CopyStart(client_id, key, "default", "segment_1",
                                      {"segment_2", "segment_3"});
    EXPECT_FALSE(copy_result.has_value());
    EXPECT_EQ(ErrorCode::REPLICA_NOT_FOUND, copy_result.error());

    // PutEnd the object.
    auto put_end_result =
        service_->PutEnd(client_id, key, "default", ReplicaType::MEMORY);
    ASSERT_TRUE(put_end_result.has_value());

    // Test Case 3: CopyStart to segment_2 and segment_3, should success.
    copy_result = service_->CopyStart(client_id, key, "default", "segment_1",
                                      {"segment_2", "segment_3"});
    EXPECT_TRUE(copy_result.has_value());
    auto copy_response = copy_result.value();
    EXPECT_EQ("segment_1", copy_response.source.get_memory_descriptor()
                               .buffer_descriptor.transport_endpoint_);
    EXPECT_EQ(2, copy_response.targets.size());

    // Test Case 4: Try remove the object, should fail because it is copying.
    auto remove_result = service_->Remove(key, "default");
    EXPECT_FALSE(remove_result.has_value());
    EXPECT_EQ(ErrorCode::REPLICA_IS_NOT_READY, remove_result.error());

    // Test Case 5: CopyStart to segment_4, should fail because there is an
    // ongoing copy task.
    copy_result = service_->CopyStart(client_id, key, "default", "segment_1",
                                      {"segment_4"});
    EXPECT_FALSE(copy_result.has_value());
    EXPECT_EQ(ErrorCode::OBJECT_HAS_REPLICATION_TASK, copy_result.error());

    // Test Case 6: CopyEnd, should success and the object now has 3 replicas.
    auto copy_end_result = service_->CopyEnd(client_id, key, "default");
    EXPECT_TRUE(copy_end_result.has_value());
    auto get_result = service_->GetReplicaList(key, "default");
    EXPECT_TRUE(get_result.has_value());
    EXPECT_EQ(3, get_result.value().replicas.size());

    // Test Case 7: Copy from a non-existent replica to segment_3 and
    // segment_4, should fail.
    copy_result =
        service_->CopyStart(client_id, key, "default", "non_existent_segment",
                            {"segment_3", "segment_4"});
    EXPECT_FALSE(copy_result.has_value());
    EXPECT_EQ(ErrorCode::REPLICA_NOT_FOUND, copy_result.error());

    // Test Case 8: Copy to segment_4 and a non-existent segment, should fail.
    copy_result = service_->CopyStart(client_id, key, "default", "segment_1",
                                      {"segment_4", "non_existent_segment"});
    EXPECT_FALSE(copy_result.has_value());
    EXPECT_EQ(ErrorCode::SEGMENT_NOT_FOUND, copy_result.error());

    // Test Case 9: Copy to segment_3 and segment_4, should skip segment_3 and
    // successfully copy to segment_4.
    copy_result = service_->CopyStart(client_id, key, "default", "segment_1",
                                      {"segment_3", "segment_4"});
    EXPECT_TRUE(copy_result.has_value());
    copy_response = copy_result.value();
    EXPECT_EQ("segment_1", copy_response.source.get_memory_descriptor()
                               .buffer_descriptor.transport_endpoint_);
    EXPECT_EQ(1, copy_response.targets
                     .size());  // Only 1 replica since segment_3 is skipped
    EXPECT_EQ("segment_4", copy_response.targets[0]
                               .get_memory_descriptor()
                               .buffer_descriptor.transport_endpoint_);

    // End the copy operation to clean up state
    copy_end_result = service_->CopyEnd(client_id, key, "default");
    EXPECT_TRUE(copy_end_result.has_value());
    get_result = service_->GetReplicaList(key, "default");
    EXPECT_TRUE(get_result.has_value());
    EXPECT_EQ(4, get_result.value().replicas.size());

    // Test Case 10: Copy to segment_4 again, should skip because it's already
    // used.
    copy_result = service_->CopyStart(client_id, key, "default", "segment_1",
                                      {"segment_4"});
    EXPECT_TRUE(copy_result.has_value());
    copy_response = copy_result.value();
    EXPECT_EQ("segment_1", copy_response.source.get_memory_descriptor()
                               .buffer_descriptor.transport_endpoint_);
    EXPECT_EQ(0,
              copy_response.targets
                  .size());  // No replicas since segment_4 is already used

    // Wait for the lease to expire
    std::this_thread::sleep_for(std::chrono::milliseconds(kv_lease_ttl * 2));

    // Test Case 11: Try remove the object, should fail because it is copying.
    remove_result = service_->Remove(key, "default");
    EXPECT_FALSE(remove_result.has_value());
    EXPECT_EQ(ErrorCode::OBJECT_HAS_REPLICATION_TASK, remove_result.error());

    // Clean up the copy operation
    copy_end_result = service_->CopyEnd(client_id, key, "default");
    EXPECT_TRUE(copy_end_result.has_value());

    // Wait for the lease to expire
    std::this_thread::sleep_for(std::chrono::milliseconds(kv_lease_ttl * 2));

    // Test Case 12: Try remove the object, should success.
    remove_result = service_->Remove(key, "default");
    EXPECT_TRUE(remove_result.has_value());
}

TEST_F(MasterServiceTest, CopyEnd) {
    std::unique_ptr<MasterService> service_(new MasterService());

    // Mount 3 segments (segment_1, segment_2, segment_3) with
    // PrepareSimpleSegment
    [[maybe_unused]] const auto context1 =
        PrepareSimpleSegment(*service_, "segment_1");
    [[maybe_unused]] const auto context2 =
        PrepareSimpleSegment(*service_, "segment_2");
    [[maybe_unused]] const auto context3 =
        PrepareSimpleSegment(*service_, "segment_3");

    UUID client_id = generate_uuid();
    UUID invalid_client_id = generate_uuid();

    // Test Case 1: CopyEnd a non-existent key, should fail.
    auto copy_end_result =
        service_->CopyEnd(client_id, "non_existent_key", "default");
    EXPECT_FALSE(copy_end_result.has_value());
    EXPECT_EQ(ErrorCode::OBJECT_NOT_FOUND, copy_end_result.error());

    // Put an object with 1 replica and preferred_segment=segment_1 for testing
    std::string key = "test_key";
    uint64_t slice_length = 1024;
    ReplicateConfig config;
    config.replica_num = 1;
    config.preferred_segment = "segment_1";

    auto put_start_result =
        service_->PutStart(client_id, key, "default", slice_length, config);
    ASSERT_TRUE(put_start_result.has_value());
    auto put_end_result =
        service_->PutEnd(client_id, key, "default", ReplicaType::MEMORY);
    ASSERT_TRUE(put_end_result.has_value());

    // Test Case 2: CopyEnd the object, should fail because there is no ongoing
    // copy task.
    copy_end_result = service_->CopyEnd(client_id, key, "default");
    EXPECT_FALSE(copy_end_result.has_value());
    EXPECT_EQ(ErrorCode::OBJECT_NO_REPLICATION_TASK, copy_end_result.error());

    // CopyStart the object to segment_2
    auto copy_start_result = service_->CopyStart(client_id, key, "default",
                                                 "segment_1", {"segment_2"});
    ASSERT_TRUE(copy_start_result.has_value());

    // Test Case 3: CopyEnd with an invalid client id, should fail.
    copy_end_result = service_->CopyEnd(invalid_client_id, key, "default");
    EXPECT_FALSE(copy_end_result.has_value());
    EXPECT_EQ(ErrorCode::ILLEGAL_CLIENT, copy_end_result.error());

    // Test Case 4: MoveEnd the object, should fail because the ongoing task is
    // Copy.
    auto move_end_result = service_->MoveEnd(client_id, key, "default");
    EXPECT_FALSE(move_end_result.has_value());
    EXPECT_EQ(ErrorCode::INVALID_PARAMS, move_end_result.error());

    // Test Case 5: CopyEnd, should success.
    copy_end_result = service_->CopyEnd(client_id, key, "default");
    EXPECT_TRUE(copy_end_result.has_value());

    // Verify we now have 2 replicas
    auto get_result = service_->GetReplicaList(key, "default");
    EXPECT_TRUE(get_result.has_value());
    EXPECT_EQ(2, get_result.value().replicas.size());

    // CopyStart the object from segment_1 to segment_3, then unmount segment_1
    copy_start_result = service_->CopyStart(client_id, key, "default",
                                            "segment_1", {"segment_3"});
    ASSERT_TRUE(copy_start_result.has_value());

    // Unmount segment_1 to simulate source gone
    auto unmount_result =
        service_->UnmountSegment(context1.segment_id, context1.client_id);
    ASSERT_TRUE(unmount_result.has_value());

    // Test Case 6: CopyEnd, should fail because the source is gone, the object
    // should have only 1 replica from segment_2.
    copy_end_result = service_->CopyEnd(client_id, key, "default");
    EXPECT_FALSE(copy_end_result.has_value());
    EXPECT_EQ(ErrorCode::REPLICA_IS_GONE, copy_end_result.error());
    get_result = service_->GetReplicaList(key, "default");
    EXPECT_TRUE(get_result.has_value());
    auto& replicas = get_result.value().replicas;
    EXPECT_EQ(1, replicas.size());
    EXPECT_EQ("segment_2", replicas[0]
                               .get_memory_descriptor()
                               .buffer_descriptor.transport_endpoint_);

    // CopyStart the object from segment_2 to segment_3, then unmount segment_3
    copy_start_result = service_->CopyStart(client_id, key, "default",
                                            "segment_2", {"segment_3"});
    ASSERT_TRUE(copy_start_result.has_value());

    // Unmount segment_3 to simulate target gone
    unmount_result =
        service_->UnmountSegment(context3.segment_id, context3.client_id);
    ASSERT_TRUE(unmount_result.has_value());

    // Test Case 7: CopyEnd, should fail because the target is gone, the object
    // should have only 1 replica from segment_2.
    copy_end_result = service_->CopyEnd(client_id, key, "default");
    EXPECT_FALSE(copy_end_result.has_value());
    EXPECT_EQ(ErrorCode::REPLICA_IS_GONE, copy_end_result.error());
    get_result = service_->GetReplicaList(key, "default");
    EXPECT_TRUE(get_result.has_value());
    replicas = get_result.value().replicas;
    EXPECT_EQ(1, replicas.size());
    EXPECT_EQ("segment_2", replicas[0]
                               .get_memory_descriptor()
                               .buffer_descriptor.transport_endpoint_);
}

TEST_F(MasterServiceTest, CopyRevoke) {
    std::unique_ptr<MasterService> service_(new MasterService());

    // Mount 2 segments (segment_1, segment_2) with
    // PrepareSimpleSegment
    [[maybe_unused]] const auto context1 =
        PrepareSimpleSegment(*service_, "segment_1");
    [[maybe_unused]] const auto context2 =
        PrepareSimpleSegment(*service_, "segment_2");

    UUID client_id = generate_uuid();
    UUID invalid_client_id = generate_uuid();

    // Test Case 1: CopyRevoke a non-existent key, should fail.
    auto copy_revoke_result =
        service_->CopyRevoke(client_id, "non_existent_key", "default");
    EXPECT_FALSE(copy_revoke_result.has_value());
    EXPECT_EQ(ErrorCode::OBJECT_NOT_FOUND, copy_revoke_result.error());

    // Put an object with 1 replica and preferred_segment=segment_1 for testing
    std::string key = "test_key";
    uint64_t slice_length = 1024;
    ReplicateConfig config;
    config.replica_num = 1;
    config.preferred_segment = "segment_1";

    auto put_start_result =
        service_->PutStart(client_id, key, "default", slice_length, config);
    ASSERT_TRUE(put_start_result.has_value());
    auto put_end_result =
        service_->PutEnd(client_id, key, "default", ReplicaType::MEMORY);
    ASSERT_TRUE(put_end_result.has_value());

    // Test Case 2: CopyRevoke the object, should fail because there is no
    // ongoing copy task.
    copy_revoke_result = service_->CopyRevoke(client_id, key, "default");
    EXPECT_FALSE(copy_revoke_result.has_value());
    EXPECT_EQ(ErrorCode::OBJECT_NO_REPLICATION_TASK,
              copy_revoke_result.error());

    // CopyStart the object to segment_2
    auto copy_start_result = service_->CopyStart(client_id, key, "default",
                                                 "segment_1", {"segment_2"});
    ASSERT_TRUE(copy_start_result.has_value());

    // Test Case 3: CopyRevoke with an invalid client id, should fail.
    copy_revoke_result =
        service_->CopyRevoke(invalid_client_id, key, "default");
    EXPECT_FALSE(copy_revoke_result.has_value());
    EXPECT_EQ(ErrorCode::ILLEGAL_CLIENT, copy_revoke_result.error());

    // Test Case 4: MoveRevoke the object, should fail because the ongoing task
    // is Copy.
    auto move_revoke_result = service_->MoveRevoke(client_id, key, "default");
    EXPECT_FALSE(move_revoke_result.has_value());
    EXPECT_EQ(ErrorCode::INVALID_PARAMS, move_revoke_result.error());

    // Test Case 5: CopyRevoke, should success.
    copy_revoke_result = service_->CopyRevoke(client_id, key, "default");
    EXPECT_TRUE(copy_revoke_result.has_value());

    // Verify we still have 1 replica (the copy was revoked)
    auto get_result = service_->GetReplicaList(key, "default");
    EXPECT_TRUE(get_result.has_value());
    EXPECT_EQ(1, get_result.value().replicas.size());

    // CopyStart the object from segment_1 to segment_2 again, then unmount
    // segment_1
    copy_start_result = service_->CopyStart(client_id, key, "default",
                                            "segment_1", {"segment_2"});
    ASSERT_TRUE(copy_start_result.has_value());

    // Unmount segment_1 to simulate source gone
    auto unmount_result =
        service_->UnmountSegment(context1.segment_id, context1.client_id);
    ASSERT_TRUE(unmount_result.has_value());

    // Test Case 6: CopyRevoke, should success even though the source is gone,
    // the object should be erased too.
    copy_revoke_result = service_->CopyRevoke(client_id, key, "default");
    EXPECT_TRUE(copy_revoke_result.has_value());

    // Verify the object has been removed.
    get_result = service_->GetReplicaList(key, "default");
    EXPECT_FALSE(get_result.has_value());
}

TEST_F(MasterServiceTest, MoveStart) {
    const uint64_t kv_lease_ttl = 50;
    auto service_config = MasterServiceConfig::builder()
                              .set_default_kv_lease_ttl(kv_lease_ttl)
                              .build();
    std::unique_ptr<MasterService> service_(new MasterService(service_config));

    // Mount 3 segments (segment_1, segment_2, segment_3) with
    // PrepareSimpleSegment
    [[maybe_unused]] const auto context1 =
        PrepareSimpleSegment(*service_, "segment_1");
    [[maybe_unused]] const auto context2 =
        PrepareSimpleSegment(*service_, "segment_2");
    [[maybe_unused]] const auto context3 =
        PrepareSimpleSegment(*service_, "segment_3");

    UUID client_id = generate_uuid();

    // Test Case 1: MoveStart a non-existent key, should fail.
    auto move_start_result = service_->MoveStart(
        client_id, "non_existent_key", "default", "segment_1", "segment_2");
    EXPECT_FALSE(move_start_result.has_value());
    EXPECT_EQ(ErrorCode::OBJECT_NOT_FOUND, move_start_result.error());

    // Put an object with 1 replica and preferred_segment=segment_1 for testing
    std::string key = "test_key";
    uint64_t slice_length = 1024;
    ReplicateConfig config;
    config.replica_num = 1;
    config.preferred_segment = "segment_1";

    auto put_start_result =
        service_->PutStart(client_id, key, "default", slice_length, config);
    ASSERT_TRUE(put_start_result.has_value());

    // Test Case 2: MoveStart the object, should fail because the only replica
    // is not completed.
    move_start_result = service_->MoveStart(client_id, key, "default",
                                            "segment_1", "segment_2");
    EXPECT_FALSE(move_start_result.has_value());
    EXPECT_EQ(ErrorCode::REPLICA_NOT_FOUND, move_start_result.error());

    // PutEnd the object.
    auto put_end_result =
        service_->PutEnd(client_id, key, "default", ReplicaType::MEMORY);
    ASSERT_TRUE(put_end_result.has_value());

    // Copy the object to segment_3.
    auto copy_start_result = service_->CopyStart(client_id, key, "default",
                                                 "segment_1", {"segment_3"});
    ASSERT_TRUE(copy_start_result.has_value());
    auto copy_end_result = service_->CopyEnd(client_id, key, "default");
    ASSERT_TRUE(copy_end_result.has_value());

    // Test Case 3: MoveStart with source and target be the same, should fail.
    move_start_result = service_->MoveStart(client_id, key, "default",
                                            "segment_1", "segment_1");
    EXPECT_FALSE(move_start_result.has_value());
    EXPECT_EQ(move_start_result.error(), ErrorCode::INVALID_PARAMS);

    // Test Case 4: MoveStart to segment_2, should succeed.
    move_start_result = service_->MoveStart(client_id, key, "default",
                                            "segment_1", "segment_2");
    EXPECT_TRUE(move_start_result.has_value());
    auto move_response = move_start_result.value();
    EXPECT_EQ("segment_1", move_response.source.get_memory_descriptor()
                               .buffer_descriptor.transport_endpoint_);
    EXPECT_TRUE(move_response.target.has_value());
    EXPECT_EQ("segment_2", move_response.target.value()
                               .get_memory_descriptor()
                               .buffer_descriptor.transport_endpoint_);

    // Test Case 5: Try remove the object, should fail because it is moving.
    auto remove_result = service_->Remove(key, "default");
    EXPECT_FALSE(remove_result.has_value());
    EXPECT_EQ(ErrorCode::REPLICA_IS_NOT_READY, remove_result.error());

    // Test Case 6: MoveStart again, should fail because there is an ongoing
    // move task.
    move_start_result = service_->MoveStart(client_id, key, "default",
                                            "segment_1", "segment_3");
    EXPECT_FALSE(move_start_result.has_value());
    EXPECT_EQ(ErrorCode::OBJECT_HAS_REPLICATION_TASK,
              move_start_result.error());

    // Test Case 7: MoveEnd, should succeed and the object now has 2 replicas
    // from segment_2 and segment_3
    auto move_end_result = service_->MoveEnd(client_id, key, "default");
    EXPECT_TRUE(move_end_result.has_value());

    auto get_result = service_->GetReplicaList(key, "default");
    EXPECT_TRUE(get_result.has_value());
    auto& replicas = get_result.value().replicas;
    EXPECT_EQ(2, replicas.size());

    // Test Case 8: Move from a non-existent replica to segment_1, should fail.
    move_start_result = service_->MoveStart(
        client_id, key, "default", "non_existent_segment", "segment_1");
    EXPECT_FALSE(move_start_result.has_value());
    EXPECT_EQ(ErrorCode::REPLICA_NOT_FOUND, move_start_result.error());

    // Test Case 8.5: Move to a non-existent target segment, should fail.
    move_start_result = service_->MoveStart(
        client_id, key, "default", "segment_2", "non_existent_segment");
    EXPECT_FALSE(move_start_result.has_value());
    EXPECT_EQ(ErrorCode::SEGMENT_NOT_FOUND, move_start_result.error());

    // Test Case 9: Move to an already existing segment, should succeed but
    // return nullopt.
    move_start_result = service_->MoveStart(client_id, key, "default",
                                            "segment_2", "segment_3");
    EXPECT_TRUE(move_start_result.has_value());
    move_response = move_start_result.value();
    EXPECT_EQ("segment_2", move_response.source.get_memory_descriptor()
                               .buffer_descriptor.transport_endpoint_);
    EXPECT_FALSE(move_response.target.has_value());

    // Test Case 10: Try remove the object, should fail because it is moving.
    std::this_thread::sleep_for(std::chrono::milliseconds(kv_lease_ttl * 2));
    remove_result = service_->Remove(key, "default");
    EXPECT_FALSE(remove_result.has_value());
    EXPECT_EQ(ErrorCode::OBJECT_HAS_REPLICATION_TASK, remove_result.error());

    // End the move.
    move_end_result = service_->MoveEnd(client_id, key, "default");
    EXPECT_TRUE(move_end_result.has_value());

    // Now the object should have only 1 replica on segment_3.
    get_result = service_->GetReplicaList(key, "default");
    EXPECT_TRUE(get_result.has_value());
    replicas = get_result.value().replicas;
    EXPECT_EQ(1, replicas.size());
    EXPECT_EQ("segment_3", replicas[0]
                               .get_memory_descriptor()
                               .buffer_descriptor.transport_endpoint_);

    // Test Case 11: Try remove the object, should succeed after lease expires.
    std::this_thread::sleep_for(std::chrono::milliseconds(kv_lease_ttl * 2));
    remove_result = service_->Remove(key, "default");
    EXPECT_TRUE(remove_result.has_value());
}

TEST_F(MasterServiceTest, MoveEnd) {
    std::unique_ptr<MasterService> service_(new MasterService());

    // Mount 2 segments (segment_1, segment_2) with
    // PrepareSimpleSegment
    [[maybe_unused]] const auto context1 =
        PrepareSimpleSegment(*service_, "segment_1");
    [[maybe_unused]] const auto context2 =
        PrepareSimpleSegment(*service_, "segment_2");

    UUID client_id = generate_uuid();
    UUID invalid_client_id = generate_uuid();

    // Test Case 1: MoveEnd a non-existent key, should fail.
    auto move_end_result =
        service_->MoveEnd(client_id, "non_existent_key", "default");
    EXPECT_FALSE(move_end_result.has_value());
    EXPECT_EQ(ErrorCode::OBJECT_NOT_FOUND, move_end_result.error());

    // Put an object with 1 replica and preferred_segment=segment_1 for testing
    std::string key = "test_key";
    uint64_t slice_length = 1024;
    ReplicateConfig config;
    config.replica_num = 1;
    config.preferred_segment = "segment_1";

    auto put_start_result =
        service_->PutStart(client_id, key, "default", slice_length, config);
    ASSERT_TRUE(put_start_result.has_value());
    auto put_end_result =
        service_->PutEnd(client_id, key, "default", ReplicaType::MEMORY);
    ASSERT_TRUE(put_end_result.has_value());

    // Test Case 2: MoveEnd the object, should fail because there is no ongoing
    // move task.
    move_end_result = service_->MoveEnd(client_id, key, "default");
    EXPECT_FALSE(move_end_result.has_value());
    EXPECT_EQ(ErrorCode::OBJECT_NO_REPLICATION_TASK, move_end_result.error());

    // MoveStart the object to segment_2
    auto move_start_result = service_->MoveStart(client_id, key, "default",
                                                 "segment_1", "segment_2");
    ASSERT_TRUE(move_start_result.has_value());

    // Test Case 3: MoveEnd with an invalid client id, should fail.
    move_end_result = service_->MoveEnd(invalid_client_id, key, "default");
    EXPECT_FALSE(move_end_result.has_value());
    EXPECT_EQ(ErrorCode::ILLEGAL_CLIENT, move_end_result.error());

    // Test Case 4: CopyEnd the object, should fail because the ongoing task is
    // Move.
    auto copy_end_result = service_->CopyEnd(client_id, key, "default");
    EXPECT_FALSE(copy_end_result.has_value());
    EXPECT_EQ(ErrorCode::INVALID_PARAMS, copy_end_result.error());

    // Test Case 5: MoveEnd, should success.
    move_end_result = service_->MoveEnd(client_id, key, "default");
    EXPECT_TRUE(move_end_result.has_value());

    // Verify we still have 1 replica (the move was successful)
    auto get_result = service_->GetReplicaList(key, "default");
    EXPECT_TRUE(get_result.has_value());
    EXPECT_EQ(1, get_result.value().replicas.size());

    // MoveStart the object from segment_2 to segment_1 again, then unmount
    // segment_2
    move_start_result = service_->MoveStart(client_id, key, "default",
                                            "segment_2", "segment_1");
    ASSERT_TRUE(move_start_result.has_value());

    // Unmount segment_2 to simulate source gone
    auto unmount_result =
        service_->UnmountSegment(context2.segment_id, context2.client_id);
    ASSERT_TRUE(unmount_result.has_value());

    // Test Case 6: MoveEnd, should fail because the source is gone.
    move_end_result = service_->MoveEnd(client_id, key, "default");
    EXPECT_FALSE(move_end_result.has_value());
    EXPECT_EQ(ErrorCode::REPLICA_IS_GONE, move_end_result.error());
}

TEST_F(MasterServiceTest, MoveRevoke) {
    std::unique_ptr<MasterService> service_(new MasterService());

    // Mount 2 segments (segment_1, segment_2) with PrepareSimpleSegment
    [[maybe_unused]] const auto context1 =
        PrepareSimpleSegment(*service_, "segment_1");
    [[maybe_unused]] const auto context2 =
        PrepareSimpleSegment(*service_, "segment_2");

    UUID client_id = generate_uuid();
    UUID invalid_client_id = generate_uuid();

    // Test Case 1: MoveRevoke a non-existent key, should fail.
    auto move_revoke_result =
        service_->MoveRevoke(client_id, "non_existent_key", "default");
    EXPECT_FALSE(move_revoke_result.has_value());
    EXPECT_EQ(ErrorCode::OBJECT_NOT_FOUND, move_revoke_result.error());

    // Put an object with 1 replica and preferred_segment=segment_1 for testing
    std::string key = "test_key";
    uint64_t slice_length = 1024;
    ReplicateConfig config;
    config.replica_num = 1;
    config.preferred_segment = "segment_1";

    auto put_start_result =
        service_->PutStart(client_id, key, "default", slice_length, config);
    ASSERT_TRUE(put_start_result.has_value());
    auto put_end_result =
        service_->PutEnd(client_id, key, "default", ReplicaType::MEMORY);
    ASSERT_TRUE(put_end_result.has_value());

    // Test Case 2: MoveRevoke the object, should fail because there is no
    // ongoing move task.
    move_revoke_result = service_->MoveRevoke(client_id, key, "default");
    EXPECT_FALSE(move_revoke_result.has_value());
    EXPECT_EQ(ErrorCode::OBJECT_NO_REPLICATION_TASK,
              move_revoke_result.error());

    // MoveStart the object from segment_1 to segment_2
    auto move_start_result = service_->MoveStart(client_id, key, "default",
                                                 "segment_1", "segment_2");
    ASSERT_TRUE(move_start_result.has_value());

    // Test Case 3: MoveRevoke with an invalid client id, should fail.
    move_revoke_result =
        service_->MoveRevoke(invalid_client_id, key, "default");
    EXPECT_FALSE(move_revoke_result.has_value());
    EXPECT_EQ(ErrorCode::ILLEGAL_CLIENT, move_revoke_result.error());

    // Test Case 4: CopyRevoke the object, should fail because the ongoing task
    // is Move.
    auto copy_revoke_result = service_->CopyRevoke(client_id, key, "default");
    EXPECT_FALSE(copy_revoke_result.has_value());
    EXPECT_EQ(ErrorCode::INVALID_PARAMS, copy_revoke_result.error());

    // Test Case 5: MoveRevoke, should succeed.
    move_revoke_result = service_->MoveRevoke(client_id, key, "default");
    EXPECT_TRUE(move_revoke_result.has_value());

    // Verify we still have 1 replica (the move was revoked)
    auto get_result = service_->GetReplicaList(key, "default");
    EXPECT_TRUE(get_result.has_value());
    auto& replicas = get_result.value().replicas;
    EXPECT_EQ(1, replicas.size());
    EXPECT_EQ("segment_1", replicas[0]
                               .get_memory_descriptor()
                               .buffer_descriptor.transport_endpoint_);

    // MoveStart the object from segment_1 to segment_2 again, then unmount
    // segment_1
    move_start_result = service_->MoveStart(client_id, key, "default",
                                            "segment_1", "segment_2");
    ASSERT_TRUE(move_start_result.has_value());

    // Unmount segment_1 to simulate source gone
    auto unmount_result =
        service_->UnmountSegment(context1.segment_id, context1.client_id);
    ASSERT_TRUE(unmount_result.has_value());

    // Test Case 6: MoveRevoke, should succeed even though the source is gone.
    move_revoke_result = service_->MoveRevoke(client_id, key, "default");
    EXPECT_TRUE(move_revoke_result.has_value());

    // The object should be erased as there is no replica left.
    get_result = service_->GetReplicaList(key, "default");
    EXPECT_FALSE(get_result.has_value());
}

TEST_F(MasterServiceTest, CreateCopyTaskTest) {
    // Reset storage space metrics.
    MasterMetricManager::instance().reset_allocated_mem_size();
    MasterMetricManager::instance().reset_total_mem_capacity();

    // Create MasterService
    std::unique_ptr<MasterService> service_(new MasterService());

    // Mount 3 segments.
    constexpr size_t kReplicaCnt = 3;
    constexpr size_t kBaseAddr = 0x300000000;
    constexpr size_t kSegmentSize = 1024 * 1024 * 16;  // 16MB
    std::vector<MountedSegmentContext> contexts;
    contexts.reserve(kReplicaCnt);
    for (size_t i = 0; i < kReplicaCnt; ++i) {
        const auto context = PrepareSimpleSegment(
            *service_, "segment_" + std::to_string(i),
            kBaseAddr + static_cast<size_t>(i) * kSegmentSize, kSegmentSize);
        contexts.push_back(context);
    }

    // The client putting the object to segment_0
    auto client_id = generate_uuid();
    std::string key1 = "test_key_1";
    uint64_t value_length = 6 * 1024 * 1024;  // 6MB
    uint64_t slice_length = value_length;
    ReplicateConfig config;
    config.replica_num = 1;
    config.preferred_segment = "segment_0";
    auto put_start_result =
        service_->PutStart(client_id, key1, "default", slice_length, config);
    EXPECT_TRUE(put_start_result.has_value());
    auto put_end_result =
        service_->PutEnd(client_id, key1, "default", ReplicaType::MEMORY);
    EXPECT_TRUE(put_end_result.has_value());

    // Copy key1 to "segment_1" and "segment_2"
    auto copy_result =
        service_->CreateCopyTask(key1, "default", {"segment_1", "segment_2"});
    EXPECT_TRUE(copy_result.has_value());

    // verify the copy task is created and assigned to the client who executed
    // the copy
    auto task = service_->QueryTask(copy_result.value());
    EXPECT_TRUE(task.has_value());
    EXPECT_EQ(TaskType::REPLICA_COPY, task.value().type);
    EXPECT_EQ(contexts[0].client_id, task.value().assigned_client);

    // Copy with empty targets should fail
    auto copy_result1 = service_->CreateCopyTask(key1, "default", {});
    EXPECT_FALSE(copy_result1.has_value());
    EXPECT_EQ(ErrorCode::INVALID_PARAMS, copy_result1.error());

    // Copy not exist key should fail
    auto copy_result2 =
        service_->CreateCopyTask("not_exist_key", "default", {"segment_1"});
    EXPECT_FALSE(copy_result2.has_value());
    EXPECT_EQ(ErrorCode::OBJECT_NOT_FOUND, copy_result2.error());

    // Copy to segment that not mounted should fail
    auto copy_result3 =
        service_->CreateCopyTask(key1, "default", {"not_mounted_segment"});
    EXPECT_FALSE(copy_result3.has_value());
    EXPECT_EQ(ErrorCode::INVALID_PARAMS, copy_result3.error());
}

TEST_F(MasterServiceTest, CreateMoveTaskTest) {
    // Reset storage space metrics.
    MasterMetricManager::instance().reset_allocated_mem_size();
    MasterMetricManager::instance().reset_total_mem_capacity();

    // Create MasterService
    std::unique_ptr<MasterService> service_(new MasterService());

    // Mount 3 segments.
    constexpr size_t kReplicaCnt = 3;
    constexpr size_t kBaseAddr = 0x300000000;
    constexpr size_t kSegmentSize = 1024 * 1024 * 16;  // 16MB
    std::vector<MountedSegmentContext> contexts;
    contexts.reserve(kReplicaCnt);
    for (size_t i = 0; i < kReplicaCnt; ++i) {
        const auto context = PrepareSimpleSegment(
            *service_, "segment_" + std::to_string(i),
            kBaseAddr + static_cast<size_t>(i) * kSegmentSize, kSegmentSize);
        contexts.push_back(context);
    }

    // The client putting the object to segment_0
    auto client_id = generate_uuid();
    std::string key1 = "test_key_1";
    uint64_t value_length = 6 * 1024 * 1024;  // 6MB
    uint64_t slice_length = value_length;
    ReplicateConfig config;
    config.replica_num = 1;
    config.preferred_segment = "segment_0";
    auto put_start_result =
        service_->PutStart(client_id, key1, "default", slice_length, config);
    EXPECT_TRUE(put_start_result.has_value());
    auto put_end_result =
        service_->PutEnd(client_id, key1, "default", ReplicaType::MEMORY);
    EXPECT_TRUE(put_end_result.has_value());

    // Move key1 from "segment_0" to "segment_1"
    auto move_result =
        service_->CreateMoveTask(key1, "default", "segment_0", "segment_1");
    EXPECT_TRUE(move_result.has_value());

    // Verify the move task is created and assigned to the client owning the
    // source segment
    auto task = service_->QueryTask(move_result.value());
    EXPECT_TRUE(task.has_value());
    EXPECT_EQ(TaskType::REPLICA_MOVE, task.value().type);
    EXPECT_EQ(contexts[0].client_id, task.value().assigned_client);

    // Move non-existent key should fail
    auto move_result1 = service_->CreateMoveTask("not_exist_key", "default",
                                                 "segment_0", "segment_1");
    EXPECT_FALSE(move_result1.has_value());
    EXPECT_EQ(ErrorCode::OBJECT_NOT_FOUND, move_result1.error());

    // Move to segment that is same as source should fail
    auto move_result_same =
        service_->CreateMoveTask(key1, "default", "segment_1", "segment_1");
    EXPECT_FALSE(move_result_same.has_value());
    EXPECT_EQ(ErrorCode::INVALID_PARAMS, move_result_same.error());

    // Move to segment that is not mounted should fail
    auto move_result2 = service_->CreateMoveTask(key1, "default", "segment_0",
                                                 "not_mounted_segment");
    EXPECT_FALSE(move_result2.has_value());
    EXPECT_EQ(ErrorCode::INVALID_PARAMS, move_result2.error());

    // Move from segment that does not have the replica should fail
    auto move_result3 =
        service_->CreateMoveTask(key1, "default", "segment_2", "segment_1");
    EXPECT_FALSE(move_result3.has_value());
    EXPECT_EQ(ErrorCode::INVALID_PARAMS, move_result3.error());

    // Move from segment that is not mounted should fail
    auto move_result4 = service_->CreateMoveTask(
        key1, "default", "not_mounted_segment", "segment_1");
    EXPECT_FALSE(move_result4.has_value());
    EXPECT_EQ(ErrorCode::INVALID_PARAMS, move_result4.error());
}

TEST_F(MasterServiceTest, QueryTaskTest) {
    // Reset storage space metrics.
    MasterMetricManager::instance().reset_allocated_mem_size();
    MasterMetricManager::instance().reset_total_mem_capacity();

    // Create MasterService
    std::unique_ptr<MasterService> service_(new MasterService());

    // Mount 3 segments.
    constexpr size_t kReplicaCnt = 3;
    constexpr size_t kBaseAddr = 0x300000000;
    constexpr size_t kSegmentSize = 1024 * 1024 * 16;  // 16MB
    std::vector<MountedSegmentContext> contexts;
    contexts.reserve(kReplicaCnt);
    for (size_t i = 0; i < kReplicaCnt; ++i) {
        const auto context = PrepareSimpleSegment(
            *service_, "segment_" + std::to_string(i),
            kBaseAddr + static_cast<size_t>(i) * kSegmentSize, kSegmentSize);
        contexts.push_back(context);
    }

    // The client putting the object to segment_0
    auto client_id = generate_uuid();
    std::string key1 = "test_key_1";
    uint64_t value_length = 6 * 1024 * 1024;  // 6MB
    uint64_t slice_length = value_length;
    ReplicateConfig config;
    config.replica_num = 1;
    config.preferred_segment = "segment_0";
    auto put_start_result =
        service_->PutStart(client_id, key1, "default", slice_length, config);
    EXPECT_TRUE(put_start_result.has_value());
    auto put_end_result =
        service_->PutEnd(client_id, key1, "default", ReplicaType::MEMORY);
    EXPECT_TRUE(put_end_result.has_value());

    // Move key1 from "segment_0" to "segment_1"
    auto move_result =
        service_->CreateMoveTask(key1, "default", "segment_0", "segment_1");
    EXPECT_TRUE(move_result.has_value());

    // Query non-existent task should fail
    auto query_result = service_->QueryTask(UUID{0, 0});
    EXPECT_FALSE(query_result.has_value());
    EXPECT_EQ(ErrorCode::TASK_NOT_FOUND, query_result.error());

    // Query the move task
    auto query_result_move = service_->QueryTask(move_result.value());
    EXPECT_TRUE(query_result_move.has_value());
    EXPECT_EQ(TaskType::REPLICA_MOVE, query_result_move.value().type);
    EXPECT_EQ(contexts[0].client_id, query_result_move.value().assigned_client);
}

TEST_F(MasterServiceTest, FetchTasksEmptyWhenNoTasks) {
    std::unique_ptr<MasterService> service_(new MasterService());
    const auto ctx0 = PrepareSimpleSegment(*service_, "segment_0", 0x300000000,
                                           kDefaultSegmentSize);

    auto fetch = service_->FetchTasks(ctx0.client_id, /*batch_size=*/16);
    ASSERT_TRUE(fetch.has_value());
    EXPECT_TRUE(fetch->empty());
}

TEST_F(MasterServiceTest, FetchTasksReturnsAssignedTasksOnlyAndDrainsQueue) {
    std::unique_ptr<MasterService> service_(new MasterService());
    const auto ctx0 = PrepareSimpleSegment(*service_, "segment_0", 0x300000000,
                                           kDefaultSegmentSize);
    const auto ctx1 = PrepareSimpleSegment(*service_, "segment_1", 0x400000000,
                                           kDefaultSegmentSize);
    // Put an object with its (only) replica on segment_0 so Copy/Move
    // assignment is deterministic.
    const UUID put_client_id = generate_uuid();
    const std::string key = "fetch_tasks_key_0";

    ReplicateConfig config;
    config.replica_num = 1;
    config.preferred_segment = "segment_0";

    ASSERT_TRUE(service_
                    ->PutStart(put_client_id, key, "default",
                               /*slice_length=*/1024, config)
                    .has_value());
    ASSERT_TRUE(
        service_->PutEnd(put_client_id, key, "default", ReplicaType::MEMORY)
            .has_value());

    // Create two tasks; both should be assigned to the client owning source
    // segment_0.
    auto copy_task_id = service_->CreateCopyTask(key, "default", {"segment_1"});
    ASSERT_TRUE(copy_task_id.has_value());

    auto move_task_id =
        service_->CreateMoveTask(key, "default", "segment_0", "segment_1");
    ASSERT_TRUE(move_task_id.has_value());

    // Fetch from client_0 should get both tasks (order not guaranteed).
    auto fetch0 = service_->FetchTasks(ctx0.client_id, /*batch_size=*/16);
    ASSERT_TRUE(fetch0.has_value());
    ASSERT_EQ(fetch0->size(), 2u);

    std::vector<UUID> fetched_ids;
    fetched_ids.reserve(fetch0->size());
    for (const auto& a : *fetch0) {
        fetched_ids.push_back(
            a.id);  // TaskAssignment is expected to carry id/type/payload
    }

    EXPECT_NE(
        std::find(fetched_ids.begin(), fetched_ids.end(), copy_task_id.value()),
        fetched_ids.end());
    EXPECT_NE(
        std::find(fetched_ids.begin(), fetched_ids.end(), move_task_id.value()),
        fetched_ids.end());

    // Fetch from client_1 should return empty (no tasks assigned to it).
    auto fetch1 = service_->FetchTasks(ctx1.client_id, /*batch_size=*/16);
    ASSERT_TRUE(fetch1.has_value());
    EXPECT_TRUE(fetch1->empty());

    // Fetch again from client_0 should be empty if pop_tasks drains pending
    // queue.
    auto fetch0_again = service_->FetchTasks(ctx0.client_id, /*batch_size=*/16);
    ASSERT_TRUE(fetch0_again.has_value());
    EXPECT_TRUE(fetch0_again->empty());
}

TEST_F(MasterServiceTest, FetchTasksRespectsBatchSize) {
    std::unique_ptr<MasterService> service_(new MasterService());

    const auto ctx0 = PrepareSimpleSegment(*service_, "segment_0", 0x300000000,
                                           kDefaultSegmentSize);
    [[maybe_unused]] const auto ctx1 = PrepareSimpleSegment(
        *service_, "segment_1", 0x400000000, kDefaultSegmentSize);

    const UUID put_client_id = generate_uuid();
    const std::string key = "fetch_tasks_key_1";

    ReplicateConfig config;
    config.replica_num = 1;
    config.preferred_segment = "segment_0";

    ASSERT_TRUE(service_
                    ->PutStart(put_client_id, key, "default",
                               /*slice_length=*/1024, config)
                    .has_value());
    ASSERT_TRUE(
        service_->PutEnd(put_client_id, key, "default", ReplicaType::MEMORY)
            .has_value());

    auto t1 = service_->CreateCopyTask(key, "default", {"segment_1"});
    ASSERT_TRUE(t1.has_value());
    auto t2 =
        service_->CreateMoveTask(key, "default", "segment_0", "segment_1");
    ASSERT_TRUE(t2.has_value());

    auto fetch_first = service_->FetchTasks(ctx0.client_id, /*batch_size=*/1);
    ASSERT_TRUE(fetch_first.has_value());
    ASSERT_EQ(fetch_first->size(), 1u);

    auto fetch_second = service_->FetchTasks(ctx0.client_id, /*batch_size=*/1);
    ASSERT_TRUE(fetch_second.has_value());
    ASSERT_EQ(fetch_second->size(), 1u);

    // Combined should contain both task ids (order not guaranteed).
    std::vector<UUID> ids;
    ids.push_back(fetch_first->at(0).id);
    ids.push_back(fetch_second->at(0).id);

    EXPECT_NE(std::find(ids.begin(), ids.end(), t1.value()), ids.end());
    EXPECT_NE(std::find(ids.begin(), ids.end(), t2.value()), ids.end());

    auto fetch_third = service_->FetchTasks(ctx0.client_id, /*batch_size=*/1);
    ASSERT_TRUE(fetch_third.has_value());
    EXPECT_TRUE(fetch_third->empty());
}

TEST_F(MasterServiceTest, UpdateTaskSuccessFlow) {
    auto service_ = std::make_unique<MasterService>();

    const auto ctx0 = PrepareSimpleSegment(*service_, "segment_0", 0x300000000,
                                           kDefaultSegmentSize);
    [[maybe_unused]] const auto ctx1 = PrepareSimpleSegment(
        *service_, "segment_1", 0x400000000, kDefaultSegmentSize);

    // Put an object with its (only) replica on segment_0 so task assignment is
    // deterministic.
    const UUID put_client_id = generate_uuid();
    const std::string key = "update_task_key_success";

    ReplicateConfig config;
    config.replica_num = 1;
    config.preferred_segment = "segment_0";

    ASSERT_TRUE(service_
                    ->PutStart(put_client_id, key, "default",
                               /*slice_length=*/1024, config)
                    .has_value());
    ASSERT_TRUE(
        service_->PutEnd(put_client_id, key, "default", ReplicaType::MEMORY)
            .has_value());

    // Create a task assigned to client owning segment_0.
    auto task_id_res = service_->CreateCopyTask(key, "default", {"segment_1"});
    ASSERT_TRUE(task_id_res.has_value());
    const UUID task_id = task_id_res.value();

    // Poll once so the task transitions to PROCESSING (typical semantics).
    auto fetched = service_->FetchTasks(ctx0.client_id, /*batch_size=*/16);
    ASSERT_TRUE(fetched.has_value());
    ASSERT_EQ(fetched->size(), 1u);
    EXPECT_EQ(fetched->at(0).id, task_id);

    // Update task to SUCCESS.
    TaskCompleteRequest req{};
    req.id = task_id;
    req.status = TaskStatus::SUCCESS;
    req.message = "done";

    auto update_res = service_->MarkTaskToComplete(ctx0.client_id, req);
    ASSERT_TRUE(update_res.has_value()) << "MarkTaskToComplete failed";

    // Verify task state via QueryTask.
    auto qt = service_->QueryTask(task_id);
    ASSERT_TRUE(qt.has_value());
    EXPECT_EQ(qt->id, task_id);
    EXPECT_EQ(qt->status, TaskStatus::SUCCESS);
    EXPECT_EQ(qt->assigned_client, ctx0.client_id);
    EXPECT_EQ(qt->message, "done");

    // Queue should be drained for that client.
    auto fetched_again =
        service_->FetchTasks(ctx0.client_id, /*batch_size=*/16);
    ASSERT_TRUE(fetched_again.has_value());
    EXPECT_TRUE(fetched_again->empty());
}

TEST_F(MasterServiceTest, UpdateTaskRejectsWrongClient) {
    auto service_ = std::make_unique<MasterService>();

    const auto ctx0 = PrepareSimpleSegment(*service_, "segment_0", 0x300000000,
                                           kDefaultSegmentSize);
    const auto ctx1 = PrepareSimpleSegment(*service_, "segment_1", 0x400000000,
                                           kDefaultSegmentSize);

    const UUID put_client_id = generate_uuid();
    const std::string key = "update_task_wrong_client";

    ReplicateConfig config;
    config.replica_num = 1;
    config.preferred_segment = "segment_0";

    ASSERT_TRUE(service_
                    ->PutStart(put_client_id, key, "default",
                               /*slice_length=*/1024, config)
                    .has_value());
    ASSERT_TRUE(
        service_->PutEnd(put_client_id, key, "default", ReplicaType::MEMORY)
            .has_value());

    auto task_id_res =
        service_->CreateMoveTask(key, "default", "segment_0", "segment_1");
    ASSERT_TRUE(task_id_res.has_value());
    const UUID task_id = task_id_res.value();

    // Poll by the correct client to take the task.
    auto fetched = service_->FetchTasks(ctx0.client_id, /*batch_size=*/16);
    ASSERT_TRUE(fetched.has_value());
    ASSERT_EQ(fetched->size(), 1u);
    EXPECT_EQ(fetched->at(0).id, task_id);

    // Try to update with a different client id, should fail.
    TaskCompleteRequest req{};
    req.id = task_id;
    req.status = TaskStatus::SUCCESS;
    req.message = "should_not_work";

    auto update_res = service_->MarkTaskToComplete(ctx1.client_id, req);
    ASSERT_FALSE(update_res.has_value());
    EXPECT_EQ(update_res.error(), ErrorCode::ILLEGAL_CLIENT);
}

TEST_F(MasterServiceTest, UpdateTaskNotFound) {
    auto service_ = std::make_unique<MasterService>();
    const auto ctx0 = PrepareSimpleSegment(*service_, "segment_0", 0x300000000,
                                           kDefaultSegmentSize);

    TaskCompleteRequest req{};
    req.id = generate_uuid();  // non-existent task id
    req.status = TaskStatus::FAILED;
    req.message = "not_found";

    auto update_res = service_->MarkTaskToComplete(ctx0.client_id, req);
    ASSERT_FALSE(update_res.has_value());
    EXPECT_EQ(update_res.error(), ErrorCode::TASK_NOT_FOUND);
}

TEST_F(MasterServiceTest,
       CreateDrainJobMarksSegmentDrainingAndSkipsAllocation) {
    auto service_config =
        MasterServiceConfig::builder().set_default_kv_lease_ttl(0).build();
    auto service_ = std::make_unique<MasterService>(service_config);

    const auto ctx0 = PrepareSimpleSegment(*service_, "segment_0", 0x300000000,
                                           kDefaultSegmentSize);
    [[maybe_unused]] const auto ctx1 = PrepareSimpleSegment(
        *service_, "segment_1", 0x400000000, kDefaultSegmentSize);

    CreateDrainJobRequest request;
    request.segments = {"segment_0"};
    request.target_segments = {"segment_1"};
    request.max_concurrency = 1;

    auto job_id = service_->CreateDrainJob(request);
    ASSERT_TRUE(job_id.has_value());

    auto segment_status = service_->QuerySegmentStatus("segment_0");
    ASSERT_TRUE(segment_status.has_value());
    EXPECT_EQ(segment_status.value(), SegmentStatus::DRAINING);

    ReplicateConfig config;
    config.replica_num = 1;
    config.preferred_segment = "segment_0";

    auto put_result = service_->PutStart(
        ctx0.client_id, "drain_skip_allocation_key", "default", 1024, config);
    ASSERT_TRUE(put_result.has_value());
    ASSERT_EQ(put_result->size(), 1u);
    EXPECT_EQ(put_result->front()
                  .get_memory_descriptor()
                  .buffer_descriptor.transport_endpoint_,
              "segment_1");
    ASSERT_TRUE(service_
                    ->PutEnd(ctx0.client_id, "drain_skip_allocation_key",
                             "default", ReplicaType::MEMORY)
                    .has_value());
}

TEST_F(MasterServiceTest, DrainJobSchedulesMoveTaskAndConvergesToDrained) {
    auto service_config =
        MasterServiceConfig::builder().set_default_kv_lease_ttl(0).build();
    auto service_ = std::make_unique<MasterService>(service_config);

    const auto ctx0 = PrepareSimpleSegment(*service_, "segment_0", 0x300000000,
                                           kDefaultSegmentSize);
    [[maybe_unused]] const auto ctx1 = PrepareSimpleSegment(
        *service_, "segment_1", 0x400000000, kDefaultSegmentSize);

    const UUID put_client_id = generate_uuid();
    const std::string key =
        PutObjectOnSegment(*service_, put_client_id, "segment_0");

    CreateDrainJobRequest request;
    request.segments = {"segment_0"};
    request.target_segments = {"segment_1"};
    request.max_concurrency = 1;

    auto job_id = service_->CreateDrainJob(request);
    ASSERT_TRUE(job_id.has_value());

    WaitUntil(
        [&] { return ExecutePendingMoveTasks(*service_, ctx0.client_id); });

    WaitUntil([&] {
        auto query = service_->QueryDrainJob(job_id.value());
        return query.has_value() && query->status == JobStatus::SUCCEEDED;
    });

    auto query = service_->QueryDrainJob(job_id.value());
    ASSERT_TRUE(query.has_value());
    EXPECT_EQ(query->status, JobStatus::SUCCEEDED);
    EXPECT_EQ(query->active_units, 0u);
    EXPECT_GE(query->succeeded_units, 1u);

    auto segment_status = service_->QuerySegmentStatus("segment_0");
    ASSERT_TRUE(segment_status.has_value());
    EXPECT_EQ(segment_status.value(), SegmentStatus::DRAINED);

    auto replicas = service_->GetReplicaList(key, "default");
    ASSERT_TRUE(replicas.has_value());
    std::unordered_set<std::string> segment_names;
    for (const auto& replica : replicas->replicas) {
        segment_names.insert(replica.get_memory_descriptor()
                                 .buffer_descriptor.transport_endpoint_);
    }
    EXPECT_TRUE(segment_names.contains("segment_1"));
    EXPECT_FALSE(segment_names.contains("segment_0"));
}

TEST_F(MasterServiceTest, CancelDrainJobRestoresSegmentStatus) {
    auto service_ = std::make_unique<MasterService>();

    [[maybe_unused]] const auto ctx0 = PrepareSimpleSegment(
        *service_, "segment_0", 0x300000000, kDefaultSegmentSize);
    [[maybe_unused]] const auto ctx1 = PrepareSimpleSegment(
        *service_, "segment_1", 0x400000000, kDefaultSegmentSize);

    CreateDrainJobRequest request;
    request.segments = {"segment_0"};
    request.target_segments = {"segment_1"};
    request.max_concurrency = 1;

    auto job_id = service_->CreateDrainJob(request);
    ASSERT_TRUE(job_id.has_value());

    auto draining_status = service_->QuerySegmentStatus("segment_0");
    ASSERT_TRUE(draining_status.has_value());
    EXPECT_EQ(draining_status.value(), SegmentStatus::DRAINING);

    auto cancel_result = service_->CancelDrainJob(job_id.value());
    ASSERT_TRUE(cancel_result.has_value());

    auto job = service_->QueryDrainJob(job_id.value());
    ASSERT_TRUE(job.has_value());
    EXPECT_EQ(job->status, JobStatus::CANCELED);

    auto restored_status = service_->QuerySegmentStatus("segment_0");
    ASSERT_TRUE(restored_status.has_value());
    EXPECT_EQ(restored_status.value(), SegmentStatus::OK);
}

TEST_F(MasterServiceTest, CancelDrainJobRejectsActiveMoveTasks) {
    auto service_config =
        MasterServiceConfig::builder().set_default_kv_lease_ttl(0).build();
    auto service_ = std::make_unique<MasterService>(service_config);

    const auto ctx0 = PrepareSimpleSegment(*service_, "segment_0", 0x300000000,
                                           kDefaultSegmentSize);
    [[maybe_unused]] const auto ctx1 = PrepareSimpleSegment(
        *service_, "segment_1", 0x400000000, kDefaultSegmentSize);

    const UUID put_client_id = generate_uuid();
    PutObjectOnSegment(*service_, put_client_id, "segment_0");

    CreateDrainJobRequest request;
    request.segments = {"segment_0"};
    request.target_segments = {"segment_1"};
    request.max_concurrency = 1;

    auto job_id = service_->CreateDrainJob(request);
    ASSERT_TRUE(job_id.has_value());

    WaitUntil([&] {
        auto fetched = service_->FetchTasks(ctx0.client_id, /*batch_size=*/16);
        return fetched.has_value() && !fetched->empty();
    });

    auto cancel_result = service_->CancelDrainJob(job_id.value());
    ASSERT_FALSE(cancel_result.has_value());
    EXPECT_EQ(cancel_result.error(), ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
}

TEST_F(MasterServiceTest, DrainJobFailsAfterRetryBudgetExhausted) {
    auto service_config =
        MasterServiceConfig::builder().set_default_kv_lease_ttl(0).build();
    auto service_ = std::make_unique<MasterService>(service_config);

    const auto ctx0 = PrepareSimpleSegment(*service_, "segment_0", 0x300000000,
                                           kDefaultSegmentSize);
    [[maybe_unused]] const auto ctx1 = PrepareSimpleSegment(
        *service_, "segment_1", 0x400000000, kDefaultSegmentSize);

    const UUID put_client_id = generate_uuid();
    PutObjectOnSegment(*service_, put_client_id, "segment_0");

    CreateDrainJobRequest request;
    request.segments = {"segment_0"};
    request.target_segments = {"segment_1"};
    request.max_concurrency = 1;

    auto job_id = service_->CreateDrainJob(request);
    ASSERT_TRUE(job_id.has_value());

    for (int attempt = 0; attempt < 3; ++attempt) {
        WaitUntil(
            [&] { return FailPendingMoveTasks(*service_, ctx0.client_id); });
    }

    WaitUntil([&] {
        auto query = service_->QueryDrainJob(job_id.value());
        return query.has_value() && query->status == JobStatus::FAILED;
    });

    auto query = service_->QueryDrainJob(job_id.value());
    ASSERT_TRUE(query.has_value());
    EXPECT_EQ(query->status, JobStatus::FAILED);
    EXPECT_EQ(query->active_units, 0u);
    EXPECT_GE(query->failed_units, 3u);

    auto segment_status = service_->QuerySegmentStatus("segment_0");
    ASSERT_TRUE(segment_status.has_value());
    EXPECT_EQ(segment_status.value(), SegmentStatus::OK);
}

}  // namespace mooncake::test
