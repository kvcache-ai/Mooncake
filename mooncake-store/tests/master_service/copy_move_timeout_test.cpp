#include "fixture.h"

namespace mooncake::test {
TEST_F(MasterServiceTest, DiscardTimeoutCopyMove) {
    const uint64_t kv_lease_ttl = 100;
    const uint64_t client_live_ttl = 600;
    const uint64_t put_discard_timeout = 1;
    const uint64_t put_release_timeout = 2;
    auto service_config =
        MasterServiceConfig::builder()
            .set_default_kv_lease_ttl(kv_lease_ttl)
            .set_client_live_ttl_sec(client_live_ttl)
            .set_put_start_discard_timeout_sec(put_discard_timeout)
            .set_put_start_release_timeout_sec(put_release_timeout)
            .build();
    std::unique_ptr<MasterService> service_(new MasterService(service_config));

    // Mount 2 segments (segment_1, segment_2) with PrepareSimpleSegment, each
    // 16 MB
    constexpr size_t kBaseAddr = 0x100000000;
    constexpr size_t kSegmentSize = 16 * 1024 * 1024;  // 16 MB
    [[maybe_unused]] const auto context1 =
        PrepareSimpleSegment(*service_, "segment_1", kBaseAddr, kSegmentSize);
    [[maybe_unused]] const auto context2 =
        PrepareSimpleSegment(*service_, "segment_2", kBaseAddr, kSegmentSize);

    UUID client_id = generate_uuid();

    const std::string copy_key = "copy_key";
    const std::string move_key = "move_key";
    uint64_t slice_length = 1024 * 1024;
    ReplicateConfig config;
    config.replica_num = 1;
    config.preferred_segment = "segment_1";

    // Put two objects for move and copy tests.
    auto put_start_result = service_->PutStart(client_id, copy_key, "default",
                                               slice_length, config);
    ASSERT_TRUE(put_start_result.has_value());
    auto put_end_result =
        service_->PutEnd(client_id, copy_key, "default", ReplicaType::MEMORY);
    ASSERT_TRUE(put_end_result.has_value());

    put_start_result = service_->PutStart(client_id, move_key, "default",
                                          slice_length, config);
    ASSERT_TRUE(put_start_result.has_value());
    put_end_result =
        service_->PutEnd(client_id, move_key, "default", ReplicaType::MEMORY);
    ASSERT_TRUE(put_end_result.has_value());

    // Start copy and move operations.
    auto copy_start_result = service_->CopyStart(client_id, copy_key, "default",
                                                 "segment_1", {"segment_2"});
    ASSERT_TRUE(copy_start_result.has_value());

    auto move_start_result = service_->MoveStart(client_id, move_key, "default",
                                                 "segment_1", "segment_2");
    ASSERT_TRUE(move_start_result.has_value());

    // Wait for the operations timeout.
    std::this_thread::sleep_for(std::chrono::seconds(put_release_timeout));

    // Put more objects to trigger eviction. Do not prefer any segments.
    config.preferred_segment = "";
    for (size_t i = 0; i < 128 * (kSegmentSize * 2 / slice_length); ++i) {
        std::string key = "test_key_" + std::to_string(i);
        auto put_start_result =
            service_->PutStart(client_id, key, "default", slice_length, config);
        if (put_start_result.has_value()) {
            auto put_end_result = service_->PutEnd(client_id, key, "default",
                                                   ReplicaType::MEMORY);
            ASSERT_TRUE(put_end_result.has_value());
        } else {
            // wait for eviction to work
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
    }

    // Try end copy and move operations, should fail because the objects are
    // evicted.
    auto copy_end_result = service_->CopyEnd(client_id, copy_key, "default");
    EXPECT_FALSE(copy_end_result.has_value());
    EXPECT_EQ(copy_end_result.error(), ErrorCode::OBJECT_NOT_FOUND);

    auto move_end_result = service_->MoveEnd(client_id, move_key, "default");
    EXPECT_FALSE(move_end_result.has_value());
    EXPECT_EQ(move_end_result.error(), ErrorCode::OBJECT_NOT_FOUND);
}

}  // namespace mooncake::test
