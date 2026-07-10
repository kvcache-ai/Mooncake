#include <gtest/gtest.h>

#include "ha/snapshot/master_snapshot_codec.h"
#include "master_service.h"
#include "master_config.h"
#include "segment.h"
#include "task_manager.h"

namespace mooncake::ha {

class MasterSnapshotCodecTest : public ::testing::Test {
   protected:
    void SetUp() override {
        // Create a minimal master service for testing
        MasterServiceConfig config;
        config.default_kv_lease_ttl = 10000;
        config.eviction_ratio = 0.1;
        master_service_ = std::make_unique<MasterService>(config);
    }

    void TearDown() override {
        master_service_.reset();
    }

    std::unique_ptr<MasterService> master_service_;
};

TEST_F(MasterSnapshotCodecTest, GetManifestContent) {
    std::string manifest = MasterSnapshotCodec::GetManifestContent();
    EXPECT_EQ(manifest, "messagepack|1.0.0|master");
}

TEST_F(MasterSnapshotCodecTest, EncodeDecodeRoundTrip) {
    // Create codec
    MasterSnapshotCodec codec;

    // Create state view
    MasterSnapshotStateView state_view(
        *master_service_,
        master_service_->segment_manager_,
        master_service_->nof_segment_manager_,
        master_service_->task_manager_);

    // Encode state
    auto encode_result = codec.Encode(state_view);
    ASSERT_TRUE(encode_result.has_value())
        << "Encode failed: " << encode_result.error().message;

    const auto& payloads = encode_result.value();

    // Verify all expected payloads are present
    EXPECT_TRUE(payloads.find("metadata") != payloads.end());
    EXPECT_TRUE(payloads.find("segments") != payloads.end());
    EXPECT_TRUE(payloads.find("task_manager") != payloads.end());

    // Create a new master service for decode
    MasterServiceConfig config;
    config.default_kv_lease_ttl = 10000;
    config.eviction_ratio = 0.1;
    auto target_service = std::make_unique<MasterService>(config);

    // Decode state into new service
    auto decode_result = codec.Decode(target_service.get(), payloads);
    ASSERT_TRUE(decode_result.has_value())
        << "Decode failed: " << decode_result.error().message;
}

TEST_F(MasterSnapshotCodecTest, DecodeWithMissingPayload) {
    MasterSnapshotCodec codec;

    // Create incomplete payload map (missing task_manager)
    std::unordered_map<std::string, std::vector<uint8_t>> incomplete_payloads;
    incomplete_payloads["metadata"] = std::vector<uint8_t>{1, 2, 3};
    incomplete_payloads["segments"] = std::vector<uint8_t>{4, 5, 6};

    auto decode_result = codec.Decode(master_service_.get(), incomplete_payloads);
    EXPECT_FALSE(decode_result.has_value());
    EXPECT_EQ(decode_result.error().code, ErrorCode::DESERIALIZE_FAIL);
}

TEST_F(MasterSnapshotCodecTest, DecodeWithNullService) {
    MasterSnapshotCodec codec;

    std::unordered_map<std::string, std::vector<uint8_t>> payloads;
    auto decode_result = codec.Decode(nullptr, payloads);
    EXPECT_FALSE(decode_result.has_value());
    EXPECT_EQ(decode_result.error().code, ErrorCode::INVALID_PARAMS);
}

}  // namespace mooncake::ha
