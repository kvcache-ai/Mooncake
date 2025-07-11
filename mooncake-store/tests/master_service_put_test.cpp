#include <glog/logging.h>
#include <gtest/gtest.h>

#include <memory>
#include <vector>

#include "master_service.h"
#include "rpc_service.h"
#include "types.h"

namespace mooncake::test {

class MasterServicePutTest : public ::testing::Test {
   protected:
    std::unique_ptr<MasterService> service_;
    UUID client_id_;
    const testing::TestInfo* test_info_;

    void SetUp() override {
        service_ = std::make_unique<MasterService>();
        client_id_ = generate_uuid();
        test_info_ = ::testing::UnitTest::GetInstance()->current_test_info();
        google::InitGoogleLogging(test_info_->name());
        FLAGS_logtostderr = true;

        constexpr size_t buffer = 0x300000000;
        constexpr size_t size = 1024 * 1024 * 16;
        std::string segment_name = "test_segment";
        Segment segment(generate_uuid(), segment_name, buffer, size);
        auto mount_result = service_->MountSegment(segment, client_id_);
        ASSERT_TRUE(mount_result.has_value());
    }

    void TearDown() override { google::ShutdownGoogleLogging(); }
};

TEST_F(MasterServicePutTest, PutMultipleReplicasEndAllSuccess) {
    std::string key = "test_multi_replica_key";
    std::vector<uint64_t> slice_lengths = {1024};
    ReplicateConfig config;
    config.replica_num = 3;

    auto put_start_result = service_->PutStart(key, slice_lengths, config);
    ASSERT_TRUE(put_start_result.has_value());
    auto replica_list = put_start_result.value();
    ASSERT_EQ(config.replica_num, replica_list.size());

    for (const auto& replica : replica_list) {
        EXPECT_EQ(ReplicaStatus::PROCESSING, replica.status);
    }

    // During put, Get/Remove should fail
    auto get_replica_result = service_->GetReplicaList(key);
    EXPECT_FALSE(get_replica_result.has_value());
    EXPECT_EQ(ErrorCode::REPLICA_IS_NOT_READY, get_replica_result.error());
    auto remove_result = service_->Remove(key);
    EXPECT_FALSE(remove_result.has_value());
    EXPECT_EQ(ErrorCode::REPLICA_IS_NOT_READY, remove_result.error());

    // Test PutEnd with multiple successful results
    std::vector<PutResult> put_results(config.replica_num, PutResult::SUCCESS);
    auto put_end_result = service_->PutEnd(key, put_results);
    EXPECT_TRUE(put_end_result.has_value());

    // Verify replica list after PutEnd
    auto final_get_result = service_->GetReplicaList(key);
    EXPECT_TRUE(final_get_result.has_value());
    replica_list = final_get_result.value();
    EXPECT_EQ(config.replica_num, replica_list.size());
    for (const auto& replica : replica_list) {
        EXPECT_EQ(ReplicaStatus::COMPLETE, replica.status);
    }
}

TEST_F(MasterServicePutTest, PutMultipleReplicasOneFailed) {
    std::string key = "test_multi_replica_one_failed_key";
    std::vector<uint64_t> slice_lengths = {1024};
    ReplicateConfig config;
    config.replica_num = 3;

    auto put_start_result = service_->PutStart(key, slice_lengths, config);
    ASSERT_TRUE(put_start_result.has_value());
    auto replica_list = put_start_result.value();
    ASSERT_EQ(config.replica_num, replica_list.size());

    // Test PutEnd with one failure
    std::vector<PutResult> put_results(config.replica_num, PutResult::SUCCESS);
    put_results[1] = PutResult::FAILED;
    auto put_end_result = service_->PutEnd(key, put_results);
    EXPECT_TRUE(put_end_result.has_value());

    // Verify replica list after PutEnd, the object should be removed
    auto final_get_result = service_->GetReplicaList(key);
    EXPECT_FALSE(final_get_result.has_value());
    EXPECT_EQ(ErrorCode::OBJECT_NOT_FOUND, final_get_result.error());
}

TEST_F(MasterServicePutTest, PutMultipleReplicasEndAllFailed) {
    std::string key = "test_multi_replica_all_failed_key";
    std::vector<uint64_t> slice_lengths = {1024};
    ReplicateConfig config;
    config.replica_num = 3;

    auto put_start_result = service_->PutStart(key, slice_lengths, config);
    ASSERT_TRUE(put_start_result.has_value());
    auto replica_list = put_start_result.value();
    ASSERT_EQ(config.replica_num, replica_list.size());

    // Test PutEnd with all failures
    std::vector<PutResult> put_results(config.replica_num, PutResult::FAILED);
    auto put_end_result = service_->PutEnd(key, put_results);
    EXPECT_TRUE(put_end_result.has_value());

    // Verify replica list after PutEnd, the object should be removed
    auto final_get_result = service_->GetReplicaList(key);
    EXPECT_FALSE(final_get_result.has_value());
    EXPECT_EQ(ErrorCode::OBJECT_NOT_FOUND, final_get_result.error());
}

}  // namespace mooncake::test

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
