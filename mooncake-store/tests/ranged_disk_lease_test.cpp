#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <unistd.h>

#include <chrono>
#include <cstring>
#include <filesystem>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include "master_config.h"
#include "real_client.h"
#include "test_server_helpers.h"
#include "types.h"

DEFINE_string(protocol, "tcp", "Transfer protocol: rdma|tcp");
DEFINE_string(device_name, "", "Device name to use, valid if protocol=rdma");

namespace mooncake {

// Exercises RealClient::execute_ranged_read's DISK/DFS post-transfer lease
// check. BatchReadFileReplicaRanges does not take a QueryResult, so the
// ranged path must reject after a successful window read when the caller's
// lease has already expired — the same contract Client::Get enforces.
class RangedDiskLeaseTest : public ::testing::Test {
   protected:
    void SetUp() override {
        tmp_dir_ = std::filesystem::temp_directory_path() /
                   ("mc_ranged_disk_lease_" + std::to_string(::getpid()));
        std::filesystem::create_directories(tmp_dir_);

        ASSERT_TRUE(master_.Start(InProcMasterConfigBuilder()
                                      .set_root_fs_dir(tmp_dir_.string())
                                      .set_enable_disk_eviction(true)
                                      .set_quota_bytes(64 * 1024)
                                      .build()));
        master_address_ = master_.master_address();

        client_ = RealClient::create();
        ASSERT_EQ(client_->setup_real("localhost:17831", "P2PHANDSHAKE",
                                      16 * 1024 * 1024, 16 * 1024 * 1024,
                                      FLAGS_protocol, "", master_address_),
                  0);
    }

    void TearDown() override {
        if (client_) {
            client_->tearDownAll();
            client_.reset();
        }
        master_.Stop();
        std::error_code ec;
        std::filesystem::remove_all(tmp_dir_, ec);
    }

    std::optional<Replica::Descriptor> WaitForDiskReplica(
        const std::string &key, std::chrono::milliseconds timeout) {
        const auto deadline = std::chrono::steady_clock::now() + timeout;
        while (std::chrono::steady_clock::now() < deadline) {
            for (const auto &replica : client_->get_replica_desc(key)) {
                if (replica.is_disk_replica()) return replica;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
        return std::nullopt;
    }

    testing::InProcMaster master_;
    std::string master_address_;
    std::filesystem::path tmp_dir_;
    std::shared_ptr<RealClient> client_;
};

TEST_F(RangedDiskLeaseTest,
       DiskRangeReadRejectsAfterTransferWhenLeaseAlreadyExpired) {
    constexpr size_t kObjectSize = 4096;
    constexpr size_t kRangeOffset = 512;
    constexpr size_t kRangeSize = 1024;
    static_assert(kRangeOffset + kRangeSize < kObjectSize);

    std::string payload(kObjectSize, '\0');
    for (size_t i = 0; i < payload.size(); ++i) {
        payload[i] = static_cast<char>(i % 251);
    }
    const std::string key = "ranged_disk_lease_expired";
    ASSERT_EQ(client_->put(key, std::span<const char>(payload)), 0);
    auto disk = WaitForDiskReplica(key, std::chrono::seconds(10));
    ASSERT_TRUE(disk.has_value()) << "DISK replica did not appear";

    auto queries = client_->batch_query({key});
    ASSERT_EQ(queries.size(), 1u);
    ASSERT_TRUE(queries[0].has_value());

    // Force the DISK/DFS BatchReadFileReplicaRanges path even when a MEMORY
    // replica is still present: pass a disk-only QueryResult with an
    // explicit expired deadline (same shape as the BatchGet lease regression).
    RealClient::RangedReadMetadata metadata{
        .query_result = QueryResult(
            std::vector<Replica::Descriptor>{*disk},
            std::chrono::steady_clock::now() - std::chrono::seconds(1),
            queries[0]->object_checksum),
        .replica = *disk,
        .total_size = kObjectSize,
    };

    auto destination = client_->allocate_client_buffer(kRangeSize);
    ASSERT_TRUE(destination.has_value());
    std::memset(destination->ptr(), 0, kRangeSize);

    auto result = client_->execute_ranged_read(
        key, destination->ptr(), /*dst_offset=*/0, kRangeOffset, kRangeSize,
        metadata, /*size_is_buffer_capacity=*/false,
        /*verify_checksum=*/false);
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::LEASE_EXPIRED);

    // Bytes may already sit in the destination; a successful transfer after
    // expiry must still not be reported as a successful ranged read.
    EXPECT_EQ(std::memcmp(destination->ptr(), payload.data() + kRangeOffset,
                          kRangeSize),
              0);
}

TEST_F(RangedDiskLeaseTest, DiskRangeReadSucceedsWithLiveLease) {
    constexpr size_t kObjectSize = 4096;
    constexpr size_t kRangeOffset = 256;
    constexpr size_t kRangeSize = 768;
    static_assert(kRangeOffset + kRangeSize < kObjectSize);

    std::string payload(kObjectSize, '\0');
    for (size_t i = 0; i < payload.size(); ++i) {
        payload[i] = static_cast<char>((i * 17) % 251);
    }
    const std::string key = "ranged_disk_lease_live";
    ASSERT_EQ(client_->put(key, std::span<const char>(payload)), 0);
    auto disk = WaitForDiskReplica(key, std::chrono::seconds(10));
    ASSERT_TRUE(disk.has_value()) << "DISK replica did not appear";

    auto queries = client_->batch_query({key});
    ASSERT_EQ(queries.size(), 1u);
    ASSERT_TRUE(queries[0].has_value());

    RealClient::RangedReadMetadata metadata{
        .query_result =
            QueryResult(std::vector<Replica::Descriptor>{*disk},
                        queries[0]->lease_timeout, queries[0]->object_checksum),
        .replica = *disk,
        .total_size = kObjectSize,
    };

    auto destination = client_->allocate_client_buffer(kRangeSize);
    ASSERT_TRUE(destination.has_value());
    std::memset(destination->ptr(), 0, kRangeSize);

    auto result = client_->execute_ranged_read(
        key, destination->ptr(), /*dst_offset=*/0, kRangeOffset, kRangeSize,
        metadata, /*size_is_buffer_capacity=*/false,
        /*verify_checksum=*/false);
    ASSERT_TRUE(result.has_value()) << toString(result.error());
    EXPECT_EQ(*result, static_cast<int64_t>(kRangeSize));
    EXPECT_EQ(std::memcmp(destination->ptr(), payload.data() + kRangeOffset,
                          kRangeSize),
              0);
}

}  // namespace mooncake
