#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include <chrono>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "allocator.h"
#include "client_service.h"
#include "test_server_helpers.h"
#include "default_config.h"

DEFINE_string(protocol, "tcp", "Transfer protocol: rdma|tcp");
DEFINE_string(device_name, "", "Device name to use, valid if protocol=rdma");
DEFINE_uint64(default_kv_lease_ttl, mooncake::DEFAULT_DEFAULT_KV_LEASE_TTL,
              "Default lease time for kv objects, must be set to the "
              "same as the master's default_kv_lease_ttl");

namespace mooncake {
namespace testing {

class BatchRemoveTest : public ::testing::Test {
   protected:
    static std::shared_ptr<Client> CreateClient(const std::string& host_name) {
        auto client_opt =
            Client::Create(host_name,       // Local hostname
                           "P2PHANDSHAKE",  // Metadata connection string
                           FLAGS_protocol,  // Transfer protocol
                           std::nullopt,  // RDMA device names (auto-discovery)
                           master_address_  // Master server address (non-HA)
            );

        EXPECT_TRUE(client_opt.has_value())
            << "Failed to create client with host_name: " << host_name;
        if (!client_opt.has_value()) {
            return nullptr;
        }
        return client_opt.value();
    }

    static void SetUpTestSuite() {
        google::InitGoogleLogging("BatchRemoveTest");
        FLAGS_logtostderr = 1;

        if (getenv("PROTOCOL")) FLAGS_protocol = getenv("PROTOCOL");
        if (getenv("DEVICE_NAME")) FLAGS_device_name = getenv("DEVICE_NAME");

        LOG(INFO) << "Protocol: " << FLAGS_protocol
                  << ", Device name: " << FLAGS_device_name;

        if (getenv("DEFAULT_KV_LEASE_TTL")) {
            default_kv_lease_ttl_ = std::stoul(getenv("DEFAULT_KV_LEASE_TTL"));
        } else {
            default_kv_lease_ttl_ = FLAGS_default_kv_lease_ttl;
        }
        LOG(INFO) << "Default KV lease TTL: " << default_kv_lease_ttl_;

        ASSERT_TRUE(master_.Start(InProcMasterConfigBuilder().build()));
        master_address_ = master_.master_address();
        LOG(INFO) << "Started in-proc master at " << master_address_
                  << ", metadata=P2PHANDSHAKE";

        InitializeClients();
        InitializeSegment();
    }

    static void TearDownTestSuite() {
        CleanupSegment();
        CleanupClients();
        master_.Stop();
        google::ShutdownGoogleLogging();
    }

    static void InitializeSegment() {
        ram_buffer_size_ = 512 * 1024 * 1024;
        segment_ptr_ = allocate_buffer_allocator_memory(ram_buffer_size_);
        LOG_ASSERT(segment_ptr_);
        auto mount_result = segment_provider_client_->MountSegment(
            segment_ptr_, ram_buffer_size_, FLAGS_protocol);
        if (!mount_result.has_value()) {
            LOG(ERROR) << "Failed to mount segment: "
                       << toString(mount_result.error());
        }
        LOG(INFO) << "Segment mounted successfully";
    }

    static void InitializeClients() {
        test_client_ = CreateClient("localhost:17813");
        ASSERT_TRUE(test_client_ != nullptr);

        segment_provider_client_ = CreateClient("localhost:17812");
        ASSERT_TRUE(segment_provider_client_ != nullptr);

        client_buffer_allocator_ =
            std::make_unique<SimpleAllocator>(128 * 1024 * 1024);
        auto register_result = test_client_->RegisterLocalMemory(
            client_buffer_allocator_->getBase(), 128 * 1024 * 1024, "cpu:0",
            false, false);
        if (!register_result.has_value()) {
            LOG(ERROR) << "Failed to register local memory: "
                       << toString(register_result.error());
        }

        test_client_ram_buffer_size_ = 512 * 1024 * 1024;
        test_client_segment_ptr_ =
            allocate_buffer_allocator_memory(test_client_ram_buffer_size_);
        LOG_ASSERT(test_client_segment_ptr_);
        auto test_client_mount_result = test_client_->MountSegment(
            test_client_segment_ptr_, test_client_ram_buffer_size_,
            FLAGS_protocol);
        if (!test_client_mount_result.has_value()) {
            LOG(ERROR) << "Failed to mount segment for test_client_: "
                       << toString(test_client_mount_result.error());
        }
        LOG(INFO) << "Test client segment mounted successfully";
    }

    static void CleanupClients() {
        if (test_client_ && test_client_segment_ptr_) {
            test_client_->UnmountSegment(test_client_segment_ptr_,
                                         test_client_ram_buffer_size_);
        }
        test_client_.reset();
        segment_provider_client_.reset();

        if (test_client_segment_ptr_) free(test_client_segment_ptr_);
        if (segment_ptr_) free(segment_ptr_);
    }

    static void CleanupSegment() {
        segment_provider_client_->UnmountSegment(segment_ptr_,
                                                 ram_buffer_size_);
    }

    void PutKey(const std::string& key, const std::string& data) {
        void* buffer = client_buffer_allocator_->allocate(data.size());
        memcpy(buffer, data.data(), data.size());
        std::vector<Slice> slices;
        slices.emplace_back(Slice{buffer, data.size()});
        ReplicateConfig config;
        config.replica_num = 1;
        auto put_result = test_client_->Put(key, slices, config);
        EXPECT_TRUE(put_result.has_value())
            << "Put operation failed: " << toString(put_result.error());
        client_buffer_allocator_->deallocate(buffer, data.size());
    }

    static std::shared_ptr<Client> test_client_;
    static std::shared_ptr<Client> segment_provider_client_;
    static std::unique_ptr<SimpleAllocator> client_buffer_allocator_;
    static void* segment_ptr_;
    static size_t ram_buffer_size_;
    static void* test_client_segment_ptr_;
    static size_t test_client_ram_buffer_size_;
    static uint64_t default_kv_lease_ttl_;
    static InProcMaster master_;
    static std::string master_address_;
};

// Static members initialization
std::shared_ptr<Client> BatchRemoveTest::test_client_ = nullptr;
std::shared_ptr<Client> BatchRemoveTest::segment_provider_client_ = nullptr;
void* BatchRemoveTest::segment_ptr_ = nullptr;
void* BatchRemoveTest::test_client_segment_ptr_ = nullptr;
std::unique_ptr<SimpleAllocator> BatchRemoveTest::client_buffer_allocator_ =
    nullptr;
size_t BatchRemoveTest::ram_buffer_size_ = 0;
size_t BatchRemoveTest::test_client_ram_buffer_size_ = 0;
uint64_t BatchRemoveTest::default_kv_lease_ttl_ = 0;
InProcMaster BatchRemoveTest::master_;
std::string BatchRemoveTest::master_address_;

// Test #1: Basic batch remove after put
TEST_F(BatchRemoveTest, BasicFunctionality) {
    std::vector<std::string> keys;
    for (int i = 0; i < 10; ++i) {
        keys.push_back("test_basic_" + std::to_string(i));
        PutKey(keys.back(), "test data");
    }

    auto results = test_client_->BatchRemove(keys);

    ASSERT_EQ(results.size(), keys.size());
    for (size_t i = 0; i < results.size(); ++i) {
        EXPECT_TRUE(results[i].has_value())
            << "Failed to remove key " << keys[i] << ": "
            << toString(results[i].error());
    }
}

// Test #2: Mixed existing and non-existing keys
TEST_F(BatchRemoveTest, MixedExistence) {
    std::vector<std::string> existing_keys;
    for (int i = 0; i < 3; ++i) {
        existing_keys.push_back("test_mixed_exist_" + std::to_string(i));
        PutKey(existing_keys.back(), "data");
    }

    std::vector<std::string> non_existing_keys;
    for (int i = 0; i < 3; ++i) {
        non_existing_keys.push_back("test_mixed_not_exist_" +
                                    std::to_string(i));
    }

    std::vector<std::string> keys = existing_keys;
    keys.insert(keys.end(), non_existing_keys.begin(), non_existing_keys.end());

    auto results = test_client_->BatchRemove(keys);

    ASSERT_EQ(results.size(), keys.size());
    // Verify existing keys removed (have value)
    for (size_t i = 0; i < existing_keys.size(); ++i) {
        EXPECT_TRUE(results[i].has_value())
            << "Existing key " << existing_keys[i] << " should be removed";
    }
    // Verify non-existing keys return error
    for (size_t i = existing_keys.size(); i < results.size(); ++i) {
        EXPECT_FALSE(results[i].has_value())
            << "Non-existing key " << keys[i] << " should return error";
    }
}

// Test #3: Empty key list and duplicate keys
TEST_F(BatchRemoveTest, EmptyAndDuplicate) {
    // Empty list
    std::vector<std::string> empty_keys;
    auto results = test_client_->BatchRemove(empty_keys);
    EXPECT_EQ(results.size(), 0);

    // Duplicate keys
    std::string key = "test_duplicate_key";
    PutKey(key, "data");

    std::vector<std::string> dup_keys = {key, key, key};
    results = test_client_->BatchRemove(dup_keys);

    ASSERT_EQ(results.size(), 3);
    EXPECT_TRUE(results[0].has_value()) << "First remove should succeed";
    // Second and third should fail (key already removed)
    EXPECT_FALSE(results[1].has_value())
        << "Second remove should fail (not found)";
    EXPECT_FALSE(results[2].has_value())
        << "Third remove should fail (not found)";
}

// Test #4: Error code consistency with single remove
TEST_F(BatchRemoveTest, ConsistencyWithSingleRemove) {
    // Test non-existing key
    std::string key1 = "test_consistency_nonexist";

    auto single_result = test_client_->Remove(key1);
    auto batch_results = test_client_->BatchRemove({key1});

    ASSERT_EQ(batch_results.size(), 1);
    // Both should fail
    EXPECT_FALSE(single_result.has_value());
    EXPECT_FALSE(batch_results[0].has_value());
    // Error codes should match
    EXPECT_EQ(single_result.error(), batch_results[0].error());

    // Test existing key
    std::string key2 = "test_consistency_exist";
    PutKey(key2, "data");

    single_result = test_client_->Remove(key2);

    std::string key3 = "test_consistency_exist2";
    PutKey(key3, "data");
    batch_results = test_client_->BatchRemove({key3});

    ASSERT_EQ(batch_results.size(), 1);
    EXPECT_TRUE(single_result.has_value())
        << "Single remove should succeed for existing key";
    EXPECT_TRUE(batch_results[0].has_value())
        << "Batch remove should succeed for existing key";
}

// Test #5: Error code format (should be ErrorCode enum values)
TEST_F(BatchRemoveTest, ErrorCodeFormat) {
    std::string key = "test_error_format";
    // Don't put, so it doesn't exist

    auto results = test_client_->BatchRemove({key});

    ASSERT_EQ(results.size(), 1);
    EXPECT_FALSE(results[0].has_value());
    // Error code should be a valid ErrorCode value
    ErrorCode error_code = results[0].error();
    EXPECT_TRUE(error_code == ErrorCode::OBJECT_NOT_FOUND ||
                error_code == ErrorCode::RPC_FAIL)
        << "Error code should be a valid ErrorCode value, got: "
        << static_cast<int>(error_code);
}

// Test #6: Large batch performance
TEST_F(BatchRemoveTest, LargeBatch) {
    const int count = 1000;
    const std::string data(1024, 'x');
    std::vector<std::string> keys;
    keys.reserve(count);

    // Prepare data
    for (int i = 0; i < count; ++i) {
        keys.push_back("test_large_" + std::to_string(i));
        PutKey(keys.back(), data);
    }

    // Measure time
    auto start = std::chrono::high_resolution_clock::now();
    auto results = test_client_->BatchRemove(keys);
    auto end = std::chrono::high_resolution_clock::now();

    std::chrono::duration<double> elapsed = end - start;

    ASSERT_EQ(results.size(), count);
    int success_count = 0;
    for (const auto& result : results) {
        if (result.has_value()) success_count++;
    }
    EXPECT_EQ(success_count, count)
        << "All " << count << " keys should be removed";
    EXPECT_LT(elapsed.count(), 5.0)
        << "Large batch should complete in reasonable time (" << elapsed.count()
        << "s)";
    LOG(INFO) << "Removed " << count << " keys in " << elapsed.count() << "s ("
              << count / elapsed.count() << " ops/sec)";
}

// Test #7: Keys with special characters
TEST_F(BatchRemoveTest, SpecialCharacters) {
    std::vector<std::string> special_keys = {
        "key with spaces",
        "key/with/slashes",
        "key:with:colons",
        "key#hash",
        "key@at",
        "unicode_\xE4\xB8\xAD\xE6\x96\x87",  // UTF-8 encoded "中文"
    };

    for (const auto& key : special_keys) {
        PutKey(key, "data");
    }

    auto results = test_client_->BatchRemove(special_keys);

    ASSERT_EQ(results.size(), special_keys.size());
    for (size_t i = 0; i < results.size(); ++i) {
        EXPECT_TRUE(results[i].has_value())
            << "Failed to remove key: " << special_keys[i];
    }
}

}  // namespace testing
}  // namespace mooncake

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
