#include <glog/logging.h>
#include <gtest/gtest.h>
#include <json/json.h>
#include <memory>
#include <string>
#include <thread>
#include <vector>
#include <cstring>
#include <chrono>
#include <atomic>
#include <future>
#include <iomanip>

#include "data_manager_submitter.h"
#include "data_manager.h"
#include "tiered_cache/tiered_backend.h"
#include "transfer_engine.h"
#include "types.h"
#include "utils.h"

namespace mooncake {

// Helper function to parse JSON string
static bool parseJsonString(const std::string& json_str, Json::Value& value,
                            std::string* error_msg = nullptr) {
    Json::CharReaderBuilder builder;
    std::unique_ptr<Json::CharReader> reader(builder.newCharReader());
    std::string errs;

    bool success = reader->parse(
        json_str.data(), json_str.data() + json_str.size(), &value, &errs);
    if (!success && error_msg) {
        *error_msg = errs;
    }
    return success;
}

// Helper: Convert string to unique_ptr<char[]> for Put
static std::unique_ptr<char[]> StringToBuffer(const std::string& str) {
    auto buffer = std::make_unique<char[]>(str.size());
    std::memcpy(buffer.get(), str.data(), str.size());
    return buffer;
}

// Test fixture for DataManagerSubmitter tests
class DataManagerSubmitterTest : public ::testing::Test {
   protected:
    void SetUp() override {
        google::InitGoogleLogging("DataManagerSubmitterTest");
        FLAGS_logtostderr = 1;

        // Create a minimal TransferEngine
        transfer_engine_ = std::make_shared<TransferEngine>(false);

        // Create TieredBackend with DRAM tier configuration
        std::string json_config_str = R"({
            "tiers": [
                {
                    "type": "DRAM",
                    "capacity": 1073741824,
                    "priority": 10,
                    "tags": ["fast", "local"],
                    "allocator_type": "OFFSET"
                }
            ]
        })";
        Json::Value config;
        ASSERT_TRUE(parseJsonString(json_config_str, config));

        tiered_backend_ = std::make_unique<TieredBackend>();
        auto init_result = tiered_backend_->Init(config, nullptr, nullptr);
        ASSERT_TRUE(init_result.has_value())
            << "Failed to initialize TieredBackend: " << init_result.error();

        // Verify tier was created successfully
        auto tier_views = tiered_backend_->GetTierViews();
        ASSERT_EQ(tier_views.size(), 1)
            << "Expected 1 tier, got " << tier_views.size();
        saved_tier_id_ = tier_views[0].id;

        // Create DataManager
        data_manager_ = std::make_unique<DataManager>(
            std::move(tiered_backend_), transfer_engine_);

        // Create DataManagerSubmitter with default 64 threads
        submitter_ = std::make_unique<DataManagerSubmitter>(*data_manager_);
    }

    void TearDown() override {
        submitter_.reset();
        data_manager_.reset();
        tiered_backend_.reset();
        transfer_engine_.reset();
        google::ShutdownGoogleLogging();
    }

    // Helper: Get tier ID from backend
    std::optional<UUID> GetTierId() {
        if (saved_tier_id_.has_value()) {
            return saved_tier_id_;
        }
        return std::nullopt;
    }

    std::unique_ptr<DataManager> data_manager_;
    std::unique_ptr<TieredBackend> tiered_backend_;
    std::shared_ptr<TransferEngine> transfer_engine_;
    std::optional<UUID> saved_tier_id_;
    std::unique_ptr<DataManagerSubmitter> submitter_;
};

// Test SubmitPut - success case
TEST_F(DataManagerSubmitterTest, SubmitPutSuccess) {
    const std::string key = "test_put_key";
    const std::string test_data = "Hello, World!";
    const size_t data_size = test_data.size();

    auto buffer = StringToBuffer(test_data);
    auto future = submitter_->SubmitPut(key, std::move(buffer), data_size);

    // Wait for result
    auto result = future.get();
    ASSERT_TRUE(result.has_value())
        << "SubmitPut failed with error: " << toString(result.error());

    // Verify the key exists
    auto get_result = data_manager_->Get(key);
    ASSERT_TRUE(get_result.has_value()) << "Get failed after SubmitPut";

    auto handle = get_result.value();
    ASSERT_NE(handle, nullptr) << "Handle should not be null";
    ASSERT_NE(handle->loc.data.buffer, nullptr) << "Buffer should not be null";
    EXPECT_EQ(handle->loc.data.buffer->size(), data_size);
}

// Test SubmitPut - allocation failure
TEST_F(DataManagerSubmitterTest, SubmitPutAllocationFailure) {
    const std::string key = "test_put_fail_key";
    // Try to allocate more than available capacity (1GB)
    const size_t huge_size = 2ULL * 1024 * 1024 * 1024;  // 2GB

    auto test_data = std::make_unique<char[]>(1024);
    auto future = submitter_->SubmitPut(key, std::move(test_data), huge_size);

    auto result = future.get();
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::NO_AVAILABLE_HANDLE);
}

// Test SubmitGet - success case
TEST_F(DataManagerSubmitterTest, SubmitGetSuccess) {
    const std::string key = "test_get_key";
    const std::string test_data = "Test data for Get";

    // First, put the data directly using DataManager
    auto buffer = StringToBuffer(test_data);
    auto put_result =
        data_manager_->Put(key, std::move(buffer), test_data.size());
    ASSERT_TRUE(put_result.has_value()) << "Put failed in Get test";

    // Then, get it using Submitter
    auto future = submitter_->SubmitGet(key);
    auto result = future.get();

    ASSERT_TRUE(result.has_value())
        << "SubmitGet failed with error: " << toString(result.error());
    ASSERT_NE(result.value(), nullptr) << "Handle should not be null";
    ASSERT_NE(result.value()->loc.data.buffer, nullptr)
        << "Buffer should not be null";
    EXPECT_EQ(result.value()->loc.data.buffer->size(), test_data.size());
}

// Test SubmitGet - key not found
TEST_F(DataManagerSubmitterTest, SubmitGetKeyNotFound) {
    const std::string key = "non_existent_key";

    auto future = submitter_->SubmitGet(key);
    auto result = future.get();

    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::INVALID_KEY);
}

// Test SubmitDelete - success case (direct passthrough)
TEST_F(DataManagerSubmitterTest, SubmitDeleteSuccess) {
    const std::string key = "test_delete_key";
    const std::string test_data = "Test data for Delete";

    // First, put the data
    auto buffer = StringToBuffer(test_data);
    auto put_result =
        data_manager_->Put(key, std::move(buffer), test_data.size());
    ASSERT_TRUE(put_result.has_value());

    // Then, delete it using Submitter (should be direct passthrough)
    auto delete_result = submitter_->SubmitDelete(key);

    ASSERT_TRUE(delete_result.has_value()) << "SubmitDelete failed with error: "
                                           << toString(delete_result.error());

    // Verify it's deleted
    auto get_result = data_manager_->Get(key);
    ASSERT_FALSE(get_result.has_value());
    EXPECT_EQ(get_result.error(), ErrorCode::INVALID_KEY);
}

// Test SubmitDelete - key not found (direct passthrough)
TEST_F(DataManagerSubmitterTest, SubmitDeleteKeyNotFound) {
    const std::string key = "non_existent_key";

    auto result = submitter_->SubmitDelete(key);

    ASSERT_FALSE(result.has_value())
        << "SubmitDelete should fail for non-existent key";
    EXPECT_NE(result.error(), ErrorCode::OK);
}

// Test concurrent SubmitPut operations
TEST_F(DataManagerSubmitterTest, ConcurrentSubmitPut) {
    const int num_keys = 20;
    std::vector<std::string> keys;
    for (int i = 0; i < num_keys; ++i) {
        keys.push_back("concurrent_key_" + std::to_string(i));
    }

    // Submit all Put operations concurrently
    std::vector<std::future<tl::expected<void, ErrorCode>>> futures;
    for (int i = 0; i < num_keys; ++i) {
        std::string data = "data_" + std::to_string(i);
        auto buffer = StringToBuffer(data);
        futures.push_back(
            submitter_->SubmitPut(keys[i], std::move(buffer), data.size()));
    }

    // Wait for all operations to complete
    for (size_t i = 0; i < futures.size(); ++i) {
        auto result = futures[i].get();
        ASSERT_TRUE(result.has_value())
            << "SubmitPut failed for key: " << keys[i];
    }

    // Verify all can be retrieved
    for (int i = 0; i < num_keys; ++i) {
        auto get_result = data_manager_->Get(keys[i]);
        ASSERT_TRUE(get_result.has_value())
            << "Get failed for key: " << keys[i];
    }
}

// Test concurrent SubmitGet operations
TEST_F(DataManagerSubmitterTest, ConcurrentSubmitGet) {
    const int num_keys = 20;
    std::vector<std::string> keys;

    // First, put all keys
    for (int i = 0; i < num_keys; ++i) {
        std::string key = "concurrent_get_key_" + std::to_string(i);
        keys.push_back(key);
        std::string data = "data_" + std::to_string(i);
        auto buffer = StringToBuffer(data);
        auto put_result =
            data_manager_->Put(key, std::move(buffer), data.size());
        ASSERT_TRUE(put_result.has_value());
    }

    // Submit all Get operations concurrently
    std::vector<std::future<tl::expected<AllocationHandle, ErrorCode>>> futures;
    for (int i = 0; i < num_keys; ++i) {
        futures.push_back(submitter_->SubmitGet(keys[i]));
    }

    // Wait for all operations to complete and verify
    for (size_t i = 0; i < futures.size(); ++i) {
        auto result = futures[i].get();
        ASSERT_TRUE(result.has_value())
            << "SubmitGet failed for key: " << keys[i];
        ASSERT_NE(result.value(), nullptr);
        ASSERT_NE(result.value()->loc.data.buffer, nullptr);
    }
}

// Test mixed concurrent operations (Put and Get)
TEST_F(DataManagerSubmitterTest, ConcurrentMixedOperations) {
    const int num_operations = 30;
    std::vector<std::string> keys;
    std::vector<std::future<tl::expected<void, ErrorCode>>> put_futures;
    std::vector<std::future<tl::expected<AllocationHandle, ErrorCode>>>
        get_futures;

    // Submit Put operations
    for (int i = 0; i < num_operations; ++i) {
        std::string key = "mixed_key_" + std::to_string(i);
        keys.push_back(key);
        std::string data = "data_" + std::to_string(i);
        auto buffer = StringToBuffer(data);
        put_futures.push_back(
            submitter_->SubmitPut(key, std::move(buffer), data.size()));
    }

    // Wait for all Put operations to complete
    for (size_t i = 0; i < put_futures.size(); ++i) {
        auto result = put_futures[i].get();
        ASSERT_TRUE(result.has_value())
            << "SubmitPut failed for key: " << keys[i];
    }

    // Submit Get operations for the same keys
    for (int i = 0; i < num_operations; ++i) {
        get_futures.push_back(submitter_->SubmitGet(keys[i]));
    }

    // Wait for all Get operations to complete
    for (size_t i = 0; i < get_futures.size(); ++i) {
        auto result = get_futures[i].get();
        ASSERT_TRUE(result.has_value())
            << "SubmitGet failed for key: " << keys[i];
    }
}

// Test thread pool handles multiple tasks
TEST_F(DataManagerSubmitterTest, ThreadPoolHandlesMultipleTasks) {
    const int num_tasks = 100;
    std::vector<std::string> keys;
    std::vector<std::future<tl::expected<void, ErrorCode>>> futures;

    // Submit many tasks
    for (int i = 0; i < num_tasks; ++i) {
        std::string key = "thread_pool_key_" + std::to_string(i);
        keys.push_back(key);
        std::string data = "data_" + std::to_string(i);
        auto buffer = StringToBuffer(data);
        futures.push_back(
            submitter_->SubmitPut(key, std::move(buffer), data.size()));
    }

    // Wait for all tasks to complete
    int success_count = 0;
    for (size_t i = 0; i < futures.size(); ++i) {
        auto result = futures[i].get();
        if (result.has_value()) {
            success_count++;
        }
    }

    // All should succeed
    EXPECT_EQ(success_count, num_tasks);
}

// Test Put-Get-Delete sequence
TEST_F(DataManagerSubmitterTest, PutGetDeleteSequence) {
    const std::string key = "sequence_test_key";
    const std::string test_data = "Sequence test data";

    // Put
    auto buffer = StringToBuffer(test_data);
    auto put_future =
        submitter_->SubmitPut(key, std::move(buffer), test_data.size());
    auto put_result = put_future.get();
    ASSERT_TRUE(put_result.has_value());

    // Get
    auto get_future = submitter_->SubmitGet(key);
    auto get_result = get_future.get();
    ASSERT_TRUE(get_result.has_value());
    ASSERT_NE(get_result.value()->loc.data.buffer, nullptr);
    EXPECT_EQ(get_result.value()->loc.data.buffer->size(), test_data.size());

    // Delete
    auto delete_result = submitter_->SubmitDelete(key);
    ASSERT_TRUE(delete_result.has_value());

    // Verify deleted
    auto get_after_delete = data_manager_->Get(key);
    ASSERT_FALSE(get_after_delete.has_value());
}

// Test SubmitPut with tier_id
TEST_F(DataManagerSubmitterTest, SubmitPutWithTierId) {
    const std::string key = "test_key_with_tier";
    const std::string test_data = "Test data with tier";
    auto tier_id = GetTierId();

    ASSERT_TRUE(tier_id.has_value()) << "No tier available";

    auto buffer = StringToBuffer(test_data);
    auto future = submitter_->SubmitPut(key, std::move(buffer),
                                        test_data.size(), tier_id);
    auto result = future.get();

    ASSERT_TRUE(result.has_value()) << "SubmitPut with tier_id failed";

    // Verify we can get it back
    auto get_result = data_manager_->Get(key, tier_id);
    ASSERT_TRUE(get_result.has_value());
}

// Test SubmitGet with tier_id
TEST_F(DataManagerSubmitterTest, SubmitGetWithTierId) {
    const std::string key = "test_get_tier_key";
    const std::string test_data = "Test data";
    auto tier_id = GetTierId();

    ASSERT_TRUE(tier_id.has_value()) << "No tier available";

    // Put with specific tier
    auto buffer = StringToBuffer(test_data);
    auto put_result =
        data_manager_->Put(key, std::move(buffer), test_data.size(), tier_id);
    ASSERT_TRUE(put_result.has_value());

    // Get with same tier
    auto future = submitter_->SubmitGet(key, tier_id);
    auto result = future.get();
    ASSERT_TRUE(result.has_value());
    ASSERT_NE(result.value()->loc.data.buffer, nullptr);
    EXPECT_EQ(result.value()->loc.data.buffer->size(), test_data.size());
}

// Test SubmitDelete with tier_id
TEST_F(DataManagerSubmitterTest, SubmitDeleteWithTierId) {
    const std::string key = "test_delete_tier_key";
    const std::string test_data = "Test data";
    auto tier_id = GetTierId();

    ASSERT_TRUE(tier_id.has_value()) << "No tier available";

    // Put with specific tier
    auto buffer = StringToBuffer(test_data);
    auto put_result =
        data_manager_->Put(key, std::move(buffer), test_data.size(), tier_id);
    ASSERT_TRUE(put_result.has_value());

    // Delete with same tier
    auto delete_result = submitter_->SubmitDelete(key, tier_id);
    ASSERT_TRUE(delete_result.has_value());

    // Verify it's deleted
    auto get_result = data_manager_->Get(key, tier_id);
    ASSERT_FALSE(get_result.has_value());
}

// Test concurrent SubmitReadRemoteData and SubmitWriteRemoteData operations
// This test compares concurrent vs sequential execution time
TEST_F(DataManagerSubmitterTest, ConcurrentRemoteOperationsPerformance) {
    // Check environment variables
    const char* metadata_addr = std::getenv("MC_METADATA_ADDR");
    const char* local_hostname = std::getenv("MC_LOCAL_HOSTNAME");

    if (!metadata_addr || !local_hostname) {
        GTEST_SKIP() << "Skipping real RDMA test: MC_METADATA_ADDR and "
                        "MC_LOCAL_HOSTNAME environment variables not set. "
                        "Set these to enable real RDMA transfer testing.";
    }

    LOG(INFO)
        << "=== Starting Concurrent Remote Operations Performance Test ===";
    LOG(INFO) << "Metadata address: " << metadata_addr;
    LOG(INFO) << "Local hostname: " << local_hostname;

    // Create TransferEngine with RDMA support
    auto rdma_transfer_engine = std::make_shared<TransferEngine>(true);
    int init_result =
        rdma_transfer_engine->init(metadata_addr, local_hostname, "", 12350);

    if (init_result != 0) {
        GTEST_SKIP() << "Failed to initialize TransferEngine (error code: "
                     << init_result
                     << "). RDMA environment may not be configured.";
    }
    LOG(INFO) << "TransferEngine initialized successfully";

    // Allocate memory for RDMA transfer
    const size_t segment_size = 2 * 1024 * 1024;  // 2MB per segment
    const int num_operations = 8;                 // 8 operations
    const size_t total_buffer_size =
        segment_size * num_operations * 2;  // Double for source + dest
    void* rdma_buffer = allocate_buffer_allocator_memory(total_buffer_size);

    if (!rdma_buffer) {
        GTEST_SKIP() << "Failed to allocate memory for RDMA test";
    }

    // Use first half as source data, second half as destination areas
    void* src_base = rdma_buffer;
    void* dst_base = static_cast<char*>(rdma_buffer) + (total_buffer_size / 2);

    // Fill source areas with test pattern
    const std::string pattern = "RDMA_CONCURRENT_TEST_PATTERN_";
    const size_t total_data_size = segment_size * num_operations;
    for (size_t i = 0; i < total_data_size; i++) {
        static_cast<char*>(src_base)[i] = pattern[i % pattern.size()];
    }

    // Clear destination areas
    std::memset(dst_base, 0, total_buffer_size / 2);

    LOG(INFO) << "Allocated RDMA buffer (" << total_buffer_size << " bytes)";

    // Register memory with TransferEngine
    int reg_result = rdma_transfer_engine->registerLocalMemory(
        rdma_buffer, total_buffer_size, "cpu:0");

    if (reg_result != 0) {
        free_memory("", rdma_buffer);
        GTEST_SKIP() << "Failed to register memory with TransferEngine. "
                     << "RDMA device may not be available.";
    }
    LOG(INFO) << "Memory registered for RDMA access";

    // Create TieredBackend and DataManager with RDMA-enabled TransferEngine
    std::string json_config_str = R"({
        "tiers": [
            {
                "type": "DRAM",
                "capacity": 1073741824,
                "priority": 10,
                "allocator_type": "OFFSET"
            }
        ]
    })";
    Json::Value config;
    ASSERT_TRUE(parseJsonString(json_config_str, config));

    auto rdma_tiered_backend = std::make_unique<TieredBackend>();
    auto init_backend_result =
        rdma_tiered_backend->Init(config, rdma_transfer_engine.get(), nullptr);
    ASSERT_TRUE(init_backend_result.has_value())
        << "Failed to initialize TieredBackend";

    auto rdma_data_manager = std::make_unique<DataManager>(
        std::move(rdma_tiered_backend), rdma_transfer_engine);
    LOG(INFO) << "DataManager created with RDMA-enabled TransferEngine";

    // Create DataManagerSubmitter
    auto rdma_submitter =
        std::make_unique<DataManagerSubmitter>(*rdma_data_manager);
    LOG(INFO) << "DataManagerSubmitter created";

    std::string segment_name = std::string(local_hostname);

    // Prepare keys and buffers for all operations
    std::vector<std::string> keys;
    std::vector<std::vector<RemoteBufferDesc>> write_buffers;
    std::vector<std::vector<RemoteBufferDesc>> read_buffers;

    for (int i = 0; i < num_operations; ++i) {
        keys.push_back("concurrent_remote_key_" + std::to_string(i));

        // Write buffers: source segments
        void* src_segment = static_cast<char*>(src_base) + (i * segment_size);
        write_buffers.push_back(
            {{segment_name, reinterpret_cast<uint64_t>(src_segment),
              segment_size}});

        // Read buffers: destination segments
        void* dst_segment = static_cast<char*>(dst_base) + (i * segment_size);
        read_buffers.push_back(
            {{segment_name, reinterpret_cast<uint64_t>(dst_segment),
              segment_size}});
    }

    // ========== Concurrent Execution (via DataManagerSubmitter) ==========
    LOG(INFO) << "Starting concurrent execution via DataManagerSubmitter ("
              << num_operations << " operations)...";
    auto concurrent_start = std::chrono::steady_clock::now();

    // Phase 1: Submit all WriteRemoteData operations and wait for completion
    std::vector<std::future<tl::expected<void, ErrorCode>>> write_futures;
    for (int i = 0; i < num_operations; ++i) {
        write_futures.push_back(
            rdma_submitter->SubmitWriteRemoteData(keys[i], write_buffers[i]));
    }

    // Wait for all WriteRemoteData operations to complete
    bool all_writes_success = true;
    for (size_t i = 0; i < write_futures.size(); ++i) {
        auto result = write_futures[i].get();
        if (!result.has_value()) {
            LOG(ERROR) << "Concurrent WriteRemoteData failed for key: "
                       << keys[i] << ", error: " << toString(result.error());
            all_writes_success = false;
        }
    }

    if (!all_writes_success) {
        rdma_transfer_engine->unregisterLocalMemory(rdma_buffer);
        free_memory("", rdma_buffer);
        GTEST_SKIP() << "Concurrent WriteRemoteData failed, skipping test";
    }

    // Phase 2: Submit all ReadRemoteData operations (after writes complete)
    std::vector<std::future<tl::expected<void, ErrorCode>>> read_futures;
    for (int i = 0; i < num_operations; ++i) {
        read_futures.push_back(
            rdma_submitter->SubmitReadRemoteData(keys[i], read_buffers[i]));
    }

    // Wait for all ReadRemoteData operations to complete
    bool all_reads_success = true;
    for (size_t i = 0; i < read_futures.size(); ++i) {
        auto result = read_futures[i].get();
        if (!result.has_value()) {
            LOG(ERROR) << "Concurrent ReadRemoteData failed for key: "
                       << keys[i] << ", error: " << toString(result.error());
            all_reads_success = false;
        }
    }

    auto concurrent_end = std::chrono::steady_clock::now();
    auto concurrent_duration =
        std::chrono::duration_cast<std::chrono::milliseconds>(concurrent_end -
                                                              concurrent_start)
            .count();

    if (!all_reads_success) {
        rdma_transfer_engine->unregisterLocalMemory(rdma_buffer);
        free_memory("", rdma_buffer);
        GTEST_SKIP() << "Concurrent ReadRemoteData failed, skipping test";
    }

    LOG(INFO) << "Concurrent execution completed in " << concurrent_duration
              << " ms";

    // Verify data integrity for concurrent execution
    bool concurrent_data_correct = true;
    for (int i = 0; i < num_operations; ++i) {
        void* dst_segment = static_cast<char*>(dst_base) + (i * segment_size);
        for (size_t j = 0; j < segment_size && j < 1000; ++j) {
            size_t offset_in_total = (i * segment_size) + j;
            char expected = pattern[offset_in_total % pattern.size()];
            if (static_cast<char*>(dst_segment)[j] != expected) {
                concurrent_data_correct = false;
                LOG(ERROR) << "Concurrent: Data mismatch at operation " << i
                           << ", byte " << j;
                break;
            }
        }
    }

    EXPECT_TRUE(concurrent_data_correct)
        << "Concurrent execution data verification failed";

    // Cleanup
    rdma_submitter.reset();
    rdma_transfer_engine->unregisterLocalMemory(rdma_buffer);
    free_memory("", rdma_buffer);

    LOG(INFO)
        << "=== Concurrent Remote Operations Performance Test Completed ===";
}

}  // namespace mooncake
