#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include <cstdint>
#include <cstring>
#include <memory>
#include <string>
#include <vector>
#include <thread>
#include <chrono>

#include "allocator.h"
#include "client_c.h"
#include "utils.h"

DEFINE_string(protocol, "tcp", "Transfer protocol: rdma|tcp");
DEFINE_string(device_name, "ibp6s0",
              "Device name to use, valid if protocol=rdma");
DEFINE_string(transfer_engine_metadata_url, "http://localhost:8080/metadata",
              "Metadata connection string for transfer engine");
DEFINE_uint64(default_kv_lease_ttl, mooncake::DEFAULT_DEFAULT_KV_LEASE_TTL,
              "Default lease time for kv objects, must be set to the "
              "same as the master's default_kv_lease_ttl");

namespace mooncake {
namespace testing {

// test pure C APIs of client_c.h
class ClientCIntegrationTest : public ::testing::Test {
   protected:
    static client_t CreateClient(const std::string& host_name) {
        client_t client =
            mooncake_client_create(host_name.c_str(),  // Local hostname
                                   FLAGS_transfer_engine_metadata_url
                                       .c_str(),  // Metadata connection string
                                   FLAGS_protocol.c_str(),     // Protocol
                                   FLAGS_device_name.c_str(),  // RDMA devices
                                   "localhost:50051"  // Master server address
            );

        EXPECT_NE(client, nullptr)
            << "Failed to create client with host_name: " << host_name;
        return client;
    }

    static void SetUpTestSuite() {
        // Initialize glog
        google::InitGoogleLogging("ClientCIntegrationTest");
        FLAGS_logtostderr = 1;

        // Override flags from environment variables if present
        if (getenv("PROTOCOL")) FLAGS_protocol = getenv("PROTOCOL");
        if (getenv("DEVICE_NAME")) FLAGS_device_name = getenv("DEVICE_NAME");
        if (getenv("MC_METADATA_SERVER"))
            FLAGS_transfer_engine_metadata_url = getenv("MC_METADATA_SERVER");

        LOG(INFO) << "Protocol: " << FLAGS_protocol
                  << ", Device name: " << FLAGS_device_name
                  << ", Metadata URL: " << FLAGS_transfer_engine_metadata_url;

        if (getenv("DEFAULT_KV_LEASE_TTL")) {
            default_kv_lease_ttl_ = std::stoul(getenv("DEFAULT_KV_LEASE_TTL"));
        } else {
            default_kv_lease_ttl_ = FLAGS_default_kv_lease_ttl;
        }
        LOG(INFO) << "Default KV lease TTL: " << default_kv_lease_ttl_;

        InitializeTestClient();
        InitializeSegmentProviderClient();
    }

    static void TearDownTestSuite() {
        CleanupSegment();
        CleanupClients();
        google::ShutdownGoogleLogging();
    }

    static void InitializeSegmentProviderClient() {
        // segment_provider_client_ only provide segments.
        segment_provider_client_ = CreateClient("localhost:17912");
        ASSERT_NE(segment_provider_client_, nullptr);

        // mount segment for segment_provider_client_
        segment_size_ = 512 * 1024 * 1024;  // 512 MB
        segment_ptr_ = mooncake_allocate_segment_memory(segment_size_);
        LOG_ASSERT(segment_ptr_);

        auto mount_result = mooncake_client_mount_segment(
            segment_provider_client_, segment_ptr_, segment_size_);
        ASSERT_EQ(mount_result, MOONCAKE_ERROR_OK) << "Failed to mount segment";
        LOG(INFO) << "Segment provider client initialized successfully";
    }

    static void InitializeTestClient() {
        // test_client_ is used for testing purposes.
        test_client_ = CreateClient("localhost:17913");
        ASSERT_NE(test_client_, nullptr);

        // register local memory for test_client_
        client_buffer_allocator_ =
            std::make_unique<SimpleAllocator>(128 * 1024 * 1024);
        auto register_result = mooncake_client_register_local_memory(
            test_client_, client_buffer_allocator_->getBase(),
            128 * 1024 * 1024, "cpu:0", false, false);
        ASSERT_EQ(register_result, MOONCAKE_ERROR_OK)
            << "Failed to register local memory";

        // Mount segment for test_client_ as well
        test_client_segment_size_ = 512 * 1024 * 1024;  // 512 MB
        test_client_segment_ptr_ =
            mooncake_allocate_segment_memory(test_client_segment_size_);
        ASSERT_NE(test_client_segment_ptr_, nullptr);

        auto test_client_mount_result = mooncake_client_mount_segment(
            test_client_, test_client_segment_ptr_, test_client_segment_size_);
        ASSERT_EQ(test_client_mount_result, MOONCAKE_ERROR_OK)
            << "Failed to mount segment for test_client_";
        LOG(INFO) << "Test client initialized successfully";
    }

    static void CleanupClients() {
        // cleanup test_client_ first
        if (test_client_) {
            mooncake_client_destroy(test_client_);
            client_buffer_allocator_.reset();
            free(test_client_segment_ptr_);
        }

        // cleanup segment_provider_client_
        if (segment_provider_client_) {
            mooncake_client_destroy(segment_provider_client_);
            free(segment_ptr_);
        }
    }

    static void CleanupSegment() {
        // test unmount_segment and unregister_local_memory
        if (segment_provider_client_ && segment_ptr_) {
            auto unmount_result = mooncake_client_unmount_segment(
                segment_provider_client_, segment_ptr_, segment_size_);
            ASSERT_EQ(unmount_result, MOONCAKE_ERROR_OK)
                << "Failed to unmount segment";
        }

        if (test_client_) {
            // unregister test
            if (client_buffer_allocator_) {
                auto unregister_result =
                    mooncake_client_unregister_local_memory(
                        test_client_, client_buffer_allocator_->getBase(),
                        false);
                ASSERT_EQ(unregister_result, MOONCAKE_ERROR_OK)
                    << "Failed to unregister local memory";
            }
            if (test_client_segment_ptr_) {
                auto unmount_result = mooncake_client_unmount_segment(
                    test_client_, test_client_segment_ptr_,
                    test_client_segment_size_);
                ASSERT_EQ(unmount_result, MOONCAKE_ERROR_OK)
                    << "Failed to unmount segment";
            }
        }
    }

    static client_t test_client_;
    static client_t segment_provider_client_;
    static std::unique_ptr<SimpleAllocator> client_buffer_allocator_;
    static void* segment_ptr_;
    static size_t segment_size_;
    static void* test_client_segment_ptr_;
    static size_t test_client_segment_size_;
    static uint64_t default_kv_lease_ttl_;
};

// Static members initialization
client_t ClientCIntegrationTest::test_client_ = nullptr;
client_t ClientCIntegrationTest::segment_provider_client_ = nullptr;
void* ClientCIntegrationTest::segment_ptr_ = nullptr;
void* ClientCIntegrationTest::test_client_segment_ptr_ = nullptr;
std::unique_ptr<SimpleAllocator>
    ClientCIntegrationTest::client_buffer_allocator_ = nullptr;
size_t ClientCIntegrationTest::segment_size_ = 0;
size_t ClientCIntegrationTest::test_client_segment_size_ = 0;
uint64_t ClientCIntegrationTest::default_kv_lease_ttl_ = 0;

// Test basic Put/Get operations through the C client
TEST_F(ClientCIntegrationTest, BasicPutGetOperations) {
    const std::string test_data = "Hello, World!";
    const std::string key = "test_key_c";

    // Prepare buffer and slice for Put
    void* buffer = client_buffer_allocator_->allocate(test_data.size());
    ASSERT_NE(buffer, nullptr);
    memcpy(buffer, test_data.data(), test_data.size());

    Slice_t put_slice;
    put_slice.ptr = buffer;
    put_slice.size = test_data.size();

    // Test Put operation
    ReplicateConfig_t config;
    config.replica_num = 1;

    auto put_result =
        mooncake_client_put(test_client_, key.c_str(), &put_slice, 1, config);
    ASSERT_EQ(put_result, MOONCAKE_ERROR_OK) << "Put operation failed";
    client_buffer_allocator_->deallocate(buffer, test_data.size());

    // Prepare buffer for Get operation (larger buffer like C++ version)
    void* get_buffer = client_buffer_allocator_->allocate(1 * 1024 * 1024);
    ASSERT_NE(get_buffer, nullptr);

    Slice_t get_slice;
    get_slice.ptr = get_buffer;
    get_slice.size = test_data.size();

    // Test Get operation
    auto get_result =
        mooncake_client_get(test_client_, key.c_str(), &get_slice, 1);
    ASSERT_EQ(get_result, MOONCAKE_ERROR_OK) << "Get operation failed";

    // Verify data
    ASSERT_EQ(get_slice.size, test_data.size());
    ASSERT_EQ(get_slice.ptr, get_buffer);
    ASSERT_EQ(memcmp(get_slice.ptr, test_data.data(), test_data.size()), 0);
    client_buffer_allocator_->deallocate(get_buffer, test_data.size());

    // Put again with the same key, should succeed
    buffer = client_buffer_allocator_->allocate(test_data.size());
    memcpy(buffer, test_data.data(), test_data.size());
    put_slice.ptr = buffer;
    put_slice.size = test_data.size();
    auto put_result2 =
        mooncake_client_put(test_client_, key.c_str(), &put_slice, 1, config);
    ASSERT_EQ(put_result2, MOONCAKE_ERROR_OK) << "Second Put operation failed";

    // Sleep for lease TTL and remove
    std::this_thread::sleep_for(
        std::chrono::milliseconds(default_kv_lease_ttl_));
    auto remove_result = mooncake_client_remove(test_client_, key.c_str());
    ASSERT_EQ(remove_result, MOONCAKE_ERROR_OK) << "Remove operation failed";
    client_buffer_allocator_->deallocate(buffer, test_data.size());
}

// Test Remove operation
TEST_F(ClientCIntegrationTest, RemoveOperation) {
    const std::string test_data = "Test data for removal";
    const std::string key = "remove_test_key_c";

    // Put data first
    void* buffer = client_buffer_allocator_->allocate(test_data.size());
    ASSERT_NE(buffer, nullptr);
    memcpy(buffer, test_data.data(), test_data.size());

    Slice_t slice;
    slice.ptr = buffer;
    slice.size = test_data.size();

    ReplicateConfig_t config;
    config.replica_num = 1;

    auto put_result =
        mooncake_client_put(test_client_, key.c_str(), &slice, 1, config);
    ASSERT_EQ(put_result, MOONCAKE_ERROR_OK) << "Put operation failed";
    client_buffer_allocator_->deallocate(buffer, test_data.size());

    // Remove the data
    auto remove_result = mooncake_client_remove(test_client_, key.c_str());
    ASSERT_EQ(remove_result, MOONCAKE_ERROR_OK) << "Remove operation failed";

    // Verify that the data is removed using IsExist operation
    auto exist_result = mooncake_client_isexist(test_client_, key.c_str());
    ASSERT_NE(exist_result, MOONCAKE_ERROR_OK)
        << "IsExist should not return OK for removed key";

    // Try to get the removed data - should fail
    void* get_buffer = client_buffer_allocator_->allocate(test_data.size());
    ASSERT_NE(get_buffer, nullptr);

    Slice_t get_slice;
    get_slice.ptr = get_buffer;
    get_slice.size = test_data.size();

    auto get_result =
        mooncake_client_get(test_client_, key.c_str(), &get_slice, 1);
    ASSERT_NE(get_result, MOONCAKE_ERROR_OK)
        << "Get should fail for removed key";
    client_buffer_allocator_->deallocate(get_buffer, test_data.size());
}

// Test local preferred allocation strategy
TEST_F(ClientCIntegrationTest, LocalPreferredAllocationTest) {
    const std::string test_data = "Test data for local preferred allocation";
    const std::string key = "local_preferred_test_key_c";

    void* buffer = client_buffer_allocator_->allocate(test_data.size());
    ASSERT_NE(buffer, nullptr);
    memcpy(buffer, test_data.data(), test_data.size());

    Slice_t slice;
    slice.ptr = buffer;
    slice.size = test_data.size();

    ReplicateConfig_t config;
    config.replica_num = 1;
    // Although there is only one segment now, in order to test the preferred
    // allocation logic, we still set it. This will prevent potential
    // compatibility issues in the future.
    config.preferred_segment = "localhost:17912";  // Local segment

    auto put_result =
        mooncake_client_put(test_client_, key.c_str(), &slice, 1, config);
    ASSERT_EQ(put_result, MOONCAKE_ERROR_OK) << "Put operation failed";
    client_buffer_allocator_->deallocate(buffer, test_data.size());

    // Verify data through Get operation
    void* get_buffer = client_buffer_allocator_->allocate(test_data.size());
    ASSERT_NE(get_buffer, nullptr);

    Slice_t get_slice;
    get_slice.ptr = get_buffer;
    get_slice.size = test_data.size();

    auto get_result =
        mooncake_client_get(test_client_, key.c_str(), &get_slice, 1);
    ASSERT_EQ(get_result, MOONCAKE_ERROR_OK) << "Get operation failed";
    ASSERT_EQ(get_slice.size, test_data.size());
    ASSERT_EQ(memcmp(get_slice.ptr, test_data.data(), test_data.size()), 0);
    client_buffer_allocator_->deallocate(get_buffer, test_data.size());

    // Clean up
    std::this_thread::sleep_for(
        std::chrono::milliseconds(default_kv_lease_ttl_));
    auto remove_result = mooncake_client_remove(test_client_, key.c_str());
    ASSERT_EQ(remove_result, MOONCAKE_ERROR_OK) << "Remove operation failed";
}

// Test large allocation operations
TEST_F(ClientCIntegrationTest, LargeAllocateTest) {
    const size_t data_size = 1 * 1024 * 1024;  // 1MB
    const uint64_t kNumBuffers = 5;
    const std::string key = "large_test_key_c";

    ReplicateConfig_t config;
    config.replica_num = 1;

    // Allocate buffers and fill with data
    std::vector<void*> buffers(kNumBuffers);
    std::vector<Slice_t> slices(kNumBuffers);

    for (size_t i = 0; i < kNumBuffers; ++i) {
        buffers[i] = client_buffer_allocator_->allocate(data_size);
        ASSERT_NE(buffers[i], nullptr);

        // Fill with pattern data
        char pattern = 'A' + i;
        memset(buffers[i], pattern, data_size);

        slices[i].ptr = buffers[i];
        slices[i].size = data_size;
    }

    // Put operation
    auto put_result = mooncake_client_put(test_client_, key.c_str(),
                                          slices.data(), kNumBuffers, config);
    ASSERT_EQ(put_result, MOONCAKE_ERROR_OK) << "Put operation failed";

    // Clear buffers before Get
    for (size_t i = 0; i < kNumBuffers; ++i) {
        memset(buffers[i], 0, data_size);
    }

    // Get operation
    auto get_result = mooncake_client_get(test_client_, key.c_str(),
                                          slices.data(), kNumBuffers);
    ASSERT_EQ(get_result, MOONCAKE_ERROR_OK) << "Get operation failed";

    // Verify data
    for (size_t i = 0; i < kNumBuffers; ++i) {
        ASSERT_EQ(slices[i].size, data_size);
        char expected_pattern = 'A' + i;
        char* buffer_data = static_cast<char*>(slices[i].ptr);
        for (size_t j = 0; j < data_size; ++j) {
            ASSERT_EQ(buffer_data[j], expected_pattern)
                << "Data mismatch at buffer " << i << " position " << j;
        }
    }

    // Remove the key
    std::this_thread::sleep_for(
        std::chrono::milliseconds(default_kv_lease_ttl_));
    auto remove_result = mooncake_client_remove(test_client_, key.c_str());
    ASSERT_EQ(remove_result, MOONCAKE_ERROR_OK) << "Remove operation failed";

    // Cleanup
    for (size_t i = 0; i < kNumBuffers; ++i) {
        client_buffer_allocator_->deallocate(buffers[i], data_size);
    }
}

// Test error handling
TEST_F(ClientCIntegrationTest, ErrorHandlingTest) {
    const std::string key = "error_test_key_c";

    // Test Get on non-existent key
    void* buffer = client_buffer_allocator_->allocate(1024);
    ASSERT_NE(buffer, nullptr);

    Slice_t slice;
    slice.ptr = buffer;
    slice.size = 1024;

    auto get_result = mooncake_client_get(test_client_, key.c_str(), &slice, 1);
    ASSERT_NE(get_result, MOONCAKE_ERROR_OK)
        << "Get should fail for non-existent key";

    // Test IsExist on non-existent key
    auto exist_result = mooncake_client_isexist(test_client_, key.c_str());
    ASSERT_NE(exist_result, MOONCAKE_ERROR_OK)
        << "IsExist should fail for non-existent key";

    // Test Remove on non-existent key
    auto remove_result = mooncake_client_remove(test_client_, key.c_str());
    ASSERT_NE(remove_result, MOONCAKE_ERROR_OK)
        << "Remove should fail for non-existent key";

    client_buffer_allocator_->deallocate(buffer, 1024);
}

// Test max slice size
TEST_F(ClientCIntegrationTest, MaxSliceSizeTest) {
    uint64_t max_size = mooncake_max_slice_size();
    ASSERT_GT(max_size, 0) << "Max slice size should be greater than 0";
    LOG(INFO) << "Max slice size: " << max_size << " bytes";
}

// Test batch Put/Get operations through the C client
TEST_F(ClientCIntegrationTest, BatchPutGetOperations) {
    int batch_sz = 100;
    std::vector<std::string> keys;
    std::vector<std::string> test_data_list;
    std::vector<BatchItem_t> batch_items;

    std::vector<Slice_span_t> slice_spans;  // 存有所有 slice_span, 用于内存管理
    std::vector<Slice_t> all_slices;        // 存有所有 slice, 用于内存管理

    for (int i = 0; i < batch_sz; i++) {
        keys.push_back("test_key_batch_put_c_" + std::to_string(i));
        test_data_list.push_back("test_data_c_" + std::to_string(i));
    }

    void* buffer = nullptr;
    void* target_buffer = nullptr;
    batch_items.reserve(batch_sz);
    slice_spans.reserve(batch_sz);
    all_slices.reserve(batch_sz);

    for (int i = 0; i < batch_sz; i++) {
        buffer = client_buffer_allocator_->allocate(test_data_list[i].size());
        ASSERT_NE(buffer, nullptr);
        memcpy(buffer, test_data_list[i].data(), test_data_list[i].size());

        // Create slice
        Slice_t slice;
        slice.ptr = buffer;
        slice.size = test_data_list[i].size();
        all_slices.push_back(slice);

        // Create slice span
        Slice_span_t slice_span;
        slice_span.slices = &all_slices[i];
        slice_span.slices_count = 1;
        slice_spans.push_back(slice_span);

        // Create batch item
        BatchItem_t item;
        item.key = keys[i].c_str();
        item.slices_span = &slice_spans[i];
        batch_items.push_back(item);
    }
    // Test Batch Put operation
    ReplicateConfig_t config;
    config.replica_num = 1;
    auto start = std::chrono::high_resolution_clock::now();
    ErrorCode_span_t batch_put_results = mooncake_client_batch_put(
        test_client_, batch_items.data(), batch_sz, config);
    // Check that all operations succeeded
    for (size_t i = 0; i < batch_put_results.results_count; i++) {
        ASSERT_EQ(batch_put_results.results[i], MOONCAKE_ERROR_OK)
            << "BatchPut operation failed for key " << keys[i];
        client_buffer_allocator_->deallocate(all_slices[i].ptr,
                                             test_data_list[i].size());
    }
    free(batch_put_results.results);
    auto end = std::chrono::high_resolution_clock::now();
    LOG(INFO) << "Time taken for BatchPut: "
              << std::chrono::duration_cast<std::chrono::microseconds>(end -
                                                                       start)
                     .count()
              << "us";

    start = std::chrono::high_resolution_clock::now();
    for (int i = 0; i < batch_sz; i++) {
        target_buffer =
            client_buffer_allocator_->allocate(test_data_list[i].size());
        ASSERT_NE(target_buffer, nullptr);

        Slice_t get_slice;
        get_slice.ptr = target_buffer;
        get_slice.size = test_data_list[i].size();

        auto get_result =
            mooncake_client_get(test_client_, keys[i].c_str(), &get_slice, 1);
        ASSERT_EQ(get_result, MOONCAKE_ERROR_OK)
            << "Get operation failed for key " << keys[i];
        ASSERT_EQ(get_slice.size, test_data_list[i].size());
        ASSERT_EQ(memcmp(get_slice.ptr, test_data_list[i].data(),
                         test_data_list[i].size()),
                  0);
        client_buffer_allocator_->deallocate(target_buffer,
                                             test_data_list[i].size());
    }
    end = std::chrono::high_resolution_clock::now();
    LOG(INFO) << "Time taken for single Get: "
              << std::chrono::duration_cast<std::chrono::microseconds>(end -
                                                                       start)
                     .count()
              << "us";

    // Prepare for Batch Get operation
    std::vector<BatchItem_t> batch_get_items;
    std::vector<Slice_span_t> get_slice_spans;
    std::vector<Slice_t> get_all_slices;

    batch_get_items.reserve(batch_sz);
    get_slice_spans.reserve(batch_sz);
    get_all_slices.reserve(batch_sz);

    for (int i = 0; i < batch_sz; i++) {
        // Allocate buffer for get
        target_buffer =
            client_buffer_allocator_->allocate(test_data_list[i].size());
        ASSERT_NE(target_buffer, nullptr);

        // Create slice
        Slice_t get_slice;
        get_slice.ptr = target_buffer;
        get_slice.size = test_data_list[i].size();
        get_all_slices.push_back(get_slice);

        // Create slice span
        Slice_span_t get_slice_span;
        get_slice_span.slices = &get_all_slices[i];
        get_slice_span.slices_count = 1;
        get_slice_spans.push_back(get_slice_span);

        // Create batch item
        BatchItem_t get_item;
        get_item.key = keys[i].c_str();
        get_item.slices_span = &get_slice_spans[i];
        batch_get_items.push_back(get_item);
    }

    // Test Batch Get operation
    start = std::chrono::high_resolution_clock::now();
    ErrorCode_span_t batch_get_results = mooncake_client_batch_get(
        test_client_, batch_get_items.data(), batch_sz);
    for (size_t i = 0; i < batch_get_results.results_count; i++) {
        ASSERT_EQ(batch_get_results.results[i], MOONCAKE_ERROR_OK)
            << "BatchGet operation failed for key " << keys[i];
    }
    free(batch_get_results.results);
    end = std::chrono::high_resolution_clock::now();

    LOG(INFO) << "Time taken for BatchGet: "
              << std::chrono::duration_cast<std::chrono::microseconds>(end -
                                                                       start)
                     .count()
              << "us";

    // Verify data from BatchGet
    for (int i = 0; i < batch_sz; i++) {
        ASSERT_EQ(get_all_slices[i].size, test_data_list[i].size());
        ASSERT_EQ(memcmp(get_all_slices[i].ptr, test_data_list[i].data(),
                         test_data_list[i].size()),
                  0);
        client_buffer_allocator_->deallocate(get_all_slices[i].ptr,
                                             test_data_list[i].size());
    }
}

// Test batch put with duplicate keys
TEST_F(ClientCIntegrationTest, BatchPutDuplicateKeys) {
    const std::string test_data = "test_data_duplicate_c";
    const std::string key = "duplicate_key_c";

    // Create two identical keys
    std::vector<std::string> keys = {key, key};
    std::vector<BatchItem_t> batch_items;
    std::vector<Slice_span_t> slice_spans;
    std::vector<Slice_t> all_slices;
    std::vector<void*> allocated_buffers;

    batch_items.reserve(2);
    slice_spans.reserve(2);
    all_slices.reserve(2);

    // Prepare data for both keys
    for (int i = 0; i < 2; i++) {
        // Allocate buffer and copy data
        void* buffer = client_buffer_allocator_->allocate(test_data.size());
        ASSERT_NE(buffer, nullptr);
        memcpy(buffer, test_data.data(), test_data.size());

        // Create slice
        Slice_t slice;
        slice.ptr = buffer;
        slice.size = test_data.size();
        all_slices.push_back(slice);

        // Create slice span
        Slice_span_t slice_span;
        slice_span.slices = &all_slices[i];
        slice_span.slices_count = 1;
        slice_spans.push_back(slice_span);

        // Create batch item
        BatchItem_t item;
        item.key = keys[i].c_str();
        item.slices_span = &slice_spans[i];
        batch_items.push_back(item);
    }

    ReplicateConfig_t config;
    config.replica_num = 1;

    // Test batch put with duplicate keys
    ErrorCode_span_t batch_put_results =
        mooncake_client_batch_put(test_client_, batch_items.data(), 2, config);

    // Check that we got results for both operations
    ASSERT_EQ(batch_put_results.results_count, 2);

    // Both operations should succeed
    // Because we currently consider `OBJECT_ALREADY_EXISTS` as success

    for (size_t i = 0; i < batch_put_results.results_count; i++) {
        ASSERT_EQ(batch_put_results.results[i], MOONCAKE_ERROR_OK)
            << "BatchPut operation failed for duplicate key at index " << i;
    }

    // Cleanup result array
    free(batch_put_results.results);

    // Clean up allocated memory
    for (int i = 0; i < 2; i++) {
        client_buffer_allocator_->deallocate(all_slices[i].ptr,
                                             test_data.size());
    }

    // Clean up the key that was successfully put
    std::this_thread::sleep_for(
        std::chrono::milliseconds(default_kv_lease_ttl_));
    auto remove_result = mooncake_client_remove(test_client_, key.c_str());
    ASSERT_EQ(remove_result, MOONCAKE_ERROR_OK) << "Remove operation failed";
}

}  // namespace testing
}  // namespace mooncake

int main(int argc, char** argv) {
    // Initialize Google Test
    ::testing::InitGoogleTest(&argc, argv);

    // Initialize Google's flags library
    gflags::ParseCommandLineFlags(&argc, &argv, false);

    // Run all tests
    return RUN_ALL_TESTS();
}
