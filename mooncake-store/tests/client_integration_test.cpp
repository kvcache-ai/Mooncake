#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "allocator.h"
#include "client.h"
#include "types.h"
#include "utils.h"

DEFINE_string(protocol, "tcp", "Transfer protocol: rdma|tcp");
DEFINE_string(device_name, "ibp6s0",
              "Device name to use, valid if protocol=rdma");
DEFINE_string(transfer_engine_metadata_url, "localhost:2379",
              "Metadata connection string for transfer engine");
DEFINE_uint64(default_kv_lease_ttl, mooncake::DEFAULT_DEFAULT_KV_LEASE_TTL,
              "Default lease time for kv objects, must be set to the "
              "same as the master's default_kv_lease_ttl");

namespace mooncake {
namespace testing {

class ClientIntegrationTest : public ::testing::Test {
   protected:
    static std::shared_ptr<Client> CreateClient(const std::string& host_name) {
        void** args =
            (FLAGS_protocol == "rdma") ? rdma_args(FLAGS_device_name) : nullptr;

        auto client_opt = Client::Create(
            host_name,                           // Local hostname
            FLAGS_transfer_engine_metadata_url,  // Metadata connection string
            FLAGS_protocol, args,
            "localhost:50051"  // Master server address
        );

        EXPECT_TRUE(client_opt.has_value())
            << "Failed to create client with host_name: " << host_name;
        if (!client_opt.has_value()) {
            return nullptr;
        }
        return *client_opt;
    }

    static void SetUpTestSuite() {
        // Initialize glog
        google::InitGoogleLogging("ClientIntegrationTest");

        FLAGS_logtostderr = 1;

        // Override flags from environment variables if present
        if (getenv("PROTOCOL")) FLAGS_protocol = getenv("PROTOCOL");
        if (getenv("DEVICE_NAME")) FLAGS_device_name = getenv("DEVICE_NAME");
        if (getenv("MC_METADATA_SERVER"))
            FLAGS_transfer_engine_metadata_url = getenv("MC_METADATA_SERVER");

        LOG(INFO) << "Protocol: " << FLAGS_protocol
                  << ", Device name: " << FLAGS_device_name
                  << ", Metadata URL: " << FLAGS_transfer_engine_metadata_url;

        InitializeClients();
        InitializeSegment();
    }

    static void TearDownTestSuite() {
        CleanupSegment();
        CleanupClients();
        google::ShutdownGoogleLogging();
    }

    static void InitializeSegment() {
        ram_buffer_size_ = 512 * 1024 * 1024;  // 512 MB
        segment_ptr_ = allocate_buffer_allocator_memory(ram_buffer_size_);
        LOG_ASSERT(segment_ptr_);
        ErrorCode rc = segment_provider_client_->MountSegment(segment_ptr_,
                                                              ram_buffer_size_);
        if (rc != ErrorCode::OK) {
            LOG(ERROR) << "Failed to mount segment: " << toString(rc);
        }
        LOG(INFO) << "Segment mounted successfully";
    }

    static void InitializeClients() {
        // This client is used for testing purposes.
        test_client_ = CreateClient("localhost:17813");
        ASSERT_TRUE(test_client_ != nullptr);

        // This client is used to provide segments.
        segment_provider_client_ = CreateClient("localhost:17812");
        ASSERT_TRUE(segment_provider_client_ != nullptr);

        client_buffer_allocator_ =
            std::make_unique<SimpleAllocator>(128 * 1024 * 1024);
        ErrorCode error_code = test_client_->RegisterLocalMemory(
            client_buffer_allocator_->getBase(), 128 * 1024 * 1024, "cpu:0",
            false, false);
        if (error_code != ErrorCode::OK) {
            LOG(ERROR) << "Failed to allocate transfer buffer: "
                       << toString(error_code);
        }

        // Mount segment for test_client_ as well
        test_client_ram_buffer_size_ = 512 * 1024 * 1024;  // 512 MB
        test_client_segment_ptr_ =
            allocate_buffer_allocator_memory(test_client_ram_buffer_size_);
        LOG_ASSERT(test_client_segment_ptr_);
        ErrorCode rc = test_client_->MountSegment(test_client_segment_ptr_,
                                                  test_client_ram_buffer_size_);
        if (rc != ErrorCode::OK) {
            LOG(ERROR) << "Failed to mount segment for test_client_: "
                       << toString(rc);
        }
        LOG(INFO) << "Test client segment mounted successfully";
    }

    static void CleanupClients() {
        // Unmount test client segment first
        if (test_client_ && test_client_segment_ptr_) {
            if (test_client_->UnmountSegment(test_client_segment_ptr_,
                                            test_client_ram_buffer_size_) !=
                ErrorCode::OK) {
                LOG(ERROR) << "Failed to unmount test client segment";
            }
        }

        if (test_client_) {
            test_client_.reset();
        }
        if (segment_provider_client_) {
            segment_provider_client_.reset();
        }
    }

    static void CleanupSegment() {
        if (segment_provider_client_->UnmountSegment(segment_ptr_,
                                                    ram_buffer_size_) !=
            ErrorCode::OK) {
            LOG(ERROR) << "Failed to unmount segment";
        }
    }

    static std::shared_ptr<Client> test_client_;
    static std::shared_ptr<Client> segment_provider_client_;
    // Here we use a simple allocator for the client buffer. In a real
    // application, user should manage the memory allocation and deallocation
    // themselves.
    static std::unique_ptr<SimpleAllocator> client_buffer_allocator_;
    static void* segment_ptr_;
    static size_t ram_buffer_size_;
    static void* test_client_segment_ptr_;
    static size_t test_client_ram_buffer_size_;
};

// Static members initialization
std::shared_ptr<Client> ClientIntegrationTest::test_client_ = nullptr;
std::shared_ptr<Client> ClientIntegrationTest::segment_provider_client_ =
    nullptr;
void* ClientIntegrationTest::segment_ptr_ = nullptr;
void* ClientIntegrationTest::test_client_segment_ptr_ = nullptr;
std::unique_ptr<SimpleAllocator>
    ClientIntegrationTest::client_buffer_allocator_ = nullptr;
size_t ClientIntegrationTest::ram_buffer_size_ = 0;
size_t ClientIntegrationTest::test_client_ram_buffer_size_ = 0;

// Test basic Put/Get operations through the client
TEST_F(ClientIntegrationTest, BasicPutGetOperations) {
    const std::string test_data = "Hello, World!";
    const std::string key = "test_key";
    void* buffer = client_buffer_allocator_->allocate(test_data.size());

    // write
    memcpy(buffer, test_data.data(), test_data.size());
    std::vector<Slice> slices;
    slices.emplace_back(Slice{buffer, test_data.size()});

    // Test Put operation
    ReplicateConfig config;
    config.replica_num = 1;
    ASSERT_EQ(test_client_->Put(key, slices, config), ErrorCode::OK);
    client_buffer_allocator_->deallocate(buffer, test_data.size());

    buffer = client_buffer_allocator_->allocate(1 * 1024 * 1024);
    slices.clear();
    slices.emplace_back(Slice{buffer, test_data.size()});
    // Verify data through Get operation
    ErrorCode error_code = test_client_->Get(key, slices);
    ASSERT_EQ(error_code, ErrorCode::OK);
    ASSERT_EQ(slices.size(), 1);
    ASSERT_EQ(slices[0].size, test_data.size());
    ASSERT_EQ(slices[0].ptr, buffer);
    ASSERT_EQ(memcmp(slices[0].ptr, test_data.data(), test_data.size()), 0);
    client_buffer_allocator_->deallocate(buffer, test_data.size());

    // Put again with the same key, should succeed
    buffer = client_buffer_allocator_->allocate(test_data.size());
    memcpy(buffer, test_data.data(), test_data.size());
    slices.clear();
    slices.emplace_back(Slice{buffer, test_data.size()});
    ASSERT_EQ(test_client_->Put(key, slices, config), ErrorCode::OK);
    std::this_thread::sleep_for(
        std::chrono::milliseconds(FLAGS_default_kv_lease_ttl));
    ASSERT_EQ(test_client_->Remove(key), ErrorCode::OK);
    client_buffer_allocator_->deallocate(buffer, test_data.size());
}

// Test Remove operation
TEST_F(ClientIntegrationTest, RemoveOperation) {
    const std::string test_data = "Test data for removal";
    const std::string key = "remove_test_key";
    void* buffer = client_buffer_allocator_->allocate(test_data.size());

    // Put data first
    memcpy(buffer, test_data.data(), test_data.size());
    std::vector<Slice> slices;
    slices.emplace_back(Slice{buffer, test_data.size()});
    ReplicateConfig config;
    config.replica_num = 1;
    ASSERT_EQ(test_client_->Put(key, slices, config), ErrorCode::OK);
    client_buffer_allocator_->deallocate(buffer, test_data.size());

    // Remove the data
    ASSERT_EQ(test_client_->Remove(key), ErrorCode::OK);

    // Try to get the removed data - should fail
    buffer = client_buffer_allocator_->allocate(test_data.size());
    slices.clear();
    slices.emplace_back(Slice{buffer, test_data.size()});
    ErrorCode error_code = test_client_->Get(key, slices);
    ASSERT_NE(error_code, ErrorCode::OK);
    client_buffer_allocator_->deallocate(buffer, test_data.size());
}

// Test local preferred allocation strategy
TEST_F(ClientIntegrationTest, LocalPreferredAllocationTest) {
    const std::string test_data = "Test data for local preferred allocation";
    const std::string key = "local_preferred_test_key";
    void* buffer = client_buffer_allocator_->allocate(test_data.size());

    // Put data with preferred segment set to local hostname
    memcpy(buffer, test_data.data(), test_data.size());
    std::vector<Slice> slices;
    slices.emplace_back(Slice{buffer, test_data.size()});

    ReplicateConfig config;
    config.replica_num = 1;
    // Although there is only one segment now, in order to test the preferred
    // allocation logic, we still set it. This will prevent potential
    // compatibility issues in the future.
    config.preferred_segment = "localhost:17812";  // Local segment

    ASSERT_EQ(test_client_->Put(key, slices, config), ErrorCode::OK);
    client_buffer_allocator_->deallocate(buffer, test_data.size());

    // Verify data through Get operation
    buffer = client_buffer_allocator_->allocate(test_data.size());
    slices.clear();
    slices.emplace_back(Slice{buffer, test_data.size()});

    Client::ObjectInfo objectinfo;
    ErrorCode error_code = test_client_->Query(key, objectinfo);
    ASSERT_EQ(error_code, ErrorCode::OK);
    ASSERT_EQ(objectinfo.replica_list.size(), 1);
    ASSERT_EQ(objectinfo.replica_list[0].buffer_descriptors.size(), 1);
    ASSERT_EQ(objectinfo.replica_list[0].buffer_descriptors[0].segment_name_,
              "localhost:17812");

    error_code = test_client_->Get(key, objectinfo, slices);
    ASSERT_EQ(error_code, ErrorCode::OK);
    ASSERT_EQ(slices.size(), 1);
    ASSERT_EQ(slices[0].size, test_data.size());
    ASSERT_EQ(memcmp(slices[0].ptr, test_data.data(), test_data.size()), 0);
    client_buffer_allocator_->deallocate(buffer, test_data.size());

    // Clean up
    std::this_thread::sleep_for(
        std::chrono::milliseconds(FLAGS_default_kv_lease_ttl));
    ASSERT_EQ(test_client_->Remove(key), ErrorCode::OK);
}

// Test heavy workload operations
TEST_F(ClientIntegrationTest, DISABLED_AllocateTest) {
    const size_t data_size = 1 * 1024 * 1024;  // 1MB
    std::string large_data(data_size, 'A');    // Fill with 'A's
    const int num_operations = 13;

    // Configure with 1 replicas for high availability
    ReplicateConfig config;
    config.replica_num = 1;

    // Perform multiple Put/Get operations
    for (int i = 0; i < num_operations; i++) {
        std::string key = "heavy_test_key_" + std::to_string(i);
        void* buffer = client_buffer_allocator_->allocate(data_size);
        ASSERT_TRUE(buffer);

        // Put operation with large data
        memcpy(buffer, large_data.data(), data_size);
        std::vector<Slice> put_slices;
        put_slices.emplace_back(Slice{buffer, data_size});
        ErrorCode error_code = test_client_->Put(key, put_slices, config);
        if (error_code != ErrorCode::OK) break;
        client_buffer_allocator_->deallocate(buffer, data_size);
        // Get and verify data
        buffer = client_buffer_allocator_->allocate(data_size);
        std::vector<Slice> get_slices;
        get_slices.emplace_back(Slice{buffer, data_size});
        error_code = test_client_->Get(key, get_slices);
        ASSERT_EQ(error_code, ErrorCode::OK);
        ASSERT_EQ(get_slices[0].size, data_size);

        std::string retrieved_data(static_cast<const char*>(get_slices[0].ptr),
                                   get_slices[0].size);
        EXPECT_EQ(retrieved_data, large_data);
        client_buffer_allocator_->deallocate(buffer, data_size);
    }

    std::string allocate_failed_key = "heavy_test_failed_key";
    void* failed_buffer = client_buffer_allocator_->allocate(data_size);
    std::vector<Slice> failed_slices;
    failed_slices.emplace_back(Slice{failed_buffer, data_size});
    memcpy(failed_buffer, large_data.data(), data_size);
    ASSERT_NE(test_client_->Put(allocate_failed_key, failed_slices, config),
              ErrorCode::OK);
    client_buffer_allocator_->deallocate(failed_buffer, data_size);

    // sleep for 2 seconds to ensure the object is marked for GC
    std::this_thread::sleep_for(std::chrono::seconds(2));

    // After removing all keys, we should be able to allocate the failed key
    void* success_buffer = client_buffer_allocator_->allocate(data_size);
    std::vector<Slice> success_slices;
    success_slices.emplace_back(Slice{success_buffer, data_size});
    memcpy(success_buffer, large_data.data(), data_size);
    ASSERT_EQ(test_client_->Put(allocate_failed_key, success_slices, config),
              ErrorCode::OK);
    client_buffer_allocator_->deallocate(success_buffer, data_size);
    ASSERT_EQ(test_client_->Remove(allocate_failed_key), ErrorCode::OK);
}

// Test large allocation operations
TEST_F(ClientIntegrationTest, LargeAllocateTest) {
    const size_t data_size = 1 * 1024 * 1024;  // 1MB
    const uint64_t kNumBuffers = 5;
    const std::string key = "large_test_key";

    // Configure with 1 replicas for high availability
    ReplicateConfig config;
    config.replica_num = 1;

    // Allocate buffers and fill with data
    std::vector<void*> buffers(kNumBuffers);
    for (size_t i = 0; i < kNumBuffers; ++i) {
        buffers[i] = client_buffer_allocator_->allocate(data_size);
        ASSERT_NE(buffers[i], nullptr);
        std::string large_data(data_size, 'A' + i);
        memcpy(buffers[i], large_data.data(), data_size);
    }

    // Create slices from buffers
    std::vector<Slice> slices;
    for (size_t i = 0; i < kNumBuffers; ++i) {
        slices.emplace_back(Slice{buffers[i], data_size});
    }

    // Put operation
    ASSERT_EQ(test_client_->Put(key, slices, config), ErrorCode::OK);

    // Clear buffers before Get
    for (size_t i = 0; i < kNumBuffers; ++i) {
        memset(buffers[i], 0, data_size);
    }

    // Get operation
    ErrorCode error_code = test_client_->Get(key, slices);
    ASSERT_EQ(error_code, ErrorCode::OK);

    // Verify data and deallocate buffers
    for (size_t i = 0; i < kNumBuffers; ++i) {
        ASSERT_EQ(slices[i].size, data_size);
        std::string retrieved_data(static_cast<const char*>(slices[i].ptr),
                                   slices[i].size);
        std::string expected_data(data_size, 'A' + i);
        EXPECT_EQ(
            memcmp(retrieved_data.data(), expected_data.data(), data_size), 0);
        client_buffer_allocator_->deallocate(buffers[i], data_size);
    }

    // Remove the key
    std::this_thread::sleep_for(
        std::chrono::milliseconds(FLAGS_default_kv_lease_ttl));
    ASSERT_EQ(test_client_->Remove(key), ErrorCode::OK);
}

// Test batch Put/Get operations through the client
TEST_F(ClientIntegrationTest, BatchPutGetOperations) {
    int batch_sz = 100;
    std::vector<std::string> keys;
    std::vector<std::string> test_data_list;
    std::unordered_map<std::string, std::vector<Slice>> batched_slices;
    for (int i = 0; i < batch_sz; i++) {
        keys.push_back("test_key_batch_put_" + std::to_string(i));
        test_data_list.push_back("test_data_" + std::to_string(i));
    }
    void* buffer = nullptr;
    void* target_buffer = nullptr;
    for (int i = 0; i < batch_sz; i++) {
        std::vector<Slice> slices;
        buffer = client_buffer_allocator_->allocate(test_data_list[i].size());
        memcpy(buffer, test_data_list[i].data(), test_data_list[i].size());
        slices.emplace_back(Slice{buffer, test_data_list[i].size()});
        batched_slices.emplace(keys[i], slices);
    }
    // Test Batch Put operation
    ReplicateConfig config;
    config.replica_num = 1;
    auto start = std::chrono::high_resolution_clock::now();
    ASSERT_EQ(test_client_->BatchPut(keys, batched_slices, config),
              ErrorCode::OK);
    auto end = std::chrono::high_resolution_clock::now();
    LOG(INFO) << "Time taken for BatchPut: "
              << std::chrono::duration_cast<std::chrono::microseconds>(end -
                                                                       start)
                     .count()
              << "us";

    start = std::chrono::high_resolution_clock::now();
    for (int i = 0; i < batch_sz; i++) {
        std::vector<Slice> slices;
        target_buffer =
            client_buffer_allocator_->allocate(test_data_list[i].size());
        slices.emplace_back(Slice{target_buffer, test_data_list[i].size()});
        ASSERT_EQ(test_client_->Get(keys[i], slices), ErrorCode::OK);
        client_buffer_allocator_->deallocate(target_buffer,
                                             test_data_list[i].size());
    }
    end = std::chrono::high_resolution_clock::now();
    LOG(INFO) << "Time taken for single Get: "
              << std::chrono::duration_cast<std::chrono::microseconds>(end -
                                                                       start)
                     .count()
              << "us";

    start = std::chrono::high_resolution_clock::now();
    std::unordered_map<std::string, std::vector<Slice>> target_batched_slices;
    for (int i = 0; i < batch_sz; i++) {
        std::vector<Slice> target_slices;
        target_buffer =
            client_buffer_allocator_->allocate(test_data_list[i].size());
        target_slices.emplace_back(
            Slice{target_buffer, test_data_list[i].size()});
        target_batched_slices.emplace(keys[i], target_slices);
    }
    ASSERT_EQ(test_client_->BatchGet(keys, target_batched_slices),
              ErrorCode::OK);
    end = std::chrono::high_resolution_clock::now();
    LOG(INFO) << "Time taken for BatchGet: "
              << std::chrono::duration_cast<std::chrono::microseconds>(end -
                                                                       start)
                     .count()
              << "us";

    for (int i = 0; i < batch_sz; i++) {
        ASSERT_EQ(target_batched_slices[keys[i]][0].size,
                  test_data_list[i].size());
        ASSERT_EQ(memcmp(target_batched_slices[keys[i]][0].ptr,
                         test_data_list[i].data(), test_data_list[i].size()),
                  0);
        client_buffer_allocator_->deallocate(
            target_batched_slices[keys[i]][0].ptr, test_data_list[i].size());
    }
}

}  // namespace testing

}  // namespace mooncake

int main(int argc, char** argv) {
    // Initialize Google's flags library
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    // Initialize Google Test
    ::testing::InitGoogleTest(&argc, argv);

    // Run all tests
    return RUN_ALL_TESTS();
}
