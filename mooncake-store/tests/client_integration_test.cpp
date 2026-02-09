#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include <cstdint>
#include <memory>
#include <string>
#include <vector>
#include <regex>
#include <unordered_set>
#include <unordered_map>
#include <thread>
#include <chrono>

#include "allocator.h"
#include "client_service.h"
#include "types.h"
#include "utils.h"
#include "test_server_helpers.h"
#include "default_config.h"

DEFINE_string(protocol, "tcp", "Transfer protocol: rdma|tcp");
DEFINE_string(device_name, "", "Device name to use, valid if protocol=rdma");
DEFINE_uint64(default_kv_lease_ttl, mooncake::DEFAULT_DEFAULT_KV_LEASE_TTL,
              "Default lease time for kv objects, must be set to the "
              "same as the master's default_kv_lease_ttl");

namespace mooncake {
namespace testing {

// Helper functions for client_id parsing
std::string FormatClientId(const UUID& client_id) {
    return std::to_string(client_id.first) + "-" +
           std::to_string(client_id.second);
}

UUID ParseClientId(const std::string& client_id_str) {
    UUID client_id{0, 0};
    size_t dash_pos = client_id_str.find('-');
    if (dash_pos != std::string::npos) {
        try {
            client_id.first = std::stoull(client_id_str.substr(0, dash_pos));
            client_id.second = std::stoull(client_id_str.substr(dash_pos + 1));
        } catch (const std::exception& e) {
            LOG(ERROR) << "Failed to parse client_id: " << e.what();
        }
    } else {
        LOG(ERROR) << "Invalid client_id format. Expected format: first-second";
    }
    return client_id;
}

class ClientIdCaptureSink : public google::LogSink {
   public:
    std::string captured_client_id;

    void send(google::LogSeverity severity, const char* full_filename,
              const char* base_filename, int line, const struct ::tm* tm_time,
              const char* message, size_t message_len) override {
        (void)severity;
        (void)full_filename;
        (void)base_filename;
        (void)line;
        (void)tm_time;

        std::string msg(message, message_len);

        size_t pos = msg.find("client_id=");
        if (pos != std::string::npos) {
            std::string client_id_str = msg.substr(pos + 10);
            client_id_str.erase(0, client_id_str.find_first_not_of(" \t\n\r"));
            client_id_str.erase(client_id_str.find_last_not_of(" \t\n\r") + 1);

            std::regex uuid_pattern(R"((\d+)-(\d+))");
            std::smatch match;
            if (std::regex_search(client_id_str, match, uuid_pattern)) {
                captured_client_id = match[0].str();
            }
        }
    }
};

class ClientIntegrationTest : public ::testing::Test {
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
        // Initialize glog
        google::InitGoogleLogging("ClientIntegrationTest");

        FLAGS_logtostderr = 1;

        // Override flags from environment variables if present
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

        // Start an in-process non-HA master without HTTP metadata server
        ASSERT_TRUE(master_.Start(InProcMasterConfigBuilder().build()));
        master_address_ = master_.master_address();
        metadata_url_ = master_.metadata_url();
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
        ram_buffer_size_ = 512 * 1024 * 1024;  // 512 MB
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
        // This client is used for testing purposes.
        // Capture test_client_ client_id from logs
        ClientIdCaptureSink* test_client_sink = new ClientIdCaptureSink();
        google::AddLogSink(test_client_sink);

        test_client_ = CreateClient("localhost:17813");
        ASSERT_TRUE(test_client_ != nullptr);

        // Wait for logs to flush
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
        google::RemoveLogSink(test_client_sink);

        if (!test_client_sink->captured_client_id.empty()) {
            UUID extracted_id =
                ParseClientId(test_client_sink->captured_client_id);
            if (extracted_id.first != 0 || extracted_id.second != 0) {
                test_client_id_ = extracted_id;
                LOG(INFO) << "Captured test_client_id: "
                          << FormatClientId(test_client_id_);
            }
        }
        delete test_client_sink;

        // This client is used to provide segments.
        // Capture segment_provider_client_ client_id from logs
        ClientIdCaptureSink* provider_client_sink = new ClientIdCaptureSink();
        google::AddLogSink(provider_client_sink);

        segment_provider_client_ = CreateClient("localhost:17812");
        ASSERT_TRUE(segment_provider_client_ != nullptr);

        // Wait for logs to flush
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
        google::RemoveLogSink(provider_client_sink);

        if (!provider_client_sink->captured_client_id.empty()) {
            UUID extracted_id =
                ParseClientId(provider_client_sink->captured_client_id);
            if (extracted_id.first != 0 || extracted_id.second != 0) {
                segment_provider_client_id_ = extracted_id;
                LOG(INFO) << "Captured segment_provider_client_id: "
                          << FormatClientId(segment_provider_client_id_);
            }
        }
        delete provider_client_sink;

        client_buffer_allocator_ =
            std::make_unique<SimpleAllocator>(128 * 1024 * 1024);
        auto register_result = test_client_->RegisterLocalMemory(
            client_buffer_allocator_->getBase(), 128 * 1024 * 1024, "cpu:0",
            false, false);
        if (!register_result.has_value()) {
            LOG(ERROR) << "Failed to register local memory: "
                       << toString(register_result.error());
        }

        // Mount segment for test_client_ as well
        test_client_ram_buffer_size_ = 512 * 1024 * 1024;  // 512 MB
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
        // Unmount test client segment first
        if (test_client_ && test_client_segment_ptr_) {
            if (!test_client_
                     ->UnmountSegment(test_client_segment_ptr_,
                                      test_client_ram_buffer_size_)
                     .has_value()) {
                LOG(ERROR) << "Failed to unmount test client segment";
            }
        }

        if (test_client_) {
            test_client_.reset();
        }
        if (segment_provider_client_) {
            segment_provider_client_.reset();
        }

        // Free segment memory
        if (test_client_segment_ptr_) {
            free(test_client_segment_ptr_);
        }
        if (segment_ptr_) {
            free(segment_ptr_);
        }
    }

    static void CleanupSegment() {
        if (!segment_provider_client_
                 ->UnmountSegment(segment_ptr_, ram_buffer_size_)
                 .has_value()) {
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
    static uint64_t default_kv_lease_ttl_;
    static InProcMaster master_;
    static std::string master_address_;
    static std::string metadata_url_;
    static UUID test_client_id_;
    static UUID segment_provider_client_id_;
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
uint64_t ClientIntegrationTest::default_kv_lease_ttl_ = 0;
InProcMaster ClientIntegrationTest::master_;
std::string ClientIntegrationTest::master_address_;
std::string ClientIntegrationTest::metadata_url_;
UUID ClientIntegrationTest::test_client_id_{0, 0};
UUID ClientIntegrationTest::segment_provider_client_id_{0, 0};

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
    auto put_result = test_client_->Put(key, slices, config);
    ASSERT_TRUE(put_result.has_value())
        << "Put operation failed: " << toString(put_result.error());
    client_buffer_allocator_->deallocate(buffer, test_data.size());

    buffer = client_buffer_allocator_->allocate(1 * 1024 * 1024);
    slices.clear();
    slices.emplace_back(Slice{buffer, test_data.size()});
    // Verify data through Get operation
    auto get_result = test_client_->Get(key, slices);
    ASSERT_TRUE(get_result.has_value())
        << "Get operation failed: " << toString(get_result.error());
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
    auto put_result2 = test_client_->Put(key, slices, config);
    ASSERT_TRUE(put_result2.has_value())
        << "Second Put operation failed: " << toString(put_result2.error());
    std::this_thread::sleep_for(
        std::chrono::milliseconds(default_kv_lease_ttl_));
    auto remove_result = test_client_->Remove(key);
    ASSERT_TRUE(remove_result.has_value())
        << "Remove operation failed: " << toString(remove_result.error());
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
    auto put_result = test_client_->Put(key, slices, config);
    ASSERT_TRUE(put_result.has_value())
        << "Put operation failed: " << toString(put_result.error());
    client_buffer_allocator_->deallocate(buffer, test_data.size());

    // Remove the data
    auto remove_result = test_client_->Remove(key);
    ASSERT_TRUE(remove_result.has_value())
        << "Remove operation failed: " << toString(remove_result.error());

    // Verify that the data is removed using Query operation
    auto query_result = test_client_->Query(key);
    ASSERT_FALSE(query_result.has_value())
        << "Query should not find the removed key: " << key;

    // Check if the key exists using IsExist
    auto exist_result = test_client_->IsExist(key);
    ASSERT_TRUE(exist_result.has_value());
    ASSERT_FALSE(exist_result.value())
        << "IsExist should return false for removed key: " << key;

    // Try to get the removed data - should fail
    buffer = client_buffer_allocator_->allocate(test_data.size());
    slices.clear();
    slices.emplace_back(Slice{buffer, test_data.size()});
    auto get_result = test_client_->Get(key, slices);
    ASSERT_FALSE(get_result.has_value()) << "Get should fail for removed key";
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

    auto put_result = test_client_->Put(key, slices, config);
    ASSERT_TRUE(put_result.has_value())
        << "Put operation failed: " << toString(put_result.error());
    client_buffer_allocator_->deallocate(buffer, test_data.size());

    // Verify data through Get operation
    buffer = client_buffer_allocator_->allocate(test_data.size());
    slices.clear();
    slices.emplace_back(Slice{buffer, test_data.size()});

    auto query_result = test_client_->Query(key);
    ASSERT_TRUE(query_result.has_value())
        << "Query operation failed: " << toString(query_result.error());
    auto replica_list = query_result.value().replicas;
    ASSERT_EQ(replica_list.size(), 1);
    ASSERT_EQ(replica_list[0]
                  .get_memory_descriptor()
                  .buffer_descriptor.transport_endpoint_,
              segment_provider_client_->GetTransportEndpoint());

    auto get_result = test_client_->Get(key, query_result.value(), slices);
    ASSERT_TRUE(get_result.has_value())
        << "Get operation failed: " << toString(get_result.error());
    ASSERT_EQ(slices.size(), 1);
    ASSERT_EQ(slices[0].size, test_data.size());
    ASSERT_EQ(memcmp(slices[0].ptr, test_data.data(), test_data.size()), 0);
    client_buffer_allocator_->deallocate(buffer, test_data.size());

    // Clean up
    std::this_thread::sleep_for(
        std::chrono::milliseconds(default_kv_lease_ttl_));
    auto remove_result2 = test_client_->Remove(key);
    ASSERT_TRUE(remove_result2.has_value())
        << "Remove operation failed: " << toString(remove_result2.error());
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
        auto put_result = test_client_->Put(key, put_slices, config);
        if (!put_result.has_value()) break;
        client_buffer_allocator_->deallocate(buffer, data_size);
        // Get and verify data
        buffer = client_buffer_allocator_->allocate(data_size);
        std::vector<Slice> get_slices;
        get_slices.emplace_back(Slice{buffer, data_size});
        auto get_result = test_client_->Get(key, get_slices);
        ASSERT_TRUE(get_result.has_value())
            << "Get operation failed: " << toString(get_result.error());
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
    auto failed_put_result =
        test_client_->Put(allocate_failed_key, failed_slices, config);
    ASSERT_FALSE(failed_put_result.has_value())
        << "Put operation should have failed";
    client_buffer_allocator_->deallocate(failed_buffer, data_size);

    // sleep for 2 seconds to ensure the object is marked for GC
    std::this_thread::sleep_for(std::chrono::seconds(2));

    // After removing all keys, we should be able to allocate the failed key
    void* success_buffer = client_buffer_allocator_->allocate(data_size);
    std::vector<Slice> success_slices;
    success_slices.emplace_back(Slice{success_buffer, data_size});
    memcpy(success_buffer, large_data.data(), data_size);
    auto success_put_result =
        test_client_->Put(allocate_failed_key, success_slices, config);
    ASSERT_TRUE(success_put_result.has_value())
        << "Put operation failed: " << toString(success_put_result.error());
    client_buffer_allocator_->deallocate(success_buffer, data_size);
    auto success_remove_result = test_client_->Remove(allocate_failed_key);
    ASSERT_TRUE(success_remove_result.has_value())
        << "Remove operation failed: "
        << toString(success_remove_result.error());
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
    auto put_result = test_client_->Put(key, slices, config);
    ASSERT_TRUE(put_result.has_value())
        << "Put operation failed: " << toString(put_result.error());

    // Clear buffers before Get
    for (size_t i = 0; i < kNumBuffers; ++i) {
        memset(buffers[i], 0, data_size);
    }

    // Get operation
    auto get_result = test_client_->Get(key, slices);
    ASSERT_TRUE(get_result.has_value())
        << "Get operation failed: " << toString(get_result.error());

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
        std::chrono::milliseconds(default_kv_lease_ttl_));
    auto remove_result = test_client_->Remove(key);
    ASSERT_TRUE(remove_result.has_value())
        << "Remove operation failed: " << toString(remove_result.error());
}

// Test batch Put/Get operations through the client
TEST_F(ClientIntegrationTest, BatchPutGetOperations) {
    int batch_sz = 100;
    std::vector<std::string> keys;
    std::vector<std::string> test_data_list;
    std::vector<std::vector<Slice>> batched_slices;
    for (int i = 0; i < batch_sz; i++) {
        keys.push_back("test_key_batch_put_" + std::to_string(i));
        test_data_list.push_back("test_data_" + std::to_string(i));
    }
    void* buffer = nullptr;
    void* target_buffer = nullptr;
    batched_slices.reserve(batch_sz);
    for (int i = 0; i < batch_sz; i++) {
        std::vector<Slice> slices;
        buffer = client_buffer_allocator_->allocate(test_data_list[i].size());
        memcpy(buffer, test_data_list[i].data(), test_data_list[i].size());
        slices.emplace_back(Slice{buffer, test_data_list[i].size()});
        batched_slices.push_back(std::move(slices));
    }
    // Test Batch Put operation
    ReplicateConfig config;
    config.replica_num = 1;
    auto start = std::chrono::high_resolution_clock::now();
    auto batch_put_results =
        test_client_->BatchPut(keys, batched_slices, config);
    // Check that all operations succeeded
    for (const auto& result : batch_put_results) {
        ASSERT_TRUE(result.has_value()) << "BatchPut operation failed";
    }
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
        auto get_result = test_client_->Get(keys[i], slices);
        ASSERT_TRUE(get_result.has_value())
            << "Get operation failed: " << toString(get_result.error());
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
    auto batch_get_results =
        test_client_->BatchGet(keys, target_batched_slices);
    for (const auto& result : batch_get_results) {
        ASSERT_TRUE(result.has_value()) << "BatchGet operation failed";
    }
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

// Test batch IsExist operations through the client
TEST_F(ClientIntegrationTest, BatchIsExistOperations) {
    int batch_size = 50;
    std::vector<std::string> keys;
    std::vector<std::string> test_data_list;
    std::vector<std::vector<Slice>> batched_slices;

    // Create test keys and data
    for (int i = 0; i < batch_size; i++) {
        keys.push_back("test_key_batch_exist_" + std::to_string(i));
        test_data_list.push_back("test_data_" + std::to_string(i));
    }

    // Put only the first half of the keys
    void* buffer = nullptr;
    batched_slices.reserve(batch_size / 2);
    for (int i = 0; i < batch_size / 2; i++) {
        std::vector<Slice> slices;
        buffer = client_buffer_allocator_->allocate(test_data_list[i].size());
        memcpy(buffer, test_data_list[i].data(), test_data_list[i].size());
        slices.emplace_back(Slice{buffer, test_data_list[i].size()});
        batched_slices.push_back(std::move(slices));
    }

    ReplicateConfig config;
    config.replica_num = 1;

    // Put the first half of keys
    std::vector<std::string> existing_keys(keys.begin(),
                                           keys.begin() + batch_size / 2);
    auto batch_put_results =
        test_client_->BatchPut(existing_keys, batched_slices, config);
    // Check that all operations succeeded
    for (const auto& result : batch_put_results) {
        ASSERT_TRUE(result.has_value()) << "BatchPut operation failed";
    }

    // Test BatchIsExist with mixed existing and non-existing keys
    auto exist_results = test_client_->BatchIsExist(keys);

    // Verify results
    ASSERT_EQ(keys.size(), exist_results.size());

    // First half should exist
    for (int i = 0; i < batch_size / 2; i++) {
        ASSERT_TRUE(exist_results[i].has_value())
            << "BatchIsExist failed for key " << keys[i];
        ASSERT_TRUE(exist_results[i].value())
            << "Key " << keys[i] << " should exist";
    }

    // Second half should not exist
    for (int i = batch_size / 2; i < batch_size; i++) {
        ASSERT_TRUE(exist_results[i].has_value())
            << "BatchIsExist failed for key " << keys[i];
        ASSERT_FALSE(exist_results[i].value())
            << "Key " << keys[i] << " should not exist";
    }

    // Test with empty keys vector
    std::vector<std::string> empty_keys;
    auto empty_results = test_client_->BatchIsExist(empty_keys);
    ASSERT_EQ(empty_results.size(), 0);

    // Clean up
    for (int i = 0; i < batch_size / 2; i++) {
        client_buffer_allocator_->deallocate(batched_slices[i][0].ptr,
                                             test_data_list[i].size());
    }
    std::this_thread::sleep_for(
        std::chrono::milliseconds(default_kv_lease_ttl_));
    for (int i = 0; i < batch_size / 2; i++) {
        auto remove_result = test_client_->Remove(keys[i]);
        ASSERT_TRUE(remove_result.has_value())
            << "Remove operation failed: " << toString(remove_result.error());
    }
}

// Test batch QueryIp operations through the client
TEST_F(ClientIntegrationTest, BatchQueryIpOperations) {
    // Skip test if we couldn't capture client_ids
    if ((test_client_id_.first == 0 && test_client_id_.second == 0) ||
        (segment_provider_client_id_.first == 0 &&
         segment_provider_client_id_.second == 0)) {
        GTEST_SKIP()
            << "Could not capture client_ids, skipping BatchQueryIp test";
    }

    // Test 1: Query IP for test_client_
    std::vector<UUID> client_ids = {test_client_id_};
    auto result = test_client_->BatchQueryIp(client_ids);

    ASSERT_TRUE(result.has_value())
        << "BatchQueryIp failed: " << toString(result.error());

    const auto& results = result.value();
    ASSERT_FALSE(results.empty()) << "BatchQueryIp returned empty results";

    auto it = results.find(test_client_id_);
    ASSERT_NE(it, results.end()) << "test_client_id not found in results";

    const auto& ip_addresses = it->second;
    ASSERT_FALSE(ip_addresses.empty())
        << "test_client_ should have at least one IP address";

    LOG(INFO) << "test_client_ IP addresses (" << ip_addresses.size() << "):";
    for (size_t i = 0; i < ip_addresses.size(); ++i) {
        LOG(INFO) << "  [" << (i + 1) << "] " << ip_addresses[i];
    }

    // Verify IP addresses are valid (should contain "127.0.0.1" or "localhost")
    bool has_valid_ip = false;
    for (const auto& ip : ip_addresses) {
        if (ip == "127.0.0.1" || ip.find("127.0.0.1") != std::string::npos ||
            ip == "localhost" || ip.find("localhost") != std::string::npos) {
            has_valid_ip = true;
            break;
        }
    }
    EXPECT_TRUE(has_valid_ip) << "Expected at least one valid IP address";

    // Test 2: Query IP for multiple client_ids
    std::vector<UUID> multiple_ids = {test_client_id_,
                                      segment_provider_client_id_};
    auto multi_result = test_client_->BatchQueryIp(multiple_ids);

    ASSERT_TRUE(multi_result.has_value())
        << "BatchQueryIp failed for multiple client_ids: "
        << toString(multi_result.error());

    const auto& multi_results = multi_result.value();

    // Verify test_client_id_ is in results
    auto test_it = multi_results.find(test_client_id_);
    if (test_it != multi_results.end()) {
        EXPECT_FALSE(test_it->second.empty())
            << "test_client_ should have IP addresses";
    }

    // Verify segment_provider_client_id_ is in results
    auto provider_it = multi_results.find(segment_provider_client_id_);
    if (provider_it != multi_results.end()) {
        EXPECT_FALSE(provider_it->second.empty())
            << "segment_provider_client_ should have IP addresses";
        LOG(INFO) << "segment_provider_client_ IP addresses ("
                  << provider_it->second.size() << "):";
        for (size_t i = 0; i < provider_it->second.size(); ++i) {
            LOG(INFO) << "  [" << (i + 1) << "] " << provider_it->second[i];
        }
    }

    // Test 3: Query with empty client_ids list
    std::vector<UUID> empty_client_ids;
    auto empty_result = test_client_->BatchQueryIp(empty_client_ids);

    ASSERT_TRUE(empty_result.has_value());
    EXPECT_TRUE(empty_result.value().empty())
        << "Empty client_ids should return empty results";

    // Test 4: Query with non-existent client_id (should be silently skipped)
    UUID non_existent_client_id = generate_uuid();
    std::vector<UUID> non_existent_ids = {non_existent_client_id};
    auto non_existent_result = test_client_->BatchQueryIp(non_existent_ids);

    ASSERT_TRUE(non_existent_result.has_value());
    // Non-existent client_id should not be in results (silently skipped)
    EXPECT_TRUE(non_existent_result.value().empty() ||
                non_existent_result.value().find(non_existent_client_id) ==
                    non_existent_result.value().end())
        << "Non-existent client_id should not be in results";
}

// Test batch put with duplicate keys
TEST_F(ClientIntegrationTest, BatchPutDuplicateKeys) {
    const std::string test_data = "test_data_duplicate";
    const std::string key = "duplicate_key";

    // Create two identical keys
    std::vector<std::string> keys = {key, key};
    std::vector<std::vector<Slice>> batched_slices;

    // Prepare data for both keys
    for (int i = 0; i < 2; i++) {
        std::vector<Slice> slices;
        void* buffer = client_buffer_allocator_->allocate(test_data.size());
        memcpy(buffer, test_data.data(), test_data.size());
        slices.emplace_back(Slice{buffer, test_data.size()});
        batched_slices.push_back(std::move(slices));
    }

    ReplicateConfig config;
    config.replica_num = 1;

    // Test batch put with duplicate keys
    auto batch_put_results =
        test_client_->BatchPut(keys, batched_slices, config);

    // Check that we got results for both operations
    ASSERT_EQ(batch_put_results.size(), 2);

    // Both of them should success
    // Because we currently consider `OBJECT_ALREADY_EXISTS` as success

    for (const auto& result : batch_put_results) {
        ASSERT_TRUE(result.has_value())
            << "BatchPut operation failed: " << toString(result.error());
    }

    // Clean up allocated memory
    for (const auto& slices : batched_slices) {
        for (const auto& slice : slices) {
            client_buffer_allocator_->deallocate(slice.ptr, slice.size);
        }
    }

    // Clean up the key that was successfully put
    std::this_thread::sleep_for(
        std::chrono::milliseconds(default_kv_lease_ttl_));
    auto remove_result = test_client_->Remove(key);
    // Remove might fail if the key wasn't actually put, which is fine
    ASSERT_TRUE(remove_result);
}

// Test BatchReplicaClear operations through the client
TEST_F(ClientIntegrationTest, BatchReplicaClearOperations) {
    // Skip test if we couldn't capture client_id
    if (test_client_id_.first == 0 && test_client_id_.second == 0) {
        GTEST_SKIP() << "Could not capture test_client_id, skipping "
                        "BatchReplicaClear test";
    }

    const std::string test_data = "Test data for BatchReplicaClear";
    std::vector<std::string> keys = {"batch_clear_key1", "batch_clear_key2",
                                     "batch_clear_key3"};

    // Test 1: Clear a single key (all segments)
    std::string key1 = keys[0];
    void* buffer = client_buffer_allocator_->allocate(test_data.size());
    memcpy(buffer, test_data.data(), test_data.size());
    std::vector<Slice> slices;
    slices.emplace_back(Slice{buffer, test_data.size()});
    ReplicateConfig config;
    config.replica_num = 1;
    auto put_result = test_client_->Put(key1, slices, config);
    ASSERT_TRUE(put_result.has_value())
        << "Put operation failed: " << toString(put_result.error());
    client_buffer_allocator_->deallocate(buffer, test_data.size());

    // Wait for lease to expire (PutEnd sets lease_timeout to now, but
    // if IsExist was called, it would grant a new lease)
    // Wait for the full lease TTL to ensure any lease granted by PutEnd or
    // other operations has expired
    std::this_thread::sleep_for(
        std::chrono::milliseconds(default_kv_lease_ttl_ + 100));
    const auto timeout = std::chrono::seconds(5);
    const auto start_time = std::chrono::steady_clock::now();
    bool cleared = false;
    std::vector<std::string> single_key = {key1};

    while (std::chrono::steady_clock::now() - start_time < timeout) {
        auto clear_result = test_client_->BatchReplicaClear(
            single_key, test_client_id_,
            "");  // Empty segment_name clears all segments
        ASSERT_TRUE(clear_result.has_value())
            << "BatchReplicaClear failed: " << toString(clear_result.error());

        if (clear_result.value().size() == 1) {
            cleared = true;
            break;
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }

    ASSERT_TRUE(cleared) << "Failed to clear key within timeout period";

    // Verify the key is removed
    auto exist_result2 = test_client_->IsExist(key1);
    ASSERT_TRUE(exist_result2.has_value());
    ASSERT_FALSE(exist_result2.value())
        << "Key should be removed after BatchReplicaClear";

    // Test 2: Clear multiple keys
    std::vector<std::string> multiple_keys = {keys[1], keys[2]};
    for (const auto& key : multiple_keys) {
        buffer = client_buffer_allocator_->allocate(test_data.size());
        memcpy(buffer, test_data.data(), test_data.size());
        slices.clear();
        slices.emplace_back(Slice{buffer, test_data.size()});
        auto put_result2 = test_client_->Put(key, slices, config);
        ASSERT_TRUE(put_result2.has_value())
            << "Put operation failed for key: " << key
            << ", error: " << toString(put_result2.error());
        client_buffer_allocator_->deallocate(buffer, test_data.size());
    }

    // Wait for lease to expire and clear
    std::this_thread::sleep_for(
        std::chrono::milliseconds(default_kv_lease_ttl_ + 100));
    const auto start_time2 = std::chrono::steady_clock::now();
    bool all_cleared = false;

    while (std::chrono::steady_clock::now() - start_time2 < timeout) {
        auto clear_result = test_client_->BatchReplicaClear(
            multiple_keys, test_client_id_, "");  // Clear all segments
        ASSERT_TRUE(clear_result.has_value())
            << "BatchReplicaClear failed: " << toString(clear_result.error());

        if (clear_result.value().size() == multiple_keys.size()) {
            all_cleared = true;
            break;
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }

    ASSERT_TRUE(all_cleared)
        << "Failed to clear all keys within timeout period";

    // Verify all keys are removed
    for (const auto& key : multiple_keys) {
        auto exist_result3 = test_client_->IsExist(key);
        ASSERT_TRUE(exist_result3.has_value());
        ASSERT_FALSE(exist_result3.value()) << "Key should be removed: " << key;
    }

    // Test 3: Clear with empty keys list
    std::vector<std::string> empty_keys;
    auto empty_result =
        test_client_->BatchReplicaClear(empty_keys, test_client_id_, "");
    ASSERT_TRUE(empty_result.has_value());
    EXPECT_TRUE(empty_result.value().empty())
        << "Empty keys should return empty results";

    // Test 4: Clear with non-existent keys (should be silently skipped)
    std::vector<std::string> non_existent_keys = {"non_existent_key1",
                                                  "non_existent_key2"};
    auto non_existent_result =
        test_client_->BatchReplicaClear(non_existent_keys, test_client_id_, "");
    ASSERT_TRUE(non_existent_result.has_value());
    // Non-existent keys should not be in results (silently skipped)
    EXPECT_TRUE(non_existent_result.value().empty())
        << "Non-existent keys should return empty results";
}

// Helper: extract transport endpoints from Query() result
static std::unordered_set<std::string> ExtractReplicaEndpoints(
    const decltype(std::declval<Client>()
                       .Query(std::declval<std::string>())
                       .value())& q) {
    std::unordered_set<std::string> endpoints;
    for (const auto& r : q.replicas) {
        // Memory replicas carry buffer_descriptor.transport_endpoint_
        endpoints.insert(
            r.get_memory_descriptor().buffer_descriptor.transport_endpoint_);
    }
    return endpoints;
}

// Helper: fill a target segment by placing many objects there until Put fails.
// Returns keys that were successfully placed on target.
static std::vector<std::string> FillSegmentUntilFull(
    const std::shared_ptr<Client>& writer_client, SimpleAllocator* writer_alloc,
    const std::string& target_segment_name, size_t value_size, int num_keys) {
    std::vector<std::string> keys;
    keys.reserve(num_keys);

    std::string payload(value_size, 'X');

    ReplicateConfig cfg;
    cfg.replica_num = 1;
    cfg.preferred_segment = target_segment_name;

    for (int i = 0; i < num_keys; ++i) {
        std::string key =
            "fill_" + target_segment_name + "_" + std::to_string(i);

        void* buf = writer_alloc->allocate(payload.size());
        if (!buf) {
            EXPECT_NE(buf, nullptr);
            return keys;
        }
        std::memcpy(buf, payload.data(), payload.size());
        std::vector<Slice> slices;
        slices.emplace_back(Slice{buf, payload.size()});

        writer_client->Put(key, slices, cfg);
        writer_alloc->deallocate(buf, payload.size());

        keys.push_back(std::move(key));
    }
    return keys;
}

// Helper: poll QueryTask until SUCCESS/FAILED or timeout.
static TaskStatus WaitTaskTerminalStatus(const std::shared_ptr<Client>& client,
                                         const UUID& task_id,
                                         std::chrono::milliseconds timeout,
                                         std::chrono::milliseconds interval) {
    const auto start = std::chrono::steady_clock::now();
    TaskStatus last = TaskStatus::PROCESSING;

    while (std::chrono::steady_clock::now() - start < timeout) {
        auto q = client->QueryTask(task_id);
        EXPECT_TRUE(q.has_value())
            << "QueryTask failed: " << toString(q.error());
        last = q.value().status;

        if (last == TaskStatus::SUCCESS || last == TaskStatus::FAILED)
            return last;
        std::this_thread::sleep_for(interval);
    }
    return last;
}

TEST_F(ClientIntegrationTest, ReplicaCopyAndMoveOperations) {
    // Create two extra clients:
    // - target_small: small segment to force allocation failure
    // - target_big: normal segment for copy/move verification
    const std::string target_small_name = "localhost:17814";
    const std::string target_big_name = "localhost:17815";

    auto target_small = CreateClient(target_small_name);
    ASSERT_TRUE(target_small != nullptr);

    auto target_big = CreateClient(target_big_name);
    ASSERT_TRUE(target_big != nullptr);

    // Mount segments for the extra clients
    constexpr size_t kSegAlign = 16 * 1024 * 1024;  // 16MB alignment
    const size_t kSmallSeg = kSegAlign;             // 16MB
    const size_t kBigSeg = 8 * kSegAlign;           // 128MB

    void* small_seg_ptr = allocate_buffer_allocator_memory(kSmallSeg);
    ASSERT_NE(small_seg_ptr, nullptr);
    auto m1 = target_small->MountSegment(small_seg_ptr, kSmallSeg);
    ASSERT_TRUE(m1.has_value())
        << "MountSegment(small) failed: " << toString(m1.error());

    void* big_seg_ptr = allocate_buffer_allocator_memory(kBigSeg);
    ASSERT_NE(big_seg_ptr, nullptr);
    auto m2 = target_big->MountSegment(big_seg_ptr, kBigSeg);
    ASSERT_TRUE(m2.has_value())
        << "MountSegment(big) failed: " << toString(m2.error());

    // --- Phase A: Fill small target (segment full) ---
    const size_t kFillValueSize = 512 * 1024;  // 512KB
    auto fill_keys = FillSegmentUntilFull(
        test_client_, client_buffer_allocator_.get(), target_small_name,
        kFillValueSize, /*num_keys=*/50);

    ASSERT_FALSE(fill_keys.empty())
        << "Failed to fill anything into target_small; test setup invalid";

    // --- Phase B: COPY retry should succeed after freeing space ---
    {
        const std::string source_key = "retry_key_success_cpp";
        std::string payload(kFillValueSize, 'A');

        // Put source object onto the source segment (test_client_ segment).
        ReplicateConfig cfg;
        cfg.replica_num = 1;
        cfg.preferred_segment = "localhost:17813";  // test_client_ name

        void* buf = client_buffer_allocator_->allocate(payload.size());
        ASSERT_NE(buf, nullptr);
        std::memcpy(buf, payload.data(), payload.size());
        std::vector<Slice> slices;
        slices.emplace_back(Slice{buf, payload.size()});
        auto put_res = test_client_->Put(source_key, slices, cfg);
        client_buffer_allocator_->deallocate(buf, payload.size());
        ASSERT_TRUE(put_res.has_value())
            << "Put(source) failed: " << toString(put_res.error());

        // Start COPY task to the full small target.
        auto copy_task =
            test_client_->CreateCopyTask(source_key, {target_small_name});
        ASSERT_TRUE(copy_task.has_value())
            << "CreateCopyTask failed: " << toString(copy_task.error());
        const auto copy_task_id = copy_task.value();

        std::this_thread::sleep_for(std::chrono::milliseconds(
            500));  // Give some time for initial attempt

        // Free space by removing one of the fill keys.
        test_client_->Remove(fill_keys.front());

        // Poll task: should succeed after space is freed.
        auto status =
            WaitTaskTerminalStatus(test_client_, copy_task_id,
                                   /*timeout=*/std::chrono::seconds(30),
                                   /*interval=*/std::chrono::milliseconds(200));

        ASSERT_EQ(status, TaskStatus::SUCCESS)
            << "COPY task did not succeed after freeing space";

        // Verify replica now exists on target_small (by endpoint)
        auto q = test_client_->Query(source_key);
        ASSERT_TRUE(q.has_value()) << "Query failed: " << toString(q.error());

        auto endpoints = ExtractReplicaEndpoints(q.value());
        EXPECT_TRUE(endpoints.contains(target_small->GetTransportEndpoint()))
            << "Expected a replica on target_small endpoint";
    }

    // --- Phase C: COPY retry should fail if we do NOT free space ---
    {
        const std::string source_key = "retry_key_fail_cpp";
        std::string payload(kFillValueSize, 'B');

        ReplicateConfig cfg;
        cfg.replica_num = 1;
        cfg.preferred_segment = "localhost:17813";

        void* buf = client_buffer_allocator_->allocate(payload.size());
        ASSERT_NE(buf, nullptr);
        std::memcpy(buf, payload.data(), payload.size());
        std::vector<Slice> slices;
        slices.emplace_back(Slice{buf, payload.size()});
        auto put_res = test_client_->Put(source_key, slices, cfg);
        client_buffer_allocator_->deallocate(buf, payload.size());
        ASSERT_TRUE(put_res.has_value())
            << "Put(source) failed: " << toString(put_res.error());

        auto copy_task =
            test_client_->CreateCopyTask(source_key, {target_small_name});
        ASSERT_TRUE(copy_task.has_value())
            << "CreateCopyTask failed: " << toString(copy_task.error());
        const auto copy_task_id = copy_task.value();

        // Do NOT free space; wait for retries to exhaust.
        auto status =
            WaitTaskTerminalStatus(test_client_, copy_task_id,
                                   /*timeout=*/std::chrono::seconds(60),
                                   /*interval=*/std::chrono::milliseconds(300));

        ASSERT_EQ(status, TaskStatus::FAILED)
            << "COPY task unexpectedly succeeded or did not fail in time";
    }

    // --- Phase D: Basic multi-client COPY and MOVE---
    {
        const int kNumKeys = 10;
        std::vector<std::string> keys;
        keys.reserve(kNumKeys);

        ReplicateConfig cfg;
        cfg.replica_num = 1;
        cfg.preferred_segment = "localhost:17813";

        // Put keys on source segment
        for (int i = 0; i < kNumKeys; ++i) {
            std::string key = "cm_key_" + std::to_string(i);
            std::string payload(4 * 1024, static_cast<char>('a' + (i % 26)));

            void* buf = client_buffer_allocator_->allocate(payload.size());
            ASSERT_NE(buf, nullptr);
            std::memcpy(buf, payload.data(), payload.size());
            std::vector<Slice> slices;
            slices.emplace_back(Slice{buf, payload.size()});

            auto r = test_client_->Put(key, slices, cfg);
            client_buffer_allocator_->deallocate(buf, payload.size());
            ASSERT_TRUE(r.has_value()) << "Put failed: " << toString(r.error());

            keys.push_back(std::move(key));
        }

        // COPY all keys to target_big
        std::vector<UUID> copy_task_ids;

        copy_task_ids.reserve(keys.size());
        for (const auto& key : keys) {
            auto t = test_client_->CreateCopyTask(key, {target_big_name});
            ASSERT_TRUE(t.has_value())
                << "CreateCopyTask failed: " << toString(t.error());
            copy_task_ids.push_back(t.value());
        }

        for (const auto& tid : copy_task_ids) {
            auto status = WaitTaskTerminalStatus(
                test_client_, tid,
                /*timeout=*/std::chrono::seconds(30),
                /*interval=*/std::chrono::milliseconds(200));
            ASSERT_EQ(status, TaskStatus::SUCCESS) << "COPY did not succeed";
        }

        // MOVE first 5 keys from source -> target_big
        std::vector<UUID> move_task_ids;

        for (int i = 0; i < 5; ++i) {
            auto t = test_client_->CreateMoveTask(keys[i], "localhost:17813",
                                                  target_big_name);
            ASSERT_TRUE(t.has_value())
                << "CreateMoveTask failed: " << toString(t.error());
            move_task_ids.push_back(t.value());
        }

        for (const auto& tid : move_task_ids) {
            auto status = WaitTaskTerminalStatus(
                test_client_, tid,
                /*timeout=*/std::chrono::seconds(30),
                /*interval=*/std::chrono::milliseconds(200));
            ASSERT_EQ(status, TaskStatus::SUCCESS) << "MOVE did not succeed";
        }

        // Verify moved keys are on target_big and (ideally) not on source
        // endpoint
        const auto source_ep = test_client_->GetTransportEndpoint();
        const auto target_ep = target_big->GetTransportEndpoint();

        for (int i = 0; i < 5; ++i) {
            auto q = test_client_->Query(keys[i]);
            ASSERT_TRUE(q.has_value())
                << "Query failed: " << toString(q.error());
            auto eps = ExtractReplicaEndpoints(q.value());

            EXPECT_TRUE(eps.contains(target_ep))
                << "Moved key missing on target_big";
            EXPECT_FALSE(eps.contains(source_ep))
                << "Moved key still present on source";
        }
    }

    // Unmount and free extra segments
    auto u1 = target_small->UnmountSegment(small_seg_ptr, kSmallSeg);
    EXPECT_TRUE(u1.has_value()) << "UnmountSegment(small) failed";
    auto u2 = target_big->UnmountSegment(big_seg_ptr, kBigSeg);
    EXPECT_TRUE(u2.has_value()) << "UnmountSegment(big) failed";

    std::free(small_seg_ptr);
    std::free(big_seg_ptr);
}

}  // namespace testing

}  // namespace mooncake

int main(int argc, char** argv) {
    // Initialize Google Test
    ::testing::InitGoogleTest(&argc, argv);

    // Initialize Google's flags library
    gflags::ParseCommandLineFlags(&argc, &argv, false);
    mooncake::init_ylt_log_level();
    // Run all tests
    return RUN_ALL_TESTS();
}
