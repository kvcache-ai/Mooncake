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

#include <unistd.h>
#include <fcntl.h>
#include <sys/mman.h>

#include "allocator.h"
#include "client_service.h"
#include "types.h"
#include "utils.h"
#include "test_server_helpers.h"
#include "default_config.h"

DEFINE_string(protocol, "cxl", "Transfer protocol: rdma|tcp|cxl");
DEFINE_string(device_name, "", "Device name to use, valid if protocol=rdma");
DEFINE_uint64(default_kv_lease_ttl, mooncake::DEFAULT_DEFAULT_KV_LEASE_TTL,
              "Default lease time for kv objects, must be set to the "
              "same as the master's default_kv_lease_ttl");
DEFINE_string(cxl_device_name, "tmp_dax_sim", "Device name for cxl");
DEFINE_uint64(cxl_device_size, 1073741824, "Device Size for cxl");
DEFINE_bool(auto_disc, false, "Auto discover tcp devices");
DEFINE_string(transfer_engine_metadata_url, "127.0.0.1:2379",
              "Metadata connection string for transfer engine");

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

class ClientIntegrationTestCxl : public ::testing::Test {
   protected:
    static std::shared_ptr<Client> CreateClient(const std::string& host_name) {
        auto client_opt = Client::Create(
            host_name,                           // Local hostname
            FLAGS_transfer_engine_metadata_url,  // Metadata connection string
            FLAGS_protocol,                      // Transfer protocol
            std::nullopt,  // RDMA device names (auto-discovery)
            master_address_);

        if (!client_opt.has_value()) {
            LOG(WARNING) << "Failed to create client with host_name: " << host_name;
            return nullptr;
        }
        return client_opt.value();
    }

    static void SetUpTestSuite() {
        // Initialize glog
        google::InitGoogleLogging("ClientIntegrationTestCxl");

        FLAGS_logtostderr = 1;

        tmp_fd = open(FLAGS_cxl_device_name.c_str(), O_RDWR | O_CREAT, 0666);
        ASSERT_GE(tmp_fd, 0);
        ASSERT_EQ(ftruncate(tmp_fd, FLAGS_cxl_device_size), 0);

        // Override flags from environment variables if present
        setenv("MC_CXL_DEV_PATH", FLAGS_cxl_device_name.c_str(), 1);
        setenv("MC_CXL_DEV_SIZE", std::to_string(FLAGS_cxl_device_size).c_str(),
               1);
        setenv("MC_MS_AUTO_DISC", std::to_string(FLAGS_auto_disc).c_str(), 0);

        if (getenv("DEFAULT_KV_LEASE_TTL")) {
            default_kv_lease_ttl_ = std::stoul(getenv("DEFAULT_KV_LEASE_TTL"));
        } else {
            default_kv_lease_ttl_ = FLAGS_default_kv_lease_ttl;
        }
        LOG(INFO) << "Default KV lease TTL: " << default_kv_lease_ttl_;

        // Start in-proc master
        InProcMasterConfig config = InProcMasterConfigBuilder()
                                        .set_enable_cxl(true)
                                        .set_cxl_path(FLAGS_cxl_device_name)
                                        .set_cxl_size(FLAGS_cxl_device_size)
                                        .build();

        ASSERT_TRUE(master_.Start(config)) << "Failed to start InProcMaster!";
        master_address_ = master_.master_address();
        LOG(INFO) << "Started in-proc master at " << master_address_;
        InitializeClients();
        InitializeSegment();
    }

    static void TearDownTestSuite() {
        CleanupSegment();
        CleanupClients();
        master_.Stop();
        google::ShutdownGoogleLogging();
        if (tmp_fd >= 0) {
            close(tmp_fd);
            unlink(FLAGS_cxl_device_name.c_str());
        }
    }

    static void InitializeSegment() {
        // Skip if client creation failed (CXL device unavailable)
        if (test_client_ == nullptr) {
            return;
        }

        // init local buffer allocator
        client_buffer_allocator_ =
            std::make_unique<SimpleAllocator>(128 * 1024 * 1024);

        // Mount segment for test_client_ as well
        ASSERT_TRUE(FLAGS_protocol == "cxl");

        size_t cxl_dev_size = 0;
        const char* env_cxl_size = getenv("MC_CXL_DEV_SIZE");
        if (env_cxl_size) {
            char* end = nullptr;
            unsigned long long val = std::strtoull(env_cxl_size, &end, 10);
            if (end != env_cxl_size && *end == '\0')
                cxl_dev_size = static_cast<size_t>(val);
        } else {
            LOG(FATAL) << "MC_CXL_DEV_SIZE environment variable not set "
                          "(required for CXL protocol)";
            return;
        }
        test_client_ram_buffer_size_ = cxl_dev_size;
        test_client_segment_ptr_ = test_client_->GetBaseAddr();

        LOG_ASSERT(test_client_segment_ptr_);
        LOG(INFO) << "test_client_segment_ptr_: " << test_client_segment_ptr_;

        auto test_client_mount_result = test_client_->MountSegment(
            test_client_segment_ptr_, test_client_ram_buffer_size_,
            FLAGS_protocol);
        if (!test_client_mount_result.has_value()) {
            LOG(ERROR) << "Failed to mount CXL segment for test_client_: "
                       << toString(test_client_mount_result.error());
        }
        LOG(INFO) << "Test client CXL segment mounted successfully";
    }

    static void InitializeClients() {
        // This client is used for testing purposes.
        // Capture test_client_ client_id from logs
        ClientIdCaptureSink* test_client_sink = new ClientIdCaptureSink();
        google::AddLogSink(test_client_sink);

        test_client_ = CreateClient("localhost:17813");
        if (test_client_ == nullptr) {
            google::RemoveLogSink(test_client_sink);
            delete test_client_sink;
            cxl_available_ = false;
            LOG(WARNING) << "CXL device not available, tests will be skipped";
            return;
        }

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
    }

    static void CleanupClients() {
        if (test_client_) {
            test_client_.reset();
        }
    }

    static void CleanupSegment() {
        // Unmount test client segment first
        if (test_client_ && test_client_segment_ptr_) {
            if (!test_client_
                     ->UnmountSegment(test_client_segment_ptr_,
                                      test_client_ram_buffer_size_)
                     .has_value()) {
                LOG(ERROR) << "Failed to unmount test client CXL segment";
            }
        }
    }

    static std::shared_ptr<Client> test_client_;
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
    static inline bool is_cxl = false;
    static inline bool cxl_available_ = true;
    static int tmp_fd;
};

// Static members initialization
std::shared_ptr<Client> ClientIntegrationTestCxl::test_client_ = nullptr;
void* ClientIntegrationTestCxl::segment_ptr_ = nullptr;
void* ClientIntegrationTestCxl::test_client_segment_ptr_ = nullptr;
std::unique_ptr<SimpleAllocator>
    ClientIntegrationTestCxl::client_buffer_allocator_ = nullptr;
size_t ClientIntegrationTestCxl::ram_buffer_size_ = 0;
size_t ClientIntegrationTestCxl::test_client_ram_buffer_size_ = 0;
uint64_t ClientIntegrationTestCxl::default_kv_lease_ttl_ = 0;
InProcMaster ClientIntegrationTestCxl::master_;
std::string ClientIntegrationTestCxl::master_address_;
std::string ClientIntegrationTestCxl::metadata_url_;
UUID ClientIntegrationTestCxl::test_client_id_{0, 0};
int ClientIntegrationTestCxl::tmp_fd = -1;

// Test basic Put/Get operations through the client
TEST_F(ClientIntegrationTestCxl, BasicPutGetOperations) {
    if (!cxl_available_) {
        GTEST_SKIP() << "CXL device not available";
    }
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

// Test batch Put/Get operations through the client
TEST_F(ClientIntegrationTestCxl, BatchPutGetOperations) {
    if (!cxl_available_) {
        GTEST_SKIP() << "CXL device not available";
    }
    int batch_sz = 10;
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