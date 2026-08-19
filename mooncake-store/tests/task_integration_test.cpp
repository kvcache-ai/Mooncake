#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include <csignal>
#include <chrono>
#include <memory>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include <ylt/coro_http/coro_http_client.hpp>
#include <ylt/struct_json/json_reader.h>
#include <ylt/struct_json/json_writer.h>
#include <ylt/reflection/user_reflect_macro.hpp>

#include "client_service.h"
#include "master_client.h"
#include "task_manager.h"
#include "types.h"
#include "utils.h"
#include "test_server_helpers.h"
#include "default_config.h"
#include "allocator.h"

DEFINE_string(protocol, "tcp", "Transfer protocol: rdma|tcp");
DEFINE_string(device_name, "", "Device name to use, valid if protocol=rdma");
DEFINE_uint64(default_kv_lease_ttl, mooncake::DEFAULT_DEFAULT_KV_LEASE_TTL,
              "Default lease time for kv objects, must be set to the "
              "same as the master's default_kv_lease_ttl");

namespace mooncake {
namespace testing {

namespace {

constexpr char kClient1Endpoint[] = "127.0.0.1:18001";
constexpr char kClient2Endpoint[] = "127.0.0.1:18002";
constexpr char kClient3Endpoint[] = "127.0.0.1:18003";

struct HttpCreateDrainJobResponse {
    bool success{false};
    std::string job_id;
    std::string status;
    int32_t error_code{0};
    std::string error_message;
};
YLT_REFL(HttpCreateDrainJobResponse, success, job_id, status, error_code,
         error_message);

struct HttpQueryDrainJobResponse {
    bool success{false};
    std::string job_id;
    int32_t type{0};
    std::string type_name;
    int32_t status{0};
    std::string status_name;
    int64_t created_at_ms_epoch{0};
    int64_t last_updated_at_ms_epoch{0};
    std::vector<std::string> segments;
    uint64_t succeeded_units{0};
    uint64_t failed_units{0};
    uint64_t blocked_units{0};
    uint64_t active_units{0};
    uint64_t migrated_bytes{0};
    std::string message;
    int32_t error_code{0};
    std::string error_message;
};
YLT_REFL(HttpQueryDrainJobResponse, success, job_id, type, type_name, status,
         status_name, created_at_ms_epoch, last_updated_at_ms_epoch, segments,
         succeeded_units, failed_units, blocked_units, active_units,
         migrated_bytes, message, error_code, error_message);

struct HttpSegmentStatusResponse {
    bool success{false};
    std::string segment;
    int32_t status{0};
    std::string status_name;
    int32_t error_code{0};
    std::string error_message;
};
YLT_REFL(HttpSegmentStatusResponse, success, segment, status, status_name,
         error_code, error_message);

tl::expected<std::string, int> HttpPostJson(const std::string& url,
                                            const std::string& body) {
    coro_http::coro_http_client client;
    auto response = client.post(url, body, coro_http::req_content_type::json);
    if (response.status != 200) {
        return tl::unexpected(response.status);
    }
    return std::string(response.resp_body);
}

}  // namespace

class TaskExecutorIntegrationTest : public ::testing::Test {
   protected:
    static void SetUpTestSuite() {
        google::InitGoogleLogging("TaskExecutorIntegrationTest");
        FLAGS_logtostderr = 1;

        // Override flags from environment variables if present
        if (getenv("PROTOCOL")) FLAGS_protocol = getenv("PROTOCOL");
        if (getenv("DEVICE_NAME")) FLAGS_device_name = getenv("DEVICE_NAME");

        LOG(INFO) << "Protocol: " << FLAGS_protocol
                  << ", Device name: " << FLAGS_device_name;

        uint64_t default_kv_lease_ttl = DEFAULT_DEFAULT_KV_LEASE_TTL;
        if (getenv("DEFAULT_KV_LEASE_TTL")) {
            default_kv_lease_ttl = std::stoul(getenv("DEFAULT_KV_LEASE_TTL"));
        } else {
            default_kv_lease_ttl = FLAGS_default_kv_lease_ttl;
        }

        // Start an in-process non-HA master with HTTP metadata server
        InProcMasterConfig config;
        config.http_metadata_port = getFreeTcpPort();
        config.default_kv_lease_ttl = default_kv_lease_ttl;

        ASSERT_TRUE(master_.Start(config));
        master_address_ = master_.master_address();
        metadata_url_ = master_.metadata_url();

        LOG(INFO) << "Started in-proc master at " << master_address_
                  << ", metadata_url=" << metadata_url_;
    }

    static void TearDownTestSuite() {
        CleanupClients();
        master_.Stop();
        google::ShutdownGoogleLogging();
    }

    void SetUp() override {
        // Create client 1
        auto client1_opt = Client::Create(
            kClient1Endpoint,
            metadata_url_.empty() ? "P2PHANDSHAKE" : metadata_url_,
            FLAGS_protocol,
            FLAGS_device_name.empty() ? std::nullopt
                                      : std::make_optional(FLAGS_device_name),
            master_address_);
        ASSERT_TRUE(client1_opt.has_value());
        client1_ = client1_opt.value();

        // Create client 2
        auto client2_opt = Client::Create(
            kClient2Endpoint,
            metadata_url_.empty() ? "P2PHANDSHAKE" : metadata_url_,
            FLAGS_protocol,
            FLAGS_device_name.empty() ? std::nullopt
                                      : std::make_optional(FLAGS_device_name),
            master_address_);
        ASSERT_TRUE(client2_opt.has_value());
        client2_ = client2_opt.value();

        // Create master client for API calls
        UUID master_client_id = generate_uuid();
        master_client_ = std::make_unique<MasterClient>(master_client_id);
        ASSERT_EQ(master_client_->Connect(master_address_), ErrorCode::OK);

        // Mount segments for both clients
        size_t segment_size = 256 * 1024 * 1024;  // 256 MB per segment

        // Client 1 segment
        client1_segment_ptr_ = allocate_buffer_allocator_memory(segment_size);
        ASSERT_NE(client1_segment_ptr_, nullptr);
        auto mount1_result =
            client1_->MountSegment(client1_segment_ptr_, segment_size);
        ASSERT_TRUE(mount1_result.has_value())
            << "Failed to mount segment for client1: "
            << toString(mount1_result.error());

        // Client 2 segment
        client2_segment_ptr_ = allocate_buffer_allocator_memory(segment_size);
        ASSERT_NE(client2_segment_ptr_, nullptr);
        auto mount2_result =
            client2_->MountSegment(client2_segment_ptr_, segment_size);
        ASSERT_TRUE(mount2_result.has_value())
            << "Failed to mount segment for client2: "
            << toString(mount2_result.error());

        // Wait for segments to be registered and clients to ping master
        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    }

    void TearDown() override {
        if (client1_ && client1_segment_ptr_) {
            free(client1_segment_ptr_);
            client1_segment_ptr_ = nullptr;
        }
        if (client2_ && client2_segment_ptr_) {
            free(client2_segment_ptr_);
            client2_segment_ptr_ = nullptr;
        }
        if (client3_ && client3_segment_ptr_) {
            free(client3_segment_ptr_);
            client3_segment_ptr_ = nullptr;
        }
    }

    static void CleanupClients() {
        // Clients will be cleaned up by shared_ptr
    }

    void MountThirdClientSegment() {
        auto client3_opt = Client::Create(
            kClient3Endpoint,
            metadata_url_.empty() ? "P2PHANDSHAKE" : metadata_url_,
            FLAGS_protocol,
            FLAGS_device_name.empty() ? std::nullopt
                                      : std::make_optional(FLAGS_device_name),
            master_address_);
        ASSERT_TRUE(client3_opt.has_value());
        client3_ = client3_opt.value();

        constexpr size_t kSegmentSize = 256 * 1024 * 1024;
        client3_segment_ptr_ = allocate_buffer_allocator_memory(kSegmentSize);
        ASSERT_NE(client3_segment_ptr_, nullptr);
        auto mount_result =
            client3_->MountSegment(client3_segment_ptr_, kSegmentSize);
        ASSERT_TRUE(mount_result.has_value())
            << "Failed to mount segment for client3: "
            << toString(mount_result.error());

        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    }

    // Wait for task to complete by polling task status
    bool WaitForTaskCompletion(
        const UUID& task_id,
        std::chrono::seconds timeout = std::chrono::seconds(30)) {
        auto start = std::chrono::steady_clock::now();
        while (std::chrono::steady_clock::now() - start < timeout) {
            auto query_result = master_client_->QueryTask(task_id);
            if (query_result.has_value()) {
                const auto& task_response = query_result.value();
                if (task_response.status == TaskStatus::SUCCESS ||
                    task_response.status == TaskStatus::FAILED) {
                    return task_response.status == TaskStatus::SUCCESS;
                }
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(200));
        }
        return false;
    }

    tl::expected<HttpCreateDrainJobResponse, int> CreateDrainJobViaHttp(
        const CreateDrainJobRequest& request) {
        std::string body;
        struct_json::to_json(request, body);
        auto response = HttpPostJson(
            master_.http_metrics_base() + "/api/v1/drain_jobs", body);
        if (!response.has_value()) {
            return tl::unexpected(response.error());
        }

        HttpCreateDrainJobResponse parsed;
        struct_json::from_json(parsed, response.value());
        return parsed;
    }

    tl::expected<HttpQueryDrainJobResponse, int> QueryDrainJobViaHttp(
        const std::string& job_id) {
        auto response = httpGet(master_.http_metrics_base() +
                                "/api/v1/drain_jobs/query?job_id=" + job_id);
        if (!response.has_value()) {
            return tl::unexpected(response.error());
        }

        HttpQueryDrainJobResponse parsed;
        struct_json::from_json(parsed, response.value());
        return parsed;
    }

    tl::expected<HttpSegmentStatusResponse, int> QuerySegmentStatusViaHttp(
        const std::string& segment_name) {
        auto response =
            httpGet(master_.http_metrics_base() +
                    "/api/v1/segments/status?segment=" + segment_name);
        if (!response.has_value()) {
            return tl::unexpected(response.error());
        }

        HttpSegmentStatusResponse parsed;
        struct_json::from_json(parsed, response.value());
        return parsed;
    }

    bool WaitForJobCompletionViaHttp(
        const std::string& job_id, HttpQueryDrainJobResponse* final_job,
        std::chrono::seconds timeout = std::chrono::seconds(30)) {
        auto start = std::chrono::steady_clock::now();
        while (std::chrono::steady_clock::now() - start < timeout) {
            auto query_result = QueryDrainJobViaHttp(job_id);
            if (query_result.has_value()) {
                if (query_result->status_name == "SUCCEEDED" ||
                    query_result->status_name == "FAILED" ||
                    query_result->status_name == "CANCELED") {
                    if (final_job != nullptr) {
                        *final_job = query_result.value();
                    }
                    return query_result->status_name == "SUCCEEDED";
                }
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(200));
        }
        return false;
    }

    void ExpectReplicaEndpoints(
        const std::string& key,
        const std::set<std::string>& expected_endpoints) {
        auto query_result = client1_->Query(key);
        ASSERT_TRUE(query_result.has_value())
            << "Failed to query replicas for key=" << key << ": "
            << toString(query_result.error());
        ASSERT_EQ(query_result->replicas.size(), expected_endpoints.size())
            << "Unexpected replica count for key=" << key;

        std::set<std::string> actual_endpoints;
        for (const auto& replica : query_result->replicas) {
            ASSERT_TRUE(replica.is_memory_replica())
                << "Expected memory replica for key=" << key;
            actual_endpoints.insert(replica.get_memory_descriptor()
                                        .buffer_descriptor.transport_endpoint_);
        }
        EXPECT_EQ(actual_endpoints, expected_endpoints)
            << "Unexpected replica placement for key=" << key;
    }

   protected:
    static InProcMaster master_;
    static std::string master_address_;
    static std::string metadata_url_;

    std::shared_ptr<Client> client1_;
    std::shared_ptr<Client> client2_;
    std::shared_ptr<Client> client3_;
    std::unique_ptr<MasterClient> master_client_;

    void* client1_segment_ptr_ = nullptr;
    void* client2_segment_ptr_ = nullptr;
    void* client3_segment_ptr_ = nullptr;
};

InProcMaster TaskExecutorIntegrationTest::master_;
std::string TaskExecutorIntegrationTest::master_address_;
std::string TaskExecutorIntegrationTest::metadata_url_;

// Test complete replica copy flow
TEST_F(TaskExecutorIntegrationTest, ReplicaCopyCompleteFlow) {
    // Step 1: Put data on client1
    std::string test_key =
        "test_copy_key_" +
        std::to_string(
            std::chrono::steady_clock::now().time_since_epoch().count());
    std::string test_data =
        "This is test data for replica copy operation. "
        "It should be copied from client1 to client2 segment.";

    std::vector<Slice> slices;
    slices.emplace_back(test_data.data(), test_data.size());
    ReplicateConfig config;
    config.replica_num = 1;
    config.preferred_segment = kClient1Endpoint;

    auto put_result = client1_->Put(test_key, slices, config);
    ASSERT_TRUE(put_result.has_value())
        << "Failed to put data: " << toString(put_result.error());

    // Wait a bit for put to complete
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    ExpectReplicaEndpoints(test_key, {kClient1Endpoint});

    // Step 2: Create copy task via master
    std::vector<std::string> targets = {kClient2Endpoint};
    auto copy_result = master_client_->CreateCopyTask(test_key, targets);
    ASSERT_TRUE(copy_result.has_value())
        << "Failed to create copy task: " << toString(copy_result.error());

    UUID task_id = copy_result.value();

    // Step 3: Wait for task to be fetched and executed by the assigned client
    bool task_completed =
        WaitForTaskCompletion(task_id, std::chrono::seconds(30));
    ASSERT_TRUE(task_completed) << "Task did not complete within timeout";

    // Step 4: Copy adds the target and preserves the source replica.
    ExpectReplicaEndpoints(test_key, {kClient1Endpoint, kClient2Endpoint});

    // Step 5: Verify data integrity.
    std::vector<uint8_t> read_buffer(test_data.size());
    std::vector<Slice> read_slices;
    read_slices.emplace_back(read_buffer.data(), read_buffer.size());

    auto get_result = client2_->Get(test_key, read_slices);
    ASSERT_TRUE(get_result.has_value())
        << "Failed to get data from client2: " << toString(get_result.error());

    std::string read_data(reinterpret_cast<const char*>(read_buffer.data()),
                          test_data.size());
    ASSERT_EQ(read_data, test_data) << "Data mismatch after copy";
}

// Test complete replica move flow
TEST_F(TaskExecutorIntegrationTest, ReplicaMoveCompleteFlow) {
    // Step 1: Put data on client1
    std::string test_key =
        "test_move_key_" +
        std::to_string(
            std::chrono::steady_clock::now().time_since_epoch().count());
    std::string test_data =
        "This is test data for replica move operation. "
        "It should be moved from client1 to client2 segment.";

    std::vector<Slice> slices;
    slices.emplace_back(test_data.data(), test_data.size());
    ReplicateConfig config;
    config.replica_num = 1;
    config.preferred_segment = kClient1Endpoint;

    auto put_result = client1_->Put(test_key, slices, config);
    ASSERT_TRUE(put_result.has_value())
        << "Failed to put data: " << toString(put_result.error());

    // Wait a bit for put to complete
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    ExpectReplicaEndpoints(test_key, {kClient1Endpoint});

    // Step 2: Create move task via master
    auto move_result = master_client_->CreateMoveTask(
        test_key, kClient1Endpoint, kClient2Endpoint);
    ASSERT_TRUE(move_result.has_value())
        << "Failed to create move task: " << toString(move_result.error());

    UUID task_id = move_result.value();

    // Step 3: Wait for task to be fetched and executed by the assigned client
    bool task_completed =
        WaitForTaskCompletion(task_id, std::chrono::seconds(30));
    ASSERT_TRUE(task_completed) << "Task did not complete within timeout";

    // Step 4: Move adds the target and removes the source replica.
    ExpectReplicaEndpoints(test_key, {kClient2Endpoint});

    // Step 5: Verify data integrity.
    std::vector<uint8_t> read_buffer(test_data.size());
    std::vector<Slice> read_slices;
    read_slices.emplace_back(read_buffer.data(), read_buffer.size());

    auto get_result = client2_->Get(test_key, read_slices);
    ASSERT_TRUE(get_result.has_value())
        << "Failed to get data from client2: " << toString(get_result.error());

    std::string read_data(reinterpret_cast<const char*>(read_buffer.data()),
                          test_data.size());
    ASSERT_EQ(read_data, test_data) << "Data mismatch after move";
}

// Test complete drain job flow
TEST_F(TaskExecutorIntegrationTest, DrainJobCompleteFlow) {
    const std::string source_segment = "127.0.0.1:18001";
    const std::string target_segment = "127.0.0.1:18002";
    const auto key_prefix =
        "test_drain_job_key_" +
        std::to_string(
            std::chrono::steady_clock::now().time_since_epoch().count());

    const auto make_payload = [](const std::string& prefix, size_t payload_size,
                                 char fill) {
        std::string payload = prefix;
        if (payload.size() < payload_size) {
            payload.resize(payload_size, fill);
        }
        return payload;
    };

    std::vector<std::pair<std::string, std::string>> preload_items;
    std::vector<std::pair<std::string, std::string>> redirected_items;
    for (int i = 0; i < 12; ++i) {
        preload_items.emplace_back(
            key_prefix + "_preload_" + std::to_string(i),
            make_payload("preload-" + std::to_string(i) + "-", 128 * 1024,
                         static_cast<char>('a' + (i % 26))));
    }
    for (int i = 0; i < 4; ++i) {
        redirected_items.emplace_back(
            key_prefix + "_redirect_" + std::to_string(i),
            make_payload("redirect-" + std::to_string(i) + "-", 32 * 1024,
                         static_cast<char>('A' + (i % 26))));
    }

    const auto put_to_preferred_segment =
        [&](const std::string& key, const std::string& value,
            const std::string& preferred_segment) {
            std::vector<Slice> slices;
            slices.emplace_back(const_cast<char*>(value.data()), value.size());
            ReplicateConfig config;
            config.replica_num = 1;
            config.preferred_segment = preferred_segment;

            auto put_result = client1_->Put(key, slices, config);
            ASSERT_TRUE(put_result.has_value())
                << "Failed to put key=" << key << ": "
                << toString(put_result.error());
        };

    const auto query_segments =
        [&](const std::string& key) -> std::vector<std::string> {
        auto query_result = client2_->Query(key);
        if (!query_result.has_value()) {
            ADD_FAILURE() << "Failed to query key=" << key << ": "
                          << toString(query_result.error());
            return {};
        }
        std::vector<std::string> segments;
        for (const auto& replica : query_result->replicas) {
            if (!replica.is_memory_replica()) {
                ADD_FAILURE() << "Expected memory replica for key=" << key;
                return {};
            }
            segments.push_back(replica.get_memory_descriptor()
                                   .buffer_descriptor.transport_endpoint_);
        }
        return segments;
    };

    const auto wait_for_segments = [&](const std::string& key,
                                       const std::string& required_segment,
                                       const std::string& forbidden_segment) {
        auto deadline =
            std::chrono::steady_clock::now() + std::chrono::seconds(10);
        std::vector<std::string> last_segments;
        while (std::chrono::steady_clock::now() < deadline) {
            last_segments = query_segments(key);
            bool has_required = false;
            bool has_forbidden = false;
            for (const auto& segment : last_segments) {
                if (segment == required_segment) {
                    has_required = true;
                }
                if (!forbidden_segment.empty() &&
                    segment == forbidden_segment) {
                    has_forbidden = true;
                }
            }
            if (has_required && !has_forbidden) {
                return;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        FAIL() << "Replica placement mismatch for key=" << key
               << ", required_segment=" << required_segment
               << ", forbidden_segment=" << forbidden_segment << ", last_seen="
               << (last_segments.empty() ? std::string("<empty>")
                                         : last_segments.front());
    };

    const auto assert_key_data = [&](const std::string& key,
                                     const std::string& expected_value) {
        std::vector<uint8_t> read_buffer(expected_value.size());
        std::vector<Slice> read_slices;
        read_slices.emplace_back(read_buffer.data(), read_buffer.size());

        auto get_result = client2_->Get(key, read_slices);
        ASSERT_TRUE(get_result.has_value())
            << "Failed to get key=" << key << ": "
            << toString(get_result.error());

        std::string read_data(reinterpret_cast<const char*>(read_buffer.data()),
                              expected_value.size());
        ASSERT_EQ(read_data, expected_value) << "Data mismatch for key=" << key;
    };

    size_t expected_migrated_bytes = 0;
    for (const auto& [key, value] : preload_items) {
        put_to_preferred_segment(key, value, source_segment);
        wait_for_segments(key, source_segment, "");
        expected_migrated_bytes += value.size();
    }

    CreateDrainJobRequest request;
    request.segments = {source_segment};
    request.target_segments = {target_segment};
    request.max_concurrency = 1;

    auto create_job_result = CreateDrainJobViaHttp(request);
    ASSERT_TRUE(create_job_result.has_value())
        << "Failed to create drain job over HTTP, status="
        << create_job_result.error();
    ASSERT_TRUE(create_job_result->success)
        << "Drain job create returned error: "
        << create_job_result->error_message;
    EXPECT_EQ(create_job_result->status, "CREATED");

    auto draining_status = QuerySegmentStatusViaHttp(source_segment);
    ASSERT_TRUE(draining_status.has_value())
        << "Failed to query segment status over HTTP, status="
        << draining_status.error();
    EXPECT_TRUE(draining_status->success);
    EXPECT_EQ(draining_status->status_name, "DRAINING");

    for (const auto& [key, value] : redirected_items) {
        put_to_preferred_segment(key, value, source_segment);
        wait_for_segments(key, target_segment, source_segment);
    }

    HttpQueryDrainJobResponse final_job;
    ASSERT_TRUE(
        WaitForJobCompletionViaHttp(create_job_result->job_id, &final_job))
        << "Drain job did not complete within timeout";

    EXPECT_EQ(final_job.status_name, "SUCCEEDED");
    EXPECT_EQ(final_job.active_units, 0u);
    EXPECT_EQ(final_job.failed_units, 0u);
    EXPECT_GE(final_job.succeeded_units, preload_items.size());
    EXPECT_GE(final_job.migrated_bytes, expected_migrated_bytes);

    auto drained_status = QuerySegmentStatusViaHttp(source_segment);
    ASSERT_TRUE(drained_status.has_value())
        << "Failed to query drained segment status over HTTP, status="
        << drained_status.error();
    EXPECT_TRUE(drained_status->success);
    EXPECT_EQ(drained_status->status_name, "DRAINED");

    for (const auto& [key, value] : preload_items) {
        wait_for_segments(key, target_segment, source_segment);
        assert_key_data(key, value);
    }
    for (const auto& [key, value] : redirected_items) {
        wait_for_segments(key, target_segment, source_segment);
        assert_key_data(key, value);
    }

    client1_.reset();
    if (client1_segment_ptr_ != nullptr) {
        free(client1_segment_ptr_);
        client1_segment_ptr_ = nullptr;
    }
    std::this_thread::sleep_for(std::chrono::seconds(1));

    for (const auto& [key, value] : preload_items) {
        wait_for_segments(key, target_segment, source_segment);
        assert_key_data(key, value);
    }
    for (const auto& [key, value] : redirected_items) {
        wait_for_segments(key, target_segment, source_segment);
        assert_key_data(key, value);
    }
}

TEST_F(TaskExecutorIntegrationTest, ReplicaCopyToMultipleTargets) {
    ASSERT_NO_FATAL_FAILURE(MountThirdClientSegment());

    std::string test_key =
        "test_multi_target_copy_key_" +
        std::to_string(
            std::chrono::steady_clock::now().time_since_epoch().count());
    std::string test_data =
        "This is test data for multiple target copy operation.";

    std::vector<Slice> slices;
    slices.emplace_back(test_data.data(), test_data.size());
    ReplicateConfig config;
    config.replica_num = 1;
    config.preferred_segment = kClient1Endpoint;

    auto put_result = client1_->Put(test_key, slices, config);
    ASSERT_TRUE(put_result.has_value())
        << "Failed to put data: " << toString(put_result.error());

    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    ExpectReplicaEndpoints(test_key, {kClient1Endpoint});

    const std::vector<std::string> target_segments = {kClient2Endpoint,
                                                      kClient3Endpoint};
    auto copy_result =
        master_client_->CreateCopyTask(test_key, target_segments);
    ASSERT_TRUE(copy_result.has_value())
        << "Failed to create copy task: " << toString(copy_result.error());

    UUID task_id = copy_result.value();

    bool task_completed =
        WaitForTaskCompletion(task_id, std::chrono::seconds(30));
    ASSERT_TRUE(task_completed) << "Task did not complete within timeout";

    ExpectReplicaEndpoints(
        test_key, {kClient1Endpoint, kClient2Endpoint, kClient3Endpoint});

    std::vector<uint8_t> read_buffer(test_data.size());
    std::vector<Slice> read_slices;
    read_slices.emplace_back(read_buffer.data(), read_buffer.size());
    auto get_result = client1_->Get(test_key, read_slices);
    ASSERT_TRUE(get_result.has_value())
        << "Failed to get data after multi-target copy: "
        << toString(get_result.error());

    std::string read_data(reinterpret_cast<const char*>(read_buffer.data()),
                          test_data.size());
    EXPECT_EQ(read_data, test_data);
}

// Test multiple copy tasks
TEST_F(TaskExecutorIntegrationTest, MultipleCopyTasks) {
    ASSERT_NO_FATAL_FAILURE(MountThirdClientSegment());

    const int num_keys = 3;
    std::vector<std::string> keys;
    std::vector<std::string> test_data_list;
    std::vector<UUID> task_ids;

    for (int i = 0; i < num_keys; ++i) {
        std::string key =
            "test_multi_copy_key_" + std::to_string(i) + "_" +
            std::to_string(
                std::chrono::steady_clock::now().time_since_epoch().count());
        std::string data = "Test data for key " + std::to_string(i);
        keys.push_back(key);
        test_data_list.push_back(data);

        std::vector<Slice> slices;
        slices.emplace_back(data.data(), data.size());
        ReplicateConfig config;
        config.replica_num = 1;
        config.preferred_segment = kClient1Endpoint;

        auto put_result = client1_->Put(key, slices, config);
        ASSERT_TRUE(put_result.has_value()) << "Failed to put key " << i;
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(300));

    for (size_t i = 0; i < keys.size(); ++i) {
        ExpectReplicaEndpoints(keys[i], {kClient1Endpoint});
        const std::vector<std::string> targets =
            i == keys.size() - 1
                ? std::vector<std::string>{kClient2Endpoint, kClient3Endpoint}
                : std::vector<std::string>{kClient2Endpoint};
        auto copy_result = master_client_->CreateCopyTask(keys[i], targets);
        ASSERT_TRUE(copy_result.has_value())
            << "Failed to create copy task for " << keys[i];
        task_ids.push_back(copy_result.value());
    }

    for (size_t i = 0; i < task_ids.size(); ++i) {
        bool completed =
            WaitForTaskCompletion(task_ids[i], std::chrono::seconds(30));
        if (!completed) {
            // Query task to get error message
            auto query_task = master_client_->QueryTask(task_ids[i]);
            if (query_task.has_value()) {
                LOG(ERROR) << "Task " << i
                           << " failed: " << query_task.value().message;
            }
        }
        ASSERT_TRUE(completed)
            << "Task " << i << " (key: " << keys[i] << ") did not complete";
    }

    for (size_t i = 0; i < keys.size(); ++i) {
        if (i == keys.size() - 1) {
            ExpectReplicaEndpoints(keys[i], {kClient1Endpoint, kClient2Endpoint,
                                             kClient3Endpoint});
        } else {
            ExpectReplicaEndpoints(keys[i],
                                   {kClient1Endpoint, kClient2Endpoint});
        }

        std::vector<uint8_t> read_buffer(test_data_list[i].size());
        std::vector<Slice> read_slices;
        read_slices.emplace_back(read_buffer.data(), read_buffer.size());
        auto get_result = client1_->Get(keys[i], read_slices);
        ASSERT_TRUE(get_result.has_value()) << "Failed to get copied key " << i;
        std::string read_data(reinterpret_cast<const char*>(read_buffer.data()),
                              test_data_list[i].size());
        EXPECT_EQ(read_data, test_data_list[i])
            << "Data mismatch for key " << i;
    }
}

// Test multiple move tasks
TEST_F(TaskExecutorIntegrationTest, MultipleMoveTasks) {
    const int num_keys = 3;
    std::vector<std::string> keys;
    std::vector<std::string> test_data_list;
    std::vector<UUID> task_ids;

    // Step 1: Put multiple keys on client1
    for (int i = 0; i < num_keys; ++i) {
        std::string key =
            "test_multi_move_key_" + std::to_string(i) + "_" +
            std::to_string(
                std::chrono::steady_clock::now().time_since_epoch().count());
        std::string data = "Test data for move key " + std::to_string(i);
        keys.push_back(key);
        test_data_list.push_back(data);

        std::vector<Slice> slices;
        slices.emplace_back(data.data(), data.size());
        ReplicateConfig config;
        config.replica_num = 1;
        config.preferred_segment = kClient1Endpoint;

        auto put_result = client1_->Put(key, slices, config);
        ASSERT_TRUE(put_result.has_value()) << "Failed to put key " << i;
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(300));

    for (const auto& key : keys) {
        ExpectReplicaEndpoints(key, {kClient1Endpoint});
        auto move_result = master_client_->CreateMoveTask(key, kClient1Endpoint,
                                                          kClient2Endpoint);
        ASSERT_TRUE(move_result.has_value())
            << "Failed to create move task for " << key;
        task_ids.push_back(move_result.value());
    }

    for (size_t i = 0; i < task_ids.size(); ++i) {
        bool completed =
            WaitForTaskCompletion(task_ids[i], std::chrono::seconds(30));
        if (!completed) {
            auto query_task = master_client_->QueryTask(task_ids[i]);
            if (query_task.has_value()) {
                LOG(ERROR) << "Task " << i
                           << " failed: " << query_task.value().message;
            }
        }
        ASSERT_TRUE(completed)
            << "Task " << i << " (key: " << keys[i] << ") did not complete";
    }

    for (size_t i = 0; i < keys.size(); ++i) {
        ExpectReplicaEndpoints(keys[i], {kClient2Endpoint});

        std::vector<uint8_t> read_buffer(test_data_list[i].size());
        std::vector<Slice> read_slices;
        read_slices.emplace_back(read_buffer.data(), read_buffer.size());

        auto get_result = client1_->Get(keys[i], read_slices);
        ASSERT_TRUE(get_result.has_value()) << "Failed to get moved key " << i;

        std::string read_data(reinterpret_cast<const char*>(read_buffer.data()),
                              test_data_list[i].size());
        ASSERT_EQ(read_data, test_data_list[i])
            << "Data mismatch for moved key " << i;
    }
}

// Test concurrent copy and move operations
TEST_F(TaskExecutorIntegrationTest, ConcurrentCopyAndMoveOperations) {
    ASSERT_NO_FATAL_FAILURE(MountThirdClientSegment());

    const int num_copy_keys = 2;
    const int num_move_keys = 2;
    std::vector<std::string> copy_keys, move_keys;
    std::vector<UUID> copy_task_ids, move_task_ids;

    for (int i = 0; i < num_copy_keys; ++i) {
        std::string key =
            "test_concurrent_copy_key_" + std::to_string(i) + "_" +
            std::to_string(
                std::chrono::steady_clock::now().time_since_epoch().count());
        std::string data = "Copy data " + std::to_string(i);
        copy_keys.push_back(key);

        std::vector<Slice> slices;
        slices.emplace_back(data.data(), data.size());
        ReplicateConfig config;
        config.replica_num = 1;
        config.preferred_segment = kClient1Endpoint;

        auto put_result = client1_->Put(key, slices, config);
        ASSERT_TRUE(put_result.has_value()) << "Failed to put copy key " << i;
    }

    for (int i = 0; i < num_move_keys; ++i) {
        std::string key =
            "test_concurrent_move_key_" + std::to_string(i) + "_" +
            std::to_string(
                std::chrono::steady_clock::now().time_since_epoch().count());
        std::string data = "Move data " + std::to_string(i);
        move_keys.push_back(key);

        std::vector<Slice> slices;
        slices.emplace_back(data.data(), data.size());
        ReplicateConfig config;
        config.replica_num = 1;
        config.preferred_segment = kClient1Endpoint;

        auto put_result = client1_->Put(key, slices, config);
        ASSERT_TRUE(put_result.has_value()) << "Failed to put move key " << i;
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(300));

    for (size_t i = 0; i < copy_keys.size(); ++i) {
        ExpectReplicaEndpoints(copy_keys[i], {kClient1Endpoint});
        const std::vector<std::string> targets =
            i == 0
                ? std::vector<std::string>{kClient2Endpoint, kClient3Endpoint}
                : std::vector<std::string>{kClient2Endpoint};
        auto copy_result =
            master_client_->CreateCopyTask(copy_keys[i], targets);
        ASSERT_TRUE(copy_result.has_value())
            << "Failed to create copy task for " << copy_keys[i];
        copy_task_ids.push_back(copy_result.value());
    }

    for (const auto& key : move_keys) {
        ExpectReplicaEndpoints(key, {kClient1Endpoint});
        auto move_result = master_client_->CreateMoveTask(key, kClient1Endpoint,
                                                          kClient2Endpoint);
        ASSERT_TRUE(move_result.has_value())
            << "Failed to create move task for " << key;
        move_task_ids.push_back(move_result.value());
    }

    for (size_t i = 0; i < copy_task_ids.size(); ++i) {
        bool completed =
            WaitForTaskCompletion(copy_task_ids[i], std::chrono::seconds(30));
        ASSERT_TRUE(completed) << "Copy task " << i << " (key: " << copy_keys[i]
                               << ") did not complete";
    }

    for (size_t i = 0; i < move_task_ids.size(); ++i) {
        bool completed =
            WaitForTaskCompletion(move_task_ids[i], std::chrono::seconds(30));
        ASSERT_TRUE(completed) << "Move task " << i << " (key: " << move_keys[i]
                               << ") did not complete";
    }

    for (size_t i = 0; i < copy_keys.size(); ++i) {
        if (i == 0) {
            ExpectReplicaEndpoints(
                copy_keys[i],
                {kClient1Endpoint, kClient2Endpoint, kClient3Endpoint});
        } else {
            ExpectReplicaEndpoints(copy_keys[i],
                                   {kClient1Endpoint, kClient2Endpoint});
        }
    }

    for (const auto& key : move_keys) {
        ExpectReplicaEndpoints(key, {kClient2Endpoint});
    }
}

}  // namespace testing
}  // namespace mooncake
