#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <thread>
#include <chrono>
#include <vector>

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
            "127.0.0.1:18001",
            metadata_url_.empty() ? "P2PHANDSHAKE" : metadata_url_,
            FLAGS_protocol,
            FLAGS_device_name.empty() ? std::nullopt
                                      : std::make_optional(FLAGS_device_name),
            master_address_);
        ASSERT_TRUE(client1_opt.has_value());
        client1_ = client1_opt.value();

        // Create client 2
        auto client2_opt = Client::Create(
            "127.0.0.1:18002",
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

        // Client IDs will be extracted from task assignments when tasks are
        // created For now, initialize with placeholder values
        client1_id_ = generate_uuid();
        client2_id_ = generate_uuid();
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
    }

    static void CleanupClients() {
        // Clients will be cleaned up by shared_ptr
    }

    // Helper to extract client_id from task assignment
    // When a task is created, we can query it to see which client it's assigned
    // to
    UUID GetClientIdFromTask(const UUID& task_id) {
        auto query_result = master_client_->QueryTask(task_id);
        if (query_result.has_value()) {
            return query_result.value().assigned_client;
        }
        return generate_uuid();  // Fallback
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

    // Get segment name from a query result
    // Note: segment name is typically the client's hostname (e.g.,
    // "127.0.0.1:18001")
    std::string GetSegmentNameFromQuery(const QueryResult& query_result) {
        if (query_result.replicas.empty()) {
            return "";
        }

        const auto& replica = query_result.replicas[0];
        if (replica.is_memory_replica()) {
            // Try to get segment name from the replica descriptor
            // For memory replicas, the segment name is stored in the buffer
            // descriptor but transport_endpoint_ might not match segment name
            // We need to extract it from the replica's segment information
            const auto& mem_desc = replica.get_memory_descriptor();
            // The segment name is typically embedded in the buffer descriptor
            // For now, use transport_endpoint_ as a fallback, but we should
            // verify it matches the actual segment name used during mount
            return mem_desc.buffer_descriptor.transport_endpoint_;
        }
        return "";
    }

   protected:
    static InProcMaster master_;
    static std::string master_address_;
    static std::string metadata_url_;

    std::shared_ptr<Client> client1_;
    std::shared_ptr<Client> client2_;
    std::unique_ptr<MasterClient> master_client_;

    UUID client1_id_;
    UUID client2_id_;

    void* client1_segment_ptr_ = nullptr;
    void* client2_segment_ptr_ = nullptr;
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
    config.replica_num = 1;  // Start with 1 replica on client1

    auto put_result = client1_->Put(test_key, slices, config);
    ASSERT_TRUE(put_result.has_value())
        << "Failed to put data: " << toString(put_result.error());

    // Wait a bit for put to complete
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    // Step 2: Verify that client1's data was put successfully and get actual
    // segment name
    auto client1_query_result = client1_->Query(test_key);
    ASSERT_TRUE(client1_query_result.has_value())
        << "Failed to query key for verification";
    ASSERT_FALSE(client1_query_result.value().replicas.empty())
        << "No replicas found after Put";

    // Extract actual segment name from replica
    std::string source_segment;
    const auto& replica = client1_query_result.value().replicas[0];
    ASSERT_TRUE(replica.is_memory_replica()) << "Expected memory replica";
    source_segment =
        replica.get_memory_descriptor().buffer_descriptor.transport_endpoint_;
    ASSERT_FALSE(source_segment.empty())
        << "Failed to extract source segment name";

    // Step 3: Determine target segment dynamically (must be different from
    // source) Choose the other client's segment as target to ensure source !=
    // target
    std::string target_segment;
    if (source_segment == "127.0.0.1:18001") {
        target_segment = "127.0.0.1:18002";
    } else if (source_segment == "127.0.0.1:18002") {
        target_segment = "127.0.0.1:18001";
    } else {
        // If source is neither, default to client2's segment
        target_segment = "127.0.0.1:18002";
    }

    // Verify source and target are different - this test only covers
    // inconsistent cases
    ASSERT_NE(source_segment, target_segment)
        << "Source and target segments must be different for this test. "
        << "Source: " << source_segment << ", Target: " << target_segment;

    // Step 4: Create copy task via master
    std::vector<std::string> targets = {target_segment};
    auto copy_result = master_client_->Copy(test_key, targets);
    ASSERT_TRUE(copy_result.has_value())
        << "Failed to create copy task: " << toString(copy_result.error());

    UUID task_id = copy_result.value();

    // Step 5: Get the actual client_id from the task assignment
    client1_id_ = GetClientIdFromTask(task_id);

    // Step 6: Wait for task to be fetched and executed by the assigned client
    bool task_completed =
        WaitForTaskCompletion(task_id, std::chrono::seconds(30));
    ASSERT_TRUE(task_completed) << "Task did not complete within timeout";

    // Step 7: Verify copy was successful by querying from client2
    auto client2_query_result = client2_->Query(test_key);
    ASSERT_TRUE(client2_query_result.has_value())
        << "Failed to query copied key";
    ASSERT_FALSE(client2_query_result.value().replicas.empty())
        << "No replicas found for copied key";

    // Step 8: Verify data integrity by getting data from client2
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
    config.replica_num = 1;  // Start with 1 replica on client1

    auto put_result = client1_->Put(test_key, slices, config);
    ASSERT_TRUE(put_result.has_value())
        << "Failed to put data: " << toString(put_result.error());

    // Wait a bit for put to complete
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    // Step 2: Verify that client1's data was put successfully and get actual
    // segment name
    auto query_result = client1_->Query(test_key);
    ASSERT_TRUE(query_result.has_value())
        << "Failed to query key for verification";
    ASSERT_FALSE(query_result.value().replicas.empty())
        << "No replicas found after Put";

    // Extract actual segment name from replica
    std::string source_segment;
    const auto& replica = query_result.value().replicas[0];
    ASSERT_TRUE(replica.is_memory_replica()) << "Expected memory replica";
    source_segment =
        replica.get_memory_descriptor().buffer_descriptor.transport_endpoint_;
    ASSERT_FALSE(source_segment.empty())
        << "Failed to extract source segment name";

    // Step 3: Determine target segment dynamically (must be different from
    // source) Choose the other client's segment as target to ensure source !=
    // target
    std::string target_segment;
    if (source_segment == "127.0.0.1:18001") {
        target_segment = "127.0.0.1:18002";
    } else if (source_segment == "127.0.0.1:18002") {
        target_segment = "127.0.0.1:18001";
    } else {
        // If source is neither, default to client2's segment
        target_segment = "127.0.0.1:18002";
    }

    // Verify source and target are different - this test only covers
    // inconsistent cases
    ASSERT_NE(source_segment, target_segment)
        << "Source and target segments must be different for this test. "
        << "Source: " << source_segment << ", Target: " << target_segment;

    // Step 4: Create move task via master
    auto move_result =
        master_client_->Move(test_key, source_segment, target_segment);
    ASSERT_TRUE(move_result.has_value())
        << "Failed to create move task: " << toString(move_result.error());

    UUID task_id = move_result.value();

    // Step 5: Get the actual client_id from the task assignment
    client1_id_ = GetClientIdFromTask(task_id);

    // Step 6: Wait for task to be fetched and executed by the assigned client
    bool task_completed =
        WaitForTaskCompletion(task_id, std::chrono::seconds(30));
    ASSERT_TRUE(task_completed) << "Task did not complete within timeout";

    // Step 7: Verify move was successful
    // The replica should now be on client2, not client1
    auto query_result_client2 = client2_->Query(test_key);
    ASSERT_TRUE(query_result_client2.has_value())
        << "Failed to query moved key from client2";
    ASSERT_FALSE(query_result_client2.value().replicas.empty())
        << "No replicas found on client2 after move";

    // Step 8: Verify data integrity
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

// Test copy to multiple target segments
TEST_F(TaskExecutorIntegrationTest, ReplicaCopyToMultipleTargets) {
    // Step 1: Put data on client1
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

    auto put_result = client1_->Put(test_key, slices, config);
    ASSERT_TRUE(put_result.has_value())
        << "Failed to put data: " << toString(put_result.error());

    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    // Step 2: Get actual source segment
    auto query_result = client1_->Query(test_key);
    ASSERT_TRUE(query_result.has_value())
        << "Failed to query key for verification";
    ASSERT_FALSE(query_result.value().replicas.empty())
        << "No replicas found after Put";

    std::string source_segment;
    const auto& replica = query_result.value().replicas[0];
    ASSERT_TRUE(replica.is_memory_replica()) << "Expected memory replica";
    source_segment =
        replica.get_memory_descriptor().buffer_descriptor.transport_endpoint_;
    ASSERT_FALSE(source_segment.empty())
        << "Failed to extract source segment name";

    // Step 3: Determine target segments (both client1 and client2, excluding
    // source)
    std::vector<std::string> target_segments;
    if (source_segment == "127.0.0.1:18001") {
        target_segments = {"127.0.0.1:18002"};  // Copy to client2 only
    } else if (source_segment == "127.0.0.1:18002") {
        target_segments = {"127.0.0.1:18001"};  // Copy to client1 only
    } else {
        // If source is neither, use both segments
        target_segments = {"127.0.0.1:18001", "127.0.0.1:18002"};
    }

    // Ensure source is not in targets
    for (const auto& target : target_segments) {
        ASSERT_NE(source_segment, target)
            << "Source segment must not be in target segments. "
            << "Source: " << source_segment << ", Target: " << target;
    }

    // Step 4: Create copy task with multiple targets
    auto copy_result = master_client_->Copy(test_key, target_segments);
    ASSERT_TRUE(copy_result.has_value())
        << "Failed to create copy task: " << toString(copy_result.error());

    UUID task_id = copy_result.value();

    // Step 5: Wait for task to complete
    bool task_completed =
        WaitForTaskCompletion(task_id, std::chrono::seconds(30));
    ASSERT_TRUE(task_completed) << "Task did not complete within timeout";

    // Step 6: Verify copy was successful on all target segments
    for (const auto& target_segment : target_segments) {
        std::shared_ptr<Client> target_client;
        if (target_segment == "127.0.0.1:18001") {
            target_client = client1_;
        } else {
            target_client = client2_;
        }

        auto target_query_result = target_client->Query(test_key);
        ASSERT_TRUE(target_query_result.has_value())
            << "Failed to query copied key from target segment: "
            << target_segment;
        ASSERT_FALSE(target_query_result.value().replicas.empty())
            << "No replicas found on target segment: " << target_segment;

        // Verify data integrity
        std::vector<uint8_t> read_buffer(test_data.size());
        std::vector<Slice> read_slices;
        read_slices.emplace_back(read_buffer.data(), read_buffer.size());

        auto get_result = target_client->Get(test_key, read_slices);
        ASSERT_TRUE(get_result.has_value())
            << "Failed to get data from target segment: " << target_segment;

        std::string read_data(reinterpret_cast<const char*>(read_buffer.data()),
                              test_data.size());
        ASSERT_EQ(read_data, test_data)
            << "Data mismatch on target segment: " << target_segment;
    }
}

// Test multiple copy tasks
TEST_F(TaskExecutorIntegrationTest, MultipleCopyTasks) {
    const int num_keys = 3;
    std::vector<std::string> keys;
    std::vector<std::string> test_data_list;
    std::vector<UUID> task_ids;

    // Step 1: Put multiple keys on client1
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

        auto put_result = client1_->Put(key, slices, config);
        ASSERT_TRUE(put_result.has_value()) << "Failed to put key " << i;
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(300));

    // Step 2: For each key, determine its source segment and create copy task
    // with appropriate target Each key may be on a different segment, so we
    // need to check each one individually
    for (const auto& key : keys) {
        // Query to get actual source segment for this key
        auto query_result = client1_->Query(key);
        ASSERT_TRUE(query_result.has_value()) << "Failed to query key: " << key;
        ASSERT_FALSE(query_result.value().replicas.empty())
            << "No replicas found for key: " << key;

        // Extract actual source segment name from replica
        std::string source_segment;
        const auto& replica = query_result.value().replicas[0];
        ASSERT_TRUE(replica.is_memory_replica())
            << "Expected memory replica for key: " << key;
        source_segment = replica.get_memory_descriptor()
                             .buffer_descriptor.transport_endpoint_;
        ASSERT_FALSE(source_segment.empty())
            << "Failed to extract source segment name for key: " << key;

        // Determine target segment dynamically (must be different from source)
        // Choose the other client's segment as target to ensure source !=
        // target
        std::string target_segment;
        if (source_segment == "127.0.0.1:18001") {
            target_segment = "127.0.0.1:18002";
        } else if (source_segment == "127.0.0.1:18002") {
            target_segment = "127.0.0.1:18001";
        } else {
            // If source is neither, default to client2's segment
            target_segment = "127.0.0.1:18002";
        }

        // Verify source and target are different - this test only covers
        // inconsistent cases
        ASSERT_NE(source_segment, target_segment)
            << "Source and target segments must be different for key: " << key
            << ". "
            << "Source: " << source_segment << ", Target: " << target_segment;

        // Step 3: Create copy task for this key
        // For the last key, use multiple target segments to test multi-target
        // copy
        std::vector<std::string> targets;
        size_t key_index =
            task_ids.size();  // Current index before adding this task
        if (key_index == keys.size() - 1 && keys.size() >= 2) {
            // Last key: use multiple targets if we have at least 2 clients
            if (source_segment == "127.0.0.1:18001") {
                targets = {
                    "127.0.0.1:18002"};  // Only one other segment available
            } else if (source_segment == "127.0.0.1:18002") {
                targets = {
                    "127.0.0.1:18001"};  // Only one other segment available
            } else {
                targets = {"127.0.0.1:18001", "127.0.0.1:18002"};
            }
        } else {
            // Other keys: use single target
            targets = {target_segment};
        }

        auto copy_result = master_client_->Copy(key, targets);
        ASSERT_TRUE(copy_result.has_value())
            << "Failed to create copy task for " << key;
        task_ids.push_back(copy_result.value());
    }

    // Step 4: Wait for all tasks to complete
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

    // Step 5: Verify all keys are accessible on their target segments
    // For each key, we need to determine which client should have the replica
    for (size_t i = 0; i < keys.size(); ++i) {
        // Determine source segment for this key
        auto query_result = client1_->Query(keys[i]);
        ASSERT_TRUE(query_result.has_value()) << "Failed to query key " << i;
        ASSERT_FALSE(query_result.value().replicas.empty())
            << "No replicas found for key " << i;

        std::string source_segment;
        const auto& replica = query_result.value().replicas[0];
        ASSERT_TRUE(replica.is_memory_replica())
            << "Expected memory replica for key " << i;
        source_segment = replica.get_memory_descriptor()
                             .buffer_descriptor.transport_endpoint_;

        // For the last key with multiple targets, verify on all target segments
        bool is_multi_target = (i == keys.size() - 1 && keys.size() >= 2);

        if (is_multi_target) {
            // Last key: verify on all target segments
            std::vector<std::string> target_segments;
            if (source_segment == "127.0.0.1:18001") {
                target_segments = {"127.0.0.1:18002"};
            } else if (source_segment == "127.0.0.1:18002") {
                target_segments = {"127.0.0.1:18001"};
            } else {
                target_segments = {"127.0.0.1:18001", "127.0.0.1:18002"};
            }

            // Verify on each target segment
            for (const auto& target_seg : target_segments) {
                std::shared_ptr<Client> target_client =
                    (target_seg == "127.0.0.1:18001") ? client1_ : client2_;

                auto target_query = target_client->Query(keys[i]);
                ASSERT_TRUE(target_query.has_value())
                    << "Failed to query key " << i
                    << " from target segment: " << target_seg;
                ASSERT_FALSE(target_query.value().replicas.empty())
                    << "No replicas found for key " << i
                    << " on segment: " << target_seg;

                // Verify data integrity
                std::vector<uint8_t> read_buffer(test_data_list[i].size());
                std::vector<Slice> read_slices;
                read_slices.emplace_back(read_buffer.data(),
                                         read_buffer.size());

                auto get_result = target_client->Get(keys[i], read_slices);
                ASSERT_TRUE(get_result.has_value())
                    << "Failed to get key " << i
                    << " from target segment: " << target_seg;

                std::string read_data(
                    reinterpret_cast<const char*>(read_buffer.data()),
                    test_data_list[i].size());
                ASSERT_EQ(read_data, test_data_list[i])
                    << "Data mismatch for key " << i
                    << " on segment: " << target_seg;
            }
        } else {
            // Other keys: verify on single target segment
            std::shared_ptr<Client> target_client;
            if (source_segment == "127.0.0.1:18001") {
                target_client = client2_;  // Copy to client2
            } else {
                target_client = client1_;  // Copy to client1
            }

            // Verify the copied replica is accessible
            auto query_result_target = target_client->Query(keys[i]);
            ASSERT_TRUE(query_result_target.has_value())
                << "Failed to query copied key " << i << " from target client";
            ASSERT_FALSE(query_result_target.value().replicas.empty())
                << "No replicas found for copied key " << i;

            // Verify data integrity
            std::vector<uint8_t> read_buffer(test_data_list[i].size());
            std::vector<Slice> read_slices;
            read_slices.emplace_back(read_buffer.data(), read_buffer.size());

            auto get_result = target_client->Get(keys[i], read_slices);
            ASSERT_TRUE(get_result.has_value())
                << "Failed to get key " << i << " from target client";

            std::string read_data(
                reinterpret_cast<const char*>(read_buffer.data()),
                test_data_list[i].size());
            ASSERT_EQ(read_data, test_data_list[i])
                << "Data mismatch for key " << i;
        }
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

        auto put_result = client1_->Put(key, slices, config);
        ASSERT_TRUE(put_result.has_value()) << "Failed to put key " << i;
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(300));

    // Step 2: For each key, determine source segment and create move task
    for (const auto& key : keys) {
        // Query to get actual source segment for this key
        auto query_result = client1_->Query(key);
        ASSERT_TRUE(query_result.has_value()) << "Failed to query key: " << key;
        ASSERT_FALSE(query_result.value().replicas.empty())
            << "No replicas found for key: " << key;

        std::string source_segment;
        const auto& replica = query_result.value().replicas[0];
        ASSERT_TRUE(replica.is_memory_replica())
            << "Expected memory replica for key: " << key;
        source_segment = replica.get_memory_descriptor()
                             .buffer_descriptor.transport_endpoint_;
        ASSERT_FALSE(source_segment.empty())
            << "Failed to extract source segment name for key: " << key;

        // Determine target segment (must be different from source)
        std::string target_segment;
        if (source_segment == "127.0.0.1:18001") {
            target_segment = "127.0.0.1:18002";
        } else if (source_segment == "127.0.0.1:18002") {
            target_segment = "127.0.0.1:18001";
        } else {
            target_segment = "127.0.0.1:18002";
        }

        ASSERT_NE(source_segment, target_segment)
            << "Source and target segments must be different for key: " << key
            << ". "
            << "Source: " << source_segment << ", Target: " << target_segment;

        // Step 3: Create move task
        auto move_result =
            master_client_->Move(key, source_segment, target_segment);
        ASSERT_TRUE(move_result.has_value())
            << "Failed to create move task for " << key;
        task_ids.push_back(move_result.value());
    }

    // Step 4: Wait for all tasks to complete
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

    // Step 5: Verify all keys are moved to target segments
    for (size_t i = 0; i < keys.size(); ++i) {
        // Determine target client for this key
        auto query_result = client1_->Query(keys[i]);
        ASSERT_TRUE(query_result.has_value()) << "Failed to query key " << i;

        std::string source_segment;
        if (!query_result.value().replicas.empty()) {
            const auto& replica = query_result.value().replicas[0];
            if (replica.is_memory_replica()) {
                source_segment = replica.get_memory_descriptor()
                                     .buffer_descriptor.transport_endpoint_;
            }
        }

        std::shared_ptr<Client> target_client;
        if (source_segment == "127.0.0.1:18001" || source_segment.empty()) {
            // Check client2 (may have moved from client1 or was originally on
            // client1)
            target_client = client2_;
        } else {
            target_client = client1_;
        }

        // Verify the moved replica is accessible on target client
        auto target_query_result = target_client->Query(keys[i]);
        ASSERT_TRUE(target_query_result.has_value())
            << "Failed to query moved key " << i << " from target client";
        ASSERT_FALSE(target_query_result.value().replicas.empty())
            << "No replicas found for moved key " << i;

        // Verify data integrity
        std::vector<uint8_t> read_buffer(test_data_list[i].size());
        std::vector<Slice> read_slices;
        read_slices.emplace_back(read_buffer.data(), read_buffer.size());

        auto get_result = target_client->Get(keys[i], read_slices);
        ASSERT_TRUE(get_result.has_value())
            << "Failed to get moved key " << i << " from target client";

        std::string read_data(reinterpret_cast<const char*>(read_buffer.data()),
                              test_data_list[i].size());
        ASSERT_EQ(read_data, test_data_list[i])
            << "Data mismatch for moved key " << i;
    }
}

// Test concurrent copy and move operations
TEST_F(TaskExecutorIntegrationTest, ConcurrentCopyAndMoveOperations) {
    const int num_copy_keys = 2;
    const int num_move_keys = 2;
    std::vector<std::string> copy_keys, move_keys;
    std::vector<std::string> copy_data_list, move_data_list;
    std::vector<UUID> copy_task_ids, move_task_ids;

    // Step 1: Put keys for copy operations
    for (int i = 0; i < num_copy_keys; ++i) {
        std::string key =
            "test_concurrent_copy_key_" + std::to_string(i) + "_" +
            std::to_string(
                std::chrono::steady_clock::now().time_since_epoch().count());
        std::string data = "Copy data " + std::to_string(i);
        copy_keys.push_back(key);
        copy_data_list.push_back(data);

        std::vector<Slice> slices;
        slices.emplace_back(data.data(), data.size());
        ReplicateConfig config;
        config.replica_num = 1;

        auto put_result = client1_->Put(key, slices, config);
        ASSERT_TRUE(put_result.has_value()) << "Failed to put copy key " << i;
    }

    // Step 2: Put keys for move operations
    for (int i = 0; i < num_move_keys; ++i) {
        std::string key =
            "test_concurrent_move_key_" + std::to_string(i) + "_" +
            std::to_string(
                std::chrono::steady_clock::now().time_since_epoch().count());
        std::string data = "Move data " + std::to_string(i);
        move_keys.push_back(key);
        move_data_list.push_back(data);

        std::vector<Slice> slices;
        slices.emplace_back(data.data(), data.size());
        ReplicateConfig config;
        config.replica_num = 1;

        auto put_result = client1_->Put(key, slices, config);
        ASSERT_TRUE(put_result.has_value()) << "Failed to put move key " << i;
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(300));

    // Step 3: Create copy tasks concurrently
    // Use multiple targets for the first copy task to test multi-target copy
    for (size_t idx = 0; idx < copy_keys.size(); ++idx) {
        const auto& key = copy_keys[idx];
        auto query_result = client1_->Query(key);
        ASSERT_TRUE(query_result.has_value())
            << "Failed to query copy key: " << key;
        ASSERT_FALSE(query_result.value().replicas.empty());

        std::string source_segment;
        const auto& replica = query_result.value().replicas[0];
        ASSERT_TRUE(replica.is_memory_replica());
        source_segment = replica.get_memory_descriptor()
                             .buffer_descriptor.transport_endpoint_;

        std::vector<std::string> targets;
        if (idx == 0 && copy_keys.size() >= 1) {
            // First copy task: use multiple targets
            if (source_segment == "127.0.0.1:18001") {
                targets = {
                    "127.0.0.1:18002"};  // Only one other segment available
            } else if (source_segment == "127.0.0.1:18002") {
                targets = {
                    "127.0.0.1:18001"};  // Only one other segment available
            } else {
                targets = {"127.0.0.1:18001", "127.0.0.1:18002"};
            }
        } else {
            // Other copy tasks: use single target
            std::string target_segment = (source_segment == "127.0.0.1:18001")
                                             ? "127.0.0.1:18002"
                                             : "127.0.0.1:18001";
            targets = {target_segment};
        }

        auto copy_result = master_client_->Copy(key, targets);
        ASSERT_TRUE(copy_result.has_value())
            << "Failed to create copy task for " << key;
        copy_task_ids.push_back(copy_result.value());
    }

    // Step 4: Create move tasks concurrently
    for (const auto& key : move_keys) {
        auto query_result = client1_->Query(key);
        ASSERT_TRUE(query_result.has_value())
            << "Failed to query move key: " << key;
        ASSERT_FALSE(query_result.value().replicas.empty());

        std::string source_segment;
        const auto& replica = query_result.value().replicas[0];
        ASSERT_TRUE(replica.is_memory_replica());
        source_segment = replica.get_memory_descriptor()
                             .buffer_descriptor.transport_endpoint_;

        std::string target_segment = (source_segment == "127.0.0.1:18001")
                                         ? "127.0.0.1:18002"
                                         : "127.0.0.1:18001";

        auto move_result =
            master_client_->Move(key, source_segment, target_segment);
        ASSERT_TRUE(move_result.has_value())
            << "Failed to create move task for " << key;
        move_task_ids.push_back(move_result.value());
    }

    // Step 5: Wait for all tasks to complete
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

    // Step 6: Verify copy results
    for (size_t i = 0; i < copy_keys.size(); ++i) {
        auto query_result = client1_->Query(copy_keys[i]);
        ASSERT_TRUE(query_result.has_value());

        std::string source_segment;
        if (!query_result.value().replicas.empty()) {
            const auto& replica = query_result.value().replicas[0];
            if (replica.is_memory_replica()) {
                source_segment = replica.get_memory_descriptor()
                                     .buffer_descriptor.transport_endpoint_;
            }
        }

        // First copy task uses multiple targets
        bool is_multi_target = (i == 0 && copy_keys.size() >= 1);

        if (is_multi_target) {
            // Verify on all target segments
            std::vector<std::string> target_segments;
            if (source_segment == "127.0.0.1:18001") {
                target_segments = {"127.0.0.1:18002"};
            } else if (source_segment == "127.0.0.1:18002") {
                target_segments = {"127.0.0.1:18001"};
            } else {
                target_segments = {"127.0.0.1:18001", "127.0.0.1:18002"};
            }

            for (const auto& target_seg : target_segments) {
                std::shared_ptr<Client> target_client =
                    (target_seg == "127.0.0.1:18001") ? client1_ : client2_;

                auto target_query = target_client->Query(copy_keys[i]);
                ASSERT_TRUE(target_query.has_value())
                    << "Copy key " << i
                    << " not found on target segment: " << target_seg;
                ASSERT_FALSE(target_query.value().replicas.empty())
                    << "No replicas for copy key " << i
                    << " on segment: " << target_seg;
            }
        } else {
            // Verify on single target segment
            std::shared_ptr<Client> target_client =
                (source_segment == "127.0.0.1:18001") ? client2_ : client1_;

            auto target_query = target_client->Query(copy_keys[i]);
            ASSERT_TRUE(target_query.has_value())
                << "Copy key " << i << " not found on target";
            ASSERT_FALSE(target_query.value().replicas.empty())
                << "No replicas for copy key " << i;
        }
    }

    // Step 7: Verify move results
    for (size_t i = 0; i < move_keys.size(); ++i) {
        auto query_result = client1_->Query(move_keys[i]);

        std::string source_segment;
        if (query_result.has_value() &&
            !query_result.value().replicas.empty()) {
            const auto& replica = query_result.value().replicas[0];
            if (replica.is_memory_replica()) {
                source_segment = replica.get_memory_descriptor()
                                     .buffer_descriptor.transport_endpoint_;
            }
        }

        std::shared_ptr<Client> target_client =
            (source_segment == "127.0.0.1:18001" || source_segment.empty())
                ? client2_
                : client1_;

        auto target_query = target_client->Query(move_keys[i]);
        ASSERT_TRUE(target_query.has_value())
            << "Move key " << i << " not found on target";
        ASSERT_FALSE(target_query.value().replicas.empty())
            << "No replicas for move key " << i;
    }
}

}  // namespace testing
}  // namespace mooncake
