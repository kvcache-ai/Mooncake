#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <unistd.h>

#include <memory>
#include <string>

#include "client_wrapper.h"
#include "e2e_utils.h"
#include "process_handler.h"
#include "types.h"
#include "utils.h"

USE_engine_flags;
FLAG_etcd_endpoints;
FLAG_master_path;
FLAG_out_dir;
FLAG_rand_seed;
DEFINE_int32(run_sec, 3600, "The number of seconds to run for each test");

constexpr int master_port_base = 50051;
constexpr int client_port_base = 12888;

namespace mooncake {
namespace testing {

class E2ERandTest : public ::testing::Test {
   protected:
    static void SetUpTestSuite() {
        // Initialize glog
        google::InitGoogleLogging("ClientIntegrationTest");

        FLAGS_logtostderr = 1;

        LOG(INFO) << "Protocol: " << FLAGS_protocol
                  << ", Device name: " << FLAGS_device_name
                  << ", Metadata URL: " << FLAGS_engine_meta_url;

        LOG(INFO) << "Random seed: " << FLAGS_rand_seed;
        srand(FLAGS_rand_seed);
    }

    static void TearDownTestSuite() { google::ShutdownGoogleLogging(); }

    static std::shared_ptr<ClientTestWrapper> CreateClientWrapper(
        const std::string& host_name) {
        auto client_opt = ClientTestWrapper::CreateClientWrapper(
            host_name,              // Local hostname
            FLAGS_engine_meta_url,  // Metadata connection string
            FLAGS_protocol, FLAGS_device_name,
            "etcd://" + FLAGS_etcd_endpoints);
        EXPECT_TRUE(client_opt.has_value()) << "Failed to create client";
        if (!client_opt.has_value()) {
            return nullptr;
        }
        return *client_opt;
    }

    static void WaitMasterViewChange() {
        sleep(ETCD_MASTER_VIEW_LEASE_TTL * 3);
    }
};

// Test the sequential delete, put and get operation from different clients.
TEST_F(E2ERandTest, RandomSequentialDeletePutGet) {
    const int master_num = 1;
    const int client_num = 2;
    const int run_sec = FLAGS_run_sec;
    const int segment_size = 1024 * 1024 * 16 * 16;
    // Large value size to trigger bugs more easily.
    const size_t value_size = 1024 * 1024 * 15;
    // Large kv range to the segment is easy to be full.
    const int kv_range = segment_size * client_num / value_size * 2;

    auto gen_key = [](int key_i) { return "key_" + std::to_string(key_i); };

    auto gen_value = [&](int key_i) {
        std::string value = "value_";

        // Create a deterministic pattern based on key_i
        std::string pattern = std::to_string(key_i) + "_";

        // Fill the value string with the pattern repeated to reach value_size
        while (value.size() < value_size) {
            value += pattern;
        }

        // Trim to exactly value_size
        value.resize(value_size);

        return value;
    };

    // Store the kv pairs to make generate operations faster.
    std::unordered_map<std::string, std::string> kv_map;
    for (int i = 0; i < kv_range; ++i) {
        kv_map[gen_key(i)] = gen_value(i);
    }

    // Start masters
    std::vector<std::unique_ptr<mooncake::testing::MasterProcessHandler>>
        masters;
    for (int i = 0; i < master_num; ++i) {
        masters.emplace_back(
            std::make_unique<mooncake::testing::MasterProcessHandler>(
                FLAGS_master_path, FLAGS_etcd_endpoints, master_port_base + i,
                i, FLAGS_out_dir));
        ASSERT_TRUE(masters.back()->start());
    }

    // Wait for the leader to be elected
    WaitMasterViewChange();

    // Create clients
    std::vector<std::shared_ptr<ClientTestWrapper>> clients;
    std::vector<std::vector<void*>> client_segments;
    for (int i = 0; i < client_num; ++i) {
        clients.emplace_back(CreateClientWrapper(
            "0.0.0.0:" + std::to_string(client_port_base + i)));
        ASSERT_TRUE(clients.back() != nullptr);
        client_segments.emplace_back();
        // Mount a segment
        void* buffer;
        ASSERT_EQ(clients.back()->Mount(segment_size, buffer), ErrorCode::OK);
        client_segments.back().emplace_back(buffer);
    }

    auto start_time = std::chrono::steady_clock::now();
    while (true) {
        // Check if we've exceeded the run time
        auto current_time = std::chrono::steady_clock::now();
        auto elapsed_seconds = std::chrono::duration_cast<std::chrono::seconds>(
                                   current_time - start_time)
                                   .count();
        if (elapsed_seconds >= run_sec) {
            break;
        }

        // For randomly selected kv pairs, delete, put and get them.
        for (int i = 0; i < kv_range; ++i) {
            if (rand() % 100 < 50) {
                continue;
            }
            auto key = gen_key(i);
            auto& value = kv_map[key];
            clients[rand() % client_num]->Delete(key);
            clients[rand() % client_num]->Put(key, value);
            std::string get_value;
            // The get operation may fail due to object eviction. But once it
            // succeeds, the value should be correct.
            if (clients[rand() % client_num]->Get(key, get_value) ==
                    ErrorCode::OK &&
                get_value != value) {
                ASSERT_EQ(get_value.size(), value.size());
                // Only display the first 10 characters from the different
                // position to avoid huge output
                for (size_t i = 0; i < get_value.size(); i++) {
                    if (get_value[i] != value[i]) {
                        std::string val_substr = value.substr(
                            i, std::min(
                                   10, static_cast<int>(get_value.size() - i)));
                        std::string get_substr = get_value.substr(
                            i, std::min(
                                   10, static_cast<int>(get_value.size() - i)));
                        LOG(ERROR)
                            << "Get value mismatch at position " << i << ": "
                            << get_substr << " != " << val_substr;
                        ASSERT_EQ(get_substr, val_substr);
                    }
                }
            }
        }
    }
}

}  // namespace testing
}  // namespace mooncake

int main(int argc, char** argv) {
    // Initialize Google Test
    ::testing::InitGoogleTest(&argc, argv);

    // Initialize gflags first to parse command line arguments
    gflags::ParseCommandLineFlags(&argc, &argv, false);

    return RUN_ALL_TESTS();
}