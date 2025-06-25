#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <unistd.h>

#include <memory>
#include <string>

#include "chaos_test_helper.h"
#include "client.h"
#include "client_test_helper.h"
#include "types.h"
#include "utils.h"

DEFINE_string(protocol, "tcp", "Transfer protocol: rdma|tcp");
DEFINE_string(device_name, "ibp6s0",
              "Device name to use, valid if protocol=rdma");
DEFINE_string(transfer_engine_metadata_url, "http://127.0.0.1:8080/metadata",
              "Metadata connection string for transfer engine");
DEFINE_string(etcd_endpoints, "localhost:2379", "Etcd endpoints");
DEFINE_string(master_path, "./mooncake-store/src/mooncake_master",
              "Path to the master executable");
DEFINE_string(out_dir, "./output", "Directory for log files");
DEFINE_int32(rand_seed, 0, "Random seed, 0 means use current time as seed");

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

        // Override flags from environment variables if present
        if (getenv("PROTOCOL")) FLAGS_protocol = getenv("PROTOCOL");
        if (getenv("DEVICE_NAME")) FLAGS_device_name = getenv("DEVICE_NAME");
        if (getenv("MC_METADATA_SERVER"))
            FLAGS_transfer_engine_metadata_url = getenv("MC_METADATA_SERVER");

        LOG(INFO) << "Protocol: " << FLAGS_protocol
                  << ", Device name: " << FLAGS_device_name
                  << ", Metadata URL: " << FLAGS_transfer_engine_metadata_url;
    }

    static void TearDownTestSuite() { google::ShutdownGoogleLogging(); }

    static std::shared_ptr<ClientTestWrapper> CreateClient(
        const std::string& host_name) {
        auto client_opt = ClientTestWrapper::CreateClient(
            host_name,                           // Local hostname
            FLAGS_transfer_engine_metadata_url,  // Metadata connection string
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
    const int run_sec = 3600;         // 1 hour
    const int segment_size = 1024 * 1024 * 16;
    const size_t value_size = 1024 * 1024;
    const int kv_range = segment_size * client_num / value_size * 2;

    unsigned int seed;
    if (FLAGS_rand_seed == 0) {
        seed = time(NULL);
    } else {
        seed = FLAGS_rand_seed;
    }
    LOG(INFO) << "Random seed: " << seed;
    srand(seed);

    auto gen_key = [](int key_i) {
        return "key_" + std::to_string(key_i);
    };

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

    std::unordered_map<std::string, std::string> kv_map;
    for (int i = 0; i < kv_range; ++i) {
        kv_map[gen_key(i)] = gen_value(i);
        kv_map_[gen_key(i)] = gen_value(i);
    }

    // Start masters
    std::vector<std::unique_ptr<mooncake::testing::MasterHandler>> masters;
    for (int i = 0; i < master_num; ++i) {
        masters.emplace_back(std::make_unique<mooncake::testing::MasterHandler>(
            FLAGS_master_path, master_port_base + i, i, FLAGS_out_dir));
        ASSERT_TRUE(masters.back()->start());
    }

    // Wait for the leader to be elected
    WaitMasterViewChange();

    // Create clients
    std::vector<std::shared_ptr<ClientTestWrapper>> clients;
    std::vector<std::vector<void*>> client_segments;
    for (int i = 0; i < client_num; ++i) {
        clients.emplace_back(
            CreateClient("0.0.0.0:" + std::to_string(client_port_base + i)));
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

        for (int i = 0; i < kv_range; ++i) {
            if (rand() % 100 < 50) {
                continue;
            }
            auto key = gen_key(i);
            auto &value = kv_map[key];
            clients[rand() % client_num]->Delete(key);
            clients[rand() % client_num]->Put(key, value);
            std::string get_value;
            if (clients[rand() % client_num]->Get(key, get_value) == ErrorCode::OK) {
                ASSERT_EQ(get_value, value);
            }
        }
    }
}

}  // namespace testing
}  // namespace mooncake

int main(int argc, char** argv) {
    // Initialize gflags first to parse command line arguments
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    
    // Initialize Google Test
    ::testing::InitGoogleTest(&argc, argv);
    
    return RUN_ALL_TESTS();
}