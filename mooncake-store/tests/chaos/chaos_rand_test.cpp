#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <unistd.h>

#include <memory>
#include <string>

#include "chaos_test_helper.h"
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
DEFINE_string(client_path, "./mooncake-store/tests/chaos/chaos_client",
              "Path to the client executable");
DEFINE_string(out_dir, "./output", "Directory for log files");
DEFINE_int32(rand_seed, 0, "Random seed, 0 means use current time as seed");

constexpr int master_port_base = 50051;
constexpr int client_port_base = 12888;

namespace mooncake {
namespace testing {

class ChaosRandTest : public ::testing::Test {
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

        master_view_helper_ = std::make_shared<MasterViewHelper>();
        EXPECT_EQ(master_view_helper_->ConnectToEtcd(FLAGS_etcd_endpoints),
                  ErrorCode::OK)
            << "Failed to connect to etcd";
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

    static void GetLeaderIndex(int& leader_index) {
        // Find out the leader
        std::string master_address;
        ViewVersionId version;
        ASSERT_EQ(master_view_helper_->GetMasterView(master_address, version),
                  ErrorCode::OK);

        // Validate master address format: should be ip:port
        size_t colon_pos = master_address.find(':');
        ASSERT_NE(colon_pos, std::string::npos)
            << "Master address should contain port part separated by ':', got: "
            << master_address;

        // Extract port part and verify it's an integer
        std::string port_str = master_address.substr(colon_pos + 1);
        ASSERT_FALSE(port_str.empty())
            << "Port part should not be empty in master address: "
            << master_address;

        // Verify port is a valid integer
        int port;
        try {
            port = std::stoi(port_str);
        } catch (const std::exception& e) {
            FAIL() << "Port part should be a valid integer, got: '" << port_str
                   << "' in master address: " << master_address;
        }
        leader_index = port - master_port_base;
    }

    static void WaitMasterViewChange() {
        sleep(ETCD_MASTER_VIEW_LEASE_TTL * 3);
    }

    static void WaitClientCrashDetection() {
        sleep(DEFAULT_CLIENT_LIVE_TTL_SEC + 5);
    }

    static std::shared_ptr<MasterViewHelper> master_view_helper_;
};

std::shared_ptr<MasterViewHelper> ChaosRandTest::master_view_helper_ = nullptr;

// Verify the system failover ability by randomly killing masters.
// This test repeats the following steps:
// 1. Randomly kill some masters.
// 2. Before the system becomes stable, verify there is no crash or unexpected
// errors.
// 3. After the system becomes stable, verify the system can properly serve
// requests.
// Small value means the kv size is far less than the segment size.
TEST_F(ChaosRandTest, RandomMasterCrashSmallValue) {
    const int master_num = 3;
    const int client_num = 5;
    const int run_sec = 3600;         // 1 hour
    const int master_kill_prob = 50;  // percentage
    const int master_start_prob = 50;  // percentage
    const int segment_size = 1024 * 1024 * 16;
    const int kv_range = 100;

    unsigned int seed;
    if (FLAGS_rand_seed == 0) {
        seed = time(NULL);
    } else {
        seed = FLAGS_rand_seed;
    }
    LOG(INFO) << "Random seed: " << seed;
    srand(seed);

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

        // Randomly kill some masters
        for (int i = 0; i < master_num; ++i) {
            if (rand() % 100 < master_kill_prob) {
                masters[i]->kill();
            }
        }

        // The system is unstable now, so put failure is allowed. However,
        // since the kv size is far less than the segment size, there should not
        // be any background eviction, which means we can safely assume if the
        // put succeeds, the get should also succeed.
        for (int key_i = 0; key_i < kv_range; ++key_i) {
            std::string key = "key_" + std::to_string(key_i);
            std::string value = "value_" + std::to_string(key_i);

            ErrorCode err = clients[rand() % client_num]->Put(key, value);
            if (err == ErrorCode::OK || err == ErrorCode::OBJECT_ALREADY_EXISTS) {
                std::string get_value;
                ASSERT_EQ(clients[rand() % client_num]->Get(key, get_value),
                          ErrorCode::OK);
                ASSERT_EQ(get_value, value);
            }
        }

        // Wait for the system to become stable
        WaitMasterViewChange();

        // After the system becomes stable, verify the system can properly serve
        // requests if there are still some master alive.
        bool has_master_alive = false;
        for (int i = 0; i < master_num; ++i) {
            if (masters[i]->is_running()) {
                has_master_alive = true;
                break;
            }
        }

        if (has_master_alive) {
            // The system is stable now, so get and put should always succeed.
            for (int key_i = 0; key_i < kv_range; ++key_i) {
                std::string key = "key_" + std::to_string(key_i);
                std::string value = "value_" + std::to_string(key_i);
                ErrorCode err = clients[rand() % client_num]->Put(key, value);
                ASSERT_TRUE(err == ErrorCode::OK || err == ErrorCode::OBJECT_ALREADY_EXISTS);
                std::string get_value;
                ASSERT_EQ(clients[rand() % client_num]->Get(key, get_value),
                          ErrorCode::OK);
                ASSERT_EQ(get_value, value);
            }
        }

        // Randomly start some masters
        for (int i = 0; i < master_num; ++i) {
            if (!masters[i]->is_running() && rand() % 100 < master_start_prob) {
                ASSERT_TRUE(masters[i]->start());
            }
        }
    }
}

// Large value means the kv size is close to the segment size, so the eviction
// is more likely to happen and it is more likely to read wrong data if there
// are bugs.
TEST_F(ChaosRandTest, RandomMasterCrashLargeValue) {
    const int master_num = 3;
    const int client_num = 5;
    const int run_sec = 3600;         // 1 hour
    const int master_kill_prob = 50;  // percentage
    const int master_start_prob = 50;  // percentage
    const int segment_size = 1024 * 1024 * 16;
    const int kv_range = 100;
    const size_t value_size = 1024 * 1024;
    static_assert(value_size * kv_range >= segment_size * client_num,
                  "The total value size is too small");

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

        // Randomly kill some masters
        for (int i = 0; i < master_num; ++i) {
            if (rand() % 100 < master_kill_prob) {
                masters[i]->kill();
            }
        }

        // Verify the data integrity when system is unstable.
        for (int key_i = 0; key_i < kv_range; ++key_i) {
            std::string key = gen_key(key_i);
            std::string value = gen_value(key_i);

            if (rand() % 2 == 0) {
                clients[rand() % client_num]->Put(key, value);
            }
            std::string get_value;
            if (clients[rand() % client_num]->Get(key, get_value) == ErrorCode::OK) {
                ASSERT_EQ(get_value, value);
            }
        }

        // Wait for the system to become stable
        WaitMasterViewChange();

        // After the system becomes stable, verify the system can properly serve
        // requests if there are still some master alive.
        bool has_master_alive = false;
        for (int i = 0; i < master_num; ++i) {
            if (masters[i]->is_running()) {
                has_master_alive = true;
                break;
            }
        }

        if (has_master_alive) {
            for (int key_i = 0; key_i < kv_range; ++key_i) {
                std::string key = gen_key(key_i);
                std::string value = gen_value(key_i);

                if (rand() % 2 == 0) {
                    clients[rand() % client_num]->Put(key, value);
                }
                std::string get_value;
                if (clients[rand() % client_num]->Get(key, get_value) == ErrorCode::OK) {
                    // First compare the first few bytes to avoid very large output message
                    ASSERT_EQ(get_value.size(), value.size());
                    ASSERT_EQ(get_value.substr(0, 20), value.substr(0, 20));
                }
            }
        }

        // Randomly start some masters
        for (int i = 0; i < master_num; ++i) {
            if (!masters[i]->is_running() && rand() % 100 < master_start_prob) {
                ASSERT_TRUE(masters[i]->start());
            }
        }
    }
}

}  // namespace testing
}  // namespace mooncake