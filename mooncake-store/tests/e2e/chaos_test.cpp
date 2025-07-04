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
FLAG_client_path;
FLAG_out_dir;

constexpr int master_port_base = 50051;
constexpr int client_port_base = 12888;

namespace mooncake {
namespace testing {

class ChaosTest : public ::testing::Test {
   protected:
    static void SetUpTestSuite() {
        // Initialize glog
        google::InitGoogleLogging("ClientIntegrationTest");

        FLAGS_logtostderr = 1;

        LOG(INFO) << "Protocol: " << FLAGS_protocol
                  << ", Device name: " << FLAGS_device_name
                  << ", Metadata URL: " << FLAGS_engine_meta_url;

        master_view_helper_ = std::make_shared<MasterViewHelper>();
        EXPECT_EQ(master_view_helper_->ConnectToEtcd(FLAGS_etcd_endpoints),
                  ErrorCode::OK)
            << "Failed to connect to etcd";
    }

    static void TearDownTestSuite() { google::ShutdownGoogleLogging(); }

    void SetUp() override {
        // Start masters
        const int master_num = 3;
        for (int i = 0; i < master_num; ++i) {
            masters_.emplace_back(
                std::make_unique<mooncake::testing::MasterProcessHandler>(
                    FLAGS_master_path, FLAGS_etcd_endpoints,
                    master_port_base + i, i, FLAGS_out_dir));
            ASSERT_TRUE(masters_.back()->start());
        }

        // Wait for the leader to be elected
        WaitMasterViewChange();

        // Get leader index
        GetLeaderIndex(leader_index_);
        ASSERT_TRUE(leader_index_ >= 0 && leader_index_ < master_num);
    }

    void TearDown() override {
        // Release masters
        masters_.clear();
    }

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

    // Instance members for master management
    std::vector<std::unique_ptr<mooncake::testing::MasterProcessHandler>>
        masters_;
    int leader_index_;
};

std::shared_ptr<MasterViewHelper> ChaosTest::master_view_helper_ = nullptr;

// Verify the system failover after the leader is killed.
TEST_F(ChaosTest, LeaderKilledFailover) {
    // Create a client
    std::shared_ptr<ClientTestWrapper> client =
        CreateClientWrapper("0.0.0.0:" + std::to_string(client_port_base));
    ASSERT_TRUE(client != nullptr);

    // Mount a segment
    void* buffer;
    ASSERT_EQ(client->Mount(1024 * 1024 * 16, buffer), ErrorCode::OK);

    // Put a key-value pair
    std::string key = "key";
    std::string value = "value";
    ASSERT_EQ(client->Put(key, value), ErrorCode::OK);

    // Kill the leader and wait for the new leader to be elected
    ASSERT_TRUE(masters_[leader_index_]->kill());
    WaitMasterViewChange();

    // Verify the segment is remounted by trying to put a new key-value pair
    std::string key2 = "key2";
    std::string value2 = "value2";
    ASSERT_EQ(client->Put(key2, value2), ErrorCode::OK);

    // Get the value
    std::string get_value2;
    ASSERT_EQ(client->Get(key2, get_value2), ErrorCode::OK);
    ASSERT_EQ(get_value2, value2);
}

// Verify the client won't be affected if non-leader masters are killed.
TEST_F(ChaosTest, BackupMasterKilled) {
    const int master_num = static_cast<int>(masters_.size());

    // Create a client
    std::shared_ptr<ClientTestWrapper> client =
        CreateClientWrapper("0.0.0.0:" + std::to_string(client_port_base));
    ASSERT_TRUE(client != nullptr);

    // Mount a segment
    void* buffer;
    ASSERT_EQ(client->Mount(1024 * 1024 * 16, buffer), ErrorCode::OK);

    // Put a key-value pair
    std::string key = "key";
    std::string value = "value";
    ASSERT_EQ(client->Put(key, value), ErrorCode::OK);

    // Kill the non-leader masters
    for (int i = 0; i < master_num; ++i) {
        if (i != leader_index_) {
            ASSERT_TRUE(masters_[i]->kill());
        }
    }

    // Get the value
    std::string get_value;
    ASSERT_EQ(client->Get(key, get_value), ErrorCode::OK);
    ASSERT_EQ(get_value, value);
}

// Verify the client can automatically remount after all masters other than one
// backed up master are killed.
TEST_F(ChaosTest, AllMastersOtherThanOneBackedUpKilledFailover) {
    const int master_num = static_cast<int>(masters_.size());

    // Create a client
    std::shared_ptr<ClientTestWrapper> client =
        CreateClientWrapper("0.0.0.0:" + std::to_string(client_port_base));
    ASSERT_TRUE(client != nullptr);

    // Mount a segment
    void* buffer;
    ASSERT_EQ(client->Mount(1024 * 1024 * 16, buffer), ErrorCode::OK);

    // Kill all masters other than one backed up master
    bool find_one_backup = false;
    for (int i = 0; i < master_num; ++i) {
        if (i != leader_index_ && !find_one_backup) {
            find_one_backup = true;
        } else {
            ASSERT_TRUE(masters_[i]->kill());
        }
    }

    // Wait for the leader to be elected
    WaitMasterViewChange();

    // Verify the segment is remounted by trying to put a new key-value pair
    std::string key = "key";
    std::string value = "value";
    ASSERT_EQ(client->Put(key, value), ErrorCode::OK);

    // Get the value
    std::string get_value;
    ASSERT_EQ(client->Get(key, get_value), ErrorCode::OK);
    ASSERT_EQ(get_value, value);
}

// Verify the client can automatically remount after all masters are killed.
TEST_F(ChaosTest, AllMastersKilledThenRestartFailover) {
    const int master_num = static_cast<int>(masters_.size());

    // Create a client
    std::shared_ptr<ClientTestWrapper> client =
        CreateClientWrapper("0.0.0.0:" + std::to_string(client_port_base));
    ASSERT_TRUE(client != nullptr);

    // Mount a segment
    void* buffer;
    ASSERT_EQ(client->Mount(1024 * 1024 * 16, buffer), ErrorCode::OK);

    // Kill all masters
    for (int i = 0; i < master_num; ++i) {
        ASSERT_TRUE(masters_[i]->kill());
    }

    // Restart the masters
    for (int i = 0; i < master_num; ++i) {
        ASSERT_TRUE(masters_[i]->start());
    }

    // Wait for the leader to be elected
    WaitMasterViewChange();

    // Verify the segment is remounted by trying to put a new key-value pair
    std::string key = "key";
    std::string value = "value";
    ASSERT_EQ(client->Put(key, value), ErrorCode::OK);

    // Get the value
    std::string get_value;
    ASSERT_EQ(client->Get(key, get_value), ErrorCode::OK);
    ASSERT_EQ(get_value, value);
}

// Verify system soundness when a client gracefully closes.
TEST_F(ChaosTest, ClientGracefulClose) {
    // Create two clients
    std::shared_ptr<ClientTestWrapper> to_close_client =
        CreateClientWrapper("0.0.0.0:" + std::to_string(client_port_base));
    ASSERT_TRUE(to_close_client != nullptr);
    std::shared_ptr<ClientTestWrapper> other_client =
        CreateClientWrapper("0.0.0.0:" + std::to_string(client_port_base + 1));
    ASSERT_TRUE(other_client != nullptr);

    // Mount a segment
    void* buffer;
    ASSERT_EQ(to_close_client->Mount(1024 * 1024 * 16, buffer), ErrorCode::OK);

    // Put a key-value pair
    std::string key = "key";
    std::string value = "value";
    ASSERT_EQ(other_client->Put(key, value), ErrorCode::OK);

    // Close the to_close_client
    to_close_client.reset();

    // Try to get the value
    std::string get_value;
    ASSERT_EQ(other_client->Get(key, get_value), ErrorCode::OBJECT_NOT_FOUND);

    // Try to put a key-value pair
    ASSERT_EQ(other_client->Put(key, value), ErrorCode::NO_AVAILABLE_HANDLE);
}

// Verify system soundness when a client is killed.
TEST_F(ChaosTest, ClientKilledFailover) {
    // Create two clients
    ClientRunnerConfig clt_runner_cfg = {
        .put_prob = 0,
        .get_prob = 0,
        .mount_prob = 1000,
        .unmount_prob = 0,
        .port = client_port_base + 0,  // index 0
        .master_server_entry = "etcd://" + FLAGS_etcd_endpoints,
        .engine_meta_url = FLAGS_engine_meta_url,
        .protocol = FLAGS_protocol,
        .device_name = FLAGS_device_name,
    };
    ClientProcessHandler to_close_client(FLAGS_client_path, 0, FLAGS_out_dir,
                                         clt_runner_cfg);
    // Set the client config to only run mount operation
    ASSERT_TRUE(to_close_client.start());
    std::shared_ptr<ClientTestWrapper> other_client =
        CreateClientWrapper("0.0.0.0:" + std::to_string(client_port_base + 1));
    ASSERT_TRUE(other_client != nullptr);

    // Wait for the to_close_client to run and mount some segment
    sleep(10);

    // Put a key-value pair
    std::string key = "key";
    std::string value = "value";
    ASSERT_EQ(other_client->Put(key, value), ErrorCode::OK);

    // Get the value
    std::string get_value;
    ASSERT_EQ(other_client->Get(key, get_value), ErrorCode::OK);
    ASSERT_EQ(get_value, value);

    // Kill the to_close_client
    ASSERT_TRUE(to_close_client.kill());

    // Wait for the leader to notice the client is killed
    WaitClientCrashDetection();

    // Try to get the value
    ASSERT_EQ(other_client->Get(key, get_value), ErrorCode::OBJECT_NOT_FOUND);

    // Try to put a key-value pair
    ASSERT_EQ(other_client->Put(key, value), ErrorCode::NO_AVAILABLE_HANDLE);
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