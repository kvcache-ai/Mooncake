#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <unistd.h>

#include <memory>
#include <string>

#include "chaos.h"
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
DEFINE_string(client_path, "./mooncake-store/tests/chaos/chaosclient",
              "Path to the client executable");
DEFINE_string(out_dir, "./output", "Directory for log files");

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

std::shared_ptr<MasterViewHelper> ChaosTest::master_view_helper_ = nullptr;

// Verify the system failover after the leader is killed.
TEST_F(ChaosTest, LeaderKilledFailover) {
    // Start masters
    const int master_num = 3;
    std::vector<std::unique_ptr<mooncake::testing::MasterHandler>> masters;
    for (int i = 0; i < master_num; ++i) {
        masters.emplace_back(std::make_unique<mooncake::testing::MasterHandler>(
            FLAGS_master_path, master_port_base + i, i, FLAGS_out_dir));
        masters.back()->start();
    }

    // Wait for the leader to be elected
    WaitMasterViewChange();

    int leader_index;
    GetLeaderIndex(leader_index);
    ASSERT_TRUE(leader_index >= 0 && leader_index < master_num);

    // Create a client
    std::shared_ptr<ClientTestWrapper> client =
        CreateClient("0.0.0.0:" + std::to_string(client_port_base));
    ASSERT_TRUE(client != nullptr);

    // Mount a segment
    void* buffer;
    ASSERT_EQ(client->Mount(1024 * 1024 * 16, buffer), ErrorCode::OK);

    // Put a key-value pair
    std::string key = "key";
    std::string value = "value";
    ASSERT_EQ(client->Put(key, value), ErrorCode::OK);

    // Kill the leader and wait for the new leader to be elected
    masters[leader_index]->kill();
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
    // Start masters
    const int master_num = 3;
    std::vector<std::unique_ptr<mooncake::testing::MasterHandler>> masters;
    for (int i = 0; i < master_num; ++i) {
        masters.emplace_back(std::make_unique<mooncake::testing::MasterHandler>(
            FLAGS_master_path, master_port_base + i, i, FLAGS_out_dir));
        masters.back()->start();
    }

    // Wait for the leader to be elected
    WaitMasterViewChange();

    int leader_index;
    GetLeaderIndex(leader_index);
    ASSERT_TRUE(leader_index >= 0 && leader_index < master_num);

    // Create a client
    std::shared_ptr<ClientTestWrapper> client =
        CreateClient("0.0.0.0:" + std::to_string(client_port_base));
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
        if (i != leader_index) {
            masters[i]->kill();
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
    // Start masters
    const int master_num = 3;
    std::vector<std::unique_ptr<mooncake::testing::MasterHandler>> masters;
    for (int i = 0; i < master_num; ++i) {
        masters.emplace_back(std::make_unique<mooncake::testing::MasterHandler>(
            FLAGS_master_path, master_port_base + i, i, FLAGS_out_dir));
        masters.back()->start();
    }

    // Wait for the leader to be elected
    WaitMasterViewChange();

    int leader_index;
    GetLeaderIndex(leader_index);
    ASSERT_TRUE(leader_index >= 0 && leader_index < master_num);

    // Create a client
    std::shared_ptr<ClientTestWrapper> client =
        CreateClient("0.0.0.0:" + std::to_string(client_port_base));
    ASSERT_TRUE(client != nullptr);

    // Mount a segment
    void* buffer;
    ASSERT_EQ(client->Mount(1024 * 1024 * 16, buffer), ErrorCode::OK);

    // Kill all masters other than one backed up master
    bool find_one_backup = false;
    for (int i = 0; i < master_num; ++i) {
        if (i != leader_index && !find_one_backup) {
            find_one_backup = true;
        } else {
            masters[i]->kill();
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

// Verify the client can automatically remount after all master are killed.
TEST_F(ChaosTest, AllMastersKilledThenRestartFailover) {
    // Start masters
    const int master_num = 3;
    std::vector<std::unique_ptr<mooncake::testing::MasterHandler>> masters;
    for (int i = 0; i < master_num; ++i) {
        masters.emplace_back(std::make_unique<mooncake::testing::MasterHandler>(
            FLAGS_master_path, master_port_base + i, i, FLAGS_out_dir));
        masters.back()->start();
    }

    // Wait for the leader to be elected
    WaitMasterViewChange();

    // Create a client
    std::shared_ptr<ClientTestWrapper> client =
        CreateClient("0.0.0.0:" + std::to_string(client_port_base));
    ASSERT_TRUE(client != nullptr);

    // Mount a segment
    void* buffer;
    ASSERT_EQ(client->Mount(1024 * 1024 * 16, buffer), ErrorCode::OK);

    // Kill all masters
    for (int i = 0; i < master_num; ++i) {
        masters[i]->kill();
    }

    // Restart the masters
    for (int i = 0; i < master_num; ++i) {
        masters[i]->start();
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
    // Start masters
    const int master_num = 3;
    std::vector<std::unique_ptr<mooncake::testing::MasterHandler>> masters;
    for (int i = 0; i < master_num; ++i) {
        masters.emplace_back(std::make_unique<mooncake::testing::MasterHandler>(
            FLAGS_master_path, master_port_base + i, i, FLAGS_out_dir));
        masters.back()->start();
    }

    // Wait for the leader to be elected
    WaitMasterViewChange();

    // Create two clients
    std::shared_ptr<ClientTestWrapper> to_close_client =
        CreateClient("0.0.0.0:" + std::to_string(client_port_base));
    ASSERT_TRUE(to_close_client != nullptr);
    std::shared_ptr<ClientTestWrapper> other_client =
        CreateClient("0.0.0.0:" + std::to_string(client_port_base + 1));
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
    // Start masters
    const int master_num = 3;
    std::vector<std::unique_ptr<mooncake::testing::MasterHandler>> masters;
    for (int i = 0; i < master_num; ++i) {
        masters.emplace_back(std::make_unique<mooncake::testing::MasterHandler>(
            FLAGS_master_path, master_port_base + i, i, FLAGS_out_dir));
        masters.back()->start();
    }

    // Wait for the leader to be elected
    WaitMasterViewChange();

    // Create two clients
    ClientHandler to_close_client(FLAGS_client_path, FLAGS_out_dir, 0);
    // Set the client config to only run mount operation
    to_close_client.start(ChaosClientConfig{.put_prob = 0, .get_prob = 0, .mount_prob = 1000, .unmount_prob = 0});
    std::shared_ptr<ClientTestWrapper> other_client =
        CreateClient("0.0.0.0:" + std::to_string(client_port_base + 1));
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
    to_close_client.kill();

    // Wait for the leader to notice the client is killed
    WaitClientCrashDetection();

    // Try to get the value
    ASSERT_EQ(other_client->Get(key, get_value), ErrorCode::OBJECT_NOT_FOUND);

    // Try to put a key-value pair
    ASSERT_EQ(other_client->Put(key, value), ErrorCode::NO_AVAILABLE_HANDLE);
}

}  // namespace testing
}  // namespace mooncake