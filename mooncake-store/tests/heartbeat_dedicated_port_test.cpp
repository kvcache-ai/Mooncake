// Functional/routing tests for the dedicated heartbeat RPC server.
//
// When the master is started with a dedicated heartbeat port, Heartbeat is
// served on a separate coro_rpc_server and removed from the main server, so
// heavy metadata RPCs cannot head-of-line-block heartbeats. When no dedicated
// port is configured (heartbeat_rpc_port == 0), Heartbeat is served on the
// main server as a legacy fallback. These tests verify the routing
// (deterministic); the actual isolation-under-load property is a perf concern
// covered separately.
//
// Coverage:
//   1. Heartbeat reaches the dedicated port and succeeds.
//   2. Heartbeat is NOT registered on the main port when the dedicated server
//      is enabled.
//   3. Non-heartbeat RPCs still work via the main port.
//   4. With the dedicated server disabled (port=0), Heartbeat is served on the
//      main port as the legacy fallback.

#include <gtest/gtest.h>

#include <cstdint>
#include <string>

#include "centralized_master_client.h"
#include "master_config.h"
#include "rpc_types.h"
#include "test_server_helpers.h"
#include "types.h"
#include "utils.h"

namespace mooncake {
namespace testing {

namespace {
// Build a HeartbeatRequest for the given client id (no tasks, lightweight).
HeartbeatRequest MakeHeartbeatRequest(const UUID& client_id) {
    HeartbeatRequest req;
    req.client_id = client_id;
    return req;
}
}  // namespace

class HeartbeatDedicatedPortTest : public ::testing::Test {
   protected:
    void SetUp() override {
        heartbeat_port_ = getFreeTcpPort();
        ASSERT_TRUE(master_.Start(InProcMasterConfigBuilder()
                                      .set_heartbeat_rpc_port(heartbeat_port_)
                                      .build()))
            << "Failed to start InProcMaster with dedicated heartbeat port";
    }
    void TearDown() override { master_.Stop(); }

    InProcMaster master_;
    int heartbeat_port_ = 0;
};

// 1. Client configured with the dedicated port sends Heartbeat there and it
// succeeds (the dedicated server has the Heartbeat handler registered).
TEST_F(HeartbeatDedicatedPortTest, HeartbeatRoutedToDedicatedPort) {
    UUID client_id = generate_uuid();
    CentralizedMasterClient client(client_id);
    client.SetHeartbeatRpcPort(static_cast<uint16_t>(heartbeat_port_));
    ASSERT_EQ(client.Connect(master_.master_address()), ErrorCode::OK);

    auto hb = client.Heartbeat(MakeHeartbeatRequest(client_id));
    EXPECT_TRUE(hb.has_value())
        << "Heartbeat should succeed via the dedicated heartbeat port";
}

// 2. When the dedicated server is enabled, the main server must NOT have the
// Heartbeat handler. A client that forces heartbeat_rpc_port=0 sends Heartbeat
// to the main port and must fail.
TEST_F(HeartbeatDedicatedPortTest, HeartbeatNotRegisteredOnMainPort) {
    UUID client_id = generate_uuid();
    CentralizedMasterClient client(client_id);  // heartbeat_rpc_port_ == 0
    ASSERT_EQ(client.Connect(master_.master_address()), ErrorCode::OK);

    auto hb = client.Heartbeat(MakeHeartbeatRequest(client_id));
    EXPECT_FALSE(hb.has_value())
        << "Heartbeat must not be served on the main port when a dedicated "
        << "heartbeat server is enabled";
}

// 3. Non-heartbeat RPCs still reach the main port and succeed, proving the
// main server still serves the rest of the API (Connect already exercises
// ServiceReady internally; ExistKey covers a data-path RPC).
TEST_F(HeartbeatDedicatedPortTest, NonHeartbeatRpcStillServedOnMainPort) {
    UUID client_id = generate_uuid();
    CentralizedMasterClient client(client_id);
    client.SetHeartbeatRpcPort(static_cast<uint16_t>(heartbeat_port_));
    ASSERT_EQ(client.Connect(master_.master_address()), ErrorCode::OK);

    auto exists = client.ExistKey("nonexistent_key_for_heartbeat_test");
    EXPECT_TRUE(exists.has_value())
        << "ExistKey should reach the main port and succeed";
}

// 4. With no dedicated heartbeat server configured (port=0, default), the
// master falls back to serving Heartbeat on the main RPC server, so a client
// without a heartbeat port succeeds.
TEST(HeartbeatDedicatedPortDisabledTest,
     HeartbeatServedOnMainPortWhenDisabled) {
    InProcMaster master;
    ASSERT_TRUE(master.Start(InProcMasterConfigBuilder().build()))
        << "Failed to start InProcMaster";

    UUID client_id = generate_uuid();
    CentralizedMasterClient client(client_id);  // heartbeat_rpc_port_ == 0
    ASSERT_EQ(client.Connect(master.master_address()), ErrorCode::OK);

    auto hb = client.Heartbeat(MakeHeartbeatRequest(client_id));
    EXPECT_TRUE(hb.has_value())
        << "Heartbeat should succeed on the main port when the dedicated "
        << "server is disabled (legacy fallback)";

    master.Stop();
}

}  // namespace testing
}  // namespace mooncake
