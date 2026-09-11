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
//   2. Reverse mismatch: legacy client vs dedicated master -> Connect fails
//      fast with HEARTBEAT_ROUTING_MISMATCH.
//   2b. Dedicated-port value mismatch (both dedicated, different ports) ->
//       Connect fails fast with HEARTBEAT_RPC_UNREACHABLE at the reachability
//       probe.
//   3. Non-heartbeat RPCs still work via the main port.
//   4. With the dedicated server disabled (port=0), Heartbeat is served on the
//      main port as the legacy fallback.
//   5. Forward mismatch: dedicated client vs legacy master -> Connect fails
//      fast with HEARTBEAT_ROUTING_MISMATCH.
//   6. Reconnect after a master restart (same ports) rebuilds the stale main
//      AND heartbeat connection pools via the is_same_addr retry instead of
//      spuriously failing.
//   7. The heartbeat fail-fast error codes round-trip through toString/fromInt.
//
// Dedicated heartbeat routing is a P2P-only feature after the centralized
// a00f757 protocol restoration.

#include <gtest/gtest.h>

#include <chrono>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "p2p/master/p2p_master_client.h"
#include "rpc_types.h"
#include "test_p2p_server_helpers.h"
#include "types.h"
#include "utils.h"

namespace mooncake {
namespace testing {

namespace {

// Build a P2PHeartbeatRequest for the given client id (no tasks, lightweight).
P2PHeartbeatRequest MakeHeartbeatRequest(const UUID& client_id) {
    P2PHeartbeatRequest req;
    req.client_id = client_id;
    return req;
}

// Lightweight in-process P2P master that can be restarted on fixed ports.
class HeartbeatTestMaster {
   public:
    // Start the master. When `dedicated` is true a dedicated heartbeat server
    // is opened (on a fresh free port, or `hb_port` if given). `rpc_port` and
    // `hb_port` let a restart pin the same ports the previous instance used.
    bool Start(bool dedicated, std::optional<int> rpc_port = std::nullopt,
               std::optional<int> hb_port = std::nullopt) {
        InProcP2PMasterConfigBuilder builder;
        rpc_port_ = rpc_port.value_or(getFreeTcpPort());
        builder.set_rpc_port(rpc_port_);
        if (dedicated) {
            heartbeat_rpc_port_ = hb_port.value_or(getFreeTcpPort());
            builder.set_heartbeat_rpc_port(heartbeat_rpc_port_);
        } else {
            heartbeat_rpc_port_ = 0;
        }
        return master_.Start(builder.build());
    }

    void Stop() { master_.Stop(); }

    int rpc_port() const { return rpc_port_; }
    int heartbeat_rpc_port() const { return heartbeat_rpc_port_; }
    std::string master_address() const { return master_.master_address(); }

   private:
    InProcP2PMaster master_;
    int rpc_port_ = 0;
    int heartbeat_rpc_port_ = 0;
};

class HeartbeatTestClient {
   public:
    explicit HeartbeatTestClient(const UUID& client_id) : client_(client_id) {}

    void SetHeartbeatRpcPort(uint16_t port) {
        client_.SetHeartbeatRpcPort(port);
    }

    ErrorCode Connect(const std::string& address) {
        return client_.Connect(address);
    }

    tl::expected<P2PHeartbeatResponse, ErrorCode> Heartbeat(
        const P2PHeartbeatRequest& request) {
        return client_.Heartbeat(request);
    }

    tl::expected<bool, ErrorCode> ExistKey(std::string_view key) {
        return client_.ExistKey(key);
    }

    std::vector<tl::expected<bool, ErrorCode>> BatchExistKey(
        const std::vector<std::string_view>& keys) {
        return client_.BatchExistKey(keys);
    }

   private:
    P2PMasterClient client_;
};

std::unique_ptr<HeartbeatTestClient> MakeClient(const UUID& client_id) {
    return std::make_unique<HeartbeatTestClient>(client_id);
}

constexpr uint16_t PortOf(int port) { return static_cast<uint16_t>(port); }

}  // namespace

// ---------------------------------------------------------------------------
// Dedicated heartbeat server enabled.
// ---------------------------------------------------------------------------
class HeartbeatDedicatedPortTest : public ::testing::Test {
   protected:
    void SetUp() override {
        master_ = std::make_unique<HeartbeatTestMaster>();
        ASSERT_TRUE(master_->Start(/*dedicated=*/true))
            << "Failed to start InProcMaster with dedicated heartbeat port";
    }
    void TearDown() override { master_->Stop(); }

    std::unique_ptr<HeartbeatTestMaster> master_;
};

// 1. Client configured with the dedicated port sends Heartbeat there and it
// succeeds (the dedicated server has the Heartbeat handler registered). This
// also implicitly proves both that HeartbeatServiceReady is registered on the
// main server (Connect queries it for the master's heartbeat port) and that
// ServiceReady is registered on the dedicated server (Connect's reachability
// probe would otherwise return HEARTBEAT_RPC_UNREACHABLE).
TEST_F(HeartbeatDedicatedPortTest, HeartbeatRoutedToDedicatedPort) {
    UUID client_id = generate_uuid();
    auto client = MakeClient(client_id);
    client->SetHeartbeatRpcPort(PortOf(master_->heartbeat_rpc_port()));
    ASSERT_EQ(client->Connect(master_->master_address()), ErrorCode::OK);

    auto hb = client->Heartbeat(MakeHeartbeatRequest(client_id));
    EXPECT_TRUE(hb.has_value())
        << "Heartbeat should succeed via the dedicated heartbeat port";
}

// 2. Reverse mismatch: the master runs a dedicated heartbeat server, but the
// client is legacy (heartbeat_rpc_port=0). Connect must fail fast with
// HEARTBEAT_ROUTING_MISMATCH — otherwise the client would route heartbeats to
// the main server, which dropped the Heartbeat handler when the dedicated
// server was enabled, and silently starve.
TEST_F(HeartbeatDedicatedPortTest,
       LegacyClientAgainstDedicatedMasterFailsConnect) {
    UUID client_id = generate_uuid();
    auto client = MakeClient(client_id);  // heartbeat_rpc_port_ == 0
    EXPECT_EQ(client->Connect(master_->master_address()),
              ErrorCode::HEARTBEAT_ROUTING_MISMATCH)
        << "A legacy client must fail fast against a dedicated-port master "
        << "instead of routing heartbeats to a main server that no longer "
        << "serves Heartbeat";
}

// 2b. Dedicated-port value mismatch: both sides are dedicated, but the client
// points at a different dedicated port than the master opened. This is not a
// routing-mode mismatch (both report dedicated), so it falls through to the
// dedicated-server reachability probe and fails with HEARTBEAT_RPC_UNREACHABLE.
TEST_F(HeartbeatDedicatedPortTest, DedicatedPortValueMismatchFailsConnect) {
    int wrong_port = getFreeTcpPort();
    ASSERT_NE(wrong_port, master_->heartbeat_rpc_port());
    UUID client_id = generate_uuid();
    auto client = MakeClient(client_id);
    client->SetHeartbeatRpcPort(PortOf(wrong_port));

    EXPECT_EQ(client->Connect(master_->master_address()),
              ErrorCode::HEARTBEAT_RPC_UNREACHABLE)
        << "A client pointed at the wrong dedicated port must fail fast at the "
        << "reachability probe rather than silently failing heartbeats";
}

// 3. Non-heartbeat RPCs still reach the main port and succeed, proving the
// main server still serves the rest of the API (Connect already exercises
// ServiceReady internally; ExistKey covers a data-path RPC).
TEST_F(HeartbeatDedicatedPortTest, NonHeartbeatRpcStillServedOnMainPort) {
    UUID client_id = generate_uuid();
    auto client = MakeClient(client_id);
    client->SetHeartbeatRpcPort(PortOf(master_->heartbeat_rpc_port()));
    ASSERT_EQ(client->Connect(master_->master_address()), ErrorCode::OK);

    auto exists = client->ExistKey("nonexistent_key_for_heartbeat_test");
    ASSERT_TRUE(exists.has_value())
        << "ExistKey should reach the main port and succeed";
    EXPECT_FALSE(exists.value());

    const std::vector<std::string_view> keys{"missing-a", "missing-b"};
    auto batch_exists = client->BatchExistKey(keys);
    ASSERT_EQ(batch_exists.size(), keys.size());
    for (const auto& result : batch_exists) {
        ASSERT_TRUE(result.has_value());
        EXPECT_FALSE(result.value());
    }
}

// ---------------------------------------------------------------------------
// Legacy fallback (no dedicated heartbeat server).
// ---------------------------------------------------------------------------
class HeartbeatLegacyMasterTest : public ::testing::Test {
   protected:
    void SetUp() override {
        master_ = std::make_unique<HeartbeatTestMaster>();
        ASSERT_TRUE(master_->Start(/*dedicated=*/false))
            << "Failed to start InProcMaster (legacy, no dedicated port)";
    }
    void TearDown() override { master_->Stop(); }

    std::unique_ptr<HeartbeatTestMaster> master_;
};

// 4. With no dedicated heartbeat server configured (port=0, default), the
// master falls back to serving Heartbeat on the main RPC server, so a client
// without a heartbeat port succeeds.
TEST_F(HeartbeatLegacyMasterTest, HeartbeatServedOnMainPortWhenDisabled) {
    UUID client_id = generate_uuid();
    auto client = MakeClient(client_id);  // heartbeat_rpc_port_ == 0
    ASSERT_EQ(client->Connect(master_->master_address()), ErrorCode::OK);

    auto hb = client->Heartbeat(MakeHeartbeatRequest(client_id));
    EXPECT_TRUE(hb.has_value())
        << "Heartbeat should succeed on the main port when the dedicated "
        << "server is disabled (legacy fallback)";
}

// 5. Forward mismatch: the master is started WITHOUT a dedicated heartbeat
// port (legacy fallback), but the client is configured with a heartbeat port
// (dedicated). Connect must fail fast with HEARTBEAT_ROUTING_MISMATCH instead
// of appearing to succeed and then silently starving heartbeats.
TEST_F(HeartbeatLegacyMasterTest, HeartbeatPortMismatchFailsConnect) {
    // Client believes it is dedicated; the master reports legacy (port=0) via
    // HeartbeatServiceReady, so the routing-mode comparison catches it.
    int phantom_port = getFreeTcpPort();
    UUID client_id = generate_uuid();
    auto client = MakeClient(client_id);
    client->SetHeartbeatRpcPort(PortOf(phantom_port));

    EXPECT_EQ(client->Connect(master_->master_address()),
              ErrorCode::HEARTBEAT_ROUTING_MISMATCH)
        << "Connect must fail fast when the client is dedicated but the master "
        << "runs in legacy heartbeat mode";
}

// ---------------------------------------------------------------------------
// Reconnect after master restart (stale connection pool rebuild).
// ---------------------------------------------------------------------------
class HeartbeatDedicatedPortReconnectTest : public ::testing::Test {
   protected:
    void SetUp() override { master_ = std::make_unique<HeartbeatTestMaster>(); }
    void TearDown() override { master_->Stop(); }

    std::unique_ptr<HeartbeatTestMaster> master_;
};

// 6. After a successful Connect, restarting the master on the same ports
// invalidates the pooled main AND heartbeat connections. Reconnecting to the
// same address (is_same_addr=true) must take the one-shot stale-pool retry on
// BOTH probes and succeed. A missing heartbeat retry would surface here as a
// spurious HEARTBEAT_RPC_UNREACHABLE despite the heartbeat server being up.
TEST_F(HeartbeatDedicatedPortReconnectTest,
       ReconnectAfterMasterRestartRebuildsStalePools) {
    ASSERT_TRUE(master_->Start(/*dedicated=*/true));
    int rpc_port = master_->rpc_port();
    int hb_port = master_->heartbeat_rpc_port();
    std::string addr = master_->master_address();

    UUID client_id = generate_uuid();
    auto client = MakeClient(client_id);
    client->SetHeartbeatRpcPort(PortOf(hb_port));
    ASSERT_EQ(client->Connect(addr), ErrorCode::OK);

    // Restart on the same ports -> both pooled connections go stale.
    master_->Stop();
    // Give the OS a moment to release the listening sockets before rebind.
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    ASSERT_TRUE(master_->Start(/*dedicated=*/true, rpc_port, hb_port));

    // Same address -> is_same_addr=true -> stale-pool retry path.
    EXPECT_EQ(client->Connect(addr), ErrorCode::OK)
        << "Reconnect to the same address should rebuild stale pools via the "
        << "is_same_addr retry, not fail with HEARTBEAT_RPC_UNREACHABLE";

    // Heartbeats must still land on the restarted dedicated heartbeat server.
    auto hb = client->Heartbeat(MakeHeartbeatRequest(client_id));
    EXPECT_TRUE(hb.has_value())
        << "Heartbeat should succeed via the dedicated port after reconnect";
}

// ---------------------------------------------------------------------------
// Error code serialization (mode-independent).
// ---------------------------------------------------------------------------
// 7. The heartbeat fail-fast error codes round-trip through toString/fromInt.
TEST(HeartbeatErrorCodeTest, HeartbeatErrorCodesRoundTrip) {
    EXPECT_EQ(toString(ErrorCode::HEARTBEAT_RPC_UNREACHABLE),
              "HEARTBEAT_RPC_UNREACHABLE");
    EXPECT_EQ(fromInt(toInt(ErrorCode::HEARTBEAT_RPC_UNREACHABLE)),
              ErrorCode::HEARTBEAT_RPC_UNREACHABLE);

    EXPECT_EQ(toString(ErrorCode::HEARTBEAT_ROUTING_MISMATCH),
              "HEARTBEAT_ROUTING_MISMATCH");
    EXPECT_EQ(fromInt(toInt(ErrorCode::HEARTBEAT_ROUTING_MISMATCH)),
              ErrorCode::HEARTBEAT_ROUTING_MISMATCH);
}

}  // namespace testing
}  // namespace mooncake
