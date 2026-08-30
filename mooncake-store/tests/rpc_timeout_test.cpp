// Unit tests for the client->master RPC timeout feature.
//
// Covers:
//   1. The ErrorCode::RPC_TIMEOUT enum value and its toString() mapping.
//   2. End-to-end: MC_RPC_TIMEOUT_MS shortens the per-request deadline so that
//      an unresponsive master surfaces ErrorCode::RPC_TIMEOUT (not RPC_FAIL),
//      and it does so within the configured budget rather than the 30s default.
//   3. HA MasterClient bounds failed leader connections while non-HA clients
//      retain the default initial-connection retry policy.
//
// The end-to-end test points a MasterClient at a "black hole" TCP listener: a
// socket that accepts the connection (so connect() succeeds) but never sends a
// response. The first RPC issued by Connect() (ServiceReady) therefore blocks
// until the request timeout fires.

#include <gtest/gtest.h>
#include <glog/logging.h>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <chrono>
#include <cstdlib>
#include <string>

#include "master_client.h"
#include "types.h"

namespace mooncake {
namespace {

// A TCP listener on 127.0.0.1 that accepts connections but never replies.
// The kernel completes the TCP handshake for backlogged connections, so a
// client's connect() succeeds; any subsequent request simply never gets a
// response, which is exactly what we need to exercise the request timeout.
class BlackHoleServer {
   public:
    BlackHoleServer() {
        fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
        EXPECT_GE(fd_, 0) << "failed to create socket";
        int opt = 1;
        ::setsockopt(fd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = ::inet_addr("127.0.0.1");
        addr.sin_port = 0;  // ephemeral port
        EXPECT_EQ(::bind(fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)),
                  0)
            << "bind failed";

        socklen_t len = sizeof(addr);
        EXPECT_EQ(::getsockname(fd_, reinterpret_cast<sockaddr*>(&addr), &len),
                  0);
        port_ = ntohs(addr.sin_port);

        // Backlog large enough that the kernel handshakes our single client.
        EXPECT_EQ(::listen(fd_, 16), 0) << "listen failed";
    }

    ~BlackHoleServer() {
        if (fd_ >= 0) {
            ::close(fd_);
        }
    }

    std::string address() const { return "127.0.0.1:" + std::to_string(port_); }

   private:
    int fd_ = -1;
    uint16_t port_ = 0;
};

}  // namespace

// Change #1 and #2: the new error code exists with the documented value and a
// matching human-readable string.
TEST(RpcTimeoutTest, ErrorCodeValueAndString) {
    EXPECT_EQ(static_cast<int32_t>(ErrorCode::RPC_TIMEOUT), -901);
    EXPECT_EQ(toString(ErrorCode::RPC_TIMEOUT), "RPC_TIMEOUT");
    // Distinct from the generic RPC failure code.
    EXPECT_NE(ErrorCode::RPC_TIMEOUT, ErrorCode::RPC_FAIL);
}

// End-to-end: a small MC_RPC_TIMEOUT_MS must be honored by every RPC and turn
// an unanswered call into ErrorCode::RPC_TIMEOUT well before the 30s default.
TEST(RpcTimeoutTest, RpcTimesOutAgainstUnresponsiveMaster) {
    constexpr int kTimeoutMs = 500;

    // The constructor reads MC_RPC_TIMEOUT_MS, so it must be set beforehand.
    ASSERT_EQ(::setenv("MC_RPC_TIMEOUT_MS", std::to_string(kTimeoutMs).c_str(),
                       /*overwrite=*/1),
              0);

    BlackHoleServer server;

    MasterClient client(generate_uuid());

    const auto start = std::chrono::steady_clock::now();
    ErrorCode rc = client.Connect(server.address());
    const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                             std::chrono::steady_clock::now() - start)
                             .count();

    ::unsetenv("MC_RPC_TIMEOUT_MS");

    // The unanswered ServiceReady RPC must be reported as a timeout, not as a
    // generic RPC failure.
    EXPECT_EQ(rc, ErrorCode::RPC_TIMEOUT)
        << "expected RPC_TIMEOUT, got " << toString(rc);

    // It must give up around the configured budget, proving the env override
    // took effect (the built-in default would be ~30s).
    EXPECT_GE(elapsed, kTimeoutMs - 100)
        << "returned too early to be the configured timeout";
    EXPECT_LT(elapsed, 5000) << "did not honor MC_RPC_TIMEOUT_MS (default 30s "
                                "timeout still active?)";
}

// During failover the heartbeat can still be connecting to the deleted
// leader's pod IP. It must return promptly so the HA loop can use the newly
// published view instead of spending the default retry budget on a stale
// peer. Bind an ephemeral loopback port without listening on it so connection
// attempts are rejected deterministically while the port remains reserved by
// this test.
TEST(RpcTimeoutTest, HaRuntimePolicyReplacesInitialConnectionPolicy) {
    int probe_fd = ::socket(AF_INET, SOCK_STREAM, 0);
    ASSERT_GE(probe_fd, 0) << "failed to create probe socket";

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = ::inet_addr("127.0.0.1");
    addr.sin_port = 0;
    ASSERT_EQ(
        ::bind(probe_fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)), 0)
        << "failed to reserve loopback port";

    socklen_t len = sizeof(addr);
    ASSERT_EQ(::getsockname(probe_fd, reinterpret_cast<sockaddr*>(&addr), &len),
              0);
    const auto port = ntohs(addr.sin_port);
    ASSERT_GT(port, 0);

    ASSERT_EQ(::setenv("MC_RPC_CONNECT_TIMEOUT_MS", "100", 1), 0);

    MasterClient client(generate_uuid());

    // Initialization keeps the normal retry policy. With an immediately
    // refused endpoint, its three one-second retry waits are observable.
    const auto initial_start = std::chrono::steady_clock::now();
    const auto initial_rc = client.Connect("127.0.0.1:" + std::to_string(port));
    const auto initial_elapsed =
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - initial_start)
            .count();

    // Client initialization has now used the pool. Entering HA runtime must
    // replace it, rather than silently retaining the initial retry policy.
    client.EnableHaConnectionPolicy();
    const auto start = std::chrono::steady_clock::now();
    const auto rc = client.Connect("127.0.0.1:" + std::to_string(port));
    const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                             std::chrono::steady_clock::now() - start)
                             .count();

    ::unsetenv("MC_RPC_CONNECT_TIMEOUT_MS");
    EXPECT_EQ(::close(probe_fd), 0);

    EXPECT_EQ(initial_rc, ErrorCode::RPC_FAIL);
    EXPECT_GE(initial_elapsed, 2500)
        << "initial connection unexpectedly lost its retry resilience";
    EXPECT_EQ(rc, ErrorCode::RPC_FAIL);
    EXPECT_LT(elapsed, 750)
        << "MasterClient retried a failed leader connection internally for "
        << elapsed << "ms";
}

// The offload data path (store->store) builds its own client pool, separate
// from the master pool, and used to ignore these variables entirely: its
// connect timeout stayed at the built-in 30s, so a read that picked a peer
// which had gone away blocked for connect_retry_count * 30s plus the waits
// between retries with no way to configure it down. Both pools now go through
// one helper, so cover the helper itself: an unset variable must leave the
// built-in default alone, and a set one must be applied.
TEST(RpcTimeoutTest, TimeoutEnvOverridesAreOptIn) {
    struct StubClientConfig {
        std::chrono::milliseconds request_timeout_duration{
            std::chrono::seconds(30)};
        std::chrono::milliseconds connect_timeout_duration{
            std::chrono::seconds(30)};
    };

    ::unsetenv("MC_RPC_TIMEOUT_MS");
    ::unsetenv("MC_RPC_CONNECT_TIMEOUT_MS");

    auto non_ha_config = detail::MakeMasterRpcClientPoolConfig();
    EXPECT_EQ(non_ha_config.connect_retry_count, 3u);
    EXPECT_EQ(non_ha_config.reconnect_wait_time,
              std::chrono::milliseconds(1000));
    EXPECT_EQ(non_ha_config.client_config.connect_timeout_duration,
              std::chrono::seconds(30));

    auto ha_config = detail::MakeMasterRpcClientPoolConfig(/*ha_enabled=*/true);
    EXPECT_EQ(ha_config.connect_retry_count, 0u);
    EXPECT_EQ(ha_config.reconnect_wait_time, std::chrono::milliseconds(0));
    EXPECT_EQ(ha_config.client_config.connect_timeout_duration,
              std::chrono::seconds(1));

    StubClientConfig defaults;
    detail::ApplyRpcTimeoutEnvOverrides(defaults);
    EXPECT_EQ(defaults.request_timeout_duration, std::chrono::seconds(30));
    EXPECT_EQ(defaults.connect_timeout_duration, std::chrono::seconds(30));

    ASSERT_EQ(::setenv("MC_RPC_TIMEOUT_MS", "1500", /*overwrite=*/1), 0);
    ASSERT_EQ(::setenv("MC_RPC_CONNECT_TIMEOUT_MS", "1500", /*overwrite=*/1),
              0);

    StubClientConfig overridden;
    detail::ApplyRpcTimeoutEnvOverrides(overridden);
    EXPECT_EQ(overridden.request_timeout_duration,
              std::chrono::milliseconds(1500));
    EXPECT_EQ(overridden.connect_timeout_duration,
              std::chrono::milliseconds(1500));

    // Explicit timeout overrides still take precedence over the HA default.
    auto master_config =
        detail::MakeMasterRpcClientPoolConfig(/*ha_enabled=*/true);
    EXPECT_EQ(master_config.connect_retry_count, 0u);
    EXPECT_EQ(master_config.reconnect_wait_time, std::chrono::milliseconds(0));
    EXPECT_EQ(master_config.client_config.connect_timeout_duration,
              std::chrono::milliseconds(1500));

    ::unsetenv("MC_RPC_TIMEOUT_MS");
    ::unsetenv("MC_RPC_CONNECT_TIMEOUT_MS");
}

}  // namespace mooncake
