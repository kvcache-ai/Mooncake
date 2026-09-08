// Tests for the client->master and store->store RPC timeout overrides.
//
// Covers:
//   1. The ErrorCode::RPC_TIMEOUT enum value and its toString() mapping.
//   2. End-to-end: MC_RPC_TIMEOUT_MS shortens the per-request deadline so that
//      an unresponsive master surfaces ErrorCode::RPC_TIMEOUT (not RPC_FAIL),
//      and it does so within the configured budget rather than the 30s default.
//   3. The offload requester honors the same timeout and error mapping.
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
#include <optional>
#include <string>

#include "master_client.h"
#include "pyclient.h"
#include "types.h"

namespace mooncake {
namespace {

class RpcTimeoutEnvTest : public ::testing::Test {
   protected:
    void SetUp() override {
        for (int i = 0; i < 3; ++i) {
            if (const char* value = std::getenv(names_[i])) {
                original_[i] = value;
            }
        }
        // These integration tests use TCP regardless of the ambient protocol.
        for (const char* name : names_) {
            ASSERT_EQ(unsetenv(name), 0);
        }
    }

    void TearDown() override {
        for (int i = 0; i < 3; ++i) {
            if (original_[i].has_value()) {
                EXPECT_EQ(setenv(names_[i], original_[i]->c_str(), 1), 0);
            } else {
                EXPECT_EQ(unsetenv(names_[i]), 0);
            }
        }
    }

    static constexpr const char* names_[] = {
        "MC_RPC_TIMEOUT_MS", "MC_RPC_CONNECT_TIMEOUT_MS", "MC_RPC_PROTOCOL"};
    std::optional<std::string> original_[3];
};

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
TEST_F(RpcTimeoutEnvTest, RpcTimesOutAgainstUnresponsiveMaster) {
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

// The offload data path (store->store) builds its own client pool, separate
// from the master pool, and used to ignore these variables entirely: its
// connect timeout stayed at the built-in 30s, so a read that picked a peer
// which had gone away blocked for connect_retry_count * 30s plus the waits
// between retries with no way to configure it down. Both pools now go through
// one helper, so cover the helper itself: an unset variable must leave the
// built-in default alone, and a set one must be applied.
TEST_F(RpcTimeoutEnvTest, TimeoutEnvOverridesAreOptIn) {
    struct StubClientConfig {
        std::chrono::milliseconds request_timeout_duration{
            std::chrono::seconds(30)};
        std::chrono::milliseconds connect_timeout_duration{
            std::chrono::seconds(30)};
    };

    StubClientConfig defaults;
    detail::ApplyRpcTimeoutOverrides(defaults,
                                     RpcTimeoutConfig::FromEnvironment());
    EXPECT_EQ(defaults.request_timeout_duration, std::chrono::seconds(30));
    EXPECT_EQ(defaults.connect_timeout_duration, std::chrono::seconds(30));
    const auto original_master_config = detail::MakeMasterRpcClientPoolConfig();

    ASSERT_EQ(::setenv("MC_RPC_TIMEOUT_MS", "1500", /*overwrite=*/1), 0);
    ASSERT_EQ(::setenv("MC_RPC_CONNECT_TIMEOUT_MS", "1000", /*overwrite=*/1),
              0);

    StubClientConfig overridden;
    detail::ApplyRpcTimeoutOverrides(overridden,
                                     RpcTimeoutConfig::FromEnvironment());
    EXPECT_EQ(overridden.request_timeout_duration,
              std::chrono::milliseconds(1500));
    EXPECT_EQ(overridden.connect_timeout_duration,
              std::chrono::milliseconds(1000));

    // The master pool is built from the same helper, so it sees them too.
    auto master_config = detail::MakeMasterRpcClientPoolConfig();
    EXPECT_EQ(master_config.client_config.request_timeout_duration,
              std::chrono::milliseconds(1500));
    EXPECT_EQ(master_config.client_config.connect_timeout_duration,
              std::chrono::milliseconds(1000));
    EXPECT_EQ(original_master_config.client_config.request_timeout_duration,
              std::chrono::seconds(30));
    EXPECT_EQ(original_master_config.client_config.connect_timeout_duration,
              std::chrono::seconds(30));
}

TEST_F(RpcTimeoutEnvTest, OverridesPreserveCallerPolicyAndUseResolvedValues) {
    struct StubClientConfig {
        std::chrono::milliseconds request_timeout_duration{7000};
        std::chrono::milliseconds connect_timeout_duration{1000};
    };
    StubClientConfig defaults;
    detail::ApplyRpcTimeoutOverrides(defaults,
                                     RpcTimeoutConfig::FromEnvironment());
    EXPECT_EQ(defaults.request_timeout_duration,
              std::chrono::milliseconds(7000));
    EXPECT_EQ(defaults.connect_timeout_duration,
              std::chrono::milliseconds(1000));

    // The helper must apply this snapshot, not re-read the environment.
    ASSERT_EQ(setenv("MC_RPC_TIMEOUT_MS", "9999", 1), 0);
    ASSERT_EQ(setenv("MC_RPC_CONNECT_TIMEOUT_MS", "9999", 1), 0);
    for (const int timeout_ms : {0, -1, 1500}) {
        SCOPED_TRACE(timeout_ms);
        RpcTimeoutConfig config;
        config.request_timeout = std::chrono::milliseconds(timeout_ms);
        StubClientConfig request_only;
        detail::ApplyRpcTimeoutOverrides(request_only, config);
        EXPECT_EQ(request_only.request_timeout_duration,
                  std::chrono::milliseconds(timeout_ms));
        EXPECT_EQ(request_only.connect_timeout_duration,
                  std::chrono::milliseconds(1000));

        config.request_timeout.reset();
        config.connect_timeout = std::chrono::milliseconds(timeout_ms);
        StubClientConfig connect_only;
        detail::ApplyRpcTimeoutOverrides(connect_only, config);
        EXPECT_EQ(connect_only.request_timeout_duration,
                  std::chrono::milliseconds(7000));
        EXPECT_EQ(connect_only.connect_timeout_duration,
                  std::chrono::milliseconds(timeout_ms));
    }
}

TEST_F(RpcTimeoutEnvTest, RpcTimesOutAgainstUnresponsiveOffloadPeer) {
    constexpr int kTimeoutMs = 500;
    ASSERT_EQ(setenv("MC_RPC_TIMEOUT_MS", "500", 1), 0);
    BlackHoleServer server;
    ClientRequester requester;

    const auto start = std::chrono::steady_clock::now();
    const auto result =
        requester.batch_get_offload_object(server.address(), {"key"}, {1});
    const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                             std::chrono::steady_clock::now() - start)
                             .count();

    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::RPC_TIMEOUT);
    EXPECT_GE(elapsed, kTimeoutMs - 100);
    EXPECT_LT(elapsed, 5000);
}

}  // namespace mooncake
