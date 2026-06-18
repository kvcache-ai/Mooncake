// Unit tests for the client->master RPC timeout feature.
//
// Covers:
//   1. The ErrorCode::RPC_TIMEOUT enum value and its toString() mapping.
//   2. End-to-end: MC_RPC_TIMEOUT_MS shortens the per-request deadline so that
//      an unresponsive master surfaces ErrorCode::RPC_TIMEOUT (not RPC_FAIL),
//      and it does so within the configured budget rather than the 30s default.
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

}  // namespace mooncake
