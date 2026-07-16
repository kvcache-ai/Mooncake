#include <glog/logging.h>
#include <gtest/gtest.h>
#include <ylt/coro_rpc/coro_rpc_server.hpp>

#include "common.h"
#include "utils.h"

namespace mooncake {
namespace {

// Regression: --host=ip:port should not break coro_rpc_server binding.
// When --host includes a port (e.g. 127.0.0.1:18007) for the TransferEngine
// data plane, the port must be stripped before the address is passed to
// coro_rpc_server, otherwise the server fails to start.

class HostPortFixTest : public ::testing::Test {
   protected:
    static void SetUpTestSuite() {
        FLAGS_logtostderr = true;
        google::InitGoogleLogging("HostPortFixTest");
    }
    static void TearDownTestSuite() { google::ShutdownGoogleLogging(); }
};

TEST_F(HostPortFixTest, StripsPortForRpcServer) {
    auto rpc_bind_host = getHostNameWithoutPort("127.0.0.1:18007");
    EXPECT_EQ(rpc_bind_host, "127.0.0.1");

    auto port = getFreeTcpPort();
    coro_rpc::coro_rpc_server server(/*thread_num=*/1, port, rpc_bind_host);
    auto ec = server.async_start();
    EXPECT_FALSE(ec.hasResult()) << "Server should start with bare hostname";
    server.stop();
}

TEST_F(HostPortFixTest, BareHostPassesThrough) {
    auto rpc_bind_host = getHostNameWithoutPort("127.0.0.1");
    EXPECT_EQ(rpc_bind_host, "127.0.0.1");

    auto port = getFreeTcpPort();
    coro_rpc::coro_rpc_server server(/*thread_num=*/1, port, rpc_bind_host);
    auto ec = server.async_start();
    EXPECT_FALSE(ec.hasResult()) << "Server should start with bare hostname";
    server.stop();
}

}  // namespace
}  // namespace mooncake
