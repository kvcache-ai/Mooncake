#include <gtest/gtest.h>

#include <ifaddrs.h>
#include <net/if.h>

#include <array>
#include <cstdlib>
#include <memory>
#include <optional>
#include <string>
#include <system_error>

#include <asio/io_context.hpp>
#include <asio/ip/tcp.hpp>

#include "common/network.h"
#include "embedded_master.h"

namespace mooncake::testing {
namespace {

using asio::ip::tcp;

// Binding another local interface at the same port verifies that the running
// listener did not bind INADDR_ANY. It also works without external networking.
std::optional<asio::ip::address_v4> NonLoopbackAddress() {
    ifaddrs* interfaces = nullptr;
    if (getifaddrs(&interfaces) != 0) {
        return std::nullopt;
    }
    std::unique_ptr<ifaddrs, decltype(&freeifaddrs)> guard(interfaces,
                                                           freeifaddrs);
    for (auto* entry = interfaces; entry; entry = entry->ifa_next) {
        if (!entry->ifa_addr || entry->ifa_addr->sa_family != AF_INET ||
            (entry->ifa_flags & IFF_LOOPBACK) != 0) {
            continue;
        }
        const auto* address =
            reinterpret_cast<const sockaddr_in*>(entry->ifa_addr);
        return asio::ip::address_v4(ntohl(address->sin_addr.s_addr));
    }
    return std::nullopt;
}

std::error_code TryBind(const asio::ip::address_v4& address, int port) {
    asio::io_context context;
    tcp::acceptor acceptor(context);
    std::error_code ec;
    acceptor.open(tcp::v4(), ec);
    if (ec) return ec;
    acceptor.set_option(tcp::acceptor::reuse_address(true), ec);
    if (ec) return ec;
    acceptor.bind(tcp::endpoint(address, static_cast<uint16_t>(port)), ec);
    return ec;
}

class EmbeddedMasterTest : public ::testing::Test {
   protected:
    void SetUp() override {
        if (const char* value = std::getenv("MC_RPC_PROTOCOL")) {
            original_protocol_ = value;
        }
        ASSERT_EQ(setenv("MC_RPC_PROTOCOL", "tcp", 1), 0);
    }

    void TearDown() override {
        if (original_protocol_) {
            EXPECT_EQ(setenv("MC_RPC_PROTOCOL", original_protocol_->c_str(), 1),
                      0);
        } else {
            EXPECT_EQ(unsetenv("MC_RPC_PROTOCOL"), 0);
        }
    }

    std::optional<std::string> original_protocol_;
};

TEST_F(EmbeddedMasterTest, AllListenersBindOnlyToLoopback) {
    const auto non_loopback = NonLoopbackAddress();
    if (!non_loopback) {
        GTEST_SKIP() << "No non-loopback IPv4 interface available";
    }

    EmbeddedMaster master;
    ASSERT_TRUE(master.Start({}));
    asio::io_context context;
    const std::array ports = {master.rpc_port(), master.http_metrics_port(),
                              master.http_metadata_port()};
    for (int port : ports) {
        SCOPED_TRACE(port);
        tcp::socket socket(context);
        std::error_code ec;
        socket.connect(tcp::endpoint(asio::ip::address_v4::loopback(), port),
                       ec);
        ASSERT_FALSE(ec) << ec.message();
        ec = TryBind(*non_loopback, port);
        EXPECT_FALSE(ec) << "Listener must not bind non-loopback interfaces: "
                         << ec.message();
    }
    EXPECT_TRUE(httpGet(master.http_metrics_base() + "/health").has_value());
    EXPECT_TRUE(httpGet("http://127.0.0.1:" +
                        std::to_string(master.http_metadata_port()) + "/health")
                    .has_value());

    master.Stop();
    for (int port : ports) {
        const auto ec = TryBind(asio::ip::address_v4::loopback(), port);
        EXPECT_FALSE(ec) << ec.message();
    }
}

class EmbeddedMasterStartFailureTest
    : public EmbeddedMasterTest,
      public ::testing::WithParamInterface<size_t> {};

TEST_P(EmbeddedMasterStartFailureTest, ReleasesListenersAndCanRestart) {
    asio::io_context context;
    tcp::acceptor blocker(context,
                          tcp::endpoint(asio::ip::address_v4::loopback(), 0));
    auto ports = getFreeTcpPorts(3);
    ASSERT_EQ(ports.size(), 3);
    ports[GetParam()] = blocker.local_endpoint().port();
    InProcMasterConfig config;
    config.rpc_port = ports[0];
    config.http_metrics_port = ports[1];
    config.http_metadata_port = ports[2];

    EmbeddedMaster master;
    EXPECT_FALSE(master.Start(config));
    EXPECT_EQ(master.rpc_port(), 0);
    EXPECT_EQ(master.http_metrics_port(), 0);
    EXPECT_EQ(master.http_metadata_port(), 0);
    EXPECT_EQ(master.service(), nullptr);
    for (size_t i = 0; i < ports.size(); ++i) {
        if (i == GetParam()) continue;
        const auto ec = TryBind(asio::ip::address_v4::loopback(), ports[i]);
        EXPECT_FALSE(ec) << "Failed start retained port " << ports[i] << ": "
                         << ec.message();
    }

    blocker.close();
    ASSERT_TRUE(master.Start(config));
    EXPECT_EQ(master.rpc_port(), ports[0]);
    EXPECT_TRUE(httpGet(master.http_metrics_base() + "/health").has_value());
}

INSTANTIATE_TEST_SUITE_P(BindFailures, EmbeddedMasterStartFailureTest,
                         ::testing::Values(0, 1, 2));

#ifndef YLT_ENABLE_IBV
TEST_F(EmbeddedMasterTest, RdmaEnvironmentFallsBackToTcpWhenIbvIsDisabled) {
    ASSERT_EQ(setenv("MC_RPC_PROTOCOL", "rdma", 1), 0);
    EmbeddedMaster master;
    ASSERT_TRUE(master.Start({}));
    asio::io_context context;
    tcp::socket socket(context);
    std::error_code ec;
    socket.connect(
        tcp::endpoint(asio::ip::address_v4::loopback(), master.rpc_port()), ec);
    EXPECT_FALSE(ec) << ec.message();
}
#endif

}  // namespace
}  // namespace mooncake::testing
