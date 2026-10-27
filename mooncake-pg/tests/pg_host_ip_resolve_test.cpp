#include <optional>
#include <string>

#include <gtest/gtest.h>

#include "pg_host_ip_resolve.h"

namespace mooncake {
namespace {

TEST(PgHostIpResolveTest, PreservesSingleNodeLoopbackDefault) {
    int toward_calls = 0;
    int hostname_calls = 0;
    HostIpResolveHooks hooks;
    hooks.local_ip_toward_peer = [&](const std::string&) {
        ++toward_calls;
        return std::optional<std::string>{"10.0.0.8"};
    };
    hooks.local_ip_from_hostname = [&]() {
        ++hostname_calls;
        return std::optional<std::string>{"10.0.0.9"};
    };

    // Torchrun single-node: MASTER_ADDR is loopback.
    auto result =
        ResolveRoutableHostIp("127.0.0.1", "127.0.0.1", hooks);
    EXPECT_EQ(result.outcome, HostIpResolveOutcome::Unchanged);
    EXPECT_EQ(result.host_ip, "127.0.0.1");
    EXPECT_EQ(toward_calls, 0);
    EXPECT_EQ(hostname_calls, 0);

    // MASTER_ADDR unset.
    result = ResolveRoutableHostIp("127.0.0.1", "", hooks);
    EXPECT_EQ(result.outcome, HostIpResolveOutcome::Unchanged);
    EXPECT_EQ(result.host_ip, "127.0.0.1");
    EXPECT_EQ(toward_calls, 0);
    EXPECT_EQ(hostname_calls, 0);
}

TEST(PgHostIpResolveTest, PrefersRouteTowardMasterAddr) {
    int hostname_calls = 0;
    HostIpResolveHooks hooks;
    hooks.local_ip_toward_peer = [](const std::string& peer) {
        EXPECT_EQ(peer, "192.168.1.10");
        return std::optional<std::string>{"192.168.1.20"};
    };
    hooks.local_ip_from_hostname = [&]() {
        ++hostname_calls;
        return std::optional<std::string>{"10.0.0.1"};
    };

    auto result =
        ResolveRoutableHostIp("127.0.0.1", "192.168.1.10", hooks);
    EXPECT_EQ(result.outcome, HostIpResolveOutcome::Resolved);
    EXPECT_EQ(result.host_ip, "192.168.1.20");
    EXPECT_EQ(hostname_calls, 0);
}

TEST(PgHostIpResolveTest, HostnameFallbackWhenRouteUnavailable) {
    HostIpResolveHooks hooks;
    hooks.local_ip_toward_peer = [](const std::string&) {
        return std::nullopt;
    };
    hooks.local_ip_from_hostname = []() {
        return std::optional<std::string>{"10.1.2.3"};
    };

    auto result =
        ResolveRoutableHostIp("127.0.0.1", "10.9.8.7", hooks);
    EXPECT_EQ(result.outcome, HostIpResolveOutcome::Resolved);
    EXPECT_EQ(result.host_ip, "10.1.2.3");
}

TEST(PgHostIpResolveTest, ExplicitHostIpTakesPrecedence) {
    int toward_calls = 0;
    HostIpResolveHooks hooks;
    hooks.local_ip_toward_peer = [&](const std::string&) {
        ++toward_calls;
        return std::optional<std::string>{"192.168.0.2"};
    };

    // set_host_ip() already replaced the default before init.
    auto result =
        ResolveRoutableHostIp("10.0.0.5", "192.168.1.10", hooks);
    EXPECT_EQ(result.outcome, HostIpResolveOutcome::Unchanged);
    EXPECT_EQ(result.host_ip, "10.0.0.5");
    EXPECT_EQ(toward_calls, 0);
}

TEST(PgHostIpResolveTest, FailFastWhenMultiNodeCannotResolve) {
    HostIpResolveHooks hooks;
    hooks.local_ip_toward_peer = [](const std::string&) {
        return std::nullopt;
    };
    hooks.local_ip_from_hostname = []() { return std::nullopt; };

    auto result =
        ResolveRoutableHostIp("127.0.0.1", "203.0.113.1", hooks);
    EXPECT_EQ(result.outcome, HostIpResolveOutcome::FailFast);
    EXPECT_EQ(result.host_ip, "127.0.0.1");
    EXPECT_NE(result.error_message.find("set_host_ip"), std::string::npos);
    EXPECT_NE(result.error_message.find("203.0.113.1"), std::string::npos);
}

}  // namespace
}  // namespace mooncake
