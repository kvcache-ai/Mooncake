// Copyright 2025 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <gtest/gtest.h>

#include "tent/common/utils/ip.h"

namespace mooncake {
namespace tent {
namespace {

TEST(TentIpUtilsTest, ParsesBracketedIpv6WithPort) {
    auto [host, port] = parseHostNameWithPort("[2001:db8::1]:9000", 1234);

    EXPECT_EQ(host, "2001:db8::1");
    EXPECT_EQ(port, 9000);
}

TEST(TentIpUtilsTest, ParsesBracketedIpv6WithoutPort) {
    auto [host, port] = parseHostNameWithPort("[2001:db8::1]", 1234);

    EXPECT_EQ(host, "2001:db8::1");
    EXPECT_EQ(port, 1234);
}

TEST(TentIpUtilsTest, ParsesRawIpv6WithoutPort) {
    auto [host, port] = parseHostNameWithPort("2001:db8::1", 1234);

    EXPECT_EQ(host, "2001:db8::1");
    EXPECT_EQ(port, 1234);
}

TEST(TentIpUtilsTest, ParsesIpv4WithPort) {
    auto [host, port] = parseHostNameWithPort("192.168.1.10:9000", 1234);

    EXPECT_EQ(host, "192.168.1.10");
    EXPECT_EQ(port, 9000);
}

TEST(TentIpUtilsTest, ParsesHostnameWithPort) {
    auto [host, port] = parseHostNameWithPort("example.com:9000", 1234);

    EXPECT_EQ(host, "example.com");
    EXPECT_EQ(port, 9000);
}

TEST(TentIpUtilsTest, InvalidBracketedIpv6UsesDefaultPort) {
    auto [host, port] = parseHostNameWithPort("[2001::db8::1]:9000", 1234);

    EXPECT_TRUE(host.empty());
    EXPECT_EQ(port, 1234);
}

TEST(TentIpUtilsTest, InvalidRawIpv6UsesDefaultPort) {
    auto [host, port] = parseHostNameWithPort("2001::db8::1", 1234);

    EXPECT_TRUE(host.empty());
    EXPECT_EQ(port, 1234);
}

TEST(TentIpUtilsTest, BracketedIpv6WithInvalidPortUsesDefaultPort) {
    auto [host, port] = parseHostNameWithPort("[2001:db8::1]:bad", 1234);

    EXPECT_EQ(host, "2001:db8::1");
    EXPECT_EQ(port, 1234);
}

TEST(TentIpUtilsTest, EmptyInputUsesDefaultPort) {
    auto [host, port] = parseHostNameWithPort("", 1234);

    EXPECT_TRUE(host.empty());
    EXPECT_EQ(port, 1234);
}

TEST(TentIpUtilsTest, BuildsBracketedIpv6Endpoint) {
    EXPECT_EQ(buildIpAddrWithPort("2001:db8::1", 9000, true),
              "[2001:db8::1]:9000");
}

TEST(TentIpUtilsTest, ParsesScopeIdWithoutPort) {
    auto [host, port] =
        parseHostNameWithPort("fe80::a236:bcff:fecb:a1be%eno2", 1234);

    EXPECT_EQ(host, "fe80::a236:bcff:fecb:a1be%eno2");
    EXPECT_EQ(port, 1234);
}

TEST(TentIpUtilsTest, ParsesScopeIdWithPort) {
    auto [host, port] =
        parseHostNameWithPort("fe80::a236:bcff:fecb:a1be%eno2:15773", 1234);

    EXPECT_EQ(host, "fe80::a236:bcff:fecb:a1be%eno2");
    EXPECT_EQ(port, 15773);
}

TEST(TentIpUtilsTest, ParsesBracketedScopeIdWithPort) {
    auto [host, port] =
        parseHostNameWithPort("[fe80::a236:bcff:fecb:a1be%eno2]:15773", 1234);

    EXPECT_EQ(host, "fe80::a236:bcff:fecb:a1be%eno2");
    EXPECT_EQ(port, 15773);
}

TEST(TentIpUtilsTest, ScopeIdWithInvalidPortUsesDefault) {
    auto [host, port] =
        parseHostNameWithPort("fe80::a236:bcff:fecb:a1be%eno2:bad", 1234);

    EXPECT_EQ(host, "fe80::a236:bcff:fecb:a1be%eno2");
    EXPECT_EQ(port, 1234);
}

}  // namespace
}  // namespace tent
}  // namespace mooncake
