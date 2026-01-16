#include <gtest/gtest.h>
#include <cstdint>
#include <cstdlib>
#include <optional>
#include <string>

#include "common.h"

namespace {

using namespace mooncake;

const uint16_t kDefaultPort = getDefaultHandshakePort();

//------------------------------------------------------------------------------
// parseFromString<T>
//------------------------------------------------------------------------------

TEST(ParseFromString, ValidUint16) {
    auto result = parseFromString<uint16_t>("42");
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(*result, 42);
}

TEST(ParseFromString, InvalidAlpha) {
    auto result = parseFromString<uint16_t>("forty-two");
    EXPECT_FALSE(result.has_value());
}

TEST(ParseFromString, ValueOutOfRange) {
    // 70000 exceeds std::numeric_limits<uint16_t>::max() (65 535)
    auto result = parseFromString<uint16_t>("70000");
    EXPECT_FALSE(result.has_value());
}

//------------------------------------------------------------------------------
// getPortFromString
//------------------------------------------------------------------------------

TEST(GetPortFromString, ReturnsParsedPort) {
    EXPECT_EQ(getPortFromString("8080", kDefaultPort), 8080);
}

TEST(GetPortFromString, FallsBackToDefaultOnInvalid) {
    EXPECT_EQ(getPortFromString("not-a-number", kDefaultPort), kDefaultPort);
}

//------------------------------------------------------------------------------
// IPv6 helpers
//------------------------------------------------------------------------------

TEST(IsValidIpV6, RecognisesValidAddress) {
    EXPECT_TRUE(isValidIpV6("2001:0db8:85a3:0000:0000:8a2e:0370:7334"));
}

TEST(IsValidIpV6, RejectsInvalidAddress) {
    EXPECT_FALSE(isValidIpV6("not::an::ip"));
}

TEST(MaybeWrapIpV6, WrapsValidIp) {
    const std::string addr = "2001:db8::1";
    EXPECT_EQ(maybeWrapIpV6(addr), "[" + addr + "]");
}

TEST(MaybeWrapIpV6, LeavesInvalidUntouched) {
    const std::string addr = "example.com";
    EXPECT_EQ(maybeWrapIpV6(addr), addr);
}

//------------------------------------------------------------------------------
// parseHostNameWithPort
//------------------------------------------------------------------------------

TEST(ParseHostNameWithPort, BracketedIpv6WithPort) {
    auto [host, port] = parseHostNameWithPort("[2001:db8::1]:1234");
    EXPECT_EQ(host, "2001:db8::1");
    EXPECT_EQ(port, 1234);
}

TEST(ParseHostNameWithPort, BracketedIpv6NoPort) {
    auto [host, port] = parseHostNameWithPort("[2001:db8::1]");
    EXPECT_EQ(host, "2001:db8::1");
    EXPECT_EQ(port, kDefaultPort);
}

TEST(ParseHostNameWithPort, RawIpv6) {
    auto [host, port] = parseHostNameWithPort("2001:db8::1");
    EXPECT_EQ(host, "2001:db8::1");
    EXPECT_EQ(port, kDefaultPort);
}

TEST(ParseHostNameWithPort, Ipv4WithPort) {
    auto [host, port] = parseHostNameWithPort("8.8.8.8:4321");
    EXPECT_EQ(host, "8.8.8.8");
    EXPECT_EQ(port, 4321);
}

TEST(ParseHostNameWithPort, Ipv4WithoutPort) {
    auto [host, port] = parseHostNameWithPort("8.8.8.8");
    EXPECT_EQ(host, "8.8.8.8");
    EXPECT_EQ(port, kDefaultPort);
}

TEST(ParseHostNameWithPort, HostWithPort) {
    auto [host, port] = parseHostNameWithPort("example.com:4321");
    EXPECT_EQ(host, "example.com");
    EXPECT_EQ(port, 4321);
}

TEST(ParseHostNameWithPort, HostWithoutPort) {
    auto [host, port] = parseHostNameWithPort("example.com");
    EXPECT_EQ(host, "example.com");
    EXPECT_EQ(port, kDefaultPort);
}

//------------------------------------------------------------------------------
// parsePortAndDevice
//------------------------------------------------------------------------------

TEST(ParsePortAndDevice, PortOnly) {
    int dev = -1;
    EXPECT_EQ(parsePortAndDevice("8080", kDefaultPort, &dev), 8080);
    EXPECT_EQ(dev, -1);
}

TEST(ParsePortAndDevice, PortAndDevice) {
    int dev = -1;
    EXPECT_EQ(parsePortAndDevice("9000:npu_2", kDefaultPort, &dev), 9000);
    EXPECT_EQ(dev, 2);
}

TEST(ParsePortAndDevice, InvalidDeviceFormatKeepsDefault) {
    int dev = -1;
    EXPECT_EQ(parsePortAndDevice("9000:npu2", kDefaultPort, &dev), 9000);
    EXPECT_EQ(dev, -1);

    EXPECT_EQ(parsePortAndDevice("9000:_2", kDefaultPort, &dev), 9000);
    EXPECT_EQ(dev, -1);
}

TEST(ParsePortAndDevice, InvalidDeviceIdSetsToZero) {
    int dev = -1;
    EXPECT_EQ(parsePortAndDevice("9000:npu_x", kDefaultPort, &dev), 9000);
    EXPECT_EQ(dev, 0);
}

//------------------------------------------------------------------------------
// parseHostNameWithPortAscend
//------------------------------------------------------------------------------

TEST(ParseHostNameWithPortAscend, BracketedIpv6WithPort) {
    int dev = -1;
    auto [host, port] = parseHostNameWithPortAscend("[2001:db8::1]:1234", &dev);
    EXPECT_EQ(host, "2001:db8::1");
    EXPECT_EQ(port, 1234);
    EXPECT_EQ(dev, -1);
}

TEST(ParseHostNameWithPortAscend, BracketedIpv6NoPort) {
    int dev = -1;
    auto [host, port] = parseHostNameWithPortAscend("[2001:db8::1]", &dev);
    EXPECT_EQ(host, "2001:db8::1");
    EXPECT_EQ(port, kDefaultPort);
    EXPECT_EQ(dev, -1);
}

TEST(ParseHostNameWithPortAscend, RawIpv6) {
    int dev = -1;
    auto [host, port] = parseHostNameWithPortAscend("2001:db8::1", &dev);
    EXPECT_EQ(host, "2001:db8::1");
    EXPECT_EQ(port, kDefaultPort);
    EXPECT_EQ(dev, -1);
}

TEST(ParseHostNameWithPortAscend, Ipv4WithPort) {
    int dev = -1;
    auto [host, port] = parseHostNameWithPortAscend("8.8.8.8:4321", &dev);
    EXPECT_EQ(host, "8.8.8.8");
    EXPECT_EQ(port, 4321);
    EXPECT_EQ(dev, -1);
}

TEST(ParseHostNameWithPortAscend, Ipv4WithoutPort) {
    int dev = -1;
    auto [host, port] = parseHostNameWithPortAscend("8.8.8.8", &dev);
    EXPECT_EQ(host, "8.8.8.8");
    EXPECT_EQ(port, kDefaultPort);
    EXPECT_EQ(dev, -1);
}

TEST(ParseHostNameWithPortAscend, HostWithPort) {
    int dev = -1;
    auto [host, port] = parseHostNameWithPortAscend("example.com:4321", &dev);
    EXPECT_EQ(host, "example.com");
    EXPECT_EQ(port, 4321);
    EXPECT_EQ(dev, -1);
}

TEST(ParseHostNameWithPortAscend, HostWithoutPort) {
    int dev = -1;
    auto [host, port] = parseHostNameWithPortAscend("example.com", &dev);
    EXPECT_EQ(host, "example.com");
    EXPECT_EQ(port, kDefaultPort);
    EXPECT_EQ(dev, -1);
}

TEST(ParseHostNameWithPortAscend, BracketedIpv6PortDevice) {
    int dev = -1;
    auto [host, port] =
        parseHostNameWithPortAscend("[2001:db8::1]:8080:npu_3", &dev);
    EXPECT_EQ(host, "2001:db8::1");
    EXPECT_EQ(port, 8080);
    EXPECT_EQ(dev, 3);
}

TEST(ParseHostNameWithPortAscend, Ipv4PortDevice) {
    int dev = -1;
    auto [host, port] = parseHostNameWithPortAscend("8.8.8.8:8080:npu_1", &dev);
    EXPECT_EQ(host, "8.8.8.8");
    EXPECT_EQ(port, 8080);
    EXPECT_EQ(dev, 1);
}

TEST(ParseHostNameWithPortAscend, Ipv4PortInvalidDevice) {
    int dev = -1;
    auto [host, port] = parseHostNameWithPortAscend("8.8.8.8:8080:npu1", &dev);
    EXPECT_EQ(host, "8.8.8.8");
    EXPECT_EQ(port, 8080);
    EXPECT_EQ(dev, -1);
}

TEST(ParseHostNameWithPortAscend, HostPortDevice) {
    int dev = -1;
    auto [host, port] =
        parseHostNameWithPortAscend("example.com:8080:npu_1", &dev);
    EXPECT_EQ(host, "example.com");
    EXPECT_EQ(port, 8080);
    EXPECT_EQ(dev, 1);
}

//------------------------------------------------------------------------------
// getHandshakeMaxLength
//------------------------------------------------------------------------------

class GetHandshakeMaxLengthTest : public ::testing::Test {
   protected:
    void SetUp() override {
        // Reset cached value before each test
        resetHandshakeMaxLength();
        // Clear the environment variable
        unsetenv("MC_HANDSHAKE_MAX_LENGTH");
    }

    void TearDown() override {
        // Clean up after each test
        resetHandshakeMaxLength();
        unsetenv("MC_HANDSHAKE_MAX_LENGTH");
    }
};

TEST_F(GetHandshakeMaxLengthTest, ReturnsDefaultWhenEnvNotSet) {
    // Default is 1MB (1 << 20 = 1048576 bytes)
    size_t expected_default = 1ull << 20;
    EXPECT_EQ(getHandshakeMaxLength(), expected_default);
}

TEST_F(GetHandshakeMaxLengthTest, ReturnsCachedValue) {
    // Verify that multiple calls return the same cached value
    size_t first_call = getHandshakeMaxLength();
    size_t second_call = getHandshakeMaxLength();
    EXPECT_EQ(first_call, second_call);
}

TEST_F(GetHandshakeMaxLengthTest, ReadsCustomValueFromEnv) {
    // Set to 4MB
    setenv("MC_HANDSHAKE_MAX_LENGTH", "4194304", 1);
    EXPECT_EQ(getHandshakeMaxLength(), 4194304);
}

TEST_F(GetHandshakeMaxLengthTest, RejectsValueTooSmall) {
    // Value below 1KB should use default
    setenv("MC_HANDSHAKE_MAX_LENGTH", "512", 1);
    size_t expected_default = 1ull << 20;
    EXPECT_EQ(getHandshakeMaxLength(), expected_default);
}

TEST_F(GetHandshakeMaxLengthTest, RejectsValueTooLarge) {
    // Value above 1GB should use default
    setenv("MC_HANDSHAKE_MAX_LENGTH", "2147483648", 1);  // 2GB
    size_t expected_default = 1ull << 20;
    EXPECT_EQ(getHandshakeMaxLength(), expected_default);
}

TEST_F(GetHandshakeMaxLengthTest, AcceptsMinimumValidValue) {
    // Minimum valid value is 1KB
    setenv("MC_HANDSHAKE_MAX_LENGTH", "1024", 1);
    EXPECT_EQ(getHandshakeMaxLength(), 1024);
}

TEST_F(GetHandshakeMaxLengthTest, AcceptsMaximumValidValue) {
    // Maximum valid value is 1GB
    setenv("MC_HANDSHAKE_MAX_LENGTH", "1073741824", 1);
    EXPECT_EQ(getHandshakeMaxLength(), 1073741824);
}

TEST_F(GetHandshakeMaxLengthTest, RejectsInvalidString) {
    // Invalid string should use default
    setenv("MC_HANDSHAKE_MAX_LENGTH", "not_a_number", 1);
    size_t expected_default = 1ull << 20;
    EXPECT_EQ(getHandshakeMaxLength(), expected_default);
}

}  // namespace
