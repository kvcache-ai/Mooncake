#include <gtest/gtest.h>

#include <cstdlib>
#include <optional>
#include <string>

#include "../src/config/client_object_checksum_config.h"
#include "environ.h"
#include "environ_test_peer.h"

namespace mooncake {
namespace {

constexpr char kName[] = "MOONCAKE_STORE_CHECKSUM";

ClientObjectChecksumConfig Load(const MapEnvironSource& source) {
    return ClientObjectChecksumConfig::FromEnvironment(Environ(source));
}

// IsEnabledAtFirstUse() reads the process environment, so tests of it must
// mutate the real environment and restore it afterwards.
class ScopedChecksumEnvironment {
   public:
    ScopedChecksumEnvironment() {
        if (const char* value = std::getenv(kName)) {
            original_ = value;
        }
        EXPECT_EQ(mooncake::test::EnvironTestPeer::UnsetEnv(kName), 0);
    }

    ~ScopedChecksumEnvironment() {
        if (original_) {
            EXPECT_EQ(mooncake::test::EnvironTestPeer::SetEnv(
                          kName, original_->c_str(), 1),
                      0);
        } else {
            EXPECT_EQ(mooncake::test::EnvironTestPeer::UnsetEnv(kName), 0);
        }
    }

    ScopedChecksumEnvironment(const ScopedChecksumEnvironment&) = delete;
    ScopedChecksumEnvironment& operator=(const ScopedChecksumEnvironment&) =
        delete;

    void Set(const char* value) {
        ASSERT_EQ(mooncake::test::EnvironTestPeer::SetEnv(kName, value, 1), 0);
    }

   private:
    std::optional<std::string> original_;
};

TEST(ClientObjectChecksumConfigTest, UnsetDisablesChecksum) {
    MapEnvironSource source;

    EXPECT_FALSE(Load(source).enabled);
}

TEST(ClientObjectChecksumConfigTest, AcceptsCanonicalBooleanValues) {
    MapEnvironSource source;

    for (const char* value : {"1", "true", "TRUE", " yes ", "on", "enable"}) {
        SCOPED_TRACE(value);
        source.Set(kName, value);
        EXPECT_TRUE(Load(source).enabled);
    }

    for (const char* value :
         {"0", "false", "FALSE", " no ", "off", "disable"}) {
        SCOPED_TRACE(value);
        source.Set(kName, value);
        EXPECT_FALSE(Load(source).enabled);
    }
}

TEST(ClientObjectChecksumConfigTest, InvalidValuesWarnAndUseDefault) {
    MapEnvironSource source;

    for (const char* value : {"", "2", "enabled", "1suffix"}) {
        SCOPED_TRACE(value);
        source.Set(kName, value);
        ::testing::internal::CaptureStderr();

        const auto config = Load(source);
        const std::string warning = ::testing::internal::GetCapturedStderr();

        EXPECT_FALSE(config.enabled);
        EXPECT_NE(warning.find("invalid value '" + std::string(value) +
                               "' for env MOONCAKE_STORE_CHECKSUM, using "
                               "default 0"),
                  std::string::npos);
    }
}

TEST(ClientObjectChecksumConfigTest, FirstUseValueIsProcessWide) {
    ScopedChecksumEnvironment environment;

    environment.Set("1");
    EXPECT_TRUE(ClientObjectChecksumConfig::IsEnabledAtFirstUse());

    environment.Set("0");
    EXPECT_TRUE(ClientObjectChecksumConfig::IsEnabledAtFirstUse());
}

TEST(ClientObjectChecksumConfigTest, LoaderReadsCurrentEnvironment) {
    MapEnvironSource source;

    source.Set(kName, "1");
    EXPECT_TRUE(Load(source).enabled);

    source.Set(kName, "0");
    EXPECT_FALSE(Load(source).enabled);
}

}  // namespace
}  // namespace mooncake
