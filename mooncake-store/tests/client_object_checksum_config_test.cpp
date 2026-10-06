#include <gtest/gtest.h>

#include <cstdlib>
#include <mutex>
#include <optional>
#include <string>

#include "../src/config/client_object_checksum_config.h"

namespace mooncake {
namespace {

std::mutex environment_mutex;

class ScopedChecksumEnvironment {
   public:
    ScopedChecksumEnvironment() : environment_lock_(environment_mutex) {
        if (const char* value = std::getenv(kName)) {
            original_ = value;
        }
        EXPECT_EQ(unsetenv(kName), 0);
    }

    ~ScopedChecksumEnvironment() {
        if (original_) {
            EXPECT_EQ(setenv(kName, original_->c_str(), 1), 0);
        } else {
            EXPECT_EQ(unsetenv(kName), 0);
        }
    }

    ScopedChecksumEnvironment(const ScopedChecksumEnvironment&) = delete;
    ScopedChecksumEnvironment& operator=(const ScopedChecksumEnvironment&) =
        delete;

    void Set(const char* value) { ASSERT_EQ(setenv(kName, value, 1), 0); }

   private:
    static constexpr const char* kName = "MOONCAKE_STORE_CHECKSUM";
    std::unique_lock<std::mutex> environment_lock_;
    std::optional<std::string> original_;
};

TEST(ClientObjectChecksumConfigTest, UnsetDisablesChecksum) {
    ScopedChecksumEnvironment environment;

    EXPECT_FALSE(ClientObjectChecksumConfig::FromEnvironment().enabled);
}

TEST(ClientObjectChecksumConfigTest, AcceptsCanonicalBooleanValues) {
    ScopedChecksumEnvironment environment;

    for (const char* value : {"1", "true", "TRUE", " yes ", "on", "enable"}) {
        SCOPED_TRACE(value);
        environment.Set(value);
        EXPECT_TRUE(ClientObjectChecksumConfig::FromEnvironment().enabled);
    }

    for (const char* value :
         {"0", "false", "FALSE", " no ", "off", "disable"}) {
        SCOPED_TRACE(value);
        environment.Set(value);
        EXPECT_FALSE(ClientObjectChecksumConfig::FromEnvironment().enabled);
    }
}

TEST(ClientObjectChecksumConfigTest, InvalidValuesWarnAndUseDefault) {
    ScopedChecksumEnvironment environment;

    for (const char* value : {"", "2", "enabled", "1suffix"}) {
        SCOPED_TRACE(value);
        environment.Set(value);
        ::testing::internal::CaptureStderr();

        const auto config = ClientObjectChecksumConfig::FromEnvironment();
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
    ScopedChecksumEnvironment environment;

    environment.Set("1");
    EXPECT_TRUE(ClientObjectChecksumConfig::FromEnvironment().enabled);

    environment.Set("0");
    EXPECT_FALSE(ClientObjectChecksumConfig::FromEnvironment().enabled);
}

}  // namespace
}  // namespace mooncake
