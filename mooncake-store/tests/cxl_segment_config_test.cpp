#include <gtest/gtest.h>

#include <cstdlib>
#include <limits>
#include <optional>
#include <string>

#include "../src/config/cxl_segment_config.h"

namespace mooncake {
namespace {

class CxlSegmentConfigTest : public ::testing::Test {
   protected:
    void SetUp() override {
        if (const char* value = std::getenv(kEnvironmentVariable)) {
            original_ = value;
        }
        ASSERT_EQ(unsetenv(kEnvironmentVariable), 0);
    }

    void TearDown() override {
        if (original_.has_value()) {
            EXPECT_EQ(setenv(kEnvironmentVariable, original_->c_str(), 1), 0);
        } else {
            EXPECT_EQ(unsetenv(kEnvironmentVariable), 0);
        }
    }

    void Set(const char* value) {
        ASSERT_EQ(setenv(kEnvironmentVariable, value, 1), 0);
    }

    static constexpr const char* kEnvironmentVariable = "MC_CXL_DEV_SIZE";

   private:
    std::optional<std::string> original_;
};

TEST_F(CxlSegmentConfigTest, UnsetLeavesDeviceSizeAbsent) {
    EXPECT_FALSE(CxlSegmentConfig::FromEnvironment().device_size.has_value());
}

TEST_F(CxlSegmentConfigTest, PreservesAcceptedSizeSyntax) {
    struct Case {
        const char* value;
        size_t expected;
    };
    const Case cases[] = {
        {"0", 0},
        {"4096", 4096},
        {" +4096 ", 4096},
    };

    for (const auto& entry : cases) {
        SCOPED_TRACE(entry.value);
        Set(entry.value);
        EXPECT_EQ(CxlSegmentConfig::FromEnvironment().device_size,
                  entry.expected);
    }

    const std::string maximum =
        std::to_string(std::numeric_limits<size_t>::max());
    Set(maximum.c_str());
    EXPECT_EQ(CxlSegmentConfig::FromEnvironment().device_size,
              std::numeric_limits<size_t>::max());
}

TEST_F(CxlSegmentConfigTest, PresentInvalidValuesResolveToZero) {
    for (const char* value :
         {"", " ", "-1", "4096bytes", "18446744073709551616"}) {
        SCOPED_TRACE(value);
        Set(value);
        const auto config = CxlSegmentConfig::FromEnvironment();
        ASSERT_TRUE(config.device_size.has_value());
        EXPECT_EQ(*config.device_size, 0);
    }
}

TEST_F(CxlSegmentConfigTest, EachConfigReadsCurrentEnvironment) {
    Set("4096");
    const auto first = CxlSegmentConfig::FromEnvironment();

    Set("8192");
    const auto second = CxlSegmentConfig::FromEnvironment();

    EXPECT_EQ(first.device_size, 4096);
    EXPECT_EQ(second.device_size, 8192);
}

}  // namespace
}  // namespace mooncake
