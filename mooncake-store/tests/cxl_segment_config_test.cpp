#include <gtest/gtest.h>

#include <limits>
#include <string>

#include "../src/config/cxl_segment_config.h"
#include "environ.h"

namespace mooncake {
namespace {

class CxlSegmentConfigTest : public ::testing::Test {
   protected:
    CxlSegmentConfig Load() const {
        return CxlSegmentConfig::FromEnvironment(Environ(source_));
    }

    void Set(const char* value) { source_.Set(kEnvironmentVariable, value); }

    static constexpr const char* kEnvironmentVariable = "MC_CXL_DEV_SIZE";

   private:
    MapEnvironSource source_;
};

TEST_F(CxlSegmentConfigTest, UnsetLeavesDeviceSizeAbsent) {
    EXPECT_FALSE(Load().device_size.has_value());
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
        EXPECT_EQ(Load().device_size, entry.expected);
    }

    const std::string maximum =
        std::to_string(std::numeric_limits<size_t>::max());
    Set(maximum.c_str());
    EXPECT_EQ(Load().device_size, std::numeric_limits<size_t>::max());
}

TEST_F(CxlSegmentConfigTest, PresentInvalidValuesResolveToZero) {
    for (const char* value :
         {"", " ", "-1", "4096bytes", "18446744073709551616"}) {
        SCOPED_TRACE(value);
        Set(value);
        const auto config = Load();
        ASSERT_TRUE(config.device_size.has_value());
        EXPECT_EQ(*config.device_size, 0);
    }
}

TEST_F(CxlSegmentConfigTest, EachConfigReadsCurrentEnvironment) {
    Set("4096");
    const auto first = Load();

    Set("8192");
    const auto second = Load();

    EXPECT_EQ(first.device_size, 4096);
    EXPECT_EQ(second.device_size, 8192);
}

}  // namespace
}  // namespace mooncake
