#include <gtest/gtest.h>

#include <cstdlib>
#include <chrono>
#include <optional>
#include <string>
#include <type_traits>
#include <utility>

#include "../src/config/nof_debug_config.h"

namespace mooncake {
namespace {

class ScopedEnvironment {
   public:
    explicit ScopedEnvironment(const char* name) : name_(name) {
        if (const char* value = std::getenv(name)) original_ = value;
        EXPECT_EQ(unsetenv(name), 0);
    }
    ~ScopedEnvironment() {
        if (original_) {
            EXPECT_EQ(setenv(name_.c_str(), original_->c_str(), 1), 0);
        } else {
            EXPECT_EQ(unsetenv(name_.c_str()), 0);
        }
    }
    void Set(const char* value) {
        ASSERT_EQ(setenv(name_.c_str(), value, 1), 0);
    }

   private:
    std::string name_;
    std::optional<std::string> original_;
};

TEST(NoFDebugConfigTest, DefaultsWhenUnset) {
    ScopedEnvironment enabled("MC_NOF_DEBUG");
    ScopedEnvironment interval("MC_NOF_DEBUG_INTERVAL_MS");
    const auto config = NoFDebugConfig::FromEnvironment();
    EXPECT_FALSE(config.enabled);
    EXPECT_EQ(config.interval_ms, std::chrono::milliseconds{1000});
}

TEST(NoFDebugConfigTest, ExposesTheIntervalAsMilliseconds) {
    EXPECT_TRUE((std::is_same_v<decltype(NoFDebugConfig{}.interval_ms),
                                std::chrono::milliseconds>));
}

TEST(NoFDebugConfigTest, EachSettingKeepsItsOwnFirstUseSnapshot) {
    ScopedEnvironment enabled("MC_NOF_DEBUG");
    ScopedEnvironment interval("MC_NOF_DEBUG_INTERVAL_MS");
    enabled.Set("yes");
    interval.Set("2");
    EXPECT_TRUE(NoFDebugConfig::IsEnabledAtFirstUse());
    enabled.Set("off");
    interval.Set("7");
    EXPECT_EQ(NoFDebugConfig::IntervalMsAtFirstUse(),
              std::chrono::milliseconds{7});
    interval.Set("9");
    EXPECT_TRUE(NoFDebugConfig::IsEnabledAtFirstUse());
    EXPECT_EQ(NoFDebugConfig::IntervalMsAtFirstUse(),
              std::chrono::milliseconds{7});
}

TEST(NoFDebugConfigTest, AcceptsOnlyLegacyTruthyTokens) {
    ScopedEnvironment enabled("MC_NOF_DEBUG");
    for (const char* value : {"1", "TRUE", "Yes", "On"}) {
        enabled.Set(value);
        EXPECT_TRUE(NoFDebugConfig::FromEnvironment().enabled) << value;
    }
    for (const char* value : {"", "0", " true ", "y", "off"}) {
        enabled.Set(value);
        EXPECT_FALSE(NoFDebugConfig::FromEnvironment().enabled) << value;
    }
}

TEST(NoFDebugConfigTest, ParsesLegacyIntervalWithoutTrimming) {
    ScopedEnvironment interval("MC_NOF_DEBUG_INTERVAL_MS");
    for (const auto& [value, expected] :
         {std::pair{"7", 7}, std::pair{"+7", 7}, std::pair{" 7", 7},
          std::pair{"17", 17}}) {
        interval.Set(value);
        EXPECT_EQ(NoFDebugConfig::FromEnvironment().interval_ms,
                  std::chrono::milliseconds{expected})
            << value;
    }
    for (const char* value : {"", "0", "-2", "7 ", "7ms", "bad"}) {
        interval.Set(value);
        EXPECT_EQ(NoFDebugConfig::FromEnvironment().interval_ms,
                  std::chrono::milliseconds{1000})
            << value;
    }
}

TEST(NoFDebugConfigTest, ReadsEachValueWhenRequested) {
    ScopedEnvironment enabled("MC_NOF_DEBUG");
    ScopedEnvironment interval("MC_NOF_DEBUG_INTERVAL_MS");
    enabled.Set("1");
    interval.Set("5");
    const auto first = NoFDebugConfig::FromEnvironment();
    EXPECT_TRUE(first.enabled);
    EXPECT_EQ(first.interval_ms, std::chrono::milliseconds{5});
    enabled.Set("0");
    interval.Set("9");
    const auto second = NoFDebugConfig::FromEnvironment();
    EXPECT_FALSE(second.enabled);
    EXPECT_EQ(second.interval_ms, std::chrono::milliseconds{9});
}

}  // namespace
}  // namespace mooncake
