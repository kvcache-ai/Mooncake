#include <gtest/gtest.h>

#include <cstdlib>
#include <chrono>
#include <optional>
#include <string>
#include <type_traits>
#include <utility>

#include "../src/config/nof_debug_config.h"
#include "environ.h"

namespace mooncake {
namespace {

// Only for the *AtFirstUse() snapshot test, which reads the process
// environment.
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

class NoFDebugConfigTest : public ::testing::Test {
   protected:
    NoFDebugConfig Load() const {
        return NoFDebugConfig::FromEnvironment(Environ(source_));
    }

    MapEnvironSource source_;
};

TEST_F(NoFDebugConfigTest, DefaultsWhenUnset) {
    const auto config = Load();
    EXPECT_FALSE(config.enabled);
    EXPECT_EQ(config.interval_ms, std::chrono::milliseconds{1000});
}

TEST_F(NoFDebugConfigTest, ExposesTheIntervalAsMilliseconds) {
    EXPECT_TRUE((std::is_same_v<decltype(NoFDebugConfig{}.interval_ms),
                                std::chrono::milliseconds>));
}

TEST_F(NoFDebugConfigTest, EachSettingKeepsItsOwnFirstUseSnapshot) {
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

TEST_F(NoFDebugConfigTest, AcceptsOnlyLegacyTruthyTokens) {
    for (const char* value : {"1", "TRUE", "Yes", "On"}) {
        source_.Set("MC_NOF_DEBUG", value);
        EXPECT_TRUE(Load().enabled) << value;
    }
    for (const char* value : {"", "0", " true ", "y", "off"}) {
        source_.Set("MC_NOF_DEBUG", value);
        EXPECT_FALSE(Load().enabled) << value;
    }
}

TEST_F(NoFDebugConfigTest, ParsesLegacyIntervalWithoutTrimming) {
    for (const auto& [value, expected] :
         {std::pair{"7", 7}, std::pair{"+7", 7}, std::pair{" 7", 7},
          std::pair{"17", 17}}) {
        source_.Set("MC_NOF_DEBUG_INTERVAL_MS", value);
        EXPECT_EQ(Load().interval_ms, std::chrono::milliseconds{expected})
            << value;
    }
    for (const char* value : {"", "0", "-2", "7 ", "7ms", "bad"}) {
        source_.Set("MC_NOF_DEBUG_INTERVAL_MS", value);
        EXPECT_EQ(Load().interval_ms, std::chrono::milliseconds{1000}) << value;
    }
}

TEST_F(NoFDebugConfigTest, ReadsEachValueWhenRequested) {
    source_.Set("MC_NOF_DEBUG", "1");
    source_.Set("MC_NOF_DEBUG_INTERVAL_MS", "5");
    const auto first = Load();
    EXPECT_TRUE(first.enabled);
    EXPECT_EQ(first.interval_ms, std::chrono::milliseconds{5});
    source_.Set("MC_NOF_DEBUG", "0");
    source_.Set("MC_NOF_DEBUG_INTERVAL_MS", "9");
    const auto second = Load();
    EXPECT_FALSE(second.enabled);
    EXPECT_EQ(second.interval_ms, std::chrono::milliseconds{9});
}

}  // namespace
}  // namespace mooncake
