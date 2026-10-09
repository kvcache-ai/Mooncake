// Tests for common utility helpers: LoadEnv/LoadIntEnv/ParseLogLevel.

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <cstdlib>

#include "conductor/common/utils.h"

namespace {

using mooncake::conductor::common::LoadEnv;
using mooncake::conductor::common::LoadIntEnv;
using mooncake::conductor::common::LogLevelConfig;
using mooncake::conductor::common::ParseLogLevel;

constexpr LogLevelConfig kDebug{google::GLOG_INFO, 1};
constexpr LogLevelConfig kInfo{google::GLOG_INFO, 0};
constexpr LogLevelConfig kWarn{google::GLOG_WARNING, 0};
constexpr LogLevelConfig kError{google::GLOG_ERROR, 0};

class EnvGuard {
   public:
    explicit EnvGuard(const char* name) : name_(name) { unsetenv(name); }
    ~EnvGuard() { unsetenv(name_); }
    void Set(const char* value) { setenv(name_, value, 1); }

   private:
    const char* name_;
};

TEST(ParseLogLevel, DefaultsToInfoWhenUnset) {
    EnvGuard guard("CONDUCTOR_LOG_LEVEL");
    EXPECT_EQ(ParseLogLevel(), kInfo);
}

TEST(ParseLogLevel, ParsesAllLevelsCaseInsensitively) {
    EnvGuard guard("CONDUCTOR_LOG_LEVEL");
    guard.Set("DEBUG");
    EXPECT_EQ(ParseLogLevel(), kDebug);
    guard.Set("debug");
    EXPECT_EQ(ParseLogLevel(), kDebug);
    guard.Set("Info");
    EXPECT_EQ(ParseLogLevel(), kInfo);
    guard.Set("WARN");
    EXPECT_EQ(ParseLogLevel(), kWarn);
    guard.Set("error");
    EXPECT_EQ(ParseLogLevel(), kError);
}

TEST(ParseLogLevel, InvalidValueFallsBackToInfo) {
    EnvGuard guard("CONDUCTOR_LOG_LEVEL");
    guard.Set("verbose");
    EXPECT_EQ(ParseLogLevel(), kInfo);
}

// DEBUG is the one level glog has no direct severity for: it shares INFO and
// is distinguished by FLAGS_v, so pin that mapping explicitly.
TEST(ParseLogLevel, DebugSharesInfoSeverityAndRaisesVerbosity) {
    EnvGuard guard("CONDUCTOR_LOG_LEVEL");
    guard.Set("DEBUG");
    const auto debug = ParseLogLevel();
    guard.Set("INFO");
    const auto info = ParseLogLevel();
    EXPECT_EQ(debug.min_severity, info.min_severity);
    EXPECT_GT(debug.verbosity, info.verbosity);
}

TEST(LoadEnv, ReturnsValueWhenSet) {
    EnvGuard guard("CONDUCTOR_TEST_STR");
    guard.Set("hello");
    EXPECT_EQ(LoadEnv("CONDUCTOR_TEST_STR", "default"), "hello");
}

TEST(LoadEnv, ReturnsDefaultWhenUnsetOrEmpty) {
    EnvGuard guard("CONDUCTOR_TEST_STR");
    EXPECT_EQ(LoadEnv("CONDUCTOR_TEST_STR", "default"), "default");
    guard.Set("");
    EXPECT_EQ(LoadEnv("CONDUCTOR_TEST_STR", "default"), "default");
}

TEST(LoadIntEnv, ReturnsParsedValue) {
    EnvGuard guard("CONDUCTOR_TEST_INT");
    guard.Set("42");
    EXPECT_EQ(LoadIntEnv("CONDUCTOR_TEST_INT", -1), 42);
    guard.Set("-7");
    EXPECT_EQ(LoadIntEnv("CONDUCTOR_TEST_INT", -1), -7);
}

TEST(LoadIntEnv, ReturnsDefaultOnUnsetOrInvalid) {
    EnvGuard guard("CONDUCTOR_TEST_INT");
    EXPECT_EQ(LoadIntEnv("CONDUCTOR_TEST_INT", 13333), 13333);
    guard.Set("not-a-number");
    EXPECT_EQ(LoadIntEnv("CONDUCTOR_TEST_INT", 13333), 13333);
    // Surrounding whitespace is rejected in numeric parsing.
    guard.Set(" 42");
    EXPECT_EQ(LoadIntEnv("CONDUCTOR_TEST_INT", 13333), 13333);
    guard.Set("42x");
    EXPECT_EQ(LoadIntEnv("CONDUCTOR_TEST_INT", 13333), 13333);
}

}  // namespace
