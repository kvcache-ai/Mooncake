// Tests for common utility helpers: LoadEnv/LoadIntEnv/ParseLogLevel.

#include <gtest/gtest.h>

#include <cstdlib>

#include "conductor/common/utils.h"

namespace {

using conductor::common::LoadEnv;
using conductor::common::LoadIntEnv;
using conductor::common::LogLevel;
using conductor::common::ParseLogLevel;

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
    EXPECT_EQ(ParseLogLevel(), LogLevel::kInfo);
}

TEST(ParseLogLevel, ParsesAllLevelsCaseInsensitively) {
    EnvGuard guard("CONDUCTOR_LOG_LEVEL");
    guard.Set("DEBUG");
    EXPECT_EQ(ParseLogLevel(), LogLevel::kDebug);
    guard.Set("debug");
    EXPECT_EQ(ParseLogLevel(), LogLevel::kDebug);
    guard.Set("Info");
    EXPECT_EQ(ParseLogLevel(), LogLevel::kInfo);
    guard.Set("WARN");
    EXPECT_EQ(ParseLogLevel(), LogLevel::kWarn);
    guard.Set("error");
    EXPECT_EQ(ParseLogLevel(), LogLevel::kError);
}

TEST(ParseLogLevel, InvalidValueFallsBackToInfo) {
    EnvGuard guard("CONDUCTOR_LOG_LEVEL");
    guard.Set("verbose");
    EXPECT_EQ(ParseLogLevel(), LogLevel::kInfo);
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
