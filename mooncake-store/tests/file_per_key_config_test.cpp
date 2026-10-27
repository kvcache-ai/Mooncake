#include "config/file_per_key_config.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <array>
#include <cstdlib>
#include <optional>
#include <string>

namespace mooncake::test {
namespace {

class FilePerKeyConfigTest : public ::testing::Test {
   protected:
    static void SetUpTestSuite() {
        google::InitGoogleLogging("FilePerKeyConfigTest");
    }

    static void TearDownTestSuite() { google::ShutdownGoogleLogging(); }

    void SetUp() override {
        original_logtostderr_ = FLAGS_logtostderr;
        FLAGS_logtostderr = true;
        for (size_t i = 0; i < kVariables.size(); ++i) {
            if (const char* value = std::getenv(kVariables[i])) {
                original_[i] = value;
            }
        }
        for (const char* name : kVariables) {
            ASSERT_EQ(unsetenv(name), 0);
        }
    }

    void TearDown() override {
        for (size_t i = 0; i < kVariables.size(); ++i) {
            if (original_[i].has_value()) {
                EXPECT_EQ(setenv(kVariables[i], original_[i]->c_str(), 1), 0);
            } else {
                EXPECT_EQ(unsetenv(kVariables[i]), 0);
            }
        }
        FLAGS_logtostderr = original_logtostderr_;
    }

   private:
    inline static constexpr std::array<const char*, 3> kVariables = {
        "MOONCAKE_OFFLOAD_FSDIR", "MOONCAKE_OFFLOAD_ENABLE_EVICTION",
        "ENABLE_EVICTION"};
    std::array<std::optional<std::string>, 3> original_;
    bool original_logtostderr_ = false;
};

TEST_F(FilePerKeyConfigTest, UnsetValuesKeepDefaults) {
    testing::internal::CaptureStderr();
    const auto config = FilePerKeyConfig::FromEnvironment();
    const auto diagnostics = testing::internal::GetCapturedStderr();

    EXPECT_EQ(config.fsdir, "file_per_key_dir");
    EXPECT_TRUE(config.enable_eviction);
    EXPECT_TRUE(config.Validate());
    EXPECT_TRUE(diagnostics.empty());
}

TEST_F(FilePerKeyConfigTest, ReadsDirectoryVerbatimWithoutValidation) {
    for (const char* value : {"", "   ", "relative/subdir", " /data "}) {
        SCOPED_TRACE(value);
        ASSERT_EQ(setenv("MOONCAKE_OFFLOAD_FSDIR", value, 1), 0);
        testing::internal::CaptureStderr();
        const auto config = FilePerKeyConfig::FromEnvironment();
        const auto diagnostics = testing::internal::GetCapturedStderr();

        EXPECT_EQ(config.fsdir, value);
        EXPECT_TRUE(diagnostics.empty());
    }
}

TEST_F(FilePerKeyConfigTest, ValidateRejectsOnlyEmptyDirectory) {
    FilePerKeyConfig config;
    config.fsdir = "";
    testing::internal::CaptureStderr();
    const bool valid = config.Validate();
    const auto diagnostics = testing::internal::GetCapturedStderr();

    EXPECT_FALSE(valid);
    EXPECT_NE(diagnostics.find("FilePerKeyConfig: fsdir is invalid"),
              std::string::npos);
    for (const char* value : {"   ", "relative/subdir", " /data "}) {
        SCOPED_TRACE(value);
        config.fsdir = value;
        EXPECT_TRUE(config.Validate());
    }
}

TEST_F(FilePerKeyConfigTest, BothEvictionNamesKeepBoolParsingAndWarnings) {
    struct Case {
        const char* value;
        bool enabled;
        bool warning;
    };
    const Case cases[] = {
        {"1", true, false},
        {"0", false, false},
        {"true", true, false},
        {"FALSE", false, false},
        {"Yes", true, false},
        {"nO", false, false},
        {"on", true, false},
        {"OFF", false, false},
        {"enable", true, false},
        {"DISABLE", false, false},
        {" \tfalse\r\n", false, false},
        {"", true, true},
        {"   ", true, true},
        {"2", true, true},
        {"-1", true, true},
        {"999999999999999999999999", true, true},
        {"falsex", true, true},
        {"invalid", true, true},
    };
    for (const char* name :
         {"MOONCAKE_OFFLOAD_ENABLE_EVICTION", "ENABLE_EVICTION"}) {
        SCOPED_TRACE(name);
        for (const auto& entry : cases) {
            SCOPED_TRACE(entry.value);
            ASSERT_EQ(setenv(name, entry.value, 1), 0);
            testing::internal::CaptureStderr();
            const auto config = FilePerKeyConfig::FromEnvironment();
            const auto diagnostics = testing::internal::GetCapturedStderr();

            EXPECT_EQ(config.enable_eviction, entry.enabled);
            const std::string expected =
                entry.warning
                    ? std::string("[Mooncake] Warning: invalid value '") +
                          entry.value + "' for env " + name +
                          ", using default 1\n"
                    : "";
            EXPECT_EQ(diagnostics, expected);
        }
        ASSERT_EQ(unsetenv(name), 0);
    }
}

TEST_F(FilePerKeyConfigTest, PreferredEvictionNameOverridesLegacyValue) {
    struct Case {
        const char* legacy;
        const char* preferred;
        bool enabled;
    };
    const Case cases[] = {{"true", "false", false}, {"false", "true", true}};
    for (const auto& entry : cases) {
        SCOPED_TRACE(entry.preferred);
        ASSERT_EQ(setenv("ENABLE_EVICTION", entry.legacy, 1), 0);
        ASSERT_EQ(
            setenv("MOONCAKE_OFFLOAD_ENABLE_EVICTION", entry.preferred, 1), 0);
        testing::internal::CaptureStderr();
        const auto config = FilePerKeyConfig::FromEnvironment();
        const auto diagnostics = testing::internal::GetCapturedStderr();

        EXPECT_EQ(config.enable_eviction, entry.enabled);
        EXPECT_TRUE(diagnostics.empty());
    }
}

TEST_F(FilePerKeyConfigTest, InvalidPreferredValueFallsBackToLegacyValue) {
    ASSERT_EQ(setenv("ENABLE_EVICTION", "false", 1), 0);
    for (const char* value : {"", "bad"}) {
        SCOPED_TRACE(value);
        ASSERT_EQ(setenv("MOONCAKE_OFFLOAD_ENABLE_EVICTION", value, 1), 0);
        testing::internal::CaptureStderr();
        const auto config = FilePerKeyConfig::FromEnvironment();
        const auto diagnostics = testing::internal::GetCapturedStderr();

        EXPECT_FALSE(config.enable_eviction);
        EXPECT_EQ(diagnostics,
                  std::string("[Mooncake] Warning: invalid value '") + value +
                      "' for env MOONCAKE_OFFLOAD_ENABLE_EVICTION, using "
                      "default 0\n");
    }
}

TEST_F(FilePerKeyConfigTest, InvalidLegacyValueWarnsEvenWithValidPreferred) {
    ASSERT_EQ(setenv("ENABLE_EVICTION", "bad", 1), 0);
    ASSERT_EQ(setenv("MOONCAKE_OFFLOAD_ENABLE_EVICTION", "false", 1), 0);
    testing::internal::CaptureStderr();
    const auto config = FilePerKeyConfig::FromEnvironment();
    const auto diagnostics = testing::internal::GetCapturedStderr();

    EXPECT_FALSE(config.enable_eviction);
    EXPECT_EQ(diagnostics,
              "[Mooncake] Warning: invalid value 'bad' for env "
              "ENABLE_EVICTION, using default 1\n");
}

TEST_F(FilePerKeyConfigTest, InvalidAliasesWarnInLegacyFirstOrder) {
    ASSERT_EQ(setenv("ENABLE_EVICTION", "bad", 1), 0);
    ASSERT_EQ(setenv("MOONCAKE_OFFLOAD_ENABLE_EVICTION", "bad", 1), 0);
    testing::internal::CaptureStderr();
    const auto config = FilePerKeyConfig::FromEnvironment();
    const auto diagnostics = testing::internal::GetCapturedStderr();

    EXPECT_TRUE(config.enable_eviction);
    EXPECT_EQ(diagnostics,
              "[Mooncake] Warning: invalid value 'bad' for env "
              "ENABLE_EVICTION, using default 1\n"
              "[Mooncake] Warning: invalid value 'bad' for env "
              "MOONCAKE_OFFLOAD_ENABLE_EVICTION, using default 1\n");
}

TEST_F(FilePerKeyConfigTest, NewConfigsReadCurrentEnvironment) {
    ASSERT_EQ(setenv("MOONCAKE_OFFLOAD_FSDIR", "first", 1), 0);
    ASSERT_EQ(setenv("ENABLE_EVICTION", "false", 1), 0);
    const auto first = FilePerKeyConfig::FromEnvironment();
    ASSERT_EQ(setenv("MOONCAKE_OFFLOAD_FSDIR", "second", 1), 0);
    ASSERT_EQ(setenv("MOONCAKE_OFFLOAD_ENABLE_EVICTION", "true", 1), 0);
    const auto second = FilePerKeyConfig::FromEnvironment();
    ASSERT_EQ(unsetenv("MOONCAKE_OFFLOAD_FSDIR"), 0);
    ASSERT_EQ(unsetenv("MOONCAKE_OFFLOAD_ENABLE_EVICTION"), 0);
    ASSERT_EQ(unsetenv("ENABLE_EVICTION"), 0);
    const auto third = FilePerKeyConfig::FromEnvironment();

    EXPECT_EQ(first.fsdir, "first");
    EXPECT_FALSE(first.enable_eviction);
    EXPECT_EQ(second.fsdir, "second");
    EXPECT_TRUE(second.enable_eviction);
    EXPECT_EQ(third.fsdir, "file_per_key_dir");
    EXPECT_TRUE(third.enable_eviction);
}

}  // namespace
}  // namespace mooncake::test
