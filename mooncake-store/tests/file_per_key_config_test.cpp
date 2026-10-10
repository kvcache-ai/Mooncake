#include "config/file_per_key_config.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <string>

#include "environ.h"

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
    }

    void TearDown() override { FLAGS_logtostderr = original_logtostderr_; }

    FilePerKeyConfig Load() const {
        return FilePerKeyConfig::FromEnvironment(Environ(source_));
    }

    MapEnvironSource source_;

   private:
    bool original_logtostderr_ = false;
};

TEST_F(FilePerKeyConfigTest, UnsetValuesKeepDefaults) {
    testing::internal::CaptureStderr();
    const auto config = Load();
    const auto diagnostics = testing::internal::GetCapturedStderr();

    EXPECT_EQ(config.fsdir, "file_per_key_dir");
    EXPECT_TRUE(config.enable_eviction);
    EXPECT_TRUE(config.Validate());
    EXPECT_TRUE(diagnostics.empty());
}

TEST_F(FilePerKeyConfigTest, ReadsDirectoryVerbatimWithoutValidation) {
    for (const char* value : {"", "   ", "relative/subdir", " /data "}) {
        SCOPED_TRACE(value);
        source_.Set("MOONCAKE_OFFLOAD_FSDIR", value);
        testing::internal::CaptureStderr();
        const auto config = Load();
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
            source_.Set(name, entry.value);
            testing::internal::CaptureStderr();
            const auto config = Load();
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
        source_.Unset(name);
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
        source_.Set("ENABLE_EVICTION", entry.legacy);
        source_.Set("MOONCAKE_OFFLOAD_ENABLE_EVICTION", entry.preferred);
        testing::internal::CaptureStderr();
        const auto config = Load();
        const auto diagnostics = testing::internal::GetCapturedStderr();

        EXPECT_EQ(config.enable_eviction, entry.enabled);
        EXPECT_TRUE(diagnostics.empty());
    }
}

TEST_F(FilePerKeyConfigTest, InvalidPreferredValueFallsBackToLegacyValue) {
    source_.Set("ENABLE_EVICTION", "false");
    for (const char* value : {"", "bad"}) {
        SCOPED_TRACE(value);
        source_.Set("MOONCAKE_OFFLOAD_ENABLE_EVICTION", value);
        testing::internal::CaptureStderr();
        const auto config = Load();
        const auto diagnostics = testing::internal::GetCapturedStderr();

        EXPECT_FALSE(config.enable_eviction);
        EXPECT_EQ(diagnostics,
                  std::string("[Mooncake] Warning: invalid value '") + value +
                      "' for env MOONCAKE_OFFLOAD_ENABLE_EVICTION, using "
                      "default 0\n");
    }
}

TEST_F(FilePerKeyConfigTest, InvalidLegacyValueWarnsEvenWithValidPreferred) {
    source_.Set("ENABLE_EVICTION", "bad");
    source_.Set("MOONCAKE_OFFLOAD_ENABLE_EVICTION", "false");
    testing::internal::CaptureStderr();
    const auto config = Load();
    const auto diagnostics = testing::internal::GetCapturedStderr();

    EXPECT_FALSE(config.enable_eviction);
    EXPECT_EQ(diagnostics,
              "[Mooncake] Warning: invalid value 'bad' for env "
              "ENABLE_EVICTION, using default 1\n");
}

TEST_F(FilePerKeyConfigTest, InvalidAliasesWarnInLegacyFirstOrder) {
    source_.Set("ENABLE_EVICTION", "bad");
    source_.Set("MOONCAKE_OFFLOAD_ENABLE_EVICTION", "bad");
    testing::internal::CaptureStderr();
    const auto config = Load();
    const auto diagnostics = testing::internal::GetCapturedStderr();

    EXPECT_TRUE(config.enable_eviction);
    EXPECT_EQ(diagnostics,
              "[Mooncake] Warning: invalid value 'bad' for env "
              "ENABLE_EVICTION, using default 1\n"
              "[Mooncake] Warning: invalid value 'bad' for env "
              "MOONCAKE_OFFLOAD_ENABLE_EVICTION, using default 1\n");
}

TEST_F(FilePerKeyConfigTest, NewConfigsReadCurrentEnvironment) {
    source_.Set("MOONCAKE_OFFLOAD_FSDIR", "first");
    source_.Set("ENABLE_EVICTION", "false");
    const auto first = Load();
    source_.Set("MOONCAKE_OFFLOAD_FSDIR", "second");
    source_.Set("MOONCAKE_OFFLOAD_ENABLE_EVICTION", "true");
    const auto second = Load();
    source_.Unset("MOONCAKE_OFFLOAD_FSDIR");
    source_.Unset("MOONCAKE_OFFLOAD_ENABLE_EVICTION");
    source_.Unset("ENABLE_EVICTION");
    const auto third = Load();

    EXPECT_EQ(first.fsdir, "first");
    EXPECT_FALSE(first.enable_eviction);
    EXPECT_EQ(second.fsdir, "second");
    EXPECT_TRUE(second.enable_eviction);
    EXPECT_EQ(third.fsdir, "file_per_key_dir");
    EXPECT_TRUE(third.enable_eviction);
}

}  // namespace
}  // namespace mooncake::test
