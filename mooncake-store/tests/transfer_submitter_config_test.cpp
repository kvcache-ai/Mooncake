#include <glog/logging.h>
#include <gtest/gtest.h>

#include <optional>
#include <string>

#include "../src/config/transfer_submitter_config.h"
#include "environ.h"

namespace mooncake {
namespace {

class TransferSubmitterConfigTest : public ::testing::Test {
   protected:
    void SetUp() override {
        google::InitGoogleLogging("TransferSubmitterConfigTest");
        original_logtostderr_ = FLAGS_logtostderr;
        FLAGS_logtostderr = true;
    }

    void TearDown() override {
        FLAGS_logtostderr = original_logtostderr_;
        google::ShutdownGoogleLogging();
    }

    TransferSubmitterConfig Load() const {
        return TransferSubmitterConfig::FromEnvironment(Environ(source_));
    }

    MapEnvironSource source_;

   private:
    bool original_logtostderr_ = false;
};

TEST_F(TransferSubmitterConfigTest, UnsetMeansAutomatic) {
    ::testing::internal::CaptureStderr();
    const auto config = Load();
    const auto diagnostics = ::testing::internal::GetCapturedStderr();

    EXPECT_FALSE(config.memcpy_enabled_override.has_value());
    EXPECT_TRUE(diagnostics.empty()) << diagnostics;
}

TEST_F(TransferSubmitterConfigTest, PreservesLegacyTokensAndFallbacks) {
    struct Case {
        const char* value;
        bool expected;
        bool expect_warning;
    };
    const Case cases[] = {
        {"1", true, false},        {"true", true, false},
        {"TRUE", true, false},     {"yes", true, false},
        {"YeS", true, false},      {"on", true, false},
        {"On", true, false},       {"0", false, false},
        {"false", false, false},   {"FALSE", false, false},
        {"no", false, false},      {"nO", false, false},
        {"off", false, false},     {"Off", false, false},
        {"", true, true},          {" true", true, true},
        {"false ", true, true},    {"\t0\n", true, true},
        {"enable", true, true},    {"disable", true, true},
        {"ENABLE", true, true},    {"DISABLE", true, true},
        {"2", true, true},         {"-1", true, true},
        {"+0", true, true},        {"00", true, true},
        {"falsejunk", true, true}, {"invalid", true, true},
    };

    for (const auto& test_case : cases) {
        SCOPED_TRACE(test_case.value);
        source_.Set("MC_STORE_MEMCPY", test_case.value);
        ::testing::internal::CaptureStderr();
        const auto config = Load();
        const auto diagnostics = ::testing::internal::GetCapturedStderr();

        EXPECT_EQ(config.memcpy_enabled_override,
                  std::optional<bool>{test_case.expected});
        if (test_case.expect_warning) {
            EXPECT_NE(diagnostics.find("Invalid value for MC_STORE_MEMCPY:"),
                      std::string::npos);
        } else {
            EXPECT_TRUE(diagnostics.empty()) << diagnostics;
        }
    }
}

TEST_F(TransferSubmitterConfigTest, InvalidValueWarningUsesLowercaseValue) {
    source_.Set("MC_STORE_MEMCPY", "BAD");
    ::testing::internal::CaptureStderr();
    const auto config = Load();
    const auto diagnostics = ::testing::internal::GetCapturedStderr();

    EXPECT_EQ(config.memcpy_enabled_override, std::optional<bool>{true});
    EXPECT_NE(diagnostics.find("Invalid value for MC_STORE_MEMCPY: bad, "
                               "defaulting to enabled"),
              std::string::npos);
}

TEST_F(TransferSubmitterConfigTest, NewConfigsReadCurrentEnvironment) {
    source_.Set("MC_STORE_MEMCPY", "0");
    const auto first = Load();
    source_.Set("MC_STORE_MEMCPY", "1");
    const auto second = Load();
    source_.Unset("MC_STORE_MEMCPY");
    const auto third = Load();

    EXPECT_EQ(first.memcpy_enabled_override, std::optional<bool>{false});
    EXPECT_EQ(second.memcpy_enabled_override, std::optional<bool>{true});
    EXPECT_FALSE(third.memcpy_enabled_override.has_value());
}

}  // namespace
}  // namespace mooncake
