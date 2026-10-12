#include <glog/logging.h>
#include <gtest/gtest.h>

#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <iterator>
#include <optional>
#include <string>

#include "../src/config/transfer_submitter_config.h"

namespace mooncake {
namespace {

constexpr const char* kEnvNames[] = {"MC_STORE_MEMCPY",
                                     "MC_STORE_TRANSFER_TIMEOUT_MS"};

class TransferSubmitterConfigTest : public ::testing::Test {
   protected:
    void SetUp() override {
        for (size_t i = 0; i < std::size(kEnvNames); ++i) {
            if (const char* value = std::getenv(kEnvNames[i])) {
                originals_[i] = value;
            }
        }
        google::InitGoogleLogging("TransferSubmitterConfigTest");
        original_logtostderr_ = FLAGS_logtostderr;
        FLAGS_logtostderr = true;
        for (const char* name : kEnvNames) {
            ASSERT_EQ(unsetenv(name), 0);
        }
    }

    void TearDown() override {
        for (size_t i = 0; i < std::size(kEnvNames); ++i) {
            if (originals_[i].has_value()) {
                EXPECT_EQ(setenv(kEnvNames[i], originals_[i]->c_str(), 1), 0);
            } else {
                EXPECT_EQ(unsetenv(kEnvNames[i]), 0);
            }
        }
        FLAGS_logtostderr = original_logtostderr_;
        google::ShutdownGoogleLogging();
    }

   private:
    std::optional<std::string> originals_[std::size(kEnvNames)];
    bool original_logtostderr_ = false;
};

TEST_F(TransferSubmitterConfigTest, UnsetMeansAutomatic) {
    ::testing::internal::CaptureStderr();
    const auto config = TransferSubmitterConfig::FromEnvironment();
    const auto diagnostics = ::testing::internal::GetCapturedStderr();

    EXPECT_FALSE(config.memcpy_enabled_override.has_value());
    EXPECT_FALSE(config.transfer_timeout_override.has_value());
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
        ASSERT_EQ(setenv("MC_STORE_MEMCPY", test_case.value, 1), 0);
        ::testing::internal::CaptureStderr();
        const auto config = TransferSubmitterConfig::FromEnvironment();
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
    ASSERT_EQ(setenv("MC_STORE_MEMCPY", "BAD", 1), 0);
    ::testing::internal::CaptureStderr();
    const auto config = TransferSubmitterConfig::FromEnvironment();
    const auto diagnostics = ::testing::internal::GetCapturedStderr();

    EXPECT_EQ(config.memcpy_enabled_override, std::optional<bool>{true});
    EXPECT_NE(diagnostics.find("Invalid value for MC_STORE_MEMCPY: bad, "
                               "defaulting to enabled"),
              std::string::npos);
}

TEST_F(TransferSubmitterConfigTest, NewConfigsReadCurrentEnvironment) {
    ASSERT_EQ(setenv("MC_STORE_MEMCPY", "0", 1), 0);
    const auto first = TransferSubmitterConfig::FromEnvironment();
    ASSERT_EQ(setenv("MC_STORE_MEMCPY", "1", 1), 0);
    const auto second = TransferSubmitterConfig::FromEnvironment();
    ASSERT_EQ(unsetenv("MC_STORE_MEMCPY"), 0);
    const auto third = TransferSubmitterConfig::FromEnvironment();

    EXPECT_EQ(first.memcpy_enabled_override, std::optional<bool>{false});
    EXPECT_EQ(second.memcpy_enabled_override, std::optional<bool>{true});
    EXPECT_FALSE(third.memcpy_enabled_override.has_value());
}

TEST_F(TransferSubmitterConfigTest, TransferTimeoutAcceptsMillisecondsInRange) {
    struct Case {
        const char* value;
        int64_t expected_ms;
    };
    const Case cases[] = {
        {"1", 1},         {"500", 500},     {"+250", 250},
        {" 2000 ", 2000}, {"60000", 60000}, {"86400000", 86400000},
    };

    for (const auto& test_case : cases) {
        SCOPED_TRACE(test_case.value);
        ASSERT_EQ(setenv("MC_STORE_TRANSFER_TIMEOUT_MS", test_case.value, 1),
                  0);
        ::testing::internal::CaptureStderr();
        const auto config = TransferSubmitterConfig::FromEnvironment();
        const auto diagnostics = ::testing::internal::GetCapturedStderr();

        EXPECT_EQ(config.transfer_timeout_override,
                  std::optional<std::chrono::milliseconds>{
                      std::chrono::milliseconds{test_case.expected_ms}});
        EXPECT_TRUE(diagnostics.empty()) << diagnostics;
    }
}

TEST_F(TransferSubmitterConfigTest,
       InvalidTransferTimeoutWarnsAndKeepsDefault) {
    for (const char* value : {"", "0", "-1", "abc", "1.5", "1500ms", "86400001",
                              "99999999999999999999"}) {
        SCOPED_TRACE(value);
        ASSERT_EQ(setenv("MC_STORE_TRANSFER_TIMEOUT_MS", value, 1), 0);
        ::testing::internal::CaptureStderr();
        const auto config = TransferSubmitterConfig::FromEnvironment();
        const auto diagnostics = ::testing::internal::GetCapturedStderr();

        EXPECT_FALSE(config.transfer_timeout_override.has_value());
        EXPECT_NE(diagnostics.find(
                      "Invalid value for MC_STORE_TRANSFER_TIMEOUT_MS: '" +
                      std::string(value) + "'"),
                  std::string::npos)
            << diagnostics;
    }
}

TEST_F(TransferSubmitterConfigTest, TransferTimeoutIsIndependentOfMemcpy) {
    ASSERT_EQ(setenv("MC_STORE_TRANSFER_TIMEOUT_MS", "750", 1), 0);
    const auto timeout_only = TransferSubmitterConfig::FromEnvironment();
    EXPECT_FALSE(timeout_only.memcpy_enabled_override.has_value());
    EXPECT_EQ(timeout_only.transfer_timeout_override,
              std::optional<std::chrono::milliseconds>{
                  std::chrono::milliseconds{750}});

    ASSERT_EQ(unsetenv("MC_STORE_TRANSFER_TIMEOUT_MS"), 0);
    ASSERT_EQ(setenv("MC_STORE_MEMCPY", "0", 1), 0);
    const auto memcpy_only = TransferSubmitterConfig::FromEnvironment();
    EXPECT_EQ(memcpy_only.memcpy_enabled_override, std::optional<bool>{false});
    EXPECT_FALSE(memcpy_only.transfer_timeout_override.has_value());
}

}  // namespace
}  // namespace mooncake
