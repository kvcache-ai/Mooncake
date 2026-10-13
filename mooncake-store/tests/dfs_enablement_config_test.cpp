#include "../src/config/dfs_enablement_config.h"

#include <gtest/gtest.h>

#include <string>

#include "environ.h"

namespace mooncake::test {
namespace {

class DfsEnablementConfigTest : public ::testing::Test {
   protected:
    DfsEnablementConfig Load() const {
        return DfsEnablementConfig::FromEnvironment(Environ(source_));
    }

    void SetEnabled(const char* value) {
        source_.Set("MOONCAKE_ENABLE_DFS", value);
    }

    void SetLegacyEnabled(const char* value) {
        source_.Set("MOONCAKE_DFS_ENABLED", value);
    }

    MapEnvironSource source_;
};

TEST_F(DfsEnablementConfigTest, DefaultsToDisabled) {
    EXPECT_FALSE(Load().enabled);
}

TEST_F(DfsEnablementConfigTest, PrimaryValueOverridesLegacyAlias) {
    SetLegacyEnabled("true");
    EXPECT_TRUE(Load().enabled);

    SetEnabled("false");
    EXPECT_FALSE(Load().enabled);
}

TEST_F(DfsEnablementConfigTest, InvalidPrimaryFallsBackToLegacyAlias) {
    SetLegacyEnabled("true");
    SetEnabled("invalid-primary");

    testing::internal::CaptureStderr();
    const auto config = Load();
    const std::string diagnostics = testing::internal::GetCapturedStderr();

    EXPECT_TRUE(config.enabled);
    EXPECT_EQ(diagnostics.find("MOONCAKE_DFS_ENABLED"), std::string::npos);
    EXPECT_NE(diagnostics.find("MOONCAKE_ENABLE_DFS"), std::string::npos);
}

TEST_F(DfsEnablementConfigTest, PreservesInvalidValueDiagnostics) {
    SetLegacyEnabled("invalid-legacy");
    SetEnabled("invalid-primary");

    testing::internal::CaptureStderr();
    const auto config = Load();
    const std::string diagnostics = testing::internal::GetCapturedStderr();

    EXPECT_FALSE(config.enabled);
    const auto legacy_position = diagnostics.find("MOONCAKE_DFS_ENABLED");
    const auto primary_position = diagnostics.find("MOONCAKE_ENABLE_DFS");
    EXPECT_NE(legacy_position, std::string::npos);
    EXPECT_NE(primary_position, std::string::npos);
    EXPECT_LT(legacy_position, primary_position);
}

}  // namespace
}  // namespace mooncake::test
