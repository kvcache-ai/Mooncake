#include "../src/config/dfs_enablement_config.h"

#include <gtest/gtest.h>

#include <cstdlib>
#include <optional>
#include <string>

namespace mooncake::test {
namespace {

class ScopedEnvVar {
   public:
    explicit ScopedEnvVar(const char* name) : name_(name) {
        if (const char* value = std::getenv(name)) {
            original_ = value;
        }
        unsetenv(name);
    }

    ~ScopedEnvVar() {
        if (original_.has_value()) {
            setenv(name_.c_str(), original_->c_str(), 1);
        } else {
            unsetenv(name_.c_str());
        }
    }

    void Set(const char* value) { setenv(name_.c_str(), value, 1); }

   private:
    std::string name_;
    std::optional<std::string> original_;
};

class DfsEnablementConfigTest : public ::testing::Test {
   protected:
    ScopedEnvVar enabled{"MOONCAKE_ENABLE_DFS"};
    ScopedEnvVar legacy_enabled{"MOONCAKE_DFS_ENABLED"};
};

TEST_F(DfsEnablementConfigTest, DefaultsToDisabled) {
    EXPECT_FALSE(DfsEnablementConfig::FromEnvironment().enabled);
}

TEST_F(DfsEnablementConfigTest, PrimaryValueOverridesLegacyAlias) {
    legacy_enabled.Set("true");
    EXPECT_TRUE(DfsEnablementConfig::FromEnvironment().enabled);

    enabled.Set("false");
    EXPECT_FALSE(DfsEnablementConfig::FromEnvironment().enabled);
}

TEST_F(DfsEnablementConfigTest, InvalidPrimaryFallsBackToLegacyAlias) {
    legacy_enabled.Set("true");
    enabled.Set("invalid-primary");

    testing::internal::CaptureStderr();
    const auto config = DfsEnablementConfig::FromEnvironment();
    const std::string diagnostics = testing::internal::GetCapturedStderr();

    EXPECT_TRUE(config.enabled);
    EXPECT_EQ(diagnostics.find("MOONCAKE_DFS_ENABLED"), std::string::npos);
    EXPECT_NE(diagnostics.find("MOONCAKE_ENABLE_DFS"), std::string::npos);
}

TEST_F(DfsEnablementConfigTest, PreservesInvalidValueDiagnostics) {
    legacy_enabled.Set("invalid-legacy");
    enabled.Set("invalid-primary");

    testing::internal::CaptureStderr();
    const auto config = DfsEnablementConfig::FromEnvironment();
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
