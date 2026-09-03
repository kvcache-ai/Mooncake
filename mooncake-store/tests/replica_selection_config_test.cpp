#include <gtest/gtest.h>

#include <cstdlib>
#include <optional>
#include <string>

#include "config/replica_selection_config.h"

namespace mooncake {
namespace {

class ReplicaSelectionConfigTest : public ::testing::Test {
   protected:
    void SetUp() override {
        if (const char* value = std::getenv("MC_STORE_REPLICA_SCORING")) {
            original_ = value;
        }
        ASSERT_EQ(unsetenv("MC_STORE_REPLICA_SCORING"), 0);
    }

    void TearDown() override {
        if (original_.has_value()) {
            EXPECT_EQ(setenv("MC_STORE_REPLICA_SCORING", original_->c_str(), 1),
                      0);
        } else {
            EXPECT_EQ(unsetenv("MC_STORE_REPLICA_SCORING"), 0);
        }
    }

   private:
    std::optional<std::string> original_;
};

TEST_F(ReplicaSelectionConfigTest, UnsetDisablesScoring) {
    EXPECT_FALSE(
        ReplicaSelectionConfig::FromEnvironment().remote_scoring_enabled);
}

TEST_F(ReplicaSelectionConfigTest, OnlyExactOneEnablesScoring) {
    struct Case {
        const char* value;
        bool enabled;
    };
    for (const auto& test_case :
         {Case{"1", true}, Case{"", false}, Case{"0", false},
          Case{"true", false}, Case{"TRUE", false}, Case{"yes", false},
          Case{"on", false}, Case{"false", false}, Case{"-1", false},
          Case{"2", false}, Case{"01", false}, Case{"+1", false},
          Case{" 1", false}, Case{"1 ", false}, Case{"1\n", false},
          Case{"1suffix", false}}) {
        SCOPED_TRACE(test_case.value);
        ASSERT_EQ(setenv("MC_STORE_REPLICA_SCORING", test_case.value, 1), 0);
        testing::internal::CaptureStderr();
        const auto config = ReplicaSelectionConfig::FromEnvironment();
        const auto diagnostics = testing::internal::GetCapturedStderr();
        EXPECT_EQ(config.remote_scoring_enabled, test_case.enabled);
        EXPECT_TRUE(diagnostics.empty());
    }
}

TEST_F(ReplicaSelectionConfigTest, NewConfigsReadTheCurrentEnvironment) {
    ASSERT_EQ(setenv("MC_STORE_REPLICA_SCORING", "1", 1), 0);
    const auto enabled_config = ReplicaSelectionConfig::FromEnvironment();

    ASSERT_EQ(unsetenv("MC_STORE_REPLICA_SCORING"), 0);
    EXPECT_FALSE(
        ReplicaSelectionConfig::FromEnvironment().remote_scoring_enabled);
    EXPECT_TRUE(enabled_config.remote_scoring_enabled);
}

}  // namespace
}  // namespace mooncake
