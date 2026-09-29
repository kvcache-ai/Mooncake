#include <gtest/gtest.h>

#include "config/replica_selection_config.h"
#include "environ.h"

namespace mooncake {
namespace {

class ReplicaSelectionConfigTest : public ::testing::Test {
   protected:
    ReplicaSelectionConfig Load() const {
        return ReplicaSelectionConfig::FromEnvironment(Environ(source_));
    }

    MapEnvironSource source_;
};

TEST_F(ReplicaSelectionConfigTest, UnsetDisablesScoring) {
    EXPECT_FALSE(Load().remote_scoring_enabled);
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
        source_.Set("MC_STORE_REPLICA_SCORING", test_case.value);
        testing::internal::CaptureStderr();
        const auto config = Load();
        const auto diagnostics = testing::internal::GetCapturedStderr();
        EXPECT_EQ(config.remote_scoring_enabled, test_case.enabled);
        EXPECT_TRUE(diagnostics.empty());
    }
}

TEST_F(ReplicaSelectionConfigTest, NewConfigsReadTheCurrentEnvironment) {
    source_.Set("MC_STORE_REPLICA_SCORING", "1");
    const auto enabled_config = Load();

    source_.Unset("MC_STORE_REPLICA_SCORING");
    EXPECT_FALSE(Load().remote_scoring_enabled);
    EXPECT_TRUE(enabled_config.remote_scoring_enabled);
}

}  // namespace
}  // namespace mooncake
