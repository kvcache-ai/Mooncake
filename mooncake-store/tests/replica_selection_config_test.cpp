// Copyright 2026 Mooncake Authors

#include "replica_selection_config.h"

#include <gtest/gtest.h>

#include <array>
#include <string_view>

namespace mooncake {
namespace {

TEST(ReplicaSelectionConfigTest, AcceptsOnlyExactPublicModes) {
    const auto legacy = ParseReplicaSelectionOptions("legacy");
    ASSERT_TRUE(legacy.has_value());
    EXPECT_EQ(legacy->mode, ReplicaSelectionMode::LEGACY);

    const auto shadow = ParseReplicaSelectionOptions("shadow");
    ASSERT_TRUE(shadow.has_value());
    EXPECT_EQ(shadow->mode, ReplicaSelectionMode::SHADOW);
    EXPECT_FALSE(shadow->collect_transfer_signals);

    const auto shadow_live = ParseReplicaSelectionOptions("shadow-live");
    ASSERT_TRUE(shadow_live.has_value());
    EXPECT_EQ(shadow_live->mode, ReplicaSelectionMode::SHADOW);
    EXPECT_TRUE(shadow_live->collect_transfer_signals);

    for (const std::string_view invalid :
         std::array<std::string_view, 8>{"", "active", "LEGACY", "SHADOW",
                                         " legacy", "shadow ", "v2", "1"}) {
        EXPECT_FALSE(ParseReplicaSelectionOptions(invalid).has_value())
            << invalid;
    }
}

TEST(ReplicaSelectionConfigTest, NamesEveryInternalModeWithoutParsingActive) {
    EXPECT_STREQ(ReplicaSelectionModeName(ReplicaSelectionMode::LEGACY),
                 "legacy");
    EXPECT_STREQ(ReplicaSelectionModeName(ReplicaSelectionMode::SHADOW),
                 "shadow");
    EXPECT_STREQ(ReplicaSelectionModeName(ReplicaSelectionMode::ACTIVE),
                 "active");
    EXPECT_STREQ(
        ReplicaSelectionModeName(static_cast<ReplicaSelectionMode>(255)),
        "unknown");
}

}  // namespace
}  // namespace mooncake
