// Copyright 2026 Mooncake Authors

#pragma once

#include <optional>
#include <string_view>

#include "replica_selector.h"

namespace mooncake {

// Public Store configuration deliberately exposes only compatibility-safe
// modes. ACTIVE remains an internal model/testing mode until the scorecard has
// demonstrated an end-to-end benefit and a separate rollout is approved.
struct ReplicaSelectionOptions {
    ReplicaSelectionMode mode{ReplicaSelectionMode::LEGACY};
    bool collect_transfer_signals{false};
};

[[nodiscard]] std::optional<ReplicaSelectionOptions>
ParseReplicaSelectionOptions(std::string_view mode) noexcept;

[[nodiscard]] const char* ReplicaSelectionModeName(
    ReplicaSelectionMode mode) noexcept;

}  // namespace mooncake
