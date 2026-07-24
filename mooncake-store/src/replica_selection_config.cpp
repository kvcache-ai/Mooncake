// Copyright 2026 Mooncake Authors

#include "replica_selection_config.h"

namespace mooncake {

std::optional<ReplicaSelectionOptions> ParseReplicaSelectionOptions(
    std::string_view mode) noexcept {
    if (mode == "legacy") {
        return ReplicaSelectionOptions{ReplicaSelectionMode::LEGACY};
    }
    if (mode == "shadow") {
        return ReplicaSelectionOptions{ReplicaSelectionMode::SHADOW};
    }
    if (mode == "shadow-live") {
        return ReplicaSelectionOptions{ReplicaSelectionMode::SHADOW, true};
    }
    return std::nullopt;
}

const char* ReplicaSelectionModeName(ReplicaSelectionMode mode) noexcept {
    switch (mode) {
        case ReplicaSelectionMode::LEGACY:
            return "legacy";
        case ReplicaSelectionMode::SHADOW:
            return "shadow";
        case ReplicaSelectionMode::ACTIVE:
            return "active";
    }
    return "unknown";
}

}  // namespace mooncake
