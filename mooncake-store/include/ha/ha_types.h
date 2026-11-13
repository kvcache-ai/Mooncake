#pragma once

#include <string>
#include "types.h"

namespace mooncake {
namespace ha {
// TODO-REF(F02-T011, receiver=UPG1-F12-S50):
// Source: UPG1-F02-B09; owner: UPG1-F12-S50.
// Stage: UPG1 / F12-S50 / centralized HA implementation; due before S50
// acceptance. Receive the rest of fixed U ha_types.h with the actual backend
// providers; this subset exposes only U admin view/state types. Close with
// full U source/type/owner verification, review and the receiving commit.
struct MasterView {
    std::string leader_address;
    ViewVersionId view_version = 0;
};

enum class MasterRuntimeState {
    kStarting,
    kStandby,
    kCandidate,
    kRecovering,
    kCatchingUp,
    kLeaderWarmup,
    kServing,
};

inline const char* MasterRuntimeStateToString(MasterRuntimeState state) {
    switch (state) {
        case MasterRuntimeState::kStarting:
            return "starting";
        case MasterRuntimeState::kStandby:
            return "standby";
        case MasterRuntimeState::kCandidate:
            return "candidate";
        case MasterRuntimeState::kRecovering:
            return "recovering";
        case MasterRuntimeState::kCatchingUp:
            return "catching_up";
        case MasterRuntimeState::kLeaderWarmup:
            return "leader_warmup";
        case MasterRuntimeState::kServing:
            return "serving";
    }
    return "unknown";
}

inline const char* MasterRuntimeRoleToString(MasterRuntimeState state) {
    switch (state) {
        case MasterRuntimeState::kLeaderWarmup:
        case MasterRuntimeState::kServing:
            return "leader";
        case MasterRuntimeState::kStarting:
        case MasterRuntimeState::kStandby:
        case MasterRuntimeState::kCandidate:
        case MasterRuntimeState::kRecovering:
        case MasterRuntimeState::kCatchingUp:
            return "standby";
    }
    return "unknown";
}

}  // namespace ha
}  // namespace mooncake
