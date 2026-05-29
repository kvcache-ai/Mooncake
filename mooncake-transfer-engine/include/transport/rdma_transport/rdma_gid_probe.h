// Copyright 2026 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef RDMA_GID_PROBE_H
#define RDMA_GID_PROBE_H

#include <cerrno>
#include <infiniband/verbs.h>

#include <algorithm>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace mooncake {

enum class AutoGidCandidateClass {
    kNetworkRoutable = 0,
    kNoNetworkRoutable = 1,
    kNetworkDegraded = 2,
    kNoNetworkDegraded = 3,
    kFallbackNonzero = 4,
};

enum class AutoGidRetryAction {
    kDoNotRetry = 0,
    kRetryWithReprobedGid = 1,
    kRetryWithObservedChange = 2,
};

struct AutoGidCandidate {
    int gid_index = -1;
    std::string gid;
    uint32_t gid_type = 0;
    bool has_network_device = false;
    bool is_ipv4_mapped = false;
    bool is_link_local_ipv6 = false;
    bool is_overlay_network = false;
    bool is_overlay_ipv4 = false;
    bool is_null_gid = false;
    bool query_succeeded = true;
};

struct AutoGidSelection {
    int gid_index = -1;
    std::string gid;
    AutoGidCandidateClass candidate_class =
        AutoGidCandidateClass::kFallbackNonzero;
};

struct AutoGidSelectionIdentity {
    int gid_index = -1;
    std::string gid;
};

inline const char* autoGidCandidateClassToString(
    AutoGidCandidateClass candidate_class) {
    switch (candidate_class) {
        case AutoGidCandidateClass::kNetworkRoutable:
            return "network-routable";
        case AutoGidCandidateClass::kNoNetworkRoutable:
            return "no-network-routable";
        case AutoGidCandidateClass::kNetworkDegraded:
            return "network-degraded";
        case AutoGidCandidateClass::kNoNetworkDegraded:
            return "no-network-degraded";
        case AutoGidCandidateClass::kFallbackNonzero:
            return "fallback-nonzero";
    }
    return "unknown";
}

inline std::optional<AutoGidCandidateClass> classifyAutoGidCandidate(
    const AutoGidCandidate& candidate) {
    if (!candidate.query_succeeded || candidate.gid_index < 0 ||
        candidate.is_null_gid) {
        return std::nullopt;
    }

    if (candidate.gid_type != IBV_GID_TYPE_ROCE_V2 &&
        candidate.gid_type != IBV_GID_TYPE_IB) {
        return std::nullopt;
    }

    const bool is_roce_v2 = candidate.gid_type == IBV_GID_TYPE_ROCE_V2;
    const bool is_overlay =
        is_roce_v2 && (candidate.is_overlay_network ||
                       (candidate.is_ipv4_mapped && candidate.is_overlay_ipv4));
    const bool is_link_local =
        is_roce_v2 && !candidate.is_ipv4_mapped && candidate.is_link_local_ipv6;
    const bool is_degraded = is_overlay || is_link_local;

    if (candidate.has_network_device) {
        return is_degraded ? AutoGidCandidateClass::kNetworkDegraded
                           : AutoGidCandidateClass::kNetworkRoutable;
    }

    return is_degraded ? AutoGidCandidateClass::kNoNetworkDegraded
                       : AutoGidCandidateClass::kNoNetworkRoutable;
}

inline int autoGidCandidateClassPriority(
    AutoGidCandidateClass candidate_class) {
    switch (candidate_class) {
        case AutoGidCandidateClass::kNetworkRoutable:
            return 0;
        case AutoGidCandidateClass::kNoNetworkRoutable:
            return 1;
        case AutoGidCandidateClass::kNetworkDegraded:
            return 2;
        case AutoGidCandidateClass::kNoNetworkDegraded:
            return 3;
        case AutoGidCandidateClass::kFallbackNonzero:
            return 4;
    }
    return 5;
}

inline std::vector<AutoGidSelection> rankAutoGidCandidates(
    const std::vector<AutoGidCandidate>& candidates) {
    std::vector<AutoGidSelection> ranked;
    int first_nonzero_fallback = -1;
    std::string first_nonzero_fallback_gid;

    for (const auto& candidate : candidates) {
        auto candidate_class = classifyAutoGidCandidate(candidate);
        if (candidate_class.has_value()) {
            ranked.push_back(
                {candidate.gid_index, candidate.gid, *candidate_class});
            continue;
        }

        if (candidate.query_succeeded && !candidate.is_null_gid &&
            first_nonzero_fallback < 0) {
            first_nonzero_fallback = candidate.gid_index;
            first_nonzero_fallback_gid = candidate.gid;
        }
    }

    std::stable_sort(
        ranked.begin(), ranked.end(),
        [](const AutoGidSelection& lhs, const AutoGidSelection& rhs) {
            int lhs_priority =
                autoGidCandidateClassPriority(lhs.candidate_class);
            int rhs_priority =
                autoGidCandidateClassPriority(rhs.candidate_class);
            if (lhs_priority != rhs_priority) {
                return lhs_priority < rhs_priority;
            }
            return lhs.gid_index < rhs.gid_index;
        });

    if (first_nonzero_fallback >= 0) {
        ranked.push_back({first_nonzero_fallback, first_nonzero_fallback_gid,
                          AutoGidCandidateClass::kFallbackNonzero});
    }

    return ranked;
}

inline std::optional<AutoGidSelection> selectBestAutoGidCandidate(
    const std::vector<AutoGidCandidate>& candidates) {
    auto ranked = rankAutoGidCandidates(candidates);
    if (ranked.empty()) {
        return std::nullopt;
    }
    return ranked.front();
}

inline bool shouldAttemptAutoGidHandshakeRetry(bool auto_gid_selection_enabled,
                                               int retry_count, int max_retries,
                                               bool failure_happened_at_rtr,
                                               int sys_errno) {
    return auto_gid_selection_enabled && retry_count < max_retries &&
           failure_happened_at_rtr && sys_errno == EINVAL;
}

inline bool didAutoGidSelectionChange(int previous_gid_index,
                                      std::string_view previous_gid,
                                      int current_gid_index,
                                      std::string_view current_gid) {
    return previous_gid_index != current_gid_index ||
           previous_gid != current_gid;
}

inline bool matchesAutoGidSelection(const AutoGidSelectionIdentity& identity,
                                    const AutoGidSelection& selection) {
    return identity.gid_index == selection.gid_index &&
           identity.gid == selection.gid;
}

inline bool hasTriedAutoGidSelection(
    const std::vector<AutoGidSelectionIdentity>& tried_selections,
    const AutoGidSelection& selection) {
    return std::any_of(tried_selections.begin(), tried_selections.end(),
                       [&](const AutoGidSelectionIdentity& identity) {
                           return matchesAutoGidSelection(identity, selection);
                       });
}

inline std::optional<AutoGidSelection> reselectAutoGidCandidate(
    const std::vector<AutoGidCandidate>& candidates, int current_gid_index,
    std::string_view current_gid,
    const std::vector<AutoGidSelectionIdentity>& tried_selections = {}) {
    auto ranked = rankAutoGidCandidates(candidates);
    for (const auto& selection : ranked) {
        if (!didAutoGidSelectionChange(current_gid_index, current_gid,
                                       selection.gid_index, selection.gid)) {
            continue;
        }
        if (hasTriedAutoGidSelection(tried_selections, selection)) {
            continue;
        }
        return selection;
    }
    return std::nullopt;
}

inline AutoGidRetryAction decideAutoGidRetryAction(
    bool reprobe_changed, int previous_gid_index, std::string_view previous_gid,
    int current_gid_index, std::string_view current_gid) {
    if (reprobe_changed) {
        return AutoGidRetryAction::kRetryWithReprobedGid;
    }
    if (didAutoGidSelectionChange(previous_gid_index, previous_gid,
                                  current_gid_index, current_gid)) {
        return AutoGidRetryAction::kRetryWithObservedChange;
    }
    return AutoGidRetryAction::kDoNotRetry;
}

}  // namespace mooncake

#endif  // RDMA_GID_PROBE_H
