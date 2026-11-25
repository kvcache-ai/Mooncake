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
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace mooncake {

enum class AutoGidCandidateClass {
    kNetworkRoutable = 0,
    kNoNetworkRoutable = 1,
    // Private-range (RFC 1918 / CGNAT) IPv4-mapped GIDs. Split out of the
    // degraded tier (#2729): datacenter RoCE fabrics routinely address NICs
    // from 10/8, and such a GID is at worst same-subnet-usable — unlike a
    // link-local fe80:: GID, which can never work across subnets. The
    // relative order of all pre-existing tiers is unchanged; only the former
    // tie between private-range IPv4 and link-local/overlay entries within
    // "degraded" is split.
    kNetworkPrivateV4 = 2,
    kNetworkDegraded = 3,
    kNoNetworkPrivateV4 = 4,
    kNoNetworkDegraded = 5,
    kFallbackNonzero = 6,
};

enum class AutoGidDataPathAction {
    kRetrySameGid = 0,
    kAdvanceGid = 1,
    kIgnoreDuplicate = 2,
    kExhausted = 3,
    kUnavailable = 4,
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

struct AutoGidDataPathAttempt {
    uint64_t endpoint_id = 0;
    uint64_t selection_epoch = 0;
};

struct PeerAutoGidSelection {
    AutoGidSelection selection;
    uint64_t selection_epoch = 0;
    bool recovery_enabled = false;
    bool observe_success = false;
    std::string auto_gid_reprobe_from;
};

// Candidates are ranked and unique by GID. The current GID is the cursor;
// reaching the end wraps to the highest-ranked candidate.
inline const AutoGidSelection* reselectAutoGidCandidate(
    const std::vector<AutoGidSelection>& candidates,
    std::string_view current_gid) {
    if (candidates.empty()) return nullptr;
    auto current = std::find_if(
        candidates.begin(), candidates.end(),
        [&](const auto& candidate) { return candidate.gid == current_gid; });
    if (current == candidates.end()) return &candidates.front();
    if (candidates.size() == 1) return nullptr;
    if (++current == candidates.end()) current = candidates.begin();
    return &*current;
}

class PeerAutoGidStateTable {
   public:
    PeerAutoGidStateTable(uint32_t max_advances = 0, bool enabled = false)
        : max_advances_(max_advances), enabled_(enabled) {}

    void configure(uint32_t max_advances, bool enabled) {
        std::lock_guard<std::mutex> guard(mutex_);
        states_.clear();
        max_advances_ = max_advances;
        enabled_ = enabled;
    }

    PeerAutoGidSelection prepare(const std::string& peer,
                                 const AutoGidSelection& default_selection) {
        return prepareInternal(peer, default_selection, nullptr, {});
    }

    PeerAutoGidSelection prepare(
        const std::string& peer, const AutoGidSelection& default_selection,
        const std::vector<AutoGidSelection>& candidates,
        std::string_view advisory_from = {}) {
        return prepareInternal(peer, default_selection, &candidates,
                               advisory_from);
    }

    bool needsCandidateScan(const std::string& peer,
                            const AutoGidSelection& default_selection,
                            std::string_view advisory_from = {}) const {
        if (!enabled_) return false;
        std::lock_guard<std::mutex> guard(mutex_);
        auto iter = states_.find(peer);
        if (iter == states_.end()) {
            return !advisory_from.empty() &&
                   advisory_from == default_selection.gid;
        }
        const State& state = iter->second;
        return (!state.exhausted && state.advance_pending) ||
               (!advisory_from.empty() && advisory_from == state.current.gid);
    }

    AutoGidDataPathAction recordCompletion(
        const std::string& peer, const AutoGidDataPathAttempt& attempt,
        bool success) {
        if (!enabled_ || attempt.endpoint_id == 0 ||
            attempt.selection_epoch == 0) {
            return AutoGidDataPathAction::kUnavailable;
        }
        std::lock_guard<std::mutex> guard(mutex_);
        auto iter = states_.find(peer);
        if (iter == states_.end()) {
            return AutoGidDataPathAction::kUnavailable;
        }
        State& state = iter->second;
        if (attempt.selection_epoch != state.selection_epoch) {
            return AutoGidDataPathAction::kIgnoreDuplicate;
        }
        if (success) {
            // Keep the epoch valid for subsequent posts on this connection.
            if (state.recovery_active) resetRecovery(state);
            return AutoGidDataPathAction::kIgnoreDuplicate;
        }
        if (state.exhausted) {
            return AutoGidDataPathAction::kExhausted;
        }
        if (state.advance_pending) {
            return AutoGidDataPathAction::kIgnoreDuplicate;
        }
        if (state.first_failed_endpoint_id == 0) {
            state.first_failed_endpoint_id = attempt.endpoint_id;
            state.recovery_active = true;
            return AutoGidDataPathAction::kRetrySameGid;
        }
        if (attempt.endpoint_id <= state.first_failed_endpoint_id) {
            return AutoGidDataPathAction::kIgnoreDuplicate;
        }
        state.advance_pending = true;
        state.advance_requested_locally = true;
        return AutoGidDataPathAction::kAdvanceGid;
    }

    void requestAdvance(const std::string& peer,
                        const AutoGidSelection& default_selection) {
        if (!enabled_) return;
        std::lock_guard<std::mutex> guard(mutex_);
        auto iter = getOrCreate(peer, default_selection);
        State& state = iter->second;
        if (state.exhausted) return;
        if (!state.advance_pending) {
            state.advance_pending = true;
            state.advance_requested_locally = true;
            state.recovery_active = true;
        }
    }

    void rememberPeerAdvertisedGid(const std::string& peer,
                                   std::string_view peer_gid) {
        if (!enabled_) return;
        std::lock_guard<std::mutex> guard(mutex_);
        auto iter = states_.find(peer);
        if (iter == states_.end()) return;
        iter->second.peer_advertised_gid = std::string(peer_gid);
        iter->second.auto_gid_reprobe_from.clear();
    }

    std::vector<std::string> refreshCandidates(
        const std::vector<AutoGidSelection>& candidates) {
        std::lock_guard<std::mutex> guard(mutex_);
        std::vector<std::string> invalidated;
        for (auto& [peer, state] : states_) {
            const auto* rebound = findByGid(candidates, state.current.gid);
            if (!rebound || rebound->gid_index != state.current.gid_index) {
                // Only a changed binding starts a new recovery cycle.
                state.remaining_advances = max_advances_;
                state.exhausted = false;
                state.first_failed_endpoint_id = 0;
                state.recovery_active = state.advance_pending;
                state.selection_epoch = nextSelectionEpoch();
                invalidated.push_back(peer);
            }
            if (rebound) {
                state.current = *rebound;
            } else {
                state.advance_pending = true;
                state.recovery_active = true;
            }
        }
        return invalidated;
    }

    size_t size() const {
        std::lock_guard<std::mutex> guard(mutex_);
        return states_.size();
    }

    bool contains(const std::string& peer) const {
        std::lock_guard<std::mutex> guard(mutex_);
        return states_.find(peer) != states_.end();
    }

   private:
    struct State {
        AutoGidSelection current;
        uint64_t selection_epoch = 0;
        uint64_t first_failed_endpoint_id = 0;
        size_t remaining_advances = 0;
        bool advance_pending = false;
        bool advance_requested_locally = false;
        bool recovery_active = false;
        bool exhausted = false;
        std::string peer_advertised_gid;
        std::string auto_gid_reprobe_from;
    };

    using StateMap = std::unordered_map<std::string, State>;

    uint64_t nextSelectionEpoch() {
        if (next_selection_epoch_ == 0) ++next_selection_epoch_;
        return next_selection_epoch_++;
    }

    typename StateMap::iterator getOrCreate(
        const std::string& peer, const AutoGidSelection& default_selection) {
        auto iter = states_.find(peer);
        if (iter != states_.end()) return iter;
        State state;
        state.current = default_selection;
        state.selection_epoch = nextSelectionEpoch();
        state.remaining_advances = max_advances_;
        return states_.emplace(peer, std::move(state)).first;
    }

    void resetRecovery(State& state) {
        state.advance_pending = false;
        state.advance_requested_locally = false;
        state.recovery_active = false;
        state.exhausted = false;
        state.first_failed_endpoint_id = 0;
        state.remaining_advances = max_advances_;
        state.auto_gid_reprobe_from.clear();
    }

    static void clearAdvanceRequest(State& state) {
        state.advance_pending = false;
        state.advance_requested_locally = false;
    }

    static const AutoGidSelection* findByGid(
        const std::vector<AutoGidSelection>& candidates, std::string_view gid) {
        auto iter = std::find_if(
            candidates.begin(), candidates.end(),
            [&](const auto& candidate) { return candidate.gid == gid; });
        return iter == candidates.end() ? nullptr : &*iter;
    }

    PeerAutoGidSelection prepareInternal(
        const std::string& peer, const AutoGidSelection& default_selection,
        const std::vector<AutoGidSelection>* candidates,
        std::string_view advisory_from) {
        if (!enabled_) return {default_selection, 0, false, false, {}};
        std::lock_guard<std::mutex> guard(mutex_);
        auto iter = getOrCreate(peer, default_selection);
        State& state = iter->second;
        bool should_advance = state.advance_pending;
        bool locally_requested = state.advance_requested_locally;

        const bool matching_advisory =
            !advisory_from.empty() && advisory_from == state.current.gid;
        if (matching_advisory) {
            should_advance = true;
        }

        if (should_advance && candidates && !candidates->empty() &&
            (!state.exhausted || matching_advisory)) {
            // A peer may need us to follow its move after local recovery ended.
            if (matching_advisory && state.remaining_advances == 0) {
                state.remaining_advances = 1;
                locally_requested = false;
            }
            state.recovery_active = true;
            const AutoGidSelection* next = nullptr;
            // Visit each alternative at most once per recovery cycle. A
            // vanished starting GID permits visiting every fresh candidate.
            state.remaining_advances = std::min(
                state.remaining_advances,
                candidates->size() -
                    (findByGid(*candidates, state.current.gid) ? 1 : 0));
            if (state.remaining_advances > 0) {
                next = reselectAutoGidCandidate(*candidates, state.current.gid);
            }
            clearAdvanceRequest(state);
            if (next) {
                if (locally_requested && !state.peer_advertised_gid.empty()) {
                    state.auto_gid_reprobe_from = state.peer_advertised_gid;
                }
                state.current = *next;
                --state.remaining_advances;
                state.selection_epoch = nextSelectionEpoch();
                state.first_failed_endpoint_id = 0;
                state.exhausted = false;
            } else {
                state.exhausted = true;
            }
        }

        return {state.current, state.selection_epoch, true,
                state.recovery_active, state.auto_gid_reprobe_from};
    }

    mutable std::mutex mutex_;
    uint32_t max_advances_;
    bool enabled_;
    uint64_t next_selection_epoch_ = 1;
    // Peer recovery survives endpoint recreation for this context's lifetime.
    StateMap states_;
};

inline const char* autoGidCandidateClassToString(
    AutoGidCandidateClass candidate_class) {
    switch (candidate_class) {
        case AutoGidCandidateClass::kNetworkRoutable:
            return "network-routable";
        case AutoGidCandidateClass::kNoNetworkRoutable:
            return "no-network-routable";
        case AutoGidCandidateClass::kNetworkPrivateV4:
            return "network-private-v4";
        case AutoGidCandidateClass::kNetworkDegraded:
            return "network-degraded";
        case AutoGidCandidateClass::kNoNetworkPrivateV4:
            return "no-network-private-v4";
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
    // Interface-name-based overlay detection (docker*/cni*/...) is the
    // reliable demotion signal; a private address range alone is not — DC
    // RoCE fabrics commonly use 10/8 (#2729).
    const bool is_overlay_iface = is_roce_v2 && candidate.is_overlay_network;
    const bool is_private_v4 = is_roce_v2 && !is_overlay_iface &&
                               candidate.is_ipv4_mapped &&
                               candidate.is_overlay_ipv4;
    const bool is_link_local =
        is_roce_v2 && !candidate.is_ipv4_mapped && candidate.is_link_local_ipv6;
    const bool is_degraded = is_overlay_iface || is_link_local;

    if (candidate.has_network_device) {
        if (is_degraded) return AutoGidCandidateClass::kNetworkDegraded;
        if (is_private_v4) return AutoGidCandidateClass::kNetworkPrivateV4;
        return AutoGidCandidateClass::kNetworkRoutable;
    }

    if (is_degraded) return AutoGidCandidateClass::kNoNetworkDegraded;
    if (is_private_v4) return AutoGidCandidateClass::kNoNetworkPrivateV4;
    return AutoGidCandidateClass::kNoNetworkRoutable;
}

inline int autoGidCandidateClassPriority(
    AutoGidCandidateClass candidate_class) {
    switch (candidate_class) {
        case AutoGidCandidateClass::kNetworkRoutable:
            return 0;
        case AutoGidCandidateClass::kNoNetworkRoutable:
            return 1;
        case AutoGidCandidateClass::kNetworkPrivateV4:
            return 2;
        case AutoGidCandidateClass::kNetworkDegraded:
            return 3;
        case AutoGidCandidateClass::kNoNetworkPrivateV4:
            return 4;
        case AutoGidCandidateClass::kNoNetworkDegraded:
            return 5;
        case AutoGidCandidateClass::kFallbackNonzero:
            return 6;
    }
    return 7;
}

inline std::vector<AutoGidSelection> rankAutoGidCandidates(
    const std::vector<AutoGidCandidate>& candidates) {
    std::vector<AutoGidSelection> ranked;
    int first_query_success_fallback = -1;
    std::string first_query_success_fallback_gid;

    for (const auto& candidate : candidates) {
        auto candidate_class = classifyAutoGidCandidate(candidate);
        if (candidate_class.has_value()) {
            ranked.push_back(
                {candidate.gid_index, candidate.gid, *candidate_class});
            continue;
        }

        if (candidate.query_succeeded && !candidate.is_null_gid &&
            first_query_success_fallback < 0) {
            first_query_success_fallback = candidate.gid_index;
            first_query_success_fallback_gid = candidate.gid;
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

    if (first_query_success_fallback >= 0) {
        ranked.push_back({first_query_success_fallback,
                          first_query_success_fallback_gid,
                          AutoGidCandidateClass::kFallbackNonzero});
    }

    std::unordered_set<std::string> seen;
    ranked.erase(std::remove_if(ranked.begin(), ranked.end(),
                                [&](const auto& selection) {
                                    return !seen.insert(selection.gid).second;
                                }),
                 ranked.end());
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

}  // namespace mooncake

#endif  // RDMA_GID_PROBE_H
