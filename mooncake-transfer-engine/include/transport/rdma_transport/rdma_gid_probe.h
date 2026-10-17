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
#include <list>
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

enum class AutoGidRetryAction {
    kDoNotRetry = 0,
    kRetryWithReprobedGid = 1,
    kRetryWithObservedChange = 2,
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

struct AutoGidSelectionIdentity {
    int gid_index = -1;
    std::string gid;
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

class PeerAutoGidStateTable {
   public:
    PeerAutoGidStateTable(size_t capacity = 1, uint32_t max_advances = 0,
                          bool enabled = false)
        : capacity_(std::max<size_t>(capacity, 1)),
          hard_capacity_(capacity_ * 2),
          max_advances_(max_advances),
          enabled_(enabled) {}

    void configure(size_t capacity, uint32_t max_advances, bool enabled) {
        std::lock_guard<std::mutex> guard(mutex_);
        states_.clear();
        lru_.clear();
        capacity_ = std::max<size_t>(capacity, 1);
        hard_capacity_ = capacity_ * 2;
        max_advances_ = max_advances;
        enabled_ = enabled;
        candidate_generation_ = 0;
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
        const State& state = iter->second.state;
        return state.advance_pending ||
               state.candidate_generation_seen != candidate_generation_ ||
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
        touch(iter);
        State& state = iter->second.state;
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
        State& state = iter->second.state;
        if (!state.advance_pending) {
            state.advance_pending = true;
            state.advance_requested_locally = true;
            state.recovery_active = true;
        }
        touch(iter);
    }

    void rememberPeerAdvertisedGid(const std::string& peer,
                                   std::string_view peer_gid) {
        if (!enabled_) return;
        std::lock_guard<std::mutex> guard(mutex_);
        auto iter = states_.find(peer);
        if (iter == states_.end()) return;
        iter->second.state.peer_advertised_gid = std::string(peer_gid);
        iter->second.state.auto_gid_reprobe_from.clear();
        touch(iter);
    }

    void markCandidatesDirty() {
        std::lock_guard<std::mutex> guard(mutex_);
        ++candidate_generation_;
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
        uint64_t candidate_generation_seen = 0;
        uint32_t advances_used = 0;
        bool advance_pending = false;
        bool advance_requested_locally = false;
        bool recovery_active = false;
        bool exhausted = false;
        std::string peer_advertised_gid;
        std::string auto_gid_reprobe_from;
        std::unordered_set<std::string> tried_gids;
    };

    struct Entry {
        State state;
        std::list<std::string>::iterator lru;
    };

    using StateMap = std::unordered_map<std::string, Entry>;

    uint64_t nextSelectionEpoch() {
        if (next_selection_epoch_ == 0) ++next_selection_epoch_;
        return next_selection_epoch_++;
    }

    typename StateMap::iterator getOrCreate(
        const std::string& peer, const AutoGidSelection& default_selection) {
        auto iter = states_.find(peer);
        if (iter != states_.end()) return iter;
        while (states_.size() >= capacity_ && evictOldestIdle()) {
        }
        if (states_.size() >= hard_capacity_) evictOldest();
        lru_.push_back(peer);
        State state;
        state.current = default_selection;
        state.selection_epoch = nextSelectionEpoch();
        state.candidate_generation_seen = candidate_generation_;
        state.tried_gids.insert(default_selection.gid);
        return states_.emplace(peer, Entry{std::move(state), --lru_.end()})
            .first;
    }

    void touch(typename StateMap::iterator iter) {
        lru_.splice(lru_.end(), lru_, iter->second.lru);
        iter->second.lru = --lru_.end();
    }

    bool evictOldestIdle() {
        for (auto lru = lru_.begin(); lru != lru_.end(); ++lru) {
            auto victim = states_.find(*lru);
            if (victim == states_.end()) {
                lru_.erase(lru);
                return true;
            }
            if (victim->second.state.recovery_active) continue;
            states_.erase(victim);
            lru_.erase(lru);
            return true;
        }
        return false;
    }

    void evictOldest() {
        while (!lru_.empty()) {
            auto oldest = lru_.begin();
            auto victim = states_.find(*oldest);
            lru_.erase(oldest);
            if (victim == states_.end()) continue;
            states_.erase(victim);
            return;
        }
    }

    void resetRecovery(State& state) {
        state.advance_pending = false;
        state.advance_requested_locally = false;
        state.recovery_active = false;
        state.exhausted = false;
        state.first_failed_endpoint_id = 0;
        state.advances_used = 0;
        state.tried_gids.clear();
        state.tried_gids.insert(state.current.gid);
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
        touch(iter);
        State& state = iter->second.state;
        bool should_advance = state.advance_pending;
        bool locally_requested = state.advance_requested_locally;

        if (!advisory_from.empty() && advisory_from == state.current.gid) {
            should_advance = true;
        }

        if (candidates &&
            state.candidate_generation_seen != candidate_generation_) {
            state.candidate_generation_seen = candidate_generation_;
            // A refreshed table starts a new bounded search without replacing
            // a still-usable selection. Old connection completions are stale.
            resetRecovery(state);
            state.selection_epoch = nextSelectionEpoch();
            const AutoGidSelection* rebound =
                findByGid(*candidates, state.current.gid);
            if (rebound) {
                state.current = *rebound;
            } else {
                should_advance = true;
            }
        }

        if (should_advance && candidates) {
            state.recovery_active = true;
            const AutoGidSelection* next = nullptr;
            if (state.advances_used < max_advances_) {
                for (const auto& candidate : *candidates) {
                    if (!state.tried_gids.count(candidate.gid)) {
                        next = &candidate;
                        break;
                    }
                }
            }
            clearAdvanceRequest(state);
            if (next) {
                if (locally_requested && !state.peer_advertised_gid.empty()) {
                    state.auto_gid_reprobe_from = state.peer_advertised_gid;
                }
                state.current = *next;
                state.tried_gids.insert(next->gid);
                ++state.advances_used;
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
    size_t capacity_;
    size_t hard_capacity_;
    uint32_t max_advances_;
    bool enabled_;
    uint64_t candidate_generation_ = 0;
    uint64_t next_selection_epoch_ = 1;
    StateMap states_;
    std::list<std::string> lru_;
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
