// Copyright 2025 Mooncake Authors
//
// Replica selection for reads: given the list of replicas the master returned
// for a key, choose which one to actually fetch from.
//
// The base policy is type + locality based (local MEMORY > local NOF_SSD >
// remote MEMORY > remote NOF_SSD > LOCAL_DISK > DISK). On top of that, when a
// key has more than one *remote* MEMORY replica, the historical code kept the
// first one master happened to return, which is effectively arbitrary. This
// header adds an opt-in scoring hook so a better remote replica can be picked
// (issue #2516).
//
// Design constraints:
//   * Disabled by default — behaviour is byte-identical to the historical
//     "first remote MEMORY" pick unless the operator sets
//     MC_STORE_REPLICA_SCORING=1 or a scorer is injected.
//   * This layer only sees what a replica descriptor carries (endpoint,
//     protocol). Richer signals (NIC role, NUMA distance, live load) live in
//     the transfer engine; they can be fed in via SetRemoteReplicaScorer()
//     without mooncake-store growing a dependency on that layer.

#pragma once

#include <cstdlib>
#include <functional>
#include <limits>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <unordered_set>
#include <vector>

#include "replica.h"

namespace mooncake {

// Scores a remote replica candidate; lower score == more preferred.
using ReplicaScorer = std::function<double(const Replica::Descriptor&)>;

namespace detail {

inline std::shared_mutex& ScorerMutex() {
    static std::shared_mutex mu;
    return mu;
}

inline ReplicaScorer& ScorerStorage() {
    static ReplicaScorer scorer;
    return scorer;
}

}  // namespace detail

// Return a snapshot (copy) of the current scorer. The copy is taken under a
// shared lock so concurrent reads are non-blocking. Callers invoke the
// returned copy outside the lock, avoiding both the data race and any
// potential deadlock from calling user code under a lock.
inline ReplicaScorer GetRemoteReplicaScorer() {
    std::shared_lock lk(detail::ScorerMutex());
    return detail::ScorerStorage();
}

// Inject a topology-/load-aware scorer (e.g. from the transfer engine).
// Takes a unique lock; expected to be called once during initialization.
inline void SetRemoteReplicaScorer(ReplicaScorer scorer) {
    std::unique_lock lk(detail::ScorerMutex());
    detail::ScorerStorage() = std::move(scorer);
}

// Built-in remote-replica score: prefer RDMA over TCP. Purely a function of
// info the replica already carries (protocol_), so it needs no topology.
inline double BuiltinRemoteReplicaScore(const Replica::Descriptor& r) {
    if (!r.is_memory_replica()) return 100.0;
    const std::string& proto =
        r.get_memory_descriptor().buffer_descriptor.protocol_;
    if (proto == "rdma") return 0.0;
    if (proto == "tcp") return 1.0;
    return 2.0;  // unknown protocol — least preferred, but still usable
}

// Whether remote-replica scoring is active. Opt-in via env; always on if a
// scorer has been injected. Env is read once; the injected-scorer check is
// live so tests / late injection take effect.
inline bool RemoteReplicaScoringEnabled() {
    static const bool env_enabled = [] {
        const char* env = std::getenv("MC_STORE_REPLICA_SCORING");
        return env && std::string(env) == "1";
    }();
    if (env_enabled) return true;
    std::shared_lock lk(detail::ScorerMutex());
    return static_cast<bool>(detail::ScorerStorage());
}

// Among the remote MEMORY replicas, return the lowest-scoring one (nullptr if
// none). Ties keep master return order (strictly-less comparison), so a
// symmetric cluster degrades to the historical "first remote MEMORY" pick.
inline const Replica::Descriptor* PickBestRemoteMemory(
    const std::vector<Replica::Descriptor>& replicas,
    const std::unordered_set<std::string>& local_endpoints) {
    ReplicaScorer scorer = GetRemoteReplicaScorer();
    const Replica::Descriptor* best = nullptr;
    double best_score = std::numeric_limits<double>::max();
    for (const auto& r : replicas) {
        if (r.status != ReplicaStatus::COMPLETE) continue;
        if (!r.is_memory_replica()) continue;
        if (local_endpoints.count(r.get_memory_descriptor()
                                      .buffer_descriptor.transport_endpoint_))
            continue;  // local replicas are handled by the caller
        double score = scorer ? scorer(r) : BuiltinRemoteReplicaScore(r);
        if (score < best_score) {
            best_score = score;
            best = &r;
        }
    }
    return best;
}

// Select the best replica from a list: prefer local MEMORY, then any MEMORY,
// then LOCAL_DISK, then DISK. Master may return replicas in any order, so we
// always scan. When scoring is enabled and there are multiple remote MEMORY
// replicas, the best-scoring one is chosen instead of the first encountered.
inline const Replica::Descriptor* SelectBestReplicaWithoutRemoteScoring(
    const std::vector<Replica::Descriptor>& replicas,
    const std::unordered_set<std::string>& local_endpoints) {
    const Replica::Descriptor* first_memory = nullptr;
    const Replica::Descriptor* first_nof = nullptr;
    for (const auto& r : replicas) {
        if (r.status != ReplicaStatus::COMPLETE) continue;
        if (r.is_memory_replica()) {
            if (local_endpoints.count(
                    r.get_memory_descriptor()
                        .buffer_descriptor.transport_endpoint_)) {
                return &r;  // local MEMORY — best case
            }
            if (!first_memory) first_memory = &r;
        } else if (r.is_nof_replica()) {
            if (local_endpoints.count(
                    r.get_nof_descriptor()
                        .buffer_descriptor.transport_endpoint_)) {
                return &r;  // local NOF_SSD — also good
            }
            if (!first_nof) first_nof = &r;
        }
    }
    if (first_memory) return first_memory;
    if (first_nof) return first_nof;

    const Replica::Descriptor* best = nullptr;
    for (const auto& r : replicas) {
        if (r.status != ReplicaStatus::COMPLETE) continue;
        if (r.is_local_disk_replica()) {
            best = &r;  // LOCAL_DISK always overrides DISK
        } else if (r.is_disk_replica() && !best) {
            best = &r;
        }
    }
    return best;
}

namespace detail {

// Apply the opt-in V1 remote-memory scorer to an already computed structural
// baseline. Keeping this step separate lets SHADOW reuse its own structural
// scan without changing any V1 enablement, scorer, or tie-breaking semantics.
inline const Replica::Descriptor* ApplyRemoteReplicaScoring(
    const std::vector<Replica::Descriptor>& replicas,
    const std::unordered_set<std::string>& local_endpoints,
    const Replica::Descriptor* baseline) {
    if (!baseline || !baseline->is_memory_replica()) return baseline;

    const auto& endpoint =
        baseline->get_memory_descriptor().buffer_descriptor.transport_endpoint_;
    if (local_endpoints.count(endpoint)) return baseline;

    if (RemoteReplicaScoringEnabled()) {
        if (const auto* scored =
                PickBestRemoteMemory(replicas, local_endpoints)) {
            return scored;
        }
    }
    return baseline;
}

}  // namespace detail

inline const Replica::Descriptor* SelectBestReplica(
    const std::vector<Replica::Descriptor>& replicas,
    const std::unordered_set<std::string>& local_endpoints) {
    const auto* baseline =
        SelectBestReplicaWithoutRemoteScoring(replicas, local_endpoints);
    return detail::ApplyRemoteReplicaScoring(replicas, local_endpoints,
                                             baseline);
}

}  // namespace mooncake
