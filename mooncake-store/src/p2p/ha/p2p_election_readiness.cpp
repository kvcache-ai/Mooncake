#include "p2p/ha/p2p_election_readiness.h"

#include <chrono>
#include <utility>

#include <glog/logging.h>

#ifdef STORE_USE_REDIS
#include "p2p/ha/oplog/p2p_standby_snapshot_service.h"
#endif

namespace mooncake {
namespace {

constexpr auto kP2PElectionReadinessCheckInterval =
    std::chrono::milliseconds(200);

}  // namespace

P2PElectionStandbyLagDecision EvaluateP2PElectionStandbyLag(
    bool latest_sequence_id_known, uint64_t latest_sequence_id,
    uint64_t applied_sequence_id, uint64_t max_lag_entries) {
    P2PElectionStandbyLagDecision decision;
    if (!latest_sequence_id_known) {
        decision.should_wait = true;
        return decision;
    }
    if (latest_sequence_id <= applied_sequence_id) {
        return decision;
    }

    decision.lag_entries = latest_sequence_id - applied_sequence_id;
    decision.should_wait = decision.lag_entries > max_lag_entries;
    return decision;
}

P2PElectionStandbyLagDecision EvaluateP2PElectionStandbyReadiness(
    StandbyState standby_state, bool latest_sequence_id_known,
    uint64_t latest_sequence_id, uint64_t applied_sequence_id,
    uint64_t max_lag_entries) {
    if (standby_state != StandbyState::WATCHING) {
        P2PElectionStandbyLagDecision decision;
        decision.should_wait = true;
        return decision;
    }
    return EvaluateP2PElectionStandbyLag(latest_sequence_id_known,
                                         latest_sequence_id,
                                         applied_sequence_id, max_lag_entries);
}

bool WaitForP2PElectionReadiness(const P2PMasterConfig& config,
                                 const std::string& self_instance_id,
                                 P2PHotStandbyService& standby,
                                 const std::function<bool()>& stopping) {
    bool waited = false;
    while (!stopping()) {
        const auto self_status = standby.GetSyncStatus();
        auto readiness_decision = EvaluateP2PElectionStandbyReadiness(
            self_status.state, self_status.primary_seq_id_known,
            self_status.primary_seq_id, self_status.applied_seq_id,
            config.oplog.election_max_standby_lag_entries);
        if (!readiness_decision.should_wait) {
            if (waited) {
                LOG(INFO) << "P2P election readiness wait completed"
                          << ", self_instance_id=" << self_instance_id
                          << ", self_endpoint=" << config.rpc.address
                          << ", standby_state="
                          << StandbyStateToString(self_status.state)
                          << ", self_applied_sequence_id="
                          << self_status.applied_seq_id
                          << ", latest_sequence_id_known="
                          << self_status.primary_seq_id_known
                          << ", latest_sequence_id="
                          << self_status.primary_seq_id;
            }
            return true;
        }

        waited = true;
        if (self_status.state != StandbyState::WATCHING) {
            LOG_EVERY_N(WARNING, 30)
                << "P2P election waiting for standby to become "
                   "promotion-ready"
                << ", self_instance_id=" << self_instance_id
                << ", self_endpoint=" << config.rpc.address
                << ", standby_state="
                << StandbyStateToString(self_status.state);
            std::this_thread::sleep_for(kP2PElectionReadinessCheckInterval);
            continue;
        }

        LOG_EVERY_N(INFO, 10)
            << "P2P election waiting for standby catch-up before election"
            << ", self_instance_id=" << self_instance_id
            << ", self_endpoint=" << config.rpc.address
            << ", self_applied_sequence_id=" << self_status.applied_seq_id
            << ", latest_sequence_id_known=" << self_status.primary_seq_id_known
            << ", latest_sequence_id=" << self_status.primary_seq_id
            << ", lag_entries=" << readiness_decision.lag_entries
            << ", max_lag_entries="
            << config.oplog.election_max_standby_lag_entries;
        std::this_thread::sleep_for(kP2PElectionReadinessCheckInterval);
    }
    return false;
}

#ifdef STORE_USE_REDIS
namespace {

constexpr int kRedisMasterRegistryTtlSeconds = 10;
constexpr auto kP2PElectionPriorityCheckInterval =
    std::chrono::milliseconds(200);

std::vector<P2PElectionPriorityCandidate> BuildP2PElectionCandidates(
    const std::vector<RedisMasterRegistryEntry>& entries) {
    std::vector<P2PElectionPriorityCandidate> candidates;
    candidates.reserve(entries.size());
    for (const auto& entry : entries) {
        P2PElectionPriorityCandidate candidate;
        candidate.instance_id = entry.instance_id;
        candidate.master_endpoint = entry.master_endpoint;
        candidate.role = entry.role;
        candidate.applied_sequence_id = entry.applied_sequence_id;
        candidates.push_back(std::move(candidate));
    }
    return candidates;
}

}  // namespace

P2PElectionPriorityDecision EvaluateP2PElectionPriority(
    const std::vector<P2PElectionPriorityCandidate>& candidates,
    const std::string& self_instance_id,
    const std::string& self_master_endpoint,
    uint64_t self_applied_sequence_id) {
    P2PElectionPriorityDecision decision;
    for (const auto& candidate : candidates) {
        if (candidate.role != "standby") {
            continue;
        }
        if (!self_instance_id.empty() &&
            candidate.instance_id == self_instance_id) {
            continue;
        }
        if (!self_master_endpoint.empty() &&
            candidate.master_endpoint == self_master_endpoint) {
            continue;
        }
        if (candidate.applied_sequence_id <= self_applied_sequence_id) {
            continue;
        }
        if (!decision.should_yield ||
            candidate.applied_sequence_id >
                decision.candidate.applied_sequence_id ||
            (candidate.applied_sequence_id ==
                 decision.candidate.applied_sequence_id &&
             candidate.master_endpoint < decision.candidate.master_endpoint)) {
            decision.should_yield = true;
            decision.candidate = candidate;
        }
    }
    return decision;
}

bool WaitForP2PElectionPriority(const P2PMasterConfig& config,
                                const std::string& self_instance_id,
                                P2PHotStandbyService& standby,
                                const std::function<bool()>& stopping) {
    RedisMasterRegistry registry(config.cluster_id, config.redis.endpoint,
                                 config.redis.username, config.redis.password,
                                 config.redis.db_index);
    const auto yield_deadline =
        std::chrono::steady_clock::now() +
        std::chrono::seconds(kRedisMasterRegistryTtlSeconds * 2);
    bool waited = false;
    while (!stopping()) {
        const auto self_status = standby.GetSyncStatus();
        std::vector<RedisMasterRegistryEntry> entries;
        auto err = registry.DiscoverAlive(
            std::chrono::seconds(kRedisMasterRegistryTtlSeconds), entries);
        if (err != ErrorCode::OK) {
            LOG_EVERY_N(WARNING, 30)
                << "P2P election priority check failed; proceeding with "
                   "normal Redis election"
                << ", error=" << toString(err);
            return true;
        }

        auto decision = EvaluateP2PElectionPriority(
            BuildP2PElectionCandidates(entries), self_instance_id,
            config.rpc.address + ":" + std::to_string(config.rpc.port),
            self_status.applied_seq_id);
        if (!decision.should_yield) {
            if (waited) {
                LOG(INFO) << "P2P election priority wait completed"
                          << ", self_instance_id=" << self_instance_id
                          << ", self_endpoint=" << config.rpc.address
                          << ", self_applied_sequence_id="
                          << self_status.applied_seq_id
                          << ", latest_sequence_id_known="
                          << self_status.primary_seq_id_known
                          << ", latest_sequence_id="
                          << self_status.primary_seq_id;
            }
            return true;
        }

        if (std::chrono::steady_clock::now() >= yield_deadline) {
            LOG(WARNING)
                << "P2P election priority yield timeout; proceeding with "
                   "normal Redis election"
                << ", self_instance_id=" << self_instance_id
                << ", self_endpoint=" << config.rpc.address
                << ", self_applied_sequence_id=" << self_status.applied_seq_id
                << ", higher_instance_id=" << decision.candidate.instance_id
                << ", higher_endpoint=" << decision.candidate.master_endpoint
                << ", higher_applied_sequence_id="
                << decision.candidate.applied_sequence_id;
            return true;
        }

        waited = true;
        LOG_EVERY_N(INFO, 10)
            << "P2P election yielding to higher applied standby"
            << ", self_instance_id=" << self_instance_id
            << ", self_endpoint=" << config.rpc.address
            << ", self_applied_sequence_id=" << self_status.applied_seq_id
            << ", higher_instance_id=" << decision.candidate.instance_id
            << ", higher_endpoint=" << decision.candidate.master_endpoint
            << ", higher_applied_sequence_id="
            << decision.candidate.applied_sequence_id;
        std::this_thread::sleep_for(kP2PElectionPriorityCheckInterval);
    }
    return false;
}
#endif

}  // namespace mooncake
