#include "p2p/ha/p2p_election_readiness.h"

#include <chrono>

#include <glog/logging.h>

#include "p2p/ha/ha_status.h"

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
            HAStatusManager::instance().SetElectionReadinessBlocked(false);
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
            HAStatusManager::instance().SetElectionReadinessBlocked(false);
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

        HAStatusManager::instance().SetElectionReadinessBlocked(
            true, readiness_decision.lag_entries,
            config.oplog.election_max_standby_lag_entries);
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
    HAStatusManager::instance().SetElectionReadinessBlocked(false);
    return false;
}

}  // namespace mooncake
