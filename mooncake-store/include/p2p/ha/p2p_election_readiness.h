#pragma once

#include <cstdint>
#include <functional>
#include <string>

#ifdef STORE_USE_REDIS
#include <vector>
#endif

#include "p2p/common/p2p_master_config.h"
#include "p2p/ha/oplog/p2p_hot_standby_service.h"

namespace mooncake {

struct P2PElectionStandbyLagDecision {
    bool should_wait{false};
    uint64_t lag_entries{0};
};

P2PElectionStandbyLagDecision EvaluateP2PElectionStandbyLag(
    bool latest_sequence_id_known, uint64_t latest_sequence_id,
    uint64_t applied_sequence_id, uint64_t max_lag_entries);

P2PElectionStandbyLagDecision EvaluateP2PElectionStandbyReadiness(
    StandbyState standby_state, bool latest_sequence_id_known,
    uint64_t latest_sequence_id, uint64_t applied_sequence_id,
    uint64_t max_lag_entries);

bool WaitForP2PElectionReadiness(const P2PMasterConfig& config,
                                 const std::string& self_instance_id,
                                 P2PHotStandbyService& standby,
                                 const std::function<bool()>& stopping);

#ifdef STORE_USE_REDIS
struct P2PElectionPriorityCandidate {
    std::string instance_id;
    std::string master_endpoint;
    std::string role;
    uint64_t applied_sequence_id{0};
};

struct P2PElectionPriorityDecision {
    bool should_yield{false};
    P2PElectionPriorityCandidate candidate;
};

P2PElectionPriorityDecision EvaluateP2PElectionPriority(
    const std::vector<P2PElectionPriorityCandidate>& candidates,
    const std::string& self_instance_id,
    const std::string& self_master_endpoint, uint64_t self_applied_sequence_id);

bool WaitForP2PElectionPriority(const P2PMasterConfig& config,
                                const std::string& self_instance_id,
                                P2PHotStandbyService& standby,
                                const std::function<bool()>& stopping);
#endif

}  // namespace mooncake
