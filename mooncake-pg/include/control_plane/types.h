#ifndef MOONCAKE_PG_CONTROL_PLANE_TYPES_H
#define MOONCAKE_PG_CONTROL_PLANE_TYPES_H

#include <cstdint>
#include <vector>

namespace mooncake {

using GlobalRank = int32_t;   // max_world_size namespace
using GroupId = int32_t;      // process group ID (= backendIndex)
using InGroupRank = int32_t;  // group-local namespace

constexpr GlobalRank kInvalidGlobalRank = -1;
constexpr GroupId kInvalidGroupId = -1;
constexpr int kMaxNumRanks = 64;

// Epoch sentinels.  All epochs start at kInvalidEpoch (0) and only increase
// from there.  A value of kInvalidEpoch means "not yet initialised" / "stale".
// The first real epoch value is >= 1.
constexpr uint64_t kInvalidEpoch = 0;
constexpr uint64_t kInitialEndpointEpoch = 1;

// Process-level state for a rank.  All transitions are driven by the
// Coordinator (authoritative); the Agent never self-demotes.
//
//   registerAgent()         Coordinator computes TE HealthySet
//        |                  (heartbeat + transfer observations)
//        v                         |
//   OFFLINE -> SYNCED ---------> HEALTHY
//        ^                         |
//        |   heartbeat timeout     |   excluded from HealthySet
//        +--- or disconnect -------+-----> SYNCED
//
enum class RankState : uint8_t {
    OFFLINE = 0,
    SYNCED = 1,
    HEALTHY = 2,
};

// Group-level, per-(group_id, rank) buffer/sync/P2P addresses.
struct GroupEndpointInfo {
    // collective
    uint64_t send_buffer[2] = {};
    uint64_t recv_buffer[2] = {};
    uint64_t send_sync[2] = {};
    uint64_t recv_sync[2] = {};
    // p2p
    uint64_t p2p_credit_region = 0;
    uint64_t p2p_ack_region = 0;
};

// Rank state inside a single GroupView.
//
// endpoint_epoch is monotonically increasing and never reset.  kInvalidEpoch
// means "no endpoint published yet".  Validity requires agent_session_epoch
// to match the rank's current session AND endpoint_epoch != kInvalidEpoch.
struct GroupMember {
    bool active = false;
    uint64_t agent_session_epoch = kInvalidEpoch;
    uint64_t endpoint_epoch = kInvalidEpoch;
    GroupEndpointInfo endpoint_info;
};

// Group lifecycle status.
//
//   declareGroup()
//       |
//       v
//   Bootstrapping  -- all active ranks HEALTHY + have endpoints -->
//   BootstrapSyncing
//       (waiting for       (Coordinator broadcasts ViewUpdate,
//        publishEndpoint     waits for ACKs from all active ranks)
//        calls)                        |
//                                      v  (all ACKs received)
//                                  Ready
//                               (group usable for data-plane transfers)
//
//   Bootstrapping      - collecting endpoints and waiting for all active ranks
//                        to become HEALTHY with valid endpoints.
//   BootstrapSyncing   - Coordinator initiated 2PC barrier; waiting for all
//                        active ranks to ACK the initial ViewUpdate.
//                        If a peer dies here, waitUntilGroupReady() hangs
//                        until its timeout.
//   Ready              - barrier complete; all ranks ready for data-plane
//                        transfers.
enum class GroupStatus : uint8_t {
    Bootstrapping = 0,
    BootstrapSyncing = 1,
    Ready = 2,
};

// Runtime states for a group.
// epoch starts at 0 and monotonically increases; the first real epoch after
// bootstrap completion is 1.
struct GroupView {
    GroupId group_id = 0;
    GroupStatus status = GroupStatus::Bootstrapping;
    uint64_t epoch = 0;
    std::vector<GroupMember> members;  // size == max_world_size_
};

// Group identity
struct GroupDescriptor {
    GroupId group_id = 0;
    std::vector<GlobalRank> rank_order;
};

// GroupDeclaration, submitted by each backend at creation time
struct GroupDeclaration {
    GroupDescriptor descriptor;
    GroupView initial_view;
    bool auto_deactivate = true;
};

// TransferObservationEvent, worker thread -> Agent queue
struct TransferObservationEvent {
    GroupId group_id = 0;
    std::vector<uint8_t> attempted_ranks;
    std::vector<uint8_t> failed_ranks;
    std::vector<uint8_t> succeeded_ranks;
};

}  // namespace mooncake

#endif  // MOONCAKE_PG_CONTROL_PLANE_TYPES_H
