#ifndef MOONCAKE_PG_CONTROL_PLANE_TYPES_H
#define MOONCAKE_PG_CONTROL_PLANE_TYPES_H

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

namespace mooncake {

// There are two rank namespaces that are easy to confuse:
//
//   * GlobalRank  - process-wide identifier, range 0 .. max_world_size-1.
//                   Used for process-level states
//   * InGroupRank - group-local identifier, range 0 .. group_size-1.
//                   Used inside a single process group and mapped to a
//                   GlobalRank through GroupView::rank_order.
using GlobalRank = int32_t;
using InGroupRank = int32_t;

using GroupId = std::string;

constexpr GlobalRank kInvalidGlobalRank = -1;
constexpr int kMaxNumRanks = 64;

// Process-level state for a rank.
// All transitions are driven by the Coordinator.
enum class RankState : uint8_t {
    Offline = 0,
    Synced = 1,
    Healthy = 2,
};

// Group-level, per-(group_id, rank) buffer/sync/P2P addresses.
struct GroupEndpointInfo {
    // Coordinator-assigned endpoint version.
    // The Agent publishes with 0 (it does not know the epoch);
    // the Coordinator fills it in before pushing the ViewUpdate.
    uint64_t endpoint_epoch = 0;

    // collective
    uint64_t send_buffer[2] = {};
    uint64_t recv_buffer[2] = {};
    uint64_t send_sync[2] = {};
    uint64_t recv_sync[2] = {};

    // p2p
    uint64_t p2p_credit_region = 0;
    uint64_t p2p_ack_region = 0;

    bool operator==(const GroupEndpointInfo&) const = default;
};

// Membership state of one rank inside a single GroupView.
enum class GroupMemberState : uint8_t {
    None = 0,      // slot has never belonged to this group
    Left = 1,      // explicitly left the group (called destroy_group)
    Inactive = 2,  // inactive member
    Active = 3,    // active member
};

// Rank state inside a single GroupView.
struct GroupMember {
    GroupMemberState status = GroupMemberState::None;
    std::optional<GroupEndpointInfo> endpoint;
    // session that published endpoint
    std::optional<uint64_t> agent_session_epoch;

    bool isActive() const { return status == GroupMemberState::Active; }
    bool isMember() const {
        return status == GroupMemberState::Active ||
               status == GroupMemberState::Inactive;
    }
    bool hasLeft() const { return status == GroupMemberState::Left; }
    bool hasEndpoint() const { return endpoint.has_value(); }

    bool operator==(const GroupMember&) const = default;
};

// Group lifecycle status.
//
//   Bootstrapping      - collecting endpoints and waiting for all active ranks
//                        to become Healthy with valid endpoints.
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

// Runtime state for a group.
struct GroupView {
    GroupId group_id;
    GroupStatus status = GroupStatus::Bootstrapping;
    uint64_t epoch = 0;
    bool auto_deactivate = true;
    std::vector<GlobalRank> rank_order;  // InGroupRank -> GlobalRank
    std::vector<GroupMember> members;    // indexed by GlobalRank

    bool operator==(const GroupView&) const = default;
};

struct LinkEvent {
    enum class EventType : uint8_t {
        None = 0,
        Success = 1,
        Failure = 2,
    };

    std::vector<EventType> events;
};

}  // namespace mooncake

#endif  // MOONCAKE_PG_CONTROL_PLANE_TYPES_H
