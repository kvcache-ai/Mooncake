#ifndef MOONCAKE_PG_CONTROL_PLANE_CONTROL_TYPES_H
#define MOONCAKE_PG_CONTROL_PLANE_CONTROL_TYPES_H

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include <ylt/struct_pack.hpp>

#include "common_types.h"
#include "error_types.h"

namespace mooncake {

// Bootstrap ID: Backend type ("cpu:" or "device:") + PyTorch-assigned group_id.
using GroupBootstrapId = std::string;
// Coordinator-assigned unique group id
using GroupId = std::string;

// Resolves a registration only against runtime groups stored under the same
// GroupBootstrapId, i.e. the same device kind and PyTorch group id.
// An exact match requires both rank_order and max_group_size to be equal.
//
// CreateOrAttach:
//   * Attach when exactly one existing group is an exact match.
//   * Create a new group when there is no exact match.
//   * Reject multiple exact matches and never modify an existing rank_order.
//
// AttachOrExtend:
//   * Attach when exactly one existing group is an exact match.
//   * Otherwise, extend only when exactly one existing rank_order is a proper
//     prefix, max_group_size matches, and the registering rank is in the
//     appended suffix.
//   * Reject zero or multiple compatible groups and never create a new
//     group.
enum class GroupBootstrapIdResolvePolicy : uint8_t {
    CreateOrAttach = 0,
    AttachOrExtend = 1,
};

// Process-level state for a rank.
// All transitions are driven by the Coordinator.
enum class RankState : uint8_t {
    Offline = 0,
    Synced = 1,
    Healthy = 2,
};

// Generic endpoint for one route/algorithm/protocol/... provider.
struct ProviderEndpoint {
    std::string key;
    uint32_t version = 0;
    std::vector<uint8_t> metadata;

    template <typename T>
    [[nodiscard]] static ProviderEndpoint encode(std::string_view key,
                                                 uint32_t version,
                                                 const T& value) {
        return {std::string(key), version,
                struct_pack::serialize<std::vector<uint8_t>>(value)};
    }

    // Check the provider's expected key and version before decoding the
    // payload.
    template <typename T>
    [[nodiscard]] PGResult<T> decode(std::string_view expected_key,
                                     uint32_t expected_version) const {
        PG_VALIDATE_ARG(key == expected_key, "endpoint key mismatch");
        PG_VALIDATE_ARG(version == expected_version,
                        "endpoint version mismatch");
        PG_VALIDATE_ARG(!metadata.empty(), "serialized data is empty");
        T value;
        size_t consumed = 0;
        const auto error = struct_pack::deserialize_to(
            value, reinterpret_cast<const char*>(metadata.data()),
            metadata.size(), consumed);
        PG_VALIDATE_ARG(
            !error, "deserialization failed: " + std::string(error.message()));
        PG_VALIDATE_ARG(consumed == metadata.size(),
                        "serialized data contains trailing bytes");
        return value;
    }

    bool operator==(const ProviderEndpoint&) const = default;
};

// Find a matching key and version, or return nullptr if none is advertised.
[[nodiscard]] inline const ProviderEndpoint* findProviderEndpoint(
    std::span<const ProviderEndpoint> endpoints, std::string_view key,
    uint32_t version) {
    const ProviderEndpoint* match = nullptr;
    for (const auto& endpoint : endpoints) {
        if (endpoint.key != key || endpoint.version != version) continue;
        PG_ASSERT(!match, "duplicate provider endpoint key and version");
        match = &endpoint;
    }
    return match;
}

// Route-specific bootstrap metadata published by one device transfer service.
using RouteEndpoint = ProviderEndpoint;

// Communicator-local metadata for protocols and algorithms.
using ProtocolEndpoint = ProviderEndpoint;
using AlgorithmEndpoint = ProviderEndpoint;

// Process-level collective buffer in DTS's peer-accessible region.
struct DeviceCollectiveWorkspaceEndpoint {
    uint64_t buffer_offset = 0;
    uint64_t buffer_size = 0;

    bool operator==(const DeviceCollectiveWorkspaceEndpoint&) const = default;
};

// Process-level metadata for the device transfer service.
struct DeviceTransferEndpoint {
    uint64_t region_address = 0;
    uint64_t region_size = 0;

    std::vector<RouteEndpoint> routes;
    // Orders publications within a rank epoch. Publishing alone does not
    // request an installation or invalidate installed resources.
    uint64_t version = 0;

    bool operator==(const DeviceTransferEndpoint&) const = default;
};

// Process-level endpoints selected by the Coordinator for one installation.
// endpoints and rank_epochs are indexed by GlobalRank.
struct DeviceTransferSnapshot {
    std::vector<std::optional<DeviceTransferEndpoint>> endpoints;
    std::vector<uint64_t> rank_epochs;
    std::vector<GlobalRank> participants;
    uint64_t version = 0;

    bool operator==(const DeviceTransferSnapshot&) const = default;
};

// Reachability and capabilities of the installed route to one GlobalRank.
// Used by the control plane and callers outside DTS, without exposing the
// route-specific data in DeviceTransferRoute.
struct DeviceTransferRouteInfo {
    bool reachable = false;
    bool directly_addressable = false;
    // Whether the source rank's GPU can issue CUDA system-scope atomics to
    // the destination rank's directly mapped GPU memory.
    bool native_atomics = false;

    bool operator==(const DeviceTransferRouteInfo&) const = default;
};

// Route capabilities and any local endpoint update from one installation.
struct DeviceTransferInstallResult {
    std::optional<DeviceTransferEndpoint> updated_endpoint;
    std::vector<DeviceTransferRouteInfo> routes;
};

enum class DeviceCollectiveAlgorithm : uint8_t {
    Ring = 1,
    OneShot = 2,
};

// Runtime, protocol and algorithm endpoints owned by one communicator.
struct DeviceGroupEndpoint {
    // Runtime-owned signal slice for synchronizing one GroupView incarnation.
    // Each slot is written only by the peer with the matching InGroupRank.
    uint64_t view_epoch_signal = 0;
    uint32_t view_epoch_signal_count = 0;

    // Available local algorithm implementations
    std::vector<DeviceCollectiveAlgorithm> supported_algorithms;

    std::vector<ProtocolEndpoint> protocols;
    std::vector<AlgorithmEndpoint> algorithms;

    [[nodiscard]] bool empty() const noexcept {
        return view_epoch_signal_count == 0 && supported_algorithms.empty() &&
               protocols.empty() && algorithms.empty();
    }

    [[nodiscard]] bool supportsAlgorithm(
        DeviceCollectiveAlgorithm algorithm) const noexcept {
        return std::find(supported_algorithms.begin(),
                         supported_algorithms.end(),
                         algorithm) != supported_algorithms.end();
    }

    bool operator==(const DeviceGroupEndpoint&) const = default;
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

    // Empty for CPU communicators and ranks without device algorithms.
    DeviceGroupEndpoint device_collective;

    bool operator==(const GroupEndpointInfo&) const = default;
};

// State of one rank inside a GroupView.
//
// Founding member:
//   None ------------(initial group declaration)-----------> Active
//
// Joining member:
//   None ----------------(registerGroup)-------------------> Inactive
//   Inactive ---(joinGroup confirms local preparation)-----> AwaitingActivation
//   AwaitingActivation --(Coordinator activates the rank)--> Active
//
// Active -----------------------(deactivate)-----------------------> Inactive
// AwaitingActivation ------(new agent session or offline)----------> Inactive
// Inactive/AwaitingActivation/Active ----(unregisterGroup)---------> Left
// Left --------------------------(registerGroup)-------------------> Inactive
//
// Only Active participates in collectives.
// AwaitingActivation may become activatable once its endpoint, health, and
// connectivity are ready; Inactive may not.
enum class GroupMemberState : uint8_t {
    None = 0,                // slot has not registered with this group
    Inactive = 1,            // registered, but not ready for activation
    AwaitingActivation = 2,  // local join preparation is complete
    Active = 3,              // committed collective participant
    Left = 4,                // explicitly left (called destroy_group)
};

// Rank state inside a single GroupView.
struct GroupMember {
    GroupMemberState status = GroupMemberState::None;
    // GPU ranks advertise a concrete preference at registration. CPU ranks
    // leave it empty because this decision does not govern CPU collectives.
    std::optional<GpuCollectiveBackend> preferred_gpu_collective_backend;
    std::optional<GroupEndpointInfo> endpoint;

    bool isNone() const { return status == GroupMemberState::None; }
    bool isActive() const { return status == GroupMemberState::Active; }
    bool isAwaitingActivation() const {
        return status == GroupMemberState::AwaitingActivation;
    }
    bool isMember() const {
        return status == GroupMemberState::Inactive ||
               status == GroupMemberState::AwaitingActivation ||
               status == GroupMemberState::Active;
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

// Coordinator-provided algorithm policy in priority order for each operation.
// The first matching operation and inclusive byte bound selects the
// implementation.
struct DeviceCollectiveAlgorithmChoice {
    OpType operation = OpType::Unknown;
    uint64_t max_bytes = 0;
    DeviceCollectiveAlgorithm algorithm = DeviceCollectiveAlgorithm::Ring;

    bool operator==(const DeviceCollectiveAlgorithmChoice&) const = default;
};

// Runtime state for a group.
struct GroupView {
    GroupId group_id;
    GroupStatus status = GroupStatus::Bootstrapping;
    uint64_t epoch = 0;
    bool auto_deactivate = true;
    // Coordinator-selected implementation for active GPU group members. It is
    // empty for CPU groups and until a GPU group leaves Bootstrapping.
    std::optional<GpuCollectiveBackend> gpu_collective_backend;
    std::vector<DeviceCollectiveAlgorithmChoice> collective_algorithm_choices;
    int32_t max_group_size = 0;          // fixed in-group slot capacity
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
    // The Coordinator-assigned epoch of the target rank observed by the
    // event source.  This is parallel to events and prevents a late event for
    // an old process incarnation from being attributed to its replacement.
    std::vector<uint64_t> target_rank_epochs;
};

}  // namespace mooncake

#endif  // MOONCAKE_PG_CONTROL_PLANE_CONTROL_TYPES_H
