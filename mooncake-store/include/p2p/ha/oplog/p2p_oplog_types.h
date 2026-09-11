// mooncake-store/include/ha/oplog/p2p_oplog_types.h
#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "p2p/ha/oplog/oplog_manager.h"
#include "p2p/common/p2p_types.h"
#include "types.h"

namespace mooncake {

// ============================================================================
// P2P OpType enum extensions
// ============================================================================
// Main branch uses OpType 1-4 (PUT_END, PUT_REVOKE, REMOVE, LEASE_RENEW).
// P2P starts from 10 to avoid conflicts with future main extensions (5-9).

// P2P-specific OpType values (use constexpr for switch-case)
constexpr OpType OpType_PUBLISH_ROUTE = static_cast<OpType>(10);
constexpr OpType OpType_WITHDRAW_ROUTE = static_cast<OpType>(11);
constexpr OpType OpType_MOUNT_SEGMENT = static_cast<OpType>(12);
constexpr OpType OpType_UNMOUNT_SEGMENT = static_cast<OpType>(13);
constexpr OpType OpType_REGISTER_CLIENT = static_cast<OpType>(15);
constexpr OpType OpType_UNREGISTER_CLIENT = static_cast<OpType>(16);

inline bool IsBestEffortP2POpLog(OpType type) {
    return type == OpType_PUBLISH_ROUTE;
}

// ============================================================================
// P2P Payload structures for OpLog entries
// ============================================================================
// Each payload is struct_pack-serialized and stored in OpLogEntry::payload.
// On the apply side, P2POpLogApplier deserializes back to the struct.
// Payload structs embed P2P data types directly.

/// Payload for REGISTER_CLIENT (OpType=15, sync).
/// Records client registration info so Standby can reconstruct routing
/// after promotion. Without this, Standby cannot serve GetWriteRoute.
struct RegisterClientPayload {
    UUID client_id{0, 0};
    std::string ip_address;
    uint16_t rpc_port = 0;
    // Segments registered by this client at registration time.
    std::vector<P2PSegment> segments;

    YLT_REFL(RegisterClientPayload, client_id, ip_address, rpc_port, segments);
};

/// Payload for UNREGISTER_CLIENT (OpType=16, sync).
/// Records that a client was proactively unregistered.
struct UnregisterClientPayload {
    UUID client_id{0, 0};

    YLT_REFL(UnregisterClientPayload, client_id);
};

/// Payload for PUBLISH_ROUTE (OpType=10, async).
/// Records a route location for an object.
struct PublishRoutePayload {
    std::string object_key;
    UUID client_id{0, 0};
    UUID segment_id{0, 0};
    size_t size = 0;

    YLT_REFL(PublishRoutePayload, object_key, client_id, segment_id, size);
};

/// Payload for WITHDRAW_ROUTE (OpType=11, sync).
/// Records that a route location was withdrawn from an object.
struct WithdrawRoutePayload {
    std::string object_key;
    UUID client_id{0, 0};
    UUID segment_id{0, 0};

    YLT_REFL(WithdrawRoutePayload, object_key, client_id, segment_id);
};

/// Payload for MOUNT_SEGMENT (OpType=12, sync).
/// Records that a segment was mounted by a client.
struct MountSegmentPayload {
    UUID client_id{0, 0};
    P2PSegment segment;

    YLT_REFL(MountSegmentPayload, client_id, segment);
};

/// Payload for UNMOUNT_SEGMENT (OpType=13, sync).
/// Records that a segment was unmounted. Standby must also remove
/// all routes referencing this client and segment (cascade delete).
struct UnmountSegmentPayload {
    UUID segment_id{0, 0};
    UUID client_id{0, 0};

    YLT_REFL(UnmountSegmentPayload, segment_id, client_id);
};

// ============================================================================
// P2P OpLog payload serialization helpers
// ============================================================================
// Payloads are stored in OpLogEntry::payload as struct_pack binary.
// P2P payloads have an independent schema from centralized metadata.

/// Serialize a P2P payload to binary (struct_pack format).
std::string SerializeP2PPayload(const RegisterClientPayload& payload);
std::string SerializeP2PPayload(const UnregisterClientPayload& payload);
std::string SerializeP2PPayload(const PublishRoutePayload& payload);
std::string SerializeP2PPayload(const WithdrawRoutePayload& payload);
std::string SerializeP2PPayload(const MountSegmentPayload& payload);
std::string SerializeP2PPayload(const UnmountSegmentPayload& payload);

/// Deserialize a binary payload to a P2P payload struct.
/// Returns true on success, false on deserialization error.
bool DeserializeP2PPayload(const std::string& data,
                           RegisterClientPayload& payload);
bool DeserializeP2PPayload(const std::string& data,
                           UnregisterClientPayload& payload);
bool DeserializeP2PPayload(const std::string& data,
                           PublishRoutePayload& payload);
bool DeserializeP2PPayload(const std::string& data,
                           WithdrawRoutePayload& payload);
bool DeserializeP2PPayload(const std::string& data,
                           MountSegmentPayload& payload);
bool DeserializeP2PPayload(const std::string& data,
                           UnmountSegmentPayload& payload);

}  // namespace mooncake
