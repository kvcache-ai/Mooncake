// mooncake-store/include/ha/oplog/p2p_oplog_types.h
#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "ha/oplog/oplog_manager.h"
#include "replica.h"
#include "types.h"

namespace mooncake {

// ============================================================================
// P2P OpType enum extensions
// ============================================================================
// Main branch uses OpType 1-4 (PUT_END, PUT_REVOKE, REMOVE, LEASE_RENEW).
// P2P starts from 10 to avoid conflicts with future main extensions (5-9).

// P2P-specific OpType values (use constexpr for switch-case)
constexpr OpType OpType_ADD_REPLICA = static_cast<OpType>(10);
constexpr OpType OpType_REMOVE_REPLICA = static_cast<OpType>(11);
constexpr OpType OpType_MOUNT_SEGMENT = static_cast<OpType>(12);
constexpr OpType OpType_UNMOUNT_SEGMENT = static_cast<OpType>(13);
constexpr OpType OpType_REMOVE_ALL = static_cast<OpType>(14);
constexpr OpType OpType_REGISTER_CLIENT = static_cast<OpType>(15);

// ============================================================================
// P2P Payload structures for OpLog entries
// ============================================================================
// Each payload is struct_pack-serialized and stored in OpLogEntry::payload.
// On the apply side, P2POpLogApplier deserializes back to the struct.
// Payload structs embed complex types (Segment, vector<Segment>) directly,
// consistent with main branch's MetadataPayload which embeds
// vector<Replica::Descriptor> the same way.

/// Payload for REGISTER_CLIENT (OpType=15, sync).
/// Records client registration info so Standby can reconstruct routing
/// after promotion. Without this, Standby cannot serve GetWriteRoute.
struct RegisterClientPayload {
    UUID client_id{0, 0};
    std::string ip_address;
    uint16_t rpc_port = 0;
    // Segments registered by this client at registration time.
    std::vector<Segment> segments;

    YLT_REFL(RegisterClientPayload, client_id, ip_address, rpc_port,
              segments);
};

/// Payload for ADD_REPLICA (OpType=10, async).
/// Records that a replica was added to an object.
struct AddReplicaPayload {
    std::string object_key;
    UUID client_id{0, 0};
    UUID segment_id{0, 0};
    size_t size = 0;
    int priority = 0;
    std::vector<std::string> tags;
    MemoryType memory_type = MemoryType::DRAM;

    YLT_REFL(AddReplicaPayload, object_key, client_id, segment_id, size,
              priority, tags, memory_type);
};

/// Payload for REMOVE_REPLICA (OpType=11, async).
/// Records that a replica was removed from an object.
struct RemoveReplicaPayload {
    std::string object_key;
    UUID client_id{0, 0};
    UUID segment_id{0, 0};

    YLT_REFL(RemoveReplicaPayload, object_key, client_id, segment_id);
};

/// Payload for MOUNT_SEGMENT (OpType=12, sync).
/// Records that a segment was mounted by a client.
struct MountSegmentPayload {
    UUID client_id{0, 0};
    Segment segment;

    YLT_REFL(MountSegmentPayload, client_id, segment);
};

/// Payload for UNMOUNT_SEGMENT (OpType=13, sync).
/// Records that a segment was unmounted. Standby must also remove
/// all replicas referencing this segment (cascade delete).
struct UnmountSegmentPayload {
    UUID segment_id{0, 0};
    UUID client_id{0, 0};

    YLT_REFL(UnmountSegmentPayload, segment_id, client_id);
};

// ============================================================================
// P2P OpLog payload serialization helpers
// ============================================================================
// Payloads are stored in OpLogEntry::payload as struct_pack binary,
// consistent with main branch's ApplyPutEnd serialization format.
// This allows potential future cross-branch compatibility.

/// Serialize a P2P payload to binary (struct_pack format).
std::string SerializeP2PPayload(const RegisterClientPayload& payload);
std::string SerializeP2PPayload(const AddReplicaPayload& payload);
std::string SerializeP2PPayload(const RemoveReplicaPayload& payload);
std::string SerializeP2PPayload(const MountSegmentPayload& payload);
std::string SerializeP2PPayload(const UnmountSegmentPayload& payload);

/// Deserialize a binary payload to a P2P payload struct.
/// Returns true on success, false on deserialization error.
bool DeserializeP2PPayload(const std::string& data,
                           RegisterClientPayload& payload);
bool DeserializeP2PPayload(const std::string& data,
                           AddReplicaPayload& payload);
bool DeserializeP2PPayload(const std::string& data,
                           RemoveReplicaPayload& payload);
bool DeserializeP2PPayload(const std::string& data,
                           MountSegmentPayload& payload);
bool DeserializeP2PPayload(const std::string& data,
                           UnmountSegmentPayload& payload);

}  // namespace mooncake