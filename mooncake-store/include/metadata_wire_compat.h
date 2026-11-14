#pragma once

#include "metadata_store.h"

namespace mooncake {
namespace metadata_wire_v2 {
// Wire shape before OBJECT_STORAGE was appended to the descriptor variant.
// Retain it solely to read supported pre-object logs and v1/v2 snapshots.
struct ReplicaDescriptor {
    ReplicaID id{0};
    std::variant<MemoryDescriptor, NoFDescriptor, DiskDescriptor,
                 LocalDiskDescriptor, DistributedFSDescriptor>
        descriptor_variant;
    ReplicaStatus status;
    YLT_REFL(ReplicaDescriptor, id, descriptor_variant, status);
};
struct Payload {
    UUID client_id{0, 0};
    uint64_t size{0};
    std::vector<ReplicaDescriptor> replicas;
    struct_pack::compatible<std::string, 1> group_id;
    struct_pack::compatible<ObjectDataType, 1> data_type;
    struct_pack::compatible<bool, 1> hard_pinned;
    struct_pack::compatible<std::string, 2> session_id;
    struct_pack::compatible<uint64_t, 3> object_checksum;
    struct_pack::compatible<std::string, 4> residency_affinity_id;
    YLT_REFL(Payload, client_id, size, replicas, group_id, data_type,
             hard_pinned, session_id, object_checksum, residency_affinity_id);
};
struct Metadata {
    UUID client_id{0, 0};
    uint64_t size{0};
    std::vector<ReplicaDescriptor> replicas;
    std::string group_id;
    ObjectDataType data_type{ObjectDataType::UNKNOWN};
    struct_pack::compatible<bool, 1> hard_pinned;
    struct_pack::compatible<std::string, 2> session_id;
    struct_pack::compatible<uint64_t, 3> object_checksum;
    struct_pack::compatible<std::string, 4> residency_affinity_id;
    YLT_REFL(Metadata, client_id, size, replicas, group_id, data_type,
             hard_pinned, session_id, object_checksum, residency_affinity_id);
};
struct Entry {
    std::string tenant_id{"default"};
    std::string key;
    Metadata metadata;
    YLT_REFL(Entry, tenant_id, key, metadata);
};
struct Chunk {
    uint64_t chunk_index{0};
    std::vector<Entry> objects;
    YLT_REFL(Chunk, chunk_index, objects);
};
inline std::vector<Replica::Descriptor> Convert(
    const std::vector<ReplicaDescriptor>& old) {
    std::vector<Replica::Descriptor> replicas;
    replicas.reserve(old.size());
    for (const auto& replica : old) {
        Replica::Descriptor converted;
        converted.id = replica.id;
        converted.status = replica.status;
        std::visit(
            [&](const auto& descriptor) {
                converted.descriptor_variant = descriptor;
            },
            replica.descriptor_variant);
        replicas.push_back(std::move(converted));
    }
    return replicas;
}
inline StandbyObjectMetadata Convert(const Metadata& old) {
    StandbyObjectMetadata metadata;
    metadata.client_id = old.client_id;
    metadata.size = old.size;
    metadata.replicas = Convert(old.replicas);
    metadata.group_id = old.group_id;
    metadata.data_type = old.data_type;
    metadata.hard_pinned = old.hard_pinned;
    metadata.session_id = old.session_id;
    metadata.object_checksum = old.object_checksum;
    metadata.residency_affinity_id = old.residency_affinity_id;
    return metadata;
}
}  // namespace metadata_wire_v2

inline struct_pack::err_code DeserializeMetadataPayload(
    MetadataPayload& payload, std::string_view bytes) {
    auto result = struct_pack::deserialize_to(payload, bytes);
    if (result == struct_pack::errc::ok) return result;
    metadata_wire_v2::Payload old;
    auto legacy = struct_pack::deserialize_to(old, bytes);
    if (legacy != struct_pack::errc::ok) return result;
    payload.client_id = old.client_id;
    payload.size = old.size;
    payload.replicas = metadata_wire_v2::Convert(old.replicas);
    payload.group_id = old.group_id;
    payload.data_type = old.data_type;
    payload.hard_pinned = old.hard_pinned;
    payload.session_id = old.session_id;
    payload.object_checksum = old.object_checksum;
    payload.residency_affinity_id = old.residency_affinity_id;
    return struct_pack::errc::ok;
}
}  // namespace mooncake
