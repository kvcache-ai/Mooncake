#pragma once

#include <cstddef>
#include <optional>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "replica.h"
#include "types.h"

namespace mooncake {

/**
 * @brief Metadata structure for Standby to store and restore object information
 *
 * This structure contains all essential metadata information needed by Standby
 * to immediately serve as Primary when promoted.
 */
struct StandbyObjectMetadata {
    UUID client_id{0, 0};
    uint64_t size{0};
    std::vector<Replica::Descriptor> replicas;
    // NOTE: Lease information is NOT stored because:
    // 1. Standby does not perform eviction, so lease info is not used
    // 2. After promotion, new Primary should grant fresh leases, not restore
    // old ones
    uint64_t last_sequence_id{
        0};  // Last OpLog sequence ID that modified this key
    std::string group_id;                    // Tenant group identifier
    ObjectDataType data_type{ObjectDataType::UNKNOWN};  // Data type classification

    StandbyObjectMetadata() = default;

    // Check if this metadata has valid replicas
    bool HasReplicas() const { return !replicas.empty(); }
};

/**
 * Segment info stored in standby's segment registry.
 * Used for recovering segment view after promotion.
 */
struct StandbySegmentInfo {
    std::string segment_name;
    std::string transport_endpoint;
    uint64_t capacity{0};
    bool is_memory_segment{false};
    std::string file_path;  // empty for memory segments

    YLT_REFL(StandbySegmentInfo, segment_name, transport_endpoint, capacity,
             is_memory_segment, file_path);
};

/**
 * @brief Standby object entry with tenant-aware key
 *
 * Replaces std::pair<std::string, StandbyObjectMetadata> for cleaner
 * struct_pack serialization and explicit tenant_id support.
 */
struct StandbyObjectEntry {
    std::string tenant_id{"default"};
    std::string key;
    StandbyObjectMetadata metadata;

    YLT_REFL(StandbyObjectEntry, tenant_id, key, metadata);
};

/**
 * Complete snapshot exported from standby at promotion time.
 * Includes applied OpLog sequence ID, all object metadata,
 * and all registered segments.
 */
struct StandbySnapshot {
    uint64_t oplog_sequence_id{0};
    std::vector<StandbySegmentInfo> segments;
    std::vector<StandbyObjectEntry> objects;

    YLT_REFL(StandbySnapshot, oplog_sequence_id, segments, objects);
};

/**
 * @brief Payload structure for struct_pack serialization (msgpack binary
 * format)
 *
 * Now uses UUID directly since struct_pack natively supports std::pair.
 */
struct MetadataPayload {
    UUID client_id{0, 0};
    uint64_t size{0};
    std::vector<Replica::Descriptor> replicas;
    struct_pack::compatible<std::string, 1> group_id;       // Tenant group
    struct_pack::compatible<ObjectDataType, 1> data_type;   // Data type

    YLT_REFL(MetadataPayload, client_id, size, replicas, group_id, data_type);

    // Convert to StandbyObjectMetadata
    StandbyObjectMetadata ToStandbyMetadata(uint64_t sequence_id) const {
        StandbyObjectMetadata meta;
        meta.client_id = client_id;
        meta.size = size;
        meta.replicas = replicas;
        meta.last_sequence_id = sequence_id;
        meta.group_id = group_id.value_or("");
        meta.data_type = data_type.value_or(ObjectDataType::UNKNOWN);
        return meta;
    }
};

/**
 * Thread-safe registry of segments known to standby.
 * Maintained by applying SEGMENT_MOUNT/UNMOUNT/UPDATE OpLog events.
 * Used to reconstruct segment view after promotion.
 */
class StandbySegmentRegistry {
  public:
    StandbySegmentRegistry() = default;

    // Segment lifecycle events
    void OnSegmentMount(const StandbySegmentInfo& info);
    void OnSegmentUnmount(const std::string& transport_endpoint);
    void OnSegmentUpdate(const StandbySegmentInfo& info);

    // Queries
    bool HasSegment(const std::string& transport_endpoint) const;
    std::optional<StandbySegmentInfo> GetSegment(
        const std::string& transport_endpoint) const;
    std::vector<StandbySegmentInfo> GetAllSegments() const;
    void Clear();

  private:
    mutable std::shared_mutex mutex_;
    std::unordered_map<std::string, StandbySegmentInfo> segments_by_endpoint_;
};

/**
 * @brief Abstract interface for metadata storage on Standby
 *
 * This interface provides basic operations for storing and managing object
 * metadata. In a full implementation, this would mirror MasterService's
 * metadata_shards_ structure.
 */
class MetadataStore {
   public:
    virtual ~MetadataStore() = default;

    // NEW: tenant-aware methods (primary API)
    virtual bool PutMetadata(const std::string& tenant_id,
                             const std::string& key,
                             const StandbyObjectMetadata& metadata) = 0;
    virtual std::optional<StandbyObjectMetadata> GetMetadata(
        const std::string& tenant_id, const std::string& key) const = 0;
    virtual bool Remove(const std::string& tenant_id,
                        const std::string& key) = 0;
    virtual bool Exists(const std::string& tenant_id,
                        const std::string& key) const = 0;
    virtual size_t GetKeyCountForTenant(const std::string& tenant_id) const = 0;

    // DEPRECATED: key-only overloads delegate to tenant-aware with "default"
    virtual bool PutMetadata(const std::string& key,
                             const StandbyObjectMetadata& metadata) {
        return PutMetadata("default", key, metadata);
    }
    virtual std::optional<StandbyObjectMetadata> GetMetadata(
        const std::string& key) const {
        return GetMetadata("default", key);
    }
    virtual bool Remove(const std::string& key) {
        return Remove("default", key);
    }
    virtual bool Exists(const std::string& key) const {
        return Exists("default", key);
    }

    // NOTE: legacy Put(key, payload) remains as default-tenant delegate.
    // StandbyMetadataStore and MockMetadataStore continue to implement it.
    virtual bool Put(const std::string& key,
                     const std::string& payload = std::string()) = 0;

    // GetKeyCount semantics unchanged - returns total across ALL tenants
    virtual size_t GetKeyCount() const = 0;
};

}  // namespace mooncake
