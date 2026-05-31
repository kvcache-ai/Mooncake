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
 * Complete snapshot exported from standby at promotion time.
 * Includes applied OpLog sequence ID, all object metadata,
 * and all registered segments.
 */
struct StandbySnapshot {
    uint64_t oplog_sequence_id{0};
    std::vector<StandbySegmentInfo> segments;
    std::vector<std::pair<std::string, StandbyObjectMetadata>> objects;

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
    // NOTE: Lease information removed - not needed by Standby

    YLT_REFL(MetadataPayload, client_id, size, replicas);

    // Convert to StandbyObjectMetadata
    StandbyObjectMetadata ToStandbyMetadata(uint64_t sequence_id) const {
        StandbyObjectMetadata meta;
        meta.client_id = client_id;
        meta.size = size;
        meta.replicas = replicas;
        meta.last_sequence_id = sequence_id;
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

    /**
     * @brief Put or update metadata for a key with structured metadata
     * @param key Object key
     * @param metadata Structured metadata object
     * @return true on success, false on failure
     */
    virtual bool PutMetadata(const std::string& key,
                             const StandbyObjectMetadata& metadata) = 0;

    /**
     * @brief Put or update metadata for a key (legacy interface for backward
     * compatibility)
     * @param key Object key
     * @param payload Optional payload data (JSON serialized metadata)
     * @return true on success, false on failure
     */
    virtual bool Put(const std::string& key,
                     const std::string& payload = std::string()) = 0;

    /**
     * @brief Get metadata for a key
     * @param key Object key
     * @return Copy of metadata if found, std::nullopt otherwise
     */
    virtual std::optional<StandbyObjectMetadata> GetMetadata(
        const std::string& key) const = 0;

    /**
     * @brief Remove metadata for a key
     * @param key Object key
     * @return true if key was found and removed, false otherwise
     */
    virtual bool Remove(const std::string& key) = 0;

    /**
     * @brief Check if a key exists
     * @param key Object key
     * @return true if key exists, false otherwise
     */
    virtual bool Exists(const std::string& key) const = 0;

    /**
     * @brief Get the count of keys in the store
     * @return Number of keys
     */
    virtual size_t GetKeyCount() const = 0;
};

}  // namespace mooncake
