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
 * @brief Segment information retained on standby for promoted leader restore.
 *
 * This is intentionally lightweight: it captures the endpoint-level registry
 * semantics needed by standby promotion, not a full allocator reconstruction.
 */
struct StandbySegmentInfo {
    std::string segment_name;
    std::string transport_endpoint;
    uint64_t capacity{0};
    bool is_memory_segment{false};
    std::string file_path;
};

/**
 * @brief Thread-safe segment registry maintained from standby replication.
 */
class StandbySegmentRegistry {
   public:
    void Upsert(const StandbySegmentInfo& info) {
        std::lock_guard<std::shared_mutex> lock(mutex_);
        segments_[info.transport_endpoint] = info;
    }

    void Remove(const std::string& transport_endpoint) {
        std::lock_guard<std::shared_mutex> lock(mutex_);
        segments_.erase(transport_endpoint);
    }

    void Replace(const std::vector<StandbySegmentInfo>& segments) {
        std::lock_guard<std::shared_mutex> lock(mutex_);
        segments_.clear();
        for (const auto& segment : segments) {
            segments_[segment.transport_endpoint] = segment;
        }
    }

    void Snapshot(std::vector<StandbySegmentInfo>& out) const {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        out.clear();
        out.reserve(segments_.size());
        for (const auto& entry : segments_) {
            out.push_back(entry.second);
        }
    }

    std::optional<StandbySegmentInfo> Find(
        const std::string& transport_endpoint) const {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        auto it = segments_.find(transport_endpoint);
        if (it == segments_.end()) {
            return std::nullopt;
        }
        return it->second;
    }

    void Clear() {
        std::lock_guard<std::shared_mutex> lock(mutex_);
        segments_.clear();
    }

   private:
    mutable std::shared_mutex mutex_;
    std::unordered_map<std::string, StandbySegmentInfo> segments_;
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
