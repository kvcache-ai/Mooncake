// mooncake-store/include/ha/oplog/p2p_standby_metadata_store.h
#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include <boost/functional/hash.hpp>

#include "ha/oplog/oplog_manager.h"
#include "metadata_store.h"
#include "replica.h"
#include "types.h"

namespace mooncake {

/// Client registration info stored in P2PStandbyMetadataStore.
struct P2PStandbyClientInfo {
    UUID client_id{0, 0};
    std::string ip_address;
    uint16_t rpc_port = 0;
    // Segments owned by this client.
    std::vector<Segment> segments;
};

/// P2P-specific standby metadata store.
///
/// Unlike main branch's MetadataStore which only stores object-level
/// StandbyObjectMetadata, P2P must also store client registration info
/// and segment mappings because:
///   1. P2P has no Snapshot fallback (main uses Snapshot + client reconnect)
///   2. Segment info directly affects GetWriteRoute routing
///   3. MOUNT_SEGMENT replay requires client_id → client record lookup
///
/// This class holds:
///   - objects_: key → replica list (mirrors Primary's object metadata)
///   - clients_: client_id → client info (ip, port, segments)
///
/// Thread safety: this class is NOT thread-safe. It is accessed by a single
/// thread — the OpLogReplicator callback thread writes via P2POpLogApplier,
/// and ExportMetadata() is called only after Replicator::Stop() has joined
/// the callback thread during promotion.
class P2PStandbyMetadataStore : public MetadataStore {
   public:
    P2PStandbyMetadataStore() = default;
    ~P2PStandbyMetadataStore() override = default;

    // ========================================================================
    // MetadataStore interface (object-level operations)
    // ========================================================================
    bool PutMetadata(const std::string& key,
                     const StandbyObjectMetadata& metadata) override;
    bool Put(const std::string& key,
             const std::string& payload = std::string()) override;
    std::optional<StandbyObjectMetadata> GetMetadata(
        const std::string& key) const override;
    bool Remove(const std::string& key) override;
    bool Exists(const std::string& key) const override;
    size_t GetKeyCount() const override;

    // ========================================================================
    // P2P-specific operations (called by P2POpLogApplier)
    // ========================================================================

    /// Add a replica to an object. Creates the object if it doesn't exist.
    void AddReplica(const std::string& object_key, const UUID& client_id,
                    const UUID& segment_id, size_t size, int priority,
                    const std::vector<std::string>& tags,
                    MemoryType memory_type);

    /// Remove a replica from an object. Removes the object if no replicas left.
    void RemoveReplica(const std::string& object_key, const UUID& client_id,
                       const UUID& segment_id);

    /// Register or update a client.
    void RegisterClient(const UUID& client_id, const std::string& ip_address,
                        uint16_t rpc_port,
                        const std::vector<Segment>& segments);

    /// Add (mount) a segment to a client.
    void AddSegment(const UUID& client_id, const Segment& segment);

    /// Remove (unmount) a segment from a client.
    /// Also removes all replicas on this segment from their objects (cascade
    /// delete).
    void RemoveSegment(const UUID& segment_id, const UUID& client_id);

    /// Remove all replicas on a segment from their objects (cascade helper).
    void RemoveReplicasBySegment(const UUID& segment_id);

    /// Remove all objects and client data. Used for REMOVE_ALL oplog entry.
    void RemoveAllMetadata();

    // ========================================================================
    // Export for Promotion
    // ========================================================================

    /// Export all metadata for promotion to Primary.
    /// This is used when Standby is promoted to Primary — the exported
    /// data is used to initialize a new P2PMasterService.
    struct ExportedMetadata {
        std::unordered_map<std::string, StandbyObjectMetadata> objects;
        std::unordered_map<UUID, P2PStandbyClientInfo, boost::hash<UUID>>
            clients;
    };

    ExportedMetadata ExportMetadata() const;

    // ========================================================================
    // Query helpers
    // ========================================================================

    /// Get client info by client_id. Returns nullptr if not found.
    const P2PStandbyClientInfo* GetClient(const UUID& client_id) const;

    /// Get all objects. For testing/diagnostics only.
    const std::unordered_map<std::string, StandbyObjectMetadata>& GetObjects()
        const {
        return objects_;
    }

    /// Get all clients. For testing/diagnostics only.
    const std::unordered_map<UUID, P2PStandbyClientInfo, boost::hash<UUID>>&
    GetClients() const {
        return clients_;
    }

   private:
    // Remove all replicas referencing a segment.
    void RemoveReplicasBySegmentInternal(const UUID& segment_id);

    // Object key → metadata (replicas, size, etc.)
    std::unordered_map<std::string, StandbyObjectMetadata> objects_;

    // Client UUID → client info (ip, port, segments)
    std::unordered_map<UUID, P2PStandbyClientInfo, boost::hash<UUID>> clients_;
};

}  // namespace mooncake