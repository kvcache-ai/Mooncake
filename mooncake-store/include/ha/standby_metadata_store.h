#pragma once

#include <cstddef>
#include <map>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "metadata_store.h"

namespace mooncake {

class StandbyMetadataStore final : public MetadataStore {
    using ObjectStore = std::unordered_map<std::string, StandbyObjectMetadata>;
    using TenantStore = std::unordered_map<std::string, ObjectStore>;

   public:
    class SnapshotCursor {
       public:
        SnapshotCursor(SnapshotCursor&&) = default;
        SnapshotCursor& operator=(SnapshotCursor&&) = default;
        SnapshotCursor(const SnapshotCursor&) = delete;
        SnapshotCursor& operator=(const SnapshotCursor&) = delete;

        bool done() const { return done_; }

       private:
        friend class StandbyMetadataStore;

        SnapshotCursor(const StandbyMetadataStore* store,
                       TenantStore::const_iterator tenant,
                       TenantStore::const_iterator tenant_end,
                       ObjectStore::const_iterator object, bool done)
            : store_(store),
              tenant_(tenant),
              tenant_end_(tenant_end),
              object_(object),
              done_(done) {}

        const StandbyMetadataStore* store_;
        TenantStore::const_iterator tenant_;
        TenantStore::const_iterator tenant_end_;
        ObjectStore::const_iterator object_;
        bool done_;
    };

    using MetadataStore::Exists;
    using MetadataStore::GetMetadata;
    using MetadataStore::PutMetadata;
    using MetadataStore::Remove;

    bool PutMetadata(const std::string& tenant_id, const std::string& key,
                     const StandbyObjectMetadata& metadata) override;
    bool RestoreMetadata(const std::string& tenant_id, const std::string& key,
                         const StandbyObjectMetadata& metadata);
    bool Put(const std::string& key,
             const std::string& payload = std::string()) override;
    std::optional<StandbyObjectMetadata> GetMetadata(
        const std::string& tenant_id, const std::string& key) const override;
    bool Remove(const std::string& tenant_id, const std::string& key) override;
    bool Exists(const std::string& tenant_id,
                const std::string& key) const override;
    size_t GetKeyCountForTenant(const std::string& tenant_id) const override;
    size_t GetKeyCount() const override;
    void Clear();

    bool PutWeightMetadata(const WeightRevisionMetadata& metadata) override;
    std::optional<WeightRevisionMetadata> GetWeightMetadata(
        const WeightRevisionIdentity& identity) const override;
    std::optional<uint64_t> GetWeightMetadataTombstoneGeneration(
        const WeightRevisionIdentity& identity) const override;
    bool RemoveWeightMetadata(const WeightRevisionIdentity& identity,
                              uint64_t metadata_generation) override;
    bool PutWeightLease(const WeightRevisionLease& lease) override;
    std::optional<WeightRevisionLease> GetWeightLease(
        uint64_t lease_id) const override;
    std::optional<WeightRevisionLease> GetWeightLeaseTombstone(
        uint64_t lease_id) const override;
    bool RemoveWeightLease(uint64_t lease_id,
                           const WeightRevisionIdentity& identity,
                           uint64_t fenced_metadata_generation) override;
    bool PutWeightOperation(
        const WeightResidencyOperation& operation) override;
    std::optional<WeightResidencyOperation> GetWeightOperation(
        uint64_t operation_id) const override;
    bool RestoreWeightMetadata(const WeightMetadataSnapshot& snapshot);
    WeightMetadataSnapshot SnapshotWeightMetadata() const;

    void Snapshot(std::vector<StandbyObjectEntry>& out) const;
    bool ValidateReplicaIds(ReplicaID& max_replica_id) const;
    bool DrainChunk(size_t count, std::vector<StandbyObjectEntry>& out);
    // Mutations must remain frozen for the lifetime of the returned cursor.
    SnapshotCursor BeginSnapshotTraversal() const;
    bool CopyNextSnapshotChunk(size_t count, SnapshotCursor& cursor,
                               std::vector<StandbyObjectEntry>& out) const;

   private:
    mutable std::mutex mutex_;
    TenantStore store_;
    std::map<WeightRevisionIdentity, WeightRevisionMetadata> weight_metadata_;
    std::map<WeightRevisionIdentity, uint64_t> weight_metadata_tombstones_;
    std::unordered_map<uint64_t, WeightRevisionLease> weight_leases_;
    std::unordered_map<uint64_t, WeightRevisionLease> weight_lease_tombstones_;
    std::unordered_map<uint64_t, WeightResidencyOperation> weight_operations_;
    uint64_t next_weight_lease_id_{1};
    uint64_t next_weight_operation_id_{1};
};

}  // namespace mooncake
