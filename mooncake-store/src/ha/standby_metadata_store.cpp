#include "ha/standby_metadata_store.h"

#include <algorithm>
#include <limits>
#include <unordered_set>

namespace mooncake {

bool StandbyMetadataStore::PutMetadata(const std::string& tenant_id,
                                       const std::string& key,
                                       const StandbyObjectMetadata& metadata) {
    std::lock_guard<std::mutex> lock(mutex_);
    store_[NormalizeTenantId(tenant_id)][key] = metadata;
    return true;
}

bool StandbyMetadataStore::RestoreMetadata(
    const std::string& tenant_id, const std::string& key,
    const StandbyObjectMetadata& metadata) {
    std::lock_guard<std::mutex> lock(mutex_);
    return store_[NormalizeTenantId(tenant_id)].emplace(key, metadata).second;
}

bool StandbyMetadataStore::Put(const std::string& key,
                               const std::string& payload) {
    (void)payload;
    return PutMetadata("default", key, StandbyObjectMetadata{});
}

std::optional<StandbyObjectMetadata> StandbyMetadataStore::GetMetadata(
    const std::string& tenant_id, const std::string& key) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto tenant = store_.find(NormalizeTenantId(tenant_id));
    if (tenant == store_.end()) {
        return std::nullopt;
    }
    auto object = tenant->second.find(key);
    return object == tenant->second.end()
               ? std::nullopt
               : std::optional<StandbyObjectMetadata>(object->second);
}

bool StandbyMetadataStore::Remove(const std::string& tenant_id,
                                  const std::string& key) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto tenant = store_.find(NormalizeTenantId(tenant_id));
    if (tenant == store_.end() || tenant->second.erase(key) == 0) {
        return false;
    }
    if (tenant->second.empty()) {
        store_.erase(tenant);
    }
    return true;
}

bool StandbyMetadataStore::Exists(const std::string& tenant_id,
                                  const std::string& key) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto tenant = store_.find(NormalizeTenantId(tenant_id));
    return tenant != store_.end() && tenant->second.contains(key);
}

size_t StandbyMetadataStore::GetKeyCountForTenant(
    const std::string& tenant_id) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto tenant = store_.find(NormalizeTenantId(tenant_id));
    return tenant == store_.end() ? 0 : tenant->second.size();
}

size_t StandbyMetadataStore::GetKeyCount() const {
    std::lock_guard<std::mutex> lock(mutex_);
    size_t count = 0;
    for (const auto& [tenant_id, objects] : store_) {
        (void)tenant_id;
        count += objects.size();
    }
    return count;
}

void StandbyMetadataStore::Clear() {
    std::lock_guard<std::mutex> lock(mutex_);
    store_.clear();
    weight_metadata_.clear();
    weight_metadata_tombstones_.clear();
    weight_leases_.clear();
    weight_lease_tombstones_.clear();
    weight_operations_.clear();
    next_weight_lease_id_ = 1;
    next_weight_operation_id_ = 1;
}

bool StandbyMetadataStore::PutWeightMetadata(
    const WeightRevisionMetadata& metadata) {
    std::lock_guard<std::mutex> lock(mutex_);
    weight_metadata_[metadata.identity] = metadata;
    return true;
}

std::optional<WeightRevisionMetadata> StandbyMetadataStore::GetWeightMetadata(
    const WeightRevisionIdentity& identity) const {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto it = weight_metadata_.find(identity);
    return it == weight_metadata_.end()
               ? std::nullopt
               : std::optional<WeightRevisionMetadata>(it->second);
}

std::optional<uint64_t>
StandbyMetadataStore::GetWeightMetadataTombstoneGeneration(
    const WeightRevisionIdentity& identity) const {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto it = weight_metadata_tombstones_.find(identity);
    return it == weight_metadata_tombstones_.end()
               ? std::nullopt
               : std::optional<uint64_t>(it->second);
}

bool StandbyMetadataStore::RemoveWeightMetadata(
    const WeightRevisionIdentity& identity, uint64_t metadata_generation) {
    std::lock_guard<std::mutex> lock(mutex_);
    weight_metadata_.erase(identity);
    weight_metadata_tombstones_[identity] = metadata_generation;
    return true;
}

bool StandbyMetadataStore::PutWeightLease(const WeightRevisionLease& lease) {
    std::lock_guard<std::mutex> lock(mutex_);
    weight_leases_[lease.lease_id] = lease;
    if (lease.lease_id < std::numeric_limits<uint64_t>::max()) {
        next_weight_lease_id_ =
            std::max(next_weight_lease_id_, lease.lease_id + 1);
    }
    return true;
}

std::optional<WeightRevisionLease> StandbyMetadataStore::GetWeightLease(
    uint64_t lease_id) const {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto it = weight_leases_.find(lease_id);
    return it == weight_leases_.end()
               ? std::nullopt
               : std::optional<WeightRevisionLease>(it->second);
}

std::optional<WeightRevisionLease>
StandbyMetadataStore::GetWeightLeaseTombstone(uint64_t lease_id) const {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto it = weight_lease_tombstones_.find(lease_id);
    return it == weight_lease_tombstones_.end()
               ? std::nullopt
               : std::optional<WeightRevisionLease>(it->second);
}

bool StandbyMetadataStore::RemoveWeightLease(
    uint64_t lease_id, const WeightRevisionIdentity& identity,
    uint64_t fenced_metadata_generation) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = weight_leases_.find(lease_id);
    WeightRevisionLease tombstone{
        .lease_id = lease_id,
        .identity = identity,
        .holder = {},
        .expires_at_ms = 0,
        .fenced_metadata_generation = fenced_metadata_generation,
    };
    if (it != weight_leases_.end()) {
        tombstone = it->second;
        weight_leases_.erase(it);
    }
    weight_lease_tombstones_[lease_id] = std::move(tombstone);
    return true;
}

bool StandbyMetadataStore::RestoreWeightMetadata(
    const WeightMetadataSnapshot& snapshot) {
    if (!ValidateWeightMetadataSnapshot(snapshot)) {
        return false;
    }

    std::map<WeightRevisionIdentity, WeightRevisionMetadata> metadata;
    std::unordered_map<uint64_t, WeightRevisionLease> leases;
    std::unordered_map<uint64_t, WeightResidencyOperation> operations;
    for (const auto& record : snapshot.metadata) {
        metadata.emplace(record.identity, record);
    }
    for (const auto& lease : snapshot.leases) {
        leases.emplace(lease.lease_id, lease);
    }
    for (const auto& operation : snapshot.operations) {
        operations.emplace(operation.operation_id, operation);
    }

    std::lock_guard<std::mutex> lock(mutex_);
    weight_metadata_ = std::move(metadata);
    weight_metadata_tombstones_.clear();
    weight_leases_ = std::move(leases);
    weight_lease_tombstones_.clear();
    weight_operations_ = std::move(operations);
    next_weight_lease_id_ = snapshot.next_lease_id;
    next_weight_operation_id_ = snapshot.next_operation_id;
    return true;
}

WeightMetadataSnapshot StandbyMetadataStore::SnapshotWeightMetadata() const {
    std::lock_guard<std::mutex> lock(mutex_);
    WeightMetadataSnapshot snapshot{
        .schema_version = 1,
        .metadata = {},
        .leases = {},
        .operations = {},
        .next_lease_id = next_weight_lease_id_,
        .next_operation_id = next_weight_operation_id_,
    };
    snapshot.metadata.reserve(weight_metadata_.size());
    for (const auto& [identity, metadata] : weight_metadata_) {
        (void)identity;
        snapshot.metadata.push_back(metadata);
    }
    snapshot.leases.reserve(weight_leases_.size());
    for (const auto& [lease_id, lease] : weight_leases_) {
        (void)lease_id;
        snapshot.leases.push_back(lease);
    }
    snapshot.operations.reserve(weight_operations_.size());
    for (const auto& [operation_id, operation] : weight_operations_) {
        (void)operation_id;
        snapshot.operations.push_back(operation);
    }
    std::sort(snapshot.leases.begin(), snapshot.leases.end(),
              [](const auto& lhs, const auto& rhs) {
                  return lhs.lease_id < rhs.lease_id;
              });
    std::sort(snapshot.operations.begin(), snapshot.operations.end(),
              [](const auto& lhs, const auto& rhs) {
                  return lhs.operation_id < rhs.operation_id;
              });
    return snapshot;
}

void StandbyMetadataStore::Snapshot(
    std::vector<StandbyObjectEntry>& out) const {
    std::lock_guard<std::mutex> lock(mutex_);
    out.clear();
    for (const auto& [tenant_id, objects] : store_) {
        for (const auto& [key, metadata] : objects) {
            out.push_back({tenant_id, key, metadata});
        }
    }
}

bool StandbyMetadataStore::ValidateReplicaIds(ReplicaID& max_replica_id) const {
    std::lock_guard<std::mutex> lock(mutex_);
    max_replica_id = 0;
    for (const auto& [tenant_id, objects] : store_) {
        (void)tenant_id;
        for (const auto& [key, metadata] : objects) {
            (void)key;
            std::unordered_set<ReplicaID> object_ids;
            for (const auto& replica : metadata.replicas) {
                if (replica.id == 0 ||
                    replica.id == std::numeric_limits<ReplicaID>::max() ||
                    !object_ids.insert(replica.id).second) {
                    return false;
                }
                max_replica_id = std::max(max_replica_id, replica.id);
            }
        }
    }
    return true;
}

bool StandbyMetadataStore::DrainChunk(size_t count,
                                      std::vector<StandbyObjectEntry>& out) {
    out.clear();
    if (count == 0) {
        return false;
    }

    std::lock_guard<std::mutex> lock(mutex_);
    out.reserve(count);
    while (!store_.empty() && out.size() < count) {
        auto tenant = store_.begin();
        auto object = tenant->second.extract(tenant->second.begin());
        out.push_back({tenant->first, std::move(object.key()),
                       std::move(object.mapped())});
        if (tenant->second.empty()) {
            store_.erase(tenant);
        }
    }
    return true;
}

StandbyMetadataStore::SnapshotCursor
StandbyMetadataStore::BeginSnapshotTraversal() const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto tenant = store_.cbegin();
    const bool done = tenant == store_.cend();
    return SnapshotCursor(
        this, tenant, store_.cend(),
        done ? ObjectStore::const_iterator() : tenant->second.cbegin(), done);
}

bool StandbyMetadataStore::CopyNextSnapshotChunk(
    size_t count, SnapshotCursor& cursor,
    std::vector<StandbyObjectEntry>& out) const {
    out.clear();
    if (count == 0 || cursor.store_ != this) {
        return false;
    }

    std::lock_guard<std::mutex> lock(mutex_);
    out.reserve(count);
    while (!cursor.done_ && out.size() < count) {
        const auto& objects = cursor.tenant_->second;
        while (cursor.object_ != objects.cend() && out.size() < count) {
            out.push_back({cursor.tenant_->first, cursor.object_->first,
                           cursor.object_->second});
            ++cursor.object_;
        }
        if (cursor.object_ == objects.cend()) {
            ++cursor.tenant_;
            cursor.done_ = cursor.tenant_ == cursor.tenant_end_;
            if (!cursor.done_) {
                cursor.object_ = cursor.tenant_->second.cbegin();
            }
        }
    }
    return true;
}

}  // namespace mooncake
