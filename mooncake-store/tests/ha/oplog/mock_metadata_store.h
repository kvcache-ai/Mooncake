// mooncake-store/tests/hot_standby_ut/mock_metadata_store.h
#pragma once

#include <map>
#include <optional>
#include <string>

#include "metadata_store.h"
#include "types.h"

namespace mooncake::test {

// In-memory MetadataStore for unit tests.
// Extracted from oplog_applier_test.cpp for reuse.
class MockMetadataStore : public MetadataStore {
   public:
    MockMetadataStore() = default;
    ~MockMetadataStore() override = default;

    // Bring base-class key-only overloads into scope (name hiding)
    using MetadataStore::Exists;
    using MetadataStore::GetMetadata;
    using MetadataStore::PutMetadata;
    using MetadataStore::Remove;

    // Tenant-aware methods (primary API)
    bool PutMetadata(const std::string& tenant_id, const std::string& key,
                     const StandbyObjectMetadata& metadata) override {
        const auto normalized = NormalizeTenantId(tenant_id);
        metadata_map_[normalized][key] = metadata;
        return true;
    }

    std::optional<StandbyObjectMetadata> GetMetadata(
        const std::string& tenant_id, const std::string& key) const override {
        const auto normalized = NormalizeTenantId(tenant_id);
        auto tenant_it = metadata_map_.find(normalized);
        if (tenant_it == metadata_map_.end()) {
            return std::nullopt;
        }
        auto it = tenant_it->second.find(key);
        if (it != tenant_it->second.end()) {
            return it->second;
        }
        return std::nullopt;
    }

    bool Remove(const std::string& tenant_id, const std::string& key) override {
        const auto normalized = NormalizeTenantId(tenant_id);
        auto tenant_it = metadata_map_.find(normalized);
        if (tenant_it == metadata_map_.end()) {
            return false;
        }
        auto it = tenant_it->second.find(key);
        if (it != tenant_it->second.end()) {
            tenant_it->second.erase(it);
            if (tenant_it->second.empty()) {
                metadata_map_.erase(tenant_it);
            }
            return true;
        }
        return false;
    }

    bool Exists(const std::string& tenant_id,
                const std::string& key) const override {
        const auto normalized = NormalizeTenantId(tenant_id);
        auto tenant_it = metadata_map_.find(normalized);
        if (tenant_it == metadata_map_.end()) {
            return false;
        }
        return tenant_it->second.find(key) != tenant_it->second.end();
    }

    size_t GetKeyCountForTenant(const std::string& tenant_id) const override {
        const auto normalized = NormalizeTenantId(tenant_id);
        auto tenant_it = metadata_map_.find(normalized);
        if (tenant_it == metadata_map_.end()) {
            return 0;
        }
        return tenant_it->second.size();
    }

    // Legacy key-only Put delegates to "default" tenant
    bool Put(const std::string& key, const std::string& payload) override {
        StandbyObjectMetadata meta;
        metadata_map_["default"][key] = meta;
        return true;
    }

    // Total count across ALL tenants
    size_t GetKeyCount() const override {
        size_t total = 0;
        for (const auto& [tenant_id, tenant_map] : metadata_map_) {
            total += tenant_map.size();
        }
        return total;
    }

    // Test helper methods
    void Clear() {
        metadata_map_.clear();
        weight_metadata_.clear();
        weight_metadata_tombstones_.clear();
        weight_leases_.clear();
        weight_lease_tombstones_.clear();
        weight_operations_.clear();
    }

    size_t Size() const { return GetKeyCount(); }

    bool Contains(const std::string& key) const {
        return Exists("default", key);
    }

    bool PutWeightMetadata(const WeightRevisionMetadata& metadata) override {
        weight_metadata_[metadata.identity] = metadata;
        return true;
    }

    std::optional<WeightRevisionMetadata> GetWeightMetadata(
        const WeightRevisionIdentity& identity) const override {
        const auto it = weight_metadata_.find(identity);
        return it == weight_metadata_.end()
                   ? std::nullopt
                   : std::optional<WeightRevisionMetadata>(it->second);
    }

    std::optional<uint64_t> GetWeightMetadataTombstoneGeneration(
        const WeightRevisionIdentity& identity) const override {
        const auto it = weight_metadata_tombstones_.find(identity);
        return it == weight_metadata_tombstones_.end()
                   ? std::nullopt
                   : std::optional<uint64_t>(it->second);
    }

    bool RemoveWeightMetadata(const WeightRevisionIdentity& identity,
                              uint64_t metadata_generation) override {
        weight_metadata_.erase(identity);
        weight_metadata_tombstones_[identity] = metadata_generation;
        return true;
    }

    bool PutWeightLease(const WeightRevisionLease& lease) override {
        weight_leases_[lease.lease_id] = lease;
        return true;
    }

    std::optional<WeightRevisionLease> GetWeightLease(
        uint64_t lease_id) const override {
        const auto it = weight_leases_.find(lease_id);
        return it == weight_leases_.end()
                   ? std::nullopt
                   : std::optional<WeightRevisionLease>(it->second);
    }

    std::optional<WeightRevisionLease> GetWeightLeaseTombstone(
        uint64_t lease_id) const override {
        const auto it = weight_lease_tombstones_.find(lease_id);
        return it == weight_lease_tombstones_.end()
                   ? std::nullopt
                   : std::optional<WeightRevisionLease>(it->second);
    }

    bool RemoveWeightLease(uint64_t lease_id,
                           const WeightRevisionIdentity& identity,
                           uint64_t fenced_metadata_generation) override {
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

    bool PutWeightOperation(
        const WeightResidencyOperation& operation) override {
        weight_operations_[operation.operation_id] = operation;
        return true;
    }

    std::optional<WeightResidencyOperation> GetWeightOperation(
        uint64_t operation_id) const override {
        const auto it = weight_operations_.find(operation_id);
        return it == weight_operations_.end()
                   ? std::nullopt
                   : std::optional<WeightResidencyOperation>(it->second);
    }

   private:
    std::map<std::string, std::map<std::string, StandbyObjectMetadata>>
        metadata_map_;
    std::map<WeightRevisionIdentity, WeightRevisionMetadata> weight_metadata_;
    std::map<WeightRevisionIdentity, uint64_t> weight_metadata_tombstones_;
    std::map<uint64_t, WeightRevisionLease> weight_leases_;
    std::map<uint64_t, WeightRevisionLease> weight_lease_tombstones_;
    std::map<uint64_t, WeightResidencyOperation> weight_operations_;
};

}  // namespace mooncake::test
