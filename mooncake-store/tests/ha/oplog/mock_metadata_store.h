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
    void Clear() { metadata_map_.clear(); }

    size_t Size() const { return GetKeyCount(); }

    bool Contains(const std::string& key) const {
        return Exists("default", key);
    }

   private:
    std::map<std::string, std::map<std::string, StandbyObjectMetadata>>
        metadata_map_;
};

}  // namespace mooncake::test
