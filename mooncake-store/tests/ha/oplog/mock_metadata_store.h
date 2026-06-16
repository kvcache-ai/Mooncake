// mooncake-store/tests/hot_standby_ut/mock_metadata_store.h
#pragma once

#include <map>
#include <optional>
#include <string>

#include "metadata_store.h"

namespace mooncake::test {

// In-memory MetadataStore for unit tests.
// Extracted from oplog_applier_test.cpp for reuse.
class MockMetadataStore : public MetadataStore {
   public:
    MockMetadataStore() = default;
    ~MockMetadataStore() override = default;

    bool PutMetadata(const std::string& key,
                     const StandbyObjectMetadata& metadata) override {
        metadata_map_[key] = metadata;
        return true;
    }

    bool Put(const std::string& key, const std::string& payload) override {
        // For testing, we can use PutMetadata with empty metadata
        StandbyObjectMetadata meta;
        metadata_map_[key] = meta;
        return true;
    }

    std::optional<StandbyObjectMetadata> GetMetadata(
        const std::string& key) const override {
        auto it = metadata_map_.find(key);
        if (it != metadata_map_.end()) {
            return it->second;
        }
        return std::nullopt;
    }

    bool Remove(const std::string& key) override {
        auto it = metadata_map_.find(key);
        if (it != metadata_map_.end()) {
            metadata_map_.erase(it);
            return true;
        }
        return false;
    }

    bool Exists(const std::string& key) const override {
        return metadata_map_.find(key) != metadata_map_.end();
    }

    size_t GetKeyCount() const override { return metadata_map_.size(); }

    // Test helper methods
    void Clear() { metadata_map_.clear(); }

    size_t Size() const { return metadata_map_.size(); }

    bool Contains(const std::string& key) const {
        return metadata_map_.count(key) > 0;
    }

   private:
    std::map<std::string, StandbyObjectMetadata> metadata_map_;
};

}  // namespace mooncake::test
