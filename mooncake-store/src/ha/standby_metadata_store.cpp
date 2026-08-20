#include "ha/standby_metadata_store.h"

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
