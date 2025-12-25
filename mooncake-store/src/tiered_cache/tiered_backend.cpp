#include <glog/logging.h>
#include <algorithm>
#include <vector>
#include <limits>

#include "tiered_cache/tiered_backend.h"
#include "tiered_cache/cache_tier.h"

namespace mooncake {

AllocationEntry::~AllocationEntry() {
    if (backend) {
        // When ref count drops to 0, call back to backend to free physical
        // resource.
        backend->FreeInternal(loc);
    }
}

TieredBackend::TieredBackend() = default;

bool TieredBackend::Init(Json::Value root, TransferEngine* engine,
                         MetadataSyncCallback sync_callback) {
    // Initialize DataCopier
    try {
        DataCopierBuilder builder;
        data_copier_ = builder.Build();
    } catch (const std::logic_error& e) {
        LOG(ERROR) << "Failed to build DataCopier: " << e.what();
        return false;
    }

    // Register callback for syncing metadata to Master
    metadata_sync_callback_ = sync_callback;

    // Initialize Tiers
    if (!root.isMember("tiers")) {
        LOG(ERROR) << "Tiered cache config is missing 'tiers' array.";
        return false;
    }

    for (const auto& tier_config : root["tiers"]) {
        UUID id = generate_uuid();
        // std::string type = tier_config["type"].asString(); // Unused for now
        int priority = tier_config["priority"].asInt();
        std::vector<std::string> tags;
        if (tier_config.isMember("tags")) {
            for (const auto& tag : tier_config["tags"])
                tags.push_back(tag.asString());
        }

        // TODO: Logic to instantiate specific CacheTier types (DRAM/SSD) goes
        // here. For example: std::unique_ptr<CacheTier> tier =
        // CacheTierFactory::Create(tier_config); tier->Init(this, engine);
        // tiers_[id] = std::move(tier);

        // Placeholder for compilation if Factory is not ready
        // tiers_[id] = std::make_unique<DramTier>();

        tier_info_[id] = {priority, tags};
    }

    LOG(INFO) << "TieredBackend initialized successfully with "
              << tier_info_.size() << " tiers.";
    return true;
}

std::vector<UUID> TieredBackend::GetSortedTiers() const {
    std::vector<UUID> ids;
    for (const auto& [id, _] : tiers_) ids.push_back(id);

    // Sort by priority descending (higher priority first)
    std::sort(ids.begin(), ids.end(), [this](UUID a, UUID b) {
        return tier_info_.at(a).priority > tier_info_.at(b).priority;
    });
    return ids;
}

bool TieredBackend::AllocateInternalRaw(size_t size,
                                        std::optional<UUID> preferred_tier,
                                        TieredLocation* out_loc) {
    if (!out_loc) return false;

    // Try preferred tier first
    if (preferred_tier.has_value()) {
        auto it = tiers_.find(*preferred_tier);
        if (it != tiers_.end()) {
            if (it->second->Allocate(size, out_loc->data)) {
                out_loc->tier_id = *preferred_tier;
                return true;
            }
        }
    }

    // Fallback: Auto-tiering based on priority
    auto sorted_tiers = GetSortedTiers();
    for (UUID tier_id : sorted_tiers) {
        if (preferred_tier.has_value() && tier_id == *preferred_tier) continue;

        auto it = tiers_.find(tier_id);
        if (it == tiers_.end() || !it->second) continue;
        auto& tier = it->second;
        if (tier->Allocate(size, out_loc->data)) {
            out_loc->tier_id = tier_id;
            return true;
        }
    }
    return false;
}

void TieredBackend::FreeInternal(const TieredLocation& loc) {
    auto it = tiers_.find(loc.tier_id);
    if (it != tiers_.end()) {
        it->second->Free(loc.data);
    }
}

AllocationHandle TieredBackend::Allocate(size_t size,
                                         std::optional<UUID> preferred_tier) {
    TieredLocation loc;
    if (AllocateInternalRaw(size, preferred_tier, &loc)) {
        // Create the handle (Ref count = 1).
        // If this handle dies without being committed, AllocationEntry
        // destructor triggers FreeInternal.
        return std::make_shared<AllocationEntry>(this, loc);
    }
    return nullptr;
}

bool TieredBackend::Write(const DataSource& source, AllocationHandle handle) {
    if (!handle) return false;
    if (!data_copier_) {
        LOG(ERROR) << "TieredBackend not initialized";
        return false;
    }
    auto it = tiers_.find(handle->loc.tier_id);
    if (it == tiers_.end()) return false;

    return data_copier_->Copy(source, handle->loc.data);
}

bool TieredBackend::Commit(const std::string& key, AllocationHandle handle) {
    if (!handle) return false;

    std::shared_ptr<MetadataEntry> entry = nullptr;

    if (metadata_sync_callback_) {
        auto result = metadata_sync_callback_(key, handle->loc.tier_id, COMMIT);

        if (!result.has_value()) {
            LOG(ERROR) << "Failed to Commit key " << key
                       << " to Master, error_code=" << result.error();
            return false;
        }
    }

    // Try to find existing entry (Global Read Lock)
    {
        std::shared_lock<std::shared_mutex> read_lock(map_mutex_);
        auto it = metadata_index_.find(key);
        if (it != metadata_index_.end()) {
            entry = it->second;
        }
    }

    // Create if not exists (Global Write Lock)
    if (!entry) {
        std::unique_lock<std::shared_mutex> write_lock(map_mutex_);
        // Double-check logic
        auto it = metadata_index_.find(key);
        if (it != metadata_index_.end()) {
            entry = it->second;
        } else {
            entry = std::make_shared<MetadataEntry>();
            metadata_index_[key] = entry;
        }
    }

    //  Update Entry (Entry Write Lock)
    // Global lock is released. We only lock this specific key's entry.
    {
        std::unique_lock<std::shared_mutex> entry_lock(entry->mutex);
        // Insert or replace the handle for this tier
        bool found = false;
        for (auto& replica : entry->replicas) {
            if (replica.first == handle->loc.tier_id) {
                replica.second = handle;
                found = true;
                break;
            }
        }

        if (!found) {
            entry->replicas.emplace_back(handle->loc.tier_id, handle);
            std::sort(entry->replicas.begin(), entry->replicas.end(),
                      [this](const std::pair<UUID, AllocationHandle>& a,
                             const std::pair<UUID, AllocationHandle>& b) {
                          return tier_info_.at(a.first).priority >
                                 tier_info_.at(b.first).priority;
                      });
        }
    }

    return true;
}

AllocationHandle TieredBackend::Get(const std::string& key,
                                    std::optional<UUID> tier_id) {
    std::shared_ptr<MetadataEntry> entry = nullptr;

    // Find Entry (Global Read Lock)
    {
        std::shared_lock<std::shared_mutex> read_lock(map_mutex_);
        auto it = metadata_index_.find(key);
        if (it == metadata_index_.end()) {
            return nullptr;
        }
        entry = it->second;
    }

    // Read Entry (Entry Read Lock)
    std::shared_lock<std::shared_mutex> entry_read_lock(entry->mutex);

    if (entry->replicas.empty()) return nullptr;

    if (tier_id.has_value()) {
        for (const auto& replica : entry->replicas) {
            if (replica.first == *tier_id) {
                return replica.second;
            }
        }
        return nullptr;
    }

    // Fallback: Return highest priority replica
    return entry->replicas.begin()->second;
}

bool TieredBackend::Delete(const std::string& key,
                           std::optional<UUID> tier_id) {
    // Hold references locally to ensure destruction happens OUTSIDE the
    // locks This is crucial for non-blocking deletions.
    AllocationHandle handle_ref = nullptr;
    std::vector<AllocationHandle> handles_to_free;

    if (tier_id.has_value()) {
        // Delete Specific Replica

        bool need_cleanup = false;
        bool found_tier = false;

        // Optimistic Delete (Global Read Lock + Entry Write Lock)
        // This is fast and allows high concurrency.
        {
            std::shared_lock<std::shared_mutex> read_lock(map_mutex_);
            auto it = metadata_index_.find(key);
            if (it != metadata_index_.end()) {
                auto entry = it->second;

                std::unique_lock<std::shared_mutex> entry_write_lock(
                    entry->mutex);
                auto tier_it = entry->replicas.end();
                for (auto it = entry->replicas.begin();
                     it != entry->replicas.end(); ++it) {
                    if (it->first == *tier_id) {
                        tier_it = it;
                        break;
                    }
                }

                if (tier_it != entry->replicas.end()) {
                    if (metadata_sync_callback_) {
                        auto result = metadata_sync_callback_(
                            key, tier_it->second->loc.tier_id, DELETE);
                        if (!result.has_value()) {
                            LOG(ERROR)
                                << "Failed to Delete key " << key << " in Tier "
                                << tier_it->second->loc.tier_id
                                << " for Master, error_code=" << result.error();
                            return false;
                        }
                    }
                    handle_ref =
                        tier_it->second;  // Capture reference (+1 ref count)
                    entry->replicas.erase(tier_it);
                    found_tier = true;
                }

                // Mark for cleanup if entry becomes empty
                if (entry->replicas.empty()) {
                    need_cleanup = true;
                }
            }
        }  // Read lock released here

        // Retry with Write Lock
        // If the entry is empty, we upgrade to a global write lock to
        // remove it. This prevents memory leaks from empty "zombie"
        // entries.
        if (need_cleanup) {
            std::unique_lock<std::shared_mutex> write_lock(map_mutex_);

            auto it = metadata_index_.find(key);
            if (it != metadata_index_.end()) {
                auto entry = it->second;

                // Double-Check Locking:
                // Another thread might have added a replica now
                std::unique_lock<std::shared_mutex> entry_lock(entry->mutex);

                if (entry->replicas.empty()) {
                    // Confirmed empty, safe to remove from global index
                    metadata_index_.erase(it);
                }
            }
        }

        return found_tier;
    } else {
        // Delete All Replicas (Full Key Deletion)
        // Requires Global Write Lock since we are modifying the map
        // structure.
        std::unique_lock<std::shared_mutex> global_write_lock(map_mutex_);
        auto it = metadata_index_.find(key);
        if (it == metadata_index_.end()) return false;
        UUID invalid_id{0, 0};
        if (metadata_sync_callback_) {
            auto result = metadata_sync_callback_(key, invalid_id, DELETE_ALL);
            if (!result.has_value()) {
                LOG(ERROR) << "Failed to Delete key " << key
                           << " for Master, error_code=" << result.error();
                return false;
            }
        }

        auto entry = it->second;

        {
            std::unique_lock<std::shared_mutex> entry_lock(entry->mutex);
            handles_to_free.reserve(entry->replicas.size());
            for (auto& replica : entry->replicas) {
                handles_to_free.push_back(replica.second);
            }
            entry->replicas.clear();
        }

        // Remove the entry from the global index
        metadata_index_.erase(it);
    }

    // Handles go out of scope here.
    // Ref count drops to 0 -> ~AllocationEntry() -> FreeInternal().
    // This happens concurrently without holding any locks.
    return true;
}

bool TieredBackend::CopyData(const std::string& key, const DataSource& source,
                             UUID dest_tier_id) {
    if (source.size == 0) return false;
    auto dest_handle = Allocate(source.size, dest_tier_id);
    if (!dest_handle) return false;

    if (!Write(source, dest_handle)) return false;

    // Commit (Add Replica)
    // Takes ownership of dest_handle into the map
    return Commit(key, dest_handle);
}

std::vector<TierView> TieredBackend::GetTierViews() const {
    std::vector<TierView> views;
    for (const auto& [id, tier] : tiers_) {
        const auto& info = tier_info_.at(id);
        size_t cap = tier->GetCapacity();
        size_t used = tier->GetUsage();
        views.push_back({id, tier->GetMemoryType(), cap, used, cap - used,
                         info.priority, info.tags});
    }
    return views;
}

const CacheTier* TieredBackend::GetTier(UUID tier_id) const {
    auto it = tiers_.find(tier_id);
    return (it != tiers_.end()) ? it->second.get() : nullptr;
}

const DataCopier& TieredBackend::GetDataCopier() const {
    CHECK(data_copier_) << "TieredBackend not initialized";
    return *data_copier_;
}

}  // namespace mooncake