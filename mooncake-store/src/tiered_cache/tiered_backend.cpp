#include <glog/logging.h>
#include <algorithm>
#include <vector>
#include <limits>

#include "tiered_cache/tiered_backend.h"
#include "tiered_cache/cache_tier.h"
#include "tiered_cache/dram_tier.h"
#ifdef USE_ASCEND_CACHE_TIER
#include "tiered_cache/ascend_tier.h"
#endif
#include "tiered_cache/storage_tier.h"
#include "tiered_cache/scheduler/client_scheduler.h"

namespace mooncake {

AllocationEntry::~AllocationEntry() {
    if (backend && loc.tier) {
        // When ref count drops to 0, free physical resource directly.
        loc.tier->Free(std::move(loc.data));
    }
}

TieredBackend::TieredBackend() = default;

TieredBackend::~TieredBackend() {
    if (scheduler_) {
        scheduler_->Stop();
    }
    // Explicitly flush all tiers to ensure pending data is written before
    // metadata and buffers (held in metadata_index_) are destroyed.
    for (auto& [id, tier] : tiers_) {
        if (tier) {
            auto res = tier->Flush();
            if (!res) {
                LOG(ERROR) << "Failed to flush tier " << id
                           << " during shutdown: " << res.error();
            }
        }
    }
}

tl::expected<void, ErrorCode> TieredBackend::Init(
    Json::Value root, TransferEngine* engine,
    MetadataSyncCallback sync_callback) {
    // Initialize DataCopier
    try {
        DataCopierBuilder builder;
        data_copier_ = builder.Build();
    } catch (const std::logic_error& e) {
        LOG(ERROR) << "Failed to build DataCopier: " << e.what();
        return tl::unexpected(ErrorCode::INTERNAL_ERROR);
    }

    // Register callback for syncing metadata to Master
    metadata_sync_callback_ = sync_callback;

    // Initialize Tiers
    if (!root.isMember("tiers")) {
        LOG(ERROR) << "Tiered cache config is missing 'tiers' array.";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }

    for (const auto& tier_config : root["tiers"]) {
        // Parse required fields
        if (!tier_config.isMember("type")) {
            LOG(ERROR) << "Tier config missing required field 'type'";
            return tl::unexpected(ErrorCode::INVALID_PARAMS);
        }
        if (!tier_config.isMember("capacity")) {
            LOG(ERROR) << "Tier config missing required field 'capacity'";
            return tl::unexpected(ErrorCode::INVALID_PARAMS);
        }
        if (!tier_config.isMember("priority")) {
            LOG(ERROR) << "Tier config missing required field 'priority'";
            return tl::unexpected(ErrorCode::INVALID_PARAMS);
        }

        std::string type = tier_config["type"].asString();
        size_t capacity = tier_config["capacity"].asUInt64();
        int priority = tier_config["priority"].asInt();

        // Validate capacity
        if (capacity == 0) {
            LOG(ERROR) << "Invalid capacity (0) for tier type " << type;
            return tl::unexpected(ErrorCode::INVALID_PARAMS);
        }

        // Parse tags
        std::vector<std::string> tags;
        if (tier_config.isMember("tags")) {
            for (const auto& tag : tier_config["tags"]) {
                tags.push_back(tag.asString());
            }
        }

        // Generate UUID for this tier
        UUID id = generate_uuid();

        // Instantiate tier based on type
        if (type == "DRAM") {
            // Parse NUMA node
            std::optional<int> numa_node;
            if (tier_config.isMember("numa_node")) {
                int node = tier_config["numa_node"].asInt();
                if (node < 0) {
                    LOG(WARNING) << "Invalid NUMA node (" << node
                                 << "), using default allocation";
                } else {
                    numa_node = node;
                }
            }
            // Parse allocator type
            BufferAllocatorType allocator_type = BufferAllocatorType::OFFSET;
            if (tier_config.isMember("allocator_type")) {
                std::string allocator_str =
                    tier_config["allocator_type"].asString();
                if (allocator_str == "OFFSET") {
                    allocator_type = BufferAllocatorType::OFFSET;
                } else if (allocator_str == "CACHELIB") {
                    allocator_type = BufferAllocatorType::CACHELIB;
                } else {
                    LOG(WARNING) << "Unknown allocator_type '" << allocator_str
                                 << "', using default OFFSET";
                }
            }
            LOG(INFO) << "Creating DRAM tier: id=" << id
                      << ", capacity=" << capacity << ", priority=" << priority
                      << ", allocator_type=" << allocator_type
                      << (numa_node.has_value()
                              ? ", numa_node=" + std::to_string(*numa_node)
                              : "");

            auto tier = std::make_unique<DramCacheTier>(
                id, capacity, tags, numa_node, allocator_type);
            auto init_result = tier->Init(this, engine);
            if (!init_result) {
                LOG(ERROR) << "Failed to initialize DRAM tier: id=" << id
                           << ", error=" << init_result.error();
                return tl::unexpected(init_result.error());
            }

            tiers_[id] = std::move(tier);
            tier_info_[id] = {priority, tags};
            LOG(INFO) << "Successfully initialized DRAM tier: id=" << id;
        }
#ifdef USE_ASCEND_CACHE_TIER
        else if (type == "ASCEND_NPU" || type == "ASCEND") {
            // Parse device_id
            int device_id = 0;
            if (tier_config.isMember("device_id")) {
                device_id = tier_config["device_id"].asInt();
            }

            LOG(INFO) << "Creating ASCEND_NPU tier: id=" << id
                      << ", capacity=" << capacity << ", priority=" << priority
                      << ", device_id=" << device_id;

            auto tier = std::make_unique<AscendCacheTier>(id, capacity, tags,
                                                          device_id);
            auto init_result = tier->Init(this, engine);
            if (!init_result) {
                LOG(ERROR) << "Failed to initialize ASCEND_NPU tier: id=" << id
                           << ", error=" << init_result.error();
                return tl::unexpected(init_result.error());
            }

            tiers_[id] = std::move(tier);
            tier_info_[id] = {priority, tags};
            LOG(INFO) << "Successfully initialized ASCEND_NPU tier: id=" << id;
        }
#endif
        else if (type == "STORAGE" || type == "DISK") {
            LOG(INFO) << "Creating Storage tier: id=" << id
                      << ", priority=" << priority;
            auto tier = std::make_unique<StorageTier>(id, tags);
            auto init_result = tier->Init(this, engine);
            if (!init_result) {
                LOG(ERROR) << "Failed to initialize Storage tier: id=" << id
                           << ", error=" << init_result.error();
                return tl::unexpected(init_result.error());
            }
            tiers_[id] = std::move(tier);
            tier_info_[id] = {priority, tags};
            LOG(INFO) << "Successfully initialized Storage tier: id=" << id;
        } else {
            LOG(ERROR) << "Unsupported tier type '" << type << "'";
            return tl::unexpected(ErrorCode::INVALID_PARAMS);
        }
    }

    // Initialize and Start Scheduler
    scheduler_ = std::make_unique<ClientScheduler>(this);
    for (const auto& [id, tier] : tiers_) {
        scheduler_->RegisterTier(tier.get());
    }
    scheduler_->Start();

    LOG(INFO) << "TieredBackend initialized successfully with "
              << tier_info_.size() << " tiers.";
    return tl::expected<void, ErrorCode>{};
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
            auto alloc_result = it->second->Allocate(size, out_loc->data);
            if (alloc_result) {
                out_loc->tier = it->second.get();
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
        auto alloc_result = tier->Allocate(size, out_loc->data);
        if (alloc_result) {
            out_loc->tier = tier.get();
            return true;
        }
    }
    return false;
}

tl::expected<AllocationHandle, ErrorCode> TieredBackend::Allocate(
    size_t size, std::optional<UUID> preferred_tier) {
    TieredLocation loc;
    if (AllocateInternalRaw(size, preferred_tier, &loc)) {
        // Create the handle (Ref count = 1).
        // If this handle dies without being committed, AllocationEntry
        // destructor triggers Free.
        return std::make_shared<AllocationEntry>(this, std::move(loc));
    }
    LOG(ERROR) << "Failed to allocate " << size << " bytes";
    return tl::make_unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
}

tl::expected<void, ErrorCode> TieredBackend::Write(const DataSource& source,
                                                   AllocationHandle handle) {
    if (!handle) return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    if (!data_copier_) {
        LOG(ERROR) << "TieredBackend not initialized";
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }
    if (!handle->loc.tier) {
        LOG(ERROR) << "Tier pointer is null";
        return tl::make_unexpected(ErrorCode::TIER_NOT_FOUND);
    }

    return data_copier_->Copy(source, handle->loc.data);
}

tl::expected<void, ErrorCode> TieredBackend::Commit(
    const std::string& key, AllocationHandle handle,
    std::optional<uint64_t> expected_version) {
    if (!handle) return tl::make_unexpected(ErrorCode::INVALID_PARAMS);

    auto tier_commit_res = handle->loc.tier->Commit(key, handle->loc.data);
    if (!tier_commit_res) {
        LOG(ERROR) << "Tier Commit failed for key " << key << ": "
                   << tier_commit_res.error();
        return tl::make_unexpected(tier_commit_res.error());
    }

    if (metadata_sync_callback_) {
        auto result =
            metadata_sync_callback_(key, handle->loc.tier->GetTierId(), COMMIT);

        if (!result.has_value()) {
            LOG(ERROR) << "Failed to Commit key " << key
                       << " to Master, error_code=" << result.error();
            return tl::make_unexpected(result.error());
        }
    }

    std::shared_ptr<MetadataEntry> entry = nullptr;

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
        // If we are doing a CAS commit but entry doesn't exist, it implies
        // the key was deleted or never existed. If expected_version > 0, this
        // is a failure.
        if (expected_version.has_value()) {
            return tl::make_unexpected(ErrorCode::CAS_FAILED);
        }

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

        // CAS Check
        if (expected_version.has_value()) {
            if (entry->version != expected_version.value()) {
                VLOG(1) << "CAS Failed for key " << key
                        << ": valid_version=" << entry->version
                        << ", expected=" << expected_version.value();
                return tl::make_unexpected(ErrorCode::CAS_FAILED);
            }
        }

        // Insert or replace the handle for this tier
        UUID current_tier_id = handle->loc.tier->GetTierId();
        bool found = false;
        for (auto& replica : entry->replicas) {
            if (replica.first == current_tier_id) {
                replica.second = handle;
                found = true;
                break;
            }
        }

        if (!found) {
            entry->replicas.emplace_back(current_tier_id, handle);
            std::sort(entry->replicas.begin(), entry->replicas.end(),
                      [this](const std::pair<UUID, AllocationHandle>& a,
                             const std::pair<UUID, AllocationHandle>& b) {
                          return tier_info_.at(a.first).priority >
                                 tier_info_.at(b.first).priority;
                      });
        }

        // Increment Version on modification
        entry->version++;
    }

    return tl::expected<void, ErrorCode>{};
}

tl::expected<AllocationHandle, ErrorCode> TieredBackend::Get(
    const std::string& key, std::optional<UUID> tier_id, bool record_access,
    uint64_t* out_version) {
    std::shared_ptr<MetadataEntry> entry = nullptr;

    // Find Entry (Global Read Lock)
    {
        std::shared_lock<std::shared_mutex> read_lock(map_mutex_);
        auto it = metadata_index_.find(key);
        if (it == metadata_index_.end()) {
            LOG(ERROR) << "Key not found: " << key;
            return tl::make_unexpected(ErrorCode::INVALID_KEY);
        }
        entry = it->second;
    }

    if (record_access && scheduler_) {
        scheduler_->OnAccess(key);
    }

    // Read Entry (Entry Read Lock)
    std::shared_lock<std::shared_mutex> entry_read_lock(entry->mutex);

    // Return current version if requested
    if (out_version) {
        *out_version = entry->version;
    }

    if (entry->replicas.empty()) {
        LOG(ERROR) << "Empty replicas for key: " << key;
        return tl::make_unexpected(ErrorCode::EMPTY_REPLICAS);
    }

    if (tier_id.has_value()) {
        for (const auto& replica : entry->replicas) {
            if (replica.first == *tier_id) {
                return replica.second;
            }
        }
        LOG(ERROR) << "Tier not found: " << *tier_id;
        return tl::make_unexpected(ErrorCode::TIER_NOT_FOUND);
    }

    // Fallback: Return highest priority replica
    if (out_version) {
        *out_version = entry->version;
    }
    return entry->replicas.begin()->second;
}

tl::expected<void, ErrorCode> TieredBackend::Delete(
    const std::string& key, std::optional<UUID> tier_id) {
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
                            key, tier_it->second->loc.tier->GetTierId(),
                            DELETE);
                        if (!result.has_value()) {
                            LOG(ERROR)
                                << "Failed to Delete key " << key << " in Tier "
                                << tier_it->second->loc.tier->GetTierId()
                                << " for Master, error_code=" << result.error();
                            return tl::make_unexpected(result.error());
                        }
                    }
                    handle_ref =
                        tier_it->second;  // Capture reference (+1 ref count)
                    entry->replicas.erase(tier_it);
                    entry->version++;  // Increment version on replica deletion
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

        if (found_tier) {
            return tl::expected<void, ErrorCode>{};
        } else {
            LOG(ERROR) << "Tier not found: " << *tier_id;
            return tl::make_unexpected(ErrorCode::TIER_NOT_FOUND);
        }
    } else {
        // Delete All Replicas (Full Key Deletion)
        // Requires Global Write Lock since we are modifying the map
        // structure.
        std::unique_lock<std::shared_mutex> global_write_lock(map_mutex_);
        auto it = metadata_index_.find(key);
        if (it == metadata_index_.end()) {
            LOG(ERROR) << "Key not found: " << key;
            return tl::make_unexpected(ErrorCode::INVALID_KEY);
        }
        UUID invalid_id{0, 0};
        if (metadata_sync_callback_) {
            auto result = metadata_sync_callback_(key, invalid_id, DELETE_ALL);
            if (!result.has_value()) {
                LOG(ERROR) << "Failed to Delete key " << key
                           << " for Master, error_code=" << result.error();
                return tl::make_unexpected(result.error());
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
    // Ref count drops to 0 -> ~AllocationEntry() -> Free().
    // This happens concurrently without holding any locks.
    return tl::expected<void, ErrorCode>{};
}

tl::expected<void, ErrorCode> TieredBackend::CopyData(
    const std::string& key, const DataSource& source, UUID dest_tier_id,
    std::optional<uint64_t> expected_version) {
    if (!source.buffer || source.buffer->size() == 0) {
        LOG(ERROR) << "Invalid source buffer or size";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    auto dest_handle = Allocate(source.buffer->size(), dest_tier_id);
    if (!dest_handle.has_value()) {
        LOG(ERROR) << "Failed to allocate memory for key: " << key
                   << " in Tier " << dest_tier_id;
        return tl::make_unexpected(dest_handle.error());
    }

    auto write_result = Write(source, dest_handle.value());
    if (!write_result.has_value()) {
        LOG(ERROR) << "Failed to write data for key: " << key << " in Tier "
                   << dest_tier_id;
        return tl::make_unexpected(write_result.error());
    }

    // Commit (Add Replica)
    // Takes ownership of dest_handle into the map
    auto commit_result = Commit(key, dest_handle.value(), expected_version);
    if (!commit_result.has_value()) {
        // If CAS failed, we should probably warn specifically
        if (commit_result.error() != ErrorCode::CAS_FAILED) {
            LOG(ERROR) << "Failed to commit key: " << key << " in Tier "
                       << dest_tier_id;
        }
        return tl::make_unexpected(commit_result.error());
    }

    return tl::expected<void, ErrorCode>{};
}

tl::expected<void, ErrorCode> TieredBackend::Transfer(const std::string& key,
                                                      UUID source_tier_id,
                                                      UUID dest_tier_id) {
    uint64_t start_version = 0;
    auto source_handle_res = Get(key, source_tier_id, false, &start_version);
    if (!source_handle_res) {
        LOG(ERROR) << "Transfer failed: Source handle not found for key "
                   << key;
        return tl::make_unexpected(source_handle_res.error());
    }
    AllocationHandle source_handle = source_handle_res.value();

    return CopyData(key, source_handle->loc.data, dest_tier_id, start_version);
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

std::vector<UUID> TieredBackend::GetReplicaTierIds(
    const std::string& key) const {
    std::shared_lock map_lock(map_mutex_);
    auto it = metadata_index_.find(key);
    if (it == metadata_index_.end()) {
        return {};
    }

    std::shared_lock entry_lock(it->second->mutex);
    std::vector<UUID> tiers;
    tiers.reserve(it->second->replicas.size());
    for (const auto& replica : it->second->replicas) {
        tiers.push_back(replica.first);
    }
    return tiers;
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