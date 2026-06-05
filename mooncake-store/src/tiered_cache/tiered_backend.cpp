#include <glog/logging.h>
#include <algorithm>
#include <vector>
#include <limits>

#include "tiered_cache/tiered_backend.h"
#include "tiered_cache/tiers/cache_tier.h"
#include "tiered_cache/tiers/dram_tier.h"
#ifdef USE_ASCEND_CACHE_TIER
#include "tiered_cache/tiers/ascend_tier.h"
#endif
#include "tiered_cache/tiers/storage_tier.h"
#include "tiered_cache/scheduler/client_scheduler.h"
#include "utils.h"

namespace mooncake {

static size_t ParseByteSize(const Json::Value& v) {
    if (v.isString()) {
        return static_cast<size_t>(string_to_byte_size(v.asString()));
    }
    return static_cast<size_t>(v.asUInt64());
}

AllocationEntry::~AllocationEntry() {
    if (loc.tier) {
        // Keep the tier alive until the final handle has released the buffer.
        loc.tier->Free(std::move(loc.data));
    }
}

TieredBackend::TieredBackend(size_t metadata_shard_count) {
    metadata_shard_count_ = metadata_shard_count > 0
                                ? metadata_shard_count
                                : kDefaultMetadataShardCount;
    metadata_shards_.reserve(metadata_shard_count_);
    for (size_t i = 0; i < metadata_shard_count_; ++i) {
        metadata_shards_.push_back(std::make_unique<MetadataShard>());
    }
}

bool TieredBackend::InnerExist(const MetadataShard& shard, std::string_view key,
                               std::optional<UUID> tier_id) {
    auto it = shard.index.find(key);
    if (it == shard.index.end()) {
        return false;
    }
    if (!tier_id.has_value()) {
        return true;
    }
    auto entry = it->second;
    std::shared_lock<std::shared_mutex> entry_lock(entry->mutex);
    for (const auto& replica : entry->replicas) {
        if (replica.first == *tier_id) {
            return true;
        }
    }
    return false;
}

ConditionalExecuteResult<void> TieredBackend::conditionalExecute(
    std::string_view key, std::optional<UUID> tier_id,
    std::function<void()> on_exists,
    std::function<void()> on_not_exists) const {
    auto& shard = GetMetadataShard(key);
    std::shared_lock<std::shared_mutex> read_lock(shard.mutex);
    const bool exists = InnerExist(shard, key, tier_id);

    ConditionalExecuteResult<void> result;
    result.key_exists = exists;
    if (exists) {
        on_exists();
    } else {
        on_not_exists();
    }
    return result;
}

TieredBackend::~TieredBackend() {
    Stop();
    Destroy();
}

void TieredBackend::Stop() {
    // Ensure this runs only once.
    bool expected = false;
    if (!is_shutting_down_.compare_exchange_strong(expected, true,
                                                   std::memory_order_acq_rel)) {
        return;  // Already shutting down or shut down.
    }

    LOG(INFO) << "TieredBackend::Stop() — stopping scheduler";

    if (scheduler_) {
        scheduler_->Stop();
    }

    LOG(INFO) << "TieredBackend::Stop() — complete";
}

void TieredBackend::Destroy() {
    bool expected = false;
    if (!is_destroyed_.compare_exchange_strong(expected, true,
                                               std::memory_order_acq_rel)) {
        return;  // Already destroyed.
    }

    LOG(INFO) << "TieredBackend::Destroy() — unmounting segments";

    // Unmount segments from Master
    for (const auto& [id, tier] : tiers_) {
        if (!tier) continue;
        const auto& info = tier_info_.at(id);
        Segment segment;
        segment.extra = P2PSegmentExtraData{};
        segment.id = id;
        segment.name = "tier_" + std::to_string(id.first) + "_" +
                       std::to_string(id.second);
        segment.size = tier->GetCapacity();
        auto& p2p_extra = segment.GetP2PExtra();
        p2p_extra.priority = info.priority;
        p2p_extra.tags = info.tags;
        p2p_extra.memory_type = tier->GetMemoryType();
        p2p_extra.usage = tier->GetUsage();

        if (segment_sync_callback_) {
            auto result = segment_sync_callback_(segment, /*mount=*/false);
            if (!result) {
                LOG(WARNING)
                    << "Failed to unmount segment on destroy: tier_id=" << id
                    << ", error=" << result.error();
            }
        }
    }

    LOG(INFO) << "TieredBackend::Destroy() — complete";
}

tl::expected<void, ErrorCode> TieredBackend::Init(
    Json::Value root, TransferEngine* engine,
    AddReplicaCallback add_replica_callback,
    RemoveReplicaCallback remove_replica_callback,
    SegmentSyncCallback segment_sync_callback) {
    // Initialize DataCopier
    try {
        DataCopierBuilder builder;
        data_copier_ = builder.Build();
    } catch (const std::logic_error& e) {
        LOG(ERROR) << "Failed to build DataCopier: " << e.what();
        return tl::unexpected(ErrorCode::INTERNAL_ERROR);
    }

    // Register callback for syncing metadata to Master
    add_replica_callback_ = add_replica_callback;
    remove_replica_callback_ = remove_replica_callback;
    // Register callback for segment lifecycle synchronization with Master
    segment_sync_callback_ = segment_sync_callback;

    // Initialize Tiers
    if (!root.isMember("tiers")) {
        LOG(ERROR) << "Tiered cache config is missing 'tiers' array.";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }

#ifdef USE_ASCEND_CACHE_TIER
    bool ascend_tier_initialized = false;
#endif

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
        size_t capacity = ParseByteSize(tier_config["capacity"]);
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
        MemoryType memory_type;
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

            auto tier = std::make_shared<DramCacheTier>(
                id, capacity, tags, numa_node, allocator_type);
            auto init_result = tier->Init(this, engine);
            if (!init_result) {
                LOG(ERROR) << "Failed to initialize DRAM tier: id=" << id
                           << ", error=" << init_result.error();
                return tl::unexpected(init_result.error());
            }

            tiers_[id] = std::move(tier);
            tier_info_[id] = {priority, tags};
            memory_type = MemoryType::DRAM;
            LOG(INFO) << "Successfully initialized DRAM tier: id=" << id;
        }
#ifdef USE_ASCEND_CACHE_TIER
        else if (type == "ASCEND_NPU" || type == "ASCEND") {
            if (ascend_tier_initialized) {
                LOG(ERROR) << "Multiple Ascend tiers are not allowed, skipping "
                              "this tier";
                continue;
            }
            // Parse device_id
            int device_id = 0;
            if (tier_config.isMember("device_id")) {
                device_id = tier_config["device_id"].asInt();
            }

            LOG(INFO) << "Creating ASCEND_NPU tier: id=" << id
                      << ", capacity=" << capacity << ", priority=" << priority
                      << ", device_id=" << device_id;

            auto tier = std::make_shared<AscendCacheTier>(id, capacity, tags,
                                                          device_id);
            auto init_result = tier->Init(this, engine);
            if (!init_result) {
                LOG(ERROR) << "Failed to initialize ASCEND_NPU tier: id=" << id
                           << ", error=" << init_result.error();
                return tl::unexpected(init_result.error());
            }

            tiers_[id] = std::move(tier);
            tier_info_[id] = {priority, tags};
            memory_type = MemoryType::ASCEND_NPU;
            LOG(INFO) << "Successfully initialized ASCEND_NPU tier: id=" << id;
            ascend_tier_initialized = true;
        }
#endif
        else if (type == "STORAGE" || type == "DISK") {
            LOG(INFO) << "Creating Storage tier: id=" << id
                      << ", capacity=" << capacity << ", priority=" << priority;
            auto tier = std::make_shared<StorageTier>(id, tags, capacity);
            auto init_result = tier->Init(this, engine, tier_config);
            if (!init_result) {
                LOG(ERROR) << "Failed to initialize Storage tier: id=" << id
                           << ", error=" << init_result.error();
                return tl::unexpected(init_result.error());
            }
            tiers_[id] = std::move(tier);
            tier_info_[id] = {priority, tags};
            memory_type = MemoryType::NVME;
            LOG(INFO) << "Successfully initialized Storage tier: id=" << id;
        } else {
            LOG(ERROR) << "Unsupported tier type '" << type << "'";
            return tl::unexpected(ErrorCode::INVALID_PARAMS);
        }

        auto mount_result =
            MountSegment(id, capacity, priority, tags, memory_type);
        if (!mount_result) {
            LOG(ERROR) << "Failed to mount tier: id=" << id
                       << ", error=" << mount_result.error();
            return mount_result;
        }
    }

    // Initialize and Start Scheduler
    scheduler_ = std::make_unique<ClientScheduler>(this, root);
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

tl::expected<void, ErrorCode> TieredBackend::MountSegment(
    UUID id, size_t capacity, int priority,
    const std::vector<std::string>& tags, MemoryType memory_type) {
    Segment segment;
    segment.extra = P2PSegmentExtraData{};
    segment.id = id;
    segment.name =
        "tier_" + std::to_string(id.first) + "_" + std::to_string(id.second);
    segment.size = capacity;
    auto& p2p_extra = segment.GetP2PExtra();
    p2p_extra.priority = priority;
    p2p_extra.tags = tags;
    p2p_extra.memory_type = memory_type;
    p2p_extra.usage = 0;

    if (segment_sync_callback_) {
        auto mount_result = segment_sync_callback_(segment, /*mount=*/true);
        if (!mount_result) {
            LOG(ERROR) << "Failed to mount segment with Master: id=" << id
                       << ", error=" << mount_result.error();
            return tl::unexpected(mount_result.error());
        }
    }
    return {};
}

tl::expected<void, ErrorCode> TieredBackend::AllocateInternalRaw(
    size_t size, std::optional<UUID> preferred_tier, TieredLocation* out_loc) {
    if (!out_loc) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    ErrorCode last_error = ErrorCode::NO_AVAILABLE_HANDLE;
    auto remember_error = [&last_error](ErrorCode error) {
        if (last_error == ErrorCode::NO_AVAILABLE_HANDLE &&
            error != ErrorCode::NO_AVAILABLE_HANDLE) {
            last_error = error;
        }
    };

    // Try preferred tier first
    if (preferred_tier.has_value()) {
        auto it = tiers_.find(*preferred_tier);
        if (it != tiers_.end()) {
            auto alloc_result = it->second->Allocate(size, out_loc->data);
            if (alloc_result) {
                out_loc->tier = it->second;
                return {};
            }
            remember_error(alloc_result.error());
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
            out_loc->tier = tier;
            return {};
        }
        remember_error(alloc_result.error());
    }
    return tl::make_unexpected(last_error);
}

tl::expected<AllocationHandle, ErrorCode> TieredBackend::Allocate(
    size_t size, std::optional<UUID> preferred_tier, bool strict) {
    if (is_shutting_down_.load(std::memory_order_acquire)) {
        LOG(ERROR) << "TieredBackend is shutting down";
        return tl::make_unexpected(ErrorCode::SHUTTING_DOWN);
    }
    TieredLocation loc;

    // Strict mode: must allocate on preferred tier
    if (strict && preferred_tier.has_value()) {
        auto it = tiers_.find(*preferred_tier);
        if (it == tiers_.end()) {
            LOG(ERROR) << "Strict allocation failed: tier not found";
            return tl::make_unexpected(ErrorCode::TIER_NOT_FOUND);
        }

        // Try allocation
        auto alloc_result = it->second->Allocate(size, loc.data);
        if (alloc_result) {
            loc.tier = it->second;
            return std::make_shared<AllocationEntry>(this, std::move(loc));
        }

        // Failed - try sync eviction if available
        if (scheduler_ &&
            alloc_result.error() == ErrorCode::NO_AVAILABLE_HANDLE) {
            bool evicted =
                scheduler_->OnAllocationFailure(*preferred_tier, size);
            if (evicted) {
                // Retry after eviction
                alloc_result = it->second->Allocate(size, loc.data);
                if (alloc_result) {
                    LOG(INFO)
                        << "Strict allocation succeeded after sync eviction";
                    loc.tier = it->second;
                    return std::make_shared<AllocationEntry>(this,
                                                             std::move(loc));
                }
            }
        }

        LOG(ERROR) << "Strict allocation failed on tier " << *preferred_tier
                   << ", error: " << alloc_result.error();
        return tl::make_unexpected(alloc_result.error());
    }

    // Non-strict mode: try preferred tier + fallback (fast path)
    auto alloc_result = AllocateInternalRaw(size, preferred_tier, &loc);
    if (alloc_result) {
        return std::make_shared<AllocationEntry>(this, std::move(loc));
    }

    // All tiers failed - try sync eviction if enabled
    if (scheduler_ && alloc_result.error() == ErrorCode::NO_AVAILABLE_HANDLE) {
        // Determine which tier to evict from
        UUID evict_tier_id;
        if (preferred_tier.has_value()) {
            evict_tier_id = *preferred_tier;
        } else {
            // No preference - evict from highest priority tier (usually DRAM)
            auto sorted = GetSortedTiers();
            if (sorted.empty()) {
                LOG(ERROR) << "Failed to allocate " << size
                           << " bytes: no tiers";
                return tl::make_unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
            }
            evict_tier_id = sorted[0];
        }

        bool evicted = scheduler_->OnAllocationFailure(evict_tier_id, size);
        if (evicted) {
            // Retry allocation after eviction
            alloc_result = AllocateInternalRaw(size, preferred_tier, &loc);
            if (alloc_result) {
                LOG(INFO) << "Allocation succeeded after sync eviction";
                return std::make_shared<AllocationEntry>(this, std::move(loc));
            }
        }
    }

    LOG(ERROR) << "Failed to allocate " << size
               << " bytes, error: " << alloc_result.error();
    return tl::make_unexpected(alloc_result.error());
}

tl::expected<void, ErrorCode> TieredBackend::Write(const DataSource& source,
                                                   AllocationHandle handle) {
    if (is_shutting_down_.load(std::memory_order_acquire)) {
        LOG(ERROR) << "TieredBackend is shutting down";
        return tl::make_unexpected(ErrorCode::SHUTTING_DOWN);
    }
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
    std::string_view key, AllocationHandle handle,
    std::optional<uint64_t> expected_version, bool record_access) {
    if (is_shutting_down_.load(std::memory_order_acquire)) {
        LOG(ERROR) << "TieredBackend is shutting down";
        return tl::make_unexpected(ErrorCode::SHUTTING_DOWN);
    }
    if (!handle) return tl::make_unexpected(ErrorCode::INVALID_PARAMS);

    std::shared_ptr<MetadataEntry> entry = nullptr;
    auto& shard = GetMetadataShard(key);

    // Try to find existing entry (Shard Read Lock)
    {
        std::shared_lock<std::shared_mutex> read_lock(shard.mutex);
        auto it = shard.index.find(key);
        if (it != shard.index.end()) {
            entry = it->second;
        }
    }

    // Create if not exists (Shard Write Lock)
    if (!entry) {
        if (expected_version.has_value()) {
            return tl::make_unexpected(ErrorCode::CAS_FAILED);
        }

        std::unique_lock<std::shared_mutex> write_lock(shard.mutex);
        auto it = shard.index.find(key);
        if (it != shard.index.end()) {
            entry = it->second;
        } else {
            entry = std::make_shared<MetadataEntry>();
            shard.index.emplace(std::string(key), entry);
        }
    }

    // CAS check BEFORE any side effects
    if (expected_version.has_value()) {
        std::shared_lock<std::shared_mutex> entry_read_lock(entry->mutex);
        if (entry->version != expected_version.value()) {
            VLOG(1) << "CAS Failed for key " << key
                    << ": valid_version=" << entry->version
                    << ", expected=" << expected_version.value();
            return tl::make_unexpected(ErrorCode::CAS_FAILED);
        }
    }

    // Side effects: tier commit + metadata sync (only after CAS passes)
    auto tier_commit_res = handle->loc.tier->Commit(key, handle->loc.data);
    if (!tier_commit_res) {
        LOG(ERROR) << "Tier Commit failed for key " << key << ": "
                   << tier_commit_res.error();
        return tl::make_unexpected(tier_commit_res.error());
    }

    UUID current_tier_id = handle->loc.tier->GetTierId();
    size_t handle_size =
        handle->loc.data.buffer ? handle->loc.data.buffer->size() : 0;

    // Update Entry (Entry Write Lock)
    {
        std::unique_lock<std::shared_mutex> entry_lock(entry->mutex);

        // Re-check version under write lock (another commit may have raced)
        if (expected_version.has_value() &&
            entry->version != expected_version.value()) {
            VLOG(1) << "CAS Failed (re-check) for key " << key;
            return tl::make_unexpected(ErrorCode::CAS_FAILED);
        }

        // Insert or replace the handle for this tier
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

    if (scheduler_) {
        scheduler_->OnCommit(key, current_tier_id, handle_size);
        if (record_access) {
            scheduler_->OnAccess(key);
        }
    }

    if (add_replica_callback_) {
        size_t data_size =
            handle->loc.data.buffer ? handle->loc.data.buffer->size() : 0;
        auto result = add_replica_callback_(key, handle->loc.tier->GetTierId(),
                                            data_size);

        if (!result.has_value()) {
            LOG(ERROR) << "Failed to Commit key " << key
                       << " to Master, error_code=" << result.error();
            return tl::make_unexpected(result.error());
        }
    }

    return tl::expected<void, ErrorCode>{};
}

tl::expected<AllocationHandle, ErrorCode> TieredBackend::Get(
    std::string_view key, std::optional<UUID> tier_id, bool record_access,
    uint64_t* out_version) {
    if (is_shutting_down_.load(std::memory_order_acquire)) {
        LOG(ERROR) << "TieredBackend is shutting down";
        return tl::make_unexpected(ErrorCode::SHUTTING_DOWN);
    }
    std::shared_ptr<MetadataEntry> entry = nullptr;

    // Find Entry (Shard Read Lock)
    {
        auto& shard = GetMetadataShard(key);
        std::shared_lock<std::shared_mutex> read_lock(shard.mutex);
        auto it = shard.index.find(key);
        if (it == shard.index.end()) {
            LOG(ERROR) << "Key not found: " << key;
            return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
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
    return entry->replicas.begin()->second;
}

bool TieredBackend::Exist(std::string_view key,
                          std::optional<UUID> tier_id) const {
    auto& shard = GetMetadataShard(key);
    std::shared_lock<std::shared_mutex> read_lock(shard.mutex);
    return InnerExist(shard, key, tier_id);
}

tl::expected<void, ErrorCode> TieredBackend::Delete(std::string_view key,
                                                    std::optional<UUID> tier_id,
                                                    bool notify_master) {
    if (is_shutting_down_.load(std::memory_order_acquire)) {
        LOG(ERROR) << "TieredBackend is shutting down";
        return tl::make_unexpected(ErrorCode::SHUTTING_DOWN);
    }
    // Hold references locally to ensure destruction happens OUTSIDE the
    // locks This is crucial for non-blocking deletions.
    AllocationHandle handle_ref = nullptr;
    std::vector<AllocationHandle> handles_to_free;

    if (tier_id.has_value()) {
        // Delete Specific Replica

        bool need_cleanup = false;
        bool found_tier = false;

        auto& shard = GetMetadataShard(key);
        // Optimistic Delete (Shard Read Lock + Entry Write Lock)
        {
            std::shared_lock<std::shared_mutex> read_lock(shard.mutex);
            auto it = shard.index.find(key);
            if (it == shard.index.end()) {
                LOG(ERROR) << "Key not found: " << key;
                return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
            }
            auto entry = it->second;

            std::unique_lock<std::shared_mutex> entry_write_lock(entry->mutex);
            auto tier_it = entry->replicas.end();
            for (auto it = entry->replicas.begin(); it != entry->replicas.end();
                 ++it) {
                if (it->first == *tier_id) {
                    tier_it = it;
                    break;
                }
            }

            if (tier_it != entry->replicas.end()) {
                if (notify_master && remove_replica_callback_) {
                    auto result = remove_replica_callback_(
                        key, tier_it->second->loc.tier->GetTierId());
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
        }  // Read lock released here

        // Retry with Write Lock
        // If the entry is empty, we upgrade to a global write lock to
        // remove it. This prevents memory leaks from empty "zombie"
        // entries.
        if (need_cleanup) {
            std::unique_lock<std::shared_mutex> write_lock(shard.mutex);

            auto it = shard.index.find(key);
            if (it != shard.index.end()) {
                auto entry = it->second;

                // Double-Check Locking:
                // Another thread might have added a replica now
                std::unique_lock<std::shared_mutex> entry_lock(entry->mutex);

                if (entry->replicas.empty()) {
                    shard.index.erase(it);
                }
            }
        }

        if (found_tier) {
            if (scheduler_) {
                scheduler_->OnDelete(key, *tier_id);
            }
            return tl::expected<void, ErrorCode>{};
        } else {
            LOG(ERROR) << "Tier not found: " << *tier_id;
            return tl::make_unexpected(ErrorCode::TIER_NOT_FOUND);
        }
    } else {
        // Delete All Replicas (Full Key Deletion)
        auto& shard = GetMetadataShard(key);
        std::unique_lock<std::shared_mutex> shard_write_lock(shard.mutex);
        auto it = shard.index.find(key);
        if (it == shard.index.end()) {
            LOG(ERROR) << "Key not found: " << key;
            return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
        }
        auto entry = it->second;

        {
            std::unique_lock<std::shared_mutex> entry_lock(entry->mutex);
            handles_to_free.reserve(entry->replicas.size());
            for (auto& replica : entry->replicas) {
                if (notify_master && remove_replica_callback_) {
                    UUID segment_id = replica.first;
                    if (replica.second && replica.second->loc.tier) {
                        segment_id = replica.second->loc.tier->GetTierId();
                    }
                    auto result = remove_replica_callback_(key, segment_id);
                    if (!result.has_value()) {
                        LOG(WARNING)
                            << "Delete(all-tiers): notify master failed"
                            << ", key=" << key << ", segment_id=" << segment_id
                            << ", error_code=" << result.error();
                    }
                }
                handles_to_free.push_back(replica.second);
            }
            entry->replicas.clear();
        }

        shard.index.erase(it);
    }

    // Handles go out of scope here.
    // Ref count drops to 0 -> ~AllocationEntry() -> Free().
    // This happens concurrently without holding any locks.
    if (scheduler_) {
        scheduler_->OnDelete(key, std::nullopt);
    }
    return tl::expected<void, ErrorCode>{};
}

tl::expected<long, ErrorCode> TieredBackend::RemoveAll() {
    if (is_shutting_down_.load(std::memory_order_acquire)) {
        LOG(ERROR) << "TieredBackend is shutting down";
        return tl::make_unexpected(ErrorCode::SHUTTING_DOWN);
    }

    long total_removed = 0;

    for (size_t s = 0; s < metadata_shard_count_; ++s) {
        auto& shard = *metadata_shards_[s];

        std::unordered_map<std::string, std::shared_ptr<MetadataEntry>,
                           StringHash, std::equal_to<>>
            drained;
        {
            std::unique_lock<std::shared_mutex> shard_write_lock(shard.mutex);
            if (shard.index.empty()) continue;
            drained.swap(shard.index);
        }

        for (auto& [key, entry] : drained) {
            // Drain replicas under the entry lock; they live in the local
            // `replicas` vector and are destructed when this iteration ends.
            std::vector<std::pair<UUID, AllocationHandle>> replicas;
            {
                std::unique_lock<std::shared_mutex> entry_lock(entry->mutex);
                replicas.swap(entry->replicas);
                entry->version++;
            }

            // Notify master per (key, segment_id) via DELETE callback.
            // Failures are logged only; do not abort local cleanup.
            if (remove_replica_callback_) {
                for (const auto& [tier_id, handle] : replicas) {
                    UUID segment_id = tier_id;
                    if (handle && handle->loc.tier) {
                        segment_id = handle->loc.tier->GetTierId();
                    }
                    auto result = remove_replica_callback_(key, segment_id);
                    if (!result.has_value()) {
                        LOG(WARNING)
                            << "RemoveAll: notify master failed"
                            << ", key=" << key << ", segment_id=" << segment_id
                            << ", error_code=" << result.error();
                    }
                }
            }

            if (scheduler_) {
                scheduler_->OnDelete(key, std::nullopt);
            }

            ++total_removed;
            // `replicas` destructs here -> AllocationHandle ref_count drops
        }
        // `drained` destructs here, releasing MetadataEntry shared_ptrs.
    }

    return total_removed;
}

tl::expected<void, ErrorCode> TieredBackend::CopyData(
    std::string_view key, const DataSource& source, UUID dest_tier_id,
    std::optional<uint64_t> expected_version, bool record_access) {
    if (is_shutting_down_.load(std::memory_order_acquire)) {
        LOG(ERROR) << "TieredBackend is shutting down";
        return tl::make_unexpected(ErrorCode::SHUTTING_DOWN);
    }
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

    auto commit_result =
        Commit(key, dest_handle.value(), expected_version, record_access);
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

tl::expected<void, ErrorCode> TieredBackend::Transfer(std::string_view key,
                                                      UUID source_tier_id,
                                                      UUID dest_tier_id,
                                                      bool record_access) {
    if (is_shutting_down_.load(std::memory_order_acquire)) {
        LOG(ERROR) << "TieredBackend is shutting down";
        return tl::make_unexpected(ErrorCode::SHUTTING_DOWN);
    }
    uint64_t start_version = 0;
    auto source_handle_res = Get(key, source_tier_id, false, &start_version);
    if (!source_handle_res) {
        LOG(ERROR) << "Transfer failed: Source handle not found for key "
                   << key;
        return tl::make_unexpected(source_handle_res.error());
    }
    AllocationHandle source_handle = source_handle_res.value();

    // Check if destination tier has enough space before attempting allocation
    size_t required_size = source_handle->loc.data.buffer->size();
    auto dest_tier_it = tiers_.find(dest_tier_id);
    if (dest_tier_it != tiers_.end()) {
        size_t dest_capacity = dest_tier_it->second->GetCapacity();
        size_t dest_usage = dest_tier_it->second->GetUsage();
        size_t dest_available =
            (dest_capacity > dest_usage) ? (dest_capacity - dest_usage) : 0;

        if (dest_available < required_size) {
            // Insufficient space, skip this transfer silently
            VLOG(2) << "Insufficient space in destination tier " << dest_tier_id
                    << " for key " << key << " (required: " << required_size
                    << ", available: " << dest_available << ")";
            return tl::make_unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
        }
    }

    return CopyData(key, source_handle->loc.data, dest_tier_id, start_version,
                    record_access);
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

std::vector<UUID> TieredBackend::GetReplicaTierIds(std::string_view key) const {
    auto& shard = GetMetadataShard(key);
    std::shared_lock map_lock(shard.mutex);
    auto it = shard.index.find(key);
    if (it == shard.index.end()) {
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

void TieredBackend::ForEachKeyBatch(
    const std::function<bool(std::vector<ReplicaLocation>&&)>& callback) const {
    // Iterate per-shard. Each shard is locked independently.
    for (size_t s = 0; s < metadata_shard_count_; ++s) {
        auto& shard = *metadata_shards_[s];

        // Copy entry pointers under shard shared lock
        std::vector<std::pair<std::string, std::shared_ptr<MetadataEntry>>>
            shard_entries;
        {
            std::shared_lock<std::shared_mutex> lock(shard.mutex);
            shard_entries.reserve(shard.index.size());
            for (const auto& [key, entry] : shard.index) {
                shard_entries.emplace_back(key, entry);
            }
        }

        // Read per-entry data outside shard lock
        std::vector<ReplicaLocation> result;
        result.reserve(shard_entries.size());
        for (auto& [key, entry] : shard_entries) {
            std::shared_lock<std::shared_mutex> entry_lock(entry->mutex);
            for (const auto& [tier_id, handle] : entry->replicas) {
                size_t size = 0;
                if (handle && handle->loc.data.buffer) {
                    size = handle->loc.data.buffer->size();
                }
                result.push_back({key, tier_id, size});
            }
        }

        if (!result.empty() && !callback(std::move(result))) return;
    }
}

AccessStats TieredBackend::GetHotKeyStats() const {
    if (scheduler_) {
        return scheduler_->GetHotKeyStats();
    }
    return {};
}

}  // namespace mooncake
