#include "storage/distributed/dfs_global_allocator.h"

#include <algorithm>
#include <filesystem>
#include <iomanip>
#include <sstream>

#include "storage/distributed/fs_adapter.h"
#include "storage/distributed/posix_fs_adapter.h"
#include "utils.h"
#ifdef USE_3FS
#include "storage/distributed/hf3fs_adapter.h"
#endif

namespace mooncake {

DfsGlobalAllocator::~DfsGlobalAllocator() {
    running_.store(false, std::memory_order_release);
    if (eviction_thread_.joinable()) eviction_thread_.join();
    if (fs_adapter_) fs_adapter_->Shutdown();
}

bool DfsGlobalAllocator::Init(const std::string& mount_path, int shard_count,
                              uint64_t shard_capacity, uint64_t alignment) {
    if (initialized_.load(std::memory_order_acquire)) return true;
    if (mount_path.empty() || shard_count <= 0 || shard_capacity == 0) {
        return false;
    }
    if (alignment == 0 || (alignment & (alignment - 1)) != 0) {
        return false;
    }

    mount_path_ = mount_path;
    shard_count_ = shard_count;
    alignment_ = alignment;
    shards_.clear();
    shards_.resize(shard_count_);

    eviction_enabled_ = GetEnvOr<bool>("MOONCAKE_DFS_EVICTION_ENABLED", true);
    eviction_high_watermark_ =
        GetEnvOr<double>("MOONCAKE_DFS_EVICTION_HIGH_WATERMARK", 0.9);
    eviction_low_watermark_ =
        GetEnvOr<double>("MOONCAKE_DFS_EVICTION_LOW_WATERMARK", 0.7);
    deferred_free_duration_ = std::chrono::seconds(
        GetEnvOr<int>("MOONCAKE_DFS_DEFERRED_FREE_SECONDS", 30));
    eviction_check_interval_ = std::chrono::seconds(
        GetEnvOr<int>("MOONCAKE_DFS_EVICTION_CHECK_INTERVAL", 5));

    std::error_code ec;
    std::filesystem::create_directories(mount_path_, ec);
    if (ec) {
        LOG(ERROR) << "Failed to create DFS mount path " << mount_path_ << ": "
                   << ec.message();
        return false;
    }

    const auto adapter_type =
        GetEnvStringOr("MOONCAKE_DFS_FS_ADAPTER",
                       GetEnvStringOr("MOONCAKE_DISTRIBUTED_FS_TYPE", "hf3fs"));
    if (adapter_type == "posix") {
        fs_adapter_ = std::make_unique<PosixFsAdapter>();
    } else if (adapter_type == "hf3fs") {
#ifdef USE_3FS
        fs_adapter_ = std::make_unique<Hf3fsAdapter>();
#else
        LOG(ERROR) << "DFS allocator hf3fs adapter requires USE_3FS";
        return false;
#endif
    } else {
        LOG(ERROR) << "Unsupported DFS fs adapter: " << adapter_type;
        return false;
    }

    if (!fs_adapter_->Init(mount_path_)) return false;

    for (int i = 0; i < shard_count_; ++i) {
        std::string path = mount_path_ + "/dfs_shard_" +
                           FormatShardIdx(i, shard_count_) + ".data";
        auto prealloc = fs_adapter_->PreallocateFile(path, shard_capacity);
        if (!prealloc) {
            LOG(ERROR) << "Failed to preallocate DFS shard " << path << ": "
                       << prealloc.error();
            return false;
        }

        auto shard = std::make_unique<ShardState>();
        shard->capacity = shard_capacity;
        uint32_t init_cap = static_cast<uint32_t>(std::max<uint64_t>(
            1, std::min<uint64_t>(shard_capacity / 4096, 64ULL * 1024)));
        uint32_t max_cap = static_cast<uint32_t>(std::max<uint64_t>(
            init_cap,
            std::min<uint64_t>(shard_capacity / 1024, 64ULL * 1024 * 1024)));
        shard->allocator =
            OffsetAllocator::create(0, shard_capacity, init_cap, max_cap);
        if (!shard->allocator) return false;
        shards_[i] = std::move(shard);
    }

    running_.store(true, std::memory_order_release);
    if (eviction_enabled_) {
        eviction_thread_ =
            std::thread(&DfsGlobalAllocator::EvictionMonitor, this);
    }
    initialized_.store(true, std::memory_order_release);
    return true;
}

tl::expected<DistributedFSDescriptor, ErrorCode> DfsGlobalAllocator::Allocate(
    const std::string& key, uint64_t size) {
    if (!initialized_.load(std::memory_order_acquire)) {
        return tl::make_unexpected(ErrorCode::DFS_SERVICE_UNAVAILABLE);
    }
    if (key.empty() || size == 0) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    auto key_lock = LockKey(key);
    int shard_idx = SelectShard(key);
    uint64_t aligned_size = AlignSize(size);
    auto& shard = *shards_[shard_idx];

    std::unique_lock handle_lock(shard.handle_mutex);
    ProcessPendingFrees(shard_idx);

    auto handle = shard.allocator->allocate(aligned_size);
    if (!handle) return tl::make_unexpected(ErrorCode::NO_AVAILABLE_HANDLE);

    uint64_t raw_offset = handle->address();
    uint64_t alloc_offset = AlignSize(raw_offset);
    auto alloc_handle =
        std::make_shared<OffsetAllocationHandle>(std::move(*handle));
    shard.offset_to_handle[alloc_offset] = std::move(alloc_handle);
    handle_lock.unlock();

    return DistributedFSDescriptor{
        mount_path_ + "/dfs_shard_" + FormatShardIdx(shard_idx, shard_count_) +
            ".data",
        alloc_offset,
        size,
        aligned_size,
        shard_idx,
    };
}

void DfsGlobalAllocator::Free(uint64_t offset, uint64_t /*aligned_size*/,
                              int shard_idx) {
    if (!initialized_.load(std::memory_order_acquire)) return;
    if (shard_idx < 0 || shard_idx >= shard_count_) return;

    auto& shard = *shards_[shard_idx];
    std::lock_guard handle_lock(shard.handle_mutex);
    auto it = shard.offset_to_handle.find(offset);
    if (it == shard.offset_to_handle.end()) return;

    {
        std::lock_guard pending_lock(shard.pending_mutex);
        shard.pending_free.push_back(
            {it->second,
             std::chrono::steady_clock::now() + deferred_free_duration_});
    }
    shard.offset_to_handle.erase(it);
}

void DfsGlobalAllocator::UpdateAccess(const std::string& key, int shard_idx,
                                      uint64_t offset) {
    if (!initialized_.load(std::memory_order_acquire)) return;
    if (shard_idx < 0 || shard_idx >= shard_count_) return;

    auto& shard = *shards_[shard_idx];
    std::lock_guard lru_lock(shard.lru_mutex);
    auto lru_it = shard.lru_index.find(key);
    if (lru_it != shard.lru_index.end()) {
        lru_it->second->second = offset;
        shard.lru_list.splice(shard.lru_list.begin(), shard.lru_list,
                              lru_it->second);
    } else {
        shard.lru_list.push_front({key, offset});
        shard.lru_index[key] = shard.lru_list.begin();
    }
}

std::vector<DfsGlobalAllocator::EvictedKey>
DfsGlobalAllocator::EvictIfNeeded() {
    std::vector<EvictedKey> all_evicted;
    if (!initialized_.load(std::memory_order_acquire)) return all_evicted;
    for (int i = 0; i < shard_count_; ++i) {
        auto evicted = EvictFromShard(i);
        all_evicted.insert(all_evicted.end(), evicted.begin(), evicted.end());
    }
    return all_evicted;
}

void DfsGlobalAllocator::SetEvictCallback(
    std::function<void(const std::string&, int, uint64_t)> cb) {
    on_evict_callback_ = std::move(cb);
}

std::string DfsGlobalAllocator::FormatShardIdx(int idx, int shard_count) {
    int width = static_cast<int>(std::max<size_t>(
        2, std::to_string(std::max(0, shard_count - 1)).size()));
    std::ostringstream oss;
    oss << std::setw(width) << std::setfill('0') << idx;
    return oss.str();
}

void DfsGlobalAllocator::ProcessPendingFrees(int shard_idx) {
    auto& shard = *shards_[shard_idx];
    std::lock_guard pending_lock(shard.pending_mutex);
    auto now = std::chrono::steady_clock::now();
    while (!shard.pending_free.empty() &&
           shard.pending_free.front().when <= now) {
        shard.pending_free.pop_front();
    }
}

std::vector<DfsGlobalAllocator::EvictedKey> DfsGlobalAllocator::EvictFromShard(
    int shard_idx) {
    std::vector<EvictedKey> evicted;
    auto& shard = *shards_[shard_idx];

    {
        std::shared_lock lock(shard.handle_mutex);
        auto report = shard.allocator->storageReport();
        double usage = 1.0 - static_cast<double>(report.totalFreeSpace) /
                                 static_cast<double>(shard.capacity);
        if (usage < eviction_high_watermark_) return evicted;
    }

    while (true) {
        std::string evict_key;
        uint64_t evict_offset = 0;
        {
            std::lock_guard lru_lock(shard.lru_mutex);
            if (shard.lru_list.empty()) break;
            auto [key, offset] = shard.lru_list.back();
            evict_key = key;
            evict_offset = offset;
            shard.lru_list.pop_back();
            shard.lru_index.erase(evict_key);
        }

        {
            std::lock_guard handle_lock(shard.handle_mutex);
            auto it = shard.offset_to_handle.find(evict_offset);
            if (it == shard.offset_to_handle.end()) {
                continue;
            }
            {
                std::lock_guard pending_lock(shard.pending_mutex);
                shard.pending_free.push_back(
                    {it->second, std::chrono::steady_clock::now() +
                                     deferred_free_duration_});
            }
            shard.offset_to_handle.erase(it);
        }

        evicted.push_back({evict_key, shard_idx, evict_offset});

        {
            std::shared_lock lock(shard.handle_mutex);
            auto report = shard.allocator->storageReport();
            double usage = 1.0 - static_cast<double>(report.totalFreeSpace) /
                                     static_cast<double>(shard.capacity);
            if (usage < eviction_low_watermark_) break;
        }
    }
    return evicted;
}

void DfsGlobalAllocator::EvictionMonitor() {
    while (running_.load(std::memory_order_acquire)) {
        for (int i = 0; i < shard_count_; ++i) {
            auto& shard = *shards_[i];
            std::lock_guard pending_lock(shard.pending_mutex);
            auto now = std::chrono::steady_clock::now();
            while (!shard.pending_free.empty() &&
                   shard.pending_free.front().when <= now) {
                shard.pending_free.pop_front();
            }
        }

        if (eviction_enabled_) {
            auto evicted = EvictIfNeeded();
            for (const auto& ev : evicted) {
                if (on_evict_callback_) {
                    on_evict_callback_(ev.key, ev.shard_idx, ev.offset);
                }
            }
        }
        std::this_thread::sleep_for(eviction_check_interval_);
    }
}

int DfsGlobalAllocator::SelectShard(const std::string& key) const {
    return std::hash<std::string>{}(key) % shard_count_;
}

uint64_t DfsGlobalAllocator::AlignSize(uint64_t size) const {
    return (size + alignment_ - 1) & ~(alignment_ - 1);
}

}  // namespace mooncake
