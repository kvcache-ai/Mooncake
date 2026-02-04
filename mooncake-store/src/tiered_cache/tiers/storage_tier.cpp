#include <glog/logging.h>

#include "tiered_cache/tiers/storage_tier.h"
#include "tiered_cache/copier_registry.h"
#include <cstring>

namespace mooncake {

StorageTier::StorageTier(UUID tier_id, const std::vector<std::string>& tags,
                         size_t capacity)
    : tier_id_(tier_id), tags_(tags), capacity_(capacity) {}

StorageTier::~StorageTier() {
    // Flush any pending data on destruction
    if (pending_batch_size_.load() > 0) {
        LOG(INFO) << "StorageTier dtor: Flushing " << pending_batch_.size()
                  << " pending items.";
        FlushInternal();
    }
}

tl::expected<void, ErrorCode> StorageTier::Init(TieredBackend* backend,
                                                TransferEngine* engine) {
    backend_ = backend;
    try {
        auto config = FileStorageConfig::FromEnvironment();
        // Validate config or set defaults if needed
        if (!config.Validate()) {
            LOG(ERROR) << "Invalid FileStorageConfig for StorageTier";
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }

        auto backend_res = CreateStorageBackend(config);
        if (!backend_res) {
            LOG(ERROR) << "Failed to create underlying storage backend: "
                       << backend_res.error();
            return tl::make_unexpected(backend_res.error());
        }
        storage_backend_ = backend_res.value();

        auto init_res = storage_backend_->Init();
        if (!init_res) {
            LOG(ERROR) << "Failed to init storage backend: "
                       << init_res.error();
            return init_res;
        }

    } catch (const std::exception& e) {
        LOG(ERROR) << "Exception during StorageTier init: " << e.what();
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }

    LOG(INFO) << "StorageTier initialized successfully.";
    return {};
}

tl::expected<void, ErrorCode> StorageTier::Allocate(size_t size,
                                                    DataSource& data) {
    try {
        // Create a Staging Buffer in DRAM.
        // This does not consume disk space yet.
        // Create a Storage Buffer (Staging mode).
        // This does not consume disk space yet.
        auto staging_buffer =
            std::make_unique<StorageBuffer>(size, storage_backend_.get());

        data.buffer = std::move(staging_buffer);
        data.type = MemoryType::NVME;  // Staging buffer, but logically part of
                                       // StorageTier

        return {};
    } catch (const std::bad_alloc& e) {
        LOG(ERROR) << "Failed to allocate staging buffer: " << e.what();
        return tl::make_unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
    }
}

tl::expected<void, ErrorCode> StorageTier::Free(DataSource data) {
    if (!data.buffer) return {};

    // Check if this buffer is in our pending batch and remove it
    if (auto* staging = dynamic_cast<StorageBuffer*>(data.buffer.get())) {
        size_t size = staging->size();
        std::string key = staging->GetKey();
        bool was_pending = false;

        if (!key.empty()) {
            std::unique_lock<std::mutex> lock(batch_mutex_);
            auto it = pending_batch_.find(key);
            if (it != pending_batch_.end() && it->second == staging) {
                // Remove from pending batch
                pending_batch_.erase(it);
                pending_batch_size_.fetch_sub(size);
                was_pending = true;
                VLOG(1) << "Removed key " << key
                        << " from pending batch (Freed explicitly)";
            }
        }

        // If not in pending batch, it was persisted - update persisted_size_
        if (!was_pending && staging->IsPersisted()) {
            // Subtract from persisted size
            size_t current = persisted_size_.load();
            while (current >= size &&
                   !persisted_size_.compare_exchange_weak(current, current - size)) {
                // Retry CAS
            }
            VLOG(1) << "Freed persisted data for key " << key << ", size=" << size;
            // Note: Actual disk file deletion would require StorageBackend::RemoveFile
            // which is not exposed via StorageBackendInterface. For now, we just
            // update the accounting. The storage backend handles its own eviction.
        }
    }

    // Staging buffer will be freed by unique_ptr
    return {};
}

tl::expected<void, ErrorCode> StorageTier::Commit(const std::string& key,
                                                  const DataSource& data) {
    auto* staging = dynamic_cast<StorageBuffer*>(data.buffer.get());
    if (!staging) {
        LOG(ERROR) << "Invalid buffer type for StorageTier commit";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    bool trigger_flush = false;
    {
        std::unique_lock<std::mutex> lock(batch_mutex_);

        // Add to pending batch
        staging->SetKey(key);
        pending_batch_[key] = staging;
        pending_batch_size_.fetch_add(staging->size());

        // Check thresholds
        const size_t batch_size = pending_batch_.size();
        if (batch_size >= batch_count_threshold_ ||
            pending_batch_size_.load() >= batch_size_threshold_) {
            trigger_flush = true;
        }
    }

    if (trigger_flush) {
        VLOG(1) << "Auto-flushing StorageTier batch.";
        return FlushInternal();
    }

    return {};
}

tl::expected<void, ErrorCode> StorageTier::Flush() { return FlushInternal(); }

tl::expected<void, ErrorCode> StorageTier::FlushInternal() {
    std::unique_lock<std::mutex> lock(batch_mutex_);
    if (pending_batch_.empty()) return {};

    // 1. Snapshot pending data for IO
    std::unordered_map<std::string, std::vector<Slice>> batch_to_write;
    std::vector<StorageBuffer*> buffers_to_persist;
    batch_to_write.reserve(pending_batch_.size());
    buffers_to_persist.reserve(pending_batch_.size());

    size_t batch_total_size = 0;
    for (const auto& kv : pending_batch_) {
        batch_to_write[kv.first] = {kv.second->ToSlice()};
        buffers_to_persist.push_back(kv.second);
        batch_total_size += kv.second->size();
    }

    // 2. Clear pending state to allow new commits
    pending_batch_.clear();
    pending_batch_size_.store(0);

    lock.unlock();

    // 3. Execute IO (Slow, concurrent)
    auto res = storage_backend_->BatchOffload(batch_to_write, nullptr);
    if (!res) {
        LOG(ERROR) << "Flush failed: " << res.error();
        return tl::make_unexpected(res.error());
    }

    // 4. Update state (Persist / Free DRAM)
    for (auto* buf : buffers_to_persist) {
        buf->Persist();
    }

    // 5. Update persisted size tracking
    persisted_size_.fetch_add(batch_total_size);

    return {};
}

size_t StorageTier::GetCapacity() const {
    // Use configured capacity if set, otherwise use config default
    if (capacity_ > 0) {
        return capacity_;
    }
    // Fallback to storage backend config
    if (storage_backend_) {
        return storage_backend_->file_storage_config_.total_size_limit;
    }
    return 1024ULL * 1024 * 1024 * 1024;  // 1TB fallback
}

size_t StorageTier::GetUsage() const {
    // Total usage = pending (staging) + persisted (on disk)
    return pending_batch_size_.load() + persisted_size_.load();
}

// Static registration of copy functions for NVME tier (Staging Buffer)
static CopierRegistrar storage_tier_copier_registrar(
    MemoryType::NVME,
    // To DRAM (NVME -> DRAM)
    [](const DataSource& src,
       const DataSource& dst) -> tl::expected<void, ErrorCode> {
        if (!src.buffer || !dst.buffer)
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        if (dst.buffer->size() < src.buffer->size())
            return tl::make_unexpected(ErrorCode::BUFFER_OVERFLOW);

        StorageBuffer* storage_buf =
            dynamic_cast<StorageBuffer*>(src.buffer.get());
        if (storage_buf) {
            return storage_buf->ReadTo(
                reinterpret_cast<void*>(dst.buffer->data()),
                dst.buffer->size());
        }

        // Fallback or error (should be StorageBuffer)
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    },
    // From DRAM (DRAM -> NVME)
    [](const DataSource& src,
       const DataSource& dst) -> tl::expected<void, ErrorCode> {
        if (!src.buffer || !dst.buffer)
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        if (dst.buffer->size() < src.buffer->size())
            return tl::make_unexpected(ErrorCode::BUFFER_OVERFLOW);

        if (dst.buffer->data() == 0) {
            return tl::make_unexpected(
                ErrorCode::INVALID_PARAMS);  // OnDisk, cannot write
        }
        memcpy(reinterpret_cast<void*>(dst.buffer->data()),
               reinterpret_cast<const void*>(src.buffer->data()),
               src.buffer->size());
        return {};
    });

}  // namespace mooncake
