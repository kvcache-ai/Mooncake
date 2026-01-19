#pragma once

#include <memory>
#include <map>
#include <string>
#include <vector>
#include <unordered_map>
#include <mutex>

#include "tiered_cache/disk_buffer.h"
#include "tiered_cache/cache_tier.h"
#include "storage_backend.h"

namespace mooncake {

class StorageTier : public CacheTier {
   public:
    StorageTier(UUID tier_id, const std::vector<std::string>& tags);

    ~StorageTier() override;

    tl::expected<void, ErrorCode> Init(TieredBackend* backend,
                                       TransferEngine* engine) override;

    tl::expected<void, ErrorCode> Allocate(size_t size,
                                           DataSource& data) override;

    tl::expected<void, ErrorCode> Free(DataSource data) override;

    tl::expected<void, ErrorCode> Commit(const std::string& key,
                                         const DataSource& data) override;

    tl::expected<void, ErrorCode> Flush() override;

    // --- Accessors ---
    UUID GetTierId() const override { return tier_id_; }
    size_t GetCapacity() const override;
    size_t GetUsage() const override;
    MemoryType GetMemoryType() const override {
        return MemoryType::NVME;
    }  // Or DISK if added
    const std::vector<std::string>& GetTags() const override { return tags_; }

   private:
    // Internal flush logic that triggers BatchOffload
    tl::expected<void, ErrorCode> FlushInternal();

    UUID tier_id_;
    std::vector<std::string> tags_;

    std::shared_ptr<StorageBackendInterface> storage_backend_;

    // Pending Write Buffer for aggregation
    // We map Key -> Data Slice (pointing to Staging Buffer data)
    // Note: The Staging Buffers must stay alive until Flush is called.
    // However, in the current TieredBackend flow, `AllocationEntry` owns the
    // buffer and keeps it alive as long as the handle exists. If handle is
    // destroyed, buffer is freed. WARNING: If user frees handle before Flush,
    // data pointer becomes invalid! FIX: We need to copy or share ownership of
    // the data in pending_batch_. For MVP, we will assume handle stays alive or
    // we hold a copy. To be safe, let's copy the data slice structure, but the
    // pointer validity relies on the `DiskStagingBuffer` not being destroyed.
    // Actually, `TieredBackend::Commit` calls `tier->Commit`. After that, the
    // handle is stored in `metadata_index_`. So the `DiskStagingBuffer`
    // (unique_ptr in loc.data) will be kept alive by the AllocationEntry in the
    // map. So it is SAFE to store Slice pointers in pending_batch_ as long as
    // we don't delete the key from TieredBackend before Flush.

    std::mutex batch_mutex_;
    std::unordered_map<std::string, StorageBuffer*> pending_batch_;
    size_t pending_batch_size_ = 0;

    // Configurable thresholds
    size_t batch_size_threshold_ = 64 * 1024 * 1024;  // 64MB
    size_t batch_count_threshold_ = 1000;
};

}  // namespace mooncake
