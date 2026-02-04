#pragma once

#include <string>
#include <vector>
#include <memory>
#include <unordered_map>
#include <optional>

#include "allocator.h"
#include "tiered_cache/tiers/cache_tier.h"
#include "transfer_engine.h"

namespace mooncake {

class DramCacheTier : public CacheTier {
   public:
    DramCacheTier(
        UUID tier_id, size_t capacity, const std::vector<std::string>& tags,
        std::optional<int> numa_node = std::nullopt,
        BufferAllocatorType allocator_type = BufferAllocatorType::OFFSET);
    ~DramCacheTier() override;

    tl::expected<void, ErrorCode> Init(TieredBackend* backend,
                                       TransferEngine* engine) override;
    tl::expected<void, ErrorCode> Allocate(size_t size,
                                           DataSource& data) override;
    tl::expected<void, ErrorCode> Free(DataSource data) override;

    UUID GetTierId() const override { return tier_id_; }
    size_t GetCapacity() const override { return capacity_; }
    size_t GetUsage() const override;
    const std::vector<std::string>& GetTags() const override { return tags_; }
    MemoryType GetMemoryType() const override { return MemoryType::DRAM; }

   private:
    UUID tier_id_;
    size_t capacity_;
    std::vector<std::string> tags_;
    std::optional<int> numa_node_;
    BufferAllocatorType allocator_type_;
    std::shared_ptr<BufferAllocatorBase> allocator_;
    TransferEngine* engine_;
    std::unique_ptr<char[], void (*)(char*)> memory_buffer_{
        nullptr, [](char* p) { delete[] p; }};
};

}  // namespace mooncake