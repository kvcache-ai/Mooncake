#pragma once

#include <chrono>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include <ylt/util/tl/expected.hpp>

#include "replica.h"
#include "types.h"

namespace mooncake {

struct DistributedStorageConfig;

/**
 * @brief Selects which DFS space-management strategy the master uses.
 *
 * SHARD keeps the existing fixed set of preallocated shard files managed by
 * DfsGlobalAllocator. BUCKET uses ImmutableBucketAllocator, which appends into
 * dynamically-created bucket files and reclaims space at bucket granularity.
 */
enum class DfsAllocatorType {
    SHARD,
    BUCKET,
};

std::optional<DfsAllocatorType> ParseDfsAllocatorType(std::string_view name);

const char* ToString(DfsAllocatorType type);

struct BatchAllocateRequest {
    std::string key;
    uint64_t size = 0;
};

struct BatchAllocateResult {
    std::string key;
    DistributedFSDescriptor descriptor;
    bool success = false;
    ErrorCode error = ErrorCode::OK;
};

/**
 * @brief Common interface for DFS space allocators.
 *
 * The master owns one implementation selected by configuration. Both
 * implementations are safe for concurrent RPC and eviction-thread access.
 * DistributedFSDescriptor remains unchanged: in bucket mode shard_idx carries
 * the bucket id.
 */
class GlobalAllocatorInterface {
   public:
    struct EvictionCandidate {
        std::string key;
        int shard_idx = 0;
        uint64_t offset = 0;
        DistributedFSDescriptor descriptor;
    };

    virtual ~GlobalAllocatorInterface() = default;

    virtual DfsAllocatorType Type() const = 0;
    virtual tl::expected<void, ErrorCode> Init(
        const DistributedStorageConfig& config) = 0;
    virtual bool IsInitialized() const = 0;

    virtual tl::expected<DistributedFSDescriptor, ErrorCode> Allocate(
        const std::string& key, uint64_t size) = 0;
    virtual std::vector<BatchAllocateResult> BatchAllocate(
        const std::vector<BatchAllocateRequest>& requests) = 0;

    virtual void Free(const std::string& key,
                      const DistributedFSDescriptor& descriptor) = 0;
    virtual void UpdateAccess(
        const std::string& key,
        const DistributedFSDescriptor& descriptor) = 0;

    virtual bool IsEvictionEnabled() const = 0;
    virtual std::chrono::seconds GetEvictionCheckInterval() const = 0;
    virtual uint64_t GetTotalCapacity() const = 0;
    virtual uint64_t GetUsedBytes() const = 0;
};

}  // namespace mooncake
