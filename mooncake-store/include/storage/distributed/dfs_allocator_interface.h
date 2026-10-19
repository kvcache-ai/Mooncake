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
 * Common interface for DFS space allocators.
 *
 * DistributedFSDescriptor remains wire-compatible. In bucket mode its
 * shard_idx member carries the bucket id.
 */
class DfsAllocatorInterface {
   public:
    struct EvictionCandidate {
        std::string key;
        int shard_idx = 0;
        uint64_t offset = 0;
        DistributedFSDescriptor descriptor;
    };

    virtual ~DfsAllocatorInterface() = default;

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
    virtual void UpdateAccess(const std::string& key,
                              const DistributedFSDescriptor& descriptor) = 0;

    virtual bool IsEvictionEnabled() const = 0;
    virtual std::chrono::seconds GetEvictionCheckInterval() const = 0;
    virtual uint64_t GetUsedBytes() const = 0;
    virtual uint64_t GetTotalCapacity() const = 0;
    // Number of backing files that hold DFS data. In shard mode this is the
    // pre-allocated shard file count; in bucket mode it is the current
    // immutable bucket file count.
    virtual uint64_t GetFileCount() const = 0;
};

}  // namespace mooncake
