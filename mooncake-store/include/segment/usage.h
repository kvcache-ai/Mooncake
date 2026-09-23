#pragma once

#include <map>
#include <string>

#include "storage_usage.h"

namespace mooncake {

struct StorageUsageSnapshot : StorageUsage {
    std::map<std::string, StorageUsage> segments;
};

// DFS allocator usage. Unlike memory/nof tiers, DFS has no per-segment
// breakdown but tracks the number of backing files (shard files in shard
// mode, immutable bucket files in bucket mode).
struct DfsUsageSnapshot {
    bool enabled = false;
    uint64_t used_bytes = 0;
    uint64_t capacity_bytes = 0;
    uint64_t file_count = 0;
};

struct TieredStorageUsageSnapshot {
    StorageUsageSnapshot memory;
    StorageUsageSnapshot nof;
    DfsUsageSnapshot dfs;
};

}  // namespace mooncake
