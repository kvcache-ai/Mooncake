#pragma once

#include <map>
#include <string>

#include "storage_usage.h"

namespace mooncake {

struct StorageUsageSnapshot : StorageUsage {
    std::map<std::string, StorageUsage> segments;
};

struct TieredStorageUsageSnapshot {
    StorageUsageSnapshot memory;
    StorageUsageSnapshot nof;
};

}  // namespace mooncake
