#include "gpu_staging_utils.h"

namespace mooncake {
namespace gpu_staging {

std::mutex& PinnedHostMemoryMutex() {
    static std::mutex mutex;
    return mutex;
}

size_t& PinnedHostMemoryBytes() {
    static size_t bytes = 0;
    return bytes;
}

std::unordered_map<void*, size_t>& PinnedHostMemoryRegions() {
    static std::unordered_map<void*, size_t> regions;
    return regions;
}

}  // namespace gpu_staging
}  // namespace mooncake
