#include <algorithm>
#include <mutex>
#include <unordered_map>
#include <vector>
#include <glog/logging.h>

#include "ub_allocator.h"

namespace mooncake {
struct UbStoreMemRange {
    void* base;
    size_t size;
};
std::mutex g_ub_store_mem_mutex;
std::vector<UbStoreMemRange> g_ub_store_mem_ranges;

void remove_store_memory_range(void* ptr) {
    std::lock_guard<std::mutex> store_lock(g_ub_store_mem_mutex);
    auto it = std::remove_if(
        g_ub_store_mem_ranges.begin(), g_ub_store_mem_ranges.end(),
        [ptr](const UbStoreMemRange& range) { return range.base == ptr; });
    g_ub_store_mem_ranges.erase(it, g_ub_store_mem_ranges.end());
}

void* ub_allocate_memory(size_t alignment, size_t total_size) {
    size_t page_size = getpagesize();
    size_t ub_alignment = std::max(alignment / page_size * page_size, page_size);
    void** ptr = nullptr;
    auto ret = posix_memalign(ptr, ub_alignment, total_size);
    if (!ret) {
        LOG(ERROR) << "failed for UB protocol, size="
                   << total_size << ", alignment : " << alignment;
        return nullptr;
    }
    LOG(INFO) << "UB:  allocated total size : " << total_size
                << ", alignment : " << alignment << " addr at " << *ptr;

    std::lock_guard<std::mutex> store_lock(g_ub_store_mem_mutex);
    g_ub_store_mem_ranges.push_back({*ptr, total_size});

    return *ptr;
}

void ub_free_memory(void* ptr) {
    if (!ptr) {
        return;
    }
    remove_store_memory_range(ptr);
    free(ptr);
    LOG(INFO) << "UB: freed  bytes at " << ptr;
}

bool ub_is_store_memory(void* addr, size_t length) {
    if (!addr || length == 0) return false;
    auto addr_start = reinterpret_cast<uintptr_t>(addr);
    uintptr_t addr_end = addr_start + length;
    std::lock_guard<std::mutex> lock(g_ub_store_mem_mutex);
    for (const auto& range : g_ub_store_mem_ranges) {
        auto range_start = reinterpret_cast<uintptr_t>(range.base);
        uintptr_t range_end = range_start + range.size;
        if (addr_start < range_end && addr_end > range_start) {
            return true;
        }
    }
    return false;
}

}  // namespace mooncake