#include <algorithm>
#include <cstddef>
#include <mutex>
#include <unordered_map>
#include <vector>
#include <glog/logging.h>
#include <numa.h>

#include "ub_allocator.h"

namespace mooncake {
struct UbStoreMemRange {
    void* base;
    size_t size;
};
std::mutex g_ub_store_mem_mutex;
std::vector<UbStoreMemRange> g_ub_store_mem_ranges;

size_t remove_store_memory_range(void* ptr) {
    std::lock_guard<std::mutex> store_lock(g_ub_store_mem_mutex);

    auto it = std::find_if(
        g_ub_store_mem_ranges.begin(), g_ub_store_mem_ranges.end(),
        [ptr](const UbStoreMemRange& range) { return range.base == ptr; });

    if (it == g_ub_store_mem_ranges.end()) {
        LOG(ERROR) << "failed for UB protocol, addr at " << ptr;
        return 0;
    }

    size_t sz = it->size;             // 先保存 size
    g_ub_store_mem_ranges.erase(it);  // 再删除
    return sz;
}

void* ub_allocate_memory(size_t alignment, size_t total_size) {
    void* ptr = numa_alloc_local(total_size);
    if (!ptr) {
        LOG(ERROR) << "failed for UB protocol, size=" << total_size
                   << ", alignment : " << alignment;
        return nullptr;
    }
    LOG(INFO) << "UB:  allocated total size : " << total_size
              << ", alignment : " << alignment << " addr at " << ptr;

    std::lock_guard<std::mutex> store_lock(g_ub_store_mem_mutex);
    g_ub_store_mem_ranges.push_back({ptr, total_size});

    return ptr;
}

void ub_free_memory(void* ptr) {
    if (!ptr) {
        return;
    }
    auto size = remove_store_memory_range(ptr);
    numa_free(ptr, size);
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
        if (addr_start >= range_start && addr_end <= range_end) {
            return true;
        }
    }
    return false;
}

}  // namespace mooncake