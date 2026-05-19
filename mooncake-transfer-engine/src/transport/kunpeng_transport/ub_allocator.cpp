#include <algorithm>
#include <mutex>
#include <unordered_map>
#include <vector>

#include <glog/logging.h>
#include <numa.h>
#include <numaif.h>
#include <sys/mman.h>

#include "ub_allocator.h"

namespace mooncake {
namespace {

struct StoreMemRange {
    void* base;
    size_t size;
};

std::mutex g_store_mem_mutex;
std::vector<StoreMemRange> g_store_mem_ranges;

void remove_store_memory_range(void* ptr) {
    std::lock_guard<std::mutex> store_lock(g_store_mem_mutex);
    auto it = std::remove_if(
        g_store_mem_ranges.begin(), g_store_mem_ranges.end(),
        [ptr](const StoreMemRange& range) { return range.base == ptr; });
    g_store_mem_ranges.erase(it, g_store_mem_ranges.end());
}

}  // namespace

void* ub_allocate_memory(size_t total_size, int numa_node) {
    int node = (numa_node >= 0) ? numa_node : 0;
    void* ptr = numa_alloc_onnode(total_size, node);
    if (!ptr) {
        LOG(ERROR) << "numa_alloc_onnode failed for UB protocol, size="
                   << total_size << ", numa_node=" << node;
        return nullptr;
    }
    LOG(INFO) << "UB: numa_alloc_onnode allocated " << total_size
              << " bytes at " << ptr << " on NUMA node " << node;
    
    std::lock_guard<std::mutex> store_lock(g_store_mem_mutex);
    g_store_mem_ranges.push_back({ptr, total_size});
    
    return ptr;
}

void ub_free_memory(void* ptr, size_t size) {
    if (!ptr) {
        return;
    }
    
    remove_store_memory_range(ptr);
    
    munmap(ptr, size);
    numa_free(ptr, size);
    
    LOG(INFO) << "UB: freed " << size << " bytes at " << ptr;
}

bool ub_is_store_memory(void* addr, size_t length) {
    if (!addr || length == 0) return false;
    auto addr_start = reinterpret_cast<uintptr_t>(addr);
    uintptr_t addr_end = addr_start + length;
    std::lock_guard<std::mutex> lock(g_store_mem_mutex);
    for (const auto& range : g_store_mem_ranges) {
        auto range_start = reinterpret_cast<uintptr_t>(range.base);
        uintptr_t range_end = range_start + range.size;
        if (addr_start < range_end && addr_end > range_start) {
            return true;
        }
    }
    return false;
}

}  // namespace mooncake