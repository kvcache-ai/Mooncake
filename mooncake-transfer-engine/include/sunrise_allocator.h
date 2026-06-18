#pragma once

#include <cstddef>
#include <cstdlib>
#include <glog/logging.h>

#if defined(USE_SUNRISE)
#include <tang_runtime_api.h>
#include <algorithm>
#include <mutex>
#include <unordered_set>
#include <vector>
#endif

#if defined(USE_SUNRISE)
namespace mooncake {
namespace sunrise_alloc_detail {
inline std::unordered_set<void*>& tangHostAllocatedSet() {
    static std::unordered_set<void*> s;
    return s;
}

inline std::unordered_set<void*>& tangDeviceAllocatedSet() {
    static std::unordered_set<void*> s;
    return s;
}

inline std::mutex& tangAllocMutex() {
    static std::mutex m;
    return m;
}

struct MemRange {
    void* base;
    size_t size;
};

inline std::vector<MemRange>& storeMemRanges() {
    static std::vector<MemRange> v;
    return v;
}

inline std::mutex& storeMemMutex() {
    static std::mutex m;
    return m;
}

inline void addStoreMemRange(void* ptr, size_t size) {
    std::lock_guard<std::mutex> lock(storeMemMutex());
    storeMemRanges().push_back({ptr, size});
}

inline void removeStoreMemRange(void* ptr) {
    std::lock_guard<std::mutex> lock(storeMemMutex());
    auto& ranges = storeMemRanges();
    ranges.erase(
        std::remove_if(ranges.begin(), ranges.end(),
                       [ptr](const MemRange& r) { return r.base == ptr; }),
        ranges.end());
}

}  // namespace sunrise_alloc_detail
}  // namespace mooncake
#endif

inline void* sunrise_allocate_memory(size_t total_size, size_t alignment = 4096,
                                     bool use_device_mem = false) {
#if defined(USE_SUNRISE)
    if (use_device_mem) {
        int cur_dev = -1;
        tangGetDevice(&cur_dev);
        if (cur_dev < 0) {
            tangError_t sd_ret = tangSetDevice(0);
            if (sd_ret != tangSuccess) {
                LOG(ERROR) << "sunrise_allocate_memory: tangSetDevice(0) "
                           << "failed: " << sd_ret << " "
                           << tangGetErrorString(sd_ret);
                return nullptr;
            }
        }
        void* ptr = nullptr;
        tangError_t ret = tangMalloc(&ptr, total_size);
        if (ret == tangSuccess && ptr) {
            {
                std::lock_guard<std::mutex> guard(
                    mooncake::sunrise_alloc_detail::tangAllocMutex());
                mooncake::sunrise_alloc_detail::tangDeviceAllocatedSet().insert(
                    ptr);
            }
            mooncake::sunrise_alloc_detail::addStoreMemRange(ptr, total_size);
            return ptr;
        }
        LOG(ERROR) << "tangMalloc failed (" << ret
                   << ") for size=" << total_size;
        return nullptr;
    }

    void* ptr = nullptr;
    tangError_t ret = tangHostAlloc(&ptr, total_size, 0);
    if (ret == tangSuccess && ptr) {
        {
            std::lock_guard<std::mutex> guard(
                mooncake::sunrise_alloc_detail::tangAllocMutex());
            mooncake::sunrise_alloc_detail::tangHostAllocatedSet().insert(ptr);
        }
        mooncake::sunrise_alloc_detail::addStoreMemRange(ptr, total_size);
        return ptr;
    }
    LOG(WARNING) << "tangHostAlloc failed (" << ret
                 << "), falling back to posix_memalign for size=" << total_size
                 << " alignment=" << alignment;
#endif
    (void)use_device_mem;
    void* fallback = nullptr;
    if (posix_memalign(&fallback, alignment, total_size) != 0) return nullptr;
    return fallback;
}

inline void sunrise_free_memory(void* ptr) {
    if (!ptr) return;
#if defined(USE_SUNRISE)
    {
        std::lock_guard<std::mutex> guard(
            mooncake::sunrise_alloc_detail::tangAllocMutex());
        if (mooncake::sunrise_alloc_detail::tangDeviceAllocatedSet().count(
                ptr)) {
            mooncake::sunrise_alloc_detail::tangDeviceAllocatedSet().erase(ptr);
            mooncake::sunrise_alloc_detail::removeStoreMemRange(ptr);
            tangError_t ret = tangFree(ptr);
            if (ret != tangSuccess) {
                LOG(WARNING)
                    << "tangFree failed (" << ret << ") for tracked ptr=" << ptr
                    << " (leaking to avoid crash)";
            }
            return;
        }
        if (mooncake::sunrise_alloc_detail::tangHostAllocatedSet().count(ptr)) {
            mooncake::sunrise_alloc_detail::tangHostAllocatedSet().erase(ptr);
            mooncake::sunrise_alloc_detail::removeStoreMemRange(ptr);
            tangError_t ret = tangFreeHost(ptr);
            if (ret != tangSuccess) {
                LOG(WARNING) << "tangFreeHost failed (" << ret
                             << ") for tracked ptr=" << ptr
                             << " (leaking to avoid crash)";
            }
            return;
        }
    }
#endif
    free(ptr);
}

inline bool sunrise_is_device_memory_range(void* ptr) {
#if defined(USE_SUNRISE)
    if (!ptr) return false;
    auto addr = reinterpret_cast<uintptr_t>(ptr);
    void* matched_base = nullptr;
    {
        std::lock_guard<std::mutex> lock(
            mooncake::sunrise_alloc_detail::storeMemMutex());
        for (const auto& range :
             mooncake::sunrise_alloc_detail::storeMemRanges()) {
            auto base_addr = reinterpret_cast<uintptr_t>(range.base);
            if (addr >= base_addr && addr < base_addr + range.size) {
                matched_base = range.base;
                break;
            }
        }
    }
    if (!matched_base) return false;
    std::lock_guard<std::mutex> guard(
        mooncake::sunrise_alloc_detail::tangAllocMutex());
    return mooncake::sunrise_alloc_detail::tangDeviceAllocatedSet().count(
               matched_base) > 0;
#else
    (void)ptr;
    return false;
#endif
}

inline bool sunrise_is_host_allocated(void* ptr) {
#if defined(USE_SUNRISE)
    if (!ptr) return false;
    auto addr = reinterpret_cast<uintptr_t>(ptr);
    void* matched_base = nullptr;
    {
        std::lock_guard<std::mutex> lock(
            mooncake::sunrise_alloc_detail::storeMemMutex());
        for (const auto& range :
             mooncake::sunrise_alloc_detail::storeMemRanges()) {
            auto base_addr = reinterpret_cast<uintptr_t>(range.base);
            if (addr >= base_addr && addr < base_addr + range.size) {
                matched_base = range.base;
                break;
            }
        }
    }
    if (!matched_base) return false;
    std::lock_guard<std::mutex> guard(
        mooncake::sunrise_alloc_detail::tangAllocMutex());
    return mooncake::sunrise_alloc_detail::tangHostAllocatedSet().count(
               matched_base) > 0;
#else
    (void)ptr;
    return false;
#endif
}
