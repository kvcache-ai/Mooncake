#include <algorithm>
#include <mutex>
#include <unordered_map>
#include <vector>

#include <glog/logging.h>
#include "config.h"
#include "acl/acl.h"
#include "transport/ascend_transport/ascend_direct_transport/adxl_compat.h"

namespace mooncake {
namespace {
constexpr size_t kFabricMemPageSize = 1024 * 1024 * 1024;  // 1G
#ifdef ASCEND_SUPPORT_FABRIC_MEM
struct AllocRecord {
    aclrtDrvMemHandle handle;
    bool is_direct_alloc;
};
std::mutex g_vmm_alloc_mutex;
std::unordered_map<void *, AllocRecord> g_vmm_alloc_records;

int allocate_physical_memory(size_t total_size, aclrtDrvMemHandle &handle) {
    int32_t user_dev_id;
    auto ret = aclrtGetDevice(&user_dev_id);
    if (ret != ACL_ERROR_NONE) {
        LOG(ERROR) << "Failed to get device: " << ret;
        return -1;
    }
    int32_t physical_dev_id;
    ret = aclrtGetPhyDevIdByLogicDevId(user_dev_id, &physical_dev_id);
    if (ret != ACL_ERROR_NONE) {
        LOG(ERROR) << "Failed to get physical dev id: " << ret;
        return -1;
    }
    aclrtPhysicalMemProp prop = {};
    prop.handleType = ACL_MEM_HANDLE_TYPE_NONE;
    prop.allocationType = ACL_MEM_ALLOCATION_TYPE_PINNED;
    prop.memAttr = ACL_MEM_P2P_HUGE1G;
    prop.location.type = ACL_MEM_LOCATION_TYPE_HOST_NUMA;
    // Only 0 2 4 6 is available for fabric mem, map 4 device to one numa.
    const int32_t kDevicesPerChip = 4;
    const int32_t kNumaNodeStep = 2;
    prop.location.id = (physical_dev_id / kDevicesPerChip) * kNumaNodeStep;
    prop.reserve = 0;
    LOG(INFO) << "Malloc host memory for numa:" << prop.location.id;
    ret = aclrtMallocPhysical(&handle, total_size, &prop, 0);
    if (ret != ACL_ERROR_NONE) {
        LOG(INFO) << "Malloc host memory for numa:" << prop.location.id
                  << " failed, try common allocate instead.";
        prop.location.type = ACL_MEM_LOCATION_TYPE_HOST;
        prop.location.id = 0;
        ret = aclrtMallocPhysical(&handle, total_size, &prop, 0);
        if (ret != ACL_ERROR_NONE) {
            LOG(INFO) << "Malloc failed, try smaller page instead.";
            prop.memAttr = ACL_MEM_P2P_HUGE;
            ret = aclrtMallocPhysical(&handle, total_size, &prop, 0);
            if (ret != ACL_ERROR_NONE) {
                LOG(ERROR) << "Failed to allocate memory: " << ret;
                return -1;
            }
        }
    }
    return 0;
}

// Direct ACL VMM allocation (always bypasses adxl MallocMem).
// Used by shm_helper when ascend_agent_mode && ascend_use_fabric_mem.
void *allocate_vmm_memory_direct_impl(size_t total_size) {
    if (total_size % kFabricMemPageSize != 0) {
        LOG(ERROR) << "VMM memory size must be a multiple of 1GB";
        return nullptr;
    }
    aclrtDrvMemHandle handle = nullptr;
    if (allocate_physical_memory(total_size, handle) != 0) {
        return nullptr;
    }
    void *va = nullptr;
    auto ret = aclrtReserveMemAddress(&va, total_size, 0, nullptr, 1);
    if (ret != ACL_ERROR_NONE) {
        LOG(ERROR) << "Failed to reserve memory: " << ret;
        (void)aclrtFreePhysical(handle);
        return nullptr;
    }
    ret = aclrtMapMem(va, total_size, 0, handle, 0);
    if (ret != ACL_ERROR_NONE) {
        LOG(ERROR) << "Failed to map memory: " << ret;
        (void)aclrtReleaseMemAddress(va);
        (void)aclrtFreePhysical(handle);
        return nullptr;
    }
    std::lock_guard<std::mutex> lock(g_vmm_alloc_mutex);
    g_vmm_alloc_records.emplace(va, AllocRecord{handle, true});
    return va;
}
#endif

struct StoreMemRange {
    void *base;
    size_t size;
};
std::mutex g_store_mem_mutex;
std::vector<StoreMemRange> g_store_mem_ranges;

void remove_store_memory_range(void *ptr) {
    std::lock_guard<std::mutex> store_lock(g_store_mem_mutex);
    auto it = std::remove_if(
        g_store_mem_ranges.begin(), g_store_mem_ranges.end(),
        [ptr](const StoreMemRange &range) { return range.base == ptr; });
    g_store_mem_ranges.erase(it, g_store_mem_ranges.end());
}
}  // namespace

void *ascend_allocate_vmm_memory_direct(size_t total_size) {
#ifdef ASCEND_SUPPORT_FABRIC_MEM
    return allocate_vmm_memory_direct_impl(total_size);
#else
    (void)total_size;
    return nullptr;
#endif
}

aclrtDrvMemHandle ascend_get_physical_handle_from_va(void *va) {
#ifdef ASCEND_SUPPORT_FABRIC_MEM
    std::lock_guard<std::mutex> lock(g_vmm_alloc_mutex);
    auto it = g_vmm_alloc_records.find(va);
    if (it == g_vmm_alloc_records.end() || !it->second.is_direct_alloc) {
        return nullptr;
    }
    return it->second.handle;
#else
    (void)va;
    return nullptr;
#endif
}

void *ascend_allocate_memory(size_t total_size, const std::string &protocol) {
    if (globalConfig().ascend_use_fabric_mem) {
        void *va = nullptr;
#ifdef ASCEND_SUPPORT_FABRIC_MEM
        if (total_size % kFabricMemPageSize != 0) {
            LOG(ERROR) << "Local buffer size must be a multiple of 1GB";
            return nullptr;
        }
        if (&adxl::AdxlEngine::MallocMem != nullptr) {
            // Try to use adxl_engine's MallocMem
            auto status = adxl::AdxlEngine::MallocMem(adxl::MemType::MEM_HOST,
                                                      total_size, &va);
            if (status != adxl::SUCCESS) {
                LOG(ERROR) << "Failed to allocate fabric memory, errmsg: "
                           << aclGetRecentErrMsg();
                return nullptr;
            }
            LOG(INFO) << "Call adxl MallocMem suc, va:" << va;
            std::lock_guard<std::mutex> lock(g_vmm_alloc_mutex);
            g_vmm_alloc_records.emplace(va, AllocRecord{nullptr, false});
            return va;
        }
        va = allocate_vmm_memory_direct_impl(total_size);
        if (va) {
            std::lock_guard<std::mutex> store_lock(g_store_mem_mutex);
            g_store_mem_ranges.push_back({va, total_size});
        }
        return va;
#else
        LOG(ERROR) << "Fabric mem mode is not supported, please upgrade Ascend "
                      "HDK and CANN.";
#endif
    }
    if (protocol == "ubshmem") {
        LOG(ERROR) << "ubshmem protocol only support fabric mem, please "
                      "configure ASCEND_ENABLE_USE_FABRIC_MEM environment.";
        return nullptr;
    }

    void *buffer = nullptr;
    auto ret = aclrtMallocHost(&buffer, total_size);
    if (ret != ACL_ERROR_NONE) {
        LOG(ERROR) << "Failed to allocate memory: " << ret;
        return nullptr;
    }
    LOG(INFO) << "aclrtMallocHost suc, addr: " << buffer;
    if (buffer) {
        std::lock_guard<std::mutex> store_lock(g_store_mem_mutex);
        g_store_mem_ranges.push_back({buffer, total_size});
    }
    return buffer;
}

bool ascend_is_store_memory(void *addr, size_t length) {
    if (!addr || length == 0) return false;
    auto addr_start = reinterpret_cast<uintptr_t>(addr);
    uintptr_t addr_end = addr_start + length;
    std::lock_guard<std::mutex> lock(g_store_mem_mutex);
    for (const auto &range : g_store_mem_ranges) {
        auto range_start = reinterpret_cast<uintptr_t>(range.base);
        uintptr_t range_end = range_start + range.size;
        if (addr_start < range_end && addr_end > range_start) {
            return true;
        }
    }
    return false;
}

void ascend_free_memory(const std::string &protocol, void *ptr) {
    // make sure ascend_use_fabric_mem do not change between malloc and free
    if (globalConfig().ascend_use_fabric_mem) {
#ifdef ASCEND_SUPPORT_FABRIC_MEM
        remove_store_memory_range(ptr);
        std::lock_guard<std::mutex> lock(g_vmm_alloc_mutex);
        auto it = g_vmm_alloc_records.find(ptr);
        if (it == g_vmm_alloc_records.end()) {
            LOG(INFO) << "Can not find record for va:" << ptr;
            return;
        }
        const AllocRecord alloc_record = it->second;
        g_vmm_alloc_records.erase(it);
        if (!alloc_record.is_direct_alloc) {
            if (&adxl::AdxlEngine::FreeMem != nullptr) {
                auto status = adxl::AdxlEngine::FreeMem(ptr);
                if (status != adxl::SUCCESS) {
                    LOG(ERROR) << "Failed to free fabric memory, errmsg: "
                               << aclGetRecentErrMsg();
                }
                return;
            }
        }
        auto ret = aclrtUnmapMem(ptr);
        if (ret != ACL_ERROR_NONE) {
            LOG(ERROR) << "Failed to unmap memory: " << ptr;
            return;
        }
        ret = aclrtReleaseMemAddress(ptr);
        if (ret != ACL_ERROR_NONE) {
            LOG(ERROR) << "Failed to release mem address: " << ptr;
            return;
        }
        ret = aclrtFreePhysical(alloc_record.handle);
        if (ret != ACL_ERROR_NONE) {
            LOG(ERROR) << "Failed to free physical mem: " << ptr;
            return;
        }
#endif
        return;
    }

    if (protocol == "ascend") {
        remove_store_memory_range(ptr);
        aclrtFreeHost(ptr);
    }
}

}  // namespace mooncake
