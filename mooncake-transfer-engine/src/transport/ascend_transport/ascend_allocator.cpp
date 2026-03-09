#include <glog/logging.h>
#include "config.h"
#if defined(USE_ASCEND_DIRECT) || defined(USE_UBSHMEM)
#include "acl/acl.h"
#include "adxl/adxl_engine.h"
#endif

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
            LOG(ERROR) << "Failed to allocate memory: " << ret;
            return -1;
        }
    }
    return 0;
}
#endif
}  // namespace

void *ascend_allocate_memory(size_t total_size, const std::string &protocol) {
    if (globalConfig().ascend_use_fabric_mem) {
        void *va = nullptr;
#ifdef ASCEND_SUPPORT_FABRIC_MEM
        if (total_size % kFabricMemPageSize != 0) {
            LOG(ERROR) << "Local buffer size must be a multiple of 1GB";
            return nullptr;
        }
#ifdef ASCEND_SUPPORT_ADXL_MALLOC_MEM
        // Try to use adxl_engine's MallocMem
        auto status = adxl::AdxlEngine::MallocMem(adxl::MemType::MEM_HOST,
                                                  total_size, &va);
        if (status != adxl::SUCCESS) {
            LOG(ERROR) << "Failed to allocate fabric memory, errmsg: "
                       << aclGetRecentErrMsg();
            return nullptr;
        }
        std::lock_guard<std::mutex> lock(g_vmm_alloc_mutex);
        g_vmm_alloc_records.emplace(va, AllocRecord{nullptr, false});
        return va;
#else
        aclrtDrvMemHandle handle = nullptr;
        if (allocate_physical_memory(total_size, handle) != 0) {
            return nullptr;
        }
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
#endif

#else
        LOG(ERROR) << "Fabric mem mode is not supported, please upgrade Ascend "
                      "HDK and CANN.";
#endif
        return va;
    }
    if (protocol == "ubshmem") {
        LOG(ERROR) << "ubshmem protocol only support fabric mem, please "
                      "configure ASCEND_ENABLE_USE_FABRIC_MEM environment.";
        return nullptr;
    }

    void *buffer = nullptr;
#ifdef USE_ASCEND_DIRECT
    auto ret = aclrtMallocHost(&buffer, total_size);
    if (ret != ACL_ERROR_NONE) {
        LOG(ERROR) << "Failed to allocate memory: " << ret;
        return nullptr;
    }
#endif
    return buffer;
}

void ascend_free_memory(const std::string &protocol, void *ptr) {
    // make sure ascend_use_fabric_mem do not change between malloc and free
    if (globalConfig().ascend_use_fabric_mem) {
#ifdef ASCEND_SUPPORT_FABRIC_MEM
        std::lock_guard<std::mutex> lock(g_vmm_alloc_mutex);
        auto it = g_vmm_alloc_records.find(ptr);
        if (it == g_vmm_alloc_records.end()) {
            LOG(INFO) << "Can not find record for va:" << ptr;
            return;
        }
        g_vmm_alloc_records.erase(it);
        if (!it->second.is_direct_alloc) {
#ifdef ASCEND_SUPPORT_ADXL_MALLOC_MEM
            auto status = adxl::AdxlEngine::FreeMem(ptr);
            if (status != adxl::SUCCESS) {
                LOG(ERROR) << "Failed to free fabric memory, errmsg: "
                           << aclGetRecentErrMsg();
            }
            return;
#endif
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
        ret = aclrtFreePhysical(it->second.handle);
        if (ret != ACL_ERROR_NONE) {
            LOG(ERROR) << "Failed to free physical mem: " << ptr;
            return;
        }
#endif
        return;
    }

    if (protocol == "ascend") {
#ifdef USE_ASCEND_DIRECT
        aclrtFreeHost(ptr);
#endif
    }
}

}  // namespace mooncake
