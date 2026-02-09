#include "cuda_alike.h"
#include <sys/types.h>

#include <iostream>

enum class MemoryBackendType { use_aclmalloc, use_aclmallocphysical, unknown };

extern "C" {

MemoryBackendType mc_probe_ub_fabric_support(int device_id) {
    aclError res = aclrtSetDevice(device_id);
    if (res != ACL_ERROR_NONE) {
        std::cerr << "Set device failed: " << device_id << ", result" << res;
        return MemoryBackendType::unknown;
    }
    aclrtPhysicalMemProp prop = {};
    prop.handleType = ACL_MEM_HANDLE_TYPE_NONE;
    prop.allocationType = ACL_MEM_ALLOCATION_TYPE_PINNED;
    prop.location.type = ACL_MEM_LOCATION_TYPE_DEVICE;
    prop.location.id = device_id;
    prop.memAttr = ACL_HBM_MEM_HUGE;

    aclrtDrvMemHandle handle;
    // try to allocate 2M fabric mem
    size_t size = 2 * 1024 * 1024;

    res = aclrtMallocPhysical(&handle, size, &prop, 0);
    if (res == ACL_ERROR_NONE) {
        aclrtFreePhysical(handle);  // success → clean up
        return MemoryBackendType::use_aclmallocphysical;
    } else {
        return MemoryBackendType::use_aclmalloc;
    }
}

void *mc_ub_fabric_malloc(ssize_t size, int device) {
    size_t granularity = 0;
    aclrtPhysicalMemProp prop = {};
    aclrtDrvMemHandle handle;
    void *ptr = nullptr;

    prop.handleType = ACL_MEM_HANDLE_TYPE_NONE;
    prop.allocationType = ACL_MEM_ALLOCATION_TYPE_PINNED;
    prop.location.type = ACL_MEM_LOCATION_TYPE_DEVICE;
    prop.location.id = device;
    prop.memAttr = ACL_HBM_MEM_HUGE;
    prop.reserve = 0;

    aclError result = aclrtMallocPhysical(&handle, size, &prop, 0);
    if (result != ACL_ERROR_NONE) {
        std::cerr << "aclrtMallocPhysical failed: " << result;
        return nullptr;
    }
    uint64_t page_type = 1;
    // now granularity is reserved to 0
    result =
        aclrtReserveMemAddress(&ptr, size, granularity, nullptr, page_type);
    if (result != ACL_ERROR_NONE) {
        std::cerr << "aclrtReserveMemAddress failed: " << result;
        (void)aclrtFreePhysical(handle);
        return nullptr;
    }
    result = aclrtMapMem(ptr, size, 0, handle, 0);
    if (result != ACL_ERROR_NONE) {
        std::cerr << "aclrtMapMem failed: " << result;
        (void)aclrtReleaseMemAddress(ptr);
        (void)aclrtFreePhysical(handle);
        return nullptr;
    }

    return ptr;
}

void mc_ub_fabric_free(void *ptr, int device) {
    aclrtDrvMemHandle handle;
    if (!ptr) {
        return;
    }
    auto result = aclrtMemRetainAllocationHandle(ptr, &handle);
    if (result != ACL_ERROR_NONE) {
        std::cerr << "aclrtMemRetainAllocationHandle failed: " << result
                  << "\n";
        return;
    } else {
        (void)aclrtUnmapMem(ptr);
        (void)aclrtReleaseMemAddress(ptr);
    }
    (void)aclrtFreePhysical(handle);
}
}
