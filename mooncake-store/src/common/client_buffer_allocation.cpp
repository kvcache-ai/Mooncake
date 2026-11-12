#include "common/client_buffer_allocation.h"

#include <cstdint>
#include <cstdlib>
#include <glog/logging.h>

#include "utils.h"

#ifdef USE_ASCEND_DIRECT
#include "acl/acl.h"
#include "config.h"
#include "common.h"
#endif

namespace mooncake {

#if defined(USE_ASCEND_DIRECT) && defined(ASCEND_SUPPORT_FABRIC_MEM)
int allocate_physical_memory(size_t total_size, aclrtDrvMemHandle& handle) {
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

size_t get_hugepage_size_from_env(unsigned int* out_flags, bool use_memfd) {
    const char* use_hp_env = std::getenv("MC_STORE_USE_HUGEPAGE");
    if (use_hp_env == nullptr) {
        return 0;
    }

    constexpr size_t SZ_2MB = 2 * 1024 * 1024;
    constexpr size_t SZ_1GB = 1024 * 1024 * 1024;

    size_t size = SZ_2MB;  // Default to 2MB

    const char* size_env = std::getenv("MC_STORE_HUGEPAGE_SIZE");
    if (size_env != nullptr) {
        size_t parsed_size = string_to_byte_size(size_env);

        if (parsed_size == SZ_2MB || parsed_size == SZ_1GB) {
            size = parsed_size;
        } else {
            LOG(WARNING) << "Invalid MC_STORE_HUGEPAGE_SIZE='" << size_env
                         << "'. Supported: 2MB, 1GB. Fallback to 2MB.";
            size = SZ_2MB;
        }
    }

    if (out_flags != nullptr) {
        if (use_memfd) {
            *out_flags |= MFD_HUGETLB;
        } else {
            *out_flags |= MAP_HUGETLB;
        }

        // Add size specific flag
        if (size == SZ_2MB) {
            if (use_memfd) {
                *out_flags |= MFD_HUGE_2MB;
            } else {
                *out_flags |= MAP_HUGE_2MB;
            }
        } else if (size == SZ_1GB) {
            if (use_memfd) {
                *out_flags |= MFD_HUGE_1GB;
            } else {
                *out_flags |= MAP_HUGE_1GB;
            }
        }
        LOG(INFO) << "Using hugepage size: "
                  << (size == SZ_2MB ? "2MB" : "1GB");
    }

    return size;
}

void* allocate_buffer_allocator_memory(size_t total_size,
                                       const std::string& protocol,
                                       size_t alignment) {
    const size_t default_alignment = facebook::cachelib::Slab::kSize;
    // Ensure total_size is a multiple of alignment
    if (alignment == default_alignment && total_size < alignment) {
        LOG(ERROR) << "Total size must be at least " << alignment;
        return nullptr;
    }
#ifdef USE_ASCEND_DIRECT
    if (protocol == "ascend" && total_size > 0) {
#ifdef ASCEND_SUPPORT_FABRIC_MEM
        if (globalConfig().ascend_use_fabric_mem) {
            aclrtDrvMemHandle handle = nullptr;
            if (allocate_physical_memory(total_size, handle) != 0) {
                return nullptr;
            }
            void* va;
            auto ret = aclrtReserveMemAddress(&va, total_size, 0, nullptr, 1);
            if (ret != ACL_ERROR_NONE) {
                LOG(ERROR) << "Failed to reserve memory: " << ret;
                return nullptr;
            }
            ret = aclrtMapMem(va, total_size, 0, handle, 0);
            if (ret != ACL_ERROR_NONE) {
                LOG(ERROR) << "Failed to map memory: " << ret;
                return nullptr;
            }
            return va;
        }
#endif
        void* buffer = nullptr;
        auto ret = aclrtMallocHost(&buffer, total_size);
        if (ret != ACL_ERROR_NONE) {
            LOG(ERROR) << "Failed to allocate memory: " << ret;
            return nullptr;
        }
        return buffer;
    }
#endif
    // Allocate aligned memory
    return aligned_alloc(alignment, total_size);
}

void free_memory(const std::string& protocol, void* ptr) {
#ifdef USE_ASCEND_DIRECT
    if (protocol == "ascend") {
#ifdef ASCEND_SUPPORT_FABRIC_MEM
        if (globalConfig().ascend_use_fabric_mem) {
            aclrtDrvMemHandle handle;
            auto ret = aclrtMemRetainAllocationHandle(ptr, &handle);
            if (ret != ACL_ERROR_NONE) {
                LOG(ERROR) << "Failed to retain allocation handle: " << ptr;
                return;
            }
            ret = aclrtUnmapMem(ptr);
            if (ret != ACL_ERROR_NONE) {
                LOG(ERROR) << "Failed to unmap memory: " << ptr;
            }
            aclrtReleaseMemAddress(ptr);
            if (ret != ACL_ERROR_NONE) {
                LOG(ERROR) << "Failed to release mem address: " << ptr;
            }
            ret = aclrtFreePhysical(handle);
            if (ret != ACL_ERROR_NONE) {
                LOG(ERROR) << "Failed to free physical handle: " << handle;
            }
            return;
        }
#endif
        aclrtFreeHost(ptr);
        return;
    }
#endif
    free(ptr);
}

}  // namespace mooncake
