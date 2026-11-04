#include "utils.h"

#include <Slab.h>
#include <glog/logging.h>
#include <boost/algorithm/string.hpp>

#include <algorithm>
#include <cerrno>
#include <csignal>
#include <cstring>
#include <sys/mman.h>
#ifdef USE_ASCEND_DIRECT
#include "acl/acl.h"
#include "config.h"
#include "common.h"
#endif

namespace mooncake {

namespace {
const std::string kMetricClusterID = [] {
    const char* value = std::getenv("MC_STORE_CLUSTER_ID");
    return value ? std::string(value) : std::string();
}();
}  // namespace

std::map<std::string, std::string> merge_labels(
    const std::map<std::string, std::string>& labels) {
    std::map<std::string, std::string> merged_labels;
    if (!kMetricClusterID.empty()) {
        merged_labels["cluster_id"] = kMetricClusterID;
    }
    merged_labels.insert(labels.begin(), labels.end());
    return merged_labels;
}

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

void* allocate_buffer_mmap_memory(size_t total_size, size_t alignment) {
    if (total_size == 0) {
        LOG(ERROR) << "Total size must be greater than 0 for hugepage mmap";
        return nullptr;
    }

    unsigned int flags = MAP_PRIVATE | MAP_ANONYMOUS | MAP_POPULATE;
    const size_t effective_alignment =
        std::max(alignment, get_hugepage_size_from_env(&flags));
    const size_t map_size = align_up(total_size, effective_alignment);

    void* ptr = mmap(nullptr, map_size, PROT_READ | PROT_WRITE, flags, -1, 0);
    if (ptr == MAP_FAILED) {
        LOG(ERROR) << "Hugepage mmap failed, size=" << map_size
                   << ", errno=" << errno << " (" << strerror(errno) << ")";
        return nullptr;
    }

    return ptr;
}

void free_buffer_mmap_memory(void* ptr, size_t total_size) {
    if (!ptr || total_size == 0) {
        return;
    }

    const size_t map_size = align_up(total_size, get_hugepage_size_from_env());
    if (munmap(ptr, map_size) != 0) {
        LOG(ERROR) << "munmap hugepage failed, size=" << map_size
                   << ", errno=" << errno << " (" << strerror(errno) << ")";
    }
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

std::string formatDeviceNames(const std::string& device_names) {
    std::stringstream ss(device_names);
    std::string item;
    std::vector<std::string> tokens;
    while (getline(ss, item, ',')) {
        tokens.push_back(item);
    }

    std::string formatted;
    for (size_t i = 0; i < tokens.size(); ++i) {
        formatted += "\"" + tokens[i] + "\"";
        if (i < tokens.size() - 1) {
            formatted += ",";
        }
    }
    return formatted;
}

std::vector<std::string> splitString(const std::string& str, char delimiter,
                                     bool trim_spaces, bool keep_empty) {
    std::vector<std::string> result;

    boost::split(
        result, str, boost::is_any_of(std::string(1, delimiter)),
        keep_empty ? boost::token_compress_off : boost::token_compress_on);

    if (trim_spaces) {
        for (auto& token : result) {
            boost::trim(token);
        }
    }

    return result;
}

std::string GetEnvStringOr(const char* name, const std::string& default_value) {
    const char* env_val = std::getenv(name);
    return env_val ? std::string(env_val) : default_value;
}

}  // namespace mooncake
