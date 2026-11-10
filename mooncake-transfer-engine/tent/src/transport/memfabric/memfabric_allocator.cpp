#include "memfabric_allocator.h"
#include "tent/transport/memfabric/memfabric_under_api.h"

#include <cstdlib>

#include <glog/logging.h>

namespace mooncake {
std::string memfabric_get_protocol(bool store) {
    const char* op = store ? std::getenv("MF_STORE_PROTOCOL") : nullptr;
    if (!op || !*op) op = std::getenv("MF_PROTOCOL");
    return op ? op : "";
}

bool memfabric_use_fabric_mem(const std::string& protocol) {
    return protocol == "ascend" &&
           memfabric_get_protocol(true) == "device_sdma";
}

void* memfabric_allocate_memory(size_t total_size,
                                const std::string& protocol) {
    void* buffer = nullptr;
    if (memfabric_use_fabric_mem(protocol)) {
        int32_t device = -1;
        auto ret = aclrtGetDevice(&device);
        if (ret != ACL_ERROR_NONE || device < 0 || device > UINT16_MAX) {
            LOG(ERROR) << "aclrtGetDevice failed for shared Host allocation, "
                       << "ret=" << ret << " device=" << device;
            return nullptr;
        }
        ret = tent::MemFabricUnderApi::SmemMemAlloc(
            static_cast<uint16_t>(device), total_size, 0, &buffer);
        if (ret != 0 || !buffer) {
            LOG(ERROR) << "smem_mem_alloc failed, ret=" << ret
                       << " size_bytes=" << total_size;
            return nullptr;
        }
        return buffer;
    }
    auto ret = aclrtMallocHost(&buffer, total_size);
    if (ret != ACL_ERROR_NONE) {
        LOG(ERROR) << "aclrtMallocHost failed: " << ret;
        return nullptr;
    }
    return buffer;
}

void* memfabric_allocate_memory_best_effort(size_t target_size,
                                            const std::string& protocol,
                                            size_t* actual_size,
                                            MemFabricHostAllocFn host_alloc,
                                            size_t host_alignment,
                                            bool defer_hugetlb_population) {
    if (actual_size == nullptr) {
        LOG(ERROR)
            << "memfabric_allocate_memory_best_effort: actual_size is null";
        return nullptr;
    }
    *actual_size = 0;
    // Native Host memory owns its page policy, independent of mmap hints.
    void* ptr =
        host_alloc && !memfabric_use_fabric_mem(protocol)
            ? host_alloc(target_size, host_alignment, defer_hugetlb_population)
            : memfabric_allocate_memory(target_size, protocol);
    if (ptr) *actual_size = target_size;
    return ptr;
}

void memfabric_free_memory(const std::string& protocol, void* ptr) {
    bool managed = false;
    const int ret = tent::MemFabricUnderApi::SmemMemFree(ptr, managed);
    if (managed) {
        if (ret != 0) {
            LOG(ERROR) << "smem_mem_free failed, ret=" << ret;
        }
        return;
    }
    if (protocol == "ascend") aclrtFreeHost(ptr);
}

}  // namespace mooncake
