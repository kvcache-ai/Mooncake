#include "memfabric_allocator.h"

#include <glog/logging.h>

namespace mooncake {

void* memfabric_allocate_memory(size_t total_size,
                                const std::string& protocol) {
    (void)protocol;
    void* buffer = nullptr;
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
    void* ptr = host_alloc ? host_alloc(target_size, host_alignment,
                                        defer_hugetlb_population)
                           : memfabric_allocate_memory(target_size, protocol);
    if (ptr) *actual_size = target_size;
    return ptr;
}

void memfabric_free_memory(const std::string& protocol, void* ptr) {
    if (protocol == "ascend") aclrtFreeHost(ptr);
}

}  // namespace mooncake
