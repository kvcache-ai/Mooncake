#pragma once

#include <string>

#if defined(USE_ASCEND_DIRECT) || defined(USE_UBSHMEM)
#include "acl/acl_rt.h"
#endif

namespace mooncake {
void* ascend_allocate_memory(size_t total_size, const std::string& protocol);

// Matches store allocate_buffer_mmap_memory(size, alignment, defer_populate).
using AscendHostAllocFn = void* (*)(size_t, size_t, bool);

// Allocate on the current agent-mode device (round-robin).
// Fabric-mem: try 100%/90%/.../50% of target (1G-aligned); host_alloc is
// ignored. Otherwise: exact-size via host_alloc, or aclrtMallocHost if
// host_alloc is null. Sets *actual_size on success.
void* ascend_allocate_memory_best_effort(
    size_t target_size, const std::string& protocol, size_t* actual_size,
    AscendHostAllocFn host_alloc = nullptr, size_t host_alignment = 0,
    bool defer_hugetlb_population = false);

void ascend_free_memory(const std::string& protocol, void* ptr);

// Check if [addr, addr+length) overlaps with any store memory range.
bool ascend_is_store_memory(void* addr, size_t length);

// Direct ACL VMM allocation, always bypasses adxl MallocMem.
// For use by shm_helper when ascend_agent_mode && ascend_use_fabric_mem.
// Requires size to be a multiple of 1GB. Returns nullptr on failure.
void* ascend_allocate_vmm_memory_direct(size_t size);

// Look up physical handle from virtual address. Returns nullptr if not found
// or if the allocation was not done via direct ACL path.
#if defined(USE_ASCEND_DIRECT) || defined(USE_UBSHMEM)
aclrtDrvMemHandle ascend_get_physical_handle_from_va(void* va);
#endif
}  // namespace mooncake
