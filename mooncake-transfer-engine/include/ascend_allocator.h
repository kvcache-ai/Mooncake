#pragma once

#include <string>

#if defined(USE_ASCEND_DIRECT) || defined(USE_UBSHMEM)
#include "acl/acl_rt.h"
#endif

namespace mooncake {
void* ascend_allocate_memory(size_t total_size, const std::string& protocol);

void ascend_free_memory(const std::string& protocol, void* ptr);

// Check if [addr, addr+length) overlaps with any store memory range.
// Used by registerLocalMemory to decide RoCE+store registration policy.
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
