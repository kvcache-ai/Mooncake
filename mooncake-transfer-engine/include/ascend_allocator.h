#pragma once

#include <cstdint>
#include <functional>
#include <string>
#include <vector>

#if defined(USE_ASCEND_DIRECT) || defined(USE_UBSHMEM)
#include "acl/acl_rt.h"
#endif

namespace mooncake {
// Config provider for the two process switches the allocator reads. The
// transfer engine registers callbacks at load time that read globalConfig()
// live, so a store that flips the switches after init is picked up
// immediately. A consumer that does not link the transfer engine (for example
// a TENT-only Python module) leaves the provider unset and either calls
// ascend_allocator_set_runtime_flags() or relies on the environment defaults.
// Priority: provider > runtime flags > environment default.
using AscendConfigBoolFn = bool (*)();

void ascend_allocator_set_config_provider(AscendConfigBoolFn agent_mode,
                                          AscendConfigBoolFn use_fabric_mem);

// Set the fallback switches used when no config provider is registered.
// Calling this again replaces both values.
void ascend_allocator_set_runtime_flags(bool agent_mode, bool use_fabric_mem);

void* ascend_allocate_memory(size_t total_size, const std::string& protocol);

// Matches store allocate_buffer_mmap_memory(size, alignment, defer_populate).
using AscendHostAllocFn = void* (*)(size_t, size_t, bool);

// Allocate on the current agent-mode device (round-robin).
// Fabric-mem: try 100%/90%/.../50% of target (1G-aligned); host_alloc is
// ignored. Otherwise: exact-size via host_alloc, or aclrtMallocHost if
// host_alloc is null. Sets *actual_size on success.
void* ascend_allocate_memory_best_effort(size_t target_size,
                                         const std::string& protocol,
                                         size_t* actual_size,
                                         AscendHostAllocFn host_alloc = nullptr,
                                         size_t host_alignment = 0,
                                         bool defer_hugetlb_population = false);

void ascend_free_memory(const std::string& protocol, void* ptr);

// Handle a peer process on this machine can import to map the same physical
// pages (ASCEND_USE_SHMEM). True only when `addr` is the base of a shared
// memfd allocation; false for private mappings, aclrtMallocHost buffers, VMM
// allocations, and for any address that is not an allocation base.
bool ascend_get_shared_host_handle(void* addr, std::string* handle);

// Check if [addr, addr+length) overlaps with any store memory range.
bool ascend_is_store_memory(void* addr, size_t length);

// Check whether addr starts inside a direct ACL VMM allocation (allocated via
// ascend_allocate_vmm_memory_direct). Returns false for adxl::MallocMem
// allocations and non-VMM host buffers.
bool ascend_is_direct_vmm_memory(void* addr, size_t length);

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
