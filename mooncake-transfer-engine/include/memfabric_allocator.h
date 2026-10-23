#pragma once

#include <string>
#include <acl/acl_rt.h>

namespace mooncake {

using MemFabricHostAllocFn = void* (*)(size_t, size_t, bool);

void* memfabric_allocate_memory(size_t total_size, const std::string& protocol);

void* memfabric_allocate_memory_best_effort(
    size_t target_size, const std::string& protocol, size_t* actual_size,
    MemFabricHostAllocFn host_alloc = nullptr, size_t host_alignment = 0,
    bool defer_hugetlb_population = false);

void memfabric_free_memory(const std::string& protocol, void* ptr);

}  // namespace mooncake
