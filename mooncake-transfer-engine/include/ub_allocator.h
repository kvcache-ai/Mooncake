#pragma once

#include <string>

namespace mooncake {

void* ub_allocate_memory(size_t total_size, int numa_node = -1);

void ub_free_memory(void* ptr, size_t size);

bool ub_is_store_memory(void* addr, size_t length);

}  // namespace mooncake