#pragma once

namespace mooncake {

void* ub_allocate_memory(size_t alignment, size_t total_size);

void ub_free_memory(void* ptr);

bool ub_is_store_memory(void* addr, size_t length);

}  // namespace mooncake