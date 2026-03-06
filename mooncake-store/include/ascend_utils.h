#pragma once

#include <string>

namespace mooncake {
void* ascend_allocate_memory(size_t total_size, const std::string& protocol);

void ascend_free_memory(const std::string& protocol, void* ptr);
}  // namespace mooncake
