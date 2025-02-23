#include <cstddef>
#include <cstdlib>
#include <string>

namespace mooncake {
/*
    @brief Allocates memory for the `BufferAllocator` class.
    @param total_size The total size of the memory to allocate.
    @return A pointer to the allocated memory.
*/
void* allocate_buffer_allocator_memory(size_t total_size);

void **rdma_args(const std::string &device_name);

}  // namespace mooncake