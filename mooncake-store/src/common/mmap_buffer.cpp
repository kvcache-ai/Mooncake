#include "common/client_buffer_allocation.h"

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <sys/mman.h>

#include <glog/logging.h>

namespace mooncake {

void* allocate_buffer_mmap_memory(size_t total_size, size_t alignment) {
    if (total_size == 0) {
        LOG(ERROR) << "Total size must be greater than 0 for hugepage mmap";
        return nullptr;
    }

    unsigned int flags = MAP_PRIVATE | MAP_ANONYMOUS | MAP_POPULATE;
    const size_t effective_alignment =
        std::max(alignment, get_hugepage_size_from_env(&flags));
    const size_t map_size = align_up(total_size, effective_alignment);

    void* ptr = mmap(nullptr, map_size, PROT_READ | PROT_WRITE, flags, -1, 0);
    if (ptr == MAP_FAILED) {
        LOG(ERROR) << "Hugepage mmap failed, size=" << map_size
                   << ", errno=" << errno << " (" << strerror(errno) << ")";
        return nullptr;
    }

    return ptr;
}

void free_buffer_mmap_memory(void* ptr, size_t total_size) {
    if (!ptr || total_size == 0) {
        return;
    }

    const size_t map_size = align_up(total_size, get_hugepage_size_from_env());
    if (munmap(ptr, map_size) != 0) {
        LOG(ERROR) << "munmap hugepage failed, size=" << map_size
                   << ", errno=" << errno << " (" << strerror(errno) << ")";
    }
}

}  // namespace mooncake
