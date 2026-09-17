#pragma once

#include <cstddef>
#include <cstdint>
#include <sys/mman.h>

#include "client/common/client_buffer_allocation.h"

namespace mooncake {

/**
 * @brief Place a memfd mapping at a 2MB-aligned address while keeping the file
 * offset at 0, so the returned base matches what a peer process mapping the fd
 * at offset 0 sees (cross-process fd sharing is unchanged).
 *
 * SPDK before 26.09 (e.g. the v23.01.1 pinned by dependencies.sh) rejects
 * spdk_mem_register() unless both the start and the length are 2MB-aligned;
 * plain mmap only guarantees 4KB alignment. Reserve a PROT_NONE region large
 * enough to contain a 2MB-aligned window, then MAP_FIXED the memfd into it
 * (which only replaces the reservation, never live mappings), and release the
 * unaligned head/tail. `size` should be a multiple of 2MB (callers align it).
 *
 * NOTE: virtual 2MB alignment is NECESSARY but NOT sufficient on the pinned
 * SPDK: in iova=pa mode spdk_mem_register additionally checks each segment's
 * PHYSICAL address (vtophys pagemap path, `paddr & MASK_2MB`), so the memory
 * must also be hugepage-backed or registration fails with -EINVAL. HugeTLB
 * mappings are automatically 2MB/1GB-aligned, so this helper is only needed
 * where the source cannot be hugepage-backed; today it is used solely by the
 * receiver (RealClient::map_shm_internal_with_device) for the shared fd
 * (1GB-hugepage fds whose MAP_FIXED here fails fall back to a plain mmap). The
 * sender (ShmHelper::allocate) always maps hugepages under
 * MC_STORE_REGISTER_SPDK=1 and needs no aligned base. SPDK >= 26.09 (commit
 * d6dc356ff) relaxes the API check to 4KB, but iova=pa still needs hugepages,
 * so a plain mmap remains insufficient for SPDK registration regardless of
 * version.
 *
 * @param size Mapping length; must be a multiple of 2MB for a clean head/tail.
 * @param fd memfd to map at offset 0.
 * @return The 2MB-aligned base, or MAP_FAILED on error.
 */
inline void* mmap_shm_2mb_aligned(size_t size, int fd) {
    const size_t reserve_size = size + SZ_2MB;
    void* hint = mmap(nullptr, reserve_size, PROT_NONE,
                      MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    if (hint == MAP_FAILED) {
        return MAP_FAILED;
    }
    const uintptr_t hint_addr = reinterpret_cast<uintptr_t>(hint);
    const uintptr_t target = static_cast<uintptr_t>(
        align_up(static_cast<size_t>(hint_addr), SZ_2MB));
    void* base =
        mmap(reinterpret_cast<void*>(target), size, PROT_READ | PROT_WRITE,
             MAP_SHARED | MAP_FIXED | MAP_POPULATE, fd, 0);
    if (base == MAP_FAILED) {
        munmap(hint, reserve_size);
        return MAP_FAILED;
    }
    // Release the unaligned head and tail of the reservation.
    const uintptr_t base_addr = reinterpret_cast<uintptr_t>(base);
    if (base_addr > hint_addr) {
        munmap(hint, base_addr - hint_addr);
    }
    const uintptr_t reserve_end = hint_addr + reserve_size;
    const uintptr_t tail = base_addr + size;
    if (tail < reserve_end) {
        munmap(reinterpret_cast<void*>(tail), reserve_end - tail);
    }
    return base;
}

}  // namespace mooncake
