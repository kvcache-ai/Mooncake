#include "shm_helper.h"

#include <cstdlib>
#include <cstring>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <fcntl.h>
#include <unistd.h>
#include <glog/logging.h>

#include "utils.h"
#include "config.h"
#ifdef USE_NOF
#include "spdk/spdk_wrapper.h"
#endif
#if defined(USE_ASCEND_DIRECT)
#include "ascend_allocator.h"
#endif

#ifndef MOONCAKE_SHM_NAME
#define MOONCAKE_SHM_NAME "mooncake_shm"
#endif

namespace mooncake {

std::mutex ShmHelper::shm_mutex_;

static int memfd_create_wrapper(const char* name, unsigned int flags) {
#ifdef __NR_memfd_create
    return syscall(__NR_memfd_create, name, flags);
#else
    return -1;
#endif
}

#ifdef USE_NOF
// Place a memfd mapping at a 2MB-aligned address while keeping the file offset
// at 0, so the returned base matches what a peer process mapping the fd at
// offset 0 sees (cross-process fd sharing is unchanged). SPDK before 26.09
// (e.g. the v23.01.1 pinned by dependencies.sh) rejects spdk_mem_register()
// unless both the start and the length are 2MB-aligned; plain mmap only
// guarantees 4KB alignment. Reserve a PROT_NONE region large enough to contain
// a 2MB-aligned window, then MAP_FIXED the memfd into it (which only replaces
// the reservation, never live mappings), and release the unaligned head/tail.
static void* mmap_shm_2mb_aligned(size_t size, int fd) {
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
#endif

ShmHelper* ShmHelper::getInstance() {
    static ShmHelper instance;
    return &instance;
}

ShmHelper::ShmHelper() {
    const char* hp = std::getenv("MC_STORE_USE_HUGEPAGE");
    use_hugepage_ = (hp != nullptr);
    // Read once at construction (ShmHelper is a singleton). Opt-in only: with
    // MC_STORE_REGISTER_SPDK=1, ShmHelper mappings are registered with SPDK so
    // NoF zero-copy transfers can DMA to/from them; otherwise the previous
    // allocation behavior is preserved exactly.
    const char* rs = std::getenv("MC_STORE_REGISTER_SPDK");
    register_spdk_ = (rs != nullptr && std::strcmp(rs, "1") == 0);
    if (register_spdk_) {
        LOG(INFO) << "MC_STORE_REGISTER_SPDK=1: shared memory will be "
                     "registered with SPDK for NoF zero-copy transfers";
    }
}

ShmHelper::~ShmHelper() { cleanup(); }

bool ShmHelper::cleanup() {
    std::lock_guard<std::mutex> lock(shm_mutex_);
    bool ret = true;
    for (auto& shm : shms_) {
        if (shm->fd != -1) {
            close(shm->fd);
            shm->fd = -1;
        }
        if (shm->base_addr) {
#ifdef USE_NOF
            if (shm->spdk_registered) {
                if (SpdkWrapper::GetInstance().UnregisterMemory(shm->base_addr,
                                                                shm->size) != 0) {
                    LOG(WARNING)
                        << "Failed to unregister shared memory from SPDK "
                           "during cleanup: "
                        << shm->base_addr;
                }
                shm->spdk_registered = false;
            }
#endif
#if defined(USE_ASCEND_DIRECT)
            if (globalConfig().ascend_agent_mode &&
                globalConfig().ascend_use_fabric_mem) {
                free_memory("ascend", shm->base_addr);
                continue;
            }
#endif
            if (munmap(shm->base_addr, shm->size) == -1) {
                LOG(ERROR) << "Failed to unmap shared memory: "
                           << strerror(errno);
                ret = false;
            }
            shm->base_addr = nullptr;
        }
    }
    shms_.clear();
    return ret;
}

void* ShmHelper::allocate(size_t size) {
    std::lock_guard<std::mutex> lock(shm_mutex_);
    // Dummy-real: FabricMem host uses VMM; non-Fabric host uses memfd+mmap like
    // non-agent / GPU shm path.
#ifdef USE_ASCEND_DIRECT
    if (globalConfig().ascend_agent_mode) {
        if (globalConfig().ascend_use_fabric_mem) {
            void* base_addr = nullptr;
            size_t alloc_size = size;
            base_addr = ascend_allocate_vmm_memory_direct(alloc_size);
            if (base_addr == nullptr) {
                throw std::runtime_error(
                    "Failed to allocate VMM shared memory");
            }
            auto shm = std::make_shared<ShmSegment>();
            shm->fd = -1;
            shm->base_addr = base_addr;
            shm->size = alloc_size;
            shm->name = MOONCAKE_SHM_NAME;
            shm->registered = false;
            shms_.push_back(shm);
            return base_addr;
        }
        // ascend_agent_mode && !ascend_use_fabric_mem: fall through to memfd
    }
#endif

    unsigned int flags = MFD_CLOEXEC;
    if (use_hugepage_) {
        bool use_memfd = true;
        size = align_up(size, get_hugepage_size_from_env(&flags, use_memfd));
        LOG(INFO) << "Using huge pages for shared memory, size: " << size;
    }
#ifdef USE_NOF
    // spdk_mem_register() requires an aligned start and an aligned length.
    // SPDK before 26.09 (e.g. the v23.01.1 pinned by dependencies.sh) only
    // accepts 2MB-aligned registrations (MASK_2MB); 4KB support arrived in
    // 26.09 (commit d6dc356ff). 2MB alignment satisfies both. When hugepages
    // are configured the size is already a hugepage multiple (and MAP_HUGETLB
    // guarantees a 2MB-aligned base); otherwise align to 2MB so the tail is
    // compatible with older SPDK. mmap() already maps whole pages, so this only
    // pads the tail and does not change the bytes visible to callers. Only
    // applied when SPDK registration is enabled (MC_STORE_REGISTER_SPDK=1) so
    // the default flow keeps the exact previous allocation sizes.
    if (register_spdk_) {
        const size_t hp_size = get_hugepage_size_from_env();
        const size_t align = hp_size > 0 ? hp_size : static_cast<size_t>(SZ_2MB);
        size = align_up(size, align);
    }
#endif

    int fd = memfd_create_wrapper(MOONCAKE_SHM_NAME, flags);
    if (fd == -1) {
        std::string extra_msg =
            use_hugepage_ ? " (Check /proc/sys/vm/nr_hugepages?)" : "";
        throw std::runtime_error("Failed to create anonymous shared memory" +
                                 extra_msg + ": " +
                                 std::string(strerror(errno)));
    }

    if (ftruncate(fd, size) == -1) {
        close(fd);
        throw std::runtime_error("Failed to set shared memory size: " +
                                 std::string(strerror(errno)));
    }

    void* base_addr = nullptr;
#ifdef USE_NOF
    // SPDK < 26.09 (v23.01.1) needs a 2MB-aligned base; the hugepage path
    // already provides one, so only the non-hugepage path needs the aligned
    // mapping (keeps MC_STORE_USE_HUGEPAGE optional).
    if (register_spdk_ && !use_hugepage_) {
        base_addr = mmap_shm_2mb_aligned(size, fd);
    } else
#endif
    {
        base_addr = mmap(nullptr, size, PROT_READ | PROT_WRITE,
                         MAP_SHARED | MAP_POPULATE, fd, 0);
    }
    if (base_addr == MAP_FAILED) {
        close(fd);
        throw std::runtime_error("Failed to map shared memory: " +
                                 std::string(strerror(errno)));
    }

    auto shm = std::make_shared<ShmSegment>();
    shm->fd = fd;
    shm->base_addr = base_addr;
    shm->size = size;
    shm->name = MOONCAKE_SHM_NAME;
    shm->registered = false;
#ifdef USE_NOF
    // Register the mapping with SPDK so NoF (NVMe-oF) RDMA transfers can DMA
    // to/from this buffer directly (spdk_rdma_get_translation). Registration
    // failure is non-fatal: the buffer stays usable for all non-NoF paths.
    // Enabled only with MC_STORE_REGISTER_SPDK=1 (read once at construction).
    if (register_spdk_) {
        if (SpdkWrapper::GetInstance().RegisterMemory(base_addr, size) != 0) {
            LOG(WARNING) << "Failed to register shared memory with SPDK: addr="
                         << base_addr << ", size=" << size
                         << "; NoF zero-copy transfers to this buffer will be "
                            "unavailable";
        } else {
            shm->spdk_registered = true;
        }
    }
#endif
    shms_.push_back(shm);

    return base_addr;
}

int ShmHelper::free(void* addr) {
    std::lock_guard<std::mutex> lock(shm_mutex_);
    for (auto it = shms_.begin(); it != shms_.end(); ++it) {
        if ((*it)->base_addr == addr) {
            if ((*it)->fd != -1) {
                close((*it)->fd);
            }
            if ((*it)->base_addr) {
#ifdef USE_NOF
                if ((*it)->spdk_registered) {
                    if (SpdkWrapper::GetInstance().UnregisterMemory(
                            (*it)->base_addr, (*it)->size) != 0) {
                        LOG(WARNING)
                            << "Failed to unregister shared memory from SPDK "
                               "during free: "
                            << (*it)->base_addr;
                    }
                    (*it)->spdk_registered = false;
                }
#endif
#if defined(USE_ASCEND_DIRECT)
                if (globalConfig().ascend_agent_mode &&
                    globalConfig().ascend_use_fabric_mem) {
                    free_memory("ascend", (*it)->base_addr);
                } else
#endif
                    if (munmap((*it)->base_addr, (*it)->size) == -1) {
                    LOG(ERROR) << "Failed to unmap shared memory during free: "
                               << strerror(errno);
                    return -1;
                }
            }
            LOG(INFO) << "Freed shared memory at " << addr
                      << ", size: " << (*it)->size;
            shms_.erase(it);
            return 0;
        }
    }
    LOG(ERROR) << "Attempted to free unknown shared memory address: " << addr;
    return -1;
}

std::shared_ptr<ShmHelper::ShmSegment> ShmHelper::get_shm(void* addr) {
    std::lock_guard<std::mutex> lock(shm_mutex_);
    for (auto& shm : shms_) {
        if (addr >= shm->base_addr &&
            reinterpret_cast<uint8_t*>(addr) <
                reinterpret_cast<uint8_t*>(shm->base_addr) + shm->size) {
            return shm;
        }
    }
    return nullptr;
}

}  // namespace mooncake
