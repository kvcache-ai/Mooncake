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

ShmHelper* ShmHelper::getInstance() {
    static ShmHelper instance;
    return &instance;
}

ShmHelper::ShmHelper() {
    const char* hp = std::getenv("MC_STORE_USE_HUGEPAGE");
    use_hugepage_ = (hp != nullptr);
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
        // get_hugepage_size_from_env reads MC_STORE_HUGEPAGE_SIZE (2MB or 1GB)
        // and sets MFD_HUGETLB | MFD_HUGE_2MB/1GB in flags.
        // mlx5 ibv_reg_mr (without IBV_ACCESS_HUGETLB) requires the MR size
        // to be a multiple of 16 * hugepage_size (ConnectX-6/7 firmware MTT
        // block constraint: 2MB -> 32MB alignment, 1GB -> 16GB alignment).
        const size_t hugepage_size =
            get_hugepage_size_from_env(&flags, use_memfd);
        const size_t rdma_align = 16 * hugepage_size;
        size = align_up(size, rdma_align);
        LOG(INFO) << "Using huge pages for shared memory, hugepage_size="
                  << hugepage_size << " rdma_align=" << rdma_align
                  << " size=" << size;
    }

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

    void* base_addr = mmap(nullptr, size, PROT_READ | PROT_WRITE,
                           MAP_SHARED | MAP_POPULATE, fd, 0);
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
