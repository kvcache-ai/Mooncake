#include "shm_helper.h"

#include <cstdlib>
#include <cstring>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <fcntl.h>
#include <unistd.h>
#include <glog/logging.h>

#include <algorithm>
#include <thread>
#include <vector>

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

#ifndef MADV_POPULATE_WRITE
#define MADV_POPULATE_WRITE 23
#endif

namespace {

// Fault a freshly mapped segment in, using several threads.
//
// mmap(MAP_POPULATE) reaches the same end state, but it does the whole
// mapping on the calling thread. On a 2 TiB two-socket host that runs at
// about 3.1 GB/s, so the 65.6 GB of HiCache host pools an SGLang prefill
// server allocates costs 21.2 s of its startup (measured on H200, kernel
// 6.8, 4 KiB pages). MADV_POPULATE_WRITE is the range form of the same
// operation, so populating disjoint chunks concurrently produces an
// identical mapping and reaches about 7.6 GB/s.
//
// MC_STORE_POPULATE_THREADS=1 restores the single-threaded behaviour.
size_t populate_thread_count() {
    if (const char* env = std::getenv("MC_STORE_POPULATE_THREADS")) {
        long n = std::strtol(env, nullptr, 10);
        if (n > 0) return static_cast<size_t>(std::min(n, 64L));
    }
    unsigned hw = std::thread::hardware_concurrency();
    return std::min<size_t>(hw ? hw : 1, 8);
}

// Populate is best effort, exactly as MAP_POPULATE is: the kernel may decline
// and the mapping is still valid, with the declined pages faulted in on first
// touch. So a failure is logged and the segment is returned, never thrown.
void populate_parallel(void* base, size_t size) {
    const size_t threads = populate_thread_count();
    const long page = sysconf(_SC_PAGESIZE);
    if (threads <= 1 || page <= 0 || size < (1UL << 30)) {
        if (madvise(base, size, MADV_POPULATE_WRITE) != 0) {
            LOG(WARNING) << "MADV_POPULATE_WRITE failed for " << size
                         << " bytes: " << strerror(errno)
                         << "; pages will fault in on first touch";
        }
        return;
    }

    size_t chunk = (size / threads / static_cast<size_t>(page)) *
                   static_cast<size_t>(page);
    if (chunk == 0) chunk = size;
    const size_t n = (size + chunk - 1) / chunk;

    std::vector<std::thread> workers;
    std::vector<int> failures(n, 0);
    workers.reserve(n);
    size_t spawned = 0;
    for (size_t i = 0; i < n; ++i) {
        try {
            workers.emplace_back([&, i] {
                char* start = static_cast<char*>(base) + i * chunk;
                size_t len = (i == n - 1) ? size - i * chunk : chunk;
                if (madvise(start, len, MADV_POPULATE_WRITE) != 0) {
                    failures[i] = errno;
                }
            });
            ++spawned;
        } catch (const std::exception& e) {
            // std::thread's constructor throws on resource exhaustion (e.g. a
            // container's pids limit). A joinable std::thread's destructor
            // calls std::terminate, so an unjoined worker here would crash
            // the process instead of staying best effort; stop spawning and
            // let the retry loop below finish the rest on this thread.
            LOG(WARNING) << "failed to start populate worker " << i << "/" << n
                         << ": " << e.what()
                         << "; finishing remaining chunks on this thread";
            break;
        }
    }
    for (auto& w : workers) w.join();

    // Any chunk that never got a worker, or whose worker's madvise was
    // declined, is retried once on this thread, so the common transient case
    // still ends fully populated.
    for (size_t i = 0; i < n; ++i) {
        if (i < spawned && failures[i] == 0) continue;
        char* start = static_cast<char*>(base) + i * chunk;
        size_t len = (i == n - 1) ? size - i * chunk : chunk;
        if (madvise(start, len, MADV_POPULATE_WRITE) != 0) {
            LOG(WARNING) << "MADV_POPULATE_WRITE declined chunk " << i << "/"
                         << n << " (" << len << " bytes): " << strerror(errno)
                         << "; those pages will fault in on first touch";
        }
    }
}

}  // namespace

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

    // MAP_POPULATE is deliberately absent: populate_parallel below reaches the
    // same end state with several threads. See its comment for the numbers.
    void* base_addr =
        mmap(nullptr, size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (base_addr == MAP_FAILED) {
        close(fd);
        throw std::runtime_error("Failed to map shared memory: " +
                                 std::string(strerror(errno)));
    }
    populate_parallel(base_addr, size);

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
    const uintptr_t address = reinterpret_cast<uintptr_t>(addr);
    for (auto& shm : shms_) {
        const uintptr_t base = reinterpret_cast<uintptr_t>(shm->base_addr);
        if (address >= base && address - base < shm->size) {
            return shm;
        }
    }
    return nullptr;
}

}  // namespace mooncake
