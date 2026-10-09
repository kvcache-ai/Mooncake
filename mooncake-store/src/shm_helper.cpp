#include "shm_helper.h"

#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <fcntl.h>
#include <unistd.h>
#include <glog/logging.h>

#include "common/client_buffer_allocation.h"
#include "config.h"
#include "config/hugepage_config.h"
#include "config/shm_spdk_registration_config.h"
#include "nof/nof_runtime.h"  // SPDK-free; compiled unconditionally
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

// Build a clear error message for a failed shared-memory allocation step.
// When the allocation uses hugepages (forced by MC_STORE_REGISTER_SPDK=1), the
// usual failure is an insufficient hugepage pool (ENOMEM at ftruncate/mmap);
// spell out exactly how many hugepages of what size are needed so the operator
// can configure them. Non-hugepage messages keep the pre-existing text exactly.
static std::string shm_alloc_error(const char* step, size_t size,
                                   bool use_hugepage, size_t hp_size, int err) {
    if (!use_hugepage) {
        return std::string("Failed to ") + step + ": " + strerror(err);
    }
    std::string msg = std::string("Failed to ") + step + " using ";
    if (hp_size >= SZ_1GB) {
        msg += std::to_string(hp_size / SZ_1GB) + "GB hugepages";
    } else {
        msg += std::to_string(hp_size / (1024 * 1024)) + "MB hugepages";
    }
    msg += " (size " + std::to_string(size) + " bytes, need " +
           std::to_string(size / hp_size) + " hugepages): " + strerror(err);
    msg +=
        ". Configure enough hugepages via /proc/sys/vm/nr_hugepages (or "
        "hugepages-2048kB/hugepages-1048576kB nr_hugepages), or unset "
        "MC_STORE_REGISTER_SPDK to skip SPDK registration";
    return msg;
}

ShmHelper* ShmHelper::getInstance() {
    // Deliberately leaked, never destroyed: ~ShmHelper -> cleanup() would
    // unregister segments from the process-global NoF page registry, whose
    // static-destruction order relative to this singleton is unspecified.
    // Leaking guarantees the exit path can never touch a finalized registry
    // or SPDK env; the kernel reclaims the mappings and registrations at
    // process exit instead. cleanup() remains available to explicit callers.
    static ShmHelper* instance = new ShmHelper();
    return instance;
}

ShmHelper::ShmHelper() {
    use_hugepage_ = HugepageConfig::IsEnabledFromEnvironment();
    // Read once at construction (ShmHelper is a singleton). Opt-in only: with
    // MC_STORE_REGISTER_SPDK=1, ShmHelper mappings are registered with the NoF
    // initiator so NoF zero-copy transfers can DMA to/from them; otherwise the
    // previous allocation behavior is preserved exactly.
    register_spdk_ = is_register_spdk_enabled();
    if (register_spdk_) {
#ifdef USE_NOF
        nof_initiator_ = CreateNofRuntime().initiator;
        if (!nof_initiator_) {
            LOG(WARNING) << "MC_STORE_REGISTER_SPDK=1 but NoF is unavailable "
                            "(e.g. MC_NOF_BACKEND=none); shared memory will "
                            "NOT be registered for NoF zero-copy transfers";
        }
        // SPDK's spdk_mem_register() requires hugepage-backed memory: its
        // vtophys notify checks each 2MB segment's PHYSICAL address for 2MB
        // alignment, which only hugepage pages satisfy (4KB-backed memory
        // always fails with -EINVAL, on v23.01.1 and every later version in
        // iova=pa mode). Virtual-address alignment alone is not sufficient, so
        // force hugepages here; if not enough are configured the allocation
        // below fails with a clear error instead of silently losing NoF
        // zero-copy. This is an override on top of
        // HugepageConfig::IsEnabledFromEnvironment() above.
        use_hugepage_ = true;
#endif
        LOG(INFO) << "MC_STORE_REGISTER_SPDK=1: shared memory will be "
                     "registered with the NoF initiator for zero-copy "
                     "transfers";
    }
}

bool ShmHelper::is_register_spdk_enabled() {
    return ShmSpdkRegistrationConfig::FromEnvironment().enabled;
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
                if (!nof_initiator_ || nof_initiator_->UnregisterMemory(
                                           shm->base_addr) != ErrorCode::OK) {
                    // The NoF translation table still holds this range: do NOT
                    // munmap or clear spdk_registered, or a later mmap could
                    // reuse the VA and NoF would silently DMA to the wrong
                    // memory. Retain the mapping (released by the OS at process
                    // exit; cleanup() only runs from explicit callers).
                    LOG(ERROR) << "Failed to unregister shared memory from the "
                                  "NoF initiator during cleanup; retaining "
                                  "mapping (never munmap): "
                               << shm->base_addr;
                    ret = false;
                    continue;
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
            shm->requested_size = alloc_size;
            shm->name = MOONCAKE_SHM_NAME;
            shm->registered = false;
            shms_.push_back(shm);
            return base_addr;
        }
        // ascend_agent_mode && !ascend_use_fabric_mem: fall through to memfd
    }
#endif

    // Remember the caller-requested size before any alignment padding (hugepage
    // or 2MB for SPDK registration). shm->size is the padded size; consumers
    // that must match the original request (e.g. DummyClient::register_buffer)
    // use requested_size instead of re-deriving the alignment policy.
    const size_t requested = size;

    unsigned int flags = MFD_CLOEXEC;
    size_t hp_size = 0;  // hugepage size when use_hugepage_ (0 otherwise)
    if (use_hugepage_) {
        bool use_memfd = true;
        hp_size = get_hugepage_size_from_env(&flags, use_memfd);
        if (!(flags & MFD_HUGETLB)) {
            // get_hugepage_size_from_env() returns 0 and sets no MFD_HUGETLB
            // flag when MC_STORE_USE_HUGEPAGE is unset -- exactly the case when
            // MC_STORE_REGISTER_SPDK=1 forces hugepages (see constructor).
            // Default to 2MB hugepages.
            flags |= MFD_HUGETLB | MFD_HUGE_2MB;
            if (hp_size == 0) {
                hp_size = SZ_2MB;
            }
            LOG(INFO) << "Using 2MB hugepages (set MC_STORE_USE_HUGEPAGE and "
                         "MC_STORE_HUGEPAGE_SIZE=1GB to use 1GB)";
        }
        size = align_up(size, hp_size);
        LOG(INFO) << "Using huge pages for shared memory, size: " << size;
    }
    // When MC_STORE_REGISTER_SPDK=1 forces hugepages, the branch above already
    // aligned size to the hugepage size, and hugetlb mmap returns a
    // hugepage-aligned base, so no separate 2MB size padding or aligned base
    // mapping (mmap_shm_2mb_aligned, common/mmap_aligned.h) is needed on the
    // sender side.
    // (The receiver maps the shared fd and aligns its own base in
    // RealClient::map_shm_internal_with_device.)

    int fd = memfd_create_wrapper(MOONCAKE_SHM_NAME, flags);
    if (fd == -1) {
        throw std::runtime_error(
            shm_alloc_error("create anonymous shared memory", size,
                            use_hugepage_, hp_size, errno));
    }

    if (ftruncate(fd, size) == -1) {
        int err = errno;
        close(fd);
        throw std::runtime_error(shm_alloc_error("set shared memory size", size,
                                                 use_hugepage_, hp_size, err));
    }

    void* base_addr = mmap(nullptr, size, PROT_READ | PROT_WRITE,
                           MAP_SHARED | MAP_POPULATE, fd, 0);
    if (base_addr == MAP_FAILED) {
        int err = errno;
        close(fd);
        throw std::runtime_error(shm_alloc_error("map shared memory", size,
                                                 use_hugepage_, hp_size, err));
    }

    auto shm = std::make_shared<ShmSegment>();
    shm->fd = fd;
    shm->base_addr = base_addr;
    shm->size = size;
    shm->requested_size = requested;
    shm->name = MOONCAKE_SHM_NAME;
    shm->registered = false;
#ifdef USE_NOF
    // Register the mapping with the NoF initiator so NoF (NVMe-oF) RDMA
    // transfers can DMA to/from this buffer directly (RDMA translation table).
    // Registration failure is non-fatal: the buffer stays usable for all
    // non-NoF paths. Enabled only with MC_STORE_REGISTER_SPDK=1 (read once at
    // construction).
    if (register_spdk_ && nof_initiator_) {
        if (!NofRangeIsRegistrable(base_addr, size)) {
            // The hugepage forcing in the constructor makes this unreachable
            // today (hugetlb mmap returns a hugepage-aligned base and size is
            // aligned to the hugepage size). A range rejected here is never
            // touched by RegisterMemory, so the munmap in free()/cleanup()
            // stays safe without a pairing unregister.
            LOG(WARNING) << "Shared memory is not 2MB-aligned; not registering "
                            "for NoF zero-copy transfers: addr="
                         << base_addr << ", size=" << size;
        } else {
            // The process-global page registry normalizes to whole 2MB pages,
            // absorbs pages already registered by the DPDK memseg walk
            // (-EBUSY) as external, and rolls back the pages it touched on
            // failure — a failed call owns no registration, so teardown can
            // munmap normally.
            if (nof_initiator_->RegisterMemory(base_addr, size) ==
                ErrorCode::OK) {
                shm->spdk_registered = true;
            } else {
                LOG(WARNING) << "Failed to register shared memory with the NoF "
                                "initiator: addr="
                             << base_addr << ", size=" << size
                             << "; NoF zero-copy transfers to this buffer will "
                                "be unavailable";
            }
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
                (*it)->fd = -1;
            }
            if ((*it)->base_addr) {
#ifdef USE_NOF
                if ((*it)->spdk_registered) {
                    if (!nof_initiator_ ||
                        nof_initiator_->UnregisterMemory((*it)->base_addr) !=
                            ErrorCode::OK) {
                        // The NoF translation table still holds this range: do
                        // NOT munmap, clear spdk_registered, or erase the
                        // segment. Retain the mapping so the VA cannot be
                        // reused while the translation is live (a later free()
                        // retries).
                        LOG(ERROR) << "Failed to unregister shared memory from "
                                      "the NoF initiator during free; "
                                      "retaining mapping (never munmap): "
                                   << (*it)->base_addr;
                        return -1;
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
