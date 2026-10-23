#include "common/store_shm_alloc.h"

#include <unistd.h>

#include <glog/logging.h>

#include "common/client_buffer_allocation.h"
#include "environ.h"
#include "environment_variables.h"
#include "memory_location.h"

namespace mooncake {
namespace {

bool env_flag_from_store_var(const EnvironmentVariable<std::string>& variable) {
    return envFlagEnabled(variable.name);
}

}  // namespace

bool store_use_shm_segment_flag() {
    using Variables = StoreShmEnvironmentVariables;
    return env_flag_from_store_var(Variables::MC_STORE_USE_SHM_SEGMENT);
}

bool store_use_shm_segment() {
    return store_use_shm_segment_flag() || envFlagEnabled("MC_FORCE_SHM");
}

bool store_shm_allow_tmpfs_fallback() {
    using Variables = StoreShmEnvironmentVariables;
    return env_flag_from_store_var(Variables::MC_STORE_SHM_ALLOW_TMPFS);
}

bool is_store_host_dram_protocol(const std::string& protocol) {
    return protocol != "ascend" && protocol != "ubshmem" && protocol != "ub" &&
           protocol != "sunrise_link" && protocol != "cxl" &&
           protocol != "nvlink_intra";
}

SharedMemoryOptions make_store_shm_options() {
    SharedMemoryOptions opt;
    const size_t hp = get_hugepage_size_from_env();
    opt.use_hugepage = hp > 0;
    opt.hugepage_size = hp;
    opt.populate = false;
    if (opt.use_hugepage) {
        using Variables = StoreShmEnvironmentVariables;
        const auto path = Environ::Read(Variables::MC_HUGETLBFS_PATH);
        if (path.has_value() && !path->empty()) {
            opt.hugetlbfs_path = *path;
        }
    }
    return opt;
}

StoreHostAllocResult allocate_store_host_segment(
    TransferEngine& te, size_t request_size, const std::string& protocol,
    const std::vector<int>& nic_numa_nodes, bool /*defer_populate*/) {
    StoreHostAllocResult result;
    if (request_size == 0) {
        LOG(ERROR) << "allocate_store_host_segment: request_size is 0";
        return result;
    }

    std::vector<int> nodes = nic_numa_nodes;
    if (protocol != "rdma" || nodes.size() <= 1) {
        nodes.clear();
    }

    const size_t hp = get_hugepage_size_from_env();
    const bool should_hp = hp > 0;
    const size_t page_sz = should_hp ? hp : static_cast<size_t>(getpagesize());

    size_t mapped_size = request_size;
    if (!nodes.empty()) {
        mapped_size = align_up(request_size, page_sz * nodes.size());
    } else if (should_hp) {
        mapped_size = align_up(request_size, page_sz);
    }

    SharedMemoryOptions opt = make_store_shm_options();
    void* ptr = te.allocateSharedMemory(mapped_size, opt);
    if (!ptr && opt.use_hugepage && store_shm_allow_tmpfs_fallback()) {
        LOG(WARNING) << "hugetlbfs SHM allocate failed, falling back to POSIX "
                        "shm (MC_STORE_SHM_ALLOW_TMPFS)";
        SharedMemoryOptions posix;
        posix.populate = false;
        ptr = te.allocateSharedMemory(mapped_size, posix);
        opt = posix;
    }
    if (!ptr) {
        LOG(ERROR) << "allocateSharedMemory failed, size=" << mapped_size
                   << " hugepage=" << (opt.use_hugepage ? "yes" : "no");
        return result;
    }

    if (!nodes.empty()) {
        if (bind_buffer_numa_segments(ptr, mapped_size, nodes, page_sz) != 0) {
            LOG(ERROR) << "bind_buffer_numa_segments failed for SHM segment";
            te.freeSharedMemory(ptr);
            return result;
        }
    }

    result.ptr = ptr;
    result.mapped_size = mapped_size;
    result.used_shm = true;
    result.used_numa = !nodes.empty();
    result.numa_nodes = std::move(nodes);
    result.page_size = page_sz;
    result.location = result.used_numa
                          ? buildSegmentsLocation(page_sz, result.numa_nodes)
                          : kWildcardLocation;
    LOG(INFO) << "Allocated Store SHM segment: " << mapped_size
              << " bytes, hugepage=" << (opt.use_hugepage ? "yes" : "no")
              << ", numa=" << (result.used_numa ? "yes" : "no")
              << ", location=" << result.location;
    return result;
}

void free_store_host_segment(TransferEngine& te,
                             const StoreHostAllocResult& alloc) {
    if (alloc.ptr) {
        te.freeSharedMemory(alloc.ptr);
    }
}

}  // namespace mooncake
