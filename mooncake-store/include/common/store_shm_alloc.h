#pragma once

#include <cstddef>
#include <string>
#include <vector>

#include "common.h"
#include "transfer_engine.h"

namespace mooncake {

// True when MC_STORE_USE_SHM_SEGMENT is set to anything other than
// 0/false/no/off. Does not include MC_FORCE_SHM; that only installs the
// transport unless this flag is also on.
bool store_use_shm_segment_flag();

// Store host DRAM segment should be a TE SHM object: explicit Store flag or
// MC_FORCE_SHM (TE already installs ShmTransport in that case).
bool store_use_shm_segment();

bool store_shm_allow_tmpfs_fallback();

// Protocols whose global_segment lives in host DRAM (not device/CXL memory).
bool is_store_host_dram_protocol(const std::string& protocol);

// use_hugepage follows MC_STORE_USE_HUGEPAGE / MC_STORE_HUGEPAGE_SIZE.
// populate is always false so Store can mbind then populate itself.
SharedMemoryOptions make_store_shm_options();

struct StoreHostAllocResult {
    void* ptr = nullptr;
    size_t mapped_size = 0;
    bool used_shm = false;
    bool used_numa = false;
    std::vector<int> numa_nodes;
    size_t page_size = 0;
    std::string location;
};

StoreHostAllocResult allocate_store_host_segment(
    TransferEngine& te, size_t request_size, const std::string& protocol,
    const std::vector<int>& nic_numa_nodes, bool defer_populate);

void free_store_host_segment(TransferEngine& te,
                             const StoreHostAllocResult& alloc);

}  // namespace mooncake
