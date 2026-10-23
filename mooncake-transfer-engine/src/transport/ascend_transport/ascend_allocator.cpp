#include <algorithm>
#include <atomic>
#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <mutex>
#include <string>
#include <sys/mman.h>
#include <unistd.h>
#include <unordered_map>
#include <vector>

#include <glog/logging.h>
#include "ascend_allocator.h"
#include "bool_parser.h"
#include "config.h"
#include "shared_segment/host_memfd.h"
#include "acl/acl.h"
#include "transport/ascend_transport/ascend_direct_transport/adxl_compat.h"
#include "transport/ascend_transport/ascend_direct_transport/context_manager.h"
#include "transport/ascend_transport/ascend_direct_transport/utils.h"

namespace mooncake {
namespace {
constexpr size_t kFabricMemPageSize = 1024ULL * 1024 * 1024;  // 1G
constexpr size_t kMemAccessDescCount = 1;
constexpr int kBestEffortStartPercent = 100;
constexpr int kBestEffortMinPercent = 50;
constexpr int kBestEffortPercentStep = 10;

std::atomic<uint32_t> g_agent_alloc_next{0};

bool BindNextAgentDevice() {
    if (!globalConfig().ascend_agent_mode) {
        return true;
    }
    auto &mgr = ContextManager::getInstance();
    if (!mgr.isInitialized()) {
        return true;
    }
    const uint32_t n = mgr.getDeviceCount();
    if (n == 0) {
        return true;
    }
    const uint32_t idx = g_agent_alloc_next.load(std::memory_order_relaxed) % n;
    if (!mgr.setCurrentContext(static_cast<int32_t>(idx))) {
        LOG(ERROR) << "Failed to set agent-mode alloc context for device "
                   << idx;
        return false;
    }
    return true;
}

void CommitAgentDeviceSlot() {
    if (!globalConfig().ascend_agent_mode) {
        return;
    }
    if (!ContextManager::getInstance().isInitialized()) {
        return;
    }
    g_agent_alloc_next.fetch_add(1, std::memory_order_relaxed);
}

size_t align_down_1g(size_t n) {
    return (n / kFabricMemPageSize) * kFabricMemPageSize;
}

struct StoreMemRange {
    void *base;
    size_t size;
};
std::mutex g_store_mem_mutex;
std::vector<StoreMemRange> g_store_mem_ranges;

void remove_store_memory_range(void *ptr) {
    std::lock_guard<std::mutex> store_lock(g_store_mem_mutex);
    auto it = std::remove_if(
        g_store_mem_ranges.begin(), g_store_mem_ranges.end(),
        [ptr](const StoreMemRange &range) { return range.base == ptr; });
    g_store_mem_ranges.erase(it, g_store_mem_ranges.end());
}

void add_store_memory_range(void *ptr, size_t size) {
    std::lock_guard<std::mutex> store_lock(g_store_mem_mutex);
    g_store_mem_ranges.push_back({ptr, size});
}

// ASCEND_USE_SHMEM. Opt-in: allocate a memfd a peer on this machine can map.
// Parsed here so the store and GlobalConfig do not have to carry the switch.
bool SharedHostMemRequested() {
    const char *value = std::getenv("ASCEND_USE_SHMEM");
    if (value == nullptr) {
        return false;
    }
    const auto parsed = TryParseBool(value);
    if (!parsed.has_value()) {
        LOG(WARNING) << "Invalid value '" << value
                     << "' for ASCEND_USE_SHMEM, treating it as disabled";
        return false;
    }
    return *parsed;
}

// Mirrors hixl GetSocTypeByName(), which classifies any SoC whose name
// contains "Ascend950" as V5 (A5). Queried once: the SoC cannot change under a
// running process.
bool IsA5Soc() {
    static const bool kIsA5 = []() -> bool {
        const char *soc_name = aclrtGetSocName();
        if (soc_name == nullptr || soc_name[0] == '\0') {
            LOG(WARNING) << "aclrtGetSocName returned no name; assuming the "
                            "SoC is not A5";
            return false;
        }
        const bool is_a5 = std::strstr(soc_name, "Ascend950") != nullptr;
        LOG(INFO) << "Ascend SoC: " << soc_name << ", A5=" << is_a5;
        return is_a5;
    }();
    return kIsA5;
}

// MC_CUSTOM_TOPO_JSON is the classic-TE switch for a user-supplied host
// topology. A set path means this process will drive host memory from a NIC,
// which is the same reason A5 cannot use aclrtMallocHost.
bool CustomTopologyConfigured() {
    const char *path = std::getenv("MC_CUSTOM_TOPO_JSON");
    return path != nullptr && path[0] != '\0';
}

// Private mmap on A5 only. Device RoCE and HCCS keep aclrtMallocHost.
bool HostMmapRequested() {
    if (!IsA5Soc()) {
        return false;
    }
    if (CustomTopologyConfigured()) {
        return true;
    }
    const char *config = std::getenv("ASCEND_GLOBAL_RESOURCE_CONFIG");
    return HasRoceHostProtocolDescInGlobalResourceConfig(config);
}

// Host segments backed by an mmap rather than aclrtMallocHost. The mapped
// length is rounded up to the THP granularity, so the fd and the length that
// munmap needs are kept here rather than re-derived at free time. `fd` is -1
// for a private mapping, which has nothing to close and nothing to share.
struct HostMappingRecord {
    int fd;
    size_t mapped_size;
};
std::mutex g_host_mapping_mutex;
std::unordered_map<void *, HostMappingRecord> g_host_mappings;

size_t align_up_to_thp(size_t total_size) {
    const uint64_t granularity = host_memfd::Granularity();
    return ((total_size + granularity - 1) / granularity) * granularity;
}

// Returns the exact requested size to the caller; the padding up to the THP
// granularity is left unused but stays mapped until the segment is freed.
void *allocate_host_mapping(size_t total_size, bool shared) {
    const size_t mapped_size = align_up_to_thp(total_size);
    int fd = -1;
    void *addr = nullptr;
    std::string error;
    const bool ok = shared
                        ? host_memfd::Create(mapped_size, fd, addr, error)
                        : host_memfd::CreatePrivate(mapped_size, addr, error);
    if (!ok) {
        LOG(ERROR) << "Failed to allocate " << (shared ? "shared" : "private")
                   << " host memory, size: " << mapped_size << ", " << error;
        return nullptr;
    }
    {
        std::lock_guard<std::mutex> lock(g_host_mapping_mutex);
        g_host_mappings.emplace(addr, HostMappingRecord{fd, mapped_size});
    }
    add_store_memory_range(addr, mapped_size);
    LOG(INFO) << (shared ? "Shared" : "Private")
              << " host memory allocated, addr: " << addr
              << ", requested: " << total_size << ", mapped: " << mapped_size;
    if (shared && !host_memfd::ShmemThpEnabled()) {
        LOG(WARNING) << "shmem transparent huge pages are disabled, so this "
                        "segment is backed by 4KiB pages";
    }
    return addr;
}

// Returns true when `ptr` was an mmap-backed host allocation and has been
// released.
bool free_host_mapping(void *ptr) {
    HostMappingRecord record{};
    {
        std::lock_guard<std::mutex> lock(g_host_mapping_mutex);
        auto it = g_host_mappings.find(ptr);
        if (it == g_host_mappings.end()) {
            return false;
        }
        record = it->second;
        g_host_mappings.erase(it);
    }
    remove_store_memory_range(ptr);
    if (munmap(ptr, record.mapped_size) != 0) {
        LOG(ERROR) << "Failed to unmap host memory: " << ptr
                   << ", size: " << record.mapped_size << ", "
                   << std::strerror(errno);
    }
    if (record.fd >= 0) {
        close(record.fd);
    }
    return true;
}

#ifdef ASCEND_SUPPORT_FABRIC_MEM
struct AllocRecord {
    aclrtDrvMemHandle handle;
    bool is_direct_alloc;
    size_t size;
};
std::mutex g_vmm_alloc_mutex;
std::unordered_map<void *, AllocRecord> g_vmm_alloc_records;

int allocate_physical_memory(size_t total_size, aclrtDrvMemHandle &handle,
                             bool quiet) {
    int32_t user_dev_id;
    auto ret = aclrtGetDevice(&user_dev_id);
    if (ret != ACL_ERROR_NONE) {
        if (!quiet) {
            LOG(ERROR) << "Failed to get device: " << ret;
        }
        return -1;
    }
    int32_t physical_dev_id;
    ret = aclrtGetPhyDevIdByLogicDevId(user_dev_id, &physical_dev_id);
    if (ret != ACL_ERROR_NONE) {
        if (!quiet) {
            LOG(ERROR) << "Failed to get physical dev id: " << ret;
        }
        return -1;
    }
    aclrtPhysicalMemProp prop = {};
    prop.handleType = ACL_MEM_HANDLE_TYPE_NONE;
    prop.allocationType = ACL_MEM_ALLOCATION_TYPE_PINNED;
    prop.memAttr = ACL_MEM_P2P_HUGE1G;
    prop.location.type = ACL_MEM_LOCATION_TYPE_HOST_NUMA;
    // Only 0 2 4 6 is available for fabric mem, map 4 device to one numa.
    const int32_t kDevicesPerChip = 4;
    const int32_t kNumaNodeStep = 2;
    prop.location.id = (physical_dev_id / kDevicesPerChip) * kNumaNodeStep;
    prop.reserve = 0;
    if (!quiet) {
        LOG(INFO) << "Malloc host memory for numa:" << prop.location.id;
    }
    ret = aclrtMallocPhysical(&handle, total_size, &prop, 0);
    if (ret != ACL_ERROR_NONE) {
        if (!quiet) {
            LOG(INFO) << "Malloc host memory for numa:" << prop.location.id
                      << " failed, try common allocate instead.";
        }
        prop.location.type = ACL_MEM_LOCATION_TYPE_HOST;
        prop.location.id = 0;
        ret = aclrtMallocPhysical(&handle, total_size, &prop, 0);
        if (ret != ACL_ERROR_NONE) {
            if (!quiet) {
                LOG(INFO) << "Malloc failed, try smaller page instead.";
            }
            prop.memAttr = ACL_MEM_P2P_HUGE;
            ret = aclrtMallocPhysical(&handle, total_size, &prop, 0);
            if (ret != ACL_ERROR_NONE) {
                if (!quiet) {
                    LOG(ERROR) << "Failed to allocate memory: " << ret;
                }
                return -1;
            }
        }
    }
    return 0;
}

// Host physical pages mapped behind a VA are only bound to the host by
// aclrtMapMem; the NPU needs an explicit grant before SDMA/HCCS can touch them.
// adxl::MallocMem grants it internally for its own allocations, so the direct
// ACL VMM path has to do the same. location.id is a driver logical id while
// aclrtGetDevice returns a user id (ASCEND_RT_VISIBLE_DEVICES), so convert.
int grant_current_device_access(void *va, size_t total_size, bool quiet) {
    int32_t user_dev_id = -1;
    auto ret = aclrtGetDevice(&user_dev_id);
    if (ret != ACL_ERROR_NONE) {
        if (!quiet) {
            LOG(ERROR) << "Failed to get device for MemSetAccess: " << ret;
        }
        return -1;
    }
    int32_t driver_dev_id = -1;
    ret = aclrtGetLogicDevIdByUserDevId(user_dev_id, &driver_dev_id);
    if (ret != ACL_ERROR_NONE) {
        if (!quiet) {
            LOG(ERROR) << "Failed to get logical device id for MemSetAccess: "
                       << ret;
        }
        return -1;
    }

    aclrtMemAccessDesc desc = {};
    desc.flags = ACL_RT_MEM_ACCESS_FLAGS_READWRITE;
    desc.location.type = ACL_MEM_LOCATION_TYPE_DEVICE;
    desc.location.id = static_cast<uint32_t>(driver_dev_id);
    ret = aclrtMemSetAccess(va, total_size, &desc, kMemAccessDescCount);
    if (ret != ACL_ERROR_NONE) {
        if (!quiet) {
            LOG(ERROR) << "Failed to set memory access: " << ret;
        }
        return -1;
    }
    return 0;
}

// Direct ACL VMM allocation (always bypasses adxl MallocMem).
// Used by shm_helper when ascend_agent_mode && ascend_use_fabric_mem.
void *allocate_vmm_memory_direct_impl(size_t total_size, bool quiet = false) {
    aclrtDrvMemHandle handle = nullptr;
    if (allocate_physical_memory(total_size, handle, quiet) != 0) {
        return nullptr;
    }
    void *va = nullptr;
    auto ret = aclrtReserveMemAddress(&va, total_size, 0, nullptr, 1);
    if (ret != ACL_ERROR_NONE) {
        if (!quiet) {
            LOG(ERROR) << "Failed to reserve memory: " << ret;
        }
        (void)aclrtFreePhysical(handle);
        return nullptr;
    }
    ret = aclrtMapMem(va, total_size, 0, handle, 0);
    if (ret != ACL_ERROR_NONE) {
        if (!quiet) {
            LOG(ERROR) << "Failed to map memory: " << ret;
        }
        (void)aclrtReleaseMemAddress(va);
        (void)aclrtFreePhysical(handle);
        return nullptr;
    }
    if (grant_current_device_access(va, total_size, quiet) != 0) {
        (void)aclrtUnmapMem(va);
        (void)aclrtReleaseMemAddress(va);
        (void)aclrtFreePhysical(handle);
        return nullptr;
    }
    std::lock_guard<std::mutex> lock(g_vmm_alloc_mutex);
    g_vmm_alloc_records.emplace(va, AllocRecord{handle, true, total_size});
    return va;
}

// Exact-size fabric alloc: use adxl when the weak symbol is present; otherwise
// direct VMM. Matches original ascend_allocate_memory behavior — adxl failure
// does not fall back to direct VMM. When quiet=true, skip failure ERROR logs
// (used by best-effort percentile probes).
void *allocate_fabric_exact(size_t total_size, bool quiet = false) {
    void *va = nullptr;
    if (&adxl::AdxlEngine::MallocMem != nullptr) {
        auto status = adxl::AdxlEngine::MallocMem(adxl::MemType::MEM_HOST,
                                                  total_size, &va);
        if (status != adxl::SUCCESS) {
            if (!quiet) {
                LOG(ERROR) << "Failed to allocate fabric memory, errmsg: "
                           << aclGetRecentErrMsg();
            }
            return nullptr;
        }
        LOG(INFO) << "Call adxl MallocMem suc, va:" << va
                  << ", size:" << total_size;
        std::lock_guard<std::mutex> lock(g_vmm_alloc_mutex);
        g_vmm_alloc_records.emplace(va,
                                    AllocRecord{nullptr, false, total_size});
        return va;
    }
    va = allocate_vmm_memory_direct_impl(total_size, quiet);
    if (va) {
        add_store_memory_range(va, total_size);
    }
    return va;
}
#endif

void *AllocateStoreMemoryImpl(size_t total_size, const std::string &protocol) {
    if (globalConfig().ascend_use_fabric_mem) {
#ifdef ASCEND_SUPPORT_FABRIC_MEM
        return allocate_fabric_exact(total_size);
#else
        (void)total_size;
        LOG(ERROR) << "Fabric mem mode is not supported, please upgrade Ascend "
                      "HDK and CANN.";
        return nullptr;
#endif
    }
    if (protocol == "ubshmem") {
        LOG(ERROR) << "ubshmem protocol only support fabric mem, please "
                      "configure ASCEND_ENABLE_USE_FABRIC_MEM environment.";
        return nullptr;
    }

    // A5 host RoCE cannot drive aclrtMallocHost memory. That only applies when
    // protocol_desc contains roce:host, or the user set MC_CUSTOM_TOPO_JSON.
    // Sharing is a separate opt-in: ASCEND_USE_SHMEM asks for a memfd that a
    // peer process on this machine can map too.
    if (SharedHostMemRequested()) {
        return allocate_host_mapping(total_size, /*shared=*/true);
    }
    if (HostMmapRequested()) {
        return allocate_host_mapping(total_size, /*shared=*/false);
    }

    void *buffer = nullptr;
    auto ret = aclrtMallocHost(&buffer, total_size);
    if (ret != ACL_ERROR_NONE) {
        LOG(ERROR) << "Failed to allocate memory: " << ret;
        return nullptr;
    }
    LOG(INFO) << "aclrtMallocHost suc, addr: " << buffer;
    if (buffer) {
        add_store_memory_range(buffer, total_size);
    }
    return buffer;
}

void *AllocateExactOnBoundDevice(size_t total_size, const std::string &protocol,
                                 size_t *actual_size,
                                 AscendHostAllocFn host_alloc,
                                 size_t host_alignment,
                                 bool defer_hugetlb_population) {
    void *ptr =
        host_alloc != nullptr
            ? host_alloc(total_size, host_alignment, defer_hugetlb_population)
            : AllocateStoreMemoryImpl(total_size, protocol);
    if (ptr == nullptr) {
        return nullptr;
    }
    *actual_size = total_size;
    CommitAgentDeviceSlot();
    return ptr;
}

#ifdef ASCEND_SUPPORT_FABRIC_MEM
void *AllocateFabricBestEffort(size_t target_size, size_t *actual_size) {
    // Try 100%, 90%, ... down to 50% of configured size (1G-aligned).
    // Skip candidates that fall below the unaligned 50% floor after round-down
    // (e.g. 2.1 GiB target → 50% is 1.05 GiB → 1 GiB must not be accepted).
    // Probe attempts are quiet; only the final failure logs ERROR.
    const size_t min_size =
        target_size * static_cast<size_t>(kBestEffortMinPercent) / 100;
    size_t last_tried = 0;
    for (int pct = kBestEffortStartPercent; pct >= kBestEffortMinPercent;
         pct -= kBestEffortPercentStep) {
        size_t sz =
            (pct == kBestEffortStartPercent)
                ? align_down_1g(target_size)
                : align_down_1g(target_size * static_cast<size_t>(pct) / 100);
        if (sz == 0 || sz < min_size || sz == last_tried) {
            continue;
        }
        last_tried = sz;
        void *ptr = allocate_fabric_exact(sz, /*quiet=*/true);
        if (ptr != nullptr) {
            *actual_size = sz;
            CommitAgentDeviceSlot();
            if (sz < target_size) {
                LOG(WARNING) << "Fabric mem best-effort: target=" << target_size
                             << ", actual=" << sz << " (" << pct << "%)";
            }
            return ptr;
        }
    }
    LOG(ERROR) << "Fabric mem best-effort failed: cannot allocate at least "
               << kBestEffortMinPercent << "% of target=" << target_size;
    return nullptr;
}
#endif
}  // namespace

void *ascend_allocate_vmm_memory_direct(size_t total_size) {
#ifdef ASCEND_SUPPORT_FABRIC_MEM
    return allocate_vmm_memory_direct_impl(total_size);
#else
    (void)total_size;
    return nullptr;
#endif
}

aclrtDrvMemHandle ascend_get_physical_handle_from_va(void *va) {
#ifdef ASCEND_SUPPORT_FABRIC_MEM
    std::lock_guard<std::mutex> lock(g_vmm_alloc_mutex);
    auto it = g_vmm_alloc_records.find(va);
    if (it == g_vmm_alloc_records.end() || !it->second.is_direct_alloc) {
        return nullptr;
    }
    return it->second.handle;
#else
    (void)va;
    return nullptr;
#endif
}

void *ascend_allocate_memory(size_t total_size, const std::string &protocol) {
    if (!BindNextAgentDevice()) {
        return nullptr;
    }
    void *ptr = AllocateStoreMemoryImpl(total_size, protocol);
    if (ptr) {
        CommitAgentDeviceSlot();
    }
    return ptr;
}

void *ascend_allocate_memory_best_effort(size_t target_size,
                                         const std::string &protocol,
                                         size_t *actual_size,
                                         AscendHostAllocFn host_alloc,
                                         size_t host_alignment,
                                         bool defer_hugetlb_population) {
    if (actual_size == nullptr) {
        LOG(ERROR) << "ascend_allocate_memory_best_effort: actual_size is null";
        return nullptr;
    }
    *actual_size = 0;
    if (!BindNextAgentDevice()) {
        return nullptr;
    }
    if (!globalConfig().ascend_use_fabric_mem) {
        return AllocateExactOnBoundDevice(target_size, protocol, actual_size,
                                          host_alloc, host_alignment,
                                          defer_hugetlb_population);
    }
    (void)protocol;
    (void)host_alloc;
    (void)host_alignment;
    (void)defer_hugetlb_population;
#ifdef ASCEND_SUPPORT_FABRIC_MEM
    return AllocateFabricBestEffort(target_size, actual_size);
#else
    LOG(ERROR) << "Fabric mem mode is not supported, please upgrade Ascend "
                  "HDK and CANN.";
    return nullptr;
#endif
}

bool ascend_get_shared_host_handle(void *addr, std::string *handle) {
    if (addr == nullptr || handle == nullptr) {
        return false;
    }
    std::lock_guard<std::mutex> lock(g_host_mapping_mutex);
    auto it = g_host_mappings.find(addr);
    // Only the allocation base is advertised. A caller registering a sub-range
    // would make peers compute offsets against the wrong origin, so such a
    // buffer simply does not advertise sharing and keeps using ADXL.
    if (it == g_host_mappings.end() || it->second.fd < 0) {
        return false;
    }
    *handle = host_memfd::ExportHandle(it->second.fd);
    return true;
}

bool ascend_is_store_memory(void *addr, size_t length) {
    if (!addr || length == 0) return false;
    auto addr_start = reinterpret_cast<uintptr_t>(addr);
    uintptr_t addr_end = addr_start + length;
    std::lock_guard<std::mutex> lock(g_store_mem_mutex);
    for (const auto &range : g_store_mem_ranges) {
        auto range_start = reinterpret_cast<uintptr_t>(range.base);
        uintptr_t range_end = range_start + range.size;
        if (addr_start < range_end && addr_end > range_start) {
            return true;
        }
    }
    return false;
}

bool ascend_is_direct_vmm_memory(void *addr, size_t length) {
    if (!addr || length == 0) return false;
    const auto addr_start = reinterpret_cast<uintptr_t>(addr);
#ifdef ASCEND_SUPPORT_FABRIC_MEM
    std::lock_guard<std::mutex> lock(g_vmm_alloc_mutex);
    for (const auto &[base, record] : g_vmm_alloc_records) {
        if (!record.is_direct_alloc) {
            continue;
        }
        // Only the start address identifies the allocation. A caller may hand
        // over a longer range than was allocated - a store segment is mounted
        // at its requested size while best-effort fabric allocation can return
        // less - and demanding full containment would silently keep that
        // buffer as MEM_HOST.
        const auto base_start = reinterpret_cast<uintptr_t>(base);
        if (addr_start >= base_start && addr_start < base_start + record.size) {
            return true;
        }
    }
#else
    (void)addr_start;
#endif
    return false;
}

void ascend_free_memory(const std::string &protocol, void *ptr) {
    // Route by the per-allocation record. Fabric mode is a process-global
    // toggled during TE init, and the shared-memfd choice is an environment
    // read at alloc time; neither can be trusted at free time.
    // Fabric/VMM allocations are recorded in g_vmm_alloc_records and
    // mmap-backed host allocations in g_host_mappings; everything else is a
    // plain aclrtMallocHost buffer.
    if (free_host_mapping(ptr)) {
        return;
    }
#ifdef ASCEND_SUPPORT_FABRIC_MEM
    std::unique_lock<std::mutex> lock(g_vmm_alloc_mutex);
    auto it = g_vmm_alloc_records.find(ptr);
    if (it != g_vmm_alloc_records.end()) {
        const AllocRecord alloc_record = it->second;
        g_vmm_alloc_records.erase(it);
        lock.unlock();
        remove_store_memory_range(ptr);
        if (!alloc_record.is_direct_alloc) {
            if (&adxl::AdxlEngine::FreeMem != nullptr) {
                auto status = adxl::AdxlEngine::FreeMem(ptr);
                if (status != adxl::SUCCESS) {
                    LOG(ERROR) << "Failed to free fabric memory, errmsg: "
                               << aclGetRecentErrMsg();
                }
                return;
            }
        }
        auto ret = aclrtUnmapMem(ptr);
        if (ret != ACL_ERROR_NONE) {
            LOG(ERROR) << "Failed to unmap memory: " << ptr;
            return;
        }
        ret = aclrtReleaseMemAddress(ptr);
        if (ret != ACL_ERROR_NONE) {
            LOG(ERROR) << "Failed to release mem address: " << ptr;
            return;
        }
        ret = aclrtFreePhysical(alloc_record.handle);
        if (ret != ACL_ERROR_NONE) {
            LOG(ERROR) << "Failed to free physical mem: " << ptr;
            return;
        }
        return;
    }
    lock.unlock();
#endif

    if (protocol == "ascend") {
        remove_store_memory_range(ptr);
        aclrtFreeHost(ptr);
    }
}

}  // namespace mooncake
