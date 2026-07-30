// Copyright 2024 KVCache.AI

#ifndef NVLINK_HOST_NUMA_ALLOCATION_H_
#define NVLINK_HOST_NUMA_ALLOCATION_H_

#include "cuda_alike.h"

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <vector>

#include "common/base/status.h"

#if defined(USE_MNNVL) && defined(USE_CUDA) && CUDA_VERSION >= 12030
#define MOONCAKE_NVLINK_HOST_NUMA_ENABLED 1
#else
#define MOONCAKE_NVLINK_HOST_NUMA_ENABLED 0
#endif

namespace mooncake {

class NvlinkHostNumaAllocationTestPeer;
class NvlinkTransport;
class NvlinkTransportTestPeer;

class NvlinkHostNumaAllocation {
   public:
    NvlinkHostNumaAllocation(const NvlinkHostNumaAllocation&) = delete;
    NvlinkHostNumaAllocation& operator=(const NvlinkHostNumaAllocation&) =
        delete;

#if MOONCAKE_NVLINK_HOST_NUMA_ENABLED
    ~NvlinkHostNumaAllocation();

    static Status DiscoverHostNumaNodes(std::vector<int>& numa_nodes);
    static Status GetAllocationGranularity(int numa_node, size_t& granularity);
    static Status Create(int numa_node, size_t requested_length,
                         size_t required_va_alignment,
                         std::unique_ptr<NvlinkHostNumaAllocation>& allocation);

    // Completed reverse-release stages are not repeated by a later call.
    [[nodiscard]] Status Release();
#else
    ~NvlinkHostNumaAllocation() = default;

    static Status DiscoverHostNumaNodes(std::vector<int>& numa_nodes) {
        numa_nodes.clear();
        return Unsupported();
    }
    static Status GetAllocationGranularity(int, size_t& granularity) {
        granularity = 0;
        return Unsupported();
    }
    static Status Create(
        int, size_t, size_t,
        std::unique_ptr<NvlinkHostNumaAllocation>& allocation) {
        allocation.reset();
        return Unsupported();
    }
    [[nodiscard]] Status Release() { return Status::OK(); }
#endif

    void* base() const { return base_; }
    size_t length() const { return length_; }
    int numaNode() const { return numa_node_; }

   private:
    friend class NvlinkHostNumaAllocationTestPeer;
    friend class NvlinkTransport;
    friend class NvlinkTransportTestPeer;

    NvlinkHostNumaAllocation() = default;

    static Status Unsupported() {
        return Status::NotSupportedTransport(
            "HOST_NUMA Fabric VMM requires USE_MNNVL, USE_CUDA, and CUDA "
            "Toolkit 12.3 or newer");
    }

#if MOONCAKE_NVLINK_HOST_NUMA_ENABLED
    struct DriverApi {
        std::function<CUresult(int*)> device_get_count;
        std::function<CUresult(CUdevice*, int)> device_get;
        std::function<CUresult(int*, CUdevice_attribute, CUdevice)>
            device_get_attribute;
        std::function<CUresult(size_t*, const CUmemAllocationProp*,
                               CUmemAllocationGranularity_flags)>
            mem_get_allocation_granularity;
        std::function<CUresult(CUmemGenericAllocationHandle*, size_t,
                               const CUmemAllocationProp*, unsigned long long)>
            mem_create;
        std::function<CUresult(CUdeviceptr*, size_t, size_t, CUdeviceptr,
                               unsigned long long)>
            mem_address_reserve;
        std::function<CUresult(CUdeviceptr, size_t, size_t,
                               CUmemGenericAllocationHandle,
                               unsigned long long)>
            mem_map;
        std::function<CUresult(CUdeviceptr, size_t, const CUmemAccessDesc*,
                               size_t)>
            mem_set_access;
        std::function<CUresult(CUdeviceptr, size_t)> mem_unmap;
        std::function<CUresult(CUdeviceptr, size_t)> mem_address_free;
        std::function<CUresult(CUmemGenericAllocationHandle)> mem_release;
        std::function<CUresult(CUmemGenericAllocationHandle*, void*)>
            mem_retain_allocation_handle;
        std::function<CUresult(void*, CUmemGenericAllocationHandle,
                               CUmemAllocationHandleType, unsigned long long)>
            mem_export_to_shareable_handle;
    };
    struct OwnedRangeRegistry;

    static DriverApi ProductionDriverApi();
    static Status ValidateVisibleGpuFabricSupportWithDriverApi(
        const DriverApi& api, std::vector<CUdevice>& visible_devices);
    static Status DiscoverHostNumaNodesWithDriverApi(
        const DriverApi& api, std::vector<int>& numa_nodes);
    static Status GetAllocationGranularityWithDriverApi(int numa_node,
                                                        const DriverApi& api,
                                                        size_t& granularity);
    static Status CreateWithDriverApi(
        int numa_node, size_t requested_length, size_t required_va_alignment,
        const DriverApi& api,
        std::unique_ptr<NvlinkHostNumaAllocation>& allocation);

    static bool RegisterOwnedRange(void* base, size_t length,
                                   const DriverApi& api);
    static bool UnregisterOwnedRange(void* base, size_t length);
    static bool FindExactOwnedRange(void* base, size_t length, DriverApi* api);
    static bool OverlapsOwnedRange(void* base, size_t length);
    static OwnedRangeRegistry& OwnedRanges();
#endif

    void* base_ = nullptr;
    size_t length_ = 0;
    int numa_node_ = -1;
    bool mapped_ = false;
    bool address_reserved_ = false;
    bool handle_owned_ = false;
    uint64_t allocation_handle_ = 0;
#if MOONCAKE_NVLINK_HOST_NUMA_ENABLED
    bool owned_range_registered_ = false;
    DriverApi driver_api_;
#endif
};

}  // namespace mooncake

#endif  // NVLINK_HOST_NUMA_ALLOCATION_H_
