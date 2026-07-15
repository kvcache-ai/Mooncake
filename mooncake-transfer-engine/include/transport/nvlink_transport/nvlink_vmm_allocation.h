// Copyright 2024 KVCache.AI

#ifndef NVLINK_VMM_ALLOCATION_H_
#define NVLINK_VMM_ALLOCATION_H_

#include "cuda_alike.h"

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>

#include "common/base/status.h"

namespace mooncake {

class NvlinkTransport;
class NvlinkTransportTestPeer;

class NvlinkVmmAllocation {
   public:
    enum class LocationType { DEVICE, HOST_NUMA };

    struct Options {
        LocationType location_type = LocationType::DEVICE;
        int location_id = 0;
        size_t requested_length = 0;
        bool fabric_exportable = false;
        // Zero selects the CUDA allocation granularity. Store global chunks can
        // request a stronger alignment (for example Cachelib slab alignment).
        size_t required_va_alignment = 0;
        // Optional bounded observability hook for the access-grant portion of
        // Create(). It is invoked once when that stage is reached.
        std::function<void(uint64_t duration_us, bool success)> access_observer;
    };

    NvlinkVmmAllocation(const NvlinkVmmAllocation&) = delete;
    NvlinkVmmAllocation& operator=(const NvlinkVmmAllocation&) = delete;

#if defined(USE_MNNVL) && defined(USE_CUDA)
    NvlinkVmmAllocation(NvlinkVmmAllocation&& other) noexcept;
    NvlinkVmmAllocation& operator=(NvlinkVmmAllocation&&) noexcept = delete;
    ~NvlinkVmmAllocation();

    static Status Create(const Options& options,
                         std::unique_ptr<NvlinkVmmAllocation>& allocation);
    static Status GetAllocationGranularity(LocationType location_type,
                                           int location_id,
                                           bool fabric_exportable,
                                           size_t& granularity);
    static Status CheckStrictFabricCapability();

    // Releases CUDA VMM resources in reverse creation order. Each completed
    // stage is committed independently; on failure the remaining ownership is
    // retained so an explicit caller can retry safely.
    [[nodiscard]] Status Release();
#else
    NvlinkVmmAllocation(NvlinkVmmAllocation&&) noexcept = default;
    NvlinkVmmAllocation& operator=(NvlinkVmmAllocation&&) noexcept = delete;
    ~NvlinkVmmAllocation() = default;

    static Status Create(const Options&,
                         std::unique_ptr<NvlinkVmmAllocation>& allocation) {
        allocation.reset();
        return Status::NotSupportedTransport(
            "NVLink VMM requires USE_MNNVL and USE_CUDA");
    }
    static Status GetAllocationGranularity(LocationType, int, bool,
                                           size_t& granularity) {
        granularity = 0;
        return Status::NotSupportedTransport(
            "NVLink VMM requires USE_MNNVL and USE_CUDA");
    }
    static Status CheckStrictFabricCapability() {
        return Status::NotSupportedTransport(
            "NVLink VMM requires USE_MNNVL and USE_CUDA");
    }
    [[nodiscard]] Status Release() { return Status::OK(); }
#endif

    void* base() const { return base_; }
    size_t length() const { return length_; }
    size_t granularity() const { return granularity_; }
    size_t va_alignment() const { return va_alignment_; }
    LocationType location_type() const { return location_type_; }
    int location_id() const { return location_id_; }
    bool fabric_exportable() const { return fabric_exportable_; }

   private:
    friend class NvlinkTransport;
    friend class NvlinkTransportTestPeer;
    NvlinkVmmAllocation() = default;

#if defined(USE_MNNVL) && defined(USE_CUDA)
    // Private fake-driver seam. Production callers use Create(),
    // GetAllocationGranularity(), and CheckStrictFabricCapability(); the friend
    // test peer exposes only a test-local alias and forwarding methods.
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
        std::function<CUresult(CUmemAllocationProp*,
                               CUmemGenericAllocationHandle)>
            mem_get_allocation_properties_from_handle;
        std::function<CUresult(CUdeviceptr*, size_t*, CUdeviceptr)>
            mem_get_address_range;
        std::function<CUresult(void*, CUmemGenericAllocationHandle,
                               CUmemAllocationHandleType, unsigned long long)>
            mem_export_to_shareable_handle;
        std::function<CUresult(CUmemGenericAllocationHandle*, void*,
                               CUmemAllocationHandleType)>
            mem_import_from_shareable_handle;
    };

    static DriverApi ProductionDriverApi();
    static Status CreateWithDriverApi(
        const Options& options, const DriverApi& api,
        std::unique_ptr<NvlinkVmmAllocation>& allocation);
    static Status GetAllocationGranularityWithDriverApi(
        LocationType location_type, int location_id, bool fabric_exportable,
        const DriverApi& api, size_t& granularity);
    static Status CheckStrictFabricCapabilityWithDriverApi(
        const DriverApi& api);
    static bool RegisterOwnedRange(void* base, size_t length);
    static bool UnregisterOwnedRange(void* base, size_t length);
    static bool IsExactOwnedRange(void* base, size_t length);
    static bool RetryCleanupPendingOwners();
    static size_t CleanupPendingOwnerCount();
    void reset() noexcept;
#endif

    void* base_ = nullptr;
    size_t length_ = 0;
    size_t granularity_ = 0;
    size_t va_alignment_ = 0;
    LocationType location_type_ = LocationType::DEVICE;
    int location_id_ = 0;
    bool fabric_exportable_ = false;
    bool mapped_ = false;
    bool address_reserved_ = false;
    bool handle_owned_ = false;
    uint64_t allocation_handle_ = 0;
#if defined(USE_MNNVL) && defined(USE_CUDA)
    bool owned_range_registered_ = false;
    DriverApi driver_api_;
#endif
};

}  // namespace mooncake

#endif  // NVLINK_VMM_ALLOCATION_H_
