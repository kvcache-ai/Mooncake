// Copyright 2024 KVCache.AI

#include "transport/nvlink_transport/nvlink_host_numa_allocation.h"

#if defined(USE_MNNVL) && defined(USE_CUDA) && CUDA_VERSION >= 12030

#include <algorithm>
#include <cstdlib>
#include <limits>
#include <mutex>
#include <string>
#include <unordered_map>
#include <utility>

#include <glog/logging.h>

namespace mooncake {
namespace {

Status cudaFailure(const char* stage, CUresult result) {
    return Status::Memory(std::string(stage) + " failed with CUDA result " +
                          std::to_string(static_cast<int>(result)));
}

Status missingDriverFunction(const char* name) {
    return Status::InvalidArgument(
        std::string("missing CUDA driver adapter: ") + name);
}

bool isPowerOfTwo(size_t value) {
    return value != 0 && (value & (value - 1)) == 0;
}

CUmemAllocationProp hostNumaProp(int numa_node) {
    CUmemAllocationProp prop = {};
    prop.type = CU_MEM_ALLOCATION_TYPE_PINNED;
    prop.location.type = CU_MEM_LOCATION_TYPE_HOST_NUMA;
    prop.location.id = numa_node;
    prop.requestedHandleTypes = CU_MEM_HANDLE_TYPE_FABRIC;
    return prop;
}

}  // namespace

struct NvlinkHostNumaAllocation::OwnedRangeRegistry {
    struct Entry {
        size_t length;
        DriverApi api;
    };

    std::mutex mutex;
    std::unordered_map<void*, Entry> ranges;
};

NvlinkHostNumaAllocation::OwnedRangeRegistry&
NvlinkHostNumaAllocation::OwnedRanges() {
    static auto* registry = new OwnedRangeRegistry();
    return *registry;
}

NvlinkHostNumaAllocation::DriverApi
NvlinkHostNumaAllocation::ProductionDriverApi() {
    return DriverApi{
        cuDeviceGetCount,
        cuDeviceGet,
        cuDeviceGetAttribute,
        cuMemGetAllocationGranularity,
        cuMemCreate,
        cuMemAddressReserve,
        cuMemMap,
        cuMemSetAccess,
        cuMemUnmap,
        cuMemAddressFree,
        cuMemRelease,
        cuMemRetainAllocationHandle,
        cuMemExportToShareableHandle,
    };
}

bool NvlinkHostNumaAllocation::RegisterOwnedRange(void* base, size_t length,
                                                  const DriverApi& api) {
    if (base == nullptr || length == 0) return false;
    auto& registry = OwnedRanges();
    std::lock_guard<std::mutex> lock(registry.mutex);
    try {
        return registry.ranges
            .emplace(base, OwnedRangeRegistry::Entry{length, api})
            .second;
    } catch (...) {
        return false;
    }
}

bool NvlinkHostNumaAllocation::UnregisterOwnedRange(void* base, size_t length) {
    auto& registry = OwnedRanges();
    std::lock_guard<std::mutex> lock(registry.mutex);
    auto entry = registry.ranges.find(base);
    if (entry == registry.ranges.end() || entry->second.length != length)
        return false;
    registry.ranges.erase(entry);
    return true;
}

bool NvlinkHostNumaAllocation::FindExactOwnedRange(void* base, size_t length,
                                                   DriverApi* api) {
    if (api == nullptr) return false;
    auto& registry = OwnedRanges();
    std::lock_guard<std::mutex> lock(registry.mutex);
    auto entry = registry.ranges.find(base);
    if (entry == registry.ranges.end() || entry->second.length != length)
        return false;
    try {
        *api = entry->second.api;
        return true;
    } catch (...) {
        return false;
    }
}

bool NvlinkHostNumaAllocation::OverlapsOwnedRange(void* base, size_t length) {
    if (base == nullptr || length == 0) return false;
    const uintptr_t begin = reinterpret_cast<uintptr_t>(base);
    const uintptr_t end = length > std::numeric_limits<uintptr_t>::max() - begin
                              ? std::numeric_limits<uintptr_t>::max()
                              : begin + length;

    auto& registry = OwnedRanges();
    std::lock_guard<std::mutex> lock(registry.mutex);
    for (const auto& [owned_base, entry] : registry.ranges) {
        const uintptr_t owned_begin = reinterpret_cast<uintptr_t>(owned_base);
        const uintptr_t owned_end = owned_begin + entry.length;
        if (begin < owned_end && owned_begin < end) return true;
    }
    return false;
}

Status NvlinkHostNumaAllocation::DiscoverHostNumaNodes(
    std::vector<int>& numa_nodes) {
    return DiscoverHostNumaNodesWithDriverApi(ProductionDriverApi(),
                                              numa_nodes);
}

Status NvlinkHostNumaAllocation::ValidateVisibleGpuFabricSupportWithDriverApi(
    const DriverApi& api, std::vector<CUdevice>& visible_devices) {
    visible_devices.clear();
    if (std::getenv("MC_USE_NVLINK_IPC") != nullptr)
        return Status::NotSupportedTransport(
            "MC_USE_NVLINK_IPC disables Fabric allocations");
    if (!api.device_get_count) return missingDriverFunction("cuDeviceGetCount");
    if (!api.device_get) return missingDriverFunction("cuDeviceGet");
    if (!api.device_get_attribute)
        return missingDriverFunction("cuDeviceGetAttribute");

    int device_count = 0;
    CUresult result = api.device_get_count(&device_count);
    if (result != CUDA_SUCCESS) return cudaFailure("cuDeviceGetCount", result);
    if (device_count <= 0)
        return Status::DeviceNotFound("no visible CUDA device");

    try {
        visible_devices.reserve(static_cast<size_t>(device_count));
    } catch (...) {
        return Status::Memory("failed to record visible CUDA devices");
    }
    for (int ordinal = 0; ordinal < device_count; ++ordinal) {
        CUdevice device;
        result = api.device_get(&device, ordinal);
        if (result != CUDA_SUCCESS) return cudaFailure("cuDeviceGet", result);

        int vmm_supported = 0;
        result = api.device_get_attribute(
            &vmm_supported,
            CU_DEVICE_ATTRIBUTE_VIRTUAL_MEMORY_MANAGEMENT_SUPPORTED, device);
        if (result != CUDA_SUCCESS)
            return cudaFailure("cuDeviceGetAttribute(VMM)", result);

        int fabric_supported = 0;
        result = api.device_get_attribute(
            &fabric_supported, CU_DEVICE_ATTRIBUTE_HANDLE_TYPE_FABRIC_SUPPORTED,
            device);
        if (result != CUDA_SUCCESS)
            return cudaFailure("cuDeviceGetAttribute(Fabric)", result);
        if (!vmm_supported || !fabric_supported)
            return Status::NotSupportedTransport(
                "a visible CUDA device lacks VMM or Fabric handle support");

#if CUDA_VERSION >= 12090
        int host_numa_vmm_supported = 0;
        result = api.device_get_attribute(
            &host_numa_vmm_supported,
            CU_DEVICE_ATTRIBUTE_HOST_NUMA_VIRTUAL_MEMORY_MANAGEMENT_SUPPORTED,
            device);
        if (result != CUDA_SUCCESS)
            return cudaFailure("cuDeviceGetAttribute(HOST_NUMA VMM)", result);
        if (!host_numa_vmm_supported)
            return Status::NotSupportedTransport(
                "a visible CUDA device lacks HOST_NUMA VMM support");
#endif

        visible_devices.push_back(device);
    }
    return Status::OK();
}

Status NvlinkHostNumaAllocation::DiscoverHostNumaNodesWithDriverApi(
    const DriverApi& api, std::vector<int>& numa_nodes) {
    numa_nodes.clear();
    std::vector<CUdevice> visible_devices;
    Status status =
        ValidateVisibleGpuFabricSupportWithDriverApi(api, visible_devices);
    if (!status.ok()) return status;

    for (CUdevice device : visible_devices) {
        int numa_node = -1;
        CUresult result = api.device_get_attribute(
            &numa_node, CU_DEVICE_ATTRIBUTE_HOST_NUMA_ID, device);
        if (result != CUDA_SUCCESS)
            return cudaFailure("cuDeviceGetAttribute(HOST_NUMA_ID)", result);
        if (numa_node < 0)
            return Status::Numa(
                "a visible CUDA device has no GPU-local host NUMA node");
        numa_nodes.push_back(numa_node);
    }

    std::sort(numa_nodes.begin(), numa_nodes.end());
    numa_nodes.erase(std::unique(numa_nodes.begin(), numa_nodes.end()),
                     numa_nodes.end());
    for (int numa_node : numa_nodes) {
        size_t ignored = 0;
        status = GetAllocationGranularityWithDriverApi(numa_node, api, ignored);
        if (!status.ok()) {
            numa_nodes.clear();
            return status;
        }
    }
    return Status::OK();
}

Status NvlinkHostNumaAllocation::GetAllocationGranularity(int numa_node,
                                                          size_t& granularity) {
    return GetAllocationGranularityWithDriverApi(
        numa_node, ProductionDriverApi(), granularity);
}

Status NvlinkHostNumaAllocation::GetAllocationGranularityWithDriverApi(
    int numa_node, const DriverApi& api, size_t& granularity) {
    granularity = 0;
    if (numa_node < 0)
        return Status::InvalidArgument("host NUMA node must be non-negative");
    if (!api.mem_get_allocation_granularity)
        return missingDriverFunction("cuMemGetAllocationGranularity");

    CUmemAllocationProp prop = hostNumaProp(numa_node);
    CUresult result = api.mem_get_allocation_granularity(
        &granularity, &prop, CU_MEM_ALLOC_GRANULARITY_MINIMUM);
    if (result != CUDA_SUCCESS)
        return cudaFailure("cuMemGetAllocationGranularity(HOST_NUMA)", result);
    if (!isPowerOfTwo(granularity)) {
        granularity = 0;
        return Status::Memory(
            "HOST_NUMA allocation granularity is not a power of two");
    }
    return Status::OK();
}

Status NvlinkHostNumaAllocation::Create(
    int numa_node, size_t requested_length, size_t required_va_alignment,
    std::unique_ptr<NvlinkHostNumaAllocation>& allocation) {
    return CreateWithDriverApi(numa_node, requested_length,
                               required_va_alignment, ProductionDriverApi(),
                               allocation);
}

Status NvlinkHostNumaAllocation::CreateWithDriverApi(
    int numa_node, size_t requested_length, size_t required_va_alignment,
    const DriverApi& api,
    std::unique_ptr<NvlinkHostNumaAllocation>& allocation) {
    allocation.reset();
    if (requested_length == 0)
        return Status::InvalidArgument(
            "HOST_NUMA allocation length must be greater than zero");
    if (!api.mem_create) return missingDriverFunction("cuMemCreate");
    if (!api.mem_address_reserve)
        return missingDriverFunction("cuMemAddressReserve");
    if (!api.mem_map) return missingDriverFunction("cuMemMap");
    if (!api.mem_set_access) return missingDriverFunction("cuMemSetAccess");
    if (!api.mem_unmap) return missingDriverFunction("cuMemUnmap");
    if (!api.mem_address_free) return missingDriverFunction("cuMemAddressFree");
    if (!api.mem_release) return missingDriverFunction("cuMemRelease");

    std::vector<CUdevice> visible_devices;
    Status status =
        ValidateVisibleGpuFabricSupportWithDriverApi(api, visible_devices);
    if (!status.ok()) return status;

    size_t granularity = 0;
    status = GetAllocationGranularityWithDriverApi(numa_node, api, granularity);
    if (!status.ok()) return status;

    size_t alignment =
        required_va_alignment == 0 ? granularity : required_va_alignment;
    if (!isPowerOfTwo(alignment) || alignment < granularity ||
        alignment % granularity != 0) {
        return Status::InvalidArgument(
            "required VA alignment must be a power-of-two multiple of CUDA "
            "granularity");
    }
    const size_t remainder = requested_length % alignment;
    size_t length = requested_length;
    if (remainder != 0) {
        const size_t padding = alignment - remainder;
        if (length > std::numeric_limits<size_t>::max() - padding)
            return Status::InvalidArgument(
                "HOST_NUMA allocation length overflows during alignment");
        length += padding;
    }

    std::vector<CUmemAccessDesc> access_descs;
    try {
        access_descs.reserve(visible_devices.size() + 1);
    } catch (...) {
        return Status::Memory("failed to allocate CUDA access descriptors");
    }
    CUmemAccessDesc host_access = {};
    host_access.location.type = CU_MEM_LOCATION_TYPE_HOST_NUMA;
    host_access.location.id = numa_node;
    host_access.flags = CU_MEM_ACCESS_FLAGS_PROT_READWRITE;
    access_descs.push_back(host_access);
    for (CUdevice device : visible_devices) {
        CUmemAccessDesc device_access = {};
        device_access.location.type = CU_MEM_LOCATION_TYPE_DEVICE;
        device_access.location.id = device;
        device_access.flags = CU_MEM_ACCESS_FLAGS_PROT_READWRITE;
        access_descs.push_back(device_access);
    }

    auto owner = std::unique_ptr<NvlinkHostNumaAllocation>(
        new NvlinkHostNumaAllocation());
    owner->length_ = length;
    owner->numa_node_ = numa_node;
    owner->driver_api_ = api;

    auto rollback = [&](Status cause) {
        Status cleanup = owner->Release();
        if (!cleanup.ok()) {
            allocation = std::move(owner);
            return Status::Memory(cause.ToString() + "; rollback incomplete: " +
                                  cleanup.ToString());
        }
        return cause;
    };

    CUmemAllocationProp prop = hostNumaProp(numa_node);
    CUmemGenericAllocationHandle handle;
    CUresult result = api.mem_create(&handle, length, &prop, 0);
    if (result != CUDA_SUCCESS) return cudaFailure("cuMemCreate", result);
    owner->allocation_handle_ = static_cast<uint64_t>(handle);
    owner->handle_owned_ = true;

    CUdeviceptr ptr = 0;
    result = api.mem_address_reserve(&ptr, length, alignment, 0, 0);
    if (result != CUDA_SUCCESS)
        return rollback(cudaFailure("cuMemAddressReserve", result));
    owner->base_ = reinterpret_cast<void*>(ptr);
    owner->address_reserved_ = true;

    result = api.mem_map(ptr, length, 0, handle, 0);
    if (result != CUDA_SUCCESS)
        return rollback(cudaFailure("cuMemMap", result));
    owner->mapped_ = true;

    result = api.mem_set_access(ptr, length, access_descs.data(),
                                access_descs.size());
    if (result != CUDA_SUCCESS)
        return rollback(cudaFailure("cuMemSetAccess", result));

    if (!RegisterOwnedRange(owner->base_, owner->length_, api))
        return rollback(
            Status::Memory("failed to register HOST_NUMA owned range"));
    owner->owned_range_registered_ = true;
    allocation = std::move(owner);
    return Status::OK();
}

NvlinkHostNumaAllocation::~NvlinkHostNumaAllocation() {
    Status status = Release();
    if (!status.ok())
        LOG(ERROR) << "NvlinkHostNumaAllocation cleanup failed: " << status;
}

Status NvlinkHostNumaAllocation::Release() {
    if (handle_release_failed_)
        return Status::Memory(
            "a previous cuMemRelease attempt failed; ownership is uncertain");

    const CUdeviceptr ptr = reinterpret_cast<CUdeviceptr>(base_);
    if (owned_range_registered_) {
        if (!UnregisterOwnedRange(base_, length_))
            return Status::Memory(
                "HOST_NUMA owned-range provenance removal failed");
        owned_range_registered_ = false;
    }
    if (mapped_) {
        CUresult result = driver_api_.mem_unmap(ptr, length_);
        if (result != CUDA_SUCCESS) return cudaFailure("cuMemUnmap", result);
        mapped_ = false;
    }
    if (address_reserved_) {
        CUresult result = driver_api_.mem_address_free(ptr, length_);
        if (result != CUDA_SUCCESS)
            return cudaFailure("cuMemAddressFree", result);
        address_reserved_ = false;
        base_ = nullptr;
    }
    if (handle_owned_) {
        const auto handle =
            static_cast<CUmemGenericAllocationHandle>(allocation_handle_);
        handle_owned_ = false;
        allocation_handle_ = 0;
        CUresult result = driver_api_.mem_release(handle);
        if (result != CUDA_SUCCESS) {
            handle_release_failed_ = true;
            return cudaFailure("cuMemRelease", result);
        }
    }
    length_ = 0;
    numa_node_ = -1;
    return Status::OK();
}

}  // namespace mooncake

#endif  // USE_MNNVL && USE_CUDA && CUDA_VERSION >= 12030
