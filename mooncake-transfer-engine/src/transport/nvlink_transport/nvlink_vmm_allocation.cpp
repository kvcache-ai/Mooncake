// Copyright 2024 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "transport/nvlink_transport/nvlink_transport.h"

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <exception>
#include <limits>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <utility>

#include <glog/logging.h>

namespace mooncake {

#if defined(USE_MNNVL) && defined(USE_CUDA)
namespace {

struct OwnedVmmRange {
    size_t length = 0;
    size_t owners = 0;
};

struct OwnedVmmRangeRegistry {
    std::mutex mutex;
    std::unordered_map<uintptr_t, OwnedVmmRange> ranges;
};

OwnedVmmRangeRegistry& ownedVmmRangeRegistry() {
    // HOST_NUMA allocations can be released by Store's atexit handler after
    // ordinary function-local static objects have already been destroyed. Keep
    // this tiny process-lifetime registry alive so late cleanup never observes
    // a destructed mutex or map.
    static auto* registry = new OwnedVmmRangeRegistry();
    return *registry;
}

struct CleanupPendingVmmOwnerNode {
    std::unique_ptr<NvlinkVmmAllocation> owner;
    CleanupPendingVmmOwnerNode* next = nullptr;
};

struct CleanupPendingVmmOwnerRegistry {
    std::atomic_flag lock = ATOMIC_FLAG_INIT;
    CleanupPendingVmmOwnerNode* head = nullptr;
    size_t count = 0;
};

CleanupPendingVmmOwnerRegistry& cleanupPendingVmmOwnerRegistry() {
    // A factory rollback can outlive ordinary static destruction if CUDA keeps
    // rejecting a teardown stage. The intrusive nodes are preallocated before
    // CUDA ownership is acquired and intentionally live until a later retry or
    // process exit, so recording a failed cleanup cannot itself allocate.
    static auto* registry = new CleanupPendingVmmOwnerRegistry();
    return *registry;
}

class CleanupPendingRegistryGuard {
   public:
    explicit CleanupPendingRegistryGuard(CleanupPendingVmmOwnerRegistry& owner)
        : owner_(owner) {
        while (owner_.lock.test_and_set(std::memory_order_acquire)) {
        }
    }

    ~CleanupPendingRegistryGuard() {
        owner_.lock.clear(std::memory_order_release);
    }

   private:
    CleanupPendingVmmOwnerRegistry& owner_;
};

void quarantineCleanupPendingVmmOwner(
    std::unique_ptr<CleanupPendingVmmOwnerNode> node) noexcept {
    auto& registry = cleanupPendingVmmOwnerRegistry();
    CleanupPendingRegistryGuard guard(registry);
    node->next = registry.head;
    registry.head = node.release();
    ++registry.count;
}

size_t cleanupPendingVmmOwnerCount() noexcept {
    auto& registry = cleanupPendingVmmOwnerRegistry();
    CleanupPendingRegistryGuard guard(registry);
    return registry.count;
}

bool retryCleanupPendingVmmOwners() noexcept {
    auto& registry = cleanupPendingVmmOwnerRegistry();
    CleanupPendingVmmOwnerNode* pending = nullptr;
    {
        CleanupPendingRegistryGuard guard(registry);
        pending = registry.head;
        registry.head = nullptr;
        registry.count = 0;
    }

    bool all_released = true;
    while (pending != nullptr) {
        std::unique_ptr<CleanupPendingVmmOwnerNode> node(pending);
        pending = pending->next;
        node->next = nullptr;

        Status status;
        try {
            status = node->owner->Release();
        } catch (const std::exception& error) {
            status = Status::Memory(
                std::string("cleanup-pending VMM retry threw: ") +
                error.what());
        } catch (...) {
            status = Status::Memory(
                "cleanup-pending VMM retry threw an unknown exception");
        }
        if (!status.ok()) {
            LOG(ERROR) << "NvlinkVmmAllocation: cleanup-pending owner retry "
                          "failed; retaining CUDA ownership: "
                       << status;
            all_released = false;
            quarantineCleanupPendingVmmOwner(std::move(node));
        }
    }
    return all_released;
}

Status cudaDriverFailure(const char* stage, CUresult result) {
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

template <typename DriverApi>
Status buildAllocationProp(const NvlinkVmmAllocation::Options& options,
                           const DriverApi& api, CUmemAllocationProp& prop) {
    prop = {};
    prop.type = CU_MEM_ALLOCATION_TYPE_PINNED;
    prop.requestedHandleTypes = options.fabric_exportable
                                    ? CU_MEM_HANDLE_TYPE_FABRIC
                                    : static_cast<CUmemAllocationHandleType>(0);

    if (options.location_type == NvlinkVmmAllocation::LocationType::HOST_NUMA) {
        prop.location.type = CU_MEM_LOCATION_TYPE_HOST_NUMA;
        prop.location.id = options.location_id;
        return Status::OK();
    }

    if (!api.device_get) return missingDriverFunction("cuDeviceGet");
    if (!api.device_get_attribute)
        return missingDriverFunction("cuDeviceGetAttribute");

    CUdevice device;
    CUresult result = api.device_get(&device, options.location_id);
    if (result != CUDA_SUCCESS) return cudaDriverFailure("cuDeviceGet", result);

    prop.location.type = CU_MEM_LOCATION_TYPE_DEVICE;
    prop.location.id = device;

    int gpu_direct_rdma_supported = 0;
    result = api.device_get_attribute(
        &gpu_direct_rdma_supported,
        CU_DEVICE_ATTRIBUTE_GPU_DIRECT_RDMA_WITH_CUDA_VMM_SUPPORTED, device);
    if (result != CUDA_SUCCESS)
        return cudaDriverFailure("cuDeviceGetAttribute(GPU Direct RDMA)",
                                 result);
    if (gpu_direct_rdma_supported) prop.allocFlags.gpuDirectRDMACapable = 1;
    return Status::OK();
}

struct VmmAllocationOwnerRegistry {
    std::mutex mutex;
    std::unordered_map<void*, std::unique_ptr<NvlinkVmmAllocation>> owners;
};

VmmAllocationOwnerRegistry& vmmAllocationOwnerRegistry() {
    // Legacy pinned allocations may intentionally remain quarantined until
    // process exit after a CUDA cleanup failure. Avoid running their owners'
    // destructors against already-destroyed provenance state.
    static auto* registry = new VmmAllocationOwnerRegistry();
    return *registry;
}

}  // namespace

NvlinkVmmAllocation::DriverApi NvlinkVmmAllocation::ProductionDriverApi() {
    DriverApi api;
    api.device_get_count = [](int* count) { return cuDeviceGetCount(count); };
    api.device_get = [](CUdevice* device, int ordinal) {
        return cuDeviceGet(device, ordinal);
    };
    api.device_get_attribute = [](int* value, CUdevice_attribute attribute,
                                  CUdevice device) {
        return cuDeviceGetAttribute(value, attribute, device);
    };
    api.mem_get_allocation_granularity =
        [](size_t* granularity, const CUmemAllocationProp* prop,
           CUmemAllocationGranularity_flags flags) {
            return cuMemGetAllocationGranularity(granularity, prop, flags);
        };
    api.mem_create = [](CUmemGenericAllocationHandle* handle, size_t size,
                        const CUmemAllocationProp* prop,
                        unsigned long long flags) {
        return cuMemCreate(handle, size, prop, flags);
    };
    api.mem_address_reserve = [](CUdeviceptr* ptr, size_t size,
                                 size_t alignment, CUdeviceptr addr,
                                 unsigned long long flags) {
        return cuMemAddressReserve(ptr, size, alignment, addr, flags);
    };
    api.mem_map = [](CUdeviceptr ptr, size_t size, size_t offset,
                     CUmemGenericAllocationHandle handle,
                     unsigned long long flags) {
        return cuMemMap(ptr, size, offset, handle, flags);
    };
    api.mem_set_access = [](CUdeviceptr ptr, size_t size,
                            const CUmemAccessDesc* desc, size_t count) {
        return cuMemSetAccess(ptr, size, desc, count);
    };
    api.mem_unmap = [](CUdeviceptr ptr, size_t size) {
        return cuMemUnmap(ptr, size);
    };
    api.mem_address_free = [](CUdeviceptr ptr, size_t size) {
        return cuMemAddressFree(ptr, size);
    };
    api.mem_release = [](CUmemGenericAllocationHandle handle) {
        return cuMemRelease(handle);
    };
    api.mem_retain_allocation_handle = [](CUmemGenericAllocationHandle* handle,
                                          void* ptr) {
        return cuMemRetainAllocationHandle(handle, ptr);
    };
    api.mem_get_allocation_properties_from_handle =
        [](CUmemAllocationProp* prop, CUmemGenericAllocationHandle handle) {
            return cuMemGetAllocationPropertiesFromHandle(prop, handle);
        };
    api.mem_get_address_range = [](CUdeviceptr* base, size_t* size,
                                   CUdeviceptr ptr) {
        return cuMemGetAddressRange(base, size, ptr);
    };
    api.mem_export_to_shareable_handle =
        [](void* shareable_handle, CUmemGenericAllocationHandle handle,
           CUmemAllocationHandleType type, unsigned long long flags) {
            return cuMemExportToShareableHandle(shareable_handle, handle, type,
                                                flags);
        };
    api.mem_import_from_shareable_handle =
        [](CUmemGenericAllocationHandle* handle, void* shareable_handle,
           CUmemAllocationHandleType type) {
            return cuMemImportFromShareableHandle(handle, shareable_handle,
                                                  type);
        };
    return api;
}

bool NvlinkVmmAllocation::RegisterOwnedRange(void* base, size_t length) {
    if (base == nullptr || length == 0) return false;
    auto& registry = ownedVmmRangeRegistry();
    std::lock_guard<std::mutex> lock(registry.mutex);
    auto& ranges = registry.ranges;
    const uintptr_t address = reinterpret_cast<uintptr_t>(base);
    auto [it, inserted] = ranges.emplace(address, OwnedVmmRange{length, 0});
    if (!inserted && it->second.length != length) return false;
    ++it->second.owners;
    return true;
}

bool NvlinkVmmAllocation::UnregisterOwnedRange(void* base, size_t length) {
    if (base == nullptr || length == 0) return false;
    auto& registry = ownedVmmRangeRegistry();
    std::lock_guard<std::mutex> lock(registry.mutex);
    auto& ranges = registry.ranges;
    const auto it = ranges.find(reinterpret_cast<uintptr_t>(base));
    if (it == ranges.end() || it->second.length != length ||
        it->second.owners == 0) {
        LOG(ERROR) << "NvlinkVmmAllocation: owned range registry mismatch";
        return false;
    }
    if (--it->second.owners == 0) ranges.erase(it);
    return true;
}

bool NvlinkVmmAllocation::IsExactOwnedRange(void* base, size_t length) {
    if (base == nullptr || length == 0) return false;
    auto& registry = ownedVmmRangeRegistry();
    std::lock_guard<std::mutex> lock(registry.mutex);
    const auto& ranges = registry.ranges;
    const auto it = ranges.find(reinterpret_cast<uintptr_t>(base));
    return it != ranges.end() && it->second.length == length &&
           it->second.owners > 0;
}

NvlinkVmmAllocation::NvlinkVmmAllocation(NvlinkVmmAllocation&& other) noexcept
    : base_(other.base_),
      length_(other.length_),
      granularity_(other.granularity_),
      va_alignment_(other.va_alignment_),
      location_type_(other.location_type_),
      location_id_(other.location_id_),
      fabric_exportable_(other.fabric_exportable_),
      mapped_(other.mapped_),
      address_reserved_(other.address_reserved_),
      handle_owned_(other.handle_owned_),
      allocation_handle_(other.allocation_handle_),
      owned_range_registered_(other.owned_range_registered_),
      driver_api_(std::move(other.driver_api_)) {
    other.base_ = nullptr;
    other.length_ = 0;
    other.granularity_ = 0;
    other.va_alignment_ = 0;
    other.mapped_ = false;
    other.address_reserved_ = false;
    other.handle_owned_ = false;
    other.allocation_handle_ = 0;
    other.owned_range_registered_ = false;
}

NvlinkVmmAllocation::~NvlinkVmmAllocation() { reset(); }

Status NvlinkVmmAllocation::Release() {
    const CUdeviceptr ptr = reinterpret_cast<CUdeviceptr>(base_);

    // Cleanup-pending Fabric memory must stop satisfying provenance checks
    // before the first unmap attempt. Otherwise a failed unmap would leave a
    // range eligible for a new remote-accessible registration while teardown
    // is already in progress. This registry transition is idempotent and does
    // not need to be repeated by later release retries.
    if (owned_range_registered_) {
        if (!UnregisterOwnedRange(base_, length_)) {
            return Status::Memory(
                "HOST_NUMA VMM owned-range provenance removal failed");
        }
        owned_range_registered_ = false;
    }

    // These operations are deliberately serialized in reverse creation order.
    // A later stage must not run after an earlier stage fails: for example, a
    // still-mapped VA cannot safely be returned to the CUDA address allocator.
    if (mapped_) {
        if (!driver_api_.mem_unmap) return missingDriverFunction("cuMemUnmap");
        CUresult result = driver_api_.mem_unmap(ptr, length_);
        if (result != CUDA_SUCCESS)
            return cudaDriverFailure("cuMemUnmap", result);
        mapped_ = false;
    }

    if (address_reserved_) {
        if (!driver_api_.mem_address_free)
            return missingDriverFunction("cuMemAddressFree");
        CUresult result = driver_api_.mem_address_free(ptr, length_);
        if (result != CUDA_SUCCESS)
            return cudaDriverFailure("cuMemAddressFree", result);
        address_reserved_ = false;
        base_ = nullptr;
    }

    if (handle_owned_) {
        if (!driver_api_.mem_release)
            return missingDriverFunction("cuMemRelease");
        CUresult result = driver_api_.mem_release(
            static_cast<CUmemGenericAllocationHandle>(allocation_handle_));
        if (result != CUDA_SUCCESS)
            return cudaDriverFailure("cuMemRelease", result);
        handle_owned_ = false;
        allocation_handle_ = 0;
    }

    base_ = nullptr;
    length_ = 0;
    granularity_ = 0;
    va_alignment_ = 0;
    return Status::OK();
}

void NvlinkVmmAllocation::reset() noexcept {
    try {
        Status status = Release();
        if (!status.ok()) {
            LOG(ERROR) << "NvlinkVmmAllocation: best-effort cleanup retained "
                          "unreleased CUDA ownership: "
                       << status;
        }
    } catch (const std::exception& error) {
        LOG(ERROR) << "NvlinkVmmAllocation: best-effort cleanup threw: "
                   << error.what();
    } catch (...) {
        LOG(ERROR) << "NvlinkVmmAllocation: best-effort cleanup threw an "
                      "unknown exception";
    }
}

bool NvlinkVmmAllocation::RetryCleanupPendingOwners() {
    return retryCleanupPendingVmmOwners();
}

size_t NvlinkVmmAllocation::CleanupPendingOwnerCount() {
    return cleanupPendingVmmOwnerCount();
}

Status NvlinkVmmAllocation::CheckStrictFabricCapability() {
    return CheckStrictFabricCapabilityWithDriverApi(ProductionDriverApi());
}

Status NvlinkVmmAllocation::CheckStrictFabricCapabilityWithDriverApi(
    const DriverApi& api) {
    if (std::getenv("MC_USE_NVLINK_IPC") != nullptr) {
        return Status::NotSupportedTransport(
            "MC_USE_NVLINK_IPC disables Fabric VMM allocations");
    }
    if (!api.device_get_count) return missingDriverFunction("cuDeviceGetCount");
    if (!api.device_get) return missingDriverFunction("cuDeviceGet");
    if (!api.device_get_attribute)
        return missingDriverFunction("cuDeviceGetAttribute");

    int device_count = 0;
    CUresult result = api.device_get_count(&device_count);
    if (result != CUDA_SUCCESS)
        return cudaDriverFailure("cuDeviceGetCount", result);
    if (device_count <= 0)
        return Status::NotSupportedTransport(
            "Fabric VMM requires at least one visible CUDA device");

    for (int ordinal = 0; ordinal < device_count; ++ordinal) {
        CUdevice device;
        result = api.device_get(&device, ordinal);
        if (result != CUDA_SUCCESS)
            return cudaDriverFailure("cuDeviceGet", result);

        int fabric_supported = 0;
        result = api.device_get_attribute(
            &fabric_supported, CU_DEVICE_ATTRIBUTE_HANDLE_TYPE_FABRIC_SUPPORTED,
            device);
        if (result != CUDA_SUCCESS)
            return cudaDriverFailure(
                "cuDeviceGetAttribute(Fabric handle support)", result);
        if (!fabric_supported) {
            return Status::NotSupportedTransport(
                "visible CUDA device " + std::to_string(ordinal) +
                " does not support Fabric allocation handles");
        }
    }
    return Status::OK();
}

Status NvlinkVmmAllocation::GetAllocationGranularity(LocationType location_type,
                                                     int location_id,
                                                     bool fabric_exportable,
                                                     size_t& granularity) {
    return GetAllocationGranularityWithDriverApi(
        location_type, location_id, fabric_exportable, ProductionDriverApi(),
        granularity);
}

Status NvlinkVmmAllocation::GetAllocationGranularityWithDriverApi(
    LocationType location_type, int location_id, bool fabric_exportable,
    const DriverApi& api, size_t& granularity) {
    granularity = 0;
    if (location_id < 0)
        return Status::InvalidArgument("VMM location id must be non-negative");
    if (!api.mem_get_allocation_granularity)
        return missingDriverFunction("cuMemGetAllocationGranularity");

    Options options;
    options.location_type = location_type;
    options.location_id = location_id;
    options.fabric_exportable = fabric_exportable;
    CUmemAllocationProp prop = {};
    Status status = buildAllocationProp(options, api, prop);
    if (!status.ok()) return status;

    CUresult result = api.mem_get_allocation_granularity(
        &granularity, &prop, CU_MEM_ALLOC_GRANULARITY_MINIMUM);
    if (result != CUDA_SUCCESS)
        return cudaDriverFailure("cuMemGetAllocationGranularity", result);
    if (!isPowerOfTwo(granularity)) {
        granularity = 0;
        return Status::Memory(
            "CUDA VMM allocation granularity is zero or not a power of two");
    }
    return Status::OK();
}

Status NvlinkVmmAllocation::Create(
    const Options& options, std::unique_ptr<NvlinkVmmAllocation>& allocation) {
    return CreateWithDriverApi(options, ProductionDriverApi(), allocation);
}

Status NvlinkVmmAllocation::CreateWithDriverApi(
    const Options& options, const DriverApi& api,
    std::unique_ptr<NvlinkVmmAllocation>& allocation) {
    allocation.reset();
    if (!RetryCleanupPendingOwners()) {
        return Status::Memory(
            "a previous VMM factory rollback is still pending cleanup");
    }
    if (options.requested_length == 0)
        return Status::InvalidArgument(
            "VMM allocation length must be greater than zero");
    if (options.location_id < 0)
        return Status::InvalidArgument("VMM location id must be non-negative");
    if (!api.mem_create) return missingDriverFunction("cuMemCreate");
    if (!api.mem_address_reserve)
        return missingDriverFunction("cuMemAddressReserve");
    if (!api.mem_map) return missingDriverFunction("cuMemMap");
    if (!api.mem_set_access) return missingDriverFunction("cuMemSetAccess");
    if (!api.mem_unmap) return missingDriverFunction("cuMemUnmap");
    if (!api.mem_address_free) return missingDriverFunction("cuMemAddressFree");
    if (!api.mem_release) return missingDriverFunction("cuMemRelease");
    if (!api.device_get_count) return missingDriverFunction("cuDeviceGetCount");
    if (!api.device_get) return missingDriverFunction("cuDeviceGet");

    if (options.fabric_exportable) {
        Status status = CheckStrictFabricCapabilityWithDriverApi(api);
        if (!status.ok()) return status;
    }

    size_t granularity = 0;
    Status status = GetAllocationGranularityWithDriverApi(
        options.location_type, options.location_id, options.fabric_exportable,
        api, granularity);
    if (!status.ok()) return status;

    size_t va_alignment = granularity;
    if (options.required_va_alignment != 0) {
        if (!isPowerOfTwo(options.required_va_alignment) ||
            options.required_va_alignment < granularity ||
            options.required_va_alignment % granularity != 0) {
            return Status::InvalidArgument(
                "required VA alignment must be a power-of-two multiple of "
                "CUDA allocation granularity");
        }
        va_alignment = options.required_va_alignment;
    }

    const size_t remainder = options.requested_length % va_alignment;
    size_t length = options.requested_length;
    if (remainder != 0) {
        const size_t padding = va_alignment - remainder;
        if (length > std::numeric_limits<size_t>::max() - padding)
            return Status::InvalidArgument(
                "VMM allocation length overflows during alignment");
        length += padding;
    }

    CUmemAllocationProp prop = {};
    status = buildAllocationProp(options, api, prop);
    if (!status.ok()) return status;

    std::unique_ptr<CleanupPendingVmmOwnerNode> cleanup_node;
    std::unique_ptr<NvlinkVmmAllocation> owner;
    try {
        // Preallocate the intrusive quarantine node before acquiring any CUDA
        // resource. Moving this node into the process-lifetime registry cannot
        // fail even under memory pressure during rollback.
        cleanup_node = std::make_unique<CleanupPendingVmmOwnerNode>();
        owner = std::unique_ptr<NvlinkVmmAllocation>(new NvlinkVmmAllocation());
    } catch (const std::exception& error) {
        return Status::Memory(std::string("VMM owner allocation failed: ") +
                              error.what());
    }
    owner->length_ = length;
    owner->granularity_ = granularity;
    owner->va_alignment_ = va_alignment;
    owner->location_type_ = options.location_type;
    owner->location_id_ = options.location_id;
    owner->fabric_exportable_ = options.fabric_exportable;
    owner->driver_api_ = api;

    auto rollback = [&](Status cause) -> Status {
        Status cleanup;
        try {
            cleanup = owner->Release();
        } catch (const std::exception& error) {
            cleanup = Status::Memory(std::string("VMM rollback threw: ") +
                                     error.what());
        } catch (...) {
            cleanup = Status::Memory("VMM rollback threw an unknown exception");
        }
        if (!cleanup.ok()) {
            LOG(ERROR) << "NvlinkVmmAllocation: factory rollback failed; "
                          "quarantining complete CUDA ownership for retry: "
                       << cleanup;
            cleanup_node->owner = std::move(owner);
            quarantineCleanupPendingVmmOwner(std::move(cleanup_node));
        }
        return cause;
    };

    CUmemGenericAllocationHandle handle;
    CUresult result = api.mem_create(&handle, length, &prop, 0);
    if (result != CUDA_SUCCESS) return cudaDriverFailure("cuMemCreate", result);
    owner->allocation_handle_ = static_cast<uint64_t>(handle);
    owner->handle_owned_ = true;

    CUdeviceptr ptr = 0;
    result = api.mem_address_reserve(&ptr, length, va_alignment, 0, 0);
    if (result != CUDA_SUCCESS) {
        status = cudaDriverFailure("cuMemAddressReserve", result);
        return rollback(status);
    }
    owner->base_ = reinterpret_cast<void*>(ptr);
    owner->address_reserved_ = true;

    result = api.mem_map(ptr, length, 0, handle, 0);
    if (result != CUDA_SUCCESS) {
        status = cudaDriverFailure("cuMemMap", result);
        return rollback(status);
    }
    owner->mapped_ = true;

    auto grant_access = [&](CUmemLocationType location_type,
                            int location_id) -> Status {
        CUmemAccessDesc access = {};
        access.location.type = location_type;
        access.location.id = location_id;
        access.flags = CU_MEM_ACCESS_FLAGS_PROT_READWRITE;
        CUresult access_result = api.mem_set_access(ptr, length, &access, 1);
        if (access_result != CUDA_SUCCESS)
            return cudaDriverFailure("cuMemSetAccess", access_result);
        return Status::OK();
    };

    if (options.location_type == LocationType::HOST_NUMA) {
        status =
            grant_access(CU_MEM_LOCATION_TYPE_HOST_NUMA, options.location_id);
        if (!status.ok()) {
            return rollback(status);
        }
    }

    int device_count = 0;
    result = api.device_get_count(&device_count);
    if (result != CUDA_SUCCESS) {
        status = cudaDriverFailure("cuDeviceGetCount", result);
        return rollback(status);
    }
    if (device_count <= 0) {
        return rollback(Status::NotSupportedTransport(
            "VMM allocation requires at least one visible CUDA device"));
    }

    for (int ordinal = 0; ordinal < device_count; ++ordinal) {
        CUdevice device;
        result = api.device_get(&device, ordinal);
        if (result != CUDA_SUCCESS) {
            status = cudaDriverFailure("cuDeviceGet", result);
            return rollback(status);
        }
        status = grant_access(CU_MEM_LOCATION_TYPE_DEVICE, device);
        if (!status.ok()) {
            return rollback(status);
        }
    }

    result = api.mem_release(handle);
    if (result != CUDA_SUCCESS) {
        status = cudaDriverFailure("cuMemRelease", result);
        return rollback(status);
    }
    owner->handle_owned_ = false;
    owner->allocation_handle_ = 0;
    if (options.location_type == LocationType::HOST_NUMA &&
        options.fabric_exportable) {
        if (!RegisterOwnedRange(owner->base_, owner->length_)) {
            return rollback(Status::Memory(
                "HOST_NUMA VMM owned-range provenance registration failed"));
        }
        owner->owned_range_registered_ = true;
    }
    allocation = std::move(owner);
    return Status::OK();
}

bool NvlinkTransport::trackPinnedVmmAllocation(
    std::unique_ptr<NvlinkVmmAllocation> owner) {
    if (!owner || owner->base() == nullptr) return false;
    void* const ptr = owner->base();
    auto& registry = vmmAllocationOwnerRegistry();
    std::lock_guard<std::mutex> lock(registry.mutex);
    return registry.owners.emplace(ptr, std::move(owner)).second;
}

bool NvlinkTransport::releasePinnedVmmAllocation(void* ptr) {
    auto& registry = vmmAllocationOwnerRegistry();
    std::lock_guard<std::mutex> lock(registry.mutex);
    auto it = registry.owners.find(ptr);
    if (it == registry.owners.end()) return false;

    try {
        Status status = it->second->Release();
        if (!status.ok()) {
            LOG(ERROR) << "NvlinkTransport: pinned VMM release failed; "
                          "retaining owner for repeated free: "
                       << status;
            return true;
        }
    } catch (const std::exception& error) {
        LOG(ERROR) << "NvlinkTransport: pinned VMM release threw; retaining "
                      "owner for repeated free: "
                   << error.what();
        return true;
    } catch (...) {
        LOG(ERROR) << "NvlinkTransport: pinned VMM release threw an unknown "
                      "exception; retaining owner for repeated free";
        return true;
    }

    registry.owners.erase(it);
    return true;
}

#endif

void* NvlinkTransport::allocatePinnedLocalMemory(size_t size) {
#if defined(USE_MNNVL) && defined(USE_CUDA)
    if (!supportsFabricMemory()) {
        void* ptr = nullptr;
        cudaMalloc(&ptr, size);
        return ptr;
    }

    int cudaDev;
    cudaError_t err = cudaGetDevice(&cudaDev);
    if (err != cudaSuccess) {
        LOG(ERROR) << "NvlinkTransport: cudaGetDevice failed: "
                   << cudaGetErrorString(err);
        return nullptr;
    }

    NvlinkVmmAllocation::Options options;
    options.location_type = NvlinkVmmAllocation::LocationType::DEVICE;
    options.location_id = cudaDev;
    options.requested_length = size;
    options.fabric_exportable = true;

    std::unique_ptr<NvlinkVmmAllocation> owner;
    Status status = NvlinkVmmAllocation::Create(options, owner);
    if (!status.ok()) {
        LOG(ERROR) << "NvlinkTransport: DEVICE Fabric VMM allocation failed: "
                   << status.ToString();
        return nullptr;
    }

    void* ptr = owner->base();
    if (!trackPinnedVmmAllocation(std::move(owner))) {
        LOG(ERROR) << "NvlinkTransport: duplicate VMM allocation address "
                   << ptr;
        return nullptr;
    }
    return ptr;
#else
    void* ptr = nullptr;
    cudaMalloc(&ptr, size);
    return ptr;
#endif
}

void NvlinkTransport::freePinnedLocalMemory(void* ptr) {
#if defined(USE_MNNVL) && defined(USE_CUDA)
    if (ptr == nullptr) return;

    if (releasePinnedVmmAllocation(ptr)) return;

    // Preserve compatibility with imported Fabric mappings until their
    // ownership moves to the registration/import RAII objects. Such mappings
    // are not present in the legacy allocation-owner map.
    CUmemGenericAllocationHandle handle;
    size_t size = 0;
    auto result = cuMemRetainAllocationHandle(&handle, ptr);
    if (result != CUDA_SUCCESS) {
        cudaGetLastError();
        cudaFree(ptr);
        return;
    }
    CUdeviceptr base = 0;
    result =
        cuMemGetAddressRange(&base, &size, reinterpret_cast<CUdeviceptr>(ptr));
    if (result == CUDA_SUCCESS) {
        cuMemUnmap(base, size);
        cuMemAddressFree(base, size);
    } else {
        LOG(ERROR) << "NvlinkTransport: cuMemGetAddressRange failed: "
                   << result;
    }
    cuMemRelease(handle);
#else
    cudaFree(ptr);
#endif
}

}  // namespace mooncake
