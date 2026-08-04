// Copyright 2026 Huawei Technologies Co., Ltd
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

#ifdef USE_CUDA

#include <glog/logging.h>

#include <array>
#include <cstring>

#include "cuda_alike.h"
#include "shared_segment_internal.h"

namespace mooncake {
namespace {
constexpr uint16_t kCudaBackendId = 2;
constexpr uint64_t kFallbackGranularity = 2ULL * 1024 * 1024;

static_assert(sizeof(CUmemFabricHandle) <= kMaxHandleBytes,
              "Fabric handle must fit into a shared segment blob");

std::string DriverError(const char* api, CUresult result) {
    const char* text = nullptr;
    cuGetErrorString(result, &text);
    return std::string(api) +
           " failed: " + (text != nullptr ? text : "unknown error");
}

int32_t QueryDeviceAttribute(CUdevice_attribute attribute, int32_t device_id,
                             const char* what) {
    CUdevice device{};
    auto result = cuDeviceGet(&device, device_id);
    if (result != CUDA_SUCCESS) {
        LOG(WARNING) << "Shared segment cannot read " << what << ": "
                     << DriverError("cuDeviceGet", result);
        return -1;
    }
    int32_t value = 0;
    result = cuDeviceGetAttribute(&value, attribute, device);
    if (result != CUDA_SUCCESS) {
        LOG(WARNING) << "Shared segment cannot read " << what << ": "
                     << DriverError("cuDeviceGetAttribute", result);
        return -1;
    }
    return value;
}

// Host allocations are pinned to the NUMA node the GPU hangs off, so a D2H
// write and the later reads stay on the near memory controller.
int32_t HostNumaNodeOf(int32_t device_id) {
    const auto numa_id = QueryDeviceAttribute(CU_DEVICE_ATTRIBUTE_HOST_NUMA_ID,
                                              device_id, "the host NUMA node");
    return numa_id < 0 ? 0 : numa_id;
}

// Fabric handles are the only handle type that survives a plain byte exchange:
// a POSIX file descriptor would additionally need an out-of-band SCM_RIGHTS
// channel between the ranks.
CUmemAllocationProp BuildHostAllocationProp(
    const SharedSegmentOptions& options) {
    CUmemAllocationProp prop = {};
    prop.type = CU_MEM_ALLOCATION_TYPE_PINNED;
    prop.requestedHandleTypes = CU_MEM_HANDLE_TYPE_FABRIC;
    prop.location.type = CU_MEM_LOCATION_TYPE_HOST_NUMA;
    prop.location.id = HostNumaNodeOf(options.device_id);
    return prop;
}

// Host memory has to be granted to the CPU and to the local GPU separately.
Status GrantAccess(CUdeviceptr ptr, uint64_t size,
                   const SharedSegmentOptions& options) {
    std::array<CUmemAccessDesc, 2> descs{};
    descs[0].location.type = CU_MEM_LOCATION_TYPE_HOST;
    descs[0].location.id = 0;
    descs[0].flags = CU_MEM_ACCESS_FLAGS_PROT_READWRITE;
    descs[1].location.type = CU_MEM_LOCATION_TYPE_DEVICE;
    descs[1].location.id = options.device_id;
    descs[1].flags = CU_MEM_ACCESS_FLAGS_PROT_READWRITE;

    auto result = cuMemSetAccess(ptr, size, descs.data(), descs.size());
    if (result != CUDA_SUCCESS) {
        return Status::Memory(DriverError("cuMemSetAccess", result));
    }
    return Status::OK();
}

class CudaSharedSegmentBackend : public SharedSegmentBackend {
   public:
    ~CudaSharedSegmentBackend() override { Release(); }

    uint64_t Granularity(const SharedSegmentOptions& options) const override;

    Status CreateOwner(uint64_t size, const SharedSegmentOptions& options,
                       uintptr_t& base_addr,
                       std::vector<uint8_t>& handle) override;

    Status ReserveLocal(uint64_t size, const SharedSegmentOptions& options,
                        uintptr_t& base_addr) override;

    Status ImportAndMap(uint64_t size, const SharedSegmentOptions& options,
                        const std::vector<uint8_t>& handle) override;

    uint16_t BackendId() const override { return kCudaBackendId; }

   private:
    Status MapAndGrant(CUdeviceptr ptr, uint64_t size,
                       const SharedSegmentOptions& options);
    void Release();

    CUdeviceptr reserved_ptr_ = 0;
    uint64_t reserved_size_ = 0;
    CUmemGenericAllocationHandle allocation_ = 0;
    bool mapped_ = false;
};

uint64_t CudaSharedSegmentBackend::Granularity(
    const SharedSegmentOptions& options) const {
    // cuMemCreate rejects a size that is not a multiple of this, so it has to
    // be queried for the very property the allocation will use.
    auto prop = BuildHostAllocationProp(options);
    size_t granularity = 0;
    auto result = cuMemGetAllocationGranularity(
        &granularity, &prop, CU_MEM_ALLOC_GRANULARITY_MINIMUM);
    if (result != CUDA_SUCCESS || granularity == 0) {
        return kFallbackGranularity;
    }
    return granularity;
}

Status CudaSharedSegmentBackend::MapAndGrant(
    CUdeviceptr ptr, uint64_t size, const SharedSegmentOptions& options) {
    auto result = cuMemMap(ptr, size, 0, allocation_, 0);
    if (result != CUDA_SUCCESS) {
        return Status::Memory(DriverError("cuMemMap", result));
    }
    mapped_ = true;
    return GrantAccess(ptr, size, options);
}

Status CudaSharedSegmentBackend::CreateOwner(
    uint64_t size, const SharedSegmentOptions& options, uintptr_t& base_addr,
    std::vector<uint8_t>& handle) {
    auto prop = BuildHostAllocationProp(options);
    auto result = cuMemCreate(&allocation_, size, &prop, 0);
    if (result != CUDA_SUCCESS) {
        allocation_ = 0;
        return Status::Memory(DriverError("cuMemCreate", result));
    }
    auto status = ReserveLocal(size, options, base_addr);
    if (!status.ok()) {
        return status;
    }
    status = MapAndGrant(reserved_ptr_, size, options);
    if (!status.ok()) {
        return status;
    }

    CUmemFabricHandle exported{};
    result = cuMemExportToShareableHandle(&exported, allocation_,
                                          CU_MEM_HANDLE_TYPE_FABRIC, 0);
    if (result != CUDA_SUCCESS) {
        return Status::Memory(
            DriverError("cuMemExportToShareableHandle", result) +
            "; sharing a segment across processes needs fabric handles, which "
            "require an IMEX channel");
    }
    const auto* bytes = reinterpret_cast<const uint8_t*>(&exported);
    handle.assign(bytes, bytes + sizeof(exported));
    return Status::OK();
}

Status CudaSharedSegmentBackend::ReserveLocal(
    uint64_t size, const SharedSegmentOptions& options, uintptr_t& base_addr) {
    auto result =
        cuMemAddressReserve(&reserved_ptr_, size, Granularity(options), 0, 0);
    if (result != CUDA_SUCCESS) {
        reserved_ptr_ = 0;
        return Status::Memory(DriverError("cuMemAddressReserve", result));
    }
    reserved_size_ = size;
    base_addr = static_cast<uintptr_t>(reserved_ptr_);
    return Status::OK();
}

Status CudaSharedSegmentBackend::ImportAndMap(
    uint64_t size, const SharedSegmentOptions& options,
    const std::vector<uint8_t>& handle) {
    if (reserved_ptr_ == 0) {
        return Status::InvalidArgument(
            "Shared segment import needs a reserved address window");
    }
    if (handle.size() != sizeof(CUmemFabricHandle)) {
        return Status::InvalidArgument(
            "Shared segment owner handle has an unexpected length");
    }
    CUmemFabricHandle imported{};
    memcpy(&imported, handle.data(), handle.size());
    auto result = cuMemImportFromShareableHandle(&allocation_, &imported,
                                                 CU_MEM_HANDLE_TYPE_FABRIC);
    if (result != CUDA_SUCCESS) {
        allocation_ = 0;
        return Status::Memory(
            DriverError("cuMemImportFromShareableHandle", result));
    }
    return MapAndGrant(reserved_ptr_, size, options);
}

void CudaSharedSegmentBackend::Release() {
    if (mapped_) {
        (void)cuMemUnmap(reserved_ptr_, reserved_size_);
        mapped_ = false;
    }
    if (allocation_ != 0) {
        (void)cuMemRelease(allocation_);
        allocation_ = 0;
    }
    if (reserved_ptr_ != 0) {
        (void)cuMemAddressFree(reserved_ptr_, reserved_size_);
        reserved_ptr_ = 0;
    }
    reserved_size_ = 0;
}
}  // namespace

std::unique_ptr<SharedSegmentBackend> CreateCudaSharedSegmentBackend() {
    // Without fabric handle support there is no way to hand a peer process a
    // handle it can import, so report the segment as unsupported instead of
    // failing later inside cuMemCreate.
    if (QueryDeviceAttribute(CU_DEVICE_ATTRIBUTE_HANDLE_TYPE_FABRIC_SUPPORTED,
                             0, "fabric handle support") <= 0) {
        // Callers probe support before every attempt, so say this only once.
        LOG_FIRST_N(WARNING, 1)
            << "Shared segments need fabric handle support, which requires an "
               "IMEX channel on this system";
        return nullptr;
    }
    return std::make_unique<CudaSharedSegmentBackend>();
}

}  // namespace mooncake

#endif  // USE_CUDA
