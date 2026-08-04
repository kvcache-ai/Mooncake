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

#ifdef USE_ASCEND_DIRECT

#include <acl/acl.h>
#include <glog/logging.h>

#include <cstring>

#include "shared_segment_internal.h"
#include "transport/ascend_transport/ascend_direct_transport/adxl_compat.h"

namespace mooncake {
namespace {
constexpr uint16_t kAscendBackendId = 1;
// Used when the runtime cannot report a granularity; matches the smallest huge
// page ACL falls back to for host allocations.
constexpr uint64_t kFallbackGranularity = 2ULL * 1024 * 1024;
constexpr size_t kMemAccessDescCount = 1;
// The owner's pages are huge pages, so a peer's reservation has to be huge-page
// backed too.
constexpr uint64_t kReserveHugePageFlag = 1;

static_assert(sizeof(adxl::MemFabricHandle) == sizeof(aclrtMemFabricHandle),
              "adxl::MemFabricHandle must match aclrtMemFabricHandle");
static_assert(alignof(adxl::MemFabricHandle) == alignof(aclrtMemFabricHandle),
              "adxl::MemFabricHandle must match aclrtMemFabricHandle");
static_assert(sizeof(aclrtMemFabricHandle) <= kMaxHandleBytes,
              "Fabric handle must fit into a shared segment blob");

// The symbols are weak, so an older adxl shared object resolves them to null
// instead of failing to load; the capability flag then tells us whether
// MallocMem of that runtime really exports.
bool IsAdxlExportedHandleAvailable() {
    if (&adxl::AdxlEngine::MallocMem == nullptr ||
        &adxl::AdxlEngine::FreeMem == nullptr ||
        &adxl::AdxlEngine::GetExportedHandle == nullptr) {
        return false;
    }
    return adxl::IsAdxlFeatureSupported(adxl::MALLOC_MEM_EXPORTED_HANDLE);
}

aclrtPhysicalMemProp BuildHostMemProp() {
    aclrtPhysicalMemProp prop = {};
    prop.handleType = ACL_MEM_HANDLE_TYPE_NONE;
    prop.allocationType = ACL_MEM_ALLOCATION_TYPE_PINNED;
    prop.reserve = 0;
    prop.memAttr = ACL_MEM_P2P_HUGE;
    prop.location.type = ACL_MEM_LOCATION_TYPE_HOST;
    prop.location.id = 0;
    return prop;
}

// Host pages are only bound to the host by Map; the NPU needs an explicit grant
// before kernels can touch them. AdxlEngine::MallocMem already does this for
// the owner.
Status GrantDeviceAccess(void* addr, uint64_t size, int32_t device_id) {
    aclrtMemAccessDesc desc{};
    desc.flags = ACL_RT_MEM_ACCESS_FLAGS_READWRITE;
    desc.location.type = ACL_MEM_LOCATION_TYPE_DEVICE;
    desc.location.id = static_cast<uint32_t>(device_id);
    auto ret = aclrtMemSetAccess(addr, size, &desc, kMemAccessDescCount);
    if (ret != ACL_ERROR_NONE) {
        return Status::Memory(
            "aclrtMemSetAccess failed for the imported shared segment, ret " +
            std::to_string(ret));
    }
    return Status::OK();
}

class AscendSharedSegmentBackend : public SharedSegmentBackend {
   public:
    ~AscendSharedSegmentBackend() override { Release(); }

    uint64_t Granularity(const SharedSegmentOptions& options) const override;

    Status CreateOwner(uint64_t size, const SharedSegmentOptions& options,
                       uintptr_t& base_addr,
                       std::vector<uint8_t>& handle) override;

    Status ReserveLocal(uint64_t size, const SharedSegmentOptions& options,
                        uintptr_t& base_addr) override;

    Status ImportAndMap(uint64_t size, const SharedSegmentOptions& options,
                        const std::vector<uint8_t>& handle) override;

    uint16_t BackendId() const override { return kAscendBackendId; }

   private:
    void Release();

    void* owner_addr_ = nullptr;
    void* reserved_addr_ = nullptr;
    aclrtDrvMemHandle imported_handle_ = nullptr;
    bool mapped_ = false;
};

uint64_t AscendSharedSegmentBackend::Granularity(
    const SharedSegmentOptions&) const {
    // adxl may still fall back to smaller pages than the property asks for;
    // that only wastes a little address space, whereas an allocation size below
    // the granularity is rejected outright.
    auto prop = BuildHostMemProp();
    size_t granularity = 0;
    auto ret = aclrtMemGetAllocationGranularity(
        &prop, ACL_RT_MEM_ALLOC_GRANULARITY_MINIMUM, &granularity);
    if (ret != ACL_ERROR_NONE || granularity == 0) {
        return kFallbackGranularity;
    }
    return granularity;
}

Status AscendSharedSegmentBackend::CreateOwner(
    uint64_t size, const SharedSegmentOptions& options, uintptr_t& base_addr,
    std::vector<uint8_t>& handle) {
    // MallocMem takes the device from the calling thread rather than an
    // argument, so a mismatch would hand peers memory pinned to the wrong NUMA
    // node and granted to the wrong device.
    int32_t current_device = -1;
    auto ret = aclrtGetDevice(&current_device);
    if (ret != ACL_ERROR_NONE || current_device != options.device_id) {
        return Status::InvalidArgument(
            "Shared segment owner must run on device " +
            std::to_string(options.device_id) + ", but the current device is " +
            std::to_string(current_device));
    }
    void* addr = nullptr;
    auto status = adxl::AdxlEngine::MallocMem(adxl::MEM_HOST, size, &addr);
    if (status != adxl::SUCCESS) {
        return Status::Memory(
            "AdxlEngine::MallocMem failed for a shared segment of " +
            std::to_string(size) + " bytes, status " + std::to_string(status));
    }
    adxl::MemFabricHandle exported{};
    status = adxl::AdxlEngine::GetExportedHandle(addr, exported);
    if (status != adxl::SUCCESS) {
        (void)adxl::AdxlEngine::FreeMem(addr);
        return Status::Memory(
            "AdxlEngine::GetExportedHandle failed for the shared segment, "
            "status " +
            std::to_string(status));
    }

    owner_addr_ = addr;
    base_addr = reinterpret_cast<uintptr_t>(addr);
    handle.assign(exported.data, exported.data + sizeof(exported.data));
    return Status::OK();
}

Status AscendSharedSegmentBackend::ReserveLocal(
    uint64_t size, const SharedSegmentOptions& options, uintptr_t& base_addr) {
    void* addr = nullptr;
    auto ret = aclrtReserveMemAddress(&addr, size, Granularity(options),
                                      nullptr, kReserveHugePageFlag);
    if (ret != ACL_ERROR_NONE) {
        return Status::Memory("aclrtReserveMemAddress failed for " +
                              std::to_string(size) + " bytes, ret " +
                              std::to_string(ret));
    }
    reserved_addr_ = addr;
    base_addr = reinterpret_cast<uintptr_t>(addr);
    return Status::OK();
}

Status AscendSharedSegmentBackend::ImportAndMap(
    uint64_t size, const SharedSegmentOptions& options,
    const std::vector<uint8_t>& handle) {
    if (reserved_addr_ == nullptr) {
        return Status::InvalidArgument(
            "Shared segment import needs a reserved address window");
    }
    if (handle.size() != sizeof(aclrtMemFabricHandle)) {
        return Status::InvalidArgument(
            "Shared segment owner handle has an unexpected length");
    }
    aclrtMemFabricHandle share_handle{};
    memcpy(share_handle.data, handle.data(), handle.size());
    auto ret = aclrtMemImportFromShareableHandleV2(
        &share_handle, ACL_MEM_SHARE_HANDLE_TYPE_FABRIC, 0, &imported_handle_);
    if (ret != ACL_ERROR_NONE) {
        return Status::Memory(
            "aclrtMemImportFromShareableHandleV2 failed for the shared "
            "segment, ret " +
            std::to_string(ret));
    }
    auto* addr = reserved_addr_;
    ret = aclrtMapMem(addr, size, 0, imported_handle_, 0);
    if (ret != ACL_ERROR_NONE) {
        return Status::Memory(
            "aclrtMapMem failed for the shared segment, ret " +
            std::to_string(ret));
    }
    mapped_ = true;
    return GrantDeviceAccess(addr, size, options.device_id);
}

void AscendSharedSegmentBackend::Release() {
    if (owner_addr_ != nullptr) {
        (void)adxl::AdxlEngine::FreeMem(owner_addr_);
        owner_addr_ = nullptr;
    }
    if (mapped_) {
        (void)aclrtUnmapMem(reserved_addr_);
        mapped_ = false;
    }
    if (imported_handle_ != nullptr) {
        (void)aclrtFreePhysical(imported_handle_);
        imported_handle_ = nullptr;
    }
    if (reserved_addr_ != nullptr) {
        (void)aclrtReleaseMemAddress(reserved_addr_);
        reserved_addr_ = nullptr;
    }
}
}  // namespace

std::unique_ptr<SharedSegmentBackend> CreateAscendSharedSegmentBackend() {
    if (!IsAdxlExportedHandleAvailable()) {
        // Callers probe support before every attempt, so say this only once.
        LOG_FIRST_N(WARNING, 1)
            << "Shared segments need an adxl runtime that exports fabric "
               "handles from MallocMem";
        return nullptr;
    }
    return std::make_unique<AscendSharedSegmentBackend>();
}

}  // namespace mooncake

#endif  // USE_ASCEND_DIRECT
