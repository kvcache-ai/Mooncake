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
constexpr size_t kBytesPerTB = 1024ULL * 1024ULL * 1024ULL * 1024ULL;
// HIXL fabric mem defaults to [40TB, 72TB); reserve after that window on A3.
constexpr uintptr_t kA3ReserveStartAddr = 72ULL * kBytesPerTB;

static_assert(sizeof(aclrtMemFabricHandle) <= kMaxHandleBytes,
              "Fabric handle must fit into a shared segment blob");

// Older AscendCL headers omit this symbol; a weak stub keeps the binary
// loadable when the runtime has not shipped NoUCMemory yet.
extern "C" __attribute__((weak)) aclError
aclrtReserveMemAddressNoUCMemory(void** virPtr, size_t size, size_t alignment,
                                 void* expectPtr, uint64_t flags);

// Weak symbols resolve to null on older adxl shared objects instead of failing
// to load; presence of ExportToShareableHandle gates the Ascend shared-segment
// path.
bool IsAdxlExportShareableHandleAvailable() {
    return &adxl::AdxlEngine::ExportToShareableHandle != nullptr;
}

// Matches HIXL's A3 (V3) SoC set used for NoUCMemory reservations.
bool IsA3Soc() {
    const char* soc_name = aclrtGetSocName();
    if (soc_name == nullptr || soc_name[0] == '\0') {
        return false;
    }
    static constexpr const char* kA3SocNames[] = {
        "Ascend910_9391", "Ascend910_9381", "Ascend910_9392",
        "Ascend910_9382", "Ascend910_9372", "Ascend910_9362"};
    for (const char* name : kA3SocNames) {
        if (std::strcmp(soc_name, name) == 0) {
            return true;
        }
    }
    return false;
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

    // After MemSetAccess the host SVM VA is device-accessible; expose it so
    // Python can wrap NPU tensors (owner via MallocMem, peer via ImportAndMap).
    uintptr_t DeviceAddr() const override {
        if (owner_addr_ != nullptr) {
            return reinterpret_cast<uintptr_t>(owner_addr_);
        }
        if (mapped_ && reserved_addr_ != nullptr) {
            return reinterpret_cast<uintptr_t>(reserved_addr_);
        }
        return 0;
    }

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
    adxl::ShareableHandle exported{};
    status = adxl::AdxlEngine::ExportToShareableHandle(addr, exported);
    if (status != adxl::SUCCESS) {
        (void)adxl::AdxlEngine::FreeMem(addr);
        return Status::Memory(
            "AdxlEngine::ExportToShareableHandle failed for the shared "
            "segment, status " +
            std::to_string(status));
    }

    owner_addr_ = addr;
    base_addr = reinterpret_cast<uintptr_t>(addr);
    handle.assign(exported.data, exported.data + sizeof(exported.data));
    return Status::OK();
}

Status AscendSharedSegmentBackend::ReserveLocal(
    uint64_t size, const SharedSegmentOptions& options, uintptr_t& base_addr) {
    (void)options;
    void* addr = nullptr;
    // Same policy as HIXL VirtualMemoryManager::ReserveMemAddress: on A3 try
    // NoUCMemory at a fixed start VA first, then fall back when unsupported.
    if (IsA3Soc() && &aclrtReserveMemAddressNoUCMemory != nullptr) {
        void* start_va = reinterpret_cast<void*>(kA3ReserveStartAddr);
        auto ret = aclrtReserveMemAddressNoUCMemory(&addr, size, 0, start_va,
                                                    kReserveHugePageFlag);
        if (ret == ACL_ERROR_NONE) {
            reserved_addr_ = addr;
            base_addr = reinterpret_cast<uintptr_t>(addr);
            return Status::OK();
        }
        if (ret != ACL_ERROR_RT_FEATURE_NOT_SUPPORT) {
            return Status::Memory(
                "aclrtReserveMemAddressNoUCMemory failed for " +
                std::to_string(size) + " bytes, ret " + std::to_string(ret));
        }
    }
    auto ret =
        aclrtReserveMemAddress(&addr, size, 0, nullptr, kReserveHugePageFlag);
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
    if (!IsAdxlExportShareableHandleAvailable()) {
        // Callers probe support before every attempt, so say this only once.
        LOG_FIRST_N(WARNING, 1)
            << "Shared segments need an adxl runtime that supports "
               "ExportToShareableHandle";
        return nullptr;
    }
    return std::make_unique<AscendSharedSegmentBackend>();
}

}  // namespace mooncake

#endif  // USE_ASCEND_DIRECT
