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

#include <algorithm>
#include <cstring>
#include <mutex>
#include <vector>

#include "shared_segment_internal.h"

#ifndef ACL_RT_VMM_EXPORT_FLAG_DISABLE_PID_VALIDATION
#define ACL_RT_VMM_EXPORT_FLAG_DISABLE_PID_VALIDATION 0x1UL
#endif

namespace mooncake {
namespace {
constexpr uint16_t kAscendBackendId = 1;
// Used when the runtime cannot report a granularity; matches the smallest huge
// page ACL falls back to for host allocations.
constexpr uint64_t kFallbackGranularity = 2ULL * 1024 * 1024;
constexpr uint64_t kGiantPageSize = 1ULL * 1024 * 1024 * 1024;
// aclrtReserveMemAddressNoUCMemory on A3 requires virtual reservation size
// (and start VA alignment) to be a multiple of 1 GiB; physical allocations
// and aclrtMapMem inside that window still operate at 2 MiB granularity.
constexpr uint64_t kA3ReserveGranularity = 1ULL * 1024 * 1024 * 1024;
constexpr size_t kMemAccessDescCount = 1;
// The owner's pages are huge pages, so reservations have to be huge-page
// backed too.
constexpr uint64_t kReserveHugePageFlag = 1;
constexpr size_t kBytesPerTB = 1024ULL * 1024ULL * 1024ULL * 1024ULL;
// HIXL fabric mem defaults to half-open [40TB, 72TB) (32TB capacity). On A3
// (SVM v2), reserve starting at 72TB so it never collides with HIXL and stays
// below the 80TB safe upper bound (older drivers truncate heap_idx >= 80TB).
constexpr uintptr_t kA3ReserveStartAddr = 72ULL * kBytesPerTB;
constexpr uintptr_t kA3SafeVaUpperBound = 80ULL * kBytesPerTB;
// aclsysGetVersionNum encodes runtime 9.2.0 as 90200000. From that version,
// the caller device id is already the id aclrtMemSetAccess expects.
constexpr int32_t kRuntimeVersionSkipLogicIdConvert = 90200000;

static_assert(sizeof(aclrtMemFabricHandle) <= kMaxHandleBytes,
              "Fabric handle must fit into a shared segment blob");

// Older AscendCL headers omit these symbols; weak declarations keep the binary
// loadable on older runtimes.
extern "C" __attribute__((weak)) aclError
aclrtReserveMemAddressNoUCMemory(void** virPtr, size_t size, size_t alignment,
                                 void* expectPtr, uint64_t flags);

extern "C" __attribute__((weak)) aclError
aclsysGetVersionNum(char* pkgName, int32_t* versionNum);

struct ReservedInterval {
    uintptr_t base = 0;
    uint64_t size = 0;
};

std::mutex g_a3_reserve_mutex;
std::vector<ReservedInterval> g_a3_reservations;

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

bool RuntimeSkipsUserToLogicConvert() {
    if (&aclsysGetVersionNum == nullptr) {
        return false;
    }
    char pkg_name[] = "runtime";
    int32_t version_num = 0;
    const aclError ret = aclsysGetVersionNum(pkg_name, &version_num);
    if (ret != ACL_ERROR_NONE) {
        return false;
    }
    return version_num >= kRuntimeVersionSkipLogicIdConvert;
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

Status CheckCurrentDevice(int32_t expected_device_id) {
    int32_t current_device = -1;
    auto ret = aclrtGetDevice(&current_device);
    if (ret != ACL_ERROR_NONE || current_device != expected_device_id) {
        return Status::InvalidArgument(
            "Shared segment Ascend VMM must run on device " +
            std::to_string(expected_device_id) +
            ", but the current device is " + std::to_string(current_device));
    }
    return Status::OK();
}

// Host pages are only bound to the host by aclrtMapMem; on HCCS hosts the NPU
// needs an explicit grant before kernels or TE can touch them. Non-HCCS (PCIe)
// hosts return ACL_ERROR_RT_FEATURE_NOT_SUPPORT, leaving the mapping
// host-accessible only (`granted = false`).
Status GrantDeviceAccess(void* addr, uint64_t size, bool& granted) {
    granted = false;
    int32_t user_id = -1;
    auto ret = aclrtGetDevice(&user_id);
    if (ret != ACL_ERROR_NONE) {
        return Status::Memory("aclrtGetDevice failed for MemSetAccess, ret " +
                              std::to_string(ret));
    }
    int32_t access_id = user_id;
    if (!RuntimeSkipsUserToLogicConvert()) {
        ret = aclrtGetLogicDevIdByUserDevId(user_id, &access_id);
        if (ret != ACL_ERROR_NONE) {
            return Status::Memory(
                "aclrtGetLogicDevIdByUserDevId failed for MemSetAccess, ret " +
                std::to_string(ret));
        }
    }
    aclrtMemAccessDesc desc{};
    desc.flags = ACL_RT_MEM_ACCESS_FLAGS_READWRITE;
    desc.location.type = ACL_MEM_LOCATION_TYPE_DEVICE;
    desc.location.id = static_cast<uint32_t>(access_id);
    ret = aclrtMemSetAccess(addr, size, &desc, kMemAccessDescCount);
    if (ret == ACL_ERROR_RT_FEATURE_NOT_SUPPORT) {
        LOG_FIRST_N(WARNING, 1)
            << "aclrtMemSetAccess is unsupported on this host (non-HCCS); "
               "Ascend VMM shared segment will remain host-accessible only";
        return Status::OK();
    }
    if (ret != ACL_ERROR_NONE) {
        return Status::Memory(
            "aclrtMemSetAccess failed for shared segment, ret " +
            std::to_string(ret));
    }
    granted = true;
    return Status::OK();
}

Status AllocateHostPhysical(uint64_t size, uintptr_t map_va,
                            aclrtDrvMemHandle& out_handle) {
    aclrtPhysicalMemProp prop = BuildHostMemProp();
    aclrtDrvMemHandle handle = nullptr;
    // Try 1GiB giant pages on A3 when both size and mapped VA are 1GiB-aligned,
    // falling back cleanly to 2MiB huge pages.
    if (IsA3Soc() && size >= kGiantPageSize && (size % kGiantPageSize) == 0 &&
        (map_va % kGiantPageSize) == 0) {
        aclrtPhysicalMemProp giant_prop = prop;
        giant_prop.memAttr = ACL_MEM_P2P_HUGE1G;
        auto giant_ret = aclrtMallocPhysical(&handle, size, &giant_prop, 0);
        if (giant_ret == ACL_ERROR_NONE) {
            out_handle = handle;
            return Status::OK();
        }
        handle = nullptr;
    }
    auto ret = aclrtMallocPhysical(&handle, size, &prop, 0);
    if (ret != ACL_ERROR_NONE) {
        return Status::Memory(
            "aclrtMallocPhysical failed for shared segment shard of " +
            std::to_string(size) + " bytes, ret " + std::to_string(ret));
    }
    out_handle = handle;
    return Status::OK();
}

uintptr_t NextAvailableA3AddrLocked(uintptr_t start_va, uint64_t size,
                                    uint64_t align) {
    uintptr_t candidate = AlignUp(start_va, align);
    bool advanced = true;
    while (advanced) {
        advanced = false;
        for (const auto& r : g_a3_reservations) {
            const uintptr_t r_end = r.base + r.size;
            if (candidate < r_end && candidate + size > r.base) {
                candidate = AlignUp(r_end, align);
                advanced = true;
            }
        }
    }
    return candidate;
}

class AscendSharedSegmentBackend : public SharedSegmentBackend {
   public:
    ~AscendSharedSegmentBackend() override { Release(); }

    uint64_t Granularity(const SharedSegmentOptions& options) const override;

    Status Prepare(uint64_t total_size, const OwnerChunk* local_chunk,
                   const SharedSegmentOptions& options, uintptr_t& base_addr,
                   std::vector<uint8_t>& handle) override;

    Status CompleteMapping(const std::vector<OwnerChunk>& chunks,
                           const std::vector<std::vector<uint8_t>>& handles,
                           const SharedSegmentOptions& options) override;

    // After MemSetAccess the host SVM VA is device-accessible on every rank;
    // expose it once the full mapping is complete so Python wraps NPU tensors.
    // On non-HCCS hosts where MemSetAccess is unsupported, returns 0 so Python
    // exposes host CPU tensors instead.
    uintptr_t DeviceAddr() const override {
        if (ready_ && device_accessible_ && reserved_addr_ != nullptr) {
            return reinterpret_cast<uintptr_t>(reserved_addr_);
        }
        return 0;
    }

    uint16_t BackendId() const override { return kAscendBackendId; }

   private:
    Status ReserveWindow(uint64_t total_size, uint64_t align);
    void Release();

    void* reserved_addr_ = nullptr;
    uint64_t reserved_size_ = 0;
    bool a3_tracked_ = false;
    aclrtDrvMemHandle owned_pa_handle_ = nullptr;
    std::vector<aclrtDrvMemHandle> imported_handles_;
    std::vector<void*> mapped_chunk_addrs_;
    bool device_accessible_ = true;
    bool ready_ = false;
};

uint64_t AscendSharedSegmentBackend::Granularity(
    const SharedSegmentOptions&) const {
    auto prop = BuildHostMemProp();
    size_t granularity = 0;
    auto ret = aclrtMemGetAllocationGranularity(
        &prop, ACL_RT_MEM_ALLOC_GRANULARITY_MINIMUM, &granularity);
    if (ret != ACL_ERROR_NONE || granularity == 0) {
        return kFallbackGranularity;
    }
    return granularity;
}

Status AscendSharedSegmentBackend::ReserveWindow(uint64_t total_size,
                                                 uint64_t align) {
    void* addr = nullptr;
    // Match HIXL VirtualMemoryManager::ReserveSingleVaBlock:
    // - On A3 (SVM v2): reserve once for the full multi-owner span via
    //   NoUCMemory starting at 72TB (past HIXL's half-open [40TB, 72TB) pool)
    //   with 1 GiB alignment and staying below the 80TB driver limit.
    // - On A5 / non-A3: pass expectPtr=nullptr to aclrtReserveMemAddress so the
    //   driver assigns the VA.
    if (IsA3Soc() && &aclrtReserveMemAddressNoUCMemory != nullptr) {
        std::lock_guard<std::mutex> lock(g_a3_reserve_mutex);
        const uint64_t reserve_align = std::max(align, kA3ReserveGranularity);
        const uint64_t reserve_size = AlignUp(total_size, reserve_align);
        const uintptr_t candidate = NextAvailableA3AddrLocked(
            kA3ReserveStartAddr, reserve_size, reserve_align);
        if (reserve_size > kA3SafeVaUpperBound ||
            candidate > kA3SafeVaUpperBound - reserve_size) {
            return Status::Memory(
                "A3 shared segment NoUCMemory reservation of " +
                std::to_string(reserve_size) + " bytes exceeds 80TB VA bound");
        }
        void* start_va = reinterpret_cast<void*>(candidate);
        auto ret = aclrtReserveMemAddressNoUCMemory(
            &addr, reserve_size, 0, start_va, kReserveHugePageFlag);
        if (ret == ACL_ERROR_NONE) {
            reserved_addr_ = addr;
            reserved_size_ = reserve_size;
            g_a3_reservations.push_back(ReservedInterval{
                reinterpret_cast<uintptr_t>(addr), reserve_size});
            a3_tracked_ = true;
            return Status::OK();
        }
        if (ret != ACL_ERROR_RT_FEATURE_NOT_SUPPORT) {
            return Status::Memory(
                "aclrtReserveMemAddressNoUCMemory failed for " +
                std::to_string(reserve_size) + " bytes at " +
                std::to_string(candidate) + ", ret " + std::to_string(ret));
        }
    }
    auto ret = aclrtReserveMemAddress(&addr, total_size, 0, nullptr,
                                      kReserveHugePageFlag);
    if (ret != ACL_ERROR_NONE) {
        return Status::Memory("aclrtReserveMemAddress failed for " +
                              std::to_string(total_size) + " bytes, ret " +
                              std::to_string(ret));
    }
    reserved_addr_ = addr;
    reserved_size_ = total_size;
    return Status::OK();
}

Status AscendSharedSegmentBackend::Prepare(uint64_t total_size,
                                           const OwnerChunk* local_chunk,
                                           const SharedSegmentOptions& options,
                                           uintptr_t& base_addr,
                                           std::vector<uint8_t>& handle) {
    auto status = CheckCurrentDevice(options.device_id);
    if (!status.ok()) {
        return status;
    }

    const uint64_t granularity = Granularity(options);
    status = ReserveWindow(total_size, granularity);
    if (!status.ok()) {
        return status;
    }
    base_addr = reinterpret_cast<uintptr_t>(reserved_addr_);

    if (local_chunk != nullptr) {
        if (local_chunk->offset > reserved_size_ ||
            local_chunk->size > reserved_size_ - local_chunk->offset) {
            return Status::InvalidArgument(
                "Shared segment local owner chunk exceeds reserved window");
        }
        void* chunk_addr =
            static_cast<uint8_t*>(reserved_addr_) + local_chunk->offset;
        const uintptr_t chunk_va = reinterpret_cast<uintptr_t>(chunk_addr);
        status =
            AllocateHostPhysical(local_chunk->size, chunk_va, owned_pa_handle_);
        if (!status.ok()) {
            return status;
        }
        // Map and grant device access BEFORE exporting the shareable handle
        // (matches HIXL's MallocMem -> ExportToShareableHandle order; exporting
        // before map/set-access breaks host PTE and MemSetAccess on A3).
        auto ret =
            aclrtMapMem(chunk_addr, local_chunk->size, 0, owned_pa_handle_, 0);
        if (ret != ACL_ERROR_NONE) {
            return Status::Memory(
                "aclrtMapMem failed for local owner shard, ret " +
                std::to_string(ret));
        }
        mapped_chunk_addrs_.push_back(chunk_addr);
        bool granted = false;
        status = GrantDeviceAccess(chunk_addr, local_chunk->size, granted);
        if (!status.ok()) {
            return status;
        }
        if (!granted) {
            device_accessible_ = false;
        }

        aclrtMemFabricHandle fabric_handle{};
        ret = aclrtMemExportToShareableHandleV2(
            owned_pa_handle_, ACL_RT_VMM_EXPORT_FLAG_DISABLE_PID_VALIDATION,
            ACL_MEM_SHARE_HANDLE_TYPE_FABRIC, &fabric_handle);
        if (ret == ACL_ERROR_NONE) {
            handle.assign(fabric_handle.data,
                          fabric_handle.data + sizeof(fabric_handle.data));
        } else if (ret == ACL_ERROR_RT_FEATURE_NOT_SUPPORT) {
            // Standalone / non-SuperPod Ascend hosts do not support
            // cross-server FABRIC handles; fall back to same-host DEFAULT VMM
            // shareable handles.
            uint64_t default_handle = 0;
            ret = aclrtMemExportToShareableHandleV2(
                owned_pa_handle_, ACL_RT_VMM_EXPORT_FLAG_DISABLE_PID_VALIDATION,
                ACL_MEM_SHARE_HANDLE_TYPE_DEFAULT, &default_handle);
            if (ret != ACL_ERROR_NONE) {
                return Status::Memory(
                    "aclrtMemExportToShareableHandleV2(DEFAULT) failed for "
                    "shared segment, ret " +
                    std::to_string(ret));
            }
            const auto* raw = reinterpret_cast<const uint8_t*>(&default_handle);
            handle.assign(raw, raw + sizeof(default_handle));
        } else {
            return Status::Memory(
                "aclrtMemExportToShareableHandleV2 failed for shared segment, "
                "ret " +
                std::to_string(ret));
        }
    }
    return Status::OK();
}

Status AscendSharedSegmentBackend::CompleteMapping(
    const std::vector<OwnerChunk>& chunks,
    const std::vector<std::vector<uint8_t>>& handles,
    const SharedSegmentOptions& options) {
    if (reserved_addr_ == nullptr) {
        return Status::InvalidArgument(
            "Shared segment import needs a reserved address window");
    }
    if (chunks.size() != handles.size()) {
        return Status::InvalidArgument(
            "Shared segment owner chunk and handle count mismatch");
    }
    auto status = CheckCurrentDevice(options.device_id);
    if (!status.ok()) {
        return status;
    }

    for (size_t i = 0; i < chunks.size(); ++i) {
        const auto& chunk = chunks[i];
        if (chunk.offset > reserved_size_ ||
            chunk.size > reserved_size_ - chunk.offset) {
            return Status::InvalidArgument(
                "Shared segment owner chunk exceeds reserved window");
        }
        if (chunk.owner_rank == options.rank_id) {
            continue;
        }
        const auto& handle = handles[i];
        aclrtDrvMemHandle imported = nullptr;
        aclError ret = ACL_ERROR_NONE;
        if (handle.size() == sizeof(aclrtMemFabricHandle)) {
            aclrtMemFabricHandle share_handle{};
            memcpy(share_handle.data, handle.data(), handle.size());
            ret = aclrtMemImportFromShareableHandleV2(
                &share_handle, ACL_MEM_SHARE_HANDLE_TYPE_FABRIC, 0, &imported);
        } else if (handle.size() == sizeof(uint64_t)) {
            uint64_t share_handle = 0;
            memcpy(&share_handle, handle.data(), sizeof(share_handle));
            ret = aclrtMemImportFromShareableHandleV2(
                &share_handle, ACL_MEM_SHARE_HANDLE_TYPE_DEFAULT, 0, &imported);
        } else {
            return Status::InvalidArgument(
                "Shared segment owner handle has an unexpected length");
        }
        if (ret != ACL_ERROR_NONE) {
            return Status::Memory(
                "aclrtMemImportFromShareableHandleV2 failed for shared "
                "segment, ret " +
                std::to_string(ret));
        }
        imported_handles_.push_back(imported);

        void* chunk_addr = static_cast<uint8_t*>(reserved_addr_) + chunk.offset;
        ret = aclrtMapMem(chunk_addr, chunk.size, 0, imported, 0);
        if (ret != ACL_ERROR_NONE) {
            return Status::Memory(
                "aclrtMapMem failed for imported shared segment shard, ret " +
                std::to_string(ret));
        }
        mapped_chunk_addrs_.push_back(chunk_addr);
        bool granted = false;
        status = GrantDeviceAccess(chunk_addr, chunk.size, granted);
        if (!status.ok()) {
            return status;
        }
        if (!granted) {
            device_accessible_ = false;
        }
    }
    ready_ = true;
    return Status::OK();
}

void AscendSharedSegmentBackend::Release() {
    ready_ = false;
    for (void* chunk_addr : mapped_chunk_addrs_) {
        (void)aclrtUnmapMem(chunk_addr);
    }
    mapped_chunk_addrs_.clear();
    for (aclrtDrvMemHandle imported : imported_handles_) {
        if (imported != nullptr) {
            (void)aclrtFreePhysical(imported);
        }
    }
    imported_handles_.clear();
    if (owned_pa_handle_ != nullptr) {
        (void)aclrtFreePhysical(owned_pa_handle_);
        owned_pa_handle_ = nullptr;
    }
    if (reserved_addr_ != nullptr) {
        const uintptr_t released_va =
            reinterpret_cast<uintptr_t>(reserved_addr_);
        const uint64_t released_size = reserved_size_;
        (void)aclrtReleaseMemAddress(reserved_addr_);
        reserved_addr_ = nullptr;
        reserved_size_ = 0;
        if (a3_tracked_) {
            std::lock_guard<std::mutex> lock(g_a3_reserve_mutex);
            const auto it = std::find_if(
                g_a3_reservations.begin(), g_a3_reservations.end(),
                [released_va, released_size](const ReservedInterval& r) {
                    return r.base == released_va && r.size == released_size;
                });
            if (it != g_a3_reservations.end()) {
                g_a3_reservations.erase(it);
            }
            a3_tracked_ = false;
        }
    }
}
}  // namespace

std::unique_ptr<SharedSegmentBackend> CreateAscendSharedSegmentBackend() {
    return std::make_unique<AscendSharedSegmentBackend>();
}

}  // namespace mooncake

#endif  // USE_ASCEND_DIRECT
