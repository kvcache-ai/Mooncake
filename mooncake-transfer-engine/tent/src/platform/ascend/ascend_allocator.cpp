// Copyright 2025 KVCache.AI
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

#include "tent/platform/ascend.h"
#include "tent/runtime/topology.h"
#include "tent/common/status.h"

#include <numa.h>
#include <glog/logging.h>
#include <acl/acl.h>

#include <cstdlib>
#include <cstring>
#include <mutex>
#include <unordered_map>

#ifdef ASCEND_SUPPORT_FABRIC_MEM
#include "transport/ascend_transport/ascend_direct_transport/adxl_compat.h"
#endif

namespace mooncake {
namespace tent {
namespace {

bool WantFabricMem(const std::shared_ptr<Config>& conf,
                   const MemoryOptions& options) {
    if (!conf) {
        return false;
    }
    LocationParser location(options.location);
    if (location.type() == "npu") {
        return false;
    }
    if (conf->get("transports/ascend_direct/fabric_mem", false)) {
        return true;
    }
    if (!conf->get("transports/ascend_direct/store_te_init", false)) {
        return false;
    }
    const char* env = std::getenv("ASCEND_ENABLE_USE_FABRIC_MEM");
    return env && std::strcmp(env, "1") == 0;
}

#ifdef ASCEND_SUPPORT_FABRIC_MEM
struct VmmRecord {
    aclrtDrvMemHandle handle = nullptr;
    bool is_adxl = false;
};

std::mutex g_vmm_mu;
std::unordered_map<void*, VmmRecord> g_vmm_records;

int AllocatePhysical(size_t total_size, aclrtDrvMemHandle& handle) {
    int32_t user_dev_id = 0;
    auto ret = aclrtGetDevice(&user_dev_id);
    if (ret != ACL_ERROR_NONE) {
        LOG(ERROR) << "Failed to get device: " << ret;
        return -1;
    }
    int32_t physical_dev_id = 0;
    ret = aclrtGetPhyDevIdByLogicDevId(user_dev_id, &physical_dev_id);
    if (ret != ACL_ERROR_NONE) {
        physical_dev_id = user_dev_id;
    }
    aclrtPhysicalMemProp prop{};
    prop.handleType = ACL_MEM_HANDLE_TYPE_NONE;
    prop.allocationType = ACL_MEM_ALLOCATION_TYPE_PINNED;
    prop.memAttr = ACL_MEM_P2P_HUGE1G;
    prop.location.type = ACL_MEM_LOCATION_TYPE_HOST_NUMA;
    constexpr int32_t kDevicesPerChip = 4;
    constexpr int32_t kNumaNodeStep = 2;
    prop.location.id = (physical_dev_id / kDevicesPerChip) * kNumaNodeStep;
    prop.reserve = 0;
    ret = aclrtMallocPhysical(&handle, total_size, &prop, 0);
    if (ret != ACL_ERROR_NONE) {
        prop.location.type = ACL_MEM_LOCATION_TYPE_HOST;
        prop.location.id = 0;
        ret = aclrtMallocPhysical(&handle, total_size, &prop, 0);
        if (ret != ACL_ERROR_NONE) {
            prop.memAttr = ACL_MEM_P2P_HUGE;
            ret = aclrtMallocPhysical(&handle, total_size, &prop, 0);
            if (ret != ACL_ERROR_NONE) {
                LOG(ERROR) << "Failed to allocate fabric physical memory: "
                           << ret;
                return -1;
            }
        }
    }
    return 0;
}

void* AllocateVmm(size_t total_size) {
    aclrtDrvMemHandle handle = nullptr;
    if (AllocatePhysical(total_size, handle) != 0) {
        return nullptr;
    }
    void* va = nullptr;
    auto ret = aclrtReserveMemAddress(&va, total_size, 0, nullptr, 1);
    if (ret != ACL_ERROR_NONE) {
        LOG(ERROR) << "Failed to reserve fabric memory: " << ret;
        (void)aclrtFreePhysical(handle);
        return nullptr;
    }
    ret = aclrtMapMem(va, total_size, 0, handle, 0);
    if (ret != ACL_ERROR_NONE) {
        LOG(ERROR) << "Failed to map fabric memory: " << ret;
        (void)aclrtReleaseMemAddress(va);
        (void)aclrtFreePhysical(handle);
        return nullptr;
    }
    std::lock_guard<std::mutex> lock(g_vmm_mu);
    g_vmm_records.emplace(va, VmmRecord{handle, false});
    return va;
}

// Prefer adxl::MallocMem when the weak symbol is present. Failure does not
// fall back to ACL VMM — same contract as classic TE allocate_fabric_exact.
void* AllocateFabric(size_t total_size) {
    if (&adxl::AdxlEngine::MallocMem != nullptr) {
        void* va = nullptr;
        auto status = adxl::AdxlEngine::MallocMem(adxl::MemType::MEM_HOST,
                                                  total_size, &va);
        if (status != adxl::SUCCESS) {
            LOG(ERROR) << "Failed to allocate fabric memory, errmsg: "
                       << aclGetRecentErrMsg();
            return nullptr;
        }
        LOG(INFO) << "Call adxl MallocMem suc, va:" << va
                  << ", size:" << total_size;
        std::lock_guard<std::mutex> lock(g_vmm_mu);
        g_vmm_records.emplace(va, VmmRecord{nullptr, true});
        return va;
    }
    return AllocateVmm(total_size);
}

bool FreeVmm(void* ptr) {
    std::unique_lock<std::mutex> lock(g_vmm_mu);
    auto it = g_vmm_records.find(ptr);
    if (it == g_vmm_records.end()) {
        return false;
    }
    const VmmRecord record = it->second;
    g_vmm_records.erase(it);
    lock.unlock();
    if (record.is_adxl) {
        if (&adxl::AdxlEngine::FreeMem != nullptr) {
            auto status = adxl::AdxlEngine::FreeMem(ptr);
            if (status != adxl::SUCCESS) {
                LOG(ERROR) << "Failed to free fabric memory, errmsg: "
                           << aclGetRecentErrMsg();
            }
        }
        return true;
    }
    (void)aclrtUnmapMem(ptr);
    (void)aclrtReleaseMemAddress(ptr);
    (void)aclrtFreePhysical(record.handle);
    return true;
}
#endif

}  // namespace

Status AscendPlatform::allocate(void** pptr, size_t size,
                                MemoryOptions& options) {
    LocationParser location(options.location);
    if (WantFabricMem(conf, options)) {
#ifdef ASCEND_SUPPORT_FABRIC_MEM
        *pptr = AllocateFabric(size);
        if (!(*pptr)) {
            return Status::InternalError(
                "Unable to allocate fabric host memory" LOC_MARK);
        }
        return Status::OK();
#else
        return Status::InternalError(
            "Fabric mem mode is not supported, please upgrade Ascend HDK and "
            "CANN." LOC_MARK);
#endif
    }
    if (location.type() == "npu") {
        int deviceLogicId = 0;
        CHECK_ASCEND(aclrtGetDevice(&deviceLogicId));
        CHECK_ASCEND(aclrtMalloc(pptr, size, ACL_MEM_MALLOC_HUGE_FIRST));
        return Status::OK();
    }
    int socket_id = 0;
    if (location.type() == "cpu") socket_id = location.index();
    *pptr = numa_alloc_onnode(size, socket_id);
    if (!(*pptr))
        return Status::InternalError("Unable to allocate DRAM memory");
    return Status::OK();
}

Status AscendPlatform::free(void* ptr, size_t size) {
#ifdef ASCEND_SUPPORT_FABRIC_MEM
    if (FreeVmm(ptr)) {
        return Status::OK();
    }
#endif
    aclrtPtrAttributes attributes;
    CHECK_ASCEND(aclrtPointerGetAttributes(ptr, &attributes));
    if (attributes.location.type == ACL_MEM_LOCATION_TYPE_DEVICE) {
        CHECK_ASCEND(aclrtFree(ptr));
    } else {
        numa_free(ptr, size);
    }
    return Status::OK();
}

Status AscendPlatform::copy(void* dst, void* src, size_t length) {
    // Unlike CUDA/ROCm, the copy is not routed to the device owning the
    // buffer; aclrtMemcpy runs in the caller thread's ACL context. Routing it
    // needs a driver-id -> user-id lookup ACL does not expose: location.id is
    // a driver id, aclrtSetDevice takes an ASCEND_RT_VISIBLE_DEVICES user id.
    // TODO: left to someone with Ascend expertise and NPU hardware to verify;
    // getting that id mapping wrong picks the wrong device silently.
    CHECK_ASCEND(aclrtMemcpy(dst, length, src, length, ACL_MEMCPY_DEFAULT));
    return Status::OK();
}

}  // namespace tent
}  // namespace mooncake
