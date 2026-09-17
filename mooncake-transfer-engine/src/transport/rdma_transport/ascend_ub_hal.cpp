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

#include "transport/rdma_transport/ascend_ub_hal.h"

#if defined(USE_1825)

#include <dlfcn.h>

#include <cstdint>
#include <map>
#include <mutex>

#include <glog/logging.h>

namespace mooncake {
namespace {

constexpr int32_t kAclMemLocationTypeDevice = 1;

// Minimal ABI declarations for aclrtPointerGetAttributes. Keeping them local
// lets a USE_1825 build run on hosts without Ascend headers or libraries.
struct AclMemLocation {
    uint32_t id;
    int32_t type;
};

struct AclPtrAttributes {
    AclMemLocation location;
    uint32_t page_size;
    uint32_t reserved[4];
};

static_assert(sizeof(AclMemLocation) == 8);
static_assert(sizeof(AclPtrAttributes) == 28);

using AclPointerGetAttributes = int (*)(const void *, AclPtrAttributes *);
using HalMemRegUbSegment = int (*)(uint32_t, uint64_t, uint64_t);
using HalMemUnRegUbSegment = int (*)(uint32_t, uint64_t, uint64_t);

struct AscendUbApi {
    void *acl_handle = nullptr;
    void *hal_handle = nullptr;
    AclPointerGetAttributes pointer_get_attributes = nullptr;
    HalMemRegUbSegment register_segment = nullptr;
    HalMemUnRegUbSegment unregister_segment = nullptr;

    AscendUbApi() {
        acl_handle = dlopen("libascendcl.so", RTLD_NOW | RTLD_LOCAL);
        if (!acl_handle) {
            LOG(WARNING) << "USE_1825: libascendcl.so is unavailable; Ascend "
                            "UB registration is disabled";
            return;
        }

        hal_handle = dlopen("libascend_hal.so", RTLD_NOW | RTLD_LOCAL);
        if (!hal_handle) {
            LOG(WARNING) << "USE_1825: libascend_hal.so is unavailable; "
                            "Ascend UB registration is disabled";
            return;
        }

        pointer_get_attributes = reinterpret_cast<AclPointerGetAttributes>(
            dlsym(acl_handle, "aclrtPointerGetAttributes"));
        register_segment = reinterpret_cast<HalMemRegUbSegment>(
            dlsym(hal_handle, "halMemRegUbSegment"));
        unregister_segment = reinterpret_cast<HalMemUnRegUbSegment>(
            dlsym(hal_handle, "halMemUnRegUbSegment"));

        if (!pointer_get_attributes || !register_segment ||
            !unregister_segment) {
            LOG(WARNING) << "USE_1825: required Ascend UB symbols are "
                            "unavailable; Ascend UB registration is disabled";
        }
    }

    bool available() const {
        return pointer_get_attributes && register_segment &&
               unregister_segment;
    }
};

struct UbEntry {
    size_t length;
    uint32_t device_id;
    size_t refcount;
};

AscendUbApi &api() {
    static AscendUbApi instance;
    return instance;
}

std::mutex &registryMutex() {
    static std::mutex mutex;
    return mutex;
}

std::map<uintptr_t, UbEntry> &registry() {
    static std::map<uintptr_t, UbEntry> entries;
    return entries;
}

}  // namespace

int ascendUbRegister(void *addr, size_t length) {
    AscendUbApi &ascend_api = api();
    if (!ascend_api.available()) return 0;

    const uintptr_t va = reinterpret_cast<uintptr_t>(addr);
    std::lock_guard<std::mutex> lock(registryMutex());
    auto &entries = registry();
    auto iter = entries.find(va);
    if (iter != entries.end()) {
        if (iter->second.length != length) {
            LOG(ERROR) << "USE_1825: Ascend UB range already registered with "
                          "a different length, addr="
                       << addr << ", existing_length=" << iter->second.length
                       << ", requested_length=" << length;
            return -1;
        }
        ++iter->second.refcount;
        return 0;
    }

    AclPtrAttributes attributes{};
    int ret = ascend_api.pointer_get_attributes(addr, &attributes);
    if (ret != 0 || attributes.location.type != kAclMemLocationTypeDevice) {
        return 0;
    }

    ret = ascend_api.register_segment(attributes.location.id, va, length);
    if (ret != 0) {
        LOG(ERROR) << "USE_1825: halMemRegUbSegment failed, device_id="
                   << attributes.location.id << ", addr=" << addr
                   << ", length=" << length << ", ret=" << ret;
        return ret;
    }

    entries.emplace(
        va, UbEntry{length, attributes.location.id, /*refcount=*/1});
    return 0;
}

int ascendUbUnregister(void *addr, size_t length) {
    AscendUbApi &ascend_api = api();
    if (!ascend_api.available()) return 0;

    const uintptr_t va = reinterpret_cast<uintptr_t>(addr);
    std::lock_guard<std::mutex> lock(registryMutex());
    auto &entries = registry();
    auto iter = entries.find(va);
    if (iter == entries.end()) return 0;

    if (iter->second.length != length) {
        LOG(ERROR) << "USE_1825: Ascend UB unregister length mismatch, addr="
                   << addr << ", registered_length=" << iter->second.length
                   << ", requested_length=" << length;
        return -1;
    }
    if (iter->second.refcount > 1) {
        --iter->second.refcount;
        return 0;
    }

    int ret = ascend_api.unregister_segment(iter->second.device_id, va, length);
    if (ret != 0) {
        // The verbs MR no longer owns this reference. Keep a zero-reference
        // entry so a later lifetime can retry HAL unregistration.
        iter->second.refcount = 0;
        LOG(ERROR) << "USE_1825: halMemUnRegUbSegment failed, device_id="
                   << iter->second.device_id << ", addr=" << addr
                   << ", length=" << length << ", ret=" << ret;
        return ret;
    }

    entries.erase(iter);
    return 0;
}

}  // namespace mooncake

#endif  // USE_1825
