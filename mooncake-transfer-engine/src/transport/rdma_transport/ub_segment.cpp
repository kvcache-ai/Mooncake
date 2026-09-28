// Copyright 2026 Huawei Technologies Co., Ltd
// Copyright 2026 KVCache.AI
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

#include "transport/rdma_transport/ub_segment.h"

#ifdef USE_ASCEND_RDMA

#include <dlfcn.h>
#include <glog/logging.h>

#include <charconv>
#include <algorithm>
#include <mutex>
#include <string_view>

#if !defined(__APPLE__)
extern "C" int halMemRegUbSegment(uint32_t, uint64_t, uint64_t)
    __attribute__((weak));
extern "C" int halMemUnRegUbSegment(uint32_t, uint64_t, uint64_t)
    __attribute__((weak));
extern "C" int aclrtGetLogicDevIdByUserDevId(int32_t, int32_t *)
    __attribute__((weak));
#endif

namespace mooncake {
namespace {

using HalRegisterFn = int (*)(uint32_t, uint64_t, uint64_t);
using HalUnregisterFn = int (*)(uint32_t, uint64_t, uint64_t);
using GetLogicDeviceIdFn = int (*)(int32_t, int32_t *);

struct AscendSymbols {
    HalRegisterFn register_segment = nullptr;
    HalUnregisterFn unregister_segment = nullptr;
    GetLogicDeviceIdFn get_logic_device_id = nullptr;
    void *hal_handle = nullptr;
    void *acl_handle = nullptr;
};

AscendSymbols &symbols() {
    static AscendSymbols result;
    static std::once_flag once;
    std::call_once(once, [] {
#if !defined(__APPLE__)
        auto &out = result;
        out.register_segment = halMemRegUbSegment;
        out.unregister_segment = halMemUnRegUbSegment;
        out.get_logic_device_id = aclrtGetLogicDevIdByUserDevId;
#else
        auto &out = result;
#endif

        out.hal_handle = dlopen("libascend_hal.so", RTLD_LAZY | RTLD_LOCAL);
        out.acl_handle = dlopen("libascendcl.so", RTLD_LAZY | RTLD_LOCAL);

        if (!out.register_segment) {
            out.register_segment = reinterpret_cast<HalRegisterFn>(
                dlsym(RTLD_DEFAULT, "halMemRegUbSegment"));
        }
        if (!out.unregister_segment) {
            out.unregister_segment = reinterpret_cast<HalUnregisterFn>(
                dlsym(RTLD_DEFAULT, "halMemUnRegUbSegment"));
        }
        if (!out.get_logic_device_id) {
            out.get_logic_device_id = reinterpret_cast<GetLogicDeviceIdFn>(
                dlsym(RTLD_DEFAULT, "aclrtGetLogicDevIdByUserDevId"));
        }

        if (!out.register_segment && out.hal_handle) {
            out.register_segment = reinterpret_cast<HalRegisterFn>(
                dlsym(out.hal_handle, "halMemRegUbSegment"));
        }
        if (!out.unregister_segment && out.hal_handle) {
            out.unregister_segment = reinterpret_cast<HalUnregisterFn>(
                dlsym(out.hal_handle, "halMemUnRegUbSegment"));
        }
        if (!out.get_logic_device_id && out.acl_handle) {
            out.get_logic_device_id = reinterpret_cast<GetLogicDeviceIdFn>(
                dlsym(out.acl_handle, "aclrtGetLogicDevIdByUserDevId"));
        }
    });
    return result;
}

bool parseNpuLocation(const std::string &location, int32_t &user_device_id) {
    constexpr std::string_view prefix(UbSegment::kNpuPrefix);
    if (location.compare(0, prefix.size(), prefix) != 0) return false;

    const std::string_view value(location.data() + prefix.size(),
                                 location.size() - prefix.size());
    if (value.empty()) return false;

    int32_t parsed = 0;
    const char *begin = value.data();
    const char *end = begin + value.size();
    auto result = std::from_chars(begin, end, parsed, 10);
    if (result.ec != std::errc() || result.ptr != end || parsed < 0)
        return false;

    user_device_id = parsed;
    return true;
}

bool symbolsAvailable(const AscendSymbols &resolved) {
    return resolved.register_segment && resolved.unregister_segment &&
           resolved.get_logic_device_id;
}

void warnSymbolsUnavailable() {
    static std::once_flag once;
    std::call_once(once, [] {
        LOG(WARNING) << "USE_ASCEND_RDMA is enabled, but Ascend UB symbols "
                        "are unavailable; UB segment registration is skipped";
    });
}

}  // namespace

int UbSegment::RegUbSegment(const std::string &location, uint64_t va,
                            uint64_t size) {
    int32_t user_device_id = -1;
    if (!parseNpuLocation(location, user_device_id)) {
        if (location.rfind(kNpuPrefix, 0) == 0) {
            LOG(ERROR) << "Invalid NPU memory location: " << location;
            return -1;
        }
        return 0;
    }
    if (va == 0 || size == 0) {
        LOG(ERROR) << "Invalid UB segment registration request: location="
                   << location << ", va=" << va << ", size=" << size;
        return -1;
    }

    auto &resolved = symbols();
    if (!symbolsAvailable(resolved)) {
        warnSymbolsUnavailable();
        return 0;
    }

    std::lock_guard<std::mutex> lock(mutex_);
    auto segment_it = segments_.find(va);
    if (segment_it != segments_.end()) {
        for (const auto &entry : segment_it->second) {
            if (entry.user_device_id == user_device_id) {
                if (entry.size != size) {
                    LOG(ERROR) << "UB segment size mismatch for location "
                               << location << ": existing=" << entry.size
                               << ", requested=" << size;
                    return -1;
                }
                LOG(INFO) << "UB segment already registered for location "
                          << location << ", va=" << va << ", size=" << size
                          << " (idempotent no-op)";
                return 0;
            }
        }
    }

    int32_t logic_device_id = -1;
    int ret = resolved.get_logic_device_id(user_device_id, &logic_device_id);
    if (ret != 0) {
        LOG(ERROR) << "aclrtGetLogicDevIdByUserDevId failed for user device "
                   << user_device_id << ", ret=" << ret;
        return ret;
    }
    ret = resolved.register_segment(static_cast<uint32_t>(logic_device_id), va,
                                    size);
    if (ret != 0) {
        LOG(ERROR) << "halMemRegUbSegment failed for user device "
                   << user_device_id << ", va=" << va << ", size=" << size
                   << ", ret=" << ret;
        return ret;
    }

    segments_[va].push_back({user_device_id, size});
    LOG(INFO) << "Registered UB segment: location=" << location
              << ", user_device_id=" << user_device_id
              << ", logic_device_id=" << logic_device_id
              << ", va=" << va << ", size=" << size;
    return 0;
}

int UbSegment::UnRegUbSegment(const std::string &location, uint64_t va) {
    int32_t user_device_id = -1;
    if (!parseNpuLocation(location, user_device_id)) return 0;

    std::lock_guard<std::mutex> lock(mutex_);
    auto segment_it = segments_.find(va);
    if (segment_it == segments_.end()) {
        LOG(INFO) << "No UB segment tracked for location " << location
                  << ", va=" << va << " (nothing to unregister)";
        return 0;
    }

    auto &resolved = symbols();
    auto &entries = segment_it->second;
    auto entry_it = std::find_if(
        entries.begin(), entries.end(), [&](const SegmentInfo &entry) {
            return entry.user_device_id == user_device_id;
        });
    if (entry_it == entries.end()) {
        LOG(INFO) << "No UB segment tracked for location " << location
                  << ", va=" << va << " on user device " << user_device_id
                  << " (nothing to unregister)";
        return 0;
    }

    int first_error = 0;
    if (symbolsAvailable(resolved)) {
        int32_t logic_device_id = -1;
        int ret = resolved.get_logic_device_id(user_device_id,
                                                &logic_device_id);
        if (ret != 0) {
            LOG(ERROR) << "aclrtGetLogicDevIdByUserDevId failed for user "
                          "device "
                       << user_device_id << ", va=" << va << ", ret=" << ret;
            first_error = ret;
        } else {
            ret = resolved.unregister_segment(
                static_cast<uint32_t>(logic_device_id), va, entry_it->size);
            if (ret != 0) {
                LOG(ERROR) << "halMemUnRegUbSegment failed for user device "
                           << user_device_id << ", va=" << va
                           << ", size=" << entry_it->size << ", ret=" << ret;
                first_error = ret;
            } else {
                LOG(INFO) << "Unregistered UB segment: location=" << location
                          << ", user_device_id=" << user_device_id
                          << ", logic_device_id=" << logic_device_id
                          << ", va=" << va << ", size=" << entry_it->size;
            }
        }
    } else {
        warnSymbolsUnavailable();
    }

    entries.erase(entry_it);
    if (entries.empty()) segments_.erase(segment_it);
    return first_error;
}

}  // namespace mooncake

#endif  // USE_ASCEND_RDMA

#ifndef USE_ASCEND_RDMA
namespace mooncake {

int UbSegment::RegUbSegment(const std::string &, uint64_t, uint64_t) {
    return 0;
}

int UbSegment::UnRegUbSegment(const std::string &, uint64_t) { return 0; }

}  // namespace mooncake
#endif  // !USE_ASCEND_RDMA
