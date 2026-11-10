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

#ifndef ASCEND_DIRECT_UTIL_H
#define ASCEND_DIRECT_UTIL_H

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <mutex>
#include <queue>
#include <string>
#include <thread>
#include <vector>

#include <acl/acl.h>
#include "ascend_affinity.h"

namespace mooncake {

#define CHECK_ACL(call)                                       \
    do {                                                      \
        auto err = (call);                                    \
        if (err != ACL_ERROR_NONE) {                          \
            LOG(ERROR) << "Call acl failed, "                 \
                       << "detail: " << aclGetRecentErrMsg(); \
            return -1;                                        \
        }                                                     \
    } while (0)

#define MAKE_GUARD(var, callback) \
    const ::mooncake::ScopeGuard const_guard_##var(callback)

#define MAKE_DISMISSABLE_GUARD(var, callback) \
    ::mooncake::ScopeGuard make_guard_##var(callback)
#define DISMISS_GUARD(var) make_guard_##var.Dismiss()

class ScopeGuard {
   public:
    ScopeGuard(const ScopeGuard&) = delete;
    ScopeGuard& operator=(const ScopeGuard&) = delete;

    explicit ScopeGuard(const std::function<void()>& on_exit_scope)
        : on_exit_scope_(on_exit_scope) {}

    ~ScopeGuard() {
        if (!dismissed_) {
            if (on_exit_scope_ != nullptr) {
                try {
                    on_exit_scope_();
                } catch (std::bad_function_call&) {
                    // no need log error
                } catch (...) {
                    // no need log error
                }
            }
        }
    }

    void Dismiss() { dismissed_ = true; }

   private:
    std::function<void()> on_exit_scope_;
    bool dismissed_ = false;
};

// Classic Ascend direct transport helpers: the ASCEND_GLOBAL_RESOURCE_CONFIG
// parser used by the dispatcher and host allocator, and the small generation
// helpers. The NUMA affinity plan/thread pool come from ascend_affinity.h.
std::string GenAdxlEngineName(const std::string& ip, const uint64_t port);

uint16_t FindAdxlListenPort(int32_t base_port, int32_t device_id);

// True only when HCCL_INTRA_ROCE_ENABLE=1. The standalone holder's
// RoceDummyRealSliceDispatcher is selected from this flag. roce:device and
// roce:host in ASCEND_GLOBAL_RESOURCE_CONFIG do not set it.
bool IsHcclIntraRoceEnabled();

// True when HCCL_INTRA_ROCE_ENABLE=1 or ASCEND_GLOBAL_RESOURCE_CONFIG sets
// comm_resource_config.protocol_desc to a roce:* entry (string or array).
bool IsRoceModeEnabled();

int SetDeviceAndGetContext(int32_t device_id, aclrtContext* out_context);

// ASCEND_GLOBAL_RESOURCE_CONFIG (and protocol_desc) helpers used by the classic
// Ascend direct transport and the classic host allocator. TENT parses its own
// configuration copy with the vendored nlohmann JSON library instead.

// Parse ASCEND_GLOBAL_RESOURCE_CONFIG JSON for roce protocol_desc only.
bool HasRoceProtocolDescInGlobalResourceConfig(const char* config_str);

// True when protocol_desc is "hccs:device", or an array containing that
// entry. "hccs:host" and "roce:*" do not match. config_str is the already
// role-resolved ASCEND_GLOBAL_RESOURCE_CONFIG.
bool HasHccsDeviceProtocolDescInGlobalResourceConfig(const char* config_str);

// True when protocol_desc is "roce:host", or an array containing that entry,
// on this object or on its "store" subtree. "roce:device" does not match.
// The store subtree is included because host memory is allocated after TE
// init, when ascend_store_te_init has already been cleared.
bool HasRoceHostProtocolDescInGlobalResourceConfig(const char* config_str);

// Resolve the effective ASCEND_GLOBAL_RESOURCE_CONFIG for the current TE role.
// Schema: the top-level object is the default config (passed verbatim to adxl
// for normal/P2P TEs); an optional "store" sub-object overrides it for a
// Store-init TE (globalConfig().ascend_store_te_init == true).
//   - null/empty or no "store" key  -> returned verbatim (backward compatible)
//   - has "store" and store-init TE  -> the "store" sub-object, serialized
//   - has "store" and normal TE      -> root with "store" removed, serialized
// A Store TE with no "store" key falls back to the default config.
std::string ResolveAscendGlobalResourceConfig(const char* config_str);

// TE support for configuring HIXL's comm_resource_config.nic_topo_path in the
// (already role-resolved) GlobalResourceConfig: fills it from Mooncake's
// MC_CUSTOM_TOPO_JSON path when the HIXL option is empty. Rules:
//   - MC_CUSTOM_TOPO_JSON unset/empty                    -> returned unchanged
//   - nic_topo_path already set (flat or nested, string, non-empty)
//                                                       -> returned unchanged
//   - otherwise the MC_CUSTOM_TOPO_JSON path is written under the nested
//     "comm_resource_config" object when one exists (any stale flat key is
//     dropped), otherwise as the flat key
//     "comm_resource_config.nic_topo_path". An empty input yields a minimal
//     {"comm_resource_config.nic_topo_path":"..."} object.
// A non-empty but unparsable input is returned unchanged (HIXL rejects it).
std::string InjectNicTopoPathFromCustomTopoEnvTe(
    const std::string& resolved_config);

}  // namespace mooncake
#endif  // ASCEND_DIRECT_UTIL_H
