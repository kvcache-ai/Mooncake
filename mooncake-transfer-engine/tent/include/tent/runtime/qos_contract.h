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

#ifndef TENT_RUNTIME_QOS_CONTRACT_H
#define TENT_RUNTIME_QOS_CONTRACT_H

#include "tent/common/config.h"
#include "tent/common/status.h"
#include "tent/common/types.h"
#include "tent/thirdparty/nlohmann/json.h"

#include <cstdint>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

namespace mooncake {
namespace tent {

struct QosPolicyFields {
    std::optional<int> priority;
    std::optional<uint64_t> max_inflight_bytes;
    std::optional<uint64_t> max_inflight_requests;
    std::optional<std::vector<std::string>> allowed_degraded_actions;
};

struct QosRequestContext {
    // Opaque administrative isolation identifier supplied by the caller. A
    // Store integration should pass its canonical Store tenant ID here rather
    // than create a second TENT-specific tenant namespace. Empty values
    // normalize to "default".
    std::string tenant_id = "default";
    // Intent is the normalized business meaning of the transfer
    // (foreground_get, background_prefetch, checkpoint, ...). When no explicit
    // selects the tenant-local intent override. Empty values normalize to
    // "unspec".
    std::string intent = "unspec";
    // Legacy caller priority. It is preserved when no resolved contract sets a
    // priority, and is overridden by the effective QoS contract priority when
    // present.
    int requested_priority = PRIO_HIGH;
};

struct EffectiveQosPolicy : public QosPolicyFields {
    bool enabled = false;
    bool matched = false;
    // Normalized request identity used for resolution and explain output.
    std::string tenant_id = "default";
    std::string intent = "unspec";
    // Name of the contract layer that supplied the most specific match:
    // compatibility_default, global_default, <tenant>.default, or
    // <tenant>.<intent>.
    std::string matched_contract = "compatibility_default";
    int requested_priority = PRIO_HIGH;
    int effective_priority = PRIO_HIGH;
};

class QosContractResolver {
   public:
    Status loadFromConfig(const Config& config);

    bool enabled() const { return enabled_; }

    Status resolve(const QosRequestContext& context,
                   EffectiveQosPolicy* out) const;

    std::string explainJson(const EffectiveQosPolicy& policy,
                            int indent = 2) const;

    static std::string intentTypeName(IntentType intent);

   private:
    struct TenantContract {
        QosPolicyFields defaults;
        std::unordered_map<std::string, QosPolicyFields> intents;
    };

    static Status parsePolicyFields(const json& node, QosPolicyFields* out,
                                    const std::string& path);
    static Status parsePriority(const json& node, int* out,
                                const std::string& path);
    static Status parseBytes(const json& node, uint64_t* out,
                             const std::string& path);
    static Status parseUint64(const json& node, uint64_t* out,
                              const std::string& path);
    static std::string normalizeKey(const std::string& value);
    static void mergeFields(QosPolicyFields* dst, const QosPolicyFields& src);
    static void fieldsToJson(json* out, const QosPolicyFields& fields);

    bool enabled_{false};
    QosPolicyFields global_defaults_;
    std::unordered_map<std::string, TenantContract> tenants_;
};

}  // namespace tent
}  // namespace mooncake

#endif  // TENT_RUNTIME_QOS_CONTRACT_H
