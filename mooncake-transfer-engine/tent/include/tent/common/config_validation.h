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

#ifndef TENT_CONFIG_VALIDATION_H
#define TENT_CONFIG_VALIDATION_H

#include "tent/common/config_lifecycle.h"
#include "tent/common/status.h"

namespace mooncake {
namespace tent {

enum class ConfigChangeDisposition : uint8_t {
    kRuntimeCandidate,
    kRestartRequired,
    kUnsupported,
};

struct ConfigChange {
    std::string path;
    ConfigChangeDisposition disposition;
};

// Capture once and use for both inputs. These are build capabilities, not
// discovered devices or proof that a backend can initialize on this host.
// Tests and offline tools may supply a target build's context explicitly.
struct ConfigValidationContext {
    std::vector<std::string> compiled_transports;
    unsigned hardware_concurrency{0};
};

ConfigValidationContext currentConfigValidationContext();

struct ConfigChangePlan {
    // Own the exact inputs analyzed. Mutating the original Config objects
    // afterwards cannot change this plan. No generation is assigned/published.
    std::shared_ptr<const Config> current;
    std::shared_ptr<const Config> candidate;
    // Follow the existing Status convention: InvalidArgument identifying the
    // field/component and reason. Each input reports its first validation
    // error.
    Status current_status;
    Status candidate_status;
    // Sorted by canonical path; values are omitted to avoid disclosing secrets.
    std::vector<ConfigChange> changes;

    // Only validates the documented scope below. Unchanged out-of-scope fields
    // are carried through; changes to them are always kUnsupported.
    // A valid plan is not permission to apply: inspect every disposition.
    bool valid() const { return current_status.ok() && candidate_status.ok(); }
};

// Side-effect-free analysis of two COMPLETE, already-loaded configurations,
// not a patch. Does not read files/environment, initialize services, change
// Config::get() compatibility, or publish/apply any runtime state.
//
// Validation scope: RPC hostname/port/threads, merge/failover policy, progress
// worker/queue enablement, runtime_queue fields consumed by construct(), and
// transport enable flags. Other fields (including policy/qos subtrees) remain
// unsupported for change, even when PR1 classifies them as runtime candidates.
//
// Known fields are compared with their consumer defaults; integer strings are
// accepted only for the RPC numbers, matching existing startup behavior.
// Backend enable flags retain absence because device/environment selection is
// outside this API. Explicitly enabling a backend requires build support.
// Conflicting flat/nested aliases are rejected, not resolved silently. Unknown
// fields use structural comparison. HP TCP enable must be nested because its
// dedicated parser consumes a transport object. Flat object aliases are outside
// this initial scope. Error messages never include input values.
// Limits: 32 path levels, 256 bytes/path, 4096 visited nodes, one Status per
// input and 128 changes; exceeding a limit makes the plan invalid
// (never a partial plan). kRuntimeCandidate is eligibility only, NOT a promise
// of live application.
ConfigChangePlan planTentConfigChange(
    const Config& current, const Config& candidate,
    const ConfigValidationContext& context = currentConfigValidationContext());

}  // namespace tent
}  // namespace mooncake

#endif  // TENT_CONFIG_VALIDATION_H
