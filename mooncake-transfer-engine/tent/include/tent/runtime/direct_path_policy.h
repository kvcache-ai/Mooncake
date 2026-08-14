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

#ifndef TENT_RUNTIME_DIRECT_PATH_POLICY_H_
#define TENT_RUNTIME_DIRECT_PATH_POLICY_H_

#include <cstdlib>
#include <string>

#include "tent/common/types.h"

namespace mooncake {
namespace tent {

enum class DirectPathMode { Disabled, Enabled, Auto };
enum class DirectPathDecision {
    UseScheduledPath,
    TryDirectPath,
    RequireDirectPath
};

class DirectPathPolicy {
   public:
    static DirectPathMode mode() {
        static const DirectPathMode cached_mode = [] {
            const char* env = std::getenv("MC_EXP_FORCE_DIRECT");
            if (!env || !*env) return DirectPathMode::Auto;
            const std::string value(env);
            if (value == "disabled" || value == "disable" ||
                value == "off" || value == "0") {
                return DirectPathMode::Disabled;
            }
            if (value == "enabled" || value == "enable" || value == "on" ||
                value == "1") {
                return DirectPathMode::Enabled;
            }
            return DirectPathMode::Auto;
        }();
        return cached_mode;
    }

    static bool prefersDirectPath(const Request& request) {
        return request.deadline_ns != 0 ||
               request.intent_type == IntentType::FOREGROUND_GET;
    }

    static DirectPathDecision decide(const Request& request) {
        switch (mode()) {
            case DirectPathMode::Disabled:
                return DirectPathDecision::UseScheduledPath;
            case DirectPathMode::Enabled:
                return DirectPathDecision::RequireDirectPath;
            case DirectPathMode::Auto:
                return prefersDirectPath(request)
                           ? DirectPathDecision::TryDirectPath
                           : DirectPathDecision::UseScheduledPath;
        }
        return DirectPathDecision::UseScheduledPath;
    }

    static bool shouldAttemptDirectPath(DirectPathDecision decision) {
        return decision != DirectPathDecision::UseScheduledPath;
    }

    static bool shouldAttemptDirectPath(const Request& request) {
        return shouldAttemptDirectPath(decide(request));
    }

    static bool requiresDirectPath(DirectPathDecision decision) {
        return decision == DirectPathDecision::RequireDirectPath;
    }

    static bool requiresDirectPath(const Request& request) {
        return requiresDirectPath(decide(request));
    }
};

}  // namespace tent
}  // namespace mooncake

#endif  // TENT_RUNTIME_DIRECT_PATH_POLICY_H_
