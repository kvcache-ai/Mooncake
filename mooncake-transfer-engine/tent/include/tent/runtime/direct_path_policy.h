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

#include <cstddef>
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
    static constexpr size_t kDirectPathSmallRequestMaxBytes =
        1ULL * 1024 * 1024;
    static constexpr size_t kDirectPathLargeRequestMinBytes =
        8ULL * 1024 * 1024;

    static int priorityForIntent(IntentType intent) {
        switch (intent) {
            case IntentType::FOREGROUND_GET:
                return PRIO_HIGH;
            case IntentType::BACKGROUND_PREFETCH:
            case IntentType::MIGRATION:
            case IntentType::CHECKPOINT:
                return PRIO_LOW;
            case IntentType::WEIGHT_LOADING:
                return PRIO_MEDIUM;
            case IntentType::STAGING_INTERNAL:
            case IntentType::INTENT_UNSPEC:
                return PRIO_HIGH;
        }
        return PRIO_HIGH;
    }

    static int priorityForRequest(const Request& request) {
        if (request.priority != PRIO_UNSPEC) return request.priority;
        if (request.deadline_ns != 0) return PRIO_HIGH;
        return priorityForIntent(request.intent_type);
    }

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

    static bool hasLatencySignal(const Request& request) {
        return request.deadline_ns != 0 ||
               request.intent_type == IntentType::FOREGROUND_GET;
    }

    static bool hasThroughputSignal(const Request& request) {
        switch (request.intent_type) {
            case IntentType::BACKGROUND_PREFETCH:
            case IntentType::MIGRATION:
            case IntentType::CHECKPOINT:
            case IntentType::WEIGHT_LOADING:
            case IntentType::STAGING_INTERNAL:
                return true;
            case IntentType::INTENT_UNSPEC:
            case IntentType::FOREGROUND_GET:
                return false;
        }
        return false;
    }

    static bool prefersDirectPath(const Request& request) {
        return hasLatencySignal(request) &&
               request.length <= kDirectPathSmallRequestMaxBytes;
    }

    static DirectPathDecision decideAuto(const Request& request) {
        if (hasThroughputSignal(request))
            return DirectPathDecision::UseScheduledPath;
        if (request.length >= kDirectPathLargeRequestMinBytes)
            return DirectPathDecision::UseScheduledPath;
        return prefersDirectPath(request)
                   ? DirectPathDecision::TryDirectPath
                   : DirectPathDecision::UseScheduledPath;
    }

    static DirectPathDecision decide(const Request& request) {
        switch (mode()) {
            case DirectPathMode::Disabled:
                return DirectPathDecision::UseScheduledPath;
            case DirectPathMode::Enabled:
                return DirectPathDecision::RequireDirectPath;
            case DirectPathMode::Auto:
                return decideAuto(request);
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
