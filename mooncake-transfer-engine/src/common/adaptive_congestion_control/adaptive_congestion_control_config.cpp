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

#include "adaptive_congestion_control_config.h"

#include <charconv>
#include <cstdlib>
#include <limits>
#include <string_view>

namespace mooncake::adaptive_cc {
namespace {

ConfigLoadResult invalid(const char* reason) {
    ConfigLoadResult result;
    result.valid = false;
    result.error = reason;
    return result;
}

bool parsePositive(const char* name, uint64_t& output) {
    const char* value = std::getenv(name);
    if (value == nullptr) return true;
    const std::string_view text(value);
    uint64_t parsed = 0;
    const auto result =
        std::from_chars(text.data(), text.data() + text.size(), parsed);
    if (text.empty() || result.ec != std::errc{} ||
        result.ptr != text.data() + text.size() || parsed == 0) {
        return false;
    }
    output = parsed;
    return true;
}

bool parsePositiveCount(const char* name, uint32_t& output) {
    uint64_t parsed = output;
    if (!parsePositive(name, parsed) ||
        parsed > std::numeric_limits<uint32_t>::max()) {
        return false;
    }
    output = static_cast<uint32_t>(parsed);
    return true;
}

}  // namespace

ConfigLoadResult loadConfigFromEnvironment() {
    ConfigLoadResult result;
    if (const char* mode = std::getenv("MC_ADAPTIVE_CC_MODE")) {
        const std::string_view value(mode);
        if (value == "off") {
            result.config.mode = Mode::kOff;
        } else if (value == "observe") {
            result.config.mode = Mode::kObserve;
        } else if (value == "enforce") {
            result.config.mode = Mode::kEnforce;
        } else {
            return invalid("MC_ADAPTIVE_CC_MODE");
        }
    }

    if (!parsePositive("MC_ADAPTIVE_CC_MIN_WINDOW_BYTES",
                       result.config.min_window_bytes)) {
        return invalid("MC_ADAPTIVE_CC_MIN_WINDOW_BYTES");
    }
    if (!parsePositive("MC_ADAPTIVE_CC_MAX_WINDOW_BYTES",
                       result.config.max_window_bytes)) {
        return invalid("MC_ADAPTIVE_CC_MAX_WINDOW_BYTES");
    }
    if (result.config.min_window_bytes > result.config.max_window_bytes) {
        return invalid("adaptive congestion window range");
    }

    uint64_t target_drain_us = result.config.target_drain_time_ns / 1000;
    if (!parsePositive("MC_ADAPTIVE_CC_TARGET_DRAIN_US", target_drain_us) ||
        target_drain_us > std::numeric_limits<uint64_t>::max() / 1000) {
        return invalid("MC_ADAPTIVE_CC_TARGET_DRAIN_US");
    }
    result.config.target_drain_time_ns = target_drain_us * 1000;

    if (!parsePositiveCount("MC_ADAPTIVE_CC_HIGH_PRESSURE_EPOCHS",
                            result.config.high_pressure_epochs)) {
        return invalid("MC_ADAPTIVE_CC_HIGH_PRESSURE_EPOCHS");
    }
    if (!parsePositiveCount("MC_ADAPTIVE_CC_LOW_PRESSURE_EPOCHS",
                            result.config.low_pressure_epochs)) {
        return invalid("MC_ADAPTIVE_CC_LOW_PRESSURE_EPOCHS");
    }
    if (!parsePositiveCount("MC_ADAPTIVE_CC_HARD_ERROR_THRESHOLD",
                            result.config.hard_error_threshold)) {
        return invalid("MC_ADAPTIVE_CC_HARD_ERROR_THRESHOLD");
    }

    uint64_t cooldown_ms = result.config.cooldown_ns / 1'000'000;
    if (!parsePositive("MC_ADAPTIVE_CC_COOLDOWN_MS", cooldown_ms) ||
        cooldown_ms > std::numeric_limits<uint64_t>::max() / 1'000'000) {
        return invalid("MC_ADAPTIVE_CC_COOLDOWN_MS");
    }
    result.config.cooldown_ns = cooldown_ms * 1'000'000;

    constexpr const char* probe_name = "MC_ADAPTIVE_CC_PROBE_WINDOW_BYTES";
    const bool probe_override = std::getenv(probe_name) != nullptr;
    if (!parsePositive(probe_name, result.config.probe_window_bytes)) {
        return invalid(probe_name);
    }
    if (probe_override &&
        result.config.probe_window_bytes > result.config.min_window_bytes) {
        return invalid("adaptive congestion probe window range");
    }
    return result;
}

}  // namespace mooncake::adaptive_cc
