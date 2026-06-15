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

#include "tent/runtime/aimd_congestion_control.h"

#include <algorithm>

namespace mooncake {
namespace tent {

AimdCongestionControlPlugin::AimdCongestionControlPlugin(
    const AimdConfig& config)
    : config_(config) {
    resetAllWindows();
}

AdmitDecision AimdCongestionControlPlugin::onAdmit(const AdmitContext& ctx) {
    if (ctx.device_id < 0 || ctx.device_id >= AimdConfig::kMaxDevices) {
        // Unknown device — always admit to avoid becoming a failure source.
        return {true, 0.0f, 0};
    }

    uint64_t window =
        window_bytes_[ctx.device_id].load(std::memory_order_relaxed);
    uint64_t inflight = ctx.device_inflight_bytes;
    uint64_t request_len = ctx.request.length;

    if (inflight + request_len > window) {
        // Over window — suggest deferral.
        return {false, config_.defer_us, 0};
    }
    return {true, 0.0f, 0};
}

void AimdCongestionControlPlugin::onCompletion(const CompletionEvent& event) {
    if (event.status != TransferStatusEnum::COMPLETED) {
        return;
    }
    if (event.device_id < 0 || event.device_id >= AimdConfig::kMaxDevices) {
        return;
    }

    // Additive Increase: grow window by one increment on success.
    auto& w = window_bytes_[event.device_id];
    uint64_t cur = w.load(std::memory_order_relaxed);
    uint64_t next = std::min(cur + config_.ai_increment_bytes,
                             config_.window_cap_bytes);
    w.store(next, std::memory_order_relaxed);
}

void AimdCongestionControlPlugin::onCongestionSignal(
    const CongestionSignal& signal) {
    if (signal.device_id < 0 ||
        signal.device_id >= AimdConfig::kMaxDevices) {
        return;
    }

    // Multiplicative Decrease: shrink window proportional to severity.
    auto& w = window_bytes_[signal.device_id];
    uint64_t cur = w.load(std::memory_order_relaxed);
    double factor = 1.0 - config_.md_factor * signal.severity;
    uint64_t reduced = static_cast<uint64_t>(cur * std::max(factor, 0.0));
    uint64_t clamped = std::max(reduced, config_.window_floor_bytes);
    w.store(clamped, std::memory_order_relaxed);
}

uint64_t AimdCongestionControlPlugin::getDeviceRateLimit(
    int /*device_id*/) const {
    // AIMD controls via window, not explicit rate limit.
    return 0;
}

const char* AimdCongestionControlPlugin::getName() const { return "aimd"; }

uint64_t AimdCongestionControlPlugin::getWindowBytes(int device_id) const {
    if (device_id < 0 || device_id >= AimdConfig::kMaxDevices) {
        return 0;
    }
    return window_bytes_[device_id].load(std::memory_order_relaxed);
}

void AimdCongestionControlPlugin::resetWindow(int device_id) {
    if (device_id >= 0 && device_id < AimdConfig::kMaxDevices) {
        window_bytes_[device_id].store(config_.initial_window_bytes,
                                       std::memory_order_relaxed);
    }
}

void AimdCongestionControlPlugin::resetAllWindows() {
    for (int i = 0; i < AimdConfig::kMaxDevices; ++i) {
        window_bytes_[i].store(config_.initial_window_bytes,
                               std::memory_order_relaxed);
    }
}

std::shared_ptr<CongestionControlPlugin> createAimdCongestionControlPlugin(
    const AimdConfig& config) {
    return std::make_shared<AimdCongestionControlPlugin>(config);
}

}  // namespace tent
}  // namespace mooncake
