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

#ifndef TENT_RUNTIME_AIMD_CONGESTION_CONTROL_H
#define TENT_RUNTIME_AIMD_CONGESTION_CONTROL_H

#include <array>
#include <atomic>
#include <cstdint>

#include "tent/runtime/congestion_control.h"

namespace mooncake {
namespace tent {

/// Configuration parameters for the AIMD congestion control plugin.
struct AimdConfig {
    /// Initial congestion window per device in bytes.
    uint64_t initial_window_bytes = 64 * 1024 * 1024;  // 64 MB

    /// Maximum congestion window per device in bytes.
    uint64_t window_cap_bytes = 256 * 1024 * 1024;  // 256 MB

    /// Minimum congestion window per device in bytes (floor).
    uint64_t window_floor_bytes = 64 * 1024;  // 64 KB

    /// Additive increase increment per successful completion in bytes.
    uint64_t ai_increment_bytes = 4 * 1024;  // 4 KB (one MTU)

    /// Multiplicative decrease factor on congestion signal.
    /// Window is reduced to window * (1.0 - md_factor * severity).
    double md_factor = 0.5;

    /// Suggested defer duration in microseconds when admission is denied.
    float defer_us = 50.0f;

    /// Maximum number of devices supported.
    static constexpr int kMaxDevices = 64;
};

/// AIMD (Additive Increase, Multiplicative Decrease) congestion control plugin.
///
/// Maintains a per-device congestion window. On each successful transfer
/// completion, the window grows by a fixed increment (additive increase).
/// On congestion signals (timeout, QP error), the window shrinks by a
/// factor proportional to signal severity (multiplicative decrease).
///
/// Thread-safe: all state uses std::atomic with relaxed ordering.
class AimdCongestionControlPlugin : public CongestionControlPlugin {
   public:
    explicit AimdCongestionControlPlugin(const AimdConfig& config = {});

    AdmitDecision onAdmit(const AdmitContext& ctx) override;
    void onCompletion(const CompletionEvent& event) override;
    void onCongestionSignal(const CongestionSignal& signal) override;
    uint64_t getDeviceRateLimit(int device_id) const override;
    const char* getName() const override;

    /// Get the current window size for a device (for diagnostics/metrics).
    uint64_t getWindowBytes(int device_id) const;

    /// Reset the window for a specific device to the initial value.
    void resetWindow(int device_id);

    /// Reset all device windows to the initial value.
    void resetAllWindows();

   private:
    AimdConfig config_;
    std::array<std::atomic<uint64_t>, AimdConfig::kMaxDevices> window_bytes_;
};

/// Factory function to create an AIMD plugin with the given config.
std::shared_ptr<CongestionControlPlugin> createAimdCongestionControlPlugin(
    const AimdConfig& config = {});

}  // namespace tent
}  // namespace mooncake

#endif  // TENT_RUNTIME_AIMD_CONGESTION_CONTROL_H
