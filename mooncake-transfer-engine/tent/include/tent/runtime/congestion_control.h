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

#ifndef TENT_RUNTIME_CONGESTION_CONTROL_H
#define TENT_RUNTIME_CONGESTION_CONTROL_H

#include <cstdint>
#include <memory>

#include "tent/common/types.h"

namespace mooncake {
namespace tent {

/// Context passed to CongestionControlPlugin::onAdmit()
struct AdmitContext {
    const Request& request;
    uint64_t global_inflight_bytes;
    uint64_t device_inflight_bytes;
    int device_id;
    int priority;
};

/// Feedback event from transfer completion path
struct CompletionEvent {
    int device_id;
    uint64_t bytes;
    double latency_us;
    TransferStatusEnum status;
    int priority;
};

/// Congestion signal from transport layer
struct CongestionSignal {
    enum Type {
        TIMEOUT_SPIKE = 0,
        QP_ERROR = 1,
        LATENCY_SPIKE = 2,
        ECN_MARK = 3,
    };
    Type type;
    int device_id;
    uint64_t timestamp_ns;
    double severity;  // 0.0 to 1.0
};

/// Admission decision returned by the plugin
struct AdmitDecision {
    bool admit;                    // true = proceed, false = defer
    float defer_us;                // suggested defer duration in microseconds
    uint64_t device_mask_override; // 0 = no override to device selection
};

/// Abstract base class for congestion control plugins.
///
/// Concrete implementations (AIMD, BBR-style, topology-aware, etc.)
/// hook into the transfer pipeline at three points:
/// 1. Admission (pre-submit) — can reject/defer requests
/// 2. Completion (post-transfer) — receives latency/throughput feedback
/// 3. Congestion signal — receives timeout/error/ECN notifications
class CongestionControlPlugin {
   public:
    virtual ~CongestionControlPlugin() = default;

    /// Called before a transfer request is submitted to a transport.
    /// Must be fast (< 100ns typical) as it's on the hot path.
    virtual AdmitDecision onAdmit(const AdmitContext& ctx) = 0;

    /// Called after a transfer completes (success or failure).
    /// Used to update internal congestion state (EWMA, windows, etc.)
    virtual void onCompletion(const CompletionEvent& event) = 0;

    /// Called when a congestion signal is detected (timeout, QP error, etc.)
    virtual void onCongestionSignal(const CongestionSignal& signal) = 0;

    /// Query the current rate limit for a specific device.
    /// Returns bytes/sec. 0 means unlimited (no rate limiting).
    virtual uint64_t getDeviceRateLimit(int device_id) const = 0;

    /// Plugin name for logging and diagnostics.
    virtual const char* getName() const = 0;
};

/// Factory function to create the default no-op plugin.
/// The no-op plugin always admits, never rate-limits, and ignores all signals.
std::shared_ptr<CongestionControlPlugin> createDefaultCongestionControlPlugin();

}  // namespace tent
}  // namespace mooncake

#endif  // TENT_RUNTIME_CONGESTION_CONTROL_H
