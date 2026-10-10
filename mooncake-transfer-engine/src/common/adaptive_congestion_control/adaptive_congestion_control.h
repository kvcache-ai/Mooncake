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

#ifndef MOONCAKE_ADAPTIVE_CONGESTION_CONTROL_H
#define MOONCAKE_ADAPTIVE_CONGESTION_CONTROL_H

#include <atomic>
#include <cstdint>
#include <mutex>

namespace mooncake::adaptive_congestion_control {

struct CoreAccess;
struct PathHandle;
class Permit;

enum class Mode : uint8_t { kOff, kObserve, kEnforce };
enum class Decision : uint8_t { kAllow, kDefer, kAvoid };
enum class PathState : uint8_t {
    kHealthy,
    kCongested,
    kSuspect,
    kQuarantined,
    kProbing,
};
enum class OutcomeClass : uint8_t {
    kSuccess,
    kCongestion,
    kReceiverPressure,
    kRouteTimeout,
    kLocalConfiguration,
    kRemoteMetadata,
    kDerivedFlush,
    kFatal,
};
enum class FailureScope : uint8_t {
    kOperation,
    kQp,
    kCq,
    kRoute,
    kPort,
    kDevice,
};

struct Config {
    Mode mode = Mode::kOff;
    uint64_t min_window_bytes = 4ULL << 20;
    uint64_t max_window_bytes = 64ULL << 20;
    uint64_t target_drain_time_ns = 2'000'000;
    uint32_t high_pressure_epochs = 2;
    uint32_t low_pressure_epochs = 3;
    uint32_t hard_error_threshold = 3;
    uint64_t cooldown_ns = 30'000'000'000ULL;
    uint64_t probe_window_bytes = 64ULL << 10;
};

struct Signals {
    uint64_t backlog_bytes = 0;
    uint64_t delivery_rate_bytes_per_sec = 0;
    uint64_t completed_bytes = 0;
    uint64_t successful_completions = 0;
    uint64_t receiver_pressure = 0;
    uint64_t route_timeouts = 0;
    uint64_t hard_errors = 0;
    uint64_t fatal_failures = 0;
    bool poller_stalled = false;
};

struct Snapshot {
    Mode mode = Mode::kOff;
    PathState state = PathState::kHealthy;
    uint64_t window_bytes = 0;
    uint64_t inflight_bytes = 0;
    uint64_t deferred_bytes = 0;
    uint32_t generation = 0;
};

class DomainState {
   public:
    explicit DomainState(Config config, uint32_t generation = 1);

    DomainState(const DomainState&) = delete;
    DomainState& operator=(const DomainState&) = delete;

   private:
    friend struct CoreAccess;
    friend class Permit;
    friend Decision tryAcquire(const struct PathHandle&, uint64_t, Permit&);
    friend bool complete(Permit&, OutcomeClass, FailureScope);
    friend void recordSignals(DomainState&, uint32_t, const Signals&);
    friend void controlTick(DomainState&, uint64_t);
    friend void resetGeneration(DomainState&, uint32_t);
    friend uint32_t generation(const DomainState&);
    friend Snapshot snapshot(const DomainState&);

    Config config_;
    std::atomic<PathState> state_{PathState::kHealthy};
    std::atomic<uint64_t> window_bytes_{0};
    std::atomic<uint64_t> inflight_bytes_{0};
    std::atomic<uint64_t> deferred_bytes_{0};
    std::atomic<uint32_t> generation_{1};

    std::atomic<uint32_t> feedback_generation_{1};
    std::atomic<uint32_t> feedback_writers_{0};
    std::atomic<uint64_t> backlog_bytes_{0};
    std::atomic<uint64_t> delivery_rate_bytes_per_sec_{0};
    std::atomic<bool> has_rate_sample_{false};
    std::atomic<uint64_t> completed_bytes_{0};
    std::atomic<uint64_t> successful_completions_{0};
    std::atomic<uint64_t> receiver_pressure_{0};
    std::atomic<uint64_t> route_timeouts_{0};
    std::atomic<uint64_t> hard_errors_{0};
    std::atomic<uint64_t> fatal_failures_{0};
    std::atomic<uint64_t> probe_successes_{0};
    std::atomic<uint64_t> probe_failures_{0};
    std::atomic<uint64_t> probe_epoch_{0};
    std::atomic<bool> poller_stalled_{false};

    std::mutex probe_mutex_;
    uint64_t active_probe_permits_ = 0;
    bool probe_draining_ = false;

    uint32_t high_pressure_streak_ = 0;
    uint32_t low_pressure_streak_ = 0;
    uint32_t hard_error_streak_ = 0;
    uint64_t quarantine_until_ns_ = 0;
};

// DomainState instances must outlive their PathHandle and every active Permit.
// Adapters should own domains at worker or controller scope, never endpoint
// scope.
struct PathHandle {
    DomainState* device = nullptr;
    DomainState* route = nullptr;
    uint32_t device_generation = 0;
    uint32_t route_generation = 0;
};

class Permit {
   public:
    Permit() = default;

    Permit(const Permit&) = delete;
    Permit& operator=(const Permit&) = delete;
    Permit(Permit&& other) noexcept;
    Permit& operator=(Permit&& other) noexcept;

    bool active() const { return active_.load(std::memory_order_acquire); }

   private:
    friend Decision tryAcquire(const PathHandle&, uint64_t, Permit&);
    friend bool complete(Permit&, OutcomeClass, FailureScope);

    std::atomic<bool> active_{false};
    DomainState* device_ = nullptr;
    DomainState* route_ = nullptr;
    uint64_t bytes_ = 0;
    uint32_t device_generation_ = 0;
    uint32_t route_generation_ = 0;
    bool device_reserved_ = false;
    bool route_reserved_ = false;
    uint64_t device_probe_epoch_ = 0;
    uint64_t route_probe_epoch_ = 0;

    void moveFrom(Permit& other) noexcept;
};

// A domain with no inflight bytes admits one indivisible slice even when it
// exceeds the window (including probing). The full byte count is reserved;
// transport-level static limits must still be enforced by the caller.
Decision tryAcquire(const PathHandle& path, uint64_t bytes, Permit& permit);
bool complete(Permit& permit, OutcomeClass outcome, FailureScope scope);
void recordSignals(DomainState& state, uint32_t generation,
                   const Signals& signals);
void controlTick(DomainState& state, uint64_t now_ns);
void resetGeneration(DomainState& state, uint32_t generation);
uint32_t generation(const DomainState& state);
Snapshot snapshot(const DomainState& state);

}  // namespace mooncake::adaptive_congestion_control

#endif  // MOONCAKE_ADAPTIVE_CONGESTION_CONTROL_H
