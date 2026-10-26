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

#ifndef TENT_RAIL_MONITOR_H
#define TENT_RAIL_MONITOR_H

#include <mutex>

#include "tent/common/config.h"
#include "tent/common/status.h"
#include "tent/runtime/topology.h"

namespace mooncake {
namespace tent {
// Each worker owns a monitor, but a shared CQ can deliver its completions
// on another worker. Protect admission, completion updates and the derived
// mappings together; topology snapshot refresh remains owner-driven.
class RailMonitor {
    const static size_t kMaxNuma = 16;

    // Upper bound on the exponential-backoff cooldown; prevents the
    // per-rail pause from growing without bound under repeated failure.
    static constexpr std::chrono::seconds kMaxCooldown{300};

   public:
    // Config keys. Exposed as constants so callers (and docs) reference
    // a single source of truth instead of duplicating the string path.
    static constexpr const char *kCfgErrorThreshold =
        "transports/rdma/rail_error_threshold";
    static constexpr const char *kCfgErrorWindowSecs =
        "transports/rdma/rail_error_window_secs";
    static constexpr const char *kCfgCooldownSecs =
        "transports/rdma/rail_cooldown_secs";
    static constexpr const char *kCfgProbeIntervalSecs =
        "transports/rdma/rail_probe_interval_secs";

   public:
    RailMonitor() = default;

    ~RailMonitor() = default;

    RailMonitor(const RailMonitor &) = delete;
    RailMonitor &operator=(const RailMonitor &) = delete;

   public:
    // The shared topology references keep segment snapshots alive while rail
    // failure handling uses their derived mappings.
    // rail_topo_json: optional JSON string describing rail topology.
    // conf: optional Config pointer; when non-null, overrides the default
    //   error_threshold / error_window_secs / cooldown_secs values via
    //   kCfgErrorThreshold / kCfgErrorWindowSecs / kCfgCooldownSecs.
    // A later call with the same NIC/memory layout only refreshes the pins
    // (segment COW snapshots); it does not rebuild rail_states_ or reprint
    // the config banner.
    Status load(std::shared_ptr<const Topology> local,
                std::shared_ptr<const Topology> remote,
                const std::string &rail_topo_json = "",
                const Config *conf = nullptr);

    bool ready() const {
        std::lock_guard<std::mutex> guard(mutex_);
        return ready_;
    }

    bool isAvailable(int local_nic, int remote_nic) const;

    // Static compatibility only: a configured pair must reach an RDMA NIC in
    // the target memory entry. Paused rails remain candidates so worker-side
    // admit() can probe recovery; this query never arms or consumes a probe.
    // Same-host same-NIC loopback remains legal, as in worker fallback.
    uint64_t localDeviceMask(const Topology::MemEntry &remote_memory,
                             bool same_machine = false) const;

    // Mutating admit: the ONLY path that arms a probe/trial. Returns true when
    // a transfer may use this rail -- Closed (no-op), or an exploratory probe /
    // Half-Open trial. Sets probe_in_flight for paused rails so a second probe
    // cannot race the first while it is on the wire. Callers that select a rail
    // but fail to post must call cancelProbe() to clear the flag.
    //
    // Admission and cross-worker completion updates share mutex_. The returned
    // generation must accompany completion/cancellation: an older request
    // cannot resolve a newer recovery probe or a rebuilt topology. Failed
    // admission returns zero and conveys no ownership.
    // A denied admission reports probe_pending for an outstanding probe or
    // work queued before the last probe/pause. This keeps late-scheduled work
    // in the same recovery attempt. probe_delayed reports the interval alone;
    // callers decide whether a retried request should wait for it. All are
    // sampled under the same state lock; an unset queued_at opts out.
    bool admit(int local_nic, int remote_nic, uint64_t &admission,
               bool *probe_pending = nullptr, bool *probe_delayed = nullptr,
               std::chrono::steady_clock::time_point queued_at = {});

    // A rejected admission can wait for an already admitted recovery probe.
    // This query neither admits another probe nor cancels the existing one.
    bool probeInFlight(int local_nic, int remote_nic) const;

    void markFailed(int local_nic, int remote_nic, uint64_t admission);

    void markRecovered(int local_nic, int remote_nic, uint64_t admission);

    void cancelProbe(int local_nic, int remote_nic, uint64_t admission);

    int findBestRemoteDevice(int local_nic, int remote_numa);

    const Topology *local() const { return local_.get(); }

    const Topology *remote() const { return remote_.get(); }

   private:
    bool isAvailableUnlocked(int local_nic, int remote_nic) const;

    Status loadFromJson(const std::string &rail_topo_json);

    Status loadDefault();

    void updateBestMapping();

   private:
    mutable std::mutex mutex_;
    bool ready_{false};
    std::shared_ptr<const Topology> local_;
    std::shared_ptr<const Topology> remote_;

    struct PairHash {
        std::size_t operator()(const std::pair<int, int> &p) const noexcept {
            return std::hash<int>()(p.first) ^
                   (std::hash<int>()(p.second) << 1);
        }
    };

    struct RailState {
        uint64_t generation = 0;
        int error_count = 0;
        std::chrono::seconds cooldown{0};
        std::chrono::steady_clock::time_point last_error{};
        std::chrono::steady_clock::time_point resume_time{};
        std::chrono::steady_clock::time_point last_probe_time{};
        bool half_open = false;
        bool probe_in_flight = false;

        bool paused() const {
            return resume_time != std::chrono::steady_clock::time_point{};
        }
    };

    std::unordered_map<std::pair<int, int>, RailState, PairHash> rail_states_;
    std::unordered_map<int, int> direct_rails_;  // keep static after loaded
    std::unordered_map<int, int> best_mapping_[kMaxNuma];

    // Never reset on topology reload; NIC IDs may be reused.
    uint64_t next_generation_ = 0;
    int error_threshold_ = 3;
    std::chrono::seconds error_window_{10};
    std::chrono::seconds cooldown_{30};
    std::chrono::seconds probe_interval_{1};
};

}  // namespace tent
}  // namespace mooncake

#endif  // TENT_RAIL_MONITOR_H
