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

#include "tent/common/config.h"
#include "tent/common/status.h"
#include "tent/runtime/topology.h"

namespace mooncake {
namespace tent {
// Not thread-safe. Each RDMA worker owns its own RailMonitor via
// WorkerContext::rails, so markFailed / markRecovered / available / load
// all execute on a single worker thread. Do not share across threads
// without adding external synchronization.
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

   public:
    RailMonitor() = default;

    ~RailMonitor() = default;

    RailMonitor(const RailMonitor &) = delete;
    RailMonitor &operator=(const RailMonitor &) = delete;

   public:
    // rail_topo_json: optional JSON string describing rail topology.
    // conf: optional Config pointer; when non-null, overrides the default
    //   error_threshold / error_window_secs / cooldown_secs values via
    //   kCfgErrorThreshold / kCfgErrorWindowSecs / kCfgCooldownSecs.
    Status load(const Topology *local, const Topology *remote,
                const std::string &rail_topo_json = "",
                const Config *conf = nullptr);

    bool ready() { return ready_; }

    bool available(int local_nic, int remote_nic);

    void markFailed(int local_nic, int remote_nic);

    void markRecovered(int local_nic, int remote_nic);

    int findBestRemoteDevice(int local_nic, int remote_numa);

    const Topology *remote() { return remote_; }

   private:
    Status loadFromJson(const std::string &rail_topo_json);

    Status loadDefault();

    void updateBestMapping();

   private:
    bool ready_{false};
    const Topology *local_{nullptr};
    const Topology *remote_{nullptr};

    struct PairHash {
        std::size_t operator()(const std::pair<int, int> &p) const noexcept {
            return std::hash<int>()(p.first) ^
                   (std::hash<int>()(p.second) << 1);
        }
    };

    struct RailState {
        int error_count = 0;
        std::chrono::seconds cooldown{0};
        std::chrono::steady_clock::time_point last_error{};
        std::chrono::steady_clock::time_point resume_time{};

        // Derived: a rail is paused iff a resume_time has been armed.
        bool paused() const {
            return resume_time != std::chrono::steady_clock::time_point{};
        }
    };

    std::unordered_map<std::pair<int, int>, RailState, PairHash> rail_states_;
    std::unordered_map<int, int> direct_rails_;  // keep static after loaded
    std::unordered_map<int, int> best_mapping_[kMaxNuma];

    int error_threshold_ = 3;
    std::chrono::seconds error_window_{10};
    std::chrono::seconds cooldown_{30};
};

}  // namespace tent
}  // namespace mooncake

#endif  // TENT_RAIL_MONITOR_H