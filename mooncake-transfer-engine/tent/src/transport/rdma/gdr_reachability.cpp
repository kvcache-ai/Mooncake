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

#include "tent/transport/rdma/gdr_reachability.h"

#include <glog/logging.h>

#include "tent/common/config.h"
#if TENT_METRICS_ENABLED
#include "tent/metrics/tent_metrics.h"
#endif

namespace mooncake {
namespace tent {
namespace {

const char *gdrScopeName(bool remote) { return remote ? "remote" : "local"; }

}  // namespace

std::atomic<bool> GdrReachability::any_exclusion_{false};

GdrReachability &GdrReachability::instance() {
    static GdrReachability inst;
    return inst;
}

void GdrReachability::configure(const Config *conf) {
    if (!conf) return;
    std::unique_lock<std::shared_mutex> lock(mutex_);
    error_threshold_ =
        conf->get("transports/rdma/gdr_error_threshold", error_threshold_);
    error_window_ = std::chrono::seconds(conf->get(
        "transports/rdma/gdr_error_window_secs", (int)error_window_.count()));
    cooldown_ = std::chrono::seconds(
        conf->get("transports/rdma/gdr_cooldown_secs", (int)cooldown_.count()));
    if (error_threshold_ < 1) error_threshold_ = 1;
}

std::string GdrReachability::localKey(const std::string &nic_name,
                                      int gpu_ordinal) {
    return "L|" + nic_name + "|" + std::to_string(gpu_ordinal);
}

std::string GdrReachability::remoteKey(const std::string &machine_id,
                                       const std::string &nic_name,
                                       int gpu_ordinal) {
    return "R|" + machine_id + "|" + nic_name + "|" +
           std::to_string(gpu_ordinal);
}

bool GdrReachability::reachable(const std::string &key, bool remote) {
    const auto now = std::chrono::steady_clock::now();
    {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        auto it = state_.find(key);
        if (it == state_.end() || !it->second.excluded) return true;
        if (now < it->second.resume_time) return false;
    }

    // Upgrade only for the first selector that observes an expired cooldown.
    // The state stays in the map so a failed probe can reuse the backoff.
    bool probe_allowed = false;
    {
        std::unique_lock<std::shared_mutex> lock(mutex_);
        auto it = state_.find(key);
        if (it == state_.end() || !it->second.excluded) return true;
        if (std::chrono::steady_clock::now() < it->second.resume_time)
            return false;
        it->second.excluded = false;
        probe_allowed = true;
    }
    if (probe_allowed)
        LOG(INFO) << "event=tent.rdma.gdr_probe_allowed scope="
                  << gdrScopeName(remote) << " path_key=" << key;
#if TENT_METRICS_ENABLED
    if (probe_allowed) {
        const auto scope = remote ? TentMetrics::RdmaGdrScope::Remote
                                  : TentMetrics::RdmaGdrScope::Local;
        TentMetrics::instance().recordRdmaGdrExcludedPath(scope, -1);
        TentMetrics::instance().recordRdmaGdrEvent(
            scope, TentMetrics::RdmaGdrEvent::ProbeAllowed);
    }
#endif
    return true;
}

void GdrReachability::markFailed(const std::string &key, bool remote) {
    auto now = std::chrono::steady_clock::now();
    bool newly_excluded = false;
    std::chrono::steady_clock::duration exclusion_cooldown{};
    {
        std::unique_lock<std::shared_mutex> lock(mutex_);
        auto &st = state_[key];
        if (st.error_count == 0 || now - st.last_error > error_window_) {
            st.error_count = 1;
        } else {
            st.error_count++;
        }
        st.last_error = now;
        if (st.cooldown.count() == 0) {
            st.cooldown = cooldown_;
        } else {
            st.cooldown *= 2;
            if (st.cooldown > kMaxCooldown) st.cooldown = kMaxCooldown;
        }
        if ((int)st.error_count >= error_threshold_) {
            st.resume_time = now + st.cooldown;
            if (!st.excluded) {
                st.excluded = true;
                st.was_excluded = true;
                newly_excluded = true;
                exclusion_cooldown = st.cooldown;
            }
            any_exclusion_.store(true, std::memory_order_relaxed);
        }
    }
    if (newly_excluded) {
        LOG(WARNING) << "event=tent.rdma.gdr_excluded scope="
                     << gdrScopeName(remote) << " path_key=" << key
                     << " cooldown_ms="
                     << std::chrono::duration_cast<std::chrono::milliseconds>(
                            exclusion_cooldown)
                            .count();
    }
#if TENT_METRICS_ENABLED
    const auto scope = remote ? TentMetrics::RdmaGdrScope::Remote
                              : TentMetrics::RdmaGdrScope::Local;
    TentMetrics::instance().recordRdmaGdrEvent(
        scope, TentMetrics::RdmaGdrEvent::FailureObserved);
    if (newly_excluded) {
        TentMetrics::instance().recordRdmaGdrExcludedPath(scope, 1);
        TentMetrics::instance().recordRdmaGdrEvent(
            scope, TentMetrics::RdmaGdrEvent::Excluded);
    }
#endif
}

void GdrReachability::markRecovered(const std::string &key, bool remote) {
    bool was_excluded = false;
#if TENT_METRICS_ENABLED
    bool still_excluded = false;
#endif
    {
        std::unique_lock<std::shared_mutex> lock(mutex_);
        auto it = state_.find(key);
        if (it == state_.end()) return;
        was_excluded = it->second.was_excluded;
#if TENT_METRICS_ENABLED
        still_excluded = it->second.excluded;
#endif
        state_.erase(it);
    }
    if (was_excluded)
        LOG(INFO) << "event=tent.rdma.gdr_recovered scope="
                  << gdrScopeName(remote) << " path_key=" << key;
#if TENT_METRICS_ENABLED
    if (was_excluded) {
        const auto scope = remote ? TentMetrics::RdmaGdrScope::Remote
                                  : TentMetrics::RdmaGdrScope::Local;
        if (still_excluded)
            TentMetrics::instance().recordRdmaGdrExcludedPath(scope, -1);
        TentMetrics::instance().recordRdmaGdrEvent(
            scope, TentMetrics::RdmaGdrEvent::Recovered);
    }
#endif
}

void GdrReachability::reportLocalFailure(const std::string &nic_name,
                                         int gpu_ordinal) {
    markFailed(localKey(nic_name, gpu_ordinal), false);
}

void GdrReachability::reportLocalSuccess(const std::string &nic_name,
                                         int gpu_ordinal) {
    markRecovered(localKey(nic_name, gpu_ordinal), false);
}

bool GdrReachability::localReachable(const std::string &nic_name,
                                     int gpu_ordinal) {
    return reachable(localKey(nic_name, gpu_ordinal), false);
}

void GdrReachability::reportRemoteFailure(const std::string &machine_id,
                                          const std::string &nic_name,
                                          int gpu_ordinal) {
    markFailed(remoteKey(machine_id, nic_name, gpu_ordinal), true);
}

void GdrReachability::reportRemoteSuccess(const std::string &machine_id,
                                          const std::string &nic_name,
                                          int gpu_ordinal) {
    markRecovered(remoteKey(machine_id, nic_name, gpu_ordinal), true);
}

bool GdrReachability::remoteReachable(const std::string &machine_id,
                                      const std::string &nic_name,
                                      int gpu_ordinal) {
    return reachable(remoteKey(machine_id, nic_name, gpu_ordinal), true);
}

}  // namespace tent
}  // namespace mooncake
