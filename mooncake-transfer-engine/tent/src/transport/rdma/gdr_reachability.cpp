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

namespace mooncake {
namespace tent {

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

bool GdrReachability::reachable(const std::string &key) {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    auto it = state_.find(key);
    if (it == state_.end()) return true;
    // Not paused, or the cooldown has expired: allow a (probe) request through.
    // The next success clears the entry; the next failure re-pauses with a
    // doubled cooldown.
    return std::chrono::steady_clock::now() >= it->second.resume_time;
}

void GdrReachability::markFailed(const std::string &key) {
    auto now = std::chrono::steady_clock::now();
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
        any_exclusion_.store(true, std::memory_order_relaxed);
    }
}

void GdrReachability::markRecovered(const std::string &key) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    state_.erase(key);
}

void GdrReachability::reportLocalFailure(const std::string &nic_name,
                                         int gpu_ordinal) {
    markFailed(localKey(nic_name, gpu_ordinal));
}

void GdrReachability::reportLocalSuccess(const std::string &nic_name,
                                         int gpu_ordinal) {
    markRecovered(localKey(nic_name, gpu_ordinal));
}

bool GdrReachability::localReachable(const std::string &nic_name,
                                     int gpu_ordinal) {
    return reachable(localKey(nic_name, gpu_ordinal));
}

void GdrReachability::reportRemoteFailure(const std::string &machine_id,
                                          const std::string &nic_name,
                                          int gpu_ordinal) {
    markFailed(remoteKey(machine_id, nic_name, gpu_ordinal));
}

void GdrReachability::reportRemoteSuccess(const std::string &machine_id,
                                          const std::string &nic_name,
                                          int gpu_ordinal) {
    markRecovered(remoteKey(machine_id, nic_name, gpu_ordinal));
}

bool GdrReachability::remoteReachable(const std::string &machine_id,
                                      const std::string &nic_name,
                                      int gpu_ordinal) {
    return reachable(remoteKey(machine_id, nic_name, gpu_ordinal));
}

}  // namespace tent
}  // namespace mooncake
