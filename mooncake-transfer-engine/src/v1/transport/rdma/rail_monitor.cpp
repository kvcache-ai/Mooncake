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

#include "v1/transport/rdma/rail_monitor.h"

namespace mooncake {
namespace v1 {

Status RailMonitor::load(const Topology *local, const Topology *remote,
                         const std::string &rail_topo_json) {
    local_ = local;
    remote_ = remote;
    if (!rail_topo_json.empty()) {
        auto status = loadFromJson(rail_topo_json);
        if (status.ok()) return status;
        LOG(WARNING) << "Failed to parse rail topo json: " << status.ToString();
    }
    return loadDefault();
}

bool RailMonitor::available(int local_nic, int remote_nic) {
    auto it = rail_states_.find(std::make_pair(local_nic, remote_nic));
    if (it == rail_states_.end()) return false;
    auto &st = it->second;
    if (st.paused) {
        auto now = std::chrono::steady_clock::now();
        if (now >= st.resume_time) {
            st.paused = false;
            st.error_count = 0;
            updateBestMapping();
            return true;
        }
        return false;
    }
    return true;
}

void RailMonitor::markFailed(int local_nic, int remote_nic) {
    auto it = rail_states_.find(std::make_pair(local_nic, remote_nic));
    if (it == rail_states_.end()) return;
    auto &st = it->second;
    auto now = std::chrono::steady_clock::now();
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
        if (st.cooldown > std::chrono::seconds(300))
            st.cooldown = std::chrono::seconds(300);
    }
    if (st.error_count >= error_threshold_) {
        st.paused = true;
        st.resume_time = now + st.cooldown;
        updateBestMapping();
    }
}

void RailMonitor::markRecovered(int local_nic, int remote_nic) {
    auto it = rail_states_.find(std::make_pair(local_nic, remote_nic));
    if (it == rail_states_.end()) return;
    auto &st = it->second;
    st.error_count = 0;
    st.paused = false;
    st.resume_time = {};
    updateBestMapping();
}

int RailMonitor::findBestRemoteDevice(int local_nic, int remote_numa) {
    if (remote_numa >= 0 && remote_numa < (int)kMaxNuma) {
        if (best_mapping_[remote_numa].count(local_nic))
            return best_mapping_[remote_numa][local_nic];
        else
            return -1;
    }
    for (remote_numa = 0; remote_numa < (int)kMaxNuma; ++remote_numa) {
        if (best_mapping_[remote_numa].count(local_nic))
            return best_mapping_[remote_numa][local_nic];
    }
    return -1;
}

/**
 * {
 *   "all": [
 *       {"local": "mlx5_0", "remote": "mlx5_1"},
 *       {"local": "mlx5_0", "remote": "mlx5_2"},
 *       {"local": "mlx5_1", "remote": "mlx5_0"}
 *   ],
 *   "direct": [
 *       {"local": "mlx5_0", "remote": "mlx5_1"},
 *       {"local": "mlx5_1", "remote": "mlx5_0"}
 *   ]
 * }
 */
Status RailMonitor::loadFromJson(const std::string &rail_topo_json) {
    Json::CharReaderBuilder builder;
    Json::Value root;
    std::string errs;
    std::istringstream iss(rail_topo_json);
    if (!Json::parseFromStream(builder, iss, &root, &errs)) {
        LOG(ERROR) << "Failed to parse JSON data: " << errs;
        return Status::InvalidArgument("Failed to parse JSON" LOC_MARK);
    }

    try {
        rail_states_.clear();
        if (root.isMember("all")) {
            for (const auto &entry : root["all"]) {
                if (!entry.isMember("local") || !entry.isMember("remote"))
                    continue;
                int local_nic = local_->getNicID(entry["local"].asString());
                int remote_nic = remote_->getNicID(entry["remote"].asString());
                rail_states_[{local_nic, remote_nic}] = RailState{};
            }
        }

        if (root.isMember("direct")) {
            for (const auto &entry : root["direct"]) {
                if (!entry.isMember("local") || !entry.isMember("remote"))
                    continue;
                int local_nic = local_->getNicID(entry["local"].asString());
                int remote_nic = remote_->getNicID(entry["remote"].asString());
                direct_rails_[local_nic] = remote_nic;
            }
        }
    } catch (const std::exception &e) {
        LOG(ERROR) << "Failed to parse JSON data: " << e.what();
        return Status::InvalidArgument("Failed to parse JSON" LOC_MARK);
    }

    ready_ = true;
    updateBestMapping();
    return Status::OK();
}

Status RailMonitor::loadDefault() {
    const int local_count = static_cast<int>(local_->getNicList().size());
    const int remote_count = static_cast<int>(remote_->getNicList().size());
    rail_states_.clear();
    direct_rails_.clear();
    std::vector<int> remote_load(remote_count, 0);

    for (int local_nic = 0; local_nic < local_count; ++local_nic) {
        for (int remote_nic = 0; remote_nic < remote_count; ++remote_nic) {
            rail_states_[{local_nic, remote_nic}] = RailState{};
        }
    }
    for (int local_nic = 0; local_nic < local_count; ++local_nic) {
        int numa_id = local_->getNicNumaID(local_nic);
        int npu_id = local_->findNpuByNic(local_nic);
        int remote_nic = remote_->findNicByNpu(npu_id);

        if (remote_nic >= 0) {
            remote_load[remote_nic]++;
            direct_rails_[local_nic] = remote_nic;
            continue;
        }

        int best_nic = -1;
        int best_nic_load = INT32_MAX;
        for (int cand = 0; cand < remote_count; ++cand) {
            if (numa_id != remote_->getNicNumaID(cand)) continue;
            if (remote_load[cand] < best_nic_load) {
                best_nic_load = remote_load[cand];
                best_nic = cand;
            }
        }
        if (best_nic >= 0) {
            remote_load[best_nic]++;
            direct_rails_[local_nic] = best_nic;
            continue;
        }

        for (int cand = 0; cand < remote_count; ++cand) {
            if (remote_load[cand] < best_nic_load) {
                best_nic_load = remote_load[cand];
                best_nic = cand;
            }
        }
        remote_load[best_nic]++;
        direct_rails_[local_nic] = best_nic;
    }

    ready_ = true;
    updateBestMapping();
    return Status::OK();
}

void RailMonitor::updateBestMapping() {
    for (size_t i = 0; i < kMaxNuma; ++i) best_mapping_[i].clear();
    std::vector<int> local_devices[kMaxNuma], remote_devices[kMaxNuma];
    std::unordered_set<int> remote_nic_set;
    const int local_nic_count = (int)local_->getNicList().size();
    const int remote_nic_count = (int)remote_->getNicList().size();
    for (int local_nic = 0; local_nic < local_nic_count; ++local_nic) {
        local_devices[local_->getNicNumaID(local_nic)].push_back(local_nic);
        auto remote_nic = direct_rails_[local_nic];
        if (!remote_nic_set.count(remote_nic)) {
            remote_devices[remote_->getNicNumaID(remote_nic)].push_back(
                remote_nic);
            remote_nic_set.insert(remote_nic);
        }
    }
    for (int remote_nic = 0; remote_nic < remote_nic_count; ++remote_nic) {
        if (!remote_nic_set.count(remote_nic))
            remote_devices[remote_->getNicNumaID(remote_nic)].push_back(
                remote_nic);
    }

    for (size_t local_numa = 0; local_numa < kMaxNuma; ++local_numa) {
        for (size_t remote_numa = 0; remote_numa < kMaxNuma; ++remote_numa) {
            auto &mapping = best_mapping_[remote_numa];
            size_t local_cnt = local_devices[local_numa].size();
            size_t remote_cnt = remote_devices[remote_numa].size();
            if (!local_cnt || !remote_cnt) continue;
            for (size_t i = 0; i < local_cnt; i++) {
                int local_nic = local_devices[local_numa][i];
                int remote_nic = -1;
                if (local_numa == remote_numa)
                    remote_nic = direct_rails_[local_nic];
                else
                    remote_nic = remote_devices[remote_numa][i % remote_cnt];
                if (!available(local_nic, remote_nic)) {
                    bool found = false;
                    for (int cand : remote_devices[remote_numa]) {
                        if (available(local_nic, cand)) {
                            remote_nic = cand;
                            found = true;
                            break;
                        }
                    }
                    if (!found) {
                        for (int cand = 0; cand < remote_nic_count; ++cand) {
                            if (available(local_nic, cand)) {
                                remote_nic = cand;
                                break;
                            }
                        }
                    }
                }
                if (remote_nic >= 0) mapping[local_nic] = remote_nic;
            }
        }
    }
}

}  // namespace v1
}  // namespace mooncake
