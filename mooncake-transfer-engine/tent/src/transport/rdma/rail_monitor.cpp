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

#include "tent/transport/rdma/rail_monitor.h"

namespace mooncake {
namespace tent {

namespace {

bool sameNicLayout(const Topology::NicEntry& a, const Topology::NicEntry& b) {
    return a.name == b.name && a.pci_bus_id == b.pci_bus_id &&
           a.type == b.type && a.numa_node == b.numa_node;
}

bool sameMemLayout(const Topology::MemEntry& a, const Topology::MemEntry& b) {
    if (a.name != b.name || a.pci_bus_id != b.pci_bus_id || a.type != b.type ||
        a.numa_node != b.numa_node)
        return false;
    for (size_t rank = 0; rank < Topology::DevicePriorityRanks; ++rank) {
        if (a.device_list[rank] != b.device_list[rank]) return false;
    }
    return true;
}

// True when two snapshots describe the same NIC/memory wiring. Segment
// metadata is copy-on-write, so a buffer register publishes a new Topology*
// even when the rail map is unchanged. Comparing layout (not pointer
// identity) lets load() refresh pins without rebuilding rail_states_.
bool sameRailLayout(const Topology* a, const Topology* b) {
    if (a == b) return true;
    if (!a || !b) return false;
    const size_t nic_count = a->getNicCount();
    const size_t mem_count = a->getMemCount();
    if (nic_count != b->getNicCount() || mem_count != b->getMemCount())
        return false;
    for (size_t i = 0; i < nic_count; ++i) {
        auto* ea = a->getNicEntry(static_cast<int>(i));
        auto* eb = b->getNicEntry(static_cast<int>(i));
        if (!ea || !eb || !sameNicLayout(*ea, *eb)) return false;
    }
    for (size_t i = 0; i < mem_count; ++i) {
        auto* ea = a->getMemEntry(static_cast<int>(i));
        auto* eb = b->getMemEntry(static_cast<int>(i));
        if (!ea || !eb || !sameMemLayout(*ea, *eb)) return false;
    }
    return true;
}

}  // namespace

Status RailMonitor::load(std::shared_ptr<const Topology> local,
                         std::shared_ptr<const Topology> remote,
                         const std::string& rail_topo_json,
                         const Config* conf) {
    const bool first_load = !ready_;
    const bool same_layout = ready_ &&
                             sameRailLayout(local_.get(), local.get()) &&
                             sameRailLayout(remote_.get(), remote.get());

    local_ = std::move(local);
    remote_ = std::move(remote);
    if (conf) {
        error_threshold_ = conf->get(kCfgErrorThreshold, error_threshold_);
        error_window_ = std::chrono::seconds(
            conf->get(kCfgErrorWindowSecs, (int)error_window_.count()));
        cooldown_ = std::chrono::seconds(
            conf->get(kCfgCooldownSecs, (int)cooldown_.count()));
        probe_interval_ = std::chrono::seconds(
            conf->get(kCfgProbeIntervalSecs, (int)probe_interval_.count()));
        // Config is identical on every COW snapshot refresh. Log once per
        // monitor so PD/e2e does not reprint the banner per slice/worker.
        if (first_load) {
            LOG(INFO) << "RailMonitor: error_threshold=" << error_threshold_
                      << " error_window=" << error_window_.count() << "s"
                      << " cooldown=" << cooldown_.count() << "s"
                      << " probe_interval=" << probe_interval_.count() << "s";
        }
    }
    if (same_layout) return Status::OK();
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
    auto& st = it->second;
    // Closed: healthy, no pause armed, no trial in flight.
    if (!st.paused() && !st.half_open) return true;
    auto now = std::chrono::steady_clock::now();

    // Half-Open: the cooldown expired and one trial transfer was admitted.
    // Do NOT admit more traffic until the trial resolves (markRecovered closes,
    // markFailed re-arms). Re-admit only after probe_interval_ so a lost
    // callback does not strand the rail forever. probe_interval_==0 admits
    // exactly one trial and relies on the callback.
    if (st.half_open) {
        if (probe_interval_.count() > 0 &&
            now - st.last_probe_time >= probe_interval_) {
            st.last_probe_time = now;
            return true;
        }
        return false;
    }

    // Open: the cooldown timer is still running. Admit one transfer every
    // probe_interval_ as an exploratory probe — a success closes the rail
    // early (defect B fix); a failure is a no-op here (markFailed sees
    // was_paused and neither escalates nor re-arms), so periodic probes never
    // multiply the cooldown.
    if (now < st.resume_time) {
        if (probe_interval_.count() > 0 &&
            now - st.last_probe_time >= probe_interval_) {
            st.last_probe_time = now;
            return true;
        }
        return false;
    }

    // Cooldown expired without markRecovered (the rail is still paused, so no
    // probe succeeded). Transition to Half-Open: admit exactly ONE trial via
    // this return true; the fallback path in selectFallbackDevice routes it.
    // Escalation happens only if the trial FAILS (markFailed), not because the
    // clock fired — elapsed time does not prove the path is healthy, so we
    // must not reopen all traffic to a still-dead peer.
    st.half_open = true;
    st.error_count = 0;
    st.last_probe_time = now;
    LOG(INFO) << "Rail half-open: local_nic=" << local_nic
              << " remote_nic=" << remote_nic
              << " (cooldown=" << st.cooldown.count()
              << "s retained, trial admitted)";
    return true;
}

void RailMonitor::markFailed(int local_nic, int remote_nic) {
    auto it = rail_states_.find(std::make_pair(local_nic, remote_nic));
    if (it == rail_states_.end()) return;
    auto& st = it->second;
    auto now = std::chrono::steady_clock::now();
    if (st.error_count == 0 || now - st.last_error > error_window_) {
        st.error_count = 1;
    } else {
        st.error_count++;
    }
    st.last_error = now;

    // Half-Open trial failed: the one transfer admitted after cooldown expiry
    // did not prove the path healthy. Escalate the cooldown and re-arm so the
    // next trial waits longer, then return to Open. This is the ONLY escalation
    // point for a paused rail — clock expiry alone never escalates.
    if (st.half_open) {
        st.half_open = false;
        st.cooldown *= 2;
        if (st.cooldown > kMaxCooldown) st.cooldown = kMaxCooldown;
        st.error_count = 0;  // fresh cycle
        st.resume_time = now + st.cooldown;
        st.last_probe_time = now;  // defer first probe by one interval
        LOG(INFO) << "Rail half-open trial failed; re-paused: local_nic="
                  << local_nic << " remote_nic=" << remote_nic
                  << " (cooldown escalated to " << st.cooldown.count() << "s)";
        updateBestMapping();
        return;
    }

    const bool was_paused = st.paused();

    if (st.error_count >= error_threshold_) {
        if (!was_paused) {
            // Escalate the cooldown only when a *fresh* pause arms (the rail
            // was healthy at this instant), never within an ongoing burst.
            // Previously cooldown was doubled on every markFailed call, so a
            // single outage -- N error WQEs landing in one 10s window -- pushed
            // a 30s pause straight to the 300s cap before the outage even
            // cleared, forcing a ~5min TCP fallback after the peer had already
            // recovered. Now the cooldown is set once per pause cycle; errors
            // arriving while already paused do not multiply it.
            //
            // cooldown == 0: the previous cycle ended with a proven-healthy
            //   recovery (markRecovered reset it), so start from the initial
            //   value. cooldown != 0 should not reach here while Closed (only
            //   markRecovered clears the pause), but guard anyway.
            if (st.cooldown.count() == 0) {
                st.cooldown = cooldown_;
            } else {
                st.cooldown *= 2;
                if (st.cooldown > kMaxCooldown) st.cooldown = kMaxCooldown;
            }
            LOG(INFO) << "Rail paused: local_nic=" << local_nic
                      << " remote_nic=" << remote_nic
                      << " (errors=" << st.error_count << " in "
                      << error_window_.count()
                      << "s, cooldown=" << st.cooldown.count() << "s)";
            st.resume_time = now + st.cooldown;
            // Defer the first probe by one probe_interval_: without this,
            // last_probe_time defaults to epoch and available() would admit a
            // probe the instant the pause arms, defeating the throttle.
            st.last_probe_time = now;
            updateBestMapping();
        }
        // Already paused (Open, exploratory probe failed): leave resume_time
        // and cooldown untouched. Re-arming or escalating here would let a
        // sustained outage extend the pause indefinitely -- the original
        // defect. The pause runs its course so available()'s probe can test
        // recovery, and escalation happens only when the Half-Open trial fails.
    }
}

void RailMonitor::markRecovered(int local_nic, int remote_nic) {
    auto it = rail_states_.find(std::make_pair(local_nic, remote_nic));
    if (it == rail_states_.end()) return;
    auto& st = it->second;
    // Fast path: a healthy rail stays healthy. 99%+ of completions land
    // here, so we must not touch best_mapping_ or write any field.
    if (!st.paused() && !st.half_open && st.error_count == 0 &&
        st.cooldown.count() == 0)
        return;
    bool was_paused = st.paused();
    bool was_half_open = st.half_open;
    // Clear all exponential-backoff memory: the next failure cycle must
    // start from the initial cooldown_, not a doubled value left over
    // from the previous cycle.
    st.error_count = 0;
    st.resume_time = {};
    st.cooldown = std::chrono::seconds(0);
    st.half_open = false;
    if (was_paused || was_half_open) {
        LOG(INFO) << "Rail recovered: local_nic=" << local_nic
                  << " remote_nic=" << remote_nic
                  << " (un-paused by successful transfer)";
        updateBestMapping();
    }
    st.last_probe_time = {};
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
Status RailMonitor::loadFromJson(const std::string& rail_topo_json) {
    try {
        auto root = json::parse(rail_topo_json);

        rail_states_.clear();
        direct_rails_.clear();

        if (root.contains("all")) {
            for (const auto& path_entry : root["all"]) {
                std::string local_nic_name = path_entry.value("local", "");
                std::string remote_nic_name = path_entry.value("remote", "");
                int local_nic_id = local_->getNicId(local_nic_name);
                int remote_nic_id = remote_->getNicId(remote_nic_name);

                if (local_nic_id >= 0 && remote_nic_id >= 0) {
                    rail_states_[{local_nic_id, remote_nic_id}] = RailState{};
                } else {
                    LOG(WARNING) << "Ignore invalid path " << local_nic_name
                                 << " -> " << remote_nic_name;
                }
            }
        }

        if (root.contains("direct")) {
            for (const auto& path_entry : root["direct"]) {
                std::string local_nic_name = path_entry.value("local", "");
                std::string remote_nic_name = path_entry.value("remote", "");
                int local_nic_id = local_->getNicId(local_nic_name);
                int remote_nic_id = remote_->getNicId(remote_nic_name);

                if (local_nic_id >= 0 && remote_nic_id >= 0) {
                    direct_rails_[local_nic_id] = remote_nic_id;
                } else {
                    LOG(WARNING) << "Ignore invalid direct path "
                                 << local_nic_name << " -> " << remote_nic_name;
                }
            }
        }
    } catch (const std::exception& ex) {
        LOG(ERROR) << "Failed to parse rail_topo_json: " << ex.what();
        return Status::InvalidArgument("Failed to parse JSON" LOC_MARK);
    }

    ready_ = true;
    updateBestMapping();
    return Status::OK();
}

static int matchRemoteNicId(const Topology* local, const Topology* remote,
                            int local_nic) {
    std::string mem_name;
    for (size_t i = 0; i < local->getMemCount(); ++i) {
        auto entry = local->getMemEntry(i);
        auto& prior_devices = entry->device_list[0];
        if (entry->type == Topology::MEM_CUDA && !prior_devices.empty() &&
            prior_devices[0] == local_nic) {
            mem_name = entry->name;
            break;
        }
    }
    if (mem_name.empty()) return -1;
    auto mem_id = remote->getMemId(mem_name);
    if (mem_id < 0) return -1;
    auto entry = remote->getMemEntry(mem_id);
    auto& prior_devices = entry->device_list[0];
    if (entry->type == Topology::MEM_CUDA && !prior_devices.empty())
        return prior_devices[0];
    return -1;
}

Status RailMonitor::loadDefault() {
    rail_states_.clear();
    direct_rails_.clear();
    int local_nic_count = (int)local_->getNicCount();
    int remote_nic_count = (int)remote_->getNicCount();
    std::vector<int> remote_load(remote_nic_count, 0);
    for (int local_nic = 0; local_nic < local_nic_count; ++local_nic) {
        for (int remote_nic = 0; remote_nic < remote_nic_count; ++remote_nic) {
            rail_states_[{local_nic, remote_nic}] = RailState{};
        }
    }
    for (int local_nic = 0; local_nic < local_nic_count; ++local_nic) {
        auto local_entry = local_->getNicEntry(local_nic);
        if (local_entry->type != Topology::NIC_RDMA) continue;
        int numa_id = local_entry->numa_node;

        // Priority 1: Same-name device matching (mlx5_0 -> mlx5_0)
        bool matched = false;
        auto local_nic_name = local_entry->name;
        for (int remote_nic = 0; remote_nic < remote_nic_count; ++remote_nic) {
            auto remote_entry = remote_->getNicEntry(remote_nic);
            if (remote_entry && remote_entry->type == Topology::NIC_RDMA &&
                remote_entry->name == local_nic_name) {
                remote_load[remote_nic]++;
                direct_rails_[local_nic] = remote_nic;
                matched = true;
                break;
            }
        }
        if (matched) continue;

        // Priority 2: CUDA memory topology matching (GPU-direct NIC)
        int remote_nic =
            matchRemoteNicId(local_.get(), remote_.get(), local_nic);
        if (remote_nic >= 0) {
            remote_load[remote_nic]++;
            direct_rails_[local_nic] = remote_nic;
            continue;
        }

        int best_nic = -1;
        int best_nic_load = INT32_MAX;
        for (int cand = 0; cand < remote_nic_count; ++cand) {
            auto cand_entry = remote_->getNicEntry(cand);
            if (!cand_entry || cand_entry->type != Topology::NIC_RDMA ||
                numa_id != cand_entry->numa_node)
                continue;
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

        for (int cand = 0; cand < remote_nic_count; ++cand) {
            auto cand_entry = remote_->getNicEntry(cand);
            if (!cand_entry || cand_entry->type != Topology::NIC_RDMA) continue;
            if (remote_load[cand] < best_nic_load) {
                best_nic_load = remote_load[cand];
                best_nic = cand;
            }
        }
        if (best_nic < 0) {
            direct_rails_[local_nic] = -1;
            continue;
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
    const int local_nic_count = (int)local_->getNicCount();
    const int remote_nic_count = (int)remote_->getNicCount();

    // Helper: clamp negative numa_node (e.g. -1 for eRDMA devices that lack
    // NUMA affinity) to 0 so it can be used safely as an array index.
    auto safeNuma = [](int numa) -> size_t {
        return (numa >= 0 && numa < (int)kMaxNuma) ? (size_t)numa : 0;
    };

    for (int local_nic = 0; local_nic < local_nic_count; ++local_nic) {
        auto local_entry = local_->getNicEntry(local_nic);
        if (!local_entry || local_entry->type != Topology::NIC_RDMA) continue;
        local_devices[safeNuma(local_entry->numa_node)].push_back(local_nic);
        auto remote_nic = direct_rails_[local_nic];
        if (!remote_nic_set.count(remote_nic)) {
            auto remote_entry = remote_->getNicEntry(remote_nic);
            if (!remote_entry || remote_entry->type != Topology::NIC_RDMA)
                continue;
            remote_devices[safeNuma(remote_entry->numa_node)].push_back(
                remote_nic);
            remote_nic_set.insert(remote_nic);
        }
    }
    for (int remote_nic = 0; remote_nic < remote_nic_count; ++remote_nic) {
        if (!remote_nic_set.count(remote_nic)) {
            auto remote_entry = remote_->getNicEntry(remote_nic);
            if (!remote_entry || remote_entry->type != Topology::NIC_RDMA)
                continue;
            remote_devices[safeNuma(remote_entry->numa_node)].push_back(
                remote_nic);
        }
    }

    for (size_t local_numa = 0; local_numa < kMaxNuma; ++local_numa) {
        for (size_t remote_numa = 0; remote_numa < kMaxNuma; ++remote_numa) {
            auto& mapping = best_mapping_[remote_numa];
            size_t local_cnt = local_devices[local_numa].size();
            size_t remote_cnt = remote_devices[remote_numa].size();
            if (!local_cnt || !remote_cnt) continue;
            for (size_t i = 0; i < local_cnt; i++) {
                int local_nic = local_devices[local_numa][i];
                int remote_nic = -1;
                if (local_numa == remote_numa) {
                    remote_nic = direct_rails_[local_nic];
                } else {
                    // Cross-NUMA: prefer a same-name remote device (e.g.
                    // mlx5_5 -> mlx5_5) before falling back to positional
                    // assignment. loadDefault() already builds direct_rails_
                    // via same-name matching (Priority 1); mirroring it here
                    // avoids mapping a local NIC to an unrelated remote NIC on
                    // a different physical/overlay network, which fails QP
                    // modify-to-RTR with "transport retry counter exceeded" on
                    // multi-bond dual-NUMA RoCEv2 fabrics (issues #2758/#2467).
                    auto local_entry = local_->getNicEntry(local_nic);
                    if (local_entry) {
                        for (int cand : remote_devices[remote_numa]) {
                            auto cand_entry = remote_->getNicEntry(cand);
                            if (cand_entry &&
                                cand_entry->name == local_entry->name) {
                                remote_nic = cand;
                                break;
                            }
                        }
                    }
                    if (remote_nic < 0)
                        remote_nic =
                            remote_devices[remote_numa][i % remote_cnt];
                }
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

}  // namespace tent
}  // namespace mooncake
