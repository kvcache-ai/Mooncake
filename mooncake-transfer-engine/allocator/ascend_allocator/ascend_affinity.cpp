// Copyright 2026 Huawei Technologies Co., Ltd
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

#include "ascend_affinity.h"

#include <dlfcn.h>
#include <pthread.h>
#include <sched.h>
#include <unistd.h>

#include <algorithm>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <exception>
#include <fstream>
#include <future>
#include <memory>
#include <optional>
#include <set>
#include <sstream>
#include <string>
#include <vector>

#include <acl/acl.h>
#include <glog/logging.h>

namespace mooncake {
namespace {

std::string FormatCpuRanges(const std::vector<int>& cpus) {
    if (cpus.empty()) {
        return "";
    }
    std::ostringstream out;
    size_t i = 0;
    while (i < cpus.size()) {
        size_t j = i;
        while (j + 1 < cpus.size() && cpus[j + 1] == cpus[j] + 1) {
            ++j;
        }
        if (i != 0) {
            out << ',';
        }
        if (j == i) {
            out << cpus[i];
        } else {
            out << cpus[i] << '-' << cpus[j];
        }
        i = j + 1;
    }
    return out.str();
}

std::optional<std::vector<int>> ParseCpuList(const std::string& text) {
    std::vector<int> cpus;
    std::string token;
    std::stringstream in(text);
    while (std::getline(in, token, ',')) {
        const auto begin = token.find_first_not_of(" \t");
        if (begin == std::string::npos) {
            continue;
        }
        const auto end = token.find_last_not_of(" \t");
        token = token.substr(begin, end - begin + 1);
        const auto dash = token.find('-');
        char* start_end = nullptr;
        char* stop_end = nullptr;
        long start = 0;
        long stop = 0;
        if (dash == std::string::npos) {
            start = std::strtol(token.c_str(), &start_end, 10);
            if (start_end == token.c_str() || *start_end != '\0' || start < 0) {
                return std::nullopt;
            }
            stop = start;
        } else {
            const std::string left = token.substr(0, dash);
            const std::string right = token.substr(dash + 1);
            start = std::strtol(left.c_str(), &start_end, 10);
            stop = std::strtol(right.c_str(), &stop_end, 10);
            if (start_end == left.c_str() || *start_end != '\0' ||
                stop_end == right.c_str() || *stop_end != '\0' || start < 0 ||
                stop < start) {
                return std::nullopt;
            }
        }
        for (long cpu = start; cpu <= stop; ++cpu) {
            cpus.push_back(static_cast<int>(cpu));
        }
    }
    if (cpus.empty()) {
        return std::nullopt;
    }
    std::sort(cpus.begin(), cpus.end());
    cpus.erase(std::unique(cpus.begin(), cpus.end()), cpus.end());
    return cpus;
}

const std::optional<std::set<int>>& AllowedCpus() {
    static const std::optional<std::set<int>> allowed = []() {
        std::ifstream status("/proc/self/status");
        if (!status) {
            return std::optional<std::set<int>>();
        }
        std::string line;
        while (std::getline(status, line)) {
            const std::string key = "Cpus_allowed_list:";
            if (line.compare(0, key.size(), key) != 0) {
                continue;
            }
            std::string value = line.substr(key.size());
            auto parsed = ParseCpuList(value);
            if (!parsed.has_value()) {
                return std::optional<std::set<int>>();
            }
            return std::optional<std::set<int>>(std::in_place, parsed->begin(),
                                                parsed->end());
        }
        return std::optional<std::set<int>>();
    }();
    return allowed;
}

std::vector<int> IntersectAllowed(const std::vector<int>& cpus) {
    const auto& allowed = AllowedCpus();
    if (!allowed.has_value()) {
        return cpus;
    }
    std::vector<int> kept;
    for (int cpu : cpus) {
        if (allowed->count(cpu) != 0) {
            kept.push_back(cpu);
        }
    }
    return kept;
}

using DsmiInfoFn = int (*)(unsigned int, unsigned int, unsigned int, void*,
                           unsigned int*);

// DSMI_MAIN_CMD_UB / URMA device name. A5 Server affinity is the NUMA node
// of that UB device; the cpulist is every CPU on the node.
constexpr unsigned int kDsmiMainCmdUb = 62;
constexpr unsigned int kDsmiUbSubUrmaName = 3;
constexpr size_t kUrmaNameCap = 64;

DsmiInfoFn LoadDsmiInfo() {
    static DsmiInfoFn fn = []() -> DsmiInfoFn {
        const char* paths[] = {
            "libdrvdsmi_host.so",
            "/usr/local/Ascend/driver/lib64/driver/libdrvdsmi_host.so",
            nullptr,
        };
        void* handle = nullptr;
        for (int i = 0; paths[i] != nullptr; ++i) {
            handle = dlopen(paths[i], RTLD_NOW | RTLD_LOCAL);
            if (handle != nullptr) {
                break;
            }
            (void)dlerror();
        }
        if (handle == nullptr) {
            return nullptr;
        }
        dlerror();
        void* sym = dlsym(handle, "dsmi_get_device_info");
        (void)dlerror();
        if (sym == nullptr) {
            return nullptr;
        }
        return reinterpret_cast<DsmiInfoFn>(sym);
    }();
    return fn;
}

bool ReadFirstLine(const std::string& path, std::string* line) {
    std::ifstream in(path);
    if (!in) {
        return false;
    }
    if (!std::getline(in, *line) || line->empty()) {
        return false;
    }
    return true;
}

// numa_out is the node id. The returned CPUs are that node's cpulist,
// intersected with this process's cpuset. Empty means do not bind.
// ACL callers provide a user device ID. DSMI expects the driver logical ID;
// ASCEND_RT_VISIBLE_DEVICES may make these differ even inside a container.
std::vector<int> CpusForUserDevice(DsmiInfoFn info, int32_t user_id,
                                   int* numa_out) {
    *numa_out = -1;
    int32_t logic_id = -1;
    const aclError logic_ret =
        aclrtGetLogicDevIdByUserDevId(user_id, &logic_id);
    if (logic_ret != ACL_ERROR_NONE || logic_id < 0) {
        LOG(WARNING) << "[affinity] cannot resolve logic id for user device "
                     << user_id << ", ret=" << logic_ret;
        return {};
    }

    char name[kUrmaNameCap];
    std::memset(name, 0, sizeof(name));
    unsigned int size = sizeof(name);
    const int ret = info(static_cast<unsigned int>(logic_id), kDsmiMainCmdUb,
                         kDsmiUbSubUrmaName, name, &size);
    if (ret != 0 || name[0] == '\0') {
        LOG(WARNING)
            << "[affinity] DSMI URMA name unavailable for logic device "
            << logic_id << ", ret=" << ret;
        return {};
    }

    const std::string numa_path =
        std::string("/sys/class/uburma/") + name + "/device/numa";
    std::string numa_text;
    if (!ReadFirstLine(numa_path, &numa_text)) {
        return {};
    }
    int numa = 0;
    if (std::sscanf(numa_text.c_str(), "%x", &numa) != 1 || numa < 0) {
        return {};
    }
    *numa_out = numa;

    const std::string cpu_path =
        "/sys/devices/system/node/node" + std::to_string(numa) + "/cpulist";
    std::string cpu_text;
    if (!ReadFirstLine(cpu_path, &cpu_text)) {
        return {};
    }
    auto parsed = ParseCpuList(cpu_text);
    if (!parsed.has_value()) {
        return {};
    }
    return IntersectAllowed(*parsed);
}

// Node ids worth probing. /sys/devices/system/node/possible lists the nodes
// the kernel was built for (e.g. "0-3"), so a large machine does not pay for
// 256 failed opens. Falls back to scanning the historical id range when the
// file is unavailable.
std::vector<int> PossibleNumaNodeIds() {
    std::string possible;
    if (ReadFirstLine("/sys/devices/system/node/possible", &possible)) {
        auto parsed = ParseCpuList(possible);
        if (parsed.has_value()) {
            return *parsed;
        }
    }
    constexpr int kMaxNodeId = 255;
    std::vector<int> ids;
    ids.reserve(kMaxNodeId + 1);
    for (int node = 0; node <= kMaxNodeId; ++node) {
        ids.push_back(node);
    }
    return ids;
}

// Every NUMA node that has CPUs this process may run on, in node order.
const std::vector<std::pair<int, std::vector<int>>>& NodeCpuLists() {
    static const std::vector<std::pair<int, std::vector<int>>> nodes = []() {
        std::vector<std::pair<int, std::vector<int>>> out;
        for (int node : PossibleNumaNodeIds()) {
            std::string text;
            if (!ReadFirstLine("/sys/devices/system/node/node" +
                                   std::to_string(node) + "/cpulist",
                               &text)) {
                continue;
            }
            auto parsed = ParseCpuList(text);
            if (!parsed.has_value()) {
                continue;
            }
            auto cpus = IntersectAllowed(*parsed);
            if (!cpus.empty()) {
                out.emplace_back(node, std::move(cpus));
            }
        }
        return out;
    }();
    return nodes;
}

void SpreadDevicesAcrossNumaNodes(
    AscendAffinityPlan& plan, const std::vector<int32_t>& logic_device_ids) {
    const auto& nodes = NodeCpuLists();
    const auto assignment =
        AllocateUnreadDevicesToNumaNodes(logic_device_ids.size(), nodes);
    if (assignment.empty()) {
        return;
    }
    for (size_t i = 0; i < plan.per_device_cpus.size() && i < assignment.size();
         ++i) {
        if (!plan.per_device_cpus[i].empty()) {
            continue;
        }
        const auto& node = nodes[assignment[i]];
        plan.per_device_cpus[i] = node.second;
        LOG(INFO) << "[affinity] device " << logic_device_ids[i]
                  << " has no readable NUMA node, spread onto node "
                  << node.first
                  << ", cpus=" << FormatCpuRanges(plan.per_device_cpus[i]);
    }
}

}  // namespace

const std::vector<std::pair<int, std::vector<int>>>& NumaNodeCpuLists() {
    return NodeCpuLists();
}

std::vector<size_t> AllocateUnreadDevicesToNumaNodes(
    size_t device_count,
    const std::vector<std::pair<int, std::vector<int>>>& nodes) {
    std::vector<size_t> assignment;
    if (nodes.size() < 2) {
        return assignment;
    }
    assignment.reserve(device_count);
    std::vector<size_t> devices_per_node(nodes.size(), 0);
    for (size_t i = 0; i < device_count; ++i) {
        // Fewest devices so far; ties keep the lower node id because `nodes`
        // is in node order and a strict comparison keeps the first one.
        size_t best = 0;
        for (size_t n = 1; n < nodes.size(); ++n) {
            if (devices_per_node[n] < devices_per_node[best]) {
                best = n;
            }
        }
        assignment.push_back(best);
        ++devices_per_node[best];
    }
    return assignment;
}

AscendAffinityPlan BuildAscendAffinityPlan(
    const std::vector<int32_t>& logic_device_ids) {
    AscendAffinityPlan plan;
    plan.per_device_cpus.assign(logic_device_ids.size(), {});
    bool same = !logic_device_ids.empty();
    int shared_numa = -1;
    const DsmiInfoFn info = logic_device_ids.empty() ? nullptr : LoadDsmiInfo();
    if (info == nullptr) {
        same = false;
    }
    for (size_t i = 0; info != nullptr && i < logic_device_ids.size(); ++i) {
        // Historical parameter name; these values originate from ACL user IDs.
        const int32_t user_id = logic_device_ids[i];
        int numa = -1;
        plan.per_device_cpus[i] = CpusForUserDevice(info, user_id, &numa);
        if (plan.per_device_cpus[i].empty()) {
            same = false;
            continue;
        }
        if (shared_numa < 0) {
            shared_numa = numa;
        } else if (numa != shared_numa) {
            same = false;
        }
    }
    const bool set = same && shared_numa >= 0 &&
                     !plan.per_device_cpus.empty() &&
                     !plan.per_device_cpus[0].empty();
    if (set) {
        plan.shared_cpus = plan.per_device_cpus[0];
        LOG(INFO) << "[affinity] set success, numa=" << shared_numa
                  << ", cpus=" << FormatCpuRanges(plan.shared_cpus);
    } else if (logic_device_ids.size() > 1) {
        SpreadDevicesAcrossNumaNodes(plan, logic_device_ids);
    }
    return plan;
}

bool TryBindCurrentThread(const std::vector<int>& cpus) {
    if (cpus.empty()) {
        return false;
    }
    cpu_set_t set;
    CPU_ZERO(&set);
    for (int cpu : cpus) {
        if (cpu < 0 || cpu >= CPU_SETSIZE) {
            return false;
        }
        CPU_SET(cpu, &set);
    }
    const int rc = pthread_setaffinity_np(pthread_self(), sizeof(set), &set);
    if (rc != 0) {
        LOG(INFO) << "[affinity] pthread_setaffinity_np failed, rc=" << rc
                  << " (" << std::strerror(rc) << ")";
        return false;
    }
    return true;
}

// AscendThreadPool implementation
AscendThreadPool::AscendThreadPool(size_t num_threads,
                                   std::string thread_name_prefix,
                                   std::vector<int> affinity_cpus)
    : running_(true), affinity_cpus_(std::move(affinity_cpus)) {
    if (thread_name_prefix.empty()) {
        thread_name_prefix = "ascend-wk-";
    }
    // If a worker cannot be created, unwinding with joinable workers would call
    // std::terminate, so stop the pool and join the workers created so far
    // before rethrowing.
    try {
        for (size_t i = 0; i < num_threads; ++i) {
            workers_.emplace_back([this, i, thread_name_prefix] {
                auto thread_name = thread_name_prefix + std::to_string(i);
                if (thread_name.size() > 15) {
                    thread_name.resize(15);
                }
                pthread_setname_np(pthread_self(), thread_name.c_str());
                if (!affinity_cpus_.empty()) {
                    (void)TryBindCurrentThread(affinity_cpus_);
                }
                while (true) {
                    std::function<void()> task;
                    {
                        std::unique_lock<std::mutex> lock(queue_mutex_);
                        condition_.wait(lock, [this] {
                            return !running_ || !tasks_.empty();
                        });

                        if (!running_ && tasks_.empty()) return;

                        task = std::move(tasks_.front());
                        tasks_.pop();
                    }
                    task();
                }
            });
        }
    } catch (...) {
        running_.store(false, std::memory_order_release);
        condition_.notify_all();
        for (std::thread& worker : workers_) {
            if (worker.joinable()) {
                worker.join();
            }
        }
        throw;
    }
}

AscendThreadPool::~AscendThreadPool() { stop(); }

void AscendThreadPool::stop() {
    bool expected = true;
    if (!running_.compare_exchange_strong(expected, false)) {
        return;  // Already stopped
    }

    condition_.notify_all();
    for (std::thread& worker : workers_) {
        if (worker.joinable()) worker.join();
    }
}

bool AscendThreadPool::isRunning() const { return running_; }

}  // namespace mooncake
