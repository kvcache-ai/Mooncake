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

#include "transport/ascend_transport/ascend_direct_transport/utils.h"

#include <dlfcn.h>
#include <pthread.h>
#include <sched.h>
#include <unistd.h>

#include <algorithm>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <exception>
#include <future>
#include <fstream>
#include <memory>
#include <optional>
#include <random>
#include <set>
#include <sstream>
#include <string>

#include <glog/logging.h>
#if __has_include(<jsoncpp/json/json.h>)
#include <jsoncpp/json/json.h>  // Ubuntu
#else
#include <json/json.h>  // CentOS
#endif

#include "common.h"
#include "config.h"

namespace mooncake {
namespace {
constexpr int32_t kPortRange = 100;
constexpr int32_t kMaxGenPortAttempts = 500;
constexpr const char* kProtocolDescFlatKey =
    "comm_resource_config.protocol_desc";
constexpr const char* kRocePrefix = "roce:";
constexpr const char* kStoreConfigKey = "store";
constexpr const char* kCommResourceConfigKey = "comm_resource_config";
constexpr const char* kNicTopoPathKey = "nic_topo_path";
constexpr const char* kNicTopoPathFlatKey =
    "comm_resource_config.nic_topo_path";
constexpr const char* kCustomTopoJsonEnv = "MC_CUSTOM_TOPO_JSON";

std::string SerializeCompactJson(const Json::Value& value) {
    Json::StreamWriterBuilder writer;
    writer["indentation"] = "";
    return Json::writeString(writer, value);
}

bool IsRoceProtocolDesc(const std::string& desc) {
    return desc.size() >= std::strlen(kRocePrefix) &&
           desc.compare(0, std::strlen(kRocePrefix), kRocePrefix) == 0;
}

bool ProtocolDescValueContainsRoce(const Json::Value& value) {
    if (value.isString()) {
        return IsRoceProtocolDesc(value.asString());
    }
    if (value.isArray()) {
        for (const auto& item : value) {
            if (item.isString() && IsRoceProtocolDesc(item.asString())) {
                return true;
            }
        }
    }
    return false;
}

bool ProtocolDescValueContainsExact(const Json::Value& value,
                                    const std::string& expected) {
    if (value.isString()) {
        return value.asString() == expected;
    }
    if (value.isArray()) {
        for (const auto& item : value) {
            if (item.isString() && item.asString() == expected) {
                return true;
            }
        }
    }
    return false;
}

const Json::Value* FindProtocolDescValue(const Json::Value& root) {
    if (root.isMember(kProtocolDescFlatKey)) {
        return &root[kProtocolDescFlatKey];
    }
    if (root.isMember("comm_resource_config") &&
        root["comm_resource_config"].isObject() &&
        root["comm_resource_config"].isMember("protocol_desc")) {
        return &root["comm_resource_config"]["protocol_desc"];
    }
    return nullptr;
}

bool ParseGlobalResourceConfigObject(const char* config_str,
                                     Json::Value& root) {
    if (config_str == nullptr || config_str[0] == '\0') {
        return false;
    }
    Json::CharReaderBuilder builder;
    builder["collectComments"] = false;
    std::string errs;
    const std::unique_ptr<Json::CharReader> reader(builder.newCharReader());
    if (!reader->parse(config_str, config_str + std::strlen(config_str), &root,
                       &errs)) {
        LOG(WARNING)
            << "Failed to parse ASCEND_GLOBAL_RESOURCE_CONFIG as JSON: "
            << errs;
        return false;
    }
    if (!root.isObject()) {
        LOG(WARNING) << "ASCEND_GLOBAL_RESOURCE_CONFIG must be a JSON object";
        return false;
    }
    return true;
}

// True when comm_resource_config.nic_topo_path is set to a non-empty string,
// accepting either the flat key or the nested comm_resource_config object.
// An explicit empty string counts as "not configured" (matching HIXL).
bool NicTopoPathIsSet(const Json::Value& root) {
    if (root.isMember(kNicTopoPathFlatKey) &&
        root[kNicTopoPathFlatKey].isString() &&
        !root[kNicTopoPathFlatKey].asString().empty()) {
        return true;
    }
    if (root.isMember(kCommResourceConfigKey) &&
        root[kCommResourceConfigKey].isObject() &&
        root[kCommResourceConfigKey].isMember(kNicTopoPathKey)) {
        const auto& value = root[kCommResourceConfigKey][kNicTopoPathKey];
        return value.isString() && !value.asString().empty();
    }
    return false;
}

bool ObjectSelectsRoceHost(const Json::Value& object) {
    if (!object.isObject()) {
        return false;
    }
    const Json::Value* protocol_desc = FindProtocolDescValue(object);
    return protocol_desc != nullptr &&
           ProtocolDescValueContainsExact(*protocol_desc, "roce:host");
}
}  // namespace

bool HasRoceProtocolDescInGlobalResourceConfig(const char* config_str) {
    Json::Value root;
    if (!ParseGlobalResourceConfigObject(config_str, root)) {
        return false;
    }
    const Json::Value* protocol_desc = FindProtocolDescValue(root);
    if (protocol_desc == nullptr) {
        return false;
    }
    return ProtocolDescValueContainsRoce(*protocol_desc);
}

bool HasHccsDeviceProtocolDescInGlobalResourceConfig(const char* config_str) {
    Json::Value root;
    if (!ParseGlobalResourceConfigObject(config_str, root)) {
        return false;
    }
    const Json::Value* protocol_desc = FindProtocolDescValue(root);
    if (protocol_desc == nullptr) {
        return false;
    }
    return ProtocolDescValueContainsExact(*protocol_desc, "hccs:device");
}

bool HasRoceHostProtocolDescInGlobalResourceConfig(const char* config_str) {
    Json::Value root;
    if (!ParseGlobalResourceConfigObject(config_str, root)) {
        return false;
    }
    if (ObjectSelectsRoceHost(root)) {
        return true;
    }
    // "store" is a sibling config, not a protocol_desc value. Host segments
    // are allocated after the store-role flag is cleared, so look here too.
    return root.isMember(kStoreConfigKey) &&
           ObjectSelectsRoceHost(root[kStoreConfigKey]);
}

std::string ResolveAscendGlobalResourceConfig(const char* config_str) {
    if (config_str == nullptr || config_str[0] == '\0') {
        return std::string();
    }
    Json::CharReaderBuilder builder;
    builder["collectComments"] = false;
    Json::Value root;
    std::string errs;
    const std::unique_ptr<Json::CharReader> reader(builder.newCharReader());
    if (!reader->parse(config_str, config_str + std::strlen(config_str), &root,
                       &errs)) {
        // Not valid JSON: leave it untouched and let adxl reject/handle it.
        return std::string(config_str);
    }
    if (!root.isObject() || !root.isMember(kStoreConfigKey)) {
        // Plain (legacy) config without a "store" override: pass verbatim.
        return std::string(config_str);
    }
    if (globalConfig().ascend_store_te_init) {
        return SerializeCompactJson(root[kStoreConfigKey]);
    }
    Json::Value normal = root;
    normal.removeMember(kStoreConfigKey);
    return SerializeCompactJson(normal);
}

std::string InjectNicTopoPathFromCustomTopoEnvTe(
    const std::string& resolved_config) {
    // TE support for configuring HIXL's comm_resource_config.nic_topo_path:
    // the MC_CUSTOM_TOPO_JSON path is written into it when it is empty.
    const char* custom_topo_json = std::getenv(kCustomTopoJsonEnv);
    if (custom_topo_json == nullptr || custom_topo_json[0] == '\0') {
        return resolved_config;
    }

    Json::Value root;
    if (resolved_config.empty()) {
        root = Json::Value(Json::objectValue);
    } else {
        Json::CharReaderBuilder builder;
        builder["collectComments"] = false;
        std::string errs;
        const std::unique_ptr<Json::CharReader> reader(builder.newCharReader());
        if (!reader->parse(resolved_config.c_str(),
                           resolved_config.c_str() + resolved_config.size(),
                           &root, &errs) ||
            !root.isObject()) {
            LOG(WARNING)
                << "Cannot inject MC_CUSTOM_TOPO_JSON into "
                   "comm_resource_config.nic_topo_path: GlobalResourceConfig "
                   "is not a JSON object";
            return resolved_config;
        }
    }

    // An explicitly configured nic_topo_path always wins over
    // MC_CUSTOM_TOPO_JSON.
    if (NicTopoPathIsSet(root)) {
        return resolved_config;
    }

    // Prefer the nested form when comm_resource_config already exists so HIXL
    // sees a single representation; drop any stale flat key in that case
    // (mirrors WithHixlListenPort).
    if (root.isMember(kCommResourceConfigKey) &&
        root[kCommResourceConfigKey].isObject()) {
        root[kCommResourceConfigKey][kNicTopoPathKey] = custom_topo_json;
        root.removeMember(kNicTopoPathFlatKey);
    } else {
        root[kNicTopoPathFlatKey] = custom_topo_json;
    }
    LOG(INFO) << "[AscendTE] comm_resource_config.nic_topo_path is empty, "
                 "injecting MC_CUSTOM_TOPO_JSON: "
              << custom_topo_json;
    return SerializeCompactJson(root);
}

bool IsRoceModeEnabled() {
    char* roce_enable_str = std::getenv("HCCL_INTRA_ROCE_ENABLE");
    if (roce_enable_str) {
        const auto roce_enable = parseFromString<int32_t>(roce_enable_str);
        if (roce_enable.has_value() && roce_enable.value() == 1) {
            return true;
        }
        LOG(WARNING) << "HCCL_INTRA_ROCE_ENABLE is not valid, value:"
                     << roce_enable_str;
    }
    char* global_resource_config = std::getenv("ASCEND_GLOBAL_RESOURCE_CONFIG");
    std::string resolved =
        ResolveAscendGlobalResourceConfig(global_resource_config);
    LOG(INFO) << "[AscendTE] resolved ASCEND_GLOBAL_RESOURCE_CONFIG "
                 "(protocol_desc): "
              << (resolved.empty() ? "(none)" : resolved);
    if (HasRoceProtocolDescInGlobalResourceConfig(resolved.c_str())) {
        return true;
    }
    return false;
}

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

}  // namespace

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
    }
    return plan;
}

namespace {
struct BoundRunner {
    std::mutex mu;
    const void* owner = nullptr;
    std::function<void(std::function<void()>)> run;
    const void* device_owner = nullptr;
    std::vector<std::function<void(std::function<void()>)>> device_runs;
};

BoundRunner& BoundAffinityRunner() {
    static BoundRunner runner;
    return runner;
}

thread_local bool on_bound_affinity_thread = false;

void RunAndWait(const std::function<void(std::function<void()>)>& run,
                std::function<void()> fn) {
    if (!run) {
        fn();
        return;
    }
    std::promise<void> done;
    auto wait = done.get_future();
    run([&fn, &done] {
        on_bound_affinity_thread = true;
        try {
            fn();
            on_bound_affinity_thread = false;
            done.set_value();
        } catch (...) {
            on_bound_affinity_thread = false;
            done.set_exception(std::current_exception());
        }
    });
    wait.get();
}
}  // namespace

void SetBoundAffinityRunner(const void* owner,
                            std::function<void(std::function<void()>)> runner) {
    auto& state = BoundAffinityRunner();
    std::lock_guard<std::mutex> lock(state.mu);
    state.owner = owner;
    state.run = std::move(runner);
}

void ClearBoundAffinityRunner(const void* owner) {
    auto& state = BoundAffinityRunner();
    std::lock_guard<std::mutex> lock(state.mu);
    if (state.owner != owner) {
        return;
    }
    state.owner = nullptr;
    state.run = nullptr;
}

void SetPerDeviceBoundAffinityRunners(
    const void* owner,
    std::vector<std::function<void(std::function<void()>)>> runners) {
    auto& state = BoundAffinityRunner();
    std::lock_guard<std::mutex> lock(state.mu);
    state.device_owner = owner;
    state.device_runs = std::move(runners);
}

void ClearPerDeviceBoundAffinityRunners(const void* owner) {
    auto& state = BoundAffinityRunner();
    std::lock_guard<std::mutex> lock(state.mu);
    if (state.device_owner != owner) {
        return;
    }
    state.device_owner = nullptr;
    state.device_runs.clear();
}

void RunOnBoundAffinityThread(std::function<void()> fn) {
    if (!fn) {
        return;
    }
    if (on_bound_affinity_thread) {
        fn();
        return;
    }
    std::function<void(std::function<void()>)> run;
    {
        auto& state = BoundAffinityRunner();
        std::lock_guard<std::mutex> lock(state.mu);
        run = state.run;
    }
    RunAndWait(run, std::move(fn));
}

void RunOnDeviceBoundAffinityThread(int32_t device_id,
                                    std::function<void()> fn) {
    if (!fn) {
        return;
    }
    if (on_bound_affinity_thread) {
        fn();
        return;
    }
    std::function<void(std::function<void()>)> run;
    {
        auto& state = BoundAffinityRunner();
        std::lock_guard<std::mutex> lock(state.mu);
        if (device_id >= 0) {
            const auto index = static_cast<size_t>(device_id);
            if (index < state.device_runs.size() && state.device_runs[index]) {
                run = state.device_runs[index];
            }
        }
        if (!run) {
            run = state.run;
        }
    }
    RunAndWait(run, std::move(fn));
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
                    condition_.wait(
                        lock, [this] { return !running_ || !tasks_.empty(); });

                    if (!running_ && tasks_.empty()) return;

                    task = std::move(tasks_.front());
                    tasks_.pop();
                }
                task();
            }
        });
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

std::string GenAdxlEngineName(const std::string& ip, const uint64_t port) {
    if (globalConfig().use_ipv6) {
        return ("[" + ip + "]") + ":" + std::to_string(port);
    }
    return ip + ":" + std::to_string(port);
}

uint16_t FindAdxlListenPort(int32_t base_port, int32_t device_id) {
    int32_t physical_dev_id;
    CHECK_ACL(aclrtGetPhyDevIdByLogicDevId(device_id, &physical_dev_id));
    static std::random_device rand_gen;
    std::uniform_int_distribution rand_dist;
    const int min_port = base_port + physical_dev_id * kPortRange;
    const int max_port = base_port + (physical_dev_id + 1) * kPortRange;
    LOG(INFO) << "Find available between " << min_port << " and " << max_port;
    bool use_ipv6 = globalConfig().use_ipv6;
    int sockfd;
    for (int attempt = 0; attempt < kMaxGenPortAttempts; ++attempt) {
        int port = min_port + rand_dist(rand_gen) % (max_port - min_port + 1);
        sockfd = socket(use_ipv6 ? AF_INET6 : AF_INET, SOCK_STREAM, 0);
        if (sockfd == -1) {
            continue;
        }
        struct timeval timeout;
        timeout.tv_sec = 1;
        timeout.tv_usec = 0;
        if (setsockopt(sockfd, SOL_SOCKET, SO_RCVTIMEO, &timeout,
                       sizeof(timeout))) {
            close(sockfd);
            sockfd = -1;
            continue;
        }
        sockaddr_storage bind_address_storage;
        memset(&bind_address_storage, 0, sizeof(bind_address_storage));
        auto* bind_addr = reinterpret_cast<sockaddr*>(&bind_address_storage);
        socklen_t addr_len;
        if (use_ipv6) {
            auto* addr6 = reinterpret_cast<sockaddr_in6*>(bind_addr);
            addr6->sin6_family = AF_INET6;
            addr6->sin6_port = htons(port);
            addr6->sin6_addr = IN6ADDR_ANY_INIT;
            addr_len = sizeof(*addr6);
        } else {
            auto* addr4 = reinterpret_cast<sockaddr_in*>(bind_addr);
            addr4->sin_family = AF_INET;
            addr4->sin_port = htons(port);
            addr4->sin_addr.s_addr = INADDR_ANY;
            addr_len = sizeof(*addr4);
        }
        if (bind(sockfd, bind_addr, addr_len) < 0) {
            close(sockfd);
            sockfd = -1;
            continue;
        }
        close(sockfd);
        return port;
    }
    return 0;
}

int SetDeviceAndGetContext(int32_t device_id, aclrtContext* out_context) {
    CHECK_ACL(aclrtSetDevice(device_id));
    CHECK_ACL(aclrtGetCurrentContext(out_context));
    return 0;
}
}  // namespace mooncake
