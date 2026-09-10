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

#include "tent/transport/ascend/resource_config.h"

#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cstdlib>
#include <cstring>
#include <utility>
#include <exception>
#include <limits>
#include <random>
#include <stdexcept>

#include <glog/logging.h>
#include <acl/acl.h>

#include "tent/common/types.h"
#include "tent/thirdparty/nlohmann/json.h"

namespace mooncake {
namespace tent {
namespace {

constexpr int32_t kPortRange = 100;
constexpr int32_t kMaxPortAttempts = 500;
constexpr int32_t kMinListenPort = 1024;
constexpr int32_t kMaxListenPort = 65535;
constexpr const char* kStoreKey = "store";
constexpr const char* kFabricKey = "fabric_memory";
constexpr const char* kFabricFlatPrefix = "fabric_memory.";
constexpr const char* kRocePrefix = "roce:";

using json = nlohmann::json;

bool EnvFlagIsOne(const char* value) {
    return value && std::strcmp(value, "1") == 0;
}

int32_t ParseEnvInt(const char* name, int32_t fallback) {
    const char* raw = std::getenv(name);
    if (!raw || raw[0] == '\0') {
        return fallback;
    }
    try {
        size_t parsed = 0;
        const auto value = std::stoll(raw, &parsed);
        if (parsed != std::strlen(raw) ||
            value < std::numeric_limits<int32_t>::min() ||
            value > std::numeric_limits<int32_t>::max()) {
            throw std::out_of_range("integer range");
        }
        return static_cast<int32_t>(value);
    } catch (const std::exception&) {
        LOG(WARNING) << name << " is not a valid integer: " << raw;
        return fallback;
    }
}

bool ValidBasePort(int32_t base_port) {
    return base_port >= kMinListenPort &&
           base_port <= kMaxListenPort - kPortRange;
}

std::string EnvOrEmpty(const char* name) {
    const char* raw = std::getenv(name);
    return raw ? std::string(raw) : std::string();
}

bool HasRoceProtocolDesc(const json& root) {
    json protocol;
    if (root.contains("comm_resource_config.protocol_desc")) {
        protocol = root["comm_resource_config.protocol_desc"];
    } else if (root.contains("comm_resource_config") &&
               root["comm_resource_config"].is_object() &&
               root["comm_resource_config"].contains("protocol_desc")) {
        protocol = root["comm_resource_config"]["protocol_desc"];
    } else {
        return false;
    }
    auto is_roce = [](const std::string& desc) {
        return desc.rfind(kRocePrefix, 0) == 0;
    };
    if (protocol.is_string()) {
        return is_roce(protocol.get<std::string>());
    }
    if (protocol.is_array()) {
        for (const auto& item : protocol) {
            if (item.is_string() && is_roce(item.get<std::string>())) {
                return true;
            }
        }
    }
    return false;
}

bool HasFabricMemory(const json& root) {
    if (!root.is_object()) {
        return false;
    }
    if (root.contains(kFabricKey)) {
        return true;
    }
    for (auto it = root.begin(); it != root.end(); ++it) {
        const auto& key = it.key();
        if (key.rfind(kFabricFlatPrefix, 0) == 0) {
            return true;
        }
    }
    return false;
}

json ParseResourceJson(const std::string& raw) {
    if (raw.empty()) {
        return json();
    }
    json parsed = json::parse(raw, nullptr, false);
    if (parsed.is_discarded() || !parsed.is_object()) {
        return json();
    }
    return parsed;
}

std::string ResolveGlobalResourceConfig(const std::string& raw,
                                        bool store_te_init) {
    if (raw.empty()) {
        return {};
    }
    json root = json::parse(raw, nullptr, false);
    if (root.is_discarded() || !root.is_object() || !root.contains(kStoreKey)) {
        return raw;
    }
    if (store_te_init) {
        return root[kStoreKey].dump();
    }
    json normal = root;
    normal.erase(kStoreKey);
    return normal.dump();
}

}  // namespace

bool ParseEnvEnabled(const char* name) {
    return EnvFlagIsOne(std::getenv(name));
}

AscendDirectOptions LoadAscendDirectOptions(
    const std::shared_ptr<Config>& conf) {
    AscendDirectOptions options;
    options.store_te_init =
        conf && conf->get("transports/ascend_direct/store_te_init", false);
    options.agent_mode =
        conf && conf->get("transports/ascend_direct/agent_mode", false);

    options.transfer_timeout_ms = kAscendDefaultTransferTimeoutMs;
    if (conf &&
        conf->contains("transports/ascend_direct/transfer_timeout_ms")) {
        options.transfer_timeout_ms = static_cast<int32_t>(
            conf->get("transports/ascend_direct/transfer_timeout_ms",
                      static_cast<long long>(kAscendDefaultTransferTimeoutMs)));
    }
    options.transfer_timeout_ms =
        ParseEnvInt("ASCEND_TRANSFER_TIMEOUT", options.transfer_timeout_ms);
    if (options.transfer_timeout_ms <= 0) {
        options.transfer_timeout_ms = kAscendDefaultTransferTimeoutMs;
    }

    options.base_port = ParseEnvInt("ASCEND_BASE_PORT", kAscendDefaultBasePort);
    if (conf && conf->contains("transports/ascend_direct/base_port")) {
        options.base_port = static_cast<int32_t>(
            conf->get("transports/ascend_direct/base_port",
                      static_cast<long long>(options.base_port)));
    }
    if (!ValidBasePort(options.base_port)) {
        LOG(WARNING) << "Invalid ASCEND_BASE_PORT/base_port="
                     << options.base_port << ", using "
                     << kAscendDefaultBasePort;
        options.base_port = kAscendDefaultBasePort;
    }

    if (conf) {
        options.rdma_tc = conf->get("transports/ascend_direct/rdma_tc", "");
        options.rdma_sl = conf->get("transports/ascend_direct/rdma_sl", "");
    }
    if (options.rdma_tc.empty()) {
        options.rdma_tc = EnvOrEmpty("ASCEND_RDMA_TC");
    }
    if (options.rdma_tc.empty()) {
        options.rdma_tc = EnvOrEmpty("HCCL_RDMA_TC");
    }
    if (options.rdma_sl.empty()) {
        options.rdma_sl = EnvOrEmpty("ASCEND_RDMA_SL");
    }
    if (options.rdma_sl.empty()) {
        options.rdma_sl = EnvOrEmpty("HCCL_RDMA_SL");
    }

    const char* local_comm = std::getenv("ASCEND_LOCAL_COMM_RES");
    options.local_comm_res = (local_comm && local_comm[0] != '\0')
                                 ? local_comm
                                 : kDefaultLocalCommRes;

    std::string raw_resource;
    if (conf &&
        conf->contains("transports/ascend_direct/global_resource_config")) {
        raw_resource =
            conf->get("transports/ascend_direct/global_resource_config", "");
    }
    if (raw_resource.empty()) {
        raw_resource = EnvOrEmpty("ASCEND_GLOBAL_RESOURCE_CONFIG");
    }
    options.global_resource_config =
        ResolveGlobalResourceConfig(raw_resource, options.store_te_init);

    bool conf_fabric =
        conf && conf->get("transports/ascend_direct/fabric_mem", false);
    json resolved = ParseResourceJson(options.global_resource_config);
    // Store isolation: the TENT shim copies fabric_mem onto Config only when
    // this engine is a Store TE. P2P TENT in the same process does not inherit
    // ASCEND_ENABLE_USE_FABRIC_MEM unless store_te_init is set on this Config.
    options.use_fabric_mem =
        conf_fabric ||
        (options.store_te_init &&
         ParseEnvEnabled("ASCEND_ENABLE_USE_FABRIC_MEM")) ||
        HasFabricMemory(resolved);

    options.roce_mode = ParseEnvEnabled("HCCL_INTRA_ROCE_ENABLE") ||
                        HasRoceProtocolDesc(resolved);
    return options;
}

std::map<std::string, std::string> BuildHixlInitOptions(
    const AscendDirectOptions& options) {
    std::map<std::string, std::string> init;
    init["AutoConnect"] = "1";
    init["LocalCommRes"] = options.local_comm_res;
    if (!options.rdma_tc.empty()) {
        init["RdmaTrafficClass"] = options.rdma_tc;
    }
    if (!options.rdma_sl.empty()) {
        init["RdmaServiceLevel"] = options.rdma_sl;
    }
    if (options.use_fabric_mem) {
        init["EnableUseFabricMem"] = "1";
    }
    if (!options.global_resource_config.empty()) {
        init["GlobalResourceConfig"] = options.global_resource_config;
    }
    return init;
}

uint16_t HixlRoceListenPort(uint16_t hixl_port) {
    constexpr int kShift = 10000;
    if (hixl_port <= static_cast<uint16_t>(65535 - kShift)) {
        return static_cast<uint16_t>(hixl_port + kShift);
    }
    return static_cast<uint16_t>(hixl_port - kShift);
}

std::map<std::string, std::string> WithHixlListenPort(
    const std::map<std::string, std::string>& init, uint16_t listen_port) {
    auto out = init;
    json root = json::object();
    auto it = out.find("GlobalResourceConfig");
    if (it != out.end() && !it->second.empty()) {
        json parsed = json::parse(it->second, nullptr, false);
        if (parsed.is_object()) {
            root = std::move(parsed);
        }
    }
    if (root.contains("comm_resource_config") &&
        root["comm_resource_config"].is_object()) {
        root["comm_resource_config"]["listen_port"] =
            static_cast<int>(listen_port);
        root.erase("comm_resource_config.listen_port");
    } else {
        // The environment commonly uses the flat-key form. Replace an
        // existing value instead of leaving two representations with
        // different ports for the HIXL parser to resolve ambiguously.
        root.erase("comm_resource_config.listen_port");
        root["comm_resource_config.listen_port"] =
            static_cast<int>(listen_port);
    }
    out["GlobalResourceConfig"] = root.dump();
    return out;
}

std::string HostIpFromSegmentName(const std::string& segment_name) {
    if (!segment_name.empty() && segment_name.front() == '[') {
        auto end = segment_name.find(']');
        if (end != std::string::npos) {
            return segment_name.substr(1, end - 1);
        }
    }
    auto pos = segment_name.rfind(':');
    if (pos != std::string::npos) {
        return segment_name.substr(0, pos);
    }
    return segment_name;
}

std::string MakeHixlEngineName(const std::string& host_ip, uint16_t port) {
    if (host_ip.find(':') != std::string::npos &&
        (host_ip.empty() || host_ip.front() != '[')) {
        return "[" + host_ip + "]:" + std::to_string(port);
    }
    return host_ip + ":" + std::to_string(port);
}

uint16_t FindHixlListenPort(int32_t base_port, int32_t device_id) {
    if (device_id < 0 || !ValidBasePort(base_port)) {
        LOG(ERROR) << "Invalid HIXL port range: base_port=" << base_port
                   << ", device_id=" << device_id;
        return 0;
    }
    int32_t physical_dev_id = device_id;
    if (aclrtGetPhyDevIdByLogicDevId(device_id, &physical_dev_id) !=
        ACL_ERROR_NONE) {
        physical_dev_id = device_id;
    }
    static std::random_device rand_gen;
    const int64_t min_port = static_cast<int64_t>(base_port) +
                             static_cast<int64_t>(physical_dev_id) * kPortRange;
    const int64_t max_port =
        static_cast<int64_t>(base_port) +
        (static_cast<int64_t>(physical_dev_id) + 1) * kPortRange;
    if (min_port < kMinListenPort || max_port > kMaxListenPort) {
        LOG(ERROR) << "HIXL port range exceeds TCP port space: [" << min_port
                   << ", " << max_port << "]";
        return 0;
    }
    LOG(INFO) << "Find HIXL listen port between " << min_port << " and "
              << max_port;
    std::uniform_int_distribution<int64_t> rand_dist(min_port, max_port);
    for (int attempt = 0; attempt < kMaxPortAttempts; ++attempt) {
        const int port = static_cast<int>(rand_dist(rand_gen));
        int sockfd = socket(AF_INET, SOCK_STREAM, 0);
        if (sockfd == -1) {
            continue;
        }
        sockaddr_in bind_address{};
        bind_address.sin_family = AF_INET;
        bind_address.sin_port = htons(static_cast<uint16_t>(port));
        bind_address.sin_addr.s_addr = INADDR_ANY;
        if (bind(sockfd, reinterpret_cast<sockaddr*>(&bind_address),
                 sizeof(bind_address)) < 0) {
            close(sockfd);
            continue;
        }
        close(sockfd);
        return static_cast<uint16_t>(port);
    }
    return 0;
}

std::vector<std::string> ParseHixlNames(const MemorySegmentDesc& detail) {
    auto names_it = detail.device_attrs.find(kHixlNamesAttr);
    if (names_it != detail.device_attrs.end() && !names_it->second.empty()) {
        json parsed = json::parse(names_it->second, nullptr, false);
        if (parsed.is_array()) {
            std::vector<std::string> names;
            for (const auto& item : parsed) {
                if (item.is_string()) {
                    names.push_back(item.get<std::string>());
                }
            }
            if (!names.empty()) {
                return names;
            }
        }
    }
    auto name_it = detail.device_attrs.find(kHixlNameAttr);
    if (name_it != detail.device_attrs.end() && !name_it->second.empty()) {
        return {name_it->second};
    }
    return {};
}

std::string ResolveRemoteHixlName(const MemorySegmentDesc& detail,
                                  uint64_t dest_addr) {
    auto names = ParseHixlNames(detail);
    if (names.empty()) {
        return {};
    }
    if (names.size() == 1) {
        return names.front();
    }
    for (const auto& buf : detail.buffers) {
        if (dest_addr < buf.addr || dest_addr - buf.addr >= buf.length) {
            continue;
        }
        auto attr = buf.transport_attrs.find(AscendDirect);
        if (attr != buf.transport_attrs.end() && !attr->second.empty()) {
            try {
                const auto idx = static_cast<size_t>(std::stoul(attr->second));
                if (idx < names.size()) {
                    return names[idx];
                }
            } catch (const std::exception&) {
            }
        }
        LocationParser location(buf.location);
        if (location.type() == "npu" && location.index() >= 0) {
            const auto idx = static_cast<size_t>(location.index());
            if (idx < names.size()) {
                return names[idx];
            }
        }
        return names.front();
    }
    return names.front();
}

}  // namespace tent
}  // namespace mooncake
