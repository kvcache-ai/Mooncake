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

#include <cstring>
#include <memory>
#include <random>

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
}  // namespace

bool HasRoceProtocolDescInGlobalResourceConfig(const char* config_str) {
    if (config_str == nullptr || config_str[0] == '\0') {
        return false;
    }
    Json::CharReaderBuilder builder;
    builder["collectComments"] = false;
    Json::Value root;
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
    const Json::Value* protocol_desc = FindProtocolDescValue(root);
    if (protocol_desc == nullptr) {
        return false;
    }
    return ProtocolDescValueContainsRoce(*protocol_desc);
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

bool IsRoceModeEnabled() {
    const char* store_te =
        globalConfig().ascend_store_te_init ? "true" : "false";
    char* roce_enable_str = std::getenv("HCCL_INTRA_ROCE_ENABLE");
    LOG(INFO) << "[AscendTE] resolving link config, te is created for store="
              << store_te << ", HCCL_INTRA_ROCE_ENABLE="
              << (roce_enable_str ? roce_enable_str : "(unset)");
    if (roce_enable_str) {
        const auto roce_enable = parseFromString<int32_t>(roce_enable_str);
        if (roce_enable.has_value() && roce_enable.value() == 1) {
            LOG(INFO) << "[AscendTE] roce mode enabled via "
                         "HCCL_INTRA_ROCE_ENABLE.";
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
        LOG(INFO) << "[AscendTE] roce mode enabled via "
                     "ASCEND_GLOBAL_RESOURCE_CONFIG protocol_desc.";
        return true;
    }
    return false;
}

// AscendThreadPool implementation
AscendThreadPool::AscendThreadPool(size_t num_threads) : running_(true) {
    for (size_t i = 0; i < num_threads; ++i) {
        workers_.emplace_back([this] {
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