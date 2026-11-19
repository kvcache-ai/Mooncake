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

#include "tent/common/config_parser.h"

#include <algorithm>
#include <stdexcept>

namespace mooncake {
namespace tent {
namespace {

Status getRpcIntegerFromConfig(const Config& config, const std::string& path,
                               uint64_t default_value, uint64_t minimum,
                               uint64_t maximum, uint64_t& output) {
    if (!config.contains(path)) {
        output = default_value;
        return Status::OK();
    }

    auto value = config.get<json>(path, json());
    if (value.is_string()) {
        const auto text = value.get<std::string>();
        try {
            size_t consumed = 0;
            const auto number = std::stoll(text, &consumed);
            if (consumed == text.size()) value = number;
        } catch (const std::invalid_argument&) {
        } catch (const std::out_of_range&) {
        }
    }
    if (!value.is_number_integer()) {
        return Status::InvalidArgument(
            path + " must be an integer or integer string" + LOC_MARK);
    }

    uint64_t number = 0;
    CHECK_STATUS(parseUnsignedConfigValue(value, path, &number));
    if (number < minimum || number > maximum) {
        return Status::InvalidArgument(
            path + " must be in range [" + std::to_string(minimum) + ", " +
            std::to_string(maximum) + "]" + LOC_MARK);
    }
    output = number;
    return Status::OK();
}

}  // namespace

Status parseBoolConfigValue(const json& node, const std::string& path,
                            bool* output) {
    if (!node.is_boolean()) {
        return Status::InvalidArgument(path + " must be a boolean" + LOC_MARK);
    }
    *output = node.get<bool>();
    return Status::OK();
}

Status getRpcServerPortFromConfig(const Config& config, uint16_t default_value,
                                  uint16_t& port) {
    uint64_t value = 0;
    CHECK_STATUS(getRpcIntegerFromConfig(config, "rpc_server_port",
                                         default_value, 0, 65535, value));
    port = static_cast<uint16_t>(value);
    return Status::OK();
}

Status getRpcServerThreadsFromConfig(const Config& config, size_t default_value,
                                     size_t& threads) {
    uint64_t value = 0;
    CHECK_STATUS(getRpcIntegerFromConfig(config, "rpc_server_threads",
                                         default_value, 1, 1024, value));
    threads = static_cast<size_t>(value);
    return Status::OK();
}

size_t getDefaultRpcServerThreads(const Config& config,
                                  unsigned hardware_concurrency) {
    // RPC defaults use explicit TCP enablement; transport selection itself
    // defaults TCP on. Keep those two defaults distinct.
    return config.get("transports/tcp/enable", false)
               ? std::min<size_t>(8, std::max<size_t>(4, hardware_concurrency))
               : 1;
}

Status validateTcpTransportSelection(bool tcp_enabled, bool hp_tcp_enabled) {
    if (hp_tcp_enabled && tcp_enabled) {
        return Status::InvalidArgument(
            "transports tcp and hp_tcp cannot be enabled together" LOC_MARK);
    }
    return Status::OK();
}

Status validateRuntimeQueueDispatchWindow(bool enabled, size_t max_owners,
                                          size_t max_bytes) {
    if (enabled && (max_owners == 0 || max_bytes == 0)) {
        return Status::InvalidArgument(
            "runtime queue dispatch window must be non-zero" LOC_MARK);
    }
    return Status::OK();
}

}  // namespace tent
}  // namespace mooncake
