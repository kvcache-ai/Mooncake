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

#ifndef TENT_CONFIG_PARSER_H
#define TENT_CONFIG_PARSER_H

#include <limits>
#include <type_traits>

#include "tent/common/config.h"

namespace mooncake {
namespace tent {

// Shared by the HP TCP parser and candidate validation. Check the original
// JSON number before narrowing; Config::get<T>() can silently wrap it.
template <typename T>
Status parseUnsignedConfigValue(const json& node, const std::string& path,
                                T* output) {
    static_assert(std::is_integral_v<T> && !std::is_same_v<T, bool>);
    uint64_t value = 0;
    if (node.is_number_unsigned()) {
        value = node.get<uint64_t>();
    } else if (node.is_number_integer()) {
        const auto signed_value = node.get<int64_t>();
        if (signed_value < 0) {
            return Status::InvalidArgument(path + " must be non-negative" +
                                           LOC_MARK);
        }
        value = static_cast<uint64_t>(signed_value);
    } else {
        return Status::InvalidArgument(path + " must be an integer" + LOC_MARK);
    }
    if (value > static_cast<uint64_t>(std::numeric_limits<T>::max())) {
        return Status::InvalidArgument(path + " is out of range" + LOC_MARK);
    }
    *output = static_cast<T>(value);
    return Status::OK();
}

Status parseBoolConfigValue(const json& node, const std::string& path,
                            bool* output);

// Startup and preflight share RPC defaults, integer-string compatibility and
// range checks. Errors identify the field and constraint without echoing
// values.
Status getRpcServerPortFromConfig(const Config& config, uint16_t default_value,
                                  uint16_t& port);
Status getRpcServerThreadsFromConfig(const Config& config, size_t default_value,
                                     size_t& threads);
size_t getDefaultRpcServerThreads(const Config& config,
                                  unsigned hardware_concurrency);

Status validateTcpTransportSelection(bool tcp_enabled, bool hp_tcp_enabled);
Status validateRuntimeQueueDispatchWindow(bool enabled, size_t max_owners,
                                          size_t max_bytes);

}  // namespace tent
}  // namespace mooncake

#endif  // TENT_CONFIG_PARSER_H
