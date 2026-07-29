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

#ifndef MOONCAKE_STORE_CLIENT_TRANSPORT_CONFIG_H_
#define MOONCAKE_STORE_CLIENT_TRANSPORT_CONFIG_H_

#include <string>

namespace mooncake::internal {

struct TransportDiscoveryPlan {
    bool transfer_engine_auto_discover;
    bool explicitly_discover_efa;
};

inline TransportDiscoveryPlan MakeTransportDiscoveryPlan(
    const std::string& protocol, bool auto_discover, bool force_tcp) {
    if (force_tcp) {
        return {.transfer_engine_auto_discover = false,
                .explicitly_discover_efa = false};
    }

    const bool explicitly_discover_efa = auto_discover && protocol == "efa";
    return {
        .transfer_engine_auto_discover =
            auto_discover && !explicitly_discover_efa,
        .explicitly_discover_efa = explicitly_discover_efa,
    };
}

}  // namespace mooncake::internal

#endif  // MOONCAKE_STORE_CLIENT_TRANSPORT_CONFIG_H_
