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

#ifndef MOONCAKE_SHOW_LINKS_H_
#define MOONCAKE_SHOW_LINKS_H_

#include <cstdint>
#include <string>
#include <vector>

namespace mooncake {

struct NicDiagInfo {
    std::string device_name;
    int numa_node;
    std::string gid;
    int gid_index;
    uint8_t port;
    int active_speed;
    int active_width;
};

class TransferEngineImpl;

std::string buildShowLinksJson(TransferEngineImpl* impl);

std::string buildShowLinksReadable(TransferEngineImpl* impl);

}  // namespace mooncake

#endif  // MOONCAKE_SHOW_LINKS_H_
