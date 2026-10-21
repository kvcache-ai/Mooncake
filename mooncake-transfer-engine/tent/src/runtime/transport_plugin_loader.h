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

#ifndef TENT_TRANSPORT_PLUGIN_LOADER_H
#define TENT_TRANSPORT_PLUGIN_LOADER_H

#include <array>
#include <memory>

#include "tent/common/types.h"

namespace mooncake {
namespace tent {

class Config;
class Topology;
class Transport;

// Fills empty slots of `transports` from the plugins in MC_TENT_PLUGIN_DIR or,
// if that is not set, in the configured plugins/dir. Does nothing when neither
// is set. Logs one line per plugin file; a file it cannot use is skipped.
void loadTransportPlugins(const std::shared_ptr<Config>& conf,
                          const std::shared_ptr<Topology>& topology,
                          std::array<std::shared_ptr<Transport>,
                                     kSupportedTransportTypes>& transports);

}  // namespace tent
}  // namespace mooncake

#endif  // TENT_TRANSPORT_PLUGIN_LOADER_H
