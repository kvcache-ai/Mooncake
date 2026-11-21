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

#include "transport/transport_registry.h"

namespace mooncake {

std::vector<TransportPlugin>& TransportRegistry::plugins() {
    static auto* plugins = new std::vector<TransportPlugin>;
    return *plugins;
}

void TransportRegistry::registerPlugin(TransportPlugin plugin) {
    plugins().push_back(plugin);
}

BuiltinTransportRegistration::BuiltinTransportRegistration(
    TransportPlugin plugin) {
    TransportRegistry::registerPlugin(plugin);
}

const TransportPlugin* TransportRegistry::find(const std::string& protocol) {
    linkBuiltinTransportRegistrations();

    const TransportPlugin* selected = nullptr;
    for (const auto& plugin : plugins()) {
        if (protocol == plugin.protocol &&
            (selected == nullptr || plugin.priority < selected->priority)) {
            selected = &plugin;
        }
    }
    return selected;
}

std::unique_ptr<Transport> TransportRegistry::create(
    const std::string& protocol) {
    const auto* plugin = find(protocol);
    return plugin ? plugin->create() : nullptr;
}

bool TransportRegistry::requiresExclusiveEngine(const std::string& protocol) {
    const auto* plugin = find(protocol);
    return plugin && plugin->requires_exclusive_engine;
}

void TransportRegistry::prepareInstall(const std::string& protocol,
                                       const std::shared_ptr<Topology>& topo) {
    const auto* plugin = find(protocol);
    if (plugin && plugin->prepare_install) {
        plugin->prepare_install(topo);
    }
}

}  // namespace mooncake
