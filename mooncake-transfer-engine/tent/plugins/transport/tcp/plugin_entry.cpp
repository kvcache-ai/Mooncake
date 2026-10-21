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

// TENT's TCP transport as a plugin (TENT_XPORT_TCP=plugin), created under the
// same condition as the built-in one in transport_loader.cpp.

#include "tent/common/config.h"
#include "tent/transport/tcp/tcp_transport.h"
#include "tent/transport_plugin.h"

namespace mooncake {
namespace tent {
namespace {

std::shared_ptr<Transport> create(const TransportPluginContext& context) {
    if (!context.conf->get("transports/tcp/enable", true)) return nullptr;
    return std::make_shared<TcpTransport>();
}

}  // namespace
}  // namespace tent
}  // namespace mooncake

extern "C" const char* tent_transport_plugin_build_id() {
    return TENT_BUILD_ID;
}

extern "C" const mooncake::tent::TransportPluginDesc*
tent_register_transport_plugin() {
    static const mooncake::tent::TransportPluginDesc desc{
        mooncake::tent::TCP, &mooncake::tent::create};
    return &desc;
}
