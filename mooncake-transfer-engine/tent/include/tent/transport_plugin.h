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

#ifndef TENT_TRANSPORT_PLUGIN_H
#define TENT_TRANSPORT_PLUGIN_H

#include <memory>

#include "tent/build_id.h"
#include "tent/common/types.h"

// A transport plugin is a shared library named libtent_xport_<name>.so; see
// docs/source/design/tent/transport-plugins.md.

// The build that TENT, or a plugin, comes from: the source revision and the
// compiler, which CMake records (tent/build_id.h), and the libstdc++ string ABI
// of the code that expands this macro. TENT and each plugin expand it in their
// own code.
#define TENT_BUILD_ID_STR_(x) #x
#define TENT_BUILD_ID_STR(x) TENT_BUILD_ID_STR_(x)
#ifdef _GLIBCXX_USE_CXX11_ABI
#define TENT_BUILD_ID \
    TENT_BUILD_SOURCE " cxx11abi=" TENT_BUILD_ID_STR(_GLIBCXX_USE_CXX11_ABI)
#else
#define TENT_BUILD_ID TENT_BUILD_SOURCE " cxx11abi=none"
#endif

namespace mooncake {
namespace tent {

class Config;
class Topology;
class Transport;

struct TransportPluginContext {
    std::shared_ptr<Config> conf;
    std::shared_ptr<Topology> topology;
};

struct TransportPluginDesc {
    TransportType type;  // the slot it provides, an existing TransportType
    // Called once per engine. Returns the transport, or nullptr when the
    // plugin does not apply to this engine (no suitable NIC, say).
    std::shared_ptr<Transport> (*create)(const TransportPluginContext&);
};

}  // namespace tent
}  // namespace mooncake

// Every plugin exports these two functions. The first is plain C and returns
// the plugin's TENT_BUILD_ID; TENT compares it with its own before it calls any
// of the plugin's C++ code. The second is called only if they match.
#define TENT_TRANSPORT_PLUGIN_BUILD_ID_ENTRY "tent_transport_plugin_build_id"
extern "C" __attribute__((visibility("default"))) const char*
tent_transport_plugin_build_id();

#define TENT_TRANSPORT_PLUGIN_ENTRY "tent_register_transport_plugin"
extern "C" __attribute__((visibility("default")))
const mooncake::tent::TransportPluginDesc*
tent_register_transport_plugin();

#endif  // TENT_TRANSPORT_PLUGIN_H
