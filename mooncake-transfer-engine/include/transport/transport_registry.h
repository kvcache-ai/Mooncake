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

#pragma once

#include <memory>
#include <string>
#include <vector>

#include "transport/transport.h"

namespace mooncake {

class Topology;

// A statically linked transport module. Every transport implementation
// registers its own descriptor, mirroring FFmpeg's allformats registry.
struct TransportPlugin {
    const char* protocol;
    std::unique_ptr<Transport> (*create)();
    bool requires_exclusive_engine;
    void (*prepare_install)(const std::shared_ptr<Topology>& topo);
    int priority;
};

template <typename T>
std::unique_ptr<Transport> createTransport() {
    return std::make_unique<T>();
}

// References every builtin transport so that its static registration is kept
// when transfer_engine is linked as a static library.
void linkBuiltinTransportRegistrations();

class BuiltinTransportRegistration;

class TransportRegistry {
   public:
    static std::unique_ptr<Transport> create(const std::string& protocol);

    static bool requiresExclusiveEngine(const std::string& protocol);

    static void prepareInstall(const std::string& protocol,
                               const std::shared_ptr<Topology>& topo);

   private:
    static const TransportPlugin* find(const std::string& protocol);
    static void registerPlugin(TransportPlugin plugin);
    static std::vector<TransportPlugin>& plugins();

    friend class BuiltinTransportRegistration;
};

class BuiltinTransportRegistration {
   public:
    explicit BuiltinTransportRegistration(TransportPlugin plugin);
};

#define MOONCAKE_REGISTER_BUILTIN_TRANSPORT(id, protocol_name, transport_type) \
    MOONCAKE_REGISTER_BUILTIN_TRANSPORT_WITH_OPTIONS(                          \
        id, protocol_name, transport_type, false, nullptr, 0)

#define MOONCAKE_REGISTER_BUILTIN_TRANSPORT_WITH_OPTIONS(                 \
    id, protocol_name, transport_type, exclusive, install_hook, priority) \
    namespace {                                                           \
    const ::mooncake::BuiltinTransportRegistration                        \
        kBuiltinTransportRegistration_##id(                               \
            {protocol_name, &::mooncake::createTransport<transport_type>, \
             exclusive, install_hook, priority});                         \
    }                                                                     \
    extern "C" void mooncake_builtin_transport_##id() {}

}  // namespace mooncake
