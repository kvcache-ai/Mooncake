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

// Plugins the loader must skip, one per VARIANT_<name> (tests/CMakeLists.txt).
// VARIANT_no_build_id builds a library without the build ID function,
// VARIANT_no_entry one without the descriptor entry point.

#include <cstdlib>
#include <stdexcept>

#include "tent/transport_plugin.h"

#ifndef VARIANT_no_build_id
extern "C" const char* tent_transport_plugin_build_id() {
#ifdef VARIANT_wrong_build
    return "another build";
#else
    return TENT_BUILD_ID;
#endif
}
#endif  // VARIANT_no_build_id

#ifndef VARIANT_no_entry

namespace {

using mooncake::tent::Transport;
using mooncake::tent::TransportPluginContext;

std::shared_ptr<Transport> create(const TransportPluginContext&) {
#ifdef VARIANT_throws
    throw std::runtime_error("no device for this plugin");
#endif
    return nullptr;
}

#ifdef VARIANT_bad_type
constexpr auto kType = mooncake::tent::kNumTransportTypes;
#else
constexpr auto kType = mooncake::tent::SHM;
#endif

}  // namespace

extern "C" const mooncake::tent::TransportPluginDesc*
tent_register_transport_plugin() {
#if defined(VARIANT_no_build_id) || defined(VARIANT_wrong_build)
    // The loader must reject these two before it calls any of their C++ code.
    std::abort();
#endif
    static const mooncake::tent::TransportPluginDesc desc{kType, &create};
    return &desc;
}

#endif  // VARIANT_no_entry
