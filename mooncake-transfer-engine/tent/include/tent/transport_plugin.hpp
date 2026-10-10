// Copyright 2025 KVCache.AI
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

// C++ helpers for transport plugins and for the core's plugin loader.
//
// A plugin defines its Transport subclass and adds one line at namespace
// scope, e.g. (plugins/transport/loopback/plugin_entry.cpp):
//
//   TENT_DEFINE_TRANSPORT_PLUGIN_WITH_HOST(
//       loopback, mooncake::tent::LoopbackTransport, mooncake::tent::UNSPEC,
//       TENT_XPORT_FLAG_SAME_MACHINE_ONLY | TENT_XPORT_FLAG_NOTIFICATION |
//           TENT_XPORT_FLAG_CANCELLATION,
//       TENT_XPORT_CAP_DRAM_TO_DRAM)
//
// which emits the C entry point. create() runs `new Class(host)`, so the
// transport learns its assigned slot; TENT_DEFINE_TRANSPORT_PLUGIN runs
// `new Class()` instead.

#ifndef TENT_TRANSPORT_PLUGIN_HPP
#define TENT_TRANSPORT_PLUGIN_HPP

#include <cstdint>
#include <exception>
#include <string>

#include "tent/common/config.h"
#include "tent/common/status.h"
#include "tent/common/types.h"
#include "tent/runtime/control_plane.h"
#include "tent/runtime/segment.h"
#include "tent/runtime/segment_manager.h"
#include "tent/runtime/topology.h"
#include "tent/runtime/transport.h"
#include "tent/transport_plugin.h"

namespace mooncake {
namespace tent {

namespace transport_plugin_detail {
constexpr uint64_t kFnvOffset = 14695981039346656037ull;
constexpr uint64_t kFnvPrime = 1099511628211ull;

constexpr uint64_t mix(uint64_t hash, uint64_t value) {
    for (int i = 0; i < 8; ++i) {
        hash ^= (value >> (8 * i)) & 0xffu;
        hash *= kFnvPrime;
    }
    return hash;
}
}  // namespace transport_plugin_detail

// FNV-1a over the ABI version, the sizes of the types a plugin and the core
// exchange, and the compiler / C++ library ABI. Plugin and core each
// evaluate it at their own compile time. It catches toolchain and build
// configuration drift and size changes; it cannot see field reordering or
// changes to virtual functions, which must bump
// TENT_TRANSPORT_PLUGIN_ABI_VERSION (tent_transport_abi_manifest_test makes
// sure every change to these headers is looked at).
constexpr uint64_t tent_transport_abi_fingerprint() {
    using transport_plugin_detail::mix;
    uint64_t h = transport_plugin_detail::kFnvOffset;
    h = mix(h, TENT_TRANSPORT_PLUGIN_ABI_VERSION);
    h = mix(h, sizeof(Transport));
    h = mix(h, sizeof(Transport::SubBatch));
    h = mix(h, sizeof(Request));
    h = mix(h, sizeof(BufferDesc));
    h = mix(h, sizeof(Status));
    h = mix(h, sizeof(Capabilities));
    h = mix(h, sizeof(MemoryOptions));
    h = mix(h, sizeof(Notification));
    h = mix(h, sizeof(TransferStatus));
    h = mix(h, sizeof(SegmentDesc));
    h = mix(h, sizeof(ControlService));
    h = mix(h, sizeof(SegmentManager));
    h = mix(h, sizeof(Topology));
    h = mix(h, sizeof(Config));
    h = mix(h, sizeof(NicLoadStats));
    h = mix(h, sizeof(DeviceDesc));
    h = mix(h, sizeof(Region));
    h = mix(h, kNumTransportTypes);
#ifdef __GNUC__
    h = mix(h, __GNUC__);
#endif
#ifdef __GLIBCXX__
    h = mix(h, __GLIBCXX__);
#endif
#ifdef _GLIBCXX_USE_CXX11_ABI
    h = mix(h, _GLIBCXX_USE_CXX11_ABI);
#endif
    h = mix(h, __cplusplus);
    return h;
}

namespace transport_plugin_detail {
// Reason for the last failed probe() or create() on this thread. Hidden, so
// there is one buffer per shared object: every source file of a plugin
// shares it, and it is never merged with the core's or another plugin's.
__attribute__((visibility("hidden"))) inline std::string& lastError() {
    thread_local std::string error;
    return error;
}

__attribute__((visibility("hidden"))) inline void setLastError(
    const std::string& message) {
    lastError() = message;
}
}  // namespace transport_plugin_detail

}  // namespace tent
}  // namespace mooncake

// Lets a test build a plugin whose fingerprint does not match its headers.
#ifdef TENT_TRANSPORT_ABI_FINGERPRINT_OVERRIDE
#define TENT_TRANSPORT_PLUGIN_FINGERPRINT_ \
    ((uint64_t)(TENT_TRANSPORT_ABI_FINGERPRINT_OVERRIDE))
#else
#define TENT_TRANSPORT_PLUGIN_FINGERPRINT_ \
    (::mooncake::tent::tent_transport_abi_fingerprint())
#endif

// Full form. PROBE is a function `int(const tent_transport_plugin_host_t*)`
// or nullptr; NEW_EXPR may use `host` (const tent_transport_plugin_host_t*).
#define TENT_DEFINE_TRANSPORT_PLUGIN_EX(NAME, VERSION, BUILTIN, FLAGS, CAPS,   \
                                        PROBE, NEW_EXPR)                       \
    namespace {                                                                \
    void* tentPluginCreate_(const tent_transport_plugin_host_t* host) {        \
        (void)host;                                                            \
        try {                                                                  \
            ::mooncake::tent::Transport* transport = NEW_EXPR;                 \
            return transport;                                                  \
        } catch (const std::exception& e) {                                    \
            ::mooncake::tent::transport_plugin_detail::setLastError(e.what()); \
        } catch (...) {                                                        \
            ::mooncake::tent::transport_plugin_detail::setLastError(           \
                "unknown exception in create()");                              \
        }                                                                      \
        return nullptr;                                                        \
    }                                                                          \
    void tentPluginDestroy_(void* transport) {                                 \
        delete static_cast<::mooncake::tent::Transport*>(transport);           \
    }                                                                          \
    const char* tentPluginLastError_() {                                       \
        return ::mooncake::tent::transport_plugin_detail::lastError().c_str(); \
    }                                                                          \
    }                                                                          \
    extern "C" __attribute__((visibility("default"))) int                      \
    tent_transport_plugin_register(tent_transport_plugin_t* out) {             \
        if (!out || out->struct_size < sizeof(tent_transport_plugin_t))        \
            return TENT_XPORT_REGISTER_E_STRUCT_SIZE;                          \
        out->struct_size = sizeof(tent_transport_plugin_t);                    \
        out->abi_version = TENT_TRANSPORT_PLUGIN_ABI_VERSION;                  \
        out->abi_fingerprint = TENT_TRANSPORT_PLUGIN_FINGERPRINT_;             \
        out->name = #NAME;                                                     \
        out->version = VERSION;                                                \
        out->builtin_type = static_cast<int>(BUILTIN);                         \
        out->flags = (FLAGS);                                                  \
        out->declared_caps = (CAPS);                                           \
        out->probe = PROBE;                                                    \
        out->create = tentPluginCreate_;                                       \
        out->destroy = tentPluginDestroy_;                                     \
        out->last_error = tentPluginLastError_;                                \
        return TENT_XPORT_REGISTER_OK;                                         \
    }

#ifndef TENT_TRANSPORT_PLUGIN_VERSION
#define TENT_TRANSPORT_PLUGIN_VERSION "0"
#endif

#define TENT_DEFINE_TRANSPORT_PLUGIN(NAME, CLASS, BUILTIN, FLAGS, CAPS)  \
    TENT_DEFINE_TRANSPORT_PLUGIN_EX(NAME, TENT_TRANSPORT_PLUGIN_VERSION, \
                                    BUILTIN, FLAGS, CAPS, nullptr,       \
                                    new CLASS())

#define TENT_DEFINE_TRANSPORT_PLUGIN_WITH_HOST(NAME, CLASS, BUILTIN, FLAGS, \
                                               CAPS)                        \
    TENT_DEFINE_TRANSPORT_PLUGIN_EX(NAME, TENT_TRANSPORT_PLUGIN_VERSION,    \
                                    BUILTIN, FLAGS, CAPS, nullptr,          \
                                    new CLASS(host))

#endif  // TENT_TRANSPORT_PLUGIN_HPP
