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

// Broken transport plugins for transport_plugin_registry_test. Each build
// defines exactly one BAD_PLUGIN_* macro; tests/CMakeLists.txt maps every
// variant to the file name and the loader outcome it must produce.

#include <stdexcept>

#include "tent/transport_plugin.hpp"

namespace mooncake {
namespace tent {
namespace bad_plugin {

class StubTransport : public Transport {
   public:
    explicit StubTransport(const tent_transport_plugin_host_t* host)
        : type_(static_cast<TransportType>(host ? host->assigned_type : 0)) {}

    Status install(std::string&, std::shared_ptr<ControlService>,
                   std::shared_ptr<Topology>,
                   std::shared_ptr<Config>) override {
#ifdef BAD_PLUGIN_INSTALL_FAILS
        return Status::InternalError("stub install failure" LOC_MARK);
#endif
        caps.dram_to_dram = true;
#ifdef BAD_PLUGIN_CAPS_OVERCLAIM
        caps.gpu_to_gpu = true;
#endif
        return Status::OK();
    }

    Status addMemoryBuffer(BufferDesc& desc, const MemoryOptions&) override {
        desc.transports.push_back(type_);
        return Status::OK();
    }

    Status removeMemoryBuffer(BufferDesc&) override { return Status::OK(); }

    const char* getName() const override { return "stub"; }

#ifdef BAD_PLUGIN_NOTIFY_OVERCLAIM
    // Declares no TENT_XPORT_FLAG_NOTIFICATION.
    bool supportNotification() const override { return true; }
#endif
#ifdef BAD_PLUGIN_CANCEL_OVERCLAIM
    // Declares no TENT_XPORT_FLAG_CANCELLATION.
    bool supportsCancellation() const override { return true; }
#endif

   private:
    TransportType type_;
};

[[maybe_unused]] int failingProbe(const tent_transport_plugin_host_t*) {
    transport_plugin_detail::setLastError("stub device not present");
    return 3;
}

[[maybe_unused]] void fillDescriptor(tent_transport_plugin_t* out,
                                     const char* name) {
    out->struct_size = sizeof(tent_transport_plugin_t);
    out->abi_version = TENT_TRANSPORT_PLUGIN_ABI_VERSION;
    out->abi_fingerprint = tent_transport_abi_fingerprint();
    out->name = name;
    out->version = "bad";
    out->builtin_type = UNSPEC;
    out->flags = TENT_XPORT_FLAG_ENABLED_BY_DEFAULT;
    out->declared_caps = TENT_XPORT_CAP_DRAM_TO_DRAM;
    out->create = [](const tent_transport_plugin_host_t* host) -> void* {
        return static_cast<Transport*>(new StubTransport(host));
    };
    out->destroy = [](void* transport) {
        delete static_cast<Transport*>(transport);
    };
}

}  // namespace bad_plugin
}  // namespace tent
}  // namespace mooncake

namespace mtb = mooncake::tent::bad_plugin;

#define BAD_FLAGS TENT_XPORT_FLAG_ENABLED_BY_DEFAULT
#define BAD_CAPS TENT_XPORT_CAP_DRAM_TO_DRAM

#if defined(BAD_PLUGIN_ABI_VERSION)
// Built against a later ABI version that kept the descriptor size.
extern "C" __attribute__((visibility("default"))) int
tent_transport_plugin_register(tent_transport_plugin_t* out) {
    mtb::fillDescriptor(out, "abi_version");
    out->abi_version = TENT_TRANSPORT_PLUGIN_ABI_VERSION + 1;
    return TENT_XPORT_REGISTER_OK;
}
#elif defined(BAD_PLUGIN_NEWER_STRUCT)
// Built against a later ABI whose descriptor grew: it refuses to write into
// the smaller descriptor of this core.
extern "C" __attribute__((visibility("default"))) int
tent_transport_plugin_register(tent_transport_plugin_t* out) {
    if (!out || out->struct_size < sizeof(tent_transport_plugin_t) + 16)
        return TENT_XPORT_REGISTER_E_STRUCT_SIZE;
    return TENT_XPORT_REGISTER_OK;
}
#elif defined(BAD_PLUGIN_FINGERPRINT)
// Compiled with TENT_TRANSPORT_ABI_FINGERPRINT_OVERRIDE.
TENT_DEFINE_TRANSPORT_PLUGIN_WITH_HOST(fingerprint, mtb::StubTransport,
                                       mooncake::tent::UNSPEC, BAD_FLAGS,
                                       BAD_CAPS)
#elif defined(BAD_PLUGIN_MISSING_ENTRY)
extern "C" __attribute__((visibility("default"))) int
tent_transport_plugin_register_v0(tent_transport_plugin_t*) {
    return TENT_XPORT_REGISTER_OK;
}
#elif defined(BAD_PLUGIN_NULL_CREATE)
TENT_DEFINE_TRANSPORT_PLUGIN_EX(
    null_create, "bad", mooncake::tent::UNSPEC, BAD_FLAGS, BAD_CAPS, nullptr,
    (mooncake::tent::transport_plugin_detail::setLastError("stub refuses"),
     nullptr))
#elif defined(BAD_PLUGIN_PROBE_FAILS)
TENT_DEFINE_TRANSPORT_PLUGIN_EX(probe_fails, "bad", mooncake::tent::UNSPEC,
                                BAD_FLAGS, BAD_CAPS, mtb::failingProbe,
                                new mtb::StubTransport(host))
#elif defined(BAD_PLUGIN_CAPS_OVERCLAIM)
TENT_DEFINE_TRANSPORT_PLUGIN_WITH_HOST(caps_overclaim, mtb::StubTransport,
                                       mooncake::tent::UNSPEC, BAD_FLAGS,
                                       BAD_CAPS)
#elif defined(BAD_PLUGIN_INSTALL_FAILS)
TENT_DEFINE_TRANSPORT_PLUGIN_WITH_HOST(install_fails, mtb::StubTransport,
                                       mooncake::tent::UNSPEC, BAD_FLAGS,
                                       BAD_CAPS)
#elif defined(BAD_PLUGIN_WRONG_NAME)
// Shipped as libtent_xport_wrongname.so.
TENT_DEFINE_TRANSPORT_PLUGIN_WITH_HOST(something_else, mtb::StubTransport,
                                       mooncake::tent::UNSPEC, BAD_FLAGS,
                                       BAD_CAPS)
#elif defined(BAD_PLUGIN_CONFLICT_TCP)
// Claims the built-in TCP slot, which the core already fills.
TENT_DEFINE_TRANSPORT_PLUGIN_WITH_HOST(tcp, mtb::StubTransport,
                                       mooncake::tent::TCP, BAD_FLAGS, BAD_CAPS)
#elif defined(BAD_PLUGIN_BUILTIN_NAME)
// Uses a built-in name without claiming the built-in slot.
TENT_DEFINE_TRANSPORT_PLUGIN_WITH_HOST(shm, mtb::StubTransport,
                                       mooncake::tent::UNSPEC, BAD_FLAGS,
                                       BAD_CAPS)
#elif defined(BAD_PLUGIN_CREATE_THROWS)
// A hand-written entry whose create() lets an exception escape.
extern "C" __attribute__((visibility("default"))) int
tent_transport_plugin_register(tent_transport_plugin_t* out) {
    mtb::fillDescriptor(out, "create_throws");
    out->create = [](const tent_transport_plugin_host_t*) -> void* {
        throw std::runtime_error("stub create exploded");
    };
    return TENT_XPORT_REGISTER_OK;
}
#elif defined(BAD_PLUGIN_PROBE_THROWS)
extern "C" __attribute__((visibility("default"))) int
tent_transport_plugin_register(tent_transport_plugin_t* out) {
    mtb::fillDescriptor(out, "probe_throws");
    out->probe = [](const tent_transport_plugin_host_t*) -> int {
        throw std::runtime_error("stub probe exploded");
    };
    return TENT_XPORT_REGISTER_OK;
}
#elif defined(BAD_PLUGIN_BUILTIN_OUT_OF_RANGE)
extern "C" __attribute__((visibility("default"))) int
tent_transport_plugin_register(tent_transport_plugin_t* out) {
    mtb::fillDescriptor(out, "builtin_range");
    out->builtin_type = 1000;
    return TENT_XPORT_REGISTER_OK;
}
#elif defined(BAD_PLUGIN_NOTIFY_OVERCLAIM)
TENT_DEFINE_TRANSPORT_PLUGIN_WITH_HOST(notify_overclaim, mtb::StubTransport,
                                       mooncake::tent::UNSPEC, BAD_FLAGS,
                                       BAD_CAPS)
#elif defined(BAD_PLUGIN_CANCEL_OVERCLAIM)
TENT_DEFINE_TRANSPORT_PLUGIN_WITH_HOST(cancel_overclaim, mtb::StubTransport,
                                       mooncake::tent::UNSPEC, BAD_FLAGS,
                                       BAD_CAPS)
#elif defined(BAD_PLUGIN_NOT_A_PLUGIN)
// Shipped as libnotaplugin.so: must be ignored by file name.
extern "C" int tent_not_a_transport_plugin(void) { return 0; }
#else
#error "define one BAD_PLUGIN_* variant"
#endif
