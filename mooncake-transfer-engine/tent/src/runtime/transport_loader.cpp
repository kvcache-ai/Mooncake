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

#include "tent/runtime/transfer_engine_impl.h"
#include "tent/transport/shm/shm_transport.h"
#include "tent/transport/tcp/tcp_transport.h"

#ifdef TENT_BUILTIN_TRANSPORTS
// Static linking: include transport headers directly
#ifdef USE_RDMA
#include "tent/transport/rdma/rdma_transport.h"
#endif

#ifdef USE_CUDA
#include "tent/transport/nvlink/nvlink_transport.h"
#include "tent/transport/mnnvl/mnnvl_transport.h"
#endif

#ifdef USE_GDS
#include "tent/transport/gds/gds_transport.h"
#endif

#ifdef USE_URING
#include "tent/transport/io_uring/io_uring_transport.h"
#endif

#if defined(USE_ASCEND) || defined(USE_ASCEND_DIRECT)
#include "tent/transport/ascend/ascend_direct_transport.h"
#endif
#endif  // TENT_BUILTIN_TRANSPORTS

#ifdef TENT_PLUGIN_TRANSPORTS
// Plugin mode: use dlopen to load transports
#include <dlfcn.h>
#include <glob.h>
#include <glog/logging.h>
#include <mutex>
#endif

namespace mooncake {
namespace tent {

#ifdef TENT_PLUGIN_TRANSPORTS

// ============================================================================
// Plugin Mode Implementation
// ============================================================================

using CreateTransportFunc = std::shared_ptr<Transport> (*)();

struct PluginHandle {
    void* handle = nullptr;
    CreateTransportFunc create_func = nullptr;
};

static std::unordered_map<TransportType, PluginHandle> g_loaded_plugins;

static const char* PLUGIN_SEARCH_PATHS[] = {"/usr/local/lib/tent/transport",
                                            "/usr/lib/tent/transport",
                                            "/opt/tent/lib/transport",
                                            "./lib/tent/transport",
                                            "./lib",
                                            nullptr};

static std::string detectPlatform() {
    static const char* libraries[] = {"libcuda.so", "libmusa.so",
                                      "libamdhip64.so", "libascendcl.so",
                                      nullptr};
    for (int i = 0; libraries[i] != nullptr; ++i) {
        void* handle = dlopen(libraries[i], RTLD_NOW | RTLD_NOLOAD);
        if (handle) {
            dlclose(handle);
            if (i == 0) return "cuda";
            if (i == 1) return "musa";
            if (i == 2) return "hip";
            if (i == 3) return "ascend";
        }
    }
    return "cpu";
}

static std::shared_ptr<Transport> tryLoadPlatformPlugin(
    const std::string& base_name, TransportType type, bool optional = true) {
    auto it = g_loaded_plugins.find(type);
    if (it != g_loaded_plugins.end() && it->second.create_func) {
        return it->second.create_func();
    }

    std::string platform = detectPlatform();
    LOG(INFO) << "Detected platform: " << platform << " for transport "
              << base_name;

    std::vector<std::string> lib_names = {
        "libtent_" + base_name + "_" + platform + ".so",
        "libtent_" + base_name + ".so",
    };

    for (const auto& lib_name : lib_names) {
        void* handle = dlopen(lib_name.c_str(), RTLD_NOW | RTLD_LOCAL);

        if (!handle) {
            for (int i = 0; PLUGIN_SEARCH_PATHS[i] != nullptr; ++i) {
                std::string full_path =
                    std::string(PLUGIN_SEARCH_PATHS[i]) + "/" + lib_name;
                handle = dlopen(full_path.c_str(), RTLD_NOW | RTLD_LOCAL);
                if (handle) {
                    LOG(INFO) << "Found plugin at: " << full_path;
                    break;
                }
            }
        }

        if (!handle) continue;

        // Capitalize first letter of base_name for symbol name
        std::string base_cap = base_name;
        if (!base_cap.empty()) base_cap[0] = toupper(base_cap[0]);
        std::string symbol_name = "Create" + base_cap + "Transport";

        auto create_func = reinterpret_cast<CreateTransportFunc>(
            dlsym(handle, symbol_name.c_str()));

        if (!create_func) {
            dlclose(handle);
            LOG(WARNING) << "Plugin " << lib_name << " missing symbol "
                         << symbol_name;
            continue;
        }

        PluginHandle plugin;
        plugin.handle = handle;
        plugin.create_func = create_func;
        g_loaded_plugins[type] = plugin;

        LOG(INFO) << "Loaded transport plugin: " << lib_name
                  << " (symbol=" << symbol_name << ")";
        return create_func();
    }

    if (optional) {
        LOG(INFO) << "Transport " << base_name
                  << " not available as plugin (tried platform=" << platform
                  << ")";
    } else {
        LOG(WARNING) << "Failed to load transport " << base_name;
    }
    return nullptr;
}

#endif  // TENT_PLUGIN_TRANSPORTS

// ============================================================================
// Common loadTransports implementation
// ============================================================================

Status TransferEngineImpl::loadTransports() {
    // Built-in transports (always available)
    if (conf_->get("transports/tcp/enable", true))
        transport_list_[TCP] = std::make_shared<TcpTransport>();

    if (conf_->get("transports/shm/enable", false))
        transport_list_[SHM] = std::make_shared<ShmTransport>();

#ifdef TENT_BUILTIN_TRANSPORTS
    // --------------------------------------------------------------------
    // STATIC MODE: Direct instantiation
    // --------------------------------------------------------------------
#ifdef USE_RDMA
    int rdma_count = topology_->getNicCount(Topology::NIC_RDMA);
    LOG(INFO) << "RDMA NIC count: " << rdma_count;
    if (conf_->get("transports/rdma/enable", true) && rdma_count > 0) {
        transport_list_[RDMA] = std::make_shared<RdmaTransport>();
        LOG(INFO) << "RDMA transport loaded";
    } else {
        LOG(WARNING) << "RDMA transport not loaded: enable="
                     << conf_->get("transports/rdma/enable", true)
                     << ", nic_count=" << rdma_count;
    }
#endif

#ifdef USE_URING
    if (conf_->get("transports/io_uring/enable", true))
        transport_list_[IOURING] = std::make_shared<IOUringTransport>();
#endif

#ifdef USE_CUDA
    bool enable_mnnvl = getenv("MC_ENABLE_MNNVL") != nullptr;
    if (enable_mnnvl) {
        if (conf_->get("transports/mnnvl/enable", true))
            transport_list_[MNNVL] = std::make_shared<MnnvlTransport>();
    } else {
        if (conf_->get("transports/nvlink/enable", true))
            transport_list_[NVLINK] = std::make_shared<NVLinkTransport>();
    }
#endif

#ifdef USE_GDS
    if (conf_->get("transports/gds/enable", false))
        transport_list_[GDS] = std::make_shared<GdsTransport>();
#endif

#if defined(USE_ASCEND) || defined(USE_ASCEND_DIRECT)
    if (conf_->get("transports/ascend_direct/enable", true)) {
        transport_list_[AscendDirect] =
            std::make_shared<AscendDirectTransport>();
    }
#endif

#else   // TENT_PLUGIN_TRANSPORTS
    // --------------------------------------------------------------------
    // PLUGIN MODE: Dynamic loading via dlopen
    // --------------------------------------------------------------------
    if (conf_->get("transports/rdma/enable", true)) {
        auto rdma = tryLoadPlatformPlugin("rdma", RDMA, true);
        if (rdma) transport_list_[RDMA] = rdma;
    }

    if (conf_->get("transports/io_uring/enable", true)) {
        auto iouring = tryLoadPlatformPlugin("iouring", IOURING, true);
        if (iouring) transport_list_[IOURING] = iouring;
    }

    bool enable_mnnvl = getenv("MC_ENABLE_MNNVL") != nullptr;
    if (enable_mnnvl) {
        auto mnnvl = tryLoadPlatformPlugin("mnnvl", MNNVL, true);
        if (mnnvl) transport_list_[MNNVL] = mnnvl;
    } else {
        auto nvlink = tryLoadPlatformPlugin("nvlink", NVLINK, true);
        if (nvlink) transport_list_[NVLINK] = nvlink;
    }

    if (conf_->get("transports/gds/enable", false)) {
        auto gds = tryLoadPlatformPlugin("gds", GDS, true);
        if (gds) transport_list_[GDS] = gds;
    }

    if (conf_->get("transports/ascend_direct/enable", true)) {
        auto ascend = tryLoadPlatformPlugin("ascend", AscendDirect, true);
        if (ascend) transport_list_[AscendDirect] = ascend;
    }
#endif  // TENT_BUILTIN_TRANSPORTS

    return Status::OK();
}

#ifdef TENT_PLUGIN_TRANSPORTS
void TransferEngineImpl::unloadPlugins() {
    for (auto& kv : g_loaded_plugins) {
        if (kv.second.handle) {
            dlclose(kv.second.handle);
        }
    }
    g_loaded_plugins.clear();
}
#endif

}  // namespace tent
}  // namespace mooncake
