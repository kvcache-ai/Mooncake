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

/**
 * @file transport_loader.h
 * @brief Transport plugin loading mechanism
 *
 * This file provides both:
 * 1. TENT_EXPORT_TRANSPORT macro - for transport implementations to export
 * themselves
 * 2. TransportLoader class - for the runtime to dynamically load transports
 */

#ifndef TENT_TRANSPORT_LOADER_H
#define TENT_TRANSPORT_LOADER_H

#include "tent/runtime/transport.h"
#include "tent/common/status.h"
#include "tent/common/types.h"
#include <memory>
#include <vector>
#include <string>
#include <unordered_map>

namespace mooncake {
namespace tent {

// ============================================================
// Plugin Export Macro (for transport implementations)
// ============================================================

/**
 * @brief Macro to export a transport as a loadable plugin
 *
 * Usage: In your transport source file (after namespace closing), add:
 *
 *   using mooncake::tent::RdmaTransport;
 *   TENT_EXPORT_TRANSPORT(RdmaTransport, "libtent_rdma.so");
 *
 * This will export a function "CreateRdmaTransport()" that can be
 * loaded via dlopen/dlsym.
 *
 * IMPORTANT: This macro only exports symbols when TENT_BUILD_PLUGIN is defined.
 * When linking statically into the main library, these symbols are not
 * generated.
 */
#ifdef TENT_BUILD_PLUGIN
#define TENT_EXPORT_TRANSPORT(TransportClass, LibraryName)                \
    extern "C" {                                                          \
    std::shared_ptr<mooncake::tent::Transport> Create##TransportClass() { \
        return std::make_shared<TransportClass>();                        \
    }                                                                     \
    const char* GetTransportPluginLibrary() { return LibraryName; }       \
    }
#else
#define TENT_EXPORT_TRANSPORT(TransportClass, \
                              LibraryName) /* No export when static */
#endif

// ============================================================
// Transport Loader (for runtime plugin loading)
// ============================================================

/**
 * @brief Simple C++ plugin loader for transport libraries
 *
 * Assumes unified build environment:
 * - GCC 11.x
 * - GLIBC 2.31+
 * - C++17
 *
 * Each transport plugin (.so) exports a function:
 *   extern "C" std::shared_ptr<Transport> Create##Name##Transport();
 */
class TransportLoader {
   public:
    struct PluginInfo {
        std::string name;      // "rdma", "tcp", etc.
        std::string library;   // "libtent_##name##.so"
        std::string symbol;    // "Create##Name##Transport"
        TransportType type;    // RDMA, TCP, etc.
        bool optional = true;  // Don't fail if load fails
    };

    TransportLoader() = default;
    ~TransportLoader();

    // Disable copy
    TransportLoader(const TransportLoader&) = delete;
    TransportLoader& operator=(const TransportLoader&) = delete;

    /**
     * @brief Load a transport plugin from shared library
     */
    Status loadPlugin(const PluginInfo& info);

    /**
     * @brief Load all configured plugins
     */
    Status loadAll(const std::vector<PluginInfo>& plugins);

    /**
     * @brief Create transport by type
     */
    std::shared_ptr<Transport> create(TransportType type);

    /**
     * @brief Get list of successfully loaded transport types
     */
    std::vector<TransportType> loadedTypes() const;

    /**
     * @brief Check if a transport type is loaded
     */
    bool isLoaded(TransportType type) const;

    /**
     * @brief Unload all plugins
     */
    void unloadAll();

   private:
    struct LoadedPlugin {
        void* handle = nullptr;                       // dlopen handle
        std::shared_ptr<Transport> (*create_func)();  // Factory function
        PluginInfo info;
    };

    std::unordered_map<TransportType, LoadedPlugin> loaded_;

    void* loadLibrary(const std::string& path);
    void* findSymbol(void* handle, const std::string& name);
};

}  // namespace tent
}  // namespace mooncake

#endif  // TENT_TRANSPORT_LOADER_H
