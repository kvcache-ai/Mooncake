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

#include "transport_plugin_loader.h"

#include <dlfcn.h>
#include <glog/logging.h>

#include <algorithm>
#include <cstdlib>
#include <cstring>
#include <exception>
#include <filesystem>
#include <string>
#include <vector>

#include "tent/common/config.h"
#include "tent/runtime/transport.h"
#include "tent/transport_plugin.h"

namespace mooncake {
namespace tent {
namespace {

namespace fs = std::filesystem;

// Plugins take TENT's symbols from the process. A TENT that was itself loaded
// with RTLD_LOCAL (inside a Python extension, say) does not provide them until
// the object containing it is moved to the global scope, for the rest of the
// process. When TENT is part of the executable this does nothing, and the
// executable must export its symbols instead.
void exposeTentToPlugins() {
    [[maybe_unused]] static const bool done = [] {
        Dl_info self{};
        if (dladdr(reinterpret_cast<void*>(&exposeTentToPlugins), &self) &&
            self.dli_fname)
            dlopen(self.dli_fname, RTLD_NOW | RTLD_NOLOAD | RTLD_GLOBAL);
        return true;
    }();
}

std::vector<fs::path> listPlugins(const std::string& dir) {
    std::vector<fs::path> files;
    std::error_code ec;
    for (fs::directory_iterator it(dir, ec), end; !ec && it != end;
         it.increment(ec)) {
        const auto name = it->path().filename().string();
        std::error_code type_ec;
        if (name.starts_with("libtent_xport_") && name.ends_with(".so") &&
            it->is_regular_file(type_ec))
            files.push_back(it->path());
    }
    if (ec)
        LOG(WARNING) << "Transport plugin directory " << dir << ": "
                     << ec.message();
    std::sort(files.begin(), files.end());
    return files;
}

using TransportArray =
    std::array<std::shared_ptr<Transport>, kSupportedTransportTypes>;

// Fills one slot of `transports` from the plugin at `path`, or logs why not.
void loadPlugin(const std::string& path, const TransportPluginContext& context,
                TransportArray& transports) {
    // Never dlclose(): the transport, and the code that destroys it, live in
    // the plugin.
    void* handle = dlopen(path.c_str(), RTLD_NOW | RTLD_LOCAL);
    if (!handle) {
        LOG(WARNING) << "Transport plugin " << path
                     << " skipped: " << dlerror();
        return;
    }
    // A plugin uses TENT's C++ classes directly, so it only runs in the build
    // it was compiled in: compare its build ID through a C function before
    // calling any of its C++ code. A mismatch is a deployment error: stop.
    auto build_id = reinterpret_cast<decltype(&tent_transport_plugin_build_id)>(
        dlsym(handle, TENT_TRANSPORT_PLUGIN_BUILD_ID_ENTRY));
    const char* id = build_id ? build_id() : nullptr;
    if (!id) {
        LOG(FATAL) << "Transport plugin " << path << " has no build ID from "
                   << TENT_TRANSPORT_PLUGIN_BUILD_ID_ENTRY
                   << "(); rebuild the plugin against this TENT";
    }
    if (std::strcmp(id, TENT_BUILD_ID) != 0) {
        LOG(FATAL) << "Transport plugin " << path << " was built for " << id
                   << ", but this TENT is " << TENT_BUILD_ID
                   << "; rebuild the plugin against this TENT";
    }
    auto entry = reinterpret_cast<decltype(&tent_register_transport_plugin)>(
        dlsym(handle, TENT_TRANSPORT_PLUGIN_ENTRY));
    const TransportPluginDesc* desc = entry ? entry() : nullptr;
    if (!desc) {
        LOG(WARNING) << "Transport plugin " << path
                     << " skipped: no descriptor from "
                     << TENT_TRANSPORT_PLUGIN_ENTRY "()";
        return;
    }
    if (!desc->create) {
        LOG(WARNING) << "Transport plugin " << path
                     << " skipped: descriptor has no create()";
        return;
    }
    const int type = desc->type;
    if (type <= UNSPEC || type >= kSupportedTransportTypes) {
        LOG(WARNING) << "Transport plugin " << path
                     << " skipped: transport type " << type
                     << " does not exist in this TENT";
        return;
    }
    const char* name = transportTypeName(desc->type);
    if (transports[type]) {
        LOG(INFO) << "Transport plugin " << path << " skipped: " << name
                  << " is already provided";
        return;
    }
    auto transport = desc->create(context);
    if (!transport) {
        LOG(INFO) << "Transport plugin " << path << " declined this engine";
        return;
    }
    transports[type] = std::move(transport);
    LOG(INFO) << "Transport plugin " << path << " loaded as " << name;
}

}  // namespace

void loadTransportPlugins(const std::shared_ptr<Config>& conf,
                          const std::shared_ptr<Topology>& topology,
                          TransportArray& transports) {
    const char* env = std::getenv("MC_TENT_PLUGIN_DIR");
    const std::string dir =
        env && *env ? env : conf->get("plugins/dir", std::string());
    if (dir.empty()) return;
    const auto files = listPlugins(dir);
    if (!files.empty()) exposeTentToPlugins();

    const TransportPluginContext context{conf, topology};
    for (const auto& file : files) {
        // The plugin's entry point and create() run plugin code.
        try {
            loadPlugin(file.string(), context, transports);
        } catch (const std::exception& e) {
            LOG(WARNING) << "Transport plugin " << file.string()
                         << " skipped: exception: " << e.what();
        } catch (...) {
            LOG(WARNING) << "Transport plugin " << file.string()
                         << " skipped: exception";
        }
    }
}

}  // namespace tent
}  // namespace mooncake
