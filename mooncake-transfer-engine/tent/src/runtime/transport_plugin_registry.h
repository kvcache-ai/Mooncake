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

#ifndef TENT_TRANSPORT_PLUGIN_REGISTRY_H
#define TENT_TRANSPORT_PLUGIN_REGISTRY_H

#include <array>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "tent/common/config.h"
#include "tent/common/status.h"
#include "tent/common/transport_slots.h"
#include "tent/common/types.h"
#include "tent/transport_plugin.h"

namespace mooncake {
namespace tent {

class Transport;
class Topology;

using TransportList =
    std::array<std::shared_ptr<Transport>, kSupportedTransportTypes>;

// What happened to one plugin file; the detail string says which check
// decided it.
enum class PluginOutcome {
    kLoaded,
    kDisabledByConfig,
    kNotApplicable,
    kRejectedNameMismatch,
    kRejectedBadEntry,
    kRejectedAbiVersion,
    kRejectedAbiFingerprint,
    kRejectedDuplicateName,
    kRejectedConflictWithBuiltin,
    kRejectedNoFreeSlot,
    kRejectedCreateFailed,
    kRejectedCapsOverclaim,
    kInstallFailed,
    kHostSymbolsNotExported,
};

const char* pluginOutcomeName(PluginOutcome outcome);

struct PluginReport {
    std::string path;
    std::string name;
    PluginOutcome outcome;
    std::string detail;
};

// What TransportPluginRegistry::inspect() found out about one plugin file.
// The descriptor fields stay zero when the entry point was not reached.
struct PluginInspection {
    std::string path;
    std::string name;  // from the file name
    std::string version;
    int builtin_type = UNSPEC;
    uint32_t flags = 0;
    uint32_t declared_caps = 0;
    uint32_t abi_version = 0;
    uint64_t abi_fingerprint = 0;
    uint32_t core_abi_version = 0;
    uint64_t core_fingerprint = 0;
    // kLoaded: every check that needs no engine passed.
    PluginOutcome outcome = PluginOutcome::kLoaded;
    std::string detail;
    int probe_rc = 0;
    std::string probe_error;
};

// Process-wide registry of transport plugins, driven by
// TransferEngineImpl::construct() in three steps:
//   ensureLoaded()    before the TransportSelector parses policies, so that
//                     plugin names resolve. Once per process: lists the
//                     plugins/dir directories (a leading $ORIGIN stands for
//                     the directory of the file the core was loaded from),
//                     dlopen()s the candidates, checks their ABI and assigns
//                     plugin slots;
//   instantiate()     at the end of loadTransports(): creates this engine's
//                     plugin transports;
//   verifyInstalled() after the install loop: drops plugins whose
//                     capabilities exceed their declaration, enforces
//                     plugins/required and summarises this engine's plugins.
// Engines share the loaded plugins and each creates its own Transport
// objects. An engine without plugins/dir creates none. Plugins are never
// dlclose()d.
class TransportPluginRegistry {
   public:
    static TransportPluginRegistry& Get();

    Status ensureLoaded(const Config& conf, const Topology* topology);

    Status instantiate(const Config& conf,
                       const std::shared_ptr<Topology>& topology,
                       TransportList& transports);

    // `summary` receives one line for the verbose banner, e.g.
    // "loopback(loaded,slot 19)", or "rdma(loaded,slot 1,<file>)" for a
    // plugin in a built-in slot; empty when the engine uses no plugins.
    Status verifyInstalled(const Config& conf, TransportList& transports,
                           std::string* summary = nullptr);

    // One report per plugin file seen in this process. Load-time outcomes
    // are process-wide; per-engine outcomes (disabled, conflicts, create,
    // install and capability checks) show the most recent engine.
    std::vector<PluginReport> reports() const;

    // Number of plugin dlopen() calls made by this registry.
    size_t dlopenCount() const;

    // Runs the loader's checks that need no engine on one file (entry point,
    // ABI version, fingerprint, names, builtin_type, probe()) without loading
    // it into the registry: no slot is assigned, no report is added and no
    // process-wide state changes. The file is dlopen()ed RTLD_LOCAL and stays
    // mapped; a dlopen() failure (an undefined symbol, for example) is
    // kRejectedBadEntry with the dlerror() text.
    static PluginInspection inspect(const std::string& path);

    // Test hooks. fillSlotsForTest() occupies `count` free plugin slots with
    // placeholder names; resetForTest() forgets all loading state (plugins
    // stay mapped) and requires that no plugin transport is alive.
    void fillSlotsForTest(int count);
    void resetForTest();

   private:
    TransportPluginRegistry() = default;

    struct Plugin {
        std::string path;
        std::string name;
        tent_transport_plugin_t desc{};
        TransportType type = UNSPEC;  // assigned plugin slot or claimed slot
        size_t report = 0;            // index into reports_
    };

    // What instantiate() did with one plugin for one engine.
    struct EngineEntry {
        size_t plugin;
        PluginOutcome outcome;
        std::string detail;
        const Transport* object = nullptr;  // created, not yet verified
        std::string transport_name;         // getName(), as the engine logs it
    };

    bool checkHostLocked(std::string& detail);
    void loadOneLocked(const std::string& path, const std::string& name,
                       const Topology* topology, const Config* conf);
    size_t addReportLocked(const std::string& path, const std::string& name,
                           PluginOutcome outcome, const std::string& detail);
    void setOutcomeLocked(size_t report, PluginOutcome outcome,
                          const std::string& detail);
    std::shared_ptr<Transport> wrap(size_t plugin, Transport* transport);
    bool installedIn(const TransportList& transports, size_t plugin) const;

    mutable std::mutex mutex_;
    bool loaded_ = false;
    std::string loaded_key_;
    std::vector<Plugin> plugins_;
    std::vector<PluginReport> reports_;
    // Per engine transport list, between instantiate() and verifyInstalled().
    std::unordered_map<const TransportList*, std::vector<EngineEntry>> pending_;
    size_t dlopen_calls_ = 0;

    // Separate from mutex_ so that destroying a plugin transport never needs
    // the main lock.
    mutable std::mutex live_mutex_;
    std::unordered_map<const Transport*, size_t> live_;  // -> index in plugins_
};

// Called at the end of TransferEngineImpl::loadTransports().
Status instantiateTransportPlugins(const Config& conf,
                                   const std::shared_ptr<Topology>& topology,
                                   TransportList& transports);

}  // namespace tent
}  // namespace mooncake

#endif  // TENT_TRANSPORT_PLUGIN_REGISTRY_H
