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

#include "transport_plugin_registry.h"

#include <dlfcn.h>
#include <glog/logging.h>

#include <algorithm>
#include <cinttypes>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <sstream>
#include <unordered_set>

#include "tent/common/transport_slots.h"
#include "tent/runtime/topology.h"
#include "tent/runtime/transport.h"
#include "tent/transport_plugin.hpp"

namespace {
// dladdr() on an internal-linkage object always names the object file this
// code was linked into, whatever another TENT copy may have interposed.
const char g_registry_anchor = 0;
}  // namespace

extern "C" const tent_transport_plugin_host_t* tent_transport_plugin_host_abi(
    void) {
    static const tent_transport_plugin_host_t host = {
        sizeof(tent_transport_plugin_host_t),
        TENT_TRANSPORT_PLUGIN_ABI_VERSION,
        mooncake::tent::tent_transport_abi_fingerprint(),
        0,
        nullptr,
        nullptr,
        nullptr};
    return &host;
}

namespace mooncake {
namespace tent {

namespace fs = std::filesystem;

namespace {

constexpr char kPrefix[] = TENT_TRANSPORT_PLUGIN_FILE_PREFIX;
constexpr char kSuffix[] = TENT_TRANSPORT_PLUGIN_FILE_SUFFIX;
constexpr size_t kMaxNameLength = 64;

bool isValidPluginName(const std::string& name) {
    if (name.empty() || name.size() > kMaxNameLength) return false;
    return std::all_of(name.begin(), name.end(), [](char c) {
        return (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '_';
    });
}

constexpr char kBadFileName[] =
    "file name is not libtent_xport_<name>.so with <name> in [a-z0-9_]";

// libtent_xport_<anything>.so; other files in a plugin directory are not
// looked at.
bool isPluginFileName(const std::string& file) {
    return file.size() > sizeof(kPrefix) - 1 + sizeof(kSuffix) - 1 &&
           file.compare(0, sizeof(kPrefix) - 1, kPrefix) == 0 &&
           file.compare(file.size() - (sizeof(kSuffix) - 1),
                        sizeof(kSuffix) - 1, kSuffix) == 0;
}

std::string pluginNameOf(const std::string& file) {
    return file.substr(
        sizeof(kPrefix) - 1,
        file.size() - (sizeof(kPrefix) - 1) - (sizeof(kSuffix) - 1));
}

bool isBuiltinTransportName(const std::string& name) {
    if (name == "unknown") return true;
    for (int t = 0; t < kNumBuiltinTransportTypes; ++t) {
        if (name == transportTypeName(static_cast<TransportType>(t)))
            return true;
    }
    return false;
}

bool isRejection(PluginOutcome outcome) {
    return outcome != PluginOutcome::kLoaded &&
           outcome != PluginOutcome::kDisabledByConfig &&
           outcome != PluginOutcome::kNotApplicable;
}

uint32_t capsToBits(const Capabilities& caps) {
    uint32_t bits = 0;
    if (caps.dram_to_dram) bits |= TENT_XPORT_CAP_DRAM_TO_DRAM;
    if (caps.dram_to_gpu) bits |= TENT_XPORT_CAP_DRAM_TO_GPU;
    if (caps.gpu_to_dram) bits |= TENT_XPORT_CAP_GPU_TO_DRAM;
    if (caps.gpu_to_gpu) bits |= TENT_XPORT_CAP_GPU_TO_GPU;
    if (caps.dram_to_file) bits |= TENT_XPORT_CAP_DRAM_TO_FILE;
    if (caps.gpu_to_file) bits |= TENT_XPORT_CAP_GPU_TO_FILE;
    return bits;
}

std::string capsToString(uint32_t bits) {
    static const std::pair<uint32_t, const char*> kNames[] = {
        {TENT_XPORT_CAP_DRAM_TO_DRAM, "dram_to_dram"},
        {TENT_XPORT_CAP_DRAM_TO_GPU, "dram_to_gpu"},
        {TENT_XPORT_CAP_GPU_TO_DRAM, "gpu_to_dram"},
        {TENT_XPORT_CAP_GPU_TO_GPU, "gpu_to_gpu"},
        {TENT_XPORT_CAP_DRAM_TO_FILE, "dram_to_file"},
        {TENT_XPORT_CAP_GPU_TO_FILE, "gpu_to_file"},
    };
    std::string out;
    for (const auto& [bit, name] : kNames) {
        if (!(bits & bit)) continue;
        if (!out.empty()) out += ",";
        out += name;
    }
    return out;
}

std::string hex(uint64_t value) {
    std::ostringstream os;
    os << "0x" << std::hex << std::setw(16) << std::setfill('0') << value;
    return os.str();
}

std::string lastErrorOf(const tent_transport_plugin_t& desc) {
    if (!desc.last_error) return "";
    const char* error = desc.last_error();
    return error ? error : "";
}

tent_transport_plugin_host_t makeHost(TransportType type,
                                      const std::string& name,
                                      const Topology* topology,
                                      const Config* conf) {
    tent_transport_plugin_host_t host{};
    host.struct_size = sizeof(host);
    host.abi_version = TENT_TRANSPORT_PLUGIN_ABI_VERSION;
    host.abi_fingerprint = tent_transport_abi_fingerprint();
    host.assigned_type = static_cast<int>(type);
    host.plugin_name = name.c_str();
    host.topology = topology;
    host.config = conf;
    return host;
}

// The directory of the file the TENT core was loaded from (the executable,
// libtent_shared.so or a Python extension). /proc/self/maps names it with an
// absolute path, whatever path and working directory it was loaded with
// (Linux only, as is TENT). Empty if it cannot be found.
std::string coreDirectory() {
    // Each line: <start>-<end> <perms> <offset> <dev> <inode> <path>
    constexpr char kRange[] = "%" SCNxPTR "-%" SCNxPTR;
    const auto anchor = reinterpret_cast<uintptr_t>(&g_registry_anchor);
    std::ifstream maps("/proc/self/maps");
    std::string line;
    while (std::getline(maps, line)) {
        uintptr_t start = 0, end = 0;
        if (std::sscanf(line.c_str(), kRange, &start, &end) != 2) continue;
        if (anchor < start || anchor >= end) continue;
        const size_t path = line.find('/');
        if (path == std::string::npos) return "";
        return fs::path(line.substr(path)).parent_path().string();
    }
    return "";
}

}  // namespace

const char* pluginOutcomeName(PluginOutcome outcome) {
    switch (outcome) {
        case PluginOutcome::kLoaded:
            return "loaded";
        case PluginOutcome::kDisabledByConfig:
            return "disabled_by_config";
        case PluginOutcome::kNotApplicable:
            return "not_applicable";
        case PluginOutcome::kRejectedNameMismatch:
            return "rejected_name_mismatch";
        case PluginOutcome::kRejectedBadEntry:
            return "rejected_bad_entry";
        case PluginOutcome::kRejectedAbiVersion:
            return "rejected_abi_version";
        case PluginOutcome::kRejectedAbiFingerprint:
            return "rejected_abi_fingerprint";
        case PluginOutcome::kRejectedDuplicateName:
            return "rejected_duplicate_name";
        case PluginOutcome::kRejectedConflictWithBuiltin:
            return "rejected_conflict_with_builtin";
        case PluginOutcome::kRejectedNoFreeSlot:
            return "rejected_no_free_slot";
        case PluginOutcome::kRejectedCreateFailed:
            return "rejected_create_failed";
        case PluginOutcome::kRejectedCapsOverclaim:
            return "rejected_caps_overclaim";
        case PluginOutcome::kInstallFailed:
            return "install_failed";
        case PluginOutcome::kHostSymbolsNotExported:
            return "host_symbols_not_exported";
    }
    return "unknown";
}

namespace {

// A list of plugin names: a JSON array of strings, or one string. Returns
// false if the key holds anything else; `present` tells whether it is set.
bool readNameList(const Config& conf, const std::string& key,
                  std::vector<std::string>& names, bool& present) {
    names.clear();
    const json value = conf.get(key, json());
    present = !value.is_null();
    if (!present) return true;
    if (value.is_string()) {
        names.push_back(value.get<std::string>());
        return true;
    }
    if (!value.is_array()) return false;
    for (const auto& item : value) {
        if (!item.is_string()) {
            names.clear();
            return false;
        }
        names.push_back(item.get<std::string>());
    }
    return true;
}

bool contains(const std::vector<std::string>& names, const std::string& name) {
    return std::find(names.begin(), names.end(), name) != names.end();
}

}  // namespace

TransportPluginRegistry& TransportPluginRegistry::Get() {
    // Leaked on purpose: plugin transports may be destroyed during static
    // destruction and their deleters call back into the registry.
    static auto* registry = new TransportPluginRegistry();
    return *registry;
}

size_t TransportPluginRegistry::addReportLocked(const std::string& path,
                                                const std::string& name,
                                                PluginOutcome outcome,
                                                const std::string& detail) {
    reports_.push_back(PluginReport{path, name, outcome, detail});
    const std::string& who = name.empty() ? path : name;
    if (isRejection(outcome)) {
        LOG(WARNING) << "[TransportPlugin] " << pluginOutcomeName(outcome)
                     << " " << who << ": " << detail;
    } else {
        LOG(INFO) << "[TransportPlugin] " << pluginOutcomeName(outcome) << " "
                  << who << ": " << detail;
    }
    return reports_.size() - 1;
}

void TransportPluginRegistry::setOutcomeLocked(size_t report,
                                               PluginOutcome outcome,
                                               const std::string& detail) {
    auto& entry = reports_[report];
    entry.outcome = outcome;
    entry.detail = detail;
    if (isRejection(outcome)) {
        LOG(WARNING) << "[TransportPlugin] " << pluginOutcomeName(outcome)
                     << " " << entry.name << ": " << detail;
    } else {
        LOG(INFO) << "[TransportPlugin] " << pluginOutcomeName(outcome) << " "
                  << entry.name << ": " << detail;
    }
}

bool TransportPluginRegistry::checkHostLocked(std::string& detail) {
    Dl_info self{};
    if (!dladdr(&g_registry_anchor, &self) || !self.dli_fname) {
        detail = "dladdr() cannot locate the TENT core";
        return false;
    }
    // Plugins resolve TENT symbols in the global scope (the main program,
    // its startup dependencies, then RTLD_GLOBAL objects), so look there,
    // not in this object's own scope as RTLD_DEFAULT would (a core linked
    // with -Bsymbolic would find itself).
    auto lookup = []() -> void* {
        void* global = dlopen(nullptr, RTLD_NOW);
        dlerror();
        return global ? dlsym(global, TENT_TRANSPORT_PLUGIN_HOST_ABI_SYMBOL)
                      : nullptr;
    };
    void* symbol = lookup();
    if (!symbol && self.dli_fname[0] != '\0') {
        // A host loaded with RTLD_LOCAL (e.g. a Python extension) is not in
        // the global scope: promote it. This is process-wide and permanent
        // and includes its dependencies; a TENT copy loaded later binds to
        // this one. It fails harmlessly for an executable, which must have
        // been linked with -rdynamic instead.
        dlopen(self.dli_fname, RTLD_NOW | RTLD_NOLOAD | RTLD_GLOBAL);
        symbol = lookup();
    }
    if (!symbol) {
        detail = std::string("TENT symbols are not visible to plugins (") +
                 TENT_TRANSPORT_PLUGIN_HOST_ABI_SYMBOL +
                 " not found; an executable that links TENT statically "
                 "must be linked with -rdynamic); TENT core: " +
                 self.dli_fname;
        return false;
    }
    // Another TENT copy is already global: refuse, and stay out of the
    // global scope.
    Dl_info found{};
    if (!dladdr(symbol, &found) || found.dli_fbase != self.dli_fbase) {
        detail = std::string("another TENT copy (") +
                 (found.dli_fname ? found.dli_fname : "?") +
                 ") provides the symbols plugins bind to; this TENT core "
                 "is " +
                 self.dli_fname;
        return false;
    }
    return true;
}

Status TransportPluginRegistry::ensureLoaded(const Config& conf,
                                             const Topology* topology) {
    const std::string dirs = conf.get("plugins/dir", std::string());
    if (dirs.empty()) {
        if (conf.contains("plugins/dir") &&
            conf.get("plugins/dir", std::string("?")) == "?") {
            LOG(WARNING) << "[TransportPlugin] plugins/dir must be a string "
                            "of ':'-separated directories; ignoring it";
        }
        return Status::OK();
    }
    std::vector<std::string> load_list;
    bool has_load_list = false;
    if (!readNameList(conf, "plugins/load", load_list, has_load_list)) {
        LOG(WARNING) << "[TransportPlugin] plugins/load must be a list of "
                        "plugin names; loading none";
    }
    std::string key = dirs;
    if (has_load_list) {
        key += " load=";
        for (const auto& name : load_list) key += name + ",";
    }

    std::lock_guard<std::mutex> lock(mutex_);
    if (loaded_) {
        if (key != loaded_key_) {
            LOG(WARNING) << "[TransportPlugin] plugin settings '" << key
                         << "' differ from those already loaded in this "
                            "process ('"
                         << loaded_key_ << "'); keeping the first";
        }
        return Status::OK();
    }
    loaded_ = true;
    loaded_key_ = key;

    // First pass: decide which files are candidates without opening any.
    // Reports are emitted afterwards, in file order.
    struct Item {
        std::string path;
        std::string name;
        bool candidate = false;
        PluginOutcome outcome = PluginOutcome::kLoaded;
        std::string detail;
    };
    std::vector<Item> items;
    std::unordered_map<std::string, std::string> seen;  // name -> first path
    std::stringstream stream(dirs);
    std::string dir;
    std::string searched;  // the directories, with $ORIGIN expanded
    while (std::getline(stream, dir, ':')) {
        if (dir.empty()) continue;
        // A leading "$ORIGIN" (only this spelling) is the directory of the
        // file the core was loaded from, with symlinks resolved.
        std::string from;
        if (dir == "$ORIGIN" || dir.rfind("$ORIGIN/", 0) == 0) {
            const std::string origin = coreDirectory();
            if (origin.empty()) {
                LOG(WARNING) << "[TransportPlugin] cannot resolve $ORIGIN in "
                                "plugin directory "
                             << dir
                             << ": /proc/self/maps shows no file for the "
                                "TENT core";
                continue;
            }
            from = " (from " + dir + ")";
            dir = origin + dir.substr(sizeof("$ORIGIN") - 1);
        }
        searched += (searched.empty() ? "" : ":") + dir;
        std::error_code ec;
        if (!fs::is_directory(dir, ec)) {
            std::string why = " is not a directory";
            if (ec == std::errc::no_such_file_or_directory)
                why = " does not exist";
            else if (ec)
                why = " is not accessible: " + ec.message();
            LOG(WARNING) << "[TransportPlugin] plugin directory " << dir << from
                         << why;
            continue;
        }
        std::vector<fs::path> files;
        fs::directory_iterator it(dir, ec), end;
        for (; !ec && it != end; it.increment(ec)) {
            if (!isPluginFileName(it->path().filename().string())) continue;
            std::error_code file_ec;
            if (!it->is_regular_file(file_ec)) continue;
            files.push_back(it->path());
        }
        if (ec) {
            LOG(WARNING) << "[TransportPlugin] cannot list plugin directory "
                         << dir << from << ": " << ec.message();
        }
        std::sort(files.begin(), files.end());
        for (const auto& file : files) {
            Item item;
            item.path = file.string();
            item.name = pluginNameOf(file.filename().string());
            if (!isValidPluginName(item.name)) {
                item.outcome = PluginOutcome::kRejectedNameMismatch;
                item.detail = kBadFileName;
            } else if (has_load_list && !contains(load_list, item.name)) {
                item.outcome = PluginOutcome::kDisabledByConfig;
                item.detail = "not listed in plugins/load";
            } else if (auto [first, inserted] =
                           seen.emplace(item.name, item.path);
                       !inserted) {
                item.outcome = PluginOutcome::kRejectedDuplicateName;
                item.detail = "shadowed by " + first->second +
                              " (the first plugins/dir entry with this name "
                              "wins)";
            } else {
                item.candidate = true;
            }
            items.push_back(std::move(item));
        }
    }
    for (const auto& name : load_list) {
        if (!seen.count(name)) {
            LOG(WARNING) << "[TransportPlugin] plugins/load lists '" << name
                         << "' but no " << kPrefix << name << kSuffix
                         << " exists in " << searched;
        }
    }
    const bool any_candidate =
        std::any_of(items.begin(), items.end(),
                    [](const Item& item) { return item.candidate; });
    // Only with something to load, touch the process symbol scope.
    std::string detail;
    const bool host_ok = any_candidate && checkHostLocked(detail);
    for (const auto& item : items) {
        if (!item.candidate)
            addReportLocked(item.path, item.name, item.outcome, item.detail);
        else if (host_ok)
            loadOneLocked(item.path, item.name, topology, &conf);
    }
    if (any_candidate && !host_ok) {
        addReportLocked(searched, "", PluginOutcome::kHostSymbolsNotExported,
                        detail);
    }
    return Status::OK();
}

namespace {

// What examinePlugin() found out about one file.
struct Examination {
    PluginOutcome outcome = PluginOutcome::kLoaded;
    std::string detail;
    bool described = false;  // the entry point filled in the descriptor
    int probe_rc = 0;
    std::string probe_error;
};

// Opens one plugin file and runs, in the loader's order, every check that
// needs no engine: the entry point, ABI version and descriptor size,
// fingerprint, names, builtin_type and probe(). kLoaded means all of them
// passed. Changes no registry state. Handles are kept even for rejected
// plugins: their static constructors have already run and unloading is never
// safe in general.
Examination examinePlugin(const std::string& path, const std::string& name,
                          const Topology* topology, const Config* conf,
                          tent_transport_plugin_t& desc) {
    Examination result;
    auto reject = [&result](PluginOutcome outcome, std::string detail) {
        result.outcome = outcome;
        result.detail = std::move(detail);
        return result;
    };
    dlerror();
    void* handle = dlopen(path.c_str(), RTLD_NOW | RTLD_LOCAL);
    if (!handle) {
        const char* error = dlerror();
        return reject(
            PluginOutcome::kRejectedBadEntry,
            std::string("dlopen failed: ") + (error ? error : "unknown error"));
    }
    dlerror();
    auto entry = reinterpret_cast<tent_transport_plugin_register_fn>(
        dlsym(handle, TENT_TRANSPORT_PLUGIN_ENTRY));
    if (!entry) {
        const char* error = dlerror();
        return reject(PluginOutcome::kRejectedBadEntry,
                      std::string("no ") + TENT_TRANSPORT_PLUGIN_ENTRY +
                          " symbol: " + (error ? error : "null"));
    }

    desc = {};
    desc.struct_size = sizeof(desc);
    desc.abi_version = TENT_TRANSPORT_PLUGIN_ABI_VERSION;
    int rc = -1;
    try {
        rc = entry(&desc);
    } catch (...) {
        return reject(PluginOutcome::kRejectedBadEntry,
                      "entry point threw an exception");
    }
    if (rc == TENT_XPORT_REGISTER_E_STRUCT_SIZE) {
        return reject(PluginOutcome::kRejectedAbiVersion,
                      "plugin descriptor is larger than the core's (" +
                          std::to_string(sizeof(desc)) +
                          " bytes): built against a newer ABI than v" +
                          std::to_string(TENT_TRANSPORT_PLUGIN_ABI_VERSION));
    }
    if (rc != TENT_XPORT_REGISTER_OK) {
        return reject(PluginOutcome::kRejectedBadEntry,
                      "entry point returned " + std::to_string(rc));
    }
    result.described = true;
    if (desc.abi_version != TENT_TRANSPORT_PLUGIN_ABI_VERSION ||
        desc.struct_size != sizeof(desc)) {
        return reject(
            PluginOutcome::kRejectedAbiVersion,
            "plugin ABI v" + std::to_string(desc.abi_version) + " (" +
                std::to_string(desc.struct_size) + "-byte descriptor), core v" +
                std::to_string(TENT_TRANSPORT_PLUGIN_ABI_VERSION) + " (" +
                std::to_string(sizeof(desc)) + "-byte descriptor)");
    }
    const uint64_t core_fingerprint = tent_transport_abi_fingerprint();
    if (desc.abi_fingerprint != core_fingerprint) {
        return reject(PluginOutcome::kRejectedAbiFingerprint,
                      "plugin " + hex(desc.abi_fingerprint) + ", core " +
                          hex(core_fingerprint) +
                          " (built with different TENT headers, compiler or "
                          "C++ library)");
    }
    if (!desc.name || name != desc.name) {
        return reject(PluginOutcome::kRejectedNameMismatch,
                      std::string("descriptor name '") +
                          (desc.name ? desc.name : "(null)") +
                          "' does not match the file name");
    }
    if (!desc.create || !desc.destroy) {
        return reject(PluginOutcome::kRejectedBadEntry,
                      "descriptor has no create() or destroy()");
    }
    if (desc.builtin_type != UNSPEC) {
        if (desc.builtin_type < 0 ||
            desc.builtin_type >= kNumBuiltinTransportTypes) {
            return reject(PluginOutcome::kRejectedBadEntry,
                          "builtin_type " + std::to_string(desc.builtin_type) +
                              " is not a built-in transport");
        }
        const auto builtin = static_cast<TransportType>(desc.builtin_type);
        if (name != transportTypeName(builtin)) {
            return reject(PluginOutcome::kRejectedNameMismatch,
                          std::string("claims built-in transport '") +
                              transportTypeName(builtin) +
                              "' under another name");
        }
    } else if (isBuiltinTransportName(name)) {
        return reject(PluginOutcome::kRejectedDuplicateName,
                      "'" + name +
                          "' is a built-in transport name; a plugin "
                          "providing it must set builtin_type");
    }

    if (desc.probe) {
        auto host = makeHost(UNSPEC, name, topology, conf);
        int probe_rc = 0;
        std::string reason;
        try {
            probe_rc = desc.probe(&host);
            if (probe_rc != 0) reason = lastErrorOf(desc);
        } catch (const std::exception& e) {
            probe_rc = -1;
            reason = std::string("probe() threw: ") + e.what();
        } catch (...) {
            probe_rc = -1;
            reason = "probe() threw an exception";
        }
        result.probe_rc = probe_rc;
        result.probe_error = reason;
        if (probe_rc != 0) {
            return reject(PluginOutcome::kNotApplicable,
                          "probe() returned " + std::to_string(probe_rc) +
                              (reason.empty() ? "" : ": " + reason));
        }
    }

    return result;
}

}  // namespace

void TransportPluginRegistry::loadOneLocked(const std::string& path,
                                            const std::string& name,
                                            const Topology* topology,
                                            const Config* conf) {
    ++dlopen_calls_;
    Plugin plugin;
    plugin.path = path;
    plugin.name = name;
    const auto examination =
        examinePlugin(path, name, topology, conf, plugin.desc);
    if (examination.outcome != PluginOutcome::kLoaded) {
        addReportLocked(path, name, examination.outcome, examination.detail);
        return;
    }
    const auto& desc = plugin.desc;
    if (desc.builtin_type != UNSPEC)
        plugin.type = static_cast<TransportType>(desc.builtin_type);
    if (plugin.type == UNSPEC) {
        TransportType slot = UNSPEC;
        switch (assignPluginTransportSlot(name, desc.flags, slot)) {
            case SlotAssignResult::kNoFreeSlot:
                addReportLocked(path, name, PluginOutcome::kRejectedNoFreeSlot,
                                "all " + std::to_string(kNumPluginSlots) +
                                    " plugin slots are in use");
                return;
            case SlotAssignResult::kDuplicateName:
                addReportLocked(
                    path, name, PluginOutcome::kRejectedDuplicateName,
                    "a plugin slot is already registered as '" + name + "'");
                return;
            case SlotAssignResult::kAssigned:
                plugin.type = slot;
                break;
        }
    }

    plugin.report =
        addReportLocked(path, name, PluginOutcome::kLoaded,
                        "slot " + std::to_string(plugin.type) + ", version " +
                            (desc.version ? desc.version : "?"));
    plugins_.push_back(std::move(plugin));
}

PluginInspection TransportPluginRegistry::inspect(const std::string& path) {
    PluginInspection result;
    result.path = path;
    result.core_abi_version = TENT_TRANSPORT_PLUGIN_ABI_VERSION;
    result.core_fingerprint = tent_transport_abi_fingerprint();
    const std::string file = fs::path(path).filename().string();
    if (isPluginFileName(file)) result.name = pluginNameOf(file);
    if (!isValidPluginName(result.name)) {
        result.outcome = PluginOutcome::kRejectedNameMismatch;
        result.detail = kBadFileName;
        return result;
    }
    tent_transport_plugin_t desc{};
    const auto examination =
        examinePlugin(path, result.name, nullptr, nullptr, desc);
    result.outcome = examination.outcome;
    result.detail = examination.detail;
    result.probe_rc = examination.probe_rc;
    result.probe_error = examination.probe_error;
    if (examination.described) {
        result.version = desc.version ? desc.version : "";
        result.builtin_type = desc.builtin_type;
        result.flags = desc.flags;
        result.declared_caps = desc.declared_caps;
        result.abi_version = desc.abi_version;
        result.abi_fingerprint = desc.abi_fingerprint;
    }
    return result;
}

std::shared_ptr<Transport> TransportPluginRegistry::wrap(size_t plugin,
                                                         Transport* transport) {
    auto destroy = plugins_[plugin].desc.destroy;
    std::shared_ptr<Transport> owner(
        transport, [this, destroy](Transport* object) {
            {
                std::lock_guard<std::mutex> lock(live_mutex_);
                live_.erase(object);
            }
            destroy(object);
        });
    std::lock_guard<std::mutex> lock(live_mutex_);
    live_[transport] = plugin;
    return owner;
}

bool TransportPluginRegistry::installedIn(const TransportList& transports,
                                          size_t plugin) const {
    const auto* object = transports[plugins_[plugin].type].get();
    if (!object) return false;
    std::lock_guard<std::mutex> lock(live_mutex_);
    auto it = live_.find(object);
    return it != live_.end() && it->second == plugin;
}

Status TransportPluginRegistry::instantiate(
    const Config& conf, const std::shared_ptr<Topology>& topology,
    TransportList& transports) {
    // An engine that does not configure plugins gets no plugin transport,
    // even if another engine in the process loaded some.
    if (conf.get("plugins/dir", std::string()).empty()) return Status::OK();
    std::vector<std::string> load_list;
    bool has_load_list = false;
    if (!readNameList(conf, "plugins/load", load_list, has_load_list))
        has_load_list = true;  // malformed: load none (warned at load time)

    std::lock_guard<std::mutex> lock(mutex_);
    auto& entries = pending_[&transports];
    entries.clear();
    for (size_t i = 0; i < plugins_.size(); ++i) {
        auto& plugin = plugins_[i];
        EngineEntry entry{i, PluginOutcome::kLoaded, "", nullptr, ""};
        const bool default_on =
            plugin.desc.flags & TENT_XPORT_FLAG_ENABLED_BY_DEFAULT;
        const std::string enable_key = "transports/" + plugin.name + "/enable";
        if (has_load_list && !contains(load_list, plugin.name)) {
            entry.outcome = PluginOutcome::kDisabledByConfig;
            entry.detail = "not listed in plugins/load";
        } else if (!conf.get(enable_key, default_on)) {
            entry.outcome = PluginOutcome::kDisabledByConfig;
            entry.detail = conf.contains(enable_key)
                               ? enable_key + " is false"
                               : enable_key +
                                     " is not set and the plugin "
                                     "is off by default";
        } else if (transports[plugin.type]) {
            entry.outcome = PluginOutcome::kRejectedConflictWithBuiltin;
            entry.detail = "the core already provides transport '" +
                           plugin.name + "' (slot " +
                           std::to_string(plugin.type) + ")";
        } else {
            auto host =
                makeHost(plugin.type, plugin.name, topology.get(), &conf);
            void* object = nullptr;
            std::string reason;
            try {
                object = plugin.desc.create(&host);
                if (!object) reason = lastErrorOf(plugin.desc);
            } catch (const std::exception& e) {
                reason = std::string("create() threw: ") + e.what();
            } catch (...) {
                reason = "create() threw an exception";
            }
            if (!object) {
                entry.outcome = PluginOutcome::kRejectedCreateFailed;
                entry.detail = "create() returned NULL" +
                               (reason.empty() ? "" : ": " + reason);
            } else {
                auto* transport = static_cast<Transport*>(object);
                const char* transport_name = transport->getName();
                transports[plugin.type] = wrap(i, transport);
                entry.object = transport;
                entry.transport_name = transport_name ? transport_name : "?";
            }
        }
        if (entry.outcome != PluginOutcome::kLoaded)
            setOutcomeLocked(plugin.report, entry.outcome, entry.detail);
        entries.push_back(std::move(entry));
    }
    return Status::OK();
}

Status TransportPluginRegistry::verifyInstalled(const Config& conf,
                                                TransportList& transports,
                                                std::string* summary) {
    if (summary) summary->clear();
    const bool configured = !conf.get("plugins/dir", std::string()).empty();
    std::vector<std::string> required;
    bool has_required = false;
    const bool required_ok =
        readNameList(conf, "plugins/required", required, has_required);
    // No plugins configured or required: nothing to do, and no locking.
    if (!configured && !has_required) return Status::OK();

    std::vector<std::shared_ptr<Transport>> dropped;
    Status result = Status::OK();
    {
        std::lock_guard<std::mutex> lock(mutex_);
        std::vector<EngineEntry> entries;
        if (configured) {
            auto it = pending_.find(&transports);
            if (it != pending_.end()) {
                entries = std::move(it->second);
                pending_.erase(it);
            }
        }
        for (auto& entry : entries) {
            if (entry.outcome != PluginOutcome::kLoaded) continue;
            auto& plugin = plugins_[entry.plugin];
            auto& slot = transports[plugin.type];
            if (slot.get() != entry.object) {
                entry.outcome = PluginOutcome::kInstallFailed;
                entry.detail = "install() failed (logged as 'Transport " +
                               entry.transport_name + " skipped')";
            } else {
                std::string excess;
                const uint32_t extra = capsToBits(slot->capabilities()) &
                                       ~plugin.desc.declared_caps;
                if (extra)
                    excess = "capabilities() reports " + capsToString(extra) +
                             " beyond declared_caps " +
                             capsToString(plugin.desc.declared_caps);
                else if (slot->supportNotification() &&
                         !(plugin.desc.flags & TENT_XPORT_FLAG_NOTIFICATION))
                    excess =
                        "supportNotification() is true without "
                        "TENT_XPORT_FLAG_NOTIFICATION";
                else if (slot->supportsCancellation() &&
                         !(plugin.desc.flags & TENT_XPORT_FLAG_CANCELLATION))
                    excess =
                        "supportsCancellation() is true without "
                        "TENT_XPORT_FLAG_CANCELLATION";
                if (!excess.empty()) {
                    entry.outcome = PluginOutcome::kRejectedCapsOverclaim;
                    entry.detail = excess;
                    dropped.push_back(std::move(slot));
                } else {
                    entry.detail =
                        "slot " + std::to_string(plugin.type) + ", version " +
                        (plugin.desc.version ? plugin.desc.version : "?") +
                        ", installed";
                }
            }
            setOutcomeLocked(plugin.report, entry.outcome, entry.detail);
        }

        // This engine's view: its own outcome where it has one, else the
        // load-time report.
        auto engineOutcome = [&](size_t report) -> const EngineEntry* {
            for (const auto& entry : entries)
                if (plugins_[entry.plugin].report == report) return &entry;
            return nullptr;
        };
        if (summary && configured) {
            for (size_t r = 0; r < reports_.size(); ++r) {
                const auto* entry = engineOutcome(r);
                std::string item = pluginOutcomeName(
                    entry ? entry->outcome : reports_[r].outcome);
                if (entry && entry->outcome == PluginOutcome::kLoaded) {
                    const auto& plugin = plugins_[entry->plugin];
                    item += ",slot " + std::to_string(plugin.type);
                    // A built-in name alone does not tell where it came from.
                    if (plugin.type < kNumBuiltinTransportTypes)
                        item += "," + plugin.path;
                }
                if (!reports_[r].name.empty())
                    item = reports_[r].name + "(" + item + ")";
                if (!summary->empty()) *summary += " ";
                *summary += item;
            }
        }

        if (!required_ok) {
            LOG(ERROR) << "[TransportPlugin] plugins/required must be a list "
                          "of plugin names";
            result = Status::PluginError(
                "plugins/required must be a list of plugin names" LOC_MARK);
        }
        for (const auto& name : required) {
            bool installed = false;
            for (size_t i = 0; i < plugins_.size() && !installed; ++i)
                installed =
                    plugins_[i].name == name && installedIn(transports, i);
            if (installed) continue;
            std::string reason;
            if (!configured) {
                reason = "plugins/dir is not set";
            } else {
                reason = "no " + std::string(kPrefix) + name + kSuffix +
                         " in plugins/dir";
                auto host = std::find_if(
                    reports_.begin(), reports_.end(), [](const auto& report) {
                        return report.outcome ==
                               PluginOutcome::kHostSymbolsNotExported;
                    });
                if (host != reports_.end()) {
                    reason = std::string(pluginOutcomeName(host->outcome)) +
                             ": " + host->detail;
                } else {
                    for (size_t r = 0; r < reports_.size(); ++r) {
                        const auto& report = reports_[r];
                        if (report.name != name) continue;
                        const auto* entry = engineOutcome(r);
                        reason = std::string(pluginOutcomeName(
                                     entry ? entry->outcome : report.outcome)) +
                                 ": " + (entry ? entry->detail : report.detail);
                        break;  // the first file with this name is the one used
                    }
                }
            }
            LOG(ERROR) << "[TransportPlugin] required transport plugin '"
                       << name << "' not loaded: " << reason;
            if (result.ok()) {
                result =
                    Status::PluginError("required transport plugin '" + name +
                                        "' not loaded: " + reason + LOC_MARK);
            }
        }
    }
    // Outside the lock; the deleters only take live_mutex_.
    for (auto& transport : dropped) transport->uninstall();
    dropped.clear();
    return result;
}

std::vector<PluginReport> TransportPluginRegistry::reports() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return reports_;
}

size_t TransportPluginRegistry::dlopenCount() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return dlopen_calls_;
}

void TransportPluginRegistry::fillSlotsForTest(int count) {
    for (int i = 0; i < count; ++i) {
        TransportType slot = UNSPEC;
        assignPluginTransportSlot("test_slot_" + std::to_string(i), 0, slot);
    }
}

void TransportPluginRegistry::resetForTest() {
    std::lock_guard<std::mutex> lock(mutex_);
    {
        std::lock_guard<std::mutex> live_lock(live_mutex_);
        if (!live_.empty()) {
            LOG(ERROR) << "[TransportPlugin] resetForTest() with "
                       << live_.size() << " live plugin transports; ignored";
            return;
        }
    }
    loaded_ = false;
    loaded_key_.clear();
    plugins_.clear();
    reports_.clear();
    pending_.clear();
    resetPluginTransportSlotsForTest();
}

Status instantiateTransportPlugins(const Config& conf,
                                   const std::shared_ptr<Topology>& topology,
                                   TransportList& transports) {
    return TransportPluginRegistry::Get().instantiate(conf, topology,
                                                      transports);
}

}  // namespace tent
}  // namespace mooncake
