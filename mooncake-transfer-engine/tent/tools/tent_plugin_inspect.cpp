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

// tent_plugin_inspect: what TENT would make of transport plugin files.
//
//   tent_plugin_inspect [--json] [--install-check] [--verbose] <file.so>...
//   tent_plugin_inspect [--json] [--install-check] [--verbose] --dir <dir>...
//
// Every file goes through the loader's checks that need no engine
// (TransportPluginRegistry::inspect()): dlopen(), the entry point, the ABI
// version and fingerprint, the names, builtin_type and probe(). With
// --install-check, the files that pass them are then loaded into one engine in
// this process, which runs create(), install() and the capability checks;
// that starts an RPC server and opens the devices the transports use, so it
// is not done by default; MC_TENT_PLUGIN_DIR, MC_TENT_CONF and MC_FORCE_TCP do
// not apply to it. In a directory, files not named libtent_xport_*.so are
// listed as ignored. The exit code is 0 when there are plugin files and every
// one is ok, 1 otherwise and 2 on a usage error.

#include <fcntl.h>
#include <unistd.h>

#include <algorithm>
#include <cstdio>
#include <filesystem>
#include <iostream>
#include <map>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include "tent/common/config.h"
#include "tent/transfer_engine.h"
#include "tent/transport_plugin.h"
#include "transport_plugin_registry.h"

namespace fs = std::filesystem;
using namespace mooncake::tent;

namespace {

struct Row {
    std::string file;
    std::string dir;  // the --dir it was listed from, if any
    bool ignored = false;
    bool unchecked = false;  // --install-check could not load it
    PluginInspection inspection;
};

std::string hex(uint64_t value) {
    char buf[19];
    std::snprintf(buf, sizeof(buf), "0x%016llx",
                  static_cast<unsigned long long>(value));
    return buf;
}

std::string bitNames(
    uint32_t bits, const std::vector<std::pair<uint32_t, const char*>>& names) {
    std::string out;
    for (const auto& [bit, name] : names) {
        if (!(bits & bit)) continue;
        if (!out.empty()) out += "|";
        out += name;
    }
    return out.empty() ? "-" : out;
}

std::string flagNames(uint32_t flags) {
    return bitNames(flags,
                    {{TENT_XPORT_FLAG_ENABLED_BY_DEFAULT, "default_on"},
                     {TENT_XPORT_FLAG_SAME_MACHINE_ONLY, "same_machine"},
                     {TENT_XPORT_FLAG_LOCAL_SEGMENT_ONLY, "local_segment"},
                     {TENT_XPORT_FLAG_HOST_STAGING_CAPABLE, "staging"},
                     {TENT_XPORT_FLAG_NOTIFICATION, "notification"},
                     {TENT_XPORT_FLAG_CANCELLATION, "cancellation"}});
}

std::string capNames(uint32_t caps) {
    return bitNames(caps, {{TENT_XPORT_CAP_DRAM_TO_DRAM, "dram_to_dram"},
                           {TENT_XPORT_CAP_DRAM_TO_GPU, "dram_to_gpu"},
                           {TENT_XPORT_CAP_GPU_TO_DRAM, "gpu_to_dram"},
                           {TENT_XPORT_CAP_GPU_TO_GPU, "gpu_to_gpu"},
                           {TENT_XPORT_CAP_DRAM_TO_FILE, "dram_to_file"},
                           {TENT_XPORT_CAP_GPU_TO_FILE, "gpu_to_file"}});
}

std::string builtinName(int type) {
    if (type == UNSPEC) return "-";
    if (type > 0 && type < kNumBuiltinTransportTypes)
        return transportTypeName(static_cast<TransportType>(type));
    return std::to_string(type);
}

// Sends what the plugins and the engine print (glog writes to stderr) to
// /dev/null while they run, so that only the report reaches the output.
class Quiet {
   public:
    explicit Quiet(bool quiet) {
        if (!quiet) return;
        std::fflush(stdout);
        std::fflush(stderr);
        const int null = open("/dev/null", O_WRONLY);
        for (int fd : {STDOUT_FILENO, STDERR_FILENO}) {
            saved_[fd] = dup(fd);
            if (saved_[fd] >= 0 && null >= 0) dup2(null, fd);
        }
        if (null >= 0) close(null);
    }
    ~Quiet() {
        std::fflush(stdout);
        std::fflush(stderr);
        for (int fd : {STDOUT_FILENO, STDERR_FILENO}) {
            if (saved_[fd] < 0) continue;
            dup2(saved_[fd], fd);
            close(saved_[fd]);
        }
    }

   private:
    int saved_[3] = {-1, -1, -1};
};

bool ok(const Row& row) {
    return !row.ignored && !row.unchecked &&
           row.inspection.outcome == PluginOutcome::kLoaded;
}

std::string outcomeOf(const Row& row) {
    if (row.ignored) return "ignored";
    if (row.unchecked) return "not_checked";
    return ok(row) ? "ok" : pluginOutcomeName(row.inspection.outcome);
}

// One spelling per directory, as the loader's paths are compared.
std::string canonicalPath(const std::string& path) {
    std::error_code ec;
    const fs::path absolute = fs::absolute(path.empty() ? "." : path, ec);
    fs::path result = fs::weakly_canonical(absolute, ec);
    if (ec) result = absolute.lexically_normal();
    // Neither drops a trailing separator after a missing last component.
    if (!result.has_filename()) result = result.parent_path();
    return result.string();
}

// A file as the loader names it: the directory canonical, the file name as
// listed (a symlinked plugin file is not resolved).
std::string fileKey(const std::string& path) {
    const fs::path file(path);
    return (fs::path(canonicalPath(file.parent_path().string())) /
            file.filename())
        .string();
}

bool isPluginFileName(const std::string& file) {
    const std::string prefix = TENT_TRANSPORT_PLUGIN_FILE_PREFIX;
    const std::string suffix = TENT_TRANSPORT_PLUGIN_FILE_SUFFIX;
    return file.size() > prefix.size() + suffix.size() &&
           file.compare(0, prefix.size(), prefix) == 0 &&
           file.compare(file.size() - suffix.size(), suffix.size(), suffix) ==
               0;
}

// Loads the files that passed inspect(), or that only probe() declined, into
// one engine and takes over the registry's verdict on each. The engine
// searches directories as the loader does: the --dir directories in the given
// order, then those of the files given by name, so another file of the same
// name in an earlier directory is loaded instead and the named one is
// reported as a duplicate. A file the engine did not report on is marked
// unchecked. Returns false if the engine could not be built.
bool engineCheck(std::vector<Row>& rows, std::string& error) {
    std::vector<std::string> dirs, names;
    std::map<std::string, std::vector<Row*>> by_path;
    auto addDir = [&dirs](const std::string& dir) {
        if (std::find(dirs.begin(), dirs.end(), dir) == dirs.end())
            dirs.push_back(dir);
    };
    for (auto& row : rows)
        if (!row.dir.empty()) addDir(canonicalPath(row.dir));
    for (auto& row : rows) {
        if (row.ignored ||
            (row.inspection.outcome != PluginOutcome::kLoaded &&
             row.inspection.outcome != PluginOutcome::kNotApplicable))
            continue;
        const fs::path path = fileKey(row.inspection.path);
        addDir(path.parent_path().string());
        if (std::find(names.begin(), names.end(), row.inspection.name) ==
            names.end())
            names.push_back(row.inspection.name);
        by_path[path.string()].push_back(&row);
    }
    if (by_path.empty()) return true;

    // These would replace the directories and transports set below.
    for (const char* name :
         {"MC_TENT_PLUGIN_DIR", "MC_TENT_CONF", "MC_FORCE_TCP"})
        unsetenv(name);
    auto conf = std::make_shared<Config>();
    conf->set("metadata_type", "p2p");
    conf->set("metadata_servers", "P2PHANDSHAKE");
    conf->set("rpc_server_hostname", "127.0.0.1");
    std::string dir_list;
    for (const auto& dir : dirs)
        dir_list += (dir_list.empty() ? "" : ":") + dir;
    conf->set("plugins/dir", dir_list);
    conf->set("plugins/load", names);
    for (const auto& name : names)
        conf->set("transports/" + name + "/enable", true);
    for (auto& [path, matches] : by_path)
        for (Row* row : matches) row->unchecked = true;
    TransferEngine engine(conf);
    if (!engine.available()) {
        error = "the engine for the install check could not be built";
        for (auto& [path, matches] : by_path)
            for (Row* row : matches) row->inspection.detail = error;
        return false;
    }
    for (const auto& report : TransportPluginRegistry::Get().reports()) {
        auto it = by_path.find(fileKey(report.path));
        if (it == by_path.end()) continue;
        for (Row* row : it->second) {
            row->unchecked = false;
            row->inspection.outcome = report.outcome;
            row->inspection.detail = report.detail;
        }
    }
    for (auto& [path, matches] : by_path)
        for (Row* row : matches)
            if (row->unchecked)
                row->inspection.detail = "the engine did not load it";
    return true;
}

void printTable(const std::vector<Row>& rows) {
    std::vector<std::vector<std::string>> cells = {
        {"FILE", "NAME", "VERSION", "BUILTIN", "FLAGS", "CAPS", "ABI",
         "FINGERPRINT", "RESULT"}};
    for (const auto& row : rows) {
        const auto& in = row.inspection;
        if (row.ignored) {
            cells.push_back(
                {row.file, "-", "-", "-", "-", "-", "-", "-", "ignored"});
            continue;
        }
        std::string result = outcomeOf(row);
        if (!ok(row) && !in.detail.empty()) result += ": " + in.detail;
        cells.push_back(
            {row.file, in.name.empty() ? "-" : in.name,
             in.version.empty() ? "-" : in.version,
             builtinName(in.builtin_type), flagNames(in.flags),
             capNames(in.declared_caps),
             std::to_string(in.abi_version) + "/" +
                 std::to_string(in.core_abi_version),
             hex(in.abi_fingerprint) + "/" + hex(in.core_fingerprint), result});
    }
    std::vector<size_t> width(cells[0].size(), 0);
    for (const auto& line : cells)
        for (size_t i = 0; i + 1 < line.size(); ++i)
            width[i] = std::max(width[i], line[i].size());
    for (const auto& line : cells) {
        std::string out;
        for (size_t i = 0; i < line.size(); ++i) {
            out += line[i];
            if (i + 1 < line.size())
                out += std::string(width[i] - line[i].size() + 2, ' ');
        }
        std::cout << out << "\n";
    }
}

void printJson(const std::vector<Row>& rows) {
    json out = json::array();
    for (const auto& row : rows) {
        const auto& in = row.inspection;
        json entry = {{"file", row.file}, {"outcome", outcomeOf(row)}};
        if (!row.ignored) {
            entry["path"] = in.path;
            entry["name"] = in.name;
            entry["version"] = in.version;
            entry["builtin_type"] = in.builtin_type;
            entry["flags"] = in.flags;
            entry["declared_caps"] = in.declared_caps;
            entry["abi_version"] = in.abi_version;
            entry["abi_fingerprint"] = hex(in.abi_fingerprint);
            entry["core_abi_version"] = in.core_abi_version;
            entry["core_fingerprint"] = hex(in.core_fingerprint);
            entry["detail"] = in.detail;
            entry["probe_rc"] = in.probe_rc;
            entry["probe_error"] = in.probe_error;
        }
        out.push_back(entry);
    }
    std::cout << out.dump(2, ' ', false, json::error_handler_t::replace)
              << "\n";
}

int usage() {
    std::cerr << "usage: tent_plugin_inspect [--json] [--install-check] "
                 "[--verbose] <file.so>...\n"
                 "       tent_plugin_inspect [--json] [--install-check] "
                 "[--verbose] --dir <dir>...\n";
    return 2;
}

}  // namespace

int main(int argc, char** argv) {
    bool as_json = false, install_check = false, verbose = false, input = false;
    std::vector<Row> rows;
    for (int i = 1; i < argc; ++i) {
        const std::string arg = argv[i];
        if (arg == "--json") {
            as_json = true;
        } else if (arg == "--install-check") {
            install_check = true;
        } else if (arg == "--verbose") {
            verbose = true;
        } else if (arg == "--dir") {
            if (++i == argc) return usage();
            input = true;
            std::error_code ec;
            std::vector<fs::path> files;
            for (fs::directory_iterator it(argv[i], ec), end; !ec && it != end;
                 it.increment(ec)) {
                std::error_code file_ec;
                if (it->is_regular_file(file_ec)) files.push_back(it->path());
            }
            if (ec) {
                std::cerr << "cannot list " << argv[i] << ": " << ec.message()
                          << "\n";
                return 2;
            }
            std::sort(files.begin(), files.end());
            for (const auto& file : files) {
                Row row;
                row.file = file.filename().string();
                row.dir = argv[i];
                row.inspection.path = file.string();
                row.ignored = !isPluginFileName(row.file);
                rows.push_back(row);
            }
        } else if (!arg.empty() && arg[0] == '-') {
            return usage();
        } else {
            Row row;
            row.file = fs::path(arg).filename().string();
            // A bare name is the file in this directory, as the loader opens
            // <dir>/<file>, not one dlopen() finds on the library path.
            row.inspection.path =
                arg.find('/') == std::string::npos ? "./" + arg : arg;
            rows.push_back(row);
            input = true;
        }
    }
    if (!input) return usage();

    std::string engine_error;
    bool engine_ok = true;
    {
        Quiet quiet(!verbose);
        for (auto& row : rows)
            if (!row.ignored)
                row.inspection =
                    TransportPluginRegistry::inspect(row.inspection.path);
        if (install_check) engine_ok = engineCheck(rows, engine_error);
    }

    if (as_json)
        printJson(rows);
    else
        printTable(rows);
    if (!engine_ok) std::cerr << engine_error << "\n";
    const bool any = std::any_of(rows.begin(), rows.end(),
                                 [](const Row& r) { return !r.ignored; });
    if (!any) std::cerr << "no plugin files\n";
    const bool all_ok = std::all_of(rows.begin(), rows.end(), [](const Row& r) {
        return r.ignored || ok(r);
    });
    return any && all_ok && engine_ok ? 0 : 1;
}
