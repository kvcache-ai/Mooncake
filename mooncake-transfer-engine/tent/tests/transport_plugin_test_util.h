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

// Helpers shared by the transport plugin tests.

#ifndef TENT_TRANSPORT_PLUGIN_TEST_UTIL_H
#define TENT_TRANSPORT_PLUGIN_TEST_UTIL_H

#include <dlfcn.h>
#include <glog/logging.h>

#include <chrono>
#include <filesystem>
#include <fstream>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include "tent/common/config.h"
#include "tent/runtime/transfer_engine_impl.h"
#include "tent/transfer_engine.h"
#include "transport_plugin_registry.h"

namespace mooncake {

// Friend of TransferEngine and TransferEngineImpl.
class TransferEngineImplTestPeer {
   public:
    static tent::TransferEngineImpl& impl(tent::TransferEngine& engine) {
        return *engine.impl_;
    }
    static std::vector<tent::TransportType> supported(
        tent::TransferEngine& engine) {
        return impl(engine).getSupportedTransports(tent::UNSPEC);
    }
    static tent::TransportList& transports(tent::TransferEngine& engine) {
        return impl(engine).transport_list_;
    }
    static tent::SegmentManager& segments(tent::TransferEngine& engine) {
        return impl(engine).metadata_->segmentManager();
    }
    static std::string localDumpedJson(tent::TransferEngine& engine) {
        return *impl(engine).metadata_->segmentManager().getLocalDumpedJson();
    }
    static std::string localJson(tent::TransferEngine& engine) {
        auto desc = impl(engine).metadata_->segmentManager().getLocal();
        return tent::json(*desc).dump();
    }
};

namespace tent {
namespace plugin_test {

inline std::string pluginDir() { return TENT_TEST_PLUGIN_DIR; }
inline std::string badPluginDir() { return TENT_TEST_BAD_PLUGIN_DIR; }
inline std::string dupPluginDir() { return TENT_TEST_DUP_PLUGIN_DIR; }
inline std::string probePluginDir() { return TENT_TEST_PROBE_PLUGIN_DIR; }

// TENT_XPORT_TCP=plugin builds a core without TCP. Cases whose premise is a
// built-in TCP transport do not apply to it; the TCP plugin itself is covered
// by the TCP tests, which load it from the plugin directory, and by
// BuiltinTransportsAsPlugins.
#if TENT_TEST_TCP_PLUGIN_ONLY
#define TENT_TEST_REQUIRE_BUILTIN_TCP() \
    GTEST_SKIP() << "TCP is a plugin in this build (TENT_XPORT_TCP=plugin)"
#else
#define TENT_TEST_REQUIRE_BUILTIN_TCP() static_cast<void>(0)
#endif

// Engine config for in-process tests: p2p metadata on loopback, TCP on, every
// other built-in transport off so results do not depend on the host or the
// build options.
inline std::shared_ptr<Config> makeConfig(const std::string& plugin_dir) {
    auto config = std::make_shared<Config>();
    config->set("metadata_type", "p2p");
    config->set("metadata_servers", "P2PHANDSHAKE");
    config->set("rpc_server_hostname", "127.0.0.1");
    config->set("transports/tcp/enable", true);
    for (const char* name :
         {"rdma", "shm", "io_uring", "gds", "nvlink", "mnnvl", "ub", "fabric",
          "tpu", "xpu", "mpcomm", "ascend_direct", "sunrise_link", "hylink",
          "hp_tcp"})
        config->set(std::string("transports/") + name + "/enable", false);
    if (!plugin_dir.empty()) {
        config->set("plugins/dir", plugin_dir);
        // The sample plugin is off unless enabled.
        config->set("transports/loopback/enable", true);
    }
    return config;
}

// Captures every glog message logged while it is alive.
class LogCapture : public google::LogSink {
   public:
    LogCapture() { google::AddLogSink(this); }
    ~LogCapture() override { google::RemoveLogSink(this); }

    void send(google::LogSeverity, const char*, const char*, int,
              const struct ::tm*, const char* message,
              size_t message_len) override {
        std::lock_guard<std::mutex> lock(mutex_);
        lines_.emplace_back(message, message_len);
    }

    int count(const std::string& needle) const {
        std::lock_guard<std::mutex> lock(mutex_);
        int n = 0;
        for (const auto& line : lines_)
            if (line.find(needle) != std::string::npos) ++n;
        return n;
    }

    bool contains(const std::string& needle) const { return count(needle) > 0; }

    std::vector<std::string> lines() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return lines_;
    }

   private:
    mutable std::mutex mutex_;
    std::vector<std::string> lines_;
};

// Number of mappings of `path` at file offset 0, i.e. how many copies of that
// shared object are loaded.
inline int loadedCopies(const std::string& path) {
    // /proc/self/maps shows resolved paths.
    std::error_code ec;
    const auto real = std::filesystem::canonical(path, ec);
    const std::string needle = ec ? path : real.string();
    std::ifstream maps("/proc/self/maps");
    std::set<std::string> paths;
    int copies = 0;
    std::string line;
    while (std::getline(maps, line)) {
        if (line.size() < needle.size() ||
            line.compare(line.size() - needle.size(), needle.size(), needle) !=
                0)
            continue;
        std::istringstream fields(line);
        std::string range, perms, offset;
        fields >> range >> perms >> offset;
        if (offset == "00000000") ++copies;
    }
    return copies;
}

inline std::map<std::string, PluginReport> reportsByName() {
    std::map<std::string, PluginReport> out;
    for (auto& report : TransportPluginRegistry::Get().reports())
        out[report.name.empty() ? report.path : report.name] = report;
    return out;
}

inline bool waitDone(TransferEngine& engine, BatchID batch,
                     TransferStatusEnum* final_state = nullptr) {
    TransferStatus status;
    for (int i = 0; i < 10000; ++i) {
        auto result = engine.getTransferStatus(batch, status);
        if (!result.ok()) return false;
        if (status.s == COMPLETED || status.s == FAILED ||
            status.s == CANCELED || status.s == TIMEOUT) {
            if (final_state) *final_state = status.s;
            return status.s == COMPLETED;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    return false;
}

// Writes `length` bytes from src (registered on `initiator`) to dst
// (registered on the engine behind `target_name`) with the given hint.
inline Status writeOnce(TransferEngine& initiator,
                        const std::string& target_name, void* src, void* dst,
                        size_t length, TransportType hint,
                        TransferStatusEnum* final_state = nullptr) {
    SegmentID handle = 0;
    CHECK_STATUS(initiator.openSegment(handle, target_name));
    auto batch = initiator.allocateBatch(1);
    Request request;
    request.opcode = Request::WRITE;
    request.source = src;
    request.target_id = handle;
    request.target_offset = reinterpret_cast<uint64_t>(dst);
    request.length = length;
    request.transport_hint = hint;
    auto status = initiator.submitTransfer(batch, {request});
    if (status.ok() && !waitDone(initiator, batch, final_state))
        status = Status::InternalError("transfer did not complete");
    initiator.freeBatch(batch);
    return status;
}

// Bytes copied by the loopback plugin of this process, or -1 if it is not
// loaded.
inline long long loopbackBytesCopied() {
    void* handle = dlopen((pluginDir() + "/libtent_xport_loopback.so").c_str(),
                          RTLD_NOW | RTLD_NOLOAD);
    if (!handle) return -1;
    using Fn = uint64_t (*)();
    auto fn =
        reinterpret_cast<Fn>(dlsym(handle, "tent_xport_loopback_bytes_copied"));
    long long value = fn ? static_cast<long long>(fn()) : -1;
    dlclose(handle);
    return value;
}

}  // namespace plugin_test
}  // namespace tent
}  // namespace mooncake

#endif  // TENT_TRANSPORT_PLUGIN_TEST_UTIL_H
