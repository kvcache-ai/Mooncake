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

// Conformance suite for one transport plugin. The plugin is loaded into two
// engines of this process (p2p metadata on 127.0.0.1) and checked against
// what the engine relies on:
//   C1 descriptor and capabilities   C6 notifications (if supported)
//   C2 lifecycle                     C7 cancellation (if supported)
//   C3 buffer registration tags      C8 failover to another transport
//   C4 data path, all-or-nothing     C9 engines destroyed with tasks in
//   C5 terminal task states             flight
// The environment names the plugin:
//   TENT_CONFORMANCE_PLUGIN      its name (file libtent_xport_<name>.so)
//   TENT_CONFORMANCE_PLUGIN_DIR  plugins/dir of both engines
//   TENT_CONFORMANCE_CONFIG      optional JSON object of configuration keys
//                                for both engines, for example
//                                {"transports/loopback/async": true}; objects
//                                under "target" and "initiator" apply to one
//                                engine only
//   TENT_CONFORMANCE_FALLBACK    the transport C8 fails over to: tcp, or shm
//                                when the plugin is tcp
// MC_TENT_PLUGIN_DIR, MC_TENT_CONF and MC_FORCE_TCP would override that
// configuration, so the suite clears them. A plugin whose probe() declines
// this host (not_applicable) is skipped, and so are the cases that move host
// memory (C4, C5, C7 to C9) for a transport that does not move DRAM to DRAM.
// The engine's internals are reached only through the test peer below.

#include <dlfcn.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <memory>
#include <optional>
#include <regex>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include "tent/common/config.h"
#include "tent/runtime/transfer_engine_impl.h"
#include "tent/transfer_engine.h"
#include "tent/transport/fault_proxy/fault_proxy_transport.h"
#include "tent/transport_plugin.hpp"
#include "transport_plugin_registry.h"

namespace mooncake {

// Friend of TransferEngine and TransferEngineImpl.
class TransferEngineImplTestPeer {
   public:
    static tent::TransferEngineImpl& impl(tent::TransferEngine& engine) {
        return *engine.impl_;
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
};

namespace tent {
namespace {

namespace fs = std::filesystem;
using Peer = ::mooncake::TransferEngineImplTestPeer;
using Clock = std::chrono::steady_clock;

constexpr auto kDeadline = std::chrono::seconds(10);

struct Settings {
    std::string plugin;
    std::string dir;
    std::string fallback;
    json both = json::object();
    json target = json::object();
    json initiator = json::object();
    std::string error;  // why the environment cannot be used
};

const Settings& settings() {
    static const Settings parsed = [] {
        Settings s;
        auto env = [](const char* name) {
            const char* value = std::getenv(name);
            return std::string(value ? value : "");
        };
        for (const char* name :
             {"MC_TENT_PLUGIN_DIR", "MC_TENT_CONF", "MC_FORCE_TCP"})
            unsetenv(name);
        s.plugin = env("TENT_CONFORMANCE_PLUGIN");
        s.dir = env("TENT_CONFORMANCE_PLUGIN_DIR");
        if (s.plugin.empty() || s.dir.empty()) {
            s.error =
                "set TENT_CONFORMANCE_PLUGIN and TENT_CONFORMANCE_PLUGIN_DIR";
            return s;
        }
        s.fallback = env("TENT_CONFORMANCE_FALLBACK");
        if (s.fallback.empty()) s.fallback = s.plugin == "tcp" ? "shm" : "tcp";
        const std::string text = env("TENT_CONFORMANCE_CONFIG");
        if (text.empty()) return s;
        const json config = json::parse(text, nullptr, false);
        if (!config.is_object()) {
            s.error = "TENT_CONFORMANCE_CONFIG is not a JSON object: " + text;
            return s;
        }
        for (auto it = config.begin(); it != config.end(); ++it) {
            if (it.key() != "target" && it.key() != "initiator") {
                s.both[it.key()] = it.value();
            } else if (!it.value().is_object()) {
                s.error = "TENT_CONFORMANCE_CONFIG: \"" + it.key() +
                          "\" must be an object";
            } else {
                (it.key() == "target" ? s.target : s.initiator) = it.value();
            }
        }
        return s;
    }();
    return parsed;
}

std::vector<std::string> pluginDirs() {
    std::vector<std::string> dirs;
    std::stringstream stream(settings().dir);
    for (std::string dir; std::getline(stream, dir, ':');)
        if (!dir.empty()) dirs.push_back(dir);
    return dirs;
}

std::string pluginFile(const std::string& name) {
    return std::string(TENT_TRANSPORT_PLUGIN_FILE_PREFIX) + name +
           TENT_TRANSPORT_PLUGIN_FILE_SUFFIX;
}

// The plugin under test, plus the fallback when it is a plugin file too.
std::vector<std::string> loadList() {
    std::vector<std::string> names = {settings().plugin};
    for (const auto& dir : pluginDirs()) {
        if (fs::exists(fs::path(dir) / pluginFile(settings().fallback))) {
            names.push_back(settings().fallback);
            break;
        }
    }
    return names;
}

enum class Role { kTarget, kInitiator };

// Sets every leaf of `value` under `path`, so that a nested object adds keys
// instead of replacing a whole subtree.
void setLeaves(Config& conf, const std::string& path, const json& value) {
    if (!value.is_object()) {
        conf.set(path, value);
        return;
    }
    for (const auto& item : value.items())
        setLeaves(conf, path.empty() ? item.key() : path + "/" + item.key(),
                  item.value());
}

std::shared_ptr<Config> makeConfig(Role role, bool with_fallback) {
    const auto& s = settings();
    auto conf = std::make_shared<Config>();
    conf->set("metadata_type", "p2p");
    conf->set("metadata_servers", "P2PHANDSHAKE");
    conf->set("rpc_server_hostname", "127.0.0.1");
    conf->set("merge_requests", false);
    // Only the plugin, and for C8 the fallback, may move data.
    for (int type = UNSPEC + 1; type < kNumBuiltinTransportTypes; ++type) {
        conf->set(std::string("transports/") +
                      transportTypeName(static_cast<TransportType>(type)) +
                      "/enable",
                  false);
    }
    conf->set("transports/ascend_direct/enable", false);
    conf->set("plugins/dir", s.dir);
    conf->set("plugins/load", loadList());
    conf->set("transports/" + s.plugin + "/enable", true);
    if (with_fallback) conf->set("transports/" + s.fallback + "/enable", true);
    setLeaves(*conf, "", s.both);
    setLeaves(*conf, "", role == Role::kTarget ? s.target : s.initiator);
    return conf;
}

// The registry's report on the plugin under test; a copy in a later
// directory is shadowed and reported as a duplicate.
std::optional<PluginReport> pluginReport() {
    std::optional<PluginReport> found;
    for (const auto& report : TransportPluginRegistry::Get().reports()) {
        if (report.name != settings().plugin) continue;
        if (!found || found->outcome == PluginOutcome::kRejectedDuplicateName)
            found = report;
    }
    return found;
}

std::string describe(const std::optional<PluginReport>& report) {
    if (!report)
        return "no " + pluginFile(settings().plugin) + " in " + settings().dir;
    return std::string(pluginOutcomeName(report->outcome)) + " (" +
           report->detail + ")";
}

bool isTerminal(TransferStatusEnum state) {
    return state == COMPLETED || state == FAILED || state == CANCELED ||
           state == TIMEOUT;
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

void fill(std::vector<char>& buffer, char seed) {
    for (size_t i = 0; i < buffer.size(); ++i)
        buffer[i] = static_cast<char>(seed + i % 251);
}

// Polls tasks [first, first + count) of a plugin sub-batch until each is
// terminal or 10 s have passed; tasks still running then are cancelled.
std::vector<TransferStatus> waitTasks(Transport* xport,
                                      Transport::SubBatchRef batch,
                                      size_t first, size_t count) {
    std::vector<TransferStatus> states(count, TransferStatus{PENDING, 0});
    const auto deadline = Clock::now() + kDeadline;
    for (bool done = false; !done && Clock::now() < deadline;) {
        done = true;
        for (size_t i = 0; i < count; ++i) {
            if (!isTerminal(states[i].s))
                xport->getTransferStatus(batch, first + i, states[i]);
            done = done && isTerminal(states[i].s);
        }
        if (!done) std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    bool stuck = false;
    for (size_t i = 0; i < count; ++i) {
        if (isTerminal(states[i].s)) continue;
        stuck = true;
        xport->cancelTransferTask(batch, first + i);
    }
    if (stuck) std::this_thread::sleep_for(std::chrono::milliseconds(100));
    return states;
}

class TransportConformance : public ::testing::Test {
   protected:
    struct Engines {
        std::unique_ptr<TransferEngine> target;
        std::unique_ptr<TransferEngine> initiator;
        SegmentID handle = 0;  // the target, opened by the initiator
    };

    void SetUp() override {
        ASSERT_TRUE(settings().error.empty()) << settings().error;
    }

    // Builds one engine and checks that the plugin is installed in it. Skips
    // the test when the plugin's probe() declines this host.
    void makeEngine(std::unique_ptr<TransferEngine>& engine, Role role,
                    bool with_fallback) {
        engine =
            std::make_unique<TransferEngine>(makeConfig(role, with_fallback));
        const auto report = pluginReport();
        if (report && report->outcome == PluginOutcome::kNotApplicable)
            GTEST_SKIP() << settings().plugin
                         << " does not apply to this host: " << report->detail;
        ASSERT_TRUE(engine->available()) << "engine construction failed";
        // Also rules out a built-in transport of the same name in the slot.
        ASSERT_TRUE(report && report->outcome == PluginOutcome::kLoaded)
            << settings().plugin
            << " is not installed in this engine: " << describe(report);
        slot_ = parseTransportType(settings().plugin);
        ASSERT_NE(slot_, UNSPEC);
        ASSERT_NE(Peer::transports(*engine)[slot_], nullptr);
        if (!with_fallback) return;
        fallback_slot_ = parseTransportType(settings().fallback);
        ASSERT_TRUE(fallback_slot_ != UNSPEC &&
                    Peer::transports(*engine)[fallback_slot_])
            << "fallback transport " << settings().fallback
            << " is not available (TENT_CONFORMANCE_FALLBACK)";
    }

    // Skips a case that moves host memory when the plugin cannot.
    void requireDram(TransferEngine& engine) {
        if (!plugin(engine)->capabilities().dram_to_dram)
            GTEST_SKIP() << "the plugin does not move DRAM to DRAM";
    }

    void makeEngines(Engines& engines, bool with_fallback = false) {
        ASSERT_NO_FATAL_FAILURE(
            makeEngine(engines.target, Role::kTarget, with_fallback));
        if (IsSkipped()) return;
        ASSERT_NO_FATAL_FAILURE(
            makeEngine(engines.initiator, Role::kInitiator, with_fallback));
        ASSERT_TRUE(
            engines.initiator
                ->openSegment(engines.handle, engines.target->getSegmentName())
                .ok());
    }

    Transport* plugin(TransferEngine& engine) {
        return Peer::transports(engine)[slot_].get();
    }

    // The descriptor, read again from the file the registry loaded.
    tent_transport_plugin_t descriptor() {
        tent_transport_plugin_t desc{};
        std::string path;
        for (const auto& report : TransportPluginRegistry::Get().reports())
            if (report.name == settings().plugin &&
                report.outcome == PluginOutcome::kLoaded)
                path = report.path;
        if (path.empty()) {
            ADD_FAILURE() << "no loaded " << pluginFile(settings().plugin);
            return desc;
        }
        void* handle = dlopen(path.c_str(), RTLD_NOW | RTLD_NOLOAD);
        if (!handle) {
            ADD_FAILURE() << "cannot reopen " << path;
            return desc;
        }
        auto entry = reinterpret_cast<tent_transport_plugin_register_fn>(
            dlsym(handle, TENT_TRANSPORT_PLUGIN_ENTRY));
        desc.struct_size = sizeof(desc);
        desc.abi_version = TENT_TRANSPORT_PLUGIN_ABI_VERSION;
        if (!entry || entry(&desc) != TENT_XPORT_REGISTER_OK)
            ADD_FAILURE() << "the entry point of " << path << " failed";
        dlclose(handle);
        return desc;
    }

    Request request(Engines& engines, Request::OpCode opcode, void* local,
                    void* remote, size_t length) {
        Request req;
        req.opcode = opcode;
        req.source = local;
        req.target_id = engines.handle;
        req.target_offset = reinterpret_cast<uint64_t>(remote);
        req.length = length;
        req.transport_hint = slot_;
        return req;
    }

    // Submits one batch and polls every task until it is terminal or the
    // deadline passes; `states` gets the last state of each task. Tasks still
    // running at the deadline are cancelled before the batch is freed.
    void run(TransferEngine& engine, const std::vector<Request>& requests,
             std::vector<TransferStatus>& states) {
        auto batch = engine.allocateBatch(requests.size());
        ASSERT_TRUE(engine.submitTransfer(batch, requests).ok());
        states.assign(requests.size(), TransferStatus{PENDING, 0});
        const auto deadline = Clock::now() + kDeadline;
        for (bool done = false; !done && Clock::now() < deadline;) {
            done = true;
            for (size_t i = 0; i < requests.size(); ++i) {
                if (isTerminal(states[i].s)) continue;
                ASSERT_TRUE(engine.getTransferStatus(batch, i, states[i]).ok());
                done = done && isTerminal(states[i].s);
            }
            if (!done)
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        bool stuck = false;
        for (size_t i = 0; i < requests.size(); ++i) {
            if (isTerminal(states[i].s)) continue;
            stuck = true;
            engine.cancelTransfer(batch, i);
        }
        if (stuck) std::this_thread::sleep_for(std::chrono::milliseconds(100));
        engine.freeBatch(batch);
    }

    void runAll(TransferEngine& engine, const std::vector<Request>& requests) {
        std::vector<TransferStatus> states;
        ASSERT_NO_FATAL_FAILURE(run(engine, requests, states));
        for (size_t i = 0; i < states.size(); ++i)
            ASSERT_EQ(states[i].s, COMPLETED) << "task " << i;
    }

    TransportType slot_ = UNSPEC;
    TransportType fallback_slot_ = UNSPEC;
};

#define MAKE_ENGINES(engines, ...)                                \
    Engines engines;                                              \
    ASSERT_NO_FATAL_FAILURE(makeEngines(engines, ##__VA_ARGS__)); \
    if (IsSkipped()) return

// C1: the descriptor is the one this core expects, and what the installed
// transport reports (capabilities, notification and cancellation support)
// stays within what it declared.
TEST_F(TransportConformance, C1_DescriptorAndCapabilities) {
    MAKE_ENGINES(engines);
    const auto& name = settings().plugin;
    EXPECT_TRUE(std::regex_match(name, std::regex("[a-z0-9_]+"))) << name;
    const auto desc = descriptor();
    ASSERT_NE(desc.name, nullptr);
    EXPECT_EQ(std::string(desc.name), name);
    EXPECT_EQ(desc.abi_version, TENT_TRANSPORT_PLUGIN_ABI_VERSION);
    EXPECT_EQ(desc.abi_fingerprint,
              tent_transport_plugin_host_abi()->abi_fingerprint);
    for (auto* engine : {engines.target.get(), engines.initiator.get()}) {
        Transport* xport = plugin(*engine);
        const uint32_t caps = capsToBits(xport->capabilities());
        EXPECT_EQ(caps & ~desc.declared_caps, 0u)
            << "capabilities 0x" << std::hex << caps << " exceed declared 0x"
            << desc.declared_caps;
        // The flags are upper bounds, like declared_caps.
        if (!(desc.flags & TENT_XPORT_FLAG_NOTIFICATION))
            EXPECT_FALSE(xport->supportNotification());
        if (!(desc.flags & TENT_XPORT_FLAG_CANCELLATION))
            EXPECT_FALSE(xport->supportsCancellation());
    }
}

// C2: engines come and go; a transport that is created but never installed
// can be destroyed; quiesce() may be called twice.
TEST_F(TransportConformance, C2_Lifecycle) {
    for (int round = 0; round < 3; ++round) {
        std::unique_ptr<TransferEngine> engine;
        ASSERT_NO_FATAL_FAILURE(makeEngine(engine, Role::kTarget, false));
        if (IsSkipped()) return;
    }

    const auto desc = descriptor();
    ASSERT_TRUE(desc.create && desc.destroy);
    Config conf;
    tent_transport_plugin_host_t host{};
    host.struct_size = sizeof(host);
    host.abi_version = TENT_TRANSPORT_PLUGIN_ABI_VERSION;
    host.abi_fingerprint = tent_transport_abi_fingerprint();
    host.assigned_type = slot_;
    host.plugin_name = settings().plugin.c_str();
    host.config = &conf;
    void* object = desc.create(&host);
    ASSERT_NE(object, nullptr)
        << (desc.last_error ? desc.last_error() : "create() returned NULL");
    desc.destroy(object);

    std::unique_ptr<TransferEngine> engine;
    ASSERT_NO_FATAL_FAILURE(makeEngine(engine, Role::kTarget, false));
    EXPECT_TRUE(plugin(*engine)->quiesce().ok());
    EXPECT_TRUE(plugin(*engine)->quiesce().ok());
}

// C3: a registered buffer carries the plugin locally and on the wire (a
// plugin slot travels by name), and loses it when unregistered.
TEST_F(TransportConformance, C3_RegistrationTags) {
    std::vector<char> buffer(1 << 20);  // outlives the engines
    MAKE_ENGINES(engines);
    TransferEngine& engine = *engines.initiator;
    const auto addr = reinterpret_cast<uint64_t>(buffer.data());
    auto wireBuffer = [&]() -> std::optional<json> {
        auto desc = json::parse(Peer::localDumpedJson(engine));
        for (const auto& entry : desc["detail"]["buffers"])
            if (entry["addr"] == addr)
                return std::optional<json>(std::in_place, entry);
        return std::nullopt;
    };

    ASSERT_TRUE(engine.registerLocalMemory(buffer.data(), buffer.size()).ok());
    auto local = Peer::segments(engine).getLocal();
    auto* desc = local->findBuffer(addr, buffer.size());
    ASSERT_NE(desc, nullptr);
    EXPECT_NE(
        std::find(desc->transports.begin(), desc->transports.end(), slot_),
        desc->transports.end());
    auto wire = wireBuffer();
    ASSERT_TRUE(wire.has_value());
    const auto& ints = (*wire)["transports"];
    const bool int_on_wire =
        std::find(ints.begin(), ints.end(), int(slot_)) != ints.end();
    if (slot_ < kFirstPluginSlot) {
        EXPECT_TRUE(int_on_wire) << wire->dump();
    } else {
        EXPECT_FALSE(int_on_wire) << wire->dump();
        ASSERT_TRUE(wire->contains("plugin_transports")) << wire->dump();
        const auto& names = (*wire)["plugin_transports"];
        EXPECT_NE(std::find(names.begin(), names.end(), settings().plugin),
                  names.end())
            << wire->dump();
    }

    ASSERT_TRUE(engine.unregisterLocalMemory(buffer.data()).ok());
    local = Peer::segments(engine).getLocal();
    EXPECT_EQ(local->findBuffer(addr, buffer.size()), nullptr);
    EXPECT_FALSE(wireBuffer().has_value());
}

// C4: 8 x 4 MiB written and read back byte for byte. A submission with one
// request outside the target's registered memory is either refused as a
// whole (nothing queued, nothing written) or accepted with only that task
// failing; the same sub-batch then takes valid requests.
TEST_F(TransportConformance, C4_DataPathAllOrNothing) {
    constexpr size_t kChunk = 4 << 20, kCount = 8;
    std::vector<char> src(kChunk * kCount), dst(kChunk * kCount, 0),
        back(kChunk * kCount, 0), unregistered(kChunk);
    MAKE_ENGINES(engines);
    requireDram(*engines.initiator);
    if (IsSkipped()) return;
    fill(src, 'a');
    auto& initiator = *engines.initiator;
    auto& target = *engines.target;
    ASSERT_TRUE(initiator.registerLocalMemory(src.data(), src.size()).ok());
    ASSERT_TRUE(initiator.registerLocalMemory(back.data(), back.size()).ok());
    ASSERT_TRUE(target.registerLocalMemory(dst.data(), dst.size()).ok());

    std::vector<Request> writes, reads;
    for (size_t i = 0; i < kCount; ++i) {
        writes.push_back(request(engines, Request::WRITE,
                                 src.data() + i * kChunk,
                                 dst.data() + i * kChunk, kChunk));
        reads.push_back(request(engines, Request::READ,
                                back.data() + i * kChunk,
                                dst.data() + i * kChunk, kChunk));
    }
    ASSERT_NO_FATAL_FAILURE(runAll(initiator, writes));
    EXPECT_EQ(std::memcmp(src.data(), dst.data(), dst.size()), 0);
    ASSERT_NO_FATAL_FAILURE(runAll(initiator, reads));
    EXPECT_EQ(std::memcmp(src.data(), back.data(), back.size()), 0);

    // The same, one level down: the plugin's own submission API.
    std::fill(dst.begin(), dst.end(), 0);
    Transport* xport = plugin(initiator);
    Transport::SubBatchRef batch = nullptr;
    ASSERT_TRUE(xport->allocateSubBatch(batch, 16).ok());
    std::vector<Request> mixed = {writes[0], writes[1], writes[2], writes[3]};
    mixed[2].target_offset = reinterpret_cast<uint64_t>(unregistered.data());
    const size_t before = batch->size();
    auto untouched = [&](size_t index) {
        const char* chunk = dst.data() + index * kChunk;
        return std::all_of(chunk, chunk + kChunk, [](char c) { return !c; });
    };
    const Status submitted = xport->submitTransferTasks(batch, mixed);
    if (!submitted.ok()) {
        EXPECT_EQ(batch->size(), before)
            << "a refused submission left tasks in the sub-batch";
        for (size_t i = before; i < before + mixed.size(); ++i) {
            TransferStatus state{INITIAL, 0};
            if (xport->getTransferStatus(batch, i, state).ok())
                EXPECT_TRUE(state.s != PENDING && state.s != COMPLETED)
                    << "task " << i << " of a refused submission was queued";
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
        for (size_t i : {0, 1, 3})
            EXPECT_TRUE(untouched(i))
                << "a refused submission wrote request " << i;
    } else {
        const auto states = waitTasks(xport, batch, before, mixed.size());
        for (size_t i = 0; i < mixed.size(); ++i)
            EXPECT_EQ(states[i].s, i == 2 ? FAILED : COMPLETED)
                << "task " << i << " of an accepted submission";
    }
    const size_t first = batch->size();
    const std::vector<Request> valid = {writes[0], writes[1], writes[3]};
    ASSERT_TRUE(xport->submitTransferTasks(batch, valid).ok());
    const auto states = waitTasks(xport, batch, first, valid.size());
    for (size_t i = 0; i < valid.size(); ++i)
        EXPECT_EQ(states[i].s, COMPLETED) << "task " << first + i;
    for (size_t i : {0, 1, 3})
        EXPECT_EQ(std::memcmp(src.data() + i * kChunk, dst.data() + i * kChunk,
                              kChunk),
                  0)
            << "request " << i;
    EXPECT_TRUE(xport->freeSubBatch(batch).ok());
}

// C5: every task reaches COMPLETED or FAILED within 10 s, and a completed
// task has moved its whole length. The engine reports a completed task's
// length itself, so the byte count is checked on the plugin's own status.
TEST_F(TransportConformance, C5_TerminalStatus) {
    constexpr size_t kChunk = 256 << 10, kCount = 16;
    std::vector<char> src(2 * kChunk * kCount), dst(2 * kChunk * kCount, 0);
    MAKE_ENGINES(engines);
    requireDram(*engines.initiator);
    if (IsSkipped()) return;
    fill(src, 'b');
    ASSERT_TRUE(
        engines.initiator->registerLocalMemory(src.data(), src.size()).ok());
    ASSERT_TRUE(
        engines.target->registerLocalMemory(dst.data(), dst.size()).ok());
    std::vector<Request> requests;
    for (size_t i = 0; i < kCount; ++i)  // with gaps, so nothing merges
        requests.push_back(request(engines, Request::WRITE,
                                   src.data() + 2 * i * kChunk,
                                   dst.data() + 2 * i * kChunk, kChunk));
    auto check = [&](const std::vector<TransferStatus>& states,
                     const char* level) {
        for (size_t i = 0; i < kCount; ++i) {
            if (!isTerminal(states[i].s))
                ADD_FAILURE() << level << ": task " << i << " is still "
                              << states[i].s << " after 10 s";
            else if (states[i].s != COMPLETED && states[i].s != FAILED)
                ADD_FAILURE() << level << ": task " << i << " ended in state "
                              << states[i].s << ", not COMPLETED or FAILED";
            if (states[i].s == COMPLETED)
                EXPECT_EQ(states[i].transferred_bytes, kChunk)
                    << level << ": task " << i << " completed with "
                    << states[i].transferred_bytes << " of " << kChunk
                    << " bytes";
        }
    };
    std::vector<TransferStatus> states;
    ASSERT_NO_FATAL_FAILURE(run(*engines.initiator, requests, states));
    check(states, "engine");

    Transport* xport = plugin(*engines.initiator);
    Transport::SubBatchRef batch = nullptr;
    ASSERT_TRUE(xport->allocateSubBatch(batch, kCount).ok());
    ASSERT_TRUE(xport->submitTransferTasks(batch, requests).ok());
    check(waitTasks(xport, batch, 0, kCount), "plugin");
    EXPECT_TRUE(xport->freeSubBatch(batch).ok());
}

// C6: notifications arrive in the order they were sent, both ways, within
// 10 s.
TEST_F(TransportConformance, C6_Notifications) {
    MAKE_ENGINES(engines);
    if (!plugin(*engines.initiator)->supportNotification() ||
        !plugin(*engines.target)->supportNotification())
        GTEST_SKIP() << "the plugin does not support notifications";
    constexpr int kCount = 16;
    auto exchange = [&](TransferEngine& from, SegmentID to,
                        TransferEngine& receiver, const std::string& tag) {
        for (int i = 0; i < kCount; ++i)
            ASSERT_TRUE(from.sendNotification(
                                to, Notification{tag, tag + std::to_string(i)})
                            .ok())
                << tag << i;
        std::vector<Notification> received;
        const auto deadline = Clock::now() + kDeadline;
        while (received.size() < kCount && Clock::now() < deadline) {
            std::vector<Notification> batch;
            ASSERT_TRUE(receiver.receiveNotification(batch).ok());
            received.insert(received.end(), batch.begin(), batch.end());
            if (batch.empty())
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        ASSERT_EQ(received.size(), size_t(kCount)) << tag;
        for (int i = 0; i < kCount; ++i) {
            EXPECT_EQ(received[i].name, tag);
            EXPECT_EQ(received[i].msg, tag + std::to_string(i))
                << "notification " << i << " out of order";
        }
    };
    ASSERT_NO_FATAL_FAILURE(exchange(*engines.initiator, engines.handle,
                                     *engines.target, "forth-"));
    SegmentID back = 0;
    ASSERT_TRUE(
        engines.target->openSegment(back, engines.initiator->getSegmentName())
            .ok());
    ASSERT_NO_FATAL_FAILURE(
        exchange(*engines.target, back, *engines.initiator, "back-"));
}

// C7: 64 x 16 MiB cancelled right after submission all end CANCELED or
// COMPLETED within 10 s.
TEST_F(TransportConformance, C7_Cancellation) {
    constexpr size_t kChunk = 16 << 20, kCount = 64;
    std::vector<char> src(kChunk, 'c'), dst(kChunk, 0);
    MAKE_ENGINES(engines);
    requireDram(*engines.initiator);
    if (IsSkipped()) return;
    if (!plugin(*engines.initiator)->supportsCancellation())
        GTEST_SKIP() << "the plugin does not support cancellation";
    auto& initiator = *engines.initiator;
    ASSERT_TRUE(initiator.registerLocalMemory(src.data(), src.size()).ok());
    ASSERT_TRUE(
        engines.target->registerLocalMemory(dst.data(), dst.size()).ok());
    std::vector<Request> requests(
        kCount,
        request(engines, Request::WRITE, src.data(), dst.data(), kChunk));
    auto batch = initiator.allocateBatch(kCount);
    ASSERT_TRUE(initiator.submitTransfer(batch, requests).ok());
    for (size_t i = 0; i < kCount; ++i) {
        auto status = initiator.cancelTransfer(batch, i);
        EXPECT_TRUE(status.ok()) << "task " << i << ": " << status.ToString();
    }
    std::vector<TransferStatus> states(kCount, TransferStatus{PENDING, 0});
    const auto deadline = Clock::now() + kDeadline;
    for (bool done = false; !done && Clock::now() < deadline;) {
        done = true;
        for (size_t i = 0; i < kCount; ++i) {
            if (!isTerminal(states[i].s))
                ASSERT_TRUE(
                    initiator.getTransferStatus(batch, i, states[i]).ok());
            done = done && isTerminal(states[i].s);
        }
        if (!done) std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    for (size_t i = 0; i < kCount; ++i)
        EXPECT_TRUE(states[i].s == CANCELED || states[i].s == COMPLETED)
            << "task " << i << " did not end after cancellation (state "
            << states[i].s << ")";
    initiator.freeBatch(batch);
}

// C8: when the plugin starts refusing submissions, the engine moves the
// transfer to the fallback transport and it still completes.
TEST_F(TransportConformance, C8_Failover) {
    constexpr size_t kSize = 1 << 20;
    std::vector<char> src(kSize);
    MAKE_ENGINES(engines, true);
    requireDram(*engines.initiator);
    if (IsSkipped()) return;
    auto& initiator = *engines.initiator;
    auto& target = *engines.target;
    void* dst = nullptr;
    ASSERT_TRUE(target.allocateLocalMemory(&dst, kSize).ok());
    ASSERT_TRUE(target.registerLocalMemory(dst, kSize).ok());
    ASSERT_TRUE(initiator.registerLocalMemory(src.data(), kSize).ok());

    auto real = Peer::transports(initiator)[slot_];
    FaultPolicy policy;
    policy.fail_after_n_submits = 1;
    auto proxy = std::make_shared<FaultProxyTransport>(real, policy);
    Peer::impl(initiator).swapTransportForTest(slot_, proxy);
    // The first submission goes through the plugin, the second fails there
    // and has to move to the fallback.
    for (char seed : {'x', 'y'}) {
        fill(src, seed);
        ASSERT_NO_FATAL_FAILURE(
            runAll(initiator,
                   {request(engines, Request::WRITE, src.data(), dst, kSize)}))
            << (seed == 'x'
                    ? "the plugin did not complete the transfer"
                    : "the engine did not fail over to " + settings().fallback);
        EXPECT_EQ(std::memcmp(src.data(), dst, kSize), 0) << "pass " << seed;
    }
    EXPECT_EQ(proxy->submitCount(), 2);
    Peer::impl(initiator).swapTransportForTest(slot_, real);

    EXPECT_TRUE(initiator.unregisterLocalMemory(src.data()).ok());
    EXPECT_TRUE(target.unregisterLocalMemory(dst).ok());
    EXPECT_TRUE(target.freeLocalMemory(dst).ok());
}

// C9: both engines are destroyed with 64 tasks in flight.
TEST_F(TransportConformance, C9_TeardownInFlight) {
    constexpr size_t kChunk = 1 << 20, kCount = 64;
    std::vector<char> src(kChunk * kCount, 'd'), dst(kChunk * kCount, 0);
    {
        MAKE_ENGINES(engines);
        requireDram(*engines.initiator);
        if (IsSkipped()) return;
        ASSERT_TRUE(
            engines.initiator->registerLocalMemory(src.data(), src.size())
                .ok());
        ASSERT_TRUE(
            engines.target->registerLocalMemory(dst.data(), dst.size()).ok());
        std::vector<Request> requests;
        for (size_t i = 0; i < kCount; ++i)
            requests.push_back(request(engines, Request::WRITE,
                                       src.data() + i * kChunk,
                                       dst.data() + i * kChunk, kChunk));
        auto batch = engines.initiator->allocateBatch(kCount);
        ASSERT_TRUE(engines.initiator->submitTransfer(batch, requests).ok());
        engines.initiator.reset();
        engines.target.reset();
    }
    SUCCEED();
}

}  // namespace
}  // namespace tent
}  // namespace mooncake
