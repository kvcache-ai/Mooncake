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

// Engine integration of transport plugins: routing by name, policy and hint,
// locality flags, required plugins, notifications and cancellation, using
// the loopback plugin.

#include <gtest/gtest.h>

#include <unistd.h>

#include <algorithm>
#include <cstring>
#include <vector>

#include <asio.hpp>
#include <optional>
#include <unordered_map>
#include <ylt/coro_http/coro_http_client.hpp>
#include <ylt/coro_http/coro_http_server.hpp>

#include "tent/common/transport_slots.h"
#include "tent/runtime/transport_selector.h"
#include "transport_plugin_test_util.h"

namespace mooncake {
namespace tent {
namespace {

using plugin_test::LogCapture;
using plugin_test::loopbackBytesCopied;
using plugin_test::makeConfig;
using plugin_test::pluginDir;
using plugin_test::writeOnce;
using Peer = ::mooncake::TransferEngineImplTestPeer;

constexpr size_t kLength = 1 << 20;

// getSupportedTransports(UNSPEC) as it was before plugin slots existed.
std::vector<TransportType> builtinOrder(const TransportList& list) {
    std::vector<TransportType> result;
    for (auto type : {MNNVL, NVLINK, XPU, RDMA, FABRIC, SUNRISE_LINK,
                      AscendDirect, SHM, TCP, HP_TCP, GDS, MPCOMM, TPU, HYLINK})
        if (list[type]) result.push_back(type);
    return result;
}

json loopbackOnlyPolicy() {
    return json::parse(
        R"([{"name":"lb","segment_type":"memory","transports":["loopback"]}])");
}

// Pretends `engine` runs on another machine, as seen by its peers.
void moveToOtherMachine(TransferEngine& engine) {
    auto& manager = Peer::segments(engine);
    ASSERT_TRUE(manager
                    .updateLocal([](SegmentDesc& desc) -> Status {
                        desc.machine_id = "another-machine";
                        return Status::OK();
                    })
                    .ok());
    ASSERT_TRUE(manager.synchronizeLocal().ok());
}

// Minimal HTTP metadata store (GET/PUT/DELETE /metadata?key=...), enough for
// metadata_type "http".
class HttpMetadataStore {
   public:
    bool start() {
        for (int attempt = 0; attempt < 5; ++attempt) {
            asio::io_context io_context;
            asio::ip::tcp::acceptor acceptor(
                io_context,
                asio::ip::tcp::endpoint(asio::ip::address_v4::loopback(), 0));
            port_ = acceptor.local_endpoint().port();
            acceptor.close();
            server_ = std::make_unique<coro_http::coro_http_server>(1, port_);
            installHandlers();
            server_->async_start();
            for (int i = 0; i < 50; ++i) {
                if (reachable()) return true;
                std::this_thread::sleep_for(std::chrono::milliseconds(20));
            }
            server_->stop();
        }
        return false;
    }

    ~HttpMetadataStore() {
        if (server_) server_->stop();
    }

    std::string endpoint() const {
        return "http://127.0.0.1:" + std::to_string(port_) + "/metadata";
    }

    std::optional<std::string> get(const std::string& suffix) const {
        std::lock_guard<std::mutex> lock(mutex_);
        for (const auto& [key, value] : store_)
            if (key.size() >= suffix.size() &&
                key.compare(key.size() - suffix.size(), suffix.size(),
                            suffix) == 0)
                return value;
        return std::nullopt;
    }

   private:
    bool reachable() {
        coro_http::coro_http_client client;
        auto result =
            client.get("http://127.0.0.1:" + std::to_string(port_) + "/health");
        return result.status == 200;
    }

    void installHandlers() {
        using namespace coro_http;
        server_->set_http_handler<GET>(
            "/health", [](coro_http_request&, coro_http_response& resp) {
                resp.set_status_and_content(status_type::ok, "OK");
            });
        server_->set_http_handler<GET>(
            "/metadata",
            [this](coro_http_request& req, coro_http_response& resp) {
                std::lock_guard<std::mutex> lock(mutex_);
                auto it =
                    store_.find(std::string(req.get_decode_query_value("key")));
                if (it == store_.end()) {
                    resp.set_status_and_content(status_type::not_found, "");
                    return;
                }
                resp.set_status_and_content(status_type::ok, it->second);
            });
        server_->set_http_handler<PUT>(
            "/metadata",
            [this](coro_http_request& req, coro_http_response& resp) {
                std::lock_guard<std::mutex> lock(mutex_);
                store_[std::string(req.get_decode_query_value("key"))] =
                    std::string(req.get_body());
                resp.set_status_and_content(status_type::ok, "");
            });
        server_->set_http_handler<http_method::DEL>(
            "/metadata",
            [this](coro_http_request& req, coro_http_response& resp) {
                std::lock_guard<std::mutex> lock(mutex_);
                store_.erase(std::string(req.get_decode_query_value("key")));
                resp.set_status_and_content(status_type::ok, "");
            });
    }

    uint16_t port_ = 0;
    std::unique_ptr<coro_http::coro_http_server> server_;
    mutable std::mutex mutex_;
    std::unordered_map<std::string, std::string> store_;
};

class TransportPluginEngineTest : public ::testing::Test {
   protected:
    void SetUp() override {
        TransportPluginRegistry::Get().resetForTest();
        src_.assign(kLength, 0);
        dst_.assign(kLength, 0);
        for (size_t i = 0; i < kLength; ++i) src_[i] = char(i * 7 + 3);
    }
    void TearDown() override { TransportPluginRegistry::Get().resetForTest(); }

    // Registers the buffers; the engines must outlive the test body only.
    void registerBuffers(TransferEngine& initiator, TransferEngine& target) {
        ASSERT_TRUE(initiator.registerLocalMemory(src_.data(), kLength).ok());
        ASSERT_TRUE(target.registerLocalMemory(dst_.data(), kLength).ok());
    }

    void unregisterBuffers(TransferEngine& initiator, TransferEngine& target) {
        initiator.unregisterLocalMemory(src_.data(), kLength);
        target.unregisterLocalMemory(dst_.data(), kLength);
    }

    Status write(TransferEngine& initiator, TransferEngine& target,
                 TransportType hint) {
        std::fill(dst_.begin(), dst_.end(), 0);
        auto status = writeOnce(initiator, target.getSegmentName(), src_.data(),
                                dst_.data(), kLength, hint);
        if (status.ok() && std::memcmp(src_.data(), dst_.data(), kLength) != 0)
            return Status::InternalError("data mismatch");
        return status;
    }

    std::vector<char> src_, dst_;
};

TEST_F(TransportPluginEngineTest, PluginSlotsComeAfterBuiltins) {
    TransferEngine engine(makeConfig(pluginDir()));
    ASSERT_TRUE(engine.available());
    const auto slot = parseTransportType("loopback");
    ASSERT_EQ(slot, kFirstPluginSlot);
    auto expected = builtinOrder(Peer::transports(engine));
    expected.push_back(slot);
    EXPECT_EQ(Peer::supported(engine), expected);
}

TEST_F(TransportPluginEngineTest, HintRoutesToPlugin) {
    TENT_TEST_REQUIRE_BUILTIN_TCP();
    TransferEngine target(makeConfig(pluginDir()));
    TransferEngine initiator(makeConfig(pluginDir()));
    ASSERT_TRUE(target.available() && initiator.available());
    registerBuffers(initiator, target);

    const auto slot = parseTransportType("loopback");
    ASSERT_NE(slot, UNSPEC);
    const auto before = loopbackBytesCopied();
    ASSERT_GE(before, 0);
    EXPECT_TRUE(write(initiator, target, slot).ok());
    EXPECT_EQ(loopbackBytesCopied() - before, (long long)kLength);

    // Same through the C API's name lookup.
    const int c_type = tent_transport_type_by_name("loopback");
    EXPECT_EQ(c_type, static_cast<int>(slot));
    EXPECT_EQ(tent_transport_type_by_name("not_a_transport"), TRANSPORT_UNSPEC);
    EXPECT_EQ(tent_transport_type_by_name(nullptr), TRANSPORT_UNSPEC);
    EXPECT_EQ(tent_transport_type_by_name("tcp"), TRANSPORT_TCP);
    EXPECT_TRUE(write(initiator, target, c_to_transport_hint(c_type)).ok());
    EXPECT_EQ(loopbackBytesCopied() - before, 2 * (long long)kLength);

    // No hint: the default policy keeps the built-in order, so TCP carries
    // it and the plugin is not touched.
    EXPECT_TRUE(write(initiator, target, UNSPEC).ok());
    EXPECT_EQ(loopbackBytesCopied() - before, 2 * (long long)kLength);
    unregisterBuffers(initiator, target);
}

// The engine with the policy is the first to load plugins in this process,
// so the plugin name must already resolve when its selector parses policies.
TEST_F(TransportPluginEngineTest, PolicyNamesResolveToPlugin) {
    auto conf = makeConfig(pluginDir());
    conf->set("policy", loopbackOnlyPolicy());
    TransferEngine initiator(conf);
    TransferEngine target(makeConfig(pluginDir()));
    ASSERT_TRUE(target.available() && initiator.available());
    registerBuffers(initiator, target);
    const auto before = loopbackBytesCopied();
    EXPECT_TRUE(write(initiator, target, UNSPEC).ok());
    EXPECT_EQ(loopbackBytesCopied() - before, (long long)kLength);
    unregisterBuffers(initiator, target);
}

// A policy's transport list is not intersected with the target buffer's
// transports (same as for built-ins), so the selector picks loopback; the
// plugin then rejects a target without loopback, and nothing falls back.
TEST_F(TransportPluginEngineTest, PolicyPinnedToPluginMissingOnTargetFails) {
    TENT_TEST_REQUIRE_BUILTIN_TCP();
    auto target_conf = makeConfig(pluginDir());
    target_conf->set("transports/loopback/enable", false);
    TransferEngine target(target_conf);
    auto conf = makeConfig(pluginDir());
    conf->set("policy", loopbackOnlyPolicy());
    TransferEngine initiator(conf);
    ASSERT_TRUE(target.available() && initiator.available());
    registerBuffers(initiator, target);
    const auto before = loopbackBytesCopied();
    EXPECT_FALSE(write(initiator, target, UNSPEC).ok());
    EXPECT_EQ(loopbackBytesCopied(), before);
    EXPECT_EQ(dst_[0], 0);
    unregisterBuffers(initiator, target);
}

// A policy that names only plugins that are not loaded restricts nothing
// (the same fail-open rule as for unknown names); this is logged.
TEST_F(TransportPluginEngineTest, PolicyNamingUnloadedPluginIsReported) {
    LogCapture log;
    auto conf = makeConfig(pluginDir());
    conf->set("policy", json::parse(R"([{"name":"vx","segment_type":"memory",)"
                                    R"("transports":["vendorx"]}])"));
    TransferEngine initiator(conf);
    TransferEngine target(makeConfig(pluginDir()));
    ASSERT_TRUE(target.available() && initiator.available());
    EXPECT_TRUE(log.contains("Policy vx names no loaded transport"));
    registerBuffers(initiator, target);
    EXPECT_TRUE(write(initiator, target, UNSPEC).ok());
    unregisterBuffers(initiator, target);
}

// Plugin names in the policy of an engine without plugins/dir are unknown
// names, whether or not another engine of the process loaded the plugin.
TEST_F(TransportPluginEngineTest, PluginNamesIgnoredWithoutPluginDir) {
    TENT_TEST_REQUIRE_BUILTIN_TCP();
    TransferEngine loader(makeConfig(pluginDir()));
    ASSERT_TRUE(loader.available());
    ASSERT_NE(parseTransportType("loopback"), UNSPEC);
    auto conf = makeConfig("");
    conf->set("policy", loopbackOnlyPolicy());
    TransferEngine initiator(conf);
    TransferEngine target(makeConfig(""));
    ASSERT_TRUE(target.available() && initiator.available());
    registerBuffers(initiator, target);
    const auto before = loopbackBytesCopied();
    EXPECT_TRUE(write(initiator, target, UNSPEC).ok());
    EXPECT_EQ(loopbackBytesCopied(), before);
    unregisterBuffers(initiator, target);
}

// A queued loopback write must not land in memory the target unregistered
// after the submit.
TEST_F(TransportPluginEngineTest, QueuedWriteAfterTargetUnregisterFails) {
    auto conf = makeConfig(pluginDir());
    conf->set("transports/loopback/async", true);
    conf->set("transports/loopback/delay_us", 300000);
    TransferEngine target(conf);
    TransferEngine initiator(conf);
    ASSERT_TRUE(target.available() && initiator.available());
    registerBuffers(initiator, target);
    SegmentID handle = 0;
    ASSERT_TRUE(initiator.openSegment(handle, target.getSegmentName()).ok());
    auto batch = initiator.allocateBatch(1);
    Request request;
    request.opcode = Request::WRITE;
    request.source = src_.data();
    request.target_id = handle;
    request.target_offset = reinterpret_cast<uint64_t>(dst_.data());
    request.length = kLength;
    request.transport_hint = parseTransportType("loopback");
    const auto before = loopbackBytesCopied();
    ASSERT_TRUE(initiator.submitTransfer(batch, {request}).ok());
    ASSERT_TRUE(target.unregisterLocalMemory(dst_.data(), kLength).ok());
    TransferStatusEnum final_state = INITIAL;
    EXPECT_FALSE(plugin_test::waitDone(initiator, batch, &final_state));
    EXPECT_EQ(final_state, FAILED);
    EXPECT_EQ(loopbackBytesCopied(), before);
    EXPECT_EQ(std::count(dst_.begin(), dst_.end(), 0), (long)kLength);
    initiator.freeBatch(batch);
    initiator.unregisterLocalMemory(src_.data(), kLength);
}

// Registrations may nest or share a start address; the loopback plugin
// serves a copy if any registration covers it, and unregistering one leaves
// the others usable.
TEST_F(TransportPluginEngineTest, NestedRegistrationsServeCopies) {
    TransferEngine target(makeConfig(pluginDir()));
    TransferEngine initiator(makeConfig(pluginDir()));
    ASSERT_TRUE(target.available() && initiator.available());
    registerBuffers(initiator, target);
    char* inner = dst_.data() + 4096;
    ASSERT_TRUE(target.registerLocalMemory(inner, 4096).ok());
    const auto slot = parseTransportType("loopback");
    const auto before = loopbackBytesCopied();
    // 8 KiB from inner: only the outer registration covers it.
    ASSERT_TRUE(writeOnce(initiator, target.getSegmentName(), src_.data(),
                          inner, 8192, slot)
                    .ok());
    EXPECT_EQ(loopbackBytesCopied() - before, 8192);
    EXPECT_EQ(std::memcmp(src_.data(), inner, 8192), 0);
    // Dropping the inner registration keeps the outer one working.
    ASSERT_TRUE(target.unregisterLocalMemory(inner, 4096).ok());
    std::fill(dst_.begin(), dst_.end(), 0);
    ASSERT_TRUE(writeOnce(initiator, target.getSegmentName(), src_.data(),
                          inner, 4096, slot)
                    .ok());
    EXPECT_EQ(std::memcmp(src_.data(), inner, 4096), 0);
    unregisterBuffers(initiator, target);
}

// A plugin dropped after install (here: capabilities beyond its
// declaration) is not listed as loaded in the banner.
TEST_F(TransportPluginEngineTest, BannerOmitsDroppedPlugins) {
    TENT_TEST_REQUIRE_BUILTIN_TCP();
    LogCapture log;
    auto conf = makeConfig(plugin_test::badPluginDir());
    conf->set("verbose", true);
    TransferEngine engine(conf);
    ASSERT_TRUE(engine.available());
    bool seen = false;
    for (const auto& line : log.lines()) {
        if (line.find(" - Loaded Transports:") == std::string::npos) continue;
        seen = true;
        EXPECT_EQ(line, " - Loaded Transports:  tcp ");
    }
    EXPECT_TRUE(seen);
    EXPECT_TRUE(log.contains("caps_overclaim(rejected_caps_overclaim)"));
}

#ifdef TENT_TEST_CLAIM_PLUGIN_DIR
// A plugin that claims a built-in slot left empty by the core (hylink is not
// compiled in) is that transport: same name, slot, rules and wire value.
TEST_F(TransportPluginEngineTest, PluginProvidesMissingBuiltin) {
    auto make = [] {
        auto conf = makeConfig(TENT_TEST_CLAIM_PLUGIN_DIR);
        conf->set("transports/loopback/enable", nullptr);
        conf->set("transports/hylink/enable", true);
        return conf;
    };
    TransferEngine target(make());
    TransferEngine initiator(make());
    ASSERT_TRUE(target.available() && initiator.available());
    auto reports = plugin_test::reportsByName();
    ASSERT_EQ(reports["hylink"].outcome, PluginOutcome::kLoaded)
        << reports["hylink"].detail;
    EXPECT_EQ(parseTransportType("hylink"), HYLINK);
    ASSERT_NE(Peer::transports(initiator)[HYLINK], nullptr);
    EXPECT_EQ(pluginTransportSlotCount(), 0);
    registerBuffers(initiator, target);

    const std::string path =
        std::string(TENT_TEST_CLAIM_PLUGIN_DIR) + "/libtent_xport_hylink.so";
    void* handle = dlopen(path.c_str(), RTLD_NOW | RTLD_NOLOAD);
    ASSERT_NE(handle, nullptr);
    auto copied = reinterpret_cast<uint64_t (*)()>(
        dlsym(handle, "tent_xport_loopback_bytes_copied"));
    ASSERT_NE(copied, nullptr);
    const auto before = copied();
    EXPECT_TRUE(write(initiator, target, HYLINK).ok());
    EXPECT_EQ(copied() - before, kLength);
    dlclose(handle);

    // On the wire it is the built-in integer, not a plugin name.
    const auto published = json::parse(Peer::localDumpedJson(target));
    bool found = false;
    for (const auto& buffer : published["detail"]["buffers"]) {
        if (buffer["addr"].get<uint64_t>() !=
            reinterpret_cast<uint64_t>(dst_.data()))
            continue;
        found = true;
        EXPECT_FALSE(buffer.contains("plugin_transports"));
        const auto& types = buffer["transports"];
        EXPECT_NE(std::find(types.begin(), types.end(), json(int(HYLINK))),
                  types.end());
    }
    EXPECT_TRUE(found);
    unregisterBuffers(initiator, target);
}
#endif

// TENT_XPORT_TCP/RDMA: in plugin and both builds the build's own plugin
// directory holds the built-in transports as plugins; a static build leaves
// none there. With TCP=plugin the TCP plugin fills the built-in slot and
// carries the data; with TCP=both the core's TCP is used and the plugin is
// rejected as a conflict. In plugin mode this executable contains neither
// transport, so the plugins run their own code.
TEST_F(TransportPluginEngineTest, BuiltinTransportsAsPlugins) {
    const std::string dir = TENT_TEST_BUILTIN_PLUGIN_DIR;
    const std::string tcp_mode = TENT_TEST_XPORT_TCP;
    const std::string rdma_mode = TENT_TEST_XPORT_RDMA;
    auto built = [&](const std::string& name) {
        const std::string file = dir + "/libtent_xport_" + name + ".so";
        return access(file.c_str(), F_OK) == 0;
    };
    EXPECT_EQ(built("tcp"), tcp_mode != "static");
    EXPECT_EQ(built("rdma"), rdma_mode != "static");

    LogCapture log;
    auto make = [&] {
        auto conf = makeConfig("");
        conf->set("plugins/dir", dir);
        conf->set("verbose", true);
        return conf;
    };
    TransferEngine target(make());
    TransferEngine initiator(make());
    ASSERT_TRUE(target.available() && initiator.available());
    auto reports = plugin_test::reportsByName();

    // Whether the TCP object comes from the plugin file: its vtable lives in
    // the object that defines the class.
    const auto& tcp = Peer::transports(initiator)[TCP];
    ASSERT_NE(tcp, nullptr);
    Dl_info info{};
    ASSERT_NE(dladdr(*reinterpret_cast<void* const*>(tcp.get()), &info), 0);
    const std::string tcp_file = dir + "/libtent_xport_tcp.so";
    const bool tcp_from_plugin = tcp_file == info.dli_fname;
    if (tcp_mode == "plugin") {
        ASSERT_EQ(reports["tcp"].outcome, PluginOutcome::kLoaded)
            << reports["tcp"].detail;
        EXPECT_TRUE(tcp_from_plugin) << info.dli_fname;
        EXPECT_TRUE(log.contains("tcp(loaded,slot " + std::to_string(TCP) +
                                 "," + tcp_file + ")"));
    } else {
        EXPECT_FALSE(tcp_from_plugin) << info.dli_fname;
        if (tcp_mode == "both")
            EXPECT_EQ(reports["tcp"].outcome,
                      PluginOutcome::kRejectedConflictWithBuiltin);
        else
            EXPECT_EQ(reports.count("tcp"), 0u);
    }
    // RDMA is off in these engines, but the plugin must still load cleanly in
    // a host that does not link the RDMA objects.
    if (rdma_mode == "static") {
        EXPECT_EQ(reports.count("rdma"), 0u);
    } else {
        ASSERT_EQ(reports.count("rdma"), 1u);
        const auto outcome = reports["rdma"].outcome;
        EXPECT_TRUE(outcome == PluginOutcome::kDisabledByConfig ||
                    outcome == PluginOutcome::kNotApplicable)
            << pluginOutcomeName(outcome) << ": " << reports["rdma"].detail;
    }

    registerBuffers(initiator, target);
    EXPECT_TRUE(write(initiator, target, TCP).ok());
    unregisterBuffers(initiator, target);
}

TEST_F(TransportPluginEngineTest, SameMachineOnlyIsEnforcedBySelector) {
    TENT_TEST_REQUIRE_BUILTIN_TCP();
    TransferEngine target(makeConfig(pluginDir()));
    TransferEngine initiator(makeConfig(pluginDir()));
    ASSERT_TRUE(target.available() && initiator.available());
    registerBuffers(initiator, target);
    moveToOtherMachine(target);
    const auto slot = parseTransportType("loopback");
    const auto before = loopbackBytesCopied();
    EXPECT_FALSE(write(initiator, target, slot).ok());
    EXPECT_EQ(loopbackBytesCopied(), before);
    // TCP has no locality rule.
    EXPECT_TRUE(write(initiator, target, TCP).ok());
    unregisterBuffers(initiator, target);
}

TEST_F(TransportPluginEngineTest, SameMachineOnlyIsEnforcedInLegacyMode) {
    auto conf = makeConfig(pluginDir());
    conf->set("use_legacy_transport_selection", true);
    TransferEngine target(makeConfig(pluginDir()));
    TransferEngine initiator(conf);
    ASSERT_TRUE(target.available() && initiator.available());
    registerBuffers(initiator, target);
    const auto slot = parseTransportType("loopback");
    auto before = loopbackBytesCopied();
    EXPECT_TRUE(write(initiator, target, slot).ok());
    EXPECT_EQ(loopbackBytesCopied() - before, (long long)kLength);

    moveToOtherMachine(target);
    ASSERT_TRUE(Peer::segments(initiator)
                    .invalidateAllCacheForRemote(target.getSegmentName())
                    .ok());
    before = loopbackBytesCopied();
    EXPECT_FALSE(write(initiator, target, slot).ok());
    EXPECT_EQ(loopbackBytesCopied(), before);
    unregisterBuffers(initiator, target);
}

TEST_F(TransportPluginEngineTest, PluginOnlyEnginesMoveDataAndNotifications) {
    auto conf = makeConfig(pluginDir());
    conf->set("transports/tcp/enable", false);
    TransferEngine target(conf);
    TransferEngine initiator(conf);
    ASSERT_TRUE(target.available() && initiator.available());
    EXPECT_EQ(Peer::supported(initiator),
              std::vector<TransportType>{parseTransportType("loopback")});
    registerBuffers(initiator, target);
    const auto before = loopbackBytesCopied();
    EXPECT_TRUE(write(initiator, target, UNSPEC).ok());
    EXPECT_EQ(loopbackBytesCopied() - before, (long long)kLength);

    SegmentID handle = 0;
    ASSERT_TRUE(initiator.openSegment(handle, target.getSegmentName()).ok());
    Notification notify{"loopback-test", "hello over loopback"};
    ASSERT_TRUE(initiator.sendNotification(handle, notify).ok());
    std::vector<Notification> received;
    for (int i = 0; i < 1000 && received.empty(); ++i) {
        ASSERT_TRUE(target.receiveNotification(received).ok());
        if (received.empty())
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    ASSERT_EQ(received.size(), 1u);
    EXPECT_EQ(received[0].msg, "hello over loopback");
    unregisterBuffers(initiator, target);
}

TEST_F(TransportPluginEngineTest, AsyncTasksPassThroughPendingAndCancel) {
    auto conf = makeConfig(pluginDir());
    conf->set("transports/loopback/async", true);
    conf->set("transports/loopback/delay_us", 300000);
    TransferEngine target(conf);
    TransferEngine initiator(conf);
    ASSERT_TRUE(target.available() && initiator.available());
    registerBuffers(initiator, target);
    SegmentID handle = 0;
    ASSERT_TRUE(initiator.openSegment(handle, target.getSegmentName()).ok());
    const auto slot = parseTransportType("loopback");
    auto submit = [&](BatchID batch) {
        Request request;
        request.opcode = Request::WRITE;
        request.source = src_.data();
        request.target_id = handle;
        request.target_offset = reinterpret_cast<uint64_t>(dst_.data());
        request.length = kLength;
        request.transport_hint = slot;
        return initiator.submitTransfer(batch, {request});
    };

    auto batch = initiator.allocateBatch(1);
    ASSERT_TRUE(submit(batch).ok());
    TransferStatus status;
    ASSERT_TRUE(initiator.getTransferStatus(batch, status).ok());
    EXPECT_EQ(status.s, PENDING);
    TransferStatusEnum final_state = INITIAL;
    EXPECT_TRUE(plugin_test::waitDone(initiator, batch, &final_state));
    EXPECT_EQ(final_state, COMPLETED);
    initiator.freeBatch(batch);

    batch = initiator.allocateBatch(1);
    const auto before = loopbackBytesCopied();
    ASSERT_TRUE(submit(batch).ok());
    ASSERT_TRUE(initiator.cancelTransfer(batch, 0).ok());
    plugin_test::waitDone(initiator, batch, &final_state);
    EXPECT_EQ(final_state, CANCELED);
    EXPECT_EQ(loopbackBytesCopied(), before);
    initiator.freeBatch(batch);
    unregisterBuffers(initiator, target);
}

TEST_F(TransportPluginEngineTest, RequiredPluginMissingFailsConstruction) {
    LogCapture log;
    auto conf = makeConfig(pluginDir());
    conf->set("plugins/required", std::vector<std::string>{"nonexist"});
    {
        TransferEngine engine(conf);
        EXPECT_FALSE(engine.available());
    }
    EXPECT_TRUE(
        log.contains("required transport plugin 'nonexist' not loaded"));
    // The process and the registry stay usable.
    TransferEngine engine(makeConfig(pluginDir()));
    EXPECT_TRUE(engine.available());
}

TEST_F(TransportPluginEngineTest, RequiredPluginWithoutDirectory) {
    LogCapture log;
    auto conf = makeConfig("");
    conf->set("plugins/required", std::vector<std::string>{"loopback"});
    TransferEngine engine(conf);
    EXPECT_FALSE(engine.available());
    EXPECT_TRUE(log.contains("plugins/dir is not set"));
}

TEST_F(TransportPluginEngineTest, RequiredPluginDisabledByConfig) {
    LogCapture log;
    auto conf = makeConfig(pluginDir());
    conf->set("plugins/required", std::vector<std::string>{"loopback"});
    conf->set("transports/loopback/enable", false);
    TransferEngine engine(conf);
    EXPECT_FALSE(engine.available());
    EXPECT_TRUE(log.contains("disabled_by_config"));
}

TEST_F(TransportPluginEngineTest, RequiredPluginPresent) {
    auto conf = makeConfig(pluginDir());
    conf->set("plugins/required", std::vector<std::string>{"loopback"});
    TransferEngine engine(conf);
    EXPECT_TRUE(engine.available());
}

TEST_F(TransportPluginEngineTest, EngineWithoutPluginDirIgnoresLoadedPlugins) {
    TransferEngine with(makeConfig(pluginDir()));
    // Enabling the plugin is not enough without plugins/dir.
    auto conf = makeConfig("");
    conf->set("transports/loopback/enable", true);
    TransferEngine without(conf);
    ASSERT_TRUE(with.available() && without.available());
    EXPECT_NE(Peer::transports(with)[kFirstPluginSlot], nullptr);
    EXPECT_EQ(Peer::transports(without)[kFirstPluginSlot], nullptr);
    EXPECT_EQ(Peer::supported(without),
              builtinOrder(Peer::transports(without)));
}

// An engine without plugins/dir must not consume the process-wide load: a
// later engine that configures plugins still gets them.
TEST_F(TransportPluginEngineTest, EngineWithoutDirDoesNotBlockLaterLoad) {
    TransferEngine first(makeConfig(""));
    ASSERT_TRUE(first.available());
    TransferEngine second(makeConfig(pluginDir()));
    ASSERT_TRUE(second.available());
    const auto slot = parseTransportType("loopback");
    ASSERT_EQ(slot, kFirstPluginSlot);
    EXPECT_NE(Peer::transports(second)[slot], nullptr);
}

// A central metadata store (metadata_type "http") sees plugin transports by
// name only, and a peer reading it maps the name back to its own slot.
TEST_F(TransportPluginEngineTest, CentralMetadataCarriesPluginNames) {
    HttpMetadataStore store;
    ASSERT_TRUE(store.start());
    auto make = [&](const std::string& name) {
        auto conf = makeConfig(pluginDir());
        conf->set("metadata_type", "http");
        conf->set("metadata_servers", store.endpoint());
        conf->set("local_segment_name", name);
        return conf;
    };
    TransferEngine target(make("tplug_central_target"));
    TransferEngine initiator(make("tplug_central_initiator"));
    ASSERT_TRUE(target.available() && initiator.available());
    registerBuffers(initiator, target);

    auto stored = store.get("tplug_central_target");
    ASSERT_TRUE(stored.has_value());
    auto published = json::parse(*stored);
    bool found = false;
    for (const auto& buffer : published["detail"]["buffers"]) {
        if (buffer["addr"].get<uint64_t>() !=
            reinterpret_cast<uint64_t>(dst_.data()))
            continue;
        found = true;
        EXPECT_EQ(buffer["plugin_transports"], json::parse(R"(["loopback"])"));
        for (const auto& type : buffer["transports"])
            EXPECT_LT(type.get<int>(), kNumBuiltinTransportTypes);
    }
    EXPECT_TRUE(found) << *stored;

    const auto before = loopbackBytesCopied();
    EXPECT_TRUE(write(initiator, target, parseTransportType("loopback")).ok());
    EXPECT_EQ(loopbackBytesCopied() - before, (long long)kLength);
    unregisterBuffers(initiator, target);
}

TEST_F(TransportPluginEngineTest, VerboseBannerListsPlugins) {
    TENT_TEST_REQUIRE_BUILTIN_TCP();
    LogCapture log;
    auto conf = makeConfig(pluginDir());
    conf->set("verbose", true);
    TransferEngine engine(conf);
    ASSERT_TRUE(engine.available());
    EXPECT_TRUE(
        log.contains(" - Transport plugins:  loopback(loaded,slot 19)"));
    EXPECT_TRUE(log.contains(" - Loaded Transports:  tcp loopback "));
}

// Selector-level checks of the plugin flags, independent of any plugin.
class PluginFlagSelectorTest : public ::testing::Test {
   protected:
    class DramTransport : public Transport {
       public:
        DramTransport() { caps.dram_to_dram = true; }
    };

    void SetUp() override { TransportPluginRegistry::Get().resetForTest(); }
    void TearDown() override { TransportPluginRegistry::Get().resetForTest(); }

    SelectionResult select(TransportType type, bool same_machine,
                           bool local_segment, bool host_staging) {
        TransportList transports;
        transports[type] = std::make_shared<DramTransport>();
        std::vector<TransportType> buffer{type};
        SelectionContext context{};
        context.segment_type = SegmentType::Memory;
        context.same_machine = same_machine;
        context.local_memory_type = MTYPE_CPU;
        context.remote_memory_type = MTYPE_CPU;
        context.buffer_transports = &buffer;
        context.transfer_size = 4096;
        context.priority_level = PRIO_HIGH;
        context.local_segment = local_segment;
        context.host_staging = host_staging;
        TransportSelector selector(std::make_shared<Config>());
        return selector.select(context, transports, 0, UNSPEC);
    }
};

TEST_F(PluginFlagSelectorTest, LocalityAndStagingFlags) {
    TransportType same_machine = UNSPEC, local_only = UNSPEC, staging = UNSPEC,
                  plain = UNSPEC;
    ASSERT_EQ(assignPluginTransportSlot("sm", TENT_XPORT_FLAG_SAME_MACHINE_ONLY,
                                        same_machine),
              SlotAssignResult::kAssigned);
    ASSERT_EQ(assignPluginTransportSlot(
                  "lo", TENT_XPORT_FLAG_LOCAL_SEGMENT_ONLY, local_only),
              SlotAssignResult::kAssigned);
    ASSERT_EQ(assignPluginTransportSlot(
                  "st", TENT_XPORT_FLAG_HOST_STAGING_CAPABLE, staging),
              SlotAssignResult::kAssigned);
    ASSERT_EQ(assignPluginTransportSlot("pl", 0, plain),
              SlotAssignResult::kAssigned);

    EXPECT_EQ(select(same_machine, true, false, false).transport, same_machine);
    EXPECT_EQ(select(same_machine, false, false, false).transport, UNSPEC);
    EXPECT_EQ(select(local_only, true, true, false).transport, local_only);
    EXPECT_EQ(select(local_only, true, false, false).transport, UNSPEC);
    EXPECT_EQ(select(plain, false, false, false).transport, plain);
    // Host staging only takes plugins that declare it.
    EXPECT_EQ(select(staging, false, false, true).transport, staging);
    EXPECT_EQ(select(plain, false, false, true).transport, UNSPEC);
}

}  // namespace
}  // namespace tent
}  // namespace mooncake
