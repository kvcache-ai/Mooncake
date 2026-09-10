// Copyright 2026 KVCache.AI
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//     http://www.apache.org/licenses/LICENSE-2.0

#include <gtest/gtest.h>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <chrono>
#include <cstdint>
#include <future>
#include <memory>
#include <mutex>
#include <set>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include "tent/metrics/config_loader.h"
#include "tent/metrics/tent_metrics.h"
#include "tent/runtime/segment_registry.h"
#include "tent/runtime/transfer_engine_impl.h"
#include "tent/transport/hp_tcp/hp_tcp_transport.h"

namespace mooncake::tent {

// Fixture access only: replace the metadata sink, never perform cleanup.
class RegistrationRollbackTestPeer {
   public:
    static SegmentManager& manager(TransferEngineImpl& engine) {
        return engine.metadata_->segmentManager();
    }
    static void setRegistry(TransferEngineImpl& engine,
                            std::unique_ptr<SegmentRegistry> registry) {
        manager(engine).registry_ = std::move(registry);
    }
};

class HighPerformanceTcpTransportTestPeer {
   public:
    static HighPerformanceTcpBufferRegistry& registry(
        HighPerformanceTcpTransport& hp) {
        return hp.registry_;
    }
};

namespace {
using namespace std::chrono_literals;

class Gate {
   public:
    void arriveAndWait() {
        reached_.set_value();
        released_.get_future().wait();
    }
    void wait() { reached_.get_future().wait(); }
    void release() { released_.set_value(); }

   private:
    std::promise<void> reached_, released_;
};

struct Trace {
    void add(std::string event) {
        std::lock_guard<std::mutex> lock(mutex);
        events.push_back(std::move(event));
    }
    void clear() {
        std::lock_guard<std::mutex> lock(mutex);
        events.clear();
    }
    std::vector<std::string> snapshot() {
        std::lock_guard<std::mutex> lock(mutex);
        return events;
    }
    std::mutex mutex;
    std::vector<std::string> events;
};

class RecordingRegistry : public SegmentRegistry {
   public:
    explicit RecordingRegistry(Trace& trace) : trace_(trace) {}
    Status putSegmentDesc(SegmentDescRef& desc) override {
        trace_.add("put");
        attempts.push_back(*desc);
        return fail_put ? Status::MetadataError("injected put failure")
                        : Status::OK();
    }
    Status getSegmentDesc(SegmentDescRef&, const std::string&) override {
        return Status::NotImplemented("unused");
    }
    Status deleteSegmentDesc(const std::string&) override {
        return Status::OK();
    }
    bool fail_put = false;
    std::vector<SegmentDesc> attempts;

   private:
    Trace& trace_;
};

// Gate/fault injection wraps the real HP registration implementation. In
// particular, the test does not remove the orphan on behalf of the engine.
class GatedHpTransport : public Transport {
   public:
    explicit GatedHpTransport(Trace& trace) : trace_(trace) {}
    Status addMemoryBuffer(std::vector<BufferDesc>& descs,
                           const MemoryOptions& options) override {
        if (before_add) before_add(descs);
        return hp.addMemoryBuffer(descs, options);
    }
    Status removeMemoryBuffer(BufferDesc& desc) override {
        if (before_remove) before_remove(desc);
        trace_.add("hp-remove");
        ++removals;
        return hp.removeMemoryBuffer(desc);
    }
    bool tracksLocalBuffer(const BufferDesc& desc) const override {
        return hp.tracksLocalBuffer(desc);
    }
    const char* getName() const override { return "gated-hp"; }

    HighPerformanceTcpTransport hp;
    std::function<void(const std::vector<BufferDesc>&)> before_add;
    std::function<void(const BufferDesc&)> before_remove;
    std::atomic<int> removals{0};

   private:
    Trace& trace_;
};

bool hasTransport(const BufferDesc& desc, TransportType type) {
    return std::find(desc.transports.begin(), desc.transports.end(), type) !=
           desc.transports.end();
}

class RecordingTcpTransport : public Transport {
   public:
    explicit RecordingTcpTransport(Trace& trace) : trace_(trace) {
        caps.dram_to_dram = true;
    }
    Status addMemoryBuffer(std::vector<BufferDesc>& descs,
                           const MemoryOptions&) override {
        std::lock_guard<std::mutex> lock(mutex_);
        for (auto& desc : descs) {
            trace_.add("tcp-add");
            entries_.insert({desc.addr, desc.length});
            if (!hasTransport(desc, TCP)) desc.transports.push_back(TCP);
            desc.transport_attrs[TCP] = "tcp";
        }
        return Status::OK();
    }
    Status removeMemoryBuffer(BufferDesc& desc) override {
        trace_.add("tcp-remove");
        ++removals;
        if (fail_remove) {
            return Status::InvalidArgument("injected tcp remove failure");
        }
        std::lock_guard<std::mutex> lock(mutex_);
        entries_.erase({desc.addr, desc.length});
        desc.transport_attrs.erase(TCP);
        desc.transports.erase(
            std::remove(desc.transports.begin(), desc.transports.end(), TCP),
            desc.transports.end());
        return Status::OK();
    }
    bool tracksLocalBuffer(const BufferDesc& desc) const override {
        std::lock_guard<std::mutex> lock(mutex_);
        return entries_.count({desc.addr, desc.length}) != 0;
    }
    const char* getName() const override { return "recording-tcp"; }

    bool fail_remove = false;
    std::atomic<int> removals{0};

   private:
    Trace& trace_;
    mutable std::mutex mutex_;
    std::set<std::pair<uint64_t, uint64_t>> entries_;
};

uint16_t getFreeTcpPort() {
    int sock = ::socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) return 0;
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = htons(0);
    if (::bind(sock, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
        ::close(sock);
        return 0;
    }
    socklen_t len = sizeof(addr);
    if (::getsockname(sock, reinterpret_cast<sockaddr*>(&addr), &len) != 0) {
        ::close(sock);
        return 0;
    }
    const int port = ntohs(addr.sin_port);
    ::close(sock);
    return static_cast<uint16_t>(port);
}

double prometheusSeries(TentMetrics& metrics, const std::string& series) {
    std::istringstream input(metrics.getPrometheusMetrics());
    std::string line;
    const std::string prefix = series + " ";
    while (std::getline(input, line)) {
        if (line.rfind(prefix, 0) != 0) continue;
        try {
            return std::stod(line.substr(prefix.size()));
        } catch (...) {
            return 0;
        }
    }
    return 0;
}

class ScopedMetrics {
   public:
    ScopedMetrics() {
        TentMetrics::instance().shutdown();
        MetricsConfig config;
        config.enabled = true;
        config.http_host = "127.0.0.1";
        config.http_port = getFreeTcpPort();
        config.report_interval_seconds = 0;
        status = TentMetrics::instance().initialize(config);
        TentMetrics::setEnabled(true);
    }
    ~ScopedMetrics() { TentMetrics::instance().shutdown(); }

    Status status;
};

bool containsText(const Status& status, const std::string& text) {
    return status.ToString().find(text) != std::string::npos;
}

std::shared_ptr<Config> testConfig() {
    auto conf = std::make_shared<Config>();
    conf->set("metadata_type", "p2p");
    conf->set("rpc_server_hostname", "127.0.0.1");
    conf->set("rpc_server_port", 0);
    conf->set("log_level", "warning");
    for (const auto* type : {"tcp", "hp_tcp", "rdma", "shm", "io_uring",
                             "nvlink", "mnnvl", "gds", "ascend_direct"}) {
        conf->set(std::string("transports/") + type + "/enable", false);
    }
    conf->set("metrics/enabled", false);
    return conf;
}

class RegistrationRollbackTest : public ::testing::Test {
   protected:
    void SetUp() override {
        engine = std::make_unique<TransferEngineImpl>(testConfig());
        ASSERT_TRUE(engine->available());
        auto sink = std::make_unique<RecordingRegistry>(trace);
        registry = sink.get();
        RegistrationRollbackTestPeer::setRegistry(*engine, std::move(sink));
        hp = std::make_shared<GatedHpTransport>(trace);
        engine->swapTransportForTest(HP_TCP, hp);
        options.type = HP_TCP;
        options.perm = kGlobalReadWrite;
    }
    void TearDown() override {
        // Members keep buffer storage alive through engine teardown, including
        // on the unfixed negative control. No manual range removal here.
        engine.reset();
    }
    void installTcp(std::shared_ptr<RecordingTcpTransport> transport) {
        tcp = std::move(transport);
        engine->swapTransportForTest(TCP, tcp);
    }
    Status add(void* ptr, size_t size, MemoryOptions add_options) {
        return engine->registerLocalMemory({ptr}, {size}, add_options);
    }
    Status add(void* ptr, size_t size) {
        return engine->registerLocalMemory({ptr}, {size}, options);
    }
    SegmentManager& manager() {
        return RegistrationRollbackTestPeer::manager(*engine);
    }
    HighPerformanceTcpBufferRegistry& hpRegistry() {
        return HighPerformanceTcpTransportTestPeer::registry(hp->hp);
    }
    uint64_t base() const { return reinterpret_cast<uint64_t>(data.data()); }
    uint64_t id() {
        auto snapshot = manager().getLocal();
        auto* desc = snapshot->findBuffer(base(), data.size());
        EXPECT_NE(desc, nullptr);
        HighPerformanceTcpBufferAttr attr;
        EXPECT_TRUE(DecodeHighPerformanceTcpBufferAttr(
                        desc->transport_attrs.at(HP_TCP), &attr)
                        .ok());
        return attr.registration_id;
    }
    std::future<Status> startFailure(Gate& gate,
                                     MemoryOptions failure_options = {}) {
        if (failure_options.type == UNSPEC) failure_options = options;
        auto gated = std::make_shared<std::atomic<bool>>(false);
        hp->before_add = [&gate, gated](const auto&) {
            if (!gated->exchange(true)) gate.arriveAndWait();
        };
        auto result = std::async(std::launch::async, [&, failure_options] {
            MemoryOptions opts = failure_options;
            return engine->registerLocalMemory({data.data(), data.data() + 16},
                                               {data.size(), 16}, opts);
        });
        gate.wait();  // the real tracker has already pinned the duplicate
        return result;
    }

    Trace trace;
    std::array<uint8_t, 128> data{};
    std::unique_ptr<TransferEngineImpl> engine;
    std::shared_ptr<GatedHpTransport> hp;
    std::shared_ptr<RecordingTcpTransport> tcp;
    RecordingRegistry* registry = nullptr;
    MemoryOptions options;
};

// A self-owned loopback fixture; destructor joins all socket workers.
struct WireServer {
    HighPerformanceTcpWorkers workers{{.worker_count = 1}};
    HighPerformanceTcpServer server;
    uint16_t port = 0;
    explicit WireServer(HighPerformanceTcpBufferRegistry& registry)
        : server({.bind_address = "127.0.0.1",
                  .port = 0,
                  .max_transfer_bytes = 1024,
                  .chunk_size = 32,
                  .progress_timeout_ms = 1000,
                  .max_connections = 2},
                 &registry, &workers) {}
    ~WireServer() {
        (void)server.stop();
        (void)workers.stop();
    }
    Status start() {
        CHECK_STATUS(workers.start());
        return server.start(&port);
    }
};

TEST_F(RegistrationRollbackTest, FailedBatchRevokesOldWireCapability) {
    ASSERT_TRUE(add(data.data(), data.size()).ok());
    const auto old_id = id();
    Gate gate;
    auto batch = startFailure(gate);
    const auto unregistered =
        engine->unregisterLocalMemory(data.data(), data.size());
    gate.release();
    const auto failed = batch.get();
    ASSERT_TRUE(unregistered.ok());
    ASSERT_TRUE(failed.IsInvalidArgument());
    ASSERT_TRUE(manager().getLocal()->getMemory().buffers.empty());

    WireServer wire(hpRegistry());
    ASSERT_TRUE(wire.start().ok());
    asio::io_context io;
    asio::ip::tcp::socket socket(io);
    socket.connect({asio::ip::make_address("127.0.0.1"), wire.port});
    const auto header = EncodeHighPerformanceTcpRequest(
        {HighPerformanceTcpOpcode::kWrite, 123, old_id, base(), data.size()});
    std::array<uint8_t, 128> payload;
    payload.fill(0x5a);
    asio::write(socket, asio::buffer(header));
    asio::write(socket, asio::buffer(payload));  // complete legal WRITE body
    std::array<uint8_t, kHighPerformanceTcpResponseSize> bytes{};
    asio::read(socket, asio::buffer(bytes));
    HighPerformanceTcpResponseFrame response;
    ASSERT_TRUE(
        DecodeHighPerformanceTcpResponse(bytes.data(), bytes.size(), &response)
            .ok());
    std::cout << "WIRE_EVIDENCE old_id_status="
              << static_cast<int>(response.status)
              << " committed=" << response.committed_bytes
              << " mutated=" << (data == payload)
              << " engine_removals=" << hp->removals.load() << '\n';
    EXPECT_EQ(response.request_id, 123u);
    EXPECT_EQ(response.status, HighPerformanceTcpStatus::kRangeRejected);
    EXPECT_EQ(response.committed_bytes, 0u);
    EXPECT_EQ(data, (std::array<uint8_t, 128>{}));
    EXPECT_EQ(hp->removals, 1);
    EXPECT_FALSE(hpRegistry().tracks(base(), data.size()));
    ASSERT_EQ(registry->attempts.size(), 2u);
    EXPECT_TRUE(registry->attempts.back().getMemory().buffers.empty());
}

TEST_F(RegistrationRollbackTest,
       RegisterFailureCleansDuplicatesBeforeSyncAndPreservesErrors) {
    auto fake_tcp = std::make_shared<RecordingTcpTransport>(trace);
    fake_tcp->fail_remove = true;
    installTcp(fake_tcp);
    MemoryOptions initial_options = options;
    initial_options.type = UNSPEC;
    const std::vector<void*> ranges{data.data(), data.data() + 64};
    ASSERT_TRUE(
        engine->registerLocalMemory(ranges, {64, 64}, initial_options).ok());
    registry->attempts.clear();
    trace.clear();

    Gate gate;
    hp->before_add = [&gate](const auto&) { gate.arriveAndWait(); };
    auto batch = std::async(std::launch::async, [&] {
        return engine->registerLocalMemory(
            {data.data(), data.data() + 64, data.data() + 16}, {64, 64, 16},
            options);
    });
    gate.wait();
    const auto unregistered = engine->unregisterLocalMemory(ranges, {64, 64});
    registry->fail_put = true;
    gate.release();
    const auto failed = batch.get();

    EXPECT_TRUE(unregistered.ok());
    ASSERT_TRUE(failed.IsInvalidArgument()) << failed.ToString();
    EXPECT_TRUE(containsText(failed, "overlapping HP TCP buffer"))
        << failed.ToString();
    EXPECT_TRUE(containsText(failed, "registration rollback cleanup failed"))
        << failed.ToString();
    EXPECT_TRUE(containsText(failed, "injected tcp remove failure"))
        << failed.ToString();
    EXPECT_TRUE(
        containsText(failed, "registration rollback metadata sync failed"))
        << failed.ToString();
    EXPECT_TRUE(containsText(failed, "injected put failure"))
        << failed.ToString();
    EXPECT_EQ(fake_tcp->removals, 2);
    EXPECT_EQ(hp->removals, 2);
    EXPECT_FALSE(hpRegistry().tracks(base(), 64));
    EXPECT_FALSE(hpRegistry().tracks(base() + 64, 64));
    ASSERT_EQ(registry->attempts.size(), 1u);
    EXPECT_TRUE(registry->attempts.back().getMemory().buffers.empty());

    const auto events = trace.snapshot();
    EXPECT_EQ(events,
              (std::vector<std::string>{"tcp-remove", "hp-remove", "tcp-remove",
                                        "hp-remove", "put"}));
}

TEST_F(RegistrationRollbackTest, NormalUnregisterStillIgnoresCleanupFailure) {
    auto fake_tcp = std::make_shared<RecordingTcpTransport>(trace);
    fake_tcp->fail_remove = true;
    installTcp(fake_tcp);
    MemoryOptions initial_options = options;
    initial_options.type = UNSPEC;
    ASSERT_TRUE(add(data.data(), data.size(), initial_options).ok());
    registry->attempts.clear();

    const auto status = engine->unregisterLocalMemory(data.data(), data.size());

    EXPECT_TRUE(status.ok()) << status.ToString();
    EXPECT_EQ(fake_tcp->removals, 1);
    EXPECT_EQ(hp->removals, 1);
    EXPECT_FALSE(hpRegistry().tracks(base(), data.size()));
    ASSERT_EQ(registry->attempts.size(), 1u);
    EXPECT_TRUE(registry->attempts.back().getMemory().buffers.empty());
}

TEST_F(RegistrationRollbackTest,
       NormalBatchUnregisterContinuesCleanupAndReturnsSyncError) {
    auto fake_tcp = std::make_shared<RecordingTcpTransport>(trace);
    fake_tcp->fail_remove = true;
    installTcp(fake_tcp);
    MemoryOptions initial_options = options;
    initial_options.type = UNSPEC;
    const std::vector<void*> ranges{data.data(), data.data() + 64};
    ASSERT_TRUE(
        engine->registerLocalMemory(ranges, {64, 64}, initial_options).ok());
    registry->attempts.clear();
    registry->fail_put = true;
    trace.clear();

    const auto status = engine->unregisterLocalMemory(ranges, {64, 64});

    EXPECT_TRUE(status.IsMetadataError()) << status.ToString();
    EXPECT_TRUE(containsText(status, "injected put failure"));
    EXPECT_EQ(fake_tcp->removals, 2);
    EXPECT_EQ(hp->removals, 2);
    EXPECT_FALSE(hpRegistry().tracks(base(), 64));
    EXPECT_FALSE(hpRegistry().tracks(base() + 64, 64));
    ASSERT_EQ(registry->attempts.size(), 1u);
    EXPECT_TRUE(registry->attempts.back().getMemory().buffers.empty());
    EXPECT_EQ(trace.snapshot(),
              (std::vector<std::string>{"tcp-remove", "hp-remove", "tcp-remove",
                                        "hp-remove", "put"}));
}

TEST_F(RegistrationRollbackTest,
       RollbackDeregistrationPairsRegisteredByteMetric) {
#if TENT_METRICS_ENABLED
    ScopedMetrics metrics;
    ASSERT_TRUE(metrics.status.ok()) << metrics.status.ToString();
    const std::string hp_bytes =
        "tent_registered_buffer_bytes{transport=\"hp_tcp\"}";
    for (auto perm : {kGlobalReadWrite, kLocalReadWrite}) {
        SCOPED_TRACE(perm);
        options.perm = perm;
        const double charge =
            perm == kGlobalReadWrite ? static_cast<double>(data.size()) : 0;
        const auto before = prometheusSeries(TentMetrics::instance(), hp_bytes);
        const auto removals_before = hp->removals.load();
        ASSERT_TRUE(add(data.data(), data.size()).ok());
        const auto after_register =
            prometheusSeries(TentMetrics::instance(), hp_bytes);
        ASSERT_EQ(after_register - before, charge);

        Gate gate;
        auto batch = startFailure(gate, options);
        const auto unregistered =
            engine->unregisterLocalMemory(data.data(), data.size());
        EXPECT_EQ(hp->removals, removals_before);
        EXPECT_EQ(prometheusSeries(TentMetrics::instance(), hp_bytes),
                  after_register);
        gate.release();
        EXPECT_TRUE(unregistered.ok());
        ASSERT_TRUE(batch.get().IsInvalidArgument());
        EXPECT_EQ(prometheusSeries(TentMetrics::instance(), hp_bytes) -
                      after_register,
                  -charge);
        EXPECT_EQ(hp->removals, removals_before + 1);
        EXPECT_FALSE(hpRegistry().tracks(base(), data.size()));
        hp->before_add = nullptr;
    }
#else
    GTEST_SKIP() << "TENT metrics are disabled in this build";
#endif
}

TEST_F(RegistrationRollbackTest, HeldLeaseDoesNotBlockMetadataOrReplacement) {
    ASSERT_TRUE(add(data.data(), data.size()).ok());
    const auto old_id = id();
    HighPerformanceTcpBufferRegistry::Lease old_lease;
    HighPerformanceTcpStatus lease_failure = HighPerformanceTcpStatus::kOk;
    ASSERT_TRUE(hpRegistry()
                    .acquireRemoteLease(base(), data.size(), old_id,
                                        HighPerformanceTcpOpcode::kWrite,
                                        &old_lease, &lease_failure)
                    .ok());
    ASSERT_TRUE(old_lease);

    Gate add_gate;
    Gate remove_gate;
    hp->before_remove = [&remove_gate](const BufferDesc&) {
        remove_gate.arriveAndWait();
    };
    auto batch = startFailure(add_gate, options);
    ASSERT_TRUE(engine->unregisterLocalMemory(data.data(), data.size()).ok());
    add_gate.release();
    remove_gate.wait();

    EXPECT_TRUE(manager()
                    .updateLocal([](SegmentDesc& desc) -> Status {
                        desc.machine_id = "writer-ran-during-hp-cleanup";
                        return Status::OK();
                    })
                    .ok());
    EXPECT_TRUE(manager().getLocal()->getMemory().buffers.empty());
    EXPECT_TRUE(hpRegistry().tracks(base(), data.size()));
    EXPECT_TRUE(add(data.data(), data.size()).IsInvalidArgument());

    remove_gate.release();
    for (int i = 0; i < 1000 && hpRegistry().tracks(base(), data.size()); ++i) {
        std::this_thread::yield();
    }
    ASSERT_FALSE(hpRegistry().tracks(base(), data.size()));
    ASSERT_EQ(batch.wait_for(20ms), std::future_status::timeout);

    ASSERT_TRUE(add(data.data(), data.size()).ok());
    const auto replacement_id = id();
    EXPECT_NE(replacement_id, old_id);
    old_lease.reset();
    ASSERT_TRUE(batch.get().IsInvalidArgument());

    auto* replacement = manager().getLocal()->findBuffer(base(), data.size());
    ASSERT_NE(replacement, nullptr);
    HighPerformanceTcpBufferAttr replacement_attr;
    ASSERT_TRUE(DecodeHighPerformanceTcpBufferAttr(
                    replacement->transport_attrs.at(HP_TCP), &replacement_attr)
                    .ok());
    EXPECT_EQ(replacement_attr.registration_id, replacement_id);
    EXPECT_TRUE(hpRegistry().tracks(base(), data.size()));
    HighPerformanceTcpBufferRegistry::Lease lease;
    EXPECT_FALSE(hpRegistry()
                     .acquireRemoteLease(base(), data.size(), old_id,
                                         HighPerformanceTcpOpcode::kWrite,
                                         &lease, &lease_failure)
                     .ok());
    EXPECT_EQ(lease_failure, HighPerformanceTcpStatus::kStaleRegistration);
    EXPECT_TRUE(hpRegistry()
                    .acquireRemoteLease(base(), data.size(), replacement_id,
                                        HighPerformanceTcpOpcode::kWrite,
                                        &lease, &lease_failure)
                    .ok());
    EXPECT_TRUE(lease);
    EXPECT_EQ(registry->attempts.back().machine_id,
              "writer-ran-during-hp-cleanup");
    EXPECT_NE(registry->attempts.back().findBuffer(base(), data.size()),
              nullptr);
    hp->before_add = nullptr;
    hp->before_remove = nullptr;
}

}  // namespace
}  // namespace mooncake::tent
