// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0

#include <gtest/gtest.h>

#include <array>
#include <atomic>
#include <chrono>
#include <cstring>
#include <deque>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "tent/runtime/segment_manager.h"
#include "tent/runtime/segment_registry.h"
#include "tent/transport/ub/buffers.h"
#include "tent/transport/ub/context.h"
#include "tent/transport/ub/endpoint_store.h"
#include "tent/transport/ub/quota.h"
#include "tent/transport/ub/rail_monitor.h"
#include "tent/transport/ub/topology_attrs.h"
#include "tent/transport/ub/ub_transport.h"
#include "tent/transport/ub/workers.h"

namespace mooncake::tent::ub {
namespace {

class FakeContext final : public Context {
   public:
    explicit FakeContext(DeviceInfo info) : info_(std::move(info)) {}
    bool valid() const noexcept override { return valid_; }
    const DeviceInfo& deviceInfo() const noexcept override { return info_; }
    int asyncFd() const noexcept override { return -1; }
    void close() { valid_ = false; }

   private:
    DeviceInfo info_;
    bool valid_{true};
};

class FakeJfc final : public Jfc {
   public:
    bool valid() const noexcept override { return valid_; }
    int eventFd() const noexcept override { return -1; }

    void push(Completion completion) {
        std::lock_guard<std::mutex> lock(mutex_);
        completions_.push_back(completion);
    }
    void poll(size_t maximum, std::vector<Completion>& output) {
        std::lock_guard<std::mutex> lock(mutex_);
        while (!completions_.empty() && output.size() < maximum) {
            output.push_back(completions_.front());
            completions_.pop_front();
        }
    }
    void close() { valid_ = false; }

   private:
    std::mutex mutex_;
    std::deque<Completion> completions_;
    bool valid_{true};
};

SegmentDescriptor makeDescriptor(uint64_t address, uint64_t length) {
    return SegmentDescriptor{
        SegmentDescriptor::kSchemaVersion, 1, 16,
        std::to_string(address) + ":" + std::to_string(length)};
}

bool parseDescriptor(const SegmentDescriptor& descriptor, uint64_t& address,
                     uint64_t& length) {
    const auto colon = descriptor.hex.find(':');
    if (descriptor.schema_version != SegmentDescriptor::kSchemaVersion ||
        descriptor.urma_api_version != 1 || descriptor.urma_abi_size != 16 ||
        colon == std::string::npos) {
        return false;
    }
    try {
        address = std::stoull(descriptor.hex.substr(0, colon));
        length = std::stoull(descriptor.hex.substr(colon + 1));
        return length != 0;
    } catch (...) {
        return false;
    }
}

class FakeLocalSegment final : public LocalSegment {
   public:
    FakeLocalSegment(uint64_t address, uint64_t length)
        : address_(address),
          length_(length),
          descriptor_(makeDescriptor(address, length)) {}
    bool valid() const noexcept override { return valid_; }
    uint64_t address() const noexcept override { return address_; }
    uint64_t length() const noexcept override { return length_; }
    const SegmentDescriptor& descriptor() const noexcept override {
        return descriptor_;
    }
    void close() { valid_ = false; }

   private:
    uint64_t address_;
    uint64_t length_;
    SegmentDescriptor descriptor_;
    bool valid_{true};
};

class FakeRemoteSegment final : public RemoteSegment {
   public:
    FakeRemoteSegment(uint64_t address, uint64_t length,
                      SegmentDescriptor descriptor)
        : address_(address),
          length_(length),
          descriptor_(std::move(descriptor)) {}
    bool valid() const noexcept override { return valid_; }
    uint64_t address() const noexcept override { return address_; }
    uint64_t length() const noexcept override { return length_; }
    const SegmentDescriptor& descriptor() const noexcept override {
        return descriptor_;
    }
    void close() { valid_ = false; }

   private:
    uint64_t address_;
    uint64_t length_;
    SegmentDescriptor descriptor_;
    bool valid_{true};
};

class FakeJetty final : public Jetty {
   public:
    FakeJetty(uint32_t id, std::shared_ptr<FakeJfc> jfc)
        : id_(id), jfc_(std::move(jfc)) {}
    bool valid() const noexcept override { return valid_; }
    uint32_t id() const noexcept override { return id_; }
    uint32_t uasid() const noexcept override { return 0; }
    std::shared_ptr<FakeJfc> jfc() const { return jfc_; }
    bool bound() const { return bound_; }
    void bind() { bound_ = true; }
    void unbind() { bound_ = false; }
    void close() {
        valid_ = false;
        bound_ = false;
    }

   private:
    uint32_t id_;
    std::shared_ptr<FakeJfc> jfc_;
    bool valid_{true};
    bool bound_{false};
};

class FakeUrmaAdapter final : public UrmaAdapter {
   public:
    explicit FakeUrmaAdapter(DeviceInfo device) : device_(std::move(device)) {}

    bool available() const noexcept override { return true; }
    uint32_t nativeApiVersion() const noexcept override { return 1; }
    size_t nativeSegmentDescriptorSize() const noexcept override { return 16; }
    Status initialize() override {
        initialized_ = true;
        return Status::OK();
    }
    Status shutdown() override {
        initialized_ = false;
        return Status::OK();
    }
    Status discoverDevices(std::vector<DeviceInfo>& devices) override {
        if (!initialized_) return Status::InvalidArgument("not initialized");
        devices = {device_};
        return Status::OK();
    }
    Status openContext(const DeviceInfo& device, ContextPtr& context) override {
        if (!initialized_ || device.topology_name != device_.topology_name) {
            return Status::DeviceNotFound("fake device missing");
        }
        context = std::make_shared<FakeContext>(device);
        return Status::OK();
    }
    Status closeContext(ContextPtr& context) override {
        if (auto fake = std::dynamic_pointer_cast<FakeContext>(context)) {
            fake->close();
        }
        context.reset();
        return Status::OK();
    }
    Status createJfc(const ContextPtr&, const JfcOptions&,
                     JfcPtr& jfc) override {
        jfc = std::make_shared<FakeJfc>();
        return Status::OK();
    }
    Status deleteJfc(JfcPtr& jfc) override {
        if (auto fake = std::dynamic_pointer_cast<FakeJfc>(jfc)) fake->close();
        jfc.reset();
        return Status::OK();
    }
    Status registerLocalSegment(const ContextPtr&, uint64_t address,
                                size_t length, const SegmentOptions& options,
                                LocalSegmentPtr& segment) override {
        if (length == 0) return Status::InvalidArgument("empty segment");
        last_registered_access_.store(options.access,
                                      std::memory_order_release);
        segment = std::make_shared<FakeLocalSegment>(address, length);
        return Status::OK();
    }
    Status unregisterLocalSegment(LocalSegmentPtr& segment) override {
        if (auto fake = std::dynamic_pointer_cast<FakeLocalSegment>(segment)) {
            fake->close();
        }
        segment.reset();
        return Status::OK();
    }
    Status importRemoteSegment(const ContextPtr&,
                               const SegmentDescriptor& descriptor,
                               const SegmentOptions&,
                               RemoteSegmentPtr& segment) override {
        uint64_t address = 0;
        uint64_t length = 0;
        if (!parseDescriptor(descriptor, address, length)) {
            return Status::InvalidArgument("bad descriptor");
        }
        segment =
            std::make_shared<FakeRemoteSegment>(address, length, descriptor);
        return Status::OK();
    }
    Status unimportRemoteSegment(RemoteSegmentPtr& segment) override {
        if (auto fake = std::dynamic_pointer_cast<FakeRemoteSegment>(segment)) {
            fake->close();
        }
        segment.reset();
        return Status::OK();
    }
    Status createJetty(const ContextPtr&, const JfcPtr& jfc,
                       const JettyOptions&, JettyPtr& jetty) override {
        auto fake_jfc = std::dynamic_pointer_cast<FakeJfc>(jfc);
        if (!fake_jfc) return Status::InvalidArgument("bad JFC");
        jetty = std::make_shared<FakeJetty>(next_jetty_id_++, fake_jfc);
        return Status::OK();
    }
    Status deleteJetty(JettyPtr& jetty) override {
        if (auto fake = std::dynamic_pointer_cast<FakeJetty>(jetty)) {
            fake->close();
        }
        jetty.reset();
        return Status::OK();
    }
    Status bindJetty(const JettyPtr& jetty, const RemoteJettyInfo&) override {
        auto fake = std::dynamic_pointer_cast<FakeJetty>(jetty);
        if (!fake || !fake->valid())
            return Status::InvalidArgument("bad Jetty");
        fake->bind();
        return Status::OK();
    }
    Status unbindJetty(const JettyPtr& jetty) override {
        if (auto fake = std::dynamic_pointer_cast<FakeJetty>(jetty)) {
            fake->unbind();
        }
        return Status::OK();
    }
    Status resetJetty(const JettyPtr&) override { return Status::OK(); }
    Status quiesceJetty(const JettyPtr& jetty, uint32_t timeout_ms,
                        std::vector<Completion>& completions) override {
        completions.clear();
        auto fake = std::dynamic_pointer_cast<FakeJetty>(jetty);
        if (!fake || !fake->valid() || timeout_ms == 0) {
            return Status::InvalidArgument("bad Jetty quiesce");
        }
        if (fail_next_quiesce_.exchange(false, std::memory_order_acq_rel)) {
            return Status::RdmaError("injected quiesce failure");
        }
        std::lock_guard<std::mutex> lock(pending_mutex_);
        auto it = pending_.begin();
        while (it != pending_.end()) {
            if (it->jetty_id != fake->id()) {
                ++it;
                continue;
            }
            completions.push_back(Completion{CompletionCategory::ENDPOINT_ERROR,
                                             0, it->request.token, 0,
                                             fake->id()});
            it = pending_.erase(it);
        }
        quiesce_calls_.fetch_add(1, std::memory_order_relaxed);
        return Status::OK();
    }
    Status post(const JettyPtr& jetty, const std::vector<WorkRequest>& requests,
                size_t& posted_count) override {
        posted_count = 0;
        auto fake = std::dynamic_pointer_cast<FakeJetty>(jetty);
        if (!fake || !fake->valid() || !fake->bound()) {
            return Status::InvalidArgument("unbound Jetty");
        }
        for (const auto& request : requests) {
            if (!request.local_segment || !request.remote_segment ||
                request.token == 0 || request.length == 0) {
                return Status::InvalidArgument("bad WR");
            }
            const auto category = next_completion_.exchange(
                CompletionCategory::SUCCESS, std::memory_order_acq_rel);
            if (hold_next_completion_.exchange(false,
                                               std::memory_order_acq_rel)) {
                std::lock_guard<std::mutex> lock(pending_mutex_);
                pending_.push_back(Pending{fake->id(), request});
                ++posted_count;
                continue;
            }
            if (category == CompletionCategory::SUCCESS) {
                if (request.operation == Operation::WRITE) {
                    std::memcpy(
                        reinterpret_cast<void*>(request.remote_address),
                        reinterpret_cast<const void*>(request.local_address),
                        request.length);
                } else {
                    std::memcpy(
                        reinterpret_cast<void*>(request.local_address),
                        reinterpret_cast<const void*>(request.remote_address),
                        request.length);
                }
            }
            fake->jfc()->push(
                Completion{category, 0, request.token,
                           category == CompletionCategory::SUCCESS
                               ? static_cast<uint32_t>(request.length)
                               : 0,
                           fake->id()});
            ++posted_count;
        }
        return Status::OK();
    }
    Status poll(const JfcPtr& jfc, size_t maximum,
                std::vector<Completion>& completions) override {
        completions.clear();
        auto fake = std::dynamic_pointer_cast<FakeJfc>(jfc);
        if (!fake || !fake->valid()) return Status::InvalidArgument("bad JFC");
        fake->poll(maximum, completions);
        return Status::OK();
    }

    void failNextCompletion(CompletionCategory category) {
        next_completion_.store(category, std::memory_order_release);
    }
    void holdNextCompletion() {
        hold_next_completion_.store(true, std::memory_order_release);
    }
    void failNextQuiesce() {
        fail_next_quiesce_.store(true, std::memory_order_release);
    }
    size_t pendingCount() const {
        std::lock_guard<std::mutex> lock(pending_mutex_);
        return pending_.size();
    }
    uint64_t quiesceCalls() const {
        return quiesce_calls_.load(std::memory_order_relaxed);
    }
    uint32_t lastRegisteredAccess() const {
        return last_registered_access_.load(std::memory_order_acquire);
    }

   private:
    struct Pending {
        uint32_t jetty_id{0};
        WorkRequest request;
    };

    DeviceInfo device_;
    bool initialized_{false};
    uint32_t next_jetty_id_{1};
    std::atomic<CompletionCategory> next_completion_{
        CompletionCategory::SUCCESS};
    std::atomic<bool> hold_next_completion_{false};
    std::atomic<bool> fail_next_quiesce_{false};
    mutable std::mutex pending_mutex_;
    std::vector<Pending> pending_;
    std::atomic<uint64_t> quiesce_calls_{0};
    std::atomic<uint32_t> last_registered_access_{0};
};

class NullRegistry final : public SegmentRegistry {
   public:
    Status getSegmentDesc(SegmentDescRef&, const std::string&) override {
        return Status::InvalidEntry("not used");
    }
    Status putSegmentDesc(SegmentDescRef&) override { return Status::OK(); }
    Status deleteSegmentDesc(const std::string&) override {
        return Status::OK();
    }
};

DeviceInfo fakeDevice() {
    DeviceInfo info;
    info.topology_name = "ub:fake0:eid0";
    info.native_device_name = "fake0";
    info.native_device_path = "/fake/fake0";
    info.eid_index = 0;
    info.eid = "0001:0000:0000:0000:0000:0000:0000:0000";
    info.active = true;
    info.capabilities.max_jfc = 4;
    info.capabilities.max_jetty = 64;
    return info;
}

std::shared_ptr<Topology> fakeTopology(bool discovery_active = true) {
    auto topology = std::make_shared<Topology>();
    Topology::NicEntry nic{.name = "ub:fake0:eid0",
                           .pci_bus_id = "0000:00:00.0",
                           .type = Topology::NIC_UB,
                           .numa_node = 0};
    auto device = fakeDevice();
    device.active = discovery_active;
    encodeTopologyDeviceAttributes(device, 0, nic.device_attrs);
    topology->nic_list_.push_back(std::move(nic));
    Topology::MemEntry memory;
    memory.name = kWildcardLocation;
    memory.type = Topology::MEM_HOST;
    memory.numa_node = -1;
    memory.device_list[0].push_back(0);
    topology->mem_list_.push_back(std::move(memory));
    return topology;
}

TEST(UbNativeDataPathTest, LocalBuffersRequestProviderLocalOnlyAccess) {
    auto adapter = std::make_shared<FakeUrmaAdapter>(fakeDevice());
    ASSERT_TRUE(adapter->initialize().ok());
    auto context = std::make_shared<UbContext>(0, fakeDevice(), adapter);
    ASSERT_TRUE(context->initialize(1, JfcOptions{}).ok());
    UbBufferManager buffers(adapter, {context});

    std::array<char, 64> storage{};
    BufferDesc descriptor{};
    descriptor.addr = reinterpret_cast<uint64_t>(storage.data());
    descriptor.length = storage.size();
    descriptor.location = kWildcardLocation;
    MemoryOptions options;
    options.perm = kLocalReadWrite;

    ASSERT_TRUE(buffers.addBuffer(descriptor, options).ok());
    EXPECT_EQ(adapter->lastRegisteredAccess(), SEGMENT_ACCESS_LOCAL_ONLY);

    EXPECT_TRUE(buffers.clear().ok());
    EXPECT_TRUE(context->shutdown().ok());
    EXPECT_TRUE(adapter->shutdown().ok());
}

TEST(UbNativeDataPathTest,
     MissingCompletionIsFencedBeforeRetryAndEventuallyCompletes) {
    auto adapter = std::make_shared<FakeUrmaAdapter>(fakeDevice());
    ASSERT_TRUE(adapter->initialize().ok());
    auto context = std::make_shared<UbContext>(0, fakeDevice(), adapter);
    ASSERT_TRUE(context->initialize(1, JfcOptions{}).ok());
    std::vector<UbContextPtr> contexts{context};
    auto topology = fakeTopology();
    UbBufferManager buffers(adapter, contexts);

    std::array<char, 64> source{};
    std::array<char, 64> target{};
    for (size_t i = 0; i < source.size(); ++i) {
        source[i] = static_cast<char>(i + 1);
    }
    BufferDesc source_desc{};
    source_desc.addr = reinterpret_cast<uint64_t>(source.data());
    source_desc.length = source.size();
    source_desc.location = kWildcardLocation;
    BufferDesc target_desc{};
    target_desc.addr = reinterpret_cast<uint64_t>(target.data());
    target_desc.length = target.size();
    target_desc.location = kWildcardLocation;
    MemoryOptions options;
    options.perm = kGlobalReadWrite;
    ASSERT_TRUE(buffers.addBuffer(source_desc, options).ok());
    ASSERT_TRUE(buffers.addBuffer(target_desc, options).ok());

    SegmentManager manager(std::make_unique<NullRegistry>());
    ASSERT_TRUE(manager
                    .updateLocal([&](SegmentDesc& segment) {
                        segment.name = "local";
                        segment.type = SegmentType::Memory;
                        segment.detail = MemorySegmentDesc{};
                        auto& memory =
                            std::get<MemorySegmentDesc>(segment.detail);
                        memory.topology = *topology;
                        memory.buffers = {source_desc, target_desc};
                        return Status::OK();
                    })
                    .ok());

    EndpointStore endpoints(adapter, 16, 1);
    RailMonitor rails;
    QuotaManager quota;
    UbParams params;
    params.worker_count = 1;
    params.poller_count = 1;
    params.slice_size = 16;
    params.max_retries = 1;
    params.slice_timeout_ms = 20;

    EndpointResolver resolver = [&](const EndpointResolveRequest& request,
                                    std::shared_ptr<UbEndpoint>& endpoint) {
        UbEndpointKey key{request.local_context->topologyId(),
                          request.remote_segment_id, request.remote_topology_id,
                          "local@ub:fake0:eid0"};
        auto status =
            endpoints.getOrCreate(key, request.local_context, endpoint);
        if (!status.ok() || endpoint->ready()) return status;
        UbBootstrapDesc peer;
        peer.local_eid = fakeDevice().eid;
        peer.endpoint_generation = 100;
        peer.jetty_ids = {777};
        return endpoint->bind(peer);
    };
    UbWorkers workers(adapter, contexts, topology, &manager, &buffers, &rails,
                      &quota, params, std::move(resolver),
                      [&](const std::shared_ptr<UbEndpoint>& endpoint) {
                          (void)endpoints.retire(endpoint);
                      });
    ASSERT_TRUE(workers.start().ok());
    adapter->holdNextCompletion();

    Request request{};
    request.opcode = Request::WRITE;
    request.source = source.data();
    request.target_id = LOCAL_SEGMENT_ID;
    request.target_offset = reinterpret_cast<uint64_t>(target.data());
    request.length = source.size();
    auto task = UbTask::create(request);
    for (size_t offset = 0; offset < request.length; offset += 16) {
        ASSERT_NE(task->addSlice(UbSliceSpec{
                      source.data() + offset,
                      reinterpret_cast<uint64_t>(target.data() + offset), 16,
                      offset, 1}),
                  nullptr);
    }
    ASSERT_TRUE(task->seal());
    ASSERT_TRUE(workers.submit(task).ok());

    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (task->transferStatus().s == PENDING &&
           std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    EXPECT_EQ(task->transferStatus().s, COMPLETED);
    EXPECT_EQ(task->transferStatus().transferred_bytes, source.size());
    EXPECT_EQ(source, target);
    EXPECT_GT(rails.aggregateBandwidth(), 0.0);
    EXPECT_GE(rails.stats(UbPostPath{0, LOCAL_SEGMENT_ID, 0, 1}).timeouts, 1U);
    EXPECT_EQ(adapter->pendingCount(), 0U);
    EXPECT_GE(adapter->quiesceCalls(), 1U);

    EXPECT_TRUE(workers.stop().ok());
    EXPECT_TRUE(endpoints.clear().ok());
    EXPECT_TRUE(buffers.clear().ok());
    EXPECT_TRUE(context->shutdown().ok());
    EXPECT_TRUE(adapter->shutdown().ok());
}

TEST(UbNativeDataPathTest, EndpointStoreNeverReusesRetiredGeneration) {
    auto adapter = std::make_shared<FakeUrmaAdapter>(fakeDevice());
    ASSERT_TRUE(adapter->initialize().ok());
    auto context = std::make_shared<UbContext>(0, fakeDevice(), adapter);
    ASSERT_TRUE(context->initialize(1, JfcOptions{}).ok());
    EndpointStore store(adapter, 2, 1);
    UbEndpointKey key{0, 9, 0, "peer@ub:fake0:eid0"};
    std::array<std::shared_ptr<UbEndpoint>, 8> concurrent;
    std::array<Status, 8> statuses;
    std::vector<std::thread> threads;
    for (size_t i = 0; i < concurrent.size(); ++i) {
        threads.emplace_back([&, i] {
            statuses[i] = store.getOrCreate(key, context, concurrent[i]);
        });
    }
    for (auto& thread : threads) thread.join();
    for (size_t i = 0; i < concurrent.size(); ++i) {
        ASSERT_TRUE(statuses[i].ok());
        EXPECT_EQ(concurrent[i], concurrent[0]);
    }
    auto first = concurrent[0];
    const uint64_t old_generation = first->generation();
    EXPECT_TRUE(store.retire(key, old_generation));
    std::shared_ptr<UbEndpoint> replacement;
    ASSERT_TRUE(store.getOrCreate(key, context, replacement).ok());
    EXPECT_GT(replacement->generation(), old_generation);
    EXPECT_FALSE(store.retire(key, old_generation));
    EXPECT_EQ(store.get(key), replacement);
    EXPECT_TRUE(store.clear().ok());
    EXPECT_TRUE(context->shutdown().ok());
    EXPECT_TRUE(adapter->shutdown().ok());
}

TEST(UbNativeDataPathTest, UbTransportRunsSelfReadWriteOverNativeControlPlane) {
    auto adapter = std::make_shared<FakeUrmaAdapter>(fakeDevice());
    // A serialized discovery snapshot is informational. Current local
    // discovery and runtime path health remain the scheduling authorities.
    auto topology = fakeTopology(false);
    auto control = std::make_shared<ControlService>("p2p", "", nullptr);
    uint16_t port = 0;
    ASSERT_TRUE(control->start(port).ok());
    const std::string segment_name =
        "127.0.0.1:" + std::to_string(static_cast<unsigned>(port));
    ASSERT_TRUE(control->segmentManager()
                    .updateLocal([&](SegmentDesc& segment) {
                        segment.name = segment_name;
                        segment.rpc_server_addr = segment_name;
                        segment.machine_id = "fake-machine";
                        segment.type = SegmentType::Memory;
                        segment.detail = MemorySegmentDesc{};
                        std::get<MemorySegmentDesc>(segment.detail).topology =
                            *topology;
                        return Status::OK();
                    })
                    .ok());

    auto config = std::make_shared<Config>();
    config->set("transports/ub/enable", true);
    config->set("transports/ub/worker_count", 1);
    config->set("transports/ub/poller_count", 1);
    config->set("transports/ub/jfc_per_context", 1);
    config->set("transports/ub/jetty_per_endpoint", 1);
    config->set("transports/ub/max_endpoints", 16);
    config->set("transports/ub/slice_size", 16);
    config->set("transports/ub/max_retries", 1);
    config->set("transports/ub/slice_timeout_ms", 1000);
    config->set("transports/ub/endpoint_cooldown_ms", 100);

    UbTransport transport(adapter);
    std::string mutable_segment_name = segment_name;
    ASSERT_TRUE(
        transport.install(mutable_segment_name, control, topology, config)
            .ok());
    EXPECT_STREQ(transport.getName(), "ub");
    EXPECT_TRUE(transport.supportsCancellation());
    EXPECT_FALSE(transport.supportNotification());
    EXPECT_TRUE(transport.capabilities().dram_to_dram);

    std::array<char, 64> source{};
    std::array<char, 64> target{};
    for (size_t i = 0; i < source.size(); ++i) {
        source[i] = static_cast<char>(100 - i);
    }
    const auto expected = source;
    BufferDesc source_desc{};
    source_desc.addr = reinterpret_cast<uint64_t>(source.data());
    source_desc.length = source.size();
    source_desc.location = kWildcardLocation;
    BufferDesc target_desc{};
    target_desc.addr = reinterpret_cast<uint64_t>(target.data());
    target_desc.length = target.size();
    target_desc.location = kWildcardLocation;
    MemoryOptions options;
    options.perm = kGlobalReadWrite;
    std::vector<BufferDesc> descriptors{source_desc, target_desc};
    ASSERT_TRUE(transport.addMemoryBuffer(descriptors, options).ok());
    ASSERT_TRUE(control->segmentManager()
                    .updateLocal([&](SegmentDesc& segment) {
                        std::get<MemorySegmentDesc>(segment.detail).buffers =
                            descriptors;
                        return Status::OK();
                    })
                    .ok());

    Transport::SubBatchRef batch = nullptr;
    ASSERT_TRUE(transport.allocateSubBatch(batch, 2).ok());
    Request request{};
    request.opcode = Request::WRITE;
    request.source = source.data();
    request.target_id = LOCAL_SEGMENT_ID;
    request.target_offset = reinterpret_cast<uint64_t>(target.data());
    request.length = source.size();
    ASSERT_TRUE(transport.submitTransferTasks(batch, {request}).ok());

    TransferStatus transfer{PENDING, 0};
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (transfer.s == PENDING &&
           std::chrono::steady_clock::now() < deadline) {
        ASSERT_TRUE(transport.getTransferStatus(batch, 0, transfer).ok());
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    EXPECT_EQ(transfer.s, COMPLETED);
    EXPECT_EQ(transfer.transferred_bytes, source.size());
    EXPECT_EQ(source, target);
    EXPECT_GT(transport.getEstimatedBandwidth(), 0.0);

    source.fill(0);
    Request read_request = request;
    read_request.opcode = Request::READ;
    ASSERT_TRUE(transport.submitTransferTasks(batch, {read_request}).ok());
    transfer = TransferStatus{PENDING, 0};
    const auto read_deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (transfer.s == PENDING &&
           std::chrono::steady_clock::now() < read_deadline) {
        ASSERT_TRUE(transport.getTransferStatus(batch, 1, transfer).ok());
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    EXPECT_EQ(transfer.s, COMPLETED);
    EXPECT_EQ(transfer.transferred_bytes, source.size());
    EXPECT_EQ(source, expected);

    ASSERT_TRUE(transport.freeSubBatch(batch).ok());
    EXPECT_EQ(batch, nullptr);

    // A failed device fence must make uninstall retryable without destroying
    // pollers, registered memory, or the held WR token.
    Transport::SubBatchRef draining_batch = nullptr;
    ASSERT_TRUE(transport.allocateSubBatch(draining_batch, 1).ok());
    adapter->holdNextCompletion();
    ASSERT_TRUE(transport.submitTransferTasks(draining_batch, {request}).ok());
    const auto posted_deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(1);
    while (adapter->pendingCount() == 0 &&
           std::chrono::steady_clock::now() < posted_deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    ASSERT_EQ(adapter->pendingCount(), 1U);
    adapter->failNextQuiesce();
    EXPECT_FALSE(transport.uninstall().ok());
    EXPECT_EQ(adapter->pendingCount(), 1U);
    EXPECT_TRUE(transport.uninstall().ok());
    EXPECT_EQ(adapter->pendingCount(), 0U);
    EXPECT_TRUE(transport.freeSubBatch(draining_batch).ok());
    EXPECT_TRUE(transport.uninstall().ok());
}

}  // namespace
}  // namespace mooncake::tent::ub
