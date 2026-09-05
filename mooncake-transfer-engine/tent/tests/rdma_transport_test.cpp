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

#include <cerrno>
#include <dlfcn.h>
#include <gtest/gtest.h>
#include <infiniband/verbs.h>
#include <sys/wait.h>
#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "tent/common/config.h"
#include "tent/common/types.h"
#include "tent/transfer_engine.h"
#include "tent/runtime/topology.h"
#include "tent/transport/rdma/context.h"
#include "tent/transport/rdma/params.h"
#include "tent/transport/rdma/quota.h"
#include "tent/transport/rdma/rdma_transport.h"
#include "tent/transport/rdma/ibv_loader.h"
#include "tent/transport/rdma/workers.h"

namespace mooncake {
namespace tent {

// Friend accessor for driving initializeContexts() without a full install().
class RdmaTransportTestPeer {
   public:
    static void bindTopology(RdmaTransport& transport,
                             std::shared_ptr<Topology> topology) {
        transport.local_topology_ = topology;
        transport.local_buffer_manager_.setTopology(topology);
        transport.params_ = std::make_shared<RdmaParams>();
        transport.conf_ = std::make_shared<Config>();
    }

    // Runs the monitorThread() 1 Hz reclaim tick without starting any worker
    // threads.
    static void reclaimEndpoints(RdmaTransport& transport) {
        Workers workers(&transport);
        workers.reclaimEndpoints();
    }

    static size_t initializeContexts(RdmaTransport& transport) {
        return transport.initializeContexts();
    }

    // Constructs Workers (which seeds the DeviceSelector from context_set_)
    // without starting any threads.
    static std::unique_ptr<Workers> makeWorkers(RdmaTransport& transport) {
        return std::make_unique<Workers>(&transport);
    }

    // Drives the decision half of handleContextEvents with a synthesized
    // event, bypassing ibv_get_async_event/ibv_ack_async_event.
    static void applyContextEvent(Workers& workers, int dev_id,
                                  RdmaContext& context,
                                  const ibv_async_event& event) {
        workers.applyContextEvent(dev_id, context, event);
    }

    // Runs the monitorThread() 1 Hz safety net for contexts whose
    // IBV_EVENT_PORT_ACTIVE never arrived, without starting any threads.
    static void resumePausedContexts(Workers& workers) {
        workers.resumePausedContexts();
    }

    static const RdmaContextSet& contextSet(const RdmaTransport& transport) {
        return transport.context_set_;
    }

    static void setNumWorkers(RdmaTransport& transport, int num_workers) {
        transport.params_->workers.num_workers = num_workers;
    }

    static void addInflight(Workers& workers, size_t worker_id, int64_t delta) {
        workers.worker_context_[worker_id].inflight_slices.fetch_add(delta);
    }

    static void setGpuToGpu(RdmaTransport& transport, bool enabled) {
        transport.caps.gpu_to_gpu = enabled;
    }

    static void setFlushGpuDirectWrites(RdmaTransport& transport,
                                        bool enabled) {
        if (!transport.conf_) {
            transport.conf_ = std::make_shared<Config>();
        }
        transport.conf_->set("transports/rdma/flush_gpu_direct_rdma_writes",
                             enabled);
    }

    using NotifyAction = RdmaTransport::NotifyCompletionAction;

    static NotifyAction classifyNotifyCompletion(ibv_wc_status status,
                                                 bool endpoint_alive,
                                                 bool endpoint_ready) {
        return RdmaTransport::classifyNotifyCompletion(status, endpoint_alive,
                                                       endpoint_ready);
    }
};

// Friend accessor for RdmaContext: TENT reaches libibverbs through a table of
// function pointers the context copies from IbvLoader, so a test can replace
// individual entries and hand the context a placeholder device instead of
// needing an RNIC.
class RdmaContextTestPeer {
   public:
    static IbvSymbols& verbs(RdmaContext& context) { return context.verbs_; }

    // Make the context look opened on `native` (never dereferenced by the
    // port-attribute paths, only passed back to the verbs) with `params`.
    static void bindDevice(RdmaContext& context, ibv_context* native,
                           std::shared_ptr<RdmaParams> params) {
        context.native_context_ = native;
        context.params_ = std::move(params);
    }

    static void unbindDevice(RdmaContext& context) {
        context.native_context_ = nullptr;
    }
};

namespace {

bool hasRdmaDevice() {
    int count = 0;
    ibv_device** devices = ibv_get_device_list(&count);
    const bool available = devices != nullptr && count > 0;
    if (devices) ibv_free_device_list(devices);
    return available;
}

class ChildProcessGuard {
   public:
    ChildProcessGuard(pid_t pid, int stop_fd) : pid_(pid), stop_fd_(stop_fd) {}

    ~ChildProcessGuard() {
        if (pid_ <= 0) return;
        close(stop_fd_);
        (void)waitpid(pid_, nullptr, 0);
    }

    int finish() {
        close(stop_fd_);
        int status = 0;
        (void)waitpid(pid_, &status, 0);
        pid_ = -1;
        stop_fd_ = -1;
        return status;
    }

    int reap() {
        int status = 0;
        (void)waitpid(pid_, &status, 0);
        close(stop_fd_);
        pid_ = -1;
        stop_fd_ = -1;
        return status;
    }

   private:
    pid_t pid_;
    int stop_fd_;
};

std::shared_ptr<Config> makeRdmaConfig() {
    auto config = std::make_shared<Config>();
    config->set("metadata_type", "p2p");
    config->set("metadata_servers", "P2PHANDSHAKE");
    config->set("transports/rdma/enable", true);
    config->set("transports/rdma/num_lanes", 1);
    config->set("transports/rdma/endpoint/max_qp_wr", 1);
    config->set("transports/tcp/enable", false);
    config->set("transports/shm/enable", false);
    return config;
}

std::shared_ptr<Topology> topologyWithRdmaNics(size_t count) {
    auto topology = std::make_shared<Topology>();
    for (size_t i = 0; i < count; ++i) {
        Topology::NicEntry nic;
        nic.name = "mlx5_" + std::to_string(i);
        nic.type = Topology::NIC_RDMA;
        nic.numa_node = 0;
        topology->nic_list_.push_back(std::move(nic));
    }

    Topology::MemEntry memory;
    memory.name = "cpu:0";
    memory.type = Topology::MEM_HOST;
    memory.numa_node = 0;
    for (size_t i = 0; i < count; ++i) {
        memory.device_list[0].push_back(static_cast<int>(i));
    }
    topology->mem_list_.push_back(std::move(memory));
    return topology;
}

bool waitBatchDone(TransferEngine& engine, BatchID batch) {
    TransferStatus status;
    for (int i = 0; i < 10000; ++i) {
        auto result = engine.getTransferStatus(batch, status);
        if (!result.ok() || status.s == TransferStatusEnum::FAILED)
            return false;
        if (status.s == TransferStatusEnum::COMPLETED) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    return false;
}

TEST(RdmaParamsTest, DefaultsKeepLaneCountsAligned) {
    RdmaParams params;

    EXPECT_EQ(params.num_lanes, 6);
    EXPECT_EQ(params.device.num_cq_list, params.num_lanes);
    EXPECT_EQ(params.endpoint.qp_mul_factor, params.num_lanes);
    EXPECT_EQ(params.workers.num_workers, params.num_lanes);
    EXPECT_EQ(params.endpoint.path_mtu, IBV_MTU_4096);
}

TEST(RdmaSubBatchTest, ReportsTaskCount) {
    RdmaSubBatch batch;
    batch.max_size = 8;

    EXPECT_EQ(batch.size(), 0);
    batch.task_list.push_back(nullptr);
    batch.task_list.push_back(nullptr);
    EXPECT_EQ(batch.size(), 2);
}

TEST(DeviceSelectorTest, PerSliceAllocationHonorsPolicy) {
    auto topology = topologyWithRdmaNics(3);

    {
        DeviceSelector selector;
        ASSERT_TRUE(selector.loadTopology(topology).ok());
        int chosen_device = -1;
        ASSERT_TRUE(
            selector
                .allocate(4096, "cpu:0", chosen_device, PRIO_HIGH, 1ULL << 1)
                .ok());
        EXPECT_EQ(chosen_device, 1);
    }

    {
        DeviceSelector selector;
        ASSERT_TRUE(selector.loadTopology(topology).ok());
        auto params = selector.getSchedulingParams();
        params.device_base_priorities = {0, 1, 2};
        params.local_rotation_interval_us = 0;
        params.numa_tier_weights[0] = 1.0;
        params.numa_tier_weights[1] = 1.0;
        params.numa_tier_weights[2] = 1.0;
        params.score_jitter_range = 0.0;
        selector.setSchedulingParams(params);

        int chosen_device = -1;
        ASSERT_TRUE(selector
                        .allocate(4096, "cpu:0", chosen_device, PRIO_LOW,
                                  (1ULL << 1) | (1ULL << 2))
                        .ok());
        EXPECT_EQ(chosen_device, 2);
    }
}

// Retiring an endpoint moves its data QPs to ERR and flushes in-flight
// transfers, so a notify completion error may only do that when it can mean the
// peer or the path is gone. The notify QP owns its buffers, MRs and CQ, so a
// local WQE/MR fault says nothing about the data path.
TEST(RdmaNotifyFaultTriageTest, LocalNotifyFaultsKeepTheDataPath) {
    using Action = RdmaTransportTestPeer::NotifyAction;
    const auto classify = &RdmaTransportTestPeer::classifyNotifyCompletion;

    EXPECT_EQ(classify(IBV_WC_LOC_LEN_ERR, true, true),
              Action::DisableNotification);
    EXPECT_EQ(classify(IBV_WC_LOC_QP_OP_ERR, true, true),
              Action::DisableNotification);
    EXPECT_EQ(classify(IBV_WC_LOC_PROT_ERR, true, true),
              Action::DisableNotification);
    EXPECT_EQ(classify(IBV_WC_LOC_ACCESS_ERR, true, true),
              Action::DisableNotification);
    EXPECT_EQ(classify(IBV_WC_MW_BIND_ERR, true, true),
              Action::DisableNotification);
}

TEST(RdmaNotifyFaultTriageTest, PathAndPeerFaultsRetireTheEndpoint) {
    using Action = RdmaTransportTestPeer::NotifyAction;
    const auto classify = &RdmaTransportTestPeer::classifyNotifyCompletion;

    // A restarted peer surfaces here, and the endpoint has to be retired for
    // the next getEndpoint() to rebuild it with a live notify QP.
    EXPECT_EQ(classify(IBV_WC_RETRY_EXC_ERR, true, true),
              Action::RetireEndpoint);
    EXPECT_EQ(classify(IBV_WC_REM_OP_ERR, true, true), Action::RetireEndpoint);
    EXPECT_EQ(classify(IBV_WC_REM_ACCESS_ERR, true, true),
              Action::RetireEndpoint);
    EXPECT_EQ(classify(IBV_WC_REM_INV_REQ_ERR, true, true),
              Action::RetireEndpoint);
    EXPECT_EQ(classify(IBV_WC_FATAL_ERR, true, true), Action::RetireEndpoint);
    // Nothing put this QP in ERR on our side, so the peer or the path did.
    EXPECT_EQ(classify(IBV_WC_WR_FLUSH_ERR, true, true),
              Action::RetireEndpoint);
}

TEST(RdmaNotifyFaultTriageTest, TeardownFlushesStayQuiet) {
    using Action = RdmaTransportTestPeer::NotifyAction;
    const auto classify = &RdmaTransportTestPeer::classifyNotifyCompletion;

    // Every WR still posted on a retiring endpoint's notify QP flushes.
    // Reporting each one floods the log and re-triggers teardown.
    EXPECT_EQ(classify(IBV_WC_WR_FLUSH_ERR, true, false), Action::SkipSilently);
    EXPECT_EQ(classify(IBV_WC_WR_FLUSH_ERR, false, false),
              Action::SkipSilently);
    // A real fault surfacing after the endpoint is gone has nothing left to
    // act on.
    EXPECT_EQ(classify(IBV_WC_RETRY_EXC_ERR, false, false), Action::ReportOnly);
}

// context_set_ is subscripted by NicID, so it must keep one slot per NIC even
// when a device is skipped. It used to push_back only on the success path,
// compacting the array so a later dev_id named the wrong RNIC or ran off it.
void expectInertContextPerNic(const RdmaContextSet& contexts, size_t expected) {
    ASSERT_EQ(contexts.size(), expected);
    for (size_t i = 0; i < contexts.size(); ++i) {
        ASSERT_NE(contexts[i], nullptr) << "slot " << i << " must be occupied";
        EXPECT_NE(contexts[i]->status(), RdmaContext::DEVICE_ENABLED)
            << "slot " << i << " must not report itself usable";
        // Inert contexts must stay safe for the whole-list consumers.
        EXPECT_EQ(contexts[i]->cq(0), nullptr);
        EXPECT_EQ(contexts[i]->notifyCq(), nullptr);
        EXPECT_LT(contexts[i]->eventFd(), 0);
        EXPECT_EQ(contexts[i]->nativeContext(), nullptr);
        // construct() sets device_name_ before it can fail, so an empty name
        // is what tells a fresh placeholder from a kept half-built context.
        EXPECT_TRUE(contexts[i]->name().empty())
            << "slot " << i << " kept a half-built context for "
            << contexts[i]->name();
    }
}

// Non-RDMA entries are skipped before construct() is ever called, so this runs
// without libibverbs devices.
TEST(RdmaNicIndexAlignmentTest, ContextSetKeepsOneSlotPerNonRdmaNic) {
    auto topology = std::make_shared<Topology>();
    ASSERT_TRUE(topology
                    ->parse(R"({"nics":[
                        {"name":"mc-tcp-0","type":1,"numa_node":0},
                        {"name":"mc-unknown-1","type":2,"numa_node":0},
                        {"name":"mc-tcp-2","type":1,"numa_node":0}]})")
                    .ok());
    ASSERT_EQ(topology->getNicCount(), static_cast<size_t>(3));

    RdmaTransport transport;
    RdmaTransportTestPeer::bindTopology(transport, topology);

    EXPECT_EQ(RdmaTransportTestPeer::initializeContexts(transport),
              static_cast<size_t>(0));
    expectInertContextPerNic(RdmaTransportTestPeer::contextSet(transport),
                             topology->getNicCount());
}

// monitorThread()'s 1 Hz tick walks every slot. An inert context never built
// an endpoint store, so an unguarded endpointStore()->reclaim() would crash --
// and only on the first heartbeat, well after startup.
TEST(RdmaNicIndexAlignmentTest, ReclaimTickSkipsInertContexts) {
    auto topology = std::make_shared<Topology>();
    ASSERT_TRUE(topology
                    ->parse(R"({"nics":[
                        {"name":"mc-tcp-0","type":1,"numa_node":0},
                        {"name":"mc-unknown-1","type":2,"numa_node":0}]})")
                    .ok());

    RdmaTransport transport;
    RdmaTransportTestPeer::bindTopology(transport, topology);
    ASSERT_EQ(RdmaTransportTestPeer::initializeContexts(transport),
              static_cast<size_t>(0));

    // Precondition that makes the unguarded call fatal.
    for (const auto& context : RdmaTransportTestPeer::contextSet(transport)) {
        ASSERT_EQ(context->endpointStore(), nullptr);
    }

    RdmaTransportTestPeer::reclaimEndpoints(transport);
}

// The construct()-failure branch. Runs on any host: this binary links
// libibverbs directly, so IbvLoader's dlclose does not unmap it.
TEST(RdmaNicIndexAlignmentTest, ContextSetKeepsOneSlotWhenConstructFails) {
    // Device names that resolve to no real RNIC, so construct() fails on any
    // host, with a non-RDMA entry in the middle to offset the indexes.
    auto topology = std::make_shared<Topology>();
    ASSERT_TRUE(topology
                    ->parse(R"({"nics":[
                        {"name":"mc-absent-rnic-0","type":0,"numa_node":0},
                        {"name":"mc-tcp-1","type":1,"numa_node":0},
                        {"name":"mc-absent-rnic-2","type":0,"numa_node":0}]})")
                    .ok());
    ASSERT_EQ(topology->getNicCount(), static_cast<size_t>(3));

    RdmaTransport transport;
    RdmaTransportTestPeer::bindTopology(transport, topology);

    EXPECT_EQ(RdmaTransportTestPeer::initializeContexts(transport),
              static_cast<size_t>(0));
    expectInertContextPerNic(RdmaTransportTestPeer::contextSet(transport),
                             topology->getNicCount());
}

// A NIC whose construct() failed is still an RDMA entry in the topology, so
// loadTopology() gives it a DeviceSelector slot. Workers must mark it
// unavailable: it can carry no traffic, so it must be neither a selection
// candidate nor part of the aggregate bandwidth the admission queue reads --
// the configured default speed is for usable NICs only.
TEST(RdmaNicIndexAlignmentTest, FailedContextIsUnavailableToSelector) {
    auto topology = std::make_shared<Topology>();
    ASSERT_TRUE(topology
                    ->parse(R"({"nics":[
                        {"name":"mc-tcp-0","type":1,"numa_node":0},
                        {"name":"mc-absent-rnic-1","type":0,"numa_node":0}]})")
                    .ok());

    RdmaTransport transport;
    RdmaTransportTestPeer::bindTopology(transport, topology);
    ASSERT_EQ(RdmaTransportTestPeer::initializeContexts(transport),
              static_cast<size_t>(0));

    auto workers = RdmaTransportTestPeer::makeWorkers(transport);
    auto* selector = workers->getDeviceSelector();
    ASSERT_NE(selector, nullptr);
    EXPECT_FALSE(selector->isDeviceAvailable(1));
    // Nothing usable: no bandwidth to predict with, rather than 400G of it.
    EXPECT_LT(selector->getAggregateEwmaBandwidth(), 0.0);
}

// refreshPortAttributes() is called from the monitor thread on port events.
// On a slot that never opened a device it must fail cleanly and leave the
// (zero) speed alone rather than touch a null ibv_context.
// Port events carry the port they concern, and a device's async fd delivers
// events for every port of that device. A context opens exactly one port, so
// an event for another port must not touch its availability. These run on an
// inert context: pause()/resume() are no-ops there and the decision under
// test is the selector flip.
class RdmaContextEventTest : public ::testing::Test {
   protected:
    void SetUp() override {
        topology_ = std::make_shared<Topology>();
        ASSERT_TRUE(topology_
                        ->parse(R"({"nics":[
                            {"name":"mc-tcp-0","type":1,"numa_node":0},
                            {"name":"mc-absent-rnic-1","type":0,"numa_node":0}]})")
                        .ok());
        RdmaTransportTestPeer::bindTopology(transport_, topology_);
        ASSERT_EQ(RdmaTransportTestPeer::initializeContexts(transport_), 0u);
        workers_ = RdmaTransportTestPeer::makeWorkers(transport_);
        selector_ = workers_->getDeviceSelector();
        ASSERT_NE(selector_, nullptr);
        // Init marked the failed context unavailable; pretend it recovered so
        // the flips below are observable.
        ASSERT_TRUE(selector_->setDeviceAvailable(kDev, true).ok());
    }

    RdmaContext& context() {
        return *RdmaTransportTestPeer::contextSet(transport_)[kDev];
    }

    void fire(ibv_event_type type, int port_num) {
        ibv_async_event event{};
        event.event_type = type;
        event.element.port_num = port_num;
        RdmaTransportTestPeer::applyContextEvent(*workers_, kDev, context(),
                                                 event);
    }

    int ourPort() { return context().portNum(); }
    int otherPort() { return ourPort() + 1; }

    static constexpr int kDev = 1;
    std::shared_ptr<Topology> topology_;
    RdmaTransport transport_;
    std::unique_ptr<Workers> workers_;
    DeviceSelector* selector_ = nullptr;
};

TEST_F(RdmaContextEventTest, PortErrAndPortActiveFlipAvailability) {
    fire(IBV_EVENT_PORT_ERR, ourPort());
    EXPECT_FALSE(selector_->isDeviceAvailable(kDev));
    EXPECT_LT(selector_->getAggregateEwmaBandwidth(), 0.0);
    fire(IBV_EVENT_PORT_ACTIVE, ourPort());
    EXPECT_TRUE(selector_->isDeviceAvailable(kDev));
    EXPECT_GT(selector_->getAggregateEwmaBandwidth(), 0.0);
}

TEST_F(RdmaContextEventTest, EventsForAnotherPortAreIgnored) {
    fire(IBV_EVENT_PORT_ERR, otherPort());
    EXPECT_TRUE(selector_->isDeviceAvailable(kDev));

    fire(IBV_EVENT_PORT_ERR, ourPort());
    ASSERT_FALSE(selector_->isDeviceAvailable(kDev));
    fire(IBV_EVENT_PORT_ACTIVE, otherPort());
    EXPECT_FALSE(selector_->isDeviceAvailable(kDev));
}

TEST_F(RdmaContextEventTest, DeviceFatalMarksUnavailableRegardlessOfPort) {
    fire(IBV_EVENT_DEVICE_FATAL, otherPort());  // device-scoped: no port
    EXPECT_FALSE(selector_->isDeviceAvailable(kDev));
}

TEST_F(RdmaContextEventTest, CqErrLeavesAvailabilityAlone) {
    fire(IBV_EVENT_CQ_ERR, ourPort());
    EXPECT_TRUE(selector_->isDeviceAvailable(kDev));
}

// ibv_query_port_speed() exists only in rdma-core >= 62. It must be resolved
// as an optional symbol: present -> non-null, absent -> null, and either way
// the mandatory verbs are still there (an older libibverbs must not lose
// RDMA over it). Compared against a direct dlsym so the expectation is
// whatever this host's library actually has.
TEST(RdmaContextPortSpeedTest, EffectiveSpeedVerbIsOptional) {
    void* lib = dlopen("libibverbs.so.1", RTLD_NOW | RTLD_LOCAL);
    if (!lib) GTEST_SKIP() << "libibverbs.so.1 not loadable";
    const bool host_has_verb = dlsym(lib, "ibv_query_port_speed") != nullptr;
    dlclose(lib);

    const auto& sym = IbvLoader::Instance().sym();
    EXPECT_EQ(sym.ibv_query_port_speed != nullptr, host_has_verb);
    // Mandatory symbols resolve regardless of the optional one.
    EXPECT_NE(sym.ibv_query_port_default, nullptr);
    EXPECT_NE(sym.ibv_open_device, nullptr);
}

// Verbs stand-ins wired through RdmaContextTestPeer::verbs(). Plain function
// pointers, so state lives in one static block.
struct FakePortVerbs {
    ibv_context native{};      // placeholder handle, never dereferenced
    uint8_t active_speed = 0;  // what ibv_query_port reports
    uint8_t active_width = 0;
    int query_port_rc = 0;
    uint64_t speed_100mbps = 0;  // what ibv_query_port_speed reports
    int query_speed_rc = 0;
    int query_speed_calls = 0;
};
FakePortVerbs fake_port;

int fakeQueryPort(ibv_context* context, uint8_t, ibv_port_attr* attr) {
    if (context != &fake_port.native) return EINVAL;
    if (fake_port.query_port_rc) return fake_port.query_port_rc;
    *attr = {};
    attr->state = IBV_PORT_ACTIVE;
    attr->active_speed = fake_port.active_speed;
    attr->active_width = fake_port.active_width;
    return 0;
}

int fakeQueryPortSpeed(ibv_context* context, uint32_t, uint64_t* speed) {
    ++fake_port.query_speed_calls;
    if (context != &fake_port.native) return EINVAL;
    if (fake_port.query_speed_rc) return fake_port.query_speed_rc;
    *speed = fake_port.speed_100mbps;
    return 0;
}

// A context whose port-attribute verbs are the fakes above, "opened" on the
// placeholder device. Exercises refreshPortAttributes()/linkSpeedGbps()
// exactly as the monitor thread does, without an RNIC.
class RdmaContextFakeVerbsTest : public ::testing::Test {
   protected:
    void SetUp() override {
        fake_port = FakePortVerbs{};
        fake_port.active_speed = 128;  // NDR
        fake_port.active_width = 2;    // 4x -> 400 Gbps encoded
        context_ = std::make_unique<RdmaContext>(transport_);
        auto& verbs = RdmaContextTestPeer::verbs(*context_);
        verbs.ibv_query_port_default = fakeQueryPort;
        verbs.ibv_query_port_speed = fakeQueryPortSpeed;
        RdmaContextTestPeer::bindDevice(*context_, &fake_port.native,
                                        std::make_shared<RdmaParams>());
    }

    void TearDown() override {
        // The context never owned the placeholder; keep its destructor away
        // from it.
        RdmaContextTestPeer::unbindDevice(*context_);
    }

    RdmaTransport transport_;
    std::unique_ptr<RdmaContext> context_;
};

TEST_F(RdmaContextFakeVerbsTest, EffectiveSpeedPreferredWhenVerbReportsIt) {
    fake_port.speed_100mbps = 2000;  // LAG down to one 200G PF
    ASSERT_EQ(context_->refreshPortAttributes(), 0);
    EXPECT_DOUBLE_EQ(context_->linkSpeedGbps(), 200.0);
    EXPECT_EQ(fake_port.query_speed_calls, 1);
}

// A transient verb failure must not revert a degraded LAG to the higher
// encoded rate: the last known effective speed is held until a query
// succeeds again (a real recovery re-fires PORT_ACTIVE / SPEED_CHANGE).
TEST_F(RdmaContextFakeVerbsTest, EffectiveSpeedHeldWhenVerbFails) {
    fake_port.speed_100mbps = 2000;
    ASSERT_EQ(context_->refreshPortAttributes(), 0);
    ASSERT_DOUBLE_EQ(context_->linkSpeedGbps(), 200.0);
    fake_port.query_speed_rc = EIO;
    ASSERT_EQ(context_->refreshPortAttributes(), 0);
    EXPECT_DOUBLE_EQ(context_->linkSpeedGbps(), 200.0);
    ASSERT_EQ(context_->refreshPortAttributes(), 0);
    EXPECT_DOUBLE_EQ(context_->linkSpeedGbps(), 200.0);
    EXPECT_EQ(context_->effectiveSpeedQueryFailures(), 2u);
    // Recovery at a new speed is picked up again.
    fake_port.query_speed_rc = 0;
    fake_port.speed_100mbps = 4000;
    ASSERT_EQ(context_->refreshPortAttributes(), 0);
    EXPECT_DOUBLE_EQ(context_->linkSpeedGbps(), 400.0);
    EXPECT_EQ(context_->effectiveSpeedQueryFailures(), 2u);
}

// A verb that succeeds but reports 0 means "nothing to say", not a failure:
// the encodings decide, as when the verb is absent.
TEST_F(RdmaContextFakeVerbsTest, EncodedRateWhenVerbReportsZero) {
    fake_port.speed_100mbps = 2000;
    ASSERT_EQ(context_->refreshPortAttributes(), 0);
    ASSERT_DOUBLE_EQ(context_->linkSpeedGbps(), 200.0);
    fake_port.speed_100mbps = 0;
    ASSERT_EQ(context_->refreshPortAttributes(), 0);
    EXPECT_DOUBLE_EQ(context_->linkSpeedGbps(), 400.0);
    EXPECT_EQ(context_->effectiveSpeedQueryFailures(), 0u);
}

TEST_F(RdmaContextFakeVerbsTest, EncodedRateWhenVerbAbsent) {
    fake_port.speed_100mbps = 2000;
    RdmaContextTestPeer::verbs(*context_).ibv_query_port_speed = nullptr;
    ASSERT_EQ(context_->refreshPortAttributes(), 0);
    EXPECT_DOUBLE_EQ(context_->linkSpeedGbps(), 400.0);
    EXPECT_EQ(fake_port.query_speed_calls, 0);
}

TEST_F(RdmaContextFakeVerbsTest, RefreshSeesRenegotiatedLink) {
    ASSERT_EQ(context_->refreshPortAttributes(), 0);
    ASSERT_DOUBLE_EQ(context_->linkSpeedGbps(), 400.0);
    fake_port.active_speed = 32;  // came back as EDR 4x
    ASSERT_EQ(context_->refreshPortAttributes(), 0);
    EXPECT_DOUBLE_EQ(context_->linkSpeedGbps(), 100.0);
}

TEST_F(RdmaContextFakeVerbsTest, RefreshFailsCleanlyWhenQueryPortFails) {
    ASSERT_EQ(context_->refreshPortAttributes(), 0);
    fake_port.query_port_rc = EIO;
    fake_port.active_speed = 32;
    EXPECT_EQ(context_->refreshPortAttributes(), -1);
    EXPECT_DOUBLE_EQ(context_->linkSpeedGbps(), 400.0);  // cached values kept
}

// The whole runtime chain: a port event reaches Workers::applyContextEvent,
// the context re-reads its (fake) port, and the selector is re-seeded only
// when the speed actually changed -- with the device marked available again
// on the new rate, not the old one.
TEST(RdmaContextEventChainTest, PortActiveReseedsOnlyWhenTheSpeedChanged) {
    auto topology = std::make_shared<Topology>();
    ASSERT_TRUE(topology
                    ->parse(R"({"nics":[
                        {"name":"mc-tcp-0","type":1,"numa_node":0},
                        {"name":"mc-absent-rnic-1","type":0,"numa_node":0}]})")
                    .ok());
    RdmaTransport transport;
    RdmaTransportTestPeer::bindTopology(transport, topology);
    ASSERT_EQ(RdmaTransportTestPeer::initializeContexts(transport), 0u);
    auto workers = RdmaTransportTestPeer::makeWorkers(transport);
    auto* selector = workers->getDeviceSelector();
    constexpr int kDev = 1;
    auto& context = *RdmaTransportTestPeer::contextSet(transport)[kDev];

    fake_port = FakePortVerbs{};
    fake_port.active_speed = 128;
    fake_port.active_width = 2;  // 400G
    auto& verbs = RdmaContextTestPeer::verbs(context);
    verbs.ibv_query_port_default = fakeQueryPort;
    verbs.ibv_query_port_speed = fakeQueryPortSpeed;
    RdmaContextTestPeer::bindDevice(context, &fake_port.native,
                                    std::make_shared<RdmaParams>());

    // Pretend init seeded it at 400G and it learned ~45 GB/s since.
    ASSERT_EQ(context.refreshPortAttributes(), 0);
    ASSERT_TRUE(
        selector->setDeviceBandwidth(kDev, context.linkSpeedGbps()).ok());
    ASSERT_TRUE(selector->setDeviceAvailable(kDev, true).ok());
    for (int i = 0; i < 64; ++i)
        ASSERT_TRUE(selector->release(kDev, 1 << 20, (1 << 20) / 45e9).ok());
    ASSERT_NEAR(selector->getAggregateEwmaBandwidth(), 45e9, 45e9 * 0.02);

    ibv_async_event event{};
    event.event_type = IBV_EVENT_PORT_ACTIVE;
    event.element.port_num = context.portNum();

    // Same speed after the flap: keep what was learned.
    RdmaTransportTestPeer::applyContextEvent(*workers, kDev, context, event);
    EXPECT_NEAR(selector->getAggregateEwmaBandwidth(), 45e9, 45e9 * 0.02);
    EXPECT_TRUE(selector->isDeviceAvailable(kDev));

    // LAG lost a PF: the effective speed halves, the seed and clamp follow.
    fake_port.speed_100mbps = 2000;
    RdmaTransportTestPeer::applyContextEvent(*workers, kDev, context, event);
    EXPECT_DOUBLE_EQ(context.linkSpeedGbps(), 200.0);
    EXPECT_DOUBLE_EQ(selector->getAggregateEwmaBandwidth(), 25e9);
    EXPECT_TRUE(selector->isDeviceAvailable(kDev));

    RdmaContextTestPeer::unbindDevice(context);
}

TEST(RdmaContextPortSpeedTest, RefreshOnInertContextIsRejected) {
    RdmaTransport transport;
    RdmaContext context(transport);
    ASSERT_EQ(context.status(), RdmaContext::DEVICE_UNINIT);
    EXPECT_EQ(context.refreshPortAttributes(), -1);
    EXPECT_DOUBLE_EQ(context.linkSpeedGbps(), 0.0);
}

// The 1 Hz recovery poll only reactivates contexts that are actually paused.
// An inert slot never opened a device, so there is no port to consult and
// nothing to hand back to the selector.
TEST_F(RdmaContextEventTest, RecoveryPollLeavesInertContextsAlone) {
    fire(IBV_EVENT_PORT_ERR, ourPort());
    ASSERT_FALSE(selector_->isDeviceAvailable(kDev));
    ASSERT_EQ(context().status(), RdmaContext::DEVICE_UNINIT);

    RdmaTransportTestPeer::resumePausedContexts(*workers_);

    EXPECT_FALSE(selector_->isDeviceAvailable(kDev));
}

TEST(RdmaContextPortStateTest, QueryOnInertContextIsRejected) {
    RdmaTransport transport;
    RdmaContext context(transport);
    ASSERT_EQ(context.status(), RdmaContext::DEVICE_UNINIT);

    ibv_port_state state = IBV_PORT_DOWN;
    EXPECT_EQ(context.queryPortState(&state), -1);
    EXPECT_EQ(state, IBV_PORT_DOWN) << "a failed query must not invent a state";
    EXPECT_EQ(context.queryPortState(nullptr), -1);
}

// A context paused by IBV_EVENT_PORT_ERR whose IBV_EVENT_PORT_ACTIVE never
// arrives (the async fd is edge-triggered, so a queued event can be stranded)
// used to stay DEVICE_PAUSED for the rest of the process' life, failing every
// transfer routed to that NIC. The monitor thread's poll must notice that the
// hardware reports the port as up and reactivate the context.
TEST(RdmaPausedContextRecoveryTest, PollActivatesPausedContextWithLivePort) {
    if (!hasRdmaDevice()) GTEST_SKIP() << "no RDMA device detected";

    int count = 0;
    ibv_device** devices = ibv_get_device_list(&count);
    ASSERT_NE(devices, nullptr);
    ASSERT_GT(count, 0);
    const std::string device_name = ibv_get_device_name(devices[0]);
    ibv_free_device_list(devices);

    auto topology = std::make_shared<Topology>();
    const std::string spec = R"({"nics":[{"name":")" + device_name +
                             R"(","type":0,"numa_node":0}]})";
    ASSERT_TRUE(topology->parse(spec).ok());

    RdmaTransport transport;
    RdmaTransportTestPeer::bindTopology(transport, topology);
    RdmaTransportTestPeer::initializeContexts(transport);
    const auto& contexts = RdmaTransportTestPeer::contextSet(transport);
    ASSERT_EQ(contexts.size(), 1u);
    RdmaContext& context = *contexts[0];
    if (context.status() != RdmaContext::DEVICE_ENABLED)
        GTEST_SKIP() << device_name << " has no usable active port";

    auto workers = RdmaTransportTestPeer::makeWorkers(transport);
    auto* selector = workers->getDeviceSelector();
    ASSERT_NE(selector, nullptr);

    // Exactly the state IBV_EVENT_PORT_ERR leaves behind, minus the event
    // that would have undone it.
    context.pause();
    ASSERT_EQ(context.status(), RdmaContext::DEVICE_PAUSED);
    ASSERT_TRUE(selector->setDeviceAvailable(0, false).ok());

    RdmaTransportTestPeer::resumePausedContexts(*workers);

    EXPECT_EQ(context.status(), RdmaContext::DEVICE_ENABLED);
    EXPECT_TRUE(selector->isDeviceAvailable(0));
}

TEST(RdmaTransportIntegrationTest, WriteThenReadAcrossProcesses) {
    if (!hasRdmaDevice()) GTEST_SKIP() << "no RDMA device detected";

    constexpr size_t kDataLength = 4 * 1024 * 1024;
    constexpr size_t kCancelTaskCount = 16;
    constexpr size_t kCancelStride = 8 * 1024 * 1024;
    constexpr size_t kBufferLength = kCancelTaskCount * kCancelStride;
    int ready_pipe[2];
    int stop_pipe[2];
    ASSERT_EQ(pipe(ready_pipe), 0);
    ASSERT_EQ(pipe(stop_pipe), 0);

    pid_t child = fork();
    ASSERT_GE(child, 0);
    if (child == 0) {
        close(ready_pipe[0]);
        close(stop_pipe[1]);

        TransferEngine server(makeRdmaConfig());
        if (!server.available()) _exit(2);
        std::vector<uint8_t> buffer(kBufferLength);
        if (!server.registerLocalMemory(buffer.data(), buffer.size()).ok())
            _exit(3);

        const std::string segment = server.getSegmentName();
        uint32_t length = static_cast<uint32_t>(segment.size());
        if (write(ready_pipe[1], &length, sizeof(length)) != sizeof(length))
            _exit(4);
        if (write(ready_pipe[1], segment.data(), length) !=
            static_cast<ssize_t>(length))
            _exit(5);

        char stop = 0;
        const ssize_t stop_result = read(stop_pipe[0], &stop, 1);
        (void)stop_result;
        (void)server.unregisterLocalMemory(buffer.data(), buffer.size());
        _exit(0);
    }

    close(ready_pipe[1]);
    close(stop_pipe[0]);
    ChildProcessGuard child_guard(child, stop_pipe[1]);

    uint32_t segment_length = 0;
    ssize_t received =
        read(ready_pipe[0], &segment_length, sizeof(segment_length));
    if (received != static_cast<ssize_t>(sizeof(segment_length))) {
        const int status = child_guard.reap();
        GTEST_SKIP() << "RDMA server initialization failed, child status "
                     << status;
    }
    std::string server_segment(segment_length, '\0');
    ASSERT_EQ(read(ready_pipe[0], server_segment.data(), segment_length),
              static_cast<ssize_t>(segment_length));

    TransferEngine client(makeRdmaConfig());
    ASSERT_TRUE(client.available());
    std::vector<uint8_t> buffer(kBufferLength);
    for (size_t i = 0; i < kDataLength; ++i) {
        buffer[i] = static_cast<uint8_t>((i * 31) & 0xff);
    }
    ASSERT_TRUE(client.registerLocalMemory(buffer.data(), buffer.size()).ok());

    SegmentID segment = 0;
    Status result;
    for (int i = 0; i < 100; ++i) {
        result = client.openSegment(segment, server_segment);
        if (result.ok()) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    ASSERT_TRUE(result.ok()) << result.ToString();

    SegmentInfo info;
    ASSERT_TRUE(client.getSegmentInfo(segment, info).ok());
    ASSERT_FALSE(info.buffers.empty());

    Request request{};
    request.opcode = Request::WRITE;
    request.source = buffer.data();
    request.target_id = segment;
    request.target_offset = info.buffers[0].base;
    request.length = kDataLength;
    request.transport_hint = RDMA;

    BatchID batch = client.allocateBatch(1);
    ASSERT_TRUE(client.submitTransfer(batch, {request}).ok());
    ASSERT_TRUE(waitBatchDone(client, batch));
    ASSERT_TRUE(client.freeBatch(batch).ok());

    request.opcode = Request::READ;
    request.source = buffer.data() + kDataLength;
    batch = client.allocateBatch(1);
    ASSERT_TRUE(client.submitTransfer(batch, {request}).ok());
    ASSERT_TRUE(waitBatchDone(client, batch));
    ASSERT_TRUE(client.freeBatch(batch).ok());
    EXPECT_EQ(
        std::memcmp(buffer.data(), buffer.data() + kDataLength, kDataLength),
        0);

    // Keep one QP/worker and one outstanding WR so the tail task remains in
    // the worker's unposted set long enough to exercise real cancellation.
    std::vector<Request> cancel_requests;
    cancel_requests.reserve(kCancelTaskCount);
    for (size_t i = 0; i < kCancelTaskCount; ++i) {
        Request cancel_request{};
        cancel_request.opcode = Request::WRITE;
        cancel_request.source = buffer.data() + i * kCancelStride;
        cancel_request.target_id = segment;
        cancel_request.target_offset = info.buffers[0].base + i * kCancelStride;
        cancel_request.length = kDataLength;
        cancel_request.transport_hint = RDMA;
        cancel_requests.push_back(cancel_request);
    }

    batch = client.allocateBatch(kCancelTaskCount);
    ASSERT_TRUE(client.submitTransfer(batch, cancel_requests).ok());
    const size_t cancel_task_id = kCancelTaskCount - 1;
    ASSERT_TRUE(client.cancelTransfer(batch, cancel_task_id).ok());
    ASSERT_TRUE(client.cancelTransfer(batch, cancel_task_id).ok());

    std::vector<TransferStatus> statuses;
    for (int poll = 0; poll < 10000; ++poll) {
        ASSERT_TRUE(client.getTransferStatus(batch, statuses).ok());
        if (std::all_of(statuses.begin(), statuses.end(),
                        [](const TransferStatus& task_status) {
                            return task_status.s != TransferStatusEnum::PENDING;
                        }))
            break;
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    ASSERT_EQ(statuses.size(), kCancelTaskCount);
    for (size_t i = 0; i < cancel_task_id; ++i) {
        EXPECT_EQ(statuses[i].s, TransferStatusEnum::COMPLETED);
    }
    EXPECT_EQ(statuses[cancel_task_id].s, TransferStatusEnum::CANCELED);
    EXPECT_LE(statuses[cancel_task_id].transferred_bytes, kDataLength);
    ASSERT_TRUE(client.freeBatch(batch).ok());

    EXPECT_TRUE(client.closeSegment(segment).ok());
    EXPECT_TRUE(
        client.unregisterLocalMemory(buffer.data(), buffer.size()).ok());

    const int status = child_guard.finish();
    ASSERT_TRUE(WIFEXITED(status));
    EXPECT_EQ(WEXITSTATUS(status), 0);
}

TEST(RdmaQuiesceTest, TransportQuiesceWithoutInstallIsOk) {
    RdmaTransport transport;
    EXPECT_TRUE(transport.quiesce().ok());
    EXPECT_TRUE(transport.quiesce().ok());
}

TEST(RdmaQuiesceTest, WorkersQuiesceWithoutStartRejectsSubmit) {
    auto topology = std::make_shared<Topology>();
    RdmaTransport transport;
    RdmaTransportTestPeer::bindTopology(transport, topology);
    auto workers = RdmaTransportTestPeer::makeWorkers(transport);

    EXPECT_TRUE(workers->quiesce().ok());
    EXPECT_TRUE(workers->quiesce().ok());

    RdmaSliceList slice_list;
    EXPECT_TRUE(workers->submit(slice_list).IsInternalError());
}

TEST(RdmaQuiesceTest, IdleWorkersDrainAndRejectSubmit) {
    auto topology = std::make_shared<Topology>();
    RdmaTransport transport;
    RdmaTransportTestPeer::bindTopology(transport, topology);
    RdmaTransportTestPeer::setNumWorkers(transport, 1);
    auto workers = RdmaTransportTestPeer::makeWorkers(transport);
    ASSERT_TRUE(workers->start().ok());

    EXPECT_TRUE(workers->quiesce().ok());
    RdmaSliceList slice_list;
    EXPECT_TRUE(workers->submit(slice_list).IsInternalError());
    EXPECT_TRUE(workers->quiesce().ok());
    ASSERT_TRUE(workers->stop().ok());
}

TEST(RdmaQuiesceTest, DrainTimeoutLeavesInflight) {
    auto topology = std::make_shared<Topology>();
    RdmaTransport transport;
    RdmaTransportTestPeer::bindTopology(transport, topology);
    RdmaTransportTestPeer::setNumWorkers(transport, 1);
    auto workers = RdmaTransportTestPeer::makeWorkers(transport);
    ASSERT_TRUE(workers->start().ok());
    RdmaTransportTestPeer::addInflight(*workers, 0, 1);

    const auto started = std::chrono::steady_clock::now();
    EXPECT_TRUE(workers->quiesce(50'000'000ull).IsInternalError());
    EXPECT_LT(std::chrono::steady_clock::now() - started,
              std::chrono::milliseconds(500));

    ASSERT_TRUE(workers->stop().ok());
}

TEST(RdmaNotifyFlushTest, ReceiveNotificationEmptyUninstalledIsOk) {
    RdmaTransport transport;
    std::vector<Notification> list;
    EXPECT_TRUE(transport.receiveNotification(list).ok());
    EXPECT_TRUE(list.empty());
}

TEST(RdmaNotifyFlushTest, ReceiveNotificationDrainsQueue) {
    auto topology = std::make_shared<Topology>();
    RdmaTransport transport;
    RdmaTransportTestPeer::bindTopology(transport, topology);
    transport.addNotificationToQueue("peer", "done");

    std::vector<Notification> list;
    EXPECT_TRUE(transport.receiveNotification(list).ok());
    ASSERT_EQ(list.size(), 1u);
    EXPECT_EQ(list[0].name, "peer");
    EXPECT_EQ(list[0].msg, "done");

    list.clear();
    EXPECT_TRUE(transport.receiveNotification(list).ok());
    EXPECT_TRUE(list.empty());
}

TEST(RdmaNotifyFlushTest, DestFlushOnGpuToGpuNotifyIsOk) {
    auto topology = std::make_shared<Topology>();
    Topology::MemEntry memory;
    memory.name = "cuda:0";
    memory.type = Topology::MEM_CUDA;
    memory.numa_node = 0;
    topology->mem_list_.push_back(std::move(memory));

    RdmaTransport transport;
    RdmaTransportTestPeer::bindTopology(transport, topology);
    RdmaTransportTestPeer::setGpuToGpu(transport, true);
    transport.addNotificationToQueue("peer", "done");

    std::vector<Notification> list;
    EXPECT_TRUE(transport.receiveNotification(list).ok());
    ASSERT_EQ(list.size(), 1u);
    EXPECT_EQ(list[0].name, "peer");
}

TEST(RdmaNotifyFlushTest, FlushDisabledStillDrainsQueue) {
    auto topology = std::make_shared<Topology>();
    Topology::MemEntry memory;
    memory.name = "cuda:0";
    memory.type = Topology::MEM_CUDA;
    memory.numa_node = 0;
    topology->mem_list_.push_back(std::move(memory));

    RdmaTransport transport;
    RdmaTransportTestPeer::bindTopology(transport, topology);
    RdmaTransportTestPeer::setGpuToGpu(transport, true);
    RdmaTransportTestPeer::setFlushGpuDirectWrites(transport, false);
    transport.addNotificationToQueue("peer", "done");

    std::vector<Notification> list;
    EXPECT_TRUE(transport.receiveNotification(list).ok());
    ASSERT_EQ(list.size(), 1u);
    list.clear();
    EXPECT_TRUE(transport.receiveNotification(list).ok());
    EXPECT_TRUE(list.empty());
}

}  // namespace
}  // namespace tent
}  // namespace mooncake
