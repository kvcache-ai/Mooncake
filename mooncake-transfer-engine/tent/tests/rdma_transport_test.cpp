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
#include "tent/transport/rdma/endpoint.h"
#include "tent/transport/rdma/endpoint_store.h"
#include "tent/transport/rdma/params.h"
#include "tent/transport/rdma/quota.h"
#include "tent/transport/rdma/rdma_transport.h"
#include "tent/transport/rdma/slice.h"
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

    using NotifyAction = RdmaTransport::NotifyCompletionAction;

    static NotifyAction classifyNotifyCompletion(ibv_wc_status status,
                                                 bool endpoint_alive,
                                                 bool endpoint_ready) {
        return RdmaTransport::classifyNotifyCompletion(status, endpoint_alive,
                                                       endpoint_ready);
    }

    static void rechargeSlice(Workers& workers, RdmaSlice* slice, int dev_id) {
        workers.rechargeSlice(slice, dev_id);
    }

    static void releaseSliceQuota(Workers& workers, RdmaSlice* slice,
                                  uint64_t now_ns, double latency) {
        workers.releaseSliceQuota(slice, now_ns, latency);
    }

    static void notePostedSlice(Workers& workers, RdmaSlice* slice) {
        workers.notePostedSlice(slice);
    }

    static void handleCompletion(Workers& workers,
                                 Workers::WorkerContext& worker,
                                 RdmaContext& context, const ibv_wc& wc,
                                 uint64_t poll_ts, bool last_in_pass = true) {
        workers.handleCompletion(worker, context, wc, poll_ts, last_in_pass);
    }

    static void markPosted(Workers& workers, Workers::WorkerContext& worker,
                           RdmaSlice* slice, uint64_t post_ts) {
        workers.markPosted(worker, slice, post_ts);
    }

    using WorkerContext = Workers::WorkerContext;

    // Stands in for start(): the lane array without any threads.
    static void makeWorkerContexts(Workers& workers, size_t n) {
        workers.worker_context_ = new WorkerContext[n];
        workers.num_workers_ = n;
    }
    static void destroyWorkerContexts(Workers& workers) {
        delete[] workers.worker_context_;
        workers.worker_context_ = nullptr;
        workers.num_workers_ = 0;
    }
    static WorkerContext& workerContext(Workers& workers, size_t i) {
        return workers.worker_context_[i];
    }
    static void retireSweptSlice(Workers& workers, WorkerContext& self,
                                 RdmaSlice* slice, uint64_t now_ns,
                                 bool bytes_moved = false) {
        workers.retireSweptSlice(self, slice, now_ns, nullptr, bytes_moved);
    }
    static void drainReclaimed(Workers& workers, WorkerContext& worker) {
        workers.drainReclaimed(worker);
    }
    // Drop the handover list without touching the set, to model a lane that
    // drained it just before another lane handed an entry over.
    static void clearReclaimed(WorkerContext& worker) {
        std::lock_guard<std::mutex> lock(worker.reclaim_mutex);
        worker.reclaimed.clear();
    }
    static bool dropUnpostableSlice(Workers& workers, WorkerContext& worker,
                                    RdmaSlice* slice) {
        return workers.dropUnpostableSlice(worker, slice);
    }

    static void setSliceTimeout(Workers& workers, uint64_t ns) {
        workers.slice_timeout_ns_ = ns;
    }
    static void expireTimedOutSlices(Workers& workers, WorkerContext& ctx,
                                     uint64_t now_ns) {
        workers.expireTimedOutSlices(ctx, now_ns);
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

    // The rest of what enable() would build, for a test that only needs a
    // context complete enough to construct endpoints on.
    static void bindResources(RdmaContext& context, ibv_pd* pd,
                              std::vector<RdmaCQ*> cqs, RdmaCQ* notify_cq) {
        context.native_pd_ = pd;
        context.cq_list_ = std::move(cqs);
        context.notify_cq_ = notify_cq;
        context.endpoint_store_ =
            std::make_shared<SIEVEEndpointStore>(context, 16);
    }

    static void unbindResources(RdmaContext& context) {
        context.native_pd_ = nullptr;
        context.cq_list_.clear();
        context.notify_cq_ = nullptr;
        context.endpoint_store_.reset();
    }

    static void unbindDevice(RdmaContext& context) {
        context.native_context_ = nullptr;
    }
};

// Friend accessor for RdmaEndPoint. Posting is the one step in the data
// path that does not go through the injectable verbs table (ibv_post_send is
// an inline that dispatches through the queue pair), so a test stands in for
// it and puts the slice on the queue pair the way submitSlices would.
class RdmaEndPointTestPeer {
   public:
    static void pretendPosted(const std::shared_ptr<RdmaEndPoint>& endpoint,
                              int qp_index, RdmaSlice* slice) {
        ASSERT_TRUE(endpoint->reserveQuota(qp_index, 1));
        slice->qp_index = qp_index;
        slice->ep_weak_ptr = endpoint;
        endpoint->slice_queue_[qp_index].push(slice);
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

// A slice that hits the software timeout turns terminal here, not on the CQ,
// and its flush completion is not guaranteed to be polled, so the timeout
// path must return the selector's inflight charge itself.
class RdmaWorkersTimeoutTest : public ::testing::Test {
   protected:
    static constexpr int kDev = 1;
    static constexpr uint64_t kLen = 1 << 20;

    void SetUp() override {
        auto topology = std::make_shared<Topology>();
        ASSERT_TRUE(topology
                        ->parse(R"({"nics":[
                        {"name":"mc-tcp-0","type":1,"numa_node":0},
                        {"name":"mc-absent-rnic-1","type":0,"numa_node":0}]})")
                        .ok());
        Topology::MemEntry mem;
        mem.name = "cpu:0";
        mem.type = Topology::MEM_HOST;
        mem.numa_node = 0;
        mem.device_list[0].push_back(1);
        topology->mem_list_.push_back(mem);

        RdmaTransportTestPeer::bindTopology(transport_, topology);
        ASSERT_EQ(RdmaTransportTestPeer::initializeContexts(transport_), 0u);
        workers_ = RdmaTransportTestPeer::makeWorkers(transport_);
        selector_ = workers_->getDeviceSelector();
        ASSERT_TRUE(selector_->setDeviceAvailable(kDev, true).ok());

        // Charge the slice to the NIC exactly as selectOptimalDevice() does.
        task_ = RdmaTaskStorage::Get().allocate();
        task_->num_slices = 1;
        task_->status_word = PENDING;
        task_->first_error = PENDING;
        task_->ref();  // the batch's reference
        task_->ref();  // the slice's reference, dropped by updateSliceStatus()
        slice_ = RdmaSliceStorage::Get().allocate();
        slice_->task = task_;
        slice_->length = kLen;
        slice_->word = PENDING;
        slice_->enqueue_ts = 0;  // enqueued at t=0
        slice_->rail_monitor = nullptr;
        int chosen = -1;
        ASSERT_TRUE(selector_->allocate(kLen, "cpu:0", chosen).ok());
        ASSERT_EQ(chosen, kDev);
        slice_->source_dev_id = chosen;
        slice_->charged_dev = chosen;
        slice_->counted_lane = 0;  // as Workers::submit() marks it
        ASSERT_EQ(selector_->getInflightBytes(kDev), kLen);

        ctx_.inflight_slice_set.insert(slice_);
        RdmaTransportTestPeer::setSliceTimeout(*workers_, 1'000'000);  // 1 ms
    }

    void TearDown() override {
        if (slice_) RdmaSliceStorage::Get().deallocate(slice_);
        if (task_) task_->deref();  // drops the batch's reference; frees it
    }

    void expire() {
        RdmaTransportTestPeer::expireTimedOutSlices(*workers_, ctx_,
                                                    /*now_ns=*/2'000'000);
    }

    void expectTimedOutAndReleased() {
        EXPECT_EQ(slice_->word, TIMEOUT);
        EXPECT_TRUE(ctx_.inflight_slice_set.empty());
        EXPECT_EQ(slice_->charged_dev, -1);
        EXPECT_EQ(selector_->getInflightBytes(kDev), 0u);
    }

    RdmaTransport transport_;
    std::unique_ptr<Workers> workers_;
    DeviceSelector* selector_ = nullptr;
    RdmaTask* task_ = nullptr;
    RdmaSlice* slice_ = nullptr;
    RdmaTransportTestPeer::WorkerContext ctx_;
};

TEST_F(RdmaWorkersTimeoutTest, EndpointGoneReleasesSelectorInflightCharge) {
    slice_->ep_weak_ptr.reset();  // endpoint already gone
    ctx_.inflight_slices.store(1);

    expire();

    expectTimedOutAndReleased();
    EXPECT_EQ(ctx_.inflight_slices.load(), 0);
}

// The slice's endpoint is alive but no longer holds it (a neighbour's retry
// popped it with PENDING and its count), so acknowledge() sweeps nothing;
// the timeout path still has to fail it and return its charge.
TEST_F(RdmaWorkersTimeoutTest, NotOnQueueStillReleasesSelectorInflightCharge) {
    auto endpoint = std::make_shared<RdmaEndPoint>();  // never constructed
    slice_->ep_weak_ptr = endpoint;
    slice_->qp_index = 0;
    ctx_.inflight_slices.store(0);  // already dropped with the neighbour's

    expire();

    expectTimedOutAndReleased();
    EXPECT_EQ(ctx_.inflight_slices.load(), 0);
}

// Two RDMA NICs, both reachable from the host buffer, and a Workers whose
// selector is set to adopt every selection sample outright.
class RdmaWorkersChargeTest : public ::testing::Test {
   protected:
    static constexpr uint64_t kLen = 1 << 20;
    static constexpr uint64_t kNow = 1'000'000'000;

    void SetUp() override {
        auto topology = std::make_shared<Topology>();
        ASSERT_TRUE(topology
                        ->parse(R"({"nics":[
                        {"name":"mc-absent-rnic-0","type":0,"numa_node":0},
                        {"name":"mc-absent-rnic-1","type":0,"numa_node":0}]})")
                        .ok());
        Topology::MemEntry mem;
        mem.name = "cpu:0";
        mem.type = Topology::MEM_HOST;
        mem.numa_node = 0;
        mem.device_list[0].push_back(0);
        mem.device_list[0].push_back(1);
        topology->mem_list_.push_back(mem);

        RdmaTransportTestPeer::bindTopology(transport_, topology);
        ASSERT_EQ(RdmaTransportTestPeer::initializeContexts(transport_), 0u);
        workers_ = RdmaTransportTestPeer::makeWorkers(transport_);
        selector_ = workers_->getDeviceSelector();
        auto params = selector_->getSchedulingParams();
        params.bandwidth_learning_rate = 0.0;  // adopt the sample
        selector_->setSchedulingParams(params);
        for (int dev : {0, 1}) {
            ASSERT_TRUE(selector_->setDeviceAvailable(dev, true).ok());
            ASSERT_TRUE(selector_->setDeviceBandwidth(dev, 25.0).ok());
        }

        task_ = RdmaTaskStorage::Get().allocate();
        task_->num_slices = 1;
        task_->status_word = PENDING;
        task_->first_error = PENDING;
        task_->ref();
        slice_ = RdmaSliceStorage::Get().allocate();
        slice_->task = task_;
        slice_->length = kLen;
        slice_->word = PENDING;
        slice_->rail_monitor = nullptr;
        // Allocated on NIC 0, as submitTransferTasks charges it.
        int chosen = -1;
        ASSERT_TRUE(
            selector_->allocate(kLen, "cpu:0", chosen, PRIO_HIGH, 1ULL << 0)
                .ok());
        ASSERT_EQ(chosen, 0);
        slice_->source_dev_id = chosen;
        slice_->charged_dev = chosen;
        ASSERT_EQ(selector_->getInflightBytes(0), kLen);
    }

    void TearDown() override {
        RdmaSliceStorage::Get().deallocate(slice_);
        task_->deref();
    }

    RdmaTransport transport_;
    std::unique_ptr<Workers> workers_;
    DeviceSelector* selector_ = nullptr;
    RdmaTask* task_ = nullptr;
    RdmaSlice* slice_ = nullptr;
};

// The CQ error path returns a slice's charge before re-submitting it, and
// the retry picks its device again. Without a fresh charge the NIC's
// inflight bytes miss the retry entirely and its completion teaches neither
// estimate, because the release is a no-op on an uncharged slice.
TEST_F(RdmaWorkersChargeTest, RetryIsRechargedAndStillLearns) {
    // The error path hands the charge back and re-submits.
    RdmaTransportTestPeer::releaseSliceQuota(*workers_, slice_, kNow, 0.0);
    ASSERT_EQ(slice_->charged_dev, -1);
    ASSERT_EQ(selector_->getInflightBytes(0), 0u);

    // The retry settles on the fallback NIC.
    RdmaTransportTestPeer::rechargeSlice(*workers_, slice_, 1);
    slice_->source_dev_id = 1;  // as selectFallbackDevice does next
    EXPECT_EQ(slice_->charged_dev, 1);
    EXPECT_EQ(selector_->getInflightBytes(1), kLen);
    EXPECT_EQ(selector_->getInflightBytes(0), 0u);

    // Its completion returns the charge and teaches the selector the rate
    // it saw. Without the recharge above the release would be a no-op on an
    // uncharged slice: the bytes would stay charged to nobody and the
    // sample would be lost.
    const double before = selector_->getAggregateEwmaBandwidth();
    RdmaTransportTestPeer::releaseSliceQuota(*workers_, slice_, kNow + kLen,
                                             kLen / 5e8);
    EXPECT_EQ(selector_->getInflightBytes(1), 0u);
    // Device 0 is untaught; device 1 adopted 0.5 GB/s in place of its seed.
    EXPECT_NEAR(selector_->getAggregateEwmaBandwidth(),
                before - 3.125e9 + 0.5e9, 1.0);
}

// A first attempt that falls back to another NIC is still charged to the
// one the allocator picked. The charge moves with the routing: the fallback
// NIC is charged first, then the original is paid back, so a release racing
// in between finds one of them on record and never both.
TEST_F(RdmaWorkersChargeTest, FallbackMovesTheChargeToTheNicItPostsOn) {
    RdmaTransportTestPeer::rechargeSlice(*workers_, slice_, 1);
    slice_->source_dev_id = 1;

    EXPECT_EQ(slice_->charged_dev, 1);
    EXPECT_EQ(selector_->getInflightBytes(0), 0u);
    EXPECT_EQ(selector_->getInflightBytes(1), kLen);

    RdmaTransportTestPeer::releaseSliceQuota(*workers_, slice_, kNow, 0.0);
    EXPECT_EQ(slice_->charged_dev, -1);
    EXPECT_EQ(selector_->getInflightBytes(0), 0u);
    EXPECT_EQ(selector_->getInflightBytes(1), 0u);
}

// A device the selector does not track cannot be charged. The slice keeps
// the charge it has rather than ending up charged to nobody, so the release
// still balances the NIC that was charged and nothing underflows.
TEST_F(RdmaWorkersChargeTest, ChargeOnAnUntrackedDeviceKeepsTheOldOne) {
    RdmaTransportTestPeer::rechargeSlice(*workers_, slice_, 999);

    EXPECT_EQ(slice_->charged_dev, 0);
    EXPECT_EQ(selector_->getInflightBytes(0), kLen);

    RdmaTransportTestPeer::releaseSliceQuota(*workers_, slice_, kNow, 0.0);
    EXPECT_EQ(slice_->charged_dev, -1);
    EXPECT_EQ(selector_->getInflightBytes(0), 0u);
}

// With qp_pools several worker lanes share a queue pair, so the lane that
// sweeps a slice off it is often not the lane that enqueued it. The sweep
// must give the accounting back to the owner: its set entry, its place in
// its inflight count, and its selector charge.
TEST(RdmaWorkersOwnershipTest, SweepByAnotherLaneIsRoutedToTheOwner) {
    constexpr int kDev = 1;
    constexpr uint64_t kLen = 1 << 20;
    auto topology = std::make_shared<Topology>();
    ASSERT_TRUE(topology
                    ->parse(R"({"nics":[
                        {"name":"mc-tcp-0","type":1,"numa_node":0},
                        {"name":"mc-absent-rnic-1","type":0,"numa_node":0}]})")
                    .ok());
    Topology::MemEntry mem;
    mem.name = "cpu:0";
    mem.type = Topology::MEM_HOST;
    mem.numa_node = 0;
    mem.device_list[0].push_back(kDev);
    topology->mem_list_.push_back(mem);

    RdmaTransport transport;
    RdmaTransportTestPeer::bindTopology(transport, topology);
    ASSERT_EQ(RdmaTransportTestPeer::initializeContexts(transport), 0u);
    auto workers = RdmaTransportTestPeer::makeWorkers(transport);
    auto* selector = workers->getDeviceSelector();
    ASSERT_TRUE(selector->setDeviceAvailable(kDev, true).ok());
    RdmaTransportTestPeer::makeWorkerContexts(*workers, 2);
    auto& lane_a = RdmaTransportTestPeer::workerContext(*workers, 0);
    auto& lane_b = RdmaTransportTestPeer::workerContext(*workers, 1);

    // Lane B enqueued and posted the slice.
    auto* task = RdmaTaskStorage::Get().allocate();
    task->num_slices = 1;
    task->status_word = PENDING;
    task->first_error = PENDING;
    task->ref();
    auto* slice = RdmaSliceStorage::Get().allocate();
    slice->task = task;
    slice->length = kLen;
    slice->word = PENDING;
    slice->rail_monitor = nullptr;
    slice->owner_worker = 1;
    slice->counted_lane = 1;  // as Workers::submit() marks it
    int chosen = -1;
    ASSERT_TRUE(selector->allocate(kLen, "cpu:0", chosen).ok());
    slice->source_dev_id = chosen;
    slice->charged_dev = chosen;
    RdmaTransportTestPeer::notePostedSlice(*workers, slice);
    lane_b.inflight_slice_set.insert(slice);
    lane_b.inflight_slices.store(1);
    // Lane A still holds an entry from an earlier attempt of the same slice
    // (a retry moved it to lane B before A's set was drained).
    lane_a.inflight_slice_set.insert(slice);
    ASSERT_EQ(selector->getInflightBytes(kDev), kLen);
    ASSERT_EQ(selector->getPostedBytes(kDev), kLen);

    // Lane A sweeps it off the shared queue pair.
    RdmaTransportTestPeer::retireSweptSlice(*workers, lane_a, slice,
                                            getCurrentTimeInNano());

    EXPECT_EQ(lane_a.inflight_slices.load(), 0);  // not lane A's to discount
    EXPECT_EQ(lane_b.inflight_slices.load(), 0);
    // A took out its own stale entry; B's is handed over, not touched.
    EXPECT_TRUE(lane_a.inflight_slice_set.empty());
    EXPECT_EQ(lane_b.inflight_slice_set.count(slice), 1u);  // owner erases it
    EXPECT_EQ(selector->getInflightBytes(kDev), 0u);
    EXPECT_EQ(selector->getPostedBytes(kDev), 0u);

    // ...on its own next pass.
    RdmaTransportTestPeer::drainReclaimed(*workers, lane_b);
    EXPECT_TRUE(lane_b.inflight_slice_set.empty());

    RdmaTransportTestPeer::destroyWorkerContexts(*workers);
    RdmaSliceStorage::Get().deallocate(slice);
    task->deref();
}

// What the completion path owes the NIC's accounting, driven with a
// synthesized work completion: a successful one hands back the slice's
// charge and its place in the backlog, then feeds the meter the bytes it
// moved. An unsuccessful one returns the same accounting but teaches
// nothing -- a flush burst must not drag the estimate down.
class RdmaWorkersCompletionTest : public ::testing::Test {
   protected:
    static constexpr int kDev = 1;
    static constexpr uint64_t kLen = 1 << 20;
    static constexpr uint64_t kNow = 1'000'000'000;

    void SetUp() override {
        auto topology = std::make_shared<Topology>();
        ASSERT_TRUE(topology
                        ->parse(R"({"nics":[
                        {"name":"mc-tcp-0","type":1,"numa_node":0},
                        {"name":"mc-absent-rnic-1","type":0,"numa_node":0}]})")
                        .ok());
        Topology::MemEntry mem;
        mem.name = "cpu:0";
        mem.type = Topology::MEM_HOST;
        mem.numa_node = 0;
        mem.device_list[0].push_back(kDev);
        topology->mem_list_.push_back(mem);

        RdmaTransportTestPeer::bindTopology(transport_, topology);
        ASSERT_EQ(RdmaTransportTestPeer::initializeContexts(transport_), 0u);
        workers_ = RdmaTransportTestPeer::makeWorkers(transport_);
        selector_ = workers_->getDeviceSelector();
        ASSERT_TRUE(selector_->setDeviceAvailable(kDev, true).ok());
        auto params = selector_->getSchedulingParams();
        params.transmit_bandwidth_learning_rate = 0.0;  // adopt outright
        params.transmit_meter_interval_ns = 0;          // sample on demand
        selector_->setSchedulingParams(params);
        ASSERT_TRUE(selector_->setDeviceBandwidth(kDev, 25.0).ok());

        task_ = RdmaTaskStorage::Get().allocate();
        task_->num_slices = 1;
        task_->status_word = PENDING;
        task_->first_error = PENDING;
        task_->ref();  // the batch's reference
        task_->ref();  // the slice's, dropped when it turns terminal
        slice_ = RdmaSliceStorage::Get().allocate();
        slice_->task = task_;
        slice_->length = kLen;
        slice_->word = PENDING;
        slice_->rail_monitor = nullptr;
        // Queued for a while before it was posted: the busy stretch must
        // start at submit_ts, not enqueue_ts.
        slice_->enqueue_ts = kNow - kLen;
        slice_->submit_ts = kNow;
        int chosen = -1;
        ASSERT_TRUE(selector_->allocate(kLen, "cpu:0", chosen).ok());
        ASSERT_EQ(chosen, kDev);
        slice_->source_dev_id = chosen;
        slice_->charged_dev = chosen;
        // Posted to the hardware, and its endpoint is still around (never
        // constructed, so acknowledge() sweeps nothing).
        endpoint_ = std::make_shared<RdmaEndPoint>();
        slice_->ep_weak_ptr = endpoint_;
        RdmaTransportTestPeer::notePostedSlice(*workers_, slice_);
        ASSERT_EQ(selector_->getPostedBytes(kDev), kLen);
        // The meter's interval opens with the NIC holding this backlog.
        selector_->maybeSampleTransmit(kDev, kNow);
    }

    void TearDown() override {
        for (auto* slice : extra_slices_)
            RdmaSliceStorage::Get().deallocate(slice);
        for (auto* task : extra_tasks_) releaseTask(task);
        if (slice_) RdmaSliceStorage::Get().deallocate(slice_);
        if (task_) releaseTask(task_);
    }

    // The endpoint is never constructed, so acknowledge() sweeps nothing
    // and a slice completed here never turns terminal; only the cancel
    // paths drop the slice's reference. Drop whatever is left.
    static void releaseTask(RdmaTask* task) {
        for (int n = task->ref_count.load(); n > 0; --n) task->deref();
    }

    // Another slice on the same NIC, charged and posted at `submit_ts` the
    // way asyncPostSend does it.
    RdmaSlice* makeSlice(uint64_t submit_ts) {
        auto* task = RdmaTaskStorage::Get().allocate();
        task->num_slices = 1;
        task->status_word = PENDING;
        task->first_error = PENDING;
        task->ref();  // the batch's reference
        task->ref();  // the slice's, dropped when it turns terminal
        auto* slice = RdmaSliceStorage::Get().allocate();
        slice->task = task;
        slice->length = kLen;
        slice->word = PENDING;
        slice->rail_monitor = nullptr;
        slice->enqueue_ts = submit_ts - kLen;
        slice->submit_ts = submit_ts;
        int chosen = -1;
        EXPECT_TRUE(selector_->allocate(kLen, "cpu:0", chosen).ok());
        slice->source_dev_id = chosen;
        slice->charged_dev = chosen;
        slice->ep_weak_ptr = endpoint_;
        RdmaTransportTestPeer::notePostedSlice(*workers_, slice);
        extra_tasks_.push_back(task);
        extra_slices_.push_back(slice);
        return slice;
    }

    void completeAt(RdmaSlice* slice, ibv_wc_status status, uint64_t poll_ts,
                    bool last_in_pass = true) {
        ibv_wc wc{};
        wc.wr_id = reinterpret_cast<uint64_t>(slice);
        wc.status = status;
        RdmaTransportTestPeer::handleCompletion(
            *workers_, ctx_, *RdmaTransportTestPeer::contextSet(transport_)[0],
            wc, poll_ts, last_in_pass);
    }

    void complete(ibv_wc_status status) {
        completeAt(slice_, status, kNow + kLen);  // one byte per nanosecond
    }

    RdmaTransport transport_;
    std::unique_ptr<Workers> workers_;
    DeviceSelector* selector_ = nullptr;
    RdmaTask* task_ = nullptr;
    RdmaSlice* slice_ = nullptr;
    std::shared_ptr<RdmaEndPoint> endpoint_;
    RdmaTransportTestPeer::WorkerContext ctx_;
    std::vector<RdmaTask*> extra_tasks_;
    std::vector<RdmaSlice*> extra_slices_;
};

// One poll pass stamps every completion it reaps with the same timestamp.
// Sampled at each of them, the first would divide its own bytes by the
// busy time of the whole group, and the rest -- same timestamp, no later
// than the baseline it just moved -- would be ignored: 0.5 GB/s for a
// link that moved 2 MiB in 2 MiB ns. The pass samples once, at its end.
TEST_F(RdmaWorkersCompletionTest, OnlyTheLastCompletionOfAPassSamples) {
    auto* second = makeSlice(kNow);  // same burst: 2 MiB posted at kNow
    ASSERT_EQ(selector_->getPostedBytes(kDev), 2 * kLen);

    completeAt(slice_, IBV_WC_SUCCESS, kNow + 2 * kLen, /*last_in_pass=*/false);
    completeAt(second, IBV_WC_SUCCESS, kNow + 2 * kLen, /*last_in_pass=*/true);

    EXPECT_NEAR(selector_->getTransmitBandwidth(kDev), 1e9, 1.0);
}

TEST_F(RdmaWorkersCompletionTest, SuccessFeedsTheMeter) {
    complete(IBV_WC_SUCCESS);

    EXPECT_EQ(slice_->charged_dev, -1);
    EXPECT_EQ(selector_->getInflightBytes(kDev), 0u);
    EXPECT_EQ(selector_->getPostedBytes(kDev), 0u);
    EXPECT_NEAR(selector_->getTransmitBandwidth(kDev), 1e9, 1.0);
}

// The workers' half of the busy-time meter. Which stretches get charged for
// the bytes is decided by the timestamps this layer hands down -- the
// slice's submit_ts when it reaches the hardware, the poll timestamp when it
// leaves -- and nothing else pins that wiring. Two bursts far apart, each
// moved at one byte per nanosecond, have to read as one byte per nanosecond:
// the wait between them is the NIC having nothing to do, not a slow link.
TEST_F(RdmaWorkersCompletionTest, IdleBetweenBurstsIsNotChargedToTheLink) {
    complete(IBV_WC_SUCCESS);  // kNow -> kNow + kLen
    ASSERT_NEAR(selector_->getTransmitBandwidth(kDev), 1e9, 1.0);
    ASSERT_EQ(selector_->getPostedBytes(kDev), 0u);  // the NIC goes idle

    // The next burst starts ten times its own wire time later.
    const uint64_t start = kNow + 10 * kLen;
    auto* second = makeSlice(start);
    completeAt(second, IBV_WC_SUCCESS, start + kLen);

    EXPECT_NEAR(selector_->getTransmitBandwidth(kDev), 1e9, 1.0);
}

// A slice that fails carries no wire time to learn from, but it does end a
// busy stretch: the meter must not later charge its bytes-less span to the
// next sample.
TEST_F(RdmaWorkersCompletionTest, AFailedSliceEndsItsStretchWithoutTeaching) {
    task_->cancel_requested.store(true);  // stop short of a re-submit
    complete(IBV_WC_RETRY_EXC_ERR);
    ASSERT_DOUBLE_EQ(selector_->getTransmitBandwidth(kDev), 3.125e9);  // seed
    ASSERT_EQ(selector_->getPostedBytes(kDev), 0u);

    // The failure abandoned the meter's interval: the next completion only
    // rebuilds the baselines, and its busy time -- not the failed slice's --
    // is what the completion after that divides its bytes into.
    const uint64_t start = kNow + 10 * kLen;
    auto* second = makeSlice(start);
    completeAt(second, IBV_WC_SUCCESS, start + kLen);
    ASSERT_DOUBLE_EQ(selector_->getTransmitBandwidth(kDev), 3.125e9);

    auto* third = makeSlice(start + kLen);
    completeAt(third, IBV_WC_SUCCESS, start + 2 * kLen);  // one byte per ns
    EXPECT_NEAR(selector_->getTransmitBandwidth(kDev), 1e9, 1.0);
}

// A slice resolved before its completion is polled -- timed out through a
// stale entry on another lane, or cancelled by a teardown -- was re-counted
// here when it was re-queued, and nothing but its completion is left to
// take it out of this lane's count again.
TEST_F(RdmaWorkersCompletionTest, ResolvedSliceStillLeavesTheLaneCount) {
    slice_->word = TIMEOUT;
    slice_->counted_lane = 0;
    ctx_.inflight_slices.store(1);

    complete(IBV_WC_SUCCESS);

    EXPECT_EQ(ctx_.inflight_slices.load(), 0);
    EXPECT_EQ(slice_->charged_dev, -1);
    EXPECT_EQ(selector_->getPostedBytes(kDev), 0u);
}

// A predecessor swept along with a COMPLETED slice did move its bytes; its
// own completion is still coming to credit them, so the meter's interval
// must survive the sweep.
TEST_F(RdmaWorkersCompletionTest, SweptAsCompletedKeepsTheMeterInterval) {
    RdmaTransportTestPeer::retireSweptSlice(*workers_, ctx_, slice_,
                                            kNow + kLen, /*bytes_moved=*/true);
    selector_->noteCompleted(kDev, kLen);
    selector_->maybeSampleTransmit(kDev, kNow + kLen);  // one byte per ns
    EXPECT_NEAR(selector_->getTransmitBandwidth(kDev), 1e9, 1.0);
}

// One swept with FAILED or TIMEOUT did not: its busy time has nothing to be
// divided into, so the next sample only rebuilds the baselines.
TEST_F(RdmaWorkersCompletionTest, SweptAsFailedAbandonsTheMeterInterval) {
    RdmaTransportTestPeer::retireSweptSlice(*workers_, ctx_, slice_,
                                            kNow + kLen);
    selector_->noteCompleted(kDev, kLen);
    selector_->maybeSampleTransmit(kDev, kNow + kLen);
    EXPECT_DOUBLE_EQ(selector_->getTransmitBandwidth(kDev), 3.125e9);  // seed
}

TEST_F(RdmaWorkersCompletionTest, FlushedCompletionTeachesNothing) {
    // A queue pair reset flushes every posted work request at once; those
    // completions carry no wire time.
    task_->cancel_requested.store(true);  // stop short of a re-submit
    complete(IBV_WC_WR_FLUSH_ERR);

    EXPECT_EQ(slice_->charged_dev, -1);
    EXPECT_EQ(selector_->getInflightBytes(kDev), 0u);
    EXPECT_EQ(selector_->getPostedBytes(kDev), 0u);
    EXPECT_DOUBLE_EQ(selector_->getTransmitBandwidth(kDev), 3.125e9);  // seed
}

// A queue pair shared by two worker lanes (the qp_pools layout): the lane
// that sweeps a completion off it is not the lane that enqueued the slices,
// so acknowledge() hands back slices belonging to somebody else. Everything
// the sweep gives back has to land on the owner.
class RdmaWorkersSharedQpTest : public ::testing::Test {
   protected:
    static constexpr int kDev = 1;
    static constexpr uint64_t kLen = 1 << 20;

    // Verbs stand-ins: enough for a context to hand out a protection domain
    // and a completion queue, and for an endpoint to create its queue pairs.
    struct FakeState {
        ibv_context native{};
        ibv_pd pd{};
        ibv_cq cq{};
        ibv_qp qp[8]{};
        int next_qp = 0;
        ibv_mr mr{};
    };
    static FakeState fake;

    static ibv_cq* createCq(ibv_context*, int, void*, ibv_comp_channel*, int) {
        return &fake.cq;
    }
    static int destroyCq(ibv_cq*) { return 0; }
    static ibv_qp* createQp(ibv_pd*, ibv_qp_init_attr*) {
        return &fake.qp[fake.next_qp++];
    }
    static int destroyQp(ibv_qp*) { return 0; }
    static int modifyQp(ibv_qp*, ibv_qp_attr*, int) { return 0; }
    static ibv_mr* regMr(ibv_pd*, void*, size_t, int) { return &fake.mr; }
    static int deregMr(ibv_mr*) { return 0; }

    void SetUp() override {
        fake = FakeState{};
        fake.native.num_comp_vectors = 1;

        auto topology = std::make_shared<Topology>();
        ASSERT_TRUE(topology
                        ->parse(R"({"nics":[
                        {"name":"mc-tcp-0","type":1,"numa_node":0},
                        {"name":"mc-absent-rnic-1","type":0,"numa_node":0}]})")
                        .ok());
        Topology::MemEntry mem;
        mem.name = "cpu:0";
        mem.type = Topology::MEM_HOST;
        mem.numa_node = 0;
        mem.device_list[0].push_back(kDev);
        topology->mem_list_.push_back(mem);
        RdmaTransportTestPeer::bindTopology(transport_, topology);
        ASSERT_EQ(RdmaTransportTestPeer::initializeContexts(transport_), 0u);
        workers_ = RdmaTransportTestPeer::makeWorkers(transport_);
        selector_ = workers_->getDeviceSelector();
        ASSERT_TRUE(selector_->setDeviceAvailable(kDev, true).ok());
        RdmaTransportTestPeer::makeWorkerContexts(*workers_, 2);

        // One queue pair for both lanes, the layout that makes the sweep
        // cross lanes.
        params_ = std::make_shared<RdmaParams>();
        params_->device.num_cq_list = 1;
        params_->endpoint.qp_mul_factor = 1;

        context_ = RdmaTransportTestPeer::contextSet(transport_)[kDev];
        auto& verbs = RdmaContextTestPeer::verbs(*context_);
        verbs.ibv_create_cq = createCq;
        verbs.ibv_destroy_cq = destroyCq;
        verbs.ibv_create_qp = createQp;
        verbs.ibv_destroy_qp = destroyQp;
        verbs.ibv_modify_qp = modifyQp;
        verbs.ibv_reg_mr_default = regMr;
        verbs.ibv_dereg_mr = deregMr;
        RdmaContextTestPeer::bindDevice(*context_, &fake.native, params_);
        ASSERT_EQ(cq_.construct(context_.get(), 4096, 0), 0);
        ASSERT_EQ(notify_cq_.construct(context_.get(), 4096, 0), 0);
        RdmaContextTestPeer::bindResources(*context_, &fake.pd, {&cq_},
                                           &notify_cq_);

        endpoint_ = std::make_shared<RdmaEndPoint>();
        ASSERT_EQ(
            endpoint_->construct(context_.get(), &params_->endpoint, "peer:0"),
            0);
    }

    void TearDown() override {
        // The endpoint goes first: deconstruct() returns its work-request
        // quota through the context's completion queue, which
        // unbindResources takes away below.
        endpoint_.reset();
        for (auto* slice : slices_) RdmaSliceStorage::Get().deallocate(slice);
        for (auto* task : tasks_) task->deref();
        if (workers_) RdmaTransportTestPeer::destroyWorkerContexts(*workers_);
        if (context_) {
            RdmaContextTestPeer::unbindResources(*context_);
            RdmaContextTestPeer::unbindDevice(*context_);
        }
    }

    // A slice charged to the NIC, owned by `lane`, and posted on the shared
    // queue pair.
    RdmaSlice* postSlice(int lane, uint64_t enqueue_ts) {
        auto* task = RdmaTaskStorage::Get().allocate();
        task->num_slices = 1;
        task->status_word = PENDING;
        task->first_error = PENDING;
        task->ref();  // the batch's reference
        task->ref();  // the slice's, dropped by updateSliceStatus()
        auto* slice = RdmaSliceStorage::Get().allocate();
        slice->task = task;
        slice->length = kLen;
        slice->word = PENDING;
        slice->rail_monitor = nullptr;
        slice->enqueue_ts = enqueue_ts;
        slice->owner_worker = lane;
        int chosen = -1;
        EXPECT_TRUE(selector_->allocate(kLen, "cpu:0", chosen).ok());
        slice->source_dev_id = chosen;
        slice->charged_dev = chosen;
        slice->counted_lane = lane;  // as Workers::submit() marks it
        auto& ctx = RdmaTransportTestPeer::workerContext(*workers_, lane);
        ctx.inflight_slices.fetch_add(1);
        // Posted straight away, as the hook inside submitSlices does it.
        RdmaTransportTestPeer::markPosted(*workers_, ctx, slice, enqueue_ts);
        RdmaEndPointTestPeer::pretendPosted(endpoint_, 0, slice);
        tasks_.push_back(task);
        slices_.push_back(slice);
        return slice;
    }

    // Drive one completion for `slice` through lane 0's poll path.
    void completeWith(RdmaSlice* slice, ibv_wc_status status,
                      uint64_t poll_ts = 1'000'000) {
        ibv_wc wc{};
        wc.wr_id = reinterpret_cast<uint64_t>(slice);
        wc.status = status;
        RdmaTransportTestPeer::handleCompletion(
            *workers_, RdmaTransportTestPeer::workerContext(*workers_, 0),
            *context_, wc, poll_ts);
    }

    // What asyncPostSend does with a re-queued slice on its next pass, minus
    // the routing it needs a live segment for: take it off the lane's queue,
    // charge it again and put it back on the queue pair at `post_ts`.
    void repost(RdmaSlice* slice, int lane, uint64_t post_ts = 1'000'000) {
        auto& ctx = RdmaTransportTestPeer::workerContext(*workers_, lane);
        std::vector<RdmaSliceList> drained;
        ctx.queues[slice->priority].pop(drained);
        ASSERT_EQ(drained.size(), 1u);
        ASSERT_EQ(drained[0].first, slice);
        RdmaTransportTestPeer::rechargeSlice(*workers_, slice, kDev);
        RdmaTransportTestPeer::markPosted(*workers_, ctx, slice, post_ts);
        RdmaEndPointTestPeer::pretendPosted(endpoint_, 0, slice);
    }

    RdmaTransport transport_;
    std::shared_ptr<RdmaParams> params_;
    std::shared_ptr<RdmaContext> context_;
    RdmaCQ cq_;
    RdmaCQ notify_cq_;
    std::shared_ptr<RdmaEndPoint> endpoint_;
    std::unique_ptr<Workers> workers_;
    DeviceSelector* selector_ = nullptr;
    std::vector<RdmaTask*> tasks_;
    std::vector<RdmaSlice*> slices_;
};

RdmaWorkersSharedQpTest::FakeState RdmaWorkersSharedQpTest::fake;

TEST_F(RdmaWorkersSharedQpTest, TimeoutSweepGivesTheOtherLaneItsSliceBack) {
    // Lane 1 posted first, so lane 0's timeout sweeps it off the queue pair
    // along with lane 0's own slice.
    auto* theirs = postSlice(/*lane=*/1, /*enqueue_ts=*/0);
    auto* ours = postSlice(/*lane=*/0, /*enqueue_ts=*/0);
    auto& lane0 = RdmaTransportTestPeer::workerContext(*workers_, 0);
    auto& lane1 = RdmaTransportTestPeer::workerContext(*workers_, 1);
    ASSERT_EQ(selector_->getInflightBytes(kDev), 2 * kLen);
    ASSERT_EQ(selector_->getPostedBytes(kDev), 2 * kLen);

    RdmaTransportTestPeer::setSliceTimeout(*workers_, 1'000'000);  // 1 ms
    RdmaTransportTestPeer::expireTimedOutSlices(*workers_, lane0,
                                                /*now_ns=*/2'000'000);

    // Both are terminal and neither is charged to the NIC any more.
    EXPECT_EQ(ours->word, TIMEOUT);
    EXPECT_EQ(theirs->word, TIMEOUT);
    EXPECT_EQ(ours->charged_dev, -1);
    EXPECT_EQ(theirs->charged_dev, -1);
    EXPECT_EQ(selector_->getInflightBytes(kDev), 0u);
    EXPECT_EQ(selector_->getPostedBytes(kDev), 0u);

    // Each lane's own count came back down, and lane 1's set entry is waiting
    // for lane 1 rather than having been erased from under it.
    EXPECT_EQ(lane0.inflight_slices.load(), 0);
    EXPECT_EQ(lane1.inflight_slices.load(), 0);
    EXPECT_TRUE(lane0.inflight_slice_set.empty());
    EXPECT_EQ(lane1.inflight_slice_set.count(theirs), 1u);
    RdmaTransportTestPeer::drainReclaimed(*workers_, lane1);
    EXPECT_TRUE(lane1.inflight_slice_set.empty());
}

// Tearing an endpoint down cancels whatever is still queued on it without
// going through a sweep, so each lane is left holding a slice that will
// never complete. The owner's next pass has to take those out: otherwise
// their NIC charge, their share of its posted backlog and their place in the
// lane's count are held for the life of the process, and the lane never goes
// idle again.
TEST_F(RdmaWorkersSharedQpTest, TeardownLeftoversAreReclaimedByTheirLane) {
    auto* theirs = postSlice(/*lane=*/1, /*enqueue_ts=*/0);
    auto* ours = postSlice(/*lane=*/0, /*enqueue_ts=*/0);
    auto& lane0 = RdmaTransportTestPeer::workerContext(*workers_, 0);
    auto& lane1 = RdmaTransportTestPeer::workerContext(*workers_, 1);
    ASSERT_EQ(selector_->getPostedBytes(kDev), 2 * kLen);

    ASSERT_EQ(endpoint_->deconstruct(), 0);
    ASSERT_EQ(ours->word, CANCELED);
    ASSERT_EQ(theirs->word, CANCELED);

    RdmaTransportTestPeer::setSliceTimeout(*workers_, 1'000'000);
    RdmaTransportTestPeer::expireTimedOutSlices(*workers_, lane0,
                                                /*now_ns=*/2'000'000);
    RdmaTransportTestPeer::expireTimedOutSlices(*workers_, lane1,
                                                /*now_ns=*/2'000'000);

    EXPECT_EQ(selector_->getInflightBytes(kDev), 0u);
    EXPECT_EQ(selector_->getPostedBytes(kDev), 0u);
    EXPECT_EQ(lane0.inflight_slices.load(), 0);
    EXPECT_EQ(lane1.inflight_slices.load(), 0);
    EXPECT_TRUE(lane0.inflight_slice_set.empty());
    EXPECT_TRUE(lane1.inflight_slice_set.empty());
}

// A slice whose attempt fails with a transient error is swept off the queue
// pair and put back on its lane's queue. The sweep takes it out of that
// lane's count, so the re-submit puts it back in -- and whatever guards the
// next decrement has to come back with it. Otherwise the completion that
// finally resolves the slice cannot take it out of the count again, and the
// lane it belongs to never reaches zero: it stops suspending, and it looks
// permanently loaded to submit()'s least-loaded-lane pick. The retry's
// own completion is a transmit sample like any other.
TEST_F(RdmaWorkersSharedQpTest, RetriedSliceIsStillCountedByItsLane) {
    auto params = selector_->getSchedulingParams();
    params.transmit_bandwidth_learning_rate = 0.0;  // adopt outright
    params.transmit_meter_interval_ns = 0;          // sample on demand
    selector_->setSchedulingParams(params);
    ASSERT_TRUE(selector_->setDeviceBandwidth(kDev, 25.0).ok());
    constexpr uint64_t kPosted = 1'000'000;

    auto* slice = postSlice(/*lane=*/0, /*enqueue_ts=*/kPosted);
    auto& lane0 = RdmaTransportTestPeer::workerContext(*workers_, 0);
    ASSERT_EQ(lane0.inflight_slices.load(), 1);

    // First attempt: a transient error, well short of max_retry_count.
    completeWith(slice, IBV_WC_RETRY_EXC_ERR, kPosted + kLen);
    ASSERT_EQ(slice->word, PENDING);
    ASSERT_EQ(slice->retry_count, 1);
    EXPECT_EQ(lane0.inflight_slices.load(), 1);  // queued again, still counted
    EXPECT_EQ(selector_->getPostedBytes(kDev), 0u);

    // Second attempt reaches the hardware and succeeds. The failure ended
    // the meter's interval, so the next sample only rebuilds its baselines:
    // taken at the re-post, as a poll pass in between would, so that the
    // retry's own 1 MiB over its own 1 MiB ns is what the completion
    // measures.
    repost(slice, /*lane=*/0, kPosted + 2 * kLen);
    selector_->maybeSampleTransmit(kDev, kPosted + 2 * kLen);
    ASSERT_DOUBLE_EQ(selector_->getTransmitBandwidth(kDev), 3.125e9);
    completeWith(slice, IBV_WC_SUCCESS, kPosted + 3 * kLen);

    EXPECT_EQ(slice->word, COMPLETED);
    EXPECT_EQ(lane0.inflight_slices.load(), 0);
    EXPECT_EQ(selector_->getInflightBytes(kDev), 0u);
    EXPECT_EQ(selector_->getPostedBytes(kDev), 0u);
    EXPECT_NEAR(selector_->getTransmitBandwidth(kDev), 1e9, 1.0);
}

// A completion sweeps everything posted before it on the queue pair off
// with COMPLETED. Those bytes moved -- their own completions, still to be
// polled, credit them -- so the sweep must not abandon the meter's
// interval the way a FAILED or TIMEOUT sweep does. The completion samples
// before it sweeps, so the evidence is the sample after: 1 MiB over the
// next 1 MiB ns reads 1 GB/s, where a reset would have that sample rebuild
// its baselines and learn nothing.
TEST_F(RdmaWorkersSharedQpTest, CompletionSweepKeepsTheMeterInterval) {
    auto params = selector_->getSchedulingParams();
    params.transmit_bandwidth_learning_rate = 0.0;  // adopt outright
    params.transmit_meter_interval_ns = 0;          // sample on demand
    selector_->setSchedulingParams(params);
    ASSERT_TRUE(selector_->setDeviceBandwidth(kDev, 25.0).ok());
    constexpr uint64_t kPosted = 1'000'000;

    auto* first = postSlice(/*lane=*/0, /*enqueue_ts=*/kPosted);
    auto* second = postSlice(/*lane=*/0, /*enqueue_ts=*/kPosted);
    selector_->maybeSampleTransmit(kDev, kPosted);  // baseline
    ASSERT_DOUBLE_EQ(selector_->getTransmitBandwidth(kDev), 3.125e9);

    // Its own sample, with `first` still posted: 1 MiB over 2 MiB ns.
    completeWith(second, IBV_WC_SUCCESS, kPosted + 2 * kLen);
    ASSERT_EQ(first->word, COMPLETED);
    ASSERT_EQ(second->word, COMPLETED);
    ASSERT_EQ(selector_->getPostedBytes(kDev), 0u);
    ASSERT_NEAR(selector_->getTransmitBandwidth(kDev), 0.5e9, 1.0);

    auto* third = postSlice(/*lane=*/0, /*enqueue_ts=*/kPosted + 2 * kLen);
    completeWith(third, IBV_WC_SUCCESS, kPosted + 3 * kLen);

    EXPECT_NEAR(selector_->getTransmitBandwidth(kDev), 1e9, 1.0);
}

// Same retry, on the layout this fixture exists for: lane 1 enqueued the
// slice, lane 0 polls the queue pair they share. The re-submit goes onto
// lane 0's queue, so lane 0 is the lane counting it from then on and the
// slice's ownership has to move with it. Left pointing at lane 1, the
// completion that finally resolves it discounts a lane that is no longer
// carrying it -- lane 1 goes negative and lane 0 never comes back down.
TEST_F(RdmaWorkersSharedQpTest, RetryPolledByAnotherLaneMovesOwnership) {
    auto* slice = postSlice(/*lane=*/1, /*enqueue_ts=*/0);
    auto& lane0 = RdmaTransportTestPeer::workerContext(*workers_, 0);
    auto& lane1 = RdmaTransportTestPeer::workerContext(*workers_, 1);
    ASSERT_EQ(lane1.inflight_slices.load(), 1);

    completeWith(slice, IBV_WC_RETRY_EXC_ERR);  // polled by lane 0
    ASSERT_EQ(slice->word, PENDING);
    EXPECT_EQ(lane0.inflight_slices.load(), 1);  // lane 0 queued it
    EXPECT_EQ(lane1.inflight_slices.load(), 0);

    repost(slice, /*lane=*/0);
    completeWith(slice, IBV_WC_SUCCESS);

    EXPECT_EQ(slice->word, COMPLETED);
    EXPECT_EQ(lane0.inflight_slices.load(), 0);
    EXPECT_EQ(lane1.inflight_slices.load(), 0);
    EXPECT_TRUE(lane0.inflight_slice_set.empty());
}

// The same retry seen from the lane the slice is leaving. Lane 1's set still
// holds an entry for it: the handover goes through lane 1's reclaim list, and
// until lane 1 drains that list its sweep can still walk onto the entry. What
// decides whether a lane may take an entry out of its set is whether the entry
// is in that set -- not whether the lane still owns the slice, which the retry
// has just changed. Keyed on ownership, lane 1 hands its own entry to lane 0,
// which does not have it, and lane 1 keeps walking a slice it no longer holds.
TEST_F(RdmaWorkersSharedQpTest, StaleSetEntryIsTakenOutByTheLaneHoldingIt) {
    auto* slice = postSlice(/*lane=*/1, /*enqueue_ts=*/0);
    auto& lane1 = RdmaTransportTestPeer::workerContext(*workers_, 1);

    completeWith(slice, IBV_WC_RETRY_EXC_ERR);  // lane 0 retries it
    ASSERT_EQ(lane1.inflight_slice_set.count(slice), 1u);

    // Lane 1 drained just before lane 0 handed the entry over, so the entry
    // is in its set with nothing queued to take it out.
    RdmaTransportTestPeer::clearReclaimed(lane1);

    RdmaTransportTestPeer::setSliceTimeout(*workers_, 1'000'000);
    RdmaTransportTestPeer::expireTimedOutSlices(*workers_, lane1,
                                                /*now_ns=*/2'000'000);

    EXPECT_TRUE(lane1.inflight_slice_set.empty());
}

// The other half of that window. While the slice waits on lane 0's queue for
// its re-post, lane 1's sweep resolves it. Posting it now would move bytes for
// a transfer that is already over, and its completion returns early at
// `word != PENDING` -- before any discount -- so lane 0's count would never
// come back down. The pre-post check has to drop an already-resolved slice the
// same way it drops a cancelled one.
TEST_F(RdmaWorkersSharedQpTest, SliceResolvedWhileQueuedIsDroppedNotPosted) {
    auto* slice = postSlice(/*lane=*/1, /*enqueue_ts=*/0);
    auto& lane0 = RdmaTransportTestPeer::workerContext(*workers_, 0);
    auto& lane1 = RdmaTransportTestPeer::workerContext(*workers_, 1);

    completeWith(slice, IBV_WC_RETRY_EXC_ERR);  // re-queued on lane 0
    ASSERT_EQ(lane0.inflight_slices.load(), 1);

    RdmaTransportTestPeer::clearReclaimed(lane1);
    RdmaTransportTestPeer::setSliceTimeout(*workers_, 1'000'000);
    RdmaTransportTestPeer::expireTimedOutSlices(*workers_, lane1,
                                                /*now_ns=*/2'000'000);
    ASSERT_EQ(slice->word, TIMEOUT);
    ASSERT_FALSE(slice->task->cancel_requested.load());

    // What lane 0's next pass does with it before generating a post path.
    EXPECT_TRUE(
        RdmaTransportTestPeer::dropUnpostableSlice(*workers_, lane0, slice));
    EXPECT_EQ(lane0.inflight_slices.load(), 0);
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

}  // namespace
}  // namespace tent
}  // namespace mooncake
