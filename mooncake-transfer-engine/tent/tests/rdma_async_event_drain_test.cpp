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

// Workers::handleContextEvents() must empty the async event fd on every call:
// the fd is edge-triggered, so anything left queued is stranded until an
// unrelated later event releases it -- and a stranded IBV_EVENT_PORT_ACTIVE
// leaves its context paused, silently failing every transfer on that NIC.
// These tests replace the event source with a scripted queue via linker
// wrapping, so no RDMA device is needed.

#include <gtest/gtest.h>
#include <infiniband/verbs.h>

#include <cerrno>
#include <cstring>
#include <deque>
#include <memory>

#include "tent/runtime/topology.h"
#include "tent/transport/rdma/context.h"
#include "tent/transport/rdma/params.h"
#include "tent/transport/rdma/rdma_transport.h"
#include "tent/transport/rdma/workers.h"

namespace {

// Stands in for the kernel's async event queue behind async_fd.
struct AsyncEventScript {
    std::deque<ibv_event_type> pending;
    // Reported once `pending` runs dry. EAGAIN mimics a drained non-blocking
    // fd; anything else mimics a real read failure.
    int drained_errno = EAGAIN;
    // Fail this many reads with EINTR before serving the queue.
    int pending_eintr = 0;
    int get_calls = 0;
    int ack_calls = 0;
};

AsyncEventScript g_script;

}  // namespace

extern "C" int __wrap_ibv_get_async_event(struct ibv_context* context,
                                          struct ibv_async_event* event) {
    (void)context;  // Never dereferenced by the code under test.
    g_script.get_calls++;
    if (g_script.pending_eintr > 0) {
        g_script.pending_eintr--;
        errno = EINTR;
        return -1;
    }
    if (g_script.pending.empty()) {
        errno = g_script.drained_errno;
        return -1;
    }
    memset(event, 0, sizeof(*event));
    event->event_type = g_script.pending.front();
    g_script.pending.pop_front();
    return 0;
}

extern "C" void __wrap_ibv_ack_async_event(struct ibv_async_event* event) {
    (void)event;
    g_script.ack_calls++;
}

namespace mooncake {
namespace tent {

// Friend accessor for driving the event loop without a full install().
class RdmaTransportTestPeer {
   public:
    static void bindTopology(RdmaTransport& transport,
                             std::shared_ptr<Topology> topology) {
        transport.local_topology_ = topology;
        transport.local_buffer_manager_.setTopology(topology);
        transport.params_ = std::make_shared<RdmaParams>();
        transport.conf_ = std::make_shared<Config>();
    }

    static size_t initializeContexts(RdmaTransport& transport) {
        return transport.initializeContexts();
    }

    static std::unique_ptr<Workers> makeWorkers(RdmaTransport& transport) {
        return std::make_unique<Workers>(&transport);
    }

    static int handleContextEvents(Workers& workers, int dev_id,
                                   std::shared_ptr<RdmaContext>& context) {
        return workers.handleContextEvents(dev_id, context);
    }

    static RdmaContextSet& contextSet(RdmaTransport& transport) {
        return transport.context_set_;
    }
};

namespace {

// Event types whose handlers touch nothing a device-less context lacks.
// IBV_EVENT_COMM_EST falls through applyContextEvent()'s default label;
// IBV_EVENT_PORT_ACTIVE takes the recovery path, where resume() is a no-op on
// an inert context and the link-speed refresh declines without a device.
// Together they cover a handled and an unhandled event.
constexpr ibv_event_type kUnhandledEvent = IBV_EVENT_COMM_EST;
constexpr ibv_event_type kHandledEvent = IBV_EVENT_PORT_ACTIVE;

class AsyncEventDrainTest : public ::testing::Test {
   protected:
    void SetUp() override {
        topology_ = std::make_shared<Topology>();
        // No device is named "mc-absent-rnic-0" on any host, so the context
        // stays inert: construct() still builds the endpoint store, but no
        // real async fd is ever opened and only the scripted queue answers.
        ASSERT_TRUE(
            topology_
                ->parse(
                    R"({"nics":[{"name":"mc-absent-rnic-0","type":0,"numa_node":0}]})")
                .ok());
        RdmaTransportTestPeer::bindTopology(transport_, topology_);
        ASSERT_EQ(RdmaTransportTestPeer::initializeContexts(transport_), 0u);
        workers_ = RdmaTransportTestPeer::makeWorkers(transport_);

        g_script = AsyncEventScript{};
    }

    int drain() {
        auto& context = RdmaTransportTestPeer::contextSet(transport_)[kDev];
        return RdmaTransportTestPeer::handleContextEvents(*workers_, kDev,
                                                          context);
    }

    static constexpr int kDev = 0;
    std::shared_ptr<Topology> topology_;
    RdmaTransport transport_;
    std::unique_ptr<Workers> workers_;
};

// The regression: a burst arriving between two epoll wakeups must be consumed
// in full, not one event at a time.
TEST_F(AsyncEventDrainTest, DrainsEveryQueuedEvent) {
    constexpr int kBurst = 5;
    for (int i = 0; i < kBurst; ++i)
        g_script.pending.push_back(kUnhandledEvent);

    EXPECT_EQ(drain(), 0);

    EXPECT_TRUE(g_script.pending.empty())
        << "one epoll wakeup must drain the whole async event queue; "
        << g_script.pending.size() << " event(s) were left stranded";
    // kBurst reads plus the trailing EAGAIN that ends the drain.
    EXPECT_EQ(g_script.get_calls, kBurst + 1);
    EXPECT_EQ(g_script.ack_calls, kBurst);
}

// A PORT_ACTIVE queued behind a burst is the case that wedged a NIC for good,
// so it must be reached and acked like any other event.
TEST_F(AsyncEventDrainTest, ReachesHandledEventQueuedBehindOthers) {
    g_script.pending.push_back(kUnhandledEvent);
    g_script.pending.push_back(kUnhandledEvent);
    g_script.pending.push_back(kHandledEvent);

    EXPECT_EQ(drain(), 0);

    EXPECT_TRUE(g_script.pending.empty());
    EXPECT_EQ(g_script.get_calls, 4);
    EXPECT_EQ(g_script.ack_calls, 3);
}

// A wakeup with nothing pending is a drained fd, not a failure.
TEST_F(AsyncEventDrainTest, EmptyQueueIsNotAnError) {
    EXPECT_EQ(drain(), 0);

    EXPECT_EQ(g_script.get_calls, 1);
    EXPECT_EQ(g_script.ack_calls, 0);
}

// A signal must not end the drain early, or the events behind it are stranded
// exactly as they were before the fix.
TEST_F(AsyncEventDrainTest, InterruptedReadIsRetried) {
    g_script.pending.push_back(kUnhandledEvent);
    g_script.pending.push_back(kUnhandledEvent);
    g_script.pending_eintr = 1;

    EXPECT_EQ(drain(), 0);

    EXPECT_TRUE(g_script.pending.empty());
    // One EINTR, two reads, one trailing EAGAIN.
    EXPECT_EQ(g_script.get_calls, 4);
    EXPECT_EQ(g_script.ack_calls, 2);
}

// A genuine read failure still aborts and propagates, so the loop cannot spin
// forever on a broken fd.
TEST_F(AsyncEventDrainTest, RealReadErrorStopsTheDrain) {
    g_script.pending.push_back(kUnhandledEvent);
    g_script.drained_errno = EIO;

    EXPECT_EQ(drain(), -1);

    EXPECT_EQ(g_script.get_calls, 2);
    EXPECT_EQ(g_script.ack_calls, 1);
}

}  // namespace
}  // namespace tent
}  // namespace mooncake
