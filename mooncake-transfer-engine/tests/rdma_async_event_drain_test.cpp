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

// WorkerPool::doProcessContextEvents() must empty the async event fd on every
// call: the fd is edge-triggered, so anything left queued is stranded until the
// next event arrives. These tests replace the event source with a scripted
// queue via linker wrapping, so no RDMA device is needed.

#include <gtest/gtest.h>

#include <cerrno>
#include <cstring>
#include <deque>
#include <memory>

#include <glog/logging.h>
#include <infiniband/verbs.h>

#include "error.h"
#include "transport/rdma_transport/rdma_context.h"
#include "transport/rdma_transport/rdma_transport.h"
#include "transport/rdma_transport/worker_pool.h"

#if defined(__has_feature)
#define MC_HAS_FEATURE(x) __has_feature(x)
#else
#define MC_HAS_FEATURE(x) 0
#endif
#if defined(__SANITIZE_ADDRESS__) || MC_HAS_FEATURE(address_sanitizer) || \
    MC_HAS_FEATURE(leak_sanitizer)
#include <sanitizer/lsan_interface.h>
#define MC_LSAN_IGNORE_OBJECT(p) __lsan_ignore_object(p)

// Suppress false positives from libnuma.so's process-wide static cache
// allocated in numa_node_to_cpus() which is intentionally retained until exit.
extern "C" __attribute__((weak, visibility("default"))) const char *
__lsan_default_suppressions() {
    return "leak:libnuma.so\n";
}
#else
#define MC_LSAN_IGNORE_OBJECT(p) ((void)(p))
#endif

using namespace mooncake;

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

extern "C" int __wrap_ibv_get_async_event(struct ibv_context *context,
                                          struct ibv_async_event *event) {
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

extern "C" void __wrap_ibv_ack_async_event(struct ibv_async_event *event) {
    (void)event;
    g_script.ack_calls++;
}

namespace mooncake {

class WorkerPoolTestPeer {
   public:
    static int processContextEvents(WorkerPool &worker_pool) {
        return worker_pool.doProcessContextEvents();
    }
};

}  // namespace mooncake

namespace {

// Only event types whose handlers touch nothing a device-less context lacks.
// IBV_EVENT_COMM_EST is not claimed by handleContextEvent(), so the caller acks
// it; IBV_EVENT_PORT_ACTIVE is claimed and only stores a recovery timestamp, so
// the handler acks it. Together they cover both ack paths.
constexpr ibv_event_type kUnclaimedEvent = IBV_EVENT_COMM_EST;
constexpr ibv_event_type kClaimedEvent = IBV_EVENT_PORT_ACTIVE;

class AsyncEventDrainTest : public ::testing::Test {
   protected:
    void SetUp() override {
        // The pool's monitorWorker busy-polls epoll_wait() on an invalid fd
        // for its whole lifetime because this context was never opened.
        // Silence it so gtest failures stay readable.
        previous_min_log_level_ = FLAGS_minloglevel;
        FLAGS_minloglevel = google::GLOG_FATAL;

        // Intentional leak: ~RdmaTransport dereferences metadata_, which is
        // null until install(). We only need it as RdmaContext's owner, same
        // as rdma_endpoint_state_test.
        transport_ = new RdmaTransport();
        MC_LSAN_IGNORE_OBJECT(transport_);
        context_ = std::make_unique<RdmaContext>(*transport_, "unused");
        // Always fails, on any host, because no device is named "unused" --
        // which keeps the context from starting a second worker pool that
        // would race us for the scripted queue. It still creates the endpoint
        // store first, and monitorWorker's reclaim tick needs that to exist.
        context_->construct();

        g_script = AsyncEventScript{};
        worker_pool_ = std::make_unique<WorkerPool>(*context_);
    }

    void TearDown() override {
        worker_pool_.reset();
        context_.reset();
        FLAGS_minloglevel = previous_min_log_level_;
    }

    int processContextEvents() {
        return WorkerPoolTestPeer::processContextEvents(*worker_pool_);
    }

    int previous_min_log_level_ = 0;
    RdmaTransport *transport_ = nullptr;
    std::unique_ptr<RdmaContext> context_;
    std::unique_ptr<WorkerPool> worker_pool_;
};

// The regression: a burst arriving between two epoll wakeups must be consumed
// in full, not one event at a time.
TEST_F(AsyncEventDrainTest, DrainsEveryQueuedEvent) {
    constexpr int kBurst = 5;
    for (int i = 0; i < kBurst; ++i)
        g_script.pending.push_back(kUnclaimedEvent);

    EXPECT_EQ(processContextEvents(), 0);

    EXPECT_TRUE(g_script.pending.empty())
        << "one epoll wakeup must drain the whole async event queue; "
        << g_script.pending.size() << " event(s) were left stranded";
    // kBurst reads plus the trailing EAGAIN that ends the drain.
    EXPECT_EQ(g_script.get_calls, kBurst + 1);
    EXPECT_EQ(g_script.ack_calls, kBurst);
}

// Both ack paths must drain, and neither may ack twice or not at all.
TEST_F(AsyncEventDrainTest, DrainsClaimedAndUnclaimedEventsAlike) {
    g_script.pending.push_back(kUnclaimedEvent);
    g_script.pending.push_back(kClaimedEvent);
    g_script.pending.push_back(kUnclaimedEvent);

    EXPECT_EQ(processContextEvents(), 0);

    EXPECT_TRUE(g_script.pending.empty());
    EXPECT_EQ(g_script.get_calls, 4);
    EXPECT_EQ(g_script.ack_calls, 3);
}

// A wakeup with nothing pending is a drained fd, not a failure.
TEST_F(AsyncEventDrainTest, EmptyQueueIsNotAnError) {
    EXPECT_EQ(processContextEvents(), 0);

    EXPECT_EQ(g_script.get_calls, 1);
    EXPECT_EQ(g_script.ack_calls, 0);
}

// A signal must not end the drain early, or the events behind it are stranded
// exactly as they were before the fix.
TEST_F(AsyncEventDrainTest, InterruptedReadIsRetried) {
    g_script.pending.push_back(kUnclaimedEvent);
    g_script.pending.push_back(kUnclaimedEvent);
    g_script.pending_eintr = 1;

    EXPECT_EQ(processContextEvents(), 0);

    EXPECT_TRUE(g_script.pending.empty());
    // One EINTR, two reads, one trailing EAGAIN.
    EXPECT_EQ(g_script.get_calls, 4);
    EXPECT_EQ(g_script.ack_calls, 2);
}

// A genuine read failure still aborts and propagates, so the loop cannot spin
// forever on a broken fd.
TEST_F(AsyncEventDrainTest, RealReadErrorStopsTheDrain) {
    g_script.pending.push_back(kUnclaimedEvent);
    g_script.drained_errno = EIO;

    EXPECT_EQ(processContextEvents(), ERR_CONTEXT);

    EXPECT_EQ(g_script.get_calls, 2);
    EXPECT_EQ(g_script.ack_calls, 1);
}

}  // namespace
