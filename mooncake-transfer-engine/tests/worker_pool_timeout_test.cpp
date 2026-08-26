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

#include <gtest/gtest.h>
#include <infiniband/verbs.h>

#include <cstdint>
#include <memory>
#include <vector>

#include "config.h"
#include "transport/rdma_transport/endpoint_store.h"
#include "transport/rdma_transport/rdma_context.h"
#include "transport/rdma_transport/rdma_endpoint.h"
#include "transport/rdma_transport/rdma_transport.h"
#include "transport/rdma_transport/worker_pool.h"
#include "transport/transport.h"

#if defined(__has_feature)
#define MC_HAS_FEATURE(x) __has_feature(x)
#else
#define MC_HAS_FEATURE(x) 0
#endif
#if defined(__SANITIZE_ADDRESS__) || MC_HAS_FEATURE(address_sanitizer)
#include <sanitizer/lsan_interface.h>
#define MC_LSAN_IGNORE_OBJECT(p) __lsan_ignore_object(p)
#else
#define MC_LSAN_IGNORE_OBJECT(p) ((void)(p))
#endif

using namespace mooncake;

namespace mooncake {

class RdmaContextTestPeer {
   public:
    static void setEndpointStore(RdmaContext &context,
                                 std::shared_ptr<EndpointStore> store) {
        context.endpoint_store_ = std::move(store);
    }
};

class WorkerPoolTestPeer {
   public:
    static std::unique_ptr<WorkerPool> createWithoutThreads(
        RdmaContext &context) {
        return std::unique_ptr<WorkerPool>(
            new WorkerPool(context, /*numa_socket_id=*/0,
                           /*start_workers=*/false));
    }

    static void trackPostedSlices(
        WorkerPool &worker_pool,
        const std::vector<Transport::Slice *> &slice_list, size_t count) {
        worker_pool.trackPostedSlices(slice_list, count);
    }

    static void untrackPostedSlices(WorkerPool &worker_pool,
                                    const ibv_wc *wc_list, int count) {
        worker_pool.untrackPostedSlices(wc_list, count);
    }

    static std::vector<Transport::Slice *> reapTimedOutPostedSlices(
        WorkerPool &worker_pool, uint64_t current_ts) {
        return worker_pool.reapTimedOutPostedSlices(current_ts);
    }

    static void handleTimedOutPostedSlices(
        WorkerPool &worker_pool,
        const std::vector<Transport::Slice *> &timed_out_slices) {
        worker_pool.handleTimedOutPostedSlices(timed_out_slices);
    }

    static bool isRailAvailable(WorkerPool &worker_pool,
                                const std::string &peer_nic_path) {
        return worker_pool.isRailAvailable(peer_nic_path);
    }

    static int redispatchCounter(WorkerPool &worker_pool) {
        return worker_pool.redispatch_counter_.load(std::memory_order_relaxed);
    }
};

}  // namespace mooncake

namespace {

class FakeEndpointStore : public EndpointStore {
   public:
    explicit FakeEndpointStore(std::shared_ptr<RdmaEndPoint> endpoint)
        : endpoint_(std::move(endpoint)) {}

    std::shared_ptr<RdmaEndPoint> getEndpoint(
        const std::string &peer_nic_path) override {
        return peer_nic_path == peer_nic_path_ ? endpoint_ : nullptr;
    }

    std::shared_ptr<RdmaEndPoint> getEndpointByPtr(
        const RdmaEndPoint *endpoint_ptr) override {
        return endpoint_.get() == endpoint_ptr ? endpoint_ : nullptr;
    }

    std::shared_ptr<RdmaEndPoint> insertEndpoint(
        const std::string &peer_nic_path, RdmaContext *context) override {
        (void)context;
        peer_nic_path_ = peer_nic_path;
        if (endpoint_) endpoint_->setPeerNicPath(peer_nic_path);
        return endpoint_;
    }

    int deleteEndpoint(const std::string &peer_nic_path) override {
        if (peer_nic_path != peer_nic_path_) return -1;
        return deleteEndpointByPtr(endpoint_.get());
    }

    int deleteEndpointByPtr(
        const RdmaEndPoint *endpoint_ptr,
        std::string *deleted_peer_nic_path = nullptr) override {
        if (deleted_peer_nic_path) *deleted_peer_nic_path = peer_nic_path_;
        if (endpoint_.get() != endpoint_ptr) return -1;
        deleted_endpoint_count_++;
        if (endpoint_) endpoint_->beginDestroy();
        return 0;
    }

    void evictEndpoint() override {}
    void reclaimEndpoint() override {}
    size_t getSize() override { return endpoint_ ? 1u : 0u; }
    int destroyQPs() override { return 0; }
    int disconnectQPs() override { return 0; }
    size_t getTotalQPNumber() override { return 0; }
    size_t waitingListSize() const override { return 0; }
    void testOnlyInsertWaiting(std::shared_ptr<RdmaEndPoint> ep) override {
        endpoint_ = std::move(ep);
    }

    int deletedEndpointCount() const { return deleted_endpoint_count_; }

   private:
    std::shared_ptr<RdmaEndPoint> endpoint_;
    std::string peer_nic_path_ = "10.0.0.2@mock_rdma1";
    int deleted_endpoint_count_ = 0;
};

class WorkerPoolTimeoutTest : public ::testing::Test {
   protected:
    int64_t saved_slice_timeout_ = 0;
    uint64_t saved_rail_pause_seconds_ = 0;
    RdmaTransport *transport_ = nullptr;
    std::unique_ptr<RdmaContext> context_;
    std::unique_ptr<WorkerPool> worker_pool_;
    std::shared_ptr<RdmaEndPoint> endpoint_;
    std::shared_ptr<FakeEndpointStore> endpoint_store_;

    void SetUp() override {
        saved_slice_timeout_ = globalConfig().slice_timeout;
        saved_rail_pause_seconds_ = globalConfig().rdma_rail_pause_seconds;

        // Leaked on purpose: RdmaTransport's destructor expects install() to
        // have initialized metadata. These tests only need a stable reference
        // for RdmaContext and never touch transport runtime state.
        transport_ = new RdmaTransport();
        MC_LSAN_IGNORE_OBJECT(transport_);
        context_ = std::make_unique<RdmaContext>(*transport_, "mock_rdma0");
        endpoint_ = std::make_shared<RdmaEndPoint>(*context_);
        endpoint_->setPeerNicPath("10.0.0.2@mock_rdma1");
        endpoint_store_ = std::make_shared<FakeEndpointStore>(endpoint_);
        RdmaContextTestPeer::setEndpointStore(*context_, endpoint_store_);
        worker_pool_ = WorkerPoolTestPeer::createWithoutThreads(*context_);
    }

    void TearDown() override {
        worker_pool_.reset();
        endpoint_store_.reset();
        endpoint_.reset();
        context_.reset();
        globalConfig().slice_timeout = saved_slice_timeout_;
        globalConfig().rdma_rail_pause_seconds = saved_rail_pause_seconds_;
    }
};

Transport::Slice makeSlice(int64_t post_ts) {
    Transport::Slice slice{};
    slice.ts = post_ts;
    slice.status = Transport::Slice::POSTED;
    slice.rdma.retry_cnt = 0;
    slice.rdma.max_retry_cnt = 1;
    return slice;
}

TEST_F(WorkerPoolTimeoutTest, DisabledSliceTimeoutDoesNotTrackPostedSlices) {
    globalConfig().slice_timeout = 0;
    auto slice = makeSlice(1000000000ll);
    std::vector<Transport::Slice *> slices{&slice};

    WorkerPoolTestPeer::trackPostedSlices(*worker_pool_, slices, slices.size());

    auto timed_out = WorkerPoolTestPeer::reapTimedOutPostedSlices(
        *worker_pool_, /*current_ts=*/100000000000ll);
    EXPECT_TRUE(timed_out.empty());
}

TEST_F(WorkerPoolTimeoutTest, ReapsPostedSliceWhenNoCompletionArrives) {
    globalConfig().slice_timeout = 1;
    auto expired = makeSlice(1000000000ll);
    auto fresh = makeSlice(1500000000ll);
    std::vector<Transport::Slice *> slices{&expired, &fresh};

    WorkerPoolTestPeer::trackPostedSlices(*worker_pool_, slices, slices.size());

    auto timed_out = WorkerPoolTestPeer::reapTimedOutPostedSlices(
        *worker_pool_, /*current_ts=*/2100000001ull);
    ASSERT_EQ(timed_out.size(), 1u);
    EXPECT_EQ(timed_out[0], &expired);

    timed_out = WorkerPoolTestPeer::reapTimedOutPostedSlices(
        *worker_pool_, /*current_ts=*/2100000001ull);
    EXPECT_TRUE(timed_out.empty())
        << "a timed-out slice should be removed from the posted set exactly "
           "once";
}

TEST_F(WorkerPoolTimeoutTest, CompletionUntracksPostedSliceBeforeTimeout) {
    globalConfig().slice_timeout = 1;
    auto completed = makeSlice(1000000000ll);
    std::vector<Transport::Slice *> slices{&completed};

    WorkerPoolTestPeer::trackPostedSlices(*worker_pool_, slices, slices.size());

    ibv_wc wc{};
    wc.wr_id = reinterpret_cast<uint64_t>(&completed);
    WorkerPoolTestPeer::untrackPostedSlices(*worker_pool_, &wc, 1);

    auto timed_out = WorkerPoolTestPeer::reapTimedOutPostedSlices(
        *worker_pool_, /*current_ts=*/100000000000ull);
    EXPECT_TRUE(timed_out.empty())
        << "a slice that produced any CQE must not later be treated as a "
           "software timeout";
}

TEST_F(WorkerPoolTimeoutTest,
       TimedOutSliceWithoutAlternativeDeletesEndpointOnly) {
    globalConfig().slice_timeout = 1;
    globalConfig().rdma_rail_pause_seconds = 30;
    auto expired = makeSlice(1000000000ll);
    expired.peer_nic_path = "10.0.0.2@mock_rdma1";
    expired.length = 4096;
    expired.rdma.dest_addr = 0xabc000;
    expired.rdma.endpoint = endpoint_.get();
    std::vector<Transport::Slice *> slices{&expired};

    WorkerPoolTestPeer::trackPostedSlices(*worker_pool_, slices, slices.size());
    auto timed_out = WorkerPoolTestPeer::reapTimedOutPostedSlices(
        *worker_pool_, /*current_ts=*/2100000001ull);
    ASSERT_EQ(timed_out.size(), 1u);

    EXPECT_EQ(endpoint_store_->deletedEndpointCount(), 0);
    EXPECT_EQ(WorkerPoolTestPeer::redispatchCounter(*worker_pool_), 0);
    EXPECT_TRUE(WorkerPoolTestPeer::isRailAvailable(*worker_pool_,
                                                    expired.peer_nic_path));

    WorkerPoolTestPeer::handleTimedOutPostedSlices(*worker_pool_, timed_out);

    EXPECT_EQ(endpoint_store_->deletedEndpointCount(), 1)
        << "timeout handling should retire the endpoint used by the posted "
           "slice so the existing QP flush/error path can drive retry/fail";
    EXPECT_EQ(WorkerPoolTestPeer::redispatchCounter(*worker_pool_), 0)
        << "without a known alternate peer rail, timeout should not pause the "
           "only known rail";
    EXPECT_TRUE(WorkerPoolTestPeer::isRailAvailable(*worker_pool_,
                                                    expired.peer_nic_path));
    EXPECT_TRUE(endpoint_->retired());
}

TEST_F(WorkerPoolTimeoutTest, TimedOutBatchDeduplicatesEndpointDelete) {
    globalConfig().slice_timeout = 1;
    globalConfig().rdma_rail_pause_seconds = 30;
    auto first = makeSlice(1000000000ll);
    auto second = makeSlice(1000000000ll);
    for (auto *slice : {&first, &second}) {
        slice->peer_nic_path = "10.0.0.2@mock_rdma1";
        slice->length = 4096;
        slice->rdma.dest_addr = 0xabc000;
        slice->rdma.endpoint = endpoint_.get();
    }

    WorkerPoolTestPeer::handleTimedOutPostedSlices(*worker_pool_,
                                                   {&first, &second});

    EXPECT_EQ(endpoint_store_->deletedEndpointCount(), 1);
    EXPECT_EQ(WorkerPoolTestPeer::redispatchCounter(*worker_pool_), 0);
    EXPECT_TRUE(WorkerPoolTestPeer::isRailAvailable(*worker_pool_,
                                                    first.peer_nic_path));
}

}  // namespace
