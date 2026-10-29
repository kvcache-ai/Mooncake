// Copyright 2026 KVCache.AI
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//     http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <gtest/gtest.h>
#include <glog/logging.h>

#include <chrono>
#include <deque>
#include <memory>
#include <thread>
#include <unordered_map>

#include "multi_transport.h"
#include "transport/rdma_transport/endpoint_store.h"
#include "transport/rdma_transport/rdma_endpoint.h"
#include "transport/rdma_transport/rdma_transport.h"
#include "transport/rdma_transport/worker_pool.h"

namespace {
// Only the verbs boundary is simulated. Posting, timeout handling, endpoint
// retirement, CQ accounting and public status queries use production code.
std::unordered_map<ibv_cq *, std::deque<ibv_wc>> completions;
std::unordered_map<ibv_qp *, std::vector<uint64_t>> posted;
int modify_error = 0;
size_t post_count = 0;
int reject_after = -1;

int postSend(ibv_qp *qp, ibv_send_wr *wr, ibv_send_wr **bad) {
    for (; wr; wr = wr->next) {
        if (reject_after == 0) {
            *bad = wr;
            reject_after = -1;
            return EIO;
        }
        if (reject_after > 0) --reject_after;
        posted[qp].push_back(wr->wr_id);
        ++post_count;
    }
    return 0;
}

int pollCq(ibv_cq *cq, int count, ibv_wc *wc) {
    auto &queue = completions[cq];
    int n = 0;
    while (n < count && !queue.empty()) {
        wc[n++] = queue.front();
        queue.pop_front();
    }
    return n;
}
}  // namespace

extern "C" ibv_qp *__wrap_ibv_create_qp(ibv_pd *, ibv_qp_init_attr *attr) {
    auto *qp = new ibv_qp{};
    qp->context = attr->send_cq->context;
    qp->send_cq = attr->send_cq;
    qp->state = IBV_QPS_RTS;
    return qp;
}
extern "C" int __wrap_ibv_modify_qp(ibv_qp *qp, ibv_qp_attr *attr, int) {
    if (modify_error) return modify_error;
    qp->state = attr->qp_state;
    return 0;
}
extern "C" int __wrap_ibv_destroy_qp(ibv_qp *qp) {
    posted.erase(qp);
    delete qp;
    return 0;
}

namespace mooncake {
class WorkerPoolTestPeer {
   public:
    static void stop(WorkerPool &pool) {
        pool.workers_running_.store(false);
        pool.cond_var_.notify_all();
        for (auto &thread : pool.worker_thread_) thread.join();
    }
    static void queue(WorkerPool &pool, Transport::Slice *slice) {
        pool.collective_slice_queue_[0][slice->peerNicPath()].push_back(slice);
        ++pool.submitted_slice_count_;
    }
    static void post(WorkerPool &pool) { pool.performPostSend(0); }
    static void poll(WorkerPool &pool) { pool.performPollCq(0); }
    static uint64_t processed(WorkerPool &pool) {
        return pool.processed_slice_count_.load();
    }
    static void checkTimeouts(WorkerPool &pool) {
        pool.last_timeout_check_ns_[0] = 0;
        pool.checkSliceTimeouts(0);
    }
};
class RdmaTransportTestPeer {
   public:
    static void bind(RdmaTransport &transport) {
        transport.metadata_ = std::make_shared<TransferMetadata>(P2PHANDSHAKE);
        transport.local_server_name_ = "timeout-test";
    }
};
class RdmaContextTestPeer {
   public:
    static void bind(RdmaContext &context, ibv_cq *cq) {
        context.cq_list_.emplace_back();
        context.cq_list_.back().native = cq;
        cq->cq_context = &context.cq_list_.back().outstanding;
        context.endpoint_store_ = std::make_shared<SIEVEEndpointStore>(8);
        context.transfer_worker_count_ = 1;
        context.endpoint_lifecycle_locks_.push_back(
            std::make_unique<std::mutex>());
        context.active_.store(true);
        context.worker_pool_ = std::make_shared<WorkerPool>(context);
        WorkerPoolTestPeer::stop(*context.worker_pool_);
    }
    static WorkerPool &pool(RdmaContext &context) {
        return *context.worker_pool_;
    }
    static void insert(RdmaContext &context, const std::string &path,
                       const std::shared_ptr<RdmaEndPoint> &endpoint) {
        std::static_pointer_cast<SIEVEEndpointStore>(context.endpoint_store_)
            ->testOnlyInsertEndpoint(path, endpoint);
    }
    static void clean(RdmaContext &context) {
        context.worker_pool_.reset();
        context.endpoint_store_->destroyQPs();
        context.endpoint_store_.reset();
        context.cq_list_.clear();
    }
};
class RdmaEndPointTestPeer {
   public:
    static void connect(RdmaEndPoint &endpoint) {
        endpoint.has_connected_ = true;
        endpoint.status_.store(RdmaEndPoint::CONNECTED);
    }
};

namespace {
class WorkerPoolTimeoutTest : public ::testing::Test {
   protected:
    void SetUp() override {
        saved_config_ = globalConfig();
        saved_log_level_ = FLAGS_minloglevel;
        FLAGS_minloglevel = google::GLOG_FATAL;
        globalConfig().slice_timeout = 1;
        globalConfig().track_rdma_posted_slices = false;
        globalConfig().conn_pause_ttl_ms = 0;
        globalConfig().rdma_signal_interval = 1;
        modify_error = 0;
        post_count = 0;
        reject_after = -1;
        verbs_.ops.post_send = postSend;
        verbs_.ops.poll_cq = pollCq;
        cq_.context = &verbs_;
        RdmaTransportTestPeer::bind(transport_);
        context_ = std::make_unique<RdmaContext>(transport_, "unused");
        RdmaContextTestPeer::bind(*context_, &cq_);
        batch_ = multi_.allocateBatchID(16);
    }
    void TearDown() override {
        // A failing assertion may leave fake WRs pending. Stop the fake device
        // before finishing those slices, so cleanup cannot mask a test hang.
        modify_error = 0;
        RdmaContextTestPeer::clean(*context_);
        for (auto &task : Transport::toBatchDesc(batch_).task_list)
            for (auto *slice : task.slice_list)
                if (slice->status == Transport::Slice::POSTED ||
                    slice->status == Transport::Slice::PENDING)
                    slice->markFailed();
        EXPECT_TRUE(multi_.freeBatchID(batch_).ok());
        completions.clear();
        posted.clear();
        globalConfig() = saved_config_;
        FLAGS_minloglevel = saved_log_level_;
    }
    std::shared_ptr<RdmaEndPoint> endpoint(const std::string &path = "peer@nic",
                                           size_t qps = 1) {
        auto ep = std::make_shared<RdmaEndPoint>(*context_);
        EXPECT_EQ(ep->construct(&cq_, qps), 0);
        ep->setPeerNicPath(path);
        RdmaEndPointTestPeer::connect(*ep);
        RdmaContextTestPeer::insert(*context_, path, ep);
        return ep;
    }
    size_t task() {
        auto &tasks = Transport::toBatchDesc(batch_).task_list;
        auto &t = tasks.emplace_back();
        t.batch_id = batch_;
        t.transport_ = &transport_;
        return tasks.size() - 1;
    }
    Transport::Slice *slice(size_t id, const std::string &path = "peer@nic") {
        auto &t = Transport::toBatchDesc(batch_).task_list[id];
        auto *s = new Transport::Slice{};
        s->task = &t;
        s->length = 64;
        s->opcode = Transport::TransferRequest::WRITE;
        s->status = Transport::Slice::PENDING;
        s->peer_nic_path = path;
        s->rdma.max_retry_cnt = 8;
        t.slice_list.push_back(s);
        __atomic_fetch_add(&t.slice_count, 1, __ATOMIC_RELAXED);
        return s;
    }
    WorkerPool &pool() { return RdmaContextTestPeer::pool(*context_); }
    void queue(Transport::Slice *s) { WorkerPoolTestPeer::queue(pool(), s); }
    void post() { WorkerPoolTestPeer::post(pool()); }
    void poll() { WorkerPoolTestPeer::poll(pool()); }
    Transport::TransferStatusEnum status(size_t id) {
        Transport::TransferStatus result;
        EXPECT_TRUE(multi_.getTransferStatus(batch_, id, result).ok());
        return result.s;
    }
    void age(Transport::Slice *s) {
        s->ts = getCurrentTimeInNano() - 2000000000LL;
    }
    void complete(Transport::Slice *s,
                  ibv_wc_status result = IBV_WC_WR_FLUSH_ERR) {
        ibv_wc wc{};
        wc.wr_id = reinterpret_cast<uint64_t>(s);
        wc.status = result;
        completions[&cq_].push_back(wc);
    }
    GlobalConfig saved_config_;
    int saved_log_level_;
    ibv_context verbs_{};
    ibv_cq cq_{};
    RdmaTransport transport_;
    std::unique_ptr<RdmaContext> context_;
    std::string name_ = "timeout-test";
    MultiTransport multi_{nullptr, name_};
    Transport::BatchID batch_;
};

TEST_F(WorkerPoolTimeoutTest, TimeoutWaitsForFlushBeforePublishingFailure) {
    auto ep = endpoint();
    auto id = task();
    auto *s = slice(id);
    queue(s);
    post();
    ASSERT_EQ(post_count, 1u);
    age(s);
    EXPECT_EQ(status(id), Transport::WAITING);
    poll();
    ASSERT_TRUE(ep->retired());
    EXPECT_EQ(status(id), Transport::WAITING);
    EXPECT_EQ(context_->cqOutstandingCount(0)->load(), 1);
    EXPECT_FALSE(ep->finishDestroy());
    complete(s);
    poll();
    EXPECT_EQ(status(id), Transport::FAILED);
    EXPECT_EQ(s->rdma.retry_cnt, 0u);
    EXPECT_EQ(context_->cqOutstandingCount(0)->load(), 0);
    EXPECT_EQ(WorkerPoolTestPeer::processed(pool()), 1u);
    EXPECT_TRUE(ep->finishDestroy());
}

TEST_F(WorkerPoolTimeoutTest, QueuedSiblingIsNotPostedAfterTimeout) {
    auto ep = endpoint();
    auto id = task();
    auto *s = slice(id);
    auto *queued = slice(id);
    queue(s);
    post();
    age(s);
    poll();
    ASSERT_TRUE(ep->retired());
    queue(queued);
    post();
    EXPECT_EQ(post_count, 1u);
    EXPECT_EQ(status(id), Transport::WAITING);
    complete(s);
    poll();
    EXPECT_EQ(status(id), Transport::FAILED);
    EXPECT_EQ(WorkerPoolTestPeer::processed(pool()), 2u);
}

TEST_F(WorkerPoolTimeoutTest, LateSuccessDoesNotUndoTimeout) {
    auto ep = endpoint();
    auto id = task();
    auto *s = slice(id);
    queue(s);
    post();
    age(s);
    poll();
    ASSERT_TRUE(ep->retired());
    complete(s, IBV_WC_SUCCESS);
    poll();
    EXPECT_EQ(status(id), Transport::FAILED);
    EXPECT_EQ(s->rdma.retry_cnt, 0u);
}

TEST_F(WorkerPoolTimeoutTest, FailedQpErrorTransitionKeepsWorkPending) {
    auto ep = endpoint();
    auto id = task();
    auto *s = slice(id);
    queue(s);
    post();
    age(s);
    modify_error = EIO;
    poll();
    EXPECT_EQ(status(id), Transport::WAITING);
    EXPECT_EQ(context_->cqOutstandingCount(0)->load(), 1);
    EXPECT_FALSE(ep->finishDestroy());
    EXPECT_EQ(WorkerPoolTestPeer::processed(pool()), 0u);
}

TEST_F(WorkerPoolTimeoutTest, YoungerSlicesOnOtherQpsDrainBeforeTaskFailure) {
    auto ep = endpoint("peer@nic", 2);
    auto id = task();
    auto *old = slice(id);
    auto *young = slice(id);
    queue(old);
    queue(young);
    post();
    ASSERT_NE(old->rdma.qp_index, young->rdma.qp_index);
    age(old);
    poll();
    ASSERT_TRUE(ep->retired());
    complete(old);
    poll();
    EXPECT_EQ(status(id), Transport::WAITING);
    EXPECT_EQ(context_->cqOutstandingCount(0)->load(), 1);
    complete(young);
    poll();
    EXPECT_EQ(status(id), Transport::FAILED);
    EXPECT_EQ(young->rdma.retry_cnt, 0u);
    EXPECT_EQ(WorkerPoolTestPeer::processed(pool()), 2u);
}

TEST_F(WorkerPoolTimeoutTest, OtherTaskOnSameQpCanStillSucceed) {
    auto ep = endpoint();
    auto expired = task(), healthy = task();
    auto *old = slice(expired);
    auto *young = slice(healthy);
    queue(old);
    queue(young);
    post();
    age(old);
    poll();
    ASSERT_TRUE(ep->retired());
    // The younger WR may have finished successfully before QP->ERR.
    complete(old);
    complete(young, IBV_WC_SUCCESS);
    poll();
    EXPECT_EQ(status(expired), Transport::FAILED);
    EXPECT_EQ(status(healthy), Transport::COMPLETED);
    EXPECT_EQ(context_->cqOutstandingCount(0)->load(), 0);
}

TEST_F(WorkerPoolTimeoutTest, SelectiveSignalingRetiresEverySliceExactlyOnce) {
    globalConfig().rdma_signal_interval = 4;
    auto ep = endpoint();
    auto id = task();
    std::vector<Transport::Slice *> slices;
    for (int i = 0; i < 4; ++i) {
        slices.push_back(slice(id));
        queue(slices.back());
    }
    post();
    age(slices.front());
    poll();
    ASSERT_TRUE(ep->retired());
    // A signaled success can cover an unsignaled prefix even after timeout
    // detection. The flushed tail must still keep the task pending.
    complete(slices[1], IBV_WC_SUCCESS);
    poll();
    EXPECT_EQ(status(id), Transport::WAITING);
    EXPECT_EQ(context_->cqOutstandingCount(0)->load(), 2);
    complete(slices[2]);
    complete(slices[3]);
    poll();
    EXPECT_EQ(status(id), Transport::FAILED);
    EXPECT_EQ(context_->cqOutstandingCount(0)->load(), 0);
    EXPECT_EQ(WorkerPoolTestPeer::processed(pool()), 4u);
}

TEST_F(WorkerPoolTimeoutTest, PartialPostFailureOnlyTracksAcceptedWork) {
    auto ep = endpoint();
    auto id = task();
    auto *accepted = slice(id), *rejected = slice(id);
    rejected->rdma.max_retry_cnt = 0;
    queue(accepted);
    queue(rejected);
    reject_after = 1;
    post();
    ASSERT_EQ(post_count, 1u);
    ASSERT_EQ(rejected->status, Transport::Slice::FAILED);
    EXPECT_EQ(context_->cqOutstandingCount(0)->load(), 1);
    age(accepted);
    poll();
    ASSERT_TRUE(ep->retired());
    EXPECT_EQ(status(id), Transport::WAITING);
    complete(accepted);
    poll();
    EXPECT_EQ(status(id), Transport::FAILED);
    EXPECT_EQ(context_->cqOutstandingCount(0)->load(), 0);
    EXPECT_EQ(WorkerPoolTestPeer::processed(pool()), 2u);
}

TEST_F(WorkerPoolTimeoutTest, CancellationReachesSiblingOnAnotherContext) {
    ibv_cq other_cq{};
    other_cq.context = &verbs_;
    auto other = std::make_unique<RdmaContext>(transport_, "other");
    RdmaContextTestPeer::bind(*other, &other_cq);
    auto ep = endpoint();
    auto other_ep = std::make_shared<RdmaEndPoint>(*other);
    ASSERT_EQ(other_ep->construct(&other_cq, 1), 0);
    other_ep->setPeerNicPath("peer@other");
    RdmaEndPointTestPeer::connect(*other_ep);
    RdmaContextTestPeer::insert(*other, "peer@other", other_ep);
    auto &other_pool = RdmaContextTestPeer::pool(*other);
    auto id = task();
    auto *old = slice(id), *young = slice(id, "peer@other");
    queue(old);
    post();
    WorkerPoolTestPeer::queue(other_pool, young);
    WorkerPoolTestPeer::post(other_pool);
    age(old);
    poll();
    WorkerPoolTestPeer::checkTimeouts(other_pool);
    EXPECT_TRUE(ep->retired());
    EXPECT_TRUE(other_ep->retired());
    complete(old);
    poll();
    EXPECT_EQ(status(id), Transport::WAITING);
    ibv_wc wc{};
    wc.wr_id = reinterpret_cast<uint64_t>(young);
    wc.status = IBV_WC_WR_FLUSH_ERR;
    completions[&other_cq].push_back(wc);
    WorkerPoolTestPeer::poll(other_pool);
    EXPECT_EQ(status(id), Transport::FAILED);
    EXPECT_EQ(young->rdma.retry_cnt, 0u);
    RdmaContextTestPeer::clean(*other);
}

TEST_F(WorkerPoolTimeoutTest, DisabledTimeoutPreservesNormalCompletion) {
    globalConfig().slice_timeout = -1;
    auto ep = endpoint();
    auto id = task();
    auto *s = slice(id);
    queue(s);
    post();
    age(s);
    poll();
    EXPECT_FALSE(ep->retired());
    EXPECT_EQ(status(id), Transport::WAITING);
    complete(s, IBV_WC_SUCCESS);
    poll();
    EXPECT_EQ(status(id), Transport::COMPLETED);
}
}  // namespace
}  // namespace mooncake
