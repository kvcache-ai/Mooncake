// Copyright 2026 KVCache.AI
// Licensed under the Apache License, Version 2.0.
#include <gtest/gtest.h>
#include "multi_transport.h"
#include "transfer_metadata.h"
#include "config.h"

namespace mooncake {
class MultiTransportTestPeer {
   public:
    static void install(MultiTransport& m, const std::string& name,
                        std::shared_ptr<Transport> t) {
        m.transport_map_[name] = std::move(t);
    }
};
namespace {
class ProbeTransport : public Transport {
   public:
    explicit ProbeTransport(bool counter) : counter_(counter) {}
    bool supportsBatchCompletionCounter() const override { return counter_; }
    int polls = 0;
    bool complete_while_submitting = false;
    uint64_t published_before_seal = 0;
    bool reject_submit = false;
    bool empty_task = false;
    std::vector<TransferTask*> tasks;
    Status submitTransfer(BatchID,
                          const std::vector<TransferRequest>&) override {
        return Status::OK();
    }
    Status submitTransferTask(
        const std::vector<TransferTask*>& incoming) override {
        for (auto* t : incoming) {
            tasks.push_back(t);
            if (empty_task) continue;
            auto* s = new Slice{};
            s->task = t;
            s->length = 17;
            s->ts = 0;
            t->slice_list.push_back(s);
            t->slice_count = 1;
            t->total_bytes = 17;
            if (complete_while_submitting) {
                s->markSuccess();
                published_before_seal =
                    toBatchDesc(t->batch_id).finished_task_count.load();
                auto* tail = new Slice{};
                tail->task = t;
                tail->length = 23;
                tail->ts = 0;
                t->slice_list.push_back(tail);
                t->slice_count = 2;
                t->total_bytes = 40;
            }
        }
        return reject_submit ? Status::InvalidArgument("test submit failure")
                             : Status::OK();
    }
    void finish(size_t i, bool failed = false) {
        auto* t = tasks.at(i);
        if (counter_) {
            auto* s = t->slice_list.back();
            if (failed)
                s->markFailed();
            else
                s->markSuccess();
        } else {
            t->transferred_bytes = t->total_bytes;
            t->success_slice_count = t->slice_count;
            t->is_finished = true;
        }
    }
    Status getTransferStatus(BatchID b, size_t i, TransferStatus& s) override {
        ++polls;
        auto& t = toBatchDesc(b).task_list.at(i);
        auto ok = __atomic_load_n(&t.success_slice_count, __ATOMIC_ACQUIRE);
        auto bad = __atomic_load_n(&t.failed_slice_count, __ATOMIC_ACQUIRE);
        s.s = ok + bad == t.slice_count ? (bad ? FAILED : COMPLETED) : WAITING;
        s.transferred_bytes =
            __atomic_load_n(&t.transferred_bytes, __ATOMIC_RELAXED);
        return Status::OK();
    }

   private:
    bool counter_;
    int registerLocalMemory(void*, size_t, const std::string&, bool,
                            bool) override {
        return 0;
    }
    int unregisterLocalMemory(void*, bool) override { return 0; }
    int registerLocalMemoryBatch(const std::vector<BufferEntry>&,
                                 const std::string&) override {
        return 0;
    }
    int unregisterLocalMemoryBatch(const std::vector<void*>&) override {
        return 0;
    }
    const char* getName() const override { return "probe"; }
};
class BatchCounterWait : public ::testing::Test {
   protected:
    std::string local = "counter-test:1";
    std::shared_ptr<TransferMetadata> md =
        std::make_shared<TransferMetadata>(P2PHANDSHAKE);
    MultiTransport multi{md, local};
    std::shared_ptr<ProbeTransport> rdma =
        std::make_shared<ProbeTransport>(true);
    std::shared_ptr<ProbeTransport> tcp =
        std::make_shared<ProbeTransport>(false);
    std::vector<Transport::BatchID> batches;
    std::vector<Transport::TransferRequest> requests;
    int64_t saved_timeout;
    void SetUp() override {
        saved_timeout = globalConfig().slice_timeout;
        globalConfig().slice_timeout = -1;
        MultiTransportTestPeer::install(multi, "rdma", rdma);
        MultiTransportTestPeer::install(multi, "tcp", tcp);
        for (auto [id, proto] : {std::pair{1, "rdma"}, std::pair{2, "tcp"}}) {
            auto d = std::make_shared<TransferMetadata::SegmentDesc>();
            d->name = "peer:" + std::to_string(id);
            d->protocol = proto;
            auto name = d->name;
            md->addLocalSegment(id, name, std::move(d));
        }
    }
    void TearDown() override {
        globalConfig().slice_timeout = saved_timeout;
        for (auto b : batches)
            delete &Transport::toBatchDesc(
                b);  // No native transfer was started.
    }
    auto batch(size_t cap = 4) {
        auto b = multi.allocateBatchID(cap);
        batches.push_back(b);
        return b;
    }
    Status submit(Transport::BatchID b, std::initializer_list<int> ids) {
        requests.clear();
        for (int id : ids) {
            Transport::TransferRequest r{};
            r.target_id = id;
            r.length = 17;
            requests.push_back(r);
        }
        return multi.submitTransfer(b, requests);
    }
    Transport::TransferStatus query(Transport::BatchID b) {
        Transport::TransferStatus s{};
        EXPECT_TRUE(multi.getBatchTransferStatus(b, s).ok());
        return s;
    }
};
TEST_F(BatchCounterWait, ZeroAndPartialCounterSkipTransportPolling) {
    auto b = batch();
    ASSERT_TRUE(submit(b, {1, 1}).ok());
    EXPECT_EQ(query(b).s, Transport::WAITING);
    EXPECT_EQ(rdma->polls, 0);
    rdma->finish(0);
    EXPECT_EQ(query(b).s, Transport::WAITING);
    EXPECT_EQ(rdma->polls, 0);
    rdma->finish(1);
    auto s = query(b);
    EXPECT_EQ(s.s, Transport::COMPLETED);
    EXPECT_EQ(s.transferred_bytes, 34u);
    int polls = rdma->polls;
    EXPECT_EQ(query(b).transferred_bytes, 34u);
    EXPECT_EQ(rdma->polls, polls);
}
TEST_F(BatchCounterWait, LegacyAndMixedBatchesKeepPolling) {
    auto b = batch();
    ASSERT_TRUE(submit(b, {1, 2}).ok());
    rdma->finish(0);
    EXPECT_EQ(query(b).s, Transport::WAITING);
    EXPECT_GT(tcp->polls, 0);
    tcp->finish(0);
    EXPECT_EQ(query(b).s, Transport::COMPLETED);
    EXPECT_EQ(query(b).transferred_bytes, 34u);
}
TEST_F(BatchCounterWait, CompletedFailureIsNotReportedAsSuccess) {
    auto b = batch(1);
    ASSERT_TRUE(submit(b, {1}).ok());
    rdma->finish(0, true);
    EXPECT_EQ(query(b).s, Transport::FAILED);
}
TEST_F(BatchCounterWait, AppendAfterCachedCompletionInvalidatesStatusAndBytes) {
    auto b = batch();
    ASSERT_TRUE(submit(b, {1}).ok());
    rdma->finish(0);
    EXPECT_EQ(query(b).s, Transport::COMPLETED);
    ASSERT_TRUE(submit(b, {1}).ok());
    EXPECT_EQ(query(b).s, Transport::WAITING);
    rdma->finish(1);
    EXPECT_EQ(query(b).transferred_bytes, 34u);
    EXPECT_EQ(query(b).s, Transport::COMPLETED);
}
TEST_F(BatchCounterWait, AppendLegacyDisablesCounterShortcut) {
    auto b = batch();
    ASSERT_TRUE(submit(b, {1}).ok());
    rdma->finish(0);
    ASSERT_TRUE(submit(b, {2}).ok());
    EXPECT_EQ(query(b).s, Transport::WAITING);
    EXPECT_GT(tcp->polls, 0);
    tcp->finish(0);
    EXPECT_EQ(query(b).s, Transport::COMPLETED);
}
TEST_F(BatchCounterWait, PublicationWaitsForAllSlicesToBeSubmitted) {
    rdma->complete_while_submitting = true;
    auto b = batch(1);
    ASSERT_TRUE(submit(b, {1}).ok());
    EXPECT_EQ(rdma->published_before_seal, 0u);
    EXPECT_EQ(query(b).s, Transport::WAITING);
    rdma->finish(0);
    EXPECT_EQ(query(b).s, Transport::COMPLETED);
    EXPECT_EQ(query(b).transferred_bytes, 40u);
}
TEST_F(BatchCounterWait, TimeoutChecksKeepPolling) {
    auto b = batch();
    ASSERT_TRUE(submit(b, {1}).ok());
    globalConfig().slice_timeout = 1;
    rdma->tasks[0]->slice_list[0]->ts = getCurrentTimeInNano() - 2000000000ll;
    EXPECT_EQ(query(b).s, Transport::FAILED);
    EXPECT_GT(rdma->polls, 0);
}
TEST_F(BatchCounterWait, EmptyCounterTaskCompletesOnSeal) {
    rdma->empty_task = true;
    auto b = batch(1);
    ASSERT_TRUE(submit(b, {1}).ok());
    EXPECT_EQ(query(b).s, Transport::COMPLETED);
    EXPECT_EQ(query(b).transferred_bytes, 0u);
}
TEST_F(BatchCounterWait, FailureWhileOtherTasksPendingIsVisible) {
    auto b = batch();
    ASSERT_TRUE(submit(b, {1, 1}).ok());
    rdma->finish(0, true);
    EXPECT_EQ(query(b).s, Transport::FAILED);
    EXPECT_GT(rdma->polls, 0);
}
TEST_F(BatchCounterWait, EmptyBatchReturnsZeroBytes) {
    auto b = batch();
    auto s = query(b);
    EXPECT_EQ(s.s, Transport::COMPLETED);
    EXPECT_EQ(s.transferred_bytes, 0u);
}
TEST_F(BatchCounterWait, RejectedSubmissionKeepsFallback) {
    rdma->reject_submit = true;
    auto b = batch();
    EXPECT_FALSE(submit(b, {1}).ok());
    EXPECT_EQ(query(b).s, Transport::WAITING);
    EXPECT_GT(rdma->polls, 0);
}
}  // namespace
}  // namespace mooncake
