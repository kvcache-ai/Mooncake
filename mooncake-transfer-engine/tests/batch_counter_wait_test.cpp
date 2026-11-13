// Copyright 2026 KVCache.AI
// Licensed under the Apache License, Version 2.0.
#include <gtest/gtest.h>
#include <algorithm>
#include <atomic>
#include <chrono>
#include <thread>
#include <vector>
#ifdef __linux__
#include <poll.h>
#include <unistd.h>
#endif
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
    // Create this many unfinished slices per task (ignored when
    // complete_while_submitting or empty_task).
    size_t slices_per_task = 1;
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
            if (complete_while_submitting) {
                auto* s = new Slice{};
                s->task = t;
                s->length = 17;
                s->ts = 0;
                t->slice_list.push_back(s);
                t->slice_count = 1;
                t->total_bytes = 17;
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
                continue;
            }
            const size_t n = slices_per_task == 0 ? 1 : slices_per_task;
            t->total_bytes = 0;
            for (size_t i = 0; i < n; ++i) {
                auto* s = new Slice{};
                s->task = t;
                s->length = 17;
                s->ts = 0;
                t->slice_list.push_back(s);
                t->total_bytes += 17;
            }
            t->slice_count = n;
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
    void finishSlice(size_t task_i, size_t slice_i, bool failed = false) {
        auto* s = tasks.at(task_i)->slice_list.at(slice_i);
        if (failed)
            s->markFailed();
        else
            s->markSuccess();
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
// NVMeoF never stores task.transferred_bytes. Its getTransferStatus sums
// slice statuses, so settledTransferredBytes() would report 0.
class SliceStatusProbe : public Transport {
   public:
    bool supportsBatchCompletionCounter() const override { return false; }
    std::vector<TransferTask*> tasks;
    Status submitTransfer(BatchID,
                          const std::vector<TransferRequest>&) override {
        return Status::OK();
    }
    Status submitTransferTask(
        const std::vector<TransferTask*>& incoming) override {
        for (auto* task : incoming) {
            tasks.push_back(task);
            auto* slice = new Slice{};
            slice->task = task;
            slice->length = 17;
            slice->ts = 0;
            task->slice_list.push_back(slice);
            task->slice_count = 1;
            task->total_bytes = 17;
        }
        return Status::OK();
    }
    void succeed(size_t index) {
        tasks.at(index)->slice_list.front()->status = Slice::SUCCESS;
    }
    void fail(size_t index) {
        auto* task = tasks.at(index);
        task->slice_list.front()->status = Slice::FAILED;
        task->failed_slice_count = 1;
    }
    Status getTransferStatus(BatchID batch, size_t index,
                             TransferStatus& status) override {
        auto& task = toBatchDesc(batch).task_list.at(index);
        uint64_t bytes = 0;
        size_t settled = 0;
        bool failed = false;
        for (auto* slice : task.slice_list) {
            if (slice->status == Slice::PENDING ||
                slice->status == Slice::POSTED) {
                continue;
            }
            ++settled;
            if (slice->status == Slice::SUCCESS) {
                bytes += slice->length;
            } else {
                failed = true;
            }
        }
        if (settled < task.slice_count) {
            status.s = WAITING;
        } else if (failed) {
            status.s = FAILED;
        } else {
            status.s = COMPLETED;
        }
        status.transferred_bytes = status.s == COMPLETED ? bytes : 0;
        return Status::OK();
    }

   private:
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
    const char* getName() const override { return "slice-status"; }
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
    std::shared_ptr<SliceStatusProbe> nvmeof =
        std::make_shared<SliceStatusProbe>();
    std::vector<Transport::BatchID> batches;
    std::vector<Transport::TransferRequest> requests;
    int64_t saved_timeout;
    void SetUp() override {
        saved_timeout = globalConfig().slice_timeout;
        globalConfig().slice_timeout = -1;
        MultiTransportTestPeer::install(multi, "rdma", rdma);
        MultiTransportTestPeer::install(multi, "tcp", tcp);
        MultiTransportTestPeer::install(multi, "nvmeof", nvmeof);
        for (auto [id, proto] : {std::pair{1, "rdma"}, std::pair{2, "tcp"},
                                 std::pair{3, "nvmeof"}}) {
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
    Status enable(Transport::BatchID b) {
        return multi.enableBatchCompletionNotification(b);
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
    auto waiting = query(b);
    EXPECT_EQ(waiting.s, Transport::WAITING);
    EXPECT_EQ(waiting.transferred_bytes, 0u);
    EXPECT_EQ(rdma->polls, 0);
    rdma->finish(0);
    waiting = query(b);
    EXPECT_EQ(waiting.s, Transport::WAITING);
    EXPECT_EQ(waiting.transferred_bytes, 0u);
    EXPECT_EQ(rdma->polls, 0);
    rdma->finish(1);
    auto s = query(b);
    EXPECT_EQ(s.s, Transport::COMPLETED);
    EXPECT_EQ(s.transferred_bytes, 34u);
    int polls = rdma->polls;
    EXPECT_EQ(query(b).transferred_bytes, 34u);
    EXPECT_EQ(rdma->polls, polls);
}
TEST_F(BatchCounterWait, LegacySliceStatusBytesStayOnTheWalk) {
    auto completed = batch(2);
    ASSERT_TRUE(submit(completed, {3, 3}).ok());
    nvmeof->succeed(0);
    nvmeof->succeed(1);
    auto done = query(completed);
    EXPECT_EQ(done.s, Transport::COMPLETED);
    EXPECT_EQ(done.transferred_bytes, 34u);
    for (const auto& task : Transport::toBatchDesc(completed).task_list) {
        const uint64_t stored = task.transferred_bytes;
        EXPECT_EQ(stored, 0u);
    }

    const size_t base = nvmeof->tasks.size();
    auto failed = batch(2);
    ASSERT_TRUE(submit(failed, {3, 3}).ok());
    nvmeof->succeed(base);
    nvmeof->fail(base + 1);
    auto status = query(failed);
    EXPECT_EQ(status.s, Transport::FAILED);
    // The successful task contributes its slice sum. settledTransferredBytes
    // is 0 because this probe never writes task.transferred_bytes.
    EXPECT_EQ(Transport::toBatchDesc(failed).settledTransferredBytes(), 0u);
    EXPECT_EQ(status.transferred_bytes, 17u);
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
    const bool explicit_protocols[] = {
        false,
#ifdef ENABLE_MULTI_PROTOCOL
        true,
#endif
    };
    for (bool explicit_protocol : explicit_protocols) {
        for (bool reject : {false, true}) {
            SCOPED_TRACE(::testing::Message()
                         << "explicit_protocol=" << explicit_protocol
                         << " reject=" << reject);
            rdma->tasks.clear();
            rdma->complete_while_submitting = true;
            rdma->reject_submit = reject;
            auto b = batch(1);
            requests.assign(1, {});
            requests[0].target_id = 1;
            requests[0].length = 17;
            Status status;
#ifdef ENABLE_MULTI_PROTOCOL
            if (explicit_protocol) {
                std::string proto = "rdma";
                status = multi.mp_submitTransfer(b, requests, proto);
            } else
#endif
            {
                status = multi.submitTransfer(b, requests);
            }
            EXPECT_EQ(status.ok(), !reject);
            auto& desc = Transport::toBatchDesc(b);
            auto& task = desc.task_list.at(0);
            // The probe completes the prefix before constructing the tail.
            EXPECT_EQ(rdma->published_before_seal, 0u);
            EXPECT_EQ(task.slice_count, 2u);
            EXPECT_EQ(task.completion_state,
                      Transport::Slice::kCompletionSealBit | 1u);
            EXPECT_FALSE(task.completion_published);
            EXPECT_FALSE(task.is_finished);
            EXPECT_EQ(desc.finished_task_count.load(), 0u);
            EXPECT_FALSE(desc.is_finished.load());
            EXPECT_EQ(query(b).s, Transport::WAITING);
            rdma->finish(0, reject);
            EXPECT_EQ(task.completion_state,
                      Transport::Slice::kCompletionSealBit | 2u);
            EXPECT_TRUE(task.completion_published);
            EXPECT_EQ(query(b).s,
                      reject ? Transport::FAILED : Transport::COMPLETED);
            if (!reject) {
                EXPECT_EQ(query(b).transferred_bytes, 40u);
            }
            EXPECT_EQ(desc.finished_task_count.load(), 1u);
#ifdef USE_EVENT_DRIVEN_COMPLETION
            EXPECT_TRUE(task.is_finished);
            EXPECT_TRUE(desc.is_finished.load());
#endif
        }
    }
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

void forget(std::vector<Transport::BatchID>& batches, Transport::BatchID id) {
    batches.erase(std::remove(batches.begin(), batches.end(), id),
                  batches.end());
}

#ifdef __linux__
bool fdReadable(int fd) {
    pollfd pfd{};
    pfd.fd = fd;
    pfd.events = POLLIN;
    return ::poll(&pfd, 1, 1000) == 1 && (pfd.revents & POLLIN);
}
#endif

// The wake is published from markSuccess after
// enableBatchCompletionNotification. These tests do not call
// getBatchTransferStatus until after drain, except where the assertion is that
// a drained id is still queryable.
TEST_F(BatchCounterWait, CompletionFdWakesWithoutStatusPoll) {
#ifndef __linux__
    EXPECT_EQ(multi.completionFd(), -1);
    std::vector<BatchCompletion> done;
    EXPECT_TRUE(
        multi.drainCompletions(done).IsCompletionNotificationUnsupported());
#else
    ASSERT_GE(multi.completionFd(), 0);
    auto b = batch(2);
    ASSERT_TRUE(enable(b).ok());
    ASSERT_TRUE(submit(b, {1, 1}).ok());
    rdma->finish(0);
    rdma->finish(1);
    ASSERT_TRUE(fdReadable(multi.completionFd()));
    std::vector<BatchCompletion> done;
    ASSERT_TRUE(multi.drainCompletions(done).ok());
    ASSERT_EQ(done.size(), 1u);
    EXPECT_EQ(done[0].batch_id, b);
    EXPECT_EQ(done[0].status.s, Transport::COMPLETED);
    EXPECT_EQ(done[0].status.transferred_bytes, 34u);
    done.clear();
    ASSERT_TRUE(multi.drainCompletions(done).ok());
    EXPECT_TRUE(done.empty());
    pollfd pfd{};
    pfd.fd = multi.completionFd();
    pfd.events = POLLIN;
    EXPECT_EQ(::poll(&pfd, 1, 0), 0);
    EXPECT_EQ(query(b).s, Transport::COMPLETED);
    EXPECT_EQ(query(b).transferred_bytes, 34u);
    // Release without a status loop: drain already observed the record.
    ASSERT_TRUE(multi.freeBatchID(b).ok());
    forget(batches, b);
#endif
}

TEST_F(BatchCounterWait, OneDrainReturnsEveryReadyBatch) {
#ifndef __linux__
    GTEST_SKIP();
#else
    auto first = batch(1);
    auto second = batch(1);
    ASSERT_TRUE(enable(first).ok());
    ASSERT_TRUE(enable(second).ok());
    ASSERT_TRUE(submit(first, {1}).ok());
    ASSERT_TRUE(submit(second, {1}).ok());
    rdma->finish(0);
    rdma->finish(1);
    std::vector<BatchCompletion> done;
    ASSERT_TRUE(multi.drainCompletions(done).ok());
    ASSERT_EQ(done.size(), 2u);
    EXPECT_EQ(done[0].batch_id, first);
    EXPECT_EQ(done[1].batch_id, second);
    EXPECT_EQ(done[0].status.s, Transport::COMPLETED);
    EXPECT_EQ(done[1].status.s, Transport::COMPLETED);
    done.clear();
    ASSERT_TRUE(multi.drainCompletions(done).ok());
    EXPECT_TRUE(done.empty());
    pollfd pfd{};
    pfd.fd = multi.completionFd();
    pfd.events = POLLIN;
    EXPECT_EQ(::poll(&pfd, 1, 0), 0);
#endif
}

TEST_F(BatchCounterWait, FreeBeforeDrainDropsTheQueuedId) {
#ifndef __linux__
    GTEST_SKIP();
#else
    auto b = batch(1);
    ASSERT_TRUE(enable(b).ok());
    ASSERT_TRUE(submit(b, {1}).ok());
    rdma->finish(0);
    ASSERT_TRUE(fdReadable(multi.completionFd()));
    ASSERT_TRUE(multi.freeBatchID(b).ok());
    forget(batches, b);
    std::vector<BatchCompletion> done;
    ASSERT_TRUE(multi.drainCompletions(done).ok());
    EXPECT_TRUE(done.empty());
#endif
}

TEST_F(BatchCounterWait, ShortSubmitWakesWhenSubmittedTasksFinish) {
#ifndef __linux__
    GTEST_SKIP();
#else
    // Capacity 4, submit 1: wake on the submitted count, not capacity.
    auto b = batch(4);
    ASSERT_TRUE(enable(b).ok());
    ASSERT_TRUE(submit(b, {1}).ok());
    rdma->finish(0);
    ASSERT_TRUE(fdReadable(multi.completionFd()));
    std::vector<BatchCompletion> done;
    ASSERT_TRUE(multi.drainCompletions(done).ok());
    ASSERT_EQ(done.size(), 1u);
    EXPECT_EQ(done[0].batch_id, b);
    EXPECT_EQ(done[0].status.s, Transport::COMPLETED);
    EXPECT_EQ(done[0].status.transferred_bytes, 17u);
#endif
}

TEST_F(BatchCounterWait, SecondSubmitRejectedWhenEventEnabled) {
#ifndef __linux__
    GTEST_SKIP();
#else
    auto b = batch(4);
    ASSERT_TRUE(enable(b).ok());
    ASSERT_TRUE(submit(b, {1}).ok());
    rdma->finish(0);
    std::vector<BatchCompletion> done;
    ASSERT_TRUE(multi.drainCompletions(done).ok());
    ASSERT_EQ(done.size(), 1u);
    auto second = submit(b, {1});
    EXPECT_TRUE(second.IsInvalidArgument());
    // Legacy (non-opted-in) append remains accepted.
    auto legacy = batch(4);
    ASSERT_TRUE(submit(legacy, {1}).ok());
    rdma->finish(1);
    ASSERT_TRUE(submit(legacy, {1}).ok());
#endif
}

TEST_F(BatchCounterWait, FailedSubmitDoesNotPublishCompletion) {
#ifndef __linux__
    GTEST_SKIP();
#else
    rdma->empty_task = true;
    rdma->reject_submit = true;
    auto b = batch(1);
    ASSERT_TRUE(enable(b).ok());
    auto status = submit(b, {1});
    EXPECT_FALSE(status.ok());
    pollfd pfd{};
    pfd.fd = multi.completionFd();
    pfd.events = POLLIN;
    EXPECT_EQ(::poll(&pfd, 1, 0), 0);
    std::vector<BatchCompletion> done;
    ASSERT_TRUE(multi.drainCompletions(done).ok());
    EXPECT_TRUE(done.empty());
#endif
}

TEST_F(BatchCounterWait, FailedSubmitWithInFlightSliceDoesNotPublish) {
#ifndef __linux__
    GTEST_SKIP();
#else
    rdma->reject_submit = true;
    auto b = batch(1);
    ASSERT_TRUE(enable(b).ok());
    auto status = submit(b, {1});
    EXPECT_FALSE(status.ok());
    rdma->finish(0);
    pollfd pfd{};
    pfd.fd = multi.completionFd();
    pfd.events = POLLIN;
    EXPECT_EQ(::poll(&pfd, 1, 0), 0);
    std::vector<BatchCompletion> done;
    ASSERT_TRUE(multi.drainCompletions(done).ok());
    EXPECT_TRUE(done.empty());
#endif
}

TEST_F(BatchCounterWait, OptInCompletionInsideSubmitPublishesOnTailSlice) {
#ifndef __linux__
    GTEST_SKIP();
#else
    rdma->complete_while_submitting = true;
    auto b = batch(1);
    ASSERT_TRUE(enable(b).ok());
    ASSERT_TRUE(submit(b, {1}).ok());
    pollfd pfd{};
    pfd.fd = multi.completionFd();
    pfd.events = POLLIN;
    EXPECT_EQ(::poll(&pfd, 1, 0), 0);
    rdma->finish(0);
    ASSERT_TRUE(fdReadable(multi.completionFd()));
    std::vector<BatchCompletion> done;
    ASSERT_TRUE(multi.drainCompletions(done).ok());
    ASSERT_EQ(done.size(), 1u);
    EXPECT_EQ(done[0].batch_id, b);
    EXPECT_EQ(done[0].status.s, Transport::COMPLETED);
    EXPECT_EQ(done[0].status.transferred_bytes, 40u);
#endif
}

TEST_F(BatchCounterWait, SynchronousCompletionPublishesAfterSuccessfulSubmit) {
#ifndef __linux__
    GTEST_SKIP();
#else
    rdma->empty_task = true;
    auto b = batch(1);
    ASSERT_TRUE(enable(b).ok());
    ASSERT_TRUE(submit(b, {1}).ok());
    ASSERT_TRUE(fdReadable(multi.completionFd()));
    std::vector<BatchCompletion> done;
    ASSERT_TRUE(multi.drainCompletions(done).ok());
    ASSERT_EQ(done.size(), 1u);
    EXPECT_EQ(done[0].batch_id, b);
    EXPECT_EQ(done[0].status.s, Transport::COMPLETED);
#endif
}

TEST_F(BatchCounterWait, OptInOnPollDrivenTransportFailsAtSubmit) {
#ifndef __linux__
    GTEST_SKIP();
#else
    auto b = batch(1);
    ASSERT_TRUE(enable(b).ok());
    auto status = submit(b, {2});  // tcp probe: not counter-capable
    EXPECT_TRUE(status.IsCompletionNotificationUnsupported());
    pollfd pfd{};
    pfd.fd = multi.completionFd();
    pfd.events = POLLIN;
    EXPECT_EQ(::poll(&pfd, 1, 0), 0);
#endif
}

TEST_F(BatchCounterWait, MissingTransportIsNotACompletionFallback) {
#ifndef __linux__
    GTEST_SKIP();
#else
    auto d = std::make_shared<TransferMetadata::SegmentDesc>();
    d->name = "peer:4";
    d->protocol = "cxl";  // not installed
    auto name = d->name;
    md->addLocalSegment(4, name, std::move(d));
    auto b = batch(1);
    ASSERT_TRUE(enable(b).ok());
    auto status = submit(b, {4});
    EXPECT_TRUE(status.IsNotSupportedTransport());
    EXPECT_FALSE(status.IsCompletionNotificationUnsupported());
#endif
}

TEST_F(BatchCounterWait, EnableRejectedWhenSliceTimeoutRequiresPolling) {
#ifndef __linux__
    GTEST_SKIP();
#else
    globalConfig().slice_timeout = 1;
    auto b = batch(1);
    EXPECT_TRUE(enable(b).IsCompletionNotificationUnsupported());
#endif
}

TEST_F(BatchCounterWait, PartialSliceFailureDoesNotWakeUntilTaskDone) {
#ifndef __linux__
    GTEST_SKIP();
#else
    rdma->slices_per_task = 2;
    auto b = batch(1);
    ASSERT_TRUE(enable(b).ok());
    ASSERT_TRUE(submit(b, {1}).ok());
    rdma->finishSlice(0, 0, true);
    pollfd pfd{};
    pfd.fd = multi.completionFd();
    pfd.events = POLLIN;
    EXPECT_EQ(::poll(&pfd, 1, 0), 0);
    rdma->finishSlice(0, 1);
    ASSERT_TRUE(fdReadable(multi.completionFd()));
    std::vector<BatchCompletion> done;
    ASSERT_TRUE(multi.drainCompletions(done).ok());
    ASSERT_EQ(done.size(), 1u);
    EXPECT_EQ(done[0].status.s, Transport::FAILED);
    // The successful 17-byte slice must not be counted. The status query
    // also ignores a FAILED task's bytes.
    EXPECT_EQ(done[0].status.transferred_bytes, 0u);
    auto queried = query(b);
    EXPECT_EQ(queried.s, Transport::FAILED);
    EXPECT_EQ(queried.transferred_bytes, 0u);
#endif
}

TEST(CompletionEventFd, DestroyWakesBlockedWaiter) {
#ifndef __linux__
    GTEST_SKIP();
#else
    auto md = std::make_shared<TransferMetadata>(P2PHANDSHAKE);
    std::string local = "eventfd-wake:1";
    int duped = -1;
    std::atomic<int> entered{0};
    std::atomic<int> woke{-1};
    std::thread waiter;
    {
        MultiTransport multi(md, local);
        duped = ::dup(multi.completionFd());
        ASSERT_GE(duped, 0);
        waiter = std::thread([&] {
            pollfd held{};
            held.fd = duped;
            held.events = POLLIN;
            entered.store(1);
            woke.store(::poll(&held, 1, 5000));
        });
        while (entered.load() == 0) std::this_thread::yield();
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }
    waiter.join();
    EXPECT_EQ(woke.load(), 1);
    uint64_t value = 0;
    ASSERT_EQ(::read(duped, &value, sizeof(value)),
              static_cast<ssize_t>(sizeof(value)));
    EXPECT_GE(value, 1u);
    pollfd again{};
    again.fd = duped;
    again.events = POLLIN;
    EXPECT_EQ(::poll(&again, 1, 0), 0);
    ::close(duped);
#endif
}

TEST_F(BatchCounterWait, EmptyEventSubmitRejected) {
#ifndef __linux__
    GTEST_SKIP();
#else
    auto b = batch(1);
    ASSERT_TRUE(enable(b).ok());
    std::vector<Transport::TransferRequest> empty;
    auto rejected = multi.submitTransfer(b, empty);
    EXPECT_TRUE(rejected.IsInvalidArgument());
    pollfd pfd{};
    pfd.fd = multi.completionFd();
    pfd.events = POLLIN;
    EXPECT_EQ(::poll(&pfd, 1, 0), 0);
    // The rejected empty submit did not consume the one submission.
    ASSERT_TRUE(submit(b, {1}).ok());
    rdma->finish(0);
    std::vector<BatchCompletion> done;
    ASSERT_TRUE(multi.drainCompletions(done).ok());
    ASSERT_EQ(done.size(), 1u);
    EXPECT_EQ(done[0].batch_id, b);
#endif
}

TEST_F(BatchCounterWait, NonOptInRdmaDoesNotPublish) {
#ifndef __linux__
    GTEST_SKIP();
#else
    auto b = batch(1);
    ASSERT_TRUE(submit(b, {1}).ok());
    rdma->finish(0);
    EXPECT_EQ(query(b).s, Transport::COMPLETED);
    EXPECT_EQ(query(b).transferred_bytes, 17u);
    pollfd pfd{};
    pfd.fd = multi.completionFd();
    pfd.events = POLLIN;
    EXPECT_EQ(::poll(&pfd, 1, 0), 0);
    std::vector<BatchCompletion> done;
    ASSERT_TRUE(multi.drainCompletions(done).ok());
    EXPECT_TRUE(done.empty());
    ASSERT_TRUE(multi.freeBatchID(b).ok());
    forget(batches, b);
#endif
}

TEST_F(BatchCounterWait, ConcurrentFinishAndDrain) {
#ifndef __linux__
    GTEST_SKIP();
#else
    auto b = batch(1);
    ASSERT_TRUE(enable(b).ok());
    ASSERT_TRUE(submit(b, {1}).ok());
    std::atomic<bool> go{false};
    std::vector<BatchCompletion> drained;
    std::thread finisher([&] {
        while (!go.load()) std::this_thread::yield();
        rdma->finish(0);
    });
    std::thread drainer([&] {
        while (!go.load()) std::this_thread::yield();
        for (int i = 0; i < 1000 && drained.empty(); ++i) {
            std::vector<BatchCompletion> got;
            if (!multi.drainCompletions(got).ok()) continue;
            drained.insert(drained.end(), got.begin(), got.end());
        }
    });
    go.store(true);
    finisher.join();
    drainer.join();
    if (drained.empty()) {
        ASSERT_TRUE(multi.drainCompletions(drained).ok());
    }
    ASSERT_EQ(drained.size(), 1u);
    EXPECT_EQ(drained[0].batch_id, b);
    EXPECT_EQ(drained[0].status.s, Transport::COMPLETED);
    EXPECT_EQ(drained[0].status.transferred_bytes, 17u);
    ASSERT_TRUE(multi.freeBatchID(b).ok());
    forget(batches, b);
#endif
}
}  // namespace
}  // namespace mooncake
