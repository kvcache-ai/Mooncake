// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0
#include "transfer_engine.h"
#include "transfer_engine_impl.h"
#include "batch_submit.h"
#include <gtest/gtest.h>
#include <atomic>
#include <barrier>
#include <future>
#include <mutex>
#include <optional>
#include <tuple>

#ifdef USE_TENT
#include "tent/transfer_engine.h"
#include "tent/runtime/transfer_engine_impl.h"
#include "tent/runtime/transport.h"
#endif

namespace mooncake {
class TransferEngineImplTestPeer {
   public:
    static void classic(TransferEngine& engine,
                        std::shared_ptr<MultiTransport> multi,
                        size_t threads = 2) {
        engine.use_tent_ = false;
        engine.impl_ = std::make_shared<TransferEngineImpl>(false);
        engine.impl_->multi_transports_ = std::move(multi);
        engine.batch_submit_executor_ = std::make_shared<BatchSubmitExecutor>(
            BatchSubmitExecutor::Config{threads, 1, 4});
    }
#ifdef USE_TENT
    static tent::TransferEngineImpl& impl(tent::TransferEngine& engine) {
        return *engine.impl_;
    }
    static void configure(tent::TransferEngine& engine, size_t threads = 2) {
        engine.batch_submit_executor_ = std::make_shared<BatchSubmitExecutor>(
            BatchSubmitExecutor::Config{threads, 1, 4});
    }
    static void facade(TransferEngine& engine,
                       std::shared_ptr<tent::TransferEngine> native) {
        engine.use_tent_ = true;
        engine.impl_tent_ = std::move(native);
    }
    static tent::Status machine(tent::TransferEngine& engine,
                                const std::string& id) {
        return engine.impl_->metadata_->segmentManager().updateLocal(
            [&](tent::SegmentDesc& desc) {
                desc.machine_id = id;
                return tent::Status::OK();
            });
    }
#endif
};

class MultiTransportBatchTestPeer {
   public:
    static void install(MultiTransport& multi, const std::string& name,
                        std::shared_ptr<Transport> transport) {
        multi.transport_map_[name] = std::move(transport);
    }
    static bool deferred(MultiTransport& multi, BatchID id) {
        std::lock_guard<std::mutex> lock(multi.deferred_cleanup_mutex_);
        return multi.deferred_cleanup_batches_.count(id) != 0;
    }
};

namespace {
using Executor = BatchSubmitExecutor;
using namespace std::chrono_literals;

// Signals a gate on the first call and again from the destructor. Declared
// after the std::async future so a failing assertion opens the gate before
// that future's destructor joins the callback.
class SignalOnce {
   public:
    explicit SignalOnce(std::promise<void>& promise) : promise_(promise) {}
    SignalOnce(const SignalOnce&) = delete;
    SignalOnce& operator=(const SignalOnce&) = delete;

    void signal() {
        if (!sent_) {
            sent_ = true;
            promise_.set_value();
        }
    }
    ~SignalOnce() { signal(); }

   private:
    std::promise<void>& promise_;
    bool sent_ = false;
};

TEST(BatchSubmit, ConfigParsingAndEligibility) {
    struct SavedEnvironment {
        const char* name;
        std::optional<std::string> previous;
        explicit SavedEnvironment(const char* key) : name(key) {
            if (const char* value = std::getenv(name)) previous = value;
        }
        ~SavedEnvironment() {
            if (previous)
                setenv(name, previous->c_str(), 1);
            else
                unsetenv(name);
        }
    } threads("MC_BATCH_SUBMIT_THREADS"), min_size("MC_BATCH_SUBMIT_MIN_SIZE");

    ASSERT_EQ(unsetenv(threads.name), 0);
    ASSERT_EQ(unsetenv(min_size.name), 0);
    auto config = Executor::Config::fromEnvironment();
    EXPECT_EQ(config.threads, 0);
    EXPECT_EQ(config.min_descriptors, 4096);
    Executor disabled(config);
    EXPECT_FALSE(disabled.eligible(65536));

    ASSERT_EQ(setenv(threads.name, "4", 1), 0);
    ASSERT_EQ(setenv(min_size.name, "8192", 1), 0);
    Executor executor(Executor::Config::fromEnvironment());
    EXPECT_EQ(executor.config().threads, 4);
    EXPECT_FALSE(executor.eligible(8191));
    EXPECT_TRUE(executor.eligible(8192));
    EXPECT_THROW((Executor({33, 1, 1})), std::invalid_argument);
    EXPECT_THROW((Executor({1, 0, 1})), std::invalid_argument);
    EXPECT_THROW((Executor({1, 1, 0})), std::invalid_argument);

    for (const char* invalid :
         {"", "abc", "4junk", "-1", "33", "184467440737095516160"}) {
        SCOPED_TRACE(invalid);
        ASSERT_EQ(setenv(threads.name, invalid, 1), 0);
        auto fallback = Executor::Config::fromEnvironment();
        EXPECT_EQ(fallback.threads, 0);
        EXPECT_EQ(fallback.min_descriptors, 8192);
    }
    ASSERT_EQ(setenv(threads.name, "4", 1), 0);
    for (const char* invalid :
         {"", "abc", "4096junk", "-1", "0", "184467440737095516160"}) {
        SCOPED_TRACE(invalid);
        ASSERT_EQ(setenv(min_size.name, invalid, 1), 0);
        auto fallback = Executor::Config::fromEnvironment();
        EXPECT_EQ(fallback.threads, 4);
        EXPECT_EQ(fallback.min_descriptors, 4096);
    }
}

TEST(BatchSubmit, DetectsOverlapBeforeAnySubBatchCanPublish) {
    const std::vector<size_t> lengths{16, 16, 16};
    EXPECT_TRUE(independentTransferDestinations(
        std::vector<uintptr_t>{32, 64, 96}, lengths));
    EXPECT_TRUE(independentTransferDestinations(
        std::vector<uintptr_t>{96, 32, 64}, lengths));
    EXPECT_FALSE(independentTransferDestinations(
        std::vector<uintptr_t>{32, 33, 64}, lengths));
    EXPECT_FALSE(independentTransferDestinations(
        std::vector<uintptr_t>{32, 32, 64}, lengths));
    EXPECT_FALSE(independentTransferDestinations(
        std::vector<uintptr_t>{UINTPTR_MAX - 1}, std::vector<size_t>{4}));
    EXPECT_FALSE(independentTransferDestinations(std::vector<uintptr_t>{32},
                                                 std::vector<size_t>{0}));
}

TEST(BatchSubmit, ShutdownStopsAdmissionAndWaitsForActiveCallbacks) {
    for (bool native_failure : {false, true}) {
        SCOPED_TRACE(native_failure);
        Executor executor({2, 1, 1});
        std::promise<void> started;
        std::atomic<int> returned{0};
        auto parent = std::async(std::launch::async, [&] {
            return executor.execute(
                1, Executor::Clock::now() + 10s, [&](auto, const auto& stop) {
                    started.set_value();
                    while (!stop.requested()) std::this_thread::yield();
                    ++returned;
                    return native_failure ? Executor::Result::TransferError(
                                                "native failure")
                                          : Executor::Result::Canceled();
                });
        });
        auto started_future = started.get_future();
        if (started_future.wait_for(2s) != std::future_status::ready) {
            executor.shutdown();
            FAIL() << "batch submit callback did not start";
        }
        executor.shutdown();
        EXPECT_EQ(returned.load(), 1);
        auto result = parent.get();
        EXPECT_FALSE(result.ok());
        EXPECT_FALSE(result.retryable);
        EXPECT_FALSE(result.isCanceled());
        EXPECT_EQ(result.error, native_failure
                                    ? "native failure"
                                    : "Batch submit executor is shutting down");
        EXPECT_FALSE(executor
                         .execute(1, Executor::Clock::now() + 1s,
                                  [](auto, const auto&) {
                                      return Executor::Result::OK();
                                  })
                         .ok());
    }
}

struct FakeAcceptedBatch {
    int submitted = 0, polls = 0, freed = 0;
    Executor::Result allocation = Executor::Result::OK();
    Executor::Result submission = Executor::Result::OK();
    SubBatchProgress progress{true, true, {}};
    bool throw_poll = false;
    std::function<void()> on_submit, on_poll;
    Executor::Result allocate() { return allocation; }
    Executor::Result submit() {
        ++submitted;
        if (on_submit) on_submit();
        return submission;
    }
    SubBatchProgress poll() {
        ++polls;
        if (on_poll) on_poll();
        if (throw_poll) throw std::runtime_error("poll failure");
        return progress;
    }
    Executor::Result release() {
        ++freed;
        return Executor::Result::OK();
    }
};

TEST(BatchSubmit, SubBatchReleasesOnlyAllocatedHandlesOnSuccessAndErrors) {
    enum class Failure { None, Allocate, Submit, Poll, Terminal, SiblingStop };
    for (auto [name, failure] :
         {std::pair{"success", Failure::None},
          std::pair{"allocation", Failure::Allocate},
          std::pair{"submit", Failure::Submit},
          std::pair{"poll exception", Failure::Poll},
          std::pair{"terminal failure", Failure::Terminal},
          std::pair{"accepted sibling stop", Failure::SiblingStop}}) {
        SCOPED_TRACE(name);
        std::atomic<bool> stopped{false};
        FakeAcceptedBatch batch;
        if (failure == Failure::Allocate)
            batch.allocation = Executor::Result::Error("allocation failed");
        if (failure == Failure::Submit)
            batch.submission = Executor::Result::SubmitError("submit failed");
        if (failure == Failure::SiblingStop)
            batch.on_submit = [&] { stopped.store(true); };
        batch.throw_poll = failure == Failure::Poll;
        batch.progress.success = failure != Failure::Terminal;
        auto result = runSubBatch(
            batch,
            Executor::Cancellation{stopped, Executor::Clock::now() + 1s});
        EXPECT_EQ(result.ok(),
                  failure == Failure::None || failure == Failure::SiblingStop);
        EXPECT_EQ(batch.submitted, failure == Failure::Allocate ? 0 : 1);
        EXPECT_EQ(
            batch.polls,
            failure == Failure::Allocate || failure == Failure::Submit ? 0 : 1);
        EXPECT_EQ(batch.freed, failure == Failure::Allocate ? 0 : 1);
        EXPECT_EQ(result.submit_failed, failure == Failure::Submit);
    }
}

TEST(BatchSubmit, SubBatchReleasesOnDeadlineAndShutdownAtEachStage) {
    enum class Stage { BeforeSubmit, AfterSubmit, PendingPoll };
    for (bool shutdown_requested : {false, true}) {
        for (auto stage :
             {Stage::BeforeSubmit, Stage::AfterSubmit, Stage::PendingPoll}) {
            SCOPED_TRACE(shutdown_requested);
            SCOPED_TRACE(static_cast<int>(stage));
            std::atomic<bool> stopped{false}, shutdown{false};
            Executor::Cancellation cancellation{
                stopped, Executor::Clock::now() + 1h, &shutdown};
            FakeAcceptedBatch batch;
            batch.progress = {false, false, {}};
            auto cancel = [&] {
                if (shutdown_requested)
                    shutdown.store(true);
                else
                    cancellation.deadline = Executor::Clock::now();
            };
            if (stage == Stage::BeforeSubmit)
                cancel();
            else if (stage == Stage::AfterSubmit)
                batch.on_submit = cancel;
            else
                batch.on_poll = cancel;
            auto result = runSubBatch(batch, cancellation);
            EXPECT_TRUE(result.isCanceled());
            EXPECT_EQ(batch.submitted, stage == Stage::BeforeSubmit ? 0 : 1);
            EXPECT_EQ(batch.polls, stage == Stage::PendingPoll ? 1 : 0);
            EXPECT_EQ(batch.freed, 1);
        }
    }
}

TEST(BatchSubmit, FailureJoinsAcceptedSiblingWithoutExpiringIt) {
    Executor executor({2, 1, 2});
    std::barrier both_running(2);
    std::promise<void> cancel_seen, release_sibling;
    auto release = release_sibling.get_future().share();
    auto parent = std::async(std::launch::async, [&] {
        return executor.execute(
            2, Executor::Clock::now() + 10s,
            [&](auto range, const auto& cancellation) -> Executor::Result {
                both_running.arrive_and_wait();
                if (range.index == 0)
                    throw std::runtime_error("first sub-batch failed");
                while (!cancellation.requested()) std::this_thread::yield();
                cancel_seen.set_value();
                EXPECT_FALSE(cancellation.expired());
                release.wait();  // A callback still owns parent input.
                return Executor::Result::OK();
            });
    });
    SignalOnce release_sibling_gate(release_sibling);
    auto cancel_future = cancel_seen.get_future();
    ASSERT_EQ(cancel_future.wait_for(2s), std::future_status::ready)
        << "sibling did not observe cancellation";
    EXPECT_EQ(parent.wait_for(0s), std::future_status::timeout);
    release_sibling_gate.signal();
    EXPECT_EQ(parent.get().error, "first sub-batch failed");
    EXPECT_TRUE(
        executor
            .execute(1, Executor::Clock::now() + 1s,
                     [](auto, const auto&) { return Executor::Result::OK(); })
            .ok());
}

TEST(BatchSubmit, RetryDecisionIncludesEveryActualError) {
    using Result = Executor::Result;
    struct Case {
        Result first, second;
        bool submit_failed, retryable;
        std::string diagnostic;
    };
    for (const auto& test : {
             Case{Result::TransferError("native failure"), Result::Canceled(),
                  false, true, "native failure"},
             Case{Result::Canceled(), Result::TransferError("native failure"),
                  false, true, "native failure"},
             Case{Result::TransferError("native failure"),
                  Result::TransferError("another failure"), false, true,
                  "native failure"},
             Case{Result::TransferError("native failure"),
                  Result::SubmitError("submit error"), true, false,
                  "native failure"},
             Case{Result::TransferError("native failure"),
                  Result::Error("construction error"), false, false,
                  "native failure"},
         }) {
        SCOPED_TRACE(test.second.error);
        Executor executor({2, 1, 1});
        std::barrier running(2);
        auto result = executor.execute(
            2, Executor::Clock::now() + 5s, [&](auto range, const auto&) {
                running.arrive_and_wait();
                return range.index == 0 ? test.first : test.second;
            });
        EXPECT_EQ(result.error, test.diagnostic);
        EXPECT_EQ(result.submit_failed, test.submit_failed);
        EXPECT_EQ(result.retryable, test.retryable);
    }
}

TEST(BatchSubmit, ConcurrentParentsKeepInputAndErrorsSeparate) {
    Executor executor({4, 1, 2});
    std::barrier launch(2);
    std::vector<int> successful_input(8193, 0);
    auto failed = std::async(std::launch::async, [&] {
        launch.arrive_and_wait();
        return executor.execute(
            8193, Executor::Clock::now() + 10s, [](auto, const auto&) {
                return Executor::Result::Error("isolated failure");
            });
    });
    auto successful = std::async(std::launch::async, [&] {
        launch.arrive_and_wait();
        return executor.execute(
            successful_input.size(), Executor::Clock::now() + 10s,
            [&](auto range, const auto&) {
                for (size_t i = range.begin; i < range.end; ++i)
                    ++successful_input[i];
                return Executor::Result::OK();
            });
    });
    EXPECT_EQ(failed.get().error, "isolated failure");
    EXPECT_TRUE(successful.get().ok());
    for (int count : successful_input) EXPECT_EQ(count, 1);
}

TEST(BatchSubmit, ExpiredParentDoesNotPublishOrRunQueuedWork) {
    for (bool admission_full : {false, true}) {
        SCOPED_TRACE(admission_full);
        Executor executor({2, 1, admission_full ? 1u : 2u});
        std::promise<void> started, release;
        std::atomic<int> occupied{0};
        auto unblock = release.get_future().share();
        auto first = std::async(std::launch::async, [&] {
            return executor.execute(2, Executor::Clock::now() + 10s,
                                    [&](auto, const auto&) {
                                        if (++occupied == 2)
                                            started.set_value();
                                        unblock.wait();
                                        return Executor::Result::OK();
                                    });
        });
        SignalOnce release_workers(release);
        ASSERT_EQ(started.get_future().wait_for(2s), std::future_status::ready)
            << "occupied callbacks did not start";
        std::atomic<int> submitted{0};
        auto expired = std::async(std::launch::async, [&] {
            return executor.execute(4, Executor::Clock::now(),
                                    [&](auto, const auto&) {
                                        ++submitted;
                                        return Executor::Result::OK();
                                    });
        });
        // Both admission and queued-work expiry must return with workers
        // occupied.
        EXPECT_EQ(expired.wait_for(2s), std::future_status::ready);
        EXPECT_EQ(submitted.load(), 0);
        release_workers.signal();
        EXPECT_TRUE(first.get().ok());
        EXPECT_FALSE(expired.get().ok());
        EXPECT_EQ(submitted.load(), 0);
        EXPECT_TRUE(executor
                        .execute(1, Executor::Clock::now() + 1s,
                                 [](auto, const auto&) {
                                     return Executor::Result::OK();
                                 })
                        .ok());
    }
}

TEST(BatchSubmit, TimeoutJoinsStartedCallbacksAndPreservesErrors) {
    for (bool fail : {false, true}) {
        SCOPED_TRACE(fail);
        Executor executor({1, 1, 1});
        std::promise<void> release;
        std::promise<bool> timeout_seen;
        auto wait = release.get_future().share();
        auto parent = std::async(std::launch::async, [&] {
            return executor.execute(
                1, Executor::Clock::now() + 1s, [&](auto, const auto& stop) {
                    const auto until = stop.deadline + 2s;
                    while (!stop.stopped.load() &&
                           Executor::Clock::now() < until)
                        std::this_thread::yield();
                    timeout_seen.set_value(stop.stopped.load());
                    wait.wait();
                    return fail ? Executor::Result::TransferError(
                                      "late native failure")
                                : Executor::Result::OK();
                });
        });
        auto observed = timeout_seen.get_future();
        const auto ready = observed.wait_for(5s);
        EXPECT_EQ(ready, std::future_status::ready);
        if (ready == std::future_status::ready) {
            EXPECT_TRUE(observed.get());
        }
        EXPECT_EQ(parent.wait_for(0s), std::future_status::timeout);
        release.set_value();
        auto result = parent.get();
        EXPECT_FALSE(result.ok());
        EXPECT_FALSE(result.retryable);
        EXPECT_EQ(result.error, fail ? "late native failure"
                                     : "Parallel batch transfer timed out");
    }
}

template <class Engine, class Builder>
void expectIndependentSubmission(Engine& engine, uint64_t peer, Builder build,
                                 std::function<void()>& on_submit) {
    std::promise<void> submitted, release;
    auto ready = submitted.get_future();
    auto proceed = release.get_future().share();
    std::atomic<int> submits{0};
    on_submit = [&] {
        if (++submits == 1) submitted.set_value();
    };
    auto parent = std::async(std::launch::async, [&] {
        return engine.tryParallelBatchTransfer(
            peer, 2,
            [&](auto begin, auto end) {
                if (begin == 0) proceed.wait();
                return build(begin, end);
            },
            10s);
    });
    EXPECT_EQ(ready.wait_for(5s), std::future_status::ready);
    EXPECT_EQ(parent.wait_for(0s), std::future_status::timeout);
    release.set_value();
    auto result = parent.get();
    on_submit = {};
    ASSERT_TRUE(result.has_value());
    EXPECT_TRUE(result->ok()) << result->ToString();
    EXPECT_EQ(submits.load(), 2);
}

template <class Builder, class Probe>
void expectFailureInfoMatchesLegacy(TransferEngine& engine, uint64_t peer,
                                    Builder build, Probe& probe) {
    auto entries = build(0, 1);
    auto id = engine.allocateBatchID(entries.size());
    auto before = probe.submits.load();
    auto submitted = engine.submitTransfer(id, entries);
    TransferStatus status{};
    if (submitted.ok()) {
        EXPECT_TRUE(engine.getBatchTransferStatus(id, status).ok());
    }
    EXPECT_TRUE(engine.freeBatchID(id).ok());
    const auto legacy_submits = probe.submits.load() - before;
    before = probe.submits.load();
    const auto observed = probe.observed.size();
    TransferEngine::ParallelBatchTransferFailureInfo failure{true, true};
    auto result = engine.tryParallelBatchTransfer(peer, 1, build, 5s, &failure);
    EXPECT_EQ(result.has_value(), probe.supported.load());
    if (!result) {
        EXPECT_EQ(probe.submits.load(), before);
        EXPECT_FALSE(failure.submit_failed);
        EXPECT_FALSE(failure.retryable);
        return;
    }
    EXPECT_EQ(result->ok(), submitted.ok() && status.s == Transport::COMPLETED);
    using State = decltype(probe.terminal.load());
    EXPECT_EQ(result->ok(), submitted.ok() && !probe.fail_submit &&
                                probe.terminal.load() == State::COMPLETED);
    EXPECT_EQ(failure.submit_failed, !submitted.ok());
    EXPECT_EQ(failure.retryable,
              submitted.ok() && (status.s == Transport::FAILED ||
                                 status.s == Transport::TIMEOUT));
    EXPECT_EQ(probe.submits.load() - before, legacy_submits);
    if (probe.fail_submit) {
        EXPECT_EQ(probe.observed.size(), observed);
    }
}

class BatchSubmitProbe : public Transport {
   public:
    std::atomic<bool> supported{true};
    bool fail_submit = false;
    std::atomic<TransferStatusEnum> terminal{TransferStatusEnum::COMPLETED};
    std::atomic<int> submits{0};
    std::function<void()> on_submit;
    std::function<void(BatchID, const TransferRequest*)> on_batch_submit;
    std::mutex mutex;
    std::vector<TransferRequest> observed;

    bool supportsParallelBatchSubmit() const override {
        return supported.load();
    }
    Status submitTransfer(BatchID,
                          const std::vector<TransferRequest>&) override {
        return Status::NotImplemented("Use MultiTransport");
    }
    Status submitTransferTask(
        const std::vector<TransferTask*>& tasks) override {
        ++submits;
        if (fail_submit) return Status::Context("Injected native submit error");
        {
            std::lock_guard<std::mutex> lock(mutex);
            for (auto* task : tasks) {
                observed.push_back(*task->request);
                task->is_finished = true;
            }
        }
        if (on_submit) on_submit();
        if (on_batch_submit)
            on_batch_submit(tasks.front()->batch_id, tasks.front()->request);
        return Status::OK();
    }
    Status getTransferStatus(BatchID id, size_t index,
                             TransferStatus& status) override {
        status.s = terminal.load();
        status.transferred_bytes =
            toBatchDesc(id).task_list[index].request->length;
        return Status::OK();
    }
    const char* getName() const override {
        return "arbitrary-batch-submit-probe";
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
};

class ClassicBatchSubmit : public ::testing::Test {
   protected:
    std::string local = "local:1";
    std::shared_ptr<TransferMetadata> metadata =
        std::make_shared<TransferMetadata>(P2PHANDSHAKE);
    std::shared_ptr<MultiTransport> multi =
        std::make_shared<MultiTransport>(metadata, local);
    std::shared_ptr<BatchSubmitProbe> transport =
        std::make_shared<BatchSubmitProbe>();
    TransferEngine engine{false};

    void SetUp() override {
        MultiTransportBatchTestPeer::install(*multi, "probe", transport);
        add(1, "probe");
        TransferEngineImplTestPeer::classic(engine, multi);
    }
    void add(uint64_t peer, const std::string& protocol) {
        auto segment = std::make_shared<TransferMetadata::SegmentDesc>();
        segment->name = "peer:" + std::to_string(peer);
        segment->protocol = protocol;
        metadata->addLocalSegment(peer, segment->name, std::move(segment));
    }
    static std::vector<TransferRequest> requests(size_t begin, size_t end) {
        std::vector<TransferRequest> result;
        for (size_t i = begin; i < end; ++i) {
            TransferRequest request{};
            request.opcode = TransferRequest::WRITE;
            request.source = reinterpret_cast<void*>(0x1000 + i * 16);
            request.target_id = 1;
            request.target_offset = 0x2000 + i * 16;
            request.length = 16;
            request.priority = 2;
            request.intent_type = 3;
            request.nic_hint = 7;
            result.push_back(request);
        }
        return result;
    }
};

TEST_F(ClassicBatchSubmit, ArbitraryTransportNameAndRequestFieldsArePreserved) {
    auto unrelated = std::make_shared<BatchSubmitProbe>();
    unrelated->supported.store(false);
    MultiTransportBatchTestPeer::install(*multi, "other", unrelated);
    ASSERT_TRUE(engine.supportsParallelBatchSubmit(1));
    auto result = engine.tryParallelBatchTransfer(1, 5, requests, 10s);
    ASSERT_TRUE(result.has_value());
    ASSERT_TRUE(result->ok()) << result->ToString();
    EXPECT_EQ(transport->submits.load(), 2);
    ASSERT_EQ(transport->observed.size(), 5u);
    for (const auto& request : transport->observed) {
        EXPECT_EQ(request.nic_hint, 7);
        EXPECT_EQ(request.priority, 2);
        EXPECT_EQ(request.intent_type, 3);
        EXPECT_EQ(request.transport_hint, 0);
    }
}

TEST_F(ClassicBatchSubmit, FailureInfoMatchesLegacyAndResets) {
    using State = Transport::TransferStatusEnum;
    for (auto [state, submit_error, supported] :
         {std::tuple{State::FAILED, false, true},
          std::tuple{State::TIMEOUT, false, true},
          std::tuple{State::COMPLETED, true, true},
          std::tuple{State::COMPLETED, false, true},
          std::tuple{State::COMPLETED, false, false}}) {
        SCOPED_TRACE(state);
        SCOPED_TRACE(submit_error);
        SCOPED_TRACE(supported);
        transport->terminal.store(state);
        transport->fail_submit = submit_error;
        transport->supported.store(supported);
        expectFailureInfoMatchesLegacy(engine, 1, requests, *transport);
    }
}

TEST_F(ClassicBatchSubmit, MixedPeerDoesNotCheckOnlyTheFirstProtocol) {
    auto other = std::make_shared<BatchSubmitProbe>();
    other->supported.store(false);
    MultiTransportBatchTestPeer::install(*multi, "other", other);
    add(1, "probe,other");
    EXPECT_FALSE(engine.supportsParallelBatchSubmit(1));
#ifdef ENABLE_MULTI_PROTOCOL
    other->supported.store(true);
    EXPECT_TRUE(engine.supportsParallelBatchSubmit(1));
#endif
}

TEST_F(ClassicBatchSubmit,
       DeferredFreeKeepsRequestsUntilParentJoinAndIsNotTransferFailure) {
    std::promise<BatchID> published;
    std::promise<void> resume_sibling;
    auto resume = resume_sibling.get_future().share();
    const TransferRequest* borrowed = nullptr;
    transport->on_batch_submit = [&](BatchID id,
                                     const TransferRequest* request) {
        if (request->source != reinterpret_cast<void*>(0x1000)) return;
        borrowed = request;
        Transport::toBatchDesc(id).active_completion_callbacks.store(1);
        published.set_value(id);
    };
    auto parent = std::async(std::launch::async, [&] {
        return engine.tryParallelBatchTransfer(
            1, 2,
            [&](size_t begin, size_t end) {
                if (begin != 0) resume.wait();
                return requests(begin, end);
            },
            10s);
    });
    SignalOnce release_resume(resume_sibling);
    auto published_future = published.get_future();
    ASSERT_EQ(published_future.wait_for(5s), std::future_status::ready)
        << "submit hook did not publish the batch";
    const auto id = published_future.get();
    const auto until = BatchSubmitExecutor::Clock::now() + 5s;
    while (!MultiTransportBatchTestPeer::deferred(*multi, id) &&
           BatchSubmitExecutor::Clock::now() < until)
        std::this_thread::yield();
    EXPECT_TRUE(MultiTransportBatchTestPeer::deferred(*multi, id));
    EXPECT_EQ(parent.wait_for(0s), std::future_status::timeout);
    EXPECT_EQ(borrowed->opcode, TransferRequest::WRITE);
    EXPECT_EQ(borrowed->source, reinterpret_cast<void*>(0x1000));
    EXPECT_EQ(borrowed->length, 16u);
    // Native cleanup owns the descriptor until this callback pin drops.
    // Do not access the id again after releasing it.
    Transport::toBatchDesc(id).active_completion_callbacks.store(0);
    release_resume.signal();
    auto result = parent.get();
    ASSERT_TRUE(result.has_value());
    EXPECT_TRUE(result->ok()) << result->ToString();
    EXPECT_EQ(transport->submits.load(), 2);
}

TEST_F(ClassicBatchSubmit,
       ReadySubBatchSubmitsBeforeSlowSiblingFinishesBuilding) {
    expectIndependentSubmission(engine, 1, requests, transport->on_submit);
}

#ifdef USE_TENT
namespace tt = tent;
struct ProbeSubBatch : tt::Transport::SubBatch {
    std::vector<tt::Request> requests;
    size_t size() const override { return requests.size(); }
};

class TentBatchSubmitProbe : public tt::Transport {
   public:
    explicit TentBatchSubmitProbe(tt::TransportType type,
                                  bool file_only = false)
        : type_(type) {
        caps.dram_to_dram = !file_only;
        caps.dram_to_file = file_only;
    }
    std::atomic<bool> supported{true};
    std::atomic<tt::TransferStatusEnum> terminal{tt::COMPLETED};
    std::atomic<int> submits{0}, releases{0};
    bool fail_submit = false;
    std::atomic<bool> fail_poll_api{false};
    std::function<void()> on_submit;
    std::mutex mutex;
    std::vector<tt::Request> observed;
    bool supportsParallelBatchSubmit() const override {
        return supported.load();
    }
    tt::Status allocateSubBatch(SubBatchRef& batch, size_t) override {
        batch = new ProbeSubBatch;
        return tt::Status::OK();
    }
    tt::Status freeSubBatch(SubBatchRef& batch) override {
        ++releases;
        delete static_cast<ProbeSubBatch*>(batch);
        batch = nullptr;
        return tt::Status::OK();
    }
    tt::Status submitTransferTasks(
        SubBatchRef batch, const std::vector<tt::Request>& requests) override {
        ++submits;
        if (fail_submit)
            return tt::Status::InternalError("Injected native submit failure");
        static_cast<ProbeSubBatch*>(batch)->requests.insert(
            static_cast<ProbeSubBatch*>(batch)->requests.end(),
            requests.begin(), requests.end());
        {
            std::lock_guard<std::mutex> lock(mutex);
            observed.insert(observed.end(), requests.begin(), requests.end());
        }
        if (on_submit) on_submit();
        return tt::Status::OK();
    }
    tt::Status getTransferStatus(SubBatchRef batch, int id,
                                 tt::TransferStatus& status) override {
        if (fail_poll_api.exchange(false))
            return tt::Status::InvalidArgument(
                "Injected native poll API error");
        status.s = terminal.load();
        status.transferred_bytes =
            static_cast<ProbeSubBatch*>(batch)->requests.at(id).length;
        return tt::Status::OK();
    }
    tt::Status addMemoryBuffer(tt::BufferDesc& desc,
                               const tt::MemoryOptions&) override {
        desc.transports.push_back(type_);
        return tt::Status::OK();
    }
    tt::Status removeMemoryBuffer(tt::BufferDesc&) override {
        return tt::Status::OK();
    }
    const char* getName() const override { return "arbitrary-tent-probe"; }

   private:
    tt::TransportType type_;
};

std::shared_ptr<tt::Config> batchSubmitConfig() {
    auto config = std::make_shared<tt::Config>();
    config->set("metadata_type", "p2p");
    config->set("rpc_server_hostname", "127.0.0.1");
    config->set("rpc_server_port", "0");
    config->set("log_level", "warning");
    config->set("enable_runtime_queue", false);
    for (int type = 1; type < tt::kSupportedTransportTypes; ++type)
        config->set("transports/" +
                        std::string(tt::transportTypeName(
                            static_cast<tt::TransportType>(type))) +
                        "/enable",
                    false);
    return config;
}

class TentBatchSubmit : public ::testing::Test {
   protected:
    std::shared_ptr<tt::TransferEngine> engine =
        std::make_shared<tt::TransferEngine>(batchSubmitConfig());
    std::shared_ptr<TentBatchSubmitProbe> transport =
        std::make_shared<TentBatchSubmitProbe>(tt::TCP);
    std::vector<uint8_t> bytes = std::vector<uint8_t>(128);
    void SetUp() override {
        ASSERT_TRUE(engine->available());
        TransferEngineImplTestPeer::configure(*engine);
        TransferEngineImplTestPeer::impl(*engine).swapTransportForTest(
            tt::TCP, transport);
        ASSERT_TRUE(
            engine->registerLocalMemory(bytes.data(), bytes.size()).ok());
    }
    void TearDown() override {
        EXPECT_TRUE(engine->unregisterLocalMemory(bytes.data()).ok());
    }
    std::shared_ptr<tt::TransferEngine> remote(tt::SegmentID& id) {
        auto peer = std::make_shared<tt::TransferEngine>(batchSubmitConfig());
        EXPECT_TRUE(peer->available());
        auto peer_transport = std::make_shared<TentBatchSubmitProbe>(tt::TCP);
        TransferEngineImplTestPeer::impl(*peer).swapTransportForTest(
            tt::TCP, peer_transport);
        EXPECT_TRUE(peer->registerLocalMemory(bytes.data(), bytes.size()).ok());
        EXPECT_TRUE(TransferEngineImplTestPeer::machine(
                        *peer, "batch-submit-remote-machine")
                        .ok());
        EXPECT_TRUE(engine->openSegment(id, peer->getSegmentName()).ok());
        return peer;
    }

    template <class Request = tt::Request>
    std::vector<Request> requests(
        size_t begin, size_t end, int hint = tt::UNSPEC,
        typename Request::OpCode opcode = Request::WRITE) {
        std::vector<Request> result;
        for (size_t i = begin; i < end; ++i) {
            Request request{};
            request.opcode = opcode;
            request.source = bytes.data() + i * 16;
            request.target_id = LOCAL_SEGMENT_ID;
            request.target_offset = reinterpret_cast<uint64_t>(request.source);
            request.length = 16;
            request.priority = tt::PRIO_LOW;
            request.intent_type = static_cast<decltype(request.intent_type)>(
                tt::IntentType::CHECKPOINT);
            request.transport_hint =
                static_cast<decltype(request.transport_hint)>(hint);
            result.push_back(request);
        }
        return result;
    }
};

TEST_F(TentBatchSubmit, FacadePreservesFieldsThroughNativeBatch) {
    TransferEngine facade(false);
    TransferEngineImplTestPeer::facade(facade, engine);
    auto result = facade.tryParallelBatchTransfer(
        LOCAL_SEGMENT_ID, 4,
        [&](auto begin, auto end) {
            return requests<TransferRequest>(begin, end, tt::TCP,
                                             TransferRequest::READ);
        },
        10s);
    ASSERT_TRUE(result.has_value());
    ASSERT_TRUE(result->ok()) << result->ToString();
    EXPECT_EQ(transport->submits.load(), 2);
    EXPECT_EQ(transport->releases.load(), 2);
    size_t total = 0;
    for (const auto& request : transport->observed) {
        // Native merging may combine adjacent ranges.
        total += request.length;
        EXPECT_EQ(request.opcode, tt::Request::READ);
        EXPECT_EQ(request.transport_hint, tt::TCP);
        EXPECT_EQ(request.priority, tt::PRIO_LOW);
        EXPECT_EQ(request.intent_type, tt::IntentType::CHECKPOINT);
    }
    EXPECT_EQ(total, 4u * 16);
}

TEST_F(TentBatchSubmit, AdmissionRespectsLocalityAndMemoryCapabilities) {
    tt::SegmentID peer_id = 0;
    auto peer = remote(peer_id);
    auto& impl = TransferEngineImplTestPeer::impl(*engine);
    for (auto type : {tt::NVLINK, tt::SHM, tt::TPU, tt::MNNVL, tt::IOURING}) {
        SCOPED_TRACE(type);
        const bool file_only = type == tt::IOURING;
        const bool admitted = type != tt::MNNVL;
        auto unsupported =
            std::make_shared<TentBatchSubmitProbe>(type, file_only);
        unsupported->supported.store(false);
        impl.swapTransportForTest(type, unsupported);
        const auto target = file_only ? LOCAL_SEGMENT_ID : peer_id;
        EXPECT_EQ(engine->supportsParallelBatchSubmit(target), admitted);
        const auto submits = transport->submits.load();
        std::atomic<int> builds{0};
        auto result = engine->tryParallelBatchTransfer(
            target, 2,
            [&](auto begin, auto end) {
                ++builds;
                auto entries = requests(begin, end);
                for (auto& request : entries) request.target_id = target;
                return entries;
            },
            10s);
        EXPECT_EQ(result.has_value(), admitted);
        if (result) {
            EXPECT_TRUE(result->ok()) << result->ToString();
        }
        EXPECT_EQ(builds.load(), admitted ? 2 : 0);
        EXPECT_EQ(transport->submits.load() - submits, admitted ? 2 : 0);
        impl.swapTransportForTest(type, nullptr);
    }
    EXPECT_TRUE(engine->closeSegment(peer_id).ok());
    EXPECT_TRUE(peer->unregisterLocalMemory(bytes.data()).ok());
}

TEST_F(TentBatchSubmit, ReadySubBatchDoesNotWaitForAllBuilders) {
    expectIndependentSubmission(
        *engine, LOCAL_SEGMENT_ID,
        [&](auto begin, auto end) { return requests(begin, end); },
        transport->on_submit);
}

TEST_F(TentBatchSubmit, FacadeFailureInfoMatchesLegacyAndResets) {
    TransferEngine facade(false);
    TransferEngineImplTestPeer::facade(facade, engine);
    for (auto [state, submit_error, supported, hint] :
         {std::tuple{tt::FAILED, false, true, tt::TCP},
          std::tuple{tt::TIMEOUT, false, true, tt::TCP},
          std::tuple{tt::COMPLETED, true, true, tt::TCP},
          std::tuple{tt::COMPLETED, false, true, tt::RDMA},
          std::tuple{tt::COMPLETED, false, true, tt::TCP},
          std::tuple{tt::COMPLETED, false, false, tt::TCP}}) {
        SCOPED_TRACE(state);
        SCOPED_TRACE(submit_error);
        SCOPED_TRACE(supported);
        SCOPED_TRACE(hint);
        transport->terminal.store(state);
        transport->fail_submit = submit_error;
        transport->supported.store(supported);
        // TENT may turn a transport submit error into a retryable native
        // failure; a disabled explicit hint is an engine submit API error.
        expectFailureInfoMatchesLegacy(
            facade, LOCAL_SEGMENT_ID,
            [&](auto begin, auto end) {
                return requests<TransferRequest>(begin, end, hint);
            },
            *transport);
    }
    tt::TransferEngine::ParallelBatchTransferFailureInfo failure{true, true};
    EXPECT_FALSE(engine->tryParallelBatchTransfer(
        LOCAL_SEGMENT_ID, 1,
        [&](auto begin, auto end) { return requests(begin, end); }, 5s,
        &failure));
    EXPECT_FALSE(failure.submit_failed);
    EXPECT_FALSE(failure.retryable);
}

TEST_F(TentBatchSubmit,
       PollApiErrorUsesOrdinaryFreeWithoutRetryOrResubmission) {
    transport->fail_poll_api.store(true);
    tt::TransferEngine::ParallelBatchTransferFailureInfo failure{true, true};
    auto result = engine->tryParallelBatchTransfer(
        LOCAL_SEGMENT_ID, 1,
        [&](auto begin, auto end) { return requests(begin, end); }, 5s,
        &failure);
    ASSERT_TRUE(result.has_value());
    EXPECT_FALSE(result->ok());
    EXPECT_FALSE(failure.submit_failed);
    EXPECT_FALSE(failure.retryable);
    EXPECT_EQ(transport->submits.load(), 1);
    EXPECT_EQ(transport->releases.load(), 1);
}

TEST_F(TentBatchSubmit, FacadeRejectsUnsupportedProducerEvent) {
    TransferEngine facade(false);
    TransferEngineImplTestPeer::facade(facade, engine);
    auto result = facade.tryParallelBatchTransfer(
        LOCAL_SEGMENT_ID, 1,
        [&](auto begin, auto end) {
            auto entries = requests<TransferRequest>(begin, end, tt::TCP);
            for (auto& entry : entries)
                entry.producer_event = reinterpret_cast<void*>(uintptr_t{1});
            return entries;
        },
        10s);
    ASSERT_TRUE(result.has_value());
    EXPECT_FALSE(result->ok());
    EXPECT_EQ(transport->submits.load(), 0);
    EXPECT_TRUE(transport->observed.empty());
}
#endif
}  // namespace
}  // namespace mooncake
