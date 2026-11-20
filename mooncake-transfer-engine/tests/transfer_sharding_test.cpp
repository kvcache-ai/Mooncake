// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0
#include "transfer_engine.h"
#include "transfer_engine_impl.h"
#include "transfer_sharding.h"
#include <gtest/gtest.h>
#include <atomic>
#include <barrier>
#include <future>
#include <mutex>
#include <optional>
#include <set>

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
        engine.sharding_executor_ = std::make_shared<TransferShardingExecutor>(
            TransferShardingExecutor::Config{threads, 1, 4});
    }
#ifdef USE_TENT
    static tent::TransferEngineImpl& impl(tent::TransferEngine& engine) {
        return *engine.impl_;
    }
    static void configure(tent::TransferEngine& engine, size_t threads = 2) {
        engine.sharding_executor_ = std::make_shared<TransferShardingExecutor>(
            TransferShardingExecutor::Config{threads, 1, 4});
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
using Executor = TransferShardingExecutor;
using namespace std::chrono_literals;

TEST(TransferSharding, PreservesExactCoverageAndTail) {
    for (size_t n : {size_t(0), size_t(1), size_t(3), size_t(8193), SIZE_MAX}) {
        for (size_t p : {size_t(0), size_t(1), size_t(2), size_t(4)}) {
            auto ranges = Executor::partition(n, p);
            size_t end = 0;
            for (const auto& range : ranges) {
                EXPECT_EQ(range.begin, end);
                EXPECT_GT(range.end, range.begin);
                end = range.end;
            }
            EXPECT_EQ(end, p == 0 ? 0 : n);
        }
    }
}

TEST(TransferSharding, DefaultAndThresholdDoNotRunCallbacks) {
    Executor disabled({0, 4096, 1});
    EXPECT_FALSE(disabled.eligible(65536));
    Executor enabled({2, 4096, 1});
    EXPECT_FALSE(enabled.eligible(4095));
    EXPECT_TRUE(enabled.eligible(4096));
    EXPECT_FALSE(
        enabled
            .execute(4, Executor::Clock::now() + 1s,
                     [](auto, const auto&) -> Executor::Result {
                         ADD_FAILURE()
                             << "Below-threshold call entered the pool";
                         return Executor::Result::OK();
                     })
            .ok());
}

TEST(TransferSharding, BatchSubmitEnvironmentConfig) {
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

    ASSERT_EQ(setenv(threads.name, "4", 1), 0);
    ASSERT_EQ(setenv(min_size.name, "8192", 1), 0);
    Executor executor(Executor::Config::fromEnvironment());
    EXPECT_EQ(executor.config().threads, 4);
    EXPECT_FALSE(executor.eligible(8191));
    EXPECT_TRUE(executor.eligible(8192));

    ASSERT_EQ(setenv(threads.name, "33", 1), 0);
    EXPECT_THROW(Executor::Config::fromEnvironment(), std::invalid_argument);
    ASSERT_EQ(setenv(threads.name, "4", 1), 0);
    ASSERT_EQ(setenv(min_size.name, "0", 1), 0);
    EXPECT_THROW(Executor::Config::fromEnvironment(), std::invalid_argument);
}

TEST(TransferSharding, DetectsOverlapBeforeAnySubBatchCanPublish) {
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

TEST(TransferSharding, RunsIndependentSubBatchesConcurrently) {
    Executor executor({4, 1, 2});
    std::barrier rendezvous(4);
    std::mutex mutex;
    std::set<std::thread::id> threads;
    std::vector<int> touched(8193, 0);
    auto result =
        executor.execute(8193, Executor::Clock::now() + 10s,
                         [&](auto range, const auto&) -> Executor::Result {
                             {
                                 std::lock_guard<std::mutex> lock(mutex);
                                 threads.insert(std::this_thread::get_id());
                             }
                             rendezvous.arrive_and_wait();
                             for (size_t i = range.begin; i < range.end; ++i)
                                 ++touched[i];
                             return Executor::Result::OK();
                         });
    ASSERT_TRUE(result.ok()) << result.error;
    EXPECT_EQ(threads.size(), 4);
    for (auto value : touched) EXPECT_EQ(value, 1);
}

TEST(TransferSharding, OneThreadUsesTheWorkerAndRejectsRecursiveWait) {
    Executor executor({1, 1, 1});
    const auto caller = std::this_thread::get_id();
    auto result = executor.execute(
        1, Executor::Clock::now() + 1s,
        [&](auto, const auto&) -> Executor::Result {
            EXPECT_NE(std::this_thread::get_id(), caller);
            auto recursive = executor.execute(
                1, Executor::Clock::now() + 1s,
                [](auto, const auto&) { return Executor::Result::OK(); });
            EXPECT_FALSE(recursive.ok());
            return Executor::Result::OK();
        });
    EXPECT_TRUE(result.ok());
}

TEST(TransferSharding, AdmissionDeadlineDoesNotPublishWork) {
    Executor executor({1, 1, 1});
    std::promise<void> started, release;
    auto wait = release.get_future().share();
    auto first = std::async(std::launch::async, [&] {
        return executor.execute(1, Executor::Clock::now() + 10s,
                                [&](auto, const auto&) {
                                    started.set_value();
                                    wait.wait();
                                    return Executor::Result::OK();
                                });
    });
    started.get_future().wait();
    bool called = false;
    auto second =
        executor.execute(1, Executor::Clock::now(), [&](auto, const auto&) {
            called = true;
            return Executor::Result::OK();
        });
    EXPECT_FALSE(second.ok());
    EXPECT_FALSE(called);
    release.set_value();
    EXPECT_TRUE(first.get().ok());
}

TEST(TransferSharding, ExceptionSettlesEverySubBatchAndPoolRemainsUsable) {
    Executor executor({2, 1, 2});
    auto failed =
        executor.execute(17, Executor::Clock::now() + 1s,
                         [](auto, const auto&) -> Executor::Result {
                             throw std::runtime_error("allocation failed");
                         });
    EXPECT_FALSE(failed.ok());
    auto next = executor.execute(
        17, Executor::Clock::now() + 1s,
        [](auto, const auto&) { return Executor::Result::OK(); });
    EXPECT_TRUE(next.ok());
}

TEST(TransferSharding, ShutdownStopsAdmissionAndWaitsForActiveCallbacks) {
    Executor executor({2, 1, 1});
    std::promise<void> started;
    std::atomic<int> returned{0};
    auto parent = std::async(std::launch::async, [&] {
        return executor.execute(1, Executor::Clock::now() + 10s,
                                [&](auto, const auto& stop) {
                                    started.set_value();
                                    while (!stop.requested())
                                        std::this_thread::yield();
                                    ++returned;
                                    return Executor::Result{-1, "worker ended"};
                                });
    });
    started.get_future().wait();
    executor.shutdown();
    EXPECT_EQ(returned.load(), 1);
    EXPECT_FALSE(parent.get().ok());
    EXPECT_FALSE(
        executor
            .execute(1, Executor::Clock::now() + 1s,
                     [](auto, const auto&) { return Executor::Result::OK(); })
            .ok());
}

struct FakeAcceptedBatch {
    int submitted = 0, polls = 0, freed = 0;
    bool fail_submit = false, throw_poll = false, fail_status = false;
    std::function<void()> on_submit;
    Executor::Result allocate() { return Executor::Result::OK(); }
    Executor::Result submit() {
        ++submitted;
        if (on_submit) on_submit();
        return fail_submit ? Executor::Result{-1, "partial submission"}
                           : Executor::Result::OK();
    }
    SubBatchProgress poll() {
        ++polls;
        if (throw_poll) throw std::runtime_error("poll failure");
        return {true, !fail_status, {}};
    }
    Executor::Result release() {
        ++freed;
        return Executor::Result::OK();
    }
};

TEST(TransferSharding, SubBatchReleasesOnSuccessAndErrors) {
    for (int mode = 0; mode < 4; ++mode) {
        std::atomic<bool> stopped{false};
        FakeAcceptedBatch batch;
        batch.fail_submit = mode == 1;
        batch.throw_poll = mode == 2;
        batch.fail_status = mode == 3;
        auto result = runSubBatch(
            batch,
            Executor::Cancellation{stopped, Executor::Clock::now() + 1s});
        EXPECT_EQ(result.ok(), mode == 0);
        EXPECT_EQ(batch.submitted, 1);
        EXPECT_EQ(batch.freed, 1);
        EXPECT_EQ(batch.polls, mode == 1 ? 0 : 1);
    }
}

TEST(TransferSharding, AllocationFailureNeverSubmitsPollsOrReleasesAHandle) {
    struct UnallocatedBatch : FakeAcceptedBatch {
        Executor::Result allocate() { return {-1, "allocation failed"}; }
    } batch;
    std::atomic<bool> stopped{false};
    auto result = runSubBatch(
        batch, Executor::Cancellation{stopped, Executor::Clock::now() + 1s});
    EXPECT_FALSE(result.ok());
    EXPECT_EQ(result.error, "allocation failed");
    EXPECT_EQ(batch.submitted, 0);
    EXPECT_EQ(batch.polls, 0);
    EXPECT_EQ(batch.freed, 0);
}

TEST(TransferSharding, SubBatchReleasesOnTimeout) {
    std::atomic<bool> stopped{false};
    Executor::Cancellation cancellation{stopped, Executor::Clock::now() + 1h};
    FakeAcceptedBatch batch;
    batch.on_submit = [&] { cancellation.deadline = Executor::Clock::now(); };
    auto result = runSubBatch(batch, cancellation);
    EXPECT_FALSE(result.ok());
    EXPECT_EQ(batch.submitted, 1);
    EXPECT_EQ(batch.polls, 0);
    EXPECT_EQ(batch.freed, 1);
}

TEST(TransferSharding, SubBatchReleasesOnShutdown) {
    struct HeldBatch : FakeAcceptedBatch {
        std::promise<void> polled;
        SubBatchProgress poll() {
            if (++polls == 1) polled.set_value();
            return {false, false, {}};
        }
    } batch;
    std::atomic<bool> stopped{false}, shutdown{false};
    auto parent = std::async(std::launch::async, [&] {
        return runSubBatch(
            batch, Executor::Cancellation{stopped, Executor::Clock::now() + 1h,
                                          &shutdown});
    });
    batch.polled.get_future().wait();
    shutdown.store(true);
    EXPECT_FALSE(parent.get().ok());
    EXPECT_EQ(batch.freed, 1);
}

TEST(TransferSharding, InvalidConfigIsRejectedWithoutSpawningThreads) {
    EXPECT_THROW((Executor({33, 1, 1})), std::invalid_argument);
    EXPECT_THROW((Executor({1, 0, 1})), std::invalid_argument);
    EXPECT_THROW((Executor({1, 1, 0})), std::invalid_argument);
}

TEST(TransferSharding, FailureJoinsAcceptedSiblingWithoutExpiringIt) {
    Executor executor({2, 1, 2});
    std::barrier both_running(2);
    std::promise<void> cancel_seen, release_sibling;
    auto release = release_sibling.get_future().share();
    auto parent = std::async(std::launch::async, [&] {
        return executor.execute(
            2, Executor::Clock::now() + 10s,
            [&](auto range, const auto& cancellation) -> Executor::Result {
                both_running.arrive_and_wait();
                if (range.index == 0) return {-1, "first sub-batch failed"};
                while (!cancellation.requested()) std::this_thread::yield();
                cancel_seen.set_value();
                EXPECT_FALSE(cancellation.expired());
                release.wait();  // A callback still owns parent input.
                return Executor::Result::OK();
            });
    });
    cancel_seen.get_future().wait();
    EXPECT_EQ(parent.wait_for(0s), std::future_status::timeout);
    release_sibling.set_value();
    EXPECT_EQ(parent.get().error, "first sub-batch failed");
}

TEST(TransferSharding, ConcurrentParentsKeepInputAndErrorsSeparate) {
    Executor executor({2, 1, 2});
    std::barrier launch(2);
    std::vector<int> successful_input(8193, 0);
    auto failed = std::async(std::launch::async, [&] {
        launch.arrive_and_wait();
        return executor.execute(
            8193, Executor::Clock::now() + 10s, [](auto, const auto&) {
                return Executor::Result{-1, "isolated failure"};
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

TEST(TransferSharding, ExpiredQueuedParentDoesNotSubmitAnySubBatch) {
    Executor executor({2, 1, 2});
    std::promise<void> started, release;
    std::atomic<int> occupied{0};
    auto unblock = release.get_future().share();
    auto first = std::async(std::launch::async, [&] {
        return executor.execute(2, Executor::Clock::now() + 10s,
                                [&](auto, const auto&) {
                                    if (++occupied == 2) started.set_value();
                                    unblock.wait();
                                    return Executor::Result::OK();
                                });
    });
    started.get_future().wait();
    std::atomic<int> submitted{0};
    auto expired = std::async(std::launch::async, [&] {
        return executor.execute(4, Executor::Clock::now(),
                                [&](auto, const auto&) {
                                    ++submitted;
                                    return Executor::Result::OK();
                                });
    });
    // An expired queued parent must return while every worker is occupied.
    EXPECT_EQ(expired.wait_for(2s), std::future_status::ready);
    EXPECT_EQ(submitted.load(), 0);
    release.set_value();
    EXPECT_TRUE(first.get().ok());
    EXPECT_FALSE(expired.get().ok());
    EXPECT_EQ(submitted.load(), 0);
    EXPECT_TRUE(
        executor
            .execute(1, Executor::Clock::now() + 1s,
                     [](auto, const auto&) { return Executor::Result::OK(); })
            .ok());
}

TEST(TransferSharding, TimeoutJoinsStartedCallbacksAndPreservesErrors) {
    // Cover active-only, active+queued, and an actual error after expiry.
    for (auto [count, fail] :
         {std::pair{1, false}, std::pair{2, false}, std::pair{2, true}}) {
        SCOPED_TRACE(count);
        SCOPED_TRACE(fail);
        Executor executor({2, 1, 2});
        std::promise<void> occupied, release_other, release_active;
        std::promise<bool> timeout_seen;
        auto other_wait = release_other.get_future().share();
        auto active_wait = release_active.get_future().share();
        auto other = std::async(std::launch::async, [&] {
            return executor.execute(1, Executor::Clock::now() + 30s,
                                    [&](auto, const auto&) {
                                        occupied.set_value();
                                        other_wait.wait();
                                        return Executor::Result::OK();
                                    });
        });
        occupied.get_future().wait();
        std::atomic<int> calls{0};
        auto parent = std::async(std::launch::async, [&] {
            return executor.execute(
                count, Executor::Clock::now() + 1s,
                [&](auto, const auto& stop) -> Executor::Result {
                    ++calls;
                    // Wait for the caller's timeout handling, with a bound so
                    // a broken implementation still allows test cleanup.
                    const auto until = stop.deadline + 2s;
                    while (!stop.stopped.load() &&
                           Executor::Clock::now() < until)
                        std::this_thread::yield();
                    timeout_seen.set_value(stop.stopped.load());
                    active_wait.wait();
                    return fail
                               ? Executor::Result{-7, "Injected active failure"}
                               : Executor::Result::OK();
                });
        });
        auto observed = timeout_seen.get_future();
        auto observed_status = observed.wait_for(5s);
        EXPECT_EQ(observed_status, std::future_status::ready);
        if (observed_status == std::future_status::ready) {
            EXPECT_TRUE(observed.get());
        }
        EXPECT_EQ(parent.wait_for(0s), std::future_status::timeout);
        release_active.set_value();
        // The unrelated parent still occupies its worker.
        EXPECT_EQ(parent.wait_for(2s), std::future_status::ready);
        EXPECT_EQ(other.wait_for(0s), std::future_status::timeout);
        release_other.set_value();
        auto result = parent.get();
        EXPECT_TRUE(other.get().ok());
        EXPECT_EQ(calls.load(), 1);
        EXPECT_EQ(result.code, fail ? -7 : -1);
        EXPECT_EQ(result.error, fail ? "Injected active failure"
                                     : "Transfer sharding timed out");
        EXPECT_TRUE(executor
                        .execute(1, Executor::Clock::now() + 1s,
                                 [](auto, const auto&) {
                                     return Executor::Result::OK();
                                 })
                        .ok());
    }
}

TEST(TransferSharding, DeadlineBeforeSubmitStillReleasesAllocatedBatch) {
    std::atomic<bool> stopped{false};
    FakeAcceptedBatch batch;
    auto result = runSubBatch(
        batch, Executor::Cancellation{stopped, Executor::Clock::now()});
    EXPECT_FALSE(result.ok());
    EXPECT_EQ(batch.submitted, 0);
    EXPECT_EQ(batch.freed, 1);
    EXPECT_EQ(batch.polls, 0);
}
class ShardingProbe : public Transport {
   public:
    std::atomic<bool> supported{true};
    std::atomic<int> submits{0};
    std::function<void()> on_submit;
    std::function<void(BatchID, const TransferRequest*)> on_batch_submit;
    std::mutex mutex;
    std::vector<TransferRequest> observed;

    bool supportsShardedTransfer() const override { return supported.load(); }
    Status submitTransfer(BatchID,
                          const std::vector<TransferRequest>&) override {
        return Status::NotImplemented("Use MultiTransport");
    }
    Status submitTransferTask(
        const std::vector<TransferTask*>& tasks) override {
        ++submits;
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
        status.s = TransferStatusEnum::COMPLETED;
        status.transferred_bytes =
            toBatchDesc(id).task_list[index].request->length;
        return Status::OK();
    }
    const char* getName() const override { return "arbitrary-sharding-probe"; }

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

class ClassicSharding : public ::testing::Test {
   protected:
    std::string local = "local:1";
    std::shared_ptr<TransferMetadata> metadata =
        std::make_shared<TransferMetadata>(P2PHANDSHAKE);
    std::shared_ptr<MultiTransport> multi =
        std::make_shared<MultiTransport>(metadata, local);
    std::shared_ptr<ShardingProbe> transport =
        std::make_shared<ShardingProbe>();
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

TEST_F(ClassicSharding, ArbitraryTransportNameAndRequestFieldsArePreserved) {
    ASSERT_TRUE(engine.supportsShardedTransfer(1));
    auto result = engine.tryShardedTransfer(1, 5, requests, 10s);
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

TEST_F(ClassicSharding, UnsupportedAdmissionBuildsNothingAndFallsBackOnce) {
    transport->supported.store(false);
    int builds = 0;
    auto result = engine.tryShardedTransfer(
        1, 4,
        [&](auto begin, auto end) {
            ++builds;
            return requests(begin, end);
        },
        10s);
    EXPECT_FALSE(result.has_value());
    EXPECT_EQ(builds, 0);
    EXPECT_EQ(transport->submits.load(), 0);
    auto id = engine.allocateBatchID(4);
    auto entries = requests(0, 4);
    ASSERT_TRUE(engine.submitTransfer(id, entries).ok());
    EXPECT_EQ(transport->submits.load(), 1);
    EXPECT_TRUE(engine.freeBatchID(id).ok());
}

TEST_F(ClassicSharding, UnrelatedUnsupportedTransportDoesNotBlockThePeer) {
    auto unsupported = std::make_shared<ShardingProbe>();
    unsupported->supported.store(false);
    MultiTransportBatchTestPeer::install(*multi, "other", unsupported);
    int builds = 0;
    auto result = engine.tryShardedTransfer(
        1, 2,
        [&](size_t begin, size_t end) {
            ++builds;
            return requests(begin, end);
        },
        10s);
    ASSERT_TRUE(result.has_value());
    EXPECT_TRUE(result->ok());
    EXPECT_EQ(builds, 2);
    EXPECT_EQ(transport->submits.load(), 2);
    EXPECT_EQ(unsupported->submits.load(), 0);
}

TEST_F(ClassicSharding, MixedPeerDoesNotCheckOnlyTheFirstProtocol) {
    auto other = std::make_shared<ShardingProbe>();
    other->supported.store(false);
    MultiTransportBatchTestPeer::install(*multi, "other", other);
    add(1, "probe,other");
    int builds = 0;
    EXPECT_FALSE(engine.tryShardedTransfer(
        1, 2,
        [&](size_t begin, size_t end) {
            ++builds;
            return requests(begin, end);
        },
        10s));
    EXPECT_EQ(builds, 0);
    EXPECT_EQ(transport->submits.load(), 0);
    EXPECT_EQ(other->submits.load(), 0);
#ifdef ENABLE_MULTI_PROTOCOL
    other->supported.store(true);
    EXPECT_TRUE(engine.supportsShardedTransfer(1));
#endif
}

TEST_F(ClassicSharding,
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
        return engine.tryShardedTransfer(
            1, 2,
            [&](size_t begin, size_t end) {
                if (begin != 0) resume.wait();
                return requests(begin, end);
            },
            10s);
    });
    const auto id = published.get_future().get();
    const auto until = TransferShardingExecutor::Clock::now() + 5s;
    while (!MultiTransportBatchTestPeer::deferred(*multi, id) &&
           TransferShardingExecutor::Clock::now() < until)
        std::this_thread::yield();
    EXPECT_TRUE(MultiTransportBatchTestPeer::deferred(*multi, id));
    EXPECT_EQ(parent.wait_for(0s), std::future_status::timeout);
    EXPECT_EQ(borrowed->opcode, TransferRequest::WRITE);
    EXPECT_EQ(borrowed->source, reinterpret_cast<void*>(0x1000));
    EXPECT_EQ(borrowed->length, 16u);
    // Native cleanup owns the descriptor until this callback pin drops.
    // Do not access the id again after releasing it.
    Transport::toBatchDesc(id).active_completion_callbacks.store(0);
    resume_sibling.set_value();
    auto result = parent.get();
    ASSERT_TRUE(result.has_value());
    EXPECT_TRUE(result->ok()) << result->ToString();
    EXPECT_EQ(transport->submits.load(), 2);
}

TEST_F(ClassicSharding, ReadySubBatchSubmitsBeforeSlowSiblingFinishesBuilding) {
    std::promise<void> submitted, release;
    auto ready = submitted.get_future();
    auto proceed = release.get_future().share();
    std::atomic<int> count{0};
    transport->on_submit = [&] {
        if (++count == 1) submitted.set_value();
    };
    auto parent = std::async(std::launch::async, [&] {
        return engine.tryShardedTransfer(
            1, 2,
            [&](size_t begin, size_t end) {
                if (begin == 0) proceed.wait();
                return requests(begin, end);
            },
            10s);
    });
    EXPECT_EQ(ready.wait_for(5s), std::future_status::ready);
    EXPECT_EQ(parent.wait_for(0s), std::future_status::timeout);
    release.set_value();
    auto result = parent.get();
    ASSERT_TRUE(result.has_value());
    EXPECT_TRUE(result->ok()) << result->ToString();
    EXPECT_EQ(transport->submits.load(), 2);
}

#ifdef USE_TENT
namespace tt = tent;
struct ProbeSubBatch : tt::Transport::SubBatch {
    std::vector<tt::Request> requests;
    size_t size() const override { return requests.size(); }
};

class TentShardingProbe : public tt::Transport {
   public:
    explicit TentShardingProbe(tt::TransportType type) : type_(type) {
        caps.dram_to_dram = true;
    }
    std::atomic<bool> supported{true}, pending{false};
    std::atomic<int> submits{0}, releases{0};
    bool fail_submit = false;
    std::atomic<bool> fail_poll{false};
    std::function<void()> on_submit, on_poll;
    std::mutex mutex;
    std::vector<tt::Request> observed;
    bool supportsShardedTransfer() const override { return supported.load(); }
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
        if (on_poll) on_poll();
        status.s = pending.load()     ? tt::PENDING
                   : fail_poll.load() ? tt::FAILED
                                      : tt::COMPLETED;
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

std::shared_ptr<tt::Config> shardingConfig(bool queue = false) {
    auto config = std::make_shared<tt::Config>();
    config->set("metadata_type", "p2p");
    config->set("rpc_server_hostname", "127.0.0.1");
    config->set("rpc_server_port", "0");
    config->set("log_level", "warning");
    config->set("enable_runtime_queue", queue);
    for (int type = 1; type < tt::kSupportedTransportTypes; ++type)
        config->set("transports/" +
                        std::string(tt::transportTypeName(
                            static_cast<tt::TransportType>(type))) +
                        "/enable",
                    false);
    return config;
}

class TentSharding : public ::testing::Test {
   protected:
    std::shared_ptr<tt::TransferEngine> engine =
        std::make_shared<tt::TransferEngine>(shardingConfig());
    std::shared_ptr<TentShardingProbe> transport =
        std::make_shared<TentShardingProbe>(tt::TCP);
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
        auto peer = std::make_shared<tt::TransferEngine>(shardingConfig());
        EXPECT_TRUE(peer->available());
        auto peer_transport = std::make_shared<TentShardingProbe>(tt::TCP);
        TransferEngineImplTestPeer::impl(*peer).swapTransportForTest(
            tt::TCP, peer_transport);
        EXPECT_TRUE(peer->registerLocalMemory(bytes.data(), bytes.size()).ok());
        EXPECT_TRUE(TransferEngineImplTestPeer::machine(
                        *peer, "sharding-remote-machine")
                        .ok());
        EXPECT_TRUE(engine->openSegment(id, peer->getSegmentName()).ok());
        return peer;
    }
    std::vector<tt::Request> requests(size_t begin, size_t end) {
        std::vector<tt::Request> result;
        for (size_t i = begin; i < end; ++i) {
            tt::Request request{};
            request.opcode = tt::Request::WRITE;
            request.source = bytes.data() + i * 16;
            request.target_id = LOCAL_SEGMENT_ID;
            request.target_offset = reinterpret_cast<uint64_t>(request.source);
            request.length = 16;
            request.priority = tt::PRIO_LOW;
            request.intent_type = tt::IntentType::CHECKPOINT;
            result.push_back(request);
        }
        return result;
    }
};

TEST_F(TentSharding, PreservesFieldsThroughNativeBatch) {
    ASSERT_TRUE(engine->supportsShardedTransfer(LOCAL_SEGMENT_ID));
    auto result = engine->tryShardedTransfer(
        LOCAL_SEGMENT_ID, 4,
        [&](auto begin, auto end) { return requests(begin, end); }, 10s);
    ASSERT_TRUE(result.has_value());
    ASSERT_TRUE(result->ok()) << result->ToString();
    EXPECT_EQ(transport->submits.load(), 2);
    EXPECT_EQ(transport->releases.load(), 2);
    size_t total = 0;
    for (const auto& request : transport->observed) {
        total += request.length;  // Native merging may combine adjacent ranges.
        EXPECT_EQ(request.transport_hint, tt::UNSPEC);
        EXPECT_EQ(request.priority, tt::PRIO_LOW);
        EXPECT_EQ(request.intent_type, tt::IntentType::CHECKPOINT);
    }
    EXPECT_EQ(total, 4u * 16);
}

TEST_F(TentSharding, UnsupportedCandidateRejectsBeforeBuilder) {
    transport->supported.store(false);
    int builds = 0;
    auto result = engine->tryShardedTransfer(
        LOCAL_SEGMENT_ID, 4,
        [&](auto begin, auto end) {
            ++builds;
            return requests(begin, end);
        },
        10s);
    EXPECT_FALSE(result.has_value());
    EXPECT_EQ(builds, 0);
    EXPECT_EQ(transport->submits.load(), 0);
}

TEST_F(TentSharding, ExplicitHintDoesNotHideAnUnsupportedFailoverCandidate) {
    auto unsupported = std::make_shared<TentShardingProbe>(tt::RDMA);
    unsupported->supported.store(false);
    ASSERT_TRUE(engine->unregisterLocalMemory(bytes.data()).ok());
    TransferEngineImplTestPeer::impl(*engine).swapTransportForTest(tt::RDMA,
                                                                   unsupported);
    ASSERT_TRUE(engine->registerLocalMemory(bytes.data(), bytes.size()).ok());
    EXPECT_FALSE(engine->supportsShardedTransfer(LOCAL_SEGMENT_ID));
    int builds = 0;
    auto result = engine->tryShardedTransfer(
        LOCAL_SEGMENT_ID, 2,
        [&](size_t begin, size_t end) {
            ++builds;
            return requests(begin, end);
        },
        10s, tt::TCP);
    EXPECT_FALSE(result.has_value());
    EXPECT_EQ(builds, 0);
    EXPECT_EQ(transport->submits.load(), 0);
    EXPECT_EQ(unsupported->submits.load(), 0);
}

TEST_F(TentSharding, LocalOnlyTransportsDoNotBlockCrossMachineTransfer) {
    tt::SegmentID peer_id = 0;
    auto peer = remote(peer_id);
    for (auto type : {tt::NVLINK, tt::SHM, tt::TPU}) {
        auto local_only = std::make_shared<TentShardingProbe>(type);
        local_only->supported.store(false);
        TransferEngineImplTestPeer::impl(*engine).swapTransportForTest(
            type, local_only);
    }
    auto result = engine->tryShardedTransfer(
        peer_id, 2,
        [&](size_t begin, size_t end) {
            auto entries = requests(begin, end);
            for (auto& request : entries) request.target_id = peer_id;
            return entries;
        },
        10s);
    ASSERT_TRUE(result.has_value());
    EXPECT_TRUE(result->ok()) << result->ToString();
    EXPECT_EQ(transport->submits.load(), 2);
    EXPECT_TRUE(engine->closeSegment(peer_id).ok());
    EXPECT_TRUE(peer->unregisterLocalMemory(bytes.data()).ok());
}

TEST_F(TentSharding, CrossMachineMnnvlRemainsAPossibleCandidate) {
    tt::SegmentID peer_id = 0;
    auto peer = remote(peer_id);
    auto unsupported = std::make_shared<TentShardingProbe>(tt::MNNVL);
    unsupported->supported.store(false);
    TransferEngineImplTestPeer::impl(*engine).swapTransportForTest(tt::MNNVL,
                                                                   unsupported);
    int builds = 0;
    auto result = engine->tryShardedTransfer(
        peer_id, 2,
        [&](size_t, size_t) {
            ++builds;
            return std::vector<tt::Request>{};
        },
        10s);
    EXPECT_FALSE(result.has_value());
    EXPECT_EQ(builds, 0);
    EXPECT_TRUE(engine->closeSegment(peer_id).ok());
    EXPECT_TRUE(peer->unregisterLocalMemory(bytes.data()).ok());
}

TEST_F(TentSharding, FileOnlyTransportDoesNotBlockMemoryTransfer) {
    class FileOnly : public TentShardingProbe {
       public:
        FileOnly() : TentShardingProbe(tt::IOURING) {
            caps.dram_to_dram = false;
            caps.dram_to_file = true;
            supported.store(false);
        }
    };
    auto file = std::make_shared<FileOnly>();
    TransferEngineImplTestPeer::impl(*engine).swapTransportForTest(tt::IOURING,
                                                                   file);
    auto result = engine->tryShardedTransfer(
        LOCAL_SEGMENT_ID, 1,
        [&](size_t begin, size_t end) { return requests(begin, end); }, 10s);
    ASSERT_TRUE(result.has_value());
    EXPECT_TRUE(result->ok());
}

TEST_F(TentSharding, RuntimeQueueAcceptsWorkerCompatibleTransport) {
    tt::TransferEngine queued(shardingConfig(true));
    ASSERT_TRUE(queued.available());
    TransferEngineImplTestPeer::configure(queued);
    auto queued_transport = std::make_shared<TentShardingProbe>(tt::TCP);
    TransferEngineImplTestPeer::impl(queued).swapTransportForTest(
        tt::TCP, queued_transport);
    ASSERT_TRUE(queued.registerLocalMemory(bytes.data(), bytes.size()).ok());
    auto result = queued.tryShardedTransfer(
        LOCAL_SEGMENT_ID, 4,
        [&](size_t begin, size_t end) { return requests(begin, end); }, 10s);
    ASSERT_TRUE(result.has_value());
    EXPECT_TRUE(result->ok()) << result->ToString();
    EXPECT_GT(queued_transport->submits.load(), 0);
    EXPECT_TRUE(queued.unregisterLocalMemory(bytes.data()).ok());
}

TEST_F(TentSharding, ReadySubBatchDoesNotWaitForAllBuilders) {
    std::promise<void> submitted, release;
    auto ready = submitted.get_future();
    auto proceed = release.get_future().share();
    std::atomic<int> count{0};
    transport->on_submit = [&] {
        if (++count == 1) submitted.set_value();
    };
    auto parent = std::async(std::launch::async, [&] {
        return engine->tryShardedTransfer(
            LOCAL_SEGMENT_ID, 2,
            [&](size_t begin, size_t end) {
                if (begin == 0) proceed.wait();
                return requests(begin, end);
            },
            10s);
    });
    EXPECT_EQ(ready.wait_for(5s), std::future_status::ready);
    EXPECT_EQ(parent.wait_for(0s), std::future_status::timeout);
    release.set_value();
    auto result = parent.get();
    ASSERT_TRUE(result.has_value());
    EXPECT_TRUE(result->ok()) << result->ToString();
}

TEST_F(TentSharding, BuilderFailureWaitsForPendingSiblingWithoutReplay) {
    std::promise<void> submitted, failed;
    auto ready = submitted.get_future().share();
    auto rejected = failed.get_future();
    transport->pending.store(true);
    transport->on_submit = [&] { submitted.set_value(); };
    auto parent = std::async(std::launch::async, [&] {
        return engine->tryShardedTransfer(
            LOCAL_SEGMENT_ID, 2,
            [&](size_t begin, size_t end) {
                if (begin != 0) {
                    ready.wait();
                    failed.set_value();
                    throw std::runtime_error("Injected builder failure");
                }
                return requests(begin, end);
            },
            10s);
    });
    EXPECT_EQ(rejected.wait_for(5s), std::future_status::ready);
    EXPECT_EQ(parent.wait_for(0s), std::future_status::timeout);
    EXPECT_EQ(transport->releases.load(), 0);
    transport->pending.store(false);
    auto result = parent.get();
    ASSERT_TRUE(result.has_value());
    EXPECT_FALSE(result->ok());
    EXPECT_EQ(transport->submits.load(), 1);
    EXPECT_EQ(transport->releases.load(), 1);
}

TEST_F(TentSharding, SubBatchUsesNativeFailover) {
    auto first = std::make_shared<TentShardingProbe>(tt::RDMA);
    first->fail_poll.store(true);
    ASSERT_TRUE(engine->unregisterLocalMemory(bytes.data()).ok());
    TransferEngineImplTestPeer::impl(*engine).swapTransportForTest(tt::RDMA,
                                                                   first);
    ASSERT_TRUE(engine->registerLocalMemory(bytes.data(), bytes.size()).ok());
    auto result = engine->tryShardedTransfer(
        LOCAL_SEGMENT_ID, 1,
        [&](size_t begin, size_t end) { return requests(begin, end); }, 10s);
    ASSERT_TRUE(result.has_value());
    EXPECT_TRUE(result->ok()) << result->ToString();
    EXPECT_EQ(first->submits.load(), 1);
    EXPECT_EQ(transport->submits.load(), 1);
    EXPECT_EQ(first->releases.load(), 1);
}

TEST_F(TentSharding, NativeSubmitFailureDoesNotReplayTheParent) {
    transport->fail_submit = true;
    auto result = engine->tryShardedTransfer(
        LOCAL_SEGMENT_ID, 1,
        [&](size_t begin, size_t end) { return requests(begin, end); }, 10s);
    ASSERT_TRUE(result.has_value());
    EXPECT_FALSE(result->ok());
    EXPECT_EQ(transport->submits.load(), 1);
    EXPECT_TRUE(transport->observed.empty());
}

TEST_F(TentSharding, NativePollErrorUsesOrdinaryFreeWithoutAnotherSubmission) {
    std::atomic<int> polls{0};
    transport->on_poll = [&] {
        if (++polls == 1)
            throw std::runtime_error("Injected native poll failure");
    };
    auto result = engine->tryShardedTransfer(
        LOCAL_SEGMENT_ID, 1,
        [&](size_t begin, size_t end) { return requests(begin, end); }, 10s);
    ASSERT_TRUE(result.has_value());
    EXPECT_FALSE(result->ok());
    EXPECT_EQ(transport->submits.load(), 1);
    EXPECT_EQ(transport->releases.load(), 1);
    EXPECT_GT(polls.load(), 1);
}

TEST_F(TentSharding, FacadePreservesExplicitHintAndIntent) {
    TransferEngine facade(false);
    TransferEngineImplTestPeer::facade(facade, engine);
    auto result = facade.tryShardedTransfer(
        LOCAL_SEGMENT_ID, 4,
        [&](size_t begin, size_t end) {
            std::vector<TransferRequest> result;
            for (const auto& source : requests(begin, end)) {
                TransferRequest entry{};
                entry.opcode = TransferRequest::READ;
                entry.source = source.source;
                entry.target_id = source.target_id;
                entry.target_offset = source.target_offset;
                entry.length = source.length;
                entry.priority = 2;
                entry.intent_type = 4;
                entry.transport_hint = tt::TCP;
                result.push_back(entry);
            }
            return result;
        },
        10s, tt::TCP);
    ASSERT_TRUE(result.has_value());
    ASSERT_TRUE(result->ok()) << result->ToString();
    for (const auto& request : transport->observed) {
        EXPECT_EQ(request.transport_hint, tt::TCP);
        EXPECT_EQ(request.intent_type, tt::IntentType::CHECKPOINT);
        EXPECT_EQ(request.opcode, tt::Request::READ);
    }
}

TEST_F(TentSharding, FacadeRejectsUnsupportedProducerEvent) {
    TransferEngine facade(false);
    TransferEngineImplTestPeer::facade(facade, engine);
    auto result = facade.tryShardedTransfer(
        LOCAL_SEGMENT_ID, 1,
        [&](size_t begin, size_t end) {
            std::vector<TransferRequest> result;
            for (const auto& source : requests(begin, end)) {
                TransferRequest entry{};
                entry.opcode = TransferRequest::WRITE;
                entry.source = source.source;
                entry.target_id = source.target_id;
                entry.target_offset = source.target_offset;
                entry.length = source.length;
                entry.producer_event = reinterpret_cast<void*>(uintptr_t{1});
                result.push_back(entry);
            }
            return result;
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
