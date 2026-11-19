// transfer_task_test.cpp
#include "transfer_task.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <future>
#include <limits>
#include <memory>
#include <numeric>
#include <optional>
#include <string>
#include <stdexcept>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

#include "types.h"
#include "pinned_buffer_pool.h"
#include "transfer_engine_impl.h"
#ifdef USE_TENT
#include "tent/transfer_engine.h"
#include "tent/runtime/transfer_engine_impl.h"
#include "tent/runtime/transport.h"
#endif
#if defined(USE_CUDA) || defined(MOONCAKE_TEST_CUDA_H2D)
#include <cuda_runtime_api.h>
#endif

namespace mooncake {

class TransferEngineImplTestPeer {
   public:
#ifdef USE_TENT
    static void InstallNativeTransport(
        TransferEngine& engine, std::shared_ptr<tent::Transport> transport) {
        engine.getTentEngine()->impl_->swapTransportForTest(
            tent::TCP, std::move(transport));
    }
#endif
    static void InstallCpuFaultTransport(TransferEngine& engine,
                                         std::shared_ptr<Transport> transport) {
        engine.impl_->multi_transports_->transport_map_.clear();
        engine.impl_->multi_transports_->transport_map_.emplace(
            "tcp", std::move(transport));
        auto metadata = engine.impl_->getMetadata();
        auto self = metadata->getSegmentDescByID(LOCAL_SEGMENT_ID);
        if (!self) {
            self = std::make_shared<TransferMetadata::SegmentDesc>();
            self->name = engine.getLocalIpAndPort();
            auto held = self;
            metadata->addLocalSegment(LOCAL_SEGMENT_ID, self->name,
                                      std::move(held));
        }
        self->protocol = "tcp";
    }
};

namespace {

class ScopedTransferTimeoutEnv {
   public:
    explicit ScopedTransferTimeoutEnv(const char* value) {
        const char* current = std::getenv("MC_TRANSFER_TIMEOUT_SEC");
        if (current != nullptr) previous_ = current;

        if (value != nullptr) {
            ::setenv("MC_TRANSFER_TIMEOUT_SEC", value, 1);
        } else {
            ::unsetenv("MC_TRANSFER_TIMEOUT_SEC");
        }
    }

    ~ScopedTransferTimeoutEnv() {
        if (previous_.has_value()) {
            ::setenv("MC_TRANSFER_TIMEOUT_SEC", previous_->c_str(), 1);
        } else {
            ::unsetenv("MC_TRANSFER_TIMEOUT_SEC");
        }
    }

   private:
    std::optional<std::string> previous_;
};

}  // namespace

// Test fixture for TransferTask tests
// Device completion faults are injected below; status publication, batch
// ownership, and caller-memory drain use the production transfer code.
class ScopedEnvVar {
   public:
    ScopedEnvVar(const char* name, const char* value) : name_(name) {
        if (const char* old_value = std::getenv(name)) {
            had_old_value_ = true;
            old_value_ = old_value;
        }
        if (value) {
            setenv(name_.c_str(), value, 1);
        } else {
            unsetenv(name_.c_str());
        }
    }

    ~ScopedEnvVar() {
        if (had_old_value_) {
            setenv(name_.c_str(), old_value_.c_str(), 1);
        } else {
            unsetenv(name_.c_str());
        }
    }

   private:
    std::string name_;
    bool had_old_value_ = false;
    std::string old_value_;
};

class TransferTaskTest : public ::testing::Test {
   protected:
    void SetUp() override {
        // Initialize glog for logging
        google::InitGoogleLogging("TransferTaskTest");
        FLAGS_logtostderr = 1;  // Output logs to stderr
        unsetenv("MC_STORE_MEMCPY");
    }

    void TearDown() override {
        unsetenv("MC_STORE_MEMCPY");
        // Cleanup glog
        google::ShutdownGoogleLogging();
    }
};

TEST(TransferIntentTest, UsesStableNamedValues) {
    EXPECT_EQ(static_cast<int>(TransferIntent::kUnspecified),
              transfer_intent_values::kUnspecified);
    EXPECT_EQ(static_cast<int>(TransferIntent::kForegroundGet),
              transfer_intent_values::kForegroundGet);
    EXPECT_EQ(static_cast<int>(TransferIntent::kBackgroundPrefetch),
              transfer_intent_values::kBackgroundPrefetch);
    EXPECT_EQ(static_cast<int>(TransferIntent::kMigration),
              transfer_intent_values::kMigration);
}

TEST(TransferIntentTest, ParsesLegacyIntegerValues) {
    EXPECT_EQ(TransferIntentFromInt(0), TransferIntent::kUnspecified);
    EXPECT_EQ(TransferIntentFromInt(1), TransferIntent::kForegroundGet);
    EXPECT_EQ(TransferIntentFromInt(2), TransferIntent::kBackgroundPrefetch);
    EXPECT_EQ(TransferIntentFromInt(3), TransferIntent::kMigration);
    EXPECT_FALSE(TransferIntentFromInt(-1).has_value());
    EXPECT_FALSE(TransferIntentFromInt(4).has_value());
}

TEST(TransferScatterApiTest, SubmitterOwnsScatterOperation) {
    using ScatterRanges = std::vector<TransferEngine::ScatterTransferRange>;
    static_assert(std::is_same_v<
                  decltype(std::declval<TransferSubmitter&>().submitScatter(
                      std::declval<const ScatterRanges&>())),
                  TransferEngine::ScatterTransferOperation>);
    static_assert(
        std::is_same_v<
            decltype(std::declval<TransferSubmitter&>().submitNativeScatter(
                std::declval<const ScatterRanges&>(),
                TransferIntent::kForegroundGet)),
            StoreScatterTransferOperation>);
}

// Test MemcpyOperationState functionality
TEST_F(TransferTaskTest, MemcpyOperationState) {
    auto state = std::make_shared<MemcpyOperationState>();

    // Initially not completed
    EXPECT_FALSE(state->is_completed());
    EXPECT_EQ(state->get_strategy(), TransferStrategy::LOCAL_MEMCPY);

    // Set completed with success
    state->set_completed(ErrorCode::OK);
    EXPECT_TRUE(state->is_completed());
    EXPECT_EQ(state->get_result(), ErrorCode::OK);
}

TEST_F(TransferTaskTest, TransferFutureTimeoutDrainsDelayedMemcpyBeforeReturn) {
    auto state = std::make_shared<MemcpyOperationState>();
    TransferFuture future(state);
    std::string destination(64, '?');
    const std::string source(64, 'M');
    std::promise<void> release;
    auto released = release.get_future();
    std::thread writer([&] {
        released.wait();
        std::memcpy(destination.data(), source.data(), source.size());
        state->set_completed(ErrorCode::OK);
    });
    auto waiter =
        std::async(std::launch::async, [&] { return future.get(10); });
    EXPECT_EQ(waiter.wait_for(std::chrono::milliseconds(60)),
              std::future_status::timeout);
    release.set_value();
    EXPECT_EQ(waiter.get(), ErrorCode::TRANSFER_FAIL);
    writer.join();
    EXPECT_EQ(destination, source);
    EXPECT_TRUE(future.isReady());
    EXPECT_EQ(future.get(), ErrorCode::OK);
}

namespace {

// A CPU fault source with a real caller-memory write, held before the transport
// completion. These tasks use the production TE status and Slice completion
// path; only the device/CQE timing is injected.
class DelayedCpuBatch {
   private:
    // The fault source models classic BatchDesc/CQE completion, including in
    // binaries that also compile TENT support.
    ScopedEnvVar classic_tent_{"MC_USE_TENT", nullptr};
    ScopedEnvVar classic_tev1_{"MC_USE_TEV1", nullptr};

   public:
    explicit DelayedCpuBatch(size_t tasks) : engine(false) {
        EXPECT_EQ(engine.init("P2PHANDSHAKE", "localhost:17941"), 0);
        id = engine.allocateBatchID(tasks);
        auto& batch = Transport::toBatchDesc(id);
        batch.task_list.resize(tasks);
        for (auto& task : batch.task_list) {
            task.batch_id = id;
            task.slice_count = 1;
            task.total_bytes = 64;
            auto* slice = new Transport::Slice{};
            slice->task = &task;
            slice->length = 64;
            slice->status = Transport::Slice::PENDING;
            task.slice_list.push_back(slice);
        }
    }

    Transport::Slice* slice(size_t index) {
        return Transport::toBatchDesc(id).task_list[index].slice_list[0];
    }

    TransferEngine engine;
    BatchID id;
};

class ThrowOnceMemcpyState final : public OperationState {
   public:
    bool is_completed() override {
        std::lock_guard<std::mutex> lock(mutex_);
        return result_.has_value();
    }
    void wait_for_completion() override {
        if (!thrown_.exchange(true)) throw std::runtime_error("injected wait");
        std::unique_lock<std::mutex> lock(mutex_);
        cv_.wait(lock, [&] { return result_.has_value(); });
    }
    TransferStrategy get_strategy() const override {
        return TransferStrategy::LOCAL_MEMCPY;
    }
    void Finish() {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            result_ = ErrorCode::OK;
        }
        cv_.notify_all();
    }

   private:
    std::atomic<bool> thrown_{false};
};

class TransientQueryFailureTransport : public Transport {
   public:
    Status submitTransfer(BatchID,
                          const std::vector<TransferRequest>&) override {
        return Status::NotImplemented("completion injection only");
    }
    Status getTransferStatus(BatchID id, size_t index,
                             TransferStatus& status) override {
        if (!failed_.exchange(true)) return Status::Context("injected query");
        auto& task = toBatchDesc(id).task_list[index];
        if (__atomic_load_n(&task.success_slice_count, __ATOMIC_ACQUIRE) == 1) {
            status.s = TransferStatusEnum::COMPLETED;
            task.is_finished = true;
        } else {
            status.s = TransferStatusEnum::WAITING;
        }
        status.transferred_bytes = 0;
        return Status::OK();
    }
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
    const char* getName() const override { return "cpu-fault"; }

   private:
    std::atomic<bool> failed_{false};
};

// A CQE can arrive after a task status was sampled but before the subsequent
// physical-drain query. Keep that interleaving deterministic without hardware.
class CompletionAfterSnapshotTransport final
    : public TransientQueryFailureTransport {
   public:
    explicit CompletionAfterSnapshotTransport(bool success)
        : success_(success) {}

    Status getTransferStatus(BatchID id, size_t index,
                             TransferStatus& status) override {
        auto& task = toBatchDesc(id).task_list[index];
        if (!completed_) {
            status.s = TransferStatusEnum::WAITING;
            status.transferred_bytes = 0;
            if (success_)
                task.slice_list[0]->markSuccess();
            else
                task.slice_list[0]->markFailed();
            completed_ = true;
        } else {
            status.s = success_ ? TransferStatusEnum::COMPLETED
                                : TransferStatusEnum::FAILED;
            status.transferred_bytes = success_ ? task.total_bytes : 0;
            task.is_finished = true;
        }
        return Status::OK();
    }

    Status getTaskDrainStatus(BatchID id, size_t index,
                              bool& drained) override {
        const auto& task = toBatchDesc(id).task_list[index];
        // This CPU fixture has no accepted I/O after the injected completion,
        // including the failure case. Default Transport only proves success.
        drained =
            __atomic_load_n(&task.success_slice_count, __ATOMIC_ACQUIRE) +
                __atomic_load_n(&task.failed_slice_count, __ATOMIC_ACQUIRE) ==
            task.slice_count;
        return Status::OK();
    }

   private:
    const bool success_;
    bool completed_ = false;
};

class DelayedCpuTransport final : public Transport {
   public:
    explicit DelayedCpuTransport(bool throws, bool success = false)
        : throws_(throws), success_(success), release_(release.get_future()) {}
    ~DelayedCpuTransport() override {
        if (worker_.joinable()) worker_.join();
    }
    Status submitTransfer(BatchID,
                          const std::vector<TransferRequest>&) override {
        return Status::NotImplemented("task submission only");
    }
    Status submitTransferTask(
        const std::vector<TransferTask*>& tasks) override {
        auto* task = tasks.at(0);
        task->slice_count = 1;
        auto* slice = new Slice{};
        slice->task = task;
        slice->length = task->request->length;
        slice->status = Slice::PENDING;
        task->slice_list.push_back(slice);
        // Keep the test's injected WR self-contained, so the RED baseline can
        // complete after its early return without dereferencing a stale vector.
        const auto request = *task->request;
        const auto* borrowed = task->request;
        worker_ = std::thread([this, request, borrowed, slice] {
            release_.wait();
            // The success case also checks production descriptor ownership,
            // after the submitting method's local vector has left scope.
            const auto& active = success_ ? *borrowed : request;
            std::memcpy(active.source,
                        reinterpret_cast<const void*>(active.target_offset),
                        active.length);
            slice->markSuccess();
        });
        accepted.set_value();
        if (success_) return Status::OK();
        if (throws_) throw std::bad_alloc();
        return Status::Context("injected failure after accepting a WR");
    }
    Status getTransferStatus(BatchID id, size_t index,
                             TransferStatus& status) override {
        auto& task = toBatchDesc(id).task_list[index];
        const auto completed =
            __atomic_load_n(&task.success_slice_count, __ATOMIC_ACQUIRE);
        status.s = completed == 1 ? TransferStatusEnum::COMPLETED
                                  : TransferStatusEnum::WAITING;
        if (completed == 1) task.is_finished = true;
        status.transferred_bytes = completed ? task.total_bytes : 0;
        return Status::OK();
    }
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
    const char* getName() const override { return "tcp"; }
    void Join() {
        if (worker_.joinable()) worker_.join();
    }
    std::promise<void> accepted, release;

   private:
    bool throws_;
    bool success_;
    std::future<void> release_;
    std::thread worker_;
};

class SlowReadyMemcpyState final : public OperationState {
   public:
    SlowReadyMemcpyState(std::string& destination, bool false_first,
                         int delay_ms = 60)
        : destination_(destination),
          false_first_(false_first),
          delay_ms_(delay_ms) {}
    bool is_completed() override {
        if (ready_) return true;
        if (false_first_ && queries_++ == 0) return false;
        std::this_thread::sleep_for(std::chrono::milliseconds(delay_ms_));
        std::fill(destination_.begin(), destination_.end(), 'D');
        std::lock_guard lock(mutex_);
        result_ = ErrorCode::OK;
        ready_ = true;
        return true;
    }
    void wait_for_completion() override { (void)is_completed(); }
    TransferStrategy get_strategy() const override {
        return TransferStrategy::LOCAL_MEMCPY;
    }
    void HoldResultLock(std::promise<void>& entered) {
        std::lock_guard lock(mutex_);
        entered.set_value();
        std::this_thread::sleep_for(std::chrono::milliseconds(60));
    }

   private:
    std::string& destination_;
    bool false_first_, ready_{false};
    int delay_ms_;
    size_t queries_{0};
};

class SlowSuccessQueryTransport final : public Transport {
   public:
    SlowSuccessQueryTransport(std::string& destination, int delay_ms)
        : destination_(destination), delay_ms_(delay_ms) {}
    Status submitTransfer(BatchID,
                          const std::vector<TransferRequest>&) override {
        return Status::NotImplemented("CPU completion injection");
    }
    Status getTransferStatus(BatchID id, size_t index,
                             TransferStatus& status) override {
        auto& task = toBatchDesc(id).task_list[index];
        if (!completed_) {
            std::this_thread::sleep_for(std::chrono::milliseconds(delay_ms_));
            std::fill(destination_.begin(), destination_.end(), 'D');
            task.slice_list[0]->markSuccess();
            completed_ = true;
        }
        status.s = TransferStatusEnum::COMPLETED;
        status.transferred_bytes = task.total_bytes;
        return Status::OK();
    }
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
    const char* getName() const override { return "cpu-slow-success"; }

   private:
    std::string& destination_;
    int delay_ms_;
    bool completed_{false};
};

}  // namespace

TEST_F(TransferTaskTest,
       ExplicitDeadlineChecksSlowReadyAndResultCollectionBeforeSuccess) {
    for (bool false_first : {false, true}) {
        std::string destination(64, '?');
        auto state =
            std::make_shared<SlowReadyMemcpyState>(destination, false_first);
        TransferFuture future(state);
        EXPECT_EQ(future.get(5), ErrorCode::TRANSFER_FAIL)
            << "false_first=" << false_first;
        EXPECT_EQ(destination, std::string(64, 'D'));
        EXPECT_TRUE(future.isReady());
        // A per-call logical timeout does not poison a later physical result.
        EXPECT_EQ(future.get(), ErrorCode::OK);
    }
    std::string destination(64, '?');
    auto state = std::make_shared<SlowReadyMemcpyState>(destination, false, 0);
    TransferFuture future(state);
    ASSERT_TRUE(future.isReady());
    std::promise<void> entered;
    auto is_entered = entered.get_future();
    std::thread holder([&] { state->HoldResultLock(entered); });
    is_entered.wait();
    // Readiness is already cached, but collecting get_result still consumes
    // this call's budget while another observer holds its metadata mutex.
    EXPECT_EQ(future.get(5), ErrorCode::TRANSFER_FAIL);
    holder.join();
    EXPECT_EQ(destination, std::string(64, 'D'));
    EXPECT_EQ(future.get(), ErrorCode::OK);
}

TEST_F(TransferTaskTest,
       DefaultDeadlineChecksFirstReadyAndPreservesExplicitOverrideAndCache) {
    ScopedTransferTimeoutEnv env("1");
    struct Case {
        const char* name;
        int query_delay_ms;
        bool cached_before_deadline;
        int64_t explicit_timeout_ms;
        ErrorCode expected;
    };
    for (const auto& scenario :
         {Case{"first slow default query", 1100, false, 0,
               ErrorCode::TRANSFER_FAIL},
          Case{"cached before default expiry", 10, true, 0, ErrorCode::OK},
          Case{"explicit budget overrides default", 1100, false, 2000,
               ErrorCode::OK}}) {
        SCOPED_TRACE(scenario.name);
        std::string destination(64, '?');
        DelayedCpuBatch batch(1);
        SlowSuccessQueryTransport transport(destination,
                                            scenario.query_delay_ms);
        Transport::toBatchDesc(batch.id).task_list[0].transport_ = &transport;
        auto state = std::make_shared<TransferEngineOperationState>(
            batch.engine, batch.id, 1);
        TransferFuture future(state);
        if (scenario.cached_before_deadline) {
            ASSERT_TRUE(future.isReady());
            std::this_thread::sleep_for(std::chrono::milliseconds(1100));
        }
        EXPECT_EQ(future.get(scenario.explicit_timeout_ms), scenario.expected);
        EXPECT_EQ(destination, std::string(64, 'D'));
        bool drained = false;
        EXPECT_TRUE(batch.engine.getBatchDrainStatus(batch.id, drained).ok());
        EXPECT_TRUE(drained);
    }
}

TEST_F(TransferTaskTest, TransferFutureWaitExceptionStillDrainsCallerMemory) {
    auto state = std::make_shared<ThrowOnceMemcpyState>();
    TransferFuture future(state);
    std::string destination(64, '?');
    std::promise<void> release;
    auto released = release.get_future();
    std::thread writer([&] {
        released.wait();
        std::fill(destination.begin(), destination.end(), 'E');
        state->Finish();
    });
    auto waiter = std::async(std::launch::async, [&] { return future.get(); });
    EXPECT_EQ(waiter.wait_for(std::chrono::milliseconds(60)),
              std::future_status::timeout);
    release.set_value();
    EXPECT_NO_THROW(EXPECT_EQ(waiter.get(), ErrorCode::TRANSFER_FAIL));
    writer.join();
    EXPECT_EQ(destination, std::string(64, 'E'));
}

TEST_F(TransferTaskTest, AbandonedTransferFutureDrainsDelayedMemcpy) {
    auto state = std::make_shared<MemcpyOperationState>();
    std::string destination(64, '?');
    std::promise<void> release, abandoning;
    auto released = release.get_future();
    auto is_abandoning = abandoning.get_future();
    std::thread writer([&] {
        released.wait();
        std::fill(destination.begin(), destination.end(), 'A');
        state->set_completed(ErrorCode::OK);
    });
    auto owner = std::async(std::launch::async, [&] {
        TransferFuture future(state);
        abandoning.set_value();
    });
    is_abandoning.wait();
    EXPECT_EQ(owner.wait_for(std::chrono::milliseconds(60)),
              std::future_status::timeout);
    release.set_value();
    owner.get();
    writer.join();
    EXPECT_EQ(destination, std::string(64, 'A'));
}

TEST_F(TransferTaskTest, TransferFutureMoveAssignmentDrainsReplacedOperation) {
    auto old_state = std::make_shared<MemcpyOperationState>();
    auto new_state = std::make_shared<MemcpyOperationState>();
    new_state->set_completed(ErrorCode::OK);
    std::string destination(64, '?');
    std::promise<void> release, replacing;
    auto released = release.get_future();
    auto is_replacing = replacing.get_future();
    std::thread writer([&] {
        released.wait();
        std::fill(destination.begin(), destination.end(), 'V');
        old_state->set_completed(ErrorCode::OK);
    });
    auto owner = std::async(std::launch::async, [&] {
        TransferFuture future(old_state);
        TransferFuture replacement(new_state);
        replacing.set_value();
        future = std::move(replacement);
        return future.get();
    });
    is_replacing.wait();
    EXPECT_EQ(owner.wait_for(std::chrono::milliseconds(60)),
              std::future_status::timeout);
    release.set_value();
    EXPECT_EQ(owner.get(), ErrorCode::OK);
    writer.join();
    EXPECT_EQ(destination, std::string(64, 'V'));
}

TEST_F(TransferTaskTest,
       TransferFutureTimeoutDrainsRemainingTasksAfterFailure) {
    DelayedCpuBatch batch(2);
    batch.slice(0)->markFailed();
    auto* delayed = batch.slice(1);
    auto state = std::make_shared<TransferEngineOperationState>(batch.engine,
                                                                batch.id, 2);
    TransferFuture future(state);
    std::string destination(64, '?');
    std::promise<void> release;
    auto released = release.get_future();
    std::thread writer([&] {
        released.wait();
        std::fill(destination.begin(), destination.end(), 'T');
        delayed->markSuccess();
    });
    auto waiter =
        std::async(std::launch::async, [&] { return future.get(10); });
    EXPECT_EQ(waiter.wait_for(std::chrono::milliseconds(60)),
              std::future_status::timeout);
    release.set_value();
    EXPECT_EQ(waiter.get(), ErrorCode::TRANSFER_FAIL);
    writer.join();
    EXPECT_EQ(destination, std::string(64, 'T'));
    EXPECT_TRUE(future.isReady());
}

TEST_F(TransferTaskTest,
       TransferEngineConfiguredTimeoutStillDrainsDelayedTask) {
    ScopedTransferTimeoutEnv env("1");
    DelayedCpuBatch batch(1);
    auto* delayed = batch.slice(0);
    auto state = std::make_shared<TransferEngineOperationState>(batch.engine,
                                                                batch.id, 1);
    TransferFuture future(state);
    std::string destination(64, '?');
    std::promise<void> release;
    auto released = release.get_future();
    std::thread writer([&] {
        released.wait();
        std::fill(destination.begin(), destination.end(), 'C');
        delayed->markSuccess();
    });
    auto waiter = std::async(std::launch::async, [&] { return future.get(); });
    EXPECT_EQ(waiter.wait_for(std::chrono::milliseconds(1150)),
              std::future_status::timeout);
    release.set_value();
    EXPECT_EQ(waiter.get(), ErrorCode::TRANSFER_FAIL);
    writer.join();
    EXPECT_EQ(destination, std::string(64, 'C'));
    EXPECT_TRUE(future.isReady());
}

TEST_F(TransferTaskTest, TransferEngineQueryFailureStillDrainsDelayedTask) {
    DelayedCpuBatch batch(1);
    TransientQueryFailureTransport fault;
    Transport::toBatchDesc(batch.id).task_list[0].transport_ = &fault;
    auto* delayed = batch.slice(0);
    auto state = std::make_shared<TransferEngineOperationState>(batch.engine,
                                                                batch.id, 1);
    TransferFuture future(state);
    std::string destination(64, '?');
    std::promise<void> release;
    auto released = release.get_future();
    std::thread writer([&] {
        released.wait();
        std::fill(destination.begin(), destination.end(), 'Q');
        delayed->markSuccess();
    });
    auto waiter = std::async(std::launch::async, [&] { return future.get(); });
    EXPECT_EQ(waiter.wait_for(std::chrono::milliseconds(60)),
              std::future_status::timeout);
    release.set_value();
    EXPECT_EQ(waiter.get(), ErrorCode::TRANSFER_FAIL);
    writer.join();
    EXPECT_EQ(destination, std::string(64, 'Q'));
}

TEST_F(TransferTaskTest, CompletionAfterTaskSnapshotKeepsFinalOutcome) {
    for (bool success : {true, false}) {
        DelayedCpuBatch batch(1);
        CompletionAfterSnapshotTransport transport(success);
        Transport::toBatchDesc(batch.id).task_list[0].transport_ = &transport;
        auto state = std::make_shared<TransferEngineOperationState>(
            batch.engine, batch.id, 1);
        TransferFuture future(state);
        EXPECT_EQ(future.get(),
                  success ? ErrorCode::OK : ErrorCode::TRANSFER_FAIL);
        bool drained = false;
        EXPECT_TRUE(batch.engine.getBatchDrainStatus(batch.id, drained).ok());
        EXPECT_TRUE(drained);
    }
}

TEST_F(TransferTaskTest, FailedOrThrowingSubmissionDrainsAlreadyAcceptedWrite) {
    ScopedEnvVar classic_tent("MC_USE_TENT", nullptr);
    ScopedEnvVar classic_tev1("MC_USE_TEV1", nullptr);
    ScopedEnvVar memcpy_disabled("MC_STORE_MEMCPY", "0");
    for (bool throws : {false, true}) {
        TransferEngine engine(false);
        ASSERT_EQ(engine.init("P2PHANDSHAKE", "127.0.0.1:0"), 0);
        auto transport = std::make_shared<DelayedCpuTransport>(throws);
        auto accepted = transport->accepted.get_future();
        TransferEngineImplTestPeer::InstallCpuFaultTransport(engine, transport);
        std::shared_ptr<StorageBackend> backend;
        TransferSubmitter submitter(engine, backend,
                                    engine.getLocalIpAndPort());
        const std::string source(64, 'S');
        std::string destination(64, '?');
        auto caller = std::async(std::launch::async, [&] {
            try {
                auto future = submitter.submit_batch_get_offload_object(
                    engine.getLocalIpAndPort(), {"key"},
                    {reinterpret_cast<uintptr_t>(source.data())},
                    {{"key", {{destination.data(), destination.size()}}}},
                    OffloadBufferAccess::kTransferEngine);
                return future.has_value();
            } catch (const std::bad_alloc&) {
                return false;
            }
        });
        accepted.wait();
        EXPECT_EQ(caller.wait_for(std::chrono::milliseconds(60)),
                  std::future_status::timeout)
            << "submit throws=" << throws;
        transport->release.set_value();
        EXPECT_FALSE(caller.get());
        transport->Join();
        EXPECT_EQ(destination, source);
    }
}

TEST_F(TransferTaskTest, SubmittedRequestDescriptorsSurviveSubmittingMethod) {
    ScopedEnvVar classic_tent("MC_USE_TENT", nullptr);
    ScopedEnvVar classic_tev1("MC_USE_TEV1", nullptr);
    ScopedEnvVar memcpy_disabled("MC_STORE_MEMCPY", "0");
    TransferEngine engine(false);
    ASSERT_EQ(engine.init("P2PHANDSHAKE", "127.0.0.1:0"), 0);
    auto transport = std::make_shared<DelayedCpuTransport>(false, true);
    TransferEngineImplTestPeer::InstallCpuFaultTransport(engine, transport);
    std::shared_ptr<StorageBackend> backend;
    TransferSubmitter submitter(engine, backend, engine.getLocalIpAndPort());
    const std::string source(64, 'R');
    std::string destination(64, '?');
    auto future = submitter.submit_batch_get_offload_object(
        engine.getLocalIpAndPort(), {"key"},
        {reinterpret_cast<uintptr_t>(source.data())},
        {{"key", {{destination.data(), destination.size()}}}},
        OffloadBufferAccess::kTransferEngine);
    ASSERT_TRUE(future);
    transport->release.set_value();
    EXPECT_EQ(future->get(), ErrorCode::OK);
    transport->Join();
    EXPECT_EQ(destination, source);
}

#ifdef USE_EVENT_DRIVEN_COMPLETION
TEST_F(TransferTaskTest,
       TransferEngineWaitsForCallbackQuiescenceAfterPublishedCompletion) {
    DelayedCpuBatch batch(1);
    auto& desc = Transport::toBatchDesc(batch.id);
    auto* delayed = batch.slice(0);
    auto state = std::make_shared<TransferEngineOperationState>(batch.engine,
                                                                batch.id, 1);
    TransferFuture future(state);
    std::string destination(64, '?');
    std::promise<void> published, release;
    auto is_published = published.get_future();
    auto released = release.get_future();
    std::thread writer([&] {
        desc.active_completion_callbacks.fetch_add(1,
                                                   std::memory_order_acq_rel);
        delayed->markSuccess();
        published.set_value();
        released.wait();
        std::fill(destination.begin(), destination.end(), 'K');
        desc.active_completion_callbacks.fetch_sub(1,
                                                   std::memory_order_acq_rel);
    });
    is_published.wait();
    EXPECT_FALSE(future.isReady());
    auto waiter = std::async(std::launch::async, [&] { return future.get(); });
    EXPECT_EQ(waiter.wait_for(std::chrono::milliseconds(60)),
              std::future_status::timeout);
    release.set_value();
    EXPECT_EQ(waiter.get(), ErrorCode::OK);
    writer.join();
    EXPECT_EQ(destination, std::string(64, 'K'));
}
#endif

// Test MemcpyWorkerPool basic functionality
TEST_F(TransferTaskTest, MemcpyWorkerPoolBasic) {
    MemcpyWorkerPool pool;

    const size_t data_size = 512;
    std::vector<char> src_data(data_size, 'X');
    std::vector<char> dest_data(data_size, 'Y');

    auto state = std::make_shared<MemcpyOperationState>();

    // Create memcpy operations
    std::vector<MemcpyOperation> operations;
    operations.emplace_back(dest_data.data(), src_data.data(), data_size);

    // Create and submit task
    MemcpyTask task(std::move(operations), state);
    pool.submitTask(std::move(task));

    // Wait for completion
    state->wait_for_completion();

    // Verify completion and result
    EXPECT_TRUE(state->is_completed());
    EXPECT_EQ(state->get_result(), ErrorCode::OK);

    // Verify data was copied correctly
    for (size_t i = 0; i < data_size; ++i) {
        EXPECT_EQ(dest_data[i], 'X');
    }
}

// Test multiple memcpy operations in one task
TEST_F(TransferTaskTest, MemcpyWorkerPoolMultipleOperations) {
    MemcpyWorkerPool pool;

    const size_t num_ops = 3;
    const size_t data_size = 256;

    std::vector<std::vector<char>> src_buffers(num_ops);
    std::vector<std::vector<char>> dest_buffers(num_ops);

    // Initialize source buffers with different patterns
    for (size_t i = 0; i < num_ops; ++i) {
        src_buffers[i].resize(data_size, 'A' + i);
        dest_buffers[i].resize(data_size, 'Z');
    }

    auto state = std::make_shared<MemcpyOperationState>();

    // Create multiple memcpy operations
    std::vector<MemcpyOperation> operations;
    for (size_t i = 0; i < num_ops; ++i) {
        operations.emplace_back(dest_buffers[i].data(), src_buffers[i].data(),
                                data_size);
    }

    // Create and submit task
    MemcpyTask task(std::move(operations), state);
    pool.submitTask(std::move(task));

    // Wait for completion
    state->wait_for_completion();

    // Verify completion and result
    EXPECT_TRUE(state->is_completed());
    EXPECT_EQ(state->get_result(), ErrorCode::OK);

    // Verify all data was copied correctly
    for (size_t i = 0; i < num_ops; ++i) {
        for (size_t j = 0; j < data_size; ++j) {
            EXPECT_EQ(dest_buffers[i][j], 'A' + i);
        }
    }
}

TEST_F(TransferTaskTest, TransferScatterHandlesFragmentedCpuBuffers) {
    constexpr size_t kBufferSize = 512;
    constexpr size_t kFragmentCount = 128;
    std::vector<char> source(kBufferSize), destination(kBufferSize, 0);
    std::iota(source.begin(), source.end(), 0);
    std::vector<size_t> completions(kFragmentCount, 0);
    std::vector<size_t> destination_offsets, source_offsets,
        lengths(kFragmentCount, 1);
    for (size_t i = 0; i < kFragmentCount; ++i) {
        destination_offsets.push_back(i * 2);
        source_offsets.push_back(i * 3);
    }

    TransferEngine engine(false);
    ASSERT_EQ(engine.init("P2PHANDSHAKE", "localhost:17931"), 0);
    if (!engine.isUsingTent()) {
        ASSERT_NE(engine.installTransport("tcp", nullptr), nullptr);
    }
    ASSERT_EQ(engine.registerLocalMemory(source.data(), source.size(), "cpu:0"),
              0);
    ASSERT_EQ(engine.registerLocalMemory(destination.data(), destination.size(),
                                         "cpu:0"),
              0);

    ASSERT_TRUE(engine
                    .transferScatter({{
                        .opcode = TransferRequest::READ,
                        .remote_segment = engine.getLocalIpAndPort(),
                        .remote_base_offset =
                            reinterpret_cast<uintptr_t>(source.data()),
                        .remote_size = source.size(),
                        .local_buffer = destination.data(),
                        .local_capacity = destination.size(),
                        .local_offsets = destination_offsets,
                        .remote_offsets = source_offsets,
                        .lengths = lengths,
                        .on_fragment_complete =
                            [&](size_t i, const Status& status) {
                                EXPECT_TRUE(status.ok());
                                ++completions[i];
                            },
                    }})
                    .ok());
    for (size_t i = 0; i < kFragmentCount; ++i) {
        EXPECT_EQ(destination[destination_offsets[i]],
                  source[source_offsets[i]]);
        EXPECT_EQ(completions[i], 1u);
    }
}

#ifdef USE_TENT
TEST_F(TransferTaskTest, NativeScatterPreservesSharedRemoteSegment) {
    ScopedEnvVar use_tent("MC_USE_TENT", "1");
    ScopedEnvVar force_tcp("MC_FORCE_TCP", "1");
    ScopedEnvVar ipv6("MC_USE_IPV6", nullptr);
    ScopedEnvVar tcp_bind("MC_TCP_BIND_ADDRESS", "127.0.0.1");
    TransferEngine client(false), server(false);
    ASSERT_EQ(client.init("P2PHANDSHAKE", "127.0.0.1:0", "", 0, "tcp"), 0);
    ASSERT_EQ(server.init("P2PHANDSHAKE", "127.0.0.1:0", "", 0, "tcp"), 0);
    ASSERT_TRUE(client.isUsingTent());
    constexpr size_t kSize = 128;
    std::vector<char> source(kSize, 'T'), destination(kSize, 0);
    ASSERT_EQ(server.registerLocalMemory(source.data(), kSize, "cpu:0"), 0);
    ASSERT_EQ(client.registerLocalMemory(destination.data(), kSize, "cpu:0"),
              0);

    const auto peer = server.getLocalIpAndPort();
    const auto shared_handle = client.openSegment(peer);
    ASSERT_NE(shared_handle, static_cast<uint64_t>(ERR_INVALID_ARGUMENT));
    std::vector<SegmentBufferInfo> buffers;
    ASSERT_EQ(client.getSegmentBuffers(shared_handle, buffers), 0);
    std::vector<size_t> offsets{0};
    std::vector<size_t> lengths{kSize};
    std::shared_ptr<StorageBackend> backend;
    {
        TransferSubmitter submitter(client, backend,
                                    client.getLocalIpAndPort());
        for (bool invalid_range : {false, true}) {
            lengths[0] = invalid_range ? kSize + 1 : kSize;
            {
                auto operation = submitter.submitNativeScatter(
                    {{.opcode = TransferRequest::READ,
                      .remote_segment = peer,
                      .remote_base_offset =
                          reinterpret_cast<uintptr_t>(source.data()),
                      .remote_size = kSize,
                      .local_buffer = destination.data(),
                      .local_capacity = kSize,
                      .local_offsets = offsets,
                      .remote_offsets = offsets,
                      .lengths = lengths}},
                    TransferIntent::kForegroundGet);
                EXPECT_EQ(operation.wait().ok(), !invalid_range);
                // Another caller may still use the same idempotently opened
                // handle after this operation finishes or is destroyed.
                EXPECT_EQ(client.getSegmentBuffers(shared_handle, buffers), 0);
            }
            EXPECT_EQ(client.getSegmentBuffers(shared_handle, buffers), 0);
            EXPECT_EQ(client.openSegment(peer), shared_handle);
        }
    }
    EXPECT_EQ(destination, source);
    EXPECT_EQ(client.closeSegment(shared_handle), 0);
    EXPECT_EQ(client.unregisterLocalMemory(destination.data()), 0);
    EXPECT_EQ(server.unregisterLocalMemory(source.data()), 0);
}

namespace {
// Ordinary CPU transport: a terminal logical result is deliberately published
// before its destination access ends. No allocator fault hooks are involved.
class NativeDrainTransport final : public tent::Transport {
   public:
    struct AcceptedBatch final : SubBatch {
        std::vector<tent::Request> requests;
        size_t size() const override { return requests.size(); }
    };
    NativeDrainTransport() { caps.dram_to_dram = true; }
    tent::Status install(std::string&, std::shared_ptr<tent::ControlService>,
                         std::shared_ptr<tent::Topology>,
                         std::shared_ptr<tent::Config>) override {
        return tent::Status::OK();
    }
    tent::Status allocateSubBatch(SubBatchRef& batch,
                                  size_t capacity) override {
        auto owner = std::make_unique<AcceptedBatch>();
        owner->requests.reserve(capacity);
        batch = owner.release();
        return tent::Status::OK();
    }
    tent::Status freeSubBatch(SubBatchRef& batch) override {
        delete static_cast<AcceptedBatch*>(batch);
        batch = nullptr;
        ++freed;
        return tent::Status::OK();
    }
    tent::Status submitTransferTasks(
        SubBatchRef batch,
        const std::vector<tent::Request>& requests) override {
        auto* accepted = static_cast<AcceptedBatch*>(batch);
        accepted->requests = requests;
        live_batch = accepted;
        if (!notified.exchange(true)) submitted.set_value();
        if (throw_after_submit)
            throw std::runtime_error("accepted CPU transfer");
        return tent::Status::OK();
    }
    tent::Status getTransferStatus(SubBatchRef batch, int task,
                                   tent::TransferStatus& status) override {
        if (slow_status.exchange(false))
            std::this_thread::sleep_for(std::chrono::milliseconds(30));
        if (query_error.load())
            return tent::Status::InvalidArgument("transient CPU query failure");
        const auto* accepted = static_cast<AcceptedBatch*>(batch);
        if (task < 0 || static_cast<size_t>(task) >= accepted->size())
            return tent::Status::InvalidArgument("unpublished CPU request");
        status = {(logical_failure || task == failed_task) ? tent::FAILED
                                                           : tent::COMPLETED,
                  accepted->requests[task].length};
        return tent::Status::OK();
    }
    tent::Status getSubBatchDrainStatus(SubBatchRef, bool& drained) override {
        if (slow_drain.exchange(false))
            std::this_thread::sleep_for(std::chrono::milliseconds(30));
        drained = released.load(std::memory_order_acquire);
        return tent::Status::OK();
    }
    tent::Status addMemoryBuffer(tent::BufferDesc& buffer,
                                 const tent::MemoryOptions&) override {
        buffer.transports.push_back(tent::TCP);
        return tent::Status::OK();
    }
    tent::Status removeMemoryBuffer(tent::BufferDesc&) override {
        return tent::Status::OK();
    }
    const char* getName() const override { return "native-cpu-drain"; }

    void Release() {
        // The last destination write occurs before the physical drain flag.
        if (live_batch)
            for (const auto& request : live_batch->requests)
                std::memcpy(
                    request.source,
                    reinterpret_cast<const void*>(request.target_offset),
                    request.length);
        query_error.store(false);
        released.store(true, std::memory_order_release);
    }
    std::promise<void> submitted;
    std::atomic<bool> released{false}, query_error{false}, slow_status{false},
        slow_drain{false};
    std::atomic<int> freed{0};
    std::atomic<bool> notified{false};
    bool throw_after_submit = false, logical_failure = false;
    int failed_task = -1;

   private:
    AcceptedBatch* live_batch = nullptr;
};

class NativeStoreDrainTest : public TransferTaskTest {
   protected:
    void SetUp() override {
        TransferTaskTest::SetUp();
        engine = std::make_unique<TransferEngine>(false);
        ASSERT_TRUE(engine->isUsingTent());
        ASSERT_EQ(engine->init("P2PHANDSHAKE", "127.0.0.1:0"), 0);
        transport = std::make_shared<NativeDrainTransport>();
        TransferEngineImplTestPeer::InstallNativeTransport(*engine, transport);
        source.assign(32, 'N');
        destination.assign(34, '?');
        ASSERT_EQ(
            engine->registerLocalMemory(source.data(), source.size(), "cpu:0"),
            0);
        ASSERT_EQ(engine->registerLocalMemory(destination.data(),
                                              destination.size(), "cpu:0"),
                  0);
        submitter = std::make_unique<TransferSubmitter>(
            *engine, backend, engine->getLocalIpAndPort());
    }
    void TearDown() override {
        submitter.reset();
        if (engine) {
            EXPECT_EQ(engine->unregisterLocalMemory(source.data()), 0);
            EXPECT_EQ(engine->unregisterLocalMemory(destination.data()), 0);
        }
        engine.reset();
        TransferTaskTest::TearDown();
    }
    TransferEngine::ScatterTransferRange Range(std::atomic<int>& callbacks,
                                               std::atomic<bool>& success) {
        return {.opcode = TransferRequest::READ,
                .remote_segment = engine->getLocalIpAndPort(),
                .remote_base_offset = reinterpret_cast<uint64_t>(source.data()),
                .remote_size = source.size(),
                .local_buffer = destination.data(),
                .local_capacity = destination.size(),
                .local_offsets = local_offsets,
                .remote_offsets = remote_offsets,
                .lengths = lengths,
                .on_fragment_complete = [&](size_t, const Status& status) {
                    success.store(status.ok());
                    ++callbacks;
                }};
    }
    std::optional<TransferFuture> Future() {
        return submitter->submit_batch_get_offload_object(
            engine->getLocalIpAndPort(), {"native"},
            {reinterpret_cast<uint64_t>(source.data())},
            {{"native", {{destination.data() + 1, source.size()}}}},
            OffloadBufferAccess::kTransferEngine,
            TransferIntent::kForegroundGet);
    }
    void CheckBytes() {
        EXPECT_EQ(destination.front(), '?');
        EXPECT_EQ(destination.back(), '?');
        EXPECT_EQ(destination.substr(1, source.size()), source);
        EXPECT_EQ(transport->freed.load(), 1);
    }
    ScopedEnvVar native_enabled{"MC_USE_TENT", "1"};
    ScopedEnvVar loopback{"MOONCAKE_LOCAL_HOSTNAME", "127.0.0.1"};
    ScopedEnvVar config{
        "MC_TENT_CONF",
        R"({"transports":{"tcp":{"enable":false},"rdma":{"enable":false},"shm":{"enable":false}},"metrics":{"enabled":false},"merge_requests":false,"enable_progress_worker":false})"};
    std::unique_ptr<TransferEngine> engine;
    std::shared_ptr<NativeDrainTransport> transport;
    std::shared_ptr<StorageBackend> backend;
    std::unique_ptr<TransferSubmitter> submitter;
    std::string source, destination;
    std::vector<size_t> local_offsets{1}, remote_offsets{0}, lengths{32};
};

TEST_F(NativeStoreDrainTest, FutureTimeoutRetainsDestinationUntilDrain) {
    auto future = Future();
    ASSERT_TRUE(future);
    auto waiter =
        std::async(std::launch::async, [&] { return future->get(1); });
    EXPECT_EQ(waiter.wait_for(std::chrono::milliseconds(20)),
              std::future_status::timeout);
    EXPECT_EQ(transport->freed.load(), 0);
    transport->Release();
    EXPECT_EQ(waiter.get(), ErrorCode::TRANSFER_FAIL);
    future.reset();
    CheckBytes();
}

TEST_F(NativeStoreDrainTest, AbandonedFutureDrainsAcceptedWrite) {
    auto future = Future();
    ASSERT_TRUE(future);
    auto waiter = std::async(std::launch::async, [&] { future.reset(); });
    EXPECT_EQ(waiter.wait_for(std::chrono::milliseconds(20)),
              std::future_status::timeout);
    transport->Release();
    waiter.get();
    CheckBytes();
}

TEST_F(NativeStoreDrainTest, SubmissionExceptionDrainsAcceptedPrefix) {
    transport->throw_after_submit = true;
    auto accepted = transport->submitted.get_future();
    auto waiter = std::async(std::launch::async, [&] {
        try {
            (void)Future();
            return false;
        } catch (const std::runtime_error&) {
            return true;
        }
    });
    EXPECT_EQ(accepted.wait_for(std::chrono::seconds(2)),
              std::future_status::ready);
    EXPECT_EQ(waiter.wait_for(std::chrono::milliseconds(20)),
              std::future_status::timeout);
    transport->Release();
    EXPECT_TRUE(waiter.get());
    CheckBytes();
}

TEST_F(NativeStoreDrainTest, ScatterFailureDefersCallbacksUntilPhysicalDrain) {
    transport->logical_failure = true;
    std::atomic<int> callbacks{0};
    std::atomic<bool> success{true};
    auto operation = submitter->submitNativeScatter(
        {Range(callbacks, success)}, TransferIntent::kForegroundGet);
    EXPECT_FALSE(operation.waitFor(std::chrono::milliseconds(1)).ok());
    EXPECT_EQ(callbacks.load(), 0);
    auto waiter =
        std::async(std::launch::async, [&] { return operation.wait(); });
    EXPECT_EQ(waiter.wait_for(std::chrono::milliseconds(20)),
              std::future_status::timeout);
    transport->Release();
    EXPECT_FALSE(waiter.get().ok());
    EXPECT_EQ(callbacks.load(), 1);
    EXPECT_FALSE(success.load());
    CheckBytes();
}

TEST_F(NativeStoreDrainTest, ScatterPreservesSuccessfulFragmentsAfterDrain) {
    transport->failed_task = 1;
    local_offsets = {1, 17};
    remote_offsets = {0, 16};
    lengths = {16, 16};
    std::atomic<int> callbacks{0};
    std::atomic<bool> success{true};
    std::array<bool, 2> fragment_ok{false, true};
    auto range = Range(callbacks, success);
    range.on_fragment_complete = [&](size_t index, const Status& status) {
        fragment_ok[index] = status.ok();
        ++callbacks;
    };
    auto operation =
        submitter->submitNativeScatter({range}, TransferIntent::kForegroundGet);
    auto waiter =
        std::async(std::launch::async, [&] { return operation.wait(); });
    EXPECT_EQ(waiter.wait_for(std::chrono::milliseconds(20)),
              std::future_status::timeout);
    EXPECT_EQ(callbacks.load(), 0);
    transport->Release();
    EXPECT_FALSE(waiter.get().ok());
    EXPECT_EQ(callbacks.load(), 2);
    EXPECT_TRUE(fragment_ok[0]);
    EXPECT_FALSE(fragment_ok[1]);
    CheckBytes();
}

TEST_F(NativeStoreDrainTest,
       ScatterQueryErrorDefersCallbacksUntilPhysicalDrain) {
    transport->query_error.store(true);
    std::atomic<int> callbacks{0};
    std::atomic<bool> success{true};
    auto operation = submitter->submitNativeScatter(
        {Range(callbacks, success)}, TransferIntent::kForegroundGet);
    EXPECT_FALSE(operation.waitFor(std::chrono::milliseconds(1)).ok());
    EXPECT_EQ(callbacks.load(), 0);
    transport->Release();
    EXPECT_FALSE(operation.wait().ok());
    EXPECT_EQ(callbacks.load(), 1);
    EXPECT_FALSE(success.load());
    CheckBytes();
}

TEST_F(NativeStoreDrainTest,
       ScatterDeadlineIncludesFinalStatusAndDrainQueries) {
    for (bool slow_drain : {false, true}) {
        transport->released.store(false);
        std::atomic<int> callbacks{0};
        std::atomic<bool> success{true};
        auto operation = submitter->submitNativeScatter(
            {Range(callbacks, success)}, TransferIntent::kForegroundGet);
        transport->Release();
        transport->slow_drain.store(slow_drain);
        transport->slow_status.store(!slow_drain);
        EXPECT_FALSE(operation.waitFor(std::chrono::milliseconds(1)).ok());
        EXPECT_EQ(callbacks.load(), 1);
        EXPECT_FALSE(success.load());
        EXPECT_FALSE(operation.wait().ok());
        EXPECT_EQ(callbacks.load(), 1);
    }
    EXPECT_EQ(transport->freed.load(), 2);
}

TEST_F(NativeStoreDrainTest, ScatterDestructorWaitsForPhysicalDrain) {
    std::atomic<int> callbacks{0};
    std::atomic<bool> success{false};
    auto operation = submitter->submitNativeScatter(
        {Range(callbacks, success)}, TransferIntent::kForegroundGet);
    auto waiter = std::async(std::launch::async,
                             [operation = std::move(operation)]() mutable {
                                 auto local = std::move(operation);
                                 // local's destructor drains before return.
                             });
    EXPECT_EQ(waiter.wait_for(std::chrono::milliseconds(20)),
              std::future_status::timeout);
    EXPECT_EQ(callbacks.load(), 0);
    transport->Release();
    waiter.get();
    EXPECT_EQ(callbacks.load(), 1);
    EXPECT_TRUE(success.load());
    CheckBytes();
}
}  // namespace
#endif

#ifdef USE_TENT
TEST_F(TransferTaskTest, TransferSubmitterScatterUsesTentDataPath) {
    TransferEngine engine(false);
    ASSERT_EQ(engine.init("P2PHANDSHAKE", "localhost:17934"), 0);
    if (!engine.isUsingTent()) GTEST_SKIP() << "TENT is unavailable";

    constexpr size_t kSize = 128;
    std::vector<char> source(kSize, 'T'), destination(kSize, 0);
    std::vector<size_t> offsets{0};
    std::vector<size_t> lengths{kSize};
    ASSERT_EQ(engine.registerLocalMemory(source.data(), kSize, "cpu:0"), 0);
    ASSERT_EQ(engine.registerLocalMemory(destination.data(), kSize, "cpu:0"),
              0);

    std::shared_ptr<StorageBackend> backend;
    TransferSubmitter submitter(engine, backend, engine.getLocalIpAndPort());
    auto operation = submitter.submitNativeScatter(
        {{.opcode = TransferRequest::READ,
          .remote_segment = engine.getLocalIpAndPort(),
          .remote_base_offset = reinterpret_cast<uintptr_t>(source.data()),
          .remote_size = kSize,
          .local_buffer = destination.data(),
          .local_capacity = kSize,
          .local_offsets = offsets,
          .remote_offsets = offsets,
          .lengths = lengths}},
        TransferIntent::kForegroundGet);
    ASSERT_TRUE(operation.wait().ok());
    EXPECT_EQ(destination, source);
    EXPECT_EQ(engine.freeEngine(), 0);
}

TEST_F(TransferTaskTest, TransferSubmitterScatterContainsCallbackFailure) {
    TransferEngine engine(false);
    ASSERT_EQ(engine.init("P2PHANDSHAKE", "localhost:17935"), 0);
    if (!engine.isUsingTent()) GTEST_SKIP() << "TENT is unavailable";

    std::vector<char> source(8, 'T'), destination(8, 0);
    std::vector<size_t> offsets{0};
    std::vector<size_t> lengths{8};
    ASSERT_EQ(engine.registerLocalMemory(source.data(), source.size(), "cpu:0"),
              0);
    ASSERT_EQ(engine.registerLocalMemory(destination.data(), destination.size(),
                                         "cpu:0"),
              0);

    std::shared_ptr<StorageBackend> backend;
    TransferSubmitter submitter(engine, backend, engine.getLocalIpAndPort());
    auto operation = submitter.submitNativeScatter(
        {{.opcode = TransferRequest::READ,
          .remote_segment = engine.getLocalIpAndPort(),
          .remote_base_offset = reinterpret_cast<uintptr_t>(source.data()),
          .remote_size = source.size(),
          .local_buffer = destination.data(),
          .local_capacity = destination.size(),
          .local_offsets = offsets,
          .remote_offsets = {},
          .lengths = lengths,
          .on_fragment_complete =
              [](size_t, const Status&) {
                  throw std::runtime_error("callback failure");
              }}},
        TransferIntent::kForegroundGet);
    EXPECT_FALSE(operation.wait().ok());
    EXPECT_EQ(engine.freeEngine(), 0);
}

TEST_F(TransferTaskTest, TransferSubmitterScatterRejectsInvalidRangeIntent) {
    TransferEngine engine(false);
    ASSERT_EQ(engine.init("P2PHANDSHAKE", "localhost:17936"), 0);
    if (!engine.isUsingTent()) GTEST_SKIP() << "TENT is unavailable";

    std::vector<char> source(8, 'T'), destination(8, 0);
    std::vector<size_t> offsets{0};
    std::vector<size_t> lengths{8};
    ASSERT_EQ(engine.registerLocalMemory(source.data(), source.size(), "cpu:0"),
              0);
    ASSERT_EQ(engine.registerLocalMemory(destination.data(), destination.size(),
                                         "cpu:0"),
              0);

    std::shared_ptr<StorageBackend> backend;
    TransferSubmitter submitter(engine, backend, engine.getLocalIpAndPort());
    auto operation = submitter.submitNativeScatter(
        {{.opcode = TransferRequest::READ,
          .remote_segment = engine.getLocalIpAndPort(),
          .remote_base_offset = reinterpret_cast<uintptr_t>(source.data()),
          .remote_size = source.size(),
          .local_buffer = destination.data(),
          .local_capacity = destination.size(),
          .local_offsets = offsets,
          .remote_offsets = offsets,
          .lengths = lengths,
          .intent_type = 4}},
        TransferIntent::kForegroundGet);

    const auto status = operation.wait();
    EXPECT_TRUE(status.IsInvalidArgument()) << status.ToString();
    EXPECT_EQ(destination, std::vector<char>(destination.size(), 0));
    EXPECT_EQ(engine.unregisterLocalMemory(source.data()), 0);
    EXPECT_EQ(engine.unregisterLocalMemory(destination.data()), 0);
    EXPECT_EQ(engine.freeEngine(), 0);
}
#endif

#ifdef USE_CUDA
TEST_F(TransferTaskTest, TransferScatterHandlesFragmentedGpuBuffers) {
    int device_count = 0;
    if (cudaGetDeviceCount(&device_count) != cudaSuccess || device_count == 0) {
        GTEST_SKIP() << "CUDA device is unavailable";
    }

    constexpr size_t kBufferSize = 512;
    constexpr size_t kFragmentCount = 128;
    std::vector<char> source(kBufferSize), cpu_destination(kBufferSize, 0);
    std::iota(source.begin(), source.end(), 0);
    void *gpu_source = nullptr, *gpu_destination = nullptr;
    ASSERT_EQ(cudaMalloc(&gpu_source, kBufferSize), cudaSuccess);
    ASSERT_EQ(cudaMalloc(&gpu_destination, kBufferSize), cudaSuccess);
    ASSERT_EQ(cudaMemcpy(gpu_source, source.data(), kBufferSize,
                         cudaMemcpyHostToDevice),
              cudaSuccess);

    TransferEngine engine(false);
    ASSERT_EQ(engine.init("P2PHANDSHAKE", "localhost:17932"), 0);
    if (!engine.isUsingTent()) {
        ASSERT_NE(engine.installTransport("tcp", nullptr), nullptr);
    }
    ASSERT_EQ(engine.registerLocalMemory(source.data(), source.size(), "cpu:0"),
              0);
    ASSERT_EQ(engine.registerLocalMemory(cpu_destination.data(),
                                         cpu_destination.size(), "cpu:0"),
              0);
    ASSERT_EQ(engine.registerLocalMemory(gpu_source, kBufferSize, "cuda:0"), 0);
    ASSERT_EQ(
        engine.registerLocalMemory(gpu_destination, kBufferSize, "cuda:0"), 0);

    std::vector<size_t> destination_offsets, source_offsets,
        lengths(kFragmentCount, 1);
    std::vector<char> expected(kBufferSize, 0), actual(kBufferSize);
    for (size_t i = 0; i < kFragmentCount; ++i) {
        destination_offsets.push_back(i * 2);
        source_offsets.push_back(i * 3);
        expected[i * 2] = source[i * 3];
    }
    auto make_transfer = [&](void* remote_source, void* local_destination) {
        return TransferEngine::ScatterTransferRange{
            .opcode = TransferRequest::READ,
            .remote_segment = engine.getLocalIpAndPort(),
            .remote_base_offset = reinterpret_cast<uintptr_t>(remote_source),
            .remote_size = kBufferSize,
            .local_buffer = local_destination,
            .local_capacity = kBufferSize,
            .local_offsets = destination_offsets,
            .remote_offsets = source_offsets,
            .lengths = lengths,
            .on_fragment_complete = {},
        };
    };

    auto invalid_transfer = make_transfer(source.data(), gpu_destination);
    invalid_transfer.remote_offsets = {};
    EXPECT_TRUE(engine.transferScatter({invalid_transfer}).IsInvalidArgument());

    ASSERT_EQ(cudaMemset(gpu_destination, 0, kBufferSize), cudaSuccess);
    ASSERT_TRUE(
        engine.transferScatter({make_transfer(source.data(), gpu_destination)})
            .ok());
    ASSERT_EQ(cudaMemcpy(actual.data(), gpu_destination, kBufferSize,
                         cudaMemcpyDeviceToHost),
              cudaSuccess);
    EXPECT_EQ(actual, expected);

    ASSERT_TRUE(engine
                    .transferScatter(
                        {make_transfer(gpu_source, cpu_destination.data())})
                    .ok());
    EXPECT_EQ(cpu_destination, expected);

    ASSERT_EQ(cudaMemset(gpu_destination, 0, kBufferSize), cudaSuccess);
    auto operation =
        engine.submitScatter({make_transfer(gpu_source, gpu_destination)});
    ASSERT_EQ(engine.freeEngine(), 0);
    ASSERT_TRUE(operation.wait().ok());
    ASSERT_EQ(cudaMemcpy(actual.data(), gpu_destination, kBufferSize,
                         cudaMemcpyDeviceToHost),
              cudaSuccess);
    EXPECT_EQ(actual, expected);

    EXPECT_EQ(cudaFree(gpu_source), cudaSuccess);
    EXPECT_EQ(cudaFree(gpu_destination), cudaSuccess);
}
#endif

// Same-host endpoints from different processes are not locally addressable.
TEST_F(TransferTaskTest, IsSameProcessEndpoint) {
    // Empty inputs -> not same-process (cannot prove locality).
    EXPECT_FALSE(TransferSubmitter::isSameProcessEndpoint("", ""));
    EXPECT_FALSE(
        TransferSubmitter::isSameProcessEndpoint("", "192.168.1.10:12345"));
    EXPECT_FALSE(
        TransferSubmitter::isSameProcessEndpoint("192.168.1.10:12345", ""));

    // Identical ip:port -> same process.
    EXPECT_TRUE(TransferSubmitter::isSameProcessEndpoint("192.168.1.10:12345",
                                                         "192.168.1.10:12345"));

    // Same host, different port -> different process, NOT local.
    // This is the regression case fixed by this change.
    EXPECT_FALSE(TransferSubmitter::isSameProcessEndpoint(
        "192.168.1.10:12345", "192.168.1.10:12346"));

    // Different hosts -> not local.
    EXPECT_FALSE(TransferSubmitter::isSameProcessEndpoint(
        "192.168.1.10:12345", "192.168.1.11:12345"));

    // Hostname endpoints (non-P2P metadata mode) compare as full strings.
    EXPECT_TRUE(TransferSubmitter::isSameProcessEndpoint("host-a", "host-a"));
    EXPECT_FALSE(TransferSubmitter::isSameProcessEndpoint("host-a", "host-b"));
}

TEST_F(TransferTaskTest, BatchGetOffloadObjectHonorsLocalMemcpySetting) {
    setenv("MC_STORE_MEMCPY", "1", 1);
    TransferEngine engine(false);
    ASSERT_EQ(engine.init("P2PHANDSHAKE", "localhost:17933"), 0);
    const std::string endpoint = engine.getLocalIpAndPort();

    std::vector<char> source(512, 'A');
    std::vector<char> destination(512, 0);
    const std::vector<std::string> keys{"key"};
    const std::vector<uint64_t> pointers{
        reinterpret_cast<uintptr_t>(source.data())};
    const std::unordered_map<std::string, std::vector<Slice>> slices{
        {"key", {{nullptr, 0}, {destination.data(), destination.size()}}}};

    {
        std::shared_ptr<StorageBackend> backend;
        TransferSubmitter submitter(engine, backend, endpoint);
        auto future = submitter.submit_batch_get_offload_object(
            endpoint, keys, pointers, slices,
            OffloadBufferAccess::kLocalAddress,
            TransferIntent::kBackgroundPrefetch);
        ASSERT_TRUE(future);
        EXPECT_EQ(future->strategy(), TransferStrategy::LOCAL_MEMCPY);
        EXPECT_EQ(future->get(), ErrorCode::OK);
        EXPECT_FALSE(submitter.submit_batch_get_offload_object(
            endpoint, keys, {std::numeric_limits<uint64_t>::max() - 7},
            {{"key", {{destination.data(), 16}}}},
            OffloadBufferAccess::kLocalAddress,
            TransferIntent::kBackgroundPrefetch));
    }
    EXPECT_EQ(destination, source);

    setenv("MC_STORE_MEMCPY", "0", 1);
    std::shared_ptr<StorageBackend> backend;
    TransferSubmitter submitter(engine, backend, endpoint);
    EXPECT_FALSE(submitter.submit_batch_get_offload_object(
        endpoint, keys, pointers, slices, OffloadBufferAccess::kLocalAddress,
        TransferIntent::kBackgroundPrefetch));
    EXPECT_EQ(engine.freeEngine(), 0);
}

#ifdef MOONCAKE_TEST_CUDA_H2D
TEST_F(TransferTaskTest, BatchGetOffloadObjectCopiesPinnedHostToGpu) {
    int device_count = 0;
    if (cudaGetDeviceCount(&device_count) != cudaSuccess || device_count == 0) {
        GTEST_SKIP() << "CUDA device is unavailable";
    }

    setenv("MC_STORE_MEMCPY", "1", 1);
    constexpr size_t kSourceOffset = 128;
    constexpr size_t kSize = 4096;
    auto pinned_buffer =
        PinnedBufferPool::AllocatePinned(kSourceOffset + kSize);
    ASSERT_NE(pinned_buffer.pinned_host.addr, nullptr);
    void* pinned_source = pinned_buffer.data;
    void* gpu_destination = nullptr;
    ASSERT_EQ(cudaMalloc(&gpu_destination, kSize), cudaSuccess);
    std::memset(static_cast<char*>(pinned_source) + kSourceOffset, 0x5a, kSize);

    TransferEngine engine(false);
    ASSERT_EQ(engine.init("P2PHANDSHAKE", "localhost:17934"), 0);
    const std::string endpoint = engine.getLocalIpAndPort();
    {
        std::shared_ptr<StorageBackend> backend;
        TransferSubmitter submitter(engine, backend, endpoint);
        auto future = submitter.submit_batch_get_offload_object(
            endpoint, {"gpu"},
            {reinterpret_cast<uintptr_t>(static_cast<char*>(pinned_source) +
                                         kSourceOffset)},
            {{"gpu", {{gpu_destination, kSize}}}},
            OffloadBufferAccess::kLocalAddress,
            TransferIntent::kBackgroundPrefetch);
        ASSERT_TRUE(future);
        EXPECT_EQ(future->get(), ErrorCode::OK);
    }

    std::vector<unsigned char> actual(kSize);
    EXPECT_EQ(cudaMemcpy(actual.data(), gpu_destination, kSize,
                         cudaMemcpyDeviceToHost),
              cudaSuccess);
    EXPECT_EQ(actual, std::vector<unsigned char>(kSize, 0x5a));
    EXPECT_EQ(engine.freeEngine(), 0);
    EXPECT_EQ(cudaFree(gpu_destination), cudaSuccess);
}
#endif
TEST_F(TransferTaskTest, CanUseLocalMemcpyRequiresSameProcessEndpoint) {
    ScopedEnvVar memcpy_enabled("MC_STORE_MEMCPY", "1");

    TransferEngine engine(false);
    ASSERT_EQ(
        engine.init("P2PHANDSHAKE", "127.0.0.1:30991", "127.0.0.1", 30991), 0);
    ASSERT_NE(engine.installTransport("tcp", nullptr), nullptr);

    std::shared_ptr<StorageBackend> storage_backend;
    TransferSubmitter submitter(engine, storage_backend, "127.0.0.1:30991");

    const auto local_endpoint = engine.getLocalIpAndPort();
    ASSERT_FALSE(local_endpoint.empty());
    EXPECT_TRUE(submitter.canUseLocalMemcpy(local_endpoint));

    EXPECT_FALSE(submitter.canUseLocalMemcpy("127.0.0.1:30992"));
    EXPECT_FALSE(submitter.canUseLocalMemcpy(""));
}

TEST_F(TransferTaskTest, CanUseLocalMemcpyHonorsMemcpyEnv) {
    ScopedEnvVar memcpy_disabled("MC_STORE_MEMCPY", "0");

    TransferEngine engine(false);
    ASSERT_EQ(
        engine.init("P2PHANDSHAKE", "127.0.0.1:30993", "127.0.0.1", 30993), 0);
    ASSERT_NE(engine.installTransport("tcp", nullptr), nullptr);

    std::shared_ptr<StorageBackend> storage_backend;
    TransferSubmitter submitter(engine, storage_backend, "127.0.0.1:30993");

    EXPECT_FALSE(submitter.canUseLocalMemcpy(engine.getLocalIpAndPort()));
}

TEST_F(TransferTaskTest, BatchWriteHonorsLocalMemcpySetting) {
    ScopedEnvVar memcpy_enabled("MC_STORE_MEMCPY", "1");

    TransferEngine engine(false);
    ASSERT_EQ(
        engine.init("P2PHANDSHAKE", "127.0.0.1:30995", "127.0.0.1", 30995), 0);

    std::vector<char> source(512, 'A');
    std::vector<char> destination(source.size(), 0);
    MemoryDescriptor memory;
    memory.buffer_descriptor.buffer_address_ =
        reinterpret_cast<uintptr_t>(destination.data());
    memory.buffer_descriptor.size_ = destination.size();
    memory.buffer_descriptor.transport_endpoint_ = engine.getLocalIpAndPort();
    memory.buffer_descriptor.protocol_ = "tcp";
    Replica::Descriptor replica;
    replica.descriptor_variant = memory;
    replica.status = ReplicaStatus::PROCESSING;

    std::shared_ptr<StorageBackend> storage_backend;
    {
        TransferSubmitter submitter(engine, storage_backend,
                                    engine.getLocalIpAndPort());
        std::vector<std::vector<Slice>> slices{
            {{source.data(), source.size()}}};
        auto future =
            submitter.submit_batch({replica}, slices, TransferRequest::WRITE,
                                   TransferIntent::kUnspecified);

        ASSERT_TRUE(future);
        EXPECT_EQ(future->strategy(), TransferStrategy::LOCAL_MEMCPY);
        EXPECT_EQ(future->get(), ErrorCode::OK);
    }
    EXPECT_EQ(destination, source);
    EXPECT_EQ(engine.freeEngine(), 0);
}

TEST_F(TransferTaskTest, RemoteTransferWaitCompletesCorrectly) {
    ScopedEnvVar memcpy_disabled("MC_STORE_MEMCPY", "0");
    ScopedEnvVar tent_config("MC_TENT_CONF", nullptr);
    constexpr size_t kSize = 16 * 1024 * 1024;
    std::vector<char> local(kSize, 0), remote(kSize, 'A');
    TransferEngine client(false), server(false);
    ASSERT_EQ(client.init("P2PHANDSHAKE", "127.0.0.1:0", "", 0, "tcp"), 0);
    ASSERT_EQ(server.init("P2PHANDSHAKE", "127.0.0.1:0", "", 0, "tcp"), 0);
    if (!client.isUsingTent()) {
        ASSERT_NE(client.installTransport("tcp", nullptr), nullptr);
        ASSERT_NE(server.installTransport("tcp", nullptr), nullptr);
    }
    ASSERT_EQ(client.registerLocalMemory(local.data(), local.size(), "cpu:0"),
              0);
    ASSERT_EQ(server.registerLocalMemory(remote.data(), remote.size(), "cpu:0"),
              0);

    MemoryDescriptor memory;
    memory.buffer_descriptor.buffer_address_ =
        reinterpret_cast<uintptr_t>(remote.data());
    memory.buffer_descriptor.size_ = remote.size();
    memory.buffer_descriptor.transport_endpoint_ = server.getLocalIpAndPort();
    memory.buffer_descriptor.protocol_ = "tcp";
    Replica::Descriptor replica;
    replica.descriptor_variant = memory;
    replica.status = ReplicaStatus::COMPLETE;
    std::shared_ptr<StorageBackend> backend;
    TransferSubmitter submitter(client, backend, client.getLocalIpAndPort());
    std::vector<Slice> slices{{local.data(), local.size()}};

    for (auto opcode : {TransferRequest::READ, TransferRequest::WRITE}) {
        if (opcode == TransferRequest::WRITE) {
            std::fill(local.begin(), local.end(), 'B');
        }
        auto future = submitter.submit(replica, slices, opcode);
        ASSERT_TRUE(future);
        EXPECT_EQ(future->strategy(), TransferStrategy::TRANSFER_ENGINE);
        ASSERT_EQ(future->get(), ErrorCode::OK);
        EXPECT_EQ(local, remote);
    }
    EXPECT_EQ(client.unregisterLocalMemory(local.data()), 0);
    EXPECT_EQ(server.unregisterLocalMemory(remote.data()), 0);
}

// Test TransferStrategy enum and stream operator
TEST_F(TransferTaskTest, TransferStrategyEnum) {
    // Test enum values
    EXPECT_EQ(static_cast<int>(TransferStrategy::LOCAL_MEMCPY), 0);
    EXPECT_EQ(static_cast<int>(TransferStrategy::TRANSFER_ENGINE), 1);
    EXPECT_EQ(static_cast<int>(TransferStrategy::FILE_READ), 2);
    EXPECT_EQ(static_cast<int>(TransferStrategy::EMPTY), 3);
    EXPECT_EQ(static_cast<int>(TransferStrategy::SPDK_NVMF), 4);

    // Test stream operator
    std::ostringstream oss;
    oss << TransferStrategy::LOCAL_MEMCPY;
    EXPECT_EQ(oss.str(), "LOCAL_MEMCPY");

    oss.str("");
    oss << TransferStrategy::TRANSFER_ENGINE;
    EXPECT_EQ(oss.str(), "TRANSFER_ENGINE");

    oss.str("");
    oss << TransferStrategy::SPDK_NVMF;
    EXPECT_EQ(oss.str(), "SPDK_NVMF");

    oss.str("");
    oss << TransferStrategy::FILE_READ;
    EXPECT_EQ(oss.str(), "FILE_READ");

    oss.str("");
    oss << TransferStrategy::EMPTY;
    EXPECT_EQ(oss.str(), "EMPTY");
}

}  // namespace mooncake

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
