#include <gtest/gtest.h>

#include <array>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <thread>
#include <vector>

#include "client_service.h"

namespace mooncake {

// Test-only completion hook: no transport hardware is needed to control
// completion ordering, timeout, and the Store dispatcher state machine.
class AsyncMemoryBatchGetTestPeer {
   public:
    static void Complete(const std::shared_ptr<AsyncMemoryBatchGet>& op,
                         std::vector<int64_t> results) {
        op->Complete(std::move(results));
    }
};

namespace {

using namespace std::chrono_literals;

class FakeBackend {
   public:
    std::shared_ptr<AsyncMemoryBatchGet> Submit(
        const std::vector<std::string>& keys,
        const std::vector<std::vector<Slice>>&) {
        auto backend =
            std::make_shared<AsyncMemoryBatchGet>(nullptr, keys.size());
        {
            std::lock_guard<std::mutex> lock(mutex_);
            backends_.push_back(backend);
        }
        cv_.notify_all();
        return backend;
    }

    std::shared_ptr<AsyncMemoryBatchGet> WaitForSubmission(size_t index) {
        std::unique_lock<std::mutex> lock(mutex_);
        if (!cv_.wait_for(lock, 3s,
                          [this, index] { return backends_.size() > index; })) {
            return nullptr;
        }
        return backends_[index];
    }

    size_t SubmissionCount() {
        std::lock_guard<std::mutex> lock(mutex_);
        return backends_.size();
    }

   private:
    std::mutex mutex_;
    std::condition_variable cv_;
    std::vector<std::shared_ptr<AsyncMemoryBatchGet>> backends_;
};

TEST(AsyncMemoryBatchGetTest, EmptyBackendOperationCompletes) {
    auto op = std::make_shared<AsyncMemoryBatchGet>(
        nullptr, std::vector<AsyncMemoryBatchGet::Entry>{});
    EXPECT_TRUE(op->ready());
    EXPECT_TRUE(op->wait().empty());
    EXPECT_TRUE(op->wait().empty());
}

TEST(AsyncMemoryBatchGetTest, TerminalErrorsKeepInputOrdering) {
    std::vector<AsyncMemoryBatchGet::Entry> entries;
    AsyncMemoryBatchGet::Entry first;
    first.key = "missing";
    first.result = static_cast<int64_t>(ErrorCode::OBJECT_NOT_FOUND);
    entries.emplace_back(std::move(first));

    AsyncMemoryBatchGet::Entry second;
    second.key = "failed";
    second.result = static_cast<int64_t>(ErrorCode::TRANSFER_FAIL);
    entries.emplace_back(std::move(second));

    auto op =
        std::make_shared<AsyncMemoryBatchGet>(nullptr, std::move(entries));
    const std::vector<int64_t> expected = {
        static_cast<int64_t>(ErrorCode::OBJECT_NOT_FOUND),
        static_cast<int64_t>(ErrorCode::TRANSFER_FAIL)};
    EXPECT_TRUE(op->ready());
    EXPECT_EQ(op->wait(), expected);
    EXPECT_EQ(op->wait(), expected);
}

TEST(AsyncMemoryBatchGetTest, TimeoutDoesNotCompleteOperation) {
    auto op = std::make_shared<AsyncMemoryBatchGet>(nullptr, 2);
    EXPECT_FALSE(op->ready());
    EXPECT_FALSE(op->wait_for(0ms).has_value());
    EXPECT_FALSE(op->wait_for(3ms).has_value());
    AsyncMemoryBatchGetTestPeer::Complete(op, {11, 17});
    EXPECT_TRUE(op->ready());
    EXPECT_EQ(op->wait_for(0ms), (std::vector<int64_t>{11, 17}));
    EXPECT_EQ(op->wait(), (std::vector<int64_t>{11, 17}));
    EXPECT_THROW(op->wait_for(-1ms), std::invalid_argument);
}

TEST(AsyncMemoryBatchGetTest, IndependentSubmissionAndOutOfOrderCompletion) {
    FakeBackend backend;
    AsyncMemoryGetManager manager(
        nullptr, 2, 32, [&backend](const auto& keys, const auto& destinations) {
            return backend.Submit(keys, destinations);
        });
    std::array<char, 16> first{};
    std::array<char, 16> second{};
    auto l0 = manager.Submit({"L0"}, {{{first.data(), first.size()}}});
    ASSERT_NE(backend.WaitForSubmission(0), nullptr);
    // L0 has not completed, yet the one submission thread must reach L1.
    auto l1 = manager.Submit({"L1"}, {{{second.data(), second.size()}}});
    auto second_backend = backend.WaitForSubmission(1);
    ASSERT_NE(second_backend, nullptr);
    EXPECT_FALSE(l0->ready());
    EXPECT_FALSE(l1->ready());
    AsyncMemoryBatchGetTestPeer::Complete(second_backend, {16});
    ASSERT_TRUE(l1->wait_for(3s).has_value());
    EXPECT_EQ(l1->wait(), (std::vector<int64_t>{16}));
    EXPECT_FALSE(l0->wait_for(0ms).has_value());
    AsyncMemoryBatchGetTestPeer::Complete(backend.WaitForSubmission(0), {16});
    EXPECT_EQ(l0->wait(), (std::vector<int64_t>{16}));
    manager.Drain();
    EXPECT_EQ(manager.InFlight(), (std::pair<size_t, size_t>{0, 0}));
}

TEST(AsyncMemoryBatchGetTest, OperationAndByteBudgetsBoundSubmission) {
    FakeBackend backend;
    AsyncMemoryGetManager manager(
        nullptr, 1, 8, [&backend](const auto& keys, const auto& destinations) {
            return backend.Submit(keys, destinations);
        });
    std::array<char, 8> memory{};
    auto first = manager.Submit({"one"}, {{{memory.data(), 8}}});
    ASSERT_NE(backend.WaitForSubmission(0), nullptr);
    EXPECT_THROW(manager.Submit({"two"}, {{{memory.data(), 8}}}),
                 std::runtime_error);
    EXPECT_THROW(manager.Submit({"too-large"}, {{{memory.data(), 9}}}),
                 std::invalid_argument);
    EXPECT_EQ(backend.SubmissionCount(), 1U);
    EXPECT_EQ(manager.InFlight(), (std::pair<size_t, size_t>{1, 8}));
    AsyncMemoryBatchGetTestPeer::Complete(backend.WaitForSubmission(0), {8});
    EXPECT_EQ(first->wait(), (std::vector<int64_t>{8}));
    manager.Drain();
    auto second = manager.Submit({"two"}, {{{memory.data(), 8}}});
    ASSERT_NE(backend.WaitForSubmission(1), nullptr);
    AsyncMemoryBatchGetTestPeer::Complete(backend.WaitForSubmission(1), {8});
    EXPECT_EQ(second->wait(), (std::vector<int64_t>{8}));
    manager.Drain();
}

TEST(AsyncMemoryBatchGetTest, RejectsOverlapsAndMalformedDescriptors) {
    FakeBackend backend;
    AsyncMemoryGetManager manager(
        nullptr, 3, 64, [&backend](const auto& keys, const auto& destinations) {
            return backend.Submit(keys, destinations);
        });
    std::array<char, 32> memory{};
    EXPECT_THROW(manager.Submit({}, {}), std::invalid_argument);
    EXPECT_THROW(manager.Submit({"dup", "dup"}, {{{memory.data(), 8}},
                                                 {{memory.data() + 16, 8}}}),
                 std::invalid_argument);
    EXPECT_THROW(manager.Submit({"self"},
                                {{{memory.data(), 8}, {memory.data() + 4, 8}}}),
                 std::invalid_argument);
    EXPECT_THROW(manager.Submit({"null"}, {{{nullptr, 8}}}),
                 std::invalid_argument);
    EXPECT_THROW(manager.Submit({"empty"}, {{{memory.data(), 0}}}),
                 std::invalid_argument);
    EXPECT_EQ(backend.SubmissionCount(), 0U);

    auto first = manager.Submit({"one"}, {{{memory.data(), 8}}});
    ASSERT_NE(backend.WaitForSubmission(0), nullptr);
    EXPECT_THROW(manager.Submit({"overlap"}, {{{memory.data() + 4, 8}}}),
                 std::invalid_argument);
    EXPECT_EQ(backend.SubmissionCount(), 1U);
    AsyncMemoryBatchGetTestPeer::Complete(backend.WaitForSubmission(0), {8});
    EXPECT_EQ(first->wait(), (std::vector<int64_t>{8}));
    manager.Drain();
    auto next = manager.Submit({"after-drain"}, {{{memory.data() + 4, 8}}});
    ASSERT_NE(backend.WaitForSubmission(1), nullptr);
    AsyncMemoryBatchGetTestPeer::Complete(backend.WaitForSubmission(1), {8});
    EXPECT_EQ(next->wait(), (std::vector<int64_t>{8}));
    manager.Drain();
}

TEST(AsyncMemoryBatchGetTest, ShutdownDrainsAndRejectsAdmissions) {
    FakeBackend backend;
    AsyncMemoryGetManager manager(
        nullptr, 1, 8, [&backend](const auto& keys, const auto& destinations) {
            return backend.Submit(keys, destinations);
        });
    std::array<char, 8> memory{};
    auto op = manager.Submit({"one"}, {{{memory.data(), 8}}});
    ASSERT_NE(backend.WaitForSubmission(0), nullptr);
    std::atomic<bool> close_returned{false};
    std::thread closer([&] {
        manager.CloseAndDrain();
        close_returned.store(true, std::memory_order_release);
    });
    EXPECT_FALSE(op->wait_for(10ms).has_value());
    EXPECT_FALSE(close_returned.load(std::memory_order_acquire));
    AsyncMemoryBatchGetTestPeer::Complete(backend.WaitForSubmission(0), {8});
    closer.join();
    EXPECT_TRUE(close_returned.load(std::memory_order_acquire));
    EXPECT_EQ(op->wait(), (std::vector<int64_t>{8}));
    EXPECT_THROW(manager.Submit({"after-close"}, {{{memory.data(), 8}}}),
                 std::runtime_error);
}

TEST(AsyncMemoryBatchGetTest, FailedSubmissionReleasesCredits) {
    AsyncMemoryGetManager manager(
        nullptr, 1, 8,
        [](const auto&, const auto&) -> std::shared_ptr<AsyncMemoryBatchGet> {
            throw std::runtime_error("injected metadata failure");
        });
    std::array<char, 8> buffer{};
    auto operation =
        manager.Submit({"broken"}, {{{buffer.data(), buffer.size()}}});
    auto results = operation->wait_for(3s);
    ASSERT_TRUE(results.has_value());
    EXPECT_EQ(
        *results,
        (std::vector<int64_t>{static_cast<int64_t>(ErrorCode::TRANSFER_FAIL)}));
    manager.Drain();
    EXPECT_EQ(manager.InFlight(), (std::pair<size_t, size_t>{0, 0}));
    auto retry = manager.Submit({"retry"}, {{{buffer.data(), buffer.size()}}});
    EXPECT_TRUE(retry->wait_for(3s).has_value());
    manager.Drain();
}

TEST(AsyncMemoryBatchGetTest, ByteBudgetIsIndependentOfOperationCount) {
    FakeBackend backend;
    AsyncMemoryGetManager manager(
        nullptr, 3, 8, [&backend](const auto& keys, const auto& destinations) {
            return backend.Submit(keys, destinations);
        });
    std::array<char, 16> data{};
    auto first = manager.Submit({"six"}, {{{data.data(), 6}}});
    ASSERT_NE(backend.WaitForSubmission(0), nullptr);
    EXPECT_THROW(manager.Submit({"three"}, {{{data.data() + 8, 3}}}),
                 std::runtime_error);
    EXPECT_EQ(backend.SubmissionCount(), 1U);
    AsyncMemoryBatchGetTestPeer::Complete(backend.WaitForSubmission(0), {6});
    EXPECT_EQ(first->wait(), (std::vector<int64_t>{6}));
    manager.Drain();
    auto second = manager.Submit({"three"}, {{{data.data() + 8, 3}}});
    ASSERT_NE(backend.WaitForSubmission(1), nullptr);
    AsyncMemoryBatchGetTestPeer::Complete(backend.WaitForSubmission(1), {3});
    EXPECT_EQ(second->wait(), (std::vector<int64_t>{3}));
    manager.Drain();
}

TEST(AsyncMemoryBatchGetTest, DroppedHandleRemainsOwnedThroughCompletion) {
    FakeBackend backend;
    AsyncMemoryGetManager manager(
        nullptr, 1, 8, [&backend](const auto& keys, const auto& destinations) {
            return backend.Submit(keys, destinations);
        });
    std::array<char, 8> data{};
    std::weak_ptr<AsyncMemoryBatchGet> dropped;
    {
        auto op = manager.Submit({"one"}, {{{data.data(), data.size()}}});
        dropped = op;
    }
    ASSERT_NE(backend.WaitForSubmission(0), nullptr);
    EXPECT_FALSE(dropped.expired());
    AsyncMemoryBatchGetTestPeer::Complete(backend.WaitForSubmission(0), {8});
    manager.CloseAndDrain();
    EXPECT_TRUE(dropped.expired());
}

// TransferFuture is constructible from any OperationState, so transport-level
// exceptions can be injected without RDMA hardware. This is what makes the
// destructor exception-safety contract testable.
class ThrowingOperationState : public OperationState {
   public:
    bool is_completed() override { return false; }

    void wait_for_completion() override {
        throw std::runtime_error("injected transport wait failure");
    }

    TransferStrategy get_strategy() const override {
        return TransferStrategy::TRANSFER_ENGINE;
    }
};

class CountingOperationState : public OperationState {
   public:
    bool is_completed() override {
        std::lock_guard<std::mutex> lock(mutex_);
        return result_.has_value();
    }

    void wait_for_completion() override {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (!result_) result_.emplace(ErrorCode::OK);
        }
        waits.fetch_add(1, std::memory_order_relaxed);
        cv_.notify_all();
    }

    TransferStrategy get_strategy() const override {
        return TransferStrategy::TRANSFER_ENGINE;
    }

    std::atomic<int> waits{0};
};

TEST(AsyncMemoryBatchGetTest, ThrowingTransferDoesNotBlockOtherDrains) {
    std::vector<AsyncMemoryBatchGet::Entry> entries;

    AsyncMemoryBatchGet::Entry failing;
    failing.key = "failing";
    failing.transfer.emplace(std::make_shared<ThrowingOperationState>());
    entries.emplace_back(std::move(failing));

    auto healthy_state = std::make_shared<CountingOperationState>();
    AsyncMemoryBatchGet::Entry healthy;
    healthy.key = "healthy";
    healthy.transfer.emplace(healthy_state);
    entries.emplace_back(std::move(healthy));

    auto op =
        std::make_shared<AsyncMemoryBatchGet>(nullptr, std::move(entries));

    std::vector<int64_t> results;
    EXPECT_NO_THROW(results = op->wait());
    ASSERT_EQ(results.size(), 2U);
    EXPECT_EQ(results[0], static_cast<int64_t>(ErrorCode::INTERNAL_ERROR));
    // The healthy transfer must still be awaited even though the first one
    // raised: one bad key cannot leave other DMA undrained.
    EXPECT_EQ(healthy_state->waits.load(), 1);
}

TEST(AsyncMemoryBatchGetTest, DestructorIsNoexceptWhenTransferWaitThrows) {
    std::vector<AsyncMemoryBatchGet::Entry> entries;
    AsyncMemoryBatchGet::Entry failing;
    failing.key = "failing";
    failing.transfer.emplace(std::make_shared<ThrowingOperationState>());
    entries.emplace_back(std::move(failing));

    auto op =
        std::make_shared<AsyncMemoryBatchGet>(nullptr, std::move(entries));
    // Escaping here would call std::terminate and abort the test binary.
    EXPECT_NO_THROW(op.reset());
}

TEST(AsyncMemoryBatchGetTest, PreflightFailureKeepsErrorClass) {
    AsyncMemoryGetManager manager(
        nullptr, 1, 8,
        [](const auto&, const auto&) -> std::shared_ptr<AsyncMemoryBatchGet> {
            throw AsyncMemoryBatchGetError(ErrorCode::INVALID_PARAMS,
                                           "unsupported replica route");
        });
    std::array<char, 8> buffer{};
    auto operation = manager.Submit({"unsupported-route"},
                                    {{{buffer.data(), buffer.size()}}});
    auto results = operation->wait_for(3s);
    ASSERT_TRUE(results.has_value());
    // Must not be flattened into TRANSFER_FAIL: the caller needs to tell a
    // routing/precondition failure apart from a real data-path failure.
    EXPECT_EQ(*results, (std::vector<int64_t>{
                            static_cast<int64_t>(ErrorCode::INVALID_PARAMS)}));
    manager.Drain();
}

TEST(AsyncMemoryBatchGetTest, CompletionIsPublishedWithoutPollingStall) {
    FakeBackend backend;
    AsyncMemoryGetManager manager(
        nullptr, 1, 8, [&backend](const auto& keys, const auto& destinations) {
            return backend.Submit(keys, destinations);
        });
    std::array<char, 8> data{};
    auto operation = manager.Submit({"one"}, {{{data.data(), data.size()}}});
    auto submitted = backend.WaitForSubmission(0);
    ASSERT_NE(submitted, nullptr);

    const auto start = std::chrono::steady_clock::now();
    AsyncMemoryBatchGetTestPeer::Complete(submitted, {8});
    ASSERT_TRUE(operation->wait_for(2s).has_value());
    const auto elapsed = std::chrono::steady_clock::now() - start;
    // Bounded backoff replaced the fixed 1ms poll. 10ms tolerates CI
    // scheduling noise while still catching a stalled dispatcher.
    EXPECT_LT(elapsed, 10ms);
    manager.Drain();
}

}  // namespace
}  // namespace mooncake
