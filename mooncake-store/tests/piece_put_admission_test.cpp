#include "piece_put_admission.h"

#include <chrono>
#include <future>

#include <gtest/gtest.h>

namespace mooncake {
namespace {

using Registry = PiecePutAdmissionRegistry;
using namespace std::chrono_literals;
const Registry::TimePoint kNow{100s};
const UUID kProducer{1, 2};

TEST(PiecePutAdmissionTest, LostRepliesDoNotRotateTheWindowTwice) {
    Registry registry;
    const auto first = registry.Get(kProducer, 0, kNow);
    ASSERT_TRUE(first);
    EXPECT_EQ(registry.Get(kProducer, 0, kNow), first);
    const auto second = registry.Get(kProducer, *first, kNow);
    ASSERT_TRUE(second);
    EXPECT_GT(*second, *first);
    EXPECT_EQ(registry.Get(kProducer, *first, kNow), second);
    auto old = registry.Acquire(kProducer, *first, kNow);
    ASSERT_TRUE(old);
    EXPECT_FALSE(*old);
    auto current = registry.Acquire(kProducer, *second, kNow);
    ASSERT_TRUE(current);
    EXPECT_TRUE(*current);
}

TEST(PiecePutAdmissionTest, NoncesAreIndependentAndMayArriveOutOfOrder) {
    Registry registry;
    const auto id = registry.Get(kProducer, 0, kNow);
    ASSERT_TRUE(id);
    auto guard = registry.Acquire(kProducer, *id, kNow);
    ASSERT_TRUE(guard);
    ASSERT_TRUE(*guard);
    EXPECT_TRUE(guard->Consume(Registry::kCapacity));
    EXPECT_TRUE(guard->Consume(1));
    EXPECT_TRUE(guard->Consume(211));
    EXPECT_FALSE(guard->Consume(1));
    EXPECT_FALSE(guard->Consume(Registry::kCapacity));
    EXPECT_TRUE(guard->Consume(Registry::kCapacity - 1));
}

TEST(PiecePutAdmissionTest, RejectsUnissuedIdsAndNeverAcceptsAnotherProducer) {
    Registry registry;
    const auto id = registry.Get(kProducer, 0, kNow);
    ASSERT_TRUE(id);
    EXPECT_FALSE(registry.Acquire(kProducer, 0, kNow));
    EXPECT_FALSE(registry.Acquire(kProducer, *id + 1, kNow));
    auto wrong = registry.Acquire(UUID{3, 4}, *id, kNow);
    ASSERT_TRUE(wrong);
    EXPECT_FALSE(*wrong);
}

TEST(PiecePutAdmissionTest, RotationWaitsForGrantPublicationGate) {
    Registry registry;
    const auto id = registry.Get(kProducer, 0, kNow);
    ASSERT_TRUE(id);
    auto guard = registry.Acquire(kProducer, *id, kNow);
    ASSERT_TRUE(guard);
    std::promise<void> started;
    auto rotating = std::async(std::launch::async, [&] {
        started.set_value();
        return registry.Get(kProducer, *id, kNow);
    });
    started.get_future().wait();
    EXPECT_EQ(rotating.wait_for(20ms), std::future_status::timeout);
    EXPECT_TRUE(guard->Consume(1));
    *guard = Registry::Guard{};
    const auto next = rotating.get();
    ASSERT_TRUE(next);
    EXPECT_GT(*next, *id);
    auto closed = registry.Acquire(kProducer, *id, kNow);
    ASSERT_TRUE(closed);
    EXPECT_FALSE(*closed);
}

TEST(PiecePutAdmissionTest, IdleSweepSkipsBusyWindowAndNeverReusesIds) {
    Registry registry;
    const auto id = registry.Get(kProducer, 0, kNow);
    ASSERT_TRUE(id);
    auto guard = registry.Acquire(kProducer, *id, kNow);
    ASSERT_TRUE(guard);
    // A separate thread avoids recursive shared_mutex acquisition in the test.
    std::async(std::launch::async, [&] {
        registry.Sweep(kNow + Registry::kIdleTimeout);
    }).get();
    EXPECT_TRUE(guard->Consume(1));
    *guard = Registry::Guard{};
    registry.Sweep(kNow + Registry::kIdleTimeout);
    auto old = registry.Acquire(kProducer, *id, kNow);
    ASSERT_TRUE(old);
    EXPECT_FALSE(*old);
    const auto next = registry.Get(kProducer, *id, kNow);
    ASSERT_TRUE(next);
    EXPECT_GT(*next, *id);
}

TEST(PiecePutAdmissionTest, ProducerLimitIsRecoveredByIndependentIdleSweep) {
    Registry registry;
    for (size_t i = 0; i < Registry::kMaxProducers; ++i) {
        ASSERT_TRUE(registry.Get(UUID{1, i}, 0, kNow));
    }
    EXPECT_FALSE(registry.Get(UUID{2, 0}, 0, kNow));
    registry.Sweep(kNow + Registry::kIdleTimeout);
    const auto next = registry.Get(UUID{2, 0}, 0, kNow);
    ASSERT_TRUE(next);
    EXPECT_GT(*next, Registry::kMaxProducers);
}

}  // namespace
}  // namespace mooncake
