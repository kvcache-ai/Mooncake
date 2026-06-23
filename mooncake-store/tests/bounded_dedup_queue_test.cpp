#include <glog/logging.h>
#include <gtest/gtest.h>

#include <atomic>
#include <string>
#include <thread>
#include <vector>

#include "tiered_cache/event_driven_scheduler/bounded_dedup_queue.h"
#include "tiered_cache/event_driven_scheduler/event_driven_policy.h"

namespace mooncake {
namespace {

MovementRequest Ev(const std::string& k) {
    return MovementRequest{MovementRequest::Kind::kReplicate, k, UUID{1, 1},
                           UUID{2, 2}, 100};
}

TEST(BoundedDedupQueueTest, CapacityLimit) {
    BoundedDedupQueue<MovementRequest> q(3);
    EXPECT_TRUE(q.TryPush("a", Ev("a")));
    EXPECT_TRUE(q.TryPush("b", Ev("b")));
    EXPECT_TRUE(q.TryPush("c", Ev("c")));
    EXPECT_FALSE(q.TryPush("d", Ev("d")));  // cap+1 dropped
    EXPECT_EQ(q.size(), 3u);
}

TEST(BoundedDedupQueueTest, DedupRejectsQueuedKey) {
    BoundedDedupQueue<MovementRequest> q(10);
    EXPECT_TRUE(q.TryPush("a", Ev("a")));
    EXPECT_FALSE(q.TryPush("a", Ev("a")));  // already queued
    EXPECT_EQ(q.size(), 1u);
}

TEST(BoundedDedupQueueTest, FifoAndRequeueAfterPop) {
    BoundedDedupQueue<MovementRequest> q(10);
    q.TryPush("a", Ev("a"));
    q.TryPush("b", Ev("b"));

    MovementRequest out;
    ASSERT_TRUE(q.TryPop(out));
    EXPECT_EQ(out.key, "a");  // FIFO order
    ASSERT_TRUE(q.TryPop(out));
    EXPECT_EQ(out.key, "b");
    EXPECT_FALSE(q.TryPop(out));  // empty

    // After popping, the de-dup marker is cleared so "a" can be re-queued.
    EXPECT_TRUE(q.TryPush("a", Ev("a")));
}

TEST(BoundedDedupQueueTest, ConcurrentProducersConsumers) {
    BoundedDedupQueue<MovementRequest> q(1024);
    constexpr int kThreads = 8;
    constexpr int kPerThread = 1000;
    std::atomic<int> popped{0};
    std::atomic<bool> stop{false};
    std::vector<std::thread> producers;
    std::vector<std::thread> consumers;

    for (int t = 0; t < kThreads; ++t) {
        producers.emplace_back([&q, t] {
            for (int i = 0; i < kPerThread; ++i) {
                q.TryPush("k" + std::to_string(t) + "_" + std::to_string(i),
                          Ev("x"));
            }
        });
        consumers.emplace_back([&q, &popped, &stop] {
            MovementRequest out;
            while (!stop.load()) {
                if (q.TryPop(out)) popped.fetch_add(1);
            }
            while (q.TryPop(out)) popped.fetch_add(1);  // final drain
        });
    }
    for (auto& p : producers) p.join();
    stop.store(true);
    for (auto& c : consumers) c.join();

    // Mainly a data-race check (run under ASAN/TSAN). Capacity means some
    // pushes are dropped, so we only bound the popped count.
    EXPECT_LE(popped.load(), kThreads * kPerThread);
    EXPECT_EQ(q.size(), 0u);
}

}  // namespace
}  // namespace mooncake
