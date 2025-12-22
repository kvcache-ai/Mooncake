#include <gtest/gtest.h>
#include <glog/logging.h>
#include <atomic>
#include <chrono>
#include <thread>
#include <vector>
#include "thread_safe_queue.h"

namespace mooncake {

class ThreadSafeQueueTest : public ::testing::Test {
   protected:
    void SetUp() override {
        google::InitGoogleLogging("ThreadSafeQueueTest");
        FLAGS_logtostderr = 1;
    }

    void TearDown() override { google::ShutdownGoogleLogging(); }
};

TEST_F(ThreadSafeQueueTest, BasicPushPop) {
    ThreadSafeQueue<int> queue(10);

    EXPECT_TRUE(queue.push(1));
    EXPECT_EQ(queue.size_approx(), 1);
    EXPECT_FALSE(queue.empty());

    auto item = queue.pop();
    EXPECT_TRUE(item.has_value());
    EXPECT_EQ(item.value(), 1);
    EXPECT_EQ(queue.size_approx(), 0);
    EXPECT_TRUE(queue.empty());
}

TEST_F(ThreadSafeQueueTest, MultipleOperations) {
    const int num_items = 1000;
    ThreadSafeQueue<int> queue(2000);

    for (int i = 0; i < num_items; ++i) {
        EXPECT_TRUE(queue.push(i));
    }
    EXPECT_EQ(queue.size_approx(), num_items);

    for (int i = 0; i < num_items; ++i) {
        auto item = queue.pop();
        EXPECT_TRUE(item.has_value());
        EXPECT_EQ(item.value(), i);
    }

    EXPECT_TRUE(queue.empty());
}

TEST_F(ThreadSafeQueueTest, BlockingPush) {
    ThreadSafeQueue<int> queue(3);
    std::atomic<bool> thread_started{false};
    std::atomic<bool> push_succeeded{false};

    EXPECT_TRUE(queue.push(1));
    EXPECT_TRUE(queue.push(2));
    EXPECT_TRUE(queue.push(3));
    EXPECT_EQ(queue.size_approx(), 3);

    std::thread producer([&]() {
        thread_started = true;
        push_succeeded = queue.push(4);
    });

    while (!thread_started) {
        std::this_thread::yield();
    }

    auto item = queue.pop();
    EXPECT_TRUE(item.has_value());

    producer.join();

    EXPECT_TRUE(push_succeeded);
    EXPECT_EQ(queue.size_approx(), 3);
}

TEST_F(ThreadSafeQueueTest, BlockingPop) {
    ThreadSafeQueue<int> queue(10);
    std::atomic<bool> thread_started{false};
    std::optional<int> popped_item = std::nullopt;

    std::thread consumer([&]() {
        thread_started = true;
        popped_item = queue.pop();
    });

    while (!thread_started) {
        std::this_thread::yield();
    }

    EXPECT_TRUE(queue.push(42));

    consumer.join();

    EXPECT_TRUE(popped_item.has_value());
    EXPECT_EQ(popped_item.value(), 42);
    EXPECT_TRUE(queue.empty());
}

TEST_F(ThreadSafeQueueTest, PushWithTimeout) {
    ThreadSafeQueue<int> queue(2);

    EXPECT_TRUE(queue.push(1));
    EXPECT_TRUE(queue.push(2));

    auto start = std::chrono::steady_clock::now();
    bool result = queue.push(3, std::chrono::milliseconds(10));
    auto end = std::chrono::steady_clock::now();

    EXPECT_FALSE(result);
    auto duration =
        std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    EXPECT_GE(duration.count(), 10);

    queue.pop();
    start = std::chrono::steady_clock::now();
    result = queue.push(3, std::chrono::milliseconds(100));
    end = std::chrono::steady_clock::now();

    EXPECT_TRUE(result);
    duration =
        std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    EXPECT_LT(duration.count(), 100);
}

TEST_F(ThreadSafeQueueTest, PopWithTimeout) {
    ThreadSafeQueue<int> queue(10);

    auto start = std::chrono::steady_clock::now();
    auto item = queue.pop(std::chrono::milliseconds(10));
    auto end = std::chrono::steady_clock::now();

    EXPECT_FALSE(item.has_value());
    auto duration =
        std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    EXPECT_GE(duration.count(), 10);

    queue.push(42);
    start = std::chrono::steady_clock::now();
    item = queue.pop(std::chrono::milliseconds(100));
    end = std::chrono::steady_clock::now();

    EXPECT_TRUE(item.has_value());
    EXPECT_EQ(item.value(), 42);
    duration =
        std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    EXPECT_LT(duration.count(), 100);
}

TEST_F(ThreadSafeQueueTest, ProducerConsumer) {
    const int num_producers = 2;
    const int num_consumers = 2;
    const int items_per_producer = 20;
    const int total_items = num_producers * items_per_producer;

    ThreadSafeQueue<int> queue(20);
    std::atomic<int> items_processed{0};
    std::vector<int> received_items;
    std::mutex items_mutex;
    std::atomic<bool> all_produced{false};

    std::mutex cv_mutex;
    std::condition_variable cv;

    auto start_time = std::chrono::steady_clock::now();
    const auto timeout = std::chrono::milliseconds(10);

    std::vector<std::thread> consumers;
    for (int i = 0; i < num_consumers; ++i) {
        consumers.emplace_back([&, i]() {
            while (true) {
                auto now = std::chrono::steady_clock::now();
                if (now - start_time > timeout && all_produced.load()) {
                    break;
                }

                auto item = queue.pop(std::chrono::milliseconds(1));
                if (item.has_value()) {
                    {
                        std::lock_guard<std::mutex> lock(items_mutex);
                        received_items.push_back(item.value());
                    }
                    items_processed++;
                    if (items_processed.load() == total_items) {
                        cv.notify_one();
                        break;
                    }
                } else if (all_produced.load() && queue.empty()) {
                    break;
                }
            }
        });
    }

    std::vector<std::thread> producers;
    for (int i = 0; i < num_producers; ++i) {
        producers.emplace_back([&, producer_id = i]() {
            for (int j = 0; j < items_per_producer; ++j) {
                int item = producer_id * 1000 + j;
                if (!queue.push(item, std::chrono::milliseconds(1))) {
                    LOG(WARNING) << "Push timeout for item " << item;
                    if (!queue.push(item, std::chrono::milliseconds(1))) {
                        LOG(ERROR)
                            << "Push failed after retry for item " << item;
                        break;
                    }
                }
            }
        });
    }

    for (auto& producer : producers) {
        producer.join();
    }
    all_produced = true;

    {
        std::unique_lock<std::mutex> lock(cv_mutex);
        cv.wait_for(lock, std::chrono::milliseconds(5),
                    [&]() { return items_processed.load() == total_items; });
    }

    queue.shutdown();

    for (auto& consumer : consumers) {
        if (consumer.joinable()) {
            consumer.join();
        }
    }

    EXPECT_EQ(items_processed.load(), total_items);
    EXPECT_EQ(received_items.size(), total_items);
}

TEST_F(ThreadSafeQueueTest, Shutdown) {
    ThreadSafeQueue<int> queue(10);

    EXPECT_FALSE(queue.is_shutdown());

    queue.shutdown();
    EXPECT_TRUE(queue.is_shutdown());

    EXPECT_FALSE(queue.push(1));

    auto item = queue.pop();
    EXPECT_FALSE(item.has_value());

    EXPECT_FALSE(queue.push(1, std::chrono::milliseconds(10)));

    item = queue.pop(std::chrono::milliseconds(10));
    EXPECT_FALSE(item.has_value());
}

TEST_F(ThreadSafeQueueTest, ShutdownWakesBlockedThreads) {
    ThreadSafeQueue<int> queue(2);
    std::atomic<int> successful_ops{0};
    std::vector<std::thread> threads;

    EXPECT_TRUE(queue.push(1));
    EXPECT_TRUE(queue.push(2));

    threads.emplace_back([&]() {
        if (queue.push(3)) {
            successful_ops++;
        }
    });
    std::this_thread::sleep_for(std::chrono::milliseconds(1));

    queue.shutdown();

    for (auto& thread : threads) {
        thread.join();
    }

    EXPECT_EQ(successful_ops.load(), 0);
    EXPECT_TRUE(queue.is_shutdown());
}

TEST_F(ThreadSafeQueueTest, StressTest) {
    const int num_operations = 10000;
    const int max_size = 50;

    ThreadSafeQueue<int> queue(max_size);
    std::atomic<int> push_count{0};
    std::atomic<int> pop_count{0};

    std::thread producer([&]() {
        for (int i = 0; i < num_operations; ++i) {
            if (queue.push(i, std::chrono::milliseconds(1))) {
                push_count++;
            }
        }
    });

    std::thread consumer([&]() {
        for (int i = 0; i < num_operations; ++i) {
            auto item = queue.pop(std::chrono::milliseconds(1));
            if (item.has_value()) {
                pop_count++;
            }
        }
    });

    producer.join();
    consumer.join();

    while (pop_count.load() < push_count.load()) {
        auto item = queue.pop(std::chrono::milliseconds(1));
        if (item.has_value()) {
            pop_count++;
        } else {
            break;
        }
    }

    EXPECT_EQ(push_count.load(), pop_count.load());
    EXPECT_TRUE(queue.empty());
}

TEST_F(ThreadSafeQueueTest, MoveSemantics) {
    struct MovableData {
        int value;
        explicit MovableData(int v) : value(v) {}
        MovableData(MovableData&& other) noexcept : value(other.value) {
            other.value = -1;
        }
        MovableData& operator=(MovableData&& other) noexcept {
            if (this != &other) {
                value = other.value;
                other.value = -1;
            }
            return *this;
        }

        MovableData(const MovableData&) = delete;
        MovableData& operator=(const MovableData&) = delete;
    };

    ThreadSafeQueue<MovableData> queue(10);

    MovableData data1(42);
    EXPECT_TRUE(queue.push(std::move(data1)));
    EXPECT_EQ(data1.value, -1);  // MovableData's move operations set the
                                 // moved-from object's value to -1
    auto item = queue.pop();
    EXPECT_TRUE(item.has_value());
    EXPECT_EQ(item.value().value, 42);
}

TEST_F(ThreadSafeQueueTest, QueueFullBehavior) {
    ThreadSafeQueue<int> queue(2);

    EXPECT_TRUE(queue.push(1));
    EXPECT_TRUE(queue.push(2));
    EXPECT_EQ(queue.size_approx(), 2);

    auto start = std::chrono::steady_clock::now();
    bool result = queue.push(3, std::chrono::milliseconds(10));
    auto end = std::chrono::steady_clock::now();

    EXPECT_FALSE(result);
    auto duration =
        std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    EXPECT_GE(duration.count(), 10);

    std::vector<std::thread> threads;
    std::atomic<int> successful_pushes{0};
    std::atomic<int> failed_pushes{0};
    for (int i = 0; i < 5; ++i) {
        threads.emplace_back([&, id = i]() {
            if (queue.push(100 + id, std::chrono::milliseconds(5))) {
                successful_pushes++;
            } else {
                failed_pushes++;
            }
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }

    EXPECT_EQ(successful_pushes, 0);
    EXPECT_EQ(failed_pushes, 5);
}

/**********************************************************************************************
 */
TEST_F(ThreadSafeQueueTest, PopBatch_EmptyQueue) {
    ThreadSafeQueue<int> queue(10);

    auto start = std::chrono::steady_clock::now();
    auto batch_opt = queue.pop_batch(5, std::chrono::milliseconds(10));
    auto end = std::chrono::steady_clock::now();

    EXPECT_FALSE(batch_opt.has_value());
    auto duration =
        std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    EXPECT_GE(duration.count(), 10);
}

TEST_F(ThreadSafeQueueTest, PopBatch_SingleItem) {
    ThreadSafeQueue<int> queue(10);

    EXPECT_TRUE(queue.push(42));

    auto batch_opt = queue.pop_batch(5, std::chrono::milliseconds(100));

    EXPECT_TRUE(batch_opt.has_value());
    auto batch = batch_opt.value();
    EXPECT_EQ(batch.size(), 1);
    EXPECT_EQ(batch[0], 42);
    EXPECT_TRUE(queue.empty());
}

TEST_F(ThreadSafeQueueTest, PopBatch_MultipleItems) {
    const int num_items = 10;
    ThreadSafeQueue<int> queue(20);

    for (int i = 0; i < num_items; ++i) {
        EXPECT_TRUE(queue.push(i));
    }

    auto batch_opt = queue.pop_batch(5, std::chrono::milliseconds(100));

    EXPECT_TRUE(batch_opt.has_value());
    auto batch = batch_opt.value();
    EXPECT_EQ(batch.size(), 5);
    for (int i = 0; i < 5; ++i) {
        EXPECT_EQ(batch[i], i);
    }

    EXPECT_EQ(queue.size_approx(), 5);
}

TEST_F(ThreadSafeQueueTest, PopBatch_LessThanMaxBatch) {
    ThreadSafeQueue<int> queue(10);

    EXPECT_TRUE(queue.push(1));
    EXPECT_TRUE(queue.push(2));

    auto batch_opt = queue.pop_batch(5, std::chrono::milliseconds(100));

    EXPECT_TRUE(batch_opt.has_value());
    auto batch = batch_opt.value();
    EXPECT_EQ(batch.size(), 2);
    EXPECT_EQ(batch[0], 1);
    EXPECT_EQ(batch[1], 2);
    EXPECT_TRUE(queue.empty());
}

TEST_F(ThreadSafeQueueTest, PopBatch_ExactBatchSize) {
    const int batch_size = 5;
    ThreadSafeQueue<int> queue(20);

    for (int i = 0; i < batch_size; ++i) {
        EXPECT_TRUE(queue.push(i));
    }

    auto batch_opt =
        queue.pop_batch(batch_size, std::chrono::milliseconds(100));

    EXPECT_TRUE(batch_opt.has_value());
    auto batch = batch_opt.value();
    EXPECT_EQ(batch.size(), batch_size);
    for (int i = 0; i < batch_size; ++i) {
        EXPECT_EQ(batch[i], i);
    }
    EXPECT_TRUE(queue.empty());
}

TEST_F(ThreadSafeQueueTest, PopBatch_ShutdownDuringWait) {
    ThreadSafeQueue<int> queue(10);
    std::atomic<bool> thread_started{false};
    std::optional<std::vector<int>> result_batch_opt;

    std::thread consumer([&]() {
        thread_started = true;
        result_batch_opt = queue.pop_batch(5, std::chrono::milliseconds(1000));
    });

    while (!thread_started) {
        std::this_thread::yield();
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(10));

    queue.shutdown();

    consumer.join();

    EXPECT_FALSE(result_batch_opt.has_value());
    EXPECT_TRUE(queue.is_shutdown());
}

TEST_F(ThreadSafeQueueTest, PopBatch_ConcurrentProducers) {
    const int num_producers = 3;
    const int items_per_producer = 10;
    const int total_items = num_producers * items_per_producer;

    ThreadSafeQueue<int> queue(total_items);
    std::vector<std::thread> producers;
    std::atomic<int> items_pushed{0};

    for (int i = 0; i < num_producers; ++i) {
        producers.emplace_back([&, producer_id = i]() {
            for (int j = 0; j < items_per_producer; ++j) {
                int item = producer_id * 100 + j;
                if (queue.push(item)) {
                    items_pushed++;
                }
            }
        });
    }

    for (auto& producer : producers) {
        producer.join();
    }

    EXPECT_EQ(items_pushed.load(), total_items);
    EXPECT_EQ(queue.size_approx(), total_items);

    int batches_received = 0;
    int total_received = 0;
    std::vector<int> all_received;

    while (total_received < total_items) {
        auto batch_opt = queue.pop_batch(7, std::chrono::milliseconds(100));
        if (batch_opt.has_value()) {
            batches_received++;
            auto& batch = batch_opt.value();
            total_received += batch.size();
            all_received.insert(all_received.end(), batch.begin(), batch.end());
        }
    }

    EXPECT_EQ(total_received, total_items);
    EXPECT_GE(batches_received, 1);
    EXPECT_LE(batches_received, (total_items + 6) / 7);

    std::sort(all_received.begin(), all_received.end());
    std::vector<int> expected;
    for (int i = 0; i < num_producers; ++i) {
        for (int j = 0; j < items_per_producer; ++j) {
            expected.push_back(i * 100 + j);
        }
    }
    std::sort(expected.begin(), expected.end());

    EXPECT_EQ(all_received, expected);
    EXPECT_TRUE(queue.empty());
}

TEST_F(ThreadSafeQueueTest, TryPopBatch_EmptyQueue) {
    ThreadSafeQueue<int> queue(10);

    auto batch_opt = queue.try_pop_batch(5);

    EXPECT_FALSE(batch_opt.has_value());
    EXPECT_TRUE(queue.empty());
}

TEST_F(ThreadSafeQueueTest, TryPopBatch_SingleItem) {
    ThreadSafeQueue<int> queue(10);

    EXPECT_TRUE(queue.push(42));

    auto batch_opt = queue.try_pop_batch(5);

    EXPECT_TRUE(batch_opt.has_value());
    auto batch = batch_opt.value();
    EXPECT_EQ(batch.size(), 1);
    EXPECT_EQ(batch[0], 42);
    EXPECT_TRUE(queue.empty());
}

TEST_F(ThreadSafeQueueTest, TryPopBatch_LessThanMaxBatch) {
    ThreadSafeQueue<int> queue(10);

    EXPECT_TRUE(queue.push(1));
    EXPECT_TRUE(queue.push(2));
    EXPECT_TRUE(queue.push(3));

    auto batch_opt = queue.try_pop_batch(5);

    EXPECT_TRUE(batch_opt.has_value());
    auto batch = batch_opt.value();
    EXPECT_EQ(batch.size(), 3);
    EXPECT_EQ(batch[0], 1);
    EXPECT_EQ(batch[1], 2);
    EXPECT_EQ(batch[2], 3);
    EXPECT_TRUE(queue.empty());
}

TEST_F(ThreadSafeQueueTest, TryPopBatch_ExactBatchSize) {
    const int batch_size = 4;
    ThreadSafeQueue<int> queue(10);

    for (int i = 0; i < batch_size; ++i) {
        EXPECT_TRUE(queue.push(i));
    }

    auto batch_opt = queue.try_pop_batch(batch_size);

    EXPECT_TRUE(batch_opt.has_value());
    auto batch = batch_opt.value();
    EXPECT_EQ(batch.size(), batch_size);
    for (int i = 0; i < batch_size; ++i) {
        EXPECT_EQ(batch[i], i);
    }
    EXPECT_TRUE(queue.empty());
}

TEST_F(ThreadSafeQueueTest, TryPopBatch_MoreThanMaxBatch) {
    ThreadSafeQueue<int> queue(10);

    for (int i = 0; i < 8; ++i) {
        EXPECT_TRUE(queue.push(i));
    }

    auto batch_opt = queue.try_pop_batch(5);

    EXPECT_TRUE(batch_opt.has_value());
    auto batch = batch_opt.value();
    EXPECT_EQ(batch.size(), 5);
    for (int i = 0; i < 5; ++i) {
        EXPECT_EQ(batch[i], i);
    }

    EXPECT_EQ(queue.size_approx(), 3);

    auto batch2_opt = queue.try_pop_batch(5);
    EXPECT_TRUE(batch2_opt.has_value());
    auto batch2 = batch2_opt.value();
    EXPECT_EQ(batch2.size(), 3);
    for (int i = 0; i < 3; ++i) {
        EXPECT_EQ(batch2[i], i + 5);
    }

    EXPECT_TRUE(queue.empty());
}

TEST_F(ThreadSafeQueueTest, TryPopBatch_ShutdownQueue) {
    ThreadSafeQueue<int> queue(10);

    EXPECT_TRUE(queue.push(1));
    EXPECT_TRUE(queue.push(2));

    queue.shutdown();

    auto batch_opt = queue.try_pop_batch(5);

    EXPECT_TRUE(batch_opt.has_value());

    EXPECT_TRUE(queue.is_shutdown());

    auto batch2_opt = queue.try_pop_batch(5);
    EXPECT_FALSE(batch2_opt.has_value());
}

TEST_F(ThreadSafeQueueTest, MixedOperations) {
    ThreadSafeQueue<int> queue(20);

    EXPECT_TRUE(queue.push(1));
    EXPECT_TRUE(queue.push(2));

    auto item = queue.pop();
    EXPECT_TRUE(item.has_value());
    EXPECT_EQ(item.value(), 1);

    auto batch_opt = queue.try_pop_batch(3);
    EXPECT_TRUE(batch_opt.has_value());
    auto batch = batch_opt.value();
    EXPECT_EQ(batch.size(), 1);
    EXPECT_EQ(batch[0], 2);

    for (int i = 0; i < 10; ++i) {
        EXPECT_TRUE(queue.push(100 + i));
    }

    auto batch2_opt = queue.pop_batch(5, std::chrono::milliseconds(100));
    EXPECT_TRUE(batch2_opt.has_value());
    auto batch2 = batch2_opt.value();
    EXPECT_EQ(batch2.size(), 5);
    for (int i = 0; i < 5; ++i) {
        EXPECT_EQ(batch2[i], 100 + i);
    }

    auto batch3_opt = queue.try_pop_batch(10);
    EXPECT_TRUE(batch3_opt.has_value());
    auto batch3 = batch3_opt.value();
    EXPECT_EQ(batch3.size(), 5);
    for (int i = 0; i < 5; ++i) {
        EXPECT_EQ(batch3[i], 105 + i);
    }

    EXPECT_TRUE(queue.empty());
}

TEST_F(ThreadSafeQueueTest, PopBatch_TimeoutBehavior) {
    ThreadSafeQueue<int> queue(10);

    auto start = std::chrono::steady_clock::now();
    auto batch_opt = queue.pop_batch(3, std::chrono::milliseconds(50));
    auto end = std::chrono::steady_clock::now();

    EXPECT_FALSE(batch_opt.has_value());
    auto duration =
        std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    EXPECT_GE(duration.count(), 50);

    std::thread producer([&]() {
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
        queue.push(1);
    });

    start = std::chrono::steady_clock::now();
    batch_opt = queue.pop_batch(3, std::chrono::milliseconds(100));
    end = std::chrono::steady_clock::now();

    producer.join();

    EXPECT_TRUE(batch_opt.has_value());
    auto batch = batch_opt.value();
    EXPECT_EQ(batch.size(), 1);
    EXPECT_EQ(batch[0], 1);
    duration =
        std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    EXPECT_GE(duration.count(), 20);
    EXPECT_LT(duration.count(), 100);
}

TEST_F(ThreadSafeQueueTest, TryPopBatch_ConcurrentAccess) {
    const int num_threads = 4;
    const int items_per_thread = 25000;
    const int total_items = num_threads * items_per_thread;

    ThreadSafeQueue<int> queue(total_items);
    std::vector<std::thread> producers;
    std::vector<std::thread> consumers;
    std::atomic<int> total_popped{0};
    std::mutex result_mutex;
    std::vector<int> all_popped;

    for (int i = 0; i < num_threads; ++i) {
        producers.emplace_back([&, thread_id = i]() {
            for (int j = 0; j < items_per_thread; ++j) {
                int value = thread_id * 1000 + j;
                queue.push(value);
            }
        });
    }

    for (int i = 0; i < num_threads; ++i) {
        consumers.emplace_back([&]() {
            while (total_popped.load() < total_items) {
                auto batch_opt = queue.try_pop_batch(10);
                if (batch_opt.has_value()) {
                    auto batch = batch_opt.value();
                    total_popped += batch.size();
                    {
                        std::lock_guard<std::mutex> lock(result_mutex);
                        all_popped.insert(all_popped.end(), batch.begin(),
                                          batch.end());
                    }
                } else {
                    if (queue.empty() && total_popped.load() < total_items) {
                        std::this_thread::sleep_for(
                            std::chrono::milliseconds(1));
                    }
                }
            }
        });
    }

    for (auto& producer : producers) {
        producer.join();
    }
    for (auto& consumer : consumers) {
        consumer.join();
    }

    EXPECT_EQ(total_popped.load(), total_items);
    EXPECT_EQ(all_popped.size(), total_items);
    EXPECT_TRUE(queue.empty());

    std::sort(all_popped.begin(), all_popped.end());
    std::vector<int> expected;
    for (int i = 0; i < num_threads; ++i) {
        for (int j = 0; j < items_per_thread; ++j) {
            expected.push_back(i * 1000 + j);
        }
    }
    std::sort(expected.begin(), expected.end());

    EXPECT_EQ(all_popped, expected);
}

TEST_F(ThreadSafeQueueTest, BatchOperationsWithCustomType) {
    struct TestData {
        int id;
        std::string name;

        TestData(int i, std::string n) : id(i), name(std::move(n)) {}
        TestData(TestData&&) = default;
        TestData& operator=(TestData&&) = default;

        TestData(const TestData&) = delete;
        TestData& operator=(const TestData&) = delete;
    };

    ThreadSafeQueue<TestData> queue(10);

    EXPECT_TRUE(queue.push(TestData(1, "Alice")));
    EXPECT_TRUE(queue.push(TestData(2, "Bob")));
    EXPECT_TRUE(queue.push(TestData(3, "Charlie")));

    auto batch_opt = queue.pop_batch(2, std::chrono::milliseconds(100));
    EXPECT_TRUE(batch_opt.has_value());
    auto& batch = batch_opt.value();
    EXPECT_EQ(batch.size(), 2);
    EXPECT_EQ(batch[0].id, 1);
    EXPECT_EQ(batch[0].name, "Alice");
    EXPECT_EQ(batch[1].id, 2);
    EXPECT_EQ(batch[1].name, "Bob");

    EXPECT_EQ(queue.size_approx(), 1);

    auto batch2_opt = queue.try_pop_batch(5);
    EXPECT_TRUE(batch2_opt.has_value());
    auto& batch2 = batch2_opt.value();
    EXPECT_EQ(batch2.size(), 1);
    EXPECT_EQ(batch2[0].id, 3);
    EXPECT_EQ(batch2[0].name, "Charlie");

    EXPECT_TRUE(queue.empty());
}

TEST_F(ThreadSafeQueueTest, PopBatch_QueueFullBehavior) {
    const int capacity = 5;
    ThreadSafeQueue<int> queue(capacity);

    for (int i = 0; i < capacity; ++i) {
        EXPECT_TRUE(queue.push(i));
    }

    EXPECT_EQ(queue.size_approx(), capacity);

    auto batch_opt = queue.pop_batch(3, std::chrono::milliseconds(100));
    EXPECT_TRUE(batch_opt.has_value());
    auto batch = batch_opt.value();
    EXPECT_EQ(batch.size(), 3);

    EXPECT_EQ(queue.size_approx(), 2);

    EXPECT_TRUE(queue.push(100));
    EXPECT_TRUE(queue.push(101));

    EXPECT_EQ(queue.size_approx(), 4);

    auto batch2_opt = queue.try_pop_batch(10);
    EXPECT_TRUE(batch2_opt.has_value());
    auto batch2 = batch2_opt.value();
    EXPECT_EQ(batch2.size(), 4);
}

TEST_F(ThreadSafeQueueTest, BatchSizeZero) {
    ThreadSafeQueue<int> queue(10);

    EXPECT_TRUE(queue.push(1));
    EXPECT_TRUE(queue.push(2));

    auto batch_opt = queue.pop_batch(0, std::chrono::milliseconds(10));
    EXPECT_FALSE(batch_opt.has_value());

    auto batch2_opt = queue.try_pop_batch(0);
    EXPECT_FALSE(batch2_opt.has_value());

    EXPECT_EQ(queue.size_approx(), 2);
}

TEST_F(ThreadSafeQueueTest, BatchOperationsPerformance) {
    const int num_items = 100000;
    const int batch_size = 100;

    ThreadSafeQueue<int> queue(num_items);

    auto start_fill = std::chrono::steady_clock::now();
    for (int i = 0; i < num_items; ++i) {
        queue.push(i);
    }
    auto end_fill = std::chrono::steady_clock::now();

    auto fill_time = std::chrono::duration_cast<std::chrono::milliseconds>(
        end_fill - start_fill);
    LOG(INFO) << "Filled " << num_items << " items in " << fill_time.count()
              << "ms";

    int total_popped = 0;
    std::vector<std::vector<int>> all_batches;
    auto start_pop = std::chrono::steady_clock::now();

    while (total_popped < num_items) {
        auto batch_opt =
            queue.pop_batch(batch_size, std::chrono::milliseconds(1000));
        if (batch_opt.has_value()) {
            auto batch = batch_opt.value();
            total_popped += batch.size();
            all_batches.push_back(batch);
        }
    }

    auto end_pop = std::chrono::steady_clock::now();
    auto pop_time = std::chrono::duration_cast<std::chrono::milliseconds>(
        end_pop - start_pop);

    LOG(INFO) << "Popped " << num_items << " items in " << all_batches.size()
              << " batches, time: " << pop_time.count() << "ms";

    EXPECT_EQ(total_popped, num_items);
    EXPECT_TRUE(queue.empty());

    int expected_value = 0;
    for (const auto& batch : all_batches) {
        for (int value : batch) {
            EXPECT_EQ(value, expected_value++);
        }
    }
}

TEST_F(ThreadSafeQueueTest, HighContentionMPSC_PerformanceAndAccuracy) {
    const int NUM_PRODUCERS = 8;
    const int NUM_EVENTS_PER_PRODUCER = 10000;
    const int QUEUE_CAPACITY = 100000;
    const int CONSUMER_BATCH_SIZE = 10;
    const int TOTAL_EVENTS = NUM_PRODUCERS * NUM_EVENTS_PER_PRODUCER;

    const int64_t BLOCKING_THRESHOLD_NS = 100000;

    ThreadSafeQueue<int> queue(QUEUE_CAPACITY);

    struct TestStats {
        std::atomic<int64_t> total_produced{0};
        std::atomic<int64_t> producer_block_count{0};
        std::atomic<int64_t> producer_failures{0};
        std::atomic<int64_t> producer_total_push_time_ns{0};

        std::atomic<int64_t> total_consumed{0};
        std::atomic<int64_t> consumer_batches_processed{0};
        std::atomic<int64_t> consumer_total_time_ns{0};

        std::atomic<int64_t> max_observed_size{0};
        std::atomic<int64_t> min_observed_size{0};

        std::atomic<int64_t> events_out_of_range{0};
    };

    TestStats stats;
    stats.min_observed_size.store(INT64_MAX);

    std::atomic<bool> producers_done{false};

    auto start_time = std::chrono::steady_clock::now();

    std::thread consumer([&]() {
        LOG(INFO) << "Consumer thread started";

        int consecutive_empty_cycles = 0;
        const int MAX_EMPTY_CYCLES = 10;
        auto consumer_start = std::chrono::steady_clock::now();

        while (consecutive_empty_cycles < MAX_EMPTY_CYCLES) {
            auto batch_start = std::chrono::steady_clock::now();
            auto batch_opt = queue.pop_batch(CONSUMER_BATCH_SIZE,
                                             std::chrono::milliseconds(10));
            auto batch_end = std::chrono::steady_clock::now();

            if (batch_opt.has_value()) {
                consecutive_empty_cycles = 0;
                auto& batch = batch_opt.value();
                int batch_size = static_cast<int>(batch.size());

                stats.total_consumed += batch_size;
                stats.consumer_batches_processed++;

                auto batch_time_ns =
                    std::chrono::duration_cast<std::chrono::nanoseconds>(
                        batch_end - batch_start)
                        .count();
                stats.consumer_total_time_ns += batch_time_ns;

                for (int value : batch) {
                    if (value < 0 || value >= TOTAL_EVENTS) {
                        stats.events_out_of_range++;
                        LOG(ERROR) << "Event out of range: " << value;
                    }
                }

                int64_t current_size = queue.size_approx();
                int64_t max_observed = stats.max_observed_size.load();
                int64_t min_observed = stats.min_observed_size.load();

                while (current_size > max_observed &&
                       !stats.max_observed_size.compare_exchange_weak(
                           max_observed, current_size)) {
                }

                if (current_size < min_observed) {
                    stats.min_observed_size.store(current_size);
                }
            } else {
                consecutive_empty_cycles++;
                if (producers_done.load() && queue.empty()) {
                    break;
                }
            }
        }

        auto consumer_end = std::chrono::steady_clock::now();
        auto consumer_duration_ms =
            std::chrono::duration_cast<std::chrono::milliseconds>(
                consumer_end - consumer_start)
                .count();

        LOG(INFO) << "Consumer thread completed: "
                  << stats.total_consumed.load() << " events, "
                  << stats.consumer_batches_processed.load()
                  << " batches, duration " << consumer_duration_ms << "ms";
    });

    std::vector<std::thread> producers;
    producers.reserve(NUM_PRODUCERS);

    for (int producer_id = 0; producer_id < NUM_PRODUCERS; ++producer_id) {
        producers.emplace_back([&, producer_id]() {
            LOG(INFO) << "Producer " << producer_id << " started";

            int64_t local_events_produced = 0;
            int64_t local_block_count = 0;
            int64_t local_failures = 0;
            int64_t local_total_push_time_ns = 0;

            auto producer_start = std::chrono::steady_clock::now();

            for (int event_id = 0; event_id < NUM_EVENTS_PER_PRODUCER;
                 ++event_id) {
                int value = producer_id * NUM_EVENTS_PER_PRODUCER + event_id;

                auto push_start = std::chrono::steady_clock::now();
                bool pushed = queue.push(value);
                auto push_end = std::chrono::steady_clock::now();

                int64_t push_duration_ns =
                    std::chrono::duration_cast<std::chrono::nanoseconds>(
                        push_end - push_start)
                        .count();

                local_total_push_time_ns += push_duration_ns;

                if (pushed) {
                    local_events_produced++;
                    stats.total_produced++;

                    if (push_duration_ns > BLOCKING_THRESHOLD_NS) {
                        local_block_count++;
                    }

                    if (event_id % 2000 == 0) {
                        int64_t current_size = queue.size_approx();
                        int64_t max_observed = stats.max_observed_size.load();
                        int64_t min_observed = stats.min_observed_size.load();

                        while (current_size > max_observed &&
                               !stats.max_observed_size.compare_exchange_weak(
                                   max_observed, current_size)) {
                        }

                        if (current_size < min_observed) {
                            stats.min_observed_size.store(current_size);
                        }
                    }
                } else {
                    LOG(WARNING) << "Producer " << producer_id
                                 << " push failed, retrying...";
                    pushed = queue.push(value, std::chrono::milliseconds(1));

                    if (pushed) {
                        local_events_produced++;
                        stats.total_produced++;
                        local_failures++;
                    } else {
                        local_failures++;
                        LOG(ERROR) << "Producer " << producer_id
                                   << " failed to push event " << event_id;
                    }
                }

                if (event_id % 1000 == 0) {
                    std::this_thread::yield();
                }
            }

            stats.producer_block_count += local_block_count;
            stats.producer_failures += local_failures;
            stats.producer_total_push_time_ns += local_total_push_time_ns;

            auto producer_end = std::chrono::steady_clock::now();
            auto producer_duration_ms =
                std::chrono::duration_cast<std::chrono::milliseconds>(
                    producer_end - producer_start)
                    .count();

            LOG(INFO) << "Producer " << producer_id
                      << " completed: " << local_events_produced << " events, "
                      << local_block_count << " blocks, " << local_failures
                      << " failures, "
                      << "duration " << producer_duration_ms << "ms";
        });
    }

    auto producer_join_start = std::chrono::steady_clock::now();
    for (auto& producer : producers) {
        producer.join();
    }
    auto producer_join_end = std::chrono::steady_clock::now();

    producers_done = true;
    LOG(INFO) << "All producers completed, waiting for consumer...";

    consumer.join();

    auto end_time = std::chrono::steady_clock::now();
    auto total_duration_ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(end_time -
                                                              start_time)
            .count();
    auto producer_only_duration_ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(
            producer_join_end - start_time)
            .count();

    double total_duration_seconds = total_duration_ms / 1000.0;
    double producer_duration_seconds = producer_only_duration_ms / 1000.0;

    double events_per_second_total = TOTAL_EVENTS / total_duration_seconds;
    double events_per_second_producer =
        TOTAL_EVENTS / producer_duration_seconds;

    int64_t actual_produced = stats.total_produced.load();
    int64_t actual_consumed = stats.total_consumed.load();

    double avg_push_time_ns = stats.producer_total_push_time_ns /
                              static_cast<double>(actual_produced);
    double avg_batch_time_ns =
        stats.consumer_total_time_ns /
        static_cast<double>(stats.consumer_batches_processed);
    double avg_batch_size =
        actual_consumed / static_cast<double>(stats.consumer_batches_processed);

    double blocking_percentage =
        stats.producer_block_count * 100.0 / actual_produced;
    double failure_percentage = stats.producer_failures * 100.0 /
                                (actual_produced + stats.producer_failures);

    int64_t max_size = stats.max_observed_size.load();
    int64_t min_size = stats.min_observed_size.load();

    LOG(INFO) << "========================================";
    LOG(INFO) << "MPSC High Contention Test Results";
    LOG(INFO) << "========================================";

    LOG(INFO) << "Data Integrity:";
    LOG(INFO) << "  Planned events: " << TOTAL_EVENTS;
    LOG(INFO) << "  Actual produced: " << actual_produced;
    LOG(INFO) << "  Actual consumed: " << actual_consumed;
    LOG(INFO) << "  Queue remaining size: " << queue.size_approx();
    LOG(INFO) << "  Queue empty: " << (queue.empty() ? "yes" : "no");
    LOG(INFO) << "  Events out of range: " << stats.events_out_of_range.load();

    EXPECT_EQ(actual_produced, TOTAL_EVENTS) << "Mismatch in produced events";
    EXPECT_EQ(actual_consumed, TOTAL_EVENTS) << "Mismatch in consumed events";
    EXPECT_TRUE(queue.empty()) << "Queue not empty after test";
    EXPECT_EQ(stats.events_out_of_range.load(), 0)
        << "Events out of range found";

    LOG(INFO) << "Performance Metrics:";
    LOG(INFO) << "  Total duration: " << total_duration_ms << "ms";
    LOG(INFO) << "  Producer duration: " << producer_only_duration_ms << "ms";
    LOG(INFO) << "  Total throughput: " << events_per_second_total
              << " events/sec";
    LOG(INFO) << "  Producer throughput: " << events_per_second_producer
              << " events/sec";
    LOG(INFO) << "  Average push time: " << avg_push_time_ns << " ns";
    LOG(INFO) << "  Average batch processing time: " << avg_batch_time_ns
              << " ns";
    LOG(INFO) << "  Average batch size: " << avg_batch_size << " events/batch";
    LOG(INFO) << "  Blocking push ratio: " << blocking_percentage << "%";
    LOG(INFO) << "  Push failure ratio: " << failure_percentage << "%";

    EXPECT_GT(events_per_second_total, 10000) << "Throughput too low";
    EXPECT_LT(avg_push_time_ns, 20000) << "Average push time too high";
    EXPECT_LT(failure_percentage, 0.1) << "Push failure rate too high";

    LOG(INFO) << "Queue Behavior:";
    LOG(INFO) << "  Max observed queue size: " << max_size;
    LOG(INFO) << "  Min observed queue size: " << min_size;
    LOG(INFO) << "  Queue size range: " << (max_size - min_size);
    LOG(INFO) << "  Total consumer batches: "
              << stats.consumer_batches_processed.load();

    EXPECT_LE(max_size, QUEUE_CAPACITY + NUM_PRODUCERS * 2)
        << "Queue size exceeds reasonable range";
    EXPECT_GE(min_size, 0) << "Queue size should not be negative";

    LOG(INFO) << "Blocking Behavior:";
    LOG(INFO) << "  Blocking push count: " << stats.producer_block_count.load();
    LOG(INFO) << "  Blocking push ratio: " << blocking_percentage << "%";

    if (blocking_percentage > 5.0) {
        LOG(WARNING) << "High blocking ratio, consider adjusting queue "
                        "capacity or number of producers";
    }

    LOG(INFO) << "Resource Usage Summary:";
    LOG(INFO) << "  Estimated memory usage: " << (max_size * sizeof(int))
              << " bytes";
    LOG(INFO) << "  Producer threads: " << NUM_PRODUCERS;
    LOG(INFO) << "  Consumer threads: 1";
    LOG(INFO) << "  Peak queue capacity usage: "
              << (max_size * 100.0 / QUEUE_CAPACITY) << "%";

    EXPECT_LE(blocking_percentage, 20.0) << "Blocking ratio too high";

    if (avg_batch_size < 10) {
        LOG(WARNING) << "Batch size too small(" << avg_batch_size
                     << "< 10), consumer efficiency low";
    }

    LOG(INFO) << "========================================";
    LOG(INFO) << "========================================";
}

TEST_F(ThreadSafeQueueTest, PerformanceBaseline) {
    ThreadSafeQueue<int> queue(100000);
    auto start = std::chrono::steady_clock::now();
    for (int i = 0; i < 1000; ++i) {
        queue.push(i);
    }
    auto end = std::chrono::steady_clock::now();
    auto avg_ns =
        std::chrono::duration_cast<std::chrono::nanoseconds>(end - start)
            .count() /
        1000.0;

    LOG(INFO) << "Baseline push time: " << avg_ns << " ns";
}

}  // namespace mooncake

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}