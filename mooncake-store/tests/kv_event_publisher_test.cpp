#include <gtest/gtest.h>
#include <glog/logging.h>
#include <msgpack.hpp>
#include <zmq.hpp>

#include <chrono>
#include <thread>
#include <atomic>
#include <future>
#include <type_traits>

#include "kv_event_publisher.h"

namespace mooncake {

class KVEventSystemTest : public ::testing::Test {
   protected:
    static void SetUpTestSuite() {
        google::InitGoogleLogging("KVEventSystemTest");
        FLAGS_logtostderr = 1;
    }

    static void TearDownTestSuite() { google::ShutdownGoogleLogging(); }

    void SetUp() override {
        create_test_descriptors();
        create_test_store_event_info();
    }

    Replica::Descriptor create_memory_replica() {
        Replica::Descriptor desc;
        uintptr_t dummy_address = 0xDEADBEEF;
        desc.descriptor_variant = MemoryDescriptor{
            .buffer_descriptor = {
                .size_ = 1024,
                .buffer_address_ = dummy_address,
                .transport_endpoint_ = "tcp://192.168.1.100:5555"}};
        desc.status = ReplicaStatus::COMPLETE;
        return desc;
    }

    Replica::Descriptor create_disk_replica() {
        Replica::Descriptor desc;
        desc.descriptor_variant = DiskDescriptor{
            .file_path = "/data/blocks/block_123.bin", .object_size = 4096};
        desc.status = ReplicaStatus::COMPLETE;
        return desc;
    }

    void create_test_descriptors() {
        memory_replica_ = create_memory_replica();
        disk_replica_ = create_disk_replica();

        replicas_mixed_ = {memory_replica_, disk_replica_};
        replicas_memory_only_ = {memory_replica_, memory_replica_};
        replicas_disk_only_ = {disk_replica_, disk_replica_};
    }

    void create_test_store_event_info() {
        std::vector<uint32_t> token_ids = {1, 2, 3, 4, 5};

        store_event_info = {"gpt-4", 1024, "0xABCD1234", "0x9876FEDC",
                            token_ids};
    }

    template <typename ConfigType>
    std::unique_ptr<KVEventSystem> create_publish_system(ConfigType&& config) {
        static_assert(
            std::is_same_v<std::decay_t<ConfigType>, KVEventPublisherConfig>,
            "ConfigType must be KVEventPublisherConfig");
        return std::make_unique<KVEventSystem>(
            std::forward<ConfigType>(config));
    }

    static void verify_block_store_serialization(const BlockStoreEvent& event) {
        msgpack::sbuffer buffer;
        msgpack::packer<msgpack::sbuffer> pk(buffer);
        event.pack(pk);

        msgpack::object_handle oh =
            msgpack::unpack(buffer.data(), buffer.size());
        msgpack::object obj = oh.get();

        EXPECT_EQ(obj.type, msgpack::type::ARRAY);
        EXPECT_EQ(obj.via.array.size, 8);
    }

   protected:
    Replica::Descriptor memory_replica_;
    Replica::Descriptor disk_replica_;
    StoreEventInfo store_event_info;
    std::vector<Replica::Descriptor> replicas_mixed_;
    std::vector<Replica::Descriptor> replicas_memory_only_;
    std::vector<Replica::Descriptor> replicas_disk_only_;
};

TEST_F(KVEventSystemTest, BlockStoreEventSerialization) {
    std::string model_name = "gpt-4";
    std::vector<uint32_t> token_ids = {1, 2, 3, 4, 5};

    BlockStoreEvent event("key123", replicas_mixed_, store_event_info);

    EXPECT_EQ(event.type_tag(), "BlockStoreEvent");
    verify_block_store_serialization(event);
}

TEST_F(KVEventSystemTest, BlockUpdateEventSerialization) {
    BlockUpdateEvent event("key123", replicas_mixed_);

    EXPECT_EQ(event.type_tag(), "BlockUpdateEvent");

    msgpack::sbuffer buffer;
    msgpack::packer<msgpack::sbuffer> pk(buffer);
    event.pack(pk);

    msgpack::object_handle oh = msgpack::unpack(buffer.data(), buffer.size());
    msgpack::object obj = oh.get();

    EXPECT_EQ(obj.type, msgpack::type::ARRAY);
    EXPECT_EQ(obj.via.array.size, 3);
}

TEST_F(KVEventSystemTest, EventBatchSerialization) {
    auto event1 = std::make_shared<BlockStoreEvent>(
        "key1", replicas_memory_only_, store_event_info);

    auto event2 =
        std::make_shared<BlockUpdateEvent>("key2", replicas_disk_only_);

    EventBatch batch({event1, event2});

    EXPECT_GT(batch.ts, 0.0);

    msgpack::sbuffer serialized = batch.serialize();
    EXPECT_GT(serialized.size(), 0);
}

TEST_F(KVEventSystemTest, KVEventSystemConstruction) {
    EXPECT_NO_THROW({
        KVEventPublisherConfig config;
        auto publisher = create_publish_system(config);
        EXPECT_TRUE(publisher->is_running());

        auto stats = publisher->get_stats();
        EXPECT_EQ(stats.producer_stats.events_created, 0);
        EXPECT_EQ(stats.event_queue_stats.queue_remain_events, 0);
        EXPECT_EQ(stats.event_queue_stats.queue_capacity,
                  config.max_queue_size);

        publisher->shutdown();
        EXPECT_FALSE(publisher->is_running());
    });
}

TEST_F(KVEventSystemTest, KVEventSystemMultipleConstruction) {
    auto publisher1 = create_publish_system(KVEventPublisherConfig{});
    auto publisher2 = create_publish_system(KVEventPublisherConfig{});

    EXPECT_TRUE(publisher1->is_running());
    EXPECT_TRUE(publisher2->is_running());

    auto future1 = publisher1->publish<BlockStoreEvent>("key1", replicas_mixed_,
                                                        store_event_info);

    auto status1 = future1.wait_for(std::chrono::seconds(2));
    EXPECT_EQ(status1, std::future_status::ready);

    auto success1 = future1.get();
    EXPECT_TRUE(success1);

    publisher1->shutdown();
    publisher2->shutdown();
}

TEST_F(KVEventSystemTest, KVEventSystemBasicPublish) {
    auto publisher_ = create_publish_system(KVEventPublisherConfig{});

    auto future_store = publisher_->publish<BlockStoreEvent>(
        "test_key_store", replicas_mixed_, store_event_info);

    auto future_update = publisher_->publish<BlockUpdateEvent>(
        "test_key_update", replicas_mixed_);

    auto future_remove_all = publisher_->publish<RemoveAllEvent>();

    auto status_store = future_store.wait_for(std::chrono::seconds(2));
    auto status_update = future_update.wait_for(std::chrono::seconds(2));
    auto status_remove_all =
        future_remove_all.wait_for(std::chrono::seconds(2));

    EXPECT_EQ(status_store, std::future_status::ready);
    EXPECT_EQ(status_update, std::future_status::ready);
    EXPECT_EQ(status_remove_all, std::future_status::ready);

    EXPECT_TRUE(future_store.get());
    EXPECT_TRUE(future_update.get());
    EXPECT_TRUE(future_remove_all.get());

    auto stats = publisher_->get_stats();
    EXPECT_EQ(stats.producer_stats.events_created, 3);

    publisher_->shutdown();
    EXPECT_FALSE(publisher_->is_running());
}

TEST_F(KVEventSystemTest, KVEventSystemConcurrentPublishing) {
    auto publisher_ =
        create_publish_system(KVEventPublisherConfig{.max_queue_size = 1000});

    const int num_threads = 4;
    const int events_per_thread = 100;
    std::atomic<int> events_published{0};
    std::atomic<int> failed_events{0};
    std::vector<std::thread> threads;
    std::vector<std::future<bool>> futures;
    std::mutex futures_mutex;

    futures.reserve(num_threads * events_per_thread);

    for (int i = 0; i < num_threads; ++i) {
        threads.emplace_back([this, i, &publisher_, &events_published, &futures,
                              &futures_mutex]() {
            for (int j = 0; j < events_per_thread; ++j) {
                std::string key =
                    "key_" + std::to_string(i) + "_" + std::to_string(j);

                try {
                    auto future = publisher_->publish<BlockStoreEvent>(
                        key, replicas_mixed_, store_event_info);
                    {
                        std::lock_guard<std::mutex> lock(futures_mutex);
                        futures.push_back(std::move(future));
                    }
                    events_published++;
                } catch (const std::exception& e) {
                    FAIL() << "Publish failed: " << e.what();
                }

                std::this_thread::sleep_for(std::chrono::microseconds(5));
            }
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    int successful_events = 0;
    for (auto& future : futures) {
        if (future.wait_for(std::chrono::seconds(2)) ==
            std::future_status::ready) {
            if (future.get()) {
                successful_events++;
            }
        }
    }

    EXPECT_EQ(events_published.load(), num_threads * events_per_thread);

    auto stats = publisher_->get_stats();
    EXPECT_GE(stats.producer_stats.events_created, events_published.load());

    publisher_->shutdown();
    EXPECT_FALSE(publisher_->is_running());
}

TEST_F(KVEventSystemTest, KVEventSystemGracefulShutdown) {
    auto publisher_ =
        create_publish_system(KVEventPublisherConfig{.max_queue_size = 100});

    std::vector<std::future<bool>> futures;
    for (int i = 0; i < 50; ++i) {
        futures.push_back(publisher_->publish<BlockStoreEvent>(
            "key_" + std::to_string(i), replicas_mixed_, store_event_info));
    }

    for (auto& future : futures) {
        future.wait_for(std::chrono::milliseconds(100));
    }

    EXPECT_NO_THROW(publisher_->shutdown());

    auto future_after_shutdown = publisher_->publish<BlockStoreEvent>(
        "should_fail", replicas_mixed_, store_event_info);

    EXPECT_EQ(future_after_shutdown.wait_for(std::chrono::milliseconds(100)),
              std::future_status::ready);
    EXPECT_FALSE(future_after_shutdown.get());
}

TEST_F(KVEventSystemTest, KVEventSystemEdgeCases) {
    auto publisher_ = create_publish_system(KVEventPublisherConfig{});

    std::vector<Replica::Descriptor> empty_replicas;
    auto future1 = publisher_->publish<BlockStoreEvent>(
        "empty_replicas_key", empty_replicas, store_event_info);

    EXPECT_EQ(future1.wait_for(std::chrono::seconds(2)),
              std::future_status::ready);
    EXPECT_TRUE(future1.get());

    std::vector<uint32_t> large_tokens(10000);
    for (size_t i = 0; i < large_tokens.size(); ++i) {
        large_tokens[i] = static_cast<uint32_t>(i);
    }

    StoreEventInfo large_info = {"large_model", 1024 * 1024, "0x12345678",
                                 "0x87654321", large_tokens};

    auto future2 = publisher_->publish<BlockStoreEvent>(
        "large_tokens_key", replicas_mixed_, large_info);

    EXPECT_EQ(future2.wait_for(std::chrono::seconds(2)),
              std::future_status::ready);
    EXPECT_TRUE(future2.get());

    publisher_->shutdown();
    EXPECT_FALSE(publisher_->is_running());
}

TEST_F(KVEventSystemTest, KVEventSystemStats) {
    auto publisher_ = create_publish_system(KVEventPublisherConfig{});

    auto initial_stats = publisher_->get_stats();
    EXPECT_EQ(initial_stats.producer_stats.events_created, 0);
    EXPECT_EQ(initial_stats.consumer_stats.total_batches, 0);
    EXPECT_EQ(initial_stats.consumer_stats.failed_events, 0);

    std::vector<std::future<bool>> futures;
    for (int i = 0; i < 10; ++i) {
        futures.push_back(publisher_->publish<BlockStoreEvent>(
            "key_" + std::to_string(i), replicas_mixed_, store_event_info));
    }

    for (auto& future : futures) {
        future.wait_for(std::chrono::seconds(1));
    }

    auto final_stats = publisher_->get_stats();
    EXPECT_GE(final_stats.producer_stats.events_created, 10);
    EXPECT_GE(final_stats.consumer_stats.total_batches, 0);
    EXPECT_EQ(final_stats.consumer_stats.failed_events, 0);

    publisher_->shutdown();
    EXPECT_FALSE(publisher_->is_running());
}

TEST_F(KVEventSystemTest, PerformanceTest) {
    auto publisher_ = create_publish_system(KVEventPublisherConfig{
        .max_queue_size = 10000,
        .max_batch_size = 100,
        .batch_timeout = std::chrono::milliseconds(500)});

    const int num_events = 1000;
    auto start_time = std::chrono::high_resolution_clock::now();

    std::vector<std::future<bool>> futures;
    futures.reserve(num_events);

    for (int i = 0; i < num_events; ++i) {
        futures.push_back(publisher_->publish<BlockStoreEvent>(
            "perf_key_" + std::to_string(i), replicas_mixed_,
            store_event_info));
    }

    int successful_events = 0;
    for (auto& future : futures) {
        if (future.wait_for(std::chrono::seconds(5)) ==
            std::future_status::ready) {
            if (future.get()) {
                successful_events++;
            }
        }
    }

    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
        end_time - start_time);

    LOG(INFO) << "Published " << num_events << " events in " << duration.count()
              << "ms";
    LOG(INFO) << "Successful events: " << successful_events << "/"
              << num_events;
    LOG(INFO) << "Throughput: " << (num_events * 1000.0 / duration.count())
              << " events/sec";

    auto stats = publisher_->get_stats();
    LOG(INFO) << "Stats - Total events: " << stats.producer_stats.events_created
              << ", Total batches: " << stats.consumer_stats.total_batches
              << ", Failed events: " << stats.consumer_stats.failed_events;

    // Allow up to 10 failed events out of 1000 for transient issues
    const int MAX_ALLOWED_FAILURES = 10;
    EXPECT_GE(successful_events, num_events - MAX_ALLOWED_FAILURES);
    publisher_->shutdown();
    EXPECT_FALSE(publisher_->is_running());
}

}  // namespace mooncake

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}