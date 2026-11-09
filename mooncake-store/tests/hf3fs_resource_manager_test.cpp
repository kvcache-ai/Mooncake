#include <gtest/gtest.h>

#include <memory>
#include <future>
#include <chrono>
#include <set>
#include <thread>
#include <vector>

#include "hf3fs/hf3fs.h"

namespace mooncake::test {

class Hf3fsResourceManagerTest : public ::testing::Test {
   protected:
    void SetUp() override {
        hf3fs_test::calls = 0;
        hf3fs_test::fail_call = 0;
    }
    void TearDown() override {
        EXPECT_EQ(hf3fs_test::live_iovs, 0);
        EXPECT_EQ(hf3fs_test::live_rings, 0);
    }
};

TEST_F(Hf3fsResourceManagerTest, ReusesResourcesAndAlternatesManagers) {
    USRBIOResourceManager first, second;
    auto* resource = first.getThreadResource();
    ASSERT_NE(resource, nullptr);
    EXPECT_EQ(first.getThreadResource(), resource);
    EXPECT_NE(second.getThreadResource(), resource);
    EXPECT_EQ(first.getThreadResource(), resource);
    EXPECT_EQ(hf3fs_test::calls, 6);
}

TEST_F(Hf3fsResourceManagerTest, ManagerAddressReuseDoesNotUseStaleTls) {
    alignas(USRBIOResourceManager)
        std::byte storage[sizeof(USRBIOResourceManager)];
    for (int i = 0; i < 20; ++i) {
        auto* manager = std::construct_at(
            reinterpret_cast<USRBIOResourceManager*>(storage));
        ASSERT_NE(manager->getThreadResource(), nullptr);
        std::destroy_at(manager);
        EXPECT_EQ(hf3fs_test::live_iovs, 0);
    }
    EXPECT_EQ(hf3fs_test::calls, 60);
}

TEST_F(Hf3fsResourceManagerTest, CleanupForcesReinitialization) {
    USRBIOResourceManager manager;
    auto* resource = manager.getThreadResource();
    ASSERT_NE(resource, nullptr);
    resource->Cleanup();
    hf3fs_test::fail_call = hf3fs_test::calls + 1;
    EXPECT_EQ(manager.getThreadResource(), nullptr);
    EXPECT_EQ(manager.getThreadResource(), resource);
    EXPECT_TRUE(resource->initialized);
}

TEST_F(Hf3fsResourceManagerTest, InitializationFailureRollsBackAndRetries) {
    for (int step = 1; step <= 3; ++step) {
        hf3fs_test::calls = 0;
        hf3fs_test::fail_call = step;
        USRBIOResourceManager manager;
        EXPECT_EQ(manager.getThreadResource(), nullptr);
        EXPECT_EQ(hf3fs_test::live_iovs, 0);
        EXPECT_EQ(hf3fs_test::live_rings, 0);
        EXPECT_NE(manager.getThreadResource(), nullptr);
    }
}

TEST_F(Hf3fsResourceManagerTest, RecycledThreadIdsDoNotGrowResourceOwnership) {
    USRBIOResourceManager manager;
    std::set<std::thread::id> thread_ids;
    for (int i = 0; i < 32; ++i) {
        std::thread::id thread_id;
        std::thread worker([&] {
            thread_id = std::this_thread::get_id();
            auto* resource = manager.getThreadResource();
            ASSERT_NE(resource, nullptr);
            EXPECT_EQ(manager.getThreadResource(), resource);
        });
        worker.join();
        thread_ids.insert(thread_id);
    }
    EXPECT_EQ(hf3fs_test::live_iovs, static_cast<int>(thread_ids.size()));
}

TEST_F(Hf3fsResourceManagerTest, CachedLookupDoesNotWaitForManagerMutex) {
    USRBIOResourceManager manager;
    std::promise<void> ready, lookup;
    auto lookup_signal = lookup.get_future();
    auto cached = std::async(std::launch::async, [&] {
        auto* resource = manager.getThreadResource();
        ready.set_value();
        lookup_signal.wait();
        return resource != nullptr && manager.getThreadResource() == resource;
    });
    ready.get_future().wait();
    hf3fs_test::create_entered = false;
    hf3fs_test::block_create = true;
    auto initializing = std::async(std::launch::async,
                                   [&] { return manager.getThreadResource(); });
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (!hf3fs_test::create_entered &&
           std::chrono::steady_clock::now() < deadline) {
        std::this_thread::yield();
    }
    const bool entered = hf3fs_test::create_entered;
    lookup.set_value();
    const auto status = cached.wait_for(std::chrono::seconds(1));
    // Release before any assertion or future destruction, even on failure.
    hf3fs_test::block_create = false;
    EXPECT_TRUE(entered);
    EXPECT_EQ(status, std::future_status::ready);
    EXPECT_TRUE(cached.get());
    EXPECT_NE(initializing.get(), nullptr);
}

TEST_F(Hf3fsResourceManagerTest, ConcurrentThreadsHaveSeparateResources) {
    USRBIOResourceManager manager;
    std::vector<std::thread> workers;
    std::vector<ThreadUSRBIOResource*> resources(16);
    for (size_t i = 0; i < resources.size(); ++i) {
        workers.emplace_back([&, i] {
            resources[i] = manager.getThreadResource();
            for (int j = 0; j < 1000; ++j) {
                EXPECT_EQ(manager.getThreadResource(), resources[i]);
            }
        });
    }
    for (auto& worker : workers) worker.join();
    for (size_t i = 0; i < resources.size(); ++i) {
        ASSERT_NE(resources[i], nullptr);
        for (size_t j = 0; j < i; ++j) EXPECT_NE(resources[i], resources[j]);
    }
}

}  // namespace mooncake::test
