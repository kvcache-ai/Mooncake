#include "local_hot_cache.h"

#include <gtest/gtest.h>

#include <atomic>
#include <string>
#include <thread>

namespace mooncake {
namespace {

TEST(LocalHotCacheConcurrencyTest, DetachedFillDoesNotRaceWithActiveKeyScans) {
    LocalHotCache cache(128, 64);
    auto* active = cache.GetFreeBlock();
    auto* filling = cache.GetFreeBlock();
    ASSERT_NE(active, nullptr);
    ASSERT_NE(filling, nullptr);

    // GetFreeBlock takes from the tail: the detached fill is scanned before
    // the active block. Its key belongs exclusively to the filling thread
    // until PutHotKey publishes it, just as in SubmitPutTask.
    std::atomic<bool> started{false};
    std::atomic<bool> stop{false};
    std::thread fill([&] {
        started.store(true);
        while (!stop.load()) {
            filling->key_ = std::string(128, 'x');
            filling->key_ = std::string(256, 'y');
        }
    });
    while (!started.load()) std::this_thread::yield();

    const std::string key(128, 'a');
    for (int i = 0; i < 1000; ++i) {
        active->key_ = key;
        EXPECT_TRUE(cache.PutHotKey(active));
        EXPECT_EQ(cache.GetHotKey(key), active);
        EXPECT_TRUE(cache.RemoveHotKey(key));
        // Removed readers must still pin the block until ReleaseHotKey.
        EXPECT_EQ(cache.GetFreeBlock(), nullptr);
        cache.ReleaseHotKey(key);
        EXPECT_EQ(active->ref_count.load(), 0);
        EXPECT_EQ(cache.GetFreeBlock(), active);
    }
    stop.store(true);
    fill.join();

    active->key_.clear();
    EXPECT_FALSE(cache.PutHotKey(active));
    filling->key_ = "completed-fill";
    EXPECT_TRUE(cache.PutHotKey(filling));
    EXPECT_EQ(cache.GetHotKey("completed-fill"), filling);
    cache.ReleaseHotKey("completed-fill");
}

}  // namespace
}  // namespace mooncake
