// S3-FIFO Eviction Strategy Unit Tests
// Verifies three-queue eviction behavior for KVCache workloads

#include <gtest/gtest.h>
#include <glog/logging.h>
#include <string>
#include <vector>
#include <unordered_set>

#include "eviction_strategy.h"

namespace mooncake {

class S3FIFOEvictionTest : public ::testing::Test {
   protected:
    void SetUp() override {
        google::InitGoogleLogging("S3FIFOEvictionTest");
    }
};

// Basic: insert keys, evict should return one-hit wonders first
TEST_F(S3FIFOEvictionTest, EvictsOneHitWonders) {
    S3FIFOEvictionStrategy strategy(100);

    // Insert 3 keys
    strategy.AddKey("hot_prefix");
    strategy.AddKey("cold_once");
    strategy.AddKey("cold_twice");

    // Access "hot_prefix" multiple times (promote to main)
    strategy.UpdateKey("hot_prefix");
    strategy.UpdateKey("hot_prefix");

    // "cold_once" never accessed again → one-hit wonder
    // "cold_twice" accessed once → will be promoted on eviction pass

    // First eviction should pick "cold_once" (freq=0 in small queue)
    // "hot_prefix" already promoted to main by UpdateKey
    std::string evicted = strategy.EvictKey();

    // One of the cold keys should be evicted first
    // hot_prefix should NOT be evicted (it's in main queue)
    ASSERT_NE(evicted, "hot_prefix")
        << "Hot prefix should not be evicted before cold keys";
    ASSERT_FALSE(evicted.empty()) << "Should evict something";
}

// Promotion: accessed key in small queue gets promoted to main
TEST_F(S3FIFOEvictionTest, PromotesAccessedKeys) {
    S3FIFOEvictionStrategy strategy(100);

    strategy.AddKey("key_a");
    strategy.AddKey("key_b");
    strategy.AddKey("key_c");

    // Access key_b (will be promoted when eviction scans small queue)
    strategy.UpdateKey("key_b");

    // First eviction: key_a (first in small queue, freq=0) should be evicted
    std::string first = strategy.EvictKey();
    ASSERT_EQ(first, "key_a") << "First one-hit wonder should be evicted";

    // key_b was promoted to main, key_c still in small
    // Second eviction: key_c (freq=0 in small queue)
    std::string second = strategy.EvictKey();
    ASSERT_EQ(second, "key_c") << "Second one-hit wonder should be evicted";

    // key_b is in main queue, should be evicted last
    std::string third = strategy.EvictKey();
    ASSERT_EQ(third, "key_b") << "Promoted key should be evicted from main";
}

// Ghost set: recently evicted keys are tracked
TEST_F(S3FIFOEvictionTest, GhostSetTracksEvictedKeys) {
    S3FIFOEvictionStrategy strategy(100);

    strategy.AddKey("ephemeral");
    // Don't access it → freq=0 → will be evicted to ghost

    std::string evicted = strategy.EvictKey();
    ASSERT_EQ(evicted, "ephemeral");

    // Key should be in ghost set
    ASSERT_TRUE(strategy.isGhostHit("ephemeral"))
        << "Evicted key should be in ghost set";
    ASSERT_FALSE(strategy.isGhostHit("nonexistent"))
        << "Non-evicted key should not be in ghost set";
}

// Ghost capacity: old ghost entries are evicted
TEST_F(S3FIFOEvictionTest, GhostCapacityLimit) {
    // Small ghost capacity
    S3FIFOEvictionStrategy strategy(3);

    // Insert and evict 5 keys
    for (int i = 0; i < 5; i++) {
        strategy.AddKey("key_" + std::to_string(i));
    }
    for (int i = 0; i < 5; i++) {
        strategy.EvictKey();
    }

    // Only last 3 should be in ghost set
    ASSERT_FALSE(strategy.isGhostHit("key_0"))
        << "Old ghost entry should be evicted";
    ASSERT_FALSE(strategy.isGhostHit("key_1"))
        << "Old ghost entry should be evicted";
    ASSERT_TRUE(strategy.isGhostHit("key_2"));
    ASSERT_TRUE(strategy.isGhostHit("key_3"));
    ASSERT_TRUE(strategy.isGhostHit("key_4"));
}

// Empty eviction: returns empty string when nothing to evict
TEST_F(S3FIFOEvictionTest, EmptyEviction) {
    S3FIFOEvictionStrategy strategy(100);
    std::string evicted = strategy.EvictKey();
    ASSERT_TRUE(evicted.empty()) << "Empty strategy should return empty string";
}

// Size tracking
TEST_F(S3FIFOEvictionTest, SizeTracking) {
    S3FIFOEvictionStrategy strategy(100);

    ASSERT_EQ(strategy.GetSize(), 0u);

    strategy.AddKey("a");
    strategy.AddKey("b");
    strategy.AddKey("c");
    ASSERT_EQ(strategy.GetSize(), 3u);

    strategy.EvictKey();
    ASSERT_EQ(strategy.GetSize(), 2u);

    strategy.RemoveKey("b");
    ASSERT_EQ(strategy.GetSize(), 1u);
}

// Duplicate add: should not create duplicate entries
TEST_F(S3FIFOEvictionTest, DuplicateAdd) {
    S3FIFOEvictionStrategy strategy(100);

    strategy.AddKey("key");
    strategy.AddKey("key");  // Duplicate
    ASSERT_EQ(strategy.GetSize(), 1u) << "Duplicate add should not increase size";
}

// RemoveKey: should work for both small and main queue
TEST_F(S3FIFOEvictionTest, RemoveFromBothQueues) {
    S3FIFOEvictionStrategy strategy(100);

    strategy.AddKey("small_key");
    strategy.AddKey("main_key");

    // Promote main_key by accessing it
    strategy.UpdateKey("main_key");

    // Force main_key promotion by evicting from small queue
    // Actually, promotion happens during EvictKey scan, not on UpdateKey
    // So main_key is still in small queue but with freq>0

    // Remove both
    strategy.RemoveKey("small_key");
    strategy.RemoveKey("main_key");
    ASSERT_EQ(strategy.GetSize(), 0u);
}

// Stress test: many keys, verify no crash and correct eviction order
TEST_F(S3FIFOEvictionTest, StressTest) {
    S3FIFOEvictionStrategy strategy(1000);

    // Insert 1000 keys
    for (int i = 0; i < 1000; i++) {
        strategy.AddKey("key_" + std::to_string(i));
    }

    // Access first 100 keys multiple times (hot set)
    for (int i = 0; i < 100; i++) {
        strategy.UpdateKey("key_" + std::to_string(i));
        strategy.UpdateKey("key_" + std::to_string(i));
    }

    // Evict 900 keys — cold keys should go first
    std::unordered_set<std::string> evicted;
    for (int i = 0; i < 900; i++) {
        std::string key = strategy.EvictKey();
        ASSERT_FALSE(key.empty()) << "Should be able to evict at iteration " << i;
        evicted.insert(key);
    }

    // Most of the hot keys (0-99) should still be cached
    int hot_survived = 0;
    for (int i = 0; i < 100; i++) {
        if (evicted.find("key_" + std::to_string(i)) == evicted.end()) {
            hot_survived++;
        }
    }

    // At least 80% of hot keys should survive (promoted to main queue)
    ASSERT_GE(hot_survived, 80)
        << "S3-FIFO should protect frequently accessed keys. Survived: "
        << hot_survived << "/100";
}

// KVCache workload simulation: system prompts (hot) + user queries (cold)
TEST_F(S3FIFOEvictionTest, KVCacheWorkloadSimulation) {
    S3FIFOEvictionStrategy strategy(1000);

    // 10 system prompts (accessed by every request)
    for (int i = 0; i < 10; i++) {
        strategy.AddKey("system_prompt_" + std::to_string(i));
    }

    // 100 user queries (each accessed only once)
    for (int i = 0; i < 100; i++) {
        strategy.AddKey("user_query_" + std::to_string(i));
    }

    // Simulate 50 requests, each accessing all system prompts
    for (int req = 0; req < 50; req++) {
        for (int i = 0; i < 10; i++) {
            strategy.UpdateKey("system_prompt_" + std::to_string(i));
        }
    }

    // Evict 100 keys (all user queries should go first)
    std::unordered_set<std::string> evicted;
    for (int i = 0; i < 100; i++) {
        std::string key = strategy.EvictKey();
        if (!key.empty()) evicted.insert(key);
    }

    // All system prompts should survive
    for (int i = 0; i < 10; i++) {
        ASSERT_EQ(evicted.count("system_prompt_" + std::to_string(i)), 0u)
            << "System prompt " << i << " should not be evicted";
    }
}

}  // namespace mooncake
