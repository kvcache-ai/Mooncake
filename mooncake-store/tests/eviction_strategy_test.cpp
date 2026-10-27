// eviction_strategy_test.cpp
#include <glog/logging.h>
#include <gtest/gtest.h>

#include <memory>

#include "eviction_strategy.h"

namespace mooncake {

// Test fixture for EvictionStrategy tests
class EvictionStrategyTest : public ::testing::Test {
   protected:
    void SetUp() override {
        // Initialize glog for logging
        google::InitGoogleLogging("EvictionStrategyTest");
        FLAGS_logtostderr = 1;  // Output logs to stderr
    }

    void TearDown() override {
        // Cleanup glog
        google::ShutdownGoogleLogging();
    }
};

// Test LRUEvictionStrategy AddKey and RemoveKey functionality
TEST_F(EvictionStrategyTest, AddAndRemoveKey) {
    LRUEvictionStrategy eviction_strategy;

    // Add keys
    EXPECT_EQ(eviction_strategy.AddKey("key1"), ErrorCode::OK);
    EXPECT_EQ(eviction_strategy.AddKey("key2"), ErrorCode::OK);

    // Check size
    EXPECT_EQ(eviction_strategy.GetSize(), 2);

    // Remove a key
    EXPECT_EQ(eviction_strategy.RemoveKey("key1"), ErrorCode::OK);
    EXPECT_EQ(eviction_strategy.GetSize(), 1);

    // Clean up
    eviction_strategy.CleanUp();
}

// Test LRUEvictionStrategy EvictKey functionality
TEST_F(EvictionStrategyTest, EvictKey) {
    LRUEvictionStrategy eviction_strategy;

    // Add keys
    EXPECT_EQ(eviction_strategy.AddKey("key1"), ErrorCode::OK);
    EXPECT_EQ(eviction_strategy.AddKey("key2"), ErrorCode::OK);

    // Evict a key
    std::string evicted_key = eviction_strategy.EvictKey();
    EXPECT_NE(evicted_key, "");

    // Check if the evicted key is equal to the expected key
    EXPECT_EQ(evicted_key, "key1");

    // Now we add more keys and update some of them
    EXPECT_EQ(eviction_strategy.AddKey("key3"), ErrorCode::OK);
    EXPECT_EQ(eviction_strategy.AddKey("key4"), ErrorCode::OK);
    EXPECT_EQ(eviction_strategy.UpdateKey("key2"), ErrorCode::OK);
    EXPECT_EQ(eviction_strategy.UpdateKey("key3"), ErrorCode::OK);

    // Evict another key
    evicted_key = eviction_strategy.EvictKey();
    EXPECT_NE(evicted_key, "");
    // Check if the evicted key is equal to the expected key
    EXPECT_EQ(evicted_key, "key4");
    // Check size
    EXPECT_EQ(eviction_strategy.GetSize(), 2);
    // Clean up
    eviction_strategy.CleanUp();
}

// Test FIFOEvictionStrategy AddKey and RemoveKey functionality
TEST_F(EvictionStrategyTest, FIFOAddAndRemoveKey) {
    FIFOEvictionStrategy eviction_strategy;

    // Add keys
    EXPECT_EQ(eviction_strategy.AddKey("key1"), ErrorCode::OK);
    EXPECT_EQ(eviction_strategy.AddKey("key2"), ErrorCode::OK);

    // Check size
    EXPECT_EQ(eviction_strategy.GetSize(), 2);

    // Remove a key
    EXPECT_EQ(
        eviction_strategy.RemoveKey("key1"),
        ErrorCode::OK);  // FIFO not support remove a randomly accessed key
    EXPECT_EQ(eviction_strategy.GetSize(), 2);

    // Clean up
    eviction_strategy.CleanUp();
}

// Test FIFOEvictionStrategy EvictKey functionality
TEST_F(EvictionStrategyTest, FIFOEvictKey) {
    FIFOEvictionStrategy eviction_strategy;

    // Add keys
    EXPECT_EQ(eviction_strategy.AddKey("key1"), ErrorCode::OK);
    EXPECT_EQ(eviction_strategy.AddKey("key2"), ErrorCode::OK);

    // Evict a key
    std::string evicted_key = eviction_strategy.EvictKey();
    EXPECT_NE(evicted_key, "");

    // Check if the evicted key is equal to the expected key
    EXPECT_EQ(evicted_key, "key1");

    // Now we add more keys and update some of them
    EXPECT_EQ(eviction_strategy.AddKey("key3"), ErrorCode::OK);
    EXPECT_EQ(eviction_strategy.AddKey("key4"), ErrorCode::OK);
    EXPECT_EQ(eviction_strategy.UpdateKey("key2"), ErrorCode::OK);
    EXPECT_EQ(eviction_strategy.UpdateKey("key3"), ErrorCode::OK);

    // Evict another key
    evicted_key = eviction_strategy.EvictKey();
    EXPECT_NE(evicted_key, "");
    // Check if the evicted key is equal to the expected key
    EXPECT_EQ(evicted_key, "key2");
    // Check size
    EXPECT_EQ(eviction_strategy.GetSize(), 2);
    // Clean up
    eviction_strategy.CleanUp();
}

}  // namespace mooncake

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
