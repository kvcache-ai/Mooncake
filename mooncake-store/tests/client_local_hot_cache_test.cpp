// client_local_hot_cache_test.cpp
#include "client.h"
#include "local_hot_cache.h"
#include "replica.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

namespace mooncake {
namespace testing {

class LocalHotCacheTest : public ::testing::Test {
   protected:
    void SetUp() override {
        google::InitGoogleLogging("LocalHotCacheTest");
        FLAGS_logtostderr = 1;
    }

    void TearDown() override {
        google::ShutdownGoogleLogging();
    }

    // Helper to create a slice with test data
    Slice CreateSlice(size_t size, char fill_char = 'A') {
        std::vector<char> data(size, fill_char);
        Slice slice;
        slice.ptr = data.data();
        slice.size = size;
        test_data_.push_back(std::move(data));  // Keep data alive
        return slice;
    }

    // Helper to verify slice data in cache
    void VerifySliceData(HotMemBlock* block, size_t expected_size, char expected_char) {
        ASSERT_NE(block, nullptr);
        ASSERT_EQ(block->size, expected_size);
        const char* data = static_cast<const char*>(block->addr);
        for (size_t i = 0; i < expected_size; ++i) {
            EXPECT_EQ(data[i], expected_char) << "Data mismatch at offset " << i;
        }
    }

   private:
    std::vector<std::vector<char>> test_data_;  // Keep test data alive
};

// Test LocalHotCache construction
TEST_F(LocalHotCacheTest, Construction) {
    const size_t cache_size = 64 * 1024 * 1024;  // 64MB = 4 blocks
    LocalHotCache cache(cache_size);
    
    EXPECT_GT(cache.GetCacheSize(), 0);
    EXPECT_EQ(cache.GetCacheSize(), 4);  // 64MB / 16MB = 4 blocks
}

// Test LocalHotCache with zero size
TEST_F(LocalHotCacheTest, ZeroSizeCache) {
    LocalHotCache cache(0);
    EXPECT_EQ(cache.GetCacheSize(), 0);
}

// Test PutHotSlice basic functionality
TEST_F(LocalHotCacheTest, PutHotSliceBasic) {
    const size_t cache_size = 32 * 1024 * 1024;  // 32MB = 2 blocks
    LocalHotCache cache(cache_size);
    
    const size_t slice_size = 1024;
    Slice slice = CreateSlice(slice_size, 'X');
    
    EXPECT_TRUE(cache.PutHotSlice("test_key_0", slice));
    EXPECT_TRUE(cache.HasHotSlice("test_key_0"));
    
    HotMemBlock* block = cache.GetHotSlice("test_key_0");
    VerifySliceData(block, slice_size, 'X');
}

// Test PutHotSlice with multiple keys
TEST_F(LocalHotCacheTest, PutHotSliceMultipleKeys) {
    const size_t cache_size = 32 * 1024 * 1024;  // 32MB = 2 blocks
    LocalHotCache cache(cache_size);
    
    // Put first key
    Slice slice1 = CreateSlice(1024, 'A');
    EXPECT_TRUE(cache.PutHotSlice("key1", slice1));
    
    // Put second key
    Slice slice2 = CreateSlice(2048, 'B');
    EXPECT_TRUE(cache.PutHotSlice("key2", slice2));
    
    EXPECT_TRUE(cache.HasHotSlice("key1"));
    EXPECT_TRUE(cache.HasHotSlice("key2"));
    
    HotMemBlock* block1 = cache.GetHotSlice("key1");
    VerifySliceData(block1, 1024, 'A');
    
    HotMemBlock* block2 = cache.GetHotSlice("key2");
    VerifySliceData(block2, 2048, 'B');
}

// Test PutHotSlice with existing key (touch LRU)
TEST_F(LocalHotCacheTest, PutHotSliceTouchExisting) {
    const size_t cache_size = 32 * 1024 * 1024;  // 32MB = 2 blocks
    LocalHotCache cache(cache_size);
    
    Slice slice = CreateSlice(1024, 'X');
    EXPECT_TRUE(cache.PutHotSlice("test_key", slice));
    
    // Put same key again (should just touch LRU, not copy data)
    Slice slice2 = CreateSlice(1024, 'Y');
    EXPECT_TRUE(cache.PutHotSlice("test_key", slice2));
    
    // Data should still be 'X' (not updated)
    HotMemBlock* block = cache.GetHotSlice("test_key");
    VerifySliceData(block, 1024, 'X');
}

// Test PutHotSlice with invalid parameters
TEST_F(LocalHotCacheTest, PutHotSliceInvalidParams) {
    const size_t cache_size = 32 * 1024 * 1024;  // 32MB = 2 blocks
    LocalHotCache cache(cache_size);
    
    // Test with null pointer
    Slice null_slice;
    null_slice.ptr = nullptr;
    null_slice.size = 1024;
    EXPECT_FALSE(cache.PutHotSlice("key", null_slice));
    
    // Test with zero size
    Slice zero_slice;
    zero_slice.ptr = malloc(1024);
    zero_slice.size = 0;
    EXPECT_FALSE(cache.PutHotSlice("key", zero_slice));
    free(zero_slice.ptr);
    
    // Test with size larger than block size
    Slice large_slice;
    large_slice.ptr = malloc(17 * 1024 * 1024);  // 17MB > 16MB
    large_slice.size = 17 * 1024 * 1024;
    EXPECT_FALSE(cache.PutHotSlice("key", large_slice));
    free(large_slice.ptr);
}

// Test LRU eviction behavior
TEST_F(LocalHotCacheTest, LRUEviction) {
    const size_t cache_size = 32 * 1024 * 1024;  // 32MB = 2 blocks
    LocalHotCache cache(cache_size);
    
    // Fill cache with 2 blocks
    Slice slice1 = CreateSlice(1024, 'A');
    EXPECT_TRUE(cache.PutHotSlice("key1", slice1));
    
    Slice slice2 = CreateSlice(1024, 'B');
    EXPECT_TRUE(cache.PutHotSlice("key2", slice2));
    
    // Add third key, should evict key1 (LRU)
    Slice slice3 = CreateSlice(1024, 'C');
    EXPECT_TRUE(cache.PutHotSlice("key3", slice3));
    
    EXPECT_FALSE(cache.HasHotSlice("key1"));  // Should be evicted
    EXPECT_TRUE(cache.HasHotSlice("key2"));
    EXPECT_TRUE(cache.HasHotSlice("key3"));
}

// Test GetHotSlice updates LRU
TEST_F(LocalHotCacheTest, GetHotSliceUpdatesLRU) {
    const size_t cache_size = 32 * 1024 * 1024;  // 32MB = 2 blocks
    LocalHotCache cache(cache_size);
    
    Slice slice1 = CreateSlice(1024, 'A');
    cache.PutHotSlice("key1", slice1);
    
    Slice slice2 = CreateSlice(1024, 'B');
    cache.PutHotSlice("key2", slice2);
    
    // Access key1, should move it to front
    HotMemBlock* block1 = cache.GetHotSlice("key1");
    ASSERT_NE(block1, nullptr);
    
    // Add third key, should evict key2 (not key1, since key1 was accessed)
    Slice slice3 = CreateSlice(1024, 'C');
    cache.PutHotSlice("key3", slice3);
    
    EXPECT_TRUE(cache.HasHotSlice("key1"));  // Should still be there
    EXPECT_FALSE(cache.HasHotSlice("key2"));  // Should be evicted
    EXPECT_TRUE(cache.HasHotSlice("key3"));
}

// Test GetHotSlice with non-existent key
TEST_F(LocalHotCacheTest, GetHotSliceMiss) {
    const size_t cache_size = 32 * 1024 * 1024;  // 32MB = 2 blocks
    LocalHotCache cache(cache_size);
    
    HotMemBlock* block = cache.GetHotSlice("non_existent_key");
    EXPECT_EQ(block, nullptr);
}

// Test LocalHotCacheHandler basic functionality
TEST_F(LocalHotCacheTest, LocalHotCacheHandlerBasic) {
    const size_t cache_size = 32 * 1024 * 1024;  // 32MB = 2 blocks
    auto cache = std::make_shared<LocalHotCache>(cache_size);
    LocalHotCacheHandler handler(cache, 2);
    
    const size_t slice_size = 1024;
    std::vector<char> data(slice_size, 'Z');
    Slice slice;
    slice.ptr = data.data();
    slice.size = slice_size;
    
    // Submit task
    EXPECT_TRUE(handler.SubmitPutTask("async_key", slice));
    
    // Wait a bit for async processing
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    
    // Verify data was cached
    EXPECT_TRUE(cache->HasHotSlice("async_key"));
    HotMemBlock* block = cache->GetHotSlice("async_key");
    VerifySliceData(block, slice_size, 'Z');
}

// Test LocalHotCacheHandler with null cache
TEST_F(LocalHotCacheTest, LocalHotCacheHandlerNullCache) {
    LocalHotCacheHandler handler(nullptr, 2);
    
    std::vector<char> data(1024, 'X');
    Slice slice;
    slice.ptr = data.data();
    slice.size = 1024;
    
    // Should return false since hot_cache_ is null
    EXPECT_FALSE(handler.SubmitPutTask("key", slice));
}

// Test concurrent access to LocalHotCache
TEST_F(LocalHotCacheTest, ConcurrentAccess) {
    const size_t cache_size = 128 * 1024 * 1024;  // 128MB = 8 blocks
    LocalHotCache cache(cache_size);
    
    const int num_threads = 4;
    const int keys_per_thread = 2;  // Total: 8 keys, cache has 8 blocks
    std::vector<std::thread> threads;
    std::atomic<int> successful_puts(0);
    std::atomic<int> successful_gets(0);
    
    // Each thread puts and gets keys
    for (int t = 0; t < num_threads; ++t) {
        threads.emplace_back([&cache, t, keys_per_thread, &successful_puts, &successful_gets]() {
            for (int i = 0; i < keys_per_thread; ++i) {
                std::string key = "thread_" + std::to_string(t) + "_key_" + std::to_string(i);
                std::vector<char> data(1024, static_cast<char>('A' + t));
                Slice slice;
                slice.ptr = data.data();
                slice.size = 1024;
                
                // Put the key
                EXPECT_TRUE(cache.PutHotSlice(key, slice));
                successful_puts++;
                
                // Get it back - should succeed since cache has enough capacity
                HotMemBlock* block = cache.GetHotSlice(key);
                EXPECT_NE(block, nullptr) << "Block should not be nullptr with sufficient cache capacity";
                successful_gets++;
                
                // Verify data integrity
                const char* cached_data = static_cast<const char*>(block->addr);
                EXPECT_EQ(cached_data[0], static_cast<char>('A' + t));
            }
        });
    }
    
    // Wait for all threads
    for (auto& thread : threads) {
        thread.join();
    }
    
    // All puts and gets should succeed with sufficient capacity
    EXPECT_EQ(successful_puts.load(), num_threads * keys_per_thread);
    EXPECT_EQ(successful_gets.load(), num_threads * keys_per_thread);
}


/**
 * Note: The following private functions are tested indirectly through public APIs:
 * - Client::updateReplicaDescriptorFromCache: Tested via Client::Get and Client::BatchGet
 * - Client::ProcessSlicesAsync: Tested via Client::Get and Client::BatchGet  
 * - Client::InitLocalHotCache: Tested via Client::Create with LOCAL_HOT_CACHE_SIZE env var
 */

/** 
 * Test InitLocalHotCache via Client::Create and IsHotCacheEnabled
 * Note: InitLocalHotCache is a private function called by Client::Create
 * Important: Client::Create requires a master server connection, so these tests may fail
 * if master server is not available. 
 */
TEST_F(LocalHotCacheTest, InitLocalHotCacheViaClientCreate) {
    // Save original env var
    const char* original_env = std::getenv("LOCAL_HOT_CACHE_SIZE");
    
    // Test 1: Invalid environment variable - InitLocalHotCache returns INVALID_PARAMS
    // but Client::Create still succeeds (just logs error), hot cache should be disabled
    {
        setenv("LOCAL_HOT_CACHE_SIZE", "invalid", 1);
        
        auto client_opt = Client::Create(
            "test_hostname",
            "http://localhost:8080/metadata",
            "tcp",
            std::nullopt,
            "localhost:50051"
        );
        
        // Client::Create may fail due to master server, but if it succeeds,
        if (client_opt.has_value()) {
            EXPECT_FALSE(client_opt.value()->IsHotCacheEnabled());
            EXPECT_EQ(client_opt.value()->GetLocalHotCacheBlockCount(), 0);
        }
    }
    
    // Test 2: Zero value - InitLocalHotCache returns INVALID_PARAMS
    {
        setenv("LOCAL_HOT_CACHE_SIZE", "0", 1);
        
        auto client_opt = Client::Create(
            "test_hostname",
            "http://localhost:8080/metadata",
            "tcp",
            std::nullopt,
            "localhost:50051"
        );
        
        // If client creation succeeded, hot cache should be disabled
        if (client_opt.has_value()) {
            EXPECT_FALSE(client_opt.value()->IsHotCacheEnabled());
            EXPECT_EQ(client_opt.value()->GetLocalHotCacheBlockCount(), 0);
        }
    }
    
    // Test 3: Size less than 1 block (16MB) - hot cache should not be enabled
    {
        setenv("LOCAL_HOT_CACHE_SIZE", "8388608", 1);  // 8MB < 16MB (1 block)
        
        auto client_opt = Client::Create(
            "test_hostname",
            "http://localhost:8080/metadata",
            "tcp",
            std::nullopt,
            "localhost:50051"
        );
        
        // If client creation succeeded, hot cache should be disabled
        // (because 8MB < 16MB results in 0 blocks, causing InitLocalHotCache to reset hot_cache_)
        if (client_opt.has_value()) {
            EXPECT_FALSE(client_opt.value()->IsHotCacheEnabled());
            EXPECT_EQ(client_opt.value()->GetLocalHotCacheBlockCount(), 0);
        }
    }
    
    // Test 4: Valid environment variable - hot cache should be enabled
    {
        setenv("LOCAL_HOT_CACHE_SIZE", "33554432", 1);  // 32MB = 2 blocks (16MB each)
        
        auto client_opt = Client::Create(
            "test_hostname",
            "http://localhost:8080/metadata",
            "tcp",
            std::nullopt,
            "localhost:50051"
        );
        
        // If client creation succeeded, InitLocalHotCache should have succeeded
        // and hot cache should be enabled
        if (client_opt.has_value()) {
            EXPECT_TRUE(client_opt.value()->IsHotCacheEnabled());
            EXPECT_EQ(client_opt.value()->GetLocalHotCacheBlockCount(), 2);
        }
    }
    
    // Test 5: No environment variable - hot cache should be disabled (valid case)
    {
        unsetenv("LOCAL_HOT_CACHE_SIZE");
        
        auto client_opt = Client::Create(
            "test_hostname",
            "http://localhost:8080/metadata",
            "tcp",
            std::nullopt,
            "localhost:50051"
        );
        
        // If client creation succeeded, InitLocalHotCache should have succeeded
        // and hot cache should be disabled (no env var means disabled)
        if (client_opt.has_value()) {
            EXPECT_FALSE(client_opt.value()->IsHotCacheEnabled());
            EXPECT_EQ(client_opt.value()->GetLocalHotCacheBlockCount(), 0);
        }
        // Note: Client::Create may fail due to master server, but InitLocalHotCache should
        // have been called and returned OK (hot cache disabled is valid)
    }
    
    // Restore original env var
    if (original_env) {
        setenv("LOCAL_HOT_CACHE_SIZE", original_env, 1);
    } else {
        unsetenv("LOCAL_HOT_CACHE_SIZE");
    }
}

/**
 * Test updateReplicaDescriptorFromCache and ProcessSlicesAsync indirectly
 * through Client::Get and Client::BatchGet
 * 
 * Note: These tests require a running master server.
 * 
 * IMPORTANT: With a single client, data is stored locally, so:
 * - ProcessSlicesAsync won't cache local transfers (transport_endpoint_ == local_hostname_)
 * - updateReplicaDescriptorFromCache won't find cached data (nothing was cached)
 * - we need multiple clients (one Put, another Get from non-local) to test cache hit scenarios
 * 
 * These tests verify the functions work correctly when hot cache is disabled,
 * but cannot fully test cache hit scenarios with a single client setup.
 */
TEST_F(LocalHotCacheTest, GetWithHotCacheEnabledButLocalTransfer) {
    // Save original env var
    const char* original_env = std::getenv("LOCAL_HOT_CACHE_SIZE");
    
    // Enable hot cache with sufficient size
    setenv("LOCAL_HOT_CACHE_SIZE", "33554432", 1);  // 32MB = 2 blocks
    
    auto client_opt = Client::Create(
        "test_hostname",
        "http://localhost:8080/metadata",
        "tcp",
        std::nullopt,
        "localhost:50051"
    );
    
    // Skip test if master server is not available
    if (!client_opt.has_value()) {
        GTEST_SKIP() << "Master server not available, skipping integration test";
    }
    
    auto client = client_opt.value();
    
    // Verify hot cache is enabled
    ASSERT_TRUE(client->IsHotCacheEnabled());
    ASSERT_GT(client->GetLocalHotCacheBlockCount(), 0);
    
    const std::string test_key = "test_hot_cache_key";
    const std::string test_data = "Test data for hot cache";
    
    // Put data first
    ReplicateConfig config;
    config.replica_num = 1;
    
    // Allocate buffer for Put
    std::vector<char> put_buffer(test_data.size());
    std::memcpy(put_buffer.data(), test_data.data(), test_data.size());
    std::vector<Slice> put_slices;
    put_slices.emplace_back(Slice{put_buffer.data(), test_data.size()});
    
    auto put_result = client->Put(test_key, put_slices, config);
    if (!put_result.has_value()) {
        GTEST_SKIP() << "Put operation failed, skipping Get test";
    }
    
    std::vector<char> get_buffer(test_data.size());
    std::vector<Slice> get_slices;
    get_slices.emplace_back(Slice{get_buffer.data(), test_data.size()});
    
    auto get_result = client->Get(test_key, get_slices);
    ASSERT_TRUE(get_result.has_value()) << "Get should succeed";
    
    // Verify data integrity
    ASSERT_GE(get_slices.size(), 1) << "At least one slice should exist";
    ASSERT_GE(get_slices[0].size, test_data.size()) << "Slice size should be at least expected data size";
    EXPECT_EQ(std::memcmp(get_slices[0].ptr, test_data.data(), test_data.size()), 0)
        << "Retrieved data should match original data";
    
    // Restore original env var
    if (original_env) {
        setenv("LOCAL_HOT_CACHE_SIZE", original_env, 1);
    } else {
        unsetenv("LOCAL_HOT_CACHE_SIZE");
    }
}

TEST_F(LocalHotCacheTest, BatchGetWithHotCacheEnabledButLocalTransfer) {
    // Save original env var
    const char* original_env = std::getenv("LOCAL_HOT_CACHE_SIZE");
    
    // Enable hot cache
    setenv("LOCAL_HOT_CACHE_SIZE", "33554432", 1);  // 32MB = 2 blocks
    
    auto client_opt = Client::Create(
        "test_hostname",
        "http://localhost:8080/metadata",
        "tcp",
        std::nullopt,
        "localhost:50051"
    );
    
    // Skip test if master server is not available
    if (!client_opt.has_value()) {
        GTEST_SKIP() << "Master server not available, skipping integration test";
    }
    
    auto client = client_opt.value();
    
    // Verify hot cache is enabled
    ASSERT_TRUE(client->IsHotCacheEnabled());
    
    const std::vector<std::string> test_keys = {"batch_key1", "batch_key2"};
    const std::vector<std::string> test_data = {"Data1", "Data2"};
    
    // Put data first
    ReplicateConfig config;
    config.replica_num = 1;
    
    for (size_t i = 0; i < test_keys.size(); ++i) {
        std::vector<char> put_buffer(test_data[i].size());
        std::memcpy(put_buffer.data(), test_data[i].data(), test_data[i].size());
        std::vector<Slice> put_slices;
        put_slices.emplace_back(Slice{put_buffer.data(), test_data[i].size()});
        
        auto put_result = client->Put(test_keys[i], put_slices, config);
        if (!put_result.has_value()) {
            GTEST_SKIP() << "Put operation failed, skipping BatchGet test";
        }
    }
    
    std::unordered_map<std::string, std::vector<Slice>> batch_slices;
    for (const auto& key : test_keys) {
        std::vector<char> buffer(1024);
        batch_slices[key].emplace_back(Slice{buffer.data(), 1024});
    }
    
    // Use BatchGet without query_results parameter (it will query internally)
    auto batch_get_result = client->BatchGet(test_keys, batch_slices);
    ASSERT_EQ(batch_get_result.size(), test_keys.size());
    for (const auto& result : batch_get_result) {
        ASSERT_TRUE(result.has_value()) << "BatchGet should succeed";
    }
    
    // Verify data integrity for each key
    for (size_t i = 0; i < test_keys.size(); ++i) {
        const auto& key = test_keys[i];
        const auto& expected_data = test_data[i];
        
        auto slices_it = batch_slices.find(key);
        ASSERT_NE(slices_it, batch_slices.end()) << "Slices should exist for key: " << key;
        ASSERT_GE(slices_it->second.size(), 1) << "At least one slice should exist for key: " << key;
        
        // Verify the first slice contains the expected data
        const auto& slice = slices_it->second[0];
        ASSERT_GE(slice.size, expected_data.size()) << "Slice size should be at least expected data size";
        EXPECT_EQ(std::memcmp(slice.ptr, expected_data.data(), expected_data.size()), 0)
            << "Data should match for key: " << key;
    }
    
    // Restore original env var
    if (original_env) {
        setenv("LOCAL_HOT_CACHE_SIZE", original_env, 1);
    } else {
        unsetenv("LOCAL_HOT_CACHE_SIZE");
    }
}

}  // namespace testing
}  // namespace mooncake

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

