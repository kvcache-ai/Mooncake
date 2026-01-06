// client_local_hot_cache_test.cpp
#include "client_service.h"
#include "local_hot_cache.h"
#include "replica.h"
#include "test_server_helpers.h"
#include "utils.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <optional>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

// Network headers for getting local IP (Linux/Unix)
#include <ifaddrs.h>
#include <netdb.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <arpa/inet.h>

namespace mooncake {
namespace testing {

// Helper function to get local IP address
// Priority: 1) Environment variable MC_TEST_LOCAL_IP, 2) System call to get
// first non-loopback IP
std::string getLocalIpAddress() {
    // Try to get IP from environment variable first (for testing flexibility)
    const char* env_ip = std::getenv("MC_TEST_LOCAL_IP");
    if (env_ip && strlen(env_ip) > 0) {
        return std::string(env_ip);
    }

    // Fallback: get local IP using system calls
    // Get the first non-loopback IPv4 address
    struct ifaddrs *ifaddr, *ifa;
    std::string ip = "127.0.0.1";  // Default fallback

    if (getifaddrs(&ifaddr) == -1) {
        return ip;
    }

    for (ifa = ifaddr; ifa != nullptr; ifa = ifa->ifa_next) {
        if (ifa->ifa_addr == nullptr) {
            continue;
        }

        // Look for IPv4 address
        if (ifa->ifa_addr->sa_family == AF_INET) {
            // Skip loopback interface
            if (strcmp(ifa->ifa_name, "lo") == 0) {
                continue;
            }

            // Check if interface is UP
            if (!(ifa->ifa_flags & IFF_UP)) {
                continue;
            }

            struct sockaddr_in* sin = (struct sockaddr_in*)ifa->ifa_addr;
            char host[NI_MAXHOST];
            if (getnameinfo((struct sockaddr*)sin, sizeof(struct sockaddr_in),
                            host, NI_MAXHOST, nullptr, 0,
                            NI_NUMERICHOST) == 0) {
                ip = std::string(host);
                break;  // Use the first non-loopback interface
            }
        }
    }

    freeifaddrs(ifaddr);
    return ip;
}

class LocalHotCacheTest : public ::testing::Test {
   protected:
    static void SetUpTestSuite() {
        google::InitGoogleLogging("LocalHotCacheTest");
        FLAGS_logtostderr = 1;

        // Start in-proc master and metadata servers (non-HA)
        ASSERT_TRUE(master_.Start(InProcMasterConfigBuilder().build()))
            << "Failed to start in-proc master";
        master_address_ = master_.master_address();
        metadata_url_ = master_.metadata_url();
        LOG(INFO) << "Started in-proc master at " << master_address_
                  << ", metadata="
                  << (metadata_url_.empty() ? "disabled" : metadata_url_);
    }

    static void TearDownTestSuite() {
        master_.Stop();
        google::ShutdownGoogleLogging();
    }

    void SetUp() override {}

    void TearDown() override {}

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
    void VerifySliceData(HotMemBlock* block, size_t expected_size,
                         char expected_char) {
        ASSERT_NE(block, nullptr);
        ASSERT_EQ(block->size, expected_size);
        const char* data = static_cast<const char*>(block->addr);
        for (size_t i = 0; i < expected_size; ++i) {
            EXPECT_EQ(data[i], expected_char)
                << "Data mismatch at offset " << i;
        }
    }

    // Helper to setup client with hot cache enabled and mount segment
    struct TestClientContext {
        std::shared_ptr<Client> client;
        void* segment_ptr;
        size_t segment_size;
        const char* original_env;
    };

    // Helper to create client with common parameters
    std::optional<std::shared_ptr<Client>> CreateTestClient(
        const std::string& hostname) {
        return Client::Create(hostname,
                              "P2PHANDSHAKE",  // use in-proc metadata server
                              "tcp", std::nullopt,
                              master_address_);  // master server address
    }

    // Shared in-proc master for tests
    static mooncake::testing::InProcMaster master_;
    static std::string master_address_;
    static std::string metadata_url_;

    TestClientContext SetupTestClientWithHotCache() {
        TestClientContext ctx;
        ctx.original_env = std::getenv("LOCAL_HOT_CACHE_SIZE");
        setenv("LOCAL_HOT_CACHE_SIZE", "33554432", 1);  // 32MB = 2 blocks

        std::string local_ip = getLocalIpAddress();
        std::string local_hostname = local_ip + ":12345";

        auto client_opt = CreateTestClient(local_hostname);
        if (!client_opt.has_value()) {
            return ctx;  // Return empty context
        }

        ctx.client = client_opt.value();
        if (!ctx.client->IsHotCacheEnabled()) {
            return ctx;  // Return context with client but hot cache not enabled
        }

        ctx.segment_size = 64 * 1024 * 1024;  // 64MB
        ctx.segment_ptr = allocate_buffer_allocator_memory(ctx.segment_size);
        if (ctx.segment_ptr == nullptr) {
            return ctx;  // Return context without segment
        }

        auto mount_result =
            ctx.client->MountSegment(ctx.segment_ptr, ctx.segment_size);
        if (!mount_result.has_value()) {
            free_memory("", ctx.segment_ptr);
            ctx.segment_ptr = nullptr;
            return ctx;  // Return context with unmounted segment
        }

        return ctx;
    }

    void CleanupTestClient(TestClientContext& ctx) {
        if (ctx.client && ctx.segment_ptr) {
            ctx.client->UnmountSegment(ctx.segment_ptr, ctx.segment_size);
            free_memory("", ctx.segment_ptr);
        } else if (ctx.segment_ptr) {
            free_memory("", ctx.segment_ptr);
        }
        if (ctx.original_env) {
            setenv("LOCAL_HOT_CACHE_SIZE", ctx.original_env, 1);
        } else {
            unsetenv("LOCAL_HOT_CACHE_SIZE");
        }
    }

    void PutTestData(Client* client, const std::string& key,
                     const std::string& data) {
        ReplicateConfig config;
        config.replica_num = 1;
        std::vector<char> put_buffer(data.size());
        std::memcpy(put_buffer.data(), data.data(), data.size());
        std::vector<Slice> put_slices;
        put_slices.emplace_back(Slice{put_buffer.data(), data.size()});
        auto put_result = client->Put(key, put_slices, config);
        // If Put fails, the test will fail when trying to Get the data
    }

   private:
    std::vector<std::vector<char>> test_data_;  // Keep test data alive
};

// Static member definitions
mooncake::testing::InProcMaster LocalHotCacheTest::master_;
std::string LocalHotCacheTest::master_address_;
std::string LocalHotCacheTest::metadata_url_;

// Test LocalHotCache construction
TEST_F(LocalHotCacheTest, Construction) {
    const size_t cache_size =
        64 * 1024 * 1024;  // 64MB = 4 blocks (default 16MB each)
    LocalHotCache cache(cache_size);

    EXPECT_GT(cache.GetCacheSize(), 0);
    EXPECT_EQ(cache.GetCacheSize(), 4);  // 64MB / 16MB = 4 blocks
    EXPECT_EQ(cache.GetBlockSize(), 16 * 1024 * 1024);  // Default 16MB
}

// Test LocalHotCache construction with custom block size
TEST_F(LocalHotCacheTest, ConstructionWithCustomBlockSize) {
    const size_t block_size = 8 * 1024 * 1024;   // 8MB
    const size_t cache_size = 32 * 1024 * 1024;  // 32MB = 4 blocks (8MB each)
    LocalHotCache cache(cache_size, block_size);

    EXPECT_GT(cache.GetCacheSize(), 0);
    EXPECT_EQ(cache.GetCacheSize(), 4);  // 32MB / 8MB = 4 blocks
    EXPECT_EQ(cache.GetBlockSize(), block_size);
}

// Test LocalHotCache with zero size
TEST_F(LocalHotCacheTest, ZeroSizeCache) {
    LocalHotCache cache(0);
    EXPECT_EQ(cache.GetCacheSize(), 0);
}

// Test PutHotSlice basic functionality
TEST_F(LocalHotCacheTest, PutHotSliceBasic) {
    const size_t cache_size =
        32 * 1024 * 1024;  // 32MB = 2 blocks (default 16MB each)
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
    const size_t cache_size =
        32 * 1024 * 1024;  // 32MB = 2 blocks (default 16MB each)
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
    large_slice.ptr =
        malloc(17 * 1024 * 1024);  // 17MB > 16MB (default block size)
    large_slice.size = 17 * 1024 * 1024;
    EXPECT_FALSE(cache.PutHotSlice("key", large_slice));
    free(large_slice.ptr);
}

// Test PutHotSlice with invalid parameters (custom block size)
TEST_F(LocalHotCacheTest, PutHotSliceInvalidParamsWithCustomBlockSize) {
    const size_t block_size = 4 * 1024 * 1024;  // 4MB
    const size_t cache_size = 8 * 1024 * 1024;  // 8MB = 2 blocks (4MB each)
    LocalHotCache cache(cache_size, block_size);

    // Test with size larger than custom block size
    Slice large_slice;
    large_slice.ptr = malloc(5 * 1024 * 1024);  // 5MB > 4MB (custom block size)
    large_slice.size = 5 * 1024 * 1024;
    EXPECT_FALSE(cache.PutHotSlice("key", large_slice));
    free(large_slice.ptr);
}

// Test LRU eviction behavior
TEST_F(LocalHotCacheTest, LRUEviction) {
    const size_t cache_size =
        32 * 1024 * 1024;  // 32MB = 2 blocks (default 16MB each)
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
    const size_t cache_size =
        32 * 1024 * 1024;  // 32MB = 2 blocks (default 16MB each)
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

    EXPECT_TRUE(cache.HasHotSlice("key1"));   // Should still be there
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
        threads.emplace_back([&cache, t, keys_per_thread, &successful_puts,
                              &successful_gets]() {
            for (int i = 0; i < keys_per_thread; ++i) {
                std::string key =
                    "thread_" + std::to_string(t) + "_key_" + std::to_string(i);
                std::vector<char> data(1024, static_cast<char>('A' + t));
                Slice slice;
                slice.ptr = data.data();
                slice.size = 1024;

                // Put the key
                EXPECT_TRUE(cache.PutHotSlice(key, slice));
                successful_puts++;

                // Get it back - should succeed since cache has enough capacity
                HotMemBlock* block = cache.GetHotSlice(key);
                EXPECT_NE(block, nullptr) << "Block should not be nullptr with "
                                             "sufficient cache capacity";
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
 * Test InitLocalHotCache via Client::Create and IsHotCacheEnabled
 * Note: InitLocalHotCache is a private function called by Client::Create
 */

// Test 1: Valid environment variable - hot cache should be enabled
TEST_F(LocalHotCacheTest, InitLocalHotCacheViaClientCreate_ValidSize) {
    // Save original env var
    const char* original_env = std::getenv("LOCAL_HOT_CACHE_SIZE");

    setenv("LOCAL_HOT_CACHE_SIZE", "33554432",
           1);  // 32MB = 2 blocks (16MB each)

    auto client_opt = CreateTestClient("localhost");
    if (client_opt.has_value()) {
        EXPECT_TRUE(client_opt.value()->IsHotCacheEnabled());
        EXPECT_EQ(client_opt.value()->GetLocalHotCacheBlockCount(), 2);
    }

    // Restore original env var
    if (original_env) {
        setenv("LOCAL_HOT_CACHE_SIZE", original_env, 1);
    } else {
        unsetenv("LOCAL_HOT_CACHE_SIZE");
    }
}

// Test 2: Invalid environment variable - InitLocalHotCache returns
// INVALID_PARAMS
TEST_F(LocalHotCacheTest, InitLocalHotCacheViaClientCreate_InvalidEnvVar) {
    // Save original env var
    const char* original_env = std::getenv("LOCAL_HOT_CACHE_SIZE");

    setenv("LOCAL_HOT_CACHE_SIZE", "invalid", 1);

    auto client_opt = CreateTestClient("localhost");
    if (client_opt.has_value()) {
        EXPECT_FALSE(client_opt.value()->IsHotCacheEnabled());
        EXPECT_EQ(client_opt.value()->GetLocalHotCacheBlockCount(), 0);
    }

    // Restore original env var
    if (original_env) {
        setenv("LOCAL_HOT_CACHE_SIZE", original_env, 1);
    } else {
        unsetenv("LOCAL_HOT_CACHE_SIZE");
    }
}

// Test 3: Zero value - InitLocalHotCache returns INVALID_PARAMS
TEST_F(LocalHotCacheTest, InitLocalHotCacheViaClientCreate_ZeroSize) {
    // Save original env var
    const char* original_env = std::getenv("LOCAL_HOT_CACHE_SIZE");

    setenv("LOCAL_HOT_CACHE_SIZE", "0", 1);

    auto client_opt = CreateTestClient("localhost");
    if (client_opt.has_value()) {
        EXPECT_FALSE(client_opt.value()->IsHotCacheEnabled());
        EXPECT_EQ(client_opt.value()->GetLocalHotCacheBlockCount(), 0);
    }

    // Restore original env var
    if (original_env) {
        setenv("LOCAL_HOT_CACHE_SIZE", original_env, 1);
    } else {
        unsetenv("LOCAL_HOT_CACHE_SIZE");
    }
}

// Test 4: Negative value - InitLocalHotCache returns INVALID_PARAMS
TEST_F(LocalHotCacheTest, InitLocalHotCacheViaClientCreate_NegativeSize) {
    // Save original env var
    const char* original_env = std::getenv("LOCAL_HOT_CACHE_SIZE");

    setenv("LOCAL_HOT_CACHE_SIZE", "-1", 1);

    auto client_opt = CreateTestClient("localhost");
    if (client_opt.has_value()) {
        EXPECT_FALSE(client_opt.value()->IsHotCacheEnabled());
        EXPECT_EQ(client_opt.value()->GetLocalHotCacheBlockCount(), 0);
    }

    if (original_env) {
        setenv("LOCAL_HOT_CACHE_SIZE", original_env, 1);
    } else {
        unsetenv("LOCAL_HOT_CACHE_SIZE");
    }
}

// Test 5: Size less than 1 block (default 16MB) - hot cache should not be
// enabled
TEST_F(LocalHotCacheTest, InitLocalHotCacheViaClientCreate_LessThanOneBlock) {
    // Save original env vars
    const char* original_cache_size = std::getenv("LOCAL_HOT_CACHE_SIZE");

    setenv("LOCAL_HOT_CACHE_SIZE", "8388608",
           1);  // 8MB < 16MB (default block size)

    auto client_opt = CreateTestClient("localhost");
    // If client creation succeeded, hot cache should be disabled
    // (because 8MB < 16MB results in 0 blocks, causing InitLocalHotCache to
    // reset hot_cache_)
    if (client_opt.has_value()) {
        EXPECT_FALSE(client_opt.value()->IsHotCacheEnabled());
        EXPECT_EQ(client_opt.value()->GetLocalHotCacheBlockCount(), 0);
    }

    // Restore original env vars
    if (original_cache_size) {
        setenv("LOCAL_HOT_CACHE_SIZE", original_cache_size, 1);
    } else {
        unsetenv("LOCAL_HOT_CACHE_SIZE");
    }
}

// Test 6: Size less than 1 block (custom 4MB) - hot cache should not be enabled
TEST_F(LocalHotCacheTest,
       InitLocalHotCacheViaClientCreate_LessThanOneBlock_CustomBlockSize) {
    // Save original env vars
    const char* original_cache_size = std::getenv("LOCAL_HOT_CACHE_SIZE");
    const char* original_block_size = std::getenv("LOCAL_HOT_BLOCK_SIZE");

    setenv("LOCAL_HOT_BLOCK_SIZE", "4194304", 1);  // 4MB custom block size
    setenv("LOCAL_HOT_CACHE_SIZE", "2097152",
           1);  // 2MB < 4MB (custom block size)

    auto client_opt = CreateTestClient("localhost");
    // If client creation succeeded, hot cache should be disabled
    // (because 2MB < 4MB results in 0 blocks, causing InitLocalHotCache to
    // reset hot_cache_)
    if (client_opt.has_value()) {
        EXPECT_FALSE(client_opt.value()->IsHotCacheEnabled());
        EXPECT_EQ(client_opt.value()->GetLocalHotCacheBlockCount(), 0);
    }

    // Restore original env vars
    if (original_cache_size) {
        setenv("LOCAL_HOT_CACHE_SIZE", original_cache_size, 1);
    } else {
        unsetenv("LOCAL_HOT_CACHE_SIZE");
    }
    if (original_block_size) {
        setenv("LOCAL_HOT_BLOCK_SIZE", original_block_size, 1);
    } else {
        unsetenv("LOCAL_HOT_BLOCK_SIZE");
    }
}

// Test 7: No environment variable - hot cache should be disabled (valid case)
TEST_F(LocalHotCacheTest, InitLocalHotCacheViaClientCreate_NoEnvVar) {
    // Save original env var
    const char* original_env = std::getenv("LOCAL_HOT_CACHE_SIZE");

    unsetenv("LOCAL_HOT_CACHE_SIZE");

    auto client_opt = CreateTestClient("localhost");
    if (client_opt.has_value()) {
        EXPECT_FALSE(client_opt.value()->IsHotCacheEnabled());
        EXPECT_EQ(client_opt.value()->GetLocalHotCacheBlockCount(), 0);
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
 * IMPORTANT: With a single client, data is stored locally, so:
 * - ProcessSlicesAsync won't cache local transfers (transport_endpoint_ ==
 * local_hostname_)
 * - updateReplicaDescriptorFromCache won't find cached data (nothing was
 * cached)
 * - we need multiple clients (one Put, another Get from non-local) to test
 * cache hit scenarios
 *
 * These tests verify the functions work correctly when hot cache is disabled,
 * but cannot fully test cache hit scenarios with a single client setup.
 */
TEST_F(LocalHotCacheTest, GetWithHotCacheEnabled) {
    // Save original env vars
    const char* original_cache_size = std::getenv("LOCAL_HOT_CACHE_SIZE");
    const char* original_block_size = std::getenv("LOCAL_HOT_BLOCK_SIZE");

    // Enable hot cache with custom block size (4MB)
    setenv("LOCAL_HOT_BLOCK_SIZE", "4194304", 1);  // 4MB
    setenv("LOCAL_HOT_CACHE_SIZE", "8388608", 1);  // 8MB = 2 blocks (4MB each)

    // Get local IP address instead of using "localhost" to avoid hostname
    // resolution issues
    std::string local_ip = getLocalIpAddress();
    std::string local_hostname =
        local_ip + ":12345";  // Use a fixed port for testing

    auto client_opt = Client::Create(
        local_hostname,
        "redis://localhost:6379",  // Redis metadata server on port 6379
        "tcp", std::nullopt,
        "localhost:50051"  // Master service on port 50051
    );

    // Skip test if master server is not available
    if (!client_opt.has_value()) {
        GTEST_SKIP()
            << "Master server not available, skipping integration test";
    }

    auto client = client_opt.value();

    // Verify hot cache is enabled
    ASSERT_TRUE(client->IsHotCacheEnabled());
    ASSERT_GT(client->GetLocalHotCacheBlockCount(), 0);

    // Mount a segment to provide storage space
    size_t segment_size = 64 * 1024 * 1024;  // 64MB
    void* segment_ptr = allocate_buffer_allocator_memory(segment_size);
    ASSERT_NE(segment_ptr, nullptr) << "Failed to allocate segment memory";

    auto mount_result = client->MountSegment(segment_ptr, segment_size);
    if (!mount_result.has_value()) {
        free_memory("", segment_ptr);
        GTEST_SKIP() << "Failed to mount segment: "
                     << toString(mount_result.error());
    }

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
        client->UnmountSegment(segment_ptr, segment_size);
        free_memory("", segment_ptr);
        GTEST_SKIP() << "Put operation failed, skipping Get test";
    }

    std::vector<char> get_buffer(test_data.size());
    std::vector<Slice> get_slices;
    get_slices.emplace_back(Slice{get_buffer.data(), test_data.size()});

    auto get_result = client->Get(test_key, get_slices);
    ASSERT_TRUE(get_result.has_value()) << "Get should succeed";

    // Verify data integrity
    ASSERT_GE(get_slices.size(), 1) << "At least one slice should exist";
    ASSERT_GE(get_slices[0].size, test_data.size())
        << "Slice size should be at least expected data size";
    EXPECT_EQ(
        std::memcmp(get_slices[0].ptr, test_data.data(), test_data.size()), 0)
        << "Retrieved data should match original data";

    // Cleanup: unmount segment and free memory
    client->UnmountSegment(segment_ptr, segment_size);
    free_memory("", segment_ptr);

    // Restore original env vars
    if (original_cache_size) {
        setenv("LOCAL_HOT_CACHE_SIZE", original_cache_size, 1);
    } else {
        unsetenv("LOCAL_HOT_CACHE_SIZE");
    }
    if (original_block_size) {
        setenv("LOCAL_HOT_BLOCK_SIZE", original_block_size, 1);
    } else {
        unsetenv("LOCAL_HOT_BLOCK_SIZE");
    }
}

TEST_F(LocalHotCacheTest, BatchGetWithHotCacheEnabled) {
    // Save original env vars
    const char* original_cache_size = std::getenv("LOCAL_HOT_CACHE_SIZE");
    const char* original_block_size = std::getenv("LOCAL_HOT_BLOCK_SIZE");

    // Enable hot cache with custom block size (4MB)
    setenv("LOCAL_HOT_BLOCK_SIZE", "4194304", 1);  // 4MB
    setenv("LOCAL_HOT_CACHE_SIZE", "8388608", 1);  // 8MB = 2 blocks (4MB each)

    // Get local IP address instead of using "localhost" to avoid hostname
    // resolution issues
    std::string local_ip = getLocalIpAddress();
    std::string local_hostname =
        local_ip + ":12345";  // Use a fixed port for testing

    auto client_opt = Client::Create(
        local_hostname,
        "redis://localhost:6379",  // Redis metadata server on port 6379
        "tcp", std::nullopt,
        "localhost:50051"  // Master service on port 50051
    );

    // Skip test if master server is not available
    if (!client_opt.has_value()) {
        GTEST_SKIP()
            << "Master server not available, skipping integration test";
    }

    auto client = client_opt.value();

    // Verify hot cache is enabled
    ASSERT_TRUE(client->IsHotCacheEnabled());

    // Mount a segment to provide storage space
    size_t segment_size = 64 * 1024 * 1024;  // 64MB
    void* segment_ptr = allocate_buffer_allocator_memory(segment_size);
    ASSERT_NE(segment_ptr, nullptr) << "Failed to allocate segment memory";

    auto mount_result = client->MountSegment(segment_ptr, segment_size);
    if (!mount_result.has_value()) {
        free_memory("", segment_ptr);
        GTEST_SKIP() << "Failed to mount segment: "
                     << toString(mount_result.error());
    }

    const std::vector<std::string> test_keys = {"batch_key1", "batch_key2"};
    const std::vector<std::string> test_data = {"Data1", "Data2"};

    // Put data first
    ReplicateConfig config;
    config.replica_num = 1;

    for (size_t i = 0; i < test_keys.size(); ++i) {
        std::vector<char> put_buffer(test_data[i].size());
        std::memcpy(put_buffer.data(), test_data[i].data(),
                    test_data[i].size());
        std::vector<Slice> put_slices;
        put_slices.emplace_back(Slice{put_buffer.data(), test_data[i].size()});

        auto put_result = client->Put(test_keys[i], put_slices, config);
        if (!put_result.has_value()) {
            client->UnmountSegment(segment_ptr, segment_size);
            free_memory("", segment_ptr);
            GTEST_SKIP() << "Put operation failed, skipping BatchGet test";
        }
    }

    std::unordered_map<std::string, std::vector<Slice>> batch_slices;
    std::vector<std::vector<char>> batch_buffers;  // Keep buffers alive
    for (size_t i = 0; i < test_keys.size(); ++i) {
        const auto& key = test_keys[i];
        const auto& expected_data = test_data[i];
        // Allocate buffer with the same size as the data we put
        batch_buffers.emplace_back(expected_data.size());
        batch_slices[key].emplace_back(
            Slice{batch_buffers.back().data(), expected_data.size()});
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
        ASSERT_NE(slices_it, batch_slices.end())
            << "Slices should exist for key: " << key;
        ASSERT_GE(slices_it->second.size(), 1)
            << "At least one slice should exist for key: " << key;

        // Verify the first slice contains the expected data
        const auto& slice = slices_it->second[0];
        ASSERT_GE(slice.size, expected_data.size())
            << "Slice size should be at least expected data size";
        EXPECT_EQ(
            std::memcmp(slice.ptr, expected_data.data(), expected_data.size()),
            0)
            << "Data should match for key: " << key;
    }

    // Cleanup: unmount segment and free memory
    client->UnmountSegment(segment_ptr, segment_size);
    free_memory("", segment_ptr);

    // Restore original env vars
    if (original_cache_size) {
        setenv("LOCAL_HOT_CACHE_SIZE", original_cache_size, 1);
    } else {
        unsetenv("LOCAL_HOT_CACHE_SIZE");
    }
    if (original_block_size) {
        setenv("LOCAL_HOT_BLOCK_SIZE", original_block_size, 1);
    } else {
        unsetenv("LOCAL_HOT_BLOCK_SIZE");
    }
}

}  // namespace testing
}  // namespace mooncake

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
