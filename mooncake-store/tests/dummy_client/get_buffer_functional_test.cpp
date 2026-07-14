#include "get_buffer_fixture.h"

namespace mooncake {
namespace testing {
TEST_F(DummyClientGetBufferTest, GetBuffer_AllocatorFallback) {
    ASSERT_TRUE(SetupStack()) << "Failed to bring up real+dummy stack";

    const std::string key = "fallback_key";
    const std::string data(kPayloadSize, 'F');
    PutData(key, data);

    // DummyClient get_buffer without warming hot cache → allocator fallback
    auto buf = dummy_client_->get_buffer(key);
    ASSERT_NE(buf, nullptr) << "get_buffer should succeed via fallback";
    EXPECT_EQ(buf->size(), data.size());
    EXPECT_FALSE(dummy_client_->is_hot_cache_ptr(buf->ptr()))
        << "Should NOT be in hot cache region (allocator fallback)";

    std::string got(static_cast<const char *>(buf->ptr()), buf->size());
    EXPECT_EQ(got, data) << "Data mismatch on allocator fallback path";
}

// ---- Test: get_buffer via hot cache shm zero-copy path ----
TEST_F(DummyClientGetBufferTest, GetBuffer_HotCachePath) {
    ASSERT_TRUE(SetupStack()) << "Failed to bring up real+dummy stack";

    const std::string key = "hot_key";
    const std::string data(kPayloadSize, 'H');
    PutData(key, data);

    // Warm hot cache: RealClient reads → async fill
    WarmHotCache(key);

    // DummyClient should hit hot cache path
    auto buf = dummy_client_->get_buffer(key);
    ASSERT_NE(buf, nullptr) << "get_buffer should succeed via hot cache";
    EXPECT_EQ(buf->size(), data.size());
    EXPECT_TRUE(dummy_client_->is_hot_cache_ptr(buf->ptr()))
        << "Should be in hot cache shm region (zero-copy path)";

    std::string got(static_cast<const char *>(buf->ptr()), buf->size());
    EXPECT_EQ(got, data) << "Data mismatch on hot cache path";
}

// ---- Test: batch_get_buffer mixed hot-cache + fallback ----
TEST_F(DummyClientGetBufferTest, BatchGetBuffer) {
    ASSERT_TRUE(SetupStack()) << "Failed to bring up real+dummy stack";

    const size_t num_keys = kBatchKeyCount;
    std::vector<std::string> keys;
    std::vector<std::string> payloads;
    keys.reserve(num_keys);
    payloads.reserve(num_keys);

    for (size_t i = 0; i < num_keys; ++i) {
        keys.push_back("batch_key_" + std::to_string(i));
        payloads.push_back(
            std::string(kPayloadSize, 'A' + static_cast<char>(i % 26)));
        PutData(keys.back(), payloads.back());
    }

    // Warm first kHotWarmCount keys into hot cache
    for (size_t i = 0; i < kHotWarmCount; ++i) {
        WarmHotCache(keys[i]);
    }

    // Batch get all keys via DummyClient
    auto results = dummy_client_->batch_get_buffer(keys);
    ASSERT_EQ(results.size(), num_keys);

    size_t hot_hits = 0;
    size_t fallback_hits = 0;
    for (size_t i = 0; i < num_keys; ++i) {
        ASSERT_NE(results[i], nullptr)
            << "batch_get_buffer returned nullptr for key " << keys[i];
        EXPECT_EQ(results[i]->size(), payloads[i].size());
        std::string got(static_cast<const char *>(results[i]->ptr()),
                        results[i]->size());
        EXPECT_EQ(got, payloads[i]) << "Data mismatch for key " << keys[i];

        if (dummy_client_->is_hot_cache_ptr(results[i]->ptr()))
            hot_hits++;
        else
            fallback_hits++;
    }
    // 4 blocks in hot cache, so at most 4 hot hits; rest go through fallback
    EXPECT_GT(hot_hits, 0) << "Should have at least one hot cache hit";
    EXPECT_GT(fallback_hits, 0) << "Should have at least one fallback hit";
}

// ---- Test: hot cache shm is properly mapped in DummyClient ----
TEST_F(DummyClientGetBufferTest, ShmMappingEstablished) {
    ASSERT_TRUE(SetupStack()) << "Failed to bring up real+dummy stack";

    // After setup_dummy, hot_cache_base_ should be non-null
    // We verify indirectly: put + warm + get via hot cache should work
    const std::string key = "shm_verify";
    const std::string data(kPayloadSize, 'S');
    PutData(key, data);
    WarmHotCache(key);

    auto buf = dummy_client_->get_buffer(key);
    ASSERT_NE(buf, nullptr);
    EXPECT_EQ(buf->size(), data.size());
    EXPECT_TRUE(dummy_client_->is_hot_cache_ptr(buf->ptr()))
        << "Warmed key should resolve via hot cache shm";

    // Verify shm mapping is valid via memcmp (byte-by-byte too slow at 500 MB)
    EXPECT_EQ(std::memcmp(buf->ptr(), data.data(), data.size()), 0)
        << "Shm content mismatch";
}

// ---- Perf: hot cache vs allocator fallback latency comparison ----
}  // namespace testing
}  // namespace mooncake
