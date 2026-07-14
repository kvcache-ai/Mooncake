#include "get_buffer_fixture.h"

namespace mooncake {
namespace testing {
TEST_F(DummyClientGetBufferTest, Perf_HotCacheVsFallback) {
    ASSERT_TRUE(SetupStack()) << "Failed to bring up real+dummy stack";

    const size_t payload_size = kPayloadSize;
    const int iterations = 20;
    const std::string hot_key = "perf_hot";
    const std::string cold_key = "perf_cold";
    const std::string data(payload_size, 'P');
    PutData(hot_key, data);
    PutData(cold_key, data);
    WarmHotCache(hot_key);

    // Benchmark: hot cache path
    auto t0 = std::chrono::steady_clock::now();
    for (int i = 0; i < iterations; ++i) {
        auto buf = dummy_client_->get_buffer(hot_key);
        ASSERT_NE(buf, nullptr);
    }
    auto t1 = std::chrono::steady_clock::now();
    double hot_ms =
        std::chrono::duration<double, std::milli>(t1 - t0).count() / iterations;

    // Benchmark: allocator fallback path
    auto t2 = std::chrono::steady_clock::now();
    for (int i = 0; i < iterations; ++i) {
        auto buf = dummy_client_->get_buffer(cold_key);
        ASSERT_NE(buf, nullptr);
    }
    auto t3 = std::chrono::steady_clock::now();
    double fallback_ms =
        std::chrono::duration<double, std::milli>(t3 - t2).count() / iterations;

    LOG(INFO) << "=== get_buffer latency (avg over " << iterations
              << " iterations, payload=" << payload_size / kMB << " MB) ===";
    LOG(INFO) << "  Hot cache path : " << std::fixed << std::setprecision(2)
              << hot_ms << " ms";
    LOG(INFO) << "  Fallback path  : " << std::fixed << std::setprecision(2)
              << fallback_ms << " ms";
    LOG(INFO) << "  Speedup        : " << std::fixed << std::setprecision(2)
              << fallback_ms / hot_ms << "x";

    // Hot cache should be faster (no data copy, no allocator round-trip)
    EXPECT_LT(hot_ms, fallback_ms)
        << "Hot cache path should be faster than fallback";
}

// ---- Perf: batch_get_buffer hot vs cold ----
TEST_F(DummyClientGetBufferTest, Perf_BatchHotVsCold) {
    ASSERT_TRUE(SetupStack()) << "Failed to bring up real+dummy stack";

    // Hot cache has 4 blocks of 1 GB; use 2 keys so both fit
    const size_t payload_size = kPayloadSize;
    const int iterations = 10;

    std::vector<std::string> hot_keys = {"batch_perf_hot_0",
                                         "batch_perf_hot_1"};
    std::vector<std::string> cold_keys = {"batch_perf_cold_0",
                                          "batch_perf_cold_1"};
    const std::string data(payload_size, 'B');

    for (auto &k : hot_keys) PutData(k, data);
    for (auto &k : cold_keys) PutData(k, data);
    for (auto &k : hot_keys) WarmHotCache(k);

    // Benchmark: batch hot cache path
    auto t0 = std::chrono::steady_clock::now();
    for (int i = 0; i < iterations; ++i) {
        auto results = dummy_client_->batch_get_buffer(hot_keys);
        for (auto &r : results) ASSERT_NE(r, nullptr);
    }
    auto t1 = std::chrono::steady_clock::now();
    double hot_ms =
        std::chrono::duration<double, std::milli>(t1 - t0).count() / iterations;

    // Benchmark: batch fallback path
    auto t2 = std::chrono::steady_clock::now();
    for (int i = 0; i < iterations; ++i) {
        auto results = dummy_client_->batch_get_buffer(cold_keys);
        for (auto &r : results) ASSERT_NE(r, nullptr);
    }
    auto t3 = std::chrono::steady_clock::now();
    double fallback_ms =
        std::chrono::duration<double, std::milli>(t3 - t2).count() / iterations;

    LOG(INFO) << "=== batch_get_buffer latency (avg over " << iterations
              << " iterations, 2 keys x " << payload_size / kMB << " MB) ===";
    LOG(INFO) << "  Hot cache path : " << std::fixed << std::setprecision(2)
              << hot_ms << " ms";
    LOG(INFO) << "  Fallback path  : " << std::fixed << std::setprecision(2)
              << fallback_ms << " ms";
    LOG(INFO) << "  Speedup        : " << std::fixed << std::setprecision(2)
              << fallback_ms / hot_ms << "x";

    EXPECT_LT(hot_ms, fallback_ms)
        << "Batch hot cache path should be faster than fallback";
}

}  // namespace testing
}  // namespace mooncake
