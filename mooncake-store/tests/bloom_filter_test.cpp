// Bloom Filter Unit Tests
// Verifies zero false negatives and acceptable false positive rate

#include <gtest/gtest.h>
#include <glog/logging.h>
#include <string>
#include <vector>
#include <random>

#include "bloom_filter.h"

namespace mooncake {

class BloomFilterTest : public ::testing::Test {
   protected:
    void SetUp() override {
        // google::InitGoogleLogging called by gtest_main
    }
};

// Zero false negatives: every added key must be found
TEST_F(BloomFilterTest, ZeroFalseNegatives) {
    BloomFilter filter(1 << 20, 3);  // 1M bits, 3 hashes

    std::vector<std::string> keys;
    for (int i = 0; i < 10000; i++) {
        keys.push_back("key_" + std::to_string(i));
        filter.Add(keys.back());
    }

    for (const auto& key : keys) {
        ASSERT_TRUE(filter.MayContain(key))
            << "False negative for key: " << key;
    }
}

// Non-existing keys should mostly return false
TEST_F(BloomFilterTest, FalsePositiveRate) {
    BloomFilter filter(1 << 20, 3);  // 1M bits, 3 hashes

    // Add 10K keys
    for (int i = 0; i < 10000; i++) {
        filter.Add("existing_" + std::to_string(i));
    }

    // Test 100K non-existing keys
    int false_positives = 0;
    for (int i = 0; i < 100000; i++) {
        if (filter.MayContain("nonexist_" + std::to_string(i))) {
            false_positives++;
        }
    }

    double fpr = static_cast<double>(false_positives) / 100000.0;
    // With 1M bits, 3 hashes, 10K keys: theoretical FPR < 1%
    ASSERT_LT(fpr, 0.05) << "False positive rate too high: " << fpr;
    LOG(INFO) << "False positive rate: " << (fpr * 100) << "%"
              << " (" << false_positives << "/100000)";
}

// Empty filter should return false for everything
TEST_F(BloomFilterTest, EmptyFilter) {
    BloomFilter filter(1 << 16, 3);

    ASSERT_FALSE(filter.MayContain("anything"));
    ASSERT_FALSE(filter.MayContain(""));
    ASSERT_FALSE(filter.MayContain("key_12345"));
}

// Reset should clear all bits
TEST_F(BloomFilterTest, Reset) {
    BloomFilter filter(1 << 16, 3);

    filter.Add("key1");
    filter.Add("key2");
    ASSERT_TRUE(filter.MayContain("key1"));
    ASSERT_TRUE(filter.MayContain("key2"));

    filter.Reset();

    ASSERT_FALSE(filter.MayContain("key1"));
    ASSERT_FALSE(filter.MayContain("key2"));
}

// Thread safety: concurrent Add and MayContain should not crash
TEST_F(BloomFilterTest, ConcurrentAccess) {
    BloomFilter filter(1 << 20, 3);
    constexpr int kNumThreads = 8;
    constexpr int kOpsPerThread = 10000;

    std::vector<std::thread> threads;
    std::atomic<int> false_negatives{0};

    // Half threads add, half threads query
    for (int t = 0; t < kNumThreads; t++) {
        threads.emplace_back([&, t]() {
            for (int i = 0; i < kOpsPerThread; i++) {
                std::string key = "t" + std::to_string(t) + "_k" + std::to_string(i);
                filter.Add(key);
                // Immediately check: must find it (no false negative)
                if (!filter.MayContain(key)) {
                    false_negatives.fetch_add(1);
                }
            }
        });
    }

    for (auto& t : threads) t.join();

    ASSERT_EQ(false_negatives.load(), 0)
        << "Concurrent access produced false negatives";
}

// KVCache key format compatibility
TEST_F(BloomFilterTest, KVCacheKeyFormat) {
    BloomFilter filter(1 << 20, 3);

    // Typical KVCache keys
    std::vector<std::string> keys = {
        "prefix_sha256_abc123def456",
        "model_llama3_layer_0_head_0_tokens_0_512",
        "system_prompt_v2_hash_7f3c2a",
        "user_session_12345_turn_3_prefix",
        "",  // empty key
        std::string(1000, 'x'),  // very long key
    };

    for (const auto& key : keys) {
        filter.Add(key);
    }

    for (const auto& key : keys) {
        ASSERT_TRUE(filter.MayContain(key))
            << "False negative for key: " << key;
    }
}

}  // namespace mooncake
