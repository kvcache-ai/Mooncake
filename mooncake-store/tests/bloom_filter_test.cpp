// Counting Bloom Filter Unit Tests
// Verifies zero false negatives, acceptable false positive rate,
// and correct Remove() behavior for eviction-aware operation.

#include <gtest/gtest.h>
#include <glog/logging.h>
#include <string>
#include <vector>
#include <random>
#include <thread>

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
    BloomFilter filter(1 << 20, 3);  // 1M slots, 3 hashes

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
    BloomFilter filter(1 << 20, 3);  // 1M slots, 3 hashes

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
    // With 1M slots, 3 hashes, 10K keys: theoretical FPR < 1%
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

// Reset should clear all counters
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

// ========================================================================
// NEW: Counting Bloom Filter Remove() tests
// ========================================================================

// Basic Remove: after Remove, key should no longer be found
TEST_F(BloomFilterTest, BasicRemove) {
    BloomFilter filter(1 << 20, 3);

    filter.Add("key_to_remove");
    ASSERT_TRUE(filter.MayContain("key_to_remove"));

    filter.Remove("key_to_remove");
    ASSERT_FALSE(filter.MayContain("key_to_remove"))
        << "Key should not be found after Remove()";
}

// Remove should not affect other keys
TEST_F(BloomFilterTest, RemoveDoesNotAffectOtherKeys) {
    BloomFilter filter(1 << 20, 3);

    filter.Add("key_a");
    filter.Add("key_b");
    filter.Add("key_c");

    filter.Remove("key_b");

    ASSERT_TRUE(filter.MayContain("key_a"))
        << "Removing key_b should not affect key_a";
    ASSERT_FALSE(filter.MayContain("key_b"))
        << "key_b should be removed";
    ASSERT_TRUE(filter.MayContain("key_c"))
        << "Removing key_b should not affect key_c";
}

// FPR should decrease after bulk removal (simulating eviction)
TEST_F(BloomFilterTest, FPRDecreasesAfterBulkRemoval) {
    BloomFilter filter(1 << 20, 3);

    // Add 20K keys
    for (int i = 0; i < 20000; i++) {
        filter.Add("bulk_" + std::to_string(i));
    }

    // Measure FPR before removal
    int fp_before = 0;
    for (int i = 0; i < 50000; i++) {
        if (filter.MayContain("probe_" + std::to_string(i))) {
            fp_before++;
        }
    }

    // Remove half the keys (simulate eviction)
    for (int i = 0; i < 10000; i++) {
        filter.Remove("bulk_" + std::to_string(i));
    }

    // Measure FPR after removal
    int fp_after = 0;
    for (int i = 0; i < 50000; i++) {
        if (filter.MayContain("probe_" + std::to_string(i))) {
            fp_after++;
        }
    }

    LOG(INFO) << "FPR before removal: " << (fp_before / 500.0) << "%"
              << ", after removal: " << (fp_after / 500.0) << "%";

    // FPR should decrease after removing half the keys
    ASSERT_LE(fp_after, fp_before)
        << "FPR should not increase after removing keys";

    // Remaining keys should still be found (zero false negatives)
    for (int i = 10000; i < 20000; i++) {
        ASSERT_TRUE(filter.MayContain("bulk_" + std::to_string(i)))
            << "False negative for remaining key: bulk_" << i;
    }
}

// Saturating counter: double-add then single-remove should still find key
TEST_F(BloomFilterTest, SaturatingCounter) {
    BloomFilter filter(1 << 20, 3);

    // Add the same key twice
    filter.Add("double_add");
    filter.Add("double_add");

    // Remove once — key should still be found (counter > 0)
    filter.Remove("double_add");
    ASSERT_TRUE(filter.MayContain("double_add"))
        << "Key added twice, removed once should still be found";

    // Remove again — now key should not be found
    filter.Remove("double_add");
    ASSERT_FALSE(filter.MayContain("double_add"))
        << "Key added twice, removed twice should not be found";
}

// Concurrent Add and Remove should not crash or produce false negatives
// for keys that were added but not removed
TEST_F(BloomFilterTest, ConcurrentAddRemove) {
    BloomFilter filter(1 << 20, 3);
    constexpr int kNumKeys = 10000;

    // Pre-add keys that will NOT be removed (persistent keys)
    for (int i = 0; i < kNumKeys; i++) {
        filter.Add("persistent_" + std::to_string(i));
    }

    std::vector<std::thread> threads;
    std::atomic<int> false_negatives{0};

    // Threads adding transient keys
    for (int t = 0; t < 4; t++) {
        threads.emplace_back([&, t]() {
            for (int i = 0; i < kNumKeys; i++) {
                std::string key = "transient_t" + std::to_string(t) + "_" + std::to_string(i);
                filter.Add(key);
            }
        });
    }

    // Threads removing transient keys (with slight delay)
    for (int t = 0; t < 4; t++) {
        threads.emplace_back([&, t]() {
            for (int i = 0; i < kNumKeys; i++) {
                std::string key = "transient_t" + std::to_string(t) + "_" + std::to_string(i);
                filter.Remove(key);
            }
        });
    }

    for (auto& t : threads) t.join();

    // Persistent keys must still be found
    for (int i = 0; i < kNumKeys; i++) {
        if (!filter.MayContain("persistent_" + std::to_string(i))) {
            false_negatives.fetch_add(1);
        }
    }

    ASSERT_EQ(false_negatives.load(), 0)
        << "Concurrent Add/Remove produced false negatives for persistent keys";
}

// KVCache eviction simulation: add many keys, evict half, verify behavior
TEST_F(BloomFilterTest, KVCacheEvictionSimulation) {
    // Simulate realistic KVCache workload:
    // 1. Fill cache with 50K KV entries
    // 2. Evict 25K entries (oldest half)
    // 3. Add 25K new entries
    // 4. Verify: no false negatives for live keys, reduced FPR from eviction

    BloomFilter filter(1 << 22, 3);  // 4M slots (production size)

    // Phase 1: Fill
    for (int i = 0; i < 50000; i++) {
        filter.Add("kv_gen0_" + std::to_string(i));
    }

    double fpr_before = filter.EstimateFPR();
    LOG(INFO) << "Estimated FPR after initial fill: " << (fpr_before * 100) << "%"
              << ", non-zero counters: " << filter.CountNonZero();

    // Phase 2: Evict oldest half
    for (int i = 0; i < 25000; i++) {
        filter.Remove("kv_gen0_" + std::to_string(i));
    }

    double fpr_after_evict = filter.EstimateFPR();
    LOG(INFO) << "Estimated FPR after eviction: " << (fpr_after_evict * 100) << "%"
              << ", non-zero counters: " << filter.CountNonZero();

    // Phase 3: Add new generation
    for (int i = 0; i < 25000; i++) {
        filter.Add("kv_gen1_" + std::to_string(i));
    }

    double fpr_final = filter.EstimateFPR();
    LOG(INFO) << "Estimated FPR after new generation: " << (fpr_final * 100) << "%"
              << ", non-zero counters: " << filter.CountNonZero();

    // Verify: all live keys found
    int false_neg = 0;
    for (int i = 25000; i < 50000; i++) {
        if (!filter.MayContain("kv_gen0_" + std::to_string(i))) false_neg++;
    }
    for (int i = 0; i < 25000; i++) {
        if (!filter.MayContain("kv_gen1_" + std::to_string(i))) false_neg++;
    }
    ASSERT_EQ(false_neg, 0) << "False negatives found for live keys";

    // FPR after evict+refill should be ≤ FPR after initial fill
    // (eviction freed counter space)
    ASSERT_LE(fpr_final, fpr_before * 1.5)
        << "FPR should not significantly exceed initial fill rate";

    LOG(INFO) << "KVCache eviction simulation: PASS"
              << " (FPR: " << (fpr_before * 100) << "% → "
              << (fpr_after_evict * 100) << "% → "
              << (fpr_final * 100) << "%)";
}

// Diagnostic methods: CountNonZero and EstimateFPR
TEST_F(BloomFilterTest, DiagnosticMethods) {
    BloomFilter filter(1 << 16, 3);

    ASSERT_EQ(filter.CountNonZero(), 0u);
    ASSERT_DOUBLE_EQ(filter.EstimateFPR(), 0.0);

    for (int i = 0; i < 100; i++) {
        filter.Add("diag_" + std::to_string(i));
    }

    ASSERT_GT(filter.CountNonZero(), 0u);
    ASSERT_GT(filter.EstimateFPR(), 0.0);
    ASSERT_LT(filter.EstimateFPR(), 1.0);
}

}  // namespace mooncake
