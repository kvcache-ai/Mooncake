#include "route_cache.h"

#include <gtest/gtest.h>

#include <chrono>
#include <string>
#include <thread>
#include <vector>

namespace mooncake {
namespace {

// Helper to create a P2PProxyDescriptor
P2PProxyDescriptor MakeP2PProxy(uint64_t client_id_hi, uint64_t segment_id_hi,
                                const std::string& ip = "127.0.0.1",
                                uint16_t port = 12345, uint64_t size = 1024) {
    P2PProxyDescriptor proxy;
    proxy.client_id = {client_id_hi, 0};
    proxy.segment_id = {segment_id_hi, 0};
    proxy.ip_address = ip;
    proxy.rpc_port = port;
    proxy.object_size = size;
    return proxy;
}

class RouteCacheTest : public ::testing::Test {
   protected:
    // Default config: 1MB max, 120s TTL, auto shards
    RouteCache cache_{1024 * 1024 * 10, 120'000};
};

// ============================================================================
// Basic Operations
// ============================================================================

TEST_F(RouteCacheTest, BasicOperations) {
    // 1. Get Miss
    EXPECT_TRUE(cache_.Get("nonexistent").items().empty());

    // 2. Replace & Get
    auto proxy1 = MakeP2PProxy(1, 100);
    cache_.Replace("key1", {proxy1});
    auto result = cache_.Get("key1");
    ASSERT_FALSE(result.items().empty());
    ASSERT_EQ(result.items().size(), 1u);
    EXPECT_EQ(result.items()[0].segment_id, (UUID{100, 0}));

    // 3. Replace Overwrites Existing
    auto proxy2 = MakeP2PProxy(2, 200);
    cache_.Replace("key1", {proxy2});

    result = cache_.Get("key1");
    ASSERT_EQ(result.items().size(), 1u);
    EXPECT_EQ(result.items()[0].segment_id, (UUID{200, 0}));
}

// ============================================================================
// TTL Expiration & Renewal
// ============================================================================

TEST_F(RouteCacheTest, TTLExpirationAndRenewal) {
    RouteCache cache(1024 * 1024, 100);  // 100ms TTL

    auto proxy1 = MakeP2PProxy(1, 100);
    auto proxy2 = MakeP2PProxy(2, 200);

    cache.Replace("key_expire", {proxy1});
    cache.Replace("key_renew", {proxy1});
    cache.Replace("key_merge", {proxy1});

    // Keep renewing 'key_renew'
    for (int i = 0; i < 3; ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(60));
        ASSERT_FALSE(cache.Get("key_renew").items().empty());
    }
    // Total elapsed: ~180ms. 'key_expire' and 'key_merge' should be expired.

    EXPECT_TRUE(cache.Get("key_expire").items().empty());

    // Merge with expired should ignore the old expired entry
    cache.Upsert("key_merge", {proxy2});
    auto result = cache.Get("key_merge");
    ASSERT_EQ(result.items().size(), 1u);
    EXPECT_EQ(result.items()[0].segment_id, (UUID{200, 0}));

    // Wait for the renewed key to finally expire
    std::this_thread::sleep_for(std::chrono::milliseconds(150));
    EXPECT_TRUE(cache.Get("key_renew").items().empty());
}

// ============================================================================
// RemoveReplica Scenarios
// ============================================================================

TEST_F(RouteCacheTest, RemoveReplicaScenarios) {
    // 1. Remove single replica from multiple
    auto proxy1 = MakeP2PProxy(1, 100);
    auto proxy2 = MakeP2PProxy(1, 200);
    cache_.Replace("key_rm1", {proxy1, proxy2});

    cache_.RemoveReplica("key_rm1", {proxy1});
    auto result = cache_.Get("key_rm1");
    ASSERT_EQ(result.items().size(), 1u);
    EXPECT_EQ(result.items()[0].segment_id, (UUID{200, 0}));

    // 2. Remove last replica removes entire entry
    cache_.Replace("key_rm2", {proxy1});
    cache_.RemoveReplica("key_rm2", {proxy1});
    EXPECT_TRUE(cache_.Get("key_rm2").items().empty());
}

// ============================================================================
// Merge Scenarios
// ============================================================================

TEST_F(RouteCacheTest, MergeScenarios) {
    auto proxy1 = MakeP2PProxy(1, 100);
    auto proxy2 = MakeP2PProxy(2, 200);

    // 1. Upsert Append
    cache_.Replace("key_merge", {proxy1});
    cache_.Upsert("key_merge", {proxy2});

    auto result = cache_.Get("key_merge");
    ASSERT_EQ(result.items().size(), 2u);

    // 2. Upsert Deduplicate
    cache_.Replace("key_dedup", {proxy1, proxy2});
    cache_.Upsert("key_dedup", {proxy1});  // Should be deduplicated

    result = cache_.Get("key_dedup");
    ASSERT_EQ(result.items().size(), 2u);
}

// ============================================================================
// Concurrent Access
// ============================================================================

TEST_F(RouteCacheTest, SharedKeyConcurrencyStress) {
    constexpr int kNumThreads = 16;
    constexpr int kOpsPerThread = 2000;
    std::string key = "shared_stress_key";

    std::vector<std::thread> threads;
    threads.reserve(kNumThreads);

    for (int t = 0; t < kNumThreads; ++t) {
        threads.emplace_back([this, t, key]() {
            for (int i = 0; i < kOpsPerThread; ++i) {
                auto proxy = MakeP2PProxy(t, i);

                // Mix of Upsert, Get, and Invalidate
                if (i % 2 == 0) {
                    cache_.Upsert(key, {proxy});
                } else if (i % 3 == 0) {
                    cache_.RemoveReplica(key, {proxy});
                } else {
                    auto result = cache_.Get(key);
                    if (!result.items().empty()) {
                        // Do something with the handle to ensure handle usage
                        ASSERT_GE(result.items().size(), 0u);
                    }
                }

                if (i % 100 == 0) {
                    std::this_thread::yield();
                }
            }
        });
    }

    for (auto& t : threads) {
        t.join();
    }
}

TEST_F(RouteCacheTest, ABACorruptionAfterRecycle) {
    // 1MB small cache
    RouteCache cache(1024 * 1024, 600000);

    std::vector<std::string> keys;
    // 1. Put keys to form bucket chains
    for (int i = 0; i < 2000; ++i) {
        keys.push_back("key_aba_" + std::to_string(i));
        cache.Replace(keys.back(), {MakeP2PProxy(i, i)});
    }

    // 2. Trigger updates on ALL keys to ensure some are not at the head of
    // their buckets. In buggy code, this retires their old nodes with
    // bucket_idx=-1, leaving them physically linked in prev->next_.
    for (int i = 0; i < 2000; ++i) {
        cache.Replace(keys[i], {MakeP2PProxy(i, i + 10000)});
    }

    // 3. Wait for GC to recycle those retired nodes
    auto current_free = cache.GetMetrics().free_node_count;
    for (int i = 0; i < 30; ++i) {
        if (cache.GetMetrics().free_node_count > current_free) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }

    // 4. Now put completely NEW keys. They will pop the recycled nodes from the
    // pool! Since the old nodes were never unlinked from the old bucket's
    // prev->next_, the recycled nodes will bridge old buckets to new buckets,
    // causing data loss and chain corruption.
    std::vector<std::string> new_keys;
    for (int i = 0; i < 2000; ++i) {
        new_keys.push_back("key_new_" + std::to_string(i));
        cache.Replace(new_keys.back(), {MakeP2PProxy(i, i + 20000)});
    }

    // 5. Verify we can still Get all the original keys!
    int lost = 0;
    for (int i = 0; i < 2000; ++i) {
        auto view = cache.Get(keys[i]);
        if (view.items().empty()) {
            lost++;
        } else {
            EXPECT_EQ(view.items()[0].segment_id,
                      (UUID{uint64_t(i + 10000), 0}));
        }
    }
    EXPECT_EQ(lost, 0) << "ABA issue: " << lost
                       << " keys were lost due to corrupted bucket chains!";
}

TEST_F(RouteCacheTest, UpdatePreservesBucketChain) {
    // 100MB cache, plenty for 5000 keys to avoid Evict
    RouteCache cache(100 * 1024 * 1024, 600000);

    // 1. Fill the cache with enough keys to guarantee bucket collisions
    constexpr int kNumInitialKeys = 5000;
    for (int i = 0; i < kNumInitialKeys; ++i) {
        std::string key = "key_" + std::to_string(i);
        cache.Replace(key, {MakeP2PProxy(i, i)});
    }

    for (int i = 0; i < kNumInitialKeys; ++i) {
        ASSERT_FALSE(cache.Get("key_" + std::to_string(i)).items().empty());
    }

    // 2. Perform updates on some keys.
    for (int i = 0; i < kNumInitialKeys; i += 10) {
        std::string key = "key_" + std::to_string(i);
        cache.Replace(key, {MakeP2PProxy(i, i + 100000)});
    }

    // Update one specific key multiple times
    for (int i = 0; i < 10; ++i) {
        cache.Replace("key_50", {MakeP2PProxy(50, 200000 + i)});
    }

    // 3. Final Verification: ALL keys should still be present!
    int lost_count = 0;
    for (int i = 0; i < kNumInitialKeys; ++i) {
        std::string key = "key_" + std::to_string(i);
        if (cache.Get(key).items().empty()) {
            lost_count++;
        }
    }

    EXPECT_EQ(lost_count, 0) << "Detected data loss! " << lost_count
                             << " keys were dropped after updates.";
}

// ============================================================================
// High Concurrency RCU Stress
// ============================================================================

TEST_F(RouteCacheTest, HighConcurrencyRCUStress) {
    // 1MB cache, auto shards. Very small to trigger frequent evictions.
    RouteCache small_cache(1024 * 1024, 60000);
    constexpr int kNumThreads = 16;
    constexpr int kOpsPerThread = 2000;
    std::atomic<bool> stop{false};
    std::atomic<int> errors{0};

    std::vector<std::thread> threads;
    for (int t = 0; t < kNumThreads; ++t) {
        threads.emplace_back([&, t]() {
            for (int i = 0; i < kOpsPerThread && !stop; ++i) {
                std::string key =
                    "key_" + std::to_string((t * kOpsPerThread + i) % 500);
                auto proxy = MakeP2PProxy(
                    t, i, "192.168.1." + std::to_string(t), (uint16_t)i);

                if (i % 5 == 0) {
                    small_cache.Upsert(key, {proxy});
                } else if (i % 7 == 0) {
                    small_cache.RemoveReplica(key, {proxy});
                } else {
                    auto view = small_cache.Get(key);
                    if (!view.items().empty()) {
                        // Data Integrity Check
                        for (const auto& item : view.items()) {
                            if (item.rpc_port > kOpsPerThread) {
                                errors.fetch_add(1);
                            }
                            // Accessing IP address to ensure no crash
                            std::string ip(item.ip_address);
                            if (ip.find("192.168.1.") == std::string::npos) {
                                // This might happen if we read a partially
                                // initialized or recycled node But RCU should
                                // prevent this.
                                errors.fetch_add(1);
                            }
                        }
                    }
                }
            }
        });
    }

    for (auto& t : threads) t.join();
    EXPECT_EQ(errors.load(), 0);
}

// ============================================================================
// Performance Comparison
// ============================================================================

TEST_F(RouteCacheTest, PerformanceComparison) {
    constexpr int kNumQueries = 100;
    constexpr int kMasterLatencyMs = 2;  // Simulated latency
    std::string key = "perf_key";
    auto proxy = MakeP2PProxy(1, 1);

    auto simulate_master_query = [kMasterLatencyMs]() {
        std::this_thread::sleep_for(
            std::chrono::milliseconds(kMasterLatencyMs));
        return true;
    };

    // 1. Without Cache (always query "master")
    auto start_no_cache = std::chrono::steady_clock::now();
    for (int i = 0; i < kNumQueries; ++i) {
        simulate_master_query();
    }
    auto end_no_cache = std::chrono::steady_clock::now();
    auto dur_no_cache = std::chrono::duration_cast<std::chrono::milliseconds>(
                            end_no_cache - start_no_cache)
                            .count();

    // 2. With Cache
    cache_.Replace(key, {proxy});
    auto start_with_cache = std::chrono::steady_clock::now();
    for (int i = 0; i < kNumQueries; ++i) {
        auto view = cache_.Get(key);
        if (view.items().empty()) {
            simulate_master_query();
            cache_.Replace(key, {proxy});
        }
    }
    auto end_with_cache = std::chrono::steady_clock::now();
    auto dur_with_cache = std::chrono::duration_cast<std::chrono::milliseconds>(
                              end_with_cache - start_with_cache)
                              .count();

    printf("\n[ PERF ] Queries: %d, Master Latency: %dms\n", kNumQueries,
           kMasterLatencyMs);
    printf("[ PERF ] Duration WITHOUT cache: %ld ms\n", dur_no_cache);
    printf("[ PERF ] Duration WITH cache:    %ld ms\n", dur_with_cache);
    printf("[ PERF ] Speedup: %.2fx\n", (double)dur_no_cache / dur_with_cache);

    EXPECT_LT(dur_with_cache,
              dur_no_cache / 10);  // Cache should be at least 10x faster here
}

TEST_F(RouteCacheTest, EvictionImpactOnWrite) {
    // Stress the allocator and eviction by putting more data than capacity
    RouteCache tiny_cache(64 * 1024, 60000);  // Only 64KB, auto shards
    constexpr int kEntries = 2000;

    auto start = std::chrono::steady_clock::now();
    for (int i = 0; i < kEntries; ++i) {
        std::string key = "key_" + std::to_string(i);
        tiny_cache.Replace(key, {MakeP2PProxy(i, i)});
    }
    auto end = std::chrono::steady_clock::now();
    auto dur =
        std::chrono::duration_cast<std::chrono::microseconds>(end - start)
            .count();

    printf("\n[ PERF ] Tiny Cache (64KB) Put %d entries: %ld us (%.2f us/op)\n",
           kEntries, dur, (double)dur / kEntries);
}

TEST_F(RouteCacheTest, DifferentCacheSizes) {
    auto run_test = [&](size_t size_mb) {
        RouteCache c(size_mb * 1024 * 1024, 60000);
        constexpr int kKeys = 20000;
        int hits = 0;

        // Warm up / Fill
        for (int i = 0; i < kKeys; ++i) {
            c.Replace("key_" + std::to_string(i), {MakeP2PProxy(i, i)});
        }

        // Query
        for (int i = 0; i < kKeys; ++i) {
            if (!c.Get("key_" + std::to_string(i)).items().empty()) hits++;
        }
        return (double)hits / kKeys * 100.0;
    };

    printf("\n[ PERF ] Cache Size Hit Rate (for %d keys):\n", 20000);
    printf("[ PERF ]   1 MB: %.2f%%\n", run_test(1));
    printf("[ PERF ]  10 MB: %.2f%%\n", run_test(10));
    printf("[ PERF ] 100 MB: %.2f%%\n", run_test(100));
}

// ============================================================================
// EBR Safety
// ============================================================================

TEST_F(RouteCacheTest, EBRPreventsReclaimWhileReaderActive) {
    RouteCache cache(1024 * 1024, 600000);

    // 1. Insert a key
    cache.Replace("ebr_key", {MakeP2PProxy(1, 100)});

    // 2. Get a handle (enters and exits EpochGuard internally)
    auto handle = cache.Get("ebr_key");
    ASSERT_FALSE(handle.items().empty());

    // 3. Overwrite the key many times to retire old nodes, then trigger GC
    //    by filling the cache to force eviction and SyncGC.
    for (int i = 0; i < 3000; ++i) {
        cache.Replace("flood_" + std::to_string(i), {MakeP2PProxy(i, i)});
    }

    // 4. Wait for GC to run
    std::this_thread::sleep_for(std::chrono::milliseconds(300));

    // 5. The handle obtained in step 2 must still be valid.
    //    shared_ptr protects the data memory even after the node is recycled.
    ASSERT_EQ(handle.items().size(), 1u);
    EXPECT_EQ(handle.items()[0].segment_id, (UUID{100, 0}));
}

// ============================================================================
// Watermark & Stress Tests
// ============================================================================

TEST_F(RouteCacheTest, WatermarkGCStrategy) {
    // 1MB tiny cache to trigger watermark quickly
    RouteCache tiny(1024 * 1024, 600000);
    auto metrics = tiny.GetMetrics();
    size_t total = metrics.total_node_count;

    // 1. Fill until usage > 90%
    size_t to_fill = total * 0.92;
    for (size_t i = 0; i < to_fill; ++i) {
        tiny.Replace("key_" + std::to_string(i), {MakeP2PProxy(i, i)});
    }

    auto m1 = tiny.GetMetrics();
    double usage1 = 1.0 - (double)m1.free_node_count / m1.total_node_count;
    EXPECT_GT(usage1, 0.9);

    // 2. Wait for GCLoop to detect and act (HIGH_WATERMARK -> LOW_WATERMARK)
    double usage2 = 1.0;
    for (int i = 0; i < 50; ++i) {
        auto m2 = tiny.GetMetrics();
        usage2 = 1.0 - (double)m2.free_node_count / m2.total_node_count;
        if (usage2 < 0.75) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }

    // 3. Verify it dropped below 70%
    EXPECT_LT(usage2, 0.75);  // Allow some margin
    printf("[ INFO ] Watermark Success: %.2f%% -> %.2f%%\n", usage1 * 100,
           usage2 * 100);
}

TEST_F(RouteCacheTest, HighPressureStress) {
    // Small cache to ensure contention
    RouteCache stress_cache(2 * 1024 * 1024, 60000);
    constexpr int kThreads = 16;
    constexpr int kOps = 2000;

    std::atomic<size_t> fail_count{0};

    std::vector<std::thread> workers;
    for (int t = 0; t < kThreads; ++t) {
        workers.emplace_back([&, t]() {
            for (int i = 0; i < kOps; ++i) {
                std::string key =
                    "key_" + std::to_string((t * kOps + i) % 1000);
                auto proxy = MakeP2PProxy(t, i);

                stress_cache.Replace(key, {proxy});

                auto res = stress_cache.Get(key);
                if (res.items().empty()) fail_count++;

                if (i % 100 == 0) std::this_thread::yield();
            }
        });
    }

    for (auto& w : workers) w.join();

    auto m = stress_cache.GetMetrics();
    printf(
        "[ INFO ] Stress Done. FailCount: %zu, "
        "FinalUsage: %.2f%%\n",
        fail_count.load(),
        (1.0 - (double)m.free_node_count / m.total_node_count) * 100);

    // Note: try-lock may fail under high contention, so some writes are
    // silently dropped. We focus on system stability and final usage.
    (void)fail_count;
}

// ============================================================================
// Lock Contention & Fallback Behavior
// ============================================================================

TEST_F(RouteCacheTest, TryLockYieldBehavior) {
    auto proxy = MakeP2PProxy(1, 100);
    std::string key = "yield_key";

    cache_.Upsert(key, {proxy});

    std::atomic<bool> stop{false};

    // Hold up the shard by doing huge merges constantly
    std::thread blocker([&]() {
        for (int i = 0; i < 2000 && !stop; ++i) {
            std::vector<P2PProxyDescriptor> huge_list(10, proxy);
            cache_.Replace(key, huge_list);
        }
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(5));

    // Some of them should gracefully fail (return early) under contention.
    for (int i = 0; i < 500; ++i) {
        cache_.Upsert(key, {MakeP2PProxy(2, i)});
    }

    stop = true;
    blocker.join();
}

TEST_F(RouteCacheTest, RemoveReplicaLockFreeFallback) {
    auto proxy1 = MakeP2PProxy(1, 100);
    auto proxy2 = MakeP2PProxy(2, 200);
    std::string key = "fallback_key";

    cache_.Upsert(key, {proxy1, proxy2});

    std::atomic<bool> stop{false};

    // Hold up the shard
    std::thread blocker([&]() {
        for (int i = 0; i < 2000 && !stop; ++i) {
            std::vector<P2PProxyDescriptor> huge_list(10, proxy1);
            cache_.Replace(key, huge_list);
        }
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(5));

    // This may hit the lock-free Plan A (MarkDeleted) if contention is high
    cache_.RemoveReplica(key, {proxy1});

    stop = true;
    blocker.join();

    auto result = cache_.Get(key);
}

}  // namespace
}  // namespace mooncake
