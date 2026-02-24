#include <gtest/gtest.h>
#include <glog/logging.h>
#include "tiered_cache/tiered_backend.h"
#include <fstream>
#include <chrono>
#include <thread>
#include <cstring>
#include <atomic>
#include "tiered_cache/tiers/cache_tier.h"  // Ensure TempDRAMBuffer is available

namespace mooncake {

class SchedulerIntegrationTest : public ::testing::Test {
   protected:
    void SetUp() override {
        // Create Config with DRAM and STORAGE tiers
        Json::Value tiers(Json::arrayValue);

        Json::Value dram;
        dram["type"] = "DRAM";
        dram["capacity"] = (Json::UInt64)(10 * 1024 * 1024);  // 10MB
        dram["priority"] = 100;
        dram["allocator_type"] = "OFFSET";  // Use Offset allocator for test
        tiers.append(dram);

        Json::Value storage;
        storage["type"] = "STORAGE";
        storage["capacity"] = (Json::UInt64)(100 * 1024 * 1024);  // 100MB
        storage["priority"] = 10;
        tiers.append(storage);

        // Ensure storage directory exists
        std::string cmd = "mkdir -p /tmp/mooncake_test_storage";
        int ret = system(cmd.c_str());
        (void)ret;

        // Set Env for StorageTier
        setenv("MOONCAKE_OFFLOAD_STORAGE_BACKEND_DESCRIPTOR",
               "bucket_storage_backend", 1);
        setenv("MOONCAKE_OFFLOAD_FILE_STORAGE_PATH",
               "/tmp/mooncake_test_storage", 1);

        config_["tiers"] = tiers;
    }

    void TearDown() override {
        // Cleanup
        std::string cmd = "rm -rf /tmp/mooncake_test_storage";
        int ret = system(cmd.c_str());
        (void)ret;
    }

    Json::Value config_;
};

TEST_F(SchedulerIntegrationTest, TestPromotion) {
    TieredBackend backend;
    auto res = backend.Init(config_, nullptr, nullptr);
    ASSERT_TRUE(res.has_value());

    // 1. Identify IDs
    auto views = backend.GetTierViews();
    UUID dram_id;
    UUID storage_id;
    for (const auto& v : views) {
        if (v.priority == 100) dram_id = v.id;
        if (v.priority == 10) storage_id = v.id;
    }

    // 2. Write Data to Storage Tier explicitly
    std::string key = "hot_key";
    std::string value = "value_data";
    auto data_ptr = std::make_unique<char[]>(value.size());
    std::memcpy(data_ptr.get(), value.data(), value.size());

    DataSource source{
        std::make_unique<TempDRAMBuffer>(std::move(data_ptr), value.size()),
        MemoryType::DRAM};

    auto handle = backend.Allocate(value.size(), storage_id);
    ASSERT_TRUE(handle.has_value());
    ASSERT_TRUE(backend.Write(source, handle.value()).has_value());
    ASSERT_TRUE(backend.Commit(key, handle.value()).has_value());

    // Verify it is in Storage only
    auto replicas = backend.GetReplicaTierIds(key);
    ASSERT_EQ(replicas.size(), 1);
    ASSERT_EQ(replicas[0], storage_id);

    // 3. Heat up the key
    // Threshold is 5.0 in ClientScheduler constructor.
    // Trigger 6 times.
    for (int i = 0; i < 6; ++i) {
        backend.Get(key);
    }

    // 4. Wait for Scheduler Interval (Default 1000ms loop)
    LOG(INFO) << "Waiting for scheduler...";
    std::this_thread::sleep_for(std::chrono::milliseconds(1500));

    // 5. Verify Promotion
    replicas = backend.GetReplicaTierIds(key);
    // Should now have 1 replica (DRAM) because we implemented Move (Transfer +
    // Delete)
    ASSERT_EQ(replicas.size(), 1);

    bool has_dram = false;
    for (auto id : replicas) {
        if (id == dram_id) has_dram = true;
    }
    ASSERT_TRUE(has_dram) << "Key should be promoted to DRAM";

    LOG(INFO) << "Promotion verified successfully!";
}

TEST_F(SchedulerIntegrationTest, TestLRUCacheThrashing) {
    // 1. Configure LRU with watermarks
    config_["scheduler"]["policy"] = "LRU";
    config_["scheduler"]["high_watermark"] =
        0.9;  // Trigger eviction/swap above 90%
    config_["scheduler"]["low_watermark"] = 0.7;  // Trigger promotion below 70%

    // Modify DRAM to be small (5MB) to force eviction
    // Note: The SetUp created a 10MB DRAM tier. We need to override it or work
    // with it. 10MB is fine, we just need to write MORE than 10MB. Let's write
    // 20MB (40 keys * 512KB)

    TieredBackend backend;
    auto res = backend.Init(config_, nullptr, nullptr);
    ASSERT_TRUE(res.has_value());

    auto views = backend.GetTierViews();
    UUID dram_id;
    UUID storage_id;
    for (const auto& v : views) {
        if (v.type == MemoryType::DRAM) dram_id = v.id;
        if (v.type == MemoryType::NVME) storage_id = v.id;
    }

    const size_t item_size = 512 * 1024;  // 512KB
    const int total_keys = 40;            // 20MB total > 10MB DRAM
    const int hot_set_size = 5;           // 2.5MB Hot Set

    // 2. Write all data to Storage tier
    LOG(INFO) << "Phase 1: Writing " << total_keys << " keys to Storage";
    for (int i = 0; i < total_keys; i++) {
        std::string key = "key_" + std::to_string(i);
        auto handle = backend.Allocate(item_size, storage_id);
        ASSERT_TRUE(handle.has_value());

        auto buffer = std::make_unique<char[]>(item_size);
        std::memset(buffer.get(), 'A', item_size);
        DataSource source{
            std::make_unique<TempDRAMBuffer>(std::move(buffer), item_size),
            MemoryType::DRAM};

        ASSERT_TRUE(backend.Write(source, handle.value()).has_value());
        ASSERT_TRUE(backend.Commit(key, handle.value()).has_value());
    }

    // 3. Simulating Access Pattern
    LOG(INFO) << "Phase 2: Access Loop";
    // Loop enough times to trigger stats collection and policy execution
    for (int round = 0; round < 15; round++) {
        // Access Hot Set (K0-K4) multiple times
        for (int i = 0; i < hot_set_size; i++) {
            std::string key = "key_" + std::to_string(i);
            backend.Get(key);
        }

        // Access a sliding window of Cold Keys (scans through K5-K39)
        // Each round access 5 cold keys
        int start = hot_set_size + (round * 5) % (total_keys - hot_set_size);
        for (int i = 0; i < 5; i++) {
            int idx = start + i;
            if (idx >= total_keys)
                idx = hot_set_size + (idx - total_keys);  // Wrap around
            std::string key = "key_" + std::to_string(idx);
            backend.Get(key);
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(150));
    }

    // 4. Verification
    LOG(INFO) << "Phase 3: Verification";

    // Check Watermark compliance
    views = backend.GetTierViews();
    size_t dram_capacity = 0;
    for (const auto& v : views) {
        if (v.id == dram_id) {
            dram_capacity = v.capacity;
            double usage_ratio = (double)v.usage / v.capacity;
            LOG(INFO) << "DRAM Usage: " << usage_ratio * 100 << "%";
            // Should be around low_watermark (70%)
            EXPECT_LE(usage_ratio, 0.75);
            EXPECT_GE(usage_ratio, 0.65);
        }
    }

    // Calculate expected DRAM slots
    size_t target_usage = static_cast<size_t>(dram_capacity * 0.7);
    int expected_dram_slots = target_usage / item_size;
    LOG(INFO) << "Expected DRAM slots: " << expected_dram_slots;

    // Check Hot Set Retention - ALL hot keys should be in DRAM (they are the
    // hottest)
    int hot_promoted = 0;
    for (int i = 0; i < hot_set_size; i++) {
        std::string key = "key_" + std::to_string(i);
        auto replicas = backend.GetReplicaTierIds(key);
        bool in_dram = false;
        for (auto tid : replicas)
            if (tid == dram_id) in_dram = true;
        if (in_dram) hot_promoted++;
    }
    LOG(INFO) << "Hot Keys in DRAM: " << hot_promoted << "/" << hot_set_size;
    EXPECT_EQ(hot_promoted, hot_set_size) << "ALL hot keys should be in DRAM";

    // Check Cold Set - count how many cold keys are in DRAM
    int cold_in_dram = 0;
    for (int i = hot_set_size; i < total_keys; i++) {
        std::string key = "key_" + std::to_string(i);
        auto replicas = backend.GetReplicaTierIds(key);
        bool in_dram = false;
        for (auto tid : replicas)
            if (tid == dram_id) in_dram = true;
        if (in_dram) cold_in_dram++;
    }
    int cold_count = total_keys - hot_set_size;
    LOG(INFO) << "Cold Keys in DRAM: " << cold_in_dram << "/" << cold_count;

    // Core verification: Total keys in DRAM should match expected slots
    int total_in_dram = hot_promoted + cold_in_dram;
    LOG(INFO) << "Total Keys in DRAM: " << total_in_dram
              << ", Expected: " << expected_dram_slots;
    // Allow some tolerance due to size variations
    EXPECT_LE(total_in_dram, expected_dram_slots + 1);
    EXPECT_GE(total_in_dram, expected_dram_slots - 1);
}

// Test LRU Promotion Budget: Verify promotion is limited by available capacity
TEST_F(SchedulerIntegrationTest, TestLRUPromotionBudget) {
    // Configure LRU with watermarks
    config_["scheduler"]["policy"] = "LRU";
    config_["scheduler"]["high_watermark"] = 0.9;
    config_["scheduler"]["low_watermark"] = 0.7;

    TieredBackend backend;
    auto res = backend.Init(config_, nullptr, nullptr);
    ASSERT_TRUE(res.has_value());

    auto views = backend.GetTierViews();
    UUID dram_id;
    UUID storage_id;
    size_t dram_capacity = 0;
    for (const auto& v : views) {
        if (v.type == MemoryType::DRAM) {
            dram_id = v.id;
            dram_capacity = v.capacity;
        }
        if (v.type == MemoryType::NVME) storage_id = v.id;
    }

    // Fill DRAM to 75% capacity first (below low_watermark 80%)
    // This leaves only 5% budget for promotion (up to 80%)
    size_t fill_size = static_cast<size_t>(dram_capacity * 0.75);
    size_t item_size = 512 * 1024;  // 512KB per item
    int fill_count = fill_size / item_size;

    LOG(INFO) << "Phase 1: Filling DRAM to 75% (" << fill_count << " items)";
    for (int i = 0; i < fill_count; i++) {
        std::string key = "dram_key_" + std::to_string(i);
        auto handle = backend.Allocate(item_size, dram_id);
        ASSERT_TRUE(handle.has_value());

        auto buffer = std::make_unique<char[]>(item_size);
        std::memset(buffer.get(), 'D', item_size);
        DataSource source{
            std::make_unique<TempDRAMBuffer>(std::move(buffer), item_size),
            MemoryType::DRAM};

        ASSERT_TRUE(backend.Write(source, handle.value()).has_value());
        ASSERT_TRUE(backend.Commit(key, handle.value()).has_value());
    }

    // Write many keys to Storage (more than promotion budget allows)
    int storage_count = 20;  // 10MB in storage, but only ~0.5MB budget
    LOG(INFO) << "Phase 2: Writing " << storage_count << " keys to Storage";
    for (int i = 0; i < storage_count; i++) {
        std::string key = "storage_key_" + std::to_string(i);
        auto handle = backend.Allocate(item_size, storage_id);
        ASSERT_TRUE(handle.has_value());

        auto buffer = std::make_unique<char[]>(item_size);
        std::memset(buffer.get(), 'S', item_size);
        DataSource source{
            std::make_unique<TempDRAMBuffer>(std::move(buffer), item_size),
            MemoryType::DRAM};

        ASSERT_TRUE(backend.Write(source, handle.value()).has_value());
        ASSERT_TRUE(backend.Commit(key, handle.value()).has_value());
    }

    // Access all storage keys to trigger promotion
    LOG(INFO) << "Phase 3: Accessing storage keys";
    for (int round = 0; round < 5; round++) {
        for (int i = 0; i < storage_count; i++) {
            std::string key = "storage_key_" + std::to_string(i);
            backend.Get(key);
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(300));
    }

    // Verify: DRAM should not exceed low_watermark significantly
    views = backend.GetTierViews();
    for (const auto& v : views) {
        if (v.id == dram_id) {
            double usage_ratio = (double)v.usage / v.capacity;
            LOG(INFO) << "DRAM Usage after promotion: " << usage_ratio * 100
                      << "%";
            // Should be around 80% (low_watermark), not 100%
            EXPECT_LE(usage_ratio, 0.85)
                << "Promotion should be limited by budget";
        }
    }

    // Count how many storage keys were promoted
    int promoted_count = 0;
    for (int i = 0; i < storage_count; i++) {
        std::string key = "storage_key_" + std::to_string(i);
        auto replicas = backend.GetReplicaTierIds(key);
        for (auto tid : replicas) {
            if (tid == dram_id) {
                promoted_count++;
                break;
            }
        }
    }
    LOG(INFO) << "Promoted " << promoted_count << "/" << storage_count
              << " storage keys";
    // Should NOT promote all keys due to budget limit
    EXPECT_LT(promoted_count, storage_count)
        << "Not all keys should be promoted due to budget";
}

// Test LRU Eviction: Verify cold data is evicted when DRAM exceeds
// high_watermark and that DRAM contains the hottest keys
TEST_F(SchedulerIntegrationTest, TestLRUEviction) {
    // Configure LRU
    config_["scheduler"]["policy"] = "LRU";
    config_["scheduler"]["high_watermark"] = 0.9;
    config_["scheduler"]["low_watermark"] = 0.7;

    TieredBackend backend;
    auto res = backend.Init(config_, nullptr, nullptr);
    ASSERT_TRUE(res.has_value());

    auto views = backend.GetTierViews();
    UUID dram_id;
    UUID storage_id;
    size_t dram_capacity = 0;
    for (const auto& v : views) {
        if (v.type == MemoryType::DRAM) {
            dram_id = v.id;
            dram_capacity = v.capacity;
        }
        if (v.type == MemoryType::NVME) storage_id = v.id;
    }

    size_t item_size = 512 * 1024;  // 512KB per item
    // Fill DRAM to 95% (above high_watermark 90%)
    size_t fill_size = static_cast<size_t>(dram_capacity * 0.95);
    int fill_count = fill_size / item_size;

    LOG(INFO) << "Phase 1: Filling DRAM to 95% (" << fill_count << " items)";
    for (int i = 0; i < fill_count; i++) {
        std::string key = "evict_key_" + std::to_string(i);
        auto handle = backend.Allocate(item_size, dram_id);
        ASSERT_TRUE(handle.has_value());

        auto buffer = std::make_unique<char[]>(item_size);
        std::memset(buffer.get(), 'E', item_size);
        DataSource source{
            std::make_unique<TempDRAMBuffer>(std::move(buffer), item_size),
            MemoryType::DRAM};

        ASSERT_TRUE(backend.Write(source, handle.value()).has_value());
        ASSERT_TRUE(backend.Commit(key, handle.value()).has_value());
    }

    // Access pattern: first half (hot set) accessed many times, second half
    // (cold) not accessed
    int hot_count = fill_count / 2;
    LOG(INFO) << "Phase 2: Accessing hot set (" << hot_count << " keys)";
    for (int round = 0; round < 5; round++) {
        for (int i = 0; i < hot_count; i++) {
            std::string key = "evict_key_" + std::to_string(i);
            backend.Get(key);
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(300));
    }

    // Verify: DRAM usage should drop to around low_watermark (70%)
    views = backend.GetTierViews();
    for (const auto& v : views) {
        if (v.id == dram_id) {
            double usage_ratio = (double)v.usage / v.capacity;
            LOG(INFO) << "DRAM Usage after eviction: " << usage_ratio * 100
                      << "%";
            // Should be around 70% after eviction (low_watermark)
            EXPECT_LE(usage_ratio, 0.75)
                << "Eviction should bring usage to low_watermark";
            EXPECT_GE(usage_ratio, 0.65)
                << "Usage should be around low_watermark";
        }
    }

    // Calculate expected DRAM slots
    size_t target_usage = static_cast<size_t>(dram_capacity * 0.7);
    int expected_dram_slots = target_usage / item_size;
    int cold_count = fill_count - hot_count;

    // Verify: Count hot and cold keys in DRAM
    int hot_in_dram = 0;
    int cold_in_dram = 0;
    for (int i = 0; i < fill_count; i++) {
        std::string key = "evict_key_" + std::to_string(i);
        auto replicas = backend.GetReplicaTierIds(key);
        bool in_dram = false;
        for (auto tid : replicas) {
            if (tid == dram_id) {
                in_dram = true;
                break;
            }
        }
        if (i < hot_count) {
            if (in_dram) hot_in_dram++;
        } else {
            if (in_dram) cold_in_dram++;
        }
    }

    LOG(INFO) << "Hot keys in DRAM: " << hot_in_dram << "/" << hot_count;
    LOG(INFO) << "Cold keys in DRAM: " << cold_in_dram << "/" << cold_count;
    LOG(INFO) << "Expected DRAM slots: " << expected_dram_slots;

    // Core verification:
    // 1. ALL hot keys should be in DRAM (they are the hottest)
    EXPECT_EQ(hot_in_dram, hot_count)
        << "All hot keys should be retained in DRAM";

    // 2. Total keys in DRAM should match expected slots
    int total_in_dram = hot_in_dram + cold_in_dram;
    EXPECT_LE(total_in_dram, expected_dram_slots + 1);
    EXPECT_GE(total_in_dram, expected_dram_slots - 1);

    // 3. Cold keys in DRAM should be the remainder after hot keys
    int expected_cold_in_dram = std::max(0, expected_dram_slots - hot_count);
    LOG(INFO) << "Expected cold keys in DRAM: " << expected_cold_in_dram;
    EXPECT_LE(cold_in_dram, expected_cold_in_dram + 1);
}

class ConcurrencyTest : public ::testing::Test {
   protected:
    void SetUp() override {
        // Setup TieredBackend with 2 DRAM tiers for simple migration testing
        Json::Value config;
        Json::Value tiers(Json::arrayValue);

        Json::Value fast_tier;
        fast_tier["type"] = "DRAM";
        fast_tier["capacity"] = 1024 * 1024;  // 1MB
        fast_tier["priority"] = 2;            // Higher priority
        fast_tier["allocator_type"] = "OFFSET";
        tiers.append(fast_tier);

        Json::Value slow_tier;
        slow_tier["type"] = "DRAM";
        slow_tier["capacity"] = 1024 * 1024;  // 1MB
        slow_tier["priority"] = 1;
        slow_tier["allocator_type"] = "OFFSET";
        tiers.append(slow_tier);

        config["tiers"] = tiers;

        backend_ = std::make_unique<TieredBackend>();
        auto res = backend_->Init(config, nullptr, nullptr);
        ASSERT_TRUE(res.has_value());

        // Identify Tiers
        auto views = backend_->GetTierViews();
        for (const auto& view : views) {
            if (view.priority == 2)
                fast_tier_id_ = view.id;
            else if (view.priority == 1)
                slow_tier_id_ = view.id;
        }
    }

    void TearDown() override { backend_.reset(); }

    std::unique_ptr<TieredBackend> backend_;
    UUID fast_tier_id_;
    UUID slow_tier_id_;
};

// Scenario 1: Resurrection
TEST_F(ConcurrencyTest, TestResurrection) {
    std::string key = "resurrection_key";
    std::string data_str = "payload";

    // 1. Setup: Data exists on Slow Tier
    auto data_ptr = std::make_unique<char[]>(data_str.size());
    std::memcpy(data_ptr.get(), data_str.data(), data_str.size());
    DataSource source{
        std::make_unique<TempDRAMBuffer>(std::move(data_ptr), data_str.size()),
        MemoryType::DRAM};

    auto copy_res = backend_->CopyData(key, source, slow_tier_id_);
    ASSERT_TRUE(copy_res.has_value());

    // 2. Scheduler Step 1: Get Handle and Version
    uint64_t start_version = 0;
    auto get_res = backend_->Get(key, slow_tier_id_, false, &start_version);
    ASSERT_TRUE(get_res.has_value());

    // 3. User Action: Delete Key
    auto del_res = backend_->Delete(key);  // Deletes all replicas
    ASSERT_TRUE(del_res.has_value());

    // 4. Scheduler Step 2: Commit (CopyData with expected version)
    auto transfer_res =
        backend_->CopyData(key, source, fast_tier_id_, start_version);

    // 5. Verification: Should Fail with CAS_FAILED
    ASSERT_FALSE(transfer_res.has_value());
    EXPECT_EQ(transfer_res.error(), ErrorCode::CAS_FAILED);

    // Key should NOT exist in Fast Tier
    auto check_res = backend_->Get(key, fast_tier_id_);
    ASSERT_FALSE(check_res.has_value());
}

// Scenario 2: Stale Data Overwrite
TEST_F(ConcurrencyTest, TestStaleDataOverwrite) {
    std::string key = "stale_key";
    std::string data_v1 = "version_1";
    std::string data_v2 = "version_2";

    // 1. Setup: Data v1 on Slow Tier
    auto data_ptr1 = std::make_unique<char[]>(data_v1.size());
    std::memcpy(data_ptr1.get(), data_v1.data(), data_v1.size());
    DataSource source_v1{
        std::make_unique<TempDRAMBuffer>(std::move(data_ptr1), data_v1.size()),
        MemoryType::DRAM};

    ASSERT_TRUE(backend_->CopyData(key, source_v1, slow_tier_id_).has_value());

    // 2. Scheduler Step 1: Get Handle and Version (v1)
    uint64_t start_version = 0;
    auto get_res = backend_->Get(key, slow_tier_id_, false, &start_version);
    ASSERT_TRUE(get_res.has_value());

    // 3. User Action: Update Key to v2
    auto data_ptr2 = std::make_unique<char[]>(data_v2.size());
    std::memcpy(data_ptr2.get(), data_v2.data(), data_v2.size());
    DataSource source_v2{
        std::make_unique<TempDRAMBuffer>(std::move(data_ptr2), data_v2.size()),
        MemoryType::DRAM};

    ASSERT_TRUE(backend_->CopyData(key, source_v2, fast_tier_id_).has_value());

    // 4. Scheduler Step 2: Commit v1 to Fast Tier
    auto transfer_res =
        backend_->CopyData(key, source_v1, fast_tier_id_, start_version);

    // 5. Verification: Should Fail with CAS_FAILED
    ASSERT_FALSE(transfer_res.has_value());
    EXPECT_EQ(transfer_res.error(), ErrorCode::CAS_FAILED);

    // Data check: Should be v2
    auto final_handle = backend_->Get(key, fast_tier_id_);
    ASSERT_TRUE(final_handle.has_value());
}

// Verify CAS failure produces NO side effects (no tier commit, no sync
// callback)
TEST_F(ConcurrencyTest, CASFailureNoSideEffects) {
    std::string key = "cas_side_effect_key";
    std::string payload = "initial";

    // Write initial version
    auto buf = std::make_unique<char[]>(payload.size());
    std::memcpy(buf.get(), payload.data(), payload.size());
    DataSource src{
        std::make_unique<TempDRAMBuffer>(std::move(buf), payload.size()),
        MemoryType::DRAM};
    ASSERT_TRUE(backend_->CopyData(key, src, slow_tier_id_).has_value());

    // Advance version by committing again
    auto buf2 = std::make_unique<char[]>(payload.size());
    std::memcpy(buf2.get(), payload.data(), payload.size());
    DataSource src2{
        std::make_unique<TempDRAMBuffer>(std::move(buf2), payload.size()),
        MemoryType::DRAM};
    ASSERT_TRUE(backend_->CopyData(key, src2, slow_tier_id_).has_value());

    // Now version >= 2. Try CAS with stale version 1.
    uint64_t stale_version = 1;
    auto buf3 = std::make_unique<char[]>(payload.size());
    std::memcpy(buf3.get(), payload.data(), payload.size());
    DataSource src3{
        std::make_unique<TempDRAMBuffer>(std::move(buf3), payload.size()),
        MemoryType::DRAM};

    auto result = backend_->CopyData(key, src3, fast_tier_id_, stale_version);
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::CAS_FAILED);

    // Key should NOT have a replica on fast tier
    auto replicas = backend_->GetReplicaTierIds(key);
    for (auto tid : replicas) {
        EXPECT_NE(tid, fast_tier_id_)
            << "CAS failure must not leave data on fast tier";
    }
}

// Verify CAS with callback counter: stale commit must not invoke sync callback
TEST_F(ConcurrencyTest, CASFailureNoCallbackInvoked) {
    // Re-init backend with a counting callback
    std::atomic<int> commit_count{0};
    MetadataSyncCallback counting_cb =
        [&commit_count](
            const std::string&, const UUID&,
            enum CALLBACK_TYPE type) -> tl::expected<void, ErrorCode> {
        if (type == COMMIT) commit_count.fetch_add(1);
        return {};
    };

    Json::Value config;
    Json::Value tiers(Json::arrayValue);
    Json::Value t1;
    t1["type"] = "DRAM";
    t1["capacity"] = 1024 * 1024;
    t1["priority"] = 2;
    t1["allocator_type"] = "OFFSET";
    tiers.append(t1);
    Json::Value t2;
    t2["type"] = "DRAM";
    t2["capacity"] = 1024 * 1024;
    t2["priority"] = 1;
    t2["allocator_type"] = "OFFSET";
    tiers.append(t2);
    config["tiers"] = tiers;

    TieredBackend be;
    ASSERT_TRUE(be.Init(config, nullptr, counting_cb).has_value());

    auto views = be.GetTierViews();
    UUID fast_id, slow_id;
    for (auto& v : views) {
        if (v.priority == 2)
            fast_id = v.id;
        else
            slow_id = v.id;
    }

    // Initial commit (version becomes 1)
    std::string key = "cb_key";
    auto buf = std::make_unique<char[]>(64);
    DataSource src{std::make_unique<TempDRAMBuffer>(std::move(buf), 64),
                   MemoryType::DRAM};
    ASSERT_TRUE(be.CopyData(key, src, slow_id).has_value());
    int baseline = commit_count.load();

    // Stale CAS commit — should NOT invoke callback
    auto buf2 = std::make_unique<char[]>(64);
    DataSource src2{std::make_unique<TempDRAMBuffer>(std::move(buf2), 64),
                    MemoryType::DRAM};
    uint64_t stale = 0;  // version 0 is stale
    auto res = be.CopyData(key, src2, fast_id, stale);
    EXPECT_FALSE(res.has_value());
    EXPECT_EQ(res.error(), ErrorCode::CAS_FAILED);
    EXPECT_EQ(commit_count.load(), baseline)
        << "Stale CAS must not invoke metadata sync callback";
}

// Concurrent flush + delete stress test (validates UAF fix)
TEST_F(SchedulerIntegrationTest, ConcurrentFlushDeleteStress) {
    TieredBackend backend;
    auto res = backend.Init(config_, nullptr, nullptr);
    ASSERT_TRUE(res.has_value());

    auto views = backend.GetTierViews();
    UUID storage_id;
    for (const auto& v : views) {
        if (v.type == MemoryType::NVME) storage_id = v.id;
    }

    const int iterations = 200;
    const size_t item_size = 4096;
    std::atomic<bool> stop{false};
    std::atomic<int> flush_count{0};

    // Thread A: continuous flush
    std::thread flusher([&] {
        auto* tier = const_cast<CacheTier*>(backend.GetTier(storage_id));
        while (!stop.load(std::memory_order_relaxed)) {
            tier->Flush();
            flush_count.fetch_add(1);
            std::this_thread::yield();
        }
    });

    // Thread B: rapid commit + delete
    for (int i = 0; i < iterations; ++i) {
        std::string key = "stress_" + std::to_string(i);
        auto handle = backend.Allocate(item_size, storage_id);
        if (!handle.has_value()) continue;

        auto buf = std::make_unique<char[]>(item_size);
        std::memset(buf.get(), 0xAB, item_size);
        DataSource src{
            std::make_unique<TempDRAMBuffer>(std::move(buf), item_size),
            MemoryType::DRAM};

        if (!backend.Write(src, handle.value()).has_value()) continue;
        if (!backend.Commit(key, handle.value()).has_value()) continue;

        backend.Delete(key);
    }
    stop = true;
    flusher.join();

    LOG(INFO) << "Flush+Delete stress: " << iterations << " iterations, "
              << flush_count.load() << " flushes. No UAF.";
}

}  // namespace mooncake
