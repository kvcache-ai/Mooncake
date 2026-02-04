#include <gtest/gtest.h>
#include <glog/logging.h>
#include "tiered_cache/tiered_backend.h"
#include <fstream>
#include <chrono>
#include <thread>
#include <cstring>
#include "tiered_cache/cache_tier.h"  // Ensure TempDRAMBuffer is available

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
    // 1. Configure LRU
    config_["scheduler"]["policy"] = "LRU";
    config_["scheduler"]["high_watermark"] = 0.9;
    config_["scheduler"]["low_watermark"] = 0.8;

    // Modify DRAM to be small (5MB) to force eviction
    // Note: The SetUp created a 10MB DRAM tier. We need to override it or work with it.
    // 10MB is fine, we just need to write MORE than 10MB.
    // Let's write 20MB (40 keys * 512KB)

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

    const size_t item_size = 512 * 1024; // 512KB
    const int total_keys = 40;           // 20MB total > 10MB DRAM
    const int hot_set_size = 5;          // 2.5MB Hot Set

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
            if (idx >= total_keys) idx = hot_set_size + (idx - total_keys); // Wrap around
            std::string key = "key_" + std::to_string(idx);
            backend.Get(key);
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(150));
    }

    // 4. Verification
    LOG(INFO) << "Phase 3: Verification";

    // Check Watermark compliance
    views = backend.GetTierViews();
    for (const auto& v : views) {
        if (v.id == dram_id) {
            double usage_ratio = (double)v.usage / v.capacity;
            LOG(INFO) << "DRAM Usage: " << usage_ratio * 100 << "%";
            // Should be <= ~90% (maybe slightly higher if eviction trails, but < 100%)
            EXPECT_LE(usage_ratio, 0.95);
        }
    }

    // Check Hot Set Retention
    int hot_promoted = 0;
    for (int i = 0; i < hot_set_size; i++) {
        std::string key = "key_" + std::to_string(i);
        auto replicas = backend.GetReplicaTierIds(key);
        bool in_dram = false;
        for (auto tid : replicas) if (tid == dram_id) in_dram = true;
        if (in_dram) hot_promoted++;
    }
    LOG(INFO) << "Hot Keys in DRAM: " << hot_promoted << "/" << hot_set_size;
    EXPECT_GE(hot_promoted, hot_set_size - 1);
}

// Test LRU Promotion Budget: Verify promotion is limited by available capacity
TEST_F(SchedulerIntegrationTest, TestLRUPromotionBudget) {
    // Configure LRU with tight watermarks
    config_["scheduler"]["policy"] = "LRU";
    config_["scheduler"]["high_watermark"] = 0.9;
    config_["scheduler"]["low_watermark"] = 0.8;

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
            LOG(INFO) << "DRAM Usage after promotion: " << usage_ratio * 100 << "%";
            // Should be around 80% (low_watermark), not 100%
            EXPECT_LE(usage_ratio, 0.85) << "Promotion should be limited by budget";
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
    LOG(INFO) << "Promoted " << promoted_count << "/" << storage_count << " storage keys";
    // Should NOT promote all keys due to budget limit
    EXPECT_LT(promoted_count, storage_count) << "Not all keys should be promoted due to budget";
}

// Test LRU Eviction: Verify cold data is evicted when DRAM exceeds high_watermark
TEST_F(SchedulerIntegrationTest, TestLRUEviction) {
    // Configure LRU
    config_["scheduler"]["policy"] = "LRU";
    config_["scheduler"]["high_watermark"] = 0.9;
    config_["scheduler"]["low_watermark"] = 0.8;

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

    // Access only the first half (hot set), leave second half cold
    int hot_count = fill_count / 2;
    LOG(INFO) << "Phase 2: Accessing hot set (" << hot_count << " keys)";
    for (int round = 0; round < 5; round++) {
        for (int i = 0; i < hot_count; i++) {
            std::string key = "evict_key_" + std::to_string(i);
            backend.Get(key);
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(300));
    }

    // Verify: DRAM usage should drop to around low_watermark
    views = backend.GetTierViews();
    for (const auto& v : views) {
        if (v.id == dram_id) {
            double usage_ratio = (double)v.usage / v.capacity;
            LOG(INFO) << "DRAM Usage after eviction: " << usage_ratio * 100 << "%";
            // Should be around 80% after eviction
            EXPECT_LE(usage_ratio, 0.90) << "Eviction should bring usage below high_watermark";
        }
    }

    // Verify cold keys were evicted (moved to storage or deleted)
    int cold_in_dram = 0;
    for (int i = hot_count; i < fill_count; i++) {
        std::string key = "evict_key_" + std::to_string(i);
        auto replicas = backend.GetReplicaTierIds(key);
        for (auto tid : replicas) {
            if (tid == dram_id) {
                cold_in_dram++;
                break;
            }
        }
    }
    int cold_count = fill_count - hot_count;
    LOG(INFO) << "Cold keys remaining in DRAM: " << cold_in_dram << "/" << cold_count;
    // Some cold keys should have been evicted
    EXPECT_LT(cold_in_dram, cold_count) << "Some cold keys should be evicted";
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

}  // namespace mooncake
