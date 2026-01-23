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
