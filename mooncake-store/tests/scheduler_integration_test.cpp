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

}  // namespace mooncake
