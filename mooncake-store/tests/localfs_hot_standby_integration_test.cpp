// mooncake-store/tests/localfs_hot_standby_integration_test.cpp
//
// End-to-end integration tests for the HA replication flow using LocalFS
// backend.  Mirrors the structure of hot_standby_integration_test.cpp but
// replaces etcd with a shared temp directory so the tests run without any
// external service.
//
// Test flow:
//   Primary OpLogManager -> LocalFsOpLogStore (WRITER)
//       -> shared filesystem directory <-
//   HotStandbyService (PollingOpLogChangeNotifier -> OpLogReplicator
//                       -> OpLogApplier -> StandbyMetadataStore)

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <filesystem>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "hot_standby_service.h"
#include "ha/oplog/oplog_manager.h"
#include "ha/oplog/oplog_store_factory.h"
#include "standby_state_machine.h"

namespace mooncake {
namespace testing {

// ============================================================
// RAII helper to ensure HotStandbyService is always stopped
// ============================================================

class StandbyServiceGuard {
   public:
    explicit StandbyServiceGuard(HotStandbyService* service)
        : service_(service) {}
    ~StandbyServiceGuard() {
        if (service_) {
            service_->Stop();
            // LocalFS has no goroutines to drain; a short sleep suffices to
            // let background polling threads exit cleanly.
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
        }
    }
    StandbyServiceGuard(const StandbyServiceGuard&) = delete;
    StandbyServiceGuard& operator=(const StandbyServiceGuard&) = delete;

   private:
    HotStandbyService* service_;
};

// ============================================================
// Fixture
// ============================================================

class LocalFsHotStandbyIntegrationTest : public ::testing::Test {
   protected:
    void SetUp() override {
        // Generate a unique temp directory per test
        static std::atomic<int> counter{0};
        test_dir_ = "/tmp/localfs_ha_test_" + std::to_string(getpid()) + "_" +
                    std::to_string(counter.fetch_add(1));
        cluster_id_ = "localfs_ha_cluster";
        poll_interval_ms_ = 100;  // fast polling for tests

        std::filesystem::create_directories(test_dir_);
    }

    void TearDown() override {
        std::error_code ec;
        std::filesystem::remove_all(test_dir_, ec);
        if (ec) {
            LOG(WARNING) << "Failed to remove test dir " << test_dir_ << ": "
                         << ec.message();
        }
    }

    // -- Helpers --

    std::unique_ptr<OpLogManager> CreatePrimaryOpLogManager() {
        auto store = OpLogStoreFactory::Create(
            OpLogStoreType::LOCAL_FS, cluster_id_, OpLogStoreRole::WRITER,
            test_dir_, poll_interval_ms_);
        if (!store) return nullptr;
        auto mgr = std::make_unique<OpLogManager>();
        mgr->SetOpLogStore(std::shared_ptr<OpLogStore>(std::move(store)));
        return mgr;
    }

    HotStandbyConfig MakeHotStandbyConfig() {
        HotStandbyConfig cfg;
        cfg.enable_verification = false;
        cfg.max_replication_lag_entries = 1000;
        cfg.oplog_store_type = OpLogStoreType::LOCAL_FS;
        cfg.oplog_store_root_dir = test_dir_;
        cfg.oplog_poll_interval_ms = poll_interval_ms_;
        return cfg;
    }

    bool WaitForSync(HotStandbyService& standby, uint64_t target_seq,
                     int timeout_sec = 30) {
        auto deadline = std::chrono::steady_clock::now() +
                        std::chrono::seconds(timeout_sec);
        while (std::chrono::steady_clock::now() < deadline) {
            auto status = standby.GetSyncStatus();
            LOG(INFO) << "Standby: state=" << StandbyStateToString(status.state)
                      << ", applied_seq_id=" << status.applied_seq_id
                      << ", primary_seq_id=" << status.primary_seq_id
                      << ", lag_entries=" << status.lag_entries;
            if (status.state == StandbyState::WATCHING &&
                status.lag_entries == 0 &&
                status.applied_seq_id >= target_seq) {
                return true;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(200));
        }
        return false;
    }

    std::string test_dir_;
    std::string cluster_id_;
    int poll_interval_ms_;
};

// ============================================================
// Test cases
// ============================================================

TEST_F(LocalFsHotStandbyIntegrationTest, TestPrimaryStandbySync) {
    // 1. Primary writes 10 entries (last one via AppendAndPersist to flush)
    auto primary = CreatePrimaryOpLogManager();
    ASSERT_NE(primary, nullptr);

    std::vector<std::string> test_keys;
    std::string payload =
        R"({"client_id_first":1,"client_id_second":2,"size":1024,"replicas":[]})";

    for (int i = 0; i < 10; ++i) {
        std::string key = "test_key_" + std::to_string(i);
        if (i < 9) {
            primary->Append(OpType::PUT_END, key, payload);
        } else {
            auto result =
                primary->AppendAndPersist(OpType::PUT_END, key, payload);
            ASSERT_TRUE(result.has_value())
                << "AppendAndPersist failed for last entry";
        }
        test_keys.push_back(key);
    }

    uint64_t last_seq_id = primary->GetLastSequenceId();
    LOG(INFO) << "Primary wrote " << last_seq_id << " OpLog entries";

    // 2. Start the standby
    HotStandbyService standby(MakeHotStandbyConfig());
    StandbyServiceGuard guard(&standby);

    ASSERT_EQ(ErrorCode::OK,
              standby.Start("", /*oplog_endpoints=*/"", cluster_id_));

    // 3. Wait for sync
    ASSERT_TRUE(WaitForSync(standby, last_seq_id))
        << "Standby failed to sync within timeout";

    // 4. Verify metadata snapshot
    std::vector<std::pair<std::string, StandbyObjectMetadata>> snapshot;
    ASSERT_TRUE(standby.ExportMetadataSnapshot(snapshot));
    LOG(INFO) << "Standby metadata snapshot size: " << snapshot.size();

    std::set<std::string> snapshot_keys;
    for (const auto& kv : snapshot) {
        snapshot_keys.insert(kv.first);
    }
    for (const auto& key : test_keys) {
        EXPECT_NE(snapshot_keys.end(), snapshot_keys.find(key))
            << "Key " << key << " not found in Standby snapshot";
    }
    EXPECT_GE(snapshot.size(), test_keys.size());

    // 5. Verify sequence IDs
    EXPECT_GE(standby.GetLatestAppliedSequenceId(), last_seq_id);
}

TEST_F(LocalFsHotStandbyIntegrationTest, TestStandbyPromotion) {
    // 1. Write entries
    auto primary = CreatePrimaryOpLogManager();
    ASSERT_NE(primary, nullptr);

    std::vector<std::string> test_keys;
    std::string payload =
        R"({"client_id_first":1,"client_id_second":2,"size":1024,"replicas":[]})";

    for (int i = 0; i < 5; ++i) {
        std::string key = "promote_test_key_" + std::to_string(i);
        if (i < 4) {
            primary->Append(OpType::PUT_END, key, payload);
        } else {
            auto result =
                primary->AppendAndPersist(OpType::PUT_END, key, payload);
            ASSERT_TRUE(result.has_value());
        }
        test_keys.push_back(key);
    }

    uint64_t last_seq_id = primary->GetLastSequenceId();

    // 2. Start standby and wait for sync
    HotStandbyService standby(MakeHotStandbyConfig());
    StandbyServiceGuard guard(&standby);

    ASSERT_EQ(ErrorCode::OK, standby.Start("", "", cluster_id_));

    ASSERT_TRUE(WaitForSync(standby, last_seq_id));

    // 3. Verify ready for promotion
    ASSERT_TRUE(standby.IsReadyForPromotion());

    // 4. Promote
    EXPECT_EQ(ErrorCode::OK, standby.Promote());

    // 5. Verify sequence ID
    EXPECT_GE(standby.GetLatestAppliedSequenceId(), last_seq_id);

    // 6. Verify metadata preserved
    std::vector<std::pair<std::string, StandbyObjectMetadata>> snapshot;
    ASSERT_TRUE(standby.ExportMetadataSnapshot(snapshot));

    std::set<std::string> snapshot_keys;
    for (const auto& kv : snapshot) {
        snapshot_keys.insert(kv.first);
    }
    for (const auto& key : test_keys) {
        EXPECT_NE(snapshot_keys.end(), snapshot_keys.find(key))
            << "Key " << key << " should be in snapshot after promotion";
    }
}

TEST_F(LocalFsHotStandbyIntegrationTest, TestFailoverScenario) {
    // 1. Primary writes data
    auto primary = CreatePrimaryOpLogManager();
    ASSERT_NE(primary, nullptr);

    std::vector<std::string> test_keys;
    std::string payload =
        R"({"client_id_first":1,"client_id_second":2,"size":1024,"replicas":[]})";

    for (int i = 0; i < 10; ++i) {
        std::string key = "failover_key_" + std::to_string(i);
        if (i < 9) {
            primary->Append(OpType::PUT_END, key, payload);
        } else {
            auto result =
                primary->AppendAndPersist(OpType::PUT_END, key, payload);
            ASSERT_TRUE(result.has_value());
        }
        test_keys.push_back(key);
    }

    uint64_t last_seq_id = primary->GetLastSequenceId();

    // 2. Start standby
    HotStandbyService standby(MakeHotStandbyConfig());
    StandbyServiceGuard guard(&standby);

    ASSERT_EQ(ErrorCode::OK, standby.Start("", "", cluster_id_));

    ASSERT_TRUE(WaitForSync(standby, last_seq_id));

    // 3. Simulate primary failure (destroy the OpLogManager)
    primary.reset();

    // 4. Verify standby data integrity
    std::vector<std::pair<std::string, StandbyObjectMetadata>> snapshot;
    ASSERT_TRUE(standby.ExportMetadataSnapshot(snapshot));

    std::set<std::string> snapshot_keys;
    for (const auto& kv : snapshot) {
        snapshot_keys.insert(kv.first);
    }
    for (const auto& key : test_keys) {
        EXPECT_NE(snapshot_keys.end(), snapshot_keys.find(key))
            << "Key " << key << " should be in Standby after Primary failure";
    }

    // 5. Promote
    ASSERT_TRUE(standby.IsReadyForPromotion());
    EXPECT_EQ(ErrorCode::OK, standby.Promote());

    // 6. Verify state
    EXPECT_GE(standby.GetLatestAppliedSequenceId(), last_seq_id);
}

TEST_F(LocalFsHotStandbyIntegrationTest, TestDataConsistency) {
    // 1. Mixed PUT and REMOVE operations
    auto primary = CreatePrimaryOpLogManager();
    ASSERT_NE(primary, nullptr);

    std::map<std::string, bool> expected_keys;  // key -> should_exist

    // PUT 5 keys
    for (int i = 0; i < 5; ++i) {
        std::string key = "put_key_" + std::to_string(i);
        std::string payload =
            R"({"client_id_first":1,"client_id_second":2,"size":1024,"replicas":[]})";
        primary->Append(OpType::PUT_END, key, payload);
        expected_keys[key] = true;
    }

    // REMOVE first 2
    for (int i = 0; i < 2; ++i) {
        std::string key = "put_key_" + std::to_string(i);
        primary->Append(OpType::REMOVE, key, "");
        expected_keys[key] = false;
    }

    // PUT 3 more (last one sync)
    for (int i = 5; i < 8; ++i) {
        std::string key = "put_key_" + std::to_string(i);
        std::string payload =
            R"({"client_id_first":1,"client_id_second":2,"size":2048,"replicas":[]})";
        if (i < 7) {
            primary->Append(OpType::PUT_END, key, payload);
        } else {
            auto result =
                primary->AppendAndPersist(OpType::PUT_END, key, payload);
            ASSERT_TRUE(result.has_value());
        }
        expected_keys[key] = true;
    }

    uint64_t last_seq_id = primary->GetLastSequenceId();
    LOG(INFO) << "Primary wrote " << last_seq_id << " OpLog entries";

    // 2. Start standby
    HotStandbyService standby(MakeHotStandbyConfig());
    StandbyServiceGuard guard(&standby);

    ASSERT_EQ(ErrorCode::OK, standby.Start("", "", cluster_id_));

    // 3. Wait for sync
    ASSERT_TRUE(WaitForSync(standby, last_seq_id));

    // 4. Verify consistency
    std::vector<std::pair<std::string, StandbyObjectMetadata>> snapshot;
    ASSERT_TRUE(standby.ExportMetadataSnapshot(snapshot));

    std::set<std::string> actual_keys;
    for (const auto& kv : snapshot) {
        actual_keys.insert(kv.first);
    }

    for (const auto& kv : expected_keys) {
        if (kv.second) {
            EXPECT_NE(actual_keys.end(), actual_keys.find(kv.first))
                << "Key " << kv.first << " should exist but not found";
        } else {
            EXPECT_EQ(actual_keys.end(), actual_keys.find(kv.first))
                << "Key " << kv.first << " should be removed but still exists";
        }
    }

    size_t expected_count = 0;
    for (const auto& kv : expected_keys) {
        if (kv.second) expected_count++;
    }
    EXPECT_EQ(expected_count, actual_keys.size());
}

TEST_F(LocalFsHotStandbyIntegrationTest, TestHighThroughputSync) {
    // 1. Create primary first (initializes directory structure)
    auto primary = CreatePrimaryOpLogManager();
    ASSERT_NE(primary, nullptr);

    // 2. Start standby (will poll for new entries)
    HotStandbyService standby(MakeHotStandbyConfig());
    StandbyServiceGuard guard(&standby);

    ASSERT_EQ(ErrorCode::OK, standby.Start("", "", cluster_id_));

    // Wait for WATCHING state
    {
        auto deadline =
            std::chrono::steady_clock::now() + std::chrono::seconds(10);
        while (std::chrono::steady_clock::now() < deadline) {
            if (standby.GetSyncStatus().state == StandbyState::WATCHING) {
                break;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
    }

    // 3. High-throughput writes (last one sync)

    const int num_writes = 100;
    std::string payload =
        R"({"client_id_first":1,"client_id_second":2,"size":1024,"replicas":[]})";

    auto write_start = std::chrono::steady_clock::now();
    for (int i = 0; i < num_writes; ++i) {
        std::string key = "throughput_key_" + std::to_string(i);
        if (i < num_writes - 1) {
            primary->Append(OpType::PUT_END, key, payload);
        } else {
            auto result =
                primary->AppendAndPersist(OpType::PUT_END, key, payload);
            ASSERT_TRUE(result.has_value());
        }
    }
    auto write_end = std::chrono::steady_clock::now();
    auto write_duration = std::chrono::duration_cast<std::chrono::milliseconds>(
        write_end - write_start);

    uint64_t last_seq_id = primary->GetLastSequenceId();
    LOG(INFO) << "Wrote " << num_writes << " entries in "
              << write_duration.count() << "ms, last_seq_id=" << last_seq_id;

    // 3. Monitor lag
    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
    uint64_t max_lag = 0;

    while (std::chrono::steady_clock::now() < deadline) {
        auto status = standby.GetSyncStatus();
        if (status.lag_entries > max_lag) {
            max_lag = status.lag_entries;
        }
        LOG(INFO) << "Standby lag: " << status.lag_entries
                  << " entries, applied_seq_id=" << status.applied_seq_id
                  << ", primary_seq_id=" << status.primary_seq_id;
        if (status.applied_seq_id >= last_seq_id && status.lag_entries == 0) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }

    // 4. Verify
    auto final_status = standby.GetSyncStatus();
    EXPECT_GE(final_status.applied_seq_id, last_seq_id)
        << "Standby should have applied all entries";
    EXPECT_EQ(0u, final_status.lag_entries)
        << "Standby lag should be zero after sync";

    LOG(INFO) << "Max lag observed: " << max_lag << " entries";
}

}  // namespace testing
}  // namespace mooncake

int main(int argc, char** argv) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    google::InitGoogleLogging(argv[0]);
    google::SetVLOGLevel("*", 1);
    FLAGS_logtostderr = 1;
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
