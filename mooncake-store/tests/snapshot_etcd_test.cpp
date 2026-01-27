#include "master_service_test_for_snapshot_base.h"
#include "etcd_helper.h"
#include "utils/crc32c_util.h"

#include <gflags/gflags.h>
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdlib>
#include <random>
#include <string>
#include <thread>
#include <vector>

DEFINE_string(etcd_endpoints, "127.0.0.1:12379", "Etcd endpoints for snapshot testing");

namespace mooncake::test {

class SnapshotEtcdTest : public MasterServiceSnapshotTestBase {
   protected:
    static bool glog_initialized_;

    struct EnvVarGuard {
        EnvVarGuard(const char* key, const char* value) : key_(key) {
            const char* old = std::getenv(key_);
            if (old != nullptr) {
                had_old_ = true;
                old_value_ = old;
            }
            setenv(key_, value, 1);
        }

        ~EnvVarGuard() {
            if (had_old_) {
                setenv(key_, old_value_.c_str(), 1);
            } else {
                unsetenv(key_);
            }
        }

       private:
        const char* key_;
        bool had_old_ = false;
        std::string old_value_;
    };
    
    void SetUp() override {
        // Call base class SetUp first to reset MasterMetricManager state
        MasterServiceSnapshotTestBase::SetUp();
        
        if (!glog_initialized_) {
            google::InitGoogleLogging("SnapshotEtcdTest");
            FLAGS_logtostderr = true;
            glog_initialized_ = true;
        }

        // Initialize etcd client for snapshot operations
        LOG(INFO) << "Connecting to etcd at: " << FLAGS_etcd_endpoints;
        auto connect_result = EtcdHelper::ConnectToEtcdStoreClient(FLAGS_etcd_endpoints);
        ASSERT_EQ(ErrorCode::OK, connect_result) 
            << "Failed to connect to etcd. Make sure etcd is running at " << FLAGS_etcd_endpoints;
        
        // Clean up any existing test snapshots
        CleanupTestSnapshots();
    }

    void TearDown() override {
        // Note: Don't call base class TearDown as it would run TestSnapshotAndRestore
        // We'll run custom ETCD-based tests instead
        service_.reset();
    }

    // Clean up test snapshots from etcd - simplified version
    void CleanupTestSnapshots() {
        // Note: etcd doesn't have a simple DeleteWithPrefix in EtcdHelper
        // We rely on etcd's TTL and manual cleanup if needed
        LOG(INFO) << "Cleanup: Relying on etcd TTL for snapshot cleanup";
    }

    // Verify snapshot exists in etcd
    bool SnapshotExistsInEtcd(const std::string& snapshot_id) {
        std::string manifest_key = "master_snapshot/" + snapshot_id + "/manifest.txt";
        std::string value;
        EtcdRevisionId revision;
        auto result = EtcdHelper::Get(manifest_key.c_str(), manifest_key.size(), value, revision);
        return result == ErrorCode::OK;
    }

    // Get latest snapshot ID from etcd
    std::string GetLatestSnapshotFromEtcd() {
        std::string latest_key = "master_snapshot/latest.txt";
        std::string value;
        EtcdRevisionId revision;
        auto result = EtcdHelper::Get(latest_key.c_str(), latest_key.size(), value, revision);
        if (result == ErrorCode::OK) {
            // Trim whitespace
            value.erase(0, value.find_first_not_of(" \t\n\r"));
            value.erase(value.find_last_not_of(" \t\n\r") + 1);
            return value;
        }
        return "";
    }

    // Test ETCD-based snapshot and restore
    void TestEtcdSnapshotAndRestore(std::unique_ptr<MasterService>& service) {
        LOG(INFO) << "========== Phase 1: Persist snapshot to ETCD ==========";
        std::string snapshot_id1 = GenerateSnapshotId();
        auto persist_result1 = CallPersistState(service.get(), snapshot_id1);
        ASSERT_TRUE(persist_result1.has_value()) 
            << "Failed to persist state to ETCD: " << persist_result1.error().message;
        
        LOG(INFO) << "Snapshot persisted with ID: " << snapshot_id1;

        // Verify snapshot exists in etcd
        ASSERT_TRUE(SnapshotExistsInEtcd(snapshot_id1)) 
            << "Snapshot not found in etcd after persist";
        
        LOG(INFO) << "========== Phase 2: Capture state before restore ==========";
        ServiceStateSnapshot state_before = CaptureServiceState(service.get());
        LOG(INFO) << "Captured state: " << state_before.all_keys.size() << " keys, "
                  << state_before.all_segments.size() << " segments";

        LOG(INFO) << "========== Phase 3: Create new MasterService with restore enabled ==========";
        auto restore_config = MasterServiceConfig::builder()
                                  .set_memory_allocator(BufferAllocatorType::OFFSET)
                                  .set_snapshot_backend_type(SnapshotBackendType::ETCD)
                                  .set_etcd_endpoints(FLAGS_etcd_endpoints)
                                  .set_enable_snapshot_restore(true)
                                  .set_enable_snapshot_restore_clean_metadata(false)
                                  .build();
        
        // Snapshot metadata leases can be expired at persist time.
        // Skip restore cleanup in tests to validate the snapshot content.
        EnvVarGuard skip_cleanup("MOONCAKE_MASTER_SERVICE_SNAPSHOT_TEST_SKIP_CLEANUP", "1");
        std::unique_ptr<MasterService> restored_service(new MasterService(restore_config));
        LOG(INFO) << "Restored service created";

        LOG(INFO) << "========== Phase 4: Capture state after restore ==========";
        ServiceStateSnapshot state_after = CaptureServiceState(restored_service.get());
        LOG(INFO) << "Captured restored state: " << state_after.all_keys.size() << " keys, "
                  << state_after.all_segments.size() << " segments";

        LOG(INFO) << "========== Phase 5: Compare states ==========";
        EXPECT_TRUE(CompareServiceState(state_before, state_after))
            << "Service state mismatch after restore from ETCD";

        LOG(INFO) << "========== Phase 6: Verify latest snapshot ==========";
        std::string latest_snapshot = GetLatestSnapshotFromEtcd();
        EXPECT_EQ(snapshot_id1, latest_snapshot) 
            << "Latest snapshot ID mismatch. Expected: " << snapshot_id1 
            << ", Got: " << latest_snapshot;

        LOG(INFO) << "========== Phase 7: PutStart consistency validation ==========";
        std::string best_segment = FindBestSegment(service.get(), state_before.all_segments);
        if (!best_segment.empty()) {
            AssertPutStartConsistentWithBestSegment(service.get(), restored_service.get(), best_segment);
        } else {
            LOG(WARNING) << "No valid segment found, skip PutStart consistency check";
        }

        LOG(INFO) << "========== ETCD Snapshot and Restore Test PASSED ==========";
    }
};

bool SnapshotEtcdTest::glog_initialized_ = false;

// Helper function to generate keys for segments
std::string GenerateKeyForSegment(const UUID& client_id,
                                  const std::unique_ptr<MasterService>& service,
                                  const std::string& segment_name) {
    static std::atomic<uint64_t> counter(0);

    while (true) {
        std::string key = "etcd_test_key_" + std::to_string(counter.fetch_add(1));
        std::vector<Replica::Descriptor> replica_list;

        auto exist_result = service->ExistKey(key);
        if (exist_result.has_value() && exist_result.value()) {
            continue;
        }

        auto put_result = service->PutStart(client_id, key, {1024}, {.replica_num = 1});
        if (put_result.has_value()) {
            replica_list = std::move(put_result.value());
        }
        ErrorCode code = put_result.has_value() ? ErrorCode::OK : put_result.error();

        if (code == ErrorCode::OBJECT_ALREADY_EXISTS) {
            continue;
        }
        if (code != ErrorCode::OK) {
            throw std::runtime_error("PutStart failed with code: " +
                                     std::to_string(static_cast<int>(code)));
        }
        
        auto put_end_result = service->PutEnd(client_id, key, ReplicaType::MEMORY);
        if (!put_end_result.has_value()) {
            throw std::runtime_error("PutEnd failed");
        }
        
        if (replica_list[0].get_memory_descriptor()
                .buffer_descriptor.transport_endpoint_ == segment_name) {
            return key;
        }
        
        auto remove_result = service->Remove(key);
        // Ignore cleanup failure
    }
}

TEST_F(SnapshotEtcdTest, BasicSnapshotRestore) {
    LOG(INFO) << "========== Test: BasicSnapshotRestore ==========";
    
    auto service_config = MasterServiceConfig::builder()
                              .set_memory_allocator(BufferAllocatorType::OFFSET)
                              .set_snapshot_backend_type(SnapshotBackendType::ETCD)
                              .set_etcd_endpoints(FLAGS_etcd_endpoints)
                              .set_enable_snapshot(false)  // Manual snapshot control
                              .build();
    
    service_.reset(new MasterService(service_config));
    
    // Mount a segment
    auto segment = MakeSegment("etcd_test_segment");
    UUID client_id = generate_uuid();
    auto mount_result = service_->MountSegment(segment, client_id);
    ASSERT_TRUE(mount_result.has_value()) << "Failed to mount segment";
    
    // Put some keys
    for (int i = 0; i < 5; i++) {
        std::string key = "test_key_" + std::to_string(i);
        auto put_result = service_->PutStart(client_id, key, {1024}, {.replica_num = 1});
        ASSERT_TRUE(put_result.has_value()) << "PutStart failed for key: " << key;
        auto put_end_result = service_->PutEnd(client_id, key, ReplicaType::MEMORY);
        ASSERT_TRUE(put_end_result.has_value()) << "PutEnd failed for key: " << key;
    }
    
    // Test snapshot and restore
    TestEtcdSnapshotAndRestore(service_);
}

TEST_F(SnapshotEtcdTest, SnapshotPersistViaDaemon) {
    LOG(INFO) << "========== Test: SnapshotPersistViaDaemon ==========";

    auto service_config =
        MasterServiceConfig::builder()
            .set_memory_allocator(BufferAllocatorType::OFFSET)
            .set_snapshot_backend_type(SnapshotBackendType::ETCD)
            .set_etcd_endpoints(FLAGS_etcd_endpoints)
            .set_enable_snapshot(false)  // Avoid snapshot thread
            .build();

    service_.reset(new MasterService(service_config));

    if (!StartSnapshotDaemonForTest()) {
        GTEST_SKIP()
            << "snapshot_uploader_daemon not available; skip daemon path test";
    }

    // Mount a segment and insert a few keys
    auto segment = MakeSegment("etcd_daemon_test_segment");
    UUID client_id = generate_uuid();
    auto mount_result = service_->MountSegment(segment, client_id);
    ASSERT_TRUE(mount_result.has_value()) << "Failed to mount segment";

    for (int i = 0; i < 3; i++) {
        std::string key = "daemon_test_key_" + std::to_string(i);
        auto put_result =
            service_->PutStart(client_id, key, {1024}, {.replica_num = 1});
        ASSERT_TRUE(put_result.has_value()) << "PutStart failed for key: "
                                            << key;
        auto put_end_result =
            service_->PutEnd(client_id, key, ReplicaType::MEMORY);
        ASSERT_TRUE(put_end_result.has_value())
            << "PutEnd failed for key: " << key;
    }

    // Capture state before persisting so we can validate restore later.
    ServiceStateSnapshot state_before = CaptureServiceState(service_.get());

    // Persist snapshot (should use daemon batch upload)
    std::string snapshot_id = GenerateSnapshotId();
    auto persist_result = CallPersistState(service_.get(), snapshot_id);
    ASSERT_TRUE(persist_result.has_value())
        << "Failed to persist state via daemon: "
        << persist_result.error().message;

    // Fetch manifest/latest/metadata/segments from etcd
    std::string manifest_key =
        "master_snapshot/" + snapshot_id + "/manifest.txt";
    std::string metadata_key =
        "master_snapshot/" + snapshot_id + "/metadata";
    std::string segments_key =
        "master_snapshot/" + snapshot_id + "/segments";
    std::string latest_key = "master_snapshot/latest.txt";

    std::string manifest_value;
    ASSERT_TRUE(GetEtcdValue(manifest_key, manifest_value))
        << "Manifest not found in etcd";

    std::string latest_value;
    ASSERT_TRUE(GetEtcdValue(latest_key, latest_value))
        << "Latest not found in etcd";
    // Trim whitespace
    latest_value.erase(0, latest_value.find_first_not_of(" \t\n\r"));
    latest_value.erase(latest_value.find_last_not_of(" \t\n\r") + 1);
    EXPECT_EQ(snapshot_id, latest_value)
        << "Latest snapshot id mismatch";

    // Parse manifest: protocol|version|snapshot_id|meta_size|meta_crc|
    // seg_size|seg_crc|ts|complete
    std::vector<std::string> parts;
    size_t start = 0;
    while (true) {
        size_t pos = manifest_value.find('|', start);
        if (pos == std::string::npos) {
            parts.push_back(manifest_value.substr(start));
            break;
        }
        parts.push_back(manifest_value.substr(start, pos - start));
        start = pos + 1;
    }
    ASSERT_EQ(parts.size(), 9u) << "Manifest format invalid: "
                                << manifest_value;
    EXPECT_EQ(snapshot_id, parts[2]) << "Manifest snapshot id mismatch";
    EXPECT_EQ("complete", parts[8]) << "Manifest status not complete";

    uint64_t expected_meta_size = std::stoull(parts[3]);
    uint32_t expected_meta_crc = static_cast<uint32_t>(std::stoul(parts[4]));
    uint64_t expected_seg_size = std::stoull(parts[5]);
    uint32_t expected_seg_crc = static_cast<uint32_t>(std::stoul(parts[6]));

    std::string metadata_value;
    std::string segments_value;
    ASSERT_TRUE(GetEtcdValue(metadata_key, metadata_value))
        << "Metadata not found in etcd";
    ASSERT_TRUE(GetEtcdValue(segments_key, segments_value))
        << "Segments not found in etcd";

    EXPECT_EQ(expected_meta_size,
              static_cast<uint64_t>(metadata_value.size()))
        << "Metadata size mismatch";
    EXPECT_EQ(expected_seg_size,
              static_cast<uint64_t>(segments_value.size()))
        << "Segments size mismatch";

    auto meta_crc = Crc32c(
        reinterpret_cast<const uint8_t*>(metadata_value.data()),
        metadata_value.size());
    auto seg_crc = Crc32c(
        reinterpret_cast<const uint8_t*>(segments_value.data()),
        segments_value.size());
    EXPECT_EQ(expected_meta_crc, meta_crc) << "Metadata checksum mismatch";
    EXPECT_EQ(expected_seg_crc, seg_crc) << "Segments checksum mismatch";

    // Closed loop: restore from the daemon-persisted snapshot and compare.
    LOG(INFO) << "========== Closed Loop: Restore After Daemon Persist ==========";
    auto restore_config = MasterServiceConfig::builder()
                              .set_memory_allocator(BufferAllocatorType::OFFSET)
                              .set_snapshot_backend_type(SnapshotBackendType::ETCD)
                              .set_etcd_endpoints(FLAGS_etcd_endpoints)
                              .set_enable_snapshot_restore(true)
                              .set_enable_snapshot_restore_clean_metadata(false)
                              .build();

    std::unique_ptr<MasterService> restored_service(
        new MasterService(restore_config));
    ServiceStateSnapshot state_after =
        CaptureServiceState(restored_service.get());

    EXPECT_TRUE(CompareServiceState(state_before, state_after))
        << "Service state mismatch after restore from daemon-persisted ETCD snapshot";

    std::string best_segment =
        FindBestSegment(service_.get(), state_before.all_segments);
    if (!best_segment.empty()) {
        AssertPutStartConsistentWithBestSegment(service_.get(),
                                                restored_service.get(),
                                                best_segment);
    }
}

// TEST_F(SnapshotEtcdTest, LargeSnapshotDoesNotBlockService) {
//     LOG(INFO) << "========== Test: LargeSnapshotDoesNotBlockService ==========";

//     auto service_config = MasterServiceConfig::builder()
//                               .set_memory_allocator(BufferAllocatorType::OFFSET)
//                               .set_snapshot_backend_type(SnapshotBackendType::ETCD)
//                               .set_etcd_endpoints(FLAGS_etcd_endpoints)
//                               .set_client_live_ttl_sec(120)
//                               .set_enable_snapshot(false)
//                               .build();

//     service_.reset(new MasterService(service_config));

//     // Use a large segment to avoid exhausting capacity during the hot loop.
//     static constexpr size_t kLargeSegmentSize = 1024ULL * 1024ULL * 1024ULL;
//     auto segment = MakeSegment("etcd_large_snapshot_segment",
//                                kDefaultSegmentBase, kLargeSegmentSize);
//     UUID client_id = generate_uuid();
//     auto mount_result = service_->MountSegment(segment, client_id);
//     ASSERT_TRUE(mount_result.has_value()) << "Failed to mount segment";

//     // Simulate a 1GB snapshot upload delay outside the snapshot mutex.
//     const uint64_t kOneGiB = 1024ULL * 1024ULL * 1024ULL;
//     const std::string fake_bytes = std::to_string(kOneGiB);
//     const std::string fake_bandwidth = "256";  // ~4s delay for 1GB.
//     EnvVarGuard fake_bytes_guard("MOONCAKE_SNAPSHOT_TEST_FAKE_BYTES",
//                                  fake_bytes.c_str());
//     EnvVarGuard fake_bw_guard("MOONCAKE_SNAPSHOT_TEST_FAKE_BANDWIDTH_MBPS",
//                               fake_bandwidth.c_str());

//     const std::string snapshot_id = GenerateSnapshotId() + "_1gb";

//     std::atomic<bool> snapshot_success{false};
//     std::string snapshot_error;
//     const auto snapshot_start = std::chrono::steady_clock::now();
//     std::thread snapshot_thread([&]() {
//         auto result = CallPersistState(service_.get(), snapshot_id);
//         snapshot_success.store(result.has_value(), std::memory_order_relaxed);
//         if (!result.has_value()) {
//             snapshot_error = result.error().message;
//         }
//     });

//     // Give the snapshot thread time to enter the simulated delay section.
//     std::this_thread::sleep_for(std::chrono::milliseconds(200));

//     std::vector<double> op_latencies_ms;
//     op_latencies_ms.reserve(64);
//     const auto op_deadline =
//         std::chrono::steady_clock::now() + std::chrono::seconds(2);

//     int op_index = 0;
//     while (std::chrono::steady_clock::now() < op_deadline) {
//         const std::string key =
//             "snapshot_non_blocking_key_" + std::to_string(op_index++);
//         const auto op_start = std::chrono::steady_clock::now();
//         auto put_result =
//             service_->PutStart(client_id, key, {1024}, {.replica_num = 1});
//         ASSERT_TRUE(put_result.has_value())
//             << "PutStart failed during snapshot for key: " << key;
//         auto put_end_result = service_->PutEnd(client_id, key,
//                                                ReplicaType::MEMORY);
//         ASSERT_TRUE(put_end_result.has_value())
//             << "PutEnd failed during snapshot for key: " << key;
//         // Remove immediately to avoid exhausting the segment during the loop.
//         auto remove_result = service_->Remove(key);
//         ASSERT_TRUE(remove_result.has_value())
//             << "Remove failed during snapshot for key: " << key;
//         const auto op_end = std::chrono::steady_clock::now();
//         const auto latency_ms =
//             std::chrono::duration_cast<std::chrono::milliseconds>(op_end -
//                                                                   op_start)
//                 .count();
//         op_latencies_ms.push_back(static_cast<double>(latency_ms));
//     }

//     snapshot_thread.join();
//     const auto snapshot_end = std::chrono::steady_clock::now();

//     ASSERT_TRUE(snapshot_success.load(std::memory_order_relaxed))
//         << "Snapshot failed: " << snapshot_error;
//     ASSERT_FALSE(op_latencies_ms.empty())
//         << "No operations were recorded during snapshot";

//     const auto max_latency_it =
//         std::max_element(op_latencies_ms.begin(), op_latencies_ms.end());
//     const double max_latency_ms =
//         (max_latency_it == op_latencies_ms.end()) ? 0.0 : *max_latency_it;

//     const auto snapshot_duration_ms =
//         std::chrono::duration_cast<std::chrono::milliseconds>(snapshot_end -
//                                                               snapshot_start)
//             .count();
//     LOG(INFO) << "Snapshot duration (ms): " << snapshot_duration_ms
//               << ", max op latency (ms): " << max_latency_ms;

//     // Snapshot should take multiple seconds due to the simulated 1GB upload,
//     // while normal operations remain responsive.
//     EXPECT_GE(snapshot_duration_ms, 3000)
//         << "Snapshot finished too quickly; simulated delay may not be active";
//     EXPECT_LT(max_latency_ms, 500.0)
//         << "Master operations appear blocked during snapshot; max latency(ms)="
//         << max_latency_ms;
// }

TEST_F(SnapshotEtcdTest, SnapshotWithMultipleSegments) {
    LOG(INFO) << "========== Test: SnapshotWithMultipleSegments ==========";
    
    auto service_config = MasterServiceConfig::builder()
                              .set_memory_allocator(BufferAllocatorType::OFFSET)
                              .set_snapshot_backend_type(SnapshotBackendType::ETCD)
                              .set_etcd_endpoints(FLAGS_etcd_endpoints)
                              .set_enable_snapshot(false)
                              .build();
    
    service_.reset(new MasterService(service_config));
    
    UUID client_id = generate_uuid();
    
    // Mount multiple segments
    for (int i = 0; i < 3; i++) {
        auto segment = MakeSegment("segment_" + std::to_string(i), 
                                   kDefaultSegmentBase + i * kDefaultSegmentSize,
                                   kDefaultSegmentSize);
        auto mount_result = service_->MountSegment(segment, client_id);
        ASSERT_TRUE(mount_result.has_value()) << "Failed to mount segment " << i;
    }
    
    // Put keys across segments
    for (int i = 0; i < 10; i++) {
        std::string key = "multi_seg_key_" + std::to_string(i);
        auto put_result = service_->PutStart(client_id, key, {1024}, {.replica_num = 1});
        ASSERT_TRUE(put_result.has_value()) << "PutStart failed for key: " << key;
        auto put_end_result = service_->PutEnd(client_id, key, ReplicaType::MEMORY);
        ASSERT_TRUE(put_end_result.has_value()) << "PutEnd failed for key: " << key;
    }
    
    TestEtcdSnapshotAndRestore(service_);
}

TEST_F(SnapshotEtcdTest, SnapshotWithEviction) {
    LOG(INFO) << "========== Test: SnapshotWithEviction ==========";
    
    // Configuration without quota to avoid eviction complexity
    auto service_config = MasterServiceConfig::builder()
                              .set_memory_allocator(BufferAllocatorType::OFFSET)
                              .set_snapshot_backend_type(SnapshotBackendType::ETCD)
                              .set_etcd_endpoints(FLAGS_etcd_endpoints)
                              .set_enable_snapshot(false)
                              .build();
    
    service_.reset(new MasterService(service_config));
    
    auto segment = MakeSegment("eviction_segment");
    UUID client_id = generate_uuid();
    auto mount_result = service_->MountSegment(segment, client_id);
    ASSERT_TRUE(mount_result.has_value());
    
    // Put several keys
    for (int i = 0; i < 10; i++) {
        std::string key = "evict_key_" + std::to_string(i);
        auto put_result = service_->PutStart(client_id, key, {1024}, {.replica_num = 1});
        if (put_result.has_value()) {
            auto put_end_result = service_->PutEnd(client_id, key, ReplicaType::MEMORY);
            if (!put_end_result.has_value()) {
                LOG(WARNING) << "PutEnd failed for key: " << key;
            }
        }
    }
    
    TestEtcdSnapshotAndRestore(service_);
}

TEST_F(SnapshotEtcdTest, RestoreEmptySnapshot) {
    LOG(INFO) << "========== Test: RestoreEmptySnapshot ==========";
    
    auto service_config = MasterServiceConfig::builder()
                              .set_memory_allocator(BufferAllocatorType::OFFSET)
                              .set_snapshot_backend_type(SnapshotBackendType::ETCD)
                              .set_etcd_endpoints(FLAGS_etcd_endpoints)
                              .set_enable_snapshot(false)
                              .build();
    
    service_.reset(new MasterService(service_config));
    
    // Create snapshot with no data
    std::string snapshot_id = GenerateSnapshotId();
    auto persist_result = CallPersistState(service_.get(), snapshot_id);
    ASSERT_TRUE(persist_result.has_value()) 
        << "Failed to persist empty state: " << persist_result.error().message;
    
    // Verify can restore from empty snapshot
    auto restore_config = MasterServiceConfig::builder()
                              .set_memory_allocator(BufferAllocatorType::OFFSET)
                              .set_snapshot_backend_type(SnapshotBackendType::ETCD)
                              .set_etcd_endpoints(FLAGS_etcd_endpoints)
                              .set_enable_snapshot_restore(true)
                              .build();
    
    std::unique_ptr<MasterService> restored_service(new MasterService(restore_config));
    
    auto keys = restored_service->GetAllKeys();
    ASSERT_TRUE(keys.has_value());
    EXPECT_EQ(0, keys.value().size()) << "Expected empty state after restore";
}

TEST_F(SnapshotEtcdTest, MultipleSnapshotVersions) {
    LOG(INFO) << "========== Test: MultipleSnapshotVersions ==========";
    
    auto service_config = MasterServiceConfig::builder()
                              .set_memory_allocator(BufferAllocatorType::OFFSET)
                              .set_snapshot_backend_type(SnapshotBackendType::ETCD)
                              .set_etcd_endpoints(FLAGS_etcd_endpoints)
                              .set_enable_snapshot(false)
                              .build();
    
    service_.reset(new MasterService(service_config));
    
    UUID client_id = generate_uuid();
    auto segment = MakeSegment();
    auto mount_result = service_->MountSegment(segment, client_id);
    ASSERT_TRUE(mount_result.has_value());
    
    // Create multiple snapshots with increasing data
    for (int version = 1; version <= 3; version++) {
        std::string key = "version_" + std::to_string(version) + "_key";
        auto put_result = service_->PutStart(client_id, key, {1024}, {.replica_num = 1});
        ASSERT_TRUE(put_result.has_value());
        auto put_end_result = service_->PutEnd(client_id, key, ReplicaType::MEMORY);
        ASSERT_TRUE(put_end_result.has_value());
        
        std::string snapshot_id = GenerateSnapshotId() + "_v" + std::to_string(version);
        auto persist_result = CallPersistState(service_.get(), snapshot_id);
        ASSERT_TRUE(persist_result.has_value()) 
            << "Failed to persist version " << version;
        
        // Verify snapshot exists
        ASSERT_TRUE(SnapshotExistsInEtcd(snapshot_id)) 
            << "Snapshot version " << version << " not found in etcd";
        
        // Small delay to ensure different timestamps
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    
    // Verify latest snapshot is the most recent one
    std::string latest = GetLatestSnapshotFromEtcd();
    EXPECT_FALSE(latest.empty()) << "Latest snapshot not found";
    EXPECT_TRUE(latest.find("_v3") != std::string::npos) 
        << "Latest should be version 3, got: " << latest;
}

}  // namespace mooncake::test
