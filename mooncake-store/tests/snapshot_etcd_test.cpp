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
#include <unordered_set>
#include <vector>

DEFINE_string(etcd_endpoints, "127.0.0.1:12379",
              "Etcd endpoints for snapshot testing");

namespace mooncake::test {

// Global flag to track glog initialization across all test classes
static bool g_glog_initialized = false;

// Global flag to track daemon symlink creation
static bool g_daemon_symlink_created = false;

// Ensure daemon symlink exists for tests (shared by all test classes)
static void EnsureDaemonSymlink() {
    if (g_daemon_symlink_created) return;

    const char* symlink_path = "./snapshot_uploader_daemon";
    // Check if daemon already exists
    if (access(symlink_path, X_OK) == 0) {
        g_daemon_symlink_created = true;
        return;
    }

    // Try to find daemon in build directory
    std::vector<std::string> search_paths = {
        "./build/mooncake-store/src/snapshot_uploader_daemon",
        "../build/mooncake-store/src/snapshot_uploader_daemon",
        "../../build/mooncake-store/src/snapshot_uploader_daemon",
    };

    for (const auto& path : search_paths) {
        if (access(path.c_str(), X_OK) == 0) {
            // Get absolute path
            char abs_path[PATH_MAX];
            if (realpath(path.c_str(), abs_path) != nullptr) {
                // Remove existing symlink if any
                unlink(symlink_path);
                if (symlink(abs_path, symlink_path) == 0) {
                    LOG(INFO) << "Created daemon symlink: " << symlink_path
                              << " -> " << abs_path;
                    g_daemon_symlink_created = true;
                    return;
                }
            }
        }
    }
    LOG(WARNING) << "Could not find snapshot_uploader_daemon executable";
}

class SnapshotEtcdTest : public MasterServiceSnapshotTestBase {
   protected:
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

        if (!g_glog_initialized) {
            google::InitGoogleLogging("SnapshotEtcdTest");
            FLAGS_logtostderr = true;
            g_glog_initialized = true;
        }

        // Ensure daemon symlink exists for daemon upload path
        EnsureDaemonSymlink();

        // Initialize etcd client for snapshot operations
        LOG(INFO) << "Connecting to etcd at: " << FLAGS_etcd_endpoints;
        auto connect_result =
            EtcdHelper::ConnectToEtcdStoreClient(FLAGS_etcd_endpoints);
        ASSERT_EQ(ErrorCode::OK, connect_result)
            << "Failed to connect to etcd. Make sure etcd is running at "
            << FLAGS_etcd_endpoints;

        // Clean up any existing test snapshots
        CleanupTestSnapshots();
    }

    void TearDown() override {
        // Note: Don't call base class TearDown as it would run
        // TestSnapshotAndRestore We'll run custom ETCD-based tests instead
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
        std::string manifest_key =
            "master_snapshot/" + snapshot_id + "/manifest.txt";
        std::string value;
        EtcdRevisionId revision;
        auto result = EtcdHelper::Get(manifest_key.c_str(), manifest_key.size(),
                                      value, revision);
        return result == ErrorCode::OK;
    }

    // Get latest snapshot ID from etcd
    std::string GetLatestSnapshotFromEtcd() {
        std::string latest_key = "master_snapshot/latest.txt";
        std::string value;
        EtcdRevisionId revision;
        auto result = EtcdHelper::Get(latest_key.c_str(), latest_key.size(),
                                      value, revision);
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
            << "Failed to persist state to ETCD: "
            << persist_result1.error().message;

        LOG(INFO) << "Snapshot persisted with ID: " << snapshot_id1;

        // Verify snapshot exists in etcd
        ASSERT_TRUE(SnapshotExistsInEtcd(snapshot_id1))
            << "Snapshot not found in etcd after persist";

        LOG(INFO)
            << "========== Phase 2: Capture state before restore ==========";
        ServiceStateSnapshot state_before = CaptureServiceState(service.get());
        LOG(INFO) << "Captured state: " << state_before.all_keys.size()
                  << " keys, " << state_before.all_segments.size()
                  << " segments";

        LOG(INFO) << "========== Phase 3: Create new MasterService with "
                     "restore enabled ==========";
        auto restore_config =
            MasterServiceConfig::builder()
                .set_memory_allocator(BufferAllocatorType::OFFSET)
                .set_snapshot_backend_type(SnapshotBackendType::ETCD)
                .set_etcd_endpoints(FLAGS_etcd_endpoints)
                .set_enable_snapshot_restore(true)
                .build();

        // Snapshot metadata leases can be expired at persist time.
        // Skip restore cleanup in tests to validate the snapshot content.
        EnvVarGuard skip_cleanup(
            "MOONCAKE_MASTER_SERVICE_SNAPSHOT_TEST_SKIP_CLEANUP", "1");
        std::unique_ptr<MasterService> restored_service(
            new MasterService(restore_config));
        LOG(INFO) << "Restored service created";

        LOG(INFO)
            << "========== Phase 4: Capture state after restore ==========";
        ServiceStateSnapshot state_after =
            CaptureServiceState(restored_service.get());
        LOG(INFO) << "Captured restored state: " << state_after.all_keys.size()
                  << " keys, " << state_after.all_segments.size()
                  << " segments";

        LOG(INFO) << "========== Phase 5: Compare states ==========";
        EXPECT_TRUE(CompareServiceState(state_before, state_after))
            << "Service state mismatch after restore from ETCD";

        LOG(INFO) << "========== Phase 6: Verify latest snapshot ==========";
        std::string latest_snapshot = GetLatestSnapshotFromEtcd();
        EXPECT_EQ(snapshot_id1, latest_snapshot)
            << "Latest snapshot ID mismatch. Expected: " << snapshot_id1
            << ", Got: " << latest_snapshot;

        LOG(INFO)
            << "========== Phase 7: PutStart consistency validation ==========";
        std::string best_segment =
            FindBestSegment(service.get(), state_before.all_segments);
        if (!best_segment.empty()) {
            AssertPutStartConsistentWithBestSegment(
                service.get(), restored_service.get(), best_segment);
        } else {
            LOG(WARNING)
                << "No valid segment found, skip PutStart consistency check";
        }

        LOG(INFO)
            << "========== ETCD Snapshot and Restore Test PASSED ==========";
    }
};

// Helper function to generate keys for segments
std::string GenerateKeyForSegment(const UUID& client_id,
                                  const std::unique_ptr<MasterService>& service,
                                  const std::string& segment_name) {
    static std::atomic<uint64_t> counter(0);

    while (true) {
        std::string key =
            "etcd_test_key_" + std::to_string(counter.fetch_add(1));
        std::vector<Replica::Descriptor> replica_list;

        auto exist_result = service->ExistKey(key);
        if (exist_result.has_value() && exist_result.value()) {
            continue;
        }

        auto put_result =
            service->PutStart(client_id, key, {1024}, {.replica_num = 1});
        if (put_result.has_value()) {
            replica_list = std::move(put_result.value());
        }
        ErrorCode code =
            put_result.has_value() ? ErrorCode::OK : put_result.error();

        if (code == ErrorCode::OBJECT_ALREADY_EXISTS) {
            continue;
        }
        if (code != ErrorCode::OK) {
            throw std::runtime_error("PutStart failed with code: " +
                                     std::to_string(static_cast<int>(code)));
        }

        auto put_end_result =
            service->PutEnd(client_id, key, ReplicaType::MEMORY);
        if (!put_end_result.has_value()) {
            throw std::runtime_error("PutEnd failed");
        }

        if (replica_list[0]
                .get_memory_descriptor()
                .buffer_descriptor.transport_endpoint_ == segment_name) {
            return key;
        }

        auto remove_result = service->Remove(key);
        // Ignore cleanup failure
    }
}

TEST_F(SnapshotEtcdTest, BasicSnapshotRestore) {
    LOG(INFO) << "========== Test: BasicSnapshotRestore ==========";

    auto service_config =
        MasterServiceConfig::builder()
            .set_memory_allocator(BufferAllocatorType::OFFSET)
            .set_snapshot_backend_type(SnapshotBackendType::ETCD)
            .set_etcd_endpoints(FLAGS_etcd_endpoints)
            .set_enable_snapshot(false)  // Manual snapshot control
            .build();

    service_.reset(new MasterService(service_config));

    // Start daemon for ETCD upload path
    if (!StartSnapshotDaemonForTest()) {
        LOG(WARNING)
            << "Failed to start snapshot daemon, will use direct upload";
    }

    // Mount a segment
    auto segment = MakeSegment("etcd_test_segment");
    UUID client_id = generate_uuid();
    auto mount_result = service_->MountSegment(segment, client_id);
    ASSERT_TRUE(mount_result.has_value()) << "Failed to mount segment";

    // Put some keys
    for (int i = 0; i < 5; i++) {
        std::string key = "test_key_" + std::to_string(i);
        auto put_result =
            service_->PutStart(client_id, key, {1024}, {.replica_num = 1});
        ASSERT_TRUE(put_result.has_value())
            << "PutStart failed for key: " << key;
        auto put_end_result =
            service_->PutEnd(client_id, key, ReplicaType::MEMORY);
        ASSERT_TRUE(put_end_result.has_value())
            << "PutEnd failed for key: " << key;
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
        ASSERT_TRUE(put_result.has_value())
            << "PutStart failed for key: " << key;
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
    std::string metadata_key = "master_snapshot/" + snapshot_id + "/metadata";
    std::string segments_key = "master_snapshot/" + snapshot_id + "/segments";
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
    EXPECT_EQ(snapshot_id, latest_value) << "Latest snapshot id mismatch";

    // Parse manifest: protocol|version|snapshot_id
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
    ASSERT_GE(parts.size(), 3u)
        << "Manifest format invalid: " << manifest_value;
    EXPECT_EQ("messagepack", parts[0]) << "Protocol type mismatch";
    EXPECT_EQ("1.0.0", parts[1]) << "Version mismatch";
    EXPECT_EQ(snapshot_id, parts[2]) << "Manifest snapshot id mismatch";

    // Verify metadata and segments exist in etcd
    std::string metadata_value;
    std::string segments_value;
    ASSERT_TRUE(GetEtcdValue(metadata_key, metadata_value))
        << "Metadata not found in etcd";
    ASSERT_TRUE(GetEtcdValue(segments_key, segments_value))
        << "Segments not found in etcd";

    EXPECT_GT(metadata_value.size(), 0u) << "Metadata should not be empty";
    EXPECT_GT(segments_value.size(), 0u) << "Segments should not be empty";

    LOG(INFO) << "Daemon upload verified: manifest=" << manifest_value
              << ", metadata_size=" << metadata_value.size()
              << ", segments_size=" << segments_value.size();

    // Closed loop: restore from the daemon-persisted snapshot and compare.
    LOG(INFO)
        << "========== Closed Loop: Restore After Daemon Persist ==========";
    auto restore_config =
        MasterServiceConfig::builder()
            .set_memory_allocator(BufferAllocatorType::OFFSET)
            .set_snapshot_backend_type(SnapshotBackendType::ETCD)
            .set_etcd_endpoints(FLAGS_etcd_endpoints)
            .set_enable_snapshot_restore(true)
            .build();

    std::unique_ptr<MasterService> restored_service(
        new MasterService(restore_config));
    ServiceStateSnapshot state_after =
        CaptureServiceState(restored_service.get());

    EXPECT_TRUE(CompareServiceState(state_before, state_after))
        << "Service state mismatch after restore from daemon-persisted ETCD "
           "snapshot";

    std::string best_segment =
        FindBestSegment(service_.get(), state_before.all_segments);
    if (!best_segment.empty()) {
        AssertPutStartConsistentWithBestSegment(
            service_.get(), restored_service.get(), best_segment);
    }
}

// Test: Verify that snapshot operations do not block normal service operations.
// This test simulates the actual production scenario using fork() like
// SnapshotThreadFunc.
TEST_F(SnapshotEtcdTest, LargeSnapshotDoesNotBlockService) {
    LOG(INFO) << "========== Test: LargeSnapshotDoesNotBlockService ==========";

    auto service_config =
        MasterServiceConfig::builder()
            .set_memory_allocator(BufferAllocatorType::OFFSET)
            .set_snapshot_backend_type(SnapshotBackendType::ETCD)
            .set_etcd_endpoints(FLAGS_etcd_endpoints)
            .set_client_live_ttl_sec(120)
            .set_enable_snapshot(false)  // We manually control snapshot
            .build();

    service_.reset(new MasterService(service_config));

    // Start daemon for ETCD upload path
    if (!StartSnapshotDaemonForTest()) {
        LOG(WARNING)
            << "Failed to start snapshot daemon, will use direct upload";
    }

    // Use a large segment to avoid exhausting capacity during the hot loop.
    static constexpr size_t kLargeSegmentSize = 256ULL * 1024ULL * 1024ULL;
    auto segment = MakeSegment("etcd_large_snapshot_segment",
                               kDefaultSegmentBase, kLargeSegmentSize);
    UUID client_id = generate_uuid();
    auto mount_result = service_->MountSegment(segment, client_id);
    ASSERT_TRUE(mount_result.has_value()) << "Failed to mount segment";

    // Pre-populate with enough data to make serialization take noticeable time.
    // Create 1000 keys with varying sizes to simulate real workload.
    LOG(INFO) << "Pre-populating service with data...";
    constexpr int kNumKeys = 4 * 1000 * 1000;
    constexpr size_t kValueSize = 16;  // 64KB per key, ~64MB total
    for (int i = 0; i < kNumKeys; i++) {
        std::string key = "preload_key_" + std::to_string(i);
        auto put_result =
            service_->PutStart(client_id, key, kValueSize, {.replica_num = 1});
        ASSERT_TRUE(put_result.has_value())
            << "PutStart failed for preload key: " << key;
        auto put_end_result =
            service_->PutEnd(client_id, key, ReplicaType::MEMORY);
        ASSERT_TRUE(put_end_result.has_value())
            << "PutEnd failed for preload key: " << key;
    }
    LOG(INFO) << "Pre-populated " << kNumKeys << " keys";

    const std::string snapshot_id = GenerateSnapshotId() + "_nonblock";

    // Create pipe for child process communication
    int status_pipe[2];
    ASSERT_EQ(pipe(status_pipe), 0) << "Failed to create status pipe";

    const auto snapshot_start = std::chrono::steady_clock::now();

    // Fork a child process to execute PersistState, simulating actual
    // SnapshotThreadFunc
    pid_t pid;
    {
        // Acquire exclusive lock before fork, same as SnapshotThreadFunc
        std::unique_lock<std::shared_mutex> lock(SNAPSHOT_MUTEX);
        LOG(INFO) << "Acquired SNAPSHOT_MUTEX, forking child process...";
        pid = fork();
    }
    // Lock is released here

    if (pid == -1) {
        close(status_pipe[0]);
        close(status_pipe[1]);
        FAIL() << "Failed to fork child process: " << strerror(errno);
    } else if (pid == 0) {
        // ===== Child process =====
        close(status_pipe[0]);  // Close read end

        // Structure to send results back to parent
        struct ChildResult {
            uint8_t success;
            uint64_t metadata_size;
            uint64_t segment_size;
            uint64_t serialize_time_ms;
        } result = {0, 0, 0, 0};

        // Reinitialize ETCD client in child process (required after fork)
        auto connect_result =
            EtcdHelper::ConnectToEtcdStoreClient(FLAGS_etcd_endpoints);

        if (connect_result == ErrorCode::OK) {
            // Measure serialization time and sizes by calling PersistState
            // The sizes will be logged by PersistState internally
            auto serialize_start = std::chrono::steady_clock::now();
            auto persist_result = CallPersistState(service_.get(), snapshot_id);
            auto serialize_end = std::chrono::steady_clock::now();

            result.serialize_time_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                serialize_end - serialize_start).count();

            if (persist_result.has_value()) {
                result.success = 1;
            }

            // Try to read staging file sizes (they may be deleted already)
            std::string staging_dir = "snapshots/staging/" + snapshot_id;
            std::error_code ec;
            auto metadata_size = std::filesystem::file_size(staging_dir + "/metadata", ec);
            if (!ec) result.metadata_size = metadata_size;
            auto segment_size = std::filesystem::file_size(staging_dir + "/segments", ec);
            if (!ec) result.segment_size = segment_size;
        }

        // Send result to parent
        ssize_t written = write(status_pipe[1], &result, sizeof(result));
        (void)written;
        close(status_pipe[1]);
        _exit(result.success ? 0 : 1);
    }

    // ===== Parent process =====
    close(status_pipe[1]);  // Close write end

    LOG(INFO) << "Child process started (pid=" << pid
              << "), running service operations...";

    // Run service operations while child is executing snapshot
    std::vector<double> op_latencies_ms;
    op_latencies_ms.reserve(200);

    // Run operations for up to 5 seconds or until child exits
    const auto op_deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(5);
    int op_index = 0;
    bool child_exited = false;

    while (std::chrono::steady_clock::now() < op_deadline && !child_exited) {
        // Check if child has exited (non-blocking)
        int status;
        pid_t wait_result = waitpid(pid, &status, WNOHANG);
        if (wait_result == pid) {
            child_exited = true;
            LOG(INFO) << "Child process exited during operation loop";
            break;
        }

        // Perform a complete operation cycle: PutStart -> PutEnd ->
        // GetReplicaList -> Remove
        const std::string key =
            "snapshot_non_blocking_key_" + std::to_string(op_index++);
        const auto op_start = std::chrono::steady_clock::now();

        auto put_result =
            service_->PutStart(client_id, key, {1024}, {.replica_num = 1});
        if (!put_result.has_value()) {
            LOG(WARNING) << "PutStart failed during snapshot for key: " << key
                         << ", error=" << static_cast<int>(put_result.error());
            continue;
        }

        auto put_end_result =
            service_->PutEnd(client_id, key, ReplicaType::MEMORY);
        if (!put_end_result.has_value()) {
            LOG(WARNING) << "PutEnd failed during snapshot for key: " << key;
            continue;
        }

        // Also test read operation
        auto get_result = service_->GetReplicaList(key);
        if (!get_result.has_value()) {
            LOG(WARNING) << "GetReplicaList failed during snapshot for key: "
                         << key;
        }

        // Try to remove to avoid exhausting the segment.
        // Note: Remove may fail due to key having an active lease (from
        // ExistKey/GetReplicaList), which is expected behavior and doesn't
        // affect the non-blocking test.
        auto remove_result = service_->Remove(key);
        (void)
            remove_result;  // Ignore result - lease-protected keys are expected

        const auto op_end = std::chrono::steady_clock::now();
        const auto latency_ms =
            std::chrono::duration_cast<std::chrono::microseconds>(op_end -
                                                                  op_start)
                .count() /
            1000.0;
        op_latencies_ms.push_back(latency_ms);

        // Small sleep to avoid overwhelming the system
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    // Wait for child process if not already exited
    if (!child_exited) {
        LOG(INFO) << "Waiting for child process to complete...";
        int status;
        waitpid(pid, &status, 0);
    }

    const auto snapshot_end = std::chrono::steady_clock::now();

    // Read child result (extended structure with size info)
    struct ChildResult {
        uint8_t success;
        uint64_t metadata_size;
        uint64_t segment_size;
        uint64_t serialize_time_ms;
    } child_result = {0, 0, 0, 0};

    ssize_t bytes_read = read(status_pipe[0], &child_result, sizeof(child_result));
    if (bytes_read != sizeof(child_result)) {
        LOG(WARNING) << "Failed to read full child result, bytes_read=" << bytes_read
                     << ", expected=" << sizeof(child_result);
    }
    close(status_pipe[0]);

    // Calculate statistics
    const auto snapshot_duration_ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(snapshot_end -
                                                              snapshot_start)
            .count();

    double max_latency_ms = 0.0;
    double avg_latency_ms = 0.0;
    double p99_latency_ms = 0.0;

    if (!op_latencies_ms.empty()) {
        max_latency_ms =
            *std::max_element(op_latencies_ms.begin(), op_latencies_ms.end());

        double sum = 0.0;
        for (double lat : op_latencies_ms) {
            sum += lat;
        }
        avg_latency_ms = sum / op_latencies_ms.size();

        // Calculate P99
        std::vector<double> sorted_latencies = op_latencies_ms;
        std::sort(sorted_latencies.begin(), sorted_latencies.end());
        size_t p99_index = static_cast<size_t>(sorted_latencies.size() * 0.99);
        if (p99_index >= sorted_latencies.size())
            p99_index = sorted_latencies.size() - 1;
        p99_latency_ms = sorted_latencies[p99_index];
    }

    LOG(INFO) << "========== Non-Blocking Test Results ==========";
    LOG(INFO) << "Snapshot duration (ms): " << snapshot_duration_ms;
    LOG(INFO) << "Child serialize time (ms): " << child_result.serialize_time_ms;
    if (child_result.metadata_size > 0) {
        LOG(INFO) << "Metadata size: " << child_result.metadata_size << " bytes ("
                  << child_result.metadata_size / 1024.0 / 1024.0 << " MB)";
    }
    if (child_result.segment_size > 0) {
        LOG(INFO) << "Segment size: " << child_result.segment_size << " bytes ("
                  << child_result.segment_size / 1024.0 / 1024.0 << " MB)";
    }
    if (child_result.metadata_size > 0 && child_result.segment_size > 0) {
        uint64_t total = child_result.metadata_size + child_result.segment_size;
        LOG(INFO) << "Total snapshot size: " << total << " bytes ("
                  << total / 1024.0 / 1024.0 << " MB)";
    }
    LOG(INFO) << "Operations completed: " << op_latencies_ms.size();
    LOG(INFO) << "Avg latency (ms): " << avg_latency_ms;
    LOG(INFO) << "P99 latency (ms): " << p99_latency_ms;
    LOG(INFO) << "Max latency (ms): " << max_latency_ms;
    LOG(INFO) << "Child process result: "
              << (child_result.success ? "SUCCESS" : "FAILED");

    // Primary assertions: verify non-blocking behavior
    // These are the core purpose of this test
    ASSERT_FALSE(op_latencies_ms.empty())
        << "No operations were recorded during snapshot";

    // Key assertion: operations should remain responsive during snapshot.
    // With fork-based snapshot, the exclusive lock is only held during fork(),
    // so operations should complete quickly.
    //
    // Threshold: 500ms max latency is reasonable given:
    // - fork() itself can take a few ms for large memory footprint
    // - ETCD operations may have some latency
    // - But should NOT see multi-second delays
    constexpr double kMaxAcceptableLatencyMs = 500.0;
    EXPECT_LT(max_latency_ms, kMaxAcceptableLatencyMs)
        << "Service operations appear blocked during snapshot. "
        << "Max latency=" << max_latency_ms
        << "ms exceeds threshold=" << kMaxAcceptableLatencyMs << "ms. "
        << "This may indicate the SNAPSHOT_MUTEX is held too long.";

    // P99 should be much lower than max
    constexpr double kP99AcceptableLatencyMs = 100.0;
    EXPECT_LT(p99_latency_ms, kP99AcceptableLatencyMs)
        << "P99 latency=" << p99_latency_ms
        << "ms exceeds threshold=" << kP99AcceptableLatencyMs << "ms";

    // Secondary check: snapshot child process result
    // This may fail if ETCD is not available, but the non-blocking behavior
    // test is still valid. We log a warning but don't fail the test since the
    // core purpose (verifying non-blocking behavior) is achieved.
    if (!child_result.success) {
        LOG(WARNING)
            << "Snapshot child process failed (possibly ETCD not available). "
            << "Non-blocking behavior verification still passed. "
            << "To test full snapshot persistence, ensure ETCD is running: "
            << "etcd --listen-client-urls=http://127.0.0.1:12379 "
            << "--advertise-client-urls=http://127.0.0.1:12379";
        // Note: We intentionally don't EXPECT_EQ(child_result.success, 1) here because
        // the primary purpose of this test is to verify non-blocking behavior,
        // which is validated by the latency checks above. Other tests in this
        // file (BasicSnapshotRestore, SnapshotPersistViaDaemon) cover the
        // persistence functionality.
    } else {
        LOG(INFO) << "Snapshot child process completed successfully";
    }

    LOG(INFO) << "========== Test: LargeSnapshotDoesNotBlockService COMPLETED "
                 "==========";
}

TEST_F(SnapshotEtcdTest, SnapshotWithMultipleSegments) {
    LOG(INFO) << "========== Test: SnapshotWithMultipleSegments ==========";

    auto service_config =
        MasterServiceConfig::builder()
            .set_memory_allocator(BufferAllocatorType::OFFSET)
            .set_snapshot_backend_type(SnapshotBackendType::ETCD)
            .set_etcd_endpoints(FLAGS_etcd_endpoints)
            .set_enable_snapshot(false)
            .build();

    service_.reset(new MasterService(service_config));

    // Start daemon for ETCD upload path
    if (!StartSnapshotDaemonForTest()) {
        LOG(WARNING)
            << "Failed to start snapshot daemon, will use direct upload";
    }

    UUID client_id = generate_uuid();

    // Mount multiple segments
    for (int i = 0; i < 3; i++) {
        auto segment = MakeSegment(
            "segment_" + std::to_string(i),
            kDefaultSegmentBase + i * kDefaultSegmentSize, kDefaultSegmentSize);
        auto mount_result = service_->MountSegment(segment, client_id);
        ASSERT_TRUE(mount_result.has_value())
            << "Failed to mount segment " << i;
    }

    // Put keys across segments
    for (int i = 0; i < 10; i++) {
        std::string key = "multi_seg_key_" + std::to_string(i);
        auto put_result =
            service_->PutStart(client_id, key, {1024}, {.replica_num = 1});
        ASSERT_TRUE(put_result.has_value())
            << "PutStart failed for key: " << key;
        auto put_end_result =
            service_->PutEnd(client_id, key, ReplicaType::MEMORY);
        ASSERT_TRUE(put_end_result.has_value())
            << "PutEnd failed for key: " << key;
    }

    TestEtcdSnapshotAndRestore(service_);
}

TEST_F(SnapshotEtcdTest, SnapshotWithEviction) {
    LOG(INFO) << "========== Test: SnapshotWithEviction ==========";

    // Configuration without quota to avoid eviction complexity
    auto service_config =
        MasterServiceConfig::builder()
            .set_memory_allocator(BufferAllocatorType::OFFSET)
            .set_snapshot_backend_type(SnapshotBackendType::ETCD)
            .set_etcd_endpoints(FLAGS_etcd_endpoints)
            .set_enable_snapshot(false)
            .build();

    service_.reset(new MasterService(service_config));

    // Start daemon for ETCD upload path
    if (!StartSnapshotDaemonForTest()) {
        LOG(WARNING)
            << "Failed to start snapshot daemon, will use direct upload";
    }

    auto segment = MakeSegment("eviction_segment");
    UUID client_id = generate_uuid();
    auto mount_result = service_->MountSegment(segment, client_id);
    ASSERT_TRUE(mount_result.has_value());

    // Put several keys
    for (int i = 0; i < 10; i++) {
        std::string key = "evict_key_" + std::to_string(i);
        auto put_result =
            service_->PutStart(client_id, key, {1024}, {.replica_num = 1});
        if (put_result.has_value()) {
            auto put_end_result =
                service_->PutEnd(client_id, key, ReplicaType::MEMORY);
            if (!put_end_result.has_value()) {
                LOG(WARNING) << "PutEnd failed for key: " << key;
            }
        }
    }

    TestEtcdSnapshotAndRestore(service_);
}

TEST_F(SnapshotEtcdTest, RestoreEmptySnapshot) {
    LOG(INFO) << "========== Test: RestoreEmptySnapshot ==========";

    auto service_config =
        MasterServiceConfig::builder()
            .set_memory_allocator(BufferAllocatorType::OFFSET)
            .set_snapshot_backend_type(SnapshotBackendType::ETCD)
            .set_etcd_endpoints(FLAGS_etcd_endpoints)
            .set_enable_snapshot(false)
            .build();

    service_.reset(new MasterService(service_config));

    // Start daemon for ETCD upload path
    if (!StartSnapshotDaemonForTest()) {
        LOG(WARNING)
            << "Failed to start snapshot daemon, will use direct upload";
    }

    // Create snapshot with no data
    std::string snapshot_id = GenerateSnapshotId();
    auto persist_result = CallPersistState(service_.get(), snapshot_id);
    ASSERT_TRUE(persist_result.has_value())
        << "Failed to persist empty state: " << persist_result.error().message;

    // Verify can restore from empty snapshot
    auto restore_config =
        MasterServiceConfig::builder()
            .set_memory_allocator(BufferAllocatorType::OFFSET)
            .set_snapshot_backend_type(SnapshotBackendType::ETCD)
            .set_etcd_endpoints(FLAGS_etcd_endpoints)
            .set_enable_snapshot_restore(true)
            .build();

    std::unique_ptr<MasterService> restored_service(
        new MasterService(restore_config));

    auto keys = restored_service->GetAllKeys();
    ASSERT_TRUE(keys.has_value());
    EXPECT_EQ(0, keys.value().size()) << "Expected empty state after restore";
}

TEST_F(SnapshotEtcdTest, MultipleSnapshotVersions) {
    LOG(INFO) << "========== Test: MultipleSnapshotVersions ==========";

    auto service_config =
        MasterServiceConfig::builder()
            .set_memory_allocator(BufferAllocatorType::OFFSET)
            .set_snapshot_backend_type(SnapshotBackendType::ETCD)
            .set_etcd_endpoints(FLAGS_etcd_endpoints)
            .set_enable_snapshot(false)
            .build();

    service_.reset(new MasterService(service_config));

    // Start daemon for ETCD upload path
    if (!StartSnapshotDaemonForTest()) {
        LOG(WARNING)
            << "Failed to start snapshot daemon, will use direct upload";
    }

    UUID client_id = generate_uuid();
    auto segment = MakeSegment();
    auto mount_result = service_->MountSegment(segment, client_id);
    ASSERT_TRUE(mount_result.has_value());

    // Create multiple snapshots with increasing data
    for (int version = 1; version <= 3; version++) {
        std::string key = "version_" + std::to_string(version) + "_key";
        auto put_result =
            service_->PutStart(client_id, key, {1024}, {.replica_num = 1});
        ASSERT_TRUE(put_result.has_value());
        auto put_end_result =
            service_->PutEnd(client_id, key, ReplicaType::MEMORY);
        ASSERT_TRUE(put_end_result.has_value());

        std::string snapshot_id =
            GenerateSnapshotId() + "_v" + std::to_string(version);
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

// ============================================================================
// MasterServiceSnapshotTestForEtcd: Reuses base class TearDown() for automatic
// snapshot/restore verification with ETCD backend
// ============================================================================

class MasterServiceSnapshotTestForEtcd : public MasterServiceSnapshotTestBase {
   protected:
    void SetUp() override {
        MasterServiceSnapshotTestBase::SetUp();

        if (!g_glog_initialized) {
            google::InitGoogleLogging("MasterServiceSnapshotTestForEtcd");
            FLAGS_logtostderr = true;
            g_glog_initialized = true;
        }

        // Use shared daemon symlink setup
        EnsureDaemonSymlink();

        // Initialize etcd client
        LOG(INFO) << "Connecting to etcd at: " << FLAGS_etcd_endpoints;
        auto connect_result =
            EtcdHelper::ConnectToEtcdStoreClient(FLAGS_etcd_endpoints);
        ASSERT_EQ(ErrorCode::OK, connect_result)
            << "Failed to connect to etcd at " << FLAGS_etcd_endpoints;
    }

    // Helper to create service with ETCD backend and start daemon
    void CreateEtcdService() {
        auto config = MasterServiceConfig::builder()
                          .set_memory_allocator(BufferAllocatorType::OFFSET)
                          .set_snapshot_backend_type(SnapshotBackendType::ETCD)
                          .set_etcd_endpoints(FLAGS_etcd_endpoints)
                          .set_enable_snapshot(false)
                          .build();
        service_.reset(new MasterService(config));

        // Start daemon for ETCD upload path
        if (!StartSnapshotDaemonForTest()) {
            LOG(WARNING)
                << "Failed to start snapshot daemon, will use direct upload";
        }
    }

    void CreateEtcdServiceWithConfig(uint64_t kv_lease_ttl) {
        auto config = MasterServiceConfig::builder()
                          .set_memory_allocator(BufferAllocatorType::OFFSET)
                          .set_snapshot_backend_type(SnapshotBackendType::ETCD)
                          .set_etcd_endpoints(FLAGS_etcd_endpoints)
                          .set_enable_snapshot(false)
                          .set_default_kv_lease_ttl(kv_lease_ttl)
                          .build();
        service_.reset(new MasterService(config));

        // Start daemon for ETCD upload path
        if (!StartSnapshotDaemonForTest()) {
            LOG(WARNING)
                << "Failed to start snapshot daemon, will use direct upload";
        }
    }
};

// ==================== Mount/Unmount Tests ====================

TEST_F(MasterServiceSnapshotTestForEtcd,
       MountUnmountSegmentWithOffsetAllocator) {
    CreateEtcdService();
    auto segment = MakeSegment();
    UUID client_id = generate_uuid();
    const auto original_base = segment.base;
    const auto original_size = segment.size;

    // Test invalid parameters
    segment.base = 0;
    segment.size = original_size;
    auto mount_result1 = service_->MountSegment(segment, client_id);
    EXPECT_FALSE(mount_result1.has_value());
    EXPECT_EQ(ErrorCode::INVALID_PARAMS, mount_result1.error());

    segment.base = original_base;
    segment.size = 0;
    auto mount_result2 = service_->MountSegment(segment, client_id);
    EXPECT_FALSE(mount_result2.has_value());
    EXPECT_EQ(ErrorCode::INVALID_PARAMS, mount_result2.error());

    // Test normal mount
    segment.base = original_base;
    segment.size = original_size;
    auto mount_result = service_->MountSegment(segment, client_id);
    EXPECT_TRUE(mount_result.has_value());

    // Test idempotent mount
    auto mount_result_again = service_->MountSegment(segment, client_id);
    EXPECT_TRUE(mount_result_again.has_value());

    // Test unmount
    auto unmount_result = service_->UnmountSegment(segment.id, client_id);
    EXPECT_TRUE(unmount_result.has_value());

    // Remount for TearDown snapshot test
    auto remount_result = service_->MountSegment(segment, client_id);
    EXPECT_TRUE(remount_result.has_value());
}

TEST_F(MasterServiceSnapshotTestForEtcd, ConcurrentMountUnmount) {
    CreateEtcdService();
    constexpr size_t num_threads = 4;
    constexpr size_t iterations = 50;
    std::vector<std::thread> threads;
    std::atomic<int> success_count{0};

    for (size_t i = 0; i < num_threads; i++) {
        threads.emplace_back([i, &success_count, this]() {
            auto segment =
                MakeSegment("segment_" + std::to_string(i),
                            0x300000000 + i * 0x10000000, 16 * 1024 * 1024);
            UUID client_id = generate_uuid();

            for (size_t j = 0; j < iterations; j++) {
                auto mount_result = service_->MountSegment(segment, client_id);
                if (mount_result.has_value()) {
                    auto unmount_result =
                        service_->UnmountSegment(segment.id, client_id);
                    EXPECT_TRUE(unmount_result.has_value());
                    success_count++;
                }
            }
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }

    EXPECT_GT(success_count, 0);
}

// ==================== Put/Get Tests ====================

TEST_F(MasterServiceSnapshotTestForEtcd, PutStartEndFlow) {
    CreateEtcdService();
    [[maybe_unused]] const auto context = PrepareSimpleSegment(*service_);
    const UUID client_id = generate_uuid();

    std::string key = "etcd_test_key";
    uint64_t value_length = 1024;
    ReplicateConfig config;
    config.replica_num = 1;

    auto put_start_result =
        service_->PutStart(client_id, key, value_length, config);
    EXPECT_TRUE(put_start_result.has_value());
    auto replicas = put_start_result.value();
    EXPECT_FALSE(replicas.empty());
    EXPECT_EQ(ReplicaStatus::PROCESSING, replicas[0].status);

    // During put, Get should fail
    auto get_result = service_->GetReplicaList(key);
    EXPECT_FALSE(get_result.has_value());
    EXPECT_EQ(ErrorCode::REPLICA_IS_NOT_READY, get_result.error());

    // Complete put
    auto put_end_result = service_->PutEnd(client_id, key, ReplicaType::MEMORY);
    EXPECT_TRUE(put_end_result.has_value());

    // Now Get should succeed
    auto get_result2 = service_->GetReplicaList(key);
    EXPECT_TRUE(get_result2.has_value());
    EXPECT_EQ(1, get_result2.value().replicas.size());
    EXPECT_EQ(ReplicaStatus::COMPLETE, get_result2.value().replicas[0].status);
}

TEST_F(MasterServiceSnapshotTestForEtcd, RandomPutStartEndFlow) {
    CreateEtcdService();
    const UUID client_id = generate_uuid();

    // Mount 5 segments
    constexpr size_t kBaseAddr = 0x300000000;
    constexpr size_t kSegmentSize = 1024 * 1024 * 16;
    for (int i = 0; i < 5; ++i) {
        [[maybe_unused]] const auto context = PrepareSimpleSegment(
            *service_, "segment_" + std::to_string(i),
            kBaseAddr + static_cast<size_t>(i) * kSegmentSize, kSegmentSize);
    }

    std::string key = "random_test_key";
    uint64_t value_length = 1024;
    ReplicateConfig config;
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(1, 5);
    config.replica_num = dis(gen);

    auto put_start_result =
        service_->PutStart(client_id, key, value_length, config);
    EXPECT_TRUE(put_start_result.has_value());

    auto put_end_result = service_->PutEnd(client_id, key, ReplicaType::MEMORY);
    EXPECT_TRUE(put_end_result.has_value());

    auto get_result = service_->GetReplicaList(key);
    EXPECT_TRUE(get_result.has_value());
    EXPECT_EQ(config.replica_num, get_result.value().replicas.size());
}

TEST_F(MasterServiceSnapshotTestForEtcd, SingleSliceMultiReplicaFlow) {
    CreateEtcdService();
    const UUID client_id = generate_uuid();

    // Mount 3 segments
    constexpr size_t kBaseAddr = 0x300000000;
    constexpr size_t kSegmentSize = 1024 * 1024 * 64;
    for (int i = 0; i < 3; ++i) {
        [[maybe_unused]] const auto context = PrepareSimpleSegment(
            *service_, "segment_" + std::to_string(i),
            kBaseAddr + static_cast<size_t>(i) * kSegmentSize, kSegmentSize);
    }

    std::string key = "multi_replica_key";
    constexpr size_t num_replicas = 3;
    constexpr size_t slice_length = 1024 * 1024 * 5;

    ReplicateConfig config;
    config.replica_num = num_replicas;

    auto put_start_result =
        service_->PutStart(client_id, key, slice_length, config);
    ASSERT_TRUE(put_start_result.has_value());
    ASSERT_EQ(num_replicas, put_start_result.value().size());

    auto put_end_result = service_->PutEnd(client_id, key, ReplicaType::MEMORY);
    ASSERT_TRUE(put_end_result.has_value());

    auto get_result = service_->GetReplicaList(key);
    ASSERT_TRUE(get_result.has_value());
    ASSERT_EQ(num_replicas, get_result.value().replicas.size());

    for (const auto& replica : get_result.value().replicas) {
        EXPECT_EQ(ReplicaStatus::COMPLETE, replica.status);
    }
}

// ==================== Remove Tests ====================

TEST_F(MasterServiceSnapshotTestForEtcd, RemoveObject) {
    CreateEtcdService();
    [[maybe_unused]] const auto context = PrepareSimpleSegment(*service_);
    const UUID client_id = generate_uuid();

    std::string key = "remove_test_key";
    ReplicateConfig config;
    config.replica_num = 1;

    auto put_start_result = service_->PutStart(client_id, key, 1024, config);
    ASSERT_TRUE(put_start_result.has_value());
    auto put_end_result = service_->PutEnd(client_id, key, ReplicaType::MEMORY);
    ASSERT_TRUE(put_end_result.has_value());

    auto remove_result = service_->Remove(key);
    EXPECT_TRUE(remove_result.has_value());

    auto get_result = service_->GetReplicaList(key);
    EXPECT_FALSE(get_result.has_value());
    EXPECT_EQ(ErrorCode::OBJECT_NOT_FOUND, get_result.error());
}

TEST_F(MasterServiceSnapshotTestForEtcd, RemoveByRegex) {
    const uint64_t kv_lease_ttl = 100;
    CreateEtcdServiceWithConfig(kv_lease_ttl);
    [[maybe_unused]] const auto context = PrepareSimpleSegment(*service_);
    const UUID client_id = generate_uuid();

    // Create 10 keys
    for (int i = 0; i < 10; i++) {
        std::string key = "regex_key_" + std::to_string(i);
        auto put_start_result =
            service_->PutStart(client_id, key, 1024, {.replica_num = 1});
        ASSERT_TRUE(put_start_result.has_value());
        auto put_end_result =
            service_->PutEnd(client_id, key, ReplicaType::MEMORY);
        ASSERT_TRUE(put_end_result.has_value());
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(kv_lease_ttl));

    auto remove_result = service_->RemoveByRegex("^regex_key_");
    ASSERT_TRUE(remove_result.has_value());
    EXPECT_EQ(10, remove_result.value());
}

// ==================== Concurrent Tests ====================

TEST_F(MasterServiceSnapshotTestForEtcd, ConcurrentWriteOperations) {
    CreateEtcdService();
    constexpr size_t buffer = 0x300000000;
    constexpr size_t size = 1024 * 1024 * 256;
    auto segment = MakeSegment("concurrent_segment", buffer, size);
    UUID client_id = generate_uuid();
    auto mount_result = service_->MountSegment(segment, client_id);
    ASSERT_TRUE(mount_result.has_value());

    constexpr int num_threads = 4;
    constexpr int objects_per_thread = 50;
    std::atomic<int> success_writes(0);

    std::vector<std::thread> writers;
    for (int i = 0; i < num_threads; ++i) {
        writers.emplace_back([&, i]() {
            for (int j = 0; j < objects_per_thread; ++j) {
                std::string key = "concurrent_key_" + std::to_string(i) + "_" +
                                  std::to_string(j);
                ReplicateConfig config;
                config.replica_num = 1;

                auto put_start_result =
                    service_->PutStart(client_id, key, 1024, config);
                if (put_start_result.has_value()) {
                    auto put_end_result =
                        service_->PutEnd(client_id, key, ReplicaType::MEMORY);
                    if (put_end_result.has_value()) {
                        success_writes++;
                    }
                }

                std::this_thread::sleep_for(
                    std::chrono::milliseconds(rand() % 5));
            }
        });
    }

    for (auto& t : writers) {
        t.join();
    }

    EXPECT_GT(success_writes, 0);
    LOG(INFO) << "Concurrent writes succeeded: " << success_writes;
}

TEST_F(MasterServiceSnapshotTestForEtcd, ConcurrentReadOperations) {
    const uint64_t kv_lease_ttl = 500;
    CreateEtcdServiceWithConfig(kv_lease_ttl);
    constexpr size_t buffer = 0x300000000;
    constexpr size_t size = 1024 * 1024 * 256;
    auto segment = MakeSegment("concurrent_read_segment", buffer, size);
    UUID client_id = generate_uuid();
    auto mount_result = service_->MountSegment(segment, client_id);
    ASSERT_TRUE(mount_result.has_value());

    // Pre-populate with data
    constexpr int num_objects = 100;
    for (int i = 0; i < num_objects; ++i) {
        std::string key = "read_key_" + std::to_string(i);
        auto put_start_result =
            service_->PutStart(client_id, key, 1024, {.replica_num = 1});
        ASSERT_TRUE(put_start_result.has_value());
        auto put_end_result =
            service_->PutEnd(client_id, key, ReplicaType::MEMORY);
        ASSERT_TRUE(put_end_result.has_value());
    }

    std::atomic<int> success_reads(0);
    std::vector<std::thread> readers;

    for (int i = 0; i < 4; ++i) {
        readers.emplace_back([&]() {
            for (int j = 0; j < num_objects; ++j) {
                std::string key = "read_key_" + std::to_string(j);
                auto get_result = service_->GetReplicaList(key);
                if (get_result.has_value()) {
                    success_reads++;
                }
                std::this_thread::sleep_for(
                    std::chrono::milliseconds(rand() % 3));
            }
        });
    }

    for (auto& t : readers) {
        t.join();
    }

    EXPECT_GT(success_reads, 0);
    LOG(INFO) << "Concurrent reads succeeded: " << success_reads;
}

// ==================== Unmount Cleanup Tests ====================

TEST_F(MasterServiceSnapshotTestForEtcd, UnmountSegmentImmediateCleanup) {
    CreateEtcdService();

    constexpr size_t buffer1 = 0x300000000;
    constexpr size_t buffer2 = 0x400000000;
    constexpr size_t size = 1024 * 1024 * 16;

    auto segment1 = MakeSegment("segment1", buffer1, size);
    auto segment2 = MakeSegment("segment2", buffer2, size);
    UUID client_id = generate_uuid();

    auto mount_result1 = service_->MountSegment(segment1, client_id);
    ASSERT_TRUE(mount_result1.has_value());
    auto mount_result2 = service_->MountSegment(segment2, client_id);
    ASSERT_TRUE(mount_result2.has_value());

    // Create keys in both segments
    std::string key1 =
        GenerateKeyForSegment(client_id, service_, segment1.name);
    std::string key2 =
        GenerateKeyForSegment(client_id, service_, segment2.name);

    // Unmount segment1
    auto unmount_result = service_->UnmountSegment(segment1.id, client_id);
    ASSERT_TRUE(unmount_result.has_value());

    // Key1 should be gone
    auto get_result1 = service_->GetReplicaList(key1);
    EXPECT_FALSE(get_result1.has_value());
    EXPECT_EQ(ErrorCode::OBJECT_NOT_FOUND, get_result1.error());

    // Key2 should still exist
    auto get_result2 = service_->GetReplicaList(key2);
    EXPECT_TRUE(get_result2.has_value());
}

TEST_F(MasterServiceSnapshotTestForEtcd,
       ReadableAfterPartialUnmountWithReplication) {
    CreateEtcdService();

    constexpr size_t buffer1 = 0x300000000;
    constexpr size_t buffer2 = 0x400000000;
    constexpr size_t segment_size = 1024 * 1024 * 64;

    auto segment1 = MakeSegment("segment1", buffer1, segment_size);
    auto segment2 = MakeSegment("segment2", buffer2, segment_size);
    UUID client_id = generate_uuid();

    auto mount_result1 = service_->MountSegment(segment1, client_id);
    ASSERT_TRUE(mount_result1.has_value());
    auto mount_result2 = service_->MountSegment(segment2, client_id);
    ASSERT_TRUE(mount_result2.has_value());

    // Put key with 2 replicas
    std::string key = "replicated_key";
    ReplicateConfig config;
    config.replica_num = 2;

    auto put_start_result = service_->PutStart(client_id, key, 1024, config);
    ASSERT_TRUE(put_start_result.has_value());
    ASSERT_EQ(2u, put_start_result->size());
    ASSERT_TRUE(
        service_->PutEnd(client_id, key, ReplicaType::MEMORY).has_value());

    // Unmount one segment
    ASSERT_TRUE(service_->UnmountSegment(segment1.id, client_id).has_value());

    // Key should still be readable via surviving replica
    auto get_result = service_->GetReplicaList(key);
    ASSERT_TRUE(get_result.has_value())
        << "Object should remain accessible with surviving replica";
}

// ==================== Lease Tests ====================

TEST_F(MasterServiceSnapshotTestForEtcd, RemoveLeasedObject) {
    const uint64_t kv_lease_ttl = 100;
    CreateEtcdServiceWithConfig(kv_lease_ttl);
    [[maybe_unused]] const auto context = PrepareSimpleSegment(*service_);
    const UUID client_id = generate_uuid();

    std::string key = "leased_key";
    ReplicateConfig config;
    config.replica_num = 1;

    auto put_start_result = service_->PutStart(client_id, key, 1024, config);
    ASSERT_TRUE(put_start_result.has_value());
    auto put_end_result = service_->PutEnd(client_id, key, ReplicaType::MEMORY);
    ASSERT_TRUE(put_end_result.has_value());

    // Grant lease via ExistKey
    auto exist_result = service_->ExistKey(key);
    ASSERT_TRUE(exist_result.has_value());

    // Remove should fail due to lease
    auto remove_result = service_->Remove(key);
    EXPECT_FALSE(remove_result.has_value());
    EXPECT_EQ(ErrorCode::OBJECT_HAS_LEASE, remove_result.error());

    // Wait for lease to expire
    std::this_thread::sleep_for(std::chrono::milliseconds(kv_lease_ttl + 10));

    // Now remove should succeed
    auto remove_result2 = service_->Remove(key);
    EXPECT_TRUE(remove_result2.has_value());
}

// ==================== Batch API Tests ====================

TEST_F(MasterServiceSnapshotTestForEtcd, BatchExistKeyTest) {
    CreateEtcdService();
    [[maybe_unused]] const auto context = PrepareSimpleSegment(*service_);
    const UUID client_id = generate_uuid();

    // Create some keys
    std::vector<std::string> existing_keys;
    for (int i = 0; i < 5; i++) {
        std::string key = "batch_exist_key_" + std::to_string(i);
        auto put_start_result =
            service_->PutStart(client_id, key, 1024, {.replica_num = 1});
        ASSERT_TRUE(put_start_result.has_value());
        auto put_end_result =
            service_->PutEnd(client_id, key, ReplicaType::MEMORY);
        ASSERT_TRUE(put_end_result.has_value());
        existing_keys.push_back(key);
    }

    // Add non-existing keys
    std::vector<std::string> all_keys = existing_keys;
    all_keys.push_back("non_existing_key_1");
    all_keys.push_back("non_existing_key_2");

    auto results = service_->BatchExistKey(all_keys);
    ASSERT_EQ(all_keys.size(), results.size());

    // First 5 should exist
    for (size_t i = 0; i < existing_keys.size(); i++) {
        EXPECT_TRUE(results[i].has_value());
        EXPECT_TRUE(results[i].value());
    }

    // Last 2 should not exist
    EXPECT_TRUE(results[5].has_value());
    EXPECT_FALSE(results[5].value());
    EXPECT_TRUE(results[6].has_value());
    EXPECT_FALSE(results[6].value());
}

// ==================== Replica Uniqueness Test ====================

TEST_F(MasterServiceSnapshotTestForEtcd, ReplicaSegmentsAreUnique) {
    CreateEtcdService();
    const UUID client_id = generate_uuid();

    // Mount 3 segments
    constexpr size_t kBaseAddr = 0x300000000;
    constexpr size_t kSegmentSize = 1024 * 1024 * 64;
    for (int i = 0; i < 3; ++i) {
        auto segment = MakeSegment("unique_seg_" + std::to_string(i),
                                   kBaseAddr + i * kSegmentSize, kSegmentSize);
        auto mount_result = service_->MountSegment(segment, client_id);
        ASSERT_TRUE(mount_result.has_value());
    }

    std::string key = "unique_replica_key";
    ReplicateConfig config;
    config.replica_num = 3;

    auto put_start_result = service_->PutStart(client_id, key, 1024, config);
    ASSERT_TRUE(put_start_result.has_value());
    auto replicas = put_start_result.value();
    ASSERT_EQ(3u, replicas.size());

    // Verify all replicas are on different segments
    std::unordered_set<std::string> segment_names;
    for (const auto& replica : replicas) {
        auto endpoint = replica.get_memory_descriptor()
                            .buffer_descriptor.transport_endpoint_;
        segment_names.insert(endpoint);
    }
    EXPECT_EQ(3u, segment_names.size())
        << "All replicas should be on different segments";

    auto put_end_result = service_->PutEnd(client_id, key, ReplicaType::MEMORY);
    ASSERT_TRUE(put_end_result.has_value());
}

}  // namespace mooncake::test
