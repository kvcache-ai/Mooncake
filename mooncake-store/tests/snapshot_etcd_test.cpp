/**
 * snapshot_etcd_test.cpp
 *
 * ETCD-specific snapshot tests that verify behavior unique to the etcd
 * backend: daemon upload path, etcd key structure, non-blocking COW fork,
 * multi-version tracking, and empty snapshot restore.
 *
 * Operation tests (mount/unmount, put/get, concurrent ops, etc.) are NOT
 * duplicated here — they live in master_service_test_for_snapshot.cpp as
 * parameterized tests (TEST_P) that run against both LOCAL_FILE and ETCD
 * backends automatically.
 *
 * Design
 * ------
 * Single test class (SnapshotEtcdTest), no TearDown override.
 *
 * Base-class TearDown calls TestSnapshotAndRestore() for every test, which:
 *   1. PersistState() — uses daemon Unix-socket path when daemon is running
 *   2. Creates a new MasterService with restore enabled
 *   3. Compares service state (keys / replicas / segments / tasks)
 *   4. Validates PutStart allocation consistency
 */

#include "master_service_test_for_snapshot_base.h"
#include "etcd_helper.h"

#include <gflags/gflags.h>
#include <algorithm>
#include <chrono>
#include <string>
#include <thread>
#include <vector>

// POSIX headers used by LargeSnapshotDoesNotBlockService (fork / pipe)
#include <sys/wait.h>
#include <unistd.h>

DEFINE_string(etcd_endpoints, "127.0.0.1:12379",
              "Etcd endpoints for snapshot testing");

namespace mooncake::test {

// ===========================================================================
// SnapshotEtcdTest
// ===========================================================================
class SnapshotEtcdTest : public MasterServiceSnapshotTestBase {
   protected:
    void SetUp() override {
        MasterServiceSnapshotTestBase::SetUp();

        static bool glog_initialized = false;
        if (!glog_initialized) {
            google::InitGoogleLogging("SnapshotEtcdTest");
            FLAGS_logtostderr = true;
            glog_initialized = true;
        }

        EnsureDaemonSymlink();

        LOG(INFO) << "Connecting to etcd at: " << FLAGS_etcd_endpoints;
        auto result =
            EtcdHelper::ConnectToEtcdStoreClient(FLAGS_etcd_endpoints);
        ASSERT_EQ(ErrorCode::OK, result)
            << "Failed to connect to etcd at " << FLAGS_etcd_endpoints;
    }

    // ==================== Service factories ====================

    void CreateEtcdService() {
        service_.reset(new MasterService(
            MasterServiceConfig::builder()
                .set_memory_allocator(BufferAllocatorType::OFFSET)
                .set_snapshot_backend_type(SnapshotBackendType::ETCD)
                .set_etcd_endpoints(FLAGS_etcd_endpoints)
                .set_enable_snapshot(false)
                .build()));

        if (!StartSnapshotDaemonForTest()) {
            LOG(WARNING)
                << "snapshot_uploader_daemon not found; "
                << "PersistState will use direct etcd upload path";
        }
    }

    // ==================== ETCD-specific helpers ====================

    bool SnapshotExistsInEtcd(const std::string& snapshot_id) {
        const std::string key =
            "master_snapshot/" + snapshot_id + "/manifest.txt";
        std::string value;
        EtcdRevisionId rev;
        return EtcdHelper::Get(key.c_str(), key.size(), value, rev) ==
               ErrorCode::OK;
    }

    std::string GetLatestSnapshotFromEtcd() {
        const std::string key = "master_snapshot/latest.txt";
        std::string value;
        EtcdRevisionId rev;
        if (EtcdHelper::Get(key.c_str(), key.size(), value, rev) !=
            ErrorCode::OK)
            return "";
        value.erase(0, value.find_first_not_of(" \t\n\r"));
        value.erase(value.find_last_not_of(" \t\n\r") + 1);
        return value;
    }
};

// ===========================================================================
// ETCD-Specific Tests
// ===========================================================================

// ---------------------------------------------------------------------------
// 1. SnapshotPersistViaDaemon
//
// Explicitly verifies the daemon upload path end-to-end:
//   - manifest.txt / latest.txt / metadata / segments must exist in etcd
//   - manifest format must be "messagepack|1.0.0|<snapshot_id>"
//   - restore from daemon-persisted snapshot must produce identical state
// ---------------------------------------------------------------------------
TEST_F(SnapshotEtcdTest, SnapshotPersistViaDaemon) {
    LOG(INFO) << "========== Test: SnapshotPersistViaDaemon ==========";

    service_.reset(new MasterService(
        MasterServiceConfig::builder()
            .set_memory_allocator(BufferAllocatorType::OFFSET)
            .set_snapshot_backend_type(SnapshotBackendType::ETCD)
            .set_etcd_endpoints(FLAGS_etcd_endpoints)
            .set_enable_snapshot(false)
            .build()));

    if (!StartSnapshotDaemonForTest()) {
        GTEST_SKIP()
            << "snapshot_uploader_daemon not available; "
            << "build it and re-run to verify the daemon upload path";
    }

    // Build state: one segment + 3 keys
    auto segment = MakeSegment("daemon_test_segment");
    UUID client_id = generate_uuid();
    ASSERT_TRUE(service_->MountSegment(segment, client_id).has_value());

    for (int i = 0; i < 3; i++) {
        const std::string key = "daemon_key_" + std::to_string(i);
        ASSERT_TRUE(
            service_->PutStart(client_id, key, {1024}, {.replica_num = 1})
                .has_value())
            << "PutStart failed for key: " << key;
        ASSERT_TRUE(
            service_->PutEnd(client_id, key, ReplicaType::MEMORY).has_value())
            << "PutEnd failed for key: " << key;
    }

    const ServiceStateSnapshot state_before =
        CaptureServiceState(service_.get());

    // PersistState via daemon (daemon running -> Unix socket -> batch etcd upload)
    const std::string snapshot_id = GenerateSnapshotId();
    {
        auto result = CallPersistState(service_.get(), snapshot_id);
        ASSERT_TRUE(result.has_value())
            << "PersistState via daemon failed: " << result.error().message;
    }

    // --- Verify etcd key structure ---
    const std::string prefix = "master_snapshot/" + snapshot_id + "/";

    std::string manifest_val, metadata_val, segments_val, latest_val;
    ASSERT_TRUE(GetEtcdValue(prefix + "manifest.txt", manifest_val))
        << "manifest.txt not found in etcd";
    ASSERT_TRUE(GetEtcdValue(prefix + "metadata", metadata_val))
        << "metadata not found in etcd";
    ASSERT_TRUE(GetEtcdValue(prefix + "segments", segments_val))
        << "segments not found in etcd";
    ASSERT_TRUE(GetEtcdValue("master_snapshot/latest.txt", latest_val))
        << "latest.txt not found in etcd";

    // latest.txt must point to this snapshot
    latest_val.erase(0, latest_val.find_first_not_of(" \t\n\r"));
    latest_val.erase(latest_val.find_last_not_of(" \t\n\r") + 1);
    EXPECT_EQ(snapshot_id, latest_val)
        << "latest.txt does not point to the expected snapshot";

    // Parse manifest: "messagepack|1.0.0|<snapshot_id>"
    {
        std::vector<std::string> parts;
        size_t start = 0;
        while (true) {
            const size_t pos = manifest_val.find('|', start);
            if (pos == std::string::npos) {
                parts.push_back(manifest_val.substr(start));
                break;
            }
            parts.push_back(manifest_val.substr(start, pos - start));
            start = pos + 1;
        }
        ASSERT_GE(parts.size(), 3u)
            << "Manifest format invalid: " << manifest_val;
        EXPECT_EQ("messagepack", parts[0]) << "Protocol mismatch";
        EXPECT_EQ("1.0.0", parts[1]) << "Version mismatch";
        EXPECT_EQ(snapshot_id, parts[2]) << "Snapshot ID in manifest mismatch";
    }

    EXPECT_GT(metadata_val.size(), 0u) << "metadata blob must not be empty";
    EXPECT_GT(segments_val.size(), 0u) << "segments blob must not be empty";

    LOG(INFO) << "Daemon upload verified — manifest=" << manifest_val
              << ", metadata=" << metadata_val.size()
              << "B, segments=" << segments_val.size() << "B";

    // Closed-loop: restore and compare state
    {
        ::setenv("MOONCAKE_MASTER_SERVICE_SNAPSHOT_TEST_SKIP_CLEANUP", "1", 1);
        std::unique_ptr<MasterService> restored(new MasterService(
            MasterServiceConfig::builder()
                .set_memory_allocator(BufferAllocatorType::OFFSET)
                .set_snapshot_backend_type(SnapshotBackendType::ETCD)
                .set_etcd_endpoints(FLAGS_etcd_endpoints)
                .set_enable_snapshot_restore(true)
                .build()));
        ::unsetenv("MOONCAKE_MASTER_SERVICE_SNAPSHOT_TEST_SKIP_CLEANUP");

        const ServiceStateSnapshot state_after =
            CaptureServiceState(restored.get());
        EXPECT_TRUE(CompareServiceState(state_before, state_after))
            << "State mismatch after restore from daemon-persisted snapshot";

        const std::string best_seg =
            FindBestSegment(service_.get(), state_before.all_segments);
        if (!best_seg.empty())
            AssertPutStartConsistentWithBestSegment(service_.get(),
                                                   restored.get(), best_seg);
    }

    // TearDown runs a second TestSnapshotAndRestore cycle.
    // Daemon is already running; PersistState reuses it automatically.
}

// ---------------------------------------------------------------------------
// 2. LargeSnapshotDoesNotBlockService
//
// Verifies that snapshot serialization (fork + serialize + etcd upload) does
// not block normal service operations.  Mirrors actual SnapshotThreadFunc:
// holds snapshot_mutex_ exclusively only during fork(), then releases it so
// the parent can continue serving requests while the child serializes.
// ---------------------------------------------------------------------------
TEST_F(SnapshotEtcdTest, LargeSnapshotDoesNotBlockService) {
    LOG(INFO)
        << "========== Test: LargeSnapshotDoesNotBlockService ==========";

    service_.reset(new MasterService(
        MasterServiceConfig::builder()
            .set_memory_allocator(BufferAllocatorType::OFFSET)
            .set_snapshot_backend_type(SnapshotBackendType::ETCD)
            .set_etcd_endpoints(FLAGS_etcd_endpoints)
            .set_client_live_ttl_sec(120)
            .set_enable_snapshot(false)
            .build()));

    if (!StartSnapshotDaemonForTest()) {
        LOG(WARNING) << "Daemon not available; using direct etcd upload path";
    }

    // 256 MB segment so the hot loop does not exhaust capacity
    static constexpr size_t kLargeSegSize = 256ULL * 1024ULL * 1024ULL;
    auto segment =
        MakeSegment("large_snap_seg", kDefaultSegmentBase, kLargeSegSize);
    UUID client_id = generate_uuid();
    ASSERT_TRUE(service_->MountSegment(segment, client_id).has_value());

    // Pre-populate ~64 MB so serialization takes measurable time
    constexpr int kNumKeys = 1000;
    constexpr size_t kValueSize = 64 * 1024;
    for (int i = 0; i < kNumKeys; i++) {
        const std::string key = "preload_key_" + std::to_string(i);
        ASSERT_TRUE(
            service_->PutStart(client_id, key, kValueSize, {.replica_num = 1})
                .has_value());
        ASSERT_TRUE(
            service_->PutEnd(client_id, key, ReplicaType::MEMORY).has_value());
    }
    LOG(INFO) << "Pre-populated " << kNumKeys << " keys (~"
              << kNumKeys * kValueSize / 1024 / 1024 << " MB)";

    const std::string snapshot_id = GenerateSnapshotId() + "_nonblock";

    int status_pipe[2];
    ASSERT_EQ(pipe(status_pipe), 0) << "Failed to create status pipe";

    const auto snapshot_start = std::chrono::steady_clock::now();

    // Acquire exclusive lock before fork — mirrors SnapshotThreadFunc
    pid_t pid;
    {
        std::unique_lock<std::shared_mutex> lock(GetSnapshotMutex());
        pid = fork();
    }

    if (pid == -1) {
        close(status_pipe[0]);
        close(status_pipe[1]);
        FAIL() << "fork() failed: " << strerror(errno);
    }

    if (pid == 0) {
        // ===== Child process =====
        close(status_pipe[0]);

        struct ChildResult {
            uint8_t success;
            uint64_t serialize_time_ms;
        } result = {0, 0};

        if (EtcdHelper::ConnectToEtcdStoreClient(FLAGS_etcd_endpoints) ==
            ErrorCode::OK) {
            const auto t0 = std::chrono::steady_clock::now();
            auto r = CallPersistState(service_.get(), snapshot_id);
            const auto t1 = std::chrono::steady_clock::now();
            result.serialize_time_ms =
                std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0)
                    .count();
            result.success = r.has_value() ? 1 : 0;
        }

        ssize_t written = write(status_pipe[1], &result, sizeof(result));
        (void)written;
        close(status_pipe[1]);
        _exit(result.success ? 0 : 1);
    }

    // ===== Parent process =====
    close(status_pipe[1]);

    std::vector<double> op_latencies_ms;
    op_latencies_ms.reserve(300);

    const auto op_deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(5);
    int op_index = 0;
    bool child_exited = false;

    while (std::chrono::steady_clock::now() < op_deadline && !child_exited) {
        int wstatus;
        if (waitpid(pid, &wstatus, WNOHANG) == pid) {
            child_exited = true;
            break;
        }

        const std::string key = "nonblock_key_" + std::to_string(op_index++);
        const auto op_start = std::chrono::steady_clock::now();

        auto put =
            service_->PutStart(client_id, key, {1024}, {.replica_num = 1});
        if (!put.has_value()) {
            LOG(WARNING) << "PutStart failed: "
                         << static_cast<int>(put.error());
            continue;
        }
        if (!service_->PutEnd(client_id, key, ReplicaType::MEMORY).has_value())
            continue;
        service_->GetReplicaList(key);
        service_->Remove(key);

        op_latencies_ms.push_back(
            std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::steady_clock::now() - op_start)
                .count() /
            1000.0);

        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    if (!child_exited) {
        int wstatus;
        waitpid(pid, &wstatus, 0);
    }

    struct ChildResult {
        uint8_t success;
        uint64_t serialize_time_ms;
    } child_result = {0, 0};
    ssize_t bytes_read =
        read(status_pipe[0], &child_result, sizeof(child_result));
    (void)bytes_read;
    close(status_pipe[0]);

    // Statistics
    double max_ms = 0, avg_ms = 0, p99_ms = 0;
    if (!op_latencies_ms.empty()) {
        for (double v : op_latencies_ms) {
            max_ms = std::max(max_ms, v);
            avg_ms += v;
        }
        avg_ms /= static_cast<double>(op_latencies_ms.size());

        std::vector<double> sorted = op_latencies_ms;
        std::sort(sorted.begin(), sorted.end());
        p99_ms = sorted[std::min(static_cast<size_t>(sorted.size() * 0.99),
                                 sorted.size() - 1)];
    }

    LOG(INFO) << "Child serialize time (ms): " << child_result.serialize_time_ms;
    LOG(INFO) << "Operations: " << op_latencies_ms.size()
              << "  avg=" << avg_ms << "ms  p99=" << p99_ms
              << "ms  max=" << max_ms << "ms";

    ASSERT_FALSE(op_latencies_ms.empty())
        << "No operations recorded during snapshot";

    constexpr double kMaxMs = 500.0;
    EXPECT_LT(max_ms, kMaxMs)
        << "Service appears blocked (max=" << max_ms << "ms > " << kMaxMs
        << "ms threshold)";

    constexpr double kP99Ms = 100.0;
    EXPECT_LT(p99_ms, kP99Ms)
        << "P99 latency too high (p99=" << p99_ms << "ms > " << kP99Ms
        << "ms threshold)";

    if (!child_result.success)
        LOG(WARNING) << "Snapshot child process failed (etcd may be slow)";
}

// ---------------------------------------------------------------------------
// 3. MultipleSnapshotVersions
//
// Verifies that latest.txt always points to the most recent snapshot across
// multiple PersistState calls with distinct snapshot IDs.
// ---------------------------------------------------------------------------
TEST_F(SnapshotEtcdTest, MultipleSnapshotVersions) {
    LOG(INFO) << "========== Test: MultipleSnapshotVersions ==========";

    CreateEtcdService();

    UUID client_id = generate_uuid();
    ASSERT_TRUE(service_->MountSegment(MakeSegment(), client_id).has_value());

    for (int v = 1; v <= 3; v++) {
        const std::string key = "version_" + std::to_string(v) + "_key";
        ASSERT_TRUE(
            service_->PutStart(client_id, key, {1024}, {.replica_num = 1})
                .has_value());
        ASSERT_TRUE(
            service_->PutEnd(client_id, key, ReplicaType::MEMORY).has_value());

        const std::string snap_id =
            GenerateSnapshotId() + "_v" + std::to_string(v);
        ASSERT_TRUE(CallPersistState(service_.get(), snap_id).has_value())
            << "PersistState failed for version " << v;
        EXPECT_TRUE(SnapshotExistsInEtcd(snap_id))
            << "Snapshot v" << v << " not found in etcd";

        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    const std::string latest = GetLatestSnapshotFromEtcd();
    EXPECT_FALSE(latest.empty()) << "latest.txt not found in etcd";
    EXPECT_NE(latest.find("_v3"), std::string::npos)
        << "latest.txt should point to v3, got: " << latest;

    // TearDown verifies snapshot-restore on the final 3-key state
}

// ---------------------------------------------------------------------------
// 4. RestoreEmptySnapshot
//
// Verifies that restoring from an empty snapshot (no segments, no keys)
// produces a service with zero keys.
// ---------------------------------------------------------------------------
TEST_F(SnapshotEtcdTest, RestoreEmptySnapshot) {
    LOG(INFO) << "========== Test: RestoreEmptySnapshot ==========";

    CreateEtcdService();  // empty service — no segments, no keys

    // Persist the empty state explicitly so we can verify the restore now
    const std::string snapshot_id = GenerateSnapshotId();
    ASSERT_TRUE(CallPersistState(service_.get(), snapshot_id).has_value())
        << "PersistState on empty service failed";

    {
        ::setenv("MOONCAKE_MASTER_SERVICE_SNAPSHOT_TEST_SKIP_CLEANUP", "1", 1);
        std::unique_ptr<MasterService> restored(new MasterService(
            MasterServiceConfig::builder()
                .set_memory_allocator(BufferAllocatorType::OFFSET)
                .set_snapshot_backend_type(SnapshotBackendType::ETCD)
                .set_etcd_endpoints(FLAGS_etcd_endpoints)
                .set_enable_snapshot_restore(true)
                .build()));
        ::unsetenv("MOONCAKE_MASTER_SERVICE_SNAPSHOT_TEST_SKIP_CLEANUP");

        auto keys = restored->GetAllKeys();
        ASSERT_TRUE(keys.has_value());
        EXPECT_EQ(0u, keys.value().size())
            << "Expected empty state after restore from empty snapshot";
    }

    // TearDown also runs TestSnapshotAndRestore on the still-empty service_
}

}  // namespace mooncake::test

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    return RUN_ALL_TESTS();
}
