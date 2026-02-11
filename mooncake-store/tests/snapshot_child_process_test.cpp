#include "master_service.h"
#include "master_metric_manager.h"
#include "serialize/serializer_backend.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <memory>
#include <regex>
#include <string>
#include <thread>
#include <vector>

#include <signal.h>
#include <sys/wait.h>
#include <unistd.h>

#include "utils/file_util.h"

namespace mooncake::test {

namespace fs = std::filesystem;

class SnapshotChildProcessTest : public ::testing::Test {
   protected:
    const std::string& tmp_dir() const { return tmp_dir_; }

    std::unique_ptr<MasterService> service_;

    static constexpr const char* kEnvSnapshotLocalPath =
        "MOONCAKE_SNAPSHOT_LOCAL_PATH";

    void SetUp() override {
        google::InitGoogleLogging("SnapshotChildProcessTest");
        FLAGS_logtostderr = true;

        // Reset metric state for isolation
        MasterMetricManager::instance().reset_allocated_mem_size();
        MasterMetricManager::instance().reset_total_mem_capacity();

        // Create temp directory
        std::string tmpl =
            (fs::temp_directory_path() / "snap_child_test_XXXXXX").string();
        char* dir = mkdtemp(tmpl.data());
        ASSERT_NE(dir, nullptr) << "Failed to create temp directory";
        tmp_dir_ = dir;

        // Set env for LocalFileBackend
        ::setenv(kEnvSnapshotLocalPath, tmp_dir().c_str(), 1);
    }

    void TearDown() override {
        service_.reset();
        if (!tmp_dir().empty() && fs::exists(tmp_dir())) {
            fs::remove_all(tmp_dir());
        }
        ::unsetenv(kEnvSnapshotLocalPath);
        google::ShutdownGoogleLogging();
    }

    // Create a default service with snapshot_restore enabled (backend initialized)
    void CreateDefaultService() {
        auto config = MasterServiceConfigBuilder()
                          .set_enable_snapshot(false)
                          .set_enable_snapshot_restore(true)
                          .set_snapshot_backup_dir(tmp_dir() + "/backup")
                          .set_snapshot_interval_seconds(100)
                          .set_snapshot_child_timeout_seconds(60)
                          .set_snapshot_retention_count(3)
                          .set_snapshot_backend_type(
                              SnapshotBackendType::LOCAL_FILE)
                          .build();
        service_ = std::make_unique<MasterService>(config);
    }

    // Helper wrappers for private methods (friend access)
    std::string CallFormatTimestamp(
        const std::chrono::system_clock::time_point& tp) {
        return service_->FormatTimestamp(tp);
    }

    void CallHandleChildExit(pid_t pid, int status,
                             const std::string& snapshot_id) {
        service_->HandleChildExit(pid, status, snapshot_id);
    }

    void CallHandleChildTimeout(pid_t pid,
                                const std::string& snapshot_id) {
        service_->HandleChildTimeout(pid, snapshot_id);
    }

    void CallCleanupOldSnapshot(int keep_count,
                                const std::string& snapshot_id) {
        service_->CleanupOldSnapshot(keep_count, snapshot_id);
    }

    tl::expected<void, SerializationError> CallUploadSnapshotFile(
        const std::vector<uint8_t>& data, const std::string& path,
        const std::string& local_filename,
        const std::string& snapshot_id) {
        return service_->UploadSnapshotFile(data, path, local_filename,
                                            snapshot_id);
    }

    SerializerBackend* GetSnapshotBackend() {
        return service_->snapshot_backend_.get();
    }

    tl::expected<void, SerializationError> CallPersistState(
        const std::string& snapshot_id) {
        return service_->PersistState(snapshot_id);
    }

    bool GetUseSnapshotBackupDir() {
        return service_->use_snapshot_backup_dir_;
    }

    // Check if a key exists in raw metadata (regardless of replica status)
    bool KeyExistsInMetadata(MasterService* svc, const std::string& key) {
        size_t shard_idx = svc->getShardIndex(key);
        auto& shard = svc->metadata_shards_[shard_idx];
        SharedMutexLocker lock(&shard.mutex, shared_lock_t{});
        return shard.metadata.find(key) != shard.metadata.end();
    }

   private:
    std::string tmp_dir_;
};

// ========== FormatTimestamp ==========

TEST_F(SnapshotChildProcessTest, FormatTimestamp_MatchesExpectedFormat) {
    CreateDefaultService();
    auto ts = CallFormatTimestamp(std::chrono::system_clock::now());
    // Expected format: YYYYMMDD_HHMMSS_mmm
    std::regex pattern(R"(^\d{8}_\d{6}_\d{3}$)");
    EXPECT_TRUE(std::regex_match(ts, pattern))
        << "FormatTimestamp returned: " << ts;
}

// ========== HandleChildExit ==========

TEST_F(SnapshotChildProcessTest, HandleChildExit_NormalSuccess) {
    CreateDefaultService();
    pid_t pid = fork();
    ASSERT_NE(pid, -1) << "fork failed";
    if (pid == 0) {
        _exit(0);
    }
    int status;
    waitpid(pid, &status, 0);
    // Should not crash; exercises the success path
    EXPECT_NO_FATAL_FAILURE(
        CallHandleChildExit(pid, status, "test_snapshot_success"));
}

TEST_F(SnapshotChildProcessTest, HandleChildExit_NormalFailure) {
    CreateDefaultService();
    pid_t pid = fork();
    ASSERT_NE(pid, -1) << "fork failed";
    if (pid == 0) {
        _exit(1);
    }
    int status;
    waitpid(pid, &status, 0);
    // Should not crash; exercises the error path
    EXPECT_NO_FATAL_FAILURE(
        CallHandleChildExit(pid, status, "test_snapshot_failure"));
}

TEST_F(SnapshotChildProcessTest, HandleChildExit_Signaled) {
    CreateDefaultService();
    pid_t pid = fork();
    ASSERT_NE(pid, -1) << "fork failed";
    if (pid == 0) {
        // Sleep long enough for parent to send signal
        sleep(300);
        _exit(0);
    }
    // Kill child with SIGKILL
    kill(pid, SIGKILL);
    int status;
    waitpid(pid, &status, 0);
    // Should not crash; exercises the signal path
    EXPECT_NO_FATAL_FAILURE(
        CallHandleChildExit(pid, status, "test_snapshot_signaled"));
}

// ========== HandleChildTimeout ==========

TEST_F(SnapshotChildProcessTest, HandleChildTimeout_KillsSleepingChild) {
    CreateDefaultService();
    pid_t pid = fork();
    ASSERT_NE(pid, -1) << "fork failed";
    if (pid == 0) {
        // Sleep indefinitely; parent will kill us
        sleep(300);
        _exit(0);
    }
    // HandleChildTimeout sends SIGTERM, waits 5s, then SIGKILL if needed
    EXPECT_NO_FATAL_FAILURE(
        CallHandleChildTimeout(pid, "test_snapshot_timeout"));

    // Verify child is truly gone
    int status;
    pid_t result = waitpid(pid, &status, WNOHANG);
    // result should be -1 (already reaped) or pid (just reaped)
    // It should NOT be 0 (still running)
    EXPECT_NE(result, 0) << "Child process should have been terminated";
}

// ========== CleanupOldSnapshot ==========

TEST_F(SnapshotChildProcessTest, CleanupOldSnapshot_KeepsRecentDeletesOld) {
    CreateDefaultService();
    auto* backend = GetSnapshotBackend();
    ASSERT_NE(backend, nullptr);

    // Create 5 fake snapshot directories with timestamp-like names
    std::vector<std::string> snapshot_ids = {
        "20240101_000000_000", "20240102_000000_000", "20240103_000000_000",
        "20240104_000000_000", "20240105_000000_000"};

    for (const auto& id : snapshot_ids) {
        std::string key = "master_snapshot/" + id + "/metadata";
        backend->UploadString(key, "dummy_metadata");
        std::string manifest_key = "master_snapshot/" + id + "/manifest.txt";
        backend->UploadString(manifest_key, "messagepack|1.0.0|" + id);
    }

    // Keep only 2, cleanup with current snapshot_id = last one
    CallCleanupOldSnapshot(2, "20240105_000000_000");

    // Verify: list remaining objects
    std::vector<std::string> remaining;
    backend->ListObjectsWithPrefix("master_snapshot/", remaining);

    // Only the 2 newest (20240104, 20240105) should remain
    for (const auto& key : remaining) {
        EXPECT_TRUE(key.find("20240104_000000_000") != std::string::npos ||
                    key.find("20240105_000000_000") != std::string::npos)
            << "Unexpected remaining key: " << key;
    }
    EXPECT_GE(remaining.size(), 2u);
}

// ========== UploadSnapshotFile ==========

TEST_F(SnapshotChildProcessTest, UploadSnapshotFile_Success) {
    CreateDefaultService();
    std::vector<uint8_t> data = {10, 20, 30, 40, 50};
    std::string path = "master_snapshot/test_upload/metadata";
    auto result =
        CallUploadSnapshotFile(data, path, "metadata", "test_upload");
    ASSERT_TRUE(result.has_value())
        << "UploadSnapshotFile failed: " << result.error().message;

    // Verify data can be downloaded back
    std::vector<uint8_t> downloaded;
    auto dl = GetSnapshotBackend()->DownloadBuffer(path, downloaded);
    ASSERT_TRUE(dl.has_value()) << dl.error();
    EXPECT_EQ(downloaded, data);
}

// ========== Auto Snapshot Thread ==========

TEST_F(SnapshotChildProcessTest, AutoSnapshot_GeneratesFiles) {
    // Create a service with snapshot enabled and short interval
    auto config =
        MasterServiceConfigBuilder()
            .set_enable_snapshot(true)
            .set_enable_snapshot_restore(false)
            .set_snapshot_backup_dir(tmp_dir() + "/backup")
            .set_snapshot_interval_seconds(2)
            .set_snapshot_child_timeout_seconds(60)
            .set_snapshot_retention_count(3)
            .set_snapshot_backend_type(SnapshotBackendType::LOCAL_FILE)
            .build();
    auto auto_service = std::make_unique<MasterService>(config);

    // Wait long enough for at least one snapshot cycle to complete
    // interval=2s, so wait ~5s to be safe
    std::this_thread::sleep_for(std::chrono::seconds(5));

    // Check if latest.txt was generated
    std::string latest_path = tmp_dir() + "/master_snapshot/latest.txt";
    bool found = fs::exists(latest_path);

    // Destroy service to stop snapshot thread before assertions
    auto_service.reset();

    EXPECT_TRUE(found) << "Snapshot thread should have generated latest.txt at "
                       << latest_path;
}

// ========== Snapshot Backup Dir ==========

TEST_F(SnapshotChildProcessTest,
       RestoreWithBackupDir_CreatesBackupFiles) {
    // Step 1: Create a snapshot using a service
    CreateDefaultService();
    auto persist_result = CallPersistState("20240601_120000_000");
    ASSERT_TRUE(persist_result.has_value())
        << "PersistState failed: " << persist_result.error().message;

    // Destroy the service
    service_.reset();

    // Step 2: Create a new service with restore + backup_dir set
    std::string backup_dir = tmp_dir() + "/backup_restore_test";
    auto config =
        MasterServiceConfigBuilder()
            .set_enable_snapshot(false)
            .set_enable_snapshot_restore(true)
            .set_snapshot_backup_dir(backup_dir)
            .set_snapshot_interval_seconds(100)
            .set_snapshot_child_timeout_seconds(60)
            .set_snapshot_retention_count(3)
            .set_snapshot_backend_type(SnapshotBackendType::LOCAL_FILE)
            .build();
    auto restore_service = std::make_unique<MasterService>(config);

    // Step 3: Verify backup files were created in {backup_dir}/restore/
    std::string restore_dir = backup_dir + "/restore";
    EXPECT_TRUE(fs::exists(restore_dir + "/manifest.txt"))
        << "manifest.txt should be backed up during restore";
    EXPECT_TRUE(fs::exists(restore_dir + "/metadata"))
        << "metadata should be backed up during restore";
    EXPECT_TRUE(fs::exists(restore_dir + "/segments"))
        << "segments should be backed up during restore";
    EXPECT_TRUE(fs::exists(restore_dir + "/task_manager"))
        << "task_manager should be backed up during restore";

    restore_service.reset();
}

TEST_F(SnapshotChildProcessTest,
       RestoreWithoutBackupDir_NoBackupFiles) {
    // Step 1: Create a snapshot using a service
    CreateDefaultService();
    auto persist_result = CallPersistState("20240601_120000_001");
    ASSERT_TRUE(persist_result.has_value())
        << "PersistState failed: " << persist_result.error().message;

    // Destroy the service
    service_.reset();

    // Step 2: Create a new service with restore but NO backup_dir
    auto config =
        MasterServiceConfigBuilder()
            .set_enable_snapshot(false)
            .set_enable_snapshot_restore(true)
            .set_snapshot_backup_dir("")  // empty = no backup
            .set_snapshot_interval_seconds(100)
            .set_snapshot_child_timeout_seconds(60)
            .set_snapshot_retention_count(3)
            .set_snapshot_backend_type(SnapshotBackendType::LOCAL_FILE)
            .build();
    auto restore_service = std::make_unique<MasterService>(config);

    // Step 3: Verify NO backup directory was created
    // With empty backup_dir, use_snapshot_backup_dir_ should be false
    // and no restore directory should exist anywhere in tmp_dir()
    bool any_restore_dir_found = false;
    for (auto& entry : fs::recursive_directory_iterator(tmp_dir())) {
        if (entry.is_directory() && entry.path().filename() == "restore") {
            any_restore_dir_found = true;
            break;
        }
    }
    EXPECT_FALSE(any_restore_dir_found)
        << "No restore backup directory should exist when backup_dir is empty";

    restore_service.reset();
}

// ========== Environment Variable Tests ==========

TEST_F(SnapshotChildProcessTest, EnableSnapshotWithoutEnvVar_Throws) {
    // Unset env var
    ::unsetenv(kEnvSnapshotLocalPath);

    auto config =
        MasterServiceConfigBuilder()
            .set_enable_snapshot(true)
            .set_enable_snapshot_restore(false)
            .set_snapshot_interval_seconds(100)
            .set_snapshot_child_timeout_seconds(60)
            .set_snapshot_retention_count(3)
            .set_snapshot_backend_type(SnapshotBackendType::LOCAL_FILE)
            .build();

    // LocalFileBackend default constructor throws when env var is missing
    EXPECT_THROW(MasterService service(config), std::runtime_error);
}

TEST_F(SnapshotChildProcessTest, DisableSnapshotWithoutEnvVar_NoThrow) {
    // Unset env var
    ::unsetenv(kEnvSnapshotLocalPath);

    auto config =
        MasterServiceConfigBuilder()
            .set_enable_snapshot(false)
            .set_enable_snapshot_restore(false)
            .set_snapshot_interval_seconds(100)
            .set_snapshot_child_timeout_seconds(60)
            .set_snapshot_retention_count(3)
            .set_snapshot_backend_type(SnapshotBackendType::LOCAL_FILE)
            .build();

    // With snapshot disabled, backend is never created, so no throw
    EXPECT_NO_THROW({
        MasterService service(config);
    });
}

// ========== Restore Dirty Data Cleanup ==========

TEST_F(SnapshotChildProcessTest, RestoreCleansNonCompleteReplica) {
    // Step 1: Create service and add both clean and dirty data
    auto config = MasterServiceConfigBuilder()
                      .set_enable_snapshot(false)
                      .set_enable_snapshot_restore(true)
                      .set_snapshot_backup_dir(tmp_dir() + "/backup")
                      .set_snapshot_interval_seconds(100)
                      .set_snapshot_child_timeout_seconds(60)
                      .set_snapshot_retention_count(3)
                      .set_snapshot_backend_type(
                          SnapshotBackendType::LOCAL_FILE)
                      .build();
    service_ = std::make_unique<MasterService>(config);

    // Mount a segment
    Segment segment;
    segment.id = generate_uuid();
    segment.name = "test_segment";
    segment.base = 0x300000000;
    segment.size = 1024 * 1024 * 16;
    segment.te_endpoint = segment.name;
    UUID client_id = generate_uuid();
    auto mount_result = service_->MountSegment(segment, client_id);
    ASSERT_TRUE(mount_result.has_value()) << "MountSegment failed";

    // Add a complete object (clean data)
    std::string clean_key = "clean_object";
    auto put_result = service_->PutStart(
        client_id, clean_key, {1024}, {.replica_num = 1});
    ASSERT_TRUE(put_result.has_value()) << "PutStart clean failed";
    auto put_end_result =
        service_->PutEnd(client_id, clean_key, ReplicaType::MEMORY);
    ASSERT_TRUE(put_end_result.has_value()) << "PutEnd clean failed";

    // Add an incomplete object (PutStart without PutEnd -> non-COMPLETE)
    std::string dirty_key = "dirty_incomplete";
    auto put_dirty_result = service_->PutStart(
        client_id, dirty_key, {1024}, {.replica_num = 1});
    ASSERT_TRUE(put_dirty_result.has_value()) << "PutStart dirty failed";
    // Intentionally NO PutEnd -> replica stays in PENDING status

    // Verify both keys exist in metadata before snapshot
    EXPECT_TRUE(service_->ExistKey(clean_key).value_or(false));
    EXPECT_TRUE(KeyExistsInMetadata(service_.get(), dirty_key))
        << "Dirty key should exist in raw metadata after PutStart";

    // Step 2: Persist state
    auto persist_result = CallPersistState("20240701_120000_000");
    ASSERT_TRUE(persist_result.has_value())
        << "PersistState failed: " << persist_result.error().message;
    service_.reset();

    // Step 3: Restore into a new service
    auto restore_config =
        MasterServiceConfigBuilder()
            .set_enable_snapshot(false)
            .set_enable_snapshot_restore(true)
            .set_snapshot_backup_dir(tmp_dir() + "/backup")
            .set_snapshot_interval_seconds(100)
            .set_snapshot_child_timeout_seconds(60)
            .set_snapshot_retention_count(3)
            .set_snapshot_backend_type(SnapshotBackendType::LOCAL_FILE)
            .build();
    auto restored_service =
        std::make_unique<MasterService>(restore_config);

    // Step 4: Verify non-COMPLETE replica was cleaned, complete one remains
    EXPECT_TRUE(restored_service->ExistKey(clean_key).value_or(false))
        << "Complete object should survive restore";
    EXPECT_FALSE(KeyExistsInMetadata(restored_service.get(), dirty_key))
        << "Non-COMPLETE object should be cleaned from metadata during restore";

    restored_service.reset();
}

TEST_F(SnapshotChildProcessTest, RestoreCleansExpiredLease) {
    // Step 1: Create service with long lease TTL
    auto config = MasterServiceConfigBuilder()
                      .set_enable_snapshot(false)
                      .set_enable_snapshot_restore(true)
                      .set_snapshot_backup_dir(tmp_dir() + "/backup")
                      .set_snapshot_interval_seconds(100)
                      .set_snapshot_child_timeout_seconds(60)
                      .set_snapshot_retention_count(3)
                      .set_snapshot_backend_type(
                          SnapshotBackendType::LOCAL_FILE)
                      .set_default_kv_lease_ttl(600000)  // 10 min lease
                      .build();
    service_ = std::make_unique<MasterService>(config);

    // Mount a segment
    Segment segment;
    segment.id = generate_uuid();
    segment.name = "test_segment";
    segment.base = 0x300000000;
    segment.size = 1024 * 1024 * 16;
    segment.te_endpoint = segment.name;
    UUID client_id = generate_uuid();
    auto mount_result = service_->MountSegment(segment, client_id);
    ASSERT_TRUE(mount_result.has_value()) << "MountSegment failed";

    // Add two complete objects via PutStart + PutEnd
    // Note: PutEnd calls GrantLease(0, ...) so lease is immediately expired
    std::string expired_key = "expired_lease_object";
    auto put_exp = service_->PutStart(
        client_id, expired_key, {1024}, {.replica_num = 1});
    ASSERT_TRUE(put_exp.has_value()) << "PutStart expired failed";
    ASSERT_TRUE(
        service_->PutEnd(client_id, expired_key, ReplicaType::MEMORY)
            .has_value())
        << "PutEnd expired failed";

    std::string normal_key = "normal_lease_object";
    auto put_norm = service_->PutStart(
        client_id, normal_key, {1024}, {.replica_num = 1});
    ASSERT_TRUE(put_norm.has_value()) << "PutStart normal failed";
    ASSERT_TRUE(
        service_->PutEnd(client_id, normal_key, ReplicaType::MEMORY)
            .has_value())
        << "PutEnd normal failed";

    // ExistKey grants a fresh lease (now + 600s) to normal_key
    EXPECT_TRUE(service_->ExistKey(normal_key).value_or(false));
    // Do NOT call ExistKey on expired_key, its lease stays expired from PutEnd

    // Step 2: Persist state
    auto persist_result = CallPersistState("20240701_120000_001");
    ASSERT_TRUE(persist_result.has_value())
        << "PersistState failed: " << persist_result.error().message;
    service_.reset();

    // Step 3: Restore into a new service
    auto restore_config =
        MasterServiceConfigBuilder()
            .set_enable_snapshot(false)
            .set_enable_snapshot_restore(true)
            .set_snapshot_backup_dir(tmp_dir() + "/backup")
            .set_snapshot_interval_seconds(100)
            .set_snapshot_child_timeout_seconds(60)
            .set_snapshot_retention_count(3)
            .set_snapshot_backend_type(SnapshotBackendType::LOCAL_FILE)
            .build();
    auto restored_service =
        std::make_unique<MasterService>(restore_config);

    // Step 4: Verify normal data retained, expired-lease data cleaned
    EXPECT_TRUE(restored_service->ExistKey(normal_key).value_or(false))
        << "Normal object with valid lease should survive restore";
    EXPECT_FALSE(
        KeyExistsInMetadata(restored_service.get(), expired_key))
        << "Lease-expired object should be cleaned during restore";

    restored_service.reset();
}

}  // namespace mooncake::test

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
