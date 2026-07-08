#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>
#include <ylt/util/tl/expected.hpp>

#include "types.h"
#include "ha/ha_types.h"

namespace mooncake {

// Forward declarations
class MasterService;
class SnapshotObjectStore;

namespace ha {
class SnapshotCatalogStore;
}

namespace test {
class MasterServiceSnapshotTestBase;
class SnapshotChildProcessTest;
}  // namespace test

#ifdef STORE_USE_ETCD
class EtcdOpLogStore;
#endif

struct MasterSnapshotManagerOptions {
    bool enable_snapshot{false};
    uint64_t snapshot_interval_seconds{0};
    uint64_t snapshot_child_timeout_seconds{0};
    uint32_t snapshot_retention_count{0};
    std::string snapshot_backup_dir;
    bool use_snapshot_backup_dir{false};
    std::string snapshot_catalog_store_type;
    std::string snapshot_catalog_store_connstring;
    std::string ha_backend_type;
    std::string ha_backend_connstring;
    std::string cluster_id;
    bool enable_ha{false};
};

/**
 * @brief MasterSnapshotManager handles snapshot lifecycle orchestration for
 * MasterService. This includes periodic snapshot scheduling, snapshot ID
 * generation, descriptor construction, child process lifecycle management,
 * timeout handling, payload upload, catalog publish, retention cleanup, and
 * snapshot metrics updates.
 *
 * This is a behavior-preserving refactor that moves snapshot orchestration
 * logic out of MasterService without changing snapshot format, restore
 * behavior, storage layout, flags, or locking semantics.
 */
class MasterSnapshotManager {
    friend class test::MasterServiceSnapshotTestBase;  // Allow test access to
                                                       // private methods
    friend class test::SnapshotChildProcessTest;       // Allow test access to
                                                       // private methods

   public:
    MasterSnapshotManager(MasterService* master_service,
                          MasterSnapshotManagerOptions options,
                          std::shared_mutex& snapshot_mutex,
                          SnapshotObjectStore* snapshot_object_store,
                          ha::SnapshotCatalogStore* snapshot_catalog_store);

    ~MasterSnapshotManager();

    void Start();
    void Stop();

   private:
    void SnapshotThreadFunc();
    void WaitForSnapshotChild(pid_t pid, const std::string& snapshot_id,
                              int log_pipe_fd);
    void HandleChildTimeout(pid_t pid, const std::string& snapshot_id);
    void HandleChildExit(pid_t pid, int status, const std::string& snapshot_id);

    tl::expected<void, SerializationError> PersistState(
        const std::string& snapshot_id);
    tl::expected<void, SerializationError> PersistState(
        const ha::SnapshotDescriptor& descriptor);
    tl::expected<ha::SnapshotDescriptor, SerializationError>
    BuildSnapshotDescriptor(const std::string& snapshot_id,
                            const std::string& manifest_path,
                            const std::string& object_prefix) const;
    tl::expected<ha::OpLogSequenceId, SerializationError>
    ResolveSnapshotSequenceId() const;

#ifdef STORE_USE_ETCD
    tl::expected<EtcdOpLogStore*, SerializationError>
    GetSnapshotBoundaryOpLogStore() const;
#endif

    tl::expected<void, SerializationError> UploadSnapshotPayloadFile(
        const std::vector<uint8_t>& data, const std::string& path,
        const std::string& local_filename, const std::string& snapshot_id);

    void CleanupOldSnapshot(int keep_count, const std::string& snapshot_id);

    std::string FormatTimestamp(
        const std::chrono::system_clock::time_point& tp);

    MasterService* master_service_;
    MasterSnapshotManagerOptions options_;

    std::shared_mutex& snapshot_mutex_;
    SnapshotObjectStore* snapshot_object_store_;
    ha::SnapshotCatalogStore* snapshot_catalog_store_;

#ifdef STORE_USE_ETCD
    mutable std::mutex snapshot_boundary_oplog_store_mutex_;
    mutable std::unique_ptr<EtcdOpLogStore> snapshot_boundary_oplog_store_;
#endif

    std::thread snapshot_thread_;
    std::atomic<bool> snapshot_running_{false};
    std::mutex snapshot_thread_mutex_;
    std::condition_variable snapshot_thread_cv_;
};

}  // namespace mooncake
