#include "master_snapshot_manager.h"

#include <fcntl.h>
#include <signal.h>
#include <sys/wait.h>
#include <unistd.h>

#include <chrono>
#include <iomanip>
#include <sstream>
#include <thread>

#include "master_service.h"
#include "master_metric_manager.h"
#include "master_snapshot_repository.h"
#include "ha/snapshot/catalog/snapshot_catalog_store.h"
#include "ha/snapshot/object/snapshot_object_store.h"
#include "ha/snapshot/snapshot_logger.h"
#include "serialize/serializer.h"
#include "segment.h"
#include "task_manager.h"
#include "utils/file_util.h"
#include "utils/zstd_util.h"

#ifdef STORE_USE_ETCD
#include "ha/oplog/etcd_oplog_store.h"
#include "etcd_helper.h"
#endif

namespace mooncake {

// Snapshot file names (moved from master_service.cpp)
static const std::string SNAPSHOT_METADATA_FILE = "metadata";
static const std::string SNAPSHOT_SEGMENTS_FILE = "segments";
static const std::string SNAPSHOT_TASK_MANAGER_FILE = "task_manager";
static const std::string SNAPSHOT_MANIFEST_FILE = "manifest.txt";
static const std::string SNAPSHOT_LATEST_FILE = "latest.txt";
static const std::string SNAPSHOT_BACKUP_SAVE_DIR =
    "mooncake_snapshot_save_backup";
static const std::string SNAPSHOT_SERIALIZER_VERSION = "1.0.0";
static const std::string SNAPSHOT_SERIALIZER_TYPE = "messagepack";

namespace {
int64_t CurrentTimeMs() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
}
}  // namespace

MasterSnapshotManager::MasterSnapshotManager(
    MasterService* master_service, MasterSnapshotManagerOptions options,
    std::shared_mutex& snapshot_mutex,
    SnapshotObjectStore* snapshot_object_store,
    ha::SnapshotCatalogStore* snapshot_catalog_store)
    : master_service_(master_service),
      options_(std::move(options)),
      snapshot_mutex_(snapshot_mutex),
      snapshot_object_store_(snapshot_object_store),
      snapshot_catalog_store_(snapshot_catalog_store),
      repository_(std::make_unique<MasterSnapshotRepository>(
          snapshot_object_store, snapshot_catalog_store,
          options_.snapshot_backup_dir, options_.use_snapshot_backup_dir)) {}

MasterSnapshotManager::~MasterSnapshotManager() { Stop(); }

void MasterSnapshotManager::Start() {
    if (snapshot_running_.load()) {
        return;
    }
    snapshot_running_ = true;
    snapshot_thread_ =
        std::thread(&MasterSnapshotManager::SnapshotThreadFunc, this);
    LOG(INFO) << "[MasterSnapshotManager] Started";
}

void MasterSnapshotManager::Stop() {
    {
        std::lock_guard<std::mutex> lk(snapshot_thread_mutex_);
        snapshot_running_ = false;
    }
    snapshot_thread_cv_.notify_all();

    if (snapshot_thread_.joinable()) {
        snapshot_thread_.join();
    }
    LOG(INFO) << "[MasterSnapshotManager] Stopped";
}

std::string MasterSnapshotManager::FormatTimestamp(
    const std::chrono::system_clock::time_point& tp) {
    auto time_t = std::chrono::system_clock::to_time_t(tp);

    std::stringstream ss;
    std::tm tm_now;
    localtime_r(&time_t, &tm_now);
    ss << std::put_time(&tm_now, "%Y%m%d_%H%M%S");

    // Add milliseconds to ensure uniqueness
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                  tp.time_since_epoch()) %
              1000;

    ss << "_" << std::setfill('0') << std::setw(3) << ms.count();

    return ss.str();
}

void MasterSnapshotManager::SnapshotThreadFunc() {
    LOG(INFO) << "[Snapshot] snapshot_thread started";
    while (snapshot_running_) {
        // Wait for the next snapshot cycle, but allow fast shutdown.
        {
            std::unique_lock<std::mutex> lk(snapshot_thread_mutex_);
            snapshot_thread_cv_.wait_for(
                lk, std::chrono::seconds(options_.snapshot_interval_seconds),
                [&] { return !snapshot_running_.load(); });
        }

        if (!snapshot_running_) {
            break;
        }

        if (!options_.enable_snapshot) {
            // Snapshot is disabled
            LOG(INFO)
                << "[Snapshot] Snapshot is disabled, waiting for next cycle";
            continue;
        }
        // Fork a child process to save current state

        std::string snapshot_id =
            FormatTimestamp(std::chrono::system_clock::now());
        LOG(INFO) << "[Snapshot] Preparing to fork child process, snapshot_id="
                  << snapshot_id;

        // Create pipe for child process logging
        int log_pipe[2];
        if (pipe(log_pipe) == -1) {
            LOG(ERROR) << "[Snapshot] Failed to create log pipe: "
                       << strerror(errno) << ", snapshot_id=" << snapshot_id;
            continue;
        }

        const std::string& snapshot_root =
            snapshot_catalog_store_->GetSnapshotRoot();
        const std::string path_prefix = snapshot_root + snapshot_id + "/";
        const std::string manifest_path = path_prefix + SNAPSHOT_MANIFEST_FILE;
        auto descriptor =
            BuildSnapshotDescriptor(snapshot_id, manifest_path, path_prefix);
        if (!descriptor) {
            LOG(ERROR) << "[Snapshot] Failed to build descriptor before fork, "
                          "snapshot_id="
                       << snapshot_id
                       << ", code=" << toString(descriptor.error().code)
                       << ", msg=" << descriptor.error().message;
            close(log_pipe[0]);
            close(log_pipe[1]);
            continue;
        }

        pid_t pid;
        {
            std::unique_lock<std::shared_mutex> lock(snapshot_mutex_);
            LOG(INFO) << "[Snapshot] Locking snapshot mutex, snapshot_id="
                      << snapshot_id;
            pid = fork();
        }
        if (pid == -1) {
            // Fork failed
            LOG(ERROR) << "[Snapshot] Failed to fork child process for state "
                          "persistence: "
                       << strerror(errno) << ", snapshot_id=" << snapshot_id;
            close(log_pipe[0]);
            close(log_pipe[1]);
        } else if (pid == 0) {
            // Child process
            // Close read end, set write end for logging
            close(log_pipe[0]);
            g_snapshot_log_pipe_fd = log_pipe[1];

            // Save current state using the configured persistence mechanism
            SNAP_LOG_INFO("[Snapshot] Child process started, snapshot_id={}",
                          snapshot_id);
            auto result = PersistState(descriptor.value());
            if (!result) {
                SNAP_LOG_ERROR(
                    "[Snapshot] Child process failed to persist state, "
                    "snapshot_id={},code={},msg={}",
                    snapshot_id, toString(result.error().code),
                    result.error().message);
                close(log_pipe[1]);
                _exit(1);  // Exit child process with error
            }
            SNAP_LOG_INFO(
                "[Snapshot] Child process successfully persisted state, "
                "snapshot_id={}",
                snapshot_id);

            close(log_pipe[1]);
            _exit(0);  // Exit child process successfully
        } else {
            // Parent process
            // Close write end, pass read end to wait function
            close(log_pipe[1]);
            WaitForSnapshotChild(pid, snapshot_id, log_pipe[0]);
            close(log_pipe[0]);
        }
    }
    LOG(INFO) << "[Snapshot] snapshot_thread stopped";
}

void MasterSnapshotManager::WaitForSnapshotChild(pid_t pid,
                                                 const std::string& snapshot_id,
                                                 int log_pipe_fd) {
    // Default 5 minute timeout
    const int64_t timeout_seconds = options_.snapshot_child_timeout_seconds;

    LOG(INFO)
        << "[Snapshot] waiting for child process to complete, snapshot_id="
        << snapshot_id << ", child_pid=" << pid
        << ", timeout=" << timeout_seconds << "s";

    // Set pipe to non-blocking mode
    int flags = fcntl(log_pipe_fd, F_GETFL, 0);
    if (flags == -1 || fcntl(log_pipe_fd, F_SETFL, flags | O_NONBLOCK) == -1) {
        LOG(WARNING) << "[Snapshot] Failed to set pipe non-blocking: "
                     << strerror(errno);
    }

    // Buffer for reading child logs
    char buf[4096];
    std::string log_buffer;

    // Helper lambda to read and output child logs
    auto flush_child_logs = [&]() {
        while (true) {
            ssize_t n = read(log_pipe_fd, buf, sizeof(buf) - 1);
            if (n > 0) {
                buf[n] = '\0';
                log_buffer += buf;
                // Output complete lines
                size_t pos;
                while ((pos = log_buffer.find('\n')) != std::string::npos) {
                    std::string line = log_buffer.substr(0, pos);
                    log_buffer.erase(0, pos + 1);
                    if (!line.empty()) {
                        LOG(INFO) << "[Snapshot:Child] " << line;
                    }
                }
            } else {
                break;
            }
        }
    };

    // Record start time
    auto start_time = std::chrono::steady_clock::now();

    // Use non-blocking polling to wait
    while (true) {
        // Read child logs first
        flush_child_logs();

        int status;
        pid_t result = waitpid(pid, &status, WNOHANG);

        if (result == -1) {
            LOG(ERROR) << "[Snapshot] Failed to wait for child process: "
                       << strerror(errno) << ", snapshot_id=" << snapshot_id
                       << ", child_pid=" << pid;
            MasterMetricManager::instance().inc_snapshot_fail();
            return;
        } else if (result == 0) {
            // Child process is still running
            auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(
                               std::chrono::steady_clock::now() - start_time)
                               .count();

            if (elapsed >= timeout_seconds) {
                // Timeout handling - flush remaining logs before killing
                flush_child_logs();
                if (!log_buffer.empty()) {
                    LOG(INFO) << "[Snapshot:Child] " << log_buffer;
                }
                HandleChildTimeout(pid, snapshot_id);
                MasterMetricManager::instance().inc_snapshot_fail();
                return;
            }

            // Brief sleep before checking again
            std::this_thread::sleep_for(std::chrono::seconds(2));
        } else {
            // Child process has exited
            // Flush remaining logs from child
            flush_child_logs();
            // Output any remaining incomplete line
            if (!log_buffer.empty()) {
                LOG(INFO) << "[Snapshot:Child] " << log_buffer;
            }

            HandleChildExit(pid, status, snapshot_id);
            auto elapsed =
                std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::steady_clock::now() - start_time)
                    .count();
            MasterMetricManager::instance().set_snapshot_duration_ms(elapsed);
            return;
        }
    }
}

void MasterSnapshotManager::HandleChildTimeout(pid_t pid,
                                               const std::string& snapshot_id) {
    LOG(WARNING) << "[Snapshot] Child process timeout, snapshot_id="
                 << snapshot_id << ", child_pid=" << pid
                 << ", killing child process";

    // Try to gracefully terminate the child process
    if (kill(pid, SIGTERM) == 0) {
        // Wait a few seconds to see if it exits gracefully
        std::this_thread::sleep_for(std::chrono::seconds(5));

        // Check if it has exited
        int status;
        if (waitpid(pid, &status, WNOHANG) == 0) {
            // Child process still not exited, force kill
            LOG(WARNING) << "[Snapshot] Child process still running, force "
                            "killing, snapshot_id="
                         << snapshot_id << ", child_pid=" << pid;
            kill(pid, SIGKILL);

            // Wait for force termination to complete
            waitpid(pid, &status, 0);
            LOG(WARNING)
                << "[Snapshot] Child process force killed, snapshot_id="
                << snapshot_id << ", child_pid=" << pid;
        } else {
            LOG(INFO) << "[Snapshot] Child process terminated gracefully after "
                         "SIGTERM, snapshot_id="
                      << snapshot_id << ", child_pid=" << pid;
        }
    } else {
        LOG(ERROR) << "[Snapshot] Failed to send SIGTERM to child process, "
                      "snapshot_id="
                   << snapshot_id << ", child_pid=" << pid
                   << ", error=" << strerror(errno);
    }
}

void MasterSnapshotManager::HandleChildExit(pid_t pid, int status,
                                            const std::string& snapshot_id) {
    if (WIFEXITED(status)) {
        int exit_code = WEXITSTATUS(status);
        if (exit_code != 0) {
            LOG(ERROR) << "[Snapshot] Child process exited with error code: "
                       << exit_code << ", snapshot_id=" << snapshot_id
                       << ", child_pid=" << pid;
            MasterMetricManager::instance().inc_snapshot_fail();
        } else {
            LOG(INFO) << "[Snapshot] Child process successfully persisted "
                         "state, snapshot_id="
                      << snapshot_id << ", child_pid=" << pid;
            MasterMetricManager::instance().inc_snapshot_success();
        }
    } else if (WIFSIGNALED(status)) {
        int signal = WTERMSIG(status);
        LOG(ERROR) << "[Snapshot] Child process terminated by signal: "
                   << signal << ", snapshot_id=" << snapshot_id
                   << ", child_pid=" << pid;
        MasterMetricManager::instance().inc_snapshot_fail();
    }
}

tl::expected<ha::OpLogSequenceId, SerializationError>
MasterSnapshotManager::ResolveSnapshotSequenceId() const {
    if (!options_.enable_ha || options_.ha_backend_type != "etcd") {
        // OpLog sequence ids start at 1. Returning 0 here is a sentinel that
        // means "no persisted OpLog boundary", so a standby that later calls
        // Recover(0) will replay from the first entry when oplog following is
        // enabled.
        return ha::OpLogSequenceId{0};
    }

#ifndef STORE_USE_ETCD
    return tl::make_unexpected(SerializationError(
        ErrorCode::UNAVAILABLE_IN_CURRENT_MODE,
        "etcd snapshot sequence resolution is unavailable in this build"));
#else
    auto oplog_store = GetSnapshotBoundaryOpLogStore();
    if (!oplog_store) {
        return tl::make_unexpected(oplog_store.error());
    }

    uint64_t sequence_id = 0;
    auto err = oplog_store.value()->GetLatestSequenceId(sequence_id);
    if (err == ErrorCode::OPLOG_ENTRY_NOT_FOUND) {
        return ha::OpLogSequenceId{0};
    }
    if (err != ErrorCode::OK) {
        return tl::make_unexpected(SerializationError(
            err, fmt::format("failed to resolve snapshot sequence boundary: {}",
                             toString(err))));
    }

    return static_cast<ha::OpLogSequenceId>(sequence_id);
#endif
}

#ifdef STORE_USE_ETCD
tl::expected<EtcdOpLogStore*, SerializationError>
MasterSnapshotManager::GetSnapshotBoundaryOpLogStore() const {
    if (options_.ha_backend_connstring.empty()) {
        return tl::make_unexpected(SerializationError(
            ErrorCode::INVALID_PARAMS,
            "etcd snapshot sequence resolution requires a backend connstring"));
    }

    std::lock_guard<std::mutex> lock(snapshot_boundary_oplog_store_mutex_);
    if (snapshot_boundary_oplog_store_ != nullptr) {
        return snapshot_boundary_oplog_store_.get();
    }

    auto err = EtcdHelper::ConnectToEtcdStoreClient(
        options_.ha_backend_connstring.c_str());
    if (err != ErrorCode::OK) {
        return tl::make_unexpected(SerializationError(
            err, fmt::format("failed to connect to etcd for snapshot boundary: "
                             "{}",
                             toString(err))));
    }

    auto oplog_store = std::make_unique<EtcdOpLogStore>(options_.cluster_id);
    err = oplog_store->Init();
    if (err != ErrorCode::OK) {
        return tl::make_unexpected(SerializationError(
            err, fmt::format("failed to initialize etcd oplog store: {}",
                             toString(err))));
    }

    snapshot_boundary_oplog_store_ = std::move(oplog_store);
    return snapshot_boundary_oplog_store_.get();
}
#endif

tl::expected<ha::SnapshotDescriptor, SerializationError>
MasterSnapshotManager::BuildSnapshotDescriptor(
    const std::string& snapshot_id, const std::string& manifest_path,
    const std::string& object_prefix) const {
    auto sequence_id = ResolveSnapshotSequenceId();
    if (!sequence_id) {
        return tl::make_unexpected(sequence_id.error());
    }

    const std::string& snapshot_root =
        snapshot_catalog_store_->GetSnapshotRoot();
    auto descriptor = ha::snapshot_catalog_store_detail::MakeSnapshotDescriptor(
        snapshot_root, snapshot_id);
    descriptor.last_included_seq = sequence_id.value();
    descriptor.producer_view_version = master_service_->view_version_;
    descriptor.manifest_key = manifest_path;
    descriptor.object_prefix = object_prefix;
    descriptor.created_at_ms = CurrentTimeMs();
    return descriptor;
}

tl::expected<void, SerializationError> MasterSnapshotManager::PersistState(
    const std::string& snapshot_id) {
    const std::string& snapshot_root =
        snapshot_catalog_store_->GetSnapshotRoot();
    const std::string path_prefix = snapshot_root + snapshot_id + "/";
    const std::string manifest_path = path_prefix + SNAPSHOT_MANIFEST_FILE;
    auto descriptor =
        BuildSnapshotDescriptor(snapshot_id, manifest_path, path_prefix);
    if (!descriptor) {
        return tl::make_unexpected(descriptor.error());
    }
    return PersistState(descriptor.value());
}

tl::expected<void, SerializationError> MasterSnapshotManager::PersistState(
    const ha::SnapshotDescriptor& descriptor) {
    const std::string& snapshot_id = descriptor.snapshot_id;
    const std::string& path_prefix = descriptor.object_prefix;
    const std::string& manifest_path = descriptor.manifest_key;

    try {
        if (!snapshot_catalog_store_) {
            return tl::make_unexpected(SerializationError(
                ErrorCode::PERSISTENT_FAIL,
                "snapshot catalog store is not initialized"));
        }

        SNAP_LOG_INFO(
            "[Snapshot] action=persisting_state start, snapshot_id={}, "
            "serializer_type={}, version={}",
            snapshot_id, SNAPSHOT_SERIALIZER_TYPE, SNAPSHOT_SERIALIZER_VERSION);
        MasterService::MetadataSerializer metadata_serializer(master_service_);
        SegmentSerializer segment_serializer(
            &master_service_->segment_manager_);
        TaskManagerSerializer task_manager_serializer(
            &master_service_->task_manager_);

        auto metadata_result = metadata_serializer.Serialize();
        if (!metadata_result) {
            SNAP_LOG_ERROR(
                "[Snapshot] metadata serialization failed, snapshot_id={}, "
                "code={}, msg={}",
                snapshot_id, toString(metadata_result.error().code),
                metadata_result.error().message);

            return tl::make_unexpected(metadata_result.error());
        }
        SNAP_LOG_INFO(
            "[Snapshot] metadata serialization_successful, snapshot_id={}",
            snapshot_id);

        auto segment_result = segment_serializer.Serialize();
        if (!segment_result) {
            SNAP_LOG_ERROR(
                "[Snapshot] segment serialization failed, snapshot_id={}, "
                "code={}, msg={}",
                snapshot_id, toString(segment_result.error().code),
                segment_result.error().message);
            return tl::make_unexpected(segment_result.error());
        }
        SNAP_LOG_INFO(
            "[Snapshot] segment serialization_successful, snapshot_id={}",
            snapshot_id);

        auto task_manager_result = task_manager_serializer.Serialize();
        if (!task_manager_result) {
            SNAP_LOG_ERROR(
                "[Snapshot] task manager serialization failed, snapshot_id={}, "
                "code={}, msg={}",
                snapshot_id, toString(task_manager_result.error().code),
                task_manager_result.error().message);
            return tl::make_unexpected(task_manager_result.error());
        }
        SNAP_LOG_INFO(
            "[Snapshot] task manager serialization_successful, snapshot_id={}",
            snapshot_id);

        const auto& serialized_metadata = metadata_result.value();
        const auto& serialized_segment = segment_result.value();
        const auto& serialized_task_manager = task_manager_result.value();

        // When backup_dir is enabled, try all uploads to ensure complete backup
        // When backup_dir is disabled, use fail-fast mode
        bool upload_success = true;
        std::string error_msg;
        SNAP_LOG_INFO("[Snapshot] Backend info: {}",
                      repository_->GetObjectStoreConnectionInfo());

        // Upload metadata
        std::string metadata_path = path_prefix + SNAPSHOT_METADATA_FILE;
        auto upload_result =
            repository_->UploadPayloadFile(serialized_metadata, metadata_path,
                                           SNAPSHOT_METADATA_FILE, snapshot_id);
        if (!upload_result) {
            SNAP_LOG_ERROR(
                "[Snapshot] metadata upload failed, snapshot_id={}, "
                "path={}, code={}, msg={}",
                snapshot_id, metadata_path,
                toString(upload_result.error().code),
                upload_result.error().message);
            if (!options_.use_snapshot_backup_dir) {
                return tl::make_unexpected(upload_result.error());
            }
            error_msg.append(upload_result.error().message + "\n");
            upload_success = false;
        }

        // Upload segment
        std::string segment_path = path_prefix + SNAPSHOT_SEGMENTS_FILE;
        upload_result =
            repository_->UploadPayloadFile(serialized_segment, segment_path,
                                           SNAPSHOT_SEGMENTS_FILE, snapshot_id);
        if (!upload_result) {
            SNAP_LOG_ERROR(
                "[Snapshot] segment upload failed, snapshot_id={}, "
                "path={}, code={}, msg={}",
                snapshot_id, segment_path, toString(upload_result.error().code),
                upload_result.error().message);
            if (!options_.use_snapshot_backup_dir) {
                return tl::make_unexpected(upload_result.error());
            }
            error_msg.append(upload_result.error().message + "\n");
            upload_success = false;
        }

        // Upload task manager
        std::string task_manager_path =
            path_prefix + SNAPSHOT_TASK_MANAGER_FILE;
        upload_result = repository_->UploadPayloadFile(
            serialized_task_manager, task_manager_path,
            SNAPSHOT_TASK_MANAGER_FILE, snapshot_id);
        if (!upload_result) {
            SNAP_LOG_ERROR(
                "[Snapshot] task_manager upload failed, snapshot_id={}, "
                "path={}, code={}, msg={}",
                snapshot_id, task_manager_path,
                toString(upload_result.error().code),
                upload_result.error().message);
            if (!options_.use_snapshot_backup_dir) {
                return tl::make_unexpected(upload_result.error());
            }
            error_msg.append(upload_result.error().message + "\n");
            upload_success = false;
        }

        // Upload manifest
        std::string manifest_content =
            fmt::format("{}|{}|{}", SNAPSHOT_SERIALIZER_TYPE,
                        SNAPSHOT_SERIALIZER_VERSION, snapshot_id);
        std::vector<uint8_t> manifest_bytes(manifest_content.begin(),
                                            manifest_content.end());
        upload_result = repository_->UploadPayloadFile(
            manifest_bytes, manifest_path, SNAPSHOT_MANIFEST_FILE, snapshot_id);
        if (!upload_result) {
            SNAP_LOG_ERROR(
                "[Snapshot] manifest upload failed, snapshot_id={}, "
                "path={}, code={}, msg={}",
                snapshot_id, manifest_path,
                toString(upload_result.error().code),
                upload_result.error().message);
            if (!options_.use_snapshot_backup_dir) {
                return tl::make_unexpected(upload_result.error());
            }
            error_msg.append(upload_result.error().message + "\n");
            upload_success = false;
        }

        if (!upload_success) {
            return tl::make_unexpected(
                SerializationError(ErrorCode::PERSISTENT_FAIL, error_msg));
        }

        // Publish snapshot catalog entry and advance the latest marker.
        std::string latest_path =
            snapshot_catalog_store_->GetSnapshotRoot() + SNAPSHOT_LATEST_FILE;
        std::string latest_content = snapshot_id;

        auto publish_result = repository_->PublishSnapshot(descriptor);
        if (publish_result != ErrorCode::OK) {
            SNAP_LOG_ERROR(
                "[Snapshot] latest update failed, snapshot_id={}, file={}, "
                "code={}",
                snapshot_id, latest_path, toString(publish_result));
            if (options_.use_snapshot_backup_dir) {
                auto save_path = fs::path(options_.snapshot_backup_dir) /
                                 SNAPSHOT_BACKUP_SAVE_DIR /
                                 SNAPSHOT_LATEST_FILE;
                auto save_result =
                    FileUtil::SaveStringToFile(latest_content, save_path);
                if (!save_result) {
                    SNAP_LOG_ERROR(
                        "[Snapshot] save latest to disk failed, "
                        "snapshot_id={}, "
                        "content={}, file={}",
                        snapshot_id, latest_content, save_path.string());
                }
            }

            return tl::make_unexpected(SerializationError(
                ErrorCode::PERSISTENT_FAIL,
                fmt::format("latest update {} failed", latest_path)));
        }
        SNAP_LOG_INFO(
            "[Snapshot] Upload latest success: {}, snapshot_id={}, "
            "content={}",
            latest_path, snapshot_id, latest_content);

        repository_->CleanupOldSnapshots(options_.snapshot_retention_count,
                                         snapshot_id);
        SNAP_LOG_INFO("[Snapshot] action=persisting_state end, snapshot_id={}",
                      snapshot_id);
    } catch (const std::exception& e) {
        SNAP_LOG_ERROR(
            "[Snapshot] Exception during state persistent, snapshot_id={}, "
            "error={}",
            snapshot_id, e.what());
        return tl::make_unexpected(SerializationError(
            ErrorCode::PERSISTENT_FAIL,
            fmt::format("Exception during state persistent: {}", e.what())));
    } catch (...) {
        SNAP_LOG_ERROR(
            "[Snapshot] Unknown exception during state persistent, "
            "snapshot_id={}",
            snapshot_id);
        return tl::make_unexpected(
            SerializationError(ErrorCode::PERSISTENT_FAIL,
                               "Unknown exception during state persistent"));
    }
    return {};
}

tl::expected<void, SerializationError>
MasterSnapshotManager::UploadSnapshotPayloadFile(
    const std::vector<uint8_t>& data, const std::string& path,
    const std::string& local_filename, const std::string& snapshot_id) {
    return repository_->UploadPayloadFile(data, path, local_filename,
                                          snapshot_id);
}

void MasterSnapshotManager::CleanupOldSnapshot(int keep_count,
                                               const std::string& snapshot_id) {
    repository_->CleanupOldSnapshots(keep_count, snapshot_id);
}

}  // namespace mooncake
