#include "serializer.h"
#include "storage_backend.h"

#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/uio.h>
#include <errno.h>
#include <cstdint>
#include <cstring>
#include <limits>

#include <optional>
#include <regex>
#include <string>
#include <thread>
#include <vector>
#include <algorithm>
#include <chrono>
#include <unordered_set>

#include <ylt/struct_pb.hpp>

#include "mutex.h"
#include "utils.h"

#include <ylt/util/tl/expected.hpp>

namespace {
struct FdGuard {
    int fd = -1;
    explicit FdGuard(int f) : fd(f) {}
    ~FdGuard() {
        if (fd >= 0) close(fd);
    }
    FdGuard(const FdGuard&) = delete;
    FdGuard& operator=(const FdGuard&) = delete;
    int get() const { return fd; }
    int release() {
        int r = fd;
        fd = -1;
        return r;
    }
};
}  // namespace

#include "storage/distributed/distributed_storage_backend.h"

namespace mooncake {

bool FilePerKeyConfig::Validate() const {
    if (fsdir.empty()) {
        LOG(ERROR) << "FilePerKeyConfig: fsdir is invalid";
        return false;
    }
    return true;
}

bool BucketBackendConfig::Validate() const {
    if (bucket_keys_limit <= 0) {
        LOG(ERROR) << "BucketBackendConfig: bucket_keys_limit must > 0";
        return false;
    }
    if (bucket_size_limit <= 0) {
        LOG(ERROR) << "BucketBackendConfig: bucket_size_limit must > 0";
        return false;
    }
    return true;
}

FilePerKeyConfig FilePerKeyConfig::FromEnvironment() {
    FilePerKeyConfig config;

    config.fsdir = GetEnvStringOr("MOONCAKE_OFFLOAD_FSDIR", config.fsdir);

    config.enable_eviction = GetEnvOr<bool>(
        "MOONCAKE_OFFLOAD_ENABLE_EVICTION",
        GetEnvOr<bool>("ENABLE_EVICTION", config.enable_eviction));

    return config;
}

BucketBackendConfig BucketBackendConfig::FromEnvironment() {
    BucketBackendConfig config;

    config.bucket_keys_limit = GetEnvOr<int64_t>(
        "MOONCAKE_OFFLOAD_BUCKET_KEYS_LIMIT", config.bucket_keys_limit);

    config.bucket_size_limit = GetEnvOr<int64_t>(
        "MOONCAKE_OFFLOAD_BUCKET_SIZE_LIMIT_BYTES", config.bucket_size_limit);

    config.max_total_size =
        GetEnvOr<int64_t>("MOONCAKE_OFFLOAD_BUCKET_MAX_TOTAL_SIZE",
                          GetEnvOr<int64_t>("MOONCAKE_BUCKET_MAX_TOTAL_SIZE",
                                            config.max_total_size));

    const auto policy_str = GetEnvStringOr(
        "MOONCAKE_OFFLOAD_BUCKET_EVICTION_POLICY",
        GetEnvStringOr("MOONCAKE_BUCKET_EVICTION_POLICY", "fifo"));
    if (policy_str == "fifo") {
        config.eviction_policy = BucketEvictionPolicy::FIFO;
    } else if (policy_str == "lru") {
        config.eviction_policy = BucketEvictionPolicy::LRU;
    } else {
        config.eviction_policy = BucketEvictionPolicy::NONE;
    }

    return config;
}

bool OffsetAllocatorBackendConfig::Validate() const {
    if (persist_mode == OffsetPersistMode::kRelaxed) {
        if (persist_interval_seconds < 5) {
            LOG(ERROR) << "OffsetAllocatorBackendConfig: "
                          "persist_interval_seconds must be >= 5 for "
                          "kRelaxed mode";
            return false;
        }
    }
    if (high_ratio <= 0.0 || high_ratio > 1.0) {
        LOG(ERROR)
            << "OffsetAllocatorBackendConfig: high_ratio must be in (0,1]";
        return false;
    }
    if (low_ratio <= 0.0 || low_ratio >= high_ratio) {
        LOG(ERROR) << "OffsetAllocatorBackendConfig: low_ratio must be in (0, "
                      "high_ratio)";
        return false;
    }
    if (keys_high_ratio <= 0.0 || keys_high_ratio > 1.0) {
        LOG(ERROR)
            << "OffsetAllocatorBackendConfig: keys_high_ratio must be in (0,1]";
        return false;
    }
    if (keys_low_ratio <= 0.0 || keys_low_ratio >= keys_high_ratio) {
        LOG(ERROR) << "OffsetAllocatorBackendConfig: keys_low_ratio must be in "
                      "(0, keys_high_ratio)";
        return false;
    }
    if (max_evict_per_offload == 0) {
        LOG(ERROR) << "OffsetAllocatorBackendConfig: max_evict_per_offload "
                      "must be > 0";
        return false;
    }
    if (fallback_evict_batch == 0) {
        LOG(ERROR)
            << "OffsetAllocatorBackendConfig: fallback_evict_batch must be > 0";
        return false;
    }
    if (max_capacity_nodes < 0) {
        LOG(ERROR)
            << "OffsetAllocatorBackendConfig: max_capacity_nodes must be >= 0";
        return false;
    }
    return true;
}

static std::optional<double> GetEnvDouble(const char* name) {
    const char* env = std::getenv(name);
    if (!env || env[0] == '\0') return std::nullopt;
    try {
        return std::stod(env);
    } catch (...) {
        return std::nullopt;
    }
}

OffsetAllocatorBackendConfig OffsetAllocatorBackendConfig::FromEnvironment() {
    OffsetAllocatorBackendConfig cfg;

    const char* pol = std::getenv("MOONCAKE_OFFSET_EVICTION_POLICY");
    if (pol) {
        std::string s(pol);
        if (s == "fifo" || s == "FIFO" || s == "Fifo") {
            cfg.eviction_policy = OffsetEvictionPolicy::FIFO;
        }
        // NONE is default; LRU reserved for phase 2
    }

    if (auto v = GetEnvDouble("MOONCAKE_OFFSET_HIGH_RATIO"))
        cfg.high_ratio = *v;
    if (auto v = GetEnvDouble("MOONCAKE_OFFSET_LOW_RATIO")) cfg.low_ratio = *v;
    // Both byte and key watermarks derive from the same ratio pair.
    cfg.keys_high_ratio = cfg.high_ratio;
    cfg.keys_low_ratio = cfg.low_ratio;

    cfg.max_capacity_nodes = GetEnvOr<int64_t>(
        "MOONCAKE_OFFSET_MAX_CAPACITY_NODES", cfg.max_capacity_nodes);

    // Read eviction cap as int64_t to guard against negative env values
    // which would wrap to SIZE_MAX with GetEnvOr<size_t>.
    auto max_evict_raw =
        GetEnvOr<int64_t>("MOONCAKE_OFFSET_MAX_EVICT_PER_OFFLOAD",
                          static_cast<int64_t>(cfg.max_evict_per_offload));
    if (max_evict_raw > 0) {
        cfg.max_evict_per_offload = static_cast<size_t>(max_evict_raw);
    } else if (max_evict_raw <= 0) {
        LOG(WARNING) << "MOONCAKE_OFFSET_MAX_EVICT_PER_OFFLOAD="
                     << max_evict_raw << " is non-positive; using default "
                     << cfg.max_evict_per_offload;
    }

    // Persistence mode
    const char* persist = std::getenv("MOONCAKE_OFFSET_PERSIST_MODE");
    if (persist) {
        std::string s(persist);
        if (s == "disabled" || s == "DISABLED") {
            cfg.persist_mode = OffsetPersistMode::kDisabled;
        } else if (s == "relaxed" || s == "RELAXED") {
            cfg.persist_mode = OffsetPersistMode::kRelaxed;
        } else if (s == "strict" || s == "STRICT") {
            cfg.persist_mode = OffsetPersistMode::kStrict;
        } else {
            LOG(WARNING) << "Unknown MOONCAKE_OFFSET_PERSIST_MODE=" << s
                         << "; using default (disabled)";
        }
    }

    cfg.persist_interval_seconds =
        GetEnvOr<int64_t>("MOONCAKE_OFFSET_PERSIST_INTERVAL_SECONDS",
                          cfg.persist_interval_seconds);

    return cfg;
}

StorageBackendInterface::StorageBackendInterface(
    const FileStorageConfig& config)
    : file_storage_config_(config) {}

std::string StorageBackend::GetActualFsdir() const {
    std::string actual_fsdir = fsdir_;
    if (actual_fsdir.rfind("moon_", 0) == 0) {
        actual_fsdir = actual_fsdir.substr(5);
    }
    return actual_fsdir;
}

void StorageBackend::RecalculateAvailableSpace() {
    if (total_space_ >= used_space_) {
        available_space_ = total_space_ - used_space_;
    } else {
        available_space_ = 0;
    }
}

bool StorageBackend::IsEvictionEnabled() const { return enable_eviction_; }

Mutex& StorageBackend::GetFilePathMutex(const std::string& path) {
    return file_path_mutexes_[std::hash<std::string>{}(path) %
                              kFilePathLockCount];
}

bool StorageBackend::IsFilePendingEviction(const std::string& path) const {
    std::shared_lock<std::shared_mutex> lock(file_queue_mutex_);
    return pending_eviction_paths_.find(path) != pending_eviction_paths_.end();
}

tl::expected<void, ErrorCode> StorageBackend::Init(uint64_t quota_bytes = 0) {
    // Skip eviction initialization if disabled
    if (!IsEvictionEnabled()) {
        initialized_.store(true, std::memory_order_release);
        return {};
    }

    if (initialized_.load(std::memory_order_acquire)) {
        LOG(WARNING) << "StorageBackend is already initialized. Skipping.";
        return {};
    }

    namespace fs = std::filesystem;
    std::string actual_fsdir = GetActualFsdir();
    fs::path storage_root = fs::path(root_dir_) / actual_fsdir;

    std::error_code ec;
    if (!fs::exists(storage_root)) {
        fs::create_directories(storage_root, ec);
        if (ec) {
            LOG(ERROR) << "Failed to create storage root directory: "
                       << storage_root;
            return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
        }
    }
    const auto space_info = fs::space(storage_root, ec);
    if (ec) {
        LOG(ERROR) << "Init: Failed to get disk space info: " << ec.message();
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }
    LOG(INFO) << "Reconstructing storage state from disk at: " << storage_root;
    std::vector<fs::directory_entry> existing_files;
    try {
        for (const auto& entry :
             fs::recursive_directory_iterator(storage_root)) {
            if (entry.is_regular_file(ec) && !ec) {
                existing_files.push_back(entry);
            }
        }
    } catch (const fs::filesystem_error& e) {
        LOG(ERROR) << "Error during disk scan for state reconstruction: "
                   << e.what();
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }
    std::sort(existing_files.begin(), existing_files.end(),
              [](const auto& a, const auto& b) {
                  struct statx stx_a, stx_b;
                  bool success_a = (statx(AT_FDCWD, a.path().c_str(), 0,
                                          STATX_BTIME, &stx_a) == 0);
                  bool success_b = (statx(AT_FDCWD, b.path().c_str(), 0,
                                          STATX_BTIME, &stx_b) == 0);

                  if (!success_a || !success_b) {
                      return false;
                  }

                  if (stx_a.stx_btime.tv_sec == stx_b.stx_btime.tv_sec) {
                      return stx_a.stx_btime.tv_nsec < stx_b.stx_btime.tv_nsec;
                  }
                  return stx_a.stx_btime.tv_sec < stx_b.stx_btime.tv_sec;
              });
    bool eviction_needed = false;
    {
        std::unique_lock<std::shared_mutex> space_lock(space_mutex_);
        std::unique_lock<std::shared_mutex> queue_lock(file_queue_mutex_);
        used_space_ = 0;

        for (const auto& entry : existing_files) {
            uint64_t file_size = entry.file_size(ec);
            if (!ec) {
                const std::string& path_str = entry.path().string();
                file_write_queue_.push_back({path_str, file_size, ""});
                file_queue_map_[path_str] = std::prev(file_write_queue_.end());
                used_space_ += file_size;
            } else {
                LOG(WARNING) << "Could not get size of existing file "
                             << entry.path() << ", skipping.";
            }
        }
        if (quota_bytes > 0) {
            total_space_ = quota_bytes;
        } else {
            constexpr double kDefaultQuotaPercentage = 0.9;
            total_space_ = static_cast<uint64_t>(space_info.capacity *
                                                 kDefaultQuotaPercentage);
        }
        if (total_space_ >= used_space_) {
            RecalculateAvailableSpace();
        } else {
            // Only enable eviction for local storage
            if (IsEvictionEnabled()) {
                eviction_needed = true;
                available_space_ = -1;
                LOG(WARNING)
                    << "Existing used space (" << used_space_
                    << ") exceeds the new quota (" << total_space_
                    << "). Eviction will be triggered after initial setup.";
            } else {
                // Eviction disabled, just log a warning but don't trigger
                // eviction
                LOG(WARNING) << "Existing used space (" << used_space_
                             << ") exceeds the new quota (" << total_space_
                             << "). Eviction is disabled.";
                RecalculateAvailableSpace();  // Still calculate available space
            }
        }
    }
    if (eviction_needed) {
        if (!InitQuotaEvict()) {
            LOG(ERROR) << "Initialization failed due to failure in enforcing "
                          "storage quota.";
            initialized_.store(false, std::memory_order_release);
            return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
        }
    }

    {
        std::unique_lock<std::shared_mutex> lock(space_mutex_);
        RecalculateAvailableSpace();

        LOG(INFO) << "Init: "
                  << "Quota: " << total_space_ << ", Used: " << used_space_
                  << ", Available: " << available_space_;
    }

    initialized_.store(true, std::memory_order_release);
    return {};
}

bool StorageBackend::InitQuotaEvict() {
    size_t initial_queue_size;
    {
        std::shared_lock<std::shared_mutex> lock(file_queue_mutex_);
        initial_queue_size = file_write_queue_.size();
    }

    const size_t kMaxEvictionAttempts = initial_queue_size + 100;
    size_t eviction_attempts = 0;

    while (eviction_attempts < kMaxEvictionAttempts) {
        if (used_space_ <= total_space_) {
            break;
        }

        FileRecord evicted = EvictFile();
        if (evicted.path.empty()) {
            LOG(ERROR) << "Failed to evict file to meet quota. "
                       << "The queue might be empty or a file is unremovable.";
            return false;
        }
        eviction_attempts++;
    }

    if (used_space_ > total_space_) {
        LOG(ERROR) << "Could not bring storage usage under quota after "
                   << eviction_attempts << " eviction attempts.";
        return false;
    }

    // Recalculate available_space_ after eviction
    {
        std::unique_lock<std::shared_mutex> lock(space_mutex_);
        RecalculateAvailableSpace();
    }

    return true;
}

tl::expected<std::vector<std::string>, ErrorCode> StorageBackend::StoreObject(
    const std::string& path, const std::vector<Slice>& slices,
    const std::string& key,
    StorageBackendInterface::EvictionHandler eviction_handler) {
    size_t total_size = 0;
    for (const auto& slice : slices) {
        total_size += slice.size;
    }

    // For eviction-enabled mode, check space and reserve
    std::vector<std::string> evicted_keys;
    uint64_t reserved_size = 0;
    if (IsEvictionEnabled()) {
        if (!initialized_.load(std::memory_order_acquire)) {
            LOG(ERROR)
                << "StorageBackend is not initialized. Call Init() before "
                   "storing objects.";
            return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
        }

        auto space_result = EnsureDiskSpace(total_size, eviction_handler);
        if (!space_result) {
            return tl::make_unexpected(space_result.error());
        }
        evicted_keys = std::move(space_result.value());
        reserved_size = total_size;
    }

    MutexLocker path_locker(&GetFilePathMutex(path));
    if (IsEvictionEnabled() && IsFilePendingEviction(path)) {
        ReleaseSpace(reserved_size);
        return tl::make_unexpected(ErrorCode::FILE_WRITE_FAIL);
    }

    // Create file and write data (common logic for both modes)
    auto file_result = CreateFileForWriting(path, reserved_size);
    if (!file_result) {
        return tl::make_unexpected(file_result.error());
    }

    auto write_result =
        WriteSlicesToFile(file_result.value(), path, slices, reserved_size);
    if (!write_result) {
        return tl::make_unexpected(write_result.error());
    }

    // For eviction-enabled mode, add file to tracking queue
    if (IsEvictionEnabled()) {
        AddFileToWriteQueue(path, total_size, key);
    }

    return evicted_keys;
}

tl::expected<std::vector<std::string>, ErrorCode> StorageBackend::StoreObject(
    const std::string& path, const std::string& str, const std::string& key,
    StorageBackendInterface::EvictionHandler eviction_handler) {
    return StoreObject(path, std::span<const char>(str.data(), str.size()), key,
                       eviction_handler);
}

tl::expected<std::vector<std::string>, ErrorCode> StorageBackend::StoreObject(
    const std::string& path, std::span<const char> data, const std::string& key,
    StorageBackendInterface::EvictionHandler eviction_handler) {
    size_t file_total_size = data.size();

    // For eviction-enabled mode, check space and reserve
    std::vector<std::string> evicted_keys;
    uint64_t reserved_size = 0;
    if (IsEvictionEnabled()) {
        if (!initialized_.load(std::memory_order_acquire)) {
            LOG(ERROR)
                << "StorageBackend is not initialized. Call Init() before "
                   "storing objects.";
            return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
        }

        auto space_result = EnsureDiskSpace(file_total_size, eviction_handler);
        if (!space_result) {
            return tl::make_unexpected(space_result.error());
        }
        evicted_keys = std::move(space_result.value());
        reserved_size = file_total_size;
    }

    MutexLocker path_locker(&GetFilePathMutex(path));
    if (IsEvictionEnabled() && IsFilePendingEviction(path)) {
        ReleaseSpace(reserved_size);
        return tl::make_unexpected(ErrorCode::FILE_WRITE_FAIL);
    }

    // Create file and write data (common logic for both modes)
    auto file_result = CreateFileForWriting(path, reserved_size);
    if (!file_result) {
        return tl::make_unexpected(file_result.error());
    }

    auto write_result =
        WriteDataToFile(file_result.value(), path, data, reserved_size);
    if (!write_result) {
        return tl::make_unexpected(write_result.error());
    }

    // For eviction-enabled mode, add file to tracking queue
    if (IsEvictionEnabled()) {
        AddFileToWriteQueue(path, file_total_size, key);
    }

    return evicted_keys;
}

tl::expected<void, ErrorCode> StorageBackend::LoadObject(
    const std::string& path, std::vector<Slice>& slices, int64_t length) {
    ResolvePath(path);
    auto file = create_file(path, FileMode::Read);
    if (!file) {
        LOG(ERROR) << "Failed to open file for reading: " << path;
        return tl::make_unexpected(ErrorCode::FILE_OPEN_FAIL);
    }

    off_t current_offset = 0;
    int64_t total_bytes_processed = 0;

    std::vector<iovec> iovs_chunk;
    off_t chunk_start_offset = 0;
    int64_t chunk_length = 0;

    auto process_chunk = [&]() -> tl::expected<void, ErrorCode> {
        if (iovs_chunk.empty()) {
            return {};
        }

        auto read_result = file->vector_read(
            iovs_chunk.data(), static_cast<int>(iovs_chunk.size()),
            chunk_start_offset);
        if (!read_result) {
            LOG(ERROR) << "vector_read failed for chunk at offset "
                       << chunk_start_offset << " for path: " << path
                       << ", error: " << read_result.error();
            return tl::make_unexpected(read_result.error());
        }
        if (*read_result != static_cast<size_t>(chunk_length)) {
            LOG(ERROR) << "Read size mismatch for chunk in path: " << path
                       << ", expected: " << chunk_length
                       << ", got: " << *read_result;
            return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
        }

        total_bytes_processed += chunk_length;

        iovs_chunk.clear();
        chunk_length = 0;

        return {};
    };

    for (const auto& slice : slices) {
        if (slice.ptr != nullptr) {
            if (iovs_chunk.empty()) {
                chunk_start_offset = current_offset;
            }
            iovs_chunk.push_back({slice.ptr, slice.size});
            chunk_length += slice.size;
        } else {
            auto result = process_chunk();
            if (!result) {
                return result;
            }

            total_bytes_processed += slice.size;
        }

        current_offset += slice.size;
    }

    auto result = process_chunk();
    if (!result) {
        return result;
    }

    if (total_bytes_processed != length) {
        LOG(ERROR) << "Total read size mismatch for: " << path
                   << ", expected: " << length
                   << ", got: " << total_bytes_processed;
        return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
    }

    return {};
}

tl::expected<void, ErrorCode> StorageBackend::LoadObject(
    const std::string& path, std::string& str, int64_t length) {
    ResolvePath(path);
    auto file = create_file(path, FileMode::Read);
    if (!file) {
        LOG(ERROR) << "Failed to open file for reading: " << path;
        return tl::make_unexpected(ErrorCode::FILE_OPEN_FAIL);
    }

    auto read_result = file->read(str, length);
    if (!read_result) {
        LOG(ERROR) << "read failed for: " << path
                   << ", error: " << read_result.error();
        return tl::make_unexpected(read_result.error());
    }
    if (*read_result != static_cast<size_t>(length)) {
        LOG(ERROR) << "Read size mismatch for: " << path
                   << ", expected: " << length << ", got: " << *read_result;
        return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
    }

    return {};
}

void StorageBackend::RemoveFile(const std::string& path) {
    namespace fs = std::filesystem;
    // TODO: attention: this function is not thread-safe, need to add lock if
    // used in multi-thread environment Check if the file exists before
    // attempting to remove it
    // TODO: add a sleep to ensure the write thread has time to create the
    // corresponding file it will be fixed in the next version
    std::this_thread::sleep_for(
        std::chrono::microseconds(50));  // sleep for 50 us

    MutexLocker path_locker(&GetFilePathMutex(path));

    // Eviction disabled, use simple delete (no queue tracking)
    if (!IsEvictionEnabled()) {
        if (fs::exists(path)) {
            std::error_code ec;
            fs::remove(path, ec);
            if (ec) {
                LOG(ERROR) << "Failed to delete file: " << path
                           << ", error: " << ec.message();
            }
        }
        return;
    }

    // Eviction-enabled logic (local mode)
    uint64_t file_size = 0;
    std::error_code ec;
    {
        std::shared_lock<std::shared_mutex> lock(file_queue_mutex_);
        auto map_it = file_queue_map_.find(path);
        if (map_it != file_queue_map_.end()) {
            file_size = map_it->second->size;
        } else {
            lock.unlock();
            LOG(WARNING) << "File not found in tracking queue, assuming it's "
                            "already removed or untracked: "
                         << path;
            return;
        }
    }

    if (fs::remove(path, ec)) {
        LOG(INFO) << "Successfully removed file: " << path;
    } else {
        if (ec && ec != std::errc::no_such_file_or_directory) {
            LOG(ERROR) << "Failed to remove file: " << path
                       << ", Error: " << ec.message();
            return;
        }
    }

    RemoveFileFromWriteQueue(path);

    ReleaseSpace(file_size);
}

void StorageBackend::RemoveByRegex(const std::string& regex_pattern) {
    namespace fs = std::filesystem;
    std::regex pattern;
    try {
        pattern = std::regex(regex_pattern, std::regex::ECMAScript);
    } catch (const std::regex_error& e) {
        LOG(ERROR) << "Invalid regex pattern for storage removal: "
                   << regex_pattern << ", error: " << e.what();
        return;
    }

    // Eviction disabled, use simple delete (no queue tracking)
    if (!IsEvictionEnabled()) {
        fs::path storage_root = fs::path(root_dir_) / fsdir_;
        if (!fs::exists(storage_root) || !fs::is_directory(storage_root)) {
            LOG(WARNING) << "Storage root directory does not exist: "
                         << storage_root;
            return;
        }

        std::vector<fs::path> paths_to_remove;

        for (const auto& entry :
             fs::recursive_directory_iterator(storage_root)) {
            if (fs::is_regular_file(entry.status())) {
                std::string filename = entry.path().filename().string();

                if (std::regex_search(filename, pattern)) {
                    paths_to_remove.push_back(entry.path());
                }
            }
        }

        for (const auto& path : paths_to_remove) {
            RemoveFile(path.string());
        }

        return;
    }

    // Eviction-enabled logic (local mode)
    std::vector<std::string> paths_to_remove;
    {
        std::shared_lock<std::shared_mutex> lock(file_queue_mutex_);
        for (const auto& record : file_write_queue_) {
            std::string filename = fs::path(record.path).filename().string();
            if (std::regex_search(filename, pattern)) {
                paths_to_remove.push_back(record.path);
            }
        }
    }

    for (const auto& path : paths_to_remove) {
        RemoveFile(path);
    }
}

void StorageBackend::RemoveAll() {
    namespace fs = std::filesystem;

    // Eviction disabled, use simple delete (no queue tracking)
    if (!IsEvictionEnabled()) {
        std::vector<std::string> paths_to_remove;
        // Iterate through the root directory and remove all files
        for (const auto& entry : fs::directory_iterator(root_dir_)) {
            if (fs::is_regular_file(entry.status())) {
                paths_to_remove.push_back(entry.path().string());
            }
        }
        for (const auto& path : paths_to_remove) {
            RemoveFile(path);
        }
        return;
    }

    // Eviction-enabled logic (local mode)
    try {
        std::vector<std::string> paths_to_remove;
        {
            std::shared_lock<std::shared_mutex> lock(file_queue_mutex_);
            paths_to_remove.reserve(file_write_queue_.size());
            for (const auto& record : file_write_queue_) {
                paths_to_remove.push_back(record.path);
            }
        }

        for (const auto& path : paths_to_remove) {
            RemoveFile(path);
        }
    } catch (const fs::filesystem_error& e) {
        LOG(ERROR) << "Filesystem error when removing all files: " << e.what();
    }
}

void StorageBackend::ResolvePath(const std::string& path) const {
    // Safely construct path using std::filesystem
    namespace fs = std::filesystem;
    fs::path full_path = path;

    // Create all parent directories if they don't exist
    std::error_code ec;
    fs::path parent_path = full_path.parent_path();
    if (!parent_path.empty() && !fs::exists(parent_path)) {
        if (!fs::create_directories(parent_path, ec) && ec) {
            LOG(ERROR) << "Failed to create directories: " << parent_path
                       << ", error: " << ec.message();
        }
    }
}

tl::expected<std::unique_ptr<StorageFile>, ErrorCode>
StorageBackend::CreateFileForWriting(const std::string& path,
                                     uint64_t reserved_size) {
    ResolvePath(path);
    auto file = create_file(path, FileMode::Write);
    if (!file) {
        LOG(ERROR) << "Failed to open file for writing: " << path;
        if (reserved_size > 0) {
            ReleaseSpace(reserved_size);
        }
        return tl::make_unexpected(ErrorCode::FILE_OPEN_FAIL);
    }
    return file;
}

tl::expected<size_t, ErrorCode> StorageBackend::WriteSlicesToFile(
    std::unique_ptr<StorageFile>& file, const std::string& path,
    const std::vector<Slice>& slices, uint64_t reserved_size) {
    std::vector<iovec> iovs;
    iovs.reserve(slices.size());
    size_t slices_total_size = 0;
    for (const auto& slice : slices) {
        iovec io{slice.ptr, slice.size};
        iovs.push_back(io);
        slices_total_size += slice.size;
    }

    auto write_result =
        file->vector_write(iovs.data(), static_cast<int>(iovs.size()), 0);
    if (!write_result) {
        LOG(ERROR) << "vector_write failed for: " << path
                   << ", error: " << write_result.error();
        if (reserved_size > 0) {
            ReleaseSpace(reserved_size);
        }
        return tl::make_unexpected(write_result.error());
    }

    if (*write_result != slices_total_size) {
        LOG(ERROR) << "Write size mismatch for: " << path
                   << ", expected: " << slices_total_size
                   << ", got: " << *write_result;
        if (reserved_size > 0) {
            ReleaseSpace(reserved_size);
        }
        return tl::make_unexpected(ErrorCode::FILE_WRITE_FAIL);
    }

    return slices_total_size;
}

tl::expected<size_t, ErrorCode> StorageBackend::WriteDataToFile(
    std::unique_ptr<StorageFile>& file, const std::string& path,
    std::span<const char> data, uint64_t reserved_size) {
    size_t file_total_size = data.size();
    auto write_result = file->write(data, file_total_size);

    if (!write_result) {
        LOG(ERROR) << "Write failed for: " << path
                   << ", error: " << write_result.error();
        if (reserved_size > 0) {
            ReleaseSpace(reserved_size);
        }
        return tl::make_unexpected(write_result.error());
    }
    if (*write_result != file_total_size) {
        LOG(ERROR) << "Write size mismatch for: " << path
                   << ", expected: " << file_total_size
                   << ", got: " << *write_result;
        if (reserved_size > 0) {
            ReleaseSpace(reserved_size);
        }
        return tl::make_unexpected(ErrorCode::FILE_WRITE_FAIL);
    }

    return file_total_size;
}

std::unique_ptr<StorageFile> StorageBackend::create_file(
    const std::string& path, FileMode mode) const {
    int flags = O_CLOEXEC;
    int access_mode = 0;
    switch (mode) {
        case FileMode::Read:
            access_mode = O_RDONLY;
            break;
        case FileMode::Write:
            access_mode = O_WRONLY | O_CREAT | O_TRUNC;
            break;
    }

#ifdef USE_URING
    // Use O_DIRECT only for reads: write latency is not sensitive in this
    // scenario, and O_DIRECT writes require 4096-byte alignment padding which
    // corrupts meta file parsing and wastes disk space on data files.
    if (use_uring_ && mode == FileMode::Read) {
        flags |= O_DIRECT;
    }
#endif

    int fd = open(path.c_str(), flags | access_mode, 0644);
    if (fd < 0) {
        return nullptr;
    }

#ifdef USE_URING
    if (use_uring_) {
        // use_direct_io mirrors the O_DIRECT flag: true for reads, false for
        // writes. This avoids unnecessary bounce-buffer allocation on the write
        // path while keeping correct alignment enforcement on the read path.
        bool use_direct_io = (mode == FileMode::Read);
        return std::make_unique<UringFile>(path, fd, 32, use_direct_io);
    }
#endif
    return std::make_unique<PosixFile>(path, fd);
}

bool StorageBackend::CheckDiskSpace(size_t required_size) {
    std::unique_lock<std::shared_mutex> lock(space_mutex_);

    if (!initialized_.load(std::memory_order_acquire)) {
        LOG(ERROR) << "CheckDiskSpace called before StorageBackend::Init was "
                      "completed.";
        return false;
    }

    bool has_enough_space = available_space_ >= required_size;

    if (has_enough_space) {
        // Also check actual disk space to handle multiple instances
        // sharing the same filesystem with independent quotas.
        namespace fs = std::filesystem;
        fs::path storage_root = fs::path(root_dir_) / GetActualFsdir();
        std::error_code ec;
        auto space_info = fs::space(storage_root, ec);
        if (!ec) {
            uint64_t actual_available = space_info.available;
            constexpr uint64_t kMinFreeSpace = 256 * kMB;
            if (actual_available < required_size + kMinFreeSpace) {
                VLOG(1) << "Actual disk space low: available="
                        << actual_available << ", required=" << required_size
                        << ". Triggering eviction.";
                has_enough_space = false;
            }
        } else {
            LOG(WARNING) << "Failed to get disk space info for " << storage_root
                         << ": " << ec.message();
        }
    }

    if (has_enough_space) {
        used_space_ += required_size;
        available_space_ -= required_size;
        VLOG(2) << "Reserved space. New available: " << available_space_
                << ", New used (this session): " << used_space_;
    }

    return has_enough_space;
}

FileRecord StorageBackend::EvictFile() {
    // Eviction is only enabled for local storage
    if (!IsEvictionEnabled()) {
        LOG(WARNING) << "Eviction is disabled. Cannot evict files.";
        return {};
    }

    // Use FIFO based strategy (earliest written first out)
    FileRecord record_to_evict = PopFileToEvictByFIFO();

    if (record_to_evict.path.empty()) {
        LOG(WARNING) << "No file selected for eviction";
        return {};
    }

    auto delete_result = DeleteEvictedFile(record_to_evict);
    if (delete_result) {
        return record_to_evict;
    }
    RestoreFileToWriteQueueFront(record_to_evict);
    return {};
}

void StorageBackend::AddFileToWriteQueue(const std::string& path, uint64_t size,
                                         const std::string& key) {
    uint64_t replaced_size = 0;
    {
        std::unique_lock<std::shared_mutex> lock(file_queue_mutex_);

        auto it = file_queue_map_.find(path);
        if (it != file_queue_map_.end()) {
            replaced_size = it->second->size;
            file_write_queue_.erase(it->second);
            file_queue_map_.erase(it);
        }

        file_write_queue_.push_back({path, size, key});
        file_queue_map_[path] = std::prev(file_write_queue_.end());
    }

    ReleaseSpace(replaced_size);
}

void StorageBackend::RemoveFileFromWriteQueue(const std::string& path) {
    std::unique_lock<std::shared_mutex> lock(file_queue_mutex_);

    auto it = file_queue_map_.find(path);
    if (it != file_queue_map_.end()) {
        file_write_queue_.erase(it->second);
        file_queue_map_.erase(it);
        VLOG(2) << "Removed file from eviction queue: " << path
                << ". New queue size: " << file_write_queue_.size();
    }
}

FileRecord StorageBackend::PopFileToEvictByFIFO() {
    while (true) {
        std::string candidate_path;
        {
            std::shared_lock<std::shared_mutex> lock(file_queue_mutex_);
            if (file_write_queue_.empty()) {
                LOG(WARNING) << "Queue is empty, cannot select file to evict";
                return {};
            }
            candidate_path = file_write_queue_.front().path;
        }

        MutexLocker path_locker(&GetFilePathMutex(candidate_path));
        std::unique_lock<std::shared_mutex> lock(file_queue_mutex_);
        if (file_write_queue_.empty() ||
            file_write_queue_.front().path != candidate_path) {
            continue;
        }

        auto map_it = file_queue_map_.find(candidate_path);
        if (map_it == file_queue_map_.end() ||
            map_it->second != file_write_queue_.begin()) {
            continue;
        }

        FileRecord record = file_write_queue_.front();
        file_queue_map_.erase(map_it);
        file_write_queue_.pop_front();
        pending_eviction_paths_.insert(record.path);
        return record;
    }
}

void StorageBackend::RestoreFileToWriteQueueFront(const FileRecord& record) {
    if (record.path.empty()) {
        return;
    }

    bool was_replaced = false;
    {
        MutexLocker path_locker(&GetFilePathMutex(record.path));
        std::unique_lock<std::shared_mutex> lock(file_queue_mutex_);
        pending_eviction_paths_.erase(record.path);
        if (file_queue_map_.find(record.path) != file_queue_map_.end()) {
            was_replaced = true;
        } else {
            file_write_queue_.push_front(record);
            file_queue_map_[record.path] = file_write_queue_.begin();
        }
    }

    if (was_replaced) {
        LOG(ERROR) << "Cannot restore evicted record because the path was "
                      "replaced: "
                   << record.path;
        ReleaseSpace(record.size);
    }
}

tl::expected<void, ErrorCode> StorageBackend::DeleteEvictedFile(
    const FileRecord& record) {
    namespace fs = std::filesystem;
    MutexLocker path_locker(&GetFilePathMutex(record.path));

    bool was_replaced = false;
    {
        std::unique_lock<std::shared_mutex> queue_lock(file_queue_mutex_);
        if (file_queue_map_.find(record.path) != file_queue_map_.end()) {
            // A newer StoreObject completed after this record was selected.
            // The old file contents were replaced in place, so release the old
            // accounting without deleting the new file.
            was_replaced = true;
            pending_eviction_paths_.erase(record.path);
        }
    }
    if (was_replaced) {
        ReleaseSpace(record.size);
        return {};
    }

    std::error_code ec;

    if (fs::remove(record.path, ec) || !ec ||
        ec == std::errc::no_such_file_or_directory) {
        {
            std::unique_lock<std::shared_mutex> queue_lock(file_queue_mutex_);
            pending_eviction_paths_.erase(record.path);
        }
        ReleaseSpace(record.size);
        return {};
    }

    LOG(ERROR) << "Failed to evict file: " << record.path
               << ", error: " << ec.message();
    return tl::make_unexpected(ErrorCode::FILE_WRITE_FAIL);
}

tl::expected<std::vector<std::string>, ErrorCode>
StorageBackend::EnsureDiskSpace(
    size_t required_size,
    StorageBackendInterface::EvictionHandler eviction_handler) {
    std::vector<std::string> evicted_keys;
    // If eviction is disabled, skip space checking and eviction
    if (!IsEvictionEnabled()) {
        return evicted_keys;
    }

    const size_t kMaxEvictionAttempts = 1000;
    size_t attempts = 0;

    bool space_reserved = CheckDiskSpace(required_size);
    if (space_reserved) {
        return evicted_keys;
    }

    uint64_t projected_available_space = 0;
    {
        std::shared_lock<std::shared_mutex> lock(space_mutex_);
        projected_available_space = available_space_;
    }

    std::vector<FileRecord> pending_evictions;
    while (projected_available_space < required_size &&
           attempts < kMaxEvictionAttempts) {
        FileRecord pending = PopFileToEvictByFIFO();
        if (pending.path.empty()) {
            LOG(ERROR) << "Failed to evict file to make space.";
            for (auto it = pending_evictions.rbegin();
                 it != pending_evictions.rend(); ++it) {
                RestoreFileToWriteQueueFront(*it);
            }
            return tl::make_unexpected(ErrorCode::FILE_WRITE_FAIL);
        }
        if (!pending.key.empty()) {
            evicted_keys.push_back(pending.key);
        }
        projected_available_space += pending.size;
        pending_evictions.push_back(std::move(pending));
        attempts++;
    }

    if (projected_available_space < required_size) {
        for (auto it = pending_evictions.rbegin();
             it != pending_evictions.rend(); ++it) {
            RestoreFileToWriteQueueFront(*it);
        }
        LOG(ERROR) << "Still insufficient disk space after selecting files for "
                      "eviction.";
        return tl::make_unexpected(ErrorCode::FILE_WRITE_FAIL);
    }

    if (eviction_handler && !evicted_keys.empty()) {
        auto notify_result = eviction_handler(evicted_keys);
        if (!notify_result) {
            for (auto it = pending_evictions.rbegin();
                 it != pending_evictions.rend(); ++it) {
                RestoreFileToWriteQueueFront(*it);
            }
            return tl::make_unexpected(notify_result.error());
        }
    }

    bool deletion_failed = false;
    for (auto& pending : pending_evictions) {
        auto delete_result = DeleteEvictedFile(pending);
        if (!delete_result) {
            deletion_failed = true;
            pending.key.clear();
            RestoreFileToWriteQueueFront(pending);
        }
    }
    if (deletion_failed) {
        return tl::make_unexpected(ErrorCode::FILE_WRITE_FAIL);
    }

    space_reserved = CheckDiskSpace(required_size);
    if (!space_reserved) {
        LOG(ERROR) << "Still insufficient disk space after evicting files.";
        return tl::make_unexpected(ErrorCode::FILE_WRITE_FAIL);
    }

    return evicted_keys;
}

tl::expected<std::vector<std::string>, ErrorCode>
StorageBackend::EvictAboveDiskWatermark(
    double high_watermark_ratio, double low_watermark_ratio,
    StorageBackendInterface::EvictionHandler eviction_handler) {
    std::vector<std::string> evicted_keys;
    if (!IsEvictionEnabled()) {
        return evicted_keys;
    }
    if (!initialized_.load(std::memory_order_acquire)) {
        LOG(ERROR)
            << "EvictAboveDiskWatermark called before StorageBackend::Init "
               "was completed.";
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }

    uint64_t used_space = 0;
    uint64_t total_space = 0;
    {
        std::shared_lock<std::shared_mutex> lock(space_mutex_);
        used_space = used_space_;
        total_space = total_space_;
    }
    if (total_space == 0) {
        return evicted_keys;
    }

    const uint64_t high_watermark_bytes =
        static_cast<uint64_t>(total_space * high_watermark_ratio);
    if (used_space <= high_watermark_bytes) {
        return evicted_keys;
    }

    const uint64_t target_used_bytes =
        static_cast<uint64_t>(total_space * low_watermark_ratio);
    size_t queue_size = 0;
    {
        std::shared_lock<std::shared_mutex> lock(file_queue_mutex_);
        queue_size = file_write_queue_.size();
    }

    const size_t max_eviction_attempts = queue_size + 100;
    size_t attempts = 0;
    uint64_t projected_used_space = used_space;
    std::vector<FileRecord> pending_evictions;
    while (projected_used_space > target_used_bytes &&
           attempts < max_eviction_attempts) {
        FileRecord pending = PopFileToEvictByFIFO();
        if (pending.path.empty()) {
            LOG(WARNING) << "Disk watermark eviction could not select a file "
                         << "for eviction; used=" << projected_used_space
                         << ", target=" << target_used_bytes;
            break;
        }
        if (!pending.key.empty()) {
            evicted_keys.push_back(pending.key);
        }
        projected_used_space = pending.size >= projected_used_space
                                   ? 0
                                   : projected_used_space - pending.size;
        pending_evictions.push_back(std::move(pending));
        attempts++;
    }

    if (eviction_handler && !evicted_keys.empty()) {
        auto notify_result = eviction_handler(evicted_keys);
        if (!notify_result) {
            for (auto it = pending_evictions.rbegin();
                 it != pending_evictions.rend(); ++it) {
                RestoreFileToWriteQueueFront(*it);
            }
            return tl::make_unexpected(notify_result.error());
        }
    }

    bool deletion_failed = false;
    for (auto& pending : pending_evictions) {
        auto delete_result = DeleteEvictedFile(pending);
        if (!delete_result) {
            deletion_failed = true;
            pending.key.clear();
            RestoreFileToWriteQueueFront(pending);
        }
    }

    {
        std::shared_lock<std::shared_mutex> lock(space_mutex_);
        used_space = used_space_;
    }

    if (used_space > target_used_bytes) {
        LOG(WARNING) << "Disk watermark eviction stopped above target: used="
                     << used_space << ", target=" << target_used_bytes
                     << ", attempts=" << attempts;
    }
    if (deletion_failed) {
        return tl::make_unexpected(ErrorCode::FILE_WRITE_FAIL);
    }
    return evicted_keys;
}

void StorageBackend::UpdateFileRecordKey(const std::string& path,
                                         const std::string& key) {
    std::unique_lock<std::shared_mutex> lock(file_queue_mutex_);
    auto it = file_queue_map_.find(path);
    if (it == file_queue_map_.end()) {
        return;
    }
    it->second->key = key;
}

void StorageBackend::ReleaseSpace(uint64_t size_to_release) {
    if (size_to_release == 0) {
        return;
    }

    try {
        std::unique_lock<std::shared_mutex> lock(space_mutex_);
        if (size_to_release <= used_space_) {
            used_space_ -= size_to_release;
            available_space_ += size_to_release;
        } else {
            available_space_ += used_space_;
            used_space_ = 0;
        }

    } catch (const std::exception& e) {
        LOG(ERROR) << "Failed to acquire lock while updating space tracking: "
                   << e.what();
    }
}

StorageBackendAdaptor::StorageBackendAdaptor(
    const FileStorageConfig& file_storage_config,
    const FilePerKeyConfig& file_per_key_config)
    : StorageBackendInterface(file_storage_config),
      file_per_key_config_(file_per_key_config),
      total_keys(0),
      total_size(0) {}

tl::expected<void, ErrorCode> StorageBackendAdaptor::Init() {
    std::string storage_root =
        file_storage_config_.storage_filepath + file_per_key_config_.fsdir;

    storage_backend_ = std::make_unique<StorageBackend>(
        file_storage_config_.storage_filepath, file_per_key_config_.fsdir,
        file_per_key_config_.enable_eviction);
    storage_backend_->use_uring_ = file_storage_config_.use_uring;
    auto init_result = storage_backend_->Init();
    if (!init_result) {
        LOG(ERROR) << "Failed to init storage backend";
        return init_result;
    }
    return {};
}

std::string StorageBackendAdaptor::ConcatSlicesToString(
    const std::vector<Slice>& slices) {
    size_t total = 0;
    for (const auto& s : slices) {
        if (s.size == 0) continue;
        total += s.size;
    }

    std::string out;
    out.reserve(total);

    for (const auto& s : slices) {
        if (s.size == 0) continue;
        out.append(reinterpret_cast<const char*>(s.ptr), s.size);
    }
    return out;
}

tl::expected<int64_t, ErrorCode> StorageBackendAdaptor::BatchOffload(
    const std::unordered_map<std::string, std::vector<Slice>>& batch_object,
    std::function<ErrorCode(const std::vector<std::string>& keys,
                            std::vector<StorageObjectMetadata>& metadatas)>
        complete_handler,
    EvictionHandler eviction_handler) {
    if (batch_object.empty()) {
        LOG(ERROR) << "batch object is empty";
        return tl::make_unexpected(ErrorCode::INVALID_KEY);
    }

    auto enable_offloading_res = IsEnableOffloading();
    if (!enable_offloading_res) {
        return tl::make_unexpected(enable_offloading_res.error());
    }
    std::vector<StorageObjectMetadata> metadatas;
    std::vector<std::string> keys;
    metadatas.reserve(batch_object.size());
    keys.reserve(batch_object.size());

    // Process each key; continue on individual failures to support partial
    // success
    for (auto& object : batch_object) {
        KVEntry kv;
        kv.key = object.first;
        auto value = object.second;

        // Test-only: Check if this key should fail (deterministic failure
        // injection)
        if (test_failure_predicate_ && test_failure_predicate_(kv.key)) {
            LOG(INFO) << "[TEST] Injecting failure for key: " << kv.key
                      << " (test failure predicate)";
            continue;  // Simulate StoreObject failure
        }

        auto path =
            ResolvePathFromKey(kv.key, file_storage_config_.storage_filepath,
                               file_per_key_config_.fsdir);
        kv.value = ConcatSlicesToString(value);

        std::string kv_buf;
        struct_pb::to_pb(kv, kv_buf);
        auto store_result = storage_backend_->StoreObject(path, kv_buf, kv.key,
                                                          eviction_handler);
        if (!store_result) {
            LOG(ERROR) << "Failed to store object for key: " << kv.key
                       << ", error: " << store_result.error()
                       << " - continuing with remaining keys";
            continue;  // Continue processing other keys
        }

        {
            MutexLocker lock(&mutex_);
            total_keys++;
            total_size += kv_buf.size();
        }

        metadatas.emplace_back(
            StorageObjectMetadata{-1, 0, static_cast<int64_t>(kv.key.size()),
                                  static_cast<int64_t>(kv.value.size()), ""});
        keys.emplace_back(kv.key);
    }

    // Only report successful keys to master
    if (complete_handler != nullptr && !keys.empty()) {
        auto error_code = complete_handler(keys, metadatas);
        if (error_code != ErrorCode::OK) {
            LOG(ERROR)
                << "Complete handler failed: " << error_code << " - "
                << keys.size()
                << " keys were successfully written to disk but master was not "
                   "notified. "
                << "Master will learn about them via ScanMeta on next restart.";
            return tl::make_unexpected(error_code);
        }
    }

    return static_cast<int64_t>(keys.size());
}

tl::expected<std::vector<std::string>, ErrorCode>
StorageBackendAdaptor::EvictAboveDiskWatermark(
    double high_watermark_ratio, double low_watermark_ratio,
    EvictionHandler eviction_handler) {
    auto eviction_result = storage_backend_->EvictAboveDiskWatermark(
        high_watermark_ratio, low_watermark_ratio, eviction_handler);
    if (!eviction_result) {
        return tl::make_unexpected(eviction_result.error());
    }
    return eviction_result;
}

tl::expected<bool, ErrorCode> StorageBackendAdaptor::IsExist(
    const std::string& key) {
    auto path = ResolvePathFromKey(key, file_storage_config_.storage_filepath,
                                   file_per_key_config_.fsdir);
    namespace fs = std::filesystem;
    return fs::exists(path);
}

tl::expected<void, ErrorCode> StorageBackendAdaptor::BatchLoad(
    std::unordered_map<std::string, Slice>& batched_slices) {
    for (const auto& [key, slice] : batched_slices) {
        KVEntry kv;
        kv.key = key;
        auto path =
            ResolvePathFromKey(kv.key, file_storage_config_.storage_filepath,
                               file_per_key_config_.fsdir);

        kv.value.resize(slice.size);

        std::string kv_buf;
        struct_pb::to_pb(kv, kv_buf);

        auto r = storage_backend_->LoadObject(path, kv_buf, kv_buf.size());
        if (!r) {
            LOG(ERROR) << "Failed to load from file";
            return tl::make_unexpected(r.error());
        }

        struct_pb::from_pb(kv, kv_buf);

        if (!kv.value.empty()) {
            std::memcpy(slice.ptr, kv.value.data(), kv.value.size());
        }
    }
    return {};
}

tl::expected<bool, ErrorCode> StorageBackendAdaptor::IsEnableOffloading() {
    if (storage_backend_->enable_eviction_) {
        return true;
    }

    if (!meta_scanned_.load(std::memory_order_acquire)) {
        LOG(ERROR) << "Metadata has not been loaded yet";
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }

    MutexLocker lock(&mutex_);

    auto is_enable_offloading =
        total_keys <= file_storage_config_.total_keys_limit &&
        total_size <= file_storage_config_.total_size_limit;

    return is_enable_offloading;
}

tl::expected<void, ErrorCode> StorageBackendAdaptor::ScanMeta(
    const std::function<
        ErrorCode(const std::vector<std::string>& keys,
                  std::vector<StorageObjectMetadata>& metadatas)>& handler) {
    namespace fs = std::filesystem;

    fs::path root = fs::path(file_storage_config_.storage_filepath) /
                    file_per_key_config_.fsdir;
    if (!fs::exists(root)) {
        meta_scanned_.store(true, std::memory_order_release);
        return {};
    }

    std::vector<std::string> keys;
    std::vector<StorageObjectMetadata> metas;

    auto flush = [&]() -> tl::expected<void, ErrorCode> {
        if (keys.empty()) return {};
        auto ec = handler(keys, metas);
        if (ec != ErrorCode::OK) return tl::make_unexpected(ec);
        keys.clear();
        metas.clear();
        return {};
    };

    MutexLocker lock(&mutex_);

    std::error_code ec_root;
    for (auto it1 = fs::directory_iterator(root, ec_root);
         !ec_root && it1 != fs::directory_iterator(); it1.increment(ec_root)) {
        if (ec_root) break;
        if (!it1->is_directory(ec_root) || ec_root) continue;

        const auto& d1 = it1->path();

        std::error_code ec_d1;
        for (auto it2 = fs::directory_iterator(d1, ec_d1);
             !ec_d1 && it2 != fs::directory_iterator(); it2.increment(ec_d1)) {
            if (ec_d1) break;
            if (!it2->is_directory(ec_d1) || ec_d1) continue;

            const auto& leaf = it2->path();

            std::error_code ec_leaf;
            for (auto it = fs::directory_iterator(leaf, ec_leaf);
                 !ec_leaf && it != fs::directory_iterator();
                 it.increment(ec_leaf)) {
                if (ec_leaf) break;
                const auto& p = it->path();
                if (!it->is_regular_file(ec_leaf) || ec_leaf) continue;

                uintmax_t sz = fs::file_size(p, ec_leaf);
                if (ec_leaf) continue;

                std::string buf;
                auto r =
                    storage_backend_->LoadObject(p.string(), buf, (int64_t)sz);
                if (!r) continue;

                KVEntry kv;
                struct_pb::from_pb(kv, buf);
                storage_backend_->UpdateFileRecordKey(p.string(), kv.key);

                total_keys++;
                total_size += buf.size();

                keys.emplace_back(std::move(kv.key));
                metas.emplace_back(StorageObjectMetadata{
                    -1, 0, (int64_t)keys.back().size(),
                    static_cast<int64_t>(kv.value.size()), ""});

                if ((int64_t)keys.size() >=
                    file_storage_config_.scanmeta_iterator_keys_limit) {
                    auto fr = flush();
                    if (!fr) return fr;
                }
            }

            auto fr = flush();
            if (!fr) return fr;
        }
    }

    meta_scanned_.store(true, std::memory_order_release);
    return {};
}

BucketIdGenerator::BucketIdGenerator(int64_t start) {
    if (start <= 0) {
        auto cur_time_stamp = time_gen();
        current_id_ = (cur_time_stamp << TIMESTAMP_SHIFT) | SEQUENCE_ID_SHIFT;
    } else {
        current_id_ = start;
    }
}

int64_t BucketIdGenerator::NextId() {
    return current_id_.fetch_add(1, std::memory_order_relaxed) + 1;
}

int64_t BucketIdGenerator::CurrentId() {
    return current_id_.load(std::memory_order_relaxed);
}

BucketStorageBackend::BucketStorageBackend(
    const FileStorageConfig& file_storage_config_,
    const BucketBackendConfig& bucket_backend_config_)
    : StorageBackendInterface(file_storage_config_),
      storage_path_(file_storage_config_.storage_filepath),
      bucket_backend_config_(bucket_backend_config_) {
    // Allocate aligned buffer for O_DIRECT I/O operations
    void* buf = nullptr;
    int ret = posix_memalign(&buf, kDirectIOAlignment, kAlignedBufferSize);
    if (ret != 0) {
        LOG(ERROR)
            << "BucketStorageBackend: Failed to allocate aligned buffer: "
            << strerror(ret);
    } else {
        aligned_io_buffer_.reset(buf);
        // Update the deleter to use free
        aligned_io_buffer_ = std::unique_ptr<void, void (*)(void*)>(
            buf, [](void* p) { free(p); });
        LOG(INFO) << "BucketStorageBackend: Allocated " << kAlignedBufferSize
                  << " bytes aligned buffer at " << buf;
    }
}

BucketStorageBackend::~BucketStorageBackend() {
    // Clear file cache to release UringFile instances before destruction
    // This ensures orderly cleanup of io_uring resources
    ClearFileCache();
}

tl::expected<int64_t, ErrorCode> BucketStorageBackend::BatchOffload(
    const std::unordered_map<std::string, std::vector<Slice>>& batch_object,
    std::function<ErrorCode(const std::vector<std::string>& keys,
                            std::vector<StorageObjectMetadata>& metadatas)>
        complete_handler,
    EvictionHandler eviction_handler) {
    if (!initialized_.load(std::memory_order_acquire)) {
        LOG(ERROR)
            << "Storage backend is not initialized. Call Init() before use.";
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }
    if (batch_object.empty()) {
        LOG(ERROR) << "batch object is empty";
        return tl::make_unexpected(ErrorCode::INVALID_KEY);
    }

    auto enable_offloading_res = IsEnableOffloading();
    if (!enable_offloading_res) {
        return tl::make_unexpected(enable_offloading_res.error());
    }
    if (!enable_offloading_res.value()) {
        return tl::make_unexpected(ErrorCode::KEYS_ULTRA_LIMIT);
    }
    auto bucket_id = bucket_id_generator_->NextId();
    std::vector<iovec> iovs;
    std::vector<StorageObjectMetadata> metadatas;
    auto build_bucket_result =
        BuildBucket(bucket_id, batch_object, iovs, metadatas);
    if (!build_bucket_result) {
        LOG(ERROR) << "Failed to build bucket with id: " << bucket_id;
        return tl::make_unexpected(build_bucket_result.error());
    }
    auto bucket = build_bucket_result.value();

    // Phase 1: eviction — remove oldest buckets from metadata maps to make
    // room. Must notify master BEFORE deleting files (Phase 2).
    const int64_t required_size = bucket->data_size + bucket->meta_size;
    auto prepare_result = PrepareEviction(required_size, bucket->keys);
    if (!prepare_result) {
        return tl::make_unexpected(prepare_result.error());
    }
    PendingEviction pending = std::move(prepare_result.value());

    // Notify master about evicted keys BEFORE touching the files.
    if (eviction_handler && !pending.keys.empty()) {
        auto notify_result = eviction_handler(pending.keys);
        if (!notify_result) {
            RestorePreparedEviction(std::move(pending));
            return tl::make_unexpected(notify_result.error());
        }
    }
    CommitPreparedEviction(pending);

    // Phase 2: delete evicted files NOW, before writing the new bucket, so
    // that the freed disk space is available for the incoming write.
    auto finalize_result = FinalizeEviction(pending);
    if (!finalize_result) {
        LOG(ERROR)
            << "FinalizeEviction failed after master committed eviction; "
               "continuing BatchOffload: "
            << finalize_result.error();
    }

    auto write_bucket_result = WriteBucket(bucket_id, bucket, iovs);
    if (!write_bucket_result) {
        LOG(ERROR) << "Failed to write bucket with id: " << bucket_id;
        ReleasePreparedWrite(pending);
        return tl::make_unexpected(write_bucket_result.error());
    }
    // Save a copy of bucket->keys before std::move(bucket) into buckets_
    // consumes the shared_ptr. Needed for complete_handler and rollback.
    const auto bucket_keys = bucket->keys;

    // Commit to metadata maps under exclusive lock FIRST.
    // This ensures any concurrent BatchLoad arriving after Master redirects
    // reads to this node can find the key in object_bucket_map_.
    // Even if complete_handler fails later, the read path is correct —
    // RollbackCommittedBucket will undo the commit safely.
    {
        SharedMutexLocker lock(&mutex_);

        ReleasePreparedWriteLocked(pending);

        // Pre-check for duplicates before modifying any state
        bool duplicate_found = false;
        for (const auto& key : bucket_keys) {
            if (object_bucket_map_.find(key) != object_bucket_map_.end()) {
                duplicate_found = true;
                break;
            }
        }

        if (!duplicate_found) {
            total_size_ += bucket->data_size + bucket->meta_size;
            object_bucket_map_.reserve(object_bucket_map_.size() +
                                       bucket_keys.size());
            for (size_t i = 0; i < bucket_keys.size(); ++i) {
                auto [it, inserted] =
                    object_bucket_map_.insert({bucket_keys[i], metadatas[i]});
                CHECK(inserted)
                    << "Reserved key became duplicated: " << bucket_keys[i];
            }
            buckets_.emplace(bucket_id, std::move(bucket));
            lru_index_.emplace(0LL, bucket_id);
        }
        if (duplicate_found) {
            LOG(ERROR) << "Reserved key became duplicated before commit, "
                          "bucket_id="
                       << bucket_id;
            lock.unlock();
            CleanupOrphanedBucket(bucket_id);
            return tl::make_unexpected(ErrorCode::OBJECT_ALREADY_EXISTS);
        }
    }
    // Lock released. From this point forward, concurrent BatchLoad
    // can find the keys and read from the committed bucket files.

    // Notify Master AFTER local index is committed.
    // metadatas[i].transport_endpoint is empty here (it gets populated by
    // complete_handler before the RPC); int64_t fields carry the metadata
    // from BuildBucket unchanged.
    if (complete_handler != nullptr) {
        auto error_code = complete_handler(bucket_keys, metadatas);
        if (error_code != ErrorCode::OK) {
            LOG(ERROR) << "Complete handler failed: " << error_code
                       << ", Key count: " << bucket_keys.size()
                       << ", Bucket id: " << bucket_id;
            // Master was NOT notified. The local index has entries that
            // Master doesn't know about — a "client can read but Master
            // doesn't know" ghost replica. Rollback the local commit
            // (removes index entries + waits for inflight reads + deletes
            // on-disk files).
            RollbackCommittedBucket(bucket_id, bucket_keys);
            return tl::make_unexpected(error_code);
        }
    }

    return bucket_id;
}

tl::expected<void, ErrorCode> BucketStorageBackend::BatchQuery(
    const std::vector<std::string>& keys,
    std::unordered_map<std::string, StorageObjectMetadata>&
        batch_object_metadata) {
    SharedMutexLocker lock(&mutex_, shared_lock);
    for (const auto& key : keys) {
        auto object_metadata_it = object_bucket_map_.find(key);
        if (object_metadata_it != object_bucket_map_.end()) {
            batch_object_metadata.emplace(key, object_metadata_it->second);
        } else {
            LOG(ERROR) << "Key " << key << " does not exist";
            return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
        }
    }
    return {};
}

tl::expected<void, ErrorCode> BucketStorageBackend::BatchLoad(
    std::unordered_map<std::string, Slice>& batch_object) {
    // Step 1: Build read plan by copying metadata under lock
    // BucketReadGuard increments inflight_reads_ to prevent deletion during IO.
    // When the guard goes out of scope, it decrements the counter.
    struct ReadPlan {
        std::string key;
        int64_t bucket_id;
        int64_t offset;
        int64_t key_size;
        int64_t data_size;
        Slice dest_slice;
    };

    // Group by bucket for efficient file access
    // BucketReadGuard tracks inflight reads - one guard per bucket being read
    std::unordered_map<int64_t, std::vector<ReadPlan>> bucket_read_plans;
    std::vector<BucketReadGuard> bucket_guards;  // RAII guards for all buckets

    {
        SharedMutexLocker lock(&mutex_, shared_lock);
        for (const auto& [key, dest_slice] : batch_object) {
            // Lookup key -> metadata
            auto object_it = object_bucket_map_.find(key);
            if (object_it == object_bucket_map_.end()) {
                LOG(ERROR) << "Key not found: " << key;
                return tl::make_unexpected(ErrorCode::INVALID_KEY);
            }
            const auto& metadata = object_it->second;

            // Lookup bucket -> BucketMetadata
            auto bucket_it = buckets_.find(metadata.bucket_id);
            if (bucket_it == buckets_.end()) {
                LOG(ERROR) << "Bucket not found for key: " << key
                           << ", bucket_id=" << metadata.bucket_id;
                return tl::make_unexpected(ErrorCode::BUCKET_NOT_FOUND);
            }

            // Validate size
            if (metadata.data_size != static_cast<int64_t>(dest_slice.size)) {
                LOG(ERROR) << "Size mismatch for key: " << key
                           << ", expected: " << metadata.data_size
                           << ", got: " << dest_slice.size;
                return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
            }

            // Create guard for this bucket if not already guarded
            // (one guard per bucket is sufficient - increments refcount once)
            if (bucket_read_plans.find(metadata.bucket_id) ==
                bucket_read_plans.end()) {
                bucket_guards.emplace_back(bucket_it->second);

                // Update LRU timestamp for this bucket.
                if (bucket_backend_config_.eviction_policy ==
                    BucketEvictionPolicy::LRU) {
                    auto now = std::chrono::steady_clock::now()
                                   .time_since_epoch()
                                   .count();
                    bucket_it->second->last_access_ns_.store(
                        now, std::memory_order_relaxed);
                }
            }

            // Copy metadata into read plan
            bucket_read_plans[metadata.bucket_id].push_back(
                ReadPlan{key, metadata.bucket_id, metadata.offset,
                         metadata.key_size, metadata.data_size, dest_slice});
        }
    }
    // Lock released here - bucket files protected by BucketReadGuards
    // which remain alive until this function returns (~line bucket_guards
    // destructor), keeping inflight_reads_ > 0 throughout the I/O phase.

    // Step 2: Perform IO without holding any locks
    for (auto& [bucket_id, read_plans] : bucket_read_plans) {
        // Open file for this bucket (cheap syscall, no lock needed)
        auto filepath_res = GetBucketDataPath(bucket_id);
        if (!filepath_res) {
            LOG(ERROR) << "Failed to get bucket data path, bucket_id="
                       << bucket_id;
            return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
        }

        auto file_res = OpenFile(filepath_res.value(), FileMode::Read);
        if (!file_res) {
            LOG(ERROR) << "Failed to open bucket file: "
                       << filepath_res.value();
            return tl::make_unexpected(file_res.error());
        }
        auto& file = file_res.value();

        // Read each key's data
        for (const auto& plan : read_plans) {
            int64_t actual_offset = plan.offset + plan.key_size;
            tl::expected<size_t, ErrorCode> read_res;

#ifdef USE_URING
            // Try to use read_aligned for O_DIRECT I/O if file is UringFile
            UringFile* uring_file = dynamic_cast<UringFile*>(file.get());
            if (uring_file != nullptr) {
                // Calculate aligned read range
                int64_t aligned_offset =
                    align_down(actual_offset, kDirectIOAlignment);
                int64_t data_end =
                    actual_offset + static_cast<int64_t>(plan.dest_slice.size);
                int64_t aligned_end = static_cast<int64_t>(align_up(
                    static_cast<size_t>(data_end), kDirectIOAlignment));
                size_t aligned_size =
                    static_cast<size_t>(aligned_end - aligned_offset);
                int64_t offset_in_buffer = actual_offset - aligned_offset;

                // Zero-copy path: read directly into the slice buffer.
                // dest_slice.ptr is 4096-aligned and oversized (from
                // AllocateBatch) to accommodate the full aligned read range.
                read_res = uring_file->read_aligned(
                    plan.dest_slice.ptr, aligned_size, aligned_offset);

                if (read_res) {
                    // Adjust ptr to point to actual data start (no memcpy)
                    batch_object.at(plan.key).ptr =
                        static_cast<char*>(plan.dest_slice.ptr) +
                        offset_in_buffer;
                    read_res = plan.dest_slice.size;
                }
            } else
#endif
            {
                // Fallback to vector_read for non-UringFile
                iovec iov{plan.dest_slice.ptr, plan.dest_slice.size};
                read_res = file->vector_read(&iov, 1, actual_offset);
            }

            if (!read_res) {
                LOG(ERROR) << "vector_read failed for key: " << plan.key
                           << ", bucket_id=" << plan.bucket_id
                           << ", error: " << read_res.error();
                return tl::make_unexpected(read_res.error());
            }

            if (read_res.value() != plan.dest_slice.size) {
                LOG(ERROR) << "Read size mismatch for key: " << plan.key
                           << ", expected: " << plan.dest_slice.size
                           << ", got: " << read_res.value();
                return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
            }
        }
    }

    // bucket_guards go out of scope here, decrementing inflight_reads_
    return {};
}

tl::expected<void, ErrorCode> BucketStorageBackend::GetBucketKeys(
    int64_t bucket_id, std::vector<std::string>& bucket_keys) {
    SharedMutexLocker locker(&mutex_, shared_lock);
    auto bucket_it = buckets_.find(bucket_id);
    if (bucket_it == buckets_.end()) {
        return tl::make_unexpected(ErrorCode::BUCKET_NOT_FOUND);
    }
    for (const auto& key : bucket_it->second->keys) {
        bucket_keys.emplace_back(key);
    }
    return {};
}

tl::expected<void, ErrorCode> BucketStorageBackend::Init() {
    namespace fs = std::filesystem;
    try {
        if (initialized_.load(std::memory_order_acquire)) {
            LOG(ERROR) << "Storage backend already initialized";
            return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
        }
        SharedMutexLocker lock(&mutex_);
        object_bucket_map_.clear();
        buckets_.clear();
        lru_index_.clear();
        total_size_ = 0;
        int64_t max_bucket_id = BucketIdGenerator::INIT_NEW_START_ID;

        for (const auto& entry :
             fs::recursive_directory_iterator(storage_path_)) {
            if (entry.is_regular_file() &&
                entry.path().extension() == BUCKET_METADATA_FILE_SUFFIX) {
                const auto& bucket_id_str = entry.path().stem();
                int64_t bucket_id = 0;
                try {
                    bucket_id = std::stoll(bucket_id_str);
                } catch (const std::exception& e) {
                    LOG(WARNING)
                        << "Skipping metadata file with a non-numeric "
                           "bucket id: "
                        << entry.path().string() << " (" << e.what() << ")";
                    continue;
                }
                auto [metadata_it, success] = buckets_.try_emplace(
                    bucket_id, std::make_shared<BucketMetadata>());
                if (success) lru_index_.emplace(0LL, bucket_id);
                if (!success) {
                    LOG(ERROR) << "Failed to load bucket " << bucket_id_str;
                    return tl::make_unexpected(
                        ErrorCode::BUCKET_ALREADY_EXISTS);
                }
                auto load_bucket_metadata_result =
                    LoadBucketMetadata(bucket_id, metadata_it->second);
                if (!load_bucket_metadata_result) {
                    LOG(ERROR)
                        << "Failed to load metadata for bucket: "
                        << bucket_id_str
                        << ", will delete the bucket's data and metadata";

                    auto bucket_data_path_res = GetBucketDataPath(bucket_id);
                    if (bucket_data_path_res) {
                        fs::remove(bucket_data_path_res.value());
                    }

                    auto bucket_meta_path_res =
                        GetBucketMetadataPath(bucket_id);
                    if (bucket_meta_path_res) {
                        fs::remove(bucket_meta_path_res.value());
                    }

                    lru_index_.erase({0LL, bucket_id});
                    buckets_.erase(bucket_id);
                    continue;
                }
                auto bucket_data_path_res = GetBucketDataPath(bucket_id);
                if (!bucket_data_path_res) {
                    LOG(ERROR) << "Failed to get data path for bucket: "
                               << bucket_id_str;
                    return tl::make_unexpected(bucket_data_path_res.error());
                }

                std::error_code bucket_data_ec;
                const auto bucket_data_status =
                    fs::status(bucket_data_path_res.value(), bucket_data_ec);
                if (bucket_data_ec &&
                    bucket_data_ec != std::errc::no_such_file_or_directory) {
                    LOG(ERROR) << "Failed to inspect bucket data file: "
                               << bucket_data_path_res.value()
                               << ", error: " << bucket_data_ec.message();
                    return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
                }
                if (bucket_data_ec == std::errc::no_such_file_or_directory ||
                    !fs::is_regular_file(bucket_data_status)) {
                    LOG(ERROR) << "Bucket metadata has no valid data file: "
                               << entry.path().string()
                               << ", will delete the bucket's remaining files";
                    CleanupOrphanedBucket(bucket_id);
                    lru_index_.erase({0LL, bucket_id});
                    buckets_.erase(bucket_id);
                    continue;
                }
                auto& meta = *(metadata_it->second);
                if (meta.data_size == 0 || meta.meta_size == 0 ||
                    meta.metadatas.empty() || meta.keys.empty()) {
                    LOG(ERROR) << "Metadata validation failed for bucket: "
                               << bucket_id_str
                               << ", will delete the bucket's data and "
                                  "metadata. Detailed values:";
                    LOG(ERROR) << "  data_size: " << meta.data_size
                               << " (should not be 0)";
                    LOG(ERROR) << "  meta_size: " << meta.meta_size
                               << " (should not be 0)";
                    LOG(ERROR)
                        << "  object_metadata.size(): " << meta.metadatas.size()
                        << " (empty: "
                        << (meta.metadatas.empty() ? "true" : "false") << ")";

                    LOG(ERROR)
                        << "  keys.size(): " << meta.keys.size()
                        << " (empty: " << (meta.keys.empty() ? "true" : "false")
                        << ")";
                    auto bucket_data_path_res = GetBucketDataPath(bucket_id);
                    if (bucket_data_path_res) {
                        fs::remove(bucket_data_path_res.value());
                    }

                    auto bucket_meta_path_res =
                        GetBucketMetadataPath(bucket_id);
                    if (bucket_meta_path_res) {
                        fs::remove(bucket_meta_path_res.value());
                    }

                    lru_index_.erase({0LL, bucket_id});
                    buckets_.erase(bucket_id);
                    continue;
                }
                if (bucket_id > max_bucket_id) {
                    max_bucket_id = bucket_id;
                }
                total_size_ += metadata_it->second->data_size +
                               metadata_it->second->meta_size;
                for (size_t i = 0; i < metadata_it->second->keys.size(); i++) {
                    object_bucket_map_.emplace(
                        metadata_it->second->keys[i],
                        StorageObjectMetadata{
                            metadata_it->first,
                            metadata_it->second->metadatas[i].offset,
                            metadata_it->second->metadatas[i].key_size,
                            metadata_it->second->metadatas[i].data_size, ""});
                }
            }
        }

        // Clean up orphaned bucket files (.bucket files without corresponding
        // .meta files) This handles the crash consistency case where data write
        // succeeded but metadata write failed
        std::unordered_set<int64_t> valid_bucket_ids;
        for (const auto& [id, _] : buckets_) {
            valid_bucket_ids.insert(id);
        }

        uint64_t orphaned_files_count = 0;
        uint64_t orphaned_space_freed = 0;

        for (const auto& entry :
             fs::recursive_directory_iterator(storage_path_)) {
            if (!entry.is_regular_file()) {
                continue;
            }

            std::string extension = entry.path().extension().string();

            // Only process .bucket files
            if (extension != BUCKET_DATA_FILE_SUFFIX) {
                continue;
            }

            // Extract bucket ID from filename (e.g., "12345.bucket" ->
            // "12345")
            auto bucket_id_str = entry.path().stem();
            int64_t bucket_id = 0;
            try {
                bucket_id = std::stoll(bucket_id_str);
            } catch (const std::exception& e) {
                LOG(WARNING)
                    << "Skipping orphan-scan file with a non-numeric "
                       "bucket id: "
                    << entry.path().string() << " (" << e.what() << ")";
                continue;
            }

            // Check if this bucket has valid metadata
            if (valid_bucket_ids.find(bucket_id) != valid_bucket_ids.end()) {
                // Valid bucket, skip it
                continue;
            }

            // This is an orphaned .bucket file without metadata
            std::error_code cleanup_ec;
            uint64_t file_size = entry.file_size(cleanup_ec);
            if (!cleanup_ec && fs::remove(entry.path(), cleanup_ec)) {
                orphaned_files_count++;
                orphaned_space_freed += file_size;
                LOG(WARNING) << "Removed orphaned bucket file (no metadata): "
                             << entry.path().string() << " (size: " << file_size
                             << " bytes, "
                             << "bucket_id: " << bucket_id << ")";
            } else if (cleanup_ec) {
                LOG(ERROR) << "Failed to remove orphaned bucket file: "
                           << entry.path().string()
                           << ", error: " << cleanup_ec.message();
            }
        }

        if (orphaned_files_count > 0) {
            LOG(INFO) << "Orphan cleanup completed: removed "
                      << orphaned_files_count
                      << " orphaned bucket file(s), freed "
                      << orphaned_space_freed << " bytes";
        }

        // When max_total_size is not explicitly set (<= 0), default to 90% of
        // the physical disk capacity to match FilePerKey backend behavior.
        if (bucket_backend_config_.max_total_size <= 0) {
            constexpr double kDefaultQuotaPercentage = 0.9;
            const auto space_info = fs::space(storage_path_);
            bucket_backend_config_.max_total_size = static_cast<int64_t>(
                space_info.capacity * kDefaultQuotaPercentage);
            LOG(INFO) << "Bucket backend max_total_size not set; using "
                      << kDefaultQuotaPercentage * 100 << "% of disk capacity: "
                      << bucket_backend_config_.max_total_size << " bytes";
        }

        bucket_id_generator_.emplace(max_bucket_id);
        if (max_bucket_id == BucketIdGenerator::INIT_NEW_START_ID) {
            LOG(INFO) << "Initialized BucketIdGenerator with fresh start. "
                         "No existing buckets found; starting from ID: "
                      << bucket_id_generator_->CurrentId();
        } else {
            LOG(INFO) << "Initialized BucketIdGenerator from existing state. "
                      << "Last used bucket ID was " << max_bucket_id;
        }
        initialized_.store(true, std::memory_order_release);
    } catch (const std::exception& e) {
        LOG(ERROR) << "Bucket storage backend initialize error: " << e.what()
                   << std::endl;
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }

    return {};
}

tl::expected<bool, ErrorCode> BucketStorageBackend::IsExist(
    const std::string& key) {
    SharedMutexLocker lock(&mutex_, shared_lock);
    return object_bucket_map_.find(key) != object_bucket_map_.end();
}

tl::expected<bool, ErrorCode> BucketStorageBackend::IsEnableOffloading() {
    // When eviction is enabled, always allow offloading since PrepareEviction
    // will manage capacity by evicting old buckets as needed.
    if (bucket_backend_config_.eviction_policy != BucketEvictionPolicy::NONE &&
        bucket_backend_config_.max_total_size > 0) {
        return true;
    }

    auto store_metadata_result = GetStoreMetadata();
    if (!store_metadata_result) {
        LOG(ERROR) << "Failed to get store metadata: "
                   << store_metadata_result.error();
        return tl::make_unexpected(store_metadata_result.error());
    }
    const auto& store_metadata = store_metadata_result.value();
    auto enable_offloading =
        store_metadata.total_keys + bucket_backend_config_.bucket_keys_limit <=
            file_storage_config_.total_keys_limit &&
        store_metadata.total_size + bucket_backend_config_.bucket_size_limit <=
            file_storage_config_.total_size_limit;
    return enable_offloading;
}

tl::expected<void, ErrorCode> BucketStorageBackend::ScanMeta(
    const std::function<
        ErrorCode(const std::vector<std::string>& keys,
                  std::vector<StorageObjectMetadata>& metadatas)>& handler) {
    while (true) {
        auto has_next_res = HasNext();
        if (!has_next_res) {
            LOG(ERROR) << "Failed to check for next bucket: "
                       << has_next_res.error();
            return tl::make_unexpected(has_next_res.error());
        }
        if (!has_next_res.value()) {
            break;
        }
        auto add_all_object_res = HandleNext(handler);
        if (!add_all_object_res) {
            LOG(ERROR) << "Failed to add all object to master: "
                       << add_all_object_res.error();
            return add_all_object_res;
        }
    }
    return {};
}

tl::expected<int64_t, ErrorCode> BucketStorageBackend::BucketScan(
    int64_t bucket_id, std::vector<std::string>& keys,
    std::vector<StorageObjectMetadata>& metadatas,
    std::vector<int64_t>& buckets, int64_t limit) {
    SharedMutexLocker lock(&mutex_, shared_lock);
    auto bucket_it = buckets_.lower_bound(bucket_id);
    for (; bucket_it != buckets_.end(); ++bucket_it) {
        if (static_cast<int64_t>(bucket_it->second->keys.size()) > limit) {
            LOG(ERROR) << "Bucket key count exceeds limit: "
                       << "bucket_id=" << bucket_it->first
                       << ", current_size=" << bucket_it->second->keys.size()
                       << ", limit=" << limit;
            return tl::make_unexpected(ErrorCode::KEYS_EXCEED_BUCKET_LIMIT);
        }
        if (static_cast<int64_t>(bucket_it->second->keys.size() + keys.size()) >
            limit) {
            return bucket_it->first;
        }
        buckets.emplace_back(bucket_it->first);
        for (size_t i = 0; i < bucket_it->second->keys.size(); i++) {
            keys.emplace_back(bucket_it->second->keys[i]);
            metadatas.emplace_back(StorageObjectMetadata{
                bucket_it->first, bucket_it->second->metadatas[i].offset,
                bucket_it->second->metadatas[i].key_size,
                bucket_it->second->metadatas[i].data_size, ""});
        }
    }
    return 0;
}

tl::expected<OffloadMetadata, ErrorCode>
BucketStorageBackend::GetStoreMetadata() {
    SharedMutexLocker lock(&mutex_, shared_lock);
    OffloadMetadata metadata(object_bucket_map_.size(), total_size_);
    return metadata;
}

tl::expected<void, ErrorCode> BucketStorageBackend::AllocateOffloadingBuckets(
    const std::unordered_map<std::string, int64_t>& offloading_objects,
    std::vector<std::vector<std::string>>& buckets_keys) {
    return GroupOffloadingKeysByBucket(offloading_objects, buckets_keys);
}

void BucketStorageBackend::ClearUngroupedOffloadingObjects() {
    MutexLocker locker(&offloading_mutex_);
    ungrouped_offloading_objects_.clear();
}

size_t BucketStorageBackend::UngroupedOffloadingObjectsSize() const {
    MutexLocker locker(&offloading_mutex_);
    return ungrouped_offloading_objects_.size();
}

tl::expected<void, ErrorCode> BucketStorageBackend::GroupOffloadingKeysByBucket(
    const std::unordered_map<std::string, int64_t>& offloading_objects,
    std::vector<std::vector<std::string>>& buckets_keys) {
    MutexLocker offloading_locker(&offloading_mutex_);
    auto& ungrouped_offloading_objects = ungrouped_offloading_objects_;
    auto it = offloading_objects.cbegin();
    int64_t residue_count = static_cast<int64_t>(
        offloading_objects.size() + ungrouped_offloading_objects.size());
    int64_t total_count = residue_count;

    auto is_exist_func =
        [this](const std::string& key) -> tl::expected<bool, ErrorCode> {
        return IsExist(key);
    };

    while (it != offloading_objects.cend()) {
        std::vector<std::string> bucket_keys;
        std::unordered_map<std::string, int64_t> bucket_objects;
        int64_t bucket_data_size = 0;

        if (!ungrouped_offloading_objects.empty()) {
            for (const auto& ungrouped_it : ungrouped_offloading_objects) {
                bucket_data_size += ungrouped_it.second;
                bucket_keys.push_back(ungrouped_it.first);
                bucket_objects.emplace(ungrouped_it.first, ungrouped_it.second);
            }
            VLOG(1) << "Ungrouped offloading objects have been processed and "
                       "cleared; count="
                    << ungrouped_offloading_objects.size();
            ungrouped_offloading_objects.clear();
        }

        for (int64_t i = static_cast<int64_t>(bucket_keys.size());
             i < bucket_backend_config_.bucket_keys_limit; ++i) {
            if (it == offloading_objects.cend()) {
                for (const auto& bucket_object : bucket_objects) {
                    ungrouped_offloading_objects.emplace(bucket_object.first,
                                                         bucket_object.second);
                }
                VLOG(1) << "Add offloading objects to ungrouped pool. "
                        << "Total ungrouped count: "
                        << ungrouped_offloading_objects.size();
                return {};
            }

            if (it->second > bucket_backend_config_.bucket_size_limit) {
                VLOG(1) << "Object size exceeds bucket size limit: "
                        << "key=" << it->first << ", object_size=" << it->second
                        << ", limit="
                        << bucket_backend_config_.bucket_size_limit;
                ++it;
                continue;
            }

            auto is_exist_result = is_exist_func(it->first);
            if (!is_exist_result) {
                LOG(ERROR) << "Failed to check existence in storage backend: "
                           << "key=" << it->first
                           << ", error=" << is_exist_result.error();
            }
            if (is_exist_result && is_exist_result.value()) {
                VLOG(1) << "Key already exists in storage backend, skipping: "
                        << "key=" << it->first;
                ++it;
                continue;
            }

            if (bucket_data_size + it->second >
                bucket_backend_config_.bucket_size_limit) {
                break;
            }

            bucket_data_size += it->second;
            bucket_keys.push_back(it->first);
            bucket_objects.emplace(it->first, it->second);
            ++it;

            if (bucket_data_size == bucket_backend_config_.bucket_size_limit) {
                break;
            }
        }

        auto bucket_keys_count = static_cast<int64_t>(bucket_keys.size());
        residue_count -= bucket_keys_count;
        buckets_keys.push_back(std::move(bucket_keys));
        VLOG(1) << "Group objects with total object count: " << total_count
                << ", current bucket object count: " << bucket_keys_count
                << ", current bucket data size: " << bucket_data_size
                << ", grouped bucket count: " << buckets_keys.size()
                << ", residue object count: " << residue_count;
    }

    return {};
}

tl::expected<std::shared_ptr<BucketMetadata>, ErrorCode>
BucketStorageBackend::BuildBucket(
    int64_t bucket_id,
    const std::unordered_map<std::string, std::vector<Slice>>& batch_object,
    std::vector<iovec>& iovs, std::vector<StorageObjectMetadata>& metadatas) {
    auto bucket = std::make_shared<BucketMetadata>();
    int64_t storage_offset = 0;
    for (const auto& object : batch_object) {
        if (object.second.empty()) {
            LOG(ERROR) << "Failed to create bucket, object is empty";
            return tl::make_unexpected(ErrorCode::INVALID_KEY);
        }
        int64_t object_total_size = 0;
        iovs.emplace_back(
            iovec{const_cast<char*>(object.first.data()), object.first.size()});
        for (const auto& slice : object.second) {
            object_total_size += slice.size;
            iovs.emplace_back(iovec{slice.ptr, slice.size});
        }
        bucket->data_size += object_total_size + object.first.size();
        bucket->metadatas.emplace_back(BucketObjectMetadata{
            storage_offset, static_cast<int64_t>(object.first.size()),
            object_total_size});
        metadatas.emplace_back(StorageObjectMetadata{
            bucket_id, storage_offset,
            static_cast<int64_t>(object.first.size()), object_total_size, ""});
        bucket->keys.push_back(object.first);
        storage_offset += object_total_size + object.first.size();
    }
    return bucket;
}

tl::expected<void, ErrorCode> BucketStorageBackend::WriteBucket(
    int64_t bucket_id, std::shared_ptr<BucketMetadata> bucket_metadata,
    std::vector<iovec>& iovs) {
    namespace fs = std::filesystem;
    auto bucket_data_path_res = GetBucketDataPath(bucket_id);
    if (!bucket_data_path_res) {
        LOG(ERROR) << "Failed to get bucket data path, bucket_id=" << bucket_id;
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }
    auto bucket_data_path = bucket_data_path_res.value();
    auto open_file_result = OpenFile(bucket_data_path, FileMode::Write);
    if (!open_file_result) {
        LOG(ERROR) << "Failed to open file for bucket writing: "
                   << bucket_data_path;
        return tl::make_unexpected(ErrorCode::FILE_OPEN_FAIL);
    }
    auto file = std::move(open_file_result.value());

#ifdef USE_URING
    // Try to use write_aligned for O_DIRECT I/O if file is UringFile
    UringFile* uring_file = dynamic_cast<UringFile*>(file.get());
    if (uring_file != nullptr) {
        size_t total_size = static_cast<size_t>(bucket_metadata->data_size);
        size_t aligned_size = align_up(total_size, kDirectIOAlignment);

        // Allocate aligned buffer if needed
        void* write_buffer = nullptr;
        std::unique_ptr<void, void (*)(void*)> temp_buffer{nullptr,
                                                           [](void*) {}};

        if (aligned_size <= kAlignedBufferSize && aligned_io_buffer_) {
            // Use the pre-allocated buffer
            write_buffer = aligned_io_buffer_.get();
        } else {
            // Allocate a temporary larger buffer
            void* buf = nullptr;
            int ret = posix_memalign(&buf, kDirectIOAlignment, aligned_size);
            if (ret != 0) {
                LOG(ERROR)
                    << "Failed to allocate aligned buffer for WriteBucket: "
                    << strerror(ret);
                return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
            }
            temp_buffer.reset(buf);
            temp_buffer = std::unique_ptr<void, void (*)(void*)>(
                buf, [](void* p) { free(p); });
            write_buffer = buf;
            LOG(WARNING) << "WriteBucket: bucket_id=" << bucket_id
                         << " requires " << aligned_size
                         << " bytes, exceeds buffer size " << kAlignedBufferSize
                         << ", using temporary allocation";
        }

        // Aggregate all iovs data into the aligned buffer
        char* dst = static_cast<char*>(write_buffer);
        for (const auto& iov : iovs) {
            memcpy(dst, iov.iov_base, iov.iov_len);
            dst += iov.iov_len;
        }

        // Zero-pad the remaining bytes
        if (aligned_size > total_size) {
            memset(dst, 0, aligned_size - total_size);
        }

        // Write using write_aligned
        auto write_result =
            uring_file->write_aligned(write_buffer, aligned_size, 0);
        if (!write_result) {
            LOG(ERROR) << "write_aligned failed for: " << bucket_id
                       << ", error: " << write_result.error();
            return tl::make_unexpected(write_result.error());
        }
        if (write_result.value() != aligned_size) {
            LOG(ERROR) << "Write size mismatch for: " << bucket_data_path
                       << ", expected: " << aligned_size
                       << ", got: " << write_result.value();
            return tl::make_unexpected(ErrorCode::FILE_WRITE_FAIL);
        }

        // Flush bucket data to stable storage before writing metadata.
        // This prevents a crash from leaving valid metadata pointing at
        // incomplete data (write-ordering durability guarantee).
        auto sync_result = uring_file->datasync();
        if (!sync_result) {
            LOG(ERROR) << "datasync failed for bucket: " << bucket_id;
            return tl::make_unexpected(ErrorCode::FILE_WRITE_FAIL);
        }

        // Invalidate cache for this file since content changed
        {
            MutexLocker cache_locker(&file_cache_mutex_);
            file_cache_.erase(bucket_data_path);
        }
    } else
#endif
    {
        // Fallback to vector_write for non-UringFile
        auto write_result = file->vector_write(iovs.data(), iovs.size(), 0);
        if (!write_result) {
            LOG(ERROR) << "vector_write failed for: " << bucket_id
                       << ", error: " << write_result.error();
            return tl::make_unexpected(write_result.error());
        }
        if (static_cast<int64_t>(write_result.value()) !=
            bucket_metadata->data_size) {
            LOG(ERROR) << "Write size mismatch for: " << bucket_data_path
                       << ", expected: " << bucket_metadata->data_size
                       << ", got: " << write_result.value();
            return tl::make_unexpected(ErrorCode::FILE_WRITE_FAIL);
        }

        // Invalidate cache for this file since content changed
        {
            MutexLocker cache_locker(&file_cache_mutex_);
            file_cache_.erase(bucket_data_path);
        }
    }
    auto store_bucket_metadata_result =
        StoreBucketMetadata(bucket_id, bucket_metadata);
    if (!store_bucket_metadata_result) {
        LOG(ERROR) << "Failed to store bucket metadata, error: "
                   << store_bucket_metadata_result.error();

        // Clean up the bucket file to prevent orphans
        std::error_code ec;
        if (fs::remove(bucket_data_path, ec)) {
            LOG(WARNING) << "Cleaned up orphaned bucket file after metadata "
                            "write failure: "
                         << bucket_data_path;
        } else if (ec) {
            LOG(ERROR) << "Failed to clean up bucket file after metadata write "
                          "failure: "
                       << bucket_data_path << ", error: " << ec.message();
        }

        return tl::make_unexpected(ErrorCode::FILE_WRITE_FAIL);
    }
    return {};
}

void BucketStorageBackend::CleanupOrphanedBucket(int64_t bucket_id) {
    namespace fs = std::filesystem;
    std::error_code ec;

    auto data_path_res = GetBucketDataPath(bucket_id);
    if (data_path_res) {
        // Evict the cached file handle before deleting the file, matching
        // the pattern in FinalizeEviction. Without this, a subsequent open
        // on the same path (e.g. after bucket_id reuse) may return a stale
        // handle pointing to the now-deleted file.
        {
            MutexLocker cache_locker(&file_cache_mutex_);
            file_cache_.erase(data_path_res.value());
        }
        if (fs::remove(data_path_res.value(), ec)) {
            LOG(INFO) << "Cleaned up orphaned bucket data file: "
                      << data_path_res.value();
        } else if (ec && ec != std::errc::no_such_file_or_directory) {
            LOG(WARNING) << "Failed to cleanup bucket data file: "
                         << data_path_res.value()
                         << ", error: " << ec.message();
        }
    }

    auto meta_path_res = GetBucketMetadataPath(bucket_id);
    if (meta_path_res) {
        ec.clear();
        if (fs::remove(meta_path_res.value(), ec)) {
            LOG(INFO) << "Cleaned up orphaned bucket metadata file: "
                      << meta_path_res.value();
        } else if (ec && ec != std::errc::no_such_file_or_directory) {
            LOG(WARNING) << "Failed to cleanup bucket metadata file: "
                         << meta_path_res.value()
                         << ", error: " << ec.message();
        }
    }
}

void BucketStorageBackend::RollbackCommittedBucket(
    int64_t bucket_id, const std::vector<std::string>& keys) {
    std::shared_ptr<BucketMetadata> bucket_meta;

    // Phase 1: Remove from metadata maps under exclusive lock.
    // This prevents new readers from finding the keys.
    {
        SharedMutexLocker lock(&mutex_);

        auto bucket_it = buckets_.find(bucket_id);
        if (bucket_it == buckets_.end()) {
            LOG(WARNING) << "RollbackCommittedBucket: bucket " << bucket_id
                         << " not found in buckets_ — already removed?";
            // Still clean up disk files in case they are orphaned
            CleanupOrphanedBucket(bucket_id);
            return;
        }

        // Save a reference for inflight-read waiting
        bucket_meta = bucket_it->second;

        // Remove all keys from object_bucket_map_
        for (const auto& key : keys) {
            auto obj_it = object_bucket_map_.find(key);
            if (obj_it != object_bucket_map_.end() &&
                obj_it->second.bucket_id == bucket_id) {
                total_size_ -=
                    obj_it->second.data_size + obj_it->second.key_size;
                object_bucket_map_.erase(obj_it);
            }
        }

        // Remove bucket metadata
        total_size_ -= bucket_meta->meta_size;
        lru_index_.erase({0LL, bucket_id});
        buckets_.erase(bucket_it);
    }

    // Phase 2: Wait for inflight reads to drain.
    // Readers that found the key before we removed it from the map
    // hold a BucketReadGuard that keeps inflight_reads_ > 0.
    // In practice this should never block: the bucket was committed and
    // rolled back within microseconds — no reader had time to acquire a
    // guard. But guard against the edge case anyway.
    //
    // Uses the same spin-then-sleep pattern as DeleteBucket (not the
    // spin-then-yield pattern of FinalizeEviction) because rollback is a
    // rare error-recovery path where CPU friendliness matters more than
    // latency.
    {
        constexpr int kMaxSpinIterations = 1000;
        constexpr auto kSleepDuration = std::chrono::microseconds(100);
        constexpr auto kMaxWaitTime = std::chrono::seconds(10);
        int spin_count = 0;
        auto wait_start = std::chrono::steady_clock::now();
        while (bucket_meta->inflight_reads_.load(std::memory_order_acquire) >
               0) {
            if (++spin_count > kMaxSpinIterations) {
                std::this_thread::sleep_for(kSleepDuration);
                spin_count = 0;
                if (std::chrono::steady_clock::now() - wait_start >
                    kMaxWaitTime) {
                    LOG(ERROR)
                        << "RollbackCommittedBucket: timed out waiting "
                        << "for inflight reads on bucket " << bucket_id
                        << " (inflight="
                        << bucket_meta->inflight_reads_.load(
                               std::memory_order_relaxed)
                        << "). Leaving orphaned files on disk; they will "
                        << "be cleaned up by Init() on next restart.";
                    // Return WITHOUT deleting files. A reader is still
                    // holding a guard, so deleting the files could cause
                    // I/O errors on the read path. This matches
                    // DeleteBucket's behavior (returns INTERNAL_ERROR
                    // instead of deleting). The orphan will be recovered
                    // by Init()'s orphan scan.
                    return;
                }
            } else {
                PAUSE();
            }
        }
    }

    // Phase 3: Delete on-disk files now that no readers remain.
    CleanupOrphanedBucket(bucket_id);

    LOG(INFO) << "RollbackCommittedBucket: rolled back bucket " << bucket_id
              << " with " << keys.size() << " keys";
}

std::map<int64_t, std::shared_ptr<BucketMetadata>>::iterator
BucketStorageBackend::SelectEvictionCandidate() {
    // Must be called with mutex_ held (exclusive).
    switch (bucket_backend_config_.eviction_policy) {
        case BucketEvictionPolicy::FIFO:
            // buckets_ is ordered by bucket_id (monotonically increasing),
            // so begin() is always the oldest bucket.
            return buckets_.begin();

        case BucketEvictionPolicy::LRU:
            // Use lru_index_ (a std::set ordered by {last_access_ns_,
            // bucket_id}) for O(log N) candidate selection.
            //
            // The index may be stale: BatchLoad updates last_access_ns_
            // atomically under a shared lock without touching lru_index_.
            // We repair lazily here (called under exclusive lock):
            //   - If the top entry's timestamp matches the actual
            //     last_access_ns_, it is the true LRU candidate.
            //   - If stale, re-insert with the correct timestamp and retry.
            //   - If the bucket no longer exists, discard the entry.
            while (!lru_index_.empty()) {
                auto top_it = lru_index_.begin();
                auto [ts, id] = *top_it;
                auto bucket_it = buckets_.find(id);
                if (bucket_it == buckets_.end()) {
                    lru_index_.erase(top_it);
                    continue;
                }
                int64_t actual_ts = bucket_it->second->last_access_ns_.load(
                    std::memory_order_relaxed);
                if (actual_ts == ts) {
                    // Correct entry: remove from index (bucket is about to be
                    // evicted) and return.
                    lru_index_.erase(top_it);
                    return bucket_it;
                }
                // Stale: repair and retry to find the true minimum.
                lru_index_.erase(top_it);
                lru_index_.emplace(actual_ts, id);
            }
            return buckets_.end();

        default:
            return buckets_.end();
    }
}

tl::expected<BucketStorageBackend::PendingEviction, ErrorCode>
BucketStorageBackend::PrepareEviction(
    int64_t required_size, const std::vector<std::string>& write_keys) {
    PendingEviction result;
    SharedMutexLocker lock(&mutex_);

    if (!write_keys.empty()) {
        for (const auto& key : write_keys) {
            if (object_bucket_map_.find(key) != object_bucket_map_.end() ||
                pending_eviction_keys_.find(key) !=
                    pending_eviction_keys_.end() ||
                pending_write_keys_.find(key) != pending_write_keys_.end()) {
                return tl::make_unexpected(ErrorCode::OBJECT_ALREADY_EXISTS);
            }
        }
        result.write_keys = write_keys;
        result.write_size = required_size;
        pending_write_size_ += required_size;
        pending_write_keys_.insert(write_keys.begin(), write_keys.end());
    }

    if (bucket_backend_config_.eviction_policy == BucketEvictionPolicy::NONE) {
        return result;
    }

    // Check actual disk space once before the loop. PrepareEviction only
    // removes metadata -- files are deleted later in FinalizeEviction -- so
    // re-checking disk space inside the loop would yield the same result
    // every iteration and is unnecessary.
    //
    // When disk is full, we calculate the space deficit and accumulate
    // the estimated freed space (data_size + meta_size) per evicted
    // bucket. Due to block alignment, actual disk usage >= data_size +
    // meta_size, so this is a safe lower bound -- we will not under-evict.
    bool initial_disk_full = false;
    uint64_t deficit = 0;
    {
        namespace fs = std::filesystem;
        std::error_code ec;
        auto space_info = fs::space(storage_path_, ec);
        if (!ec) {
            uint64_t actual_available = space_info.available;
            constexpr uint64_t kMinFreeSpace = 256 * kMB;
            // Watermark eviction passes a synthetic required_size to drive
            // quota-based cleanup. It is not a real incoming write, so it
            // should not be counted as physical disk free-space demand.
            uint64_t req_sz = (!write_keys.empty() && required_size > 0)
                                  ? static_cast<uint64_t>(required_size)
                                  : 0;
            initial_disk_full = actual_available < req_sz + kMinFreeSpace;
            if (initial_disk_full) {
                deficit = req_sz + kMinFreeSpace - actual_available;
                LOG(WARNING) << "[Evict] Actual disk space too low: available="
                             << actual_available << ", required=" << req_sz
                             << ", deficit=" << deficit
                             << ". Will evict buckets to free space.";
            }
        } else {
            LOG(WARNING) << "[Evict] Failed to get disk space info for "
                         << storage_path_ << ": " << ec.message();
        }
    }

    size_t evict_count = 0;
    constexpr size_t kMaxEvictionBuckets = 1000;
    uint64_t accumulated_freed_space = 0;
    const int64_t synthetic_required_size =
        write_keys.empty() ? required_size : 0;

    while (!buckets_.empty() && evict_count < kMaxEvictionBuckets) {
        bool quota_exceeded = total_size_ + pending_eviction_size_ +
                                  pending_write_size_ +
                                  synthetic_required_size >
                              bucket_backend_config_.max_total_size;

        bool disk_still_full =
            initial_disk_full && (accumulated_freed_space < deficit);

        if (!quota_exceeded && !disk_still_full) break;

        if (evict_count == 0) {
            LOG(INFO) << "[Evict] triggered: total=" << total_size_ << "/"
                      << bucket_backend_config_.max_total_size
                      << " required=" << required_size
                      << " disk_full=" << initial_disk_full;
        }

        auto evict_it = SelectEvictionCandidate();
        if (evict_it == buckets_.end()) break;

        int64_t evict_id = evict_it->first;
        std::shared_ptr<BucketMetadata> evict_meta =
            std::move(evict_it->second);
        buckets_.erase(evict_it);

        int64_t evicted_size = evict_meta->meta_size;
        // Remove all keys belonging to this bucket from the object map.
        for (const auto& key : evict_meta->keys) {
            auto obj_it = object_bucket_map_.find(key);
            if (obj_it != object_bucket_map_.end() &&
                obj_it->second.bucket_id == evict_id) {
                const int64_t object_size =
                    obj_it->second.data_size + obj_it->second.key_size;
                total_size_ -= object_size;
                evicted_size += object_size;
                object_bucket_map_.erase(obj_it);
            }
        }
        total_size_ -= evict_meta->meta_size;
        result.evicted_size += evicted_size;

        // Collect for notification and file deletion.
        for (const auto& key : evict_meta->keys) {
            pending_eviction_keys_.insert(key);
            result.keys.push_back(key);
        }
        accumulated_freed_space +=
            static_cast<uint64_t>(evict_meta->data_size) +
            static_cast<uint64_t>(evict_meta->meta_size);
        result.buckets.emplace_back(evict_id, std::move(evict_meta));
        evict_count++;
    }

    const bool quota_exceeded = total_size_ + pending_eviction_size_ +
                                    pending_write_size_ +
                                    synthetic_required_size >
                                bucket_backend_config_.max_total_size;
    pending_eviction_size_ += result.evicted_size;
    if (!write_keys.empty() && quota_exceeded) {
        RestorePreparedEvictionLocked(std::move(result));
        return tl::make_unexpected(ErrorCode::FILE_WRITE_FAIL);
    }

    if (!result.buckets.empty()) {
        LOG(INFO) << "[Evict] prepared: buckets=" << result.buckets.size()
                  << " keys=" << result.keys.size()
                  << " total_after=" << total_size_;
    }

    return result;
}

void BucketStorageBackend::RestorePreparedEviction(PendingEviction&& pending) {
    SharedMutexLocker lock(&mutex_);
    RestorePreparedEvictionLocked(std::move(pending));
}

void BucketStorageBackend::RestorePreparedEvictionLocked(
    PendingEviction&& pending) {
    ReleasePreparedWriteLocked(pending);

    CHECK_GE(pending_eviction_size_, pending.evicted_size);
    pending_eviction_size_ -= pending.evicted_size;
    for (const auto& key : pending.keys) {
        pending_eviction_keys_.erase(key);
    }
    for (auto& [bucket_id, bucket_meta] : pending.buckets) {
        if (!bucket_meta || buckets_.find(bucket_id) != buckets_.end()) {
            continue;
        }

        for (size_t i = 0; i < bucket_meta->keys.size(); ++i) {
            const auto& key = bucket_meta->keys[i];
            const auto& object_meta = bucket_meta->metadatas[i];
            object_bucket_map_[key] = StorageObjectMetadata{
                bucket_id, object_meta.offset, object_meta.key_size,
                object_meta.data_size, ""};
            total_size_ += object_meta.data_size + object_meta.key_size;
        }
        total_size_ += bucket_meta->meta_size;
        if (bucket_backend_config_.eviction_policy ==
            BucketEvictionPolicy::LRU) {
            lru_index_.emplace(
                bucket_meta->last_access_ns_.load(std::memory_order_relaxed),
                bucket_id);
        }
        buckets_.emplace(bucket_id, std::move(bucket_meta));
    }
}

void BucketStorageBackend::CommitPreparedEviction(
    const PendingEviction& pending) {
    if (pending.evicted_size == 0) {
        return;
    }

    SharedMutexLocker lock(&mutex_);
    CHECK_GE(pending_eviction_size_, pending.evicted_size);
    pending_eviction_size_ -= pending.evicted_size;
    for (const auto& key : pending.keys) {
        pending_eviction_keys_.erase(key);
    }
}

void BucketStorageBackend::ReleasePreparedWrite(
    const PendingEviction& pending) {
    if (pending.write_size == 0) {
        return;
    }

    SharedMutexLocker lock(&mutex_);
    ReleasePreparedWriteLocked(pending);
}

void BucketStorageBackend::ReleasePreparedWriteLocked(
    const PendingEviction& pending) {
    CHECK_GE(pending_write_size_, pending.write_size);
    pending_write_size_ -= pending.write_size;
    for (const auto& key : pending.write_keys) {
        pending_write_keys_.erase(key);
    }
}

tl::expected<void, ErrorCode> BucketStorageBackend::FinalizeEviction(
    const PendingEviction& pending) {
    namespace fs = std::filesystem;

    constexpr int kMaxSpinIterations = 1000;
    constexpr auto kMaxWaitTime = std::chrono::seconds(10);
    size_t cleanup_failed_count = 0;

    for (const auto& [bucket_id, bucket_meta] : pending.buckets) {
        bool bucket_cleanup_failed = false;

        // The master has already committed the replica removal. Attempt to
        // remove persisted metadata before waiting for readers. When this
        // succeeds, a later timeout or data-file deletion failure leaves an
        // orphan data file instead of a bucket that Init() could recover.
        std::error_code ec;
        auto meta_path = GetBucketMetadataPath(bucket_id);
        if (meta_path) {
            fs::remove(meta_path.value(), ec);
            if (ec && ec != std::errc::no_such_file_or_directory) {
                LOG(ERROR)
                    << "FinalizeEviction: failed to remove metadata file: "
                    << meta_path.value() << ", error: " << ec.message();
                bucket_cleanup_failed = true;
            }
        }

        // Wait for in-flight reads that started before the bucket was removed
        // from the metadata maps in PrepareEviction.
        bool timed_out = false;
        int spin_count = 0;
        auto wait_start = std::chrono::steady_clock::now();
        while (bucket_meta->inflight_reads_.load(std::memory_order_acquire) >
               0) {
            if (++spin_count > kMaxSpinIterations) {
                std::this_thread::yield();
                spin_count = 0;
                if (std::chrono::steady_clock::now() - wait_start >
                    kMaxWaitTime) {
                    LOG(ERROR)
                        << "FinalizeEviction: timed out waiting for in-flight "
                           "reads, bucket_id="
                        << bucket_id << ", inflight_reads="
                        << bucket_meta->inflight_reads_.load(
                               std::memory_order_relaxed);
                    timed_out = true;
                    break;
                }
            } else {
                PAUSE();
            }
        }
        if (timed_out) {
            cleanup_failed_count++;
            continue;
        }

        ec.clear();
        auto data_path = GetBucketDataPath(bucket_id);
        if (data_path) {
            // Evict the cached file handle before deleting the file to prevent
            // stale handles from accumulating in the cache.
            {
                MutexLocker cache_locker(&file_cache_mutex_);
                file_cache_.erase(data_path.value());
            }
            fs::remove(data_path.value(), ec);
            if (ec && ec != std::errc::no_such_file_or_directory) {
                // File remains on disk as an orphan; disk space is not freed.
                // WriteBucket may then fail with ENOSPC. Orphan will be cleaned
                // up on service restart.
                LOG(ERROR) << "FinalizeEviction: failed to remove data file: "
                           << data_path.value() << ", error: " << ec.message();
                bucket_cleanup_failed = true;
            }
        }
        if (bucket_cleanup_failed) {
            cleanup_failed_count++;
        }
    }
    if (!pending.buckets.empty()) {
        LOG(INFO) << "[Evict] finalized: attempted=" << pending.buckets.size()
                  << " cleanup_failed=" << cleanup_failed_count;
    }
    if (cleanup_failed_count != 0) {
        return tl::make_unexpected(ErrorCode::FILE_WRITE_FAIL);
    }
    return {};
}

tl::expected<std::vector<std::string>, ErrorCode>
BucketStorageBackend::EvictAboveDiskWatermark(
    double high_watermark_ratio, double low_watermark_ratio,
    EvictionHandler eviction_handler) {
    std::vector<std::string> evicted_keys;
    if (bucket_backend_config_.eviction_policy == BucketEvictionPolicy::NONE ||
        bucket_backend_config_.max_total_size <= 0) {
        return evicted_keys;
    }
    if (!initialized_.load(std::memory_order_acquire)) {
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }

    int64_t total_size = 0;
    int64_t max_total_size = bucket_backend_config_.max_total_size;
    {
        SharedMutexLocker lock(&mutex_, shared_lock);
        total_size = total_size_;
    }

    const auto high_watermark_bytes =
        static_cast<int64_t>(max_total_size * high_watermark_ratio);
    if (total_size <= high_watermark_bytes) {
        return evicted_keys;
    }

    const auto target_total_size =
        static_cast<int64_t>(max_total_size * low_watermark_ratio);
    const auto synthetic_required_size = max_total_size - target_total_size;
    auto prepare_result = PrepareEviction(synthetic_required_size);
    if (!prepare_result) {
        return tl::make_unexpected(prepare_result.error());
    }
    PendingEviction pending = std::move(prepare_result.value());
    evicted_keys = pending.keys;

    if (eviction_handler && !pending.keys.empty()) {
        auto notify_result = eviction_handler(pending.keys);
        if (!notify_result) {
            RestorePreparedEviction(std::move(pending));
            return tl::make_unexpected(notify_result.error());
        }
    }
    CommitPreparedEviction(pending);
    auto finalize_result = FinalizeEviction(pending);
    if (!finalize_result) {
        LOG(ERROR)
            << "FinalizeEviction failed after master committed eviction; "
               "returning evicted keys: "
            << finalize_result.error();
    }
    return evicted_keys;
}

tl::expected<void, ErrorCode> BucketStorageBackend::DeleteBucket(
    int64_t bucket_id) {
    namespace fs = std::filesystem;
    std::shared_ptr<BucketMetadata> bucket_metadata;
    std::vector<std::string> keys_to_remove;

    // Step 1: Remove bucket from metadata maps under exclusive lock
    {
        SharedMutexLocker lock(&mutex_);

        auto bucket_it = buckets_.find(bucket_id);
        if (bucket_it == buckets_.end()) {
            LOG(WARNING) << "DeleteBucket: bucket not found, bucket_id="
                         << bucket_id;
            return tl::make_unexpected(ErrorCode::BUCKET_NOT_FOUND);
        }

        // Move the shared_ptr out - we now own it
        bucket_metadata = std::move(bucket_it->second);
        lru_index_.erase(
            {bucket_metadata->last_access_ns_.load(std::memory_order_relaxed),
             bucket_id});
        buckets_.erase(bucket_it);

        // Collect keys to remove (they reference this bucket)
        keys_to_remove = bucket_metadata->keys;
        for (const auto& key : keys_to_remove) {
            auto obj_it = object_bucket_map_.find(key);
            if (obj_it != object_bucket_map_.end() &&
                obj_it->second.bucket_id == bucket_id) {
                total_size_ -=
                    obj_it->second.data_size + obj_it->second.key_size;
                object_bucket_map_.erase(obj_it);
            }
        }

        // Subtract metadata size
        total_size_ -= bucket_metadata->meta_size;
    }
    // Lock released - new readers can't find this bucket anymore

    // Step 2: Wait for in-flight reads to complete
    // Readers that started before we removed from buckets_ still hold guards
    constexpr int kMaxSpinIterations = 1000;
    constexpr auto kSleepDuration = std::chrono::microseconds(100);
    constexpr auto kMaxWaitTime = std::chrono::seconds(10);

    int spin_count = 0;
    auto wait_start = std::chrono::steady_clock::now();
    while (bucket_metadata->inflight_reads_.load(std::memory_order_acquire) >
           0) {
        if (++spin_count > kMaxSpinIterations) {
            // After spinning, sleep briefly and check timeout
            std::this_thread::sleep_for(kSleepDuration);
            spin_count = 0;  // Reset and continue waiting

            auto elapsed = std::chrono::steady_clock::now() - wait_start;
            if (elapsed > kMaxWaitTime) {
                LOG(ERROR)
                    << "DeleteBucket: timed out waiting for in-flight reads"
                    << ", bucket_id=" << bucket_id << ", inflight_reads="
                    << bucket_metadata->inflight_reads_.load(
                           std::memory_order_relaxed);
                return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
            }
        } else {
            PAUSE();
        }
    }

    // Step 3: Safe to delete files now - no readers are using them
    std::error_code ec;

    auto data_path_res = GetBucketDataPath(bucket_id);
    if (data_path_res) {
        fs::remove(data_path_res.value(), ec);
        if (ec && ec != std::errc::no_such_file_or_directory) {
            LOG(WARNING) << "DeleteBucket: failed to remove data file: "
                         << data_path_res.value()
                         << ", error: " << ec.message();
        }
    }

    auto meta_path_res = GetBucketMetadataPath(bucket_id);
    if (meta_path_res) {
        ec.clear();
        fs::remove(meta_path_res.value(), ec);
        if (ec && ec != std::errc::no_such_file_or_directory) {
            LOG(WARNING) << "DeleteBucket: failed to remove metadata file: "
                         << meta_path_res.value()
                         << ", error: " << ec.message();
        }
    }

    LOG(INFO) << "DeleteBucket: successfully deleted bucket_id=" << bucket_id
              << ", keys_removed=" << keys_to_remove.size();
    return {};
}

tl::expected<void, ErrorCode> BucketStorageBackend::StoreBucketMetadata(
    int64_t id, std::shared_ptr<BucketMetadata> metadata) {
    auto meta_path_res = GetBucketMetadataPath(id);
    if (!meta_path_res) {
        LOG(ERROR) << "Failed to get bucket metadata path, bucket_id=" << id;
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }
    auto meta_path = meta_path_res.value();
    auto open_file_result = OpenFile(meta_path, FileMode::Write);
    if (!open_file_result) {
        LOG(ERROR) << "Failed to open file for bucket writing: " << meta_path;
        return tl::make_unexpected(ErrorCode::FILE_OPEN_FAIL);
    }
    auto file = std::move(open_file_result.value());
    std::string str;
    struct_pb::to_pb(*metadata, str);
    auto write_result = file->write(str, str.size());
    if (!write_result) {
        LOG(ERROR) << "Write failed for: " << meta_path
                   << ", error: " << write_result.error();
        return tl::make_unexpected(write_result.error());
    }
    if (write_result.value() != str.size()) {
        LOG(ERROR) << "Write size mismatch for: " << meta_path
                   << ", expected: " << str.size()
                   << ", got: " << write_result.value();
        return tl::make_unexpected(ErrorCode::FILE_WRITE_FAIL);
    }
    metadata->meta_size = str.size();
    return {};
}

tl::expected<void, ErrorCode> BucketStorageBackend::LoadBucketMetadata(
    int64_t id, std::shared_ptr<BucketMetadata> metadata) {
    auto meta_path_res = GetBucketMetadataPath(id);
    if (!meta_path_res) {
        LOG(ERROR) << "Failed to get bucket metadata path, bucket_id=" << id;
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }
    auto meta_path = meta_path_res.value();
    auto open_file_result = OpenFile(meta_path, FileMode::Read);
    if (!open_file_result) {
        LOG(ERROR) << "Failed to open file for reading: " << meta_path;
        return tl::make_unexpected(open_file_result.error());
    }
    auto file = std::move(open_file_result.value());
    std::string str;
    int64_t size = std::filesystem::file_size(meta_path);
    auto read_result = file->read(str, size);
    if (!read_result) {
        LOG(ERROR) << "read failed for: " << meta_path
                   << ", error: " << read_result.error();
        return tl::make_unexpected(read_result.error());
    }
    if (static_cast<int64_t>(read_result.value()) != size) {
        LOG(ERROR) << "Read size mismatch for: " << meta_path
                   << ", expected: " << size
                   << ", got: " << read_result.value();
        return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
    }
    try {
        struct_pb::from_pb(*metadata, str);
        metadata->meta_size = size;
    } catch (const std::exception& e) {
        LOG(ERROR) << "Metadata parsing failed with exception: " << e.what();
        return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
    } catch (...) {
        LOG(ERROR) << "Metadata parsing failed with unknown exception";
        return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
    }
    return {};
}

tl::expected<std::string, ErrorCode> BucketStorageBackend::GetBucketDataPath(
    int64_t bucket_id) {
    std::string sep =
        storage_path_.empty() || storage_path_.back() == '/' ? "" : "/";
    return storage_path_ + sep + std::to_string(bucket_id) +
           BUCKET_DATA_FILE_SUFFIX;
}

tl::expected<std::string, ErrorCode>
BucketStorageBackend::GetBucketMetadataPath(int64_t bucket_id) {
    std::string sep =
        storage_path_.empty() || storage_path_.back() == '/' ? "" : "/";
    return storage_path_ + sep + std::to_string(bucket_id) +
           BUCKET_METADATA_FILE_SUFFIX;
}

tl::expected<std::unique_ptr<StorageFile>, ErrorCode>
BucketStorageBackend::OpenFile(const std::string& path, FileMode mode) const {
    int flags = O_CLOEXEC;
    int access_mode = 0;
    switch (mode) {
        case FileMode::Read:
            access_mode = O_RDONLY;
            break;
        case FileMode::Write:
            access_mode = O_WRONLY | O_CREAT | O_TRUNC;
            break;
    }

#ifdef USE_URING
    // Use O_DIRECT only for reads: write latency is not sensitive in this
    // scenario, and O_DIRECT writes require 4096-byte alignment padding which
    // corrupts meta file parsing and wastes disk space on data files.
    if (file_storage_config_.use_uring && mode == FileMode::Read) {
        flags |= O_DIRECT;
    }
#endif

    int fd = open(path.c_str(), flags | access_mode, 0644);
    if (fd < 0) {
        LOG(ERROR) << "Failed to open file: " << path << ", errno=" << errno
                   << " (" << strerror(errno) << ")";
        return tl::make_unexpected(ErrorCode::FILE_OPEN_FAIL);
    }
#ifdef USE_URING
    if (file_storage_config_.use_uring && mode == FileMode::Read) {
        return std::make_unique<UringFile>(path, fd, 32, true);
    }
#endif
    return std::make_unique<PosixFile>(path, fd);
}

tl::expected<std::shared_ptr<StorageFile>, ErrorCode>
BucketStorageBackend::GetOrOpenFile(const std::string& path,
                                    FileMode mode) const {
    // Only cache read-mode files (write mode needs O_TRUNC which invalidates
    // cache)
    if (mode == FileMode::Read) {
        MutexLocker locker(&file_cache_mutex_);
        auto it = file_cache_.find(path);
        if (it != file_cache_.end()) {
            return it->second;
        }
    }

    // Open new file
    auto result = OpenFile(path, mode);
    if (!result) {
        return tl::make_unexpected(result.error());
    }

    auto file = std::shared_ptr<StorageFile>(std::move(result.value()));

    // Cache read-mode files
    if (mode == FileMode::Read) {
        MutexLocker locker(&file_cache_mutex_);
        file_cache_[path] = file;
    }

    return file;
}

void BucketStorageBackend::ClearFileCache() {
    MutexLocker locker(&file_cache_mutex_);
    file_cache_.clear();
}

tl::expected<void, ErrorCode> BucketStorageBackend::HandleNext(
    const std::function<
        ErrorCode(const std::vector<std::string>& keys,
                  std::vector<StorageObjectMetadata>& metadatas)>& handler) {
    MutexLocker locker(&iterator_mutex_);
    std::vector<std::string> keys;
    std::vector<StorageObjectMetadata> metadatas;
    std::vector<int64_t> buckets;
    auto key_iterator_result =
        BucketScan(next_bucket_, keys, metadatas, buckets,
                   file_storage_config_.scanmeta_iterator_keys_limit);
    if (!key_iterator_result) {
        LOG(ERROR) << "Bucket scan failed, error : "
                   << key_iterator_result.error();
        return tl::make_unexpected(key_iterator_result.error());
    }
    auto handle_result = handler(keys, metadatas);
    if (handle_result != ErrorCode::OK) {
        LOG(ERROR) << "Key iterator failed, error : " << handle_result;
        return tl::make_unexpected(handle_result);
    }
    next_bucket_ = key_iterator_result.value();
    return {};
}

tl::expected<bool, ErrorCode> BucketStorageBackend::HasNext() {
    MutexLocker locker(&iterator_mutex_);
    return next_bucket_ != 0;
}

tl::expected<std::shared_ptr<StorageFile>, ErrorCode>
BucketStorageBackend::GetFileInstance() const {
    // Create a temporary file to get access to the file instance
    // This is used for external buffer registration with UringFile
    namespace fs = std::filesystem;

    std::string temp_path =
        (fs::path(storage_path_) / "temp_for_registration").string();

    auto open_result = OpenFile(temp_path, FileMode::Write);
    if (!open_result) {
        LOG(ERROR) << "Failed to open temporary file for GetFileInstance: "
                   << temp_path;
        return tl::make_unexpected(open_result.error());
    }

    auto file = std::move(open_result.value());

    // Remove the temporary file from disk now that the fd is open.
    // The fd remains valid (Unix semantics) until the StorageFile is destroyed.
    fs::remove(temp_path);

    // Convert unique_ptr to shared_ptr
    return std::shared_ptr<StorageFile>(std::move(file));
}

// ============================================================================
// OffsetAllocatorStorageBackend Implementation
// ============================================================================

OffsetAllocatorStorageBackend::OffsetAllocatorStorageBackend(
    const FileStorageConfig& file_storage_config_,
    const OffsetAllocatorBackendConfig& offset_backend_config)
    : StorageBackendInterface(file_storage_config_),
      storage_path_(file_storage_config_.storage_filepath),
      cfg_(offset_backend_config) {
    capacity_ = file_storage_config_.total_size_limit;
}

OffsetAllocatorStorageBackend::~OffsetAllocatorStorageBackend() {
    try {
        if (cfg_.persist_mode == OffsetPersistMode::kDisabled) return;
        if (!initialized_.load(std::memory_order_acquire)) return;
        if (!data_file_) return;

        if (!metadata_dirty_.load(std::memory_order_relaxed)) {
            // Nothing to persist.
            return;
        }

        // Best-effort final checkpoint on graceful shutdown.
        auto sync_res = data_file_->datasync();
        if (!sync_res) {
            LOG(ERROR) << "Final checkpoint datasync failed: "
                       << static_cast<int>(sync_res.error());
            return;
        }

        auto save_res = SaveMetadata(all_evicted_this_batch_);
        if (!save_res) {
            LOG(ERROR) << "Final checkpoint SaveMetadata failed: "
                       << static_cast<int>(save_res.error());
            return;
        }

        all_evicted_this_batch_.clear();
        LOG(INFO) << "Final persistence checkpoint completed on shutdown";
    } catch (const std::exception& e) {
        LOG(ERROR) << "Final checkpoint threw: " << e.what();
    } catch (...) {
        LOG(ERROR) << "Final checkpoint threw unknown exception";
    }
}

std::string OffsetAllocatorStorageBackend::GetDataFilePath() const {
    return (std::filesystem::path(storage_path_) / "kv_cache.data").string();
}

std::string OffsetAllocatorStorageBackend::GetMetaFilePath() const {
    return (std::filesystem::path(storage_path_) / "kv_cache.meta").string();
}

//-----------------------------------------------------------------------------

tl::expected<void, ErrorCode> OffsetAllocatorStorageBackend::Init() {
    namespace fs = std::filesystem;
    try {
        if (initialized_.load(std::memory_order_acquire)) {
            LOG(ERROR) << "Storage backend already initialized";
            return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
        }

        // Validate capacity - allocator requires positive capacity
        if (capacity_ == 0) {
            LOG(ERROR) << "Invalid capacity for OffsetAllocatorStorageBackend: "
                       << capacity_ << ". Capacity must be > 0";
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }

        // Ensure storage path exists
        {
            std::error_code ec;
            fs::create_directories(storage_path_, ec);
            if (ec) {
                LOG(ERROR) << "Failed to create storage directory: "
                           << storage_path_;
                return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
            }
        }

        // ---- Resolve watermarks ----
        high_watermark_bytes_ =
            cfg_.high_watermark_bytes > 0
                ? cfg_.high_watermark_bytes
                : static_cast<int64_t>(capacity_ * cfg_.high_ratio);
        low_watermark_bytes_ =
            cfg_.low_watermark_bytes > 0
                ? cfg_.low_watermark_bytes
                : static_cast<int64_t>(capacity_ * cfg_.low_ratio);
        high_watermark_keys_ =
            cfg_.high_watermark_keys > 0
                ? cfg_.high_watermark_keys
                : static_cast<int64_t>(file_storage_config_.total_keys_limit *
                                       cfg_.keys_high_ratio);
        low_watermark_keys_ =
            cfg_.low_watermark_keys > 0
                ? cfg_.low_watermark_keys
                : static_cast<int64_t>(file_storage_config_.total_keys_limit *
                                       cfg_.keys_low_ratio);

        // Auto-nudge ratio-derived low watermarks when integer
        // truncation collapses them to the same value as high
        if (cfg_.low_watermark_bytes == 0 && high_watermark_bytes_ > 0 &&
            low_watermark_bytes_ >= high_watermark_bytes_) {
            low_watermark_bytes_ =
                std::max<int64_t>(1, high_watermark_bytes_ - 1);
        }
        if (cfg_.low_watermark_keys == 0 && high_watermark_keys_ > 0 &&
            low_watermark_keys_ >= high_watermark_keys_) {
            low_watermark_keys_ =
                std::max<int64_t>(1, high_watermark_keys_ - 1);
        }

        // Validate watermarks
        if (low_watermark_bytes_ >= high_watermark_bytes_) {
            LOG(ERROR) << "Invalid watermark: low_bytes="
                       << low_watermark_bytes_
                       << " >= high_bytes=" << high_watermark_bytes_;
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
        if (low_watermark_keys_ >= high_watermark_keys_) {
            LOG(ERROR) << "Invalid watermark: low_keys=" << low_watermark_keys_
                       << " >= high_keys=" << high_watermark_keys_;
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }

        // Clamp watermarks to not exceed capacity / total_keys_limit
        if (high_watermark_bytes_ > static_cast<int64_t>(capacity_)) {
            LOG(WARNING) << "high_watermark_bytes clamped from "
                         << high_watermark_bytes_
                         << " to capacity=" << capacity_;
            high_watermark_bytes_ = static_cast<int64_t>(capacity_);
            low_watermark_bytes_ =
                std::min(low_watermark_bytes_, high_watermark_bytes_ - 1);
        }
        if (high_watermark_keys_ > file_storage_config_.total_keys_limit) {
            LOG(WARNING) << "high_watermark_keys clamped from "
                         << high_watermark_keys_ << " to total_keys_limit="
                         << file_storage_config_.total_keys_limit;
            high_watermark_keys_ = file_storage_config_.total_keys_limit;
            low_watermark_keys_ =
                std::min(low_watermark_keys_, high_watermark_keys_ - 1);
        }

        // Guard against zero low-watermark on very small capacity
        if (low_watermark_bytes_ <= 0 && high_watermark_bytes_ > 0) {
            low_watermark_bytes_ =
                std::max<int64_t>(1, high_watermark_bytes_ / 2);
        }
        if (low_watermark_keys_ <= 0 && high_watermark_keys_ > 0) {
            low_watermark_keys_ =
                std::max<int64_t>(1, high_watermark_keys_ / 2);
        }

        // ---- Recovery path ----
        if (cfg_.persist_mode != OffsetPersistMode::kDisabled &&
            TryRecoverFromMetadata()) {
            last_persist_time_us_.store(
                std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::steady_clock::now().time_since_epoch())
                    .count(),
                std::memory_order_relaxed);
            initialized_.store(true, std::memory_order_release);
            LOG(INFO) << "OffsetAllocatorStorageBackend recovered: "
                      << total_keys_.load() << " keys, " << total_size_.load()
                      << " bytes";
            return {};
        }

        // ---- Fresh start path ----
        LOG(INFO) << "Fresh start path";

        // Close any recovery fd and clean up stale metadata
        data_file_.reset();
        {
            std::error_code ec;
            fs::remove(GetMetaFilePath(), ec);
            fs::remove(GetMetaFilePath() + ".tmp", ec);
        }

        // Clear in-memory maps (V1: no persistence, start fresh)
        {
            std::vector<std::unique_ptr<SharedMutexLocker>> shard_locks;
            shard_locks.reserve(kNumShards);
            for (size_t i = 0; i < kNumShards; ++i) {
                shard_locks.emplace_back(
                    std::make_unique<SharedMutexLocker>(&shards_[i].mutex));
                shards_[i].map.clear();
            }
            total_size_.store(0, std::memory_order_relaxed);
            total_keys_.store(0, std::memory_order_relaxed);
        }

        // Get data file path
        data_file_path_ = GetDataFilePath();

        // Open/truncate data file in read-write mode
        int flags = O_CLOEXEC | O_RDWR | O_CREAT | O_TRUNC;
        int raw_fd = open(data_file_path_.c_str(), flags, 0644);
        if (raw_fd < 0) {
            LOG(ERROR) << "Failed to open data file: " << data_file_path_;
            return tl::make_unexpected(ErrorCode::FILE_OPEN_FAIL);
        }
        FdGuard fd_guard(raw_fd);

        // Use fallocate if available, otherwise ftruncate
        if (fallocate(fd_guard.get(), 0, 0, capacity_) != 0) {
            // Fallback to ftruncate
            if (ftruncate(fd_guard.get(), capacity_) != 0) {
                LOG(ERROR) << "Failed to preallocate file: " << data_file_path_
                           << ", capacity: " << capacity_
                           << ", error: " << strerror(errno);
                return tl::make_unexpected(ErrorCode::FILE_WRITE_FAIL);
            }
        }

        // Release fd to StorageFile (takes ownership and will close it)
#ifdef USE_URING
        if (file_storage_config_.use_uring) {
            data_file_ = std::make_unique<UringFile>(
                data_file_path_, fd_guard.release(), 32, true);
        } else
#endif
        {
            data_file_ = std::make_unique<PosixFile>(data_file_path_,
                                                     fd_guard.release());
        }
        if (cfg_.persist_mode != OffsetPersistMode::kDisabled) {
            data_file_->SetDeleteOnWriteFail(false);
        }

        // Create allocator with tuned node capacity
        constexpr int64_t kMinObjectSize = 256;
        constexpr int64_t kMaxNodeRamBytes =
            512LL * 1024 * 1024;  // 512MB node RAM budget
        constexpr uint32_t kRamBasedMaxNodes =
            static_cast<uint32_t>(kMaxNodeRamBytes / 56);
        constexpr uint32_t kAbsoluteMaxNodes =
            std::min<uint32_t>(kRamBasedMaxNodes, 32U << 20);

        uint32_t max_nodes = (1U << 20);  // default 1M nodes
        if (cfg_.max_capacity_nodes > 0) {
            if (cfg_.max_capacity_nodes > kAbsoluteMaxNodes) {
                LOG(WARNING)
                    << "max_capacity_nodes " << cfg_.max_capacity_nodes
                    << " exceeds RAM budget; clamped to " << kAbsoluteMaxNodes;
                max_nodes = kAbsoluteMaxNodes;
            } else {
                max_nodes = static_cast<uint32_t>(cfg_.max_capacity_nodes);
            }
        } else {
            int64_t auto_nodes = std::max<int64_t>(
                1LL << 20, std::min<int64_t>(capacity_ / kMinObjectSize,
                                             kAbsoluteMaxNodes));
            max_nodes = static_cast<uint32_t>(auto_nodes);
        }
        uint32_t init_nodes = std::min<uint32_t>(128U * 1024, max_nodes);
        allocator_ = offset_allocator::OffsetAllocator::create(
            0, capacity_, init_nodes, max_nodes);
        if (!allocator_) {
            LOG(ERROR) << "Failed to create OffsetAllocator";
            return tl::make_unexpected(ErrorCode::FILE_WRITE_FAIL);
        }

        // Initialize eviction index
        {
            MutexLocker ev(&eviction_mutex_);
            fifo_index_.clear();
            insert_seq_.store(0, std::memory_order_relaxed);
        }

        // Initialize persist timestamp (avoid immediate checkpoint
        // on first write)
        last_persist_time_us_.store(
            std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::steady_clock::now().time_since_epoch())
                .count(),
            std::memory_order_relaxed);

        initialized_.store(true, std::memory_order_release);
        LOG(INFO) << "OffsetAllocatorStorageBackend initialized, capacity: "
                  << capacity_
                  << " bytes, high_watermark: " << high_watermark_bytes_
                  << " bytes, " << high_watermark_keys_ << " keys"
                  << ", data file: " << data_file_path_;
    } catch (const std::exception& e) {
        LOG(ERROR) << "OffsetAllocatorStorageBackend initialize error: "
                   << e.what();
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }

    return {};
}

// EvictToMakeRoom
//-----------------------------------------------------------------------------

//-----------------------------------------------------------------------------
// ShouldPersistNow
//-----------------------------------------------------------------------------

bool OffsetAllocatorStorageBackend::ShouldPersistNow() const {
    if (cfg_.persist_mode == OffsetPersistMode::kDisabled) return false;
    if (cfg_.persist_mode == OffsetPersistMode::kStrict) return true;
    using TimePointUs = std::chrono::time_point<std::chrono::steady_clock,
                                                std::chrono::microseconds>;
    auto last_tp = TimePointUs(std::chrono::microseconds(
        last_persist_time_us_.load(std::memory_order_relaxed)));
    return (std::chrono::steady_clock::now() - last_tp) >=
           std::chrono::seconds(cfg_.persist_interval_seconds);
}

//-----------------------------------------------------------------------------
// SaveMetadata
//-----------------------------------------------------------------------------

tl::expected<void, ErrorCode> OffsetAllocatorStorageBackend::SaveMetadata(
    const std::unordered_set<std::string>& evicted_keys_this_batch) {
    namespace fs = std::filesystem;

    if (!metadata_dirty_.exchange(false, std::memory_order_relaxed) &&
        evicted_keys_this_batch.empty()) {
        return {};
    }

    int step = test_metadata_write_failure_step_.exchange(
        0, std::memory_order_relaxed);
    try {
        struct DirtyGuard {
            std::atomic<bool>& dirty;
            std::atomic<int64_t>& failures;
            bool active = true;
            DirtyGuard(std::atomic<bool>& d, std::atomic<int64_t>& f)
                : dirty(d), failures(f) {}
            ~DirtyGuard() {
                if (active) {
                    dirty.store(true, std::memory_order_relaxed);
                    failures.fetch_add(1, std::memory_order_relaxed);
                }
            }
            void disarm() { active = false; }
        } dirty_guard(metadata_dirty_, metadata_save_failures_);

        auto t_start = std::chrono::steady_clock::now();

        // 1. Serialize allocator
        std::vector<SerializedByte> alloc_buf;
        ErrorCode ec = serialize_to(*allocator_, alloc_buf);
        if (ec != ErrorCode::OK || alloc_buf.empty()) {
            return tl::make_unexpected(
                ec != ErrorCode::OK ? ec : ErrorCode::INTERNAL_ERROR);
        }
        if (step == 1) return tl::make_unexpected(ErrorCode::FILE_WRITE_FAIL);

        // 2. Build metadata struct
        OffsetAllocatorPersistedMetadata meta;
        meta.version = 1;
        meta.allocator_state.assign(
            reinterpret_cast<const char*>(alloc_buf.data()), alloc_buf.size());
        meta.evicted_keys_this_batch.assign(evicted_keys_this_batch.begin(),
                                            evicted_keys_this_batch.end());

        {
            MutexLocker ev(&eviction_mutex_);
            meta.insert_seq = insert_seq_.load(std::memory_order_relaxed);
            meta.fifo_entries.reserve(fifo_index_.size());
            for (const auto& [seq, key] : fifo_index_) {
                meta.fifo_entries.push_back({seq, key});
            }
        }

        // 3. Serialize metadata
        std::string buf;
        try {
            struct_pb::to_pb(meta, buf);
        } catch (const std::exception& e) {
            LOG(ERROR) << "struct_pb::to_pb failed in SaveMetadata: "
                       << e.what();
            return tl::make_unexpected(ErrorCode::FILE_WRITE_FAIL);
        }
        if (buf.empty()) {
            LOG(ERROR) << "struct_pb produced empty metadata buffer";
            return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
        }

        // 4. Atomic write via tmp + rename
        std::string meta_path = GetMetaFilePath();
        std::string tmp_path = meta_path + ".tmp";

        int fd = open(tmp_path.c_str(),
                      O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
        if (fd < 0) {
            LOG(ERROR) << "Failed to create " << tmp_path << ": "
                       << strerror(errno);
            return tl::make_unexpected(ErrorCode::FILE_WRITE_FAIL);
        }

        const char* ptr = buf.data();
        size_t remaining = buf.size();
        while (remaining > 0) {
            ssize_t n = write(fd, ptr, remaining);
            if (n <= 0) {
                if (n < 0 && errno == EINTR) continue;
                LOG(ERROR) << "write failed on " << tmp_path << ": "
                           << (n < 0 ? strerror(errno) : "returned 0");
                close(fd);
                unlink(tmp_path.c_str());
                return tl::make_unexpected(ErrorCode::FILE_WRITE_FAIL);
            }
            ptr += n;
            remaining -= n;
        }
        if (step == 2) {
            close(fd);
            unlink(tmp_path.c_str());
            return tl::make_unexpected(ErrorCode::FILE_WRITE_FAIL);
        }

        // Use fsync (not fdatasync) for the metadata file: metadata is
        // small and we need inode metadata (file size) durable as well.
        if (fsync(fd) != 0) {
            LOG(ERROR) << "fsync failed on " << tmp_path;
            close(fd);
            unlink(tmp_path.c_str());
            return tl::make_unexpected(ErrorCode::FILE_WRITE_FAIL);
        }
        close(fd);

        if (rename(tmp_path.c_str(), meta_path.c_str()) != 0) {
            LOG(ERROR) << "rename " << tmp_path << " -> " << meta_path
                       << " failed: " << strerror(errno);
            unlink(tmp_path.c_str());
            return tl::make_unexpected(ErrorCode::FILE_WRITE_FAIL);
        }
        if (step == 3) return tl::make_unexpected(ErrorCode::FILE_WRITE_FAIL);

        // fsync parent directory
        fs::path parent = fs::path(meta_path).parent_path();
        if (parent.empty()) parent = ".";
        int dir_fd = open(parent.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
        if (dir_fd < 0) {
            LOG(ERROR) << "open parent dir failed: " << strerror(errno);
            return tl::make_unexpected(ErrorCode::FILE_WRITE_FAIL);
        }
        FdGuard dir_guard(dir_fd);
        if (fsync(dir_fd) != 0) {
            LOG(ERROR) << "fsync parent dir failed: " << strerror(errno);
            return tl::make_unexpected(ErrorCode::FILE_WRITE_FAIL);
        }

        dirty_guard.disarm();

        last_save_metadata_cost_us_.store(
            std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::steady_clock::now() - t_start)
                .count(),
            std::memory_order_relaxed);

        return {};
    } catch (const std::exception& e) {
        // std::bad_alloc or other exception during serialization.
        // DirtyGuard restores metadata_dirty_ on scope exit.
        LOG(ERROR) << "SaveMetadata failed with exception: " << e.what();
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }
}

//-----------------------------------------------------------------------------
// LoadMetadata
//-----------------------------------------------------------------------------

tl::expected<OffsetAllocatorPersistedMetadata, ErrorCode>
OffsetAllocatorStorageBackend::LoadMetadata() {
    std::string meta_path = GetMetaFilePath();

    int fd = open(meta_path.c_str(), O_RDONLY | O_CLOEXEC);
    if (fd < 0) {
        LOG(ERROR) << "Failed to open " << meta_path << ": " << strerror(errno);
        return tl::make_unexpected(ErrorCode::FILE_OPEN_FAIL);
    }
    FdGuard closer(fd);

    struct stat st;
    if (fstat(fd, &st) != 0) {
        LOG(ERROR) << "fstat failed on " << meta_path << ": "
                   << strerror(errno);
        return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
    }
    if (st.st_size == 0) {
        LOG(ERROR) << "Metadata file is empty: " << meta_path;
        return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
    }

    std::string buf(static_cast<size_t>(st.st_size), '\0');
    size_t read_bytes = 0;
    char* ptr = buf.data();
    while (read_bytes < static_cast<size_t>(st.st_size)) {
        ssize_t n = pread(fd, ptr, static_cast<size_t>(st.st_size) - read_bytes,
                          static_cast<off_t>(read_bytes));
        if (n < 0) {
            if (errno == EINTR) continue;
            LOG(ERROR) << "Read failed from " << meta_path << ": "
                       << strerror(errno);
            return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
        }
        if (n == 0) {
            LOG(ERROR) << "Unexpected EOF from " << meta_path;
            return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
        }
        read_bytes += static_cast<size_t>(n);
        ptr += n;
    }

    OffsetAllocatorPersistedMetadata meta;
    try {
        struct_pb::from_pb(meta, buf);
    } catch (const std::exception& e) {
        LOG(ERROR) << "struct_pb::from_pb failed for " << meta_path << ": "
                   << e.what();
        return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
    }

    if (meta.version != 1) {
        LOG(ERROR) << "Unsupported metadata version " << meta.version << " in "
                   << meta_path;
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (meta.allocator_state.empty()) {
        LOG(ERROR) << "Empty allocator_state in " << meta_path;
        return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
    }

    return meta;
}

//-----------------------------------------------------------------------------
// RebuildShardMapsFromAllocator
//-----------------------------------------------------------------------------

void OffsetAllocatorStorageBackend::RebuildShardMapsFromAllocator() {
    const int64_t kMaxKeyLen = 1 * 1024 * 1024;
    int64_t rebuilt = 0;
    int64_t skipped = 0;

    // Lock all shards during rebuild (Init is single-threaded, but
    // locking documents the invariant).
    std::vector<std::unique_ptr<SharedMutexLocker>> shard_locks;
    shard_locks.reserve(kNumShards);
    for (size_t i = 0; i < kNumShards; ++i) {
        shard_locks.emplace_back(
            std::make_unique<SharedMutexLocker>(&shards_[i].mutex));
    }
    total_size_.store(0, std::memory_order_relaxed);
    total_keys_.store(0, std::memory_order_relaxed);

    allocator_->visit_used_nodes([&](uint64_t real_offset, uint64_t alloc_size,
                                     uint32_t node_index) {
        // Helper: free a corrupt/unrecoverable node via RAII.
        // Creates a temporary handle with requested_size=0 to avoid
        // m_allocated_size underflow, then lets it go out of scope.
        auto free_leaked_node = [&]() {
            auto h = allocator_->createHandleAtNode(node_index, real_offset,
                                                    /*requested_size=*/0);
        };

        if (real_offset + RecordHeader::SIZE >
            static_cast<uint64_t>(capacity_)) {
            LOG(ERROR) << "[Recover] offset " << real_offset
                       << " + header exceeds capacity " << capacity_;
            free_leaked_node();
            ++skipped;
            return;
        }

        char hdr_buf[RecordHeader::SIZE];
        iovec hdr_iov = {hdr_buf, sizeof(hdr_buf)};
        auto hdr_res = data_file_->vector_read(&hdr_iov, 1, real_offset);
        if (!hdr_res || hdr_res.value() != RecordHeader::SIZE) {
            LOG(ERROR) << "[Recover] Failed to read header at " << real_offset;
            free_leaked_node();
            ++skipped;
            return;
        }
        RecordHeader header{};
        std::memcpy(&header.key_len, hdr_buf, sizeof(header.key_len));
        std::memcpy(&header.value_len, hdr_buf + sizeof(header.key_len),
                    sizeof(header.value_len));

        if (header.key_len > kMaxKeyLen) {
            LOG(ERROR) << "[Recover] Invalid key_len " << header.key_len
                       << " at " << real_offset;
            free_leaked_node();
            ++skipped;
            return;
        }

        uint64_t record_size =
            RecordHeader::SIZE + header.key_len + header.value_len;
        if (record_size > alloc_size) {
            LOG(ERROR) << "[Recover] record_size " << record_size
                       << " > alloc_size " << alloc_size << " at "
                       << real_offset;
            free_leaked_node();
            ++skipped;
            return;
        }
        if (record_size >
            static_cast<uint64_t>(std::numeric_limits<uint32_t>::max())) {
            LOG(ERROR) << "[Recover] record_size " << record_size
                       << " exceeds uint32_t at " << real_offset;
            free_leaked_node();
            ++skipped;
            return;
        }
        if (real_offset + record_size > static_cast<uint64_t>(capacity_)) {
            LOG(ERROR) << "[Recover] record past capacity at " << real_offset;
            free_leaked_node();
            ++skipped;
            return;
        }

        std::string key(header.key_len, '\0');
        iovec key_iov = {key.data(), static_cast<size_t>(header.key_len)};
        auto key_res = data_file_->vector_read(
            &key_iov, 1, real_offset + RecordHeader::SIZE);
        if (!key_res ||
            key_res.value() != static_cast<size_t>(header.key_len)) {
            LOG(ERROR) << "[Recover] Failed to read key at " << real_offset;
            free_leaked_node();
            ++skipped;
            return;
        }

        auto handle = allocator_->createHandleAtNode(node_index, real_offset,
                                                     record_size);
        if (!handle.has_value()) {
            LOG(ERROR) << "[Recover] createHandleAtNode failed for " << key
                       << " at " << real_offset;
            free_leaked_node();
            ++skipped;
            return;
        }

        auto allocation_ptr = std::make_shared<RefCountedAllocationHandle>(
            std::move(handle.value()));

        size_t shard_idx = ShardForKey(key);
        auto [it, inserted] = shards_[shard_idx].map.emplace(
            key, ObjectEntry(real_offset, static_cast<uint32_t>(record_size),
                             header.value_len, std::move(allocation_ptr),
                             /*fifo_seq=*/0));

        if (!inserted) {
            LOG(ERROR) << "[Recover] Duplicate key " << key << " at "
                       << real_offset << " (node_idx=" << node_index
                       << " existing_offset=" << it->second.offset
                       << ") -- kept existing, freed duplicate (RAII)";
            ++skipped;
            return;
        }

        total_size_.fetch_add(static_cast<int64_t>(record_size),
                              std::memory_order_relaxed);
        total_keys_.fetch_add(1, std::memory_order_relaxed);
        ++rebuilt;
    });

    LOG(INFO) << "[Recover] RebuildShardMaps: " << rebuilt << " keys rebuilt, "
              << skipped << " skipped (corrupt)";
    if (rebuilt == 0) {
        LOG(WARNING) << "[Recover] No records recovered -- data file "
                        "may be empty or corrupt";
    }
}

//-----------------------------------------------------------------------------
// RestoreAndRepairFifoIndex
//-----------------------------------------------------------------------------

void OffsetAllocatorStorageBackend::RestoreAndRepairFifoIndex(
    const OffsetAllocatorPersistedMetadata& meta) {
    uint64_t max_seq = meta.insert_seq;
    for (const auto& [seq, _] : meta.fifo_entries) {
        max_seq = std::max(max_seq, seq);
    }
    insert_seq_.store(max_seq + 1, std::memory_order_relaxed);

    {
        MutexLocker ev(&eviction_mutex_);
        fifo_index_.clear();
        std::unordered_set<std::string> fifo_keys;

        // Phase A: restore from persisted entries
        for (const auto& [seq, key] : meta.fifo_entries) {
            if (seq >= meta.insert_seq) {
                LOG(WARNING) << "[Recover] seq " << seq << " >= insert_seq "
                             << meta.insert_seq << " -- skipping";
                continue;
            }
            size_t shard_idx = ShardForKey(key);
            SharedMutexLocker lk(&shards_[shard_idx].mutex);
            auto it = shards_[shard_idx].map.find(key);
            if (it == shards_[shard_idx].map.end()) {
                LOG(WARNING) << "[Recover] key " << key
                             << " not in shard map -- skipping";
                continue;
            }
            if (fifo_index_.count(seq)) {
                LOG(WARNING) << "[Recover] Duplicate seq " << seq
                             << " -- deferred to Phase B";
                continue;
            }
            fifo_index_[seq] = key;
            fifo_keys.insert(key);
            it->second.fifo_seq = seq;
        }

        // Phase B: repair -- assign new seq to any shard-map entry
        // not yet in the FIFO index
        for (size_t s = 0; s < kNumShards; ++s) {
            SharedMutexLocker lk(&shards_[s].mutex);
            for (auto& [key, entry] : shards_[s].map) {
                if (!fifo_keys.count(key)) {
                    uint64_t new_seq =
                        insert_seq_.fetch_add(1, std::memory_order_relaxed);
                    entry.fifo_seq = new_seq;
                    fifo_index_[new_seq] = key;
                    fifo_keys.insert(key);
                }
            }
        }

        // Phase C: delete evicted keys that were resurrected
        for (const auto& key : meta.evicted_keys_this_batch) {
            size_t shard_idx = ShardForKey(key);
            SharedMutexLocker lk(&shards_[shard_idx].mutex);
            auto it = shards_[shard_idx].map.find(key);
            if (it != shards_[shard_idx].map.end()) {
                total_size_.fetch_sub(
                    static_cast<int64_t>(it->second.total_size),
                    std::memory_order_relaxed);
                total_keys_.fetch_sub(1, std::memory_order_relaxed);
                fifo_index_.erase(it->second.fifo_seq);
                shards_[shard_idx].map.erase(it);
            }
        }
    }

    LOG(INFO) << "[Recover] FIFO index restored: " << fifo_index_.size()
              << " entries";
}

//-----------------------------------------------------------------------------
// TryRecoverFromMetadata
//-----------------------------------------------------------------------------

bool OffsetAllocatorStorageBackend::TryRecoverFromMetadata() {
    try {
        namespace fs = std::filesystem;

        struct FallbackGuard {
            std::atomic<int64_t>& ctr;
            bool disarmed = false;
            FallbackGuard(std::atomic<int64_t>& c) : ctr(c) {}
            ~FallbackGuard() {
                if (!disarmed) ctr.fetch_add(1, std::memory_order_relaxed);
            }
            void disarm() { disarmed = true; }
        } fallback_guard(metadata_load_fallbacks_);

        std::string meta_path = GetMetaFilePath();
        std::string tmp_path = meta_path + ".tmp";
        std::error_code ec_tmp, ec_meta;

        // Clean up stale tmp
        if (fs::exists(tmp_path, ec_tmp)) {
            if (!fs::exists(meta_path, ec_meta)) {
                fs::remove(tmp_path, ec_tmp);
                LOG(INFO) << "Orphaned .meta.tmp cleaned up";
                fallback_guard.disarm();
                return false;
            }
            fs::remove(tmp_path, ec_tmp);
        }

        if (!fs::exists(meta_path, ec_meta)) {
            fallback_guard.disarm();
            return false;
        }

        LOG(INFO) << "Recovery path: " << meta_path << " found";

        // Load metadata
        auto meta_result = LoadMetadata();
        if (!meta_result) {
            LOG(ERROR) << "LoadMetadata failed, falling back to fresh start";
            return false;
        }
        OffsetAllocatorPersistedMetadata meta = std::move(meta_result.value());

        // Deserialize allocator into local shared_ptr; only move to
        // member on full success
        std::shared_ptr<offset_allocator::OffsetAllocator> alloc;
        try {
            const auto* data = reinterpret_cast<const SerializedByte*>(
                meta.allocator_state.data());
            std::vector<SerializedByte> alloc_buf(
                data, data + meta.allocator_state.size());
            alloc =
                deserialize_from<offset_allocator::OffsetAllocator>(alloc_buf);
        } catch (const std::exception& e) {
            LOG(ERROR) << "Allocator deserialization threw: " << e.what()
                       << " -- fresh start";
            return false;
        }
        if (!alloc) {
            LOG(ERROR) << "Allocator deserialization returned null"
                          " -- fresh start";
            return false;
        }

        // Verify capacity match
        {
            auto metrics = alloc->get_metrics();
            if (static_cast<uint64_t>(metrics.capacity) != capacity_) {
                LOG(WARNING) << "Allocator capacity mismatch: serialized "
                             << metrics.capacity << " vs current " << capacity_
                             << ". total_size_limit may have changed; "
                                "falling back to fresh start.";
                return false;
            }
        }

        // Open data file without truncation
        data_file_path_ = GetDataFilePath();
        int flags = O_CLOEXEC | O_RDWR;
        int raw_fd = open(data_file_path_.c_str(), flags, 0644);
        if (raw_fd < 0) {
            LOG(ERROR) << "Failed to open data file: " << data_file_path_;
            return false;
        }
        FdGuard fd_guard(raw_fd);

        // Verify data file size matches capacity_
        struct stat st;
        if (fstat(fd_guard.get(), &st) != 0) {
            LOG(ERROR) << "fstat failed on data file: " << strerror(errno);
            return false;
        }
        if (static_cast<uint64_t>(st.st_size) != capacity_) {
            LOG(WARNING) << "data file size mismatch: expected " << capacity_
                         << ", got " << st.st_size
                         << " -- falling back to fresh start";
            return false;
        }

#ifdef USE_URING
        if (file_storage_config_.use_uring) {
            data_file_ = std::make_unique<UringFile>(
                data_file_path_, fd_guard.release(), 32, true);
        } else
#endif
        {
            data_file_ = std::make_unique<PosixFile>(data_file_path_,
                                                     fd_guard.release());
        }
        data_file_->SetDeleteOnWriteFail(false);

        // Move allocator to member only after all checks pass
        allocator_ = std::move(alloc);

        // Clear and rebuild shard maps
        {
            std::vector<std::unique_ptr<SharedMutexLocker>> locks;
            locks.reserve(kNumShards);
            for (size_t i = 0; i < kNumShards; ++i) {
                locks.emplace_back(
                    std::make_unique<SharedMutexLocker>(&shards_[i].mutex));
                shards_[i].map.clear();
            }
        }
        total_size_.store(0, std::memory_order_relaxed);
        total_keys_.store(0, std::memory_order_relaxed);

        RebuildShardMapsFromAllocator();
        RestoreAndRepairFifoIndex(meta);

        fallback_guard.disarm();

        LOG(INFO) << "OffsetAllocatorStorageBackend recovered: "
                  << total_keys_.load() << " keys, " << total_size_.load()
                  << " bytes";
        return true;
    } catch (const std::exception& e) {
        LOG(ERROR) << "Recovery failed with exception: " << e.what();
        return false;
    } catch (...) {
        LOG(ERROR) << "Recovery failed with unknown exception";
        return false;
    }
}

void OffsetAllocatorStorageBackend::EvictToMakeRoom(
    int64_t required_bytes, size_t min_victims,
    const std::unordered_set<std::string>& batch_keys,
    std::vector<std::string>& out_evicted) {
    if (cfg_.eviction_policy == OffsetEvictionPolicy::NONE) return;

    MutexLocker ev(&eviction_mutex_);
    size_t n = 0;

    while (n < cfg_.max_evict_per_offload) {
        int64_t cur_size = total_size_.load(std::memory_order_relaxed);
        int64_t cur_keys = total_keys_.load(std::memory_order_relaxed);
        bool below_bytes = (cur_size + required_bytes <= low_watermark_bytes_);
        bool below_keys = (cur_keys <= low_watermark_keys_);
        // Stop when both byte and key-count watermarks are satisfied
        // and we have met the minimum victim count.
        if (below_bytes && below_keys && n >= min_victims) break;

        if (fifo_index_.empty()) break;

        auto oldest = fifo_index_.begin();
        uint64_t vseq = oldest->first;
        std::string vkey = oldest->second;

        // Skip batch_keys prefix — keys being written in this batch.
        // Do NOT erase their FIFO slots (they may fail allocate() and
        // need the slot to remain in the index for future eviction).
        // Worst-case comparison cost: O(|batch_keys_prefix|), bounded.
        while (oldest != fifo_index_.end() &&
               batch_keys.count(oldest->second)) {
            ++oldest;
        }
        if (oldest == fifo_index_.end()) break;  // all are batch_keys
        vkey = oldest->second;
        vseq = oldest->first;

        size_t shard_idx = ShardForKey(vkey);
        auto& shard = shards_[shard_idx];
        {
            SharedMutexLocker lk(&shard.mutex);
            auto it = shard.map.find(vkey);
            if (it == shard.map.end() || it->second.fifo_seq != vseq) {
                // Orphan slot in fifo_index_: the key is no longer in
                // shard.map, or its fifo_seq was replaced by a newer
                // overwrite.  Overwrites are cleaned up in BatchOffload
                // Step-4 (fifo_index_.erase(old_seq) under the lock),
                // so today this branch is only reachable if a future
                // per-key delete path neglects to also erase from
                // fifo_index_.  Keep the lazy-repair as a defense.
                fifo_index_.erase(oldest);
                metadata_dirty_.store(true, std::memory_order_relaxed);
                ++n;  // counted toward scan budget
                continue;
            }
            // Defensive assertions against double-evict underflow.
            // Precondition: single heartbeat_thread_ serialises offload;
            // if concurrent offload is added, these must be re-evaluated.
            DCHECK_GE(total_size_.load(std::memory_order_relaxed),
                      it->second.total_size);
            DCHECK_GE(total_keys_.load(std::memory_order_relaxed), 1);
            total_size_.fetch_sub(it->second.total_size,
                                  std::memory_order_relaxed);
            total_keys_.fetch_sub(1, std::memory_order_relaxed);
            shard.map.erase(it);
            if (cfg_.persist_mode != OffsetPersistMode::kDisabled) {
                all_evicted_this_batch_.insert(vkey);
            }
            metadata_dirty_.store(true, std::memory_order_relaxed);
        }
        fifo_index_.erase(oldest);
        out_evicted.push_back(std::move(vkey));
        ++n;
    }
}

//-----------------------------------------------------------------------------

tl::expected<int64_t, ErrorCode> OffsetAllocatorStorageBackend::BatchOffload(
    const std::unordered_map<std::string, std::vector<Slice>>& batch_object,
    std::function<ErrorCode(const std::vector<std::string>& keys,
                            std::vector<StorageObjectMetadata>& metadatas)>
        complete_handler,
    EvictionHandler eviction_handler) {
    // ================================================================
    // SINGLE-WRITER PRECONDITION
    //
    // BatchOffload, EvictToMakeRoom, and the watermark accounting on
    // total_size_ / total_keys_ assume only ONE thread calls
    // BatchOffload at a time (currently guaranteed by FileStorage's
    // single heartbeat_thread_).  The atomics make individual loads
    // and stores atomic, but the read-modify-write sequences are NOT
    // atomic across threads.  If concurrent offload is added, the
    // DCHECK_GE guards in EvictToMakeRoom must also be re-evaluated.
    // ================================================================

    if (!initialized_.load(std::memory_order_acquire)) {
        LOG(ERROR)
            << "Storage backend is not initialized. Call Init() before use.";
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }
    if (batch_object.empty()) {
        LOG(ERROR) << "BatchOffload called with empty batch";
        return tl::make_unexpected(ErrorCode::INVALID_KEY);
    }

    auto enable_offloading_res = IsEnableOffloading();
    if (!enable_offloading_res) {
        return tl::make_unexpected(enable_offloading_res.error());
    }
    if (!enable_offloading_res.value()) {
        return tl::make_unexpected(ErrorCode::KEYS_ULTRA_LIMIT);
    }

    const bool eviction_on =
        (cfg_.eviction_policy != OffsetEvictionPolicy::NONE) &&
        (eviction_handler != nullptr);

    if (cfg_.eviction_policy != OffsetEvictionPolicy::NONE &&
        eviction_handler == nullptr) {
        LOG_FIRST_N(WARNING, 1)
            << "Eviction policy is " << static_cast<int>(cfg_.eviction_policy)
            << " but eviction_handler is null; eviction is disabled. "
               "IsEnableOffloading() will still return true.";
    }

    // Build the set of keys being offloaded in this batch so that
    // EvictToMakeRoom does not evict them.
    std::unordered_set<std::string> batch_keys;
    if (eviction_on) {
        for (const auto& [k, _] : batch_object) batch_keys.insert(k);
    }

    std::vector<std::string> keys;
    std::vector<StorageObjectMetadata> metadatas;
    keys.reserve(batch_object.size());
    metadatas.reserve(batch_object.size());

    std::vector<std::string> evicted_keys;

    for (const auto& [key, slices] : batch_object) {
        if (slices.empty()) continue;

        if (test_failure_predicate_ && test_failure_predicate_(key)) {
            LOG(INFO) << "[TEST] Injecting failure for key: " << key
                      << " (test failure predicate)";
            continue;
        }

        uint64_t total_value_size = 0;
        for (const auto& slice : slices) {
            total_value_size += slice.size;
        }
        if (total_value_size > UINT32_MAX) {
            LOG(ERROR) << "Object too large for SSD offload for key: " << key
                       << ", size: " << total_value_size;
            continue;
        }
        uint32_t value_size = static_cast<uint32_t>(total_value_size);

        RecordHeader header{.key_len = static_cast<uint32_t>(key.size()),
                            .value_len = value_size};

        size_t record_size =
            RecordHeader::SIZE + header.key_len + header.value_len;

        if (record_size > UINT32_MAX) {
            LOG(ERROR) << "Record too large for key: " << key
                       << ", record_size=" << record_size;
            continue;
        }

        // ---- (A) Proactive eviction (watermark-driven) ----
        if (eviction_on) {
            int64_t cur_size = total_size_.load(std::memory_order_relaxed);
            int64_t cur_keys = total_keys_.load(std::memory_order_relaxed);
            bool over_bytes = (cur_size + static_cast<int64_t>(record_size) >
                               high_watermark_bytes_);
            bool over_keys = (cur_keys > high_watermark_keys_);
            if (over_bytes || over_keys) {
                size_t min_v = over_keys ? cfg_.fallback_evict_batch : 0;
                EvictToMakeRoom(static_cast<int64_t>(record_size), min_v,
                                batch_keys, evicted_keys);
            }
        }

        // ---- (B) Notify master + accumulate tombstone ----
        if (eviction_on && eviction_handler && !evicted_keys.empty()) {
            if (cfg_.persist_mode != OffsetPersistMode::kDisabled) {
                all_evicted_this_batch_.insert(evicted_keys.begin(),
                                               evicted_keys.end());
            }
            auto notify_result = eviction_handler(evicted_keys);
            if (!notify_result) {
                return tl::make_unexpected(notify_result.error());
            }
            evicted_keys.clear();
        }

        // ---- (C) Allocate ----
        auto allocation = allocator_->allocate(record_size);

        // ---- (D) Fallback eviction (nullopt retry loop) ----
        if (!allocation.has_value() && eviction_on) {
            uint64_t prev_largest =
                allocator_->get_metrics().largest_free_region_;
            size_t fallback_total_evicted = 0;
            const size_t kMaxFallbackEvicted = cfg_.max_evict_per_offload;

            while (!allocation.has_value() &&
                   fallback_total_evicted < kMaxFallbackEvicted) {
                size_t before = evicted_keys.size();
                EvictToMakeRoom(static_cast<int64_t>(record_size),
                                cfg_.fallback_evict_batch, batch_keys,
                                evicted_keys);
                fallback_total_evicted += (evicted_keys.size() - before);

                if (eviction_handler && !evicted_keys.empty()) {
                    if (cfg_.persist_mode != OffsetPersistMode::kDisabled) {
                        all_evicted_this_batch_.insert(evicted_keys.begin(),
                                                       evicted_keys.end());
                    }
                    auto notify_result = eviction_handler(evicted_keys);
                    if (!notify_result) {
                        return tl::make_unexpected(notify_result.error());
                    }
                    evicted_keys.clear();
                }

                uint64_t now_largest =
                    allocator_->get_metrics().largest_free_region_;
                if (before == 0)
                    break;  // evicted_keys is cleared after each notify
                if (now_largest <= prev_largest) break;
                prev_largest = now_largest;
                allocation = allocator_->allocate(record_size);
            }
        }

        if (!allocation.has_value()) {
            if (eviction_on) {
                eviction_skips_.fetch_add(1, std::memory_order_relaxed);
                LOG(WARNING) << "Skipping key after eviction attempts: " << key;
                continue;
            } else {
                LOG(ERROR) << "Failed to allocate " << record_size
                           << " bytes for key: " << key
                           << " - stopping processing";
                break;
            }
        }

        uint64_t offset = allocation->address();

        // ---- (E) Disk write ----
        std::vector<iovec> iovs;
        iovs.reserve(2 + 1 + slices.size());

        iovs.push_back(
            {const_cast<char*>(reinterpret_cast<const char*>(&header.key_len)),
             sizeof(header.key_len)});
        iovs.push_back({const_cast<char*>(
                            reinterpret_cast<const char*>(&header.value_len)),
                        sizeof(header.value_len)});

        iovs.push_back({const_cast<char*>(key.data()),
                        static_cast<size_t>(header.key_len)});

        for (const auto& slice : slices) {
            iovs.push_back({slice.ptr, slice.size});
        }

        auto write_result =
            data_file_->vector_write(iovs.data(), iovs.size(), offset);
        if (!write_result) {
            LOG(ERROR) << "Failed to write record for key: " << key
                       << ", error: " << write_result.error();
            continue;
        }
        if (write_result.value() != record_size) {
            LOG(ERROR) << "Write size mismatch for key: " << key
                       << ", expected: " << record_size
                       << ", got: " << write_result.value();
            continue;
        }

        // ---- (F) Metadata update with FIFO index maintenance ----
        {
            auto allocation_ptr = std::make_shared<RefCountedAllocationHandle>(
                std::move(allocation.value()));
            size_t shard_idx = ShardForKey(key);
            auto& shard = shards_[shard_idx];

            std::optional<MutexLocker> ev_lock;
            if (eviction_on) ev_lock.emplace(&eviction_mutex_);
            SharedMutexLocker shard_lock(&shard.mutex);

            auto it = shard.map.find(key);
            int64_t size_delta = static_cast<int64_t>(record_size);
            bool is_new_key = (it == shard.map.end());
            uint64_t seq = 0;

            if (eviction_on) {
                seq = insert_seq_.fetch_add(1, std::memory_order_relaxed);
                if (!is_new_key) {
                    size_delta -= static_cast<int64_t>(it->second.total_size);
                    fifo_index_.erase(it->second.fifo_seq);
                }
            } else if (!is_new_key) {
                size_delta -= static_cast<int64_t>(it->second.total_size);
            }

            shard.map.insert_or_assign(
                key, ObjectEntry(offset, static_cast<uint32_t>(record_size),
                                 value_size, std::move(allocation_ptr), seq));
            metadata_dirty_.store(true, std::memory_order_relaxed);
            if (eviction_on) fifo_index_.emplace(seq, key);

            total_size_.fetch_add(size_delta, std::memory_order_relaxed);
            if (is_new_key) {
                total_keys_.fetch_add(1, std::memory_order_relaxed);
            }
        }

        keys.push_back(key);
        metadatas.push_back(
            StorageObjectMetadata{0, static_cast<int64_t>(offset),
                                  static_cast<int64_t>(header.key_len),
                                  static_cast<int64_t>(value_size), ""});
    }

    // ---- Persistence barrier (kRelaxed / kStrict) ----
    // Reconstruct set of keys successfully written in this batch
    std::unordered_set<std::string> successfully_written_keys_set(keys.begin(),
                                                                  keys.end());

    const bool persist_enabled =
        (cfg_.persist_mode == OffsetPersistMode::kRelaxed ||
         cfg_.persist_mode == OffsetPersistMode::kStrict);

    // Filter stale tombstones on EVERY persist-enabled batch,
    // regardless of whether a checkpoint is triggered.  If we only
    // filter inside ShouldPersistNow(), a key evicted in batch N
    // and re-written in batch M (when no checkpoint fires) keeps
    // its tombstone in all_evicted_this_batch_.  The NEXT checkpoint
    // would then write the stale tombstone to meta, causing Phase C
    // to delete the valid re-written key on recovery.
    if (persist_enabled && !all_evicted_this_batch_.empty() &&
        !successfully_written_keys_set.empty()) {
        size_t removed = 0;
        for (const auto& k : successfully_written_keys_set) {
            removed += all_evicted_this_batch_.erase(k);
        }
        if (removed > 0) {
            LOG(INFO) << "Cleared " << removed << " stale eviction tombstones";
        }
    }

    if (persist_enabled && metadata_dirty_.load(std::memory_order_relaxed) &&
        ShouldPersistNow()) {
        // data fsync (REQUIRED: ensures metadata points to
        // durable data)
        auto sync_res = data_file_->datasync();
        if (!sync_res) {
            LOG(ERROR) << "datasync failed: "
                       << static_cast<int>(sync_res.error());
            if (cfg_.persist_mode == OffsetPersistMode::kStrict) {
                return tl::make_unexpected(sync_res.error());
            }
            // kRelaxed: skip this checkpoint, retry next interval
        } else {
            auto save_res = SaveMetadata(all_evicted_this_batch_);
            if (!save_res) {
                auto fails = metadata_consecutive_failures_.fetch_add(
                                 1, std::memory_order_relaxed) +
                             1;
                // Log every 10th consecutive failure (LOG_FIRST_N
                // counts by call-site, not by content — use LOG(WARNING)).
                if (fails % 10 == 0) {
                    LOG(WARNING) << "Checkpoint " << fails
                                 << " consecutive failures (mode="
                                 << static_cast<int>(cfg_.persist_mode)
                                 << "): " << static_cast<int>(save_res.error());
                }
                if (cfg_.persist_mode == OffsetPersistMode::kStrict) {
                    return tl::make_unexpected(save_res.error());
                }
                // kRelaxed: keep metadata_dirty_=true (DirtyGuard
                // restores it), retry next interval
            } else {
                metadata_consecutive_failures_.store(0,
                                                     std::memory_order_relaxed);
                all_evicted_this_batch_.clear();
                last_persist_time_us_.store(
                    std::chrono::duration_cast<std::chrono::microseconds>(
                        std::chrono::steady_clock::now().time_since_epoch())
                        .count(),
                    std::memory_order_relaxed);
            }
        }
    }

    // ---- Post-loop flush: notify master of any evicted keys that
    // were accumulated by the last (possibly allocate-failing) key.
    if (eviction_on && eviction_handler && !evicted_keys.empty()) {
        auto notify_result = eviction_handler(evicted_keys);
        if (!notify_result) {
            return tl::make_unexpected(notify_result.error());
        }
        evicted_keys.clear();
    }

    // ---- Complete handler ----
    if (complete_handler != nullptr && !keys.empty()) {
        auto error_code = complete_handler(keys, metadatas);
        if (error_code != ErrorCode::OK) {
            LOG(ERROR) << "Complete handler failed: " << error_code << " - "
                       << keys.size()
                       << " keys were successfully written to disk but "
                          "master was not notified. "
                       << "Master will learn about them via ScanMeta on "
                          "next restart.";
            return tl::make_unexpected(error_code);
        }
    }

    return static_cast<int64_t>(keys.size());
}

tl::expected<void, ErrorCode> OffsetAllocatorStorageBackend::BatchLoad(
    std::unordered_map<std::string, Slice>& batched_slices) {
    if (!initialized_.load(std::memory_order_acquire)) {
        LOG(ERROR)
            << "Storage backend is not initialized. Call Init() before use.";
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }

    // Step 1: Build read plan by copying metadata under shard locks
    // No locks held during actual disk I/O
    struct ReadPlan {
        std::string key;
        uint64_t offset;
        uint32_t value_size;
        AllocationPtr allocation;  // Refcounted handle keeps allocation alive
        Slice dest_slice;
    };

    std::vector<ReadPlan> read_plans;
    read_plans.reserve(batched_slices.size());

    for (const auto& [key, dest_slice] : batched_slices) {
        size_t shard_idx = ShardForKey(key);
        auto& shard = shards_[shard_idx];
        SharedMutexLocker lock(&shard.mutex, /*shared_mode=*/shared_lock);

        // Lookup entry in shard's map
        auto it = shard.map.find(key);
        if (it == shard.map.end()) {
            LOG(ERROR) << "Key not found: " << key;
            return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
        }

        const auto& entry = it->second;

        // Validate destination size matches stored value size
        if (dest_slice.size != entry.value_size) {
            LOG(ERROR) << "Size mismatch for key: " << key
                       << ", expected: " << entry.value_size
                       << ", got: " << dest_slice.size;
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }

        // Copy metadata and increment refcount on allocation
        // This keeps the physical extent alive even if key is evicted
        read_plans.push_back(
            ReadPlan{key, entry.offset, entry.value_size,
                     entry.allocation,  // shared_ptr copy, increments refcount
                     dest_slice});

        // Lock released here; allocation stays alive via shared_ptr
    }

    // Step 2: Perform disk I/O without holding any locks
    // Allocations are kept alive by shared_ptr references in read_plans
    for (const auto& plan : read_plans) {
        // Read header first
        RecordHeader header;
        iovec header_iovs[2] = {{&header.key_len, sizeof(header.key_len)},
                                {&header.value_len, sizeof(header.value_len)}};
        auto read_header_result =
            data_file_->vector_read(header_iovs, 2, plan.offset);
        if (!read_header_result) {
            LOG(ERROR) << "Failed to read header for key: " << plan.key
                       << ", error: " << read_header_result.error();
            return tl::make_unexpected(read_header_result.error());
        }

        if (read_header_result.value() != RecordHeader::SIZE) {
            LOG(ERROR) << "Header read size mismatch for key: " << plan.key;
            return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
        }

        // Validate header matches metadata
        if (!header.ValidateAgainstMetadata(plan.value_size)) {
            LOG(ERROR) << "Stored value_len mismatch for key: " << plan.key
                       << ", metadata: " << plan.value_size
                       << ", header: " << header.value_len;
            return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
        }

        // Read key from disk
        std::string stored_key(header.key_len, '\0');
        iovec key_iov = {stored_key.data(), header.key_len};
        auto read_key_result = data_file_->vector_read(
            &key_iov, 1, plan.offset + RecordHeader::SIZE);
        if (!read_key_result) {
            LOG(ERROR) << "Failed to read key for: " << plan.key
                       << ", error: " << read_key_result.error();
            return tl::make_unexpected(read_key_result.error());
        }

        if (read_key_result.value() != header.key_len) {
            LOG(ERROR) << "Key read size mismatch for: " << plan.key;
            return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
        }

        // Validate key matches expected
        auto validate_result = header.ValidateKey(plan.key, stored_key);
        if (!validate_result) {
            return tl::make_unexpected(validate_result.error());
        }

        // Read value into destination slice
        iovec value_iov = {plan.dest_slice.ptr, plan.dest_slice.size};
        auto read_value_result = data_file_->vector_read(
            &value_iov, 1, plan.offset + RecordHeader::SIZE + header.key_len);
        if (!read_value_result) {
            LOG(ERROR) << "Failed to read value for key: " << plan.key
                       << ", error: " << read_value_result.error();
            return tl::make_unexpected(read_value_result.error());
        }

        if (read_value_result.value() != header.value_len) {
            LOG(ERROR) << "Value read size mismatch for key: " << plan.key
                       << ", expected: " << header.value_len
                       << ", got: " << read_value_result.value();
            return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
        }
    }

    // read_plans destructor releases all AllocationPtr references
    // Physical extents freed only when last reference drops

    return {};
}

//-----------------------------------------------------------------------------

tl::expected<bool, ErrorCode> OffsetAllocatorStorageBackend::IsExist(
    const std::string& key) {
    if (!initialized_.load(std::memory_order_acquire)) {
        LOG(ERROR)
            << "Storage backend is not initialized. Call Init() before use.";
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }

    size_t shard_idx = ShardForKey(key);
    auto& shard = shards_[shard_idx];
    SharedMutexLocker lock(&shard.mutex, /*shared_mode=*/shared_lock);
    return shard.map.find(key) != shard.map.end();
}

//-----------------------------------------------------------------------------

tl::expected<bool, ErrorCode>
OffsetAllocatorStorageBackend::IsEnableOffloading() {
    if (!initialized_.load(std::memory_order_acquire)) {
        LOG(ERROR)
            << "Storage backend is not initialized. Call Init() before use.";
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }

    // When eviction is enabled, BatchOffload's EvictToMakeRoom is
    // responsible for making room — do not block offload here.
    if (cfg_.eviction_policy != OffsetEvictionPolicy::NONE) {
        return true;
    }

    // Eviction disabled: keep the original quota-check behavior.
    bool within_size_limit = total_size_.load(std::memory_order_relaxed) <
                             file_storage_config_.total_size_limit;
    bool within_keys_limit = total_keys_.load(std::memory_order_relaxed) <
                             file_storage_config_.total_keys_limit;
    return within_size_limit && within_keys_limit;
}

//-----------------------------------------------------------------------------

tl::expected<void, ErrorCode> OffsetAllocatorStorageBackend::ScanMeta(
    const std::function<
        ErrorCode(const std::vector<std::string>& keys,
                  std::vector<StorageObjectMetadata>& metadatas)>& handler) {
    if (!initialized_.load(std::memory_order_acquire)) {
        LOG(ERROR)
            << "Storage backend is not initialized. Call Init() before use.";
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }

    // V1: Scan only in-memory maps (no disk scanning)
    // Lock all shards to get consistent cross-shard snapshot
    std::vector<std::string> keys;
    std::vector<StorageObjectMetadata> metadatas;

    // Helper function: sends accumulated keys/metadatas to handler and clears
    // buffers Called when batch size reaches scanmeta_iterator_keys_limit to
    // avoid sending all keys at once.
    auto flush = [&]() -> tl::expected<void, ErrorCode> {
        if (keys.empty()) return {};
        auto error_code = handler(keys, metadatas);
        if (error_code != ErrorCode::OK) {
            LOG(ERROR) << "ScanMeta handler failed: " << error_code;
            return tl::make_unexpected(error_code);
        }
        keys.clear();
        metadatas.clear();
        return {};
    };

    {
        std::vector<std::unique_ptr<SharedMutexLocker>> shard_locks;
        shard_locks.reserve(kNumShards);

        // Lock all shards and estimate total size
        size_t estimated_total = 0;
        for (size_t i = 0; i < kNumShards; ++i) {
            shard_locks.emplace_back(std::make_unique<SharedMutexLocker>(
                &shards_[i].mutex, shared_lock));  // Acquire shared (read) lock
            estimated_total += shards_[i].map.size();
        }

        keys.reserve(
            std::min(estimated_total,
                     static_cast<size_t>(
                         file_storage_config_.scanmeta_iterator_keys_limit)));
        metadatas.reserve(
            std::min(estimated_total,
                     static_cast<size_t>(
                         file_storage_config_.scanmeta_iterator_keys_limit)));

        // Scan all shards, calling handler when limit is reached
        for (size_t i = 0; i < kNumShards; ++i) {
            for (const auto& [key, entry] : shards_[i].map) {
                keys.push_back(key);
                metadatas.push_back(StorageObjectMetadata{
                    0,  // bucket_id not used
                    static_cast<int64_t>(entry.offset),
                    static_cast<int64_t>(entry.total_size - RecordHeader::SIZE -
                                         entry.value_size),  // key_size
                    static_cast<int64_t>(entry.value_size), ""});

                // Call handler when batch limit is reached
                if (static_cast<int64_t>(keys.size()) >=
                    file_storage_config_.scanmeta_iterator_keys_limit) {
                    auto flush_result = flush();
                    if (!flush_result) {
                        return flush_result;
                    }
                }
            }
        }
    }  // Release all shard locks

    // Flush remaining keys
    auto flush_result = flush();
    if (!flush_result) {
        return flush_result;
    }

    return {};
}

//-----------------------------------------------------------------------------

tl::expected<std::shared_ptr<StorageBackendInterface>, ErrorCode>
CreateStorageBackend(const FileStorageConfig& config) {
    switch (config.storage_backend_type) {
        case StorageBackendType::kBucket: {
            auto bucket_backend_config = BucketBackendConfig::FromEnvironment();
            if (!bucket_backend_config.Validate()) {
                throw std::invalid_argument(
                    "Invalid StorageBackend configuration");
            }
            return std::make_shared<BucketStorageBackend>(
                config, bucket_backend_config);
        }
        case StorageBackendType::kFilePerKey: {
            auto file_per_key_backend_config =
                FilePerKeyConfig::FromEnvironment();
            if (!file_per_key_backend_config.Validate()) {
                throw std::invalid_argument(
                    "Invalid StorageBackend configuration");
            }
            return std::make_shared<StorageBackendAdaptor>(
                config, file_per_key_backend_config);
        }
        case StorageBackendType::kOffsetAllocator: {
            auto offset_backend_config =
                OffsetAllocatorBackendConfig::FromEnvironment();
            if (!offset_backend_config.Validate()) {
                throw std::invalid_argument(
                    "Invalid OffsetAllocatorBackendConfig");
            }
            return std::make_shared<OffsetAllocatorStorageBackend>(
                config, offset_backend_config);
        }
        case StorageBackendType::kDistributed: {
            auto distributed_config =
                DistributedStorageConfig::FromEnvironment();
            if (!distributed_config.Validate()) {
                throw std::invalid_argument(
                    "Invalid DistributedStorage configuration");
            }
            std::unique_ptr<FileSystemAdapter> adapter;
            if (distributed_config.fs_adapter_type == "hf3fs") {
#ifdef USE_3FS
                adapter = std::make_unique<Hf3fsAdapter>();
#else
                return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
#endif
            } else {
                return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
            }
            return std::make_shared<DistributedStorageBackend>(
                config, distributed_config, std::move(adapter));
        }
        default: {
            LOG(ERROR) << "Unsupported backend type: "
                       << static_cast<int>(config.storage_backend_type);
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
    }
}

}  // namespace mooncake
