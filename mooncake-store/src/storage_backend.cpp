#include "storage_backend.h"

#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/uio.h>
#include <errno.h>
#include <cstring>

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

    config.enable_eviction =
        GetEnvOr<bool>("ENABLE_EVICTION", config.enable_eviction);

    return config;
}

BucketBackendConfig BucketBackendConfig::FromEnvironment() {
    BucketBackendConfig config;

    config.bucket_keys_limit = GetEnvOr<int64_t>(
        "MOONCAKE_OFFLOAD_BUCKET_KEYS_LIMIT", config.bucket_keys_limit);

    config.bucket_size_limit = GetEnvOr<int64_t>(
        "MOONCAKE_OFFLOAD_BUCKET_SIZE_LIMIT_BYTES", config.bucket_size_limit);

    return config;
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

bool StorageBackend::IsEvictionEnabled() const {
    // First check user configuration
    if (!enable_eviction_) {
        return false;
    }

#ifdef USE_3FS
    // Eviction is only enabled for local storage, not for 3FS
    return !is_3fs_dir_;
#else
    // If 3FS is not compiled in, eviction is enabled if user config allows
    return true;
#endif
}

tl::expected<void, ErrorCode> StorageBackend::Init(uint64_t quota_bytes = 0) {
    // Skip eviction initialization for 3FS mode
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
            // Only enable eviction for local storage, not for 3FS
            if (IsEvictionEnabled()) {
                eviction_needed = true;
                available_space_ = -1;
                LOG(WARNING)
                    << "Existing used space (" << used_space_
                    << ") exceeds the new quota (" << total_space_
                    << "). Eviction will be triggered after initial setup.";
            } else {
                // For 3FS mode, just log a warning but don't trigger eviction
                LOG(WARNING) << "Existing used space (" << used_space_
                             << ") exceeds the new quota (" << total_space_
                             << "). Eviction is disabled for 3FS mode.";
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
    const std::string& key) {
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

        auto space_result = EnsureDiskSpace(total_size);
        if (!space_result) {
            return tl::make_unexpected(space_result.error());
        }
        evicted_keys = std::move(space_result.value());
        reserved_size = total_size;
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
    const std::string& path, const std::string& str, const std::string& key) {
    return StoreObject(path, std::span<const char>(str.data(), str.size()), key);
}

tl::expected<std::vector<std::string>, ErrorCode> StorageBackend::StoreObject(
    const std::string& path, std::span<const char> data,
    const std::string& key) {
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

        auto space_result = EnsureDiskSpace(file_total_size);
        if (!space_result) {
            return tl::make_unexpected(space_result.error());
        }
        evicted_keys = std::move(space_result.value());
        reserved_size = file_total_size;
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

    // For 3FS mode, use original logic (no queue tracking)
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

    // For 3FS mode, use original logic (no queue tracking)
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
            std::error_code ec;
            if (fs::remove(path, ec)) {
                VLOG(1) << "Removed file by regex: " << path;
            } else {
                LOG(ERROR) << "Failed to delete file: " << path
                           << ", error: " << ec.message();
            }
        }

        return;
    }

    // Eviction-enabled logic (local mode)
    std::list<FileRecord> records_to_remove;
    uint64_t total_freed_space = 0;
    {
        std::shared_lock<std::shared_mutex> lock(file_queue_mutex_);
        for (const auto& record : file_write_queue_) {
            std::string filename = fs::path(record.path).filename().string();
            if (std::regex_search(filename, pattern)) {
                records_to_remove.push_back(record);
            }
        }
    }

    for (const auto& record : records_to_remove) {
        std::error_code ec;
        if (fs::remove(record.path, ec)) {
            RemoveFileFromWriteQueue(record.path);
            total_freed_space += record.size;
            VLOG(1) << "Removed file by regex: " << record.path;
        } else {
            if (ec && ec == std::errc::no_such_file_or_directory) {
                RemoveFileFromWriteQueue(record.path);
                total_freed_space += record.size;
            } else {
                LOG(ERROR) << "Failed to delete file: " << record.path
                           << ", error: " << ec.message();
            }
        }
    }

    ReleaseSpace(total_freed_space);

    return;
}

void StorageBackend::RemoveAll() {
    namespace fs = std::filesystem;

    // For 3FS mode, use original logic (no queue tracking)
    if (!IsEvictionEnabled()) {
        // Iterate through the root directory and remove all files
        for (const auto& entry : fs::directory_iterator(root_dir_)) {
            if (fs::is_regular_file(entry.status())) {
                std::error_code ec;
                fs::remove(entry.path(), ec);
                if (ec) {
                    LOG(ERROR) << "Failed to delete file: " << entry.path()
                               << ", error: " << ec.message();
                }
            }
        }
        return;
    }

    // Eviction-enabled logic (local mode)
    try {
        std::list<FileRecord> records_to_remove;
        {
            std::unique_lock<std::shared_mutex> lock(file_queue_mutex_);
            records_to_remove = std::move(file_write_queue_);
            file_queue_map_.clear();
        }
        uint64_t total_freed_space = 0;
        for (const auto& record : records_to_remove) {
            total_freed_space += record.size;
            std::error_code ec;
            fs::remove(record.path, ec);
            if (ec && ec != std::errc::no_such_file_or_directory) {
                LOG(ERROR) << "RemoveAll: Failed to delete file " << record.path
                           << ", error: " << ec.message();
            }
        }

        ReleaseSpace(total_freed_space);

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

    int fd = open(path.c_str(), flags | access_mode, 0644);
    if (fd < 0) {
        return nullptr;
    }

#ifdef USE_3FS
    if (is_3fs_dir_) {
        if (hf3fs_reg_fd(fd, 0) > 0) {
            close(fd);
            return nullptr;
        }
        return resource_manager_ ? std::make_unique<ThreeFSFile>(
                                       path, fd, resource_manager_.get())
                                 : nullptr;
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
        LOG(WARNING)
            << "Eviction is disabled for 3FS mode. Cannot evict files.";
        return {};
    }

    // Use FIFO based strategy (earliest written first out)
    FileRecord record_to_evict = SelectFileToEvictByFIFO();

    if (record_to_evict.path.empty()) {
        LOG(WARNING) << "No file selected for eviction";
        return {};
    }

    namespace fs = std::filesystem;
    std::error_code ec;
    uint64_t file_size = record_to_evict.size;

    if (fs::remove(record_to_evict.path, ec)) {
        RemoveFileFromWriteQueue(record_to_evict.path);
        ReleaseSpace(file_size);
        return record_to_evict;
    } else {
        if (!ec || ec == std::errc::no_such_file_or_directory) {
            RemoveFileFromWriteQueue(record_to_evict.path);
            return record_to_evict;
        } else {
            LOG(ERROR) << "Failed to evict file: " << record_to_evict.path
                       << ", error: " << ec.message();
            RemoveFileFromWriteQueue(record_to_evict.path);
            return {};
        }
    }
}

void StorageBackend::AddFileToWriteQueue(const std::string& path,
                                         uint64_t size,
                                         const std::string& key) {
    std::unique_lock<std::shared_mutex> lock(file_queue_mutex_);

    auto it = file_queue_map_.find(path);
    if (it != file_queue_map_.end()) {
        file_write_queue_.erase(it->second);
        file_queue_map_.erase(it);
    }

    file_write_queue_.push_back({path, size, key});
    file_queue_map_[path] = std::prev(file_write_queue_.end());
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

FileRecord StorageBackend::SelectFileToEvictByFIFO() {
    std::unique_lock<std::shared_mutex> lock(file_queue_mutex_);
    if (file_write_queue_.empty()) {
        LOG(WARNING) << "Queue is empty, cannot select file to evict";
        return {};
    }

    return file_write_queue_.front();
}

tl::expected<std::vector<std::string>, ErrorCode> StorageBackend::EnsureDiskSpace(
    size_t required_size) {
    std::vector<std::string> evicted_keys;
    // If eviction is disabled (3FS mode), skip space checking and eviction
    // Let 3FS filesystem handle space management itself
    if (!IsEvictionEnabled()) {
        return evicted_keys;
    }

    const size_t kMaxEvictionAttempts = 1000;
    size_t attempts = 0;

    bool space_reserved = CheckDiskSpace(required_size);

    while (!space_reserved && attempts < kMaxEvictionAttempts) {
        FileRecord evicted = EvictFile();
        if (evicted.path.empty()) {
            LOG(ERROR) << "Failed to evict file to make space.";
            return tl::make_unexpected(ErrorCode::FILE_WRITE_FAIL);
        }
        if (!evicted.key.empty()) {
            evicted_keys.push_back(evicted.key);
        }
        attempts++;

        space_reserved = CheckDiskSpace(required_size);
    }

    if (!space_reserved) {
        LOG(ERROR) << "Still insufficient disk space after evicting files.";
        return tl::make_unexpected(ErrorCode::FILE_WRITE_FAIL);
    }

    return evicted_keys;
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
    auto init_result = storage_backend_->Init();
    if (!init_result) {
        LOG(ERROR) << "Failed to init storage backend";
        return init_result;
    }
    return {};
}

std::string StorageBackendAdaptor::SanitizeKey(const std::string& key) const {
    // Set of invalid filesystem characters to be replaced
    constexpr std::string_view kInvalidChars = "/\\:*?\"<>|";
    std::string sanitized_key;
    sanitized_key.reserve(key.size());

    for (char c : key) {
        // Replace invalid characters with underscore
        sanitized_key.push_back(
            kInvalidChars.find(c) != std::string_view::npos ? '_' : c);
    }
    return sanitized_key;
}

std::string StorageBackendAdaptor::ResolvePath(const std::string& key) const {
    // Compute hash of the key
    size_t hash = std::hash<std::string>{}(key);

    // Use low 8 bits to create 2-level directory structure (e.g. "a1/b2")
    char dir1 =
        static_cast<char>('a' + (hash & 0x0F));  // Lower 4 bits -> 16 dirs
    char dir2 = static_cast<char>(
        'a' + ((hash >> 4) & 0x0F));  // Next 4 bits -> 16 subdirs

    // Safely construct path using std::filesystem
    namespace fs = std::filesystem;
    fs::path dir_path = fs::path(std::string(1, dir1)) / std::string(1, dir2);

    // Combine directory path with sanitized filename
    fs::path full_path = fs::path(file_storage_config_.storage_filepath) /
                         file_per_key_config_.fsdir / dir_path /
                         SanitizeKey(key);

    return full_path.lexically_normal().string();
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
    std::function<void(const std::string& evicted_key)> eviction_handler) {
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

        auto path = ResolvePath(kv.key);
        kv.value = ConcatSlicesToString(value);

        std::string kv_buf;
        struct_pb::to_pb(kv, kv_buf);
        auto store_result = storage_backend_->StoreObject(path, kv_buf, kv.key);
        if (!store_result) {
            LOG(ERROR) << "Failed to store object for key: " << kv.key
                       << ", error: " << store_result.error()
                       << " - continuing with remaining keys";
            continue;  // Continue processing other keys
        }

        // Notify eviction handler about any evicted keys
        if (eviction_handler) {
            for (const auto& evicted_key : store_result.value()) {
                eviction_handler(evicted_key);
            }
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

tl::expected<bool, ErrorCode> StorageBackendAdaptor::IsExist(
    const std::string& key) {
    auto path = ResolvePath(key);
    namespace fs = std::filesystem;
    return fs::exists(path);
}

tl::expected<void, ErrorCode> StorageBackendAdaptor::BatchLoad(
    const std::unordered_map<std::string, Slice>& batched_slices) {
    for (const auto& [key, slice] : batched_slices) {
        KVEntry kv;
        kv.key = key;
        auto path = ResolvePath(kv.key);

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
        meta_scanned_.store(true, std::memory_order_acquire);
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

    meta_scanned_.store(true, std::memory_order_acquire);
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
      bucket_backend_config_(bucket_backend_config_) {}

tl::expected<int64_t, ErrorCode> BucketStorageBackend::BatchOffload(
    const std::unordered_map<std::string, std::vector<Slice>>& batch_object,
    std::function<ErrorCode(const std::vector<std::string>& keys,
                            std::vector<StorageObjectMetadata>& metadatas)>
        complete_handler,
    std::function<void(const std::string& evicted_key)> /*eviction_handler*/) {
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
    auto write_bucket_result = WriteBucket(bucket_id, bucket, iovs);
    if (!write_bucket_result) {
        LOG(ERROR) << "Failed to write bucket with id: " << bucket_id;
        return tl::make_unexpected(write_bucket_result.error());
    }
    if (complete_handler != nullptr) {
        auto error_code = complete_handler(bucket->keys, metadatas);
        if (error_code != ErrorCode::OK) {
            LOG(ERROR) << "Complete handler failed: " << error_code
                       << ", Key count: " << bucket->keys.size()
                       << ", Bucket id: " << bucket_id;
            return tl::make_unexpected(error_code);
        }
    }

    // Commit to metadata maps under exclusive lock
    // Check for duplicate keys and rollback if any found
    SharedMutexLocker lock(&mutex_);

    // Pre-check for duplicates before modifying any state
    for (const auto& key : bucket->keys) {
        if (object_bucket_map_.find(key) != object_bucket_map_.end()) {
            LOG(WARNING) << "Duplicate key detected in BatchOffload: " << key
                         << ", bucket_id=" << bucket_id
                         << ". Returning OBJECT_ALREADY_EXISTS.";
            // Release lock and cleanup the orphaned bucket files
            lock.unlock();
            CleanupOrphanedBucket(bucket_id);
            return tl::make_unexpected(ErrorCode::OBJECT_ALREADY_EXISTS);
        }
    }

    // No duplicates found, safe to commit
    total_size_ += bucket->data_size + bucket->meta_size;
    object_bucket_map_.reserve(object_bucket_map_.size() + bucket->keys.size());
    for (size_t i = 0; i < bucket->keys.size(); ++i) {
        // Use insert instead of emplace to be explicit about not overwriting
        auto [it, inserted] = object_bucket_map_.insert(
            {bucket->keys[i], std::move(metadatas[i])});
        if (!inserted) {
            LOG(ERROR) << "Unexpected duplicate key after pre-check: "
                       << bucket->keys[i] << ", bucket_id=" << bucket_id;
        }
    }
    buckets_.emplace(bucket_id, std::move(bucket));
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
    const std::unordered_map<std::string, Slice>& batch_object) {
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
            // Read value (skip key in file: offset + key_size)
            iovec iov{plan.dest_slice.ptr, plan.dest_slice.size};
            auto read_res =
                file->vector_read(&iov, 1, plan.offset + plan.key_size);

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
        total_size_ = 0;
        int64_t max_bucket_id = BucketIdGenerator::INIT_NEW_START_ID;
        for (const auto& entry :
             fs::recursive_directory_iterator(storage_path_)) {
            if (entry.is_regular_file() &&
                entry.path().extension() == BUCKET_METADATA_FILE_SUFFIX) {
                const auto& bucket_id_str = entry.path().stem();
                int64_t bucket_id = std::stoll(bucket_id_str);
                auto [metadata_it, success] = buckets_.try_emplace(
                    bucket_id, std::make_shared<BucketMetadata>());
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
            int64_t bucket_id = std::stoll(bucket_id_str);

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
    auto bucket_id_it = object_bucket_map_.find(key);
    if (bucket_id_it != object_bucket_map_.end()) {
        return true;
    }
    return false;
}

tl::expected<bool, ErrorCode> BucketStorageBackend::IsEnableOffloading() {
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
                LOG(ERROR) << "Object size exceeds bucket size limit: "
                           << "key=" << it->first
                           << ", object_size=" << it->second << ", limit="
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

    int fd = open(path.c_str(), flags | access_mode, 0644);
    if (fd < 0) {
        return tl::make_unexpected(ErrorCode::FILE_OPEN_FAIL);
    }
    return std::make_unique<PosixFile>(path, fd);
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

// ============================================================================
// OffsetAllocatorStorageBackend Implementation
// ============================================================================

OffsetAllocatorStorageBackend::OffsetAllocatorStorageBackend(
    const FileStorageConfig& file_storage_config_)
    : StorageBackendInterface(file_storage_config_),
      storage_path_(file_storage_config_.storage_filepath) {
    capacity_ = file_storage_config_.total_size_limit;
}

std::string OffsetAllocatorStorageBackend::GetDataFilePath() const {
    return (std::filesystem::path(storage_path_) / "kv_cache.data").string();
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

        // Clear in-memory maps (V1: no persistence, start fresh)
        // Lock all shards to ensure exclusive access during initialization
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

        // RAII wrapper to ensure fd is closed on all error paths
        struct FdGuard {
            int fd;
            explicit FdGuard(int fd) : fd(fd) {}
            ~FdGuard() {
                if (fd >= 0) {
                    close(fd);
                }
            }
            // Release ownership (caller takes responsibility)
            int release() {
                int ret = fd;
                fd = -1;
                return ret;
            }
            // Get fd without releasing (for operations)
            int get() const { return fd; }
        };

        // Open/truncate data file in read-write mode
        // We need raw fd for fallocate, so open directly
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

        // Release fd to PosixFile (PosixFile takes ownership and will close it)
        data_file_ =
            std::make_unique<PosixFile>(data_file_path_, fd_guard.release());

        // Create allocator with base=0, size=capacity
        allocator_ = offset_allocator::OffsetAllocator::create(0, capacity_);
        if (!allocator_) {
            LOG(ERROR) << "Failed to create OffsetAllocator";
            return tl::make_unexpected(ErrorCode::FILE_WRITE_FAIL);
        }

        initialized_.store(true, std::memory_order_release);
        LOG(INFO) << "OffsetAllocatorStorageBackend initialized, capacity: "
                  << capacity_ << " bytes, data file: " << data_file_path_;
    } catch (const std::exception& e) {
        LOG(ERROR) << "OffsetAllocatorStorageBackend initialize error: "
                   << e.what();
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }

    return {};
}

//-----------------------------------------------------------------------------

tl::expected<int64_t, ErrorCode> OffsetAllocatorStorageBackend::BatchOffload(
    const std::unordered_map<std::string, std::vector<Slice>>& batch_object,
    std::function<ErrorCode(const std::vector<std::string>& keys,
                            std::vector<StorageObjectMetadata>& metadatas)>
        complete_handler,
    std::function<void(const std::string& evicted_key)> /*eviction_handler*/) {
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

    std::vector<std::string> keys;
    std::vector<StorageObjectMetadata> metadatas;
    keys.reserve(batch_object.size());
    metadatas.reserve(batch_object.size());

    // Process each object in the batch; continue on individual failures to
    // support partial success
    for (const auto& [key, slices] : batch_object) {
        if (slices.empty()) {
            // Skip empty slices (empty values are allowed but not stored)
            continue;
        }

        // Test-only: Check if this key should fail (deterministic failure
        // injection)
        if (test_failure_predicate_ && test_failure_predicate_(key)) {
            LOG(INFO) << "[TEST] Injecting failure for key: " << key
                      << " (test failure predicate)";
            continue;  // Simulate allocation/write failure
        }

        // Calculate total value size
        uint32_t value_size = 0;
        for (const auto& slice : slices) {
            value_size += static_cast<uint32_t>(slice.size);
        }

        // Prepare record header
        RecordHeader header{.key_len = static_cast<uint32_t>(key.size()),
                            .value_len = value_size};

        // Use size_t for record_size to handle large objects (up to 4GB per
        // RecordHeader)
        size_t record_size =
            RecordHeader::SIZE + header.key_len + header.value_len;

        // Step 1: Allocate space (allocator is thread-safe, ensures unique
        // offsets) No locks held during allocation
        auto allocation = allocator_->allocate(record_size);
        if (!allocation.has_value()) {
            LOG(ERROR) << "Failed to allocate " << record_size
                       << " bytes for key: " << key
                       << " - stopping processing for this batch";
            break;  // Stop processing other keys as space is likely exhausted
        }

        uint64_t offset = allocation->address();

        // Step 2: Write data to disk (no metadata locks held during I/O)
        std::vector<iovec> iovs;
        iovs.reserve(2 + slices.size());

        // Header
        iovs.push_back(
            {const_cast<char*>(reinterpret_cast<const char*>(&header.key_len)),
             sizeof(header.key_len)});
        iovs.push_back({const_cast<char*>(
                            reinterpret_cast<const char*>(&header.value_len)),
                        sizeof(header.value_len)});

        // Key
        iovs.push_back({const_cast<char*>(key.data()),
                        static_cast<size_t>(header.key_len)});

        // Value slices
        for (const auto& slice : slices) {
            iovs.push_back({slice.ptr, slice.size});
        }

        auto write_result =
            data_file_->vector_write(iovs.data(), iovs.size(), offset);
        if (!write_result) {
            LOG(ERROR) << "Failed to write record for key: " << key
                       << ", error: " << write_result.error()
                       << " - continuing with remaining keys";
            // Allocation handle is still local (not yet stored in the metadata
            // map) and will be freed automatically when going out of scope.
            continue;  // Continue processing other keys
        }

        // Handle the case where the data was written partially.
        size_t written = write_result.value();
        if (written != record_size) {
            LOG(ERROR) << "Write size mismatch for key: " << key
                       << ", expected: " << record_size << ", got: " << written
                       << " - continuing with remaining keys";
            continue;  // Continue processing other keys
        }

        // Step 3: Wrap allocation in refcounted handle
        auto allocation_ptr = std::make_shared<RefCountedAllocationHandle>(
            std::move(allocation.value()));

        // Step 4: Update metadata map under exclusive shard lock
        // Lock only the shard for this key (other shards can proceed in
        // parallel)
        {
            size_t shard_idx = ShardForKey(key);
            auto& shard = shards_[shard_idx];
            SharedMutexLocker lock(&shard.mutex);

            // Check if key exists to update size accounting
            auto it = shard.map.find(key);
            int64_t size_delta = static_cast<int64_t>(record_size);
            bool is_new_key = (it == shard.map.end());

            if (!is_new_key) {
                // Overwrite: subtract old size
                size_delta -= static_cast<int64_t>(it->second.total_size);
                // Old AllocationPtr will be dropped, refcount decremented
                // Physical extent freed when last reader releases it
            }

            // Update map (insert_or_assign handles both insert and overwrite)
            shard.map.insert_or_assign(
                key, ObjectEntry(offset, record_size, value_size,
                                 std::move(allocation_ptr)));

            // Update total size atomically (lock-free, separate from map
            // updates)
            total_size_.fetch_add(size_delta, std::memory_order_relaxed);

            // Update total keys only if inserting a new key
            if (is_new_key) {
                total_keys_.fetch_add(1, std::memory_order_relaxed);
            }
        }

        keys.push_back(key);
        metadatas.push_back(StorageObjectMetadata{
            0,  // bucket_id not used for this backend
            static_cast<int64_t>(offset), static_cast<int64_t>(header.key_len),
            static_cast<int64_t>(value_size), ""});
    }

    // Invoke complete handler only if we have successful keys to report
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

//-----------------------------------------------------------------------------

tl::expected<void, ErrorCode> OffsetAllocatorStorageBackend::BatchLoad(
    const std::unordered_map<std::string, Slice>& batched_slices) {
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

    // TODO: See if free space check is needed here.
    // Check quota limits only (atomic counters, completely lock-free!)
    bool within_size_limit = total_size_.load(std::memory_order_relaxed) <
                             file_storage_config_.total_size_limit;

    // Check keys limit (atomic counter maintained during BatchOffload)
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
            return std::make_shared<OffsetAllocatorStorageBackend>(config);
        }
        default: {
            LOG(FATAL) << "Unsupported backend type";
            return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
        }
    }
}

}  // namespace mooncake
