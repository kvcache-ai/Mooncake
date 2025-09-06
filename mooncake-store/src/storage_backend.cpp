#include "storage_backend.h"

#include <fcntl.h>
#include <unistd.h>
#include <sys/uio.h>
#include <string>
#include <vector>
#include <regex>

namespace mooncake {

tl::expected<void, ErrorCode> StorageBackend::StoreObject(
    const std::string& path, const std::vector<Slice>& slices) {
    ResolvePath(path);
    auto file = create_file(path, FileMode::Write);
    if (!file) {
        LOG(INFO) << "Failed to open file for writing: " << path;
        return tl::make_unexpected(ErrorCode::FILE_OPEN_FAIL);
    }

    std::vector<iovec> iovs;
    size_t slices_total_size = 0;
    for (const auto& slice : slices) {
        iovec io{slice.ptr, slice.size};
        iovs.push_back(io);
        slices_total_size += slice.size;
    }

    auto write_result =
        file->vector_write(iovs.data(), static_cast<int>(iovs.size()), 0);
    if (!write_result) {
        LOG(INFO) << "vector_write failed for: " << path
                  << ", error: " << write_result.error();
        return tl::make_unexpected(write_result.error());
    }

    if (*write_result != slices_total_size) {
        LOG(INFO) << "Write size mismatch for: " << path
                  << ", expected: " << slices_total_size
                  << ", got: " << *write_result;
        return tl::make_unexpected(ErrorCode::FILE_WRITE_FAIL);
    }

    return {};
}

tl::expected<void, ErrorCode> StorageBackend::StoreObject(
    const std::string& path, const std::string& str) {
    return StoreObject(path, std::span<const char>(str.data(), str.size()));
}

tl::expected<void, ErrorCode> StorageBackend::StoreObject(
    const std::string& path, std::span<const char> data) {
    ResolvePath(path);
    auto file = create_file(path, FileMode::Write);
    if (!file) {
        LOG(INFO) << "Failed to open file for writing: " << path;
        return tl::make_unexpected(ErrorCode::FILE_OPEN_FAIL);
    }

    size_t file_total_size = data.size();
    auto write_result = file->write(data, file_total_size);

    if (!write_result) {
        LOG(INFO) << "Write failed for: " << path
                  << ", error: " << write_result.error();
        return tl::make_unexpected(write_result.error());
    }
    if (*write_result != file_total_size) {
        LOG(INFO) << "Write size mismatch for: " << path
                  << ", expected: " << file_total_size
                  << ", got: " << *write_result;
        return tl::make_unexpected(ErrorCode::FILE_WRITE_FAIL);
    }

    return {};
}

tl::expected<void, ErrorCode> StorageBackend::LoadObject(
    const std::string& path, std::vector<Slice>& slices, size_t length) {
    ResolvePath(path);
    auto file = create_file(path, FileMode::Read);
    if (!file) {
        LOG(INFO) << "Failed to open file for reading: " << path;
        return tl::make_unexpected(ErrorCode::FILE_OPEN_FAIL);
    }

    off_t current_offset = 0;
    size_t total_bytes_processed = 0;

    std::vector<iovec> iovs_chunk;
    off_t chunk_start_offset = 0;
    size_t chunk_length = 0;

    auto process_chunk = [&]() -> tl::expected<void, ErrorCode> {
        if (iovs_chunk.empty()) {
            return {};
        }

        auto read_result = file->vector_read(
            iovs_chunk.data(), static_cast<int>(iovs_chunk.size()),
            chunk_start_offset);
        if (!read_result) {
            LOG(INFO) << "vector_read failed for chunk at offset "
                      << chunk_start_offset << " for path: " << path
                      << ", error: " << read_result.error();
            return tl::make_unexpected(read_result.error());
        }
        if (*read_result != chunk_length) {
            LOG(INFO) << "Read size mismatch for chunk in path: " << path
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
        LOG(INFO) << "Total read size mismatch for: " << path
                  << ", expected: " << length
                  << ", got: " << total_bytes_processed;
        return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
    }

    return {};
}

tl::expected<void, ErrorCode> StorageBackend::LoadObject(
    const std::string& path, std::string& str, size_t length) {
    ResolvePath(path);
    auto file = create_file(path, FileMode::Read);
    if (!file) {
        LOG(INFO) << "Failed to open file for reading: " << path;
        return tl::make_unexpected(ErrorCode::FILE_OPEN_FAIL);
    }

    auto read_result = file->read(str, length);
    if (!read_result) {
        LOG(INFO) << "read failed for: " << path
                  << ", error: " << read_result.error();
        return tl::make_unexpected(read_result.error());
    }
    if (*read_result != length) {
        LOG(INFO) << "Read size mismatch for: " << path
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
    if (fs::exists(path)) {
        std::error_code ec;
        fs::remove(path, ec);
        if (ec) {
            LOG(ERROR) << "Failed to delete file: " << path
                       << ", error: " << ec.message();
        }
    }
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

    fs::path storage_root = fs::path(root_dir_) / fsdir_;
    if (!fs::exists(storage_root) || !fs::is_directory(storage_root)) {
        LOG(WARNING) << "Storage root directory does not exist: "
                     << storage_root;
        return;
    }

    std::vector<fs::path> paths_to_remove;

    for (const auto& entry : fs::recursive_directory_iterator(storage_root)) {
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

void StorageBackend::RemoveAll() {
    namespace fs = std::filesystem;
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
            LOG(INFO) << "Failed to create directories: " << parent_path
                      << ", error: " << ec.message();
        }
    }
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

}  // namespace mooncake
