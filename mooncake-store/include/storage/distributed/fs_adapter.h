#pragma once

#include <sys/stat.h>
#include <sys/uio.h>

#include <span>
#include <string>
#include <vector>

#include "types.h"

namespace mooncake {

struct FileInfo {
    std::string name;
    size_t size;
};

/**
 * @brief Abstract interface for distributed filesystem adapters.
 *
 * Encapsulates file-level I/O differences across DFS implementations
 * (3FS, CephFS, JuiceFS, etc.). DistributedStorageBackend depends only
 * on this interface and is unaware of the concrete DFS.
 */
class FileSystemAdapter {
   public:
    virtual ~FileSystemAdapter() = default;

    // === File I/O ===

    virtual tl::expected<size_t, ErrorCode> WriteFile(
        const std::string& path, std::span<const char> data) = 0;

    // Read file into a pre-allocated buffer (zero-copy into Slice.ptr)
    virtual tl::expected<size_t, ErrorCode> ReadFile(const std::string& path,
                                                     void* buf, size_t len) = 0;

    virtual tl::expected<size_t, ErrorCode> VectorWriteFile(
        const std::string& path, const iovec* iov, int iovcnt,
        off_t offset) = 0;

    virtual tl::expected<size_t, ErrorCode> VectorReadFile(
        const std::string& path, const iovec* iov, int iovcnt,
        off_t offset) = 0;

    // === File management ===

    virtual tl::expected<void, ErrorCode> DeleteFile(
        const std::string& path) = 0;

    virtual tl::expected<bool, ErrorCode> FileExists(
        const std::string& path) = 0;

    virtual tl::expected<std::vector<std::string>, ErrorCode> ListFiles(
        const std::string& dir) = 0;

    virtual tl::expected<size_t, ErrorCode> GetFileSize(
        const std::string& path) {
        struct stat st;
        if (::stat(path.c_str(), &st) != 0) {
            if (errno == ENOENT) {
                return tl::make_unexpected(ErrorCode::FILE_NOT_FOUND);
            }
            return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
        }
        return static_cast<size_t>(st.st_size);
    }

    // === Batch operations (default implementations, adapters may override) ===

    virtual tl::expected<void, ErrorCode> DeleteFiles(
        const std::vector<std::string>& paths) {
        for (const auto& path : paths) {
            auto result = DeleteFile(path);
            if (!result) return result;
        }
        return {};
    }

    virtual tl::expected<std::vector<FileInfo>, ErrorCode> ListFilesWithInfo(
        const std::string& dir) {
        auto files = ListFiles(dir);
        if (!files) return tl::make_unexpected(files.error());

        std::vector<FileInfo> result;
        result.reserve(files->size());
        for (const auto& name : *files) {
            std::string full_path = dir + "/" + name;
            auto size = GetFileSize(full_path);
            if (size) {
                result.push_back({name, *size});
            } else if (size.error() != ErrorCode::FILE_NOT_FOUND) {
                return tl::make_unexpected(size.error());
            }
        }
        return result;
    }

    // === Lifecycle ===

    virtual tl::expected<void, ErrorCode> Init(
        const std::string& mount_path) = 0;

    virtual tl::expected<void, ErrorCode> Shutdown() = 0;

    // === Identity ===

    virtual const char* GetName() const = 0;
};

}  // namespace mooncake
