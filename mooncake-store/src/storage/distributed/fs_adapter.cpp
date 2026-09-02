#include "storage/distributed/fs_adapter.h"

#include <fcntl.h>
#include <unistd.h>

#include <cerrno>
#include <filesystem>

namespace mooncake {
namespace {

tl::expected<void, ErrorCode> WriteAll(int fd, std::span<const char> data) {
    size_t written = 0;
    while (written < data.size()) {
        const ssize_t n =
            ::write(fd, data.data() + written, data.size() - written);
        if (n < 0 && errno == EINTR) continue;
        if (n <= 0) {
            return tl::make_unexpected(ErrorCode::FILE_WRITE_FAIL);
        }
        written += static_cast<size_t>(n);
    }
    return {};
}

tl::expected<void, ErrorCode> SyncParentDirectory(const std::string& path) {
    std::filesystem::path parent = std::filesystem::path(path).parent_path();
    if (parent.empty()) parent = ".";
    const int fd = ::open(parent.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    if (fd < 0) return tl::make_unexpected(ErrorCode::FILE_WRITE_FAIL);
    const int sync_result = ::fsync(fd);
    const int saved_errno = errno;
    const int close_result = ::close(fd);
    errno = saved_errno;
    if (sync_result != 0 || close_result != 0) {
        return tl::make_unexpected(ErrorCode::FILE_WRITE_FAIL);
    }
    return {};
}

}  // namespace

tl::expected<void, ErrorCode> FileSystemAdapter::AtomicWriteFile(
    const std::string& path, std::span<const char> data) {
    const std::string tmp_path = path + ".tmp";
    const int fd = ::open(tmp_path.c_str(),
                          O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
    if (fd < 0) return tl::make_unexpected(ErrorCode::FILE_WRITE_FAIL);

    auto result = WriteAll(fd, data);
    if (result && ::fsync(fd) != 0) {
        result = tl::make_unexpected(ErrorCode::FILE_WRITE_FAIL);
    }
    const int saved_errno = errno;
    const int close_result = ::close(fd);
    errno = saved_errno;
    if (!result || close_result != 0) {
        ::unlink(tmp_path.c_str());
        return tl::make_unexpected(ErrorCode::FILE_WRITE_FAIL);
    }
    if (::rename(tmp_path.c_str(), path.c_str()) != 0) {
        const int rename_errno = errno;
        ::unlink(tmp_path.c_str());
        errno = rename_errno;
        return tl::make_unexpected(ErrorCode::FILE_WRITE_FAIL);
    }
    return SyncParentDirectory(path);
}

tl::expected<void, ErrorCode> FileSystemAdapter::AppendAndSyncFile(
    const std::string& path, std::span<const char> data) {
    const int fd =
        ::open(path.c_str(), O_WRONLY | O_CREAT | O_APPEND | O_CLOEXEC, 0644);
    if (fd < 0) return tl::make_unexpected(ErrorCode::FILE_WRITE_FAIL);
    auto result = WriteAll(fd, data);
    if (result && ::fsync(fd) != 0) {
        result = tl::make_unexpected(ErrorCode::FILE_WRITE_FAIL);
    }
    const int saved_errno = errno;
    const int close_result = ::close(fd);
    errno = saved_errno;
    if (!result || close_result != 0) {
        return tl::make_unexpected(ErrorCode::FILE_WRITE_FAIL);
    }
    return {};
}

}  // namespace mooncake
