#include "storage/distributed/posix_fs_adapter.h"

#include <dirent.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/uio.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <filesystem>

namespace mooncake {

namespace {

tl::expected<void, ErrorCode> ValidateIov(const iovec* iov, int iovcnt) {
    if (iovcnt < 0 || (iovcnt > 0 && iov == nullptr)) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    for (int i = 0; i < iovcnt; ++i) {
        if (!iov[i].iov_base && iov[i].iov_len > 0) {
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
    }
    return {};
}

ErrorCode ReadOpenError() {
    return errno == ENOENT ? ErrorCode::FILE_NOT_FOUND
                           : ErrorCode::FILE_OPEN_FAIL;
}

}  // namespace

tl::expected<void, ErrorCode> PosixFsAdapter::Init(
    const std::string& mount_path) {
    mount_path_ = mount_path;
    std::error_code ec;
    std::filesystem::create_directories(mount_path_, ec);
    if (ec) return tl::make_unexpected(ErrorCode::FILE_WRITE_FAIL);
    return {};
}

tl::expected<void, ErrorCode> PosixFsAdapter::Shutdown() { return {}; }

tl::expected<size_t, ErrorCode> PosixFsAdapter::WriteFile(
    const std::string& path, std::span<const char> data) {
    int fd =
        ::open(path.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
    if (fd < 0) return tl::make_unexpected(ErrorCode::FILE_OPEN_FAIL);

    size_t total_written = 0;
    while (total_written < data.size()) {
        ssize_t ret = ::write(fd, data.data() + total_written,
                              data.size() - total_written);
        if (ret < 0) {
            int saved_errno = errno;
            ::close(fd);
            errno = saved_errno;
            return tl::make_unexpected(ErrorCode::FILE_WRITE_FAIL);
        }
        if (ret == 0) break;
        total_written += static_cast<size_t>(ret);
    }

    if (::close(fd) != 0) {
        return tl::make_unexpected(ErrorCode::FILE_WRITE_FAIL);
    }
    if (total_written != data.size()) {
        return tl::make_unexpected(ErrorCode::FILE_WRITE_FAIL);
    }
    return total_written;
}

tl::expected<size_t, ErrorCode> PosixFsAdapter::ReadFile(
    const std::string& path, void* buf, size_t len) {
    if (!buf && len > 0) return tl::make_unexpected(ErrorCode::INVALID_PARAMS);

    int fd = ::open(path.c_str(), O_RDONLY | O_CLOEXEC);
    if (fd < 0) return tl::make_unexpected(ReadOpenError());

    size_t total_read = 0;
    char* dest = static_cast<char*>(buf);
    while (total_read < len) {
        ssize_t ret = ::read(fd, dest + total_read, len - total_read);
        if (ret < 0) {
            int saved_errno = errno;
            ::close(fd);
            errno = saved_errno;
            return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
        }
        if (ret == 0) break;
        total_read += static_cast<size_t>(ret);
    }

    if (::close(fd) != 0) {
        return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
    }
    return total_read;
}

tl::expected<size_t, ErrorCode> PosixFsAdapter::VectorWriteFile(
    const std::string& path, const iovec* iov, int iovcnt, off_t offset) {
    auto valid = ValidateIov(iov, iovcnt);
    if (!valid) return tl::make_unexpected(valid.error());
    if (offset < 0) return tl::make_unexpected(ErrorCode::INVALID_PARAMS);

    int fd =
        ::open(path.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
    if (fd < 0) return tl::make_unexpected(ErrorCode::FILE_OPEN_FAIL);
    auto result = WriteAt(fd, iov, iovcnt, offset);
    int saved_errno = errno;
    ::close(fd);
    errno = saved_errno;
    return result;
}

tl::expected<size_t, ErrorCode> PosixFsAdapter::VectorReadFile(
    const std::string& path, const iovec* iov, int iovcnt, off_t offset) {
    auto valid = ValidateIov(iov, iovcnt);
    if (!valid) return tl::make_unexpected(valid.error());
    if (offset < 0) return tl::make_unexpected(ErrorCode::INVALID_PARAMS);

    int fd = ::open(path.c_str(), O_RDONLY | O_CLOEXEC);
    if (fd < 0) return tl::make_unexpected(ReadOpenError());
    auto result = ReadAt(fd, const_cast<iovec*>(iov), iovcnt, offset);
    int saved_errno = errno;
    ::close(fd);
    errno = saved_errno;
    return result;
}

tl::expected<void, ErrorCode> PosixFsAdapter::DeleteFile(
    const std::string& path) {
    if (::unlink(path.c_str()) != 0) {
        if (errno == ENOENT)
            return tl::make_unexpected(ErrorCode::FILE_NOT_FOUND);
        return tl::make_unexpected(ErrorCode::FILE_WRITE_FAIL);
    }
    return {};
}

tl::expected<bool, ErrorCode> PosixFsAdapter::FileExists(
    const std::string& path) {
    if (::access(path.c_str(), F_OK) == 0) return true;
    if (errno == ENOENT) return false;
    return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
}

tl::expected<std::vector<std::string>, ErrorCode> PosixFsAdapter::ListFiles(
    const std::string& dir) {
    DIR* d = ::opendir(dir.c_str());
    if (!d) {
        if (errno == ENOENT)
            return tl::make_unexpected(ErrorCode::FILE_NOT_FOUND);
        return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
    }

    std::vector<std::string> result;
    while (auto* entry = ::readdir(d)) {
        std::string name = entry->d_name;
        if (name == "." || name == "..") continue;
        if (entry->d_type == DT_DIR) continue;
        if (entry->d_type == DT_UNKNOWN) {
            struct stat st;
            if (::stat((dir + "/" + name).c_str(), &st) == 0 &&
                S_ISDIR(st.st_mode)) {
                continue;
            }
        }
        result.push_back(std::move(name));
    }
    ::closedir(d);
    return result;
}

tl::expected<int, ErrorCode> PosixFsAdapter::OpenFile(const std::string& path) {
    int fd = ::open(path.c_str(), O_RDWR | O_CREAT | O_CLOEXEC, 0644);
    if (fd < 0) return tl::make_unexpected(ErrorCode::FILE_OPEN_FAIL);
    return fd;
}

tl::expected<void, ErrorCode> PosixFsAdapter::CloseFile(int fd) {
    if (fd < 0) return tl::make_unexpected(ErrorCode::FILE_INVALID_HANDLE);
    if (::close(fd) != 0) {
        return tl::make_unexpected(ErrorCode::FILE_INVALID_HANDLE);
    }
    return {};
}

tl::expected<void, ErrorCode> PosixFsAdapter::PreallocateFile(
    const std::string& path, uint64_t size) {
    int fd = ::open(path.c_str(), O_RDWR | O_CREAT | O_CLOEXEC, 0644);
    if (fd < 0) return tl::make_unexpected(ErrorCode::FILE_OPEN_FAIL);

    int rc = ::ftruncate(fd, static_cast<off_t>(size));
    int saved_errno = errno;
    ::close(fd);
    errno = saved_errno;
    if (rc != 0) return tl::make_unexpected(ErrorCode::FILE_WRITE_FAIL);
    return {};
}

tl::expected<size_t, ErrorCode> PosixFsAdapter::WriteAt(int fd,
                                                        const iovec* iov,
                                                        int iovcnt,
                                                        int64_t offset) {
    if (fd < 0 || offset < 0) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    auto valid = ValidateIov(iov, iovcnt);
    if (!valid) return tl::make_unexpected(valid.error());
    if (iovcnt == 0) return size_t{0};

    ssize_t ret = ::pwritev(fd, iov, iovcnt, static_cast<off_t>(offset));
    if (ret < 0) return tl::make_unexpected(ErrorCode::FILE_WRITE_FAIL);
    return static_cast<size_t>(ret);
}

tl::expected<size_t, ErrorCode> PosixFsAdapter::ReadAt(int fd, iovec* iov,
                                                       int iovcnt,
                                                       int64_t offset) {
    if (fd < 0 || offset < 0) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    auto valid = ValidateIov(iov, iovcnt);
    if (!valid) return tl::make_unexpected(valid.error());
    if (iovcnt == 0) return size_t{0};

    ssize_t ret = ::preadv(fd, iov, iovcnt, static_cast<off_t>(offset));
    if (ret < 0) return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
    return static_cast<size_t>(ret);
}

}  // namespace mooncake
