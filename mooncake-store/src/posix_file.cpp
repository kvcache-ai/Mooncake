#include <algorithm>
#include <cerrno>
#include <string>
#include <sys/uio.h>
#include <unistd.h>
#include <vector>
#include <glog/logging.h>

#include "file_interface.h"
#include "iovec_cursor.h"

namespace mooncake {

using detail::ConsumeIovecs;
using detail::SkipEmptyIovecs;

PosixFile::PosixFile(const std::string &filename, int fd)
    : StorageFile(filename, fd) {
    if (fd < 0) {
        error_code_ = ErrorCode::FILE_INVALID_HANDLE;
    }
}

tl::expected<void, ErrorCode> PosixFile::datasync() {
    if (fdatasync(fd_) != 0) {
        record_sys_errno(errno);
        LOG(ERROR) << "fdatasync failed: " << strerror(errno);
        return tl::make_unexpected(ErrorCode::FILE_WRITE_FAIL);
    }
    return {};
}

PosixFile::~PosixFile() {
    if (fd_ >= 0) {
        if (close(fd_) != 0) {
            LOG(WARNING) << "Failed to close file: " << filename_;
        }
        // If the file was opened with an error code indicating a write failure,
        // attempt to delete the file to prevent corruption.
        if (delete_on_write_fail_ &&
            error_code_ == ErrorCode::FILE_WRITE_FAIL) {
            if (::unlink(filename_.c_str()) == -1) {
                LOG(ERROR) << "Failed to delete corrupted file: " << filename_;
            } else {
                LOG(INFO) << "Deleted corrupted file: " << filename_;
            }
        }
    }
    fd_ = -1;
}

tl::expected<size_t, ErrorCode> PosixFile::write(const std::string &buffer,
                                                 size_t length) {
    return write(std::span<const char>(buffer.data(), length), length);
}

tl::expected<size_t, ErrorCode> PosixFile::write(std::span<const char> data,
                                                 size_t length) {
    if (fd_ < 0) {
        return make_error<size_t>(ErrorCode::FILE_NOT_FOUND);
    }

    if (length == 0) {
        return make_error<size_t>(ErrorCode::FILE_INVALID_BUFFER);
    }

    size_t remaining = length;
    size_t written_bytes = 0;
    const char *ptr = data.data();

    while (remaining > 0) {
        ssize_t written = ::write(fd_, ptr, remaining);
        if (written == -1) {
            if (errno == EINTR) continue;
            record_sys_errno(errno);
            return make_error<size_t>(ErrorCode::FILE_WRITE_FAIL);
        }
        remaining -= written;
        ptr += written;
        written_bytes += written;
    }

    if (written_bytes != length) {
        return make_error<size_t>(ErrorCode::FILE_WRITE_FAIL);
    }
    return written_bytes;
}

tl::expected<size_t, ErrorCode> PosixFile::read(std::string &buffer,
                                                size_t length) {
    if (fd_ < 0) {
        return make_error<size_t>(ErrorCode::FILE_NOT_FOUND);
    }

    if (length == 0) {
        return make_error<size_t>(ErrorCode::FILE_INVALID_BUFFER);
    }

    buffer.resize(length);
    size_t read_bytes = 0;
    char *ptr = buffer.data();

    while (read_bytes < length) {
        ssize_t n = ::read(fd_, ptr, length - read_bytes);
        if (n == -1) {
            if (errno == EINTR) continue;
            record_sys_errno(errno);
            buffer.clear();
            return make_error<size_t>(ErrorCode::FILE_READ_FAIL);
        }
        if (n == 0) break;  // EOF
        read_bytes += n;
        ptr += n;
    }

    buffer.resize(read_bytes);
    if (read_bytes != length) {
        return make_error<size_t>(ErrorCode::FILE_READ_FAIL);
    }
    return read_bytes;
}

tl::expected<size_t, ErrorCode> PosixFile::vector_write(const iovec *iov,
                                                        int iovcnt,
                                                        off_t offset) {
    if (fd_ < 0) {
        return make_error<size_t>(ErrorCode::FILE_NOT_FOUND);
    }
    if (iovcnt < 0) {
        return make_error<size_t>(ErrorCode::FILE_INVALID_BUFFER);
    }

    // A short pwritev is not an error (e.g. the filesystem shut down in the
    // middle of the write): write the rest, and if the device is failing the
    // next call reports why with errno.
    std::vector<iovec> rest(iov, iov + iovcnt);
    size_t first = 0;
    size_t written = 0;
    while (SkipEmptyIovecs(rest, first)) {
        ssize_t ret = ::pwritev(fd_, rest.data() + first,
                                static_cast<int>(rest.size() - first),
                                offset + static_cast<off_t>(written));
        if (ret < 0) {
            if (errno == EINTR) continue;
            record_sys_errno(errno);
            return make_error<size_t>(ErrorCode::FILE_WRITE_FAIL);
        }
        // Writing nothing and reporting no error would loop forever.
        if (ret == 0) {
            return make_error<size_t>(ErrorCode::FILE_WRITE_FAIL);
        }
        written += static_cast<size_t>(ret);
        ConsumeIovecs(rest, first, static_cast<size_t>(ret));
    }
    return written;
}

tl::expected<size_t, ErrorCode> PosixFile::vector_read(const iovec *iov,
                                                       int iovcnt,
                                                       off_t offset) {
    if (fd_ < 0) {
        return make_error<size_t>(ErrorCode::FILE_NOT_FOUND);
    }
    if (iovcnt < 0) {
        return make_error<size_t>(ErrorCode::FILE_INVALID_BUFFER);
    }

    // Read until the iovecs are full or EOF. A short preadv before EOF (e.g.
    // a bad block) is not an error; the next call reports it with errno.
    // Returning fewer bytes than asked therefore always means EOF.
    std::vector<iovec> rest(iov, iov + iovcnt);
    size_t first = 0;
    size_t read_bytes = 0;
    while (SkipEmptyIovecs(rest, first)) {
        ssize_t ret = ::preadv(fd_, rest.data() + first,
                               static_cast<int>(rest.size() - first),
                               offset + static_cast<off_t>(read_bytes));
        if (ret < 0) {
            if (errno == EINTR) continue;
            record_sys_errno(errno);
            return make_error<size_t>(ErrorCode::FILE_READ_FAIL);
        }
        if (ret == 0) break;  // EOF
        read_bytes += static_cast<size_t>(ret);
        ConsumeIovecs(rest, first, static_cast<size_t>(ret));
    }
    return read_bytes;
}

}  // namespace mooncake
