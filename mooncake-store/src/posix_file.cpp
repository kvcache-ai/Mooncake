#include <cerrno>
#include <fcntl.h>
#include <string>
#include <sys/uio.h>
#include <unistd.h>
#include <glog/logging.h>

#include "file_interface.h"

namespace mooncake {
PosixFile::PosixFile(const std::string &filename, int fd)
    : StorageFile(filename, fd) {
    if (fd < 0) {
        error_code_ = ErrorCode::FILE_INVALID_HANDLE;
    }
}

std::atomic<int> StorageFile::sync_failure_for_test_{
    static_cast<int>(StorageFile::SyncKind::kNone)};

tl::expected<void, ErrorCode> StorageFile::writeback_wait() {
    if (ConsumeSyncFailureForTest(SyncKind::kWritebackWait)) {
        return tl::make_unexpected(ErrorCode::FILE_WRITE_FAIL);
    }
    if (fd_ < 0) {
        return tl::make_unexpected(ErrorCode::FILE_INVALID_HANDLE);
    }
    // Offsets 0/0 mean "the whole file". WAIT_BEFORE|WRITE|WAIT_AFTER is the
    // combination sync_file_range(2) documents for writing out a range and
    // waiting for it; without WAIT_BEFORE the kernel runs the writeback in
    // WB_SYNC_NONE mode, which skips a folio that is already under writeback
    // rather than waiting for it, so a page re-dirtied concurrently could be
    // left behind.
    if (sync_file_range(fd_, 0, 0,
                        SYNC_FILE_RANGE_WAIT_BEFORE | SYNC_FILE_RANGE_WRITE |
                            SYNC_FILE_RANGE_WAIT_AFTER) != 0) {
        const int err = errno;
        LOG(ERROR) << "sync_file_range failed for " << filename_ << ": "
                   << strerror(err);
        return tl::make_unexpected(ErrorCode::FILE_WRITE_FAIL);
    }
    return {};
}

tl::expected<void, ErrorCode> PosixFile::datasync() {
    if (ConsumeSyncFailureForTest(SyncKind::kDatasync)) {
        return tl::make_unexpected(ErrorCode::FILE_WRITE_FAIL);
    }
    if (fdatasync(fd_) != 0) {
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

    ssize_t ret = ::pwritev(fd_, iov, iovcnt, offset);
    if (ret < 0) {
        return make_error<size_t>(ErrorCode::FILE_WRITE_FAIL);
    }

    return ret;
}

tl::expected<size_t, ErrorCode> PosixFile::vector_read(const iovec *iov,
                                                       int iovcnt,
                                                       off_t offset) {
    if (fd_ < 0) {
        return make_error<size_t>(ErrorCode::FILE_NOT_FOUND);
    }

    ssize_t ret = ::preadv(fd_, iov, iovcnt, offset);
    if (ret < 0) {
        return make_error<size_t>(ErrorCode::FILE_READ_FAIL);
    }

    return ret;
}

}  // namespace mooncake
