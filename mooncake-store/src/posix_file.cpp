#include <string>
#include <vector>
#include <sys/uio.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/file.h>
#include <cerrno>
#include <limits>

#include "file_interface.h"

namespace mooncake {
PosixFile::PosixFile(const std::string& filename, int fd) : StorageFile(filename, fd) {
    if (fd < 0) {
        error_code_ = ErrorCode::FILE_INVALID_HANDLE;
    }
}

PosixFile::~PosixFile() {
    if (fd_ >= 0) {
        if (close(fd_) != 0) {
            LOG(WARNING) << "Failed to close file: " << filename_;
        }
        // If the file was opened with an error code indicating a write failure,
        // attempt to delete the file to prevent corruption.
        if (error_code_ == ErrorCode::FILE_WRITE_FAIL) {
            if (::unlink(filename_.c_str()) == -1) {
                LOG(ERROR) << "Failed to delete corrupted file: " << filename_;
            } else {
                LOG(INFO) << "Deleted corrupted file: " << filename_;
            }
        } 
    }
    fd_ = -1;
}

ssize_t PosixFile::write(std::span<const char> data) {
    if (fd_ < 0) return -1;
    if (data.empty()) return -1;

    auto lock = acquire_write_lock();
    if (!lock.is_locked()) return -1;

    size_t written = 0;
    while (written < data.size()) {
        ssize_t ret = ::write(fd_, data.data() + written, data.size() - written);
        if (ret == -1) {
            if (errno == EINTR) continue;
            return -1;
        }
        written += ret;
    }
    return written;
}

ssize_t PosixFile::write(const std::string &buffer, size_t length) {
    if (fd_ < 0) {
        error_code_ = ErrorCode::FILE_NOT_FOUND;
        return -1;
    }
    if (length == 0) {
        error_code_ = ErrorCode::FILE_INVALID_BUFFER;
        return -1;
    }

    if (length > static_cast<size_t>(std::numeric_limits<ssize_t>::max())) {
        error_code_ = ErrorCode::FILE_INVALID_BUFFER;
        return -1;
    }

    auto lock = acquire_write_lock();
    if (!lock.is_locked()) {
        error_code_ = ErrorCode::FILE_LOCK_FAIL;
        return -1;
    }

    size_t remaining = length;
    size_t written_bytes = 0;
    const char* ptr = buffer.data();

    while (remaining > 0) {
        ssize_t written = ::write(fd_, ptr, remaining);
        if (written == -1) {
            if (errno == EINTR) continue;  
            error_code_ = ErrorCode::FILE_WRITE_FAIL;
            return -1;
        }
        remaining -= written;
        ptr += written;
        written_bytes += written;
    }

    if (remaining > 0) {
        error_code_ = ErrorCode::FILE_WRITE_FAIL;
        return -1;
    }   

    return written_bytes;
}

ssize_t PosixFile::read(std::string &buffer, size_t length) {
    if (fd_ < 0) {
        error_code_ = ErrorCode::FILE_NOT_FOUND;
        return -1;
    }
    if (length == 0) {
        error_code_ = ErrorCode::FILE_INVALID_BUFFER;
        return -1;
    }

    if (length > static_cast<size_t>(std::numeric_limits<ssize_t>::max())) {
        error_code_ = ErrorCode::FILE_INVALID_BUFFER;
        return -1;
    }

    auto lock = acquire_read_lock();
    if (!lock.is_locked()) {
        error_code_ = ErrorCode::FILE_LOCK_FAIL;
        return -1;
    }

    buffer.resize(length);
    size_t read_bytes = 0;
    char* ptr = &buffer[0];

    while (read_bytes < length) {
        ssize_t n = ::read(fd_, ptr, length - read_bytes);
        if (n == -1) {
            if (errno == EINTR) continue;  
            error_code_ = ErrorCode::FILE_READ_FAIL;
            buffer.clear();
            return -1;
        }
        if (n == 0) break;  // EOF
        read_bytes += n;
        ptr += n;
    }

    buffer.resize(read_bytes); // shrink to actual read size
    return read_bytes;
}

ssize_t PosixFile::vector_write(const iovec *iov, int iovcnt, off_t offset) {
    if (fd_ < 0) {
        error_code_ = ErrorCode::FILE_NOT_FOUND;
        return -1;
    }

    auto lock = acquire_write_lock();
    if (!lock.is_locked()) {
        error_code_ = ErrorCode::FILE_LOCK_FAIL;
        return -1;
    }

    ssize_t ret = ::pwritev(fd_, iov, iovcnt, offset);

    if (ret < 0) {
        error_code_ = ErrorCode::FILE_WRITE_FAIL;
        return -1;
    }

    return ret;
}

ssize_t PosixFile::vector_read(const iovec *iov, int iovcnt, off_t offset) {
    if (fd_ < 0) {
        error_code_ = ErrorCode::FILE_NOT_FOUND;
        return -1;
    }

    auto lock = acquire_read_lock();
    if (!lock.is_locked()) {
        error_code_ = ErrorCode::FILE_LOCK_FAIL;
        return -1;
    }

    ssize_t ret = ::preadv(fd_, iov, iovcnt, offset);

    if (ret < 0) {
        error_code_ = ErrorCode::FILE_READ_FAIL;
        return -1;
    }
    return ret;
}

} // namespace mooncake