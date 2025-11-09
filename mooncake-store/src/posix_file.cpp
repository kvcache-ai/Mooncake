#include <cerrno>
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