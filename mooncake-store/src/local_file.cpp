#include "local_file.h"

#include <fcntl.h>
#include <sys/file.h>
#include <sys/uio.h>
#include <unistd.h>

#include <fstream>
#include <string>
#include <vector>

namespace mooncake {
LocalFile::LocalFile(const std::string &filename, FILE *file, ErrorCode ec)
    : filename_(filename), file_(file), error_code_(ec) {
    if (!file_ || ferror(file_)) {
        error_code_ = ErrorCode::FILE_INVALID_HANDLE;
    } else if (ec != ErrorCode::OK) {
        error_code_ = ec;
    }
}

LocalFile::~LocalFile() {
    if (file_) {
        release_lock();
        if (fclose(file_) != 0) {
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
    file_ = nullptr;
}

ssize_t LocalFile::write(const std::string &buffer, size_t length) {
    if (file_ == nullptr) {
        error_code_ = ErrorCode::FILE_NOT_FOUND;
        return -1;
    }
    if (length == 0 || buffer.empty()) {
        error_code_ = ErrorCode::FILE_INVALID_BUFFER;
        return -1;
    }

    if (length > static_cast<size_t>(std::numeric_limits<ssize_t>::max())) {
        error_code_ = ErrorCode::FILE_INVALID_BUFFER;
        return -1;
    }

    if (acquire_write_lock() == -1) {
        error_code_ = ErrorCode::FILE_LOCK_FAIL;
        return -1;
    }

    size_t remaining = length;
    size_t written_bytes = 0;
    const char *ptr = buffer.data();

    while (remaining > 0) {
        size_t written = fwrite(ptr, 1, remaining, file_);
        if (written == 0) break;
        remaining -= written;
        ptr += written;
        written_bytes += written;
    }

    if (remaining > 0) {
        error_code_ = ErrorCode::FILE_WRITE_FAIL;
        return -1;
    }

    if (release_lock() == -1) {
        error_code_ = ErrorCode::FILE_LOCK_FAIL;
        LOG(INFO) << "Failed to release lock on file: " << filename_;
    }

    if (ferror(file_)) {
        error_code_ = ErrorCode::FILE_WRITE_FAIL;
        return -1;
    }

    return written_bytes;
}

ssize_t LocalFile::read(std::string &buffer, size_t length) {
    if (file_ == nullptr) {
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

    if (acquire_read_lock() == -1) {
        error_code_ = ErrorCode::FILE_LOCK_FAIL;
        return -1;
    }

    buffer.resize(length);
    size_t read_bytes = fread(&buffer[0], 1, length, file_);

    if (release_lock() == -1) {
        error_code_ = ErrorCode::FILE_LOCK_FAIL;
        LOG(INFO) << "Failed to release lock on file: " << filename_;
    }

    if (ferror(file_)) {
        error_code_ = ErrorCode::FILE_READ_FAIL;
        buffer.clear();
        return -1;
    }

    buffer.resize(read_bytes);  // shrink to actual read size
    return read_bytes;
}

ssize_t LocalFile::pwritev(const iovec *iov, int iovcnt, off_t offset) {
    if (!file_) {
        error_code_ = ErrorCode::FILE_NOT_FOUND;
        return -1;
    }

    int fd = fileno(file_);

    if (fd == -1) {
        error_code_ = ErrorCode::FILE_INVALID_HANDLE;
        LOG(ERROR) << "Invalid file handle for: " << filename_;
        return -1;
    }

    if (acquire_write_lock() == -1) {
        error_code_ = ErrorCode::FILE_LOCK_FAIL;
        return -1;
    }

    size_t total_length = 0;
    for (int i = 0; i < iovcnt; ++i) {
        total_length += iov[i].iov_len;
    }

    ssize_t ret = ::pwritev(fd, iov, iovcnt, offset);

    if (release_lock() == -1) {
        error_code_ = ErrorCode::FILE_LOCK_FAIL;
        LOG(INFO) << "Failed to release lock on file: " << filename_;
    }
    if (ret < 0) {
        error_code_ = ErrorCode::FILE_WRITE_FAIL;
        return -1;
    }

    return ret;
}

ssize_t LocalFile::preadv(const iovec *iov, int iovcnt, off_t offset) {
    if (!file_) {
        error_code_ = ErrorCode::FILE_NOT_FOUND;
        return -1;
    }

    int fd = fileno(file_);

    if (fd == -1) {
        error_code_ = ErrorCode::FILE_INVALID_HANDLE;
        LOG(ERROR) << "Invalid file handle for: " << filename_;
        return -1;
    }

    if (acquire_read_lock() == -1) {
        error_code_ = ErrorCode::FILE_LOCK_FAIL;
        return -1;
    }

    ssize_t ret = ::preadv(fd, iov, iovcnt, offset);

    if (release_lock() == -1) {
        error_code_ = ErrorCode::FILE_LOCK_FAIL;
        LOG(INFO) << "Failed to release lock on file: " << filename_;
    }

    if (ret < 0) {
        error_code_ = ErrorCode::FILE_READ_FAIL;
        return -1;
    }
    return ret;
}

int LocalFile::acquire_write_lock() {
    if (flock(fileno(file_), LOCK_EX) == -1) {
        return -1;
    }
    is_locked_ = true;
    return 0;
}

int LocalFile::acquire_read_lock() {
    if (flock(fileno(file_), LOCK_SH) == -1) {
        return -1;
    }
    is_locked_ = true;
    return 0;
}

int LocalFile::release_lock() {
    if (!is_locked_) return 0;
    if (flock(fileno(file_), LOCK_UN) == -1) {
        return -1;
    }
    is_locked_ = false;
    return 0;
}

}  // namespace mooncake