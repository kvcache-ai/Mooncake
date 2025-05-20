#include "local_file.h"

#include <string>
#include <vector>

#include "types.h"

namespace mooncake {

using namespace std;

LocalFile::LocalFile(const string &file_name, FILE *file, ErrorCode ec)
    : file_name_(file_name), file_(file), error_code_(ec) {}

LocalFile::~LocalFile() {
    if (file_) {
        fclose(file_);
        file_ = nullptr;
    }
}

ssize_t LocalFile::read(void *buffer, size_t length) {
    if (!file_) {
        error_code_ = ErrorCode::BAD_FILE;
        return -1;
    }

    if (length > static_cast<size_t>(std::numeric_limits<ssize_t>::max())) {
        error_code_ = ErrorCode::BAD_ARGS;
        return -1;
    }

    size_t bytes_remaining = length;
    size_t bytes_read = 0;
    while (bytes_remaining > 0) {
        size_t ret = fread(reinterpret_cast<char*>(buffer) + bytes_read,
                           1, bytes_remaining, file_);
        if (ret == 0) {
            if (feof(file_)) {
                break;
            }
            if (ferror(file_)) {
                error_code_ = ErrorCode::BAD_ARGS;
                return -1;
            }
        }
        bytes_read += ret;
        bytes_remaining -= ret;
    }

    return static_cast<ssize_t>(bytes_read);
}

ssize_t LocalFile::write(const void *buffer, size_t length) {
    if (!file_) {
        error_code_ = ErrorCode::BAD_FILE;
        return -1;
    }

    if (length > static_cast<size_t>(std::numeric_limits<ssize_t>::max())) {
        error_code_ = ErrorCode::BAD_ARGS;
        return -1;
    }

    size_t bytes_remaining = length;
    size_t bytes_written = 0;
    while (bytes_remaining > 0) {
        size_t ret = fwrite(
            reinterpret_cast<const char*>(buffer) + bytes_written,
            1, bytes_remaining, file_);
        if (ret == 0) {
            if (ferror(file_)) {
                // TODO: convert errno into ErrorCode
                error_code_ = ErrorCode::BAD_FILE;
                return -1;
            }
            break;
        }
        bytes_written += ret;
        bytes_remaining -= ret;
    }

    return static_cast<ssize_t>(bytes_written);
}

ssize_t LocalFile::preadv(const iovec *iov, int iovcnt, off_t offset) {
    if (!file_) {
        error_code_ = ErrorCode::BAD_FILE;
        return -1;
    }

    int fd = fileno(file_);
    ssize_t ret = ::preadv(fd, iov, iovcnt, offset);
    if (ret == -1) {
        // TODO: convert errno into ErrorCode
        error_code_ = ErrorCode::BAD_FILE;
    }
    return ret;
}

ssize_t LocalFile::pwritev(const iovec *iov, int iovcnt, off_t offset) {
    if (!file_) {
        error_code_ = ErrorCode::BAD_FILE;
        return -1;
    }

    if (iov == nullptr || iovcnt <= 0) {
        error_code_ = ErrorCode::BAD_FILE;
        return -1;
    }

    int fd = fileno(file_);
    auto ret = ::pwritev(fd, iov, iovcnt, offset);
    if (ret == -1) {
        // TODO: convert errno into ErrorCode
        error_code_ = ErrorCode::BAD_FILE;
    }
    return ret;
}

}  // namespace mooncake
