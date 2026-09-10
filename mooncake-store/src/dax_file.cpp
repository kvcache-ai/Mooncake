// Copyright 2026 KVCache.AI

#include "dax_file.h"

#include <fcntl.h>
#include <glog/logging.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>

namespace mooncake {

DaxFile::DaxFile(const std::string &path, int fd, void *base, size_t size)
    : StorageFile(path, fd), base_(base), size_(size) {
    // Never unlink the backing path on a failed write: for a character
    // device that would remove the device node itself.
    delete_on_write_fail_ = false;
}

tl::expected<std::shared_ptr<DaxFile>, ErrorCode> DaxFile::Open(
    const std::string &path, size_t size) {
    if (path.empty() || size == 0) {
        LOG(ERROR) << "DaxFile: path must be non-empty and size > 0";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    int fd = ::open(path.c_str(), O_RDWR | O_CLOEXEC);
    if (fd < 0) {
        LOG(ERROR) << "DaxFile: failed to open " << path << ": "
                   << strerror(errno);
        return tl::make_unexpected(ErrorCode::FILE_OPEN_FAIL);
    }

    struct stat st{};
    if (::fstat(fd, &st) != 0) {
        LOG(ERROR) << "DaxFile: fstat failed on " << path << ": "
                   << strerror(errno);
        ::close(fd);
        return tl::make_unexpected(ErrorCode::FILE_OPEN_FAIL);
    }
    if (S_ISREG(st.st_mode) && static_cast<uint64_t>(st.st_size) < size) {
        if (::ftruncate(fd, static_cast<off_t>(size)) != 0) {
            LOG(ERROR) << "DaxFile: failed to grow " << path << " to " << size
                       << " bytes: " << strerror(errno);
            ::close(fd);
            return tl::make_unexpected(ErrorCode::FILE_OPEN_FAIL);
        }
    }

    void *base =
        ::mmap(nullptr, size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (base == MAP_FAILED) {
        LOG(ERROR) << "DaxFile: mmap of " << size << " bytes from " << path
                   << " failed: " << strerror(errno)
                   << " (device-DAX requires the length to be a multiple of "
                      "the device alignment, typically 2 MiB)";
        ::close(fd);
        return tl::make_unexpected(ErrorCode::FILE_OPEN_FAIL);
    }

    return std::shared_ptr<DaxFile>(new DaxFile(path, fd, base, size));
}

DaxFile::~DaxFile() {
    if (base_ != nullptr && ::munmap(base_, size_) != 0) {
        LOG(WARNING) << "DaxFile: munmap failed for " << filename_ << ": "
                     << strerror(errno);
    }
    base_ = nullptr;
    if (fd_ >= 0 && ::close(fd_) != 0) {
        LOG(WARNING) << "DaxFile: failed to close " << filename_;
    }
    fd_ = -1;
}

tl::expected<void, ErrorCode> DaxFile::datasync() {
    if (base_ == nullptr) {
        return make_error<void>(ErrorCode::FILE_NOT_FOUND);
    }
    // ponytail: msync covers fsdax and regular files. Device-DAX chardevs
    // have no fsync op, so the kernel answers EINVAL; there is no page cache
    // to flush and stores already landed in device memory. Add CLWB+SFENCE
    // here if crash-consistent PMEM durability is ever required.
    if (::msync(base_, size_, MS_SYNC) != 0) {
        if (errno == EINVAL) {
            LOG_FIRST_N(INFO, 1) << "DaxFile: msync unsupported on "
                                 << filename_ << "; treating as no-op";
            return {};
        }
        LOG(ERROR) << "DaxFile: msync failed for " << filename_ << ": "
                   << strerror(errno);
        return make_error<void>(ErrorCode::FILE_WRITE_FAIL);
    }
    return {};
}

tl::expected<size_t, ErrorCode> DaxFile::write(const std::string &, size_t) {
    return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
}

tl::expected<size_t, ErrorCode> DaxFile::write(std::span<const char>, size_t) {
    return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
}

tl::expected<size_t, ErrorCode> DaxFile::read(std::string &, size_t) {
    return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
}

bool DaxFile::InRange(off_t offset, size_t length) const {
    if (offset < 0) return false;
    const auto start = static_cast<uint64_t>(offset);
    return start <= size_ && length <= size_ - start;
}

tl::expected<size_t, ErrorCode> DaxFile::vector_write(const iovec *iov,
                                                      int iovcnt,
                                                      off_t offset) {
    if (base_ == nullptr) {
        return make_error<size_t>(ErrorCode::FILE_NOT_FOUND);
    }
    if (iov == nullptr || iovcnt < 0) {
        return make_error<size_t>(ErrorCode::FILE_INVALID_BUFFER);
    }
    size_t total = 0;
    for (int i = 0; i < iovcnt; ++i) total += iov[i].iov_len;
    if (!InRange(offset, total)) {
        LOG(ERROR) << "DaxFile: write of " << total << " bytes at offset "
                   << offset << " exceeds mapping of " << size_ << " bytes";
        return make_error<size_t>(ErrorCode::FILE_WRITE_FAIL);
    }
    char *dst = static_cast<char *>(base_) + offset;
    for (int i = 0; i < iovcnt; ++i) {
        if (iov[i].iov_len > 0) {
            std::memcpy(dst, iov[i].iov_base, iov[i].iov_len);
            dst += iov[i].iov_len;
        }
    }
    return total;
}

tl::expected<size_t, ErrorCode> DaxFile::vector_read(const iovec *iov,
                                                     int iovcnt, off_t offset) {
    if (base_ == nullptr) {
        return make_error<size_t>(ErrorCode::FILE_NOT_FOUND);
    }
    if (iov == nullptr || iovcnt < 0) {
        return make_error<size_t>(ErrorCode::FILE_INVALID_BUFFER);
    }
    size_t total = 0;
    for (int i = 0; i < iovcnt; ++i) total += iov[i].iov_len;
    if (!InRange(offset, total)) {
        LOG(ERROR) << "DaxFile: read of " << total << " bytes at offset "
                   << offset << " exceeds mapping of " << size_ << " bytes";
        return make_error<size_t>(ErrorCode::FILE_READ_FAIL);
    }
    const char *src = static_cast<const char *>(base_) + offset;
    for (int i = 0; i < iovcnt; ++i) {
        if (iov[i].iov_len > 0) {
            std::memcpy(iov[i].iov_base, src, iov[i].iov_len);
            src += iov[i].iov_len;
        }
    }
    return total;
}

}  // namespace mooncake
