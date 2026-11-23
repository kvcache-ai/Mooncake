// Copyright 2026 KVCache.AI
// Licensed under the Apache License, Version 2.0.
#include "ssd_read_source.h"

#include <cerrno>
#include <fcntl.h>
#include <limits>
#include <sys/stat.h>
#include <unistd.h>
#include <utility>
#include <cstdio>

namespace mooncake::ssd_offload {
namespace {

class ScopedFd {
   public:
    explicit ScopedFd(int fd) : fd_(fd) {}
    ~ScopedFd() {
        if (fd_ >= 0) close(fd_);
    }
    int get() const { return fd_; }
    void Release() { fd_ = -1; }

   private:
    int fd_;
};

class PosixReadSource final : public ReadSource {
   public:
    PosixReadSource(int fd, ReadSourceInfo info, std::shared_ptr<void> lifetime)
        : fd_(fd), info_(info), lifetime_(std::move(lifetime)) {}
    ~PosixReadSource() override { close(fd_); }
    ReadSourceInfo info() const noexcept override { return info_; }
    std::optional<int> native_read_fd() const noexcept override { return fd_; }
    ReadResult ReadAt(std::span<std::byte> buffer,
                      uint64_t file_offset) noexcept override {
        constexpr uint64_t max_offset = std::numeric_limits<off_t>::max();
        if (file_offset > max_offset ||
            buffer.size() > max_offset - file_offset ||
            buffer.size() >
                static_cast<size_t>(std::numeric_limits<ssize_t>::max()))
            return {0, Error::kOverflow};
        if (info_.io_mode == ReadIoMode::kDirect) {
            if (reinterpret_cast<uintptr_t>(buffer.data()) %
                        kDirectReadAlignment !=
                    0 ||
                buffer.size() % kDirectReadAlignment != 0 ||
                file_offset % kDirectReadAlignment != 0)
                return {0, Error::kInvalidArgument};
            if (buffer.empty()) return {};
            // A successful short direct read may end at an unaligned EOF.
            // Return it to the existing payload-coverage check; issuing a
            // suffix pread would violate O_DIRECT alignment and turn a valid
            // payload into EINVAL. EINTR accepts no bytes, so retry unchanged.
            ssize_t result;
            do {
                result = pread(fd_, buffer.data(), buffer.size(),
                               static_cast<off_t>(file_offset));
            } while (result < 0 && errno == EINTR);
            if (result < 0) {
                const auto error = errno;
                std::fprintf(stderr,
                             "SSD_FILE_READ mode=direct fd=%d offset=%llu "
                             "requested=%zu errno=%d\n",
                             fd_, static_cast<unsigned long long>(file_offset),
                             buffer.size(), error);
                return {0, Error::kIoError};
            }
            return {static_cast<size_t>(result), Error::kOk};
        }
        size_t bytes = 0;
        while (bytes < buffer.size()) {
            const auto result =
                pread(fd_, buffer.data() + bytes, buffer.size() - bytes,
                      static_cast<off_t>(file_offset + bytes));
            if (result < 0) {
                if (errno == EINTR) continue;
                const auto error = errno;
                std::fprintf(stderr,
                             "SSD_FILE_READ mode=buffered fd=%d offset=%llu "
                             "requested=%zu completed=%zu errno=%d\n",
                             fd_, static_cast<unsigned long long>(file_offset),
                             buffer.size(), bytes, error);
                return {bytes, Error::kIoError};
            }
            if (result == 0) break;
            bytes += static_cast<size_t>(result);
        }
        return {bytes, Error::kOk};
    }

   private:
    int fd_;
    const ReadSourceInfo info_;
    std::shared_ptr<void> lifetime_;
};

}  // namespace

ReadSourceResult OpenPosixReadSource(const std::string& path,
                                     ReadSourceInfo info,
                                     std::shared_ptr<void> lifetime) {
    if (info.alignment == 0 || (info.alignment & (info.alignment - 1)) != 0)
        return {Error::kInvalidArgument, {}};
    if (info.io_mode != ReadIoMode::kBuffered &&
        info.io_mode != ReadIoMode::kDirect)
        return {Error::kInvalidArgument, {}};
    if (info.io_mode == ReadIoMode::kDirect &&
        info.alignment != kDirectReadAlignment)
        return {Error::kInvalidArgument, {}};
    constexpr uint64_t max_offset = std::numeric_limits<off_t>::max();
    if (info.value_file_offset > max_offset ||
        info.object_bytes > max_offset - info.value_file_offset)
        return {Error::kOverflow, {}};

    int flags = O_RDONLY | O_CLOEXEC | O_NOFOLLOW | O_NONBLOCK;
    if (info.io_mode == ReadIoMode::kDirect) {
#ifdef O_DIRECT
        flags |= O_DIRECT;
#else
        return {Error::kNotSupported, {}};
#endif
    }
    ScopedFd fd(open(path.c_str(), flags));
    if (fd.get() < 0) return {Error::kIoError, {}};
    struct stat metadata{};
    if (fstat(fd.get(), &metadata) != 0) return {Error::kIoError, {}};
    if (!S_ISREG(metadata.st_mode) || metadata.st_size < 0 ||
        info.value_file_offset + info.object_bytes >
            static_cast<uint64_t>(metadata.st_size))
        return {Error::kInvalidArgument, {}};
    const int status = fcntl(fd.get(), F_GETFL);
    if (status < 0 || fcntl(fd.get(), F_SETFL, status & ~O_NONBLOCK) < 0)
        return {Error::kIoError, {}};
    auto source =
        std::make_shared<PosixReadSource>(fd.get(), info, std::move(lifetime));
    fd.Release();
    return {Error::kOk, std::move(source)};
}

}  // namespace mooncake::ssd_offload
