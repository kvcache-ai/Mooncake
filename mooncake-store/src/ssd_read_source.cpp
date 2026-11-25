// Copyright 2026 KVCache.AI
// Licensed under the Apache License, Version 2.0.
#include "ssd_read_source.h"

#include <cerrno>
#include <fcntl.h>
#include <limits>
#include <sys/stat.h>
#include <unistd.h>
#include <utility>

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
    PosixReadSource(int fd, uint64_t file_bytes, std::shared_ptr<void> lifetime)
        : fd_(fd), file_bytes_(file_bytes), lifetime_(std::move(lifetime)) {}
    ~PosixReadSource() override { close(fd_); }
    uint64_t file_bytes() const noexcept override { return file_bytes_; }
    std::optional<int> native_read_fd() const noexcept override { return fd_; }
    ReadResult ReadAt(std::span<std::byte> buffer,
                      uint64_t file_offset) noexcept override {
        constexpr uint64_t max_offset = std::numeric_limits<off_t>::max();
        if (file_offset > max_offset ||
            buffer.size() > max_offset - file_offset ||
            buffer.size() >
                static_cast<size_t>(std::numeric_limits<ssize_t>::max()))
            return {0, Error::kOverflow};
        if (!buffer.empty() && !buffer.data())
            return {0, Error::kInvalidArgument};
        size_t bytes = 0;
        while (bytes < buffer.size()) {
            const auto result =
                pread(fd_, buffer.data() + bytes, buffer.size() - bytes,
                      static_cast<off_t>(file_offset + bytes));
            if (result < 0) {
                if (errno == EINTR) continue;
                return {bytes, Error::kIoError, errno};
            }
            if (result == 0) break;
            bytes += static_cast<size_t>(result);
        }
        return {bytes, Error::kOk};
    }

   private:
    int fd_;
    const uint64_t file_bytes_;
    std::shared_ptr<void> lifetime_;
};

}  // namespace

ReadSourceResult OpenPosixReadSource(const std::string& path,
                                     uint64_t min_file_bytes,
                                     std::shared_ptr<void> lifetime) {
    constexpr uint64_t max_offset = std::numeric_limits<off_t>::max();
    if (min_file_bytes > max_offset) return {Error::kOverflow, {}};

    int raw_fd = -1;
    do {
        raw_fd =
            open(path.c_str(), O_RDONLY | O_CLOEXEC | O_NOFOLLOW | O_NONBLOCK);
    } while (raw_fd < 0 && errno == EINTR);
    ScopedFd fd(raw_fd);
    if (fd.get() < 0) return {Error::kIoError, {}, errno};
    struct stat metadata{};
    if (fstat(fd.get(), &metadata) != 0) return {Error::kIoError, {}, errno};
    if (!S_ISREG(metadata.st_mode) || metadata.st_size < 0 ||
        min_file_bytes > static_cast<uint64_t>(metadata.st_size))
        return {Error::kInvalidArgument, {}};
    const int status = fcntl(fd.get(), F_GETFL);
    if (status < 0 || fcntl(fd.get(), F_SETFL, status & ~O_NONBLOCK) < 0)
        return {Error::kIoError, {}, errno};
    auto source = std::make_shared<PosixReadSource>(
        fd.get(), static_cast<uint64_t>(metadata.st_size), std::move(lifetime));
    fd.Release();
    return {Error::kOk, std::move(source)};
}

}  // namespace mooncake::ssd_offload
