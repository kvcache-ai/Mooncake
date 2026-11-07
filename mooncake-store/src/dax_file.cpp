// Copyright 2026 KVCache.AI

#include "dax_file.h"

#include <fcntl.h>
#include <glog/logging.h>
#include <sys/file.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <cstdint>
#include <cstring>

#if defined(__x86_64__)
#include <cpuid.h>
#include <immintrin.h>
#endif

namespace mooncake {

namespace {

#if defined(__x86_64__)
constexpr uintptr_t kCacheLine = 64;

__attribute__((target("clwb"))) void FlushClwb(uintptr_t p, uintptr_t end) {
    for (; p < end; p += kCacheLine) _mm_clwb(reinterpret_cast<void *>(p));
}

__attribute__((target("clflushopt"))) void FlushClflushopt(uintptr_t p,
                                                           uintptr_t end) {
    for (; p < end; p += kCacheLine) {
        _mm_clflushopt(reinterpret_cast<void *>(p));
    }
}

void FlushClflush(uintptr_t p, uintptr_t end) {
    for (; p < end; p += kCacheLine) {
        _mm_clflush(reinterpret_cast<const void *>(p));
    }
}

using FlushFn = void (*)(uintptr_t, uintptr_t);

// CLWB writes the line back and keeps it cached; CLFLUSHOPT and CLFLUSH
// (every x86-64 CPU) evict it.
FlushFn PickFlush() {
    unsigned eax, ebx, ecx, edx;
    if (__get_cpuid_count(7, 0, &eax, &ebx, &ecx, &edx)) {
        if (ebx & bit_CLWB) return FlushClwb;
        if (ebx & bit_CLFLUSHOPT) return FlushClflushopt;
    }
    return FlushClflush;
}

// Writes [addr, addr + len) back to the persistence domain, fenced before any
// later store (e.g. the metadata checkpoint that references the record).
void PersistRange(const void *addr, size_t len) {
    static const FlushFn flush = PickFlush();
    const auto begin = reinterpret_cast<uintptr_t>(addr);
    flush(begin & ~(kCacheLine - 1), begin + len);
    _mm_sfence();
}

// Below this a cached memcpy is as fast; above it Optane write bandwidth is
// highest with non-temporal stores (Yang et al., FAST '20).
constexpr size_t kStreamMin = 256;

// memcpy with non-temporal stores for the whole cache lines of dst. A cached
// store first reads each destination line in (read-for-ownership), which on
// PMEM costs a media read per line and evicts at 64 B, below Optane's 256 B
// internal granularity. The caller must _mm_sfence() before publishing dst.
void StreamCopy(char *dst, const char *src, size_t len) {
    const size_t head = std::min(
        len, (kCacheLine - reinterpret_cast<uintptr_t>(dst) % kCacheLine) %
                 kCacheLine);
    std::memcpy(dst, src, head);
    dst += head, src += head, len -= head;
    for (; len >= kCacheLine;
         dst += kCacheLine, src += kCacheLine, len -= kCacheLine) {
        auto *d = reinterpret_cast<__m128i *>(dst);
        const auto *s = reinterpret_cast<const __m128i *>(src);
        _mm_stream_si128(d, _mm_loadu_si128(s));
        _mm_stream_si128(d + 1, _mm_loadu_si128(s + 1));
        _mm_stream_si128(d + 2, _mm_loadu_si128(s + 2));
        _mm_stream_si128(d + 3, _mm_loadu_si128(s + 3));
    }
    std::memcpy(dst, src, len);
}
#endif

}  // namespace

DaxFile::DaxFile(const std::string &path, int fd, void *base, size_t size,
                 bool flush_cpu_cache)
    : StorageFile(path, fd),
      base_(base),
      size_(size),
      flush_cpu_cache_(flush_cpu_cache) {
    // Never unlink the backing path on a failed write: for a character
    // device that would remove the device node itself.
    delete_on_write_fail_ = false;
}

tl::expected<std::shared_ptr<DaxFile>, ErrorCode> DaxFile::Open(
    const std::string &path, size_t size, bool flush_cpu_cache) {
    if (path.empty() || size == 0) {
        LOG(ERROR) << "DaxFile: path must be non-empty and size > 0";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
#if !defined(__x86_64__)
    if (flush_cpu_cache) {
        LOG(ERROR) << "DaxFile: CPU cache flush is only implemented for x86-64";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
#endif

    int fd = ::open(path.c_str(), O_RDWR | O_CLOEXEC);
    if (fd < 0) {
        LOG(ERROR) << "DaxFile: failed to open " << path << ": "
                   << strerror(errno);
        return tl::make_unexpected(ErrorCode::FILE_OPEN_FAIL);
    }

    // One owner per arena: a second client (or backend) mapping the same
    // device would run its own allocator and overwrite this one's records.
    // Released when the fd is closed.
    if (::flock(fd, LOCK_EX | LOCK_NB) != 0) {
        LOG(ERROR) << "DaxFile: " << path
                   << " is already in use by another live client ("
                   << strerror(errno)
                   << "); each DAX arena needs its own device";
        ::close(fd);
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

    return std::shared_ptr<DaxFile>(
        new DaxFile(path, fd, base, size, flush_cpu_cache));
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
    // msync covers fsdax and regular files. Device-DAX chardevs have no fsync
    // op, so the kernel answers EINVAL; there is no page cache to flush. CPU
    // caches are not flushed here: with flush_cpu_cache, vector_write has
    // already persisted every completed write.
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
        const auto *src = static_cast<const char *>(iov[i].iov_base);
        const size_t len = iov[i].iov_len;
#if defined(__x86_64__)
        if (len >= kStreamMin) {
            StreamCopy(dst, src, len);
            dst += len;
            continue;
        }
#endif
        if (len > 0) std::memcpy(dst, src, len);
        dst += len;
    }
#if defined(__x86_64__)
    // PersistRange fences too. Streamed lines are already out of the cache,
    // so re-flushing them is only a cheap miss.
    if (flush_cpu_cache_ && total > 0) {
        PersistRange(static_cast<char *>(base_) + offset, total);
    } else {
        _mm_sfence();  // order streamed stores before the caller publishes
    }
#endif
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
