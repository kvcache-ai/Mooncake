#include "storage/distributed/posix_fs_adapter.h"

#include <dirent.h>
#include <fcntl.h>
#include <glog/logging.h>
#include <sys/stat.h>
#include <sys/uio.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <climits>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <limits>
#include <memory>
#include <vector>

namespace mooncake {

namespace {

tl::expected<void, ErrorCode> ValidateIov(const iovec* iov, int iovcnt) {
    if (iovcnt < 0 || (iovcnt > 0 && iov == nullptr)) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    for (int i = 0; i < iovcnt; ++i) {
        if (!iov[i].iov_base && iov[i].iov_len > 0) {
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
    }
    return {};
}

ErrorCode ReadOpenError() {
    return errno == ENOENT ? ErrorCode::FILE_NOT_FOUND
                           : ErrorCode::FILE_OPEN_FAIL;
}

// Smallest logical block size O_DIRECT accepts on common block devices.
constexpr uint64_t kMinDirectIoAlignment = 512;
// Upper bound of the per-thread bounce buffer; larger reads are chunked.
constexpr size_t kMaxBounceBytes = 8ULL << 20;

bool IsAligned(uint64_t value, uint64_t alignment) {
    return (value & (alignment - 1)) == 0;
}

uint64_t AlignDown(uint64_t value, uint64_t alignment) {
    return value & ~(alignment - 1);
}

uint64_t AlignUp(uint64_t value, uint64_t alignment) {
    return AlignDown(value + alignment - 1, alignment);
}

// O_DIRECT accepts the caller's iovecs as-is only when the offset, every
// buffer address and every length are block aligned.
bool IovsAreDirectAligned(const iovec* iov, int iovcnt, int64_t offset,
                          uint64_t alignment) {
    if (!IsAligned(static_cast<uint64_t>(offset), alignment)) return false;
    for (int i = 0; i < iovcnt; ++i) {
        if (!IsAligned(reinterpret_cast<uintptr_t>(iov[i].iov_base),
                       alignment) ||
            !IsAligned(iov[i].iov_len, alignment)) {
            return false;
        }
    }
    return true;
}

bool DirectIOAlignmentFits(int fd, uint64_t alignment) {
#ifdef STATX_DIOALIGN
    struct statx stx{};
    if (::statx(fd, "", AT_EMPTY_PATH, STATX_DIOALIGN, &stx) == 0 &&
        (stx.stx_mask & STATX_DIOALIGN) != 0) {
        return stx.stx_dio_offset_align != 0 &&
               stx.stx_dio_offset_align <= alignment &&
               stx.stx_dio_mem_align <= alignment;
    }
#endif
    return true;
}

bool SetDirectFlag(int fd) {
#ifdef O_DIRECT
    const int flags = ::fcntl(fd, F_GETFL);
    return flags >= 0 && ::fcntl(fd, F_SETFL, flags | O_DIRECT) == 0;
#else
    // Platforms without O_DIRECT (e.g. macOS) refuse direct I/O at Init.
    (void)fd;
    errno = ENOTSUP;
    return false;
#endif
}

// Sums the iovec lengths; false if the total does not fit in ssize_t.
bool TotalIovLength(const iovec* iov, int iovcnt, int64_t offset,
                    size_t& total) {
    constexpr uint64_t kMax = std::numeric_limits<ssize_t>::max();
    uint64_t sum = 0;
    for (int i = 0; i < iovcnt; ++i) {
        if (iov[i].iov_len > kMax - sum) return false;
        sum += iov[i].iov_len;
    }
    if (sum > kMax - static_cast<uint64_t>(offset)) return false;
    total = static_cast<size_t>(sum);
    return true;
}

// Thread-local aligned bounce buffer, grown on demand up to the chunk size.
char* GetBounceBuffer(size_t size, uint64_t alignment) {
    struct Free {
        void operator()(char* p) const { std::free(p); }
    };
    thread_local std::unique_ptr<char, Free> data;
    thread_local size_t capacity = 0;
    thread_local uint64_t data_alignment = 0;
    if (data && capacity >= size && data_alignment >= alignment) {
        return data.get();
    }
    void* buf = nullptr;
    if (::posix_memalign(&buf, alignment, size) != 0) return nullptr;
    data.reset(static_cast<char*>(buf));
    capacity = size;
    data_alignment = alignment;
    return data.get();
}

// Copies between a flat buffer and an iovec array, remembering the position
// in the iovecs across calls.
class IovCursor {
   public:
    IovCursor(const iovec* iov, int iovcnt) : iov_(iov), iovcnt_(iovcnt) {}

    // Copies the next `n` iovec bytes into `dst`.
    void Gather(char* dst, size_t n) {
        Walk(n, [&](char* p, size_t len) {
            std::memcpy(dst, p, len);
            dst += len;
        });
    }

    // Copies `n` bytes from `src` into the next iovec bytes.
    void Scatter(const char* src, size_t n) {
        Walk(n, [&](char* p, size_t len) {
            std::memcpy(p, src, len);
            src += len;
        });
    }

   private:
    template <typename Fn>
    void Walk(size_t n, Fn&& fn) {
        while (n > 0 && index_ < iovcnt_) {
            const iovec& v = iov_[index_];
            const size_t len = std::min(n, v.iov_len - offset_);
            if (len > 0) fn(static_cast<char*>(v.iov_base) + offset_, len);
            n -= len;
            offset_ += len;
            if (offset_ == v.iov_len) {
                ++index_;
                offset_ = 0;
            }
        }
    }

    const iovec* iov_;
    int iovcnt_;
    int index_ = 0;
    size_t offset_ = 0;
};

ssize_t PreadRetry(int fd, void* buf, size_t len, int64_t offset) {
    ssize_t ret;
    do {
        ret = ::pread(fd, buf, len, static_cast<off_t>(offset));
    } while (ret < 0 && errno == EINTR);
    return ret;
}

ssize_t PreadvRetry(int fd, iovec* iov, int iovcnt, int64_t offset) {
    ssize_t ret;
    do {
        ret = ::preadv(fd, iov, iovcnt, static_cast<off_t>(offset));
    } while (ret < 0 && errno == EINTR);
    return ret;
}

// Writes every byte of `iov` at `offset`, advancing `iov` past short writes.
bool PwritevAll(int fd, iovec* iov, int iovcnt, uint64_t offset) {
    int index = 0;
    while (index < iovcnt) {
        ssize_t n;
        do {
            n = ::pwritev(fd, iov + index, iovcnt - index,
                          static_cast<off_t>(offset));
        } while (n < 0 && errno == EINTR);
        if (n <= 0) return false;
        offset += static_cast<uint64_t>(n);
        size_t left = static_cast<size_t>(n);
        while (index < iovcnt && left >= iov[index].iov_len) {
            left -= iov[index].iov_len;
            ++index;
        }
        if (left > 0) {
            iov[index].iov_base =
                static_cast<char*>(iov[index].iov_base) + left;
            iov[index].iov_len -= left;
        }
    }
    return true;
}

bool FsyncRetry(int fd) {
    int rc;
    do {
        rc = ::fsync(fd);
    } while (rc != 0 && errno == EINTR);
    return rc == 0;
}

}  // namespace

PosixFsAdapter::PosixFsAdapter(bool direct_io, uint64_t alignment,
                               bool direct_fsync)
    : direct_io_(direct_io),
      direct_fsync_(direct_fsync),
      alignment_(alignment) {
    if (direct_fsync_ && !direct_io_) {
        LOG(WARNING) << "DFS POSIX fsync applies only to direct I/O, which is "
                        "off; writes are not fsynced";
        direct_fsync_ = false;
    }
}

tl::expected<void, ErrorCode> PosixFsAdapter::Init(
    const std::string& mount_path) {
    mount_path_ = mount_path;
    std::error_code ec;
    std::filesystem::create_directories(mount_path_, ec);
    if (ec) return tl::make_unexpected(ErrorCode::FILE_WRITE_FAIL);
    if (direct_io_) return ProbeDirectIo();
    return {};
}

// Writes, reads back and, with direct_fsync, fsyncs one aligned block of a
// private O_DIRECT file under the mount, so a filesystem that cannot serve
// the configured direct I/O fails Init instead of the first request.
tl::expected<void, ErrorCode> PosixFsAdapter::ProbeDirectIo() {
    if (alignment_ < kMinDirectIoAlignment ||
        (alignment_ & (alignment_ - 1)) != 0) {
        LOG(ERROR) << "DFS POSIX direct I/O needs a power-of-two alignment >= "
                   << kMinDirectIoAlignment << ", got " << alignment_;
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    char host[256] = {};
    ::gethostname(host, sizeof(host) - 1);
    const std::string path =
        mount_path_ + "/.mooncake_direct_io_probe_" + host + "_" +
        std::to_string(::getpid()) + "_" +
        std::to_string(
            std::chrono::steady_clock::now().time_since_epoch().count());
    int fd;
    do {
        fd = ::open(path.c_str(), O_RDWR | O_CREAT | O_EXCL | O_CLOEXEC, 0600);
    } while (fd < 0 && errno == EINTR);
    if (fd < 0) {
        LOG(ERROR) << "Failed to create DFS direct I/O probe " << path << ": "
                   << std::strerror(errno);
        return tl::make_unexpected(ErrorCode::FILE_OPEN_FAIL);
    }

    std::string failure;
    char* block = GetBounceBuffer(alignment_, alignment_);
    if (!DirectIOAlignmentFits(fd, alignment_)) {
        failure = "filesystem needs an alignment larger than " +
                  std::to_string(alignment_);
    } else if (!SetDirectFlag(fd)) {
        failure = std::string("setting O_DIRECT: ") + std::strerror(errno);
    } else if (!block) {
        failure = "no memory for the probe block";
    } else {
        std::memset(block, 0, alignment_);
        iovec iov{block, static_cast<size_t>(alignment_)};
        if (!PwritevAll(fd, &iov, 1, 0)) {
            failure = std::string("O_DIRECT write: ") + std::strerror(errno);
        } else if (PreadRetry(fd, block, alignment_, 0) !=
                   static_cast<ssize_t>(alignment_)) {
            failure = std::string("O_DIRECT read: ") + std::strerror(errno);
        } else if (direct_fsync_ && !FsyncRetry(fd)) {
            failure = std::string("fsync: ") + std::strerror(errno);
        }
    }
    ::close(fd);
    ::unlink(path.c_str());
    if (!failure.empty()) {
        LOG(ERROR) << "DFS POSIX direct I/O is enabled but not usable under "
                   << mount_path_ << ": " << failure;
        return tl::make_unexpected(ErrorCode::NOT_SUPPORTED);
    }
    LOG(INFO) << "DFS POSIX I/O uses O_DIRECT under " << mount_path_
              << "; writes are zero-padded to " << alignment_ << " bytes"
              << (direct_fsync_ ? " and fsynced once per file per batch" : "");
    return {};
}

tl::expected<void, ErrorCode> PosixFsAdapter::Shutdown() { return {}; }

tl::expected<void, ErrorCode> PosixFsAdapter::DeleteFile(
    const std::string& path) {
    if (::unlink(path.c_str()) != 0) {
        if (errno == ENOENT)
            return tl::make_unexpected(ErrorCode::FILE_NOT_FOUND);
        return tl::make_unexpected(ErrorCode::FILE_WRITE_FAIL);
    }
    return {};
}

tl::expected<bool, ErrorCode> PosixFsAdapter::FileExists(
    const std::string& path) {
    if (::access(path.c_str(), F_OK) == 0) return true;
    if (errno == ENOENT) return false;
    return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
}

tl::expected<std::vector<std::string>, ErrorCode> PosixFsAdapter::ListFiles(
    const std::string& dir) {
    DIR* d = ::opendir(dir.c_str());
    if (!d) {
        if (errno == ENOENT)
            return tl::make_unexpected(ErrorCode::FILE_NOT_FOUND);
        return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
    }

    std::vector<std::string> result;
    while (auto* entry = ::readdir(d)) {
        std::string name = entry->d_name;
        if (name == "." || name == "..") continue;
        if (entry->d_type == DT_DIR) continue;
        if (entry->d_type == DT_UNKNOWN) {
            struct stat st;
            if (::stat((dir + "/" + name).c_str(), &st) == 0 &&
                S_ISDIR(st.st_mode)) {
                continue;
            }
        }
        result.push_back(std::move(name));
    }
    ::closedir(d);
    return result;
}

tl::expected<void, ErrorCode> PosixFsAdapter::EnableDirectIo(
    int fd, const std::string& path) {
    if (!direct_io_ || SetDirectFlag(fd)) return {};
    LOG(ERROR) << "Failed to set O_DIRECT on " << path << ": "
               << std::strerror(errno);
    return tl::make_unexpected(ErrorCode::FILE_OPEN_FAIL);
}

tl::expected<int, ErrorCode> PosixFsAdapter::OpenFile(const std::string& path) {
    int fd = ::open(path.c_str(), O_RDWR | O_CREAT | O_CLOEXEC, 0644);
    if (fd < 0) return tl::make_unexpected(ErrorCode::FILE_OPEN_FAIL);
    auto direct = EnableDirectIo(fd, path);
    if (!direct) {
        ::close(fd);
        return tl::make_unexpected(direct.error());
    }
    return fd;
}

tl::expected<int, ErrorCode> PosixFsAdapter::OpenExistingFile(
    const std::string& path) {
    int fd;
    do {
        fd = ::open(path.c_str(), O_RDWR | O_CLOEXEC);
    } while (fd < 0 && errno == EINTR);
    if (fd < 0) return tl::make_unexpected(ReadOpenError());
    auto direct = EnableDirectIo(fd, path);
    if (!direct) {
        ::close(fd);
        return tl::make_unexpected(direct.error());
    }
    return fd;
}

tl::expected<void, ErrorCode> PosixFsAdapter::CloseFile(int fd) {
    if (fd < 0) return tl::make_unexpected(ErrorCode::FILE_INVALID_HANDLE);
    if (::close(fd) != 0) {
        return tl::make_unexpected(ErrorCode::FILE_INVALID_HANDLE);
    }
    return {};
}

tl::expected<void, ErrorCode> PosixFsAdapter::PreallocateFile(
    const std::string& path, uint64_t size) {
    int fd = ::open(path.c_str(), O_RDWR | O_CREAT | O_CLOEXEC, 0644);
    if (fd < 0) return tl::make_unexpected(ErrorCode::FILE_OPEN_FAIL);

    int rc = ::ftruncate(fd, static_cast<off_t>(size));
    int saved_errno = errno;
    ::close(fd);
    errno = saved_errno;
    if (rc != 0) return tl::make_unexpected(ErrorCode::FILE_WRITE_FAIL);
    return {};
}

tl::expected<size_t, ErrorCode> PosixFsAdapter::WriteAt(int fd,
                                                        const iovec* iov,
                                                        int iovcnt,
                                                        int64_t offset) {
    if (fd < 0 || offset < 0) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    auto valid = ValidateIov(iov, iovcnt);
    if (!valid) return tl::make_unexpected(valid.error());
    if (iovcnt == 0) return size_t{0};

    if (direct_io_) return DirectWrite(fd, iov, iovcnt, offset);

    ssize_t ret;
    do {
        ret = ::pwritev(fd, iov, iovcnt, static_cast<off_t>(offset));
    } while (ret < 0 && errno == EINTR);
    if (ret < 0) return tl::make_unexpected(ErrorCode::FILE_WRITE_FAIL);
    return static_cast<size_t>(ret);
}

// Writes the whole request and zero-fills up to the next alignment boundary.
// Aligned iovecs go to the kernel as-is, with only the unaligned tail copied
// into a padded block; anything else is gathered through a bounce buffer.
tl::expected<size_t, ErrorCode> PosixFsAdapter::DirectWrite(int fd,
                                                            const iovec* iov,
                                                            int iovcnt,
                                                            int64_t offset) {
    size_t total = 0;
    if (!IsAligned(static_cast<uint64_t>(offset), alignment_) ||
        !TotalIovLength(iov, iovcnt, offset, total)) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (total == 0) return size_t{0};

    bool zero_copy = iovcnt < IOV_MAX;
    for (int i = 0; i < iovcnt && zero_copy; ++i) {
        zero_copy = IsAligned(reinterpret_cast<uintptr_t>(iov[i].iov_base),
                              alignment_) &&
                    (i + 1 == iovcnt || IsAligned(iov[i].iov_len, alignment_));
    }
    if (zero_copy) {
        std::vector<iovec> out(iov, iov + iovcnt);
        const size_t last_len = out.back().iov_len;
        const size_t prefix =
            static_cast<size_t>(AlignDown(last_len, alignment_));
        const size_t tail = last_len - prefix;
        if (tail > 0) {
            char* block = GetBounceBuffer(alignment_, alignment_);
            if (!block) return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
            std::memcpy(block, static_cast<char*>(out.back().iov_base) + prefix,
                        tail);
            std::memset(block + tail, 0, alignment_ - tail);
            out.back().iov_len = prefix;
            out.push_back({block, static_cast<size_t>(alignment_)});
        }
        if (!PwritevAll(fd, out.data(), static_cast<int>(out.size()),
                        static_cast<uint64_t>(offset))) {
            return tl::make_unexpected(ErrorCode::FILE_WRITE_FAIL);
        }
        return total;
    }

    const uint64_t chunk =
        std::max<uint64_t>(AlignDown(kMaxBounceBytes, alignment_), alignment_);
    const size_t buf_size = static_cast<size_t>(
        std::min<uint64_t>(chunk, AlignUp(total, alignment_)));
    char* buf = GetBounceBuffer(buf_size, alignment_);
    if (!buf) return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);

    IovCursor in(iov, iovcnt);
    size_t done = 0;
    uint64_t pos = static_cast<uint64_t>(offset);
    while (done < total) {
        const size_t n = std::min(buf_size, total - done);
        in.Gather(buf, n);
        const size_t padded = static_cast<size_t>(AlignUp(n, alignment_));
        std::memset(buf + n, 0, padded - n);
        iovec one{buf, padded};
        if (!PwritevAll(fd, &one, 1, pos)) {
            return tl::make_unexpected(ErrorCode::FILE_WRITE_FAIL);
        }
        done += n;
        pos += padded;
    }
    return total;
}

tl::expected<void, ErrorCode> PosixFsAdapter::SyncFile(int fd) {
    if (fd < 0) return tl::make_unexpected(ErrorCode::FILE_INVALID_HANDLE);
    if (!direct_fsync_) return {};
    if (!FsyncRetry(fd)) return tl::make_unexpected(ErrorCode::FILE_WRITE_FAIL);
    return {};
}

tl::expected<size_t, ErrorCode> PosixFsAdapter::ReadAt(int fd, iovec* iov,
                                                       int iovcnt,
                                                       int64_t offset) {
    if (fd < 0 || offset < 0) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    auto valid = ValidateIov(iov, iovcnt);
    if (!valid) return tl::make_unexpected(valid.error());
    if (iovcnt == 0) return size_t{0};

    if (direct_io_ && !IovsAreDirectAligned(iov, iovcnt, offset, alignment_)) {
        return DirectRead(fd, iov, iovcnt, offset);
    }
    const ssize_t ret = PreadvRetry(fd, iov, iovcnt, offset);
    if (ret < 0) return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
    return static_cast<size_t>(ret);
}

// Reads the aligned range covering [offset, offset + total) into a bounce
// buffer, chunk by chunk, and scatters the requested bytes into `iov`.
tl::expected<size_t, ErrorCode> PosixFsAdapter::DirectRead(int fd, iovec* iov,
                                                           int iovcnt,
                                                           int64_t offset) {
    size_t total = 0;
    if (!TotalIovLength(iov, iovcnt, offset, total)) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (total == 0) return size_t{0};

    const uint64_t start = AlignDown(static_cast<uint64_t>(offset), alignment_);
    const uint64_t end =
        AlignUp(static_cast<uint64_t>(offset) + total, alignment_);
    const uint64_t chunk =
        std::max<uint64_t>(AlignDown(kMaxBounceBytes, alignment_), alignment_);
    const size_t buf_size = static_cast<size_t>(std::min(chunk, end - start));
    char* buf = GetBounceBuffer(buf_size, alignment_);
    if (!buf) return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);

    IovCursor out(iov, iovcnt);
    size_t done = 0;
    uint64_t pos = start;
    size_t skip = static_cast<size_t>(static_cast<uint64_t>(offset) - start);
    while (done < total) {
        const size_t n =
            static_cast<size_t>(std::min<uint64_t>(buf_size, end - pos));
        const ssize_t got = PreadRetry(fd, buf, n, pos);
        if (got < 0) {
            if (done > 0) break;
            return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
        }
        if (static_cast<size_t>(got) <= skip) break;  // EOF
        const size_t useful =
            std::min(static_cast<size_t>(got) - skip, total - done);
        out.Scatter(buf + skip, useful);
        done += useful;
        skip = 0;
        pos += static_cast<uint64_t>(got);
        if (static_cast<size_t>(got) < n) break;
    }
    return done;
}

}  // namespace mooncake
