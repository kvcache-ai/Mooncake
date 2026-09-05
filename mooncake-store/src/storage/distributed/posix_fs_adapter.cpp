#include "storage/distributed/posix_fs_adapter.h"

#include <dirent.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/uio.h>
#include <unistd.h>

#include <cerrno>
#include <algorithm>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <limits>
#include <memory>
#include <mutex>

namespace mooncake {

namespace {

// O_DIRECT alignment assumed for direct reads: offset, buffer address and
// length must all be multiples of the filesystem block size. 4096 covers the
// common 512-byte and 4K-block cases.
constexpr uint64_t kDirectIoAlignment = 4096;

// Single pread chunk cap so a large window never overflows ssize_t.
constexpr uint64_t kDirectIoMaxChunk = 1ULL << 30;

// Upper bound on pooled O_DIRECT staging buffers. Sized to cover the batch
// read fan-out so a fully busy pool is the common steady state; beyond this a
// read falls back to a transient allocation rather than blocking on a slot.
constexpr size_t kMaxDirectStagingBuffers = 128;

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

}  // namespace

tl::expected<void, ErrorCode> PosixFsAdapter::Init(
    const std::string& mount_path) {
    mount_path_ = mount_path;
    std::error_code ec;
    std::filesystem::create_directories(mount_path_, ec);
    if (ec) return tl::make_unexpected(ErrorCode::FILE_WRITE_FAIL);
    return {};
}

tl::expected<void, ErrorCode> PosixFsAdapter::Shutdown() { return {}; }

tl::expected<size_t, ErrorCode> PosixFsAdapter::WriteFile(
    const std::string& path, std::span<const char> data) {
    int fd =
        ::open(path.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
    if (fd < 0) return tl::make_unexpected(ErrorCode::FILE_OPEN_FAIL);

    size_t total_written = 0;
    while (total_written < data.size()) {
        ssize_t ret = ::write(fd, data.data() + total_written,
                              data.size() - total_written);
        if (ret < 0) {
            int saved_errno = errno;
            ::close(fd);
            errno = saved_errno;
            return tl::make_unexpected(ErrorCode::FILE_WRITE_FAIL);
        }
        if (ret == 0) break;
        total_written += static_cast<size_t>(ret);
    }

    if (::close(fd) != 0) {
        return tl::make_unexpected(ErrorCode::FILE_WRITE_FAIL);
    }
    if (total_written != data.size()) {
        return tl::make_unexpected(ErrorCode::FILE_WRITE_FAIL);
    }
    return total_written;
}

tl::expected<size_t, ErrorCode> PosixFsAdapter::ReadFile(
    const std::string& path, void* buf, size_t len) {
    if (!buf && len > 0) return tl::make_unexpected(ErrorCode::INVALID_PARAMS);

    int fd = ::open(path.c_str(), O_RDONLY | O_CLOEXEC);
    if (fd < 0) return tl::make_unexpected(ReadOpenError());

    size_t total_read = 0;
    char* dest = static_cast<char*>(buf);
    while (total_read < len) {
        ssize_t ret = ::read(fd, dest + total_read, len - total_read);
        if (ret < 0) {
            int saved_errno = errno;
            ::close(fd);
            errno = saved_errno;
            return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
        }
        if (ret == 0) break;
        total_read += static_cast<size_t>(ret);
    }

    if (::close(fd) != 0) {
        return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
    }
    return total_read;
}

tl::expected<size_t, ErrorCode> PosixFsAdapter::VectorWriteFile(
    const std::string& path, const iovec* iov, int iovcnt, off_t offset) {
    auto valid = ValidateIov(iov, iovcnt);
    if (!valid) return tl::make_unexpected(valid.error());
    if (offset < 0) return tl::make_unexpected(ErrorCode::INVALID_PARAMS);

    int fd =
        ::open(path.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
    if (fd < 0) return tl::make_unexpected(ErrorCode::FILE_OPEN_FAIL);
    auto result = WriteAt(fd, iov, iovcnt, offset);
    int saved_errno = errno;
    ::close(fd);
    errno = saved_errno;
    return result;
}

tl::expected<size_t, ErrorCode> PosixFsAdapter::VectorReadFile(
    const std::string& path, const iovec* iov, int iovcnt, off_t offset) {
    auto valid = ValidateIov(iov, iovcnt);
    if (!valid) return tl::make_unexpected(valid.error());
    if (offset < 0) return tl::make_unexpected(ErrorCode::INVALID_PARAMS);

    int fd = ::open(path.c_str(), O_RDONLY | O_CLOEXEC);
    if (fd < 0) return tl::make_unexpected(ReadOpenError());
    auto result = ReadAt(fd, const_cast<iovec*>(iov), iovcnt, offset);
    int saved_errno = errno;
    ::close(fd);
    errno = saved_errno;
    return result;
}

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

tl::expected<int, ErrorCode> PosixFsAdapter::OpenFile(const std::string& path) {
    int fd = ::open(path.c_str(), O_RDWR | O_CREAT | O_CLOEXEC, 0644);
    if (fd < 0) return tl::make_unexpected(ErrorCode::FILE_OPEN_FAIL);
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

    ssize_t ret = ::pwritev(fd, iov, iovcnt, static_cast<off_t>(offset));
    if (ret < 0) return tl::make_unexpected(ErrorCode::FILE_WRITE_FAIL);
    return static_cast<size_t>(ret);
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

    ssize_t ret = ::preadv(fd, iov, iovcnt, static_cast<off_t>(offset));
    if (ret < 0) return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
    return static_cast<size_t>(ret);
}

tl::expected<int, ErrorCode> PosixFsAdapter::OpenFileDirect(
    const std::string& path) {
#ifdef O_DIRECT
    int fd = ::open(path.c_str(), O_RDONLY | O_DIRECT | O_CLOEXEC);
    if (fd < 0 &&
        (errno == EINVAL || errno == EOPNOTSUPP || errno == ENOTSUP)) {
        // The filesystem does not support O_DIRECT; fall back to a buffered
        // read-only handle. DirectReadAt stays correct, it just goes through
        // the page cache.
        fd = ::open(path.c_str(), O_RDONLY | O_CLOEXEC);
    }
#else
    // Platforms without O_DIRECT (e.g. macOS) get a buffered read-only
    // handle; DirectReadAt stays correct through the page cache.
    int fd = ::open(path.c_str(), O_RDONLY | O_CLOEXEC);
#endif
    if (fd < 0) {
        if (errno == ENOENT) {
            return tl::make_unexpected(ErrorCode::FILE_NOT_FOUND);
        }
        return tl::make_unexpected(ErrorCode::FILE_OPEN_FAIL);
    }
    return fd;
}

PosixFsAdapter::DirectStaging::~DirectStaging() { ::free(ptr); }

PosixFsAdapter::DirectStaging* PosixFsAdapter::AcquireDirectStaging(
    size_t size) {
    // Fast path: reuse an idle slot that is already large enough.
    {
        std::lock_guard<std::mutex> lock(direct_staging_mutex_);
        for (auto& slot : direct_staging_pool_) {
            if (!slot->in_use && slot->capacity >= size) {
                slot->in_use = true;
                return slot.get();
            }
        }
        if (direct_staging_pool_.size() >= kMaxDirectStagingBuffers) {
            // Pool exhausted; caller uses a transient allocation instead of
            // blocking a worker thread on a sibling read.
            return nullptr;
        }
    }

    // No reusable slot and the pool has room: allocate outside the lock so a
    // slow posix_memalign does not stall other readers.
    void* raw = nullptr;
    if (::posix_memalign(&raw, kDirectIoAlignment, size) != 0) {
        return nullptr;
    }
    auto fresh = std::make_unique<DirectStaging>();
    fresh->ptr = raw;
    fresh->capacity = size;
    fresh->in_use = true;

    std::lock_guard<std::mutex> lock(direct_staging_mutex_);
    // Re-check the cap under the lock: a concurrent reader may have appended
    // while we were allocating.
    if (direct_staging_pool_.size() >= kMaxDirectStagingBuffers) {
        // ~DirectStaging frees the buffer as `fresh` goes out of scope; the
        // caller falls back to a transient allocation.
        return nullptr;
    }
    DirectStaging* slot = fresh.get();
    direct_staging_pool_.push_back(std::move(fresh));
    return slot;
}

void PosixFsAdapter::ReleaseDirectStaging(DirectStaging* slot) {
    if (slot == nullptr) return;
    std::lock_guard<std::mutex> lock(direct_staging_mutex_);
    slot->in_use = false;
}

tl::expected<size_t, ErrorCode> PosixFsAdapter::DirectReadAt(
    int fd, iovec* iov, int iovcnt, int64_t offset) {
    if (fd < 0 || offset < 0) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    auto valid = ValidateIov(iov, iovcnt);
    if (!valid) return tl::make_unexpected(valid.error());
    if (iovcnt == 0) return size_t{0};

    uint64_t total_length = 0;
    for (int i = 0; i < iovcnt; ++i) {
        if (iov[i].iov_len >
            std::numeric_limits<uint64_t>::max() - total_length) {
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
        total_length += iov[i].iov_len;
    }
    if (total_length == 0) return size_t{0};

    // O_DIRECT cannot take the caller's unaligned offset/buffers, so read the
    // covering aligned window into a staging buffer and copy the requested
    // range out. The window may extend past EOF; only `need` bytes have to
    // come back. Staging buffers are pooled across reads so the 128-way batch
    // path does not hammer the allocator with a memalign/free per key.
    const uint64_t window_start =
        static_cast<uint64_t>(offset) & ~(kDirectIoAlignment - 1);
    const uint64_t skip = static_cast<uint64_t>(offset) - window_start;
    if (total_length > std::numeric_limits<uint64_t>::max() - skip) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    const uint64_t need = skip + total_length;
    if (need > std::numeric_limits<uint64_t>::max() -
                   (kDirectIoAlignment - 1)) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    const uint64_t window_size =
        (need + kDirectIoAlignment - 1) & ~(kDirectIoAlignment - 1);
    if (window_size > std::numeric_limits<size_t>::max() ||
        window_start >
            static_cast<uint64_t>(std::numeric_limits<off_t>::max()) ||
        need > static_cast<uint64_t>(std::numeric_limits<off_t>::max()) -
                   window_start) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    // RAII over the staging memory: hands a pooled slot back, or frees a
    // transient allocation, whichever this read ended up using.
    DirectStaging* pooled = AcquireDirectStaging(static_cast<size_t>(window_size));
    void* transient = nullptr;
    char* window = nullptr;
    if (pooled != nullptr) {
        window = static_cast<char*>(pooled->ptr);
    } else {
        if (::posix_memalign(&transient, kDirectIoAlignment,
                             static_cast<size_t>(window_size)) != 0) {
            return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
        }
        window = static_cast<char*>(transient);
    }
    struct StagingGuard {
        PosixFsAdapter* self;
        DirectStaging* pooled;
        void* transient;
        ~StagingGuard() {
            if (pooled != nullptr) {
                self->ReleaseDirectStaging(pooled);
            } else {
                ::free(transient);
            }
        }
    } guard{this, pooled, transient};

    uint64_t done = 0;
    ErrorCode error = ErrorCode::OK;
    while (done < need) {
        const uint64_t left = std::min(window_size - done, kDirectIoMaxChunk);
        const ssize_t ret =
            ::pread(fd, window + done, static_cast<size_t>(left),
                    static_cast<off_t>(window_start + done));
        if (ret < 0) {
            if (errno == EINTR) continue;
            error = ErrorCode::FILE_READ_FAIL;
            break;
        }
        if (ret == 0) break;  // EOF before the requested range was covered
        done += static_cast<uint64_t>(ret);
    }
    if (error == ErrorCode::OK && done < need) {
        error = ErrorCode::FILE_READ_FAIL;
    }

    if (error == ErrorCode::OK) {
        const char* src = window + skip;
        uint64_t remaining = total_length;
        for (int i = 0; i < iovcnt && remaining > 0; ++i) {
            const size_t n =
                std::min(static_cast<uint64_t>(iov[i].iov_len), remaining);
            if (n > 0) {
                std::memcpy(iov[i].iov_base, src, n);
                src += n;
                remaining -= n;
            }
        }
    }

    if (error != ErrorCode::OK) return tl::make_unexpected(error);
    return static_cast<size_t>(total_length);
}

}  // namespace mooncake
