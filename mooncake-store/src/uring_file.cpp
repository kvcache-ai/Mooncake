#ifdef USE_URING

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <string>
#include <sys/mman.h>
#include <sys/uio.h>
#include <unistd.h>
#include <vector>

#include <glog/logging.h>
#include <liburing.h>

#include "file_interface.h"

namespace mooncake {

// ============================================================================
// GlobalBufInfo — process-wide buffer registration state.
//
// register_buffer() is called once from the storage-backend init path.
// Each thread-local ring picks up the registration lazily on first I/O.
// ============================================================================
struct GlobalBufInfo {
    std::atomic<void*>  base{nullptr};
    std::atomic<size_t> size{0};
};
static GlobalBufInfo g_buf;

// ============================================================================
// SharedUringRing — per-thread io_uring ring.
//
// Design goals:
//  - One ring per *thread*: eliminates all mutex contention between threads.
//    Different threads can perform I/O fully concurrently.
//  - Within one thread: no lock needed (single owner), so multiple SQEs can
//    be batched before waiting, exposing NVMe queue depth > 1.
//  - Buffer registration: stored globally in g_buf, registered lazily on
//    each thread-local ring on first use (ensure_buf_registered()).
//  - File-descriptor registration (IOSQE_FIXED_FILE) intentionally omitted:
//    the per-I/O fdget() overhead (~50 ns) is negligible compared to the
//    mutex contention that the old global ring imposed (> 1 ms per read).
// ============================================================================
class SharedUringRing {
   public:
    static constexpr unsigned QUEUE_DEPTH = 32;
    static constexpr size_t   MIN_CHUNK   = 4096;

    static SharedUringRing& instance() {
        thread_local SharedUringRing tl_ring;
        return tl_ring;
    }

    SharedUringRing(const SharedUringRing&)            = delete;
    SharedUringRing& operator=(const SharedUringRing&) = delete;

    bool is_initialized()       const { return initialized_; }
    bool is_buffer_registered() const { return buf_registered_; }
    void* buffer_base()         const { return buf_base_; }
    size_t buffer_size()        const { return buf_size_; }

    // -----------------------------------------------------------------
    // Buffer registration
    // -----------------------------------------------------------------

    // Register a buffer on THIS thread's ring.  Called lazily from each
    // thread before the first fixed-buffer I/O.
    bool ensure_buf_registered() {
        if (buf_registered_) return true;
        if (buf_register_failed_) return false;  // don't retry after failure
        if (!initialized_) return false;
        void*  b = g_buf.base.load(std::memory_order_acquire);
        size_t s = g_buf.size.load(std::memory_order_acquire);
        if (!b || !s) return false;
        struct iovec iov{b, s};
        int ret = io_uring_register_buffers(&ring_, &iov, 1);
        if (ret < 0) {
            int err = -ret;
            LOG(WARNING) << "[SharedUringRing] io_uring_register_buffers failed"
                         << " errno=" << err << " (" << strerror(err) << ")"
                         << " buf=" << b << " size=" << s
                         << " pages=" << (s >> 12)
                         << " — falling back to non-fixed-buffer I/O";
            buf_register_failed_ = true;
            return false;
        }
        buf_registered_ = true;
        buf_base_        = b;
        buf_size_        = s;
        LOG(INFO) << "[SharedUringRing] tid registered buffer addr=" << b
                  << " size=" << s;
        return true;
    }

    void unregister_buf_local() {
        if (!initialized_ || !buf_registered_) return;
        io_uring_unregister_buffers(&ring_);
        buf_registered_ = false;
        buf_base_        = nullptr;
        buf_size_        = 0;
    }

    // -----------------------------------------------------------------
    // I/O primitives  (no mutex — caller is the sole owner of this ring)
    // -----------------------------------------------------------------

    tl::expected<size_t, ErrorCode> read(int fd, void* buf, size_t len,
                                         off_t off) {
        ensure_buf_registered();
        bool fix = in_registered_buf(buf, len);
        return submit_rw(/*write=*/false, fd, buf, len, off, fix);
    }

    tl::expected<size_t, ErrorCode> write(int fd, const void* buf, size_t len,
                                          off_t off) {
        return submit_rw(/*write=*/true, fd,
                         const_cast<void*>(buf), len, off,
                         /*use_fixed_buf=*/false);
    }

    tl::expected<size_t, ErrorCode> vector_read(int fd, const iovec* iovs,
                                                int cnt, off_t off) {
        return submit_vector(/*write=*/false, fd, iovs, cnt, off);
    }

    tl::expected<size_t, ErrorCode> vector_write(int fd, const iovec* iovs,
                                                 int cnt, off_t off) {
        return submit_vector(/*write=*/true, fd, iovs, cnt, off);
    }

    // Descriptor for one independently-addressed read in a batch.
    struct ReadDesc {
        void*  buf;
        size_t len;
        off_t  off;
    };

    /// Submit up to QUEUE_DEPTH reads at once (each at its own offset), then
    /// collect completions. Repeat until all @p cnt descs are done.
    /// This gives the NVMe device queue depth > 1 within a single thread.
    tl::expected<size_t, ErrorCode> batch_read(int fd, const ReadDesc* descs,
                                               int cnt) {
        ensure_buf_registered();
        size_t total     = 0;
        int    remaining = cnt;
        int    idx       = 0;

        while (remaining > 0) {
            int batch = std::min(remaining, static_cast<int>(QUEUE_DEPTH));

            for (int i = 0; i < batch; ++i) {
                struct io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
                if (!sqe) {
                    LOG(ERROR) << "[SharedUringRing] SQ full (batch_read)";
                    return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
                }
                const auto& d = descs[idx + i];
                if (buf_registered_ && in_registered_buf(d.buf, d.len))
                    io_uring_prep_read_fixed(sqe, fd, d.buf, d.len, d.off, 0);
                else
                    io_uring_prep_read(sqe, fd, d.buf, d.len, d.off);
            }

            auto res = collect(batch);
            if (!res) return res;
            total     += res.value();
            idx       += batch;
            remaining -= batch;
        }
        return total;
    }

    /// Issue IORING_FSYNC_DATASYNC.  Blocks until complete.
    tl::expected<void, ErrorCode> fsync(int fd) {
        if (!initialized_) return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);

        struct io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
        if (!sqe) {
            LOG(ERROR) << "[SharedUringRing] SQ full (fsync)";
            return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
        }
        io_uring_prep_fsync(sqe, fd, IORING_FSYNC_DATASYNC);

        auto res = collect(1);
        if (!res) return tl::make_unexpected(res.error());
        return {};
    }

   private:
    // -----------------------------------------------------------------
    // Construction / destruction
    // -----------------------------------------------------------------

    SharedUringRing() {
        int ret = io_uring_queue_init(QUEUE_DEPTH, &ring_, 0);
        if (ret < 0) {
            LOG(ERROR) << "[SharedUringRing] io_uring_queue_init failed: "
                       << strerror(-ret);
            return;
        }
        initialized_ = true;
        LOG(INFO) << "[SharedUringRing] thread-local ring initialised "
                     "queue_depth=" << QUEUE_DEPTH;
    }

    ~SharedUringRing() {
        if (!initialized_) return;
        if (buf_registered_) io_uring_unregister_buffers(&ring_);
        io_uring_queue_exit(&ring_);
    }

    // -----------------------------------------------------------------
    // Internal helpers
    // -----------------------------------------------------------------

    bool in_registered_buf(const void* buf, size_t len) const {
        if (!buf_registered_ || !buf_base_ || !buf_size_) return false;
        uintptr_t ba = reinterpret_cast<uintptr_t>(buf);
        uintptr_t rb = reinterpret_cast<uintptr_t>(buf_base_);
        return ba >= rb && (ba + len) <= (rb + buf_size_);
    }

    static size_t next_pow2(size_t n) {
        if (n == 0) return 1;
        --n;
        n |= n >> 1; n |= n >> 2; n |= n >> 4;
        n |= n >> 8; n |= n >> 16; n |= n >> 32;
        return n + 1;
    }

    static size_t calc_chunk(size_t remaining, unsigned depth) {
        size_t s = (remaining + depth - 1) / depth;
        s = next_pow2(s);
        return std::max(s, MIN_CHUNK);
    }

    // Drain exactly @expected CQEs and accumulate bytes.
    tl::expected<size_t, ErrorCode> collect(int expected) {
        int ret = io_uring_submit_and_wait(&ring_, expected);
        if (ret < 0) {
            LOG(ERROR) << "[SharedUringRing] io_uring_submit_and_wait: "
                       << strerror(-ret);
            return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
        }
        size_t   total = 0;
        bool     err   = false;
        unsigned head, cnt = 0;
        struct io_uring_cqe* cqe;
        io_uring_for_each_cqe(&ring_, head, cqe) {
            if (cqe->res < 0) {
                LOG(ERROR) << "[SharedUringRing] CQE error: "
                           << strerror(-cqe->res);
                err = true;
            } else {
                total += static_cast<size_t>(cqe->res);
            }
            ++cnt;
        }
        io_uring_cq_advance(&ring_, cnt);
        if (err) return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
        return total;
    }

    // Chunked contiguous read or write.
    tl::expected<size_t, ErrorCode> submit_rw(bool is_write, int fd, void* buf,
                                               size_t len, off_t off,
                                               bool use_fixed_buf) {
        const ErrorCode err_code =
            is_write ? ErrorCode::FILE_WRITE_FAIL : ErrorCode::FILE_READ_FAIL;
        const bool fix_buf = (use_fixed_buf && buf_registered_);

        char*  ptr       = static_cast<char*>(buf);
        size_t total     = 0;
        size_t remaining = len;
        off_t  cur       = off;

        while (remaining > 0) {
            size_t   cs = calc_chunk(remaining, QUEUE_DEPTH);
            unsigned n  = std::min(
                static_cast<unsigned>((remaining + cs - 1) / cs), QUEUE_DEPTH);

            for (unsigned i = 0; i < n; ++i) {
                size_t chunk = std::min(cs, remaining);

                struct io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
                if (!sqe) {
                    LOG(ERROR) << "[SharedUringRing] SQ full";
                    return tl::make_unexpected(err_code);
                }

                if (is_write) {
                    if (fix_buf)
                        io_uring_prep_write_fixed(sqe, fd, ptr, chunk, cur, 0);
                    else
                        io_uring_prep_write(sqe, fd, ptr, chunk, cur);
                } else {
                    if (fix_buf)
                        io_uring_prep_read_fixed(sqe, fd, ptr, chunk, cur, 0);
                    else
                        io_uring_prep_read(sqe, fd, ptr, chunk, cur);
                }

                ptr       += chunk;
                cur       += static_cast<off_t>(chunk);
                remaining -= chunk;
                if (remaining == 0) break;
            }

            auto res = collect(static_cast<int>(n));
            if (!res) return res;
            total += res.value();
            if (!is_write && res.value() == 0) break;  // read EOF
            if (is_write && res.value() == 0) {
                LOG(ERROR) << "[SharedUringRing] zero bytes written";
                return tl::make_unexpected(ErrorCode::FILE_WRITE_FAIL);
            }
        }
        return total;
    }

    // Scatter/gather read or write (one SQE per iovec, sequential offsets).
    tl::expected<size_t, ErrorCode> submit_vector(bool is_write, int fd,
                                                  const iovec* iovs, int cnt,
                                                  off_t off) {
        const ErrorCode err_code =
            is_write ? ErrorCode::FILE_WRITE_FAIL : ErrorCode::FILE_READ_FAIL;

        size_t total     = 0;
        off_t  cur       = off;
        int    remaining = cnt;
        int    idx       = 0;

        while (remaining > 0) {
            int batch = std::min(remaining, static_cast<int>(QUEUE_DEPTH));

            for (int i = 0; i < batch; ++i) {
                struct io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
                if (!sqe) {
                    LOG(ERROR) << "[SharedUringRing] SQ full (vector)";
                    return tl::make_unexpected(err_code);
                }

                if (is_write)
                    io_uring_prep_write(sqe, fd, iovs[idx].iov_base,
                                        iovs[idx].iov_len, cur);
                else
                    io_uring_prep_read(sqe, fd, iovs[idx].iov_base,
                                       iovs[idx].iov_len, cur);

                cur += static_cast<off_t>(iovs[idx].iov_len);
                ++idx;
            }

            auto res = collect(batch);
            if (!res) return res;
            total     += res.value();
            remaining -= batch;
        }
        return total;
    }

    // -----------------------------------------------------------------
    // Data members
    // -----------------------------------------------------------------
    struct io_uring ring_{};
    bool initialized_    = false;

    bool   buf_registered_      = false;
    bool   buf_register_failed_ = false;  // set on first failure; skip retries
    void*  buf_base_            = nullptr;
    size_t buf_size_            = 0;
};

// ============================================================================
// UringFile — thin wrapper over SharedUringRing
// ============================================================================

UringFile::UringFile(const std::string& filename, int fd,
                     unsigned /*queue_depth*/, bool use_direct_io)
    : StorageFile(filename, fd),
      use_direct_io_(use_direct_io) {
    if (fd < 0) {
        error_code_ = ErrorCode::FILE_INVALID_HANDLE;
        return;
    }
    if (!SharedUringRing::instance().is_initialized()) {
        LOG(WARNING) << "[UringFile] thread-local ring not available for "
                     << filename;
    }
    if (use_direct_io_) {
        LOG(INFO) << "[UringFile] O_DIRECT mode enabled for " << filename;
    }
}

UringFile::~UringFile() {
    auto t0 = std::chrono::steady_clock::now();

    if (fd_ >= 0) {
        if (close(fd_) != 0) {
            LOG(WARNING) << "[UringFile] close failed: " << filename_;
        }
        if (error_code_ == ErrorCode::FILE_WRITE_FAIL) {
            if (::unlink(filename_.c_str()) == -1)
                LOG(ERROR) << "[UringFile] failed to delete corrupted file: "
                           << filename_;
            else
                LOG(INFO) << "[UringFile] deleted corrupted file: " << filename_;
        }
    }
    fd_ = -1;

    auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                          std::chrono::steady_clock::now() - t0)
                          .count();
    if (elapsed_ms > 1) {
        LOG(WARNING) << "[UringFile::~UringFile] cleanup took " << elapsed_ms
                     << "ms for " << filename_;
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

void* UringFile::alloc_aligned_buffer(size_t size) const {
    size_t aligned = ((size + ALIGNMENT_ - 1) / ALIGNMENT_) * ALIGNMENT_;
    void* ptr = nullptr;
    if (posix_memalign(&ptr, ALIGNMENT_, aligned) != 0) {
        LOG(ERROR) << "[UringFile] posix_memalign(" << aligned << ") failed";
        return nullptr;
    }
    return ptr;
}

void UringFile::free_aligned_buffer(void* ptr) const {
    if (ptr) free(ptr);
}

bool UringFile::in_registered_buffer(const void* buf, size_t len) const {
    auto& r = SharedUringRing::instance();
    if (!r.is_buffer_registered()) return false;
    uintptr_t ba = reinterpret_cast<uintptr_t>(buf);
    uintptr_t rb = reinterpret_cast<uintptr_t>(r.buffer_base());
    return ba >= rb && (ba + len) <= (rb + r.buffer_size());
}

// ---------------------------------------------------------------------------
// write
// ---------------------------------------------------------------------------

tl::expected<size_t, ErrorCode> UringFile::write(const std::string& buffer,
                                                  size_t length) {
    return write(std::span<const char>(buffer.data(), length), length);
}

tl::expected<size_t, ErrorCode> UringFile::write(std::span<const char> data,
                                                  size_t length) {
    if (fd_ < 0) return make_error<size_t>(ErrorCode::FILE_NOT_FOUND);
    if (length == 0) return make_error<size_t>(ErrorCode::FILE_INVALID_BUFFER);

    void*       bounce    = nullptr;
    const char* src       = data.data();
    size_t      write_len = length;

    if (use_direct_io_) {
        write_len = ((length + ALIGNMENT_ - 1) / ALIGNMENT_) * ALIGNMENT_;
        bounce    = alloc_aligned_buffer(write_len);
        if (!bounce) return make_error<size_t>(ErrorCode::FILE_WRITE_FAIL);
        std::memcpy(bounce, data.data(), length);
        if (write_len > length)
            std::memset(static_cast<char*>(bounce) + length, 0,
                        write_len - length);
        src = static_cast<char*>(bounce);
    }

    auto res = SharedUringRing::instance().write(fd_, src, write_len, 0);

    if (bounce) free_aligned_buffer(bounce);

    if (!res) return make_error<size_t>(res.error());
    if (res.value() < length)
        return make_error<size_t>(ErrorCode::FILE_WRITE_FAIL);
    return length;
}

// ---------------------------------------------------------------------------
// read
// ---------------------------------------------------------------------------

tl::expected<size_t, ErrorCode> UringFile::read(std::string& buffer,
                                                 size_t length) {
    if (fd_ < 0) return make_error<size_t>(ErrorCode::FILE_NOT_FOUND);
    if (length == 0) return make_error<size_t>(ErrorCode::FILE_INVALID_BUFFER);

    void*  bounce   = nullptr;
    char*  read_ptr = nullptr;
    size_t read_len = length;

    if (use_direct_io_) {
        read_len = ((length + ALIGNMENT_ - 1) / ALIGNMENT_) * ALIGNMENT_;
        bounce   = alloc_aligned_buffer(read_len);
        if (!bounce) return make_error<size_t>(ErrorCode::FILE_READ_FAIL);
        read_ptr = static_cast<char*>(bounce);
    } else {
        buffer.resize(length);
        read_ptr = buffer.data();
    }

    auto res = SharedUringRing::instance().read(fd_, read_ptr, read_len, 0);

    if (use_direct_io_) {
        if (res) {
            size_t actual = std::min(res.value(), length);
            buffer.assign(static_cast<char*>(bounce), actual);
        }
        free_aligned_buffer(bounce);
    }

    if (!res) return make_error<size_t>(res.error());
    size_t got = use_direct_io_ ? std::min(res.value(), length) : res.value();
    if (!use_direct_io_) buffer.resize(got);
    if (got == 0) return make_error<size_t>(ErrorCode::FILE_READ_FAIL);
    return got;
}

// ---------------------------------------------------------------------------
// write_aligned / read_aligned
// ---------------------------------------------------------------------------

tl::expected<size_t, ErrorCode> UringFile::write_aligned(const void* buffer,
                                                          size_t length,
                                                          off_t offset) {
    if (fd_ < 0) return make_error<size_t>(ErrorCode::FILE_NOT_FOUND);
    if (!buffer || length == 0)
        return make_error<size_t>(ErrorCode::FILE_INVALID_BUFFER);

    if (use_direct_io_) {
        if (reinterpret_cast<uintptr_t>(buffer) % ALIGNMENT_)
            return make_error<size_t>(ErrorCode::FILE_INVALID_BUFFER);
        if (length % ALIGNMENT_ || offset % ALIGNMENT_)
            return make_error<size_t>(ErrorCode::FILE_INVALID_BUFFER);
    }

    return SharedUringRing::instance().write(fd_, buffer, length, offset);
}

tl::expected<size_t, ErrorCode> UringFile::read_aligned(void* buffer,
                                                         size_t length,
                                                         off_t offset) {
    if (fd_ < 0) return make_error<size_t>(ErrorCode::FILE_NOT_FOUND);
    if (!buffer || length == 0)
        return make_error<size_t>(ErrorCode::FILE_INVALID_BUFFER);

    if (use_direct_io_) {
        if (reinterpret_cast<uintptr_t>(buffer) % ALIGNMENT_)
            return make_error<size_t>(ErrorCode::FILE_INVALID_BUFFER);
        if (length % ALIGNMENT_ || offset % ALIGNMENT_)
            return make_error<size_t>(ErrorCode::FILE_INVALID_BUFFER);
    }

    return SharedUringRing::instance().read(fd_, buffer, length, offset);
}

// ---------------------------------------------------------------------------
// batch_read — submit multiple independent reads in one ring submission
// ---------------------------------------------------------------------------

tl::expected<size_t, ErrorCode> UringFile::batch_read(const ReadDesc* descs,
                                                       int cnt) {
    if (fd_ < 0) return make_error<size_t>(ErrorCode::FILE_NOT_FOUND);
    if (!descs || cnt <= 0)
        return make_error<size_t>(ErrorCode::FILE_INVALID_BUFFER);

    // Map UringFile::ReadDesc → SharedUringRing::ReadDesc (same layout, but
    // ensure they stay in sync if either changes).
    static_assert(sizeof(ReadDesc) == sizeof(SharedUringRing::ReadDesc),
                  "ReadDesc layout mismatch");
    const auto* ring_descs =
        reinterpret_cast<const SharedUringRing::ReadDesc*>(descs);
    return SharedUringRing::instance().batch_read(fd_, ring_descs, cnt);
}

// ---------------------------------------------------------------------------
// vector_write / vector_read
// ---------------------------------------------------------------------------

tl::expected<size_t, ErrorCode> UringFile::vector_write(const iovec* iov,
                                                         int iovcnt,
                                                         off_t offset) {
    if (fd_ < 0) return make_error<size_t>(ErrorCode::FILE_NOT_FOUND);
    auto start = std::chrono::steady_clock::now();
    auto res = SharedUringRing::instance().vector_write(fd_, iov, iovcnt,
                                                        offset);
    auto us = std::chrono::duration_cast<std::chrono::microseconds>(
                  std::chrono::steady_clock::now() - start)
                  .count();
    if (us > 1000)
        LOG(INFO) << "[UringFile::vector_write] fd=" << fd_
                  << " iovcnt=" << iovcnt << " time=" << us << "us";
    return res;
}

tl::expected<size_t, ErrorCode> UringFile::vector_read(const iovec* iov,
                                                        int iovcnt,
                                                        off_t offset) {
    if (fd_ < 0) return make_error<size_t>(ErrorCode::FILE_NOT_FOUND);

    size_t expected_bytes = 0;
    for (int i = 0; i < iovcnt; ++i) expected_bytes += iov[i].iov_len;
    auto start = std::chrono::steady_clock::now();

    auto res = SharedUringRing::instance().vector_read(fd_, iov, iovcnt,
                                                       offset);

    auto us = std::chrono::duration_cast<std::chrono::microseconds>(
                  std::chrono::steady_clock::now() - start)
                  .count();
    if (us > 1000 || expected_bytes > 1024 * 1024) {
        double mbps = (us > 0)
                          ? (static_cast<double>(expected_bytes) / 1048576.0) /
                                (static_cast<double>(us) / 1e6)
                          : 0;
        LOG(INFO) << "[UringFile::vector_read] fd=" << fd_
                  << " iovcnt=" << iovcnt << " bytes=" << expected_bytes
                  << " time=" << us << "us (" << (us / 1000.0) << "ms)"
                  << " throughput=" << mbps << "MB/s";
    }

    if (!res) {
        LOG(ERROR) << "[UringFile::vector_read] FAILED fd=" << fd_
                   << " offset=" << offset << " iovcnt=" << iovcnt
                   << " expected=" << expected_bytes;
    }
    return res;
}

// ---------------------------------------------------------------------------
// datasync
// ---------------------------------------------------------------------------

tl::expected<void, ErrorCode> UringFile::datasync() {
    if (fd_ < 0) return make_error<void>(ErrorCode::FILE_NOT_FOUND);
    auto res = SharedUringRing::instance().fsync(fd_);
    if (!res) {
        LOG(ERROR) << "[UringFile::datasync] fsync failed for: " << filename_;
        return make_error<void>(ErrorCode::FILE_WRITE_FAIL);
    }
    return {};
}

// ---------------------------------------------------------------------------
// Buffer registration
// ---------------------------------------------------------------------------

bool UringFile::register_global_buffer(void* buffer, size_t length) {
    if (!buffer || length == 0) {
        LOG(ERROR) << "[UringFile::register_global_buffer] invalid buffer or length";
        return false;
    }
    // Disable Transparent Huge Pages on this region before pinning.
    // io_uring uses FOLL_LONGTERM to pin pages; on kernel 5.15 the kernel
    // must split any 2MB THP into 4KB pages before long-term pinning, which
    // can fail (ENOMEM) when huge pages are in use or memory is fragmented.
    // MADV_NOHUGEPAGE prevents the kernel from backing this range with THPs,
    // making pin_user_pages() reliable regardless of system THP policy.
    if (madvise(buffer, length, MADV_NOHUGEPAGE) != 0) {
        LOG(WARNING) << "[UringFile::register_global_buffer] madvise(NOHUGEPAGE)"
                     << " failed errno=" << errno << " (" << strerror(errno)
                     << ") — continuing anyway";
    }
    g_buf.base.store(buffer, std::memory_order_release);
    g_buf.size.store(length, std::memory_order_release);
    bool ok = SharedUringRing::instance().ensure_buf_registered();
    if (ok) {
        LOG(INFO) << "[UringFile::register_global_buffer] registered"
                  << " addr=" << buffer << " size=" << length
                  << " pages=" << (length >> 12);
    } else {
        LOG(WARNING) << "[UringFile::register_global_buffer] registration failed"
                     << " addr=" << buffer << " size=" << length
                     << " — I/O will use regular (non-fixed-buffer) io_uring,"
                     << " which is correct but slightly less optimal";
    }
    return ok;
}

void UringFile::unregister_global_buffer() {
    g_buf.base.store(nullptr, std::memory_order_release);
    g_buf.size.store(0, std::memory_order_release);
    SharedUringRing::instance().unregister_buf_local();
}

bool UringFile::register_buffer(void* buffer, size_t length) {
    if (!buffer || length == 0) {
        LOG(ERROR) << "[UringFile::register_buffer] invalid buffer or length";
        return false;
    }
    if (use_direct_io_) {
        if (reinterpret_cast<uintptr_t>(buffer) % ALIGNMENT_) {
            LOG(ERROR) << "[UringFile::register_buffer] buffer not aligned";
            return false;
        }
        if (length % ALIGNMENT_) {
            LOG(ERROR) << "[UringFile::register_buffer] length not aligned";
            return false;
        }
    }
    // Publish globally; each thread-local ring registers lazily on first I/O.
    g_buf.base.store(buffer, std::memory_order_release);
    g_buf.size.store(length, std::memory_order_release);
    // Register on the calling thread's ring immediately.
    bool ok = SharedUringRing::instance().ensure_buf_registered();
    LOG(INFO) << "[UringFile::register_buffer] addr=" << buffer
              << " size=" << length << " calling-thread-registered=" << ok;
    return ok;
}

void UringFile::unregister_buffer() {
    // Clear the global state so no new thread picks it up.
    g_buf.base.store(nullptr, std::memory_order_release);
    g_buf.size.store(0, std::memory_order_release);
    // Unregister on the calling thread's ring.
    SharedUringRing::instance().unregister_buf_local();
}

bool UringFile::is_buffer_registered() const {
    return SharedUringRing::instance().is_buffer_registered();
}


}  // namespace mooncake

#endif  // USE_URING
