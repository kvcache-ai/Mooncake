#ifdef USE_URING

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <mutex>
#include <string>
#include <sys/uio.h>
#include <unistd.h>
#include <vector>

#include <glog/logging.h>
#include <liburing.h>

#include "file_interface.h"

namespace mooncake {

// ============================================================================
// SharedUringRing — process-wide io_uring ring shared by all UringFile instances
//
// Design goals:
//  - One ring per process: io_uring_queue_init / io_uring_queue_exit happen
//    exactly once, eliminating the per-file mmap/munmap + TLB-shootdown cost.
//  - Dynamic fd registration: alloc_slot() / free_slot() call
//    io_uring_register_files_update(), which is a lightweight syscall that
//    does NOT tear down the ring.
//  - Thread safety: ring_mu_ serialises all SQ/CQ operations. slot_mu_
//    protects the slot-allocator metadata independently.
// ============================================================================
class SharedUringRing {
   public:
    static constexpr unsigned QUEUE_DEPTH   = 32;
    static constexpr int      MAX_FILES     = 1024;
    static constexpr size_t   MIN_CHUNK     = 4096;

    static SharedUringRing& instance() {
        static SharedUringRing s_ring;
        return s_ring;
    }

    SharedUringRing(const SharedUringRing&)            = delete;
    SharedUringRing& operator=(const SharedUringRing&) = delete;

    bool is_initialized()     const { return initialized_; }
    bool files_registered()   const { return files_registered_; }
    bool is_buffer_registered() const { return buf_registered_; }
    void* buffer_base()       const { return buf_base_; }
    size_t buffer_size()      const { return buf_size_; }

    // -----------------------------------------------------------------
    // Slot management
    // -----------------------------------------------------------------

    /// Register @p fd into a free slot in the kernel's registered-files table.
    /// Returns slot index >= 0 on success, -1 on failure.
    int alloc_slot(int fd) {
        if (!initialized_ || !files_registered_) return -1;

        std::lock_guard<std::mutex> lk(slot_mu_);
        int slot = -1;
        if (!free_slots_.empty()) {
            slot = free_slots_.back();
            free_slots_.pop_back();
        } else if (watermark_ < MAX_FILES) {
            slot = watermark_++;
        } else {
            LOG(ERROR) << "[SharedUringRing] registered-files table full ("
                       << MAX_FILES << " slots)";
            return -1;
        }

        int ret = io_uring_register_files_update(&ring_, slot, &fd, 1);
        if (ret < 0) {
            LOG(WARNING) << "[SharedUringRing] io_uring_register_files_update "
                            "failed (slot="
                         << slot << "): " << strerror(-ret)
                         << " — fd will be used directly";
            free_slots_.push_back(slot);
            return -1;
        }
        return slot;
    }

    /// Release slot back to the pool (writes -1 into the kernel's table).
    void free_slot(int slot) {
        if (slot < 0 || !initialized_ || !files_registered_) return;
        std::lock_guard<std::mutex> lk(slot_mu_);
        int minus_one = -1;
        io_uring_register_files_update(&ring_, slot, &minus_one, 1);
        free_slots_.push_back(slot);
    }

    // -----------------------------------------------------------------
    // Buffer registration (at most one global buffer at a time)
    // -----------------------------------------------------------------

    bool register_buffer(void* buf, size_t len) {
        if (!initialized_) return false;
        std::lock_guard<std::mutex> lk(ring_mu_);
        if (buf_registered_) {
            LOG(WARNING) << "[SharedUringRing] a buffer is already registered";
            return false;
        }
        struct iovec iov{buf, len};
        int ret = io_uring_register_buffers(&ring_, &iov, 1);
        if (ret < 0) {
            LOG(ERROR) << "[SharedUringRing] io_uring_register_buffers failed: "
                       << strerror(-ret);
            return false;
        }
        buf_registered_ = true;
        buf_base_       = buf;
        buf_size_       = len;
        LOG(INFO) << "[SharedUringRing] registered buffer addr=" << buf
                  << " size=" << len;
        return true;
    }

    void unregister_buffer() {
        if (!initialized_ || !buf_registered_) return;
        std::lock_guard<std::mutex> lk(ring_mu_);
        io_uring_unregister_buffers(&ring_);
        buf_registered_ = false;
        buf_base_       = nullptr;
        buf_size_       = 0;
    }

    // -----------------------------------------------------------------
    // I/O primitives — all thread-safe via ring_mu_
    // -----------------------------------------------------------------

    tl::expected<size_t, ErrorCode> read(int slot, int raw_fd, void* buf,
                                         size_t len, off_t off,
                                         bool use_fixed_buf) {
        return submit_rw(/*write=*/false, slot, raw_fd, buf, len, off,
                         use_fixed_buf);
    }

    tl::expected<size_t, ErrorCode> write(int slot, int raw_fd,
                                          const void* buf, size_t len,
                                          off_t off, bool use_fixed_buf) {
        return submit_rw(/*write=*/true, slot, raw_fd,
                         // cast away const — io_uring_prep_write takes void*
                         const_cast<void*>(buf), len, off, use_fixed_buf);
    }

    tl::expected<size_t, ErrorCode> vector_read(int slot, int raw_fd,
                                                const iovec* iovs, int cnt,
                                                off_t off) {
        return submit_vector(/*write=*/false, slot, raw_fd, iovs, cnt, off);
    }

    tl::expected<size_t, ErrorCode> vector_write(int slot, int raw_fd,
                                                 const iovec* iovs, int cnt,
                                                 off_t off) {
        return submit_vector(/*write=*/true, slot, raw_fd, iovs, cnt, off);
    }

   private:
    // -----------------------------------------------------------------
    // Construction / destruction
    // -----------------------------------------------------------------

    SharedUringRing() {
        std::fill(fd_table_, fd_table_ + MAX_FILES, -1);

        int ret = io_uring_queue_init(QUEUE_DEPTH, &ring_, 0);
        if (ret < 0) {
            LOG(ERROR) << "[SharedUringRing] io_uring_queue_init failed: "
                       << strerror(-ret);
            return;
        }

        // Pre-register a table of MAX_FILES slots (all -1 = empty).
        // Individual fds are inserted later via io_uring_register_files_update.
        ret = io_uring_register_files(&ring_, fd_table_, MAX_FILES);
        if (ret < 0) {
            LOG(WARNING) << "[SharedUringRing] io_uring_register_files failed: "
                         << strerror(-ret)
                         << " — continuing without registered files";
            files_registered_ = false;
        } else {
            files_registered_ = true;
        }

        initialized_ = true;
        LOG(INFO) << "[SharedUringRing] initialised  queue_depth=" << QUEUE_DEPTH
                  << "  max_files=" << MAX_FILES
                  << "  files_registered=" << files_registered_;
    }

    ~SharedUringRing() {
        if (!initialized_) return;
        // buffer and files are cleaned up automatically when the ring fd is
        // closed inside io_uring_queue_exit, but unregister explicitly so the
        // sequence is clear.
        if (buf_registered_)    io_uring_unregister_buffers(&ring_);
        if (files_registered_)  io_uring_unregister_files(&ring_);
        io_uring_queue_exit(&ring_);
    }

    // -----------------------------------------------------------------
    // Internal helpers
    // -----------------------------------------------------------------

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

    // Drain exactly @expected CQEs and accumulate bytes. Caller holds ring_mu_.
    tl::expected<size_t, ErrorCode> collect(int expected) {
        int ret = io_uring_submit_and_wait(&ring_, expected);
        if (ret < 0) {
            LOG(ERROR) << "[SharedUringRing] io_uring_submit_and_wait: "
                       << strerror(-ret);
            return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
        }
        size_t total = 0;
        bool   err   = false;
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
    tl::expected<size_t, ErrorCode> submit_rw(bool is_write, int slot,
                                               int raw_fd, void* buf,
                                               size_t len, off_t off,
                                               bool use_fixed_buf) {
        const bool use_slot = (files_registered_ && slot >= 0);
        const int  target   = use_slot ? slot : raw_fd;
        const bool fix_buf  = (use_fixed_buf && buf_registered_);
        const ErrorCode err_code =
            is_write ? ErrorCode::FILE_WRITE_FAIL : ErrorCode::FILE_READ_FAIL;

        std::lock_guard<std::mutex> lk(ring_mu_);

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
                        io_uring_prep_write_fixed(sqe, target, ptr, chunk, cur, 0);
                    else
                        io_uring_prep_write(sqe, target, ptr, chunk, cur);
                } else {
                    if (fix_buf)
                        io_uring_prep_read_fixed(sqe, target, ptr, chunk, cur, 0);
                    else
                        io_uring_prep_read(sqe, target, ptr, chunk, cur);
                }
                if (use_slot) io_uring_sqe_set_flags(sqe, IOSQE_FIXED_FILE);

                ptr       += chunk;
                cur       += static_cast<off_t>(chunk);
                remaining -= chunk;
                if (remaining == 0) break;
            }

            auto res = collect(static_cast<int>(n));
            if (!res) return res;
            total += res.value();
            if (res.value() == 0) break;  // EOF
        }
        return total;
    }

    // Scatter/gather read or write (one SQE per iovec entry).
    tl::expected<size_t, ErrorCode> submit_vector(bool is_write, int slot,
                                                  int raw_fd,
                                                  const iovec* iovs, int cnt,
                                                  off_t off) {
        const bool use_slot = (files_registered_ && slot >= 0);
        const int  target   = use_slot ? slot : raw_fd;
        const ErrorCode err_code =
            is_write ? ErrorCode::FILE_WRITE_FAIL : ErrorCode::FILE_READ_FAIL;

        std::lock_guard<std::mutex> lk(ring_mu_);

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
                    io_uring_prep_write(sqe, target, iovs[idx].iov_base,
                                        iovs[idx].iov_len, cur);
                else
                    io_uring_prep_read(sqe, target, iovs[idx].iov_base,
                                       iovs[idx].iov_len, cur);

                if (use_slot) io_uring_sqe_set_flags(sqe, IOSQE_FIXED_FILE);
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
    bool initialized_     = false;
    bool files_registered_ = false;

    std::mutex ring_mu_;   // Serialises SQ/CQ access

    std::mutex       slot_mu_;
    int              fd_table_[MAX_FILES];
    std::vector<int> free_slots_;
    int              watermark_ = 0;

    bool   buf_registered_ = false;
    void*  buf_base_       = nullptr;
    size_t buf_size_       = 0;
};

// ============================================================================
// UringFile — thin wrapper over SharedUringRing
// ============================================================================

UringFile::UringFile(const std::string& filename, int fd,
                     unsigned /*queue_depth*/, bool use_direct_io)
    : StorageFile(filename, fd),
      slot_index_(-1),
      use_direct_io_(use_direct_io) {
    if (fd < 0) {
        error_code_ = ErrorCode::FILE_INVALID_HANDLE;
        return;
    }

    auto& ring = SharedUringRing::instance();
    if (!ring.is_initialized()) {
        LOG(WARNING) << "[UringFile] SharedUringRing not available — "
                        "fd will be used directly";
        // Still usable: slot_index_ stays -1, raw fd used in every SQE
    } else {
        slot_index_ = ring.alloc_slot(fd);
        if (slot_index_ < 0) {
            LOG(WARNING) << "[UringFile] Could not alloc slot for fd=" << fd
                         << " (" << filename << ") — using raw fd";
        }
    }

    if (use_direct_io_) {
        LOG(INFO) << "[UringFile] O_DIRECT mode enabled for " << filename;
    }
}

UringFile::~UringFile() {
    auto t0 = std::chrono::steady_clock::now();

    // Release the registered-files slot — no ring teardown, just a syscall
    // that writes -1 into one slot of the kernel's file table.
    SharedUringRing::instance().free_slot(slot_index_);
    slot_index_ = -1;

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
    auto& ring = SharedUringRing::instance();
    if (!ring.is_buffer_registered()) return false;
    uintptr_t ba  = reinterpret_cast<uintptr_t>(buf);
    uintptr_t rb  = reinterpret_cast<uintptr_t>(ring.buffer_base());
    return ba >= rb && (ba + len) <= (rb + ring.buffer_size());
}

// ---------------------------------------------------------------------------
// write — arbitrary (possibly unaligned) span
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

    auto res = SharedUringRing::instance().write(
        slot_index_, fd_, src, write_len, 0, /*use_fixed_buf=*/false);

    if (bounce) free_aligned_buffer(bounce);

    if (!res) return make_error<size_t>(res.error());
    if (res.value() < length)
        return make_error<size_t>(ErrorCode::FILE_WRITE_FAIL);
    return length;
}

// ---------------------------------------------------------------------------
// read — reads into a std::string (allocates or uses aligned bounce)
// ---------------------------------------------------------------------------

tl::expected<size_t, ErrorCode> UringFile::read(std::string& buffer,
                                                 size_t length) {
    if (fd_ < 0) return make_error<size_t>(ErrorCode::FILE_NOT_FOUND);
    if (length == 0) return make_error<size_t>(ErrorCode::FILE_INVALID_BUFFER);

    void*  bounce     = nullptr;
    char*  read_ptr   = nullptr;
    size_t read_len   = length;

    if (use_direct_io_) {
        read_len = ((length + ALIGNMENT_ - 1) / ALIGNMENT_) * ALIGNMENT_;
        bounce   = alloc_aligned_buffer(read_len);
        if (!bounce) return make_error<size_t>(ErrorCode::FILE_READ_FAIL);
        read_ptr = static_cast<char*>(bounce);
    } else {
        buffer.resize(length);
        read_ptr = buffer.data();
    }

    auto res = SharedUringRing::instance().read(
        slot_index_, fd_, read_ptr, read_len, 0, /*use_fixed_buf=*/false);

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
// write_aligned / read_aligned — zero-copy O_DIRECT interface
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

    bool fix = in_registered_buffer(buffer, length);
    return SharedUringRing::instance().write(slot_index_, fd_, buffer, length,
                                             offset, fix);
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

    bool fix = in_registered_buffer(buffer, length);
    return SharedUringRing::instance().read(slot_index_, fd_, buffer, length,
                                            offset, fix);
}

// ---------------------------------------------------------------------------
// vector_write / vector_read
// ---------------------------------------------------------------------------

tl::expected<size_t, ErrorCode> UringFile::vector_write(const iovec* iov,
                                                         int iovcnt,
                                                         off_t offset) {
    if (fd_ < 0) return make_error<size_t>(ErrorCode::FILE_NOT_FOUND);
    auto start = std::chrono::steady_clock::now();
    auto res = SharedUringRing::instance().vector_write(slot_index_, fd_, iov,
                                                        iovcnt, offset);
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

    auto res = SharedUringRing::instance().vector_read(slot_index_, fd_, iov,
                                                       iovcnt, offset);

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
// Buffer registration — delegates to the shared ring
// ---------------------------------------------------------------------------

bool UringFile::register_buffer(void* buffer, size_t length) {
    auto& ring = SharedUringRing::instance();
    if (!ring.is_initialized()) {
        LOG(ERROR) << "[UringFile::register_buffer] SharedUringRing not ready";
        return false;
    }
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
    return ring.register_buffer(buffer, length);
}

void UringFile::unregister_buffer() {
    SharedUringRing::instance().unregister_buffer();
}

bool UringFile::is_buffer_registered() const {
    return SharedUringRing::instance().is_buffer_registered();
}

}  // namespace mooncake

#endif  // USE_URING
