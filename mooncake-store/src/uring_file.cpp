#ifdef USE_URING

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <climits>
#include <cstring>
#include <memory>
#include <string>
#include <sys/mman.h>
#include <sys/uio.h>
#include <thread>
#include <unistd.h>
#include <vector>

#include <glog/logging.h>
#include <liburing.h>

#include "file_interface.h"
#include "uring_submit.h"

namespace mooncake {

// ============================================================================
// GlobalBufInfo — process-wide buffer registration state.
//
// register_buffer() is called once from the storage-backend init path.
// Each thread-local ring picks up the registration lazily on first I/O.
// ============================================================================
struct GlobalBufInfo {
    std::atomic<void*> base{nullptr};
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
    static constexpr size_t MIN_CHUNK = 4096;

    static SharedUringRing& instance() {
        thread_local SharedUringRing tl_ring;
        return tl_ring;
    }

    SharedUringRing(const SharedUringRing&) = delete;
    SharedUringRing& operator=(const SharedUringRing&) = delete;

    bool is_initialized() const { return initialized_; }
    // errno of the first failed CQE of the last I/O call; 0 if none failed.
    int cqe_errno() const { return cqe_errno_; }
    bool is_buffer_registered() const { return buf_registered_; }
    void* buffer_base() const { return buf_base_; }
    size_t buffer_size() const { return buf_size_; }

    // -----------------------------------------------------------------
    // Buffer registration
    // -----------------------------------------------------------------

    // Register a buffer on THIS thread's ring.  Called lazily from each
    // thread before the first fixed-buffer I/O.
    bool ensure_buf_registered() {
        if (buf_registered_) return true;
        if (buf_register_failed_) return false;  // don't retry after failure
        if (!initialized_) return false;
        void* b = g_buf.base.load(std::memory_order_acquire);
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
        buf_base_ = b;
        buf_size_ = s;
        LOG(INFO) << "[SharedUringRing] tid registered buffer addr=" << b
                  << " size=" << s;
        return true;
    }

    void unregister_buf_local() {
        if (!initialized_ || !buf_registered_) return;
        io_uring_unregister_buffers(&ring_);
        buf_registered_ = false;
        buf_base_ = nullptr;
        buf_size_ = 0;
    }

    // -----------------------------------------------------------------
    // I/O primitives  (no mutex — caller is the sole owner of this ring)
    // -----------------------------------------------------------------

    tl::expected<size_t, ErrorCode> read(int fd, void* buf, size_t len,
                                         off_t off) {
        cqe_errno_ = 0;
        if (!initialized_)
            return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
        ensure_buf_registered();
        bool fix = in_registered_buf(buf, len);
        return submit_rw(/*write=*/false, fd, buf, len, off, fix);
    }

    tl::expected<size_t, ErrorCode> write(int fd, const void* buf, size_t len,
                                          off_t off) {
        cqe_errno_ = 0;
        if (!initialized_)
            return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
        return submit_rw(/*write=*/true, fd, const_cast<void*>(buf), len, off,
                         /*use_fixed_buf=*/false);
    }

    tl::expected<size_t, ErrorCode> vector_read(int fd, const iovec* iovs,
                                                int cnt, off_t off) {
        cqe_errno_ = 0;
        if (!initialized_)
            return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
        return submit_vector(/*write=*/false, fd, iovs, cnt, off);
    }

    tl::expected<size_t, ErrorCode> vector_write(int fd, const iovec* iovs,
                                                 int cnt, off_t off) {
        cqe_errno_ = 0;
        if (!initialized_)
            return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
        return submit_vector(/*write=*/true, fd, iovs, cnt, off);
    }

    // Descriptor for one independently-addressed read in a batch.
    using ReadDesc = UringFile::ReadDesc;

    /// Submit up to QUEUE_DEPTH reads at once (each at its own offset), then
    /// collect completions. Repeat until all @p cnt descs are done.
    /// This gives the NVMe device queue depth > 1 within a single thread.
    tl::expected<void, ErrorCode> batch_read(int fd, ReadDesc* descs, int cnt) {
        cqe_errno_ = 0;
        if (!initialized_)
            return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
        ensure_buf_registered();
        for (int i = 0; i < cnt; ++i) {
            descs[i].bytes_read = 0;
            descs[i].error = ErrorCode::OK;
            descs[i].completed = false;
        }
        int remaining = cnt;
        int idx = 0;

        while (remaining > 0) {
            int batch = std::min(remaining, static_cast<int>(QUEUE_DEPTH));
            if (io_uring_sq_space_left(&ring_) < static_cast<unsigned>(batch)) {
                LOG(ERROR) << "[SharedUringRing] insufficient SQ space for "
                           << batch << " batch reads";
                return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
            }
            uint64_t op = next_operation_tag();

            for (int i = 0; i < batch; ++i) {
                struct io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
                if (!sqe) {
                    LOG(ERROR) << "[SharedUringRing] SQ full (batch_read)";
                    return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
                }
                auto& d = descs[idx + i];
                if (buf_registered_ && in_registered_buf(d.buf, d.len))
                    io_uring_prep_read_fixed(sqe, fd, d.buf, d.len, d.off, 0);
                else
                    io_uring_prep_read(sqe, fd, d.buf, d.len, d.off);
                sqe->user_data = op | static_cast<uint64_t>(i + 1);
            }

            auto res = collect_batch(batch, op, descs + idx);
            if (!res) return res;
            for (int i = 0; i < batch; ++i) {
                auto& d = descs[idx + i];
                auto done = finish_short(/*write=*/false, fd, d,
                                         in_registered_buf(d.buf, d.len));
                if (!done) {
                    d.error = ErrorCode::FILE_READ_FAIL;
                    return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
                }
                d.bytes_read = done.value();
            }
            idx += batch;
            remaining -= batch;
        }
        return {};
    }

    /// Issue IORING_FSYNC_DATASYNC.  Blocks until complete.
    tl::expected<void, ErrorCode> fsync(int fd) {
        cqe_errno_ = 0;
        if (!initialized_)
            return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);

        struct io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
        if (!sqe) {
            LOG(ERROR) << "[SharedUringRing] SQ full (fsync)";
            return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
        }
        uint64_t op = next_operation_tag();
        io_uring_prep_fsync(sqe, fd, IORING_FSYNC_DATASYNC);
        sqe->user_data = op;

        auto res = collect(1, op);
        if (!res) return tl::make_unexpected(res.error());
        return {};
    }

   private:
    // -----------------------------------------------------------------
    // Construction / destruction
    // -----------------------------------------------------------------

    SharedUringRing() { initialize_ring(); }

    ~SharedUringRing() { shutdown_ring(); }

    // -----------------------------------------------------------------
    // Internal helpers
    // -----------------------------------------------------------------

    bool initialize_ring() {
        ring_ = {};
        int ret = io_uring_queue_init(QUEUE_DEPTH, &ring_, 0);
        if (ret < 0) {
            LOG(ERROR) << "[SharedUringRing] io_uring_queue_init failed: "
                       << strerror(-ret);
            return false;
        }
        initialized_ = true;
        LOG(INFO) << "[SharedUringRing] thread-local ring initialised "
                     "queue_depth="
                  << QUEUE_DEPTH;
        return true;
    }

    void shutdown_ring() {
        if (!initialized_) return;
        if (buf_registered_) io_uring_unregister_buffers(&ring_);
        io_uring_queue_exit(&ring_);
        initialized_ = false;
        buf_registered_ = false;
        buf_base_ = nullptr;
        buf_size_ = 0;
    }

    void reset_ring() {
        shutdown_ring();
        if (!initialize_ring()) {
            LOG(ERROR) << "[SharedUringRing] failed to recover io_uring";
        }
    }

    detail::UringSubmitResult submit_pending() {
        return detail::submit_all_pending(
            [this] { return io_uring_sq_ready(&ring_); },
            [this](unsigned pending) {
                return io_uring_submit_and_wait(&ring_, pending);
            },
            [] { std::this_thread::yield(); });
    }

    bool wait_cqe(struct io_uring_cqe** cqe) {
        unsigned transient_retries = 0;
        while (true) {
            int ret = io_uring_peek_cqe(&ring_, cqe);
            if (ret == -EAGAIN) ret = io_uring_wait_cqe(&ring_, cqe);
            if (ret == 0) return true;
            if ((ret == -EINTR || ret == -EAGAIN) && transient_retries++ < 64) {
                std::this_thread::yield();
                continue;
            }
            LOG(ERROR) << "[SharedUringRing] CQE wait error: "
                       << strerror(-ret);
            return false;
        }
    }

    bool drain_submitted(unsigned submitted, uint64_t op_id) {
        unsigned processed = 0;
        while (processed < submitted) {
            struct io_uring_cqe* cqe;
            if (!wait_cqe(&cqe)) return false;
            if ((cqe->user_data & ~BATCH_INDEX_MASK) == op_id) ++processed;
            io_uring_cq_advance(&ring_, 1);
        }
        return true;
    }

    bool prepare_completions(int expected, uint64_t op_id) {
        auto submit = submit_pending();
        if (submit.error == 0 && submit.pending == 0 &&
            submit.submitted == static_cast<unsigned>(expected)) {
            return true;
        }

        LOG(ERROR) << "[SharedUringRing] io_uring submission incomplete: "
                   << "expected=" << expected
                   << " submitted=" << submit.submitted
                   << " pending=" << submit.pending
                   << " error=" << submit.error;
        drain_submitted(submit.submitted, op_id);
        reset_ring();
        return false;
    }

    bool in_registered_buf(const void* buf, size_t len) const {
        if (!buf_registered_ || !buf_base_ || !buf_size_) return false;
        uintptr_t buf_addr = reinterpret_cast<uintptr_t>(buf);
        uintptr_t rb = reinterpret_cast<uintptr_t>(buf_base_);
        return buf_addr >= rb && (buf_addr + len) <= (rb + buf_size_);
    }

    static size_t next_pow2(size_t n) {
        if (n == 0) return 1;
        --n;
        n |= n >> 1;
        n |= n >> 2;
        n |= n >> 4;
        n |= n >> 8;
        n |= n >> 16;
        n |= n >> 32;
        return n + 1;
    }

    static size_t calc_chunk(size_t remaining, unsigned depth) {
        size_t s = (remaining + depth - 1) / depth;
        s = next_pow2(s);
        return std::max(s, MIN_CHUNK);
    }

    static size_t max_rw_count() {
        static const size_t value = [] {
            long page_size = sysconf(_SC_PAGESIZE);
            if (page_size <= 0) page_size = 4096;
            return static_cast<size_t>(INT_MAX) &
                   ~(static_cast<size_t>(page_size) - 1);
        }();
        return value;
    }

    static constexpr unsigned BATCH_INDEX_BITS = 8;
    static constexpr uint64_t BATCH_INDEX_MASK =
        (uint64_t{1} << BATCH_INDEX_BITS) - 1;
    static_assert(QUEUE_DEPTH <= BATCH_INDEX_MASK,
                  "QUEUE_DEPTH exceeds batch index capacity");

    uint64_t next_operation_tag() { return (++op_id_) << BATCH_INDEX_BITS; }

    // Drain exactly @expected CQEs matching @op_id and
    // accumulate bytes.  Stale CQEs (user_data != op_id, e.g. from
    // a previous batch_read / submit_rw that hit an error and left
    // in-flight SQEs) are silently consumed and discarded.
    //
    // Uses peek-then-wait: after io_uring_submit_and_wait most CQEs
    // are already available, so io_uring_peek_cqe() succeeds without
    // a syscall in the common case.  io_uring_wait_cqe() only kicks
    // in when the ring is unexpectedly drained (stale CQE storms).
    tl::expected<size_t, ErrorCode> collect(int expected, uint64_t op_id) {
        if (!prepare_completions(expected, op_id)) {
            return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
        }
        size_t total = 0;
        bool err = false;
        int processed = 0;

        while (processed < expected) {
            struct io_uring_cqe* cqe;
            if (!wait_cqe(&cqe)) {
                reset_ring();
                return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
            }
            if (cqe->user_data != op_id) {
                // Stale CQE from a previous failed operation.
                io_uring_cq_advance(&ring_, 1);
                continue;
            }
            if (cqe->res < 0) {
                LOG(ERROR) << "[SharedUringRing] CQE error: "
                           << strerror(-cqe->res);
                record_cqe_errno(cqe->res);
                err = true;
            } else {
                total += static_cast<size_t>(cqe->res);
            }
            io_uring_cq_advance(&ring_, 1);
            ++processed;
        }
        if (err) return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
        return total;
    }

    tl::expected<void, ErrorCode> collect_batch(int expected, uint64_t op_id,
                                                ReadDesc* descs) {
        if (!prepare_completions(expected, op_id)) {
            return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
        }

        bool has_error = false;
        int processed = 0;
        while (processed < expected) {
            struct io_uring_cqe* cqe;
            if (!wait_cqe(&cqe)) {
                reset_ring();
                return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
            }

            const uint64_t cqe_op = cqe->user_data & ~BATCH_INDEX_MASK;
            if (cqe_op != op_id) {
                io_uring_cq_advance(&ring_, 1);
                continue;
            }

            const uint64_t encoded_index = cqe->user_data & BATCH_INDEX_MASK;
            if (encoded_index == 0 ||
                encoded_index > static_cast<uint64_t>(expected)) {
                LOG(ERROR) << "[SharedUringRing] invalid batch CQE index: "
                           << encoded_index;
                has_error = true;
            } else {
                auto& desc = descs[encoded_index - 1];
                desc.completed = true;
                if (cqe->res < 0) {
                    LOG(ERROR) << "[SharedUringRing] batch CQE error: "
                               << strerror(-cqe->res);
                    record_cqe_errno(cqe->res);
                    desc.error = ErrorCode::FILE_READ_FAIL;
                    has_error = true;
                } else {
                    desc.bytes_read = static_cast<size_t>(cqe->res);
                }
            }
            io_uring_cq_advance(&ring_, 1);
            ++processed;
        }

        if (has_error) return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
        return {};
    }

    // Chunked contiguous read or write.
    tl::expected<size_t, ErrorCode> submit_rw(bool is_write, int fd, void* buf,
                                              size_t len, off_t off,
                                              bool use_fixed_buf) {
        const ErrorCode err_code =
            is_write ? ErrorCode::FILE_WRITE_FAIL : ErrorCode::FILE_READ_FAIL;
        const bool fix_buf = (use_fixed_buf && buf_registered_);

        char* ptr = static_cast<char*>(buf);
        size_t total = 0;
        size_t remaining = len;
        off_t cur = off;

        while (remaining > 0) {
            size_t cs = calc_chunk(remaining, QUEUE_DEPTH);
            uint64_t op = next_operation_tag();
            ReadDesc chunks[QUEUE_DEPTH];
            int n = 0;

            while (remaining > 0 && n < static_cast<int>(QUEUE_DEPTH)) {
                size_t chunk = std::min({cs, remaining, max_rw_count()});

                struct io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
                if (!sqe) {
                    LOG(ERROR) << "[SharedUringRing] SQ full";
                    return tl::make_unexpected(err_code);
                }

                prep_rw(sqe, is_write, fix_buf, fd, ptr, chunk, cur);
                sqe->user_data = op | static_cast<uint64_t>(n + 1);
                chunks[n++] = ReadDesc{ptr, chunk, cur};

                ptr += chunk;
                cur += static_cast<off_t>(chunk);
                remaining -= chunk;
            }

            if (!collect_batch(n, op, chunks)) {
                return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
            }
            for (int i = 0; i < n; ++i) {
                auto done = finish_short(is_write, fd, chunks[i], fix_buf);
                if (!done) return done;
                total += done.value();
                if (done.value() < chunks[i].len) return total;  // read EOF
            }
        }
        return total;
    }

    static void prep_rw(struct io_uring_sqe* sqe, bool is_write, bool fix_buf,
                        int fd, char* ptr, size_t len, off_t off) {
        if (is_write) {
            if (fix_buf)
                io_uring_prep_write_fixed(sqe, fd, ptr, len, off, 0);
            else
                io_uring_prep_write(sqe, fd, ptr, len, off);
        } else {
            if (fix_buf)
                io_uring_prep_read_fixed(sqe, fd, ptr, len, off, 0);
            else
                io_uring_prep_read(sqe, fd, ptr, len, off);
        }
    }

    // io_uring completes a read or write short, without an errno, when it
    // fails after moving some bytes. As with pwritev/preadv, that count is
    // not an error: submit the rest, so that its CQE reports why (EIO on a
    // dead disk). A write that moves nothing fails. A read ends at EOF: a
    // 0-byte result, or a stop off a 4 KiB boundary (only EOF stops there,
    // and an O_DIRECT read could not resume from it).
    tl::expected<size_t, ErrorCode> finish_short(bool is_write, int fd,
                                                 const ReadDesc& d,
                                                 bool use_fixed_buf) {
        constexpr off_t kBlock = 4096;
        size_t done = d.bytes_read;
        size_t moved = d.bytes_read;  // by the last completion
        while (done < d.len) {
            if (moved == 0) {
                if (!is_write) return done;
                LOG(ERROR) << "[SharedUringRing] zero bytes written";
                return tl::make_unexpected(ErrorCode::FILE_WRITE_FAIL);
            }
            const off_t stop = d.off + static_cast<off_t>(done);
            if (!is_write && stop % kBlock != 0) return done;
            auto rest =
                submit_one(is_write, fd, static_cast<char*>(d.buf) + done,
                           d.len - done, stop, use_fixed_buf);
            if (!rest) return rest;
            moved = rest.value();
            done += moved;
        }
        return done;
    }

    // One read or write SQE for the rest of a short completion. No larger
    // than the SQE that completed short, so it needs no chunking.
    tl::expected<size_t, ErrorCode> submit_one(bool is_write, int fd, char* ptr,
                                               size_t len, off_t off,
                                               bool use_fixed_buf) {
        struct io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
        if (!sqe) {
            LOG(ERROR) << "[SharedUringRing] SQ full (short completion)";
            return tl::make_unexpected(is_write ? ErrorCode::FILE_WRITE_FAIL
                                                : ErrorCode::FILE_READ_FAIL);
        }
        prep_rw(sqe, is_write, use_fixed_buf && buf_registered_, fd, ptr, len,
                off);
        const uint64_t op = next_operation_tag();
        sqe->user_data = op | 1;
        ReadDesc d{ptr, len, off};
        if (!collect_batch(1, op, &d)) {
            return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
        }
        return d.bytes_read;
    }

    // Scatter/gather read or write (one SQE per iovec, sequential offsets).
    tl::expected<size_t, ErrorCode> submit_vector(bool is_write, int fd,
                                                  const iovec* iovs, int cnt,
                                                  off_t off) {
        const ErrorCode err_code =
            is_write ? ErrorCode::FILE_WRITE_FAIL : ErrorCode::FILE_READ_FAIL;
        const size_t max_io = max_rw_count();

        size_t total = 0;
        off_t cur = off;
        int remaining = cnt;
        int idx = 0;

        while (remaining > 0) {
            if (iovs[idx].iov_len > max_io) {
                auto res = submit_rw(is_write, fd, iovs[idx].iov_base,
                                     iovs[idx].iov_len, cur,
                                     /*use_fixed_buf=*/false);
                if (!res) return res;
                total += res.value();
                if (res.value() < iovs[idx].iov_len) return total;  // read EOF
                cur += static_cast<off_t>(iovs[idx].iov_len);
                ++idx;
                --remaining;
                continue;
            }

            int batch = 0;
            while (batch < remaining && batch < static_cast<int>(QUEUE_DEPTH) &&
                   iovs[idx + batch].iov_len <= max_io) {
                ++batch;
            }

            uint64_t op = next_operation_tag();
            ReadDesc parts[QUEUE_DEPTH];
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
                sqe->user_data = op | static_cast<uint64_t>(i + 1);
                parts[i] = ReadDesc{iovs[idx].iov_base, iovs[idx].iov_len, cur};

                cur += static_cast<off_t>(iovs[idx].iov_len);
                ++idx;
            }

            if (!collect_batch(batch, op, parts)) {
                return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
            }
            for (int i = 0; i < batch; ++i) {
                auto done = finish_short(is_write, fd, parts[i], false);
                if (!done) return done;
                total += done.value();
                if (done.value() < parts[i].len) return total;  // read EOF
            }
            remaining -= batch;
        }
        return total;
    }

    // -----------------------------------------------------------------
    // Data members
    // -----------------------------------------------------------------
    struct io_uring ring_{};
    bool initialized_ = false;

    bool buf_registered_ = false;
    bool buf_register_failed_ = false;  // set on first failure; skip retries
    void* buf_base_ = nullptr;
    size_t buf_size_ = 0;
    uint64_t op_id_ = 0;  // monotonic ID for stale CQE filtering
    int cqe_errno_ = 0;   // see cqe_errno()

    void record_cqe_errno(int res) {
        if (cqe_errno_ == 0) cqe_errno_ = -res;
    }
};

// ============================================================================
// UringFile — thin wrapper over SharedUringRing
// ============================================================================

UringFile::UringFile(const std::string& filename, int fd,
                     unsigned /*queue_depth*/, bool use_direct_io)
    : StorageFile(filename, fd), use_direct_io_(use_direct_io) {
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
        if (delete_on_write_fail_ &&
            error_code_ == ErrorCode::FILE_WRITE_FAIL) {
            if (::unlink(filename_.c_str()) == -1)
                LOG(ERROR) << "[UringFile] failed to delete corrupted file: "
                           << filename_;
            else
                LOG(INFO) << "[UringFile] deleted corrupted file: "
                          << filename_;
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

void UringFile::record_ring_errno() {
    record_sys_errno(SharedUringRing::instance().cqe_errno());
}

bool UringFile::in_registered_buffer(const void* buf, size_t len) const {
    auto& r = SharedUringRing::instance();
    if (!r.is_buffer_registered()) return false;
    uintptr_t buf_addr = reinterpret_cast<uintptr_t>(buf);
    uintptr_t rb = reinterpret_cast<uintptr_t>(r.buffer_base());
    return buf_addr >= rb && (buf_addr + len) <= (rb + r.buffer_size());
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

    void* bounce = nullptr;
    const char* src = data.data();
    size_t write_len = length;

    if (use_direct_io_) {
        write_len = ((length + ALIGNMENT_ - 1) / ALIGNMENT_) * ALIGNMENT_;
        bounce = alloc_aligned_buffer(write_len);
        if (!bounce) return make_error<size_t>(ErrorCode::FILE_WRITE_FAIL);
        std::memcpy(bounce, data.data(), length);
        if (write_len > length)
            std::memset(static_cast<char*>(bounce) + length, 0,
                        write_len - length);
        src = static_cast<char*>(bounce);
    }

    auto res = SharedUringRing::instance().write(fd_, src, write_len, 0);

    if (bounce) free_aligned_buffer(bounce);

    if (!res) {
        record_ring_errno();
        return make_error<size_t>(res.error());
    }
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

    void* bounce = nullptr;
    char* read_ptr = nullptr;
    size_t read_len = length;

    if (use_direct_io_) {
        read_len = ((length + ALIGNMENT_ - 1) / ALIGNMENT_) * ALIGNMENT_;
        bounce = alloc_aligned_buffer(read_len);
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

    if (!res) {
        record_ring_errno();
        return make_error<size_t>(res.error());
    }
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

    auto res = SharedUringRing::instance().write(fd_, buffer, length, offset);
    if (!res) record_ring_errno();
    return res;
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

    auto res = SharedUringRing::instance().read(fd_, buffer, length, offset);
    if (!res) record_ring_errno();
    return res;
}

// ---------------------------------------------------------------------------
// batch_read — submit multiple independent reads in one ring submission
// ---------------------------------------------------------------------------

tl::expected<void, ErrorCode> UringFile::batch_read(ReadDesc* descs, int cnt) {
    if (fd_ < 0) return make_error<void>(ErrorCode::FILE_NOT_FOUND);
    if (!descs || cnt <= 0)
        return make_error<void>(ErrorCode::FILE_INVALID_BUFFER);

    for (int i = 0; i < cnt; ++i) {
        if (!descs[i].buf || descs[i].len == 0)
            return make_error<void>(ErrorCode::FILE_INVALID_BUFFER);
        if (use_direct_io_ &&
            (reinterpret_cast<uintptr_t>(descs[i].buf) % ALIGNMENT_ ||
             descs[i].len % ALIGNMENT_ || descs[i].off % ALIGNMENT_)) {
            return make_error<void>(ErrorCode::FILE_INVALID_BUFFER);
        }
    }

    auto res = SharedUringRing::instance().batch_read(fd_, descs, cnt);
    if (!res) record_ring_errno();
    return res;
}

// ---------------------------------------------------------------------------
// vector_write / vector_read
// ---------------------------------------------------------------------------

namespace {

size_t IovecTotalLen(const iovec* iov, int iovcnt) {
    size_t total = 0;
    for (int i = 0; i < iovcnt; ++i) total += iov[i].iov_len;
    return total;
}

bool IsIovecRegionAligned(const iovec* iov, int iovcnt, off_t offset,
                          size_t alignment) {
    if (offset % static_cast<off_t>(alignment) != 0) return false;
    for (int i = 0; i < iovcnt; ++i) {
        if (reinterpret_cast<uintptr_t>(iov[i].iov_base) % alignment != 0) {
            return false;
        }
        if (iov[i].iov_len % alignment != 0) return false;
    }
    return true;
}

struct AlignedBufferDeleter {
    void operator()(void* ptr) const {
        if (ptr) free(ptr);
    }
};
using AlignedBufferPtr = std::unique_ptr<void, AlignedBufferDeleter>;

void CopyFromIovec(char* dst, const iovec* iov, int iovcnt) {
    for (int i = 0; i < iovcnt; ++i) {
        std::memcpy(dst, iov[i].iov_base, iov[i].iov_len);
        dst += iov[i].iov_len;
    }
}

void CopyToIovec(char* src, iovec* iov, int iovcnt) {
    for (int i = 0; i < iovcnt; ++i) {
        std::memcpy(iov[i].iov_base, src, iov[i].iov_len);
        src += iov[i].iov_len;
    }
}

}  // namespace

tl::expected<size_t, ErrorCode> UringFile::vector_write(const iovec* iov,
                                                        int iovcnt,
                                                        off_t offset) {
    if (fd_ < 0) return make_error<size_t>(ErrorCode::FILE_NOT_FOUND);
    if (!iov || iovcnt <= 0)
        return make_error<size_t>(ErrorCode::FILE_INVALID_BUFFER);

    const size_t total = IovecTotalLen(iov, iovcnt);
    // Writing nothing is a no-op success.
    if (total == 0) return 0;

    auto start = std::chrono::steady_clock::now();
    tl::expected<size_t, ErrorCode> res;

    if (!use_direct_io_ ||
        IsIovecRegionAligned(iov, iovcnt, offset, ALIGNMENT_)) {
        res =
            SharedUringRing::instance().vector_write(fd_, iov, iovcnt, offset);
    } else {
        const off_t aligned_off = offset & ~static_cast<off_t>(ALIGNMENT_ - 1);
        const size_t head = static_cast<size_t>(offset - aligned_off);
        const size_t span = head + total;
        const size_t aligned_len =
            ((span + ALIGNMENT_ - 1) / ALIGNMENT_) * ALIGNMENT_;

        AlignedBufferPtr bounce(alloc_aligned_buffer(aligned_len));
        if (!bounce) return make_error<size_t>(ErrorCode::FILE_WRITE_FAIL);

        // Preserve bytes outside [offset, offset+total) within the aligned
        // region; otherwise padding would clobber neighboring data.
        const bool needs_rmw = (head != 0) || (aligned_len != total);
        if (needs_rmw) {
            auto read_res = SharedUringRing::instance().read(
                fd_, bounce.get(), aligned_len, aligned_off);
            if (!read_res) {
                // A real read error must not proceed with a zero-filled
                // bounce buffer — that would silently corrupt neighbors.
                record_ring_errno();
                return make_error<size_t>(ErrorCode::FILE_READ_FAIL);
            }
            if (read_res.value() < aligned_len) {
                // EOF / short read: only zero the unread suffix.
                std::memset(static_cast<char*>(bounce.get()) + read_res.value(),
                            0, aligned_len - read_res.value());
            }
        }
        CopyFromIovec(static_cast<char*>(bounce.get()) + head, iov, iovcnt);

        res = SharedUringRing::instance().write(fd_, bounce.get(), aligned_len,
                                                aligned_off);

        if (res && res.value() >= span) {
            res = total;
        } else if (res) {
            res = make_error<size_t>(ErrorCode::FILE_WRITE_FAIL);
        }
    }
    if (!res) record_ring_errno();

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
    if (!iov || iovcnt <= 0)
        return make_error<size_t>(ErrorCode::FILE_INVALID_BUFFER);

    const size_t expected_bytes = IovecTotalLen(iov, iovcnt);
    // Reading nothing is a no-op success.
    if (expected_bytes == 0) return 0;

    auto start = std::chrono::steady_clock::now();
    tl::expected<size_t, ErrorCode> res;

    if (!use_direct_io_ ||
        IsIovecRegionAligned(iov, iovcnt, offset, ALIGNMENT_)) {
        res = SharedUringRing::instance().vector_read(fd_, iov, iovcnt, offset);
    } else {
        const off_t aligned_off = offset & ~static_cast<off_t>(ALIGNMENT_ - 1);
        const size_t head = static_cast<size_t>(offset - aligned_off);
        const size_t span = head + expected_bytes;
        const size_t aligned_len =
            ((span + ALIGNMENT_ - 1) / ALIGNMENT_) * ALIGNMENT_;

        AlignedBufferPtr bounce(alloc_aligned_buffer(aligned_len));
        if (!bounce) return make_error<size_t>(ErrorCode::FILE_READ_FAIL);

        res = SharedUringRing::instance().read(fd_, bounce.get(), aligned_len,
                                               aligned_off);
        if (res && res.value() >= span) {
            CopyToIovec(static_cast<char*>(bounce.get()) + head,
                        const_cast<iovec*>(iov), iovcnt);
            res = expected_bytes;
        } else if (res) {
            res = make_error<size_t>(ErrorCode::FILE_READ_FAIL);
        }
    }

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
        record_ring_errno();
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
        record_ring_errno();
        LOG(ERROR) << "[UringFile::datasync] fsync failed for: " << filename_;
        return tl::make_unexpected(ErrorCode::FILE_WRITE_FAIL);
    }
    return {};
}

// ---------------------------------------------------------------------------
// Buffer registration
// ---------------------------------------------------------------------------

bool UringFile::register_global_buffer(void* buffer, size_t length) {
    if (!buffer || length == 0) {
        LOG(ERROR)
            << "[UringFile::register_global_buffer] invalid buffer or length";
        return false;
    }
    // Disable Transparent Huge Pages on this region before pinning.
    // io_uring uses FOLL_LONGTERM to pin pages; on kernel 5.15 the kernel
    // must split any 2MB THP into 4KB pages before long-term pinning, which
    // can fail (ENOMEM) when huge pages are in use or memory is fragmented.
    // MADV_NOHUGEPAGE prevents the kernel from backing this range with THPs,
    // making pin_user_pages() reliable regardless of system THP policy.
    if (madvise(buffer, length, MADV_NOHUGEPAGE) != 0) {
        LOG(WARNING)
            << "[UringFile::register_global_buffer] madvise(NOHUGEPAGE)"
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
        LOG(WARNING)
            << "[UringFile::register_global_buffer] registration failed"
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
