#ifdef USE_URING

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <string>
#include <sys/uio.h>
#include <unistd.h>
#include <cmath>

#include <glog/logging.h>
#include <liburing.h>

#include "file_interface.h"

namespace mooncake {

// ---------------------------------------------------------------------------
// Construction / Destruction
// ---------------------------------------------------------------------------

UringFile::UringFile(const std::string &filename, int fd, unsigned queue_depth, bool use_direct_io)
    : StorageFile(filename, fd),
      ring_initialized_(false),
      files_registered_(false),
      buffer_registered_(false),
      queue_depth_(queue_depth),
      use_direct_io_(use_direct_io),
      registered_buffer_(nullptr),
      registered_buffer_size_(0) {
    if (fd < 0) {
        error_code_ = ErrorCode::FILE_INVALID_HANDLE;
        return;
    }

    int ret = io_uring_queue_init(queue_depth, &ring_, 0);
    if (ret < 0) {
        LOG(ERROR) << "Failed to initialize io_uring: " << strerror(-ret);
        error_code_ = ErrorCode::FILE_INVALID_HANDLE;
        return;
    }
    ring_initialized_ = true;

    // Register the file descriptor to avoid per-I/O fd lookup overhead.
    ret = io_uring_register_files(&ring_, &fd_, 1);
    if (ret >= 0) {
        files_registered_ = true;
    } else {
        LOG(WARNING) << "[UringFile] io_uring_register_files failed: "
                     << strerror(-ret) << " (continuing without)";
    }

    if (use_direct_io_) {
        LOG(INFO) << "[UringFile] O_DIRECT mode enabled for " << filename;
    }
}

UringFile::~UringFile() {
    auto dtor_start = std::chrono::steady_clock::now();

    if (ring_initialized_) {
        if (buffer_registered_) {
            io_uring_unregister_buffers(&ring_);
        }
        if (files_registered_) {
            io_uring_unregister_files(&ring_);
        }
        io_uring_queue_exit(&ring_);
    }

    if (fd_ >= 0) {
        if (close(fd_) != 0) {
            LOG(WARNING) << "Failed to close file: " << filename_;
        }
        if (error_code_ == ErrorCode::FILE_WRITE_FAIL) {
            if (::unlink(filename_.c_str()) == -1) {
                LOG(ERROR) << "Failed to delete corrupted file: " << filename_;
            } else {
                LOG(INFO) << "Deleted corrupted file: " << filename_;
            }
        }
    }
    fd_ = -1;

    auto dtor_end = std::chrono::steady_clock::now();
    auto dtor_elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        dtor_end - dtor_start).count();
    if (dtor_elapsed_ms > 1) {
        LOG(WARNING) << "[UringFile::~UringFile] cleanup took " << dtor_elapsed_ms
                     << "ms for " << filename_;
    }
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

namespace {
    inline size_t next_power_of_2(size_t n) {
        if (n == 0) return 1;
        n--;
        n |= n >> 1;
        n |= n >> 2;
        n |= n >> 4;
        n |= n >> 8;
        n |= n >> 16;
        n |= n >> 32;
        return n + 1;
    }
}

void* UringFile::alloc_aligned_buffer(size_t size) const {
    // Align size up to ALIGNMENT_
    size_t aligned_size = ((size + ALIGNMENT_ - 1) / ALIGNMENT_) * ALIGNMENT_;
    void* ptr = nullptr;
    if (posix_memalign(&ptr, ALIGNMENT_, aligned_size) != 0) {
        LOG(ERROR) << "[UringFile] Failed to allocate aligned buffer of size " << aligned_size;
        return nullptr;
    }
    return ptr;
}

void UringFile::free_aligned_buffer(void* ptr) const {
    if (ptr) {
        free(ptr);
    }
}

/**
 * @brief Calculate optimal chunk size for parallel I/O
 *
 * Strategy:
 * 1. Chunk size must be a power of 2
 * 2. Chunk size = max(min_chunk_size, optimal_size)
 * 3. optimal_size = the size that can fully utilize remaining queue depth
 *
 * @param total_len Total length to transfer
 * @param available_depth Available queue depth slots
 * @param min_chunk_size Minimum chunk size (must be power of 2)
 * @return Optimal chunk size (power of 2)
 */
size_t UringFile::calculate_chunk_size(size_t total_len,
                                       unsigned available_depth,
                                       size_t min_chunk_size) const {
    if (total_len == 0 || available_depth == 0) {
        return min_chunk_size;
    }

    // Calculate the size that would fully utilize available queue depth
    size_t optimal_size = (total_len + available_depth - 1) / available_depth;

    // Round up to next power of 2
    optimal_size = next_power_of_2(optimal_size);

    // Return max(min_chunk_size, optimal_size)
    return std::max(min_chunk_size, optimal_size);
}

// ---------------------------------------------------------------------------
// Internal helpers - I/O submission
// ---------------------------------------------------------------------------

tl::expected<size_t, ErrorCode> UringFile::submit_and_wait_n(int n) {
    int ret = io_uring_submit_and_wait(&ring_, n);
    if (ret < 0) {
        LOG(ERROR) << "[UringFile] io_uring_submit_and_wait failed: "
                   << strerror(-ret);
        return make_error<size_t>(ErrorCode::INTERNAL_ERROR);
    }

    size_t total_bytes = 0;
    bool has_error = false;
    unsigned head;
    unsigned count = 0;
    struct io_uring_cqe *cqe;

    io_uring_for_each_cqe(&ring_, head, cqe) {
        if (cqe->res < 0) {
            LOG(ERROR) << "[UringFile] I/O failed: " << strerror(-cqe->res);
            has_error = true;
        } else {
            total_bytes += static_cast<size_t>(cqe->res);
        }
        count++;
    }
    io_uring_cq_advance(&ring_, count);

    if (has_error) {
        return make_error<size_t>(ErrorCode::INTERNAL_ERROR);
    }
    return total_bytes;
}

// ---------------------------------------------------------------------------
// Write implementation with intelligent chunking
// ---------------------------------------------------------------------------

tl::expected<size_t, ErrorCode> UringFile::write(const std::string &buffer,
                                                 size_t length) {
    return write(std::span<const char>(buffer.data(), length), length);
}

tl::expected<size_t, ErrorCode> UringFile::write(std::span<const char> data,
                                                 size_t length) {
    if (fd_ < 0 || !ring_initialized_) {
        return make_error<size_t>(ErrorCode::FILE_NOT_FOUND);
    }
    if (length == 0) {
        return make_error<size_t>(ErrorCode::FILE_INVALID_BUFFER);
    }

    constexpr size_t MIN_CHUNK_SIZE = 4096;

    // If using O_DIRECT, allocate aligned buffer and copy data
    void* aligned_buffer = nullptr;
    const char* source_ptr = data.data();
    size_t actual_length = length;

    if (use_direct_io_) {
        // Align length up to ALIGNMENT_
        actual_length = ((length + ALIGNMENT_ - 1) / ALIGNMENT_) * ALIGNMENT_;
        aligned_buffer = alloc_aligned_buffer(actual_length);
        if (!aligned_buffer) {
            return make_error<size_t>(ErrorCode::FILE_WRITE_FAIL);
        }
        // Copy data to aligned buffer and zero-pad if necessary
        std::memcpy(aligned_buffer, data.data(), length);
        if (actual_length > length) {
            std::memset(static_cast<char*>(aligned_buffer) + length, 0, actual_length - length);
        }
        source_ptr = static_cast<const char*>(aligned_buffer);
    }

    size_t total_written = 0;
    const char *ptr = source_ptr;
    size_t remaining = actual_length;
    off_t current_offset = 0;
    int target_fd = files_registered_ ? 0 : fd_;

    while (remaining > 0) {
        size_t chunk_size = calculate_chunk_size(remaining, queue_depth_, MIN_CHUNK_SIZE);
        unsigned num_chunks = std::min(
            static_cast<unsigned>((remaining + chunk_size - 1) / chunk_size),
            queue_depth_
        );

        for (unsigned i = 0; i < num_chunks; i++) {
            size_t this_chunk = std::min(chunk_size, remaining);

            struct io_uring_sqe *sqe = io_uring_get_sqe(&ring_);
            if (!sqe) {
                LOG(ERROR) << "[UringFile::write] Failed to get SQE";
                if (aligned_buffer) free_aligned_buffer(aligned_buffer);
                return make_error<size_t>(ErrorCode::FILE_WRITE_FAIL);
            }

            io_uring_prep_write(sqe, target_fd, ptr, this_chunk, current_offset);
            if (files_registered_) {
                io_uring_sqe_set_flags(sqe, IOSQE_FIXED_FILE);
            }

            ptr += this_chunk;
            current_offset += this_chunk;
            remaining -= this_chunk;
            if (remaining == 0) break;
        }

        auto result = submit_and_wait_n(num_chunks);
        if (!result) {
            if (aligned_buffer) free_aligned_buffer(aligned_buffer);
            return make_error<size_t>(ErrorCode::FILE_WRITE_FAIL);
        }

        size_t bytes_written = result.value();
        if (bytes_written == 0) {
            LOG(ERROR) << "[UringFile::write] Zero bytes written";
            if (aligned_buffer) free_aligned_buffer(aligned_buffer);
            return make_error<size_t>(ErrorCode::FILE_WRITE_FAIL);
        }

        total_written += bytes_written;
    }

    if (aligned_buffer) {
        free_aligned_buffer(aligned_buffer);
    }

    // For O_DIRECT, we may have written more than requested (due to alignment)
    // but return the original length
    if (total_written < length) {
        LOG(WARNING) << "[UringFile::write] Incomplete write: "
                     << total_written << " / " << length;
        return make_error<size_t>(ErrorCode::FILE_WRITE_FAIL);
    }

    return length;  // Return original length, not aligned length
}

// ---------------------------------------------------------------------------
// Read implementation with intelligent chunking
// ---------------------------------------------------------------------------

tl::expected<size_t, ErrorCode> UringFile::read(std::string &buffer,
                                                size_t length) {
    if (fd_ < 0 || !ring_initialized_) {
        return make_error<size_t>(ErrorCode::FILE_NOT_FOUND);
    }
    if (length == 0) {
        return make_error<size_t>(ErrorCode::FILE_INVALID_BUFFER);
    }

    constexpr size_t MIN_CHUNK_SIZE = 4096;

    // If using O_DIRECT, allocate aligned buffer
    void* aligned_buffer = nullptr;
    char* read_ptr = nullptr;
    size_t actual_length = length;

    if (use_direct_io_) {
        // Align length up to ALIGNMENT_
        actual_length = ((length + ALIGNMENT_ - 1) / ALIGNMENT_) * ALIGNMENT_;
        aligned_buffer = alloc_aligned_buffer(actual_length);
        if (!aligned_buffer) {
            return make_error<size_t>(ErrorCode::FILE_READ_FAIL);
        }
        read_ptr = static_cast<char*>(aligned_buffer);
    } else {
        buffer.resize(length);
        read_ptr = buffer.data();
    }

    char *ptr = read_ptr;
    size_t remaining = actual_length;
    size_t total_read = 0;
    off_t current_offset = 0;
    int target_fd = files_registered_ ? 0 : fd_;

    while (remaining > 0) {
        size_t chunk_size = calculate_chunk_size(remaining, queue_depth_, MIN_CHUNK_SIZE);
        unsigned num_chunks = std::min(
            static_cast<unsigned>((remaining + chunk_size - 1) / chunk_size),
            queue_depth_
        );

        size_t batch_size = 0;
        for (unsigned i = 0; i < num_chunks; i++) {
            size_t this_chunk = std::min(chunk_size, remaining);

            struct io_uring_sqe *sqe = io_uring_get_sqe(&ring_);
            if (!sqe) {
                LOG(ERROR) << "[UringFile::read] Failed to get SQE";
                if (aligned_buffer) {
                    free_aligned_buffer(aligned_buffer);
                } else {
                    buffer.clear();
                }
                return make_error<size_t>(ErrorCode::FILE_READ_FAIL);
            }

            io_uring_prep_read(sqe, target_fd, ptr, this_chunk, current_offset);
            if (files_registered_) {
                io_uring_sqe_set_flags(sqe, IOSQE_FIXED_FILE);
            }

            ptr += this_chunk;
            current_offset += this_chunk;
            batch_size += this_chunk;
            remaining -= this_chunk;
            if (remaining == 0) break;
        }

        auto result = submit_and_wait_n(num_chunks);
        if (!result) {
            if (aligned_buffer) {
                free_aligned_buffer(aligned_buffer);
            } else {
                buffer.clear();
            }
            return make_error<size_t>(ErrorCode::FILE_READ_FAIL);
        }

        size_t bytes_read = result.value();
        if (bytes_read == 0) {
            break;  // EOF
        }

        total_read += bytes_read;
        if (bytes_read < batch_size) {
            break;  // EOF
        }
    }

    // Copy from aligned buffer to std::string if using O_DIRECT
    if (use_direct_io_) {
        size_t actual_read = std::min(total_read, length);
        buffer.assign(static_cast<const char*>(aligned_buffer), actual_read);
        free_aligned_buffer(aligned_buffer);
        total_read = actual_read;
    } else {
        buffer.resize(total_read);
    }

    if (total_read != length && total_read == 0) {
        return make_error<size_t>(ErrorCode::FILE_READ_FAIL);
    }

    return total_read;
}

// ---------------------------------------------------------------------------
// Zero-copy aligned I/O interface for O_DIRECT
// ---------------------------------------------------------------------------

tl::expected<size_t, ErrorCode> UringFile::read_aligned(void* buffer,
                                                         size_t length,
                                                         off_t offset) {
    MutexLocker lock(&ring_mutex_);
    if (fd_ < 0 || !ring_initialized_) {
        return make_error<size_t>(ErrorCode::FILE_NOT_FOUND);
    }
    if (length == 0 || buffer == nullptr) {
        return make_error<size_t>(ErrorCode::FILE_INVALID_BUFFER);
    }

    // Verify alignment when using O_DIRECT
    if (use_direct_io_) {
        if (reinterpret_cast<uintptr_t>(buffer) % ALIGNMENT_ != 0) {
            LOG(ERROR) << "[UringFile::read_aligned] Buffer not aligned to " << ALIGNMENT_;
            return make_error<size_t>(ErrorCode::FILE_INVALID_BUFFER);
        }
        if (length % ALIGNMENT_ != 0) {
            LOG(ERROR) << "[UringFile::read_aligned] Length not aligned to " << ALIGNMENT_;
            return make_error<size_t>(ErrorCode::FILE_INVALID_BUFFER);
        }
        if (offset % ALIGNMENT_ != 0) {
            LOG(ERROR) << "[UringFile::read_aligned] Offset not aligned to " << ALIGNMENT_;
            return make_error<size_t>(ErrorCode::FILE_INVALID_BUFFER);
        }
    }

    constexpr size_t MIN_CHUNK_SIZE = 4096;

    char* ptr = static_cast<char*>(buffer);
    size_t remaining = length;
    size_t total_read = 0;
    off_t current_offset = offset;
    int target_fd = files_registered_ ? 0 : fd_;

    // Check if this buffer falls within the registered buffer range
    bool use_fixed_buffer = (buffer_registered_ &&
                             reinterpret_cast<uintptr_t>(buffer) >=
                                 reinterpret_cast<uintptr_t>(registered_buffer_) &&
                             reinterpret_cast<uintptr_t>(buffer) + length <=
                                 reinterpret_cast<uintptr_t>(registered_buffer_) +
                                     registered_buffer_size_);

    while (remaining > 0) {
        size_t chunk_size = calculate_chunk_size(remaining, queue_depth_, MIN_CHUNK_SIZE);
        unsigned num_chunks = std::min(
            static_cast<unsigned>((remaining + chunk_size - 1) / chunk_size),
            queue_depth_
        );

        size_t batch_size = 0;
        for (unsigned i = 0; i < num_chunks; i++) {
            size_t this_chunk = std::min(chunk_size, remaining);

            struct io_uring_sqe *sqe = io_uring_get_sqe(&ring_);
            if (!sqe) {
                LOG(ERROR) << "[UringFile::read_aligned] Failed to get SQE";
                return make_error<size_t>(ErrorCode::FILE_READ_FAIL);
            }

            if (use_fixed_buffer) {
                // Use registered fixed buffer - avoids get_user_pages() overhead
                io_uring_prep_read_fixed(sqe, target_fd, ptr, this_chunk, current_offset, 0);
            } else {
                // Use regular buffer
                io_uring_prep_read(sqe, target_fd, ptr, this_chunk, current_offset);
            }

            if (files_registered_) {
                io_uring_sqe_set_flags(sqe, IOSQE_FIXED_FILE);
            }

            ptr += this_chunk;
            current_offset += this_chunk;
            batch_size += this_chunk;
            remaining -= this_chunk;
            if (remaining == 0) break;
        }

        auto result = submit_and_wait_n(num_chunks);
        if (!result) {
            return make_error<size_t>(ErrorCode::FILE_READ_FAIL);
        }

        size_t bytes_read = result.value();
        if (bytes_read == 0) {
            break;  // EOF
        }

        total_read += bytes_read;
        if (bytes_read < batch_size) {
            break;  // EOF
        }
    }

    return total_read;
}

tl::expected<size_t, ErrorCode> UringFile::write_aligned(const void* buffer,
                                                          size_t length,
                                                          off_t offset) {
    if (fd_ < 0 || !ring_initialized_) {
        return make_error<size_t>(ErrorCode::FILE_NOT_FOUND);
    }
    if (length == 0 || buffer == nullptr) {
        return make_error<size_t>(ErrorCode::FILE_INVALID_BUFFER);
    }

    // Verify alignment when using O_DIRECT
    if (use_direct_io_) {
        if (reinterpret_cast<uintptr_t>(buffer) % ALIGNMENT_ != 0) {
            LOG(ERROR) << "[UringFile::write_aligned] Buffer not aligned to " << ALIGNMENT_;
            return make_error<size_t>(ErrorCode::FILE_INVALID_BUFFER);
        }
        if (length % ALIGNMENT_ != 0) {
            LOG(ERROR) << "[UringFile::write_aligned] Length not aligned to " << ALIGNMENT_;
            return make_error<size_t>(ErrorCode::FILE_INVALID_BUFFER);
        }
        if (offset % ALIGNMENT_ != 0) {
            LOG(ERROR) << "[UringFile::write_aligned] Offset not aligned to " << ALIGNMENT_;
            return make_error<size_t>(ErrorCode::FILE_INVALID_BUFFER);
        }
    }

    constexpr size_t MIN_CHUNK_SIZE = 4096;

    const char* ptr = static_cast<const char*>(buffer);
    size_t remaining = length;
    size_t total_written = 0;
    off_t current_offset = offset;
    int target_fd = files_registered_ ? 0 : fd_;

    // Check if this buffer falls within the registered buffer range
    bool use_fixed_buffer = (buffer_registered_ &&
                             reinterpret_cast<uintptr_t>(buffer) >=
                                 reinterpret_cast<uintptr_t>(registered_buffer_) &&
                             reinterpret_cast<uintptr_t>(buffer) + length <=
                                 reinterpret_cast<uintptr_t>(registered_buffer_) +
                                     registered_buffer_size_);

    while (remaining > 0) {
        size_t chunk_size = calculate_chunk_size(remaining, queue_depth_, MIN_CHUNK_SIZE);
        unsigned num_chunks = std::min(
            static_cast<unsigned>((remaining + chunk_size - 1) / chunk_size),
            queue_depth_
        );

        for (unsigned i = 0; i < num_chunks; i++) {
            size_t this_chunk = std::min(chunk_size, remaining);

            struct io_uring_sqe *sqe = io_uring_get_sqe(&ring_);
            if (!sqe) {
                LOG(ERROR) << "[UringFile::write_aligned] Failed to get SQE";
                return make_error<size_t>(ErrorCode::FILE_WRITE_FAIL);
            }

            if (use_fixed_buffer) {
                // Use registered fixed buffer - avoids get_user_pages() overhead
                io_uring_prep_write_fixed(sqe, target_fd, ptr, this_chunk, current_offset, 0);
            } else {
                // Use regular buffer
                io_uring_prep_write(sqe, target_fd, ptr, this_chunk, current_offset);
            }

            if (files_registered_) {
                io_uring_sqe_set_flags(sqe, IOSQE_FIXED_FILE);
            }

            ptr += this_chunk;
            current_offset += this_chunk;
            remaining -= this_chunk;
            if (remaining == 0) break;
        }

        auto result = submit_and_wait_n(num_chunks);
        if (!result) {
            return make_error<size_t>(ErrorCode::FILE_WRITE_FAIL);
        }

        size_t bytes_written = result.value();
        if (bytes_written == 0) {
            LOG(ERROR) << "[UringFile::write_aligned] Zero bytes written";
            return make_error<size_t>(ErrorCode::FILE_WRITE_FAIL);
        }

        total_written += bytes_written;
    }

    if (total_written != length) {
        LOG(WARNING) << "[UringFile::write_aligned] Incomplete write: "
                     << total_written << " / " << length;
        return make_error<size_t>(ErrorCode::FILE_WRITE_FAIL);
    }

    return total_written;
}

// ---------------------------------------------------------------------------
// Vectored I/O  —  multi-SQE parallel submission
//
// Each iovec is submitted as an independent SQE so the NVMe device can
// serve them concurrently, matching the pipelining approach used in the
// benchmark.  When iovcnt > queue_depth_, requests are batched.
// ---------------------------------------------------------------------------

tl::expected<size_t, ErrorCode> UringFile::vector_write(const iovec *iov,
                                                         int iovcnt,
                                                         off_t offset) {
    if (fd_ < 0 || !ring_initialized_) {
        return make_error<size_t>(ErrorCode::FILE_NOT_FOUND);
    }

    int target_fd = files_registered_ ? 0 : fd_;
    size_t total_written = 0;
    off_t cur_offset = offset;
    int remaining = iovcnt;
    int idx = 0;

    while (remaining > 0) {
        int batch = std::min(remaining, static_cast<int>(queue_depth_));

        for (int i = 0; i < batch; i++) {
            struct io_uring_sqe *sqe = io_uring_get_sqe(&ring_);
            if (!sqe) {
                LOG(ERROR) << "[UringFile::vector_write] Failed to get SQE";
                return make_error<size_t>(ErrorCode::FILE_WRITE_FAIL);
            }

            io_uring_prep_write(sqe, target_fd, iov[idx].iov_base,
                                iov[idx].iov_len, cur_offset);
            if (files_registered_) {
                io_uring_sqe_set_flags(sqe, IOSQE_FIXED_FILE);
            }

            cur_offset += static_cast<off_t>(iov[idx].iov_len);
            idx++;
        }

        auto result = submit_and_wait_n(batch);
        if (!result) {
            return make_error<size_t>(ErrorCode::FILE_WRITE_FAIL);
        }
        total_written += result.value();
        remaining -= batch;
    }

    return total_written;
}

tl::expected<size_t, ErrorCode> UringFile::vector_read(const iovec *iov,
                                                        int iovcnt,
                                                        off_t offset) {
    if (fd_ < 0 || !ring_initialized_) {
        return make_error<size_t>(ErrorCode::FILE_NOT_FOUND);
    }

    size_t expected_bytes = 0;
    for (int i = 0; i < iovcnt; ++i) {
        expected_bytes += iov[i].iov_len;
    }

    auto start = std::chrono::steady_clock::now();

    int target_fd = files_registered_ ? 0 : fd_;
    size_t total_read = 0;
    off_t cur_offset = offset;
    int remaining = iovcnt;
    int idx = 0;

    while (remaining > 0) {
        int batch = std::min(remaining, static_cast<int>(queue_depth_));

        for (int i = 0; i < batch; i++) {
            struct io_uring_sqe *sqe = io_uring_get_sqe(&ring_);
            if (!sqe) {
                LOG(ERROR) << "[UringFile::vector_read] Failed to get SQE";
                return make_error<size_t>(ErrorCode::FILE_READ_FAIL);
            }

            io_uring_prep_read(sqe, target_fd, iov[idx].iov_base,
                               iov[idx].iov_len, cur_offset);
            if (files_registered_) {
                io_uring_sqe_set_flags(sqe, IOSQE_FIXED_FILE);
            }

            cur_offset += static_cast<off_t>(iov[idx].iov_len);
            idx++;
        }

        auto result = submit_and_wait_n(batch);
        if (!result) {
            auto end = std::chrono::steady_clock::now();
            auto elapsed_us =
                std::chrono::duration_cast<std::chrono::microseconds>(
                    end - start)
                    .count();
            LOG(ERROR) << "[UringFile::vector_read] FAILED: fd=" << fd_
                       << ", offset=" << offset << ", iovcnt=" << iovcnt
                       << ", expected_bytes=" << expected_bytes
                       << ", time=" << elapsed_us << "us";
            return make_error<size_t>(ErrorCode::FILE_READ_FAIL);
        }
        total_read += result.value();
        remaining -= batch;
    }

    auto end = std::chrono::steady_clock::now();
    auto elapsed_us =
        std::chrono::duration_cast<std::chrono::microseconds>(end - start)
            .count();

    if (elapsed_us > 1000 || expected_bytes > 1024 * 1024) {
        double throughput_mbps =
            (elapsed_us > 0)
                ? (static_cast<double>(total_read) / (1024.0 * 1024.0)) /
                      (static_cast<double>(elapsed_us) / 1000000.0)
                : 0;
        LOG(INFO) << "[UringFile::vector_read] fd=" << fd_
                  << ", offset=" << offset << ", iovcnt=" << iovcnt
                  << ", bytes=" << total_read << ", time=" << elapsed_us
                  << "us (" << (elapsed_us / 1000.0) << "ms)"
                  << ", throughput=" << throughput_mbps << "MB/s";
    }

    return total_read;
}

// ---------------------------------------------------------------------------
// Buffer registration for high-performance I/O
// ---------------------------------------------------------------------------

bool UringFile::register_buffer(void* buffer, size_t length) {
    if (!ring_initialized_) {
        LOG(ERROR) << "[UringFile::register_buffer] io_uring not initialized";
        return false;
    }

    if (buffer_registered_) {
        LOG(WARNING) << "[UringFile::register_buffer] Buffer already registered, unregistering first";
        unregister_buffer();
    }

    if (!buffer || length == 0) {
        LOG(ERROR) << "[UringFile::register_buffer] Invalid buffer or length";
        return false;
    }

    // Verify alignment when using O_DIRECT
    if (use_direct_io_) {
        if (reinterpret_cast<uintptr_t>(buffer) % ALIGNMENT_ != 0) {
            LOG(ERROR) << "[UringFile::register_buffer] Buffer not aligned to " << ALIGNMENT_;
            return false;
        }
        if (length % ALIGNMENT_ != 0) {
            LOG(ERROR) << "[UringFile::register_buffer] Length not aligned to " << ALIGNMENT_;
            return false;
        }
    }

    registered_iovec_.iov_base = buffer;
    registered_iovec_.iov_len = length;

    int ret = io_uring_register_buffers(&ring_, &registered_iovec_, 1);
    if (ret < 0) {
        LOG(ERROR) << "[UringFile::register_buffer] io_uring_register_buffers failed: "
                   << strerror(-ret);
        return false;
    }

    registered_buffer_ = buffer;
    registered_buffer_size_ = length;
    buffer_registered_ = true;

    LOG(INFO) << "[UringFile::register_buffer] Successfully registered buffer: "
              << "addr=" << buffer << ", size=" << length;
    return true;
}

void UringFile::unregister_buffer() {
    if (!buffer_registered_) {
        return;
    }

    if (ring_initialized_) {
        int ret = io_uring_unregister_buffers(&ring_);
        if (ret < 0) {
            LOG(ERROR) << "[UringFile::unregister_buffer] io_uring_unregister_buffers failed: "
                       << strerror(-ret);
        } else {
            LOG(INFO) << "[UringFile::unregister_buffer] Successfully unregistered buffer";
        }
    }

    registered_buffer_ = nullptr;
    registered_buffer_size_ = 0;
    buffer_registered_ = false;
}

}  // namespace mooncake

#endif  // USE_URING
