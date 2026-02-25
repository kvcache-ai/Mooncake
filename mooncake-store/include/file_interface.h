#pragma once

#include <string>
#include <unordered_map>
#include <sys/uio.h>
#include <cstdio>
#include "types.h"
#include "mutex.h"
#include <atomic>
#include <thread>
#include <sys/file.h>
#ifdef USE_URING
#include <liburing.h>
#endif

namespace mooncake {
class FileLockRAII {
   public:
    enum class LockType { READ, WRITE };

    FileLockRAII(int fd, LockType type) : fd_(fd), locked_(false) {
        if (type == LockType::READ) {
            locked_ = (flock(fd_, LOCK_SH) == 0);
        } else {
            locked_ = (flock(fd_, LOCK_EX) == 0);
        }
    }

    ~FileLockRAII() {
        if (locked_) {
            flock(fd_, LOCK_UN);
        }
    }

    FileLockRAII(const FileLockRAII &) = delete;
    FileLockRAII &operator=(const FileLockRAII &) = delete;

    FileLockRAII(FileLockRAII &&other) noexcept
        : fd_(other.fd_), locked_(other.locked_) {
        other.locked_ = false;
    }

    bool is_locked() const { return locked_; }

   private:
    int fd_;
    bool locked_;
};

/**
 * @class LocalFile
 * @brief RAII wrapper for file operations with thread-safe locking support
 *
 * Provides thread-safe file I/O operations including read/write and vectorized
 * I/O. Implements proper resource management through RAII pattern.
 */
class StorageFile {
   public:
    StorageFile(const std::string &filename, int fd)
        : filename_(filename),
          fd_(fd),
          error_code_(ErrorCode::OK),
          is_locked_(false) {}
    /**
     * @brief Destructor
     * @note Automatically closes the file and releases resources
     */
    virtual ~StorageFile() = default;

    /**
     * @brief Writes data from buffer to file
     * @param buffer Input buffer containing data to write
     * @param length Number of bytes to write
     * @return tl::expected<size_t, ErrorCode> containing number of bytes
     * written on success, or ErrorCode on failure
     * @note Thread-safe operation with write locking
     */
    virtual tl::expected<size_t, ErrorCode> write(const std::string &buffer,
                                                  size_t length) = 0;

    /**
     * @brief Writes data from buffer to file
     * @param data Input span containing data to write
     * @param length Number of bytes to write
     * @return tl::expected<size_t, ErrorCode> containing number of bytes
     * written on success, or ErrorCode on failure
     * @note Thread-safe operation with write locking
     */
    virtual tl::expected<size_t, ErrorCode> write(std::span<const char> data,
                                                  size_t length) = 0;

    /**
     * @brief Reads data from file into buffer
     * @param buffer Output buffer for read data
     * @param length Maximum number of bytes to read
     * @return tl::expected<size_t, ErrorCode> containing number of bytes read
     * on success, or ErrorCode on failure
     * @note Thread-safe operation with read locking
     */
    virtual tl::expected<size_t, ErrorCode> read(std::string &buffer,
                                                 size_t length) = 0;

    /**
     * @brief Scattered write at specified file offset
     * @param iov Array of I/O vectors
     * @param iovcnt Number of elements in iov array
     * @param offset File offset to write at
     * @return tl::expected<size_t, ErrorCode> containing total bytes written on
     * success, or ErrorCode on failure
     * @note Thread-safe operation with write locking
     */
    virtual tl::expected<size_t, ErrorCode> vector_write(const iovec *iov,
                                                         int iovcnt,
                                                         off_t offset) = 0;

    /**
     * @brief Scattered read from specified file offset
     * @param iov Array of I/O vectors
     * @param iovcnt Number of elements in iov array
     * @param offset File offset to read from
     * @return tl::expected<size_t, ErrorCode> containing total bytes read on
     * success, or ErrorCode on failure
     * @note Thread-safe operation with read locking
     */
    virtual tl::expected<size_t, ErrorCode> vector_read(const iovec *iov,
                                                        int iovcnt,
                                                        off_t offset) = 0;

    template <typename T>
    tl::expected<T, ErrorCode> make_error(ErrorCode code) {
        error_code_ = code;
        return tl::make_unexpected(code);
    }

    /**
     * @brief file locking mechanism
     */
    FileLockRAII acquire_write_lock() {
        return FileLockRAII(fd_, FileLockRAII::LockType::WRITE);
    }

    FileLockRAII acquire_read_lock() {
        return FileLockRAII(fd_, FileLockRAII::LockType::READ);
    }

    /**
     * @brief Gets the current error code
     * @return Current error code
     */
    ErrorCode get_error_code() { return error_code_; }

   protected:
    std::string filename_;
    int fd_;
    ErrorCode error_code_{ErrorCode::OK};
    std::atomic<bool> is_locked_{false};
};

class PosixFile : public StorageFile {
   public:
    PosixFile(const std::string &filename, int fd);
    ~PosixFile() override;

    tl::expected<size_t, ErrorCode> write(const std::string &buffer,
                                          size_t length) override;
    tl::expected<size_t, ErrorCode> write(std::span<const char> data,
                                          size_t length) override;
    tl::expected<size_t, ErrorCode> read(std::string &buffer,
                                         size_t length) override;
    tl::expected<size_t, ErrorCode> vector_write(const iovec *iov, int iovcnt,
                                                 off_t offset) override;
    tl::expected<size_t, ErrorCode> vector_read(const iovec *iov, int iovcnt,
                                                off_t offset) override;
};

#ifdef USE_URING
class UringFile : public StorageFile {
   public:
    UringFile(const std::string &filename, int fd, unsigned queue_depth = 32,
              bool use_direct_io = false);
    ~UringFile() override;

    tl::expected<size_t, ErrorCode> write(const std::string &buffer,
                                          size_t length) override;
    tl::expected<size_t, ErrorCode> write(std::span<const char> data,
                                          size_t length) override;
    tl::expected<size_t, ErrorCode> read(std::string &buffer,
                                         size_t length) override;
    tl::expected<size_t, ErrorCode> vector_write(const iovec *iov, int iovcnt,
                                                 off_t offset) override;
    tl::expected<size_t, ErrorCode> vector_read(const iovec *iov, int iovcnt,
                                                off_t offset) override;

    // Zero-copy interface for O_DIRECT: caller must provide aligned buffer
    tl::expected<size_t, ErrorCode> read_aligned(void *buffer, size_t length,
                                                 off_t offset = 0);
    tl::expected<size_t, ErrorCode> write_aligned(const void *buffer,
                                                  size_t length,
                                                  off_t offset = 0);

    // Flush data to stable storage via
    // io_uring_prep_fsync(IORING_FSYNC_DATASYNC). Must be called after write
    // and before writing dependent metadata files.
    tl::expected<void, ErrorCode> datasync();

    // Buffer registration interface for high-performance I/O
    // Register a single buffer with io_uring to avoid get_user_pages() overhead
    // Returns true on success, false on failure
    bool register_buffer(void *buffer, size_t length);

    // Unregister previously registered buffer
    void unregister_buffer();

    // Check if a buffer is currently registered
    bool is_buffer_registered() const { return buffer_registered_; }

   private:
    struct io_uring ring_;
    bool ring_initialized_;
    bool files_registered_;
    bool buffer_registered_;
    unsigned queue_depth_;
    bool use_direct_io_;
    static constexpr size_t ALIGNMENT_ =
        4096;  // O_DIRECT alignment requirement

    // Registered buffer info
    void *registered_buffer_;
    size_t registered_buffer_size_;
    struct iovec registered_iovec_;

    /// Submit all pending SQEs and wait for exactly @p n completions.
    /// Returns the total bytes transferred, or an error.
    tl::expected<size_t, ErrorCode> submit_and_wait_n(int n);

    /// Calculate optimal chunk size for parallel I/O based on:
    /// - total_len: remaining bytes to transfer
    /// - available_depth: number of queue slots available
    /// - min_chunk_size: minimum chunk size (must be power of 2)
    /// Returns a power-of-2 chunk size that maximizes queue utilization.
    size_t calculate_chunk_size(size_t total_len, unsigned available_depth,
                                size_t min_chunk_size) const;

    /// Allocate aligned buffer for O_DIRECT
    void *alloc_aligned_buffer(size_t size) const;

    /// Free aligned buffer
    void free_aligned_buffer(void *ptr) const;

    /// Mutex to serialize concurrent access to ring_
    mutable Mutex ring_mutex_;
};
#endif  // USE_URING

}  // namespace mooncake

#ifdef USE_3FS
#include <hf3fs/hf3fs.h>
#endif
