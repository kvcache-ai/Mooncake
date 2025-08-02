#pragma once

#include <string>
#include <unordered_map>
#include <sys/uio.h>
#include <cstdio>
#include "types.h"
#include <atomic>
#include <thread>
#include <sys/file.h>

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

}  // namespace mooncake

#ifdef USE_3FS
#include <hf3fs/hf3fs.h>
#endif
