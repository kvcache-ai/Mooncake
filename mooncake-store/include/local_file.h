#pragma once

#include <string>
#include <sys/uio.h>
#include <cstdio>
#include "types.h"
#include <atomic>

namespace mooncake {

/**
 * @class LocalFile
 * @brief RAII wrapper for file operations with thread-safe locking support
 * 
 * Provides thread-safe file I/O operations including read/write and vectorized I/O.
 * Implements proper resource management through RAII pattern.
 */
class LocalFile {
public:
    /**
     * @brief Constructs a LocalFile instance
     * @param filename Path to the file being managed
     * @param file FILE pointer to the opened file
     * @param ec Initial error code (defaults to OK)
     * @note Takes ownership of the FILE pointer
     */
    explicit LocalFile(const std::string& filename, FILE *file, ErrorCode ec = ErrorCode::OK);
    
    /**
     * @brief Destructor
     * @note Automatically closes the file and releases resources
     */
    ~LocalFile();

    /**
     * @brief Reads data from file into buffer
     * @param buffer Output buffer for read data
     * @param length Maximum number of bytes to read
     * @return Number of bytes read on success, -1 on error
     * @note Thread-safe operation with read locking
     */
    ssize_t read(std::string &buffer, size_t length);

    /**
     * @brief Writes data from buffer to file
     * @param buffer Input buffer containing data to write
     * @param length Number of bytes to write
     * @return Number of bytes written on success, -1 on error
     * @note Thread-safe operation with write locking
     */
    ssize_t write(const std::string &buffer, size_t length);

    /**
     * @brief Scattered read from specified file offset
     * @param iov Array of I/O vectors
     * @param iovcnt Number of elements in iov array
     * @param offset File offset to read from
     * @return Total bytes read on success, -1 on error
     * @note Thread-safe operation with read locking
     */
    ssize_t preadv(const iovec *iov, int iovcnt, off_t offset);

    /**
     * @brief Scattered write at specified file offset
     * @param iov Array of I/O vectors
     * @param iovcnt Number of elements in iov array
     * @param offset File offset to write at
     * @return Total bytes written on success, -1 on error
     * @note Thread-safe operation with write locking
     */
    ssize_t pwritev(const iovec *iov, int iovcnt, off_t offset);

    /**
     * @brief Gets the current error code
     * @return Current error code
     */
    ErrorCode get_error_code(){
        return error_code_;
    }

private:
    /**
     * @brief file locking mechanism
     */
    int acquire_write_lock();
    int acquire_read_lock();
    int release_lock();

    std::string filename_;
    FILE *file_;
    ErrorCode error_code_;
    std::atomic<bool> is_locked_{false};
};

} // namespace mooncake


