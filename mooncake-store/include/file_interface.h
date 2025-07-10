#pragma once

#include <string>
#include <unordered_map>
#include <sys/uio.h>
#include <cstdio>
#include "types.h"
#include <atomic>
#include <thread>
#include <sys/stat.h>  // for fstat(), struct stat
#include <unistd.h>    // for file descriptor operations

#ifdef USE_3FS
#include <hf3fs_usrbio.h>
#endif

namespace mooncake {
#ifdef USE_3FS
// Forward declaration of USRBIOResourceManager
struct ThreeFSParams {
    // 3FS cluster related parameters
    
    // USRBIO related parameters
    std::string mount_root = "/";    // Mount point root directory
    size_t iov_size = 1024 * 1024;         // Shared memory size (1MB)
    size_t ior_entries = 1024;             // Maximum number of requests in IO ring
    //`0` for no control with I/O depth.
    // If greater than 0, then only when `io_depth` I/O requests are in queue, they will be issued to server as a batch.
    // If smaller than 0, then USRBIO will wait for at most `-io_depth` I/O requests are in queue and issue them in one batch. 
    // If io_depth is 0, then USRBIO will issue all the prepared I/O requests to server ASAP.
    size_t io_depth = 0;                  // IO batch processing depth
    int ior_timeout = 0;                   // IO timeout (milliseconds)
    
};

class USRBIOResourceManager {
public:

    USRBIOResourceManager() {}

    void setDefaultParams(const ThreeFSParams& params) {
        default_params_ = params;
    }

    struct ThreadUSRBIOResource* getThreadResource(
        const ThreeFSParams &params);

    struct ThreadUSRBIOResource* getThreadResource() {
        return getThreadResource(default_params_);
    }

    ~USRBIOResourceManager();


private:
    USRBIOResourceManager(const USRBIOResourceManager &) = delete;
    USRBIOResourceManager &operator=(const USRBIOResourceManager &) = delete;
    ThreeFSParams default_params_;

    // Thread resources map protection lock
    std::mutex resource_map_mutex;

    // ThreadID to resource mapping
    std::unordered_map<std::thread::id, struct ThreadUSRBIOResource *>
        thread_resources;
};

// Thread level USRBIO resource structure
struct ThreadUSRBIOResource {
  // USRBIO resources
  struct hf3fs_iov iov_;
  struct hf3fs_ior ior_read_;
  struct hf3fs_ior ior_write_;

  // Resource initialization status
  bool initialized;

  // Resource belongs to parameters
  ThreeFSParams params;

  ThreadUSRBIOResource() : initialized(false) {}

  // Initialize resource
  bool Initialize(const ThreeFSParams &params);

  // Cleanup resource
  void Cleanup();

  ~ThreadUSRBIOResource() { Cleanup(); }
};
#endif // USE_3FS
/**
 * @class LocalFile
 * @brief RAII wrapper for file operations with thread-safe locking support
 * 
 * Provides thread-safe file I/O operations including read/write and vectorized I/O.
 * Implements proper resource management through RAII pattern.
 */
class StorageFile {
public:    

    StorageFile(const std::string &filename)
        : filename_(filename), error_code_(ErrorCode::OK), is_locked_(false) {}
    /**
     * @brief Destructor
     * @note Automatically closes the file and releases resources
     */
    virtual ~StorageFile() = default;

    /**
     * @brief Reads data from file into buffer
     * @param buffer Output buffer for read data
     * @param length Maximum number of bytes to read
     * @return Number of bytes read on success, -1 on error
     * @note Thread-safe operation with read locking
     */
    virtual ssize_t read(std::string &buffer, size_t length) = 0;

    /**
     * @brief Writes data from buffer to file
     * @param buffer Input buffer containing data to write
     * @param length Number of bytes to write
     * @return Number of bytes written on success, -1 on error
     * @note Thread-safe operation with write locking
     */
    virtual ssize_t write(const std::string &buffer, size_t length) = 0;

    /**
     * @brief Scattered read from specified file offset
     * @param iov Array of I/O vectors
     * @param iovcnt Number of elements in iov array
     * @param offset File offset to read from
     * @return Total bytes read on success, -1 on error
     * @note Thread-safe operation with read locking
     */
    virtual ssize_t preadv(const iovec *iov, int iovcnt, off_t offset) = 0;

    /**
     * @brief Scattered write at specified file offset
     * @param iov Array of I/O vectors
     * @param iovcnt Number of elements in iov array
     * @param offset File offset to write at
     * @return Total bytes written on success, -1 on error
     * @note Thread-safe operation with write locking
     */
    virtual ssize_t pwritev(const iovec *iov, int iovcnt, off_t offset) = 0;

    /**
     * @brief Gets the current length/size of the file
     * @return File size in bytes on success, -1 on error
     * @note Thread-safe operation
     */
    virtual ssize_t length() = 0;

    /**
     * @brief Gets the current error code
     * @return Current error code
     */
    ErrorCode get_error_code(){
        return error_code_;
    }

    /**
     * @brief file locking mechanism
     */
    virtual int acquire_write_lock() = 0;
    virtual int acquire_read_lock() = 0;
    virtual int release_lock() = 0;

protected:
    std::string filename_;
    ErrorCode error_code_;
    std::atomic<bool> is_locked_{false};
};

class PosixFile : public StorageFile {
public:
    PosixFile(const std::string &filename, FILE *file);
    ~PosixFile() override;

    ssize_t read(std::string &buffer, size_t length) override;
    ssize_t write(const std::string &buffer, size_t length) override;
    ssize_t preadv(const iovec *iov, int iovcnt, off_t offset) override;
    ssize_t pwritev(const iovec *iov, int iovcnt, off_t offset) override;

    int acquire_write_lock() override;
    int acquire_read_lock() override;
    int release_lock() override;

    ssize_t length() override {
        if (!file_) {
            error_code_ = ErrorCode::FILE_INVALID_HANDLE;
            return -1;
        }

        // Save current position
        long current_pos = ftell(file_);
        if (current_pos == -1) {
            error_code_ = ErrorCode::FILE_INVALID_HANDLE;
            return -1;
        }

        // Seek to end to get size
        if (fseek(file_, 0, SEEK_END) != 0) {
            error_code_ = ErrorCode::FILE_INVALID_HANDLE;
            return -1;
        }

        long size = ftell(file_);
        if (size == -1) {
            error_code_ = ErrorCode::FILE_INVALID_HANDLE;
            return -1;
        }

        // Restore original position
        if (fseek(file_, current_pos, SEEK_SET) != 0) {
            error_code_ = ErrorCode::FILE_INVALID_HANDLE;
            return -1;
        }

        return static_cast<ssize_t>(size);
    }

private:
    FILE *file_;

};

#ifdef USE_3FS
class ThreeFSFile : public StorageFile {
public:
    ThreeFSFile(const std::string &filename, int fd, USRBIOResourceManager* resource_manager);
    ~ThreeFSFile() override;
    static std::unique_ptr<ThreeFSFile> create(
    const std::string& filename, int fd, USRBIOResourceManager* resource_manager);

    ssize_t read(std::string &buffer, size_t length) override;
    ssize_t write(const std::string &buffer, size_t length) override;  
    ssize_t preadv(const iovec *iov, int iovcnt, off_t offset) override;
    ssize_t pwritev(const iovec *iov, int iovcnt, off_t offset) override;

    int acquire_write_lock() override;
    int acquire_read_lock() override;
    int release_lock() override;

    // static long get_elapsed_us(const struct timespec& start);

    ssize_t length() override {
        if (fd_ < 0) {
            error_code_ = ErrorCode::FILE_INVALID_HANDLE;
            return -1;
        }

        struct stat st;
        if (fstat(fd_, &st) != 0) {
            error_code_ = ErrorCode::FILE_INVALID_HANDLE;
            return -1;
        }

        return static_cast<ssize_t>(st.st_size);
    }

private:

    int fd_;
    USRBIOResourceManager* resource_manager_;
};
#endif // USE_3FS
} // namespace mooncake


