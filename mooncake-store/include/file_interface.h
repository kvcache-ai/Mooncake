#pragma once

#include <string>
#include <unordered_map>
#include <sys/uio.h>
#include <cstdio>
#include "types.h"
#include <atomic>
#include <thread>
#include <sys/file.h>

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


    FileLockRAII(const FileLockRAII&) = delete;
    FileLockRAII& operator=(const FileLockRAII&) = delete;

    FileLockRAII(FileLockRAII&& other) noexcept 
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
 * Provides thread-safe file I/O operations including read/write and vectorized I/O.
 * Implements proper resource management through RAII pattern.
 */
class StorageFile {
public:    

    StorageFile(const std::string &filename, int fd)
        : filename_(filename), fd_(fd), error_code_(ErrorCode::OK), is_locked_(false) {}
    /**
     * @brief Destructor
     * @note Automatically closes the file and releases resources
     */
    virtual ~StorageFile() = default;

    /**
     * @brief Writes data from buffer to file
     * @param buffer Input buffer containing data to write
     * @param length Number of bytes to write
     * @return Number of bytes written on success, -1 on error
     * @note Thread-safe operation with write locking
     */
    virtual ssize_t write(const std::string &buffer, size_t length) = 0;

    /**
     * @brief Reads data from file into buffer
     * @param buffer Output buffer for read data
     * @param length Maximum number of bytes to read
     * @return Number of bytes read on success, -1 on error
     * @note Thread-safe operation with read locking
     */
    virtual ssize_t read(std::string &buffer, size_t length) = 0;

    /**
     * @brief Scattered write at specified file offset
     * @param iov Array of I/O vectors
     * @param iovcnt Number of elements in iov array
     * @param offset File offset to write at
     * @return Total bytes written on success, -1 on error
     * @note Thread-safe operation with write locking
     */
    virtual ssize_t vector_write(const iovec *iov, int iovcnt, off_t offset) = 0;

    /**
     * @brief Scattered read from specified file offset
     * @param iov Array of I/O vectors
     * @param iovcnt Number of elements in iov array
     * @param offset File offset to read from
     * @return Total bytes read on success, -1 on error
     * @note Thread-safe operation with read locking
     */
    virtual ssize_t vector_read(const iovec *iov, int iovcnt, off_t offset) = 0;

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
    ErrorCode get_error_code(){
        return error_code_;
    }

protected:
    std::string filename_;
    int fd_;
    ErrorCode error_code_;
    std::atomic<bool> is_locked_{false};
};

class PosixFile : public StorageFile {
public:
    PosixFile(const std::string &filename, int fd);
    ~PosixFile() override;

    ssize_t write(const std::string &buffer, size_t length) override;
    ssize_t read(std::string &buffer, size_t length) override;
    ssize_t vector_write(const iovec *iov, int iovcnt, off_t offset) override;
    ssize_t vector_read(const iovec *iov, int iovcnt, off_t offset) override;

};

#ifdef USE_3FS
class ThreeFSFile : public StorageFile {
public:
    ThreeFSFile(const std::string &filename, int fd, USRBIOResourceManager* resource_manager);
    ~ThreeFSFile() override;

    ssize_t write(const std::string &buffer, size_t length) override;  
    ssize_t read(std::string &buffer, size_t length) override;
    ssize_t vector_write(const iovec *iov, int iovcnt, off_t offset) override;
    ssize_t vector_read(const iovec *iov, int iovcnt, off_t offset) override;

private:
    USRBIOResourceManager* resource_manager_;
};
#endif // USE_3FS
} // namespace mooncake


