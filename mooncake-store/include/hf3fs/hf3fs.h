#pragma once

#include <string>
#include <unordered_map>
#include <mutex>
#include <thread>
#include <hf3fs_usrbio.h>
#include "types.h"

namespace mooncake {

class StorageFile;

// Forward declaration of USRBIOResourceManager
struct Hf3fsConfig {
    // 3FS cluster related parameters

    // USRBIO related parameters
    std::string mount_root = "/";  // Mount point root directory
    size_t iov_size = 32 << 20;    // Shared memory size (32MB)
    size_t ior_entries = 16;       // Maximum number of requests in IO ring
    //`0` for no control with I/O depth.
    // If greater than 0, then only when `io_depth` I/O requests are in queue,
    // they will be issued to server as a batch. If smaller than 0, then USRBIO
    // will wait for at most `-io_depth` I/O requests are in queue and issue
    // them in one batch. If io_depth is 0, then USRBIO will issue all the
    // prepared I/O requests to server ASAP.
    size_t io_depth = 0;  // IO batch processing depth
    int ior_timeout = 0;  // IO timeout (milliseconds)
};

class USRBIOResourceManager {
   public:
    USRBIOResourceManager() {}

    void setDefaultParams(const Hf3fsConfig &config) {
        default_config_ = config;
    }

    struct ThreadUSRBIOResource *getThreadResource(const Hf3fsConfig &config);

    struct ThreadUSRBIOResource *getThreadResource() {
        return getThreadResource(default_config_);
    }

    ~USRBIOResourceManager();

   private:
    USRBIOResourceManager(const USRBIOResourceManager &) = delete;
    USRBIOResourceManager &operator=(const USRBIOResourceManager &) = delete;
    Hf3fsConfig default_config_;

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
    Hf3fsConfig config_;

    ThreadUSRBIOResource() : initialized(false) {}

    // Initialize resource
    bool Initialize(const Hf3fsConfig &config);

    // Cleanup resource
    void Cleanup();

    ~ThreadUSRBIOResource() { Cleanup(); }
};

class ThreeFSFile : public StorageFile {
   public:
    ThreeFSFile(const std::string &filename, int fd,
                USRBIOResourceManager *resource_manager);
    ~ThreeFSFile() override;

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

   private:
    USRBIOResourceManager *resource_manager_;
};

}  // namespace mooncake