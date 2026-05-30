#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

namespace mooncake {

/**
 * @brief Send a file descriptor + data payload over a Unix socket (SCM_RIGHTS).
 * @return bytes sent on success, -1 on error.
 */
int ipc_send_fd(int socket, int fd, void *data, size_t data_len);

/**
 * @brief Receive a file descriptor + data payload from a Unix socket.
 * @return the received fd on success, -1 on error.
 */
int ipc_recv_fd(int socket, void *data, size_t data_len);

/**
 * @brief Manages anonymous shared memory segments backed by memfd.
 *
 * Each segment is created via memfd_create + mmap. The fd can be passed
 * to other processes (via Unix socket SCM_RIGHTS) for cross-process
 * zero-copy sharing.
 *
 * Thread-safe singleton; all operations are mutex-protected.
 */
class ShmHelper {
   public:
    struct ShmSegment {
        int fd = -1;
        void *base_addr = nullptr;
        size_t size = 0;
        std::string name;
        bool registered = false;
        bool is_local = false;
    };

    static ShmHelper *getInstance();

    void *allocate(size_t size);
    int free(void *addr);

    bool cleanup();

    // Find the segment that contains the given address
    std::shared_ptr<ShmSegment> get_shm(void *addr);

    const std::vector<std::shared_ptr<ShmSegment>> &get_shms() const {
        return shms_;
    }

    bool is_hugepage() const { return use_hugepage_; }

    ShmHelper(const ShmHelper &) = delete;
    ShmHelper &operator=(const ShmHelper &) = delete;

   private:
    ShmHelper();
    ~ShmHelper();

    std::vector<std::shared_ptr<ShmSegment>> shms_;
    static std::mutex shm_mutex_;
    bool use_hugepage_ = false;
};

}  // namespace mooncake
