#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

namespace mooncake {

/**
 * @brief Manages anonymous shared memory segments backed by memfd.
 *
 * Each segment is created via memfd_create + mmap. The fd can be passed to
 * other processes via UdsConnection::sendFd() for cross-process zero-copy
 * sharing.
 *
 * Thread-safe singleton; all operations are mutex-protected.
 */
class ShmHelper {
   public:
    struct ShmSegment {
        int fd = -1;
        void *base_addr = nullptr;
        // Size of the actual mapping; may be padded up to the hugepage/2MB
        // boundary when SPDK registration is enabled (MC_STORE_REGISTER_SPDK=1).
        size_t size = 0;
        // Size the caller requested in allocate(); == size unless padded. Callers
        // that must match the original request (e.g. DummyClient::register_buffer)
        // compare against this, not size, so they don't re-derive the alignment.
        size_t requested_size = 0;
        std::string name;
        bool registered = false;
        bool spdk_registered = false;
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
    bool register_spdk_ = false;
};

}  // namespace mooncake
