#pragma once

#include <cstddef>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

namespace mooncake {

class ShmHelper {
   public:
    struct ShmSegment {
        int fd = -1;
        void* base_addr = nullptr;
        size_t size = 0;
        std::string name;
        bool registered = false;
        bool is_local = false;
    };

    static ShmHelper* getInstance();

    void* allocate(size_t size);
    int free(void* addr);

    bool cleanup();

    // Get the shm that contains the given address
    // Returns a shared_ptr to ensure the segment remains valid
    std::shared_ptr<ShmSegment> get_shm(void* addr);

    const std::vector<std::shared_ptr<ShmSegment>>& get_shms() const {
        return shms_;
    }

    // Host buffers may be registered after setup starts the heartbeat.
    void set_registered(const std::shared_ptr<ShmSegment>& shm, bool value) {
        std::lock_guard<std::mutex> lock(shm_mutex_);
        if (shm) shm->registered = value;
    }

    size_t count_registered() const {
        std::lock_guard<std::mutex> lock(shm_mutex_);
        size_t count = 0;
        for (const auto& shm : shms_) {
            if (shm->registered) ++count;
        }
        return count;
    }

    std::vector<std::shared_ptr<ShmSegment>> get_registered_snapshot() const {
        std::lock_guard<std::mutex> lock(shm_mutex_);
        std::vector<std::shared_ptr<ShmSegment>> registered;
        for (const auto& shm : shms_) {
            if (shm->registered) registered.push_back(shm);
        }
        return registered;
    }

    bool is_hugepage() const { return use_hugepage_; }

    ShmHelper(const ShmHelper&) = delete;
    ShmHelper& operator=(const ShmHelper&) = delete;

   private:
    ShmHelper();
    ~ShmHelper();

    std::vector<std::shared_ptr<ShmSegment>> shms_;
    static std::mutex shm_mutex_;
    bool use_hugepage_ = false;
};

}  // namespace mooncake
