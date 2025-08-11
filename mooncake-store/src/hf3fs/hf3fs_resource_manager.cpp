#include "file_interface.h"

namespace mooncake {
// ============================================================================
// USRBIO Resource manager Implementation
// ============================================================================
bool ThreadUSRBIOResource::Initialize(const Hf3fsConfig &config) {
    if (initialized) {
        return true;
    }

    this->config_ = config;

    // Create shared memory
    int ret = hf3fs_iovcreate(&iov_, config.mount_root.c_str(), config.iov_size,
                              0, -1);
    if (ret < 0) {
        return false;
    }

    // Create read I/O ring
    ret = hf3fs_iorcreate4(&ior_read_, config.mount_root.c_str(),
                           config.ior_entries, true, config.io_depth,
                           config.ior_timeout, -1, 0);
    if (ret < 0) {
        hf3fs_iovdestroy(&iov_);
        return false;
    }

    // Create write I/O ring
    ret = hf3fs_iorcreate4(&ior_write_, config.mount_root.c_str(),
                           config.ior_entries, false, config.io_depth,
                           config.ior_timeout, -1, 0);
    if (ret < 0) {
        hf3fs_iordestroy(&ior_read_);
        hf3fs_iovdestroy(&iov_);
        return false;
    }

    initialized = true;
    return true;
}

void ThreadUSRBIOResource::Cleanup() {
    if (!initialized) {
        return;
    }

    // Destroy USRBIO resources
    hf3fs_iordestroy(&ior_write_);
    hf3fs_iordestroy(&ior_read_);
    hf3fs_iovdestroy(&iov_);

    initialized = false;
}

// Resource manager implementation
struct ThreadUSRBIOResource *USRBIOResourceManager::getThreadResource(
    const Hf3fsConfig &config) {
    std::thread::id thread_id = std::this_thread::get_id();

    {
        std::lock_guard<std::mutex> lock(resource_map_mutex);

        // Find if current thread already has resources
        auto it = thread_resources.find(thread_id);
        if (it != thread_resources.end()) {
            return it->second;
        }

        // Create new thread resources
        ThreadUSRBIOResource *resource = new ThreadUSRBIOResource();
        if (!resource->Initialize(config)) {
            delete resource;
            return nullptr;
        }

        // Store resource mapping
        thread_resources[thread_id] = resource;
        return resource;
    }
}

USRBIOResourceManager::~USRBIOResourceManager() {
    // Clean up all thread resources
    for (auto &pair : thread_resources) {
        delete pair.second;
    }
    thread_resources.clear();
}

}  // namespace mooncake