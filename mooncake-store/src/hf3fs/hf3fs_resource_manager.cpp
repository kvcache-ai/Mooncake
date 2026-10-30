#include "hf3fs/hf3fs.h"

#include <atomic>

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

namespace {
std::atomic<uint64_t> next_manager_id{1};
}  // namespace

USRBIOResourceManager::USRBIOResourceManager()
    : cache_id_(next_manager_id.fetch_add(1, std::memory_order_relaxed)) {}

// Like the I/O using the returned pointer, lookup must not race destruction
// of this manager. TLS borrows resources; it never extends their lifetime.
struct ThreadUSRBIOResource *USRBIOResourceManager::getThreadResource(
    const Hf3fsConfig &config) {
    struct Cache {
        uint64_t manager_id = 0;
        ThreadUSRBIOResource *resource = nullptr;
    };
    static thread_local Cache cache;

    // Check the identity before dereferencing the borrowed pointer: a previous
    // manager may have been destroyed, even at the same address as this one.
    if (cache.manager_id == cache_id_ && cache.resource->initialized) {
        return cache.resource;
    }

    std::lock_guard<std::mutex> lock(resource_map_mutex);
    auto &resource = thread_resources[std::this_thread::get_id()];
    if (!resource) resource = std::make_unique<ThreadUSRBIOResource>();
    // Cleanup() invalidates the fast path. Failed initialization is retried on
    // the next lookup, without caching a partially initialized resource.
    if (!resource->Initialize(config)) return nullptr;
    cache = {cache_id_, resource.get()};
    return resource.get();
}

USRBIOResourceManager::~USRBIOResourceManager() = default;

}  // namespace mooncake
