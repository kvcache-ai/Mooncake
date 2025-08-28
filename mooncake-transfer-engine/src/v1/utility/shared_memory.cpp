// Copyright 2024 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <v1/utility/shared_memory.h>

namespace mooncake {
namespace v1 {

Status SharedMemoryManager::createOrAttach(
    const std::string& shm_name, const std::shared_ptr<Topology>& topology) {
    name_ = shm_name;
    size_t sz = sizeof(SharedHeader);
    int flags = O_RDWR | O_CREAT | O_EXCL;
    created_ = true;
    fd_ = shm_open(shm_name.c_str(), flags, 0666);
    if (fd_ < 0) {
        if (errno == EEXIST) {
            fd_ = shm_open(shm_name.c_str(), O_RDWR, 0666);
            created_ = false;
        } else {
            return Status::InternalError("shm_open failed: " +
                                         std::string(strerror(errno)));
        }
    }
    if (ftruncate(fd_, sz) != 0) {
        close(fd_);
        fd_ = -1;
        return Status::InternalError("ftruncate failed: " +
                                     std::string(strerror(errno)));
    }
    void* ptr = mmap(nullptr, sz, PROT_READ | PROT_WRITE, MAP_SHARED, fd_, 0);
    if (ptr == MAP_FAILED) {
        close(fd_);
        fd_ = -1;
        return Status::InternalError("mmap failed: " +
                                     std::string(strerror(errno)));
    }

    hdr_ = reinterpret_cast<SharedHeader*>(ptr);
    size_ = sz;
    if (created_) {
        CHECK_STATUS(initializeHeader(topology));
    } else {
        // sanity checks
        if (hdr_->magic != SHM_MAGIC || hdr_->version != SHM_VERSION) {
            munmap(ptr, sz);
            close(fd_);
            hdr_ = nullptr;
            fd_ = -1;
            return Status::InternalError(
                "shared header mismatch or uninitialized");
        }
    }
    return Status::OK();
}

void SharedMemoryManager::detach() {
    if (hdr_) {
        munmap((void*)hdr_, size_);
        hdr_ = nullptr;
    }
    if (fd_ >= 0) {
        close(fd_);
        fd_ = -1;
    }
}

int SharedMemoryManager::lock() {
    if (!hdr_) return EINVAL;
    int rc = pthread_mutex_lock(&hdr_->global_mutex);
    if (rc == 0) return 0;
    if (rc == EOWNERDEAD) {
        // previous owner died while holding lock -> we must make state
        // consistent call recovery
        int rc2 = pthread_mutex_consistent(&hdr_->global_mutex);
        if (rc2 != 0) {
            // cannot make it consistent
            return rc2;
        }
        // reclaim dead pids under lock
        reclaimDeadPidsInternal();
        return 0;
    }
    return rc;
}

int SharedMemoryManager::unlock() {
    if (!hdr_) return EINVAL;
    int rc = pthread_mutex_unlock(&hdr_->global_mutex);
    return rc;
}

Status SharedMemoryManager::initializeHeader(
    const std::shared_ptr<Topology>& topology) {
    // called only by creator
    memset(hdr_, 0, size_);
    hdr_->version = SHM_VERSION;
    hdr_->num_devices = static_cast<int>(topology->getDeviceList().size());
    CHECK_STATUS(initMutex(&hdr_->global_mutex));

    // initialize device entries (caller can fill real values later under
    // lock)
    for (int i = 0; i < (int)hdr_->num_devices; ++i) {
        hdr_->devices[i].dev_id = i;
        hdr_->devices[i].numa_id = topology->findDeviceNumaID(i);
        hdr_->devices[i].bw_gbps = topology->findDeviceBandwidth(i);
        hdr_->devices[i].active_bytes = 0;
        for (int s = 0; s < MAX_PID_SLOTS; ++s) {
            hdr_->devices[i].pid_usages[s].pid = 0;
            hdr_->devices[i].pid_usages[s].used_bytes = 0;
        }
    }

    std::atomic_thread_fence(std::memory_order_seq_cst);
    hdr_->magic = SHM_MAGIC;
    return Status::OK();
}

Status SharedMemoryManager::initMutex(pthread_mutex_t* m) {
    pthread_mutexattr_t attr;
    int rc = pthread_mutexattr_init(&attr);
    if (rc != 0) return Status::InternalError("pthread_mutexattr_init failed");
    rc = pthread_mutexattr_setpshared(&attr, PTHREAD_PROCESS_SHARED);
    if (rc != 0) {
        pthread_mutexattr_destroy(&attr);
        return Status::InternalError("setpshared failed");
    }
    rc = pthread_mutexattr_setrobust(&attr, PTHREAD_MUTEX_ROBUST);
    if (rc != 0) {
        pthread_mutexattr_destroy(&attr);
        return Status::InternalError("setrobust failed");
    }
    rc = pthread_mutex_init(m, &attr);
    pthread_mutexattr_destroy(&attr);
    if (rc != 0) return Status::InternalError("pthread_mutex_init failed");
    return Status::OK();
}

// reclaim implementation: caller must hold the mutex or call via lock()
void SharedMemoryManager::reclaimDeadPidsInternal() {
    if (!hdr_) return;
    for (int i = 0; i < hdr_->num_devices; ++i) {
        SharedDeviceEntry& dev = hdr_->devices[i];
        for (int s = 0; s < MAX_PID_SLOTS; ++s) {
            pid_t p = dev.pid_usages[s].pid;
            if (p == 0) continue;
            if (!isPidAlive(p)) {
                uint64_t used = dev.pid_usages[s].used_bytes;
                // safety: dev.active_bytes >= used generally, but clamp
                // defensively
                uint64_t cur = dev.active_bytes;
                if (cur >= used)
                    dev.active_bytes = cur - used;
                else
                    dev.active_bytes = 0;
                dev.pid_usages[s].pid = 0;
                dev.pid_usages[s].used_bytes = 0;
            }
        }
    }
}

}  // namespace v1
}  // namespace mooncake
