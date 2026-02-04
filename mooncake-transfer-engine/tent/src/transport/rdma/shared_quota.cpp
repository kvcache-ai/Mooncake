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

#include "tent/transport/rdma/shared_quota.h"
#include "tent/common/utils/os.h"

namespace mooncake {
namespace tent {
SharedQuotaManager::SharedQuotaManager(DeviceQuota* local_quota)
    : hdr_(nullptr),
      fd_(-1),
      size_(sizeof(SharedHeader)),
      created_(false),
      local_quota_(local_quota) {}

SharedQuotaManager::~SharedQuotaManager() { detach(); }

Status SharedQuotaManager::attach(const std::string& shm_name) {
    name_ = shm_name;

    // Open or create shared memory (mode 0666)
    fd_ = shm_open(name_.c_str(), O_RDWR | O_CREAT, 0666);
    if (fd_ < 0) {
        return Status::InternalError("shm_open failed: " +
                                     std::string(strerror(errno)));
    }

    // Ensure size
    if (ftruncate(fd_, static_cast<off_t>(size_)) != 0) {
        int e = errno;
        close(fd_);
        fd_ = -1;
        return Status::InternalError("ftruncate failed: " +
                                     std::string(strerror(e)));
    }

    // mmap
    void* ptr =
        mmap(nullptr, size_, PROT_READ | PROT_WRITE, MAP_SHARED, fd_, 0);
    if (ptr == MAP_FAILED) {
        int e = errno;
        close(fd_);
        fd_ = -1;
        return Status::InternalError("mmap failed: " +
                                     std::string(strerror(e)));
    }

    hdr_ = reinterpret_cast<SharedHeader*>(ptr);

    if (hdr_->magic != SHM_MAGIC || hdr_->version != SHM_VERSION) {
        created_ = true;
        Status s = initializeHeader();
        if (!s.ok()) {
            munmap(ptr, size_);
            close(fd_);
            hdr_ = nullptr;
            fd_ = -1;
            return s;
        }
    } else {
        created_ = false;
    }

    Status s = attachProcess();
    if (!s.ok()) return s;
    return Status::OK();
}

Status SharedQuotaManager::detach() {
    if (hdr_) {
        detachProcess();
        munmap(hdr_, size_);
        hdr_ = nullptr;
    }
    if (fd_ >= 0) {
        close(fd_);
        fd_ = -1;
    }
    return Status::OK();
}

Status SharedQuotaManager::initializeHeader() {
    memset(hdr_, 0, size_);
    Status s = initMutex(&hdr_->global_mutex);
    if (!s.ok()) {
        return s;
    }
    hdr_->version = SHM_VERSION;
    hdr_->magic = SHM_MAGIC;
    return Status::OK();
}

Status SharedQuotaManager::initMutex(pthread_mutex_t* m) {
    pthread_mutexattr_t attr;
    if (pthread_mutexattr_init(&attr) != 0) {
        return Status::InternalError("pthread_mutexattr_init failed");
    }
    if (pthread_mutexattr_setpshared(&attr, PTHREAD_PROCESS_SHARED) != 0) {
        pthread_mutexattr_destroy(&attr);
        return Status::InternalError("pthread_mutexattr_setpshared failed");
    }
#if defined(PTHREAD_MUTEX_ROBUST)
    if (pthread_mutexattr_setrobust(&attr, PTHREAD_MUTEX_ROBUST) != 0) {
        pthread_mutexattr_destroy(&attr);
        return Status::InternalError("pthread_mutexattr_setrobust failed");
    }
#endif
    if (pthread_mutex_init(m, &attr) != 0) {
        pthread_mutexattr_destroy(&attr);
        return Status::InternalError("pthread_mutex_init failed");
    }
    pthread_mutexattr_destroy(&attr);
    return Status::OK();
}

// attempt to acquire global lock; handle EOWNERDEAD
int SharedQuotaManager::lock() {
    if (!hdr_) return EINVAL;
    int rc = pthread_mutex_lock(&hdr_->global_mutex);
    if (rc == 0) return 0;
    if (rc == EOWNERDEAD) {
        // make consistent so others can continue
#if defined(PTHREAD_MUTEX_ROBUST)
        int rc2 = pthread_mutex_consistent(&hdr_->global_mutex);
        if (rc2 != 0) {
            return rc2;
        }
#endif
        // We hold the lock now — repair data if needed
        reclaimDeadPidsInternal();
        return 0;
    }
    return rc;
}

int SharedQuotaManager::unlock() {
    if (!hdr_) return EINVAL;
    return pthread_mutex_unlock(&hdr_->global_mutex);
}

void SharedQuotaManager::reclaimDeadPidsInternal() {
    if (!hdr_) return;
    // Assumes caller holds lock (but we also call from lock() on EOWNERDEAD)
    for (int i = 0; i < hdr_->num_devices; ++i) {
        // skip unused slots (dev_name empty)
        if (hdr_->devices[i].dev_name[0] == '\0') continue;
        SharedDeviceEntry& dev = hdr_->devices[i];
        for (int s = 0; s < MAX_PID_SLOTS; ++s) {
            pid_t p = dev.pid_usages[s].pid;
            if (p == 0) continue;
            if (!isPidAlive(p)) {
                // zero out slot — we'll recompute active_bytes in diffusion
                dev.pid_usages[s].pid = 0;
                dev.pid_usages[s].used_bytes = 0;
            }
        }
    }
}

bool SharedQuotaManager::isPidAlive(pid_t pid) {
    if (pid <= 0) return false;
    int r = kill(pid, 0);
    if (r == 0) return true;
    if (errno == ESRCH) return false;
    return true;  // other errors (EPERM) -> treat as alive
}

int SharedQuotaManager::findDeviceIdByNameLocked(const std::string& dev_name) {
    if (!hdr_) return -1;
    for (int i = 0; i < hdr_->num_devices; ++i) {
        if (hdr_->devices[i].dev_name[0] == '\0') continue;
        if (strncmp(hdr_->devices[i].dev_name, dev_name.c_str(),
                    sizeof(hdr_->devices[i].dev_name)) == 0)
            return i;
    }
    return -1;
}

PidUsage* SharedQuotaManager::findOrCreatePidSlotLocked(int dev_id, pid_t pid) {
    if (!hdr_) return nullptr;
    if (dev_id < 0 || dev_id >= hdr_->num_devices) return nullptr;
    SharedDeviceEntry& dev = hdr_->devices[dev_id];
    PidUsage* empty = nullptr;
    for (int s = 0; s < MAX_PID_SLOTS; ++s) {
        if (dev.pid_usages[s].pid == pid) return &dev.pid_usages[s];
        if (dev.pid_usages[s].pid == 0 && empty == nullptr)
            empty = &dev.pid_usages[s];
    }
    if (empty) {
        empty->pid = pid;
        empty->used_bytes = 0;
    }
    return empty;
}

PidUsage* SharedQuotaManager::findPidSlotLocked(int dev_id, pid_t pid) {
    if (!hdr_) return nullptr;
    if (dev_id < 0 || dev_id >= hdr_->num_devices) return nullptr;
    SharedDeviceEntry& dev = hdr_->devices[dev_id];
    for (int s = 0; s < MAX_PID_SLOTS; ++s) {
        if (dev.pid_usages[s].pid == pid) return &dev.pid_usages[s];
    }
    return nullptr;
}

Status SharedQuotaManager::attachProcess() {
    if (!hdr_) return Status::InvalidArgument("not attached");

    int rc = lock();
    if (rc != 0) {
        return Status::InternalError("failed to lock shared mutex: " +
                                     std::string(strerror(rc)));
    }

    auto topo = local_quota_->getTopology();
    for (size_t i = 0; i < topo->getNicCount(); ++i) {
        if (topo->getNicType(i) != Topology::NIC_RDMA) continue;
        auto dev_name = topo->getNicName(i);
        if (findDeviceIdByNameLocked(dev_name) >= 0) continue;
        int empty_idx = -1;
        for (int j = 0; j < MAX_DEVICES; ++j) {
            if (hdr_->devices[j].dev_name[0] == '\0') {
                empty_idx = j;
                break;
            }
        }
        if (empty_idx < 0) continue;
        strncpy(hdr_->devices[empty_idx].dev_name, dev_name.c_str(), 56);
        hdr_->devices[empty_idx].active_bytes = 0;
    }

    int count = 0;
    for (int i = 0; i < MAX_DEVICES; ++i)
        if (hdr_->devices[i].dev_name[0] != '\0') ++count;
    hdr_->num_devices = count;

    unlock();
    return Status::OK();
}

Status SharedQuotaManager::detachProcess() { return Status::OK(); }

Status SharedQuotaManager::diffusion() {
    if (!hdr_) return Status::InvalidArgument("not attached");
    pid_t pid = getpid();
    int rc = lock();
    if (rc != 0)
        return Status::InternalError("lock failed: " +
                                     std::string(strerror(rc)));
    for (int d = 0; d < hdr_->num_devices; ++d) {
        std::string dev_name = hdr_->devices[d].dev_name;
        auto dev_id = local_quota_->getTopology()->getNicId(dev_name);
        if (dev_name.empty() || dev_id < 0) continue;
        PidUsage* slot = findOrCreatePidSlotLocked(dev_id, pid);
        if (!slot) {
            unlock();
            return Status::InternalError("no free pid slot for device");
        }
        auto used_bytes = local_quota_->getActiveBytes(dev_id);
        slot->used_bytes = used_bytes;
        uint64_t sum = 0;
        for (int s = 0; s < MAX_PID_SLOTS; ++s)
            sum += hdr_->devices[d].pid_usages[s].used_bytes;
        uint64_t diffusion_active_bytes =
            sum < used_bytes ? 0 : sum - used_bytes;
        hdr_->devices[d].active_bytes = sum;
        local_quota_->setDiffusionActiveBytes(dev_id, diffusion_active_bytes);
    }
    unlock();
    return Status::OK();
}

}  // namespace tent
}  // namespace mooncake
