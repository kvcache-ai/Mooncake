// Copyright 2025 KVCache.AI
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

#include "v1/transport/rdma/scheduler.h"

#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <cerrno>
#include <cstring>
#include <cstdint>
#include <chrono>
#include <thread>
#include <fstream>
#include <iostream>
#include <pthread.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <glog/logging.h>
#include <infiniband/verbs.h>
#include <set>
#include <vector>
#include <string>
#include <unordered_map>
#include <algorithm>

namespace mooncake {
namespace v1 {

static uint64_t now_ms() {
    using namespace std::chrono;
    return duration_cast<milliseconds>(steady_clock::now().time_since_epoch())
        .count();
}

constexpr size_t DEVNAME_LEN = 64;
constexpr size_t MAX_DEVICES = 64;
constexpr size_t MAX_LEASES_PER_DEVICE = 4096;
const uint32_t MAGIC_NUMBER = 0x6D6FCAFE;

// total wait for follower to see ftruncate+magic
static constexpr int SHM_OPEN_WAIT_MS = 5000;
static constexpr int SHM_POLL_INTERVAL_MS = 10;  // poll interval

// Shared memory layout (only in cpp)

// Shared memory layout
struct SharedLease {
    bool is_valid;
    LeaseId lease_id;
    uint64_t allocated_bytes;
    uint64_t expires_at_ms;  // epoch ms
    pid_t owner_pid;
};

struct SharedDeviceEntry {
    char name[DEVNAME_LEN];
    int numa_node;
    uint32_t lease_count;
    uint64_t outstanding_bytes;
    uint32_t free_slot;
    SharedLease leases[MAX_LEASES_PER_DEVICE];
};

struct SharedState {
    uint32_t magic;
    pthread_mutex_t mtx;
    uint32_t device_count;
    SharedDeviceEntry devices[MAX_DEVICES];
    LeaseId lease_id_counter;
};

// Create or open shared state mapping.
// - name : shm name (must start with '/')
// - size : total shm size to allocate (must be >= sizeof(SharedState))
// - need_init: output -> true if caller is the creator and must initialize the
// state Returns mapped pointer (SharedState*) on success, nullptr on error
// (errno set or Status returned separately).
static SharedState* createSharedState(const char* name, bool& need_init) {
    const size_t size = sizeof(SharedState);
    if (!name) {
        errno = EINVAL;
        return nullptr;
    }

    // Try create (atomic O_EXCL helps avoid two creators)
    int fd = shm_open(name, O_RDWR | O_CREAT | O_EXCL, 0666);
    if (fd >= 0) {
        // We are the creator
        need_init = true;

        if (ftruncate(fd, static_cast<off_t>(size)) != 0) {
            int saved_errno = errno;
            // cleanup half-created shm
            shm_unlink(name);
            close(fd);
            errno = saved_errno;
            return nullptr;
        }

        // Map only the SharedState region (we don't need to map the full file
        // here)
        void* map =
            mmap(nullptr, size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
        // close fd early — mapping remains valid
        close(fd);

        if (map == MAP_FAILED) {
            int saved_errno = errno;
            shm_unlink(name);
            errno = saved_errno;
            return nullptr;
        }

        // zero the shared region so fields are deterministic (magic==0)
        std::memset(map, 0, size);
        return reinterpret_cast<SharedState*>(map);
    }

    // Creation failed
    if (errno != EEXIST) {
        return nullptr;
    }

    // File already exists: open and wait until creator sets size and publishes
    // magic.
    int fd2 = -1;
    int waited = 0;
    bool opened = false;
    while (waited < SHM_OPEN_WAIT_MS) {
        fd2 = shm_open(name, O_RDWR, 0666);
        if (fd2 >= 0) {
            // check file size (creator should have ftruncated)
            struct stat st;
            if (fstat(fd2, &st) == 0 &&
                static_cast<size_t>(st.st_size) >= size) {
                opened = true;
                break;
            }
            close(fd2);
        }
        std::this_thread::sleep_for(
            std::chrono::milliseconds(SHM_POLL_INTERVAL_MS));
        waited += SHM_POLL_INTERVAL_MS;
    }
    if (!opened) {
        errno = ETIMEDOUT;
        return nullptr;
    }

    // map the shared state region
    void* map = mmap(nullptr, size, PROT_READ | PROT_WRITE, MAP_SHARED, fd2, 0);
    close(fd2);
    if (map == MAP_FAILED) {
        return nullptr;
    }

    SharedState* s = reinterpret_cast<SharedState*>(map);

    // wait for magic to become MAGIC_NUMBER (creator will set it when ready)
    waited = 0;
    while (waited < SHM_OPEN_WAIT_MS) {
        uint32_t v = __atomic_load_n(&s->magic, __ATOMIC_SEQ_CST);
        if (v == MAGIC_NUMBER) {
            need_init = false;
            return s;
        }
        std::this_thread::sleep_for(
            std::chrono::milliseconds(SHM_POLL_INTERVAL_MS));
        waited += SHM_POLL_INTERVAL_MS;
    }

    // timed out waiting for magic
    munmap(map, size);
    errno = ETIMEDOUT;
    return nullptr;
}

// lock helper that handles robust mutex owner-dead
static int robust_mutex_lock(pthread_mutex_t* m) {
    int rc = pthread_mutex_lock(m);
    if (rc == EOWNERDEAD) {
        LOG(INFO) << "robust mutex owner died, trying to recover";
        return pthread_mutex_consistent(m);
    }
    return rc;
}

static int getNumaNodeOfDevice(const char* device_name) {
    char path[DEVNAME_LEN + 32];
    char resolved_path[DEVNAME_LEN];
    // Get the PCI bus id for the infiniband device. Note that
    // "/sys/class/infiniband/mlx5_X/" is a symlink to
    // "/sys/devices/pciXXXX:XX/XXXX:XX:XX.X/infiniband/mlx5_X/".
    snprintf(path, sizeof(path), "/sys/class/infiniband/%s/../..", device_name);
    if (realpath(path, resolved_path) == NULL) {
        PLOG(ERROR) << "listInfiniBandDevices: realpath " << path << " failed";
        return 0;  // default to node 0
    }

    int numa_node = 0;
    snprintf(path, sizeof(path), "%s/numa_node", resolved_path);
    std::ifstream(path) >> numa_node;
    return numa_node;
}

static Status discoverAllDevices(SharedState* state) {
    int num_devices = 0;
    struct ibv_device** device_list = ibv_get_device_list(&num_devices);
    if (!device_list || num_devices <= 0) return Status::OK();
    if (robust_mutex_lock(&state->mtx) != 0)
        return Status::InternalError("mutex lock failed");
    for (int i = 0; i < num_devices; ++i) {
        std::string device_name = ibv_get_device_name(device_list[i]);
        int numa_node = getNumaNodeOfDevice(device_name.c_str());
        std::strncpy(state->devices[i].name, device_name.c_str(),
                     DEVNAME_LEN - 1);
        state->devices[i].name[DEVNAME_LEN - 1] = '\0';
        state->devices[i].numa_node = numa_node;
        state->devices[i].lease_count = 0;
    }
    ibv_free_device_list(device_list);
    state->device_count = num_devices;
    pthread_mutex_unlock(&state->mtx);
    return Status::OK();
}

// Creator calls this after it has fully initialized the shared fields
// (including mutex). It will set magic to MAGIC_NUMBER and msync the header to
// publish readiness.
static Status markSharedStateInited(SharedState* state) {
    if (!state) {
        return Status::InternalError("markSharedStateInited: null state");
    }

    // If already inited, just return OK.
    if (__atomic_load_n(&state->magic, __ATOMIC_SEQ_CST) == MAGIC_NUMBER) {
        return Status::OK();
    }

    // Initialize pthread mutex attr for process-shared robust mutex
    pthread_mutexattr_t mattr;
    if (pthread_mutexattr_init(&mattr) != 0) {
        return Status::InternalError("pthread_mutexattr_init failed");
    }
    if (pthread_mutexattr_setpshared(&mattr, PTHREAD_PROCESS_SHARED) != 0) {
        pthread_mutexattr_destroy(&mattr);
        return Status::InternalError("pthread_mutexattr_setpshared failed");
    }

    // Make mutex robust so if an owner dies, next locker can recover.
    pthread_mutexattr_setrobust(&mattr, PTHREAD_MUTEX_ROBUST);

    if (pthread_mutex_init(&state->mtx, &mattr) != 0) {
        pthread_mutexattr_destroy(&mattr);
        return Status::InternalError("pthread_mutex_init failed");
    }

    pthread_mutexattr_destroy(&mattr);

    // Initialize other metadata fields (device table, counters)
    state->device_count = 0;
    state->lease_id_counter = 1;

    CHECK_STATUS(discoverAllDevices(state));

    // Ensure all initialized memory is visible to other processes before
    // setting magic.
    if (msync(reinterpret_cast<void*>(state), sizeof(SharedState), MS_SYNC) !=
        0) {
        return Status::InternalError(std::string("msync failed: ") +
                                     std::strerror(errno));
    }

    // Publish magic (atomic store)
    __atomic_store_n(&state->magic, MAGIC_NUMBER, __ATOMIC_SEQ_CST);

    // Ensure magic is flushed so followers waiting on it see it
    if (msync(reinterpret_cast<void*>(&state->magic), sizeof(state->magic),
              MS_SYNC) != 0) {
        return Status::InternalError(std::string("msync(magic) failed: ") +
                                     std::strerror(errno));
    }

    return Status::OK();
}

static Status unmapSharedState(SharedState* state) {
    if (!state) return Status::InvalidArgument("unmapSharedState: null");
    if (munmap(reinterpret_cast<void*>(state), sizeof(SharedState)) != 0) {
        return Status::InternalError(std::string("munmap failed: ") +
                                     std::strerror(errno));
    }
    return Status::OK();
}

Scheduler::Scheduler(const std::string& shm_name, Duration default_ttl)
    : state_(nullptr), shm_name_(shm_name), default_ttl_(default_ttl) {
    bool need_init = false;
    state_ = createSharedState(shm_name_.c_str(), need_init);
    if (!state_) {
        PLOG(ERROR) << "Failed to create or open shared state: " << shm_name_;
        return;
    }
    if (!need_init) return;
    auto status = markSharedStateInited(state_);
    if (!status.ok()) {
        PLOG(ERROR) << "Failed to mark shared state initialized: "
                    << status.ToString();
        unmapSharedState(state_);
        state_ = nullptr;
        return;
    }
    for (uint32_t i = 0; i < state_->device_count; ++i) {
        device_name_cache_.emplace(state_->devices[i].name, i);
    }
}

Scheduler::~Scheduler() {
    if (state_) {
        unmapSharedState(state_);
        state_ = nullptr;
    }
}

size_t Scheduler::reclaimExpiredLeasesUnlocked() {
    size_t reclaimed = 0;
    uint64_t now = now_ms();
    for (uint32_t i = 0; i < state_->device_count; ++i) {
        auto& dev = state_->devices[i];
        uint32_t valid_count = 0;
        uint64_t outstanding = 0;
        uint32_t first_free = MAX_LEASES_PER_DEVICE;
        for (uint32_t k = 0; k < dev.lease_count; ++k) {
            if (dev.leases[k].is_valid &&
                (dev.leases[k].expires_at_ms == 0 ||
                 now <= dev.leases[k].expires_at_ms)) {
                if (k != valid_count) {
                    dev.leases[valid_count] = dev.leases[k];
                }
                outstanding += dev.leases[valid_count].allocated_bytes;
                ++valid_count;
            } else if (dev.leases[k].is_valid) {
                dev.leases[k].is_valid = false;
                ++reclaimed;
                if (k < first_free) first_free = k;
            } else if (k < first_free) {
                first_free = k;
            }
        }
        dev.lease_count = valid_count;
        dev.outstanding_bytes = outstanding;
        dev.free_slot = (valid_count < MAX_LEASES_PER_DEVICE)
                            ? std::min(first_free, valid_count)
                            : MAX_LEASES_PER_DEVICE;
    }
    return reclaimed;
}

size_t Scheduler::reclaimExpiredLeases() {
    if (!state_ || robust_mutex_lock(&state_->mtx)) return 0;
    size_t reclaimed = reclaimExpiredLeasesUnlocked();
    pthread_mutex_unlock(&state_->mtx);
    return reclaimed;
}

bool Scheduler::closeJob(LeaseId lease) {
    if (!state_ || robust_mutex_lock(&state_->mtx) != 0) return false;
    for (uint32_t i = 0; i < state_->device_count; ++i) {
        auto& dev = state_->devices[i];
        for (uint32_t j = 0; j < dev.lease_count; ++j) {
            if (dev.leases[j].is_valid && dev.leases[j].lease_id == lease) {
                dev.outstanding_bytes -= dev.leases[j].allocated_bytes;
                dev.leases[j].is_valid = false;
                if (j < dev.free_slot) dev.free_slot = j;
                pthread_mutex_unlock(&state_->mtx);
                return true;
            }
        }
    }
    pthread_mutex_unlock(&state_->mtx);
    return false;
}

Status Scheduler::createJob(LeaseId& lease, int& selected_index,
                            uint64_t length,
                            const std::vector<std::string>& device_list) {
    if (!state_) return Status::InternalError("no shared state");
    if (length == 0)
        return Status::InvalidArgument("total_bytes must be positive");
    if (device_list.empty())
        return Status::InvalidArgument("prefer_devices is empty");
    if (robust_mutex_lock(&state_->mtx) != 0)
        return Status::InternalError("mutex lock failed");

    pid_t me = getpid();
    uint64_t now = now_ms();
    uint64_t ttl_ms = static_cast<uint64_t>(default_ttl_.count());
    reclaimExpiredLeasesUnlocked();

    selected_index = -1;
    uint64_t min_outstanding_bytes = UINT64_MAX;
    for (const auto& device : device_list) {
        if (!device_name_cache_.count(device)) continue;
        int idx = device_name_cache_[device];
        auto& dev = state_->devices[idx];
        if (dev.lease_count < MAX_LEASES_PER_DEVICE &&
            dev.outstanding_bytes < min_outstanding_bytes) {
            selected_index = idx;
            min_outstanding_bytes = dev.outstanding_bytes;
        }
    }

    if (selected_index == -1) {
        pthread_mutex_unlock(&state_->mtx);
        return Status::DeviceNotFound("no idle devices");
    }

    auto& dev = state_->devices[selected_index];
    lease = state_->lease_id_counter++;
    uint32_t slot = dev.free_slot;
    if (slot >= MAX_LEASES_PER_DEVICE) {
        pthread_mutex_unlock(&state_->mtx);
        return Status::InternalError("no free lease slots");
    }
    dev.leases[slot].lease_id = lease;
    dev.leases[slot].allocated_bytes = length;
    dev.leases[slot].expires_at_ms = now + ttl_ms;
    dev.leases[slot].owner_pid = me;
    dev.leases[slot].is_valid = true;
    if (slot == dev.lease_count) ++dev.lease_count;

    dev.free_slot = (dev.lease_count < MAX_LEASES_PER_DEVICE)
                        ? dev.lease_count
                        : MAX_LEASES_PER_DEVICE;
    for (uint32_t i = slot + 1; i < dev.lease_count; ++i) {
        if (!dev.leases[i].is_valid) {
            dev.free_slot = i;
            break;
        }
    }
    dev.outstanding_bytes += length;
    pthread_mutex_unlock(&state_->mtx);
    return Status::OK();
}

Status Scheduler::getEstimatedDeviceLoads(
    std::unordered_map<std::string, uint64_t>& result) {
    if (!state_) return Status::InternalError("no shared state");
    if (robust_mutex_lock(&state_->mtx) != 0)
        return Status::InternalError("mutex lock failed");

    result.clear();
    result.reserve(state_->device_count);
    reclaimExpiredLeasesUnlocked();
    for (uint32_t i = 0; i < state_->device_count; ++i) {
        result.emplace(state_->devices[i].name,
                       state_->devices[i].outstanding_bytes);
    }
    pthread_mutex_unlock(&state_->mtx);
    return Status::OK();
}

}  // namespace v1
}  // namespace mooncake