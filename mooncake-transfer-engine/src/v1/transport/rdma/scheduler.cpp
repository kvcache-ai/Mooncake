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
constexpr size_t MAX_LEASES_PER_DEVICE = 64;
const uint32_t MAGIC_NUMBER = 0x6D6FCAFE;

// total wait for follower to see ftruncate+magic
static constexpr int SHM_OPEN_WAIT_MS = 5000;
static constexpr int SHM_POLL_INTERVAL_MS = 10;  // poll interval

// Shared memory layout (only in cpp)

// Shared memory layout
struct SharedLease {
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
}

Scheduler::~Scheduler() {
    if (state_) {
        unmapSharedState(state_);
        state_ = nullptr;
    }
}

int Scheduler::findDeviceIndexByName(const std::string& name) {
    if (!state_) return -1;
    for (uint32_t i = 0; i < state_->device_count; ++i) {
        if (strncmp(state_->devices[i].name, name.c_str(), DEVNAME_LEN) == 0)
            return static_cast<int>(i);
    }
    return -1;
}

size_t Scheduler::reclaimExpiredLeasesUnlocked() {
    size_t reclaimed = 0;
    uint64_t now = now_ms();
    for (uint32_t i = 0; i < state_->device_count; ++i) {
        auto& dev = state_->devices[i];
        uint64_t dev_outstanding = 0;
        uint32_t j = 0;
        for (uint32_t k = 0; k < dev.lease_count; ++k) {
            if (dev.leases[k].expires_at_ms == 0 ||
                now <= dev.leases[k].expires_at_ms) {
                dev.leases[j++] = dev.leases[k];
                dev_outstanding += dev.leases[k].allocated_bytes;
            } else {
                ++reclaimed;
            }
        }
        dev.lease_count = j;
        dev.outstanding_bytes = dev_outstanding;
    }
    return reclaimed;
}

size_t Scheduler::reclaimExpiredLeases() {
    if (!state_ || robust_mutex_lock(&state_->mtx)) return 0;
    size_t reclaimed = reclaimExpiredLeasesUnlocked();
    pthread_mutex_unlock(&state_->mtx);
    return reclaimed;
}

bool Scheduler::closeJob(const JobLease& lease) {
    if (!state_) return false;
    if (robust_mutex_lock(&state_->mtx) != 0) return false;
    bool found = false;
    for (uint32_t i = 0; i < state_->device_count; ++i) {
        auto& dev = state_->devices[i];
        for (uint32_t j = 0; j < dev.lease_count; ++j) {
            if (dev.leases[j].lease_id == lease.id) {
                // Subtract closed lease's bytes from outstanding_bytes
                dev.outstanding_bytes -= dev.leases[j].allocated_bytes;
                // Shift remaining leases
                for (uint32_t k = j; k < dev.lease_count - 1; ++k) {
                    dev.leases[k] = dev.leases[k + 1];
                }
                --dev.lease_count;
                found = true;
                break;
            }
        }
        if (found) break;
    }
    pthread_mutex_unlock(&state_->mtx);
    return found;
}

Status Scheduler::createJobs(uint64_t total_bytes,
                             const std::vector<std::string>& device_list,
                             std::vector<JobLease>& jobs,
                             Duration requested_ttl) {
    jobs.clear();
    if (!state_) return Status::InternalError("no shared state");
    if (total_bytes == 0)
        return Status::InvalidArgument("total_bytes must be positive");
    if (device_list.empty())
        return Status::InvalidArgument("prefer_devices is empty");

    if (requested_ttl.count() <= 0) requested_ttl = default_ttl_;
    uint64_t ttl_ms = static_cast<uint64_t>(requested_ttl.count());

    if (robust_mutex_lock(&state_->mtx) != 0) {
        return Status::InternalError("mutex lock failed");
    }

    pid_t me = getpid();
    reclaimExpiredLeasesUnlocked();

    constexpr uint64_t GRANULARITY = 128 * 1024;
    uint64_t remaining = total_bytes;

    // Collect available devices pair<index, outstanding_bytes>
    std::vector<std::pair<int, uint64_t>> avail;
    for (const auto& device : device_list) {
        int idx = findDeviceIndexByName(device);
        if (idx >= 0 &&
            state_->devices[idx].lease_count < MAX_LEASES_PER_DEVICE) {
            avail.emplace_back(idx, state_->devices[idx].outstanding_bytes);
        }
    }

    if (avail.empty()) {
        pthread_mutex_unlock(&state_->mtx);
        return Status::DeviceNotFound("no idle devices");
    }

    // Sort by outstanding_bytes for load balancing, then by index
    std::sort(avail.begin(), avail.end(), [](const auto& a, const auto& b) {
        if (a.second != b.second) return a.second < b.second;
        return a.first < b.first;
    });

    std::unordered_map<int, uint64_t> device_bytes;
    size_t num_devices = avail.size();
    uint64_t total_units = remaining / GRANULARITY;
    uint64_t units_per_device = total_units / num_devices;
    uint64_t extra_units = total_units % num_devices;
    for (size_t i = 0; i < num_devices; ++i) {
        int d = avail[i].first;
        uint64_t units = units_per_device + (i < extra_units ? 1 : 0);
        device_bytes[d] = units * GRANULARITY;
    }

    uint64_t leftover_bytes = remaining % GRANULARITY;
    if (leftover_bytes > 0) device_bytes[avail[0].first] += leftover_bytes;

    // Create one lease per device with combined bytes
    for (const auto& [d, bytes] : device_bytes) {
        if (bytes == 0) continue;
        auto& dev = state_->devices[d];

        LeaseId id = state_->lease_id_counter++;
        uint32_t lc = dev.lease_count;
        dev.leases[lc].lease_id = id;
        dev.leases[lc].allocated_bytes = bytes;
        dev.leases[lc].expires_at_ms = now_ms() + ttl_ms;
        dev.leases[lc].owner_pid = me;
        dev.lease_count = lc + 1;
        dev.outstanding_bytes += bytes;  // Update outstanding_bytes

        JobLease jl;
        jl.id = id;
        jl.nic_name = std::string(dev.name);
        jl.bytes = bytes;
        jl.expires_at = Clock::now() + std::chrono::milliseconds(ttl_ms);
        jl.owner_pid = me;
        jobs.push_back(std::move(jl));
    }

    pthread_mutex_unlock(&state_->mtx);
    return Status::OK();
}
}  // namespace v1
}  // namespace mooncake