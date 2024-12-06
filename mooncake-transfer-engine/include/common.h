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

#ifndef COMMON_H
#define COMMON_H

#include <glog/logging.h>
#include <numa.h>
#include <sys/mman.h>
#include <sys/time.h>

#include <atomic>
#include <cstdint>
#include <ctime>
#include <thread>

#include "error.h"

#if defined(__x86_64__)
#include <immintrin.h>
#define PAUSE() _mm_pause()
#else
#define PAUSE()
#endif

#define likely(x) __glibc_likely(x)
#define unlikely(x) __glibc_unlikely(x)

namespace mooncake {
const static int LOCAL_SEGMENT_ID = 0;

static inline int bindToSocket(int socket_id) {
    if (unlikely(numa_available() < 0)) {
        LOG(ERROR) << "The platform does not support NUMA";
        return ERR_NUMA;
    }
    cpu_set_t cpu_set;
    CPU_ZERO(&cpu_set);
    int num_nodes = numa_num_configured_nodes();
    if (socket_id < 0 || socket_id >= num_nodes) socket_id = 0;
    struct bitmask *cpu_list = numa_allocate_cpumask();
    numa_node_to_cpus(socket_id, cpu_list);
    int nr_cpus = 0;
    for (int cpu = 0; cpu < numa_num_possible_cpus(); ++cpu) {
        if (numa_bitmask_isbitset(cpu_list, cpu) &&
            numa_bitmask_isbitset(numa_all_cpus_ptr, cpu)) {
            CPU_SET(cpu, &cpu_set);
            nr_cpus++;
        }
    }
    numa_free_cpumask(cpu_list);
    if (nr_cpus == 0)
        return 0;
    if (pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpu_set)) {
        LOG(ERROR) << "Failed to set socket affinity";
        return ERR_NUMA;
    }
    return 0;
}

static inline int64_t getCurrentTimeInNano() {
    const int64_t kNanosPerSecond = 1000 * 1000 * 1000;
    struct timespec ts;
    if (clock_gettime(CLOCK_REALTIME, &ts)) {
        PLOG(ERROR) << "Failed to read real-time lock";
        return ERR_CLOCK;
    }
    return (int64_t{ts.tv_sec} * kNanosPerSecond + int64_t{ts.tv_nsec});
}

uint16_t getDefaultHandshakePort();

static inline std::pair<std::string, uint16_t> parseHostNameWithPort(
    const std::string &server_name) {
    uint16_t port = getDefaultHandshakePort();
    auto pos = server_name.find(':');
    if (pos == server_name.npos) return std::make_pair(server_name, port);
    auto trimmed_server_name = server_name.substr(0, pos);
    auto port_str = server_name.substr(pos + 1);
    int val = std::atoi(port_str.c_str());
    if (val <= 0 || val > 65535)
        PLOG(WARNING) << "Illegal port number in " << server_name
                      << ". Use default port " << port << " instead";
    else
        port = (uint16_t)val;
    return std::make_pair(trimmed_server_name, port);
}

static inline ssize_t writeFully(int fd, const void *buf, size_t len) {
    char *pos = (char *)buf;
    size_t nbytes = len;
    while (nbytes) {
        ssize_t rc = write(fd, pos, nbytes);
        if (rc < 0 && (errno == EAGAIN || errno == EINTR))
            continue;
        else if (rc < 0) {
            PLOG(ERROR) << "Socket write failed";
            return rc;
        } else if (rc == 0) {
            LOG(WARNING) << "Socket write incompleted: expected " << len
                         << " bytes, actual " << len - nbytes << " bytes";
            return len - nbytes;
        }
        pos += rc;
        nbytes -= rc;
    }
    return len;
}

static inline ssize_t readFully(int fd, void *buf, size_t len) {
    char *pos = (char *)buf;
    size_t nbytes = len;
    while (nbytes) {
        ssize_t rc = read(fd, pos, nbytes);
        if (rc < 0 && (errno == EAGAIN || errno == EINTR))
            continue;
        else if (rc < 0) {
            PLOG(ERROR) << "Socket read failed";
            return rc;
        } else if (rc == 0) {
            LOG(WARNING) << "Socket read incompleted: expected " << len
                         << " bytes, actual " << len - nbytes << " bytes";
            return len - nbytes;
        }
        pos += rc;
        nbytes -= rc;
    }
    return len;
}

static inline int writeString(int fd, const std::string &str) {
    uint64_t length = str.size();
    if (writeFully(fd, &length, sizeof(length)) != (ssize_t)sizeof(length))
        return ERR_SOCKET;
    if (writeFully(fd, str.data(), length) != (ssize_t)length)
        return ERR_SOCKET;
    return 0;
}

static inline std::string readString(int fd) {
    const static size_t kMaxLength = 1ull << 20;
    uint64_t length = 0;
    if (readFully(fd, &length, sizeof(length)) != (ssize_t)sizeof(length))
        return "";
    if (length > kMaxLength) return "";
    std::string str;
    std::vector<char> buffer(length);
    if (readFully(fd, buffer.data(), length) != (ssize_t)length) return "";

    str.assign(buffer.data(), length);
    return str;
}

const static std::string NIC_PATH_DELIM = "@";
static inline const std::string getServerNameFromNicPath(
    const std::string &nic_path) {
    size_t pos = nic_path.find(NIC_PATH_DELIM);
    if (pos == nic_path.npos) return "";
    return nic_path.substr(0, pos);
}

static inline const std::string getNicNameFromNicPath(
    const std::string &nic_path) {
    size_t pos = nic_path.find(NIC_PATH_DELIM);
    if (pos == nic_path.npos) return "";
    return nic_path.substr(pos + 1);
}

static inline const std::string MakeNicPath(const std::string &server_name,
                                            const std::string &nic_name) {
    return server_name + NIC_PATH_DELIM + nic_name;
}

static inline bool overlap(const void *a, size_t a_len, const void *b,
                           size_t b_len) {
    return (a >= b && a < (char *)b + b_len) ||
           (b >= a && b < (char *)a + a_len);
}

class RWSpinlock {
    union RWTicket {
        constexpr RWTicket() : whole(0) {}
        uint64_t whole;
        uint32_t readWrite;
        struct {
            uint16_t write;
            uint16_t read;
            uint16_t users;
        };
    } ticket;

   private:
    static void asm_volatile_memory() { asm volatile("" ::: "memory"); }

    template <class T>
    static T load_acquire(T *addr) {
        T t = *addr;
        asm_volatile_memory();
        return t;
    }

    template <class T>
    static void store_release(T *addr, T v) {
        asm_volatile_memory();
        *addr = v;
    }

   public:
    RWSpinlock() {}

    RWSpinlock(RWSpinlock const &) = delete;
    RWSpinlock &operator=(RWSpinlock const &) = delete;

    void lock() { writeLockNice(); }

    bool tryLock() {
        RWTicket t;
        uint64_t old = t.whole = load_acquire(&ticket.whole);
        if (t.users != t.write) return false;
        ++t.users;
        return __sync_bool_compare_and_swap(&ticket.whole, old, t.whole);
    }

    void writeLockAggressive() {
        uint32_t count = 0;
        uint16_t val = __sync_fetch_and_add(&ticket.users, 1);
        while (val != load_acquire(&ticket.write)) {
            PAUSE();
            if (++count > 1000) std::this_thread::yield();
        }
    }

    void writeLockNice() {
        uint32_t count = 0;
        while (!tryLock()) {
            PAUSE();
            if (++count > 1000) std::this_thread::yield();
        }
    }

    void unlockAndLockShared() {
        uint16_t val = __sync_fetch_and_add(&ticket.read, 1);
        (void)val;
    }

    void unlock() {
        RWTicket t;
        t.whole = load_acquire(&ticket.whole);
        ++t.read;
        ++t.write;
        store_release(&ticket.readWrite, t.readWrite);
    }

    void lockShared() {
        uint_fast32_t count = 0;
        while (!tryLockShared()) {
            PAUSE();
            if (++count > 1000) std::this_thread::yield();
        }
    }

    bool tryLockShared() {
        RWTicket t, old;
        old.whole = t.whole = load_acquire(&ticket.whole);
        old.users = old.read;
        ++t.read;
        ++t.users;
        return __sync_bool_compare_and_swap(&ticket.whole, old.whole, t.whole);
    }

    void unlockShared() { __sync_fetch_and_add(&ticket.write, 1); }

   public:
    struct WriteGuard {
        WriteGuard(RWSpinlock &lock) : lock(lock) { lock.lock(); }

        WriteGuard(const WriteGuard &) = delete;

        WriteGuard &operator=(const WriteGuard &) = delete;

        ~WriteGuard() { lock.unlock(); }

        RWSpinlock &lock;
    };

    struct ReadGuard {
        ReadGuard(RWSpinlock &lock) : lock(lock) { lock.lockShared(); }

        ReadGuard(const ReadGuard &) = delete;

        ReadGuard &operator=(const ReadGuard &) = delete;

        ~ReadGuard() { lock.unlockShared(); }

        RWSpinlock &lock;
    };

   private:
    const static int64_t kExclusiveLock = INT64_MIN / 2;

    std::atomic<int64_t> lock_;
    uint64_t padding_[15];
};

class TicketLock {
   public:
    TicketLock() : next_ticket_(0), now_serving_(0) {}

    void lock() {
        int my_ticket = next_ticket_.fetch_add(1, std::memory_order_relaxed);
        while (now_serving_.load(std::memory_order_acquire) != my_ticket) {
            std::this_thread::yield();
        }
    }

    void unlock() { now_serving_.fetch_add(1, std::memory_order_release); }

   private:
    std::atomic<int> next_ticket_;
    std::atomic<int> now_serving_;
    uint64_t padding_[14];
};

class SimpleRandom {
   public:
    SimpleRandom(uint32_t seed) : current(seed) {}

    static SimpleRandom &Get() {
        static std::atomic<uint64_t> g_incr_val(0);
        thread_local SimpleRandom g_random(getCurrentTimeInNano() +
                                           g_incr_val.fetch_add(1));
        return g_random;
    }

    // 生成下一个伪随机数
    uint32_t next() {
        current = (a * current + c) % m;
        return current;
    }

    uint32_t next(uint32_t max) { return next() % max; }

   private:
    uint32_t current;
    static const uint32_t a = 1664525;
    static const uint32_t c = 1013904223;
    static const uint32_t m = 0xFFFFFFFF;
};
}  // namespace mooncake

#endif  // COMMON_H