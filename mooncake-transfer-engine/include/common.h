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
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/mman.h>
#include <sys/time.h>
#include <unistd.h>

#include <atomic>
#include <charconv>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <ctime>
#include <iomanip>
#include <iostream>
#include <optional>
#include <sstream>
#include <thread>

#include "error.h"

#if defined(__x86_64__)
#include <immintrin.h>
#define PAUSE() _mm_pause()
#elif defined(__aarch64__) || defined(__arm__)
#define PAUSE() __asm__ __volatile__("yield")
#else
#define PAUSE()
#endif

#ifndef likely
#define likely(x) __glibc_likely(x)
#define unlikely(x) __glibc_unlikely(x)
#endif

namespace mooncake {
const static int LOCAL_SEGMENT_ID = 0;

enum class HandShakeRequestType {
    Connection = 0,
    Metadata = 1,
    Notify = 2,
    // placeholder for old protocol without RequestType
    OldProtocol = 0xff,
};

static inline std::string getHostname() {
    char hostname[256];
    if (gethostname(hostname, 256)) {
        PLOG(ERROR) << "Failed to get hostname";
        return "";
    }
    return hostname;
}

static inline int bindToSocket(int socket_id) {
    if (unlikely(numa_available() < 0)) {
        LOG(WARNING) << "The platform does not support NUMA";
        return ERR_NUMA;
    }
    cpu_set_t cpu_set;
    CPU_ZERO(&cpu_set);
    if (socket_id < 0 || socket_id >= numa_num_configured_nodes())
        socket_id = 0;
    struct bitmask *cpu_list = numa_allocate_cpumask();
    numa_node_to_cpus(socket_id, cpu_list);
    int nr_possible_cpus = numa_num_possible_cpus();
    int nr_cpus = 0;
    for (int cpu = 0; cpu < nr_possible_cpus; ++cpu) {
        if (numa_bitmask_isbitset(cpu_list, cpu) &&
            numa_bitmask_isbitset(numa_all_cpus_ptr, cpu)) {
            CPU_SET(cpu, &cpu_set);
            nr_cpus++;
        }
    }
    numa_free_cpumask(cpu_list);
    if (nr_cpus == 0) return 0;
    if (pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpu_set)) {
        LOG(ERROR) << "bindToSocket: pthread_setaffinity_np failed";
        return ERR_NUMA;
    }
    return 0;
}

static inline int64_t getCurrentTimeInNano() {
    const int64_t kNanosPerSecond = 1000 * 1000 * 1000;
    struct timespec ts;
    if (clock_gettime(CLOCK_REALTIME, &ts)) {
        PLOG(ERROR) << "getCurrentTimeInNano: clock_gettime failed";
        return ERR_CLOCK;
    }
    return (int64_t{ts.tv_sec} * kNanosPerSecond + int64_t{ts.tv_nsec});
}

static inline std::string getCurrentDateTime() {
    auto now = std::chrono::system_clock::now();
    auto time_t_now = std::chrono::system_clock::to_time_t(now);
    auto local_time = *std::localtime(&time_t_now);
    auto micros = std::chrono::duration_cast<std::chrono::microseconds>(
                      now.time_since_epoch()) %
                  1000000;
    std::ostringstream oss;
    oss << std::put_time(&local_time, "%Y-%m-%d %H:%M:%S") << "."
        << std::setw(6) << std::setfill('0') << micros.count();
    return oss.str();
}

uint16_t getDefaultHandshakePort();

template <typename T>
std::optional<T> parseFromString(std::string_view str) {
    T result = T();
    auto [ptr, ec] =
        std::from_chars(str.data(), str.data() + str.size(), result);
    if (ec != std::errc() || ptr != str.data() + str.size()) {
        return {};
    }
    return {std::move(result)};
}

static inline uint16_t getPortFromString(std::string_view port_string,
                                         uint16_t default_port) {
    std::optional<uint16_t> port = parseFromString<uint16_t>(port_string);
    if (port.has_value()) {
        return *port;
    }
    LOG(WARNING) << "Illegal port number in " << port_string
                 << ". Use default port " << default_port << " instead";
    return default_port;
}

static inline bool isValidIpV6(const std::string &address) {
    sockaddr_in6 addr;
    std::memset(&addr, 0, sizeof(addr));
    // Handle IPv6 addresses with scope ID (e.g., fe80::1%eth0)
    // inet_pton doesn't accept scope ID, so we need to strip it first
    size_t scope_pos = address.find('%');
    if (scope_pos == std::string::npos) {
        // No scope ID, validate directly without string copy
        return inet_pton(AF_INET6, address.c_str(), &addr.sin6_addr) == 1;
    }

    // Found scope ID. Check if there's a port after it (colon after %)
    // If there's a port, this is NOT a valid pure IPv6 address
    if (address.find(':', scope_pos) != std::string::npos) {
        // Has port after scope ID (e.g., fe80::1%eth0:12345), not a pure IPv6
        return false;
    }

    // No port, just strip the scope ID for validation
    std::string addr_to_check = address.substr(0, scope_pos);
    return inet_pton(AF_INET6, addr_to_check.c_str(), &addr.sin6_addr) == 1;
}

static inline std::string maybeWrapIpV6(const std::string &address) {
    if (isValidIpV6(address)) {
        return "[" + address + "]";
    }
    return address;
}

// Helper struct to hold IPv6 parsing result
struct IPv6ParseResult {
    bool matched;           // Whether the input was recognized as IPv6
    std::string host;       // Extracted host (empty if not matched)
    std::string_view port;  // Port string view (empty if no port found)
};

// Helper function to extract IPv6 host and port string from server_name
// Returns: {matched, host, port_string_view}
// - matched: true if input is IPv6 format, false otherwise
// - host: the IPv6 address (without brackets, with scope ID if present)
// - port: string_view of the port portion (empty if no port)
static inline IPv6ParseResult extractIPv6HostAndPort(
    const std::string &server_name) {
    if (server_name.starts_with("[")) {
        // [ipv6] or [ipv6]:port
        const size_t closing_bracket_pos = server_name.find(']');
        if (closing_bracket_pos != std::string::npos) {
            std::string potentialHost =
                server_name.substr(1, closing_bracket_pos - 1);
            if (isValidIpV6(potentialHost)) {
                const size_t colon_pos =
                    server_name.find(':', closing_bracket_pos);
                std::string_view port_str;
                if (colon_pos != std::string::npos) {
                    port_str =
                        std::string_view(server_name).substr(colon_pos + 1);
                }
                return {true, std::move(potentialHost), port_str};
            }
        }
        // Not valid ipv6, fallback to ipv4/host/etc mode
        return {false, "", ""};
    }

    if (isValidIpV6(server_name)) {
        // Pure IPv6 address without port
        return {true, server_name, ""};
    }

    // Handle IPv6 with scope ID but without brackets (e.g., fe80::1%eth0:12345)
    const size_t scope_pos = server_name.find('%');
    if (scope_pos != std::string::npos) {
        const size_t colon_after_scope = server_name.find(':', scope_pos);
        if (colon_after_scope != std::string::npos) {
            std::string host = server_name.substr(0, colon_after_scope);
            if (isValidIpV6(host)) {
                return {true, std::move(host),
                        std::string_view(server_name)
                            .substr(colon_after_scope + 1)};
            }
        } else {
            // No port after scope ID, just return the address with default port
            if (isValidIpV6(server_name)) {
                return {true, server_name, ""};
            }
        }
    }

    return {false, "", ""};
}

static inline std::pair<std::string, uint16_t> parseHostNameWithPort(
    const std::string &server_name) {
    uint16_t port = getDefaultHandshakePort();

    auto result = extractIPv6HostAndPort(server_name);
    if (result.matched) {
        return {
            std::move(result.host),
            result.port.empty() ? port : getPortFromString(result.port, port)};
    }

    // non ipv6 cases:
    const size_t colon_pos = server_name.rfind(':');

    if (colon_pos == server_name.npos) {
        return {server_name, port};
    }
    return {server_name.substr(0, colon_pos),
            getPortFromString(server_name.substr(colon_pos + 1), port)};
}

static inline uint16_t parsePortAndDevice(std::string_view suffix,
                                          uint16_t default_port,
                                          int *device_id) {
    auto colon_pos = suffix.find(':');
    if (colon_pos == suffix.npos) {
        return getPortFromString(suffix, default_port);
    }
    auto port_str = suffix.substr(0, colon_pos);
    auto npu_str = suffix.substr(colon_pos + 1);

    auto npu_ops = npu_str.find('_');
    if (npu_ops != npu_str.npos && npu_ops != 0 &&
        npu_ops != npu_str.size() - 1) {
        *device_id =
            parseFromString<int>(npu_str.substr(npu_ops + 1)).value_or(0);
    }
    return getPortFromString(port_str, default_port);
}

static inline std::pair<std::string, uint16_t> parseHostNameWithPortAscend(
    const std::string &server_name, int *device_id) {
    uint16_t port = getDefaultHandshakePort();

    auto result = extractIPv6HostAndPort(server_name);
    if (result.matched) {
        return {std::move(result.host),
                result.port.empty()
                    ? port
                    : parsePortAndDevice(result.port, port, device_id)};
    }

    // non ipv6 cases:
    auto colon_pos = server_name.find(':');
    if (colon_pos == server_name.npos) {
        return std::make_pair(server_name, port);
    }

    return {
        server_name.substr(0, colon_pos),
        parsePortAndDevice(server_name.substr(colon_pos + 1), port, device_id)};
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

static inline int writeString(int fd, const HandShakeRequestType type,
                              const std::string &str) {
    uint8_t byte = static_cast<uint8_t>(type);
    // LOG(INFO) << "writeString: type " << (int)byte << ", str(" << str.size()
    //           << "): " << str;
    uint64_t length =
        str.size() +
        (type == HandShakeRequestType::OldProtocol ? 0 : sizeof(byte));
    if (writeFully(fd, &length, sizeof(length)) != (ssize_t)sizeof(length))
        return ERR_SOCKET;
    if (type != HandShakeRequestType::OldProtocol) {
        if (writeFully(fd, &byte, sizeof(byte)) != (ssize_t)sizeof(byte))
            return ERR_SOCKET;
    }
    if (writeFully(fd, str.data(), str.size()) != (ssize_t)str.size())
        return ERR_SOCKET;
    return 0;
}

static inline std::pair<HandShakeRequestType, std::string> readString(int fd) {
    HandShakeRequestType type = HandShakeRequestType::Connection;

    const static size_t kMaxLength = 1ull << 20;
    uint64_t length = 0;
    ssize_t n = readFully(fd, &length, sizeof(length));
    if (n != (ssize_t)sizeof(length)) {
        LOG(ERROR) << "readString: failed to read length, got: " << n;
        return {type, ""};
    }

    if (length > kMaxLength) {
        LOG(ERROR) << "readString: too large length from socket: " << length;
        return {type, ""};
    }

    std::string str;
    std::vector<char> buffer(length);
    n = readFully(fd, buffer.data(), length);
    if (n != (ssize_t)length) {
        LOG(ERROR) << "readString: unexpected length, got: " << n
                   << ", expected: " << length;
        return {type, ""};
    }

    if (buffer[0] <= static_cast<char>(HandShakeRequestType::Notify)) {
        type = static_cast<HandShakeRequestType>(buffer[0]);
        str.assign(buffer.data() + sizeof(char), length - sizeof(char));
    } else {
        type = HandShakeRequestType::OldProtocol;
        // Old protocol, no type
        str.assign(buffer.data(), length);
    }

    return {type, str};
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