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
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <ctime>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <thread>

#if defined(__x86_64__)
#include <immintrin.h>
#define PAUSE() _mm_pause()
#else
#define PAUSE()
#endif

#ifndef likely
#define likely(x) __glibc_likely(x)
#define unlikely(x) __glibc_unlikely(x)
#endif

#ifdef USE_CUDA
#include <bits/stdint-uintn.h>
#include <cuda_runtime.h>
#endif

namespace mooncake {
namespace v1 {
const static int LOCAL_SEGMENT_ID = 0;

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
        return -1;
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

#ifdef USE_CUDA
static inline bool genericMemcpy(void *dst, void *src, size_t length) {
    auto ret = cudaMemcpy(dst, src, length, cudaMemcpyDefault);
    return ret == cudaSuccess;
}
#else
static inline bool genericMemcpy(void *dst, void *src, size_t length) {
    memcpy(dst, src, length);
    return true;
}
#endif

static inline int hexCharToValue(char c) {
    if (c >= '0' && c <= '9') return c - '0';
    if (c >= 'A' && c <= 'F') return 10 + c - 'A';
    if (c >= 'a' && c <= 'f') return 10 + c - 'a';
    throw std::invalid_argument("Invalid hexadecimal character");
}

static inline std::string serializeBinaryData(const void *data, size_t length) {
    if (!data) {
        throw std::invalid_argument("Data pointer cannot be null");
    }

    std::string hexString;
    hexString.reserve(length * 2);

    const unsigned char *byteData = static_cast<const unsigned char *>(data);
    for (size_t i = 0; i < length; ++i) {
        hexString.push_back("0123456789ABCDEF"[(byteData[i] >> 4) & 0x0F]);
        hexString.push_back("0123456789ABCDEF"[byteData[i] & 0x0F]);
    }

    return hexString;
}

static inline void deserializeBinaryData(const std::string &hexString,
                                         std::vector<unsigned char> &buffer) {
    if (hexString.length() % 2 != 0) {
        throw std::invalid_argument("Input string length must be even");
    }

    buffer.clear();
    buffer.reserve(hexString.length() / 2);

    for (size_t i = 0; i < hexString.length(); i += 2) {
        int high = hexCharToValue(hexString[i]);
        int low = hexCharToValue(hexString[i + 1]);
        buffer.push_back(static_cast<unsigned char>((high << 4) | low));
    }
}

}  // namespace v1
}  // namespace mooncake

#endif  // COMMON_H