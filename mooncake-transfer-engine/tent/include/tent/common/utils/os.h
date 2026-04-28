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

#ifndef TENT_OS_H
#define TENT_OS_H

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
#elif defined(__aarch64__) || defined(__arm__)
#define PAUSE() __asm__ __volatile__("yield")
#else
#define PAUSE()
#endif

namespace mooncake {
namespace tent {
static inline int bindToSocket(int socket_id) {
    if (numa_available() < 0) {
        LOG(WARNING) << "The platform does not support NUMA";
        return -1;
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
        return -1;
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

// Fast timestamp counter using RDTSCP instruction
// Returns CPU cycles since boot, useful for measuring intervals
// Only ~10-20 CPU cycles overhead vs ~50-100ns for clock_gettime
#if defined(__x86_64__)
static inline uint64_t rdtscp() {
    unsigned int aux;
    uint64_t rax, rdx;
    __asm__ __volatile__("rdtscp" : "=a"(rax), "=d"(rdx), "=c"(aux));
    return (rdx << 32) | rax;
}
#elif defined(__aarch64__) || defined(__arm__)
static inline uint64_t rdtscp() {
    uint64_t cntvct;
    __asm__ __volatile__("mrs %0, cntvct_el0" : "=r"(cntvct));
    return cntvct;
}
#else
static inline uint64_t rdtscp() {
    return getCurrentTimeInNano();  // Fallback to clock_gettime
}
#endif

// Convert TSC cycles to nanoseconds using calibrated TSC frequency
namespace detail {
// TSC frequency in MHz (initialized once)
inline double g_tsc_mhz = 0.0;

// One-time calibration of TSC frequency
inline double calibrateTscFrequency() {
    if (g_tsc_mhz > 0.0) return g_tsc_mhz;

    // Measure TSC frequency by comparing with clock_gettime
    const int calibration_ms = 10;  // 10ms calibration
    auto start_tsc = rdtscp();
    auto start_ns = std::chrono::steady_clock::now();

    std::this_thread::sleep_for(std::chrono::milliseconds(calibration_ms));

    auto end_tsc = rdtscp();
    auto end_ns = std::chrono::steady_clock::now();

    uint64_t tsc_diff = end_tsc - start_tsc;
    auto ns_diff =
        std::chrono::duration_cast<std::chrono::nanoseconds>(end_ns - start_ns)
            .count();

    // TSC frequency in MHz = (cycles / nanoseconds) * 1000
    g_tsc_mhz = (static_cast<double>(tsc_diff) / ns_diff) * 1000.0;

    return g_tsc_mhz;
}
}  // namespace detail

// Initialize fast timer by calibrating TSC frequency at startup.
// Call this once at program startup to avoid lazy calibration overhead.
static inline void initFastTimer() { detail::calibrateTscFrequency(); }

// Convert TSC cycles to nanoseconds (fast path, ~5 cycles)
static inline uint64_t tscToNanos(uint64_t tsc_cycles) {
    double tsc_mhz = detail::g_tsc_mhz;
    return static_cast<uint64_t>(tsc_cycles / tsc_mhz);
}

// Get current time in nanoseconds using RDTSCP (faster than clock_gettime)
// For interval measurement only, not absolute time
static inline uint64_t getFastTimeNanos() { return tscToNanos(rdtscp()); }

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

}  // namespace tent
}  // namespace mooncake

#endif  // TENT_OS_H