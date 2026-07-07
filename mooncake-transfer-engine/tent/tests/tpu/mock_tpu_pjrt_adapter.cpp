// Copyright 2026 KVCache.AI
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

// Mock TPU/PJRT adapter for unit testing TpuPjrtShim and TpuPlatform without
// TPU hardware or a PJRT runtime. It implements the C ABI declared in
// tpu_pjrt_abi.h using ordinary host memory: "device" buffers are plain
// allocations the test registers via the mock_* test helpers below, and D2H/H2D
// copies are memcpy. This lets the routing and shim logic be exercised on any
// Linux host (see tpu_pjrt_shim_test.cpp).

#include <cstring>
#include <mutex>
#include <unordered_map>

#include "tent/platform/tpu_pjrt_abi.h"

namespace {
std::mutex g_mutex;
// Registered fake device buffers: pointer -> device ordinal.
std::unordered_map<const void *, int> g_device_registry;
int g_device_count = 4;
}  // namespace

extern "C" {

// --- ABI required by TpuPjrtShim -------------------------------------------

int mc_tpu_pjrt_init(void) { return 0; }

int mc_tpu_pjrt_is_device_ptr(const void *addr) {
    std::lock_guard<std::mutex> lock(g_mutex);
    return g_device_registry.count(addr) ? 1 : 0;
}

int mc_tpu_pjrt_device_index(const void *addr) {
    std::lock_guard<std::mutex> lock(g_mutex);
    auto it = g_device_registry.find(addr);
    return it == g_device_registry.end() ? -1 : it->second;
}

int mc_tpu_pjrt_copy_d2h(void *host_dst, const void *device_src, size_t len) {
    if (!host_dst || !device_src) return 1;
    std::memcpy(host_dst, device_src, len);
    return 0;
}

int mc_tpu_pjrt_copy_h2d(void *device_dst, const void *host_src, size_t len) {
    if (!device_dst || !host_src) return 1;
    std::memcpy(device_dst, host_src, len);
    return 0;
}

int mc_tpu_pjrt_device_count(void) { return g_device_count; }

int mc_tpu_pjrt_device_numa(int index) {
    // Deterministic fake affinity: even devices on node 0, odd on node 1.
    if (index < 0 || index >= g_device_count) return -1;
    return index % 2;
}

// --- Test-only helpers (not part of the shim ABI) --------------------------

void mock_tpu_pjrt_register_device(const void *addr, int index) {
    std::lock_guard<std::mutex> lock(g_mutex);
    g_device_registry[addr] = index;
}

void mock_tpu_pjrt_reset(void) {
    std::lock_guard<std::mutex> lock(g_mutex);
    g_device_registry.clear();
    g_device_count = 4;
}

void mock_tpu_pjrt_set_device_count(int count) {
    std::lock_guard<std::mutex> lock(g_mutex);
    g_device_count = count;
}

}  // extern "C"
