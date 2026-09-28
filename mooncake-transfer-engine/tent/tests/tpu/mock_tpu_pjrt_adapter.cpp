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

// Mock TPU/PJRT adapter for unit testing TpuPjrtShim, TpuPlatform and
// TpuTransport without TPU hardware or a PJRT runtime. It implements the C ABI
// declared in tpu_pjrt_abi.h, so the routing, classification and staging logic
// can be exercised on any Linux host (see tpu_pjrt_shim_test.cpp and
// tpu_transport_test.cpp).
//
// Two properties of real PJRT/TPU are modelled deliberately, because both are
// load-bearing for TENT's correctness and neither is obvious:
//
//  1. The device "pointer" is an opaque TOKEN, not the buffer's data. On real
//     hardware PJRT_Buffer_UnsafePointer returns an internal handle that is
//     host-dereferenceable but reads as unrelated bytes. So the token here is a
//     separate read-only mapping poisoned with kPoison, and the buffer contents
//     live in a shadow allocation reachable only through the copy entrypoints.
//     Any code path that "helpfully" memcpy()s from a device token therefore
//     reads poison instead of silently producing the right answer -- which is
//     what a plain-host-memory mock would have done, and why an
//     interior-pointer bug could pass a green test suite.
//
//  2. Addresses may be INTERIOR to a registered buffer. TENT stages transfers
//  in
//     chunks and passes `token + chunk_offset` for every chunk after the first,
//     so classification and copies resolve ranges, and a copy that would run
//     past a buffer's end is rejected rather than truncated.

#include <sys/mman.h>

#include <cstdint>
#include <cstring>
#include <mutex>
#include <vector>

#include "tent/platform/tpu_pjrt_abi.h"

namespace {
// Byte filling the token mapping. Nothing should ever read it; if a test sees
// 0xDD it means something dereferenced a device token directly.
constexpr unsigned char kPoison = 0xDD;

struct Buffer {
    uintptr_t token;        // opaque handle handed out to TENT
    unsigned char *shadow;  // where the bytes actually live
    size_t size;
    int device;
};

std::mutex g_mutex;
std::vector<Buffer> g_device_registry;
int g_device_count = 4;

// Returns the buffer containing [addr, addr + len), or nullptr. len == 0 only
// checks that `addr` itself is inside a buffer.
Buffer *findLocked(const void *addr, size_t len) {
    auto a = reinterpret_cast<uintptr_t>(addr);
    for (auto &b : g_device_registry) {
        if (a < b.token || a >= b.token + b.size) continue;
        if (len > b.token + b.size - a) return nullptr;  // runs past the end
        return &b;
    }
    return nullptr;
}
}  // namespace

extern "C" {

// --- ABI required by TpuPjrtShim -------------------------------------------

int mc_tpu_pjrt_init(void) { return 0; }

int mc_tpu_pjrt_is_device_ptr(const void *addr) {
    std::lock_guard<std::mutex> lock(g_mutex);
    return findLocked(addr, 0) ? 1 : 0;
}

int mc_tpu_pjrt_device_index(const void *addr) {
    std::lock_guard<std::mutex> lock(g_mutex);
    const Buffer *b = findLocked(addr, 0);
    return b ? b->device : -1;
}

int mc_tpu_pjrt_copy_d2h(void *host_dst, const void *device_src, size_t len) {
    if (!host_dst || !device_src) return 1;
    std::lock_guard<std::mutex> lock(g_mutex);
    const Buffer *b = findLocked(device_src, len);
    if (!b) return 1;
    size_t offset = reinterpret_cast<uintptr_t>(device_src) - b->token;
    std::memcpy(host_dst, b->shadow + offset, len);
    return 0;
}

int mc_tpu_pjrt_copy_h2d(void *device_dst, const void *host_src, size_t len) {
    if (!device_dst || !host_src) return 1;
    std::lock_guard<std::mutex> lock(g_mutex);
    const Buffer *b = findLocked(device_dst, len);
    if (!b) return 1;
    size_t offset = reinterpret_cast<uintptr_t>(device_dst) - b->token;
    std::memcpy(b->shadow + offset, host_src, len);
    return 0;
}

int mc_tpu_pjrt_device_count(void) { return g_device_count; }

int mc_tpu_pjrt_device_numa(int index) {
    // Deterministic fake affinity: even devices on node 0, odd on node 1.
    if (index < 0 || index >= g_device_count) return -1;
    return index % 2;
}

// --- Test-only helpers (not part of the shim ABI) --------------------------

// Creates a fake device buffer of `size` bytes on device `index` and returns
// its token. The token is readable (like PJRT's unsafe pointer) but holds
// poison, never the buffer's data.
void *mock_tpu_pjrt_register_device(size_t size, int index) {
    void *token = mmap(nullptr, size, PROT_READ | PROT_WRITE,
                       MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    if (token == MAP_FAILED) return nullptr;
    std::memset(token, kPoison, size);
    mprotect(token, size, PROT_READ);

    std::lock_guard<std::mutex> lock(g_mutex);
    g_device_registry.push_back(Buffer{reinterpret_cast<uintptr_t>(token),
                                       new unsigned char[size](), size, index});
    return token;
}

// Direct access to a buffer's bytes, for seeding inputs and asserting outputs.
// Accepts an interior token address and returns the matching shadow address.
void *mock_tpu_pjrt_device_data(const void *token) {
    std::lock_guard<std::mutex> lock(g_mutex);
    Buffer *b = findLocked(token, 0);
    if (!b) return nullptr;
    return b->shadow + (reinterpret_cast<uintptr_t>(token) - b->token);
}

unsigned char mock_tpu_pjrt_poison_byte(void) { return kPoison; }

void mock_tpu_pjrt_reset(void) {
    std::lock_guard<std::mutex> lock(g_mutex);
    for (auto &b : g_device_registry) {
        munmap(reinterpret_cast<void *>(b.token), b.size);
        delete[] b.shadow;
    }
    g_device_registry.clear();
    g_device_count = 4;
}

void mock_tpu_pjrt_set_device_count(int count) {
    std::lock_guard<std::mutex> lock(g_mutex);
    g_device_count = count;
}

}  // extern "C"
