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

// Mock TPU buffer provider (tpu_buffer_provider.h) for testing Mooncake's
// built-in TPU adapter without TPU hardware or a real provider such as
// tpu-sync. Buffers are plain host memory; test-only helpers create them and
// expose their bytes and lifetime counters.

#include <atomic>
#include <cstdint>
#include <cstring>
#include <map>
#include <memory>
#include <mutex>
#include <vector>

#include "tent/platform/tpu_buffer_provider.h"

namespace {

struct Buffer {
    std::vector<uint8_t> data;
    int device;
    int numa;
};

std::mutex g_mutex;
std::map<mc_tpu_buffer_t, std::unique_ptr<Buffer>> g_buffers;
mc_tpu_buffer_t g_next_handle = 1;
std::atomic<int> g_released{0};
std::atomic<bool> g_fail_copies{false};

Buffer *findLocked(mc_tpu_buffer_t handle) {
    auto it = g_buffers.find(handle);
    return it == g_buffers.end() ? nullptr : it->second.get();
}

size_t mockSize(mc_tpu_buffer_t handle) {
    std::lock_guard<std::mutex> lock(g_mutex);
    Buffer *b = findLocked(handle);
    return b ? b->data.size() : 0;
}

int mockDevice(mc_tpu_buffer_t handle) {
    std::lock_guard<std::mutex> lock(g_mutex);
    Buffer *b = findLocked(handle);
    return b ? b->device : -1;
}

int mockNuma(mc_tpu_buffer_t handle) {
    std::lock_guard<std::mutex> lock(g_mutex);
    Buffer *b = findLocked(handle);
    return b ? b->numa : -1;
}

int mockCopyToHost(mc_tpu_buffer_t handle, size_t offset, void *dst,
                   size_t len) {
    if (g_fail_copies) return 1;
    std::lock_guard<std::mutex> lock(g_mutex);
    Buffer *b = findLocked(handle);
    if (!b || offset > b->data.size() || len > b->data.size() - offset) {
        return 1;
    }
    std::memcpy(dst, b->data.data() + offset, len);
    return 0;
}

int mockCopyFromHost(mc_tpu_buffer_t handle, size_t offset, const void *src,
                     size_t len) {
    if (g_fail_copies) return 1;
    std::lock_guard<std::mutex> lock(g_mutex);
    Buffer *b = findLocked(handle);
    if (!b || offset > b->data.size() || len > b->data.size() - offset) {
        return 1;
    }
    std::memcpy(b->data.data() + offset, src, len);
    return 0;
}

void mockRelease(mc_tpu_buffer_t handle) {
    std::lock_guard<std::mutex> lock(g_mutex);
    if (g_buffers.erase(handle) != 0) ++g_released;
}

}  // namespace

extern "C" {

// --- Provider entry point --------------------------------------------------

int mc_tpu_buffer_provider_get(uint32_t requested_version,
                               mc_tpu_buffer_provider_t *out) {
    if (!out || requested_version != MC_TPU_BUFFER_PROVIDER_ABI_VERSION) {
        return 1;
    }
    out->abi_version = MC_TPU_BUFFER_PROVIDER_ABI_VERSION;
    out->name = "mock";
    out->size = mockSize;
    out->device_index = mockDevice;
    out->numa_node = mockNuma;
    out->copy_to_host = mockCopyToHost;
    out->copy_from_host = mockCopyFromHost;
    out->release = mockRelease;
    return 0;
}

// --- Test-only helpers ------------------------------------------------------

// Creates a buffer filled with a recognizable byte pattern; returns its handle.
mc_tpu_buffer_t mock_provider_create_buffer(size_t size, int device, int numa) {
    auto buffer = std::make_unique<Buffer>();
    buffer->data.resize(size);
    for (size_t i = 0; i < size; ++i) buffer->data[i] = (uint8_t)(i % 251);
    buffer->device = device;
    buffer->numa = numa;
    std::lock_guard<std::mutex> lock(g_mutex);
    const mc_tpu_buffer_t handle = g_next_handle++;
    g_buffers[handle] = std::move(buffer);
    return handle;
}

// Direct access to a live buffer's bytes, or nullptr once released.
uint8_t *mock_provider_buffer_data(mc_tpu_buffer_t handle) {
    std::lock_guard<std::mutex> lock(g_mutex);
    Buffer *b = findLocked(handle);
    return b ? b->data.data() : nullptr;
}

int mock_provider_live_count(void) {
    std::lock_guard<std::mutex> lock(g_mutex);
    return static_cast<int>(g_buffers.size());
}

int mock_provider_released_count(void) { return g_released; }

void mock_provider_set_fail_copies(int fail) { g_fail_copies = fail != 0; }

}  // extern "C"
