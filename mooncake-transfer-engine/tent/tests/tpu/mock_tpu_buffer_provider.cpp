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

// Mock TPU buffer provider (tpu_buffer_provider.h) backed by host memory.
// DMA mappings are only recorded, refusing what PJRT would refuse: unaligned
// or overlapping ranges.

#include <unistd.h>

#include <atomic>
#include <cstdint>
#include <cstring>
#include <iterator>
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
std::atomic<size_t> g_device_alignment{0};  // 0: any offset
std::atomic<int> g_rejected{0};
std::map<uintptr_t, size_t> g_dma_mappings;  // host base -> length
std::atomic<bool> g_enable_dma_map{true};
std::atomic<bool> g_fail_dma_map{false};
std::atomic<int> g_unmapped_copies{0};

Buffer *findLocked(mc_tpu_buffer_t handle) {
    auto it = g_buffers.find(handle);
    return it == g_buffers.end() ? nullptr : it->second.get();
}

// Counts a copy whose host range is not inside one live DMA mapping.
void countUnmappedLocked(const void *host, size_t len) {
    const auto a = reinterpret_cast<uintptr_t>(host);
    auto it = g_dma_mappings.upper_bound(a);
    if (it != g_dma_mappings.begin()) {
        --it;
        const size_t offset = a - it->first;
        if (offset <= it->second && len <= it->second - offset) return;
    }
    ++g_unmapped_copies;
}

bool rejectOffset(size_t offset) {
    const size_t alignment = g_device_alignment;
    if (alignment == 0 || offset % alignment == 0) return false;
    ++g_rejected;
    return true;
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
    if (g_fail_copies || rejectOffset(offset)) return 1;
    std::lock_guard<std::mutex> lock(g_mutex);
    Buffer *b = findLocked(handle);
    if (!b || offset > b->data.size() || len > b->data.size() - offset) {
        return 1;
    }
    std::memcpy(dst, b->data.data() + offset, len);
    countUnmappedLocked(dst, len);
    return 0;
}

int mockCopyFromHost(mc_tpu_buffer_t handle, size_t offset, const void *src,
                     size_t len) {
    if (g_fail_copies || rejectOffset(offset)) return 1;
    std::lock_guard<std::mutex> lock(g_mutex);
    Buffer *b = findLocked(handle);
    if (!b || offset > b->data.size() || len > b->data.size() - offset) {
        return 1;
    }
    std::memcpy(b->data.data() + offset, src, len);
    countUnmappedLocked(src, len);
    return 0;
}

void mockRelease(mc_tpu_buffer_t handle) {
    std::lock_guard<std::mutex> lock(g_mutex);
    if (g_buffers.erase(handle) != 0) ++g_released;
}

int mockDmaMap(void *host, size_t len) {
    const auto a = reinterpret_cast<uintptr_t>(host);
    const auto page = static_cast<uintptr_t>(sysconf(_SC_PAGESIZE));
    if (g_fail_dma_map || a == 0 || len == 0 || a % page || len % page) {
        return 1;
    }
    std::lock_guard<std::mutex> lock(g_mutex);
    auto next = g_dma_mappings.lower_bound(a);
    if (next != g_dma_mappings.end() && next->first < a + len) return 1;
    if (next != g_dma_mappings.begin()) {
        auto prev = std::prev(next);
        if (prev->first + prev->second > a) return 1;
    }
    g_dma_mappings[a] = len;
    return 0;
}

int mockDmaUnmap(void *host) {
    std::lock_guard<std::mutex> lock(g_mutex);
    return g_dma_mappings.erase(reinterpret_cast<uintptr_t>(host)) ? 0 : 1;
}

}  // namespace

extern "C" {

int mc_tpu_buffer_provider_get(mc_tpu_buffer_provider_t *out) {
    if (!out) {
        return 1;
    }
    out->name = "mock";
    out->size = mockSize;
    out->device_index = mockDevice;
    out->numa_node = mockNuma;
    out->copy_to_host = mockCopyToHost;
    out->copy_from_host = mockCopyFromHost;
    out->release = mockRelease;
    if (g_enable_dma_map) {
        out->dma_map = mockDmaMap;
        out->dma_unmap = mockDmaUnmap;
    }
    return 0;
}

// --- Test-only helpers ------------------------------------------------------

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

// A live buffer's bytes, or nullptr once released.
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

// Copies at device offsets not a multiple of `alignment` fail, copying nothing.
void mock_provider_set_device_alignment(size_t alignment) {
    g_device_alignment = alignment;
}

int mock_provider_rejected_count(void) { return g_rejected; }

// Applies from the next binding on; 0 models a provider without DMA mapping.
void mock_provider_set_enable_dma_map(int enable) {
    g_enable_dma_map = enable != 0;
}

void mock_provider_set_fail_dma_map(int fail) { g_fail_dma_map = fail != 0; }

int mock_provider_dma_mapping_count(void) {
    std::lock_guard<std::mutex> lock(g_mutex);
    return static_cast<int>(g_dma_mappings.size());
}

int mock_provider_unmapped_copy_count(void) { return g_unmapped_copies; }

}  // extern "C"
