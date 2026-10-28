// Copyright(C) 2025 Advanced Micro Devices, Inc. All rights reserved.
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

#ifndef BATCH_COPY_H_
#define BATCH_COPY_H_

#include <hip/hip_runtime.h>

#include <cstddef>
#include <cstdint>
#include <deque>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

namespace mooncake {

// Copies many (src, dst, len) ranges with one kernel launch instead of one
// hipMemcpyAsync per range. Each hipMemcpyAsync costs a few microseconds on
// the host, which caps small copies (KV pages, head slices, scales) at well
// under 1 GB/s; the kernel runs them at link speed and spreads one batch
// over several XGMI links at once.
//
// The kernel is compiled with hiprtc on first use (libhiprtc is loaded with
// dlopen, so it is not a link dependency). If that fails, available() turns
// false for the device and the caller keeps using hipMemcpyAsync.
class BatchCopyKernel {
   public:
    struct Range {
        uint64_t src;
        uint64_t dst;
        uint64_t len;
    };

    // Ranges one launch can take; a batch with more uses several launches.
    static constexpr size_t kSlotRanges = 8192;

    BatchCopyKernel(int num_slots, int max_grid);
    ~BatchCopyKernel();

    BatchCopyKernel(const BatchCopyKernel&) = delete;
    BatchCopyKernel& operator=(const BatchCopyKernel&) = delete;

    // Whether the kernel can run on device_id (loads it on first call).
    bool available(int device_id);

    // Enqueue ranges[0, n) on stream, which belongs to the current device
    // device_id. Returns how many leading ranges were enqueued; the rest
    // must be copied another way (only after a failed launch).
    size_t enqueue(int device_id, hipStream_t stream, const Range* ranges,
                   size_t n);

   private:
    // Each launch holds a descriptor slot until the GPU has run it, which
    // the slot's event tells. With every slot busy, enqueue() waits for the
    // oldest launch; that needs no host progress, so it cannot deadlock.
    struct Device {
        bool loaded = false;
        bool failed = false;
        hipModule_t module = nullptr;
        hipFunction_t function = nullptr;
        Range* host = nullptr;    // pinned descriptor staging
        Range* device = nullptr;  // what the kernel reads
        std::mutex mutex;         // guards the slot lists
        std::vector<hipEvent_t> events;
        std::vector<int> free_slots;
        std::deque<int> busy_slots;  // oldest first
    };

    // Called with dev.mutex held. Returns -1 only if waiting failed.
    int acquireSlot(Device& dev);

    bool load(int device_id, Device& dev);

    const int num_slots_;
    const int max_grid_;
    std::mutex mutex_;  // guards devices_ and code_objects_
    std::unordered_map<int, Device> devices_;
    // Code objects by GPU architecture, shared by devices of the same arch.
    std::unordered_map<std::string, std::string> code_objects_;
};

}  // namespace mooncake

#endif  // BATCH_COPY_H_
