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

#pragma once

// Pinned-memory staging pool for GPU buffers transferred over TCP.
//
// The legacy GPU path stages every 64KB slice through a freshly allocated
// pageable buffer with a synchronous cudaMemcpy, serializing GPU copies with
// socket IO on the transport's single io thread. This pool replaces that
// with: one persistent pinned buffer per slot, one non-blocking stream and
// one event per slot. A whole transfer is staged with a single asynchronous
// copy that is ordered after the producing compute stream via an event, and
// the socket layer then streams from (or into) the pinned slot directly.
//
// Environment variables:
//   MC_TCP_GPU_STAGING          "0"/"false"/"no" disables the pool; any
//                               transfer then uses the legacy per-chunk path.
//   MC_TCP_GPU_STAGING_SLOT_MB  capacity of one slot in MiB (default 16).
//   MC_TCP_GPU_STAGING_SLOTS    number of slots (default 8).
//
// acquire() returns nullptr when the pool is disabled, the request is larger
// than one slot, or no slot is free; callers must fall back to the legacy
// path in that case.

#include <glog/logging.h>

#include <cstdlib>
#include <mutex>
#include <string>
#include <vector>

// HIP is intentionally excluded: the pool calls the runtime API by its cuda*
// names, which gpu_vendor/hip.h does not alias, and mooncake-integration
// compiles this header without hipify. HIP builds use the stub below, i.e.
// the legacy per-chunk staging path, exactly as before this pool existed.
// cuda_alike.h (not cuda_runtime_api.h) lets the MUSA/MLU/... vendor headers
// supply their cuda* aliases.
#if defined(USE_CUDA) || defined(USE_MUSA) || defined(USE_MLU) ||    \
    defined(USE_MACA) || defined(USE_HYGON) || defined(USE_COREX) || \
    defined(USE_SUPA)
#include "cuda_alike.h"
#define MOONCAKE_TCP_GPU_STAGING_AVAILABLE 1
#endif

#ifdef MOONCAKE_TCP_GPU_STAGING_AVAILABLE

class GpuStagingPool {
   public:
    struct Slot {
        char* buffer = nullptr;
        size_t capacity = 0;
        int device = -1;
        cudaStream_t stream = nullptr;
        cudaEvent_t event = nullptr;
        bool in_use = false;
    };

    // Intentionally leaked: CUDA teardown order at process exit is undefined,
    // so the pool (and its pinned memory) is never freed.
    static GpuStagingPool* instance() {
        static GpuStagingPool* pool = new GpuStagingPool();
        return pool;
    }

    bool enabled() const { return enabled_; }

    // Returns nullptr when disabled, oversized, or exhausted.
    Slot* acquire(size_t size) {
        if (!enabled_ || size > slot_bytes_) {
            if (enabled_ && size > slot_bytes_) {
                // Silent fallback once caused an attribution incident
                // (slot smaller than vLLM's merged ~115MB transfers, so the
                // fast path never engaged and nobody noticed). Warn once.
                static std::once_flag once;
                std::call_once(once, [&] {
                    LOG(WARNING)
                        << "GpuStagingPool: request size " << size
                        << " bytes exceeds slot capacity " << slot_bytes_
                        << " bytes; falling back to legacy per-chunk staging. "
                        << "Increase MC_TCP_GPU_STAGING_SLOT_MB.";
                });
            }
            return nullptr;
        }
        std::lock_guard<std::mutex> lock(mutex_);
        for (auto& slot : slots_) {
            if (!slot.in_use) {
                slot.in_use = true;
                return &slot;
            }
        }
        static std::once_flag exhausted_once;
        std::call_once(exhausted_once, [&] {
            LOG(WARNING) << "GpuStagingPool: all slots busy; falling back to "
                            "legacy per-chunk staging (consider more slots).";
        });
        return nullptr;
    }

    void release(Slot* slot) {
        if (!slot) return;
        std::lock_guard<std::mutex> lock(mutex_);
        slot->in_use = false;
    }

    // GPU -> pinned. Records an event on the caller's current stream (the
    // compute stream that produced the data), makes the slot stream wait for
    // it, submits one asynchronous copy, and blocks until it completes.
    // The caller must hold the slot via acquire().
    bool stageDeviceToHost(Slot* slot, const void* src, size_t size) {
        if (cudaSetDevice(slot->device) != cudaSuccess) {
            LOG(ERROR) << "GpuStagingPool: cudaSetDevice failed";
            return false;
        }
        cudaEventRecord(slot->event, cudaStreamPerThread);
        cudaStreamWaitEvent(slot->stream, slot->event, 0);
        cudaError_t err = cudaMemcpyAsync(slot->buffer, src, size,
                                          cudaMemcpyDefault, slot->stream);
        if (err != cudaSuccess) {
            LOG(ERROR) << "GpuStagingPool: D2H cudaMemcpyAsync failed: "
                       << cudaGetErrorString(err);
            return false;
        }
        cudaEventRecord(slot->event, slot->stream);
        err = cudaEventSynchronize(slot->event);
        if (err != cudaSuccess) {
            LOG(ERROR) << "GpuStagingPool: D2H event sync failed: "
                       << cudaGetErrorString(err);
            return false;
        }
        return true;
    }

    // GPU -> pinned, multi-segment gather: copies each srcs[i] (sizes[i]
    // bytes) into the slot at consecutive offsets, in order, all on the
    // slot's non-blocking stream. Blocks until the whole gather completes.
    // The caller must hold the slot via acquire() and guarantee the total
    // fits the slot capacity (acquire already enforces this).
    //
    // Ordering: a device-wide sync orders the copies after whatever compute
    // stream produced the source data. The caller invokes this only after
    // the producing work has finished (vLLM: after the prefill forward), so
    // the sync is cheap; per-stream event ordering would need the producer
    // stream, which the engine does not know.
    bool gatherDeviceToHost(Slot* slot, const std::vector<const void*>& srcs,
                            const std::vector<size_t>& sizes) {
        if (cudaSetDevice(slot->device) != cudaSuccess) {
            LOG(ERROR) << "GpuStagingPool: cudaSetDevice failed";
            return false;
        }
        cudaDeviceSynchronize();
#if CUDART_VERSION >= 12080
        // One batched launch for the whole gather: ~175k tiny per-block
        // copies of a 100k-token request take ~11 s when issued one
        // cudaMemcpyAsync at a time (launch-latency bound, ~63 us each);
        // batched, the same 5.7 GB gather is bandwidth bound (<1 s).
        // Destinations are consecutive non-overlapping slices of the slot,
        // sources are distinct KV blocks, so the batch's "no ordering
        // guarantees within the batch" is safe. Mirrors scatterHostToDevice.
        {
            std::vector<void*> dsts(srcs.size());
            size_t off = 0;
            for (size_t i = 0; i < srcs.size(); ++i) {
                dsts[i] = slot->buffer + off;
                off += sizes[i];
            }
            cudaMemcpyAttributes attrs{};
            attrs.srcAccessOrder = cudaMemcpySrcAccessOrderStream;
            size_t attrs_idx = 0;
#if CUDART_VERSION >= 13000
            cudaError_t err = cudaMemcpyBatchAsync(
                dsts.data(), srcs.data(), sizes.data(), srcs.size(), &attrs,
                &attrs_idx, 1, slot->stream);
#else
            // CUDA 12.8 introduced cudaMemcpyBatchAsync with mutable (void**)
            // pointer arrays and an extra failIdx out-param; 13.0 made the
            // arrays const-correct and dropped failIdx.
            std::vector<void*> srcs_mut(srcs.size());
            for (size_t i = 0; i < srcs.size(); ++i) {
                srcs_mut[i] = const_cast<void*>(srcs[i]);
            }
            std::vector<size_t> sizes_mut(sizes.begin(), sizes.end());
            size_t fail_idx = 0;
            cudaError_t err = cudaMemcpyBatchAsync(
                dsts.data(), srcs_mut.data(), sizes_mut.data(), srcs.size(),
                &attrs, &attrs_idx, 1, &fail_idx, slot->stream);
#endif
            if (err == cudaSuccess) {
                cudaEventRecord(slot->event, slot->stream);
                err = cudaEventSynchronize(slot->event);
                if (err != cudaSuccess) {
                    LOG(ERROR) << "GpuStagingPool: gather event sync failed: "
                               << cudaGetErrorString(err);
                    return false;
                }
                return true;
            }
            LOG(WARNING) << "GpuStagingPool: cudaMemcpyBatchAsync failed: "
                         << cudaGetErrorString(err)
                         << "; falling back to per-segment copies "
                            "(re-copying is idempotent: same bytes, same "
                            "destinations)";
        }
#endif
        size_t off = 0;
        for (size_t i = 0; i < srcs.size(); ++i) {
            cudaError_t err =
                cudaMemcpyAsync(slot->buffer + off, srcs[i], sizes[i],
                                cudaMemcpyDefault, slot->stream);
            if (err != cudaSuccess) {
                LOG(ERROR) << "GpuStagingPool: gather cudaMemcpyAsync failed "
                              "at segment "
                           << i << ": " << cudaGetErrorString(err);
                return false;
            }
            off += sizes[i];
        }
        cudaEventRecord(slot->event, slot->stream);
        cudaError_t err = cudaEventSynchronize(slot->event);
        if (err != cudaSuccess) {
            LOG(ERROR) << "GpuStagingPool: gather event sync failed: "
                       << cudaGetErrorString(err);
            return false;
        }
        return true;
    }

    // pinned -> GPU, multi-segment scatter: copies the bytes sitting in the
    // slot (read contiguously from offset 0, in order) out to dsts[i]
    // (sizes[i] bytes each), all on the slot's non-blocking stream. Blocks
    // until the whole scatter completes. Exact mirror of gatherDeviceToHost.
    // The caller must hold the slot via acquire() and guarantee the slot
    // bytes are complete (the peer's WRITE has reached a terminal status).
    //
    // Ordering: no device-wide sync is needed — the event sync guarantees
    // the copies are complete, so any kernel launched afterwards (vLLM
    // marks the request received only after this returns) observes them.
    bool scatterHostToDevice(Slot* slot, const std::vector<void*>& dsts,
                             const std::vector<size_t>& sizes) {
        if (cudaSetDevice(slot->device) != cudaSuccess) {
            LOG(ERROR) << "GpuStagingPool: cudaSetDevice failed";
            return false;
        }
#if CUDART_VERSION >= 12080
        // One batched launch for the whole scatter. Per-block payloads are
        // a strided slice of each KV block (kv_block_len < block_len), so
        // they cannot be coalesced into larger copies, and issuing ~10k
        // tiny cudaMemcpyAsync one at a time is launch-latency bound
        // (observed ~40 ms for ~10.5k segments of a 346 MB batch, vs ~6 ms
        // for vLLM's old bulk-H2D + index_copy path). The copies are
        // independent (non-overlapping destinations), which is exactly
        // what the batch API requires.
        {
            std::vector<const void*> srcs(dsts.size());
            size_t off = 0;
            for (size_t i = 0; i < dsts.size(); ++i) {
                srcs[i] = slot->buffer + off;
                off += sizes[i];
            }
            cudaMemcpyAttributes attrs{};
            attrs.srcAccessOrder = cudaMemcpySrcAccessOrderStream;
            size_t attrs_idx = 0;
#if CUDART_VERSION >= 13000
            cudaError_t err = cudaMemcpyBatchAsync(
                dsts.data(), srcs.data(), sizes.data(), dsts.size(), &attrs,
                &attrs_idx, 1, slot->stream);
#else
            // CUDA 12.8 introduced cudaMemcpyBatchAsync with mutable (void**)
            // pointer arrays and an extra failIdx out-param; 13.0 made the
            // arrays const-correct and dropped failIdx.
            std::vector<void*> dsts_mut(dsts.begin(), dsts.end());
            std::vector<void*> srcs_mut(srcs.size());
            for (size_t i = 0; i < srcs.size(); ++i) {
                srcs_mut[i] = const_cast<void*>(srcs[i]);
            }
            std::vector<size_t> sizes_mut(sizes.begin(), sizes.end());
            size_t fail_idx = 0;
            cudaError_t err = cudaMemcpyBatchAsync(
                dsts_mut.data(), srcs_mut.data(), sizes_mut.data(), dsts.size(),
                &attrs, &attrs_idx, 1, &fail_idx, slot->stream);
#endif
            if (err == cudaSuccess) {
                cudaEventRecord(slot->event, slot->stream);
                err = cudaEventSynchronize(slot->event);
                if (err != cudaSuccess) {
                    LOG(ERROR) << "GpuStagingPool: scatter event sync failed: "
                               << cudaGetErrorString(err);
                    return false;
                }
                return true;
            }
            LOG(WARNING) << "GpuStagingPool: cudaMemcpyBatchAsync failed: "
                         << cudaGetErrorString(err)
                         << "; falling back to per-segment copies "
                            "(re-copying is idempotent: same bytes, same "
                            "destinations)";
        }
#endif
        size_t off = 0;
        for (size_t i = 0; i < dsts.size(); ++i) {
            cudaError_t err =
                cudaMemcpyAsync(dsts[i], slot->buffer + off, sizes[i],
                                cudaMemcpyDefault, slot->stream);
            if (err != cudaSuccess) {
                LOG(ERROR) << "GpuStagingPool: scatter cudaMemcpyAsync failed "
                              "at segment "
                           << i << ": " << cudaGetErrorString(err);
                return false;
            }
            off += sizes[i];
        }
        cudaEventRecord(slot->event, slot->stream);
        cudaError_t err = cudaEventSynchronize(slot->event);
        if (err != cudaSuccess) {
            LOG(ERROR) << "GpuStagingPool: scatter event sync failed: "
                       << cudaGetErrorString(err);
            return false;
        }
        return true;
    }

    // pinned -> GPU. Submits one asynchronous copy on the slot stream and
    // blocks until it is applied to device memory. Callers that report
    // completion only after this returns preserve the "payload applied at the
    // destination" semantics of the v2 protocol.
    bool stageHostToDevice(Slot* slot, void* dst, size_t size) {
        if (cudaSetDevice(slot->device) != cudaSuccess) {
            LOG(ERROR) << "GpuStagingPool: cudaSetDevice failed";
            return false;
        }
        cudaError_t err = cudaMemcpyAsync(dst, slot->buffer, size,
                                          cudaMemcpyDefault, slot->stream);
        if (err != cudaSuccess) {
            LOG(ERROR) << "GpuStagingPool: H2D cudaMemcpyAsync failed: "
                       << cudaGetErrorString(err);
            return false;
        }
        cudaEventRecord(slot->event, slot->stream);
        err = cudaEventSynchronize(slot->event);
        if (err != cudaSuccess) {
            LOG(ERROR) << "GpuStagingPool: H2D event sync failed: "
                       << cudaGetErrorString(err);
            return false;
        }
        return true;
    }

   private:
    GpuStagingPool() {
        const char* enable_env = getenv("MC_TCP_GPU_STAGING");
        if (enable_env && (std::string(enable_env) == "0" ||
                           std::string(enable_env) == "false" ||
                           std::string(enable_env) == "no")) {
            LOG(INFO) << "GpuStagingPool: disabled by MC_TCP_GPU_STAGING";
            return;
        }

        slot_bytes_ = parseEnvBytes("MC_TCP_GPU_STAGING_SLOT_MB", 16, 1, 16384);
        size_t num_slots = parseEnvSize("MC_TCP_GPU_STAGING_SLOTS", 8, 1, 64);

        int device = 0;
        if (cudaGetDevice(&device) != cudaSuccess) {
            LOG(WARNING) << "GpuStagingPool: CUDA unavailable, pool disabled";
            return;
        }

        slots_.resize(num_slots);
        for (auto& slot : slots_) {
            slot.capacity = slot_bytes_;
            slot.device = device;
            cudaError_t err =
                cudaHostAlloc(reinterpret_cast<void**>(&slot.buffer),
                              slot_bytes_, cudaHostAllocDefault);
            if (err != cudaSuccess || !slot.buffer) {
                LOG(WARNING)
                    << "GpuStagingPool: cudaHostAlloc failed (" << slot_bytes_
                    << " bytes): " << cudaGetErrorString(err)
                    << ", pool disabled";
                destroySlots();
                return;
            }
            err =
                cudaStreamCreateWithFlags(&slot.stream, cudaStreamNonBlocking);
            if (err != cudaSuccess) {
                LOG(WARNING) << "GpuStagingPool: cudaStreamCreateWithFlags "
                                "failed: "
                             << cudaGetErrorString(err) << ", pool disabled";
                destroySlots();
                return;
            }
            err = cudaEventCreateWithFlags(&slot.event, cudaEventDisableTiming);
            if (err != cudaSuccess) {
                LOG(WARNING) << "GpuStagingPool: cudaEventCreateWithFlags "
                                "failed: "
                             << cudaGetErrorString(err) << ", pool disabled";
                destroySlots();
                return;
            }
        }

        enabled_ = true;
        LOG(INFO) << "GpuStagingPool: " << num_slots << " x " << slot_bytes_
                  << " bytes pinned slots ready";
    }

    ~GpuStagingPool() { destroySlots(); }

    void destroySlots() {
        for (auto& slot : slots_) {
            if (slot.event) cudaEventDestroy(slot.event);
            if (slot.stream) cudaStreamDestroy(slot.stream);
            if (slot.buffer) cudaFreeHost(slot.buffer);
            slot = Slot{};
        }
        slots_.clear();
        enabled_ = false;
    }

    static size_t parseEnvSize(const char* name, size_t default_val,
                               size_t min_val, size_t max_val) {
        const char* env = getenv(name);
        if (!env) return default_val;
        try {
            size_t val = std::stoull(env);
            if (val < min_val || val > max_val) {
                LOG(WARNING)
                    << name << "=" << env << " out of range [" << min_val
                    << ", " << max_val << "], using default " << default_val;
                return default_val;
            }
            return val;
        } catch (const std::exception&) {
            LOG(WARNING) << name << "=" << env << " invalid, using default "
                         << default_val;
            return default_val;
        }
    }

    static size_t parseEnvBytes(const char* name, size_t default_mib,
                                size_t min_mib, size_t max_mib) {
        return parseEnvSize(name, default_mib, min_mib, max_mib) * 1024 * 1024;
    }

    bool enabled_ = false;
    size_t slot_bytes_ = 0;
    std::vector<Slot> slots_;
    std::mutex mutex_;
};

#else

// Stub for builds without GPU support (or HIP, whose non-hipified TUs cannot
// resolve the cuda* runtime names): every request falls back to the legacy
// per-chunk staging path. Slot keeps a `buffer` member because session code
// references slot->buffer on paths that are unreachable once acquire()
// returns nullptr, but still have to compile.
class GpuStagingPool {
   public:
    struct Slot {
        char* buffer = nullptr;
    };

    static GpuStagingPool* instance() {
        static GpuStagingPool pool;
        return &pool;
    }

    bool enabled() const { return false; }
    Slot* acquire(size_t) { return nullptr; }
    void release(Slot*) {}
    bool stageDeviceToHost(Slot*, const void*, size_t) { return false; }
    bool stageHostToDevice(Slot*, void*, size_t) { return false; }
    bool gatherDeviceToHost(Slot*, const std::vector<const void*>&,
                            const std::vector<size_t>&) {
        return false;
    }
};

#endif
