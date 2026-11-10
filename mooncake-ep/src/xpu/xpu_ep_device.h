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

// Device helpers shared by the XPU EP kernels (the low-latency Buffer and
// the ElasticBuffer): peer addressing, cooperative copies, and the ready-flag
// protocol described at BufferPair in xpu_ep_common.h.

#pragma once

#include <sycl/sycl.hpp>

#include <cstdint>

#include "xpu_ep_common.h"
#include "xpu_ep_kernels.h"

namespace mooncake {
namespace xpu_ep {

using vec4 = sycl::vec<uint32_t, 4>;

constexpr int kWGSize = 512;
constexpr int kNumWarps = kWGSize / kWarpSize;

template <typename T>
using device_atomic =
    sycl::atomic_ref<T, sycl::memory_order::relaxed, sycl::memory_scope::device,
                     sycl::access::address_space::global_space>;

XPU_EP_INLINE uint8_t* rebase(const PeerView& pv, const void* local, int rank) {
    void* b = pv.peer_bases[rank];
    if (!b) return nullptr;
    return static_cast<uint8_t*>(b) +
           (static_cast<const uint8_t*>(local) -
            static_cast<const uint8_t*>(pv.local_base));
}

// Copies `bytes` (a multiple of 16) with `lanes` cooperating work-items.
XPU_EP_INLINE void coop_copy(uint8_t* dst, const uint8_t* src, size_t bytes,
                             int lane, int lanes) {
    auto* d = reinterpret_cast<vec4*>(dst);
    auto* s = reinterpret_cast<const vec4*>(src);
    for (size_t j = lane; j < bytes / sizeof(vec4); j += lanes) d[j] = s[j];
}

// coop_copy from a peer: a remote load takes a PCIe round trip, so each
// work-item issues kPullUnroll independent loads before storing any.
constexpr int kPullUnroll = 8;
XPU_EP_INLINE void coop_pull(uint8_t* dst, const uint8_t* src, size_t bytes,
                             int lane, int lanes) {
    auto* d = reinterpret_cast<vec4*>(dst);
    auto* s = reinterpret_cast<const vec4*>(src);
    const size_t n = bytes / sizeof(vec4);
    for (size_t base = lane; base < n;
         base += static_cast<size_t>(lanes) * kPullUnroll) {
        vec4 r[kPullUnroll];
#pragma unroll
        for (int u = 0; u < kPullUnroll; ++u) {
            const size_t i = base + static_cast<size_t>(u) * lanes;
            if (i < n) r[u] = s[i];
        }
#pragma unroll
        for (int u = 0; u < kPullUnroll; ++u) {
            const size_t i = base + static_cast<size_t>(u) * lanes;
            if (i < n) d[i] = r[u];
        }
    }
}

// A poll iteration takes a few thousand cycles; anything longer between two
// clock reads is the thread having been preempted and resumed.
constexpr uint64_t kMaxClockStep = 1ull << 20;

// Spins until *flag is non-zero. Returns 0 when `src_rank` is (or becomes,
// after timeout_ticks) inactive, matching the CUDA receive loop.
//
// The cycle counter is per execution unit, so a thread resumed on another
// one sees it jump in either direction; summing only short steps keeps such a
// jump from firing the timeout.
XPU_EP_INLINE int wait_flag(const int* flag, int32_t* active_ranks,
                            int src_rank, int64_t timeout_ticks) {
    device_atomic<int32_t> active(active_ranks[src_rank]);
    uint64_t prev = device_clock(), waited = 0;
    int v;
    while ((v = uc_load(flag)) == 0) {
        if (timeout_ticks >= 0) {
            const uint64_t now = device_clock();
            const uint64_t step = now - prev;
            if (step < kMaxClockStep) waited += step;
            prev = now;
            if (waited > static_cast<uint64_t>(timeout_ticks)) active.store(0);
        }
        if (active.load() == 0) return 0;
    }
    return v;
}

// Every work-group calls this once at the end of a send kernel. Returns true
// in exactly one work-group -- the last to arrive -- once every other
// work-group's staging stores have been made visible system-wide.
XPU_EP_INLINE bool arrive_last(sycl::nd_item<1> it, int* done_counter,
                               int* is_last) {
    sycl::atomic_fence(sycl::memory_order::release, sycl::memory_scope::system);
    sycl::group_barrier(it.get_group());
    if (it.get_local_id(0) == 0) {
        sycl::atomic_ref<int, sycl::memory_order::acq_rel,
                         sycl::memory_scope::device,
                         sycl::access::address_space::global_space>
            done(*done_counter);
        const int prev = done.fetch_add(1);
        const bool last = prev == static_cast<int>(it.get_group_range(0)) - 1;
        if (last) done.store(0);
        *is_last = last;
    }
    sycl::group_barrier(it.get_group());
    if (!*is_last) return false;
    sycl::atomic_fence(sycl::memory_order::acquire, sycl::memory_scope::system);
    return true;
}

// The last work-group of a send kernel tells every rank this one's staging is
// ready, by setting ready[this rank] in that rank's copy of the current half.
// The flag is 1, or with `values`, values[r] + 1 for rank r, which saves the
// receiver a remote read.
XPU_EP_INLINE void post_ready(int tid, const PeerView& pv, int* ready, int rank,
                              int num_ranks, const int* values = nullptr) {
    sycl::atomic_fence(sycl::memory_order::release, sycl::memory_scope::system);
    for (int r = tid; r < num_ranks; r += kWGSize) {
        int* flag =
            reinterpret_cast<int*>(rebase(pv, ready + rank * kReadyStride, r));
        if (flag) uc_store(flag, values ? values[r] + 1 : 1);
    }
}

XPU_EP_INLINE void clean_ready(int tid, int* next_ready, int num_ranks) {
    for (int r = tid; r < num_ranks; r += kWGSize)
        uc_store(next_ready + r * kReadyStride, 0);
}

// A system-scope acquire invalidates the caches this work-group reads
// through (its Xe-core's L1 and the shared L3), so the leader issuing it
// before a barrier covers every work-item -- not one per work-item, which at
// 512 lanes per work-group dominates a decode-sized receive.
XPU_EP_INLINE void group_acquire(sycl::nd_item<1> it) {
    sycl::group_barrier(it.get_group());
    if (it.get_local_id(0) == 0)
        sycl::atomic_fence(sycl::memory_order::acquire,
                           sycl::memory_scope::system);
    sycl::group_barrier(it.get_group());
}

}  // namespace xpu_ep
}  // namespace mooncake
