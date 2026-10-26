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

// Definitions shared by the Intel XPU (SYCL) Mooncake EP host code and
// kernels.

#pragma once

#include <mooncake_ep_dispatch_slots.h>
#include <sycl/sycl.hpp>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <stdexcept>
#include <string>

// Declared at global scope so it keeps the builtin's mangled name.
#ifdef __SYCL_DEVICE_ONLY__
extern SYCL_EXTERNAL uint64_t __spirv_ReadClockKHR(int);
#endif

namespace mooncake {
namespace xpu_ep {

#define XPU_EP_INLINE inline __attribute__((always_inline))

// Kernels are written for a 32-wide sub-group, the CUDA warp size.
constexpr int kWarpSize = 32;
constexpr int kNumPerChannels = 128;  // FP8 scale granularity
constexpr int kBf16Size = 2;
constexpr size_t kLineBytes = 64;

inline size_t round_up(size_t v, size_t a) { return (v + a - 1) / a * a; }

constexpr int kTopkShadowMaxTokens = 1024;
constexpr int kTopkShadowMaxTopk = 32;
constexpr int kTopkShadowSlots = kEpDispatchSlots;
constexpr size_t kTopkShadowBytes = static_cast<size_t>(kTopkShadowMaxTokens) *
                                    kTopkShadowMaxTopk * sizeof(int64_t);
// Per (token, top-k) slot in the destination expert's list, kept with the
// top-k snapshot so combine knows where its rows are.
constexpr size_t kSlotShadowBytes = static_cast<size_t>(kTopkShadowMaxTokens) *
                                    kTopkShadowMaxTopk * sizeof(int32_t);

// Transfers are pulled: each rank stages what it sends in its own half of the
// shared buffer and flags the peers, which read it over PCIe P2P. On B60 a GPU
// writing to several peers at once falls to 0.35 GB/s per peer, while reading
// from several holds 13 GB/s per peer (26 GB/s total); writing to one peer is
// 20 GB/s. So peer writes are only ever the ready flags.
//
// Each half holds, from its base:
//   ready[num_ranks]        one cache line per source rank, written by peers
//   staging                 written by this rank, either
//     dispatch: see DispatchStaging
//     combine:  row[num_local][num_ranks][T] (expert output rows, ordered as
//               the receiving dispatch listed them) or, when the dispatch had
//               top-k weights, row[num_ranks][T] (per source token, the
//               weighted sum of this rank's experts' rows)
// A call uses one half; consecutive calls alternate halves, as on CUDA.
struct HalfLayout {
    int* ready;  // rank r's flag at ready[r * kReadyStride]
    uint8_t* staging;
};

constexpr int kReadyStride = kLineBytes / sizeof(int);

inline size_t dispatch_msg_bytes(int hidden, bool use_fp8);

// Dispatch staging, by offset. Peers read everything before local_stage:
//   msg[T]                  line-padded token messages
//   count[E], list[E][T]    tokens this rank sends to each expert; a list
//                           entry is token | rank_list position << 16
//   rank_count[R], rank_list[R][T]
//                           the distinct tokens this rank sends to each rank;
//                           an entry is token | picks on that rank << 16
//   rank_pick[R][T][kTopkShadowMaxTopk]
//                           those picks as {local expert | list slot << 16,
//                           top-k weight}, written when weights are given
// local_stage[R][T] is this rank's own: the receive kernel pulls each token
// a peer sent here into it once, then copies it to every expert that picked it.
// Offsets follow the BF16 message size, so FP8 and BF16 calls share a layout.
struct DispatchStaging {
    size_t counts, lists, rank_counts, rank_lists, rank_picks, local_stage, end;
    DispatchStaging(int hidden, int T, int num_experts, int num_ranks) {
        const size_t msg = dispatch_msg_bytes(hidden, false);
        const size_t t = T, r = num_ranks;
        counts = t * msg;
        lists = counts + round_up(num_experts * sizeof(int), kLineBytes);
        rank_counts =
            lists + round_up(num_experts * t * sizeof(int), kLineBytes);
        rank_lists = rank_counts + round_up(r * sizeof(int), kLineBytes);
        rank_picks = rank_lists + round_up(r * t * sizeof(int), kLineBytes);
        local_stage =
            rank_picks +
            round_up(r * t * kTopkShadowMaxTopk * 2 * sizeof(int), kLineBytes);
        end = local_stage + r * t * msg;
    }
};

// What a rank received from a dispatch given top-k weights, kept per dispatch
// for its combine to pre-reduce: per source rank, the number of distinct
// tokens, and per token its picks of this rank's experts (as rank_pick).
// Indexed with the maximum token and top-k counts, so one layout fits all.
inline size_t recv_table_ints(int num_ranks) {
    return static_cast<size_t>(num_ranks) *
           (1 + static_cast<size_t>(kTopkShadowMaxTokens) *
                    (1 + 2 * kTopkShadowMaxTopk));
}

struct BufferPair {
    size_t total_bytes = 0;
    size_t half_bytes = 0;
    HalfLayout halves[2];

    BufferPair(void* base, int num_max_dispatch_tokens_per_rank, int hidden,
               int num_ranks, int num_experts) {
        const size_t T = num_max_dispatch_tokens_per_rank;
        const size_t ready_bytes = round_up(num_ranks * kLineBytes, 4096);
        const size_t dispatch_bytes =
            DispatchStaging(hidden, static_cast<int>(T), num_experts, num_ranks)
                .end;
        const size_t combine_bytes =
            static_cast<size_t>(num_experts) * T * hidden * kBf16Size;
        half_bytes = round_up(
            ready_bytes + std::max(dispatch_bytes, combine_bytes), 4096);
        for (int i = 0; i < 2; ++i) {
            uint8_t* h = static_cast<uint8_t*>(base) + i * half_bytes;
            halves[i] = {reinterpret_cast<int*>(h), h + ready_bytes};
        }
        total_bytes = 2 * half_bytes;
    }
};

inline size_t get_ep_buffer_size_hint(int num_max_dispatch_tokens_per_rank,
                                      int hidden, int num_ranks,
                                      int num_experts) {
    return BufferPair(nullptr, num_max_dispatch_tokens_per_rank, hidden,
                      num_ranks, num_experts)
        .total_bytes;
}

// Dispatch message: [hidden BF16 or FP8 values][FP8 only: hidden / 128
// float scales], padded to whole cache lines. Message t is source token t, so
// it needs no header.
inline size_t dispatch_msg_bytes(int hidden, bool use_fp8) {
    return round_up(use_fp8
                        ? hidden + (hidden / kNumPerChannels) * sizeof(float)
                        : static_cast<size_t>(hidden) * kBf16Size,
                    kLineBytes);
}

// Matches CUDA pack2<int, int64_t>: count in the low word, offset high.
inline int64_t pack_range(int count, int begin) {
    return static_cast<int64_t>(
        (static_cast<uint64_t>(static_cast<uint32_t>(begin)) << 32) |
        static_cast<uint32_t>(count));
}

inline void unpack_range(int64_t packed, int& count, int& begin) {
    const uint64_t u = static_cast<uint64_t>(packed);
    count = static_cast<int>(u & 0xffffffffu);
    begin = static_cast<int>(u >> 32);
}

#define XPU_EP_CHECK(cond, msg)                                           \
    do {                                                                  \
        if (!(cond))                                                      \
            throw std::runtime_error(std::string("Mooncake EP (XPU): ") + \
                                     (msg));                              \
    } while (0)

// ---------- Device helpers ----------

// Sub-group-scope cycle counter. Only ever compared against a reading taken
// by the same work-item, so the scope is sufficient for a timeout.
XPU_EP_INLINE uint64_t device_clock() {
#ifdef __SYCL_DEVICE_ONLY__
    return __spirv_ReadClockKHR(3);
#else
    return 0;
#endif
}

// Peers write this GPU's ready flags over PCIe, and those writes do not update
// this GPU's caches. So this GPU's own stores to flag lines (the next-half
// clean, the initial zeroing) are uncached (hint 0x7: uncached at L1-L3): a
// dirty line written back later would erase a peer's flag. Flags are polled
// uncached. Staging is written with plain stores and read by peers after a
// system-scope release (writer) and acquire (reader).
template <typename T>
XPU_EP_INLINE T uc_load(const T* ptr) {
    static_assert(sizeof(T) == 4, "uc_load supports 32-bit types");
#ifdef __SYCL_DEVICE_ONLY__
    const uint32_t v = *reinterpret_cast<const volatile uint32_t*>(
        __builtin_intel_sycl_ptr_annotation(
            reinterpret_cast<uint32_t*>(const_cast<T*>(ptr)),
            "sycl-cache-read-hint", 0x7));
    T out;
    __builtin_memcpy(&out, &v, sizeof(T));
    return out;
#else
    return *ptr;
#endif
}

// 16-byte store with the same uncached hint (used to zero the buffer).
template <typename V>
XPU_EP_INLINE void uc_store_vec(V* ptr, const V& value) {
#ifdef __SYCL_DEVICE_ONLY__
    *__builtin_intel_sycl_ptr_annotation(ptr, "sycl-cache-write-hint", 0x7) =
        value;
#else
    *ptr = value;
#endif
}

template <typename T>
XPU_EP_INLINE void uc_store(T* ptr, T value) {
    static_assert(sizeof(T) == 4, "uc_store supports 32-bit types");
#ifdef __SYCL_DEVICE_ONLY__
    uint32_t v;
    __builtin_memcpy(&v, &value, sizeof(T));
    *reinterpret_cast<volatile uint32_t*>(__builtin_intel_sycl_ptr_annotation(
        reinterpret_cast<uint32_t*>(ptr), "sycl-cache-write-hint", 0x7)) = v;
#else
    *ptr = value;
#endif
}

XPU_EP_INLINE float bf16_bits_to_float(uint16_t bits) {
    return sycl::bit_cast<float>(static_cast<uint32_t>(bits) << 16);
}

// Round-to-nearest-even, matching __float2bfloat16.
XPU_EP_INLINE uint16_t float_to_bf16_bits(float f) {
    const uint32_t u = sycl::bit_cast<uint32_t>(f);
    if ((u & 0x7fffffffu) > 0x7f800000u) return 0x7fc0;  // NaN
    return static_cast<uint16_t>((u + 0x7fffu + ((u >> 16) & 1u)) >> 16);
}

// float -> FP8 E4M3 (torch.float8_e4m3fn), round-to-nearest-even, saturating
// to +-448 like __nv_cvt_float2_to_fp8x2(..., __NV_SATFINITE, __NV_E4M3).
XPU_EP_INLINE uint8_t float_to_e4m3(float f) {
    const uint32_t u = sycl::bit_cast<uint32_t>(f);
    const uint8_t sign = static_cast<uint8_t>((u >> 24) & 0x80u);
    const uint32_t a = u & 0x7fffffffu;
    if (a > 0x7f800000u) return sign | 0x7f;   // NaN
    if (a >= 0x43e00000u) return sign | 0x7e;  // >= 448 (and inf): saturate
    if (a < 0x3c800000u) {                     // < 2^-6: subnormal or zero
        // Subnormal step is 2^-9; scale into an integer and round to even.
        const float scaled = sycl::bit_cast<float>(a) * 512.0f;
        const float r = sycl::rint(scaled);
        return sign | static_cast<uint8_t>(r);
    }
    // Normal: keep 3 mantissa bits, rebias exponent 127 -> 7.
    const uint32_t rounded = a + 0x7ffffu + ((a >> 20) & 1u);
    const uint32_t exp = (rounded >> 23) - 120;
    const uint32_t man = (rounded >> 20) & 0x7u;
    const uint32_t bits = (exp << 3) | man;
    return sign | static_cast<uint8_t>(bits > 0x7e ? 0x7e : bits);
}

}  // namespace xpu_ep
}  // namespace mooncake
