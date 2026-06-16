#pragma once

#include <cstdint>
#include <type_traits>

#include <mooncake_ep_utils.cuh>
#include <elastic/mooncake_ep_elastic_ptx.cuh>
#include <transport/device/comm_device.cuh>

namespace mooncake::elastic::transport {

struct WorldTeam {};
struct ScaleupTeam {};
struct ScaleoutTeam {};

constexpr int kRedAddReleaseHighWordLast = 0;
constexpr int kRedAddReleaseLowWordLast = 1 << 0;

// Mooncake Device API adapter for DeepEP's NCCL GIN usage.
//
// DeepEP elastic kernels express all remote communication through a small GIN
// surface: symmetric-pointer translation, put, put_value, RED/add style signals
// and QP flushes.  Mooncake maps that surface onto Device API semantics:
//
//   get_sym_ptr  -> mc_route_put, returning local/P2P peer VA or nullptr
//   put          -> local/P2P warp copy, otherwise mc_rdma_put
//   put_value    -> local/P2P release store, otherwise mc_rdma_put/mc_signal
//   flush        -> no-op; Device API operations are ordered by release/fence and
//                   explicit kernel barriers in the imported elastic kernels
//
// Team tags are kept as types so official DeepEP template code can remain close
// to the source while the actual routing is decided by Mooncake CommCtx.
struct MooncakeGin {
    device::CommCtx ctx;
    int qp_idx = 0;
    int sharing_mode = 0;
    int qps_per_rank = 1;
    int scaleout_rank_idx = 0;
    int scaleup_rank_idx = 0;
    int num_scaleup_ranks = 0;

    __device__ __forceinline__ MooncakeGin(const device::CommCtx& ctx,
                                           int qp_idx,
                                           int sharing_mode,
                                           int num_qps,
                                           int scaleout_rank_idx = 0,
                                           int scaleup_rank_idx = 0,
                                           int num_scaleup_ranks = 0,
                                           int num_ranks = 1)
        : ctx(ctx),
          qp_idx(qp_idx),
          sharing_mode(sharing_mode),
          qps_per_rank(max(1, num_qps / max(1, num_ranks))),
          scaleout_rank_idx(scaleout_rank_idx),
          scaleup_rank_idx(scaleup_rank_idx),
          num_scaleup_ranks(num_scaleup_ranks) {}

    template <typename team_t>
    __device__ __forceinline__ int world_rank(int dst_rank) const {
        if (num_scaleup_ranks <= 0) return dst_rank;
        if constexpr (std::is_same_v<team_t, ScaleupTeam>) {
            return scaleout_rank_idx * num_scaleup_ranks + dst_rank;
        } else if constexpr (std::is_same_v<team_t, ScaleoutTeam>) {
            return dst_rank * num_scaleup_ranks + scaleup_rank_idx;
        } else {
            return dst_rank;
        }
    }

    template <typename team_t>
    __device__ __forceinline__ bool is_nvlink_accessible(int dst_rank) const {
        dst_rank = world_rank<team_t>(dst_rank);
        return dst_rank == ctx.rank || device::mc_comm_p2p_available(ctx, dst_rank);
    }

    template <typename team_t>
    __device__ __forceinline__ void* get_sym_ptr(void* ptr, int dst_rank) const {
        dst_rank = world_rank<team_t>(dst_rank);
        return device::mc_route_put(ctx, dst_rank, ptr);
    }

    template <typename team_t>
    __device__ __forceinline__ const void* get_sym_ptr(const void* ptr, int dst_rank) const {
        dst_rank = world_rank<team_t>(dst_rank);
        return device::mc_route_put(ctx, dst_rank, const_cast<void*>(ptr));
    }

    template <typename team_t>
    __device__ __forceinline__ void put(void* dst_ptr, const void* src_ptr,
                                        int num_bytes, int dst_rank,
                                        int /*flags*/ = 0) const {
        dst_rank = world_rank<team_t>(dst_rank);
        auto routed = device::mc_route_put(ctx, dst_rank, dst_ptr);
        if (routed != nullptr) {
            const auto src_addr = reinterpret_cast<uintptr_t>(src_ptr);
            const auto dst_addr = reinterpret_cast<uintptr_t>(routed);
            if (((src_addr | dst_addr | static_cast<uintptr_t>(num_bytes)) &
                 (sizeof(int4) - 1)) == 0) {
                const auto* src = reinterpret_cast<const int4*>(src_ptr);
                auto* dst = reinterpret_cast<int4*>(routed);
                const int num_int4 = num_bytes / static_cast<int>(sizeof(int4));
                for (int i = 0; i < num_int4; ++i) {
                    dst[i] = device::mc_ld_nc(src + i);
                }
            } else {
                auto* dst_bytes = reinterpret_cast<uint8_t*>(routed);
                const auto* src_bytes = reinterpret_cast<const uint8_t*>(src_ptr);
                for (int i = 0; i < num_bytes; ++i) {
                    dst_bytes[i] = src_bytes[i];
                }
            }
            // `put` is used both by full data-moving warps and by individual
            // notify lanes.  Do not place a full-warp barrier inside the
            // transport primitive: divergent notify calls would deadlock.  Each
            // participating lane copies the complete payload for its request,
            // so a system fence is sufficient to publish the writes.
            __threadfence_system();
        } else {
            device::mc_rdma_put(ctx, qp_idx, dst_rank, qps_per_rank, src_ptr,
                                dst_ptr, static_cast<uint32_t>(num_bytes), 0);
        }
    }

    template <typename team_t, typename value_t>
    __device__ __forceinline__ void put_value(value_t* dst_ptr, value_t value,
                                              int dst_rank, int flags = 0) const {
        dst_rank = world_rank<team_t>(dst_rank);
        auto* routed = static_cast<value_t*>(device::mc_route_put(ctx, dst_rank, dst_ptr));
        if (routed != nullptr) {
            if constexpr (sizeof(value_t) == sizeof(int32_t)) {
                device::mc_st_release(reinterpret_cast<int*>(routed),
                                      static_cast<int32_t>(value));
            } else {
                *routed = value;
                __threadfence_system();
            }
        } else {
            if constexpr (sizeof(value_t) == sizeof(int32_t)) {
                device::mc_signal(ctx, dst_rank, qp_idx, qps_per_rank,
                                  reinterpret_cast<int*>(dst_ptr),
                                  static_cast<int32_t>(value));
            } else {
                // Device RDMA WRITE sources must be registered GDR addresses;
                // a by-value scalar lives in thread-local storage and is not a
                // valid IBGDA source.  Current elastic uses remote int64
                // put_value only for single-writer, zeroed notify slots, so a
                // split 32-bit RED add is equivalent to writing the packed word.
                auto* words = reinterpret_cast<int32_t*>(dst_ptr);
                const auto signed_value = static_cast<int64_t>(value);
                const auto low = static_cast<int32_t>(
                    static_cast<uint64_t>(signed_value) & 0xffffffffull);
                const auto high = static_cast<int32_t>(signed_value >> 32);
                if ((flags & kRedAddReleaseLowWordLast) == 0) {
                    if (low != 0) {
                        device::mc_red_add(ctx, dst_rank, qp_idx, qps_per_rank,
                                           words, low);
                    }
                    if (high != 0) {
                        device::mc_red_add(ctx, dst_rank, qp_idx, qps_per_rank,
                                           words + 1, high);
                    }
                } else {
                    if (high != 0) {
                        device::mc_red_add(ctx, dst_rank, qp_idx, qps_per_rank,
                                           words + 1, high);
                    }
                    if (low != 0) {
                        device::mc_red_add(ctx, dst_rank, qp_idx, qps_per_rank,
                                           words, low);
                    }
                }
            }
        }
    }

    template <typename team_t, typename value_t>
    __device__ __forceinline__ void red_add_rel(value_t* dst_ptr, value_t value,
                                                int dst_rank, int flags = 0) const {
        if constexpr (sizeof(value_t) == sizeof(int32_t)) {
            dst_rank = world_rank<team_t>(dst_rank);
            auto* routed = static_cast<int*>(device::mc_route_put(ctx, dst_rank, dst_ptr));
            if (routed != nullptr) {
                device::mc_atomic_add_release(routed, static_cast<int>(value));
            } else {
                device::mc_red_add(ctx, dst_rank, qp_idx, qps_per_rank,
                                   reinterpret_cast<int*>(dst_ptr), static_cast<int32_t>(value));
            }
        } else if constexpr (sizeof(value_t) == sizeof(uint64_t) ||
                             sizeof(value_t) == sizeof(int64_t)) {
            dst_rank = world_rank<team_t>(dst_rank);
            auto* routed = static_cast<int64_t*>(
                device::mc_route_put(ctx, dst_rank, dst_ptr));
            if (routed != nullptr) {
                // Some official elastic paths use the high 32 bits as the
                // readiness word (notify counters), while others use the low 32
                // bits as the terminal flag (hybrid channel tails).  Splitting a
                // 64-bit RED into two 32-bit atomics can therefore publish the
                // wrong half first for one of the protocols.  Use one system-
                // scope 64-bit RED on the routed local/P2P VA so the packed
                // value is updated atomically with release ordering.
                ptx::red_add_rel_sys(routed, static_cast<int64_t>(value));
            } else {
                // Mooncake's current Device API only exposes 32-bit remote
                // reduction.  Do not emulate the 64-bit add with an RDMA WRITE
                // from a thread-local scalar: IBGDA WQEs use the registered GDR
                // buffer lkey, so a stack/local address is not a valid DMA
                // source on true cross-node runs.  Split the packed signal into
                // two 32-bit remote reductions instead, publishing the readiness
                // word last.  Most notify counters use high word as the ready
                // count; hybrid channel tails use low word as the finish flag.
                auto* words = reinterpret_cast<int32_t*>(dst_ptr);
                const auto signed_value = static_cast<int64_t>(value);
                const auto low = static_cast<int32_t>(
                    static_cast<uint64_t>(signed_value) & 0xffffffffull);
                const auto high = static_cast<int32_t>(signed_value >> 32);
                if ((flags & kRedAddReleaseLowWordLast) == 0) {
                    if (low != 0) {
                        device::mc_red_add(ctx, dst_rank, qp_idx, qps_per_rank,
                                           words, low);
                    }
                    if (high != 0) {
                        device::mc_red_add(ctx, dst_rank, qp_idx, qps_per_rank,
                                           words + 1, high);
                    }
                } else {
                    if (high != 0) {
                        device::mc_red_add(ctx, dst_rank, qp_idx, qps_per_rank,
                                           words + 1, high);
                    }
                    if (low != 0) {
                        device::mc_red_add(ctx, dst_rank, qp_idx, qps_per_rank,
                                           words, low);
                    }
                }
            }
        } else {
            put_value<team_t>(dst_ptr, value, dst_rank, flags);
        }
    }

    __device__ __forceinline__ void flush() const { __threadfence_system(); }
};

}  // namespace mooncake::elastic::transport
