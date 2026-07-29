#pragma once

#include <cstdint>
#include <type_traits>

#include <mooncake_ep_utils.cuh>
#include <elastic/mooncake_ep_elastic_launch.cuh>
#include <elastic/mooncake_ep_elastic_layout.cuh>
#include <elastic/mooncake_ep_elastic_ptx.cuh>
#include <transport/device/comm_device.cuh>
#ifdef USE_NCCL_DEVICE
#include <transport/device/nccl_device.cuh>
#endif

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
//   flush        -> no-op; Device API operations are ordered by release/fence
//   and
//                   explicit kernel barriers in the imported elastic kernels
//
// Team tags are kept as types so official DeepEP template code can remain close
// to the source while the actual routing is decided by Mooncake CommCtx.
struct IbgdaOps {
    using Context = device::CommCtx;
    static constexpr bool kIsNccl = false;
    device::CommCtx ctx;
    int qp_idx = 0;
    int sharing_mode = 0;
    int qps_per_rank = 1;
    int scaleout_rank_idx = 0;
    int scaleup_rank_idx = 0;
    int num_scaleup_ranks = 0;

    __device__ __forceinline__ IbgdaOps(const device::CommCtx& ctx, int qp_idx,
                                        int sharing_mode, int num_qps,
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
        return dst_rank == ctx.rank ||
               device::mc_comm_p2p_available(ctx, dst_rank);
    }

    template <typename team_t>
    __device__ __forceinline__ void* get_sym_ptr(void* ptr,
                                                 int dst_rank) const {
        dst_rank = world_rank<team_t>(dst_rank);
        return device::mc_route_put(ctx, dst_rank, ptr);
    }

    template <typename team_t>
    __device__ __forceinline__ const void* get_sym_ptr(const void* ptr,
                                                       int dst_rank) const {
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
#ifdef MOONCAKE_EP_USE_MUSA
                    ptx::st_na(dst + i, device::mc_ld_nc(src + i));
#else
                    dst[i] = device::mc_ld_nc(src + i);
#endif
                }
            } else {
                auto* dst_bytes = reinterpret_cast<uint8_t*>(routed);
                const auto* src_bytes =
                    reinterpret_cast<const uint8_t*>(src_ptr);
                for (int i = 0; i < num_bytes; ++i) {
#ifdef MOONCAKE_EP_USE_MUSA
                    reinterpret_cast<volatile uint8_t*>(dst_bytes)[i] =
                        reinterpret_cast<const volatile uint8_t*>(src_bytes)[i];
#else
                    dst_bytes[i] = src_bytes[i];
#endif
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
                                              int dst_rank,
                                              int flags = 0) const {
        dst_rank = world_rank<team_t>(dst_rank);
        auto* routed =
            static_cast<value_t*>(device::mc_route_put(ctx, dst_rank, dst_ptr));
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
                // split 32-bit RED add is equivalent to writing the packed
                // word.
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
                                                int dst_rank,
                                                int flags = 0) const {
        if constexpr (sizeof(value_t) == sizeof(int32_t)) {
            dst_rank = world_rank<team_t>(dst_rank);
            auto* routed =
                static_cast<int*>(device::mc_route_put(ctx, dst_rank, dst_ptr));
            if (routed != nullptr) {
                device::mc_atomic_add_release(routed, static_cast<int>(value));
            } else {
                device::mc_red_add(ctx, dst_rank, qp_idx, qps_per_rank,
                                   reinterpret_cast<int*>(dst_ptr),
                                   static_cast<int32_t>(value));
            }
        } else if constexpr (sizeof(value_t) == sizeof(uint64_t) ||
                             sizeof(value_t) == sizeof(int64_t)) {
            dst_rank = world_rank<team_t>(dst_rank);
            auto* routed = static_cast<int64_t*>(
                device::mc_route_put(ctx, dst_rank, dst_ptr));
            if (routed != nullptr) {
                // Some official elastic paths use the high 32 bits as the
                // readiness word (notify counters), while others use the low 32
                // bits as the terminal flag (hybrid channel tails).  Splitting
                // a 64-bit RED into two 32-bit atomics can therefore publish
                // the wrong half first for one of the protocols.  Use one
                // system- scope 64-bit RED on the routed local/P2P VA so the
                // packed value is updated atomically with release ordering.
                ptx::red_add_rel_sys(routed, static_cast<int64_t>(value));
            } else {
                // Mooncake's current Device API only exposes 32-bit remote
                // reduction.  Do not emulate the 64-bit add with an RDMA WRITE
                // from a thread-local scalar: IBGDA WQEs use the registered GDR
                // buffer lkey, so a stack/local address is not a valid DMA
                // source on true cross-node runs.  Split the packed signal into
                // two 32-bit remote reductions instead, publishing the
                // readiness word last.  Most notify counters use high word as
                // the ready count; hybrid channel tails use low word as the
                // finish flag.
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

    template <typename team_t>
    __device__ __forceinline__ void reset_completion_tail(
        int /*channel_idx*/, int /*sender_rank*/) const {}

    template <typename team_t, typename value_t>
    __device__ __forceinline__ value_t read_completion_tail(
        value_t* ptr, int /*channel_idx*/, int /*sender_rank*/) const {
        return ptx::ld_acquire_sys<value_t>(ptr);
    }

    template <typename team_t, typename value_t>
    __device__ __forceinline__ void clear_completion_tail(
        value_t* ptr, int /*channel_idx*/, int /*sender_rank*/) const {
        *ptr = 0;
    }

    template <typename team_t, typename value_t>
    __device__ __forceinline__ void publish_tail(value_t* dst_ptr,
                                                 int channel_idx,
                                                 value_t absolute_value,
                                                 value_t delta, int dst_rank,
                                                 int flags = 0) const {
        (void)channel_idx;
        (void)absolute_value;
        red_add_rel<team_t>(dst_ptr, delta, dst_rank, flags);
    }

    __device__ __forceinline__ void flush() const { __threadfence_system(); }
};

#ifdef USE_NCCL_DEVICE

// NCCL implementation of the same compile-time kernel surface as IbgdaOps.
// Team-to-world-rank translation is arithmetic; LSA peers use direct symmetric
// pointers, while non-LSA peers use NCCL GIN on the full world team.
struct NcclOps {
    using Context = NcclContext;
    static constexpr bool kIsNccl = true;

    Context ctx;
    int qp_idx = 0;
    int sharing_mode = 0;
    int qps_per_rank = 1;
    int scaleout_rank_idx = 0;
    int scaleup_rank_idx = 0;
    int num_scaleup_ranks = 0;
    int num_ranks = 1;

    __device__ __forceinline__ NcclOps(const Context& ctx, int qp_idx,
                                       int sharing_mode, int num_qps,
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
          num_scaleup_ranks(num_scaleup_ranks),
          num_ranks(max(1, num_ranks)) {}

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

    __device__ __forceinline__ bool is_lsa_world_rank(int peer) const {
        return peer >= ctx.lsa_first_rank &&
               peer < ctx.lsa_first_rank + ctx.lsa_size;
    }

    template <typename team_t>
    __device__ __forceinline__ bool is_direct_world_rank(int peer) const {
        if constexpr (std::is_same_v<team_t, ScaleupTeam>) {
            return true;
        } else if constexpr (std::is_same_v<team_t, ScaleoutTeam>) {
            return peer == ctx.world_rank;
        } else {
            return is_lsa_world_rank(peer);
        }
    }

    template <typename team_t>
    __device__ __forceinline__ bool is_nvlink_accessible(int dst_rank) const {
        return is_direct_world_rank<team_t>(world_rank<team_t>(dst_rank));
    }

    template <typename team_t>
    __device__ __forceinline__ bool is_gin_peer(int dst_rank) const {
        const int peer = world_rank<team_t>(dst_rank);
        return peer != ctx.world_rank && !is_direct_world_rank<team_t>(peer);
    }

    template <typename team_t>
    __device__ __forceinline__ void* get_sym_ptr(void* ptr,
                                                 int dst_rank) const {
        const int peer = world_rank<team_t>(dst_rank);
        if (!is_direct_world_rank<team_t>(peer)) return nullptr;
        return device::mc_nccl_peer_ptr(ctx.device, peer, ptr);
    }

    template <typename team_t>
    __device__ __forceinline__ const void* get_sym_ptr(const void* ptr,
                                                       int dst_rank) const {
        const int peer = world_rank<team_t>(dst_rank);
        if (!is_direct_world_rank<team_t>(peer)) return nullptr;
        return device::mc_nccl_peer_ptr(ctx.device, peer, ptr);
    }

    template <typename team_t>
    __device__ __forceinline__ void put(void* dst_ptr, const void* src_ptr,
                                        int num_bytes, int dst_rank,
                                        int /*flags*/ = 0) const {
        dst_rank = world_rank<team_t>(dst_rank);
        if (is_direct_world_rank<team_t>(dst_rank)) {
            auto* routed = static_cast<uint8_t*>(
                device::mc_nccl_peer_ptr(ctx.device, dst_rank, dst_ptr));
            const auto src_addr = reinterpret_cast<uintptr_t>(src_ptr);
            const auto dst_addr = reinterpret_cast<uintptr_t>(routed);
            if (((src_addr | dst_addr | static_cast<uintptr_t>(num_bytes)) &
                 (sizeof(int4) - 1)) == 0) {
                const auto* src = reinterpret_cast<const int4*>(src_ptr);
                auto* dst = reinterpret_cast<int4*>(routed);
                const int count = num_bytes / static_cast<int>(sizeof(int4));
                for (int i = 0; i < count; ++i)
                    dst[i] = device::mc_ld_nc(src + i);
            } else {
                const auto* src = static_cast<const uint8_t*>(src_ptr);
                for (int i = 0; i < num_bytes; ++i) routed[i] = src[i];
            }
            __threadfence_system();
        } else {
            // These kernels elect the issuing thread before bulk puts, while
            // notify paths intentionally issue one independent request per
            // calling lane. Passing zero selects every such caller.
            device::mc_nccl_put(ctx.device, qp_idx, dst_rank, qps_per_rank,
                                src_ptr, dst_ptr,
                                static_cast<uint32_t>(num_bytes), 0);
        }
    }

    template <typename team_t, typename value_t>
    __device__ __forceinline__ void put_value(value_t* dst_ptr, value_t value,
                                              int dst_rank,
                                              int /*flags*/ = 0) const {
        static_assert(
            std::is_scalar_v<value_t> && std::is_trivially_copyable_v<value_t>,
            "NCCL EP put_value requires a trivially copyable scalar");
        static_assert(sizeof(value_t) == sizeof(uint32_t) ||
                          sizeof(value_t) == sizeof(uint64_t),
                      "NCCL EP put_value supports only 4- or 8-byte values");
        dst_rank = world_rank<team_t>(dst_rank);
        if (is_direct_world_rank<team_t>(dst_rank)) {
            auto* routed = static_cast<value_t*>(
                device::mc_nccl_peer_ptr(ctx.device, dst_rank, dst_ptr));
            ptx::st_release_sys(routed, value);
        } else {
            device::mc_nccl_put_value(ctx.device, qp_idx, dst_rank,
                                      qps_per_rank, dst_ptr, value, 0);
        }
    }

    template <typename team_t, typename value_t>
    __device__ __forceinline__ void red_add_rel(value_t* dst_ptr, value_t value,
                                                int dst_rank,
                                                int flags = 0) const {
        (void)flags;
        dst_rank = world_rank<team_t>(dst_rank);
        auto* routed = static_cast<value_t*>(
            device::mc_nccl_peer_ptr(ctx.device, dst_rank, dst_ptr));
        if constexpr (sizeof(value_t) == sizeof(int32_t)) {
            device::mc_atomic_add_release(reinterpret_cast<int*>(routed),
                                          static_cast<int>(value));
        } else if constexpr (sizeof(value_t) == sizeof(uint64_t) ||
                             sizeof(value_t) == sizeof(int64_t)) {
            ptx::red_add_rel_sys(reinterpret_cast<int64_t*>(routed),
                                 static_cast<int64_t>(value));
        } else {
            *routed = value;
            __threadfence_system();
        }
    }

    __device__ __forceinline__ uint64_t* completion_tail_ptr(
        int channel_idx, int sender_world_rank) const {
        return layout::GinSignalLayout(ctx.gin_signal_base, num_ranks,
                                       ctx.gin_context_count)
            .get_completion_tail_ptr(channel_idx, sender_world_rank);
    }

    template <typename team_t>
    __device__ __forceinline__ void reset_completion_tail(
        int channel_idx, int sender_rank) const {
        if (!is_gin_peer<team_t>(sender_rank)) return;
        const int sender_world_rank = world_rank<team_t>(sender_rank);
        device::mc_nccl_gin_reset_signal(
            ctx.device, qp_idx,
            completion_tail_ptr(channel_idx, sender_world_rank), 0);
    }

    template <typename team_t, typename value_t>
    __device__ __forceinline__ value_t read_completion_tail(
        value_t* direct_ptr, int channel_idx, int sender_rank) const {
        static_assert(sizeof(value_t) == sizeof(uint64_t),
                      "NCCL completion tails must be 64-bit");
        if (!is_gin_peer<team_t>(sender_rank))
            return ptx::ld_acquire_sys<value_t>(direct_ptr);
        const int sender_world_rank = world_rank<team_t>(sender_rank);
        return static_cast<value_t>(device::mc_nccl_gin_read_signal(
            ctx.device, qp_idx,
            completion_tail_ptr(channel_idx, sender_world_rank)));
    }

    template <typename team_t, typename value_t>
    __device__ __forceinline__ void clear_completion_tail(
        value_t* direct_ptr, int /*channel_idx*/, int sender_rank) const {
        if (!is_gin_peer<team_t>(sender_rank)) *direct_ptr = 0;
    }

    template <typename team_t, typename value_t>
    __device__ __forceinline__ void publish_tail(value_t* dst_ptr,
                                                 int channel_idx,
                                                 value_t absolute_value,
                                                 value_t delta, int dst_rank,
                                                 int flags = 0) const {
        static_assert(sizeof(value_t) == sizeof(uint64_t),
                      "NCCL completion tails must be 64-bit");
        const int dst_world_rank = world_rank<team_t>(dst_rank);
        if (is_direct_world_rank<team_t>(dst_world_rank)) {
            put_value<team_t>(dst_ptr, absolute_value, dst_rank, flags);
            return;
        }

        // A strong signal is the NCCL Device API completion primitive: once
        // the receiver observes this delta, every preceding payload put to the
        // same peer on this context is settled. The signal lives in GIN-only
        // storage and mirrors the ordinary direct-path tail value.
        device::mc_nccl_gin_signal_add(
            ctx.device, dst_world_rank, qp_idx, qps_per_rank,
            completion_tail_ptr(channel_idx, ctx.world_rank),
            static_cast<uint64_t>(delta), 0);
    }

    __device__ __forceinline__ uint64_t* gin_signal_ptr(int num_ranks, int tag,
                                                        int sender_rank,
                                                        int context_idx) const {
        return layout::GinSignalLayout(ctx.gin_signal_base, num_ranks,
                                       ctx.gin_context_count)
            .get_signal_ptr(tag, sender_rank, context_idx);
    }

    __device__ __forceinline__ void gin_signal_add(int dst_world_rank,
                                                   int context_idx,
                                                   uint64_t* signal_ptr,
                                                   uint64_t value) const {
        device::mc_nccl_gin_signal_add(ctx.device, dst_world_rank, context_idx,
                                       qps_per_rank, signal_ptr, value, 0);
    }

    __device__ __forceinline__ uint64_t
    gin_read_signal(int context_idx, const uint64_t* signal_ptr) const {
        return device::mc_nccl_gin_read_signal(ctx.device, context_idx,
                                               signal_ptr);
    }

    __device__ __forceinline__ void gin_wait_signal(int context_idx,
                                                    const uint64_t* signal_ptr,
                                                    uint64_t epoch) const {
        device::mc_nccl_gin_wait_signal(ctx.device, context_idx, signal_ptr,
                                        epoch, 0);
    }

    __device__ __forceinline__ void flush() const {
        if (blockIdx.x == 0 &&
            static_cast<int>(threadIdx.x) < ctx.gin_context_count) {
            device::mc_nccl_flush(ctx.device, static_cast<int>(threadIdx.x), 0);
        }
    }
};

#endif  // USE_NCCL_DEVICE

}  // namespace mooncake::elastic::transport
