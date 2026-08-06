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
    static constexpr int kNumQPs = MAX_QP_COUNT;
    static constexpr int kAggregateRequests = 0;
#ifdef MOONCAKE_EP_USE_MUSA
    static constexpr int kNumDispatchWarps = 4;
    static constexpr int kNumDispatchEpilogueWarps = 4;
    static constexpr int kNumCombineWarps = 4;
    static constexpr int kNumCombineEpilogueWarps = 4;
#else
    static constexpr int kNumDispatchWarps = 8;
    static constexpr int kNumDispatchEpilogueWarps = 8;
    static constexpr int kNumCombineWarps = 8;
    static constexpr int kNumCombineEpilogueWarps = 8;
#endif
    static constexpr int kNumHybridScaleoutWarps = 4;
    static constexpr int kNumHybridForwardWarps = 4;
    static constexpr int kNumHybridScaleupWarps = 4;
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
// LSA peers use direct symmetric pointers. Hybrid scale-out uses NCCL's rail
// team, while non-hybrid GIN operations use the full world team.
struct NcclOps {
    using Context = NcclContext;
    static constexpr bool kIsNccl = true;
    static constexpr int kNumQPs = 65;
    static constexpr int kAggregateRequests =
        ncclGinOptFlagsAggregateRequests;
    static constexpr int kNumDispatchWarps = 27;
    static constexpr int kNumDispatchEpilogueWarps = 27;
    static constexpr int kNumCombineWarps = 28;
    static constexpr int kNumCombineEpilogueWarps = 28;
    static constexpr int kNumHybridScaleoutWarps = 8;
    static constexpr int kNumHybridForwardWarps = 8;
    static constexpr int kNumHybridScaleupWarps = 8;

    // Keep this adapter compact: it is captured by several kernel lambdas.
    // Team/rank/window state already lives in the pre-bound handle, so retaining
    // a second NcclContext here would create per-thread local-memory spills.
    device::NcclGinHandle gin;

    __device__ __forceinline__ NcclOps(
        const Context& ctx, int qp_idx, int sharing_mode, int /*num_qps*/,
        int /*scaleout_rank_idx*/ = 0, int /*scaleup_rank_idx*/ = 0,
        int /*num_scaleup_ranks*/ = 0, int /*num_ranks*/ = 1)
        : gin(ctx.device, static_cast<unsigned int>(qp_idx),
              sharing_mode == 0 ? device::NcclGinResourceSharing::kCta
                                : device::NcclGinResourceSharing::kGpu) {}

    template <typename team_t>
    __device__ __forceinline__ bool is_nvlink_accessible(int dst_rank) const {
        if constexpr (std::is_same_v<team_t, ScaleupTeam>) {
            return true;
        } else if constexpr (std::is_same_v<team_t, ScaleoutTeam>) {
            return gin.railRank() == dst_rank;
        } else {
            return gin.worldRankInLsa(dst_rank);
        }
    }

    template <typename team_t>
    __device__ __forceinline__ bool is_gin_peer(int dst_rank) const {
        return !is_nvlink_accessible<team_t>(dst_rank);
    }

    template <typename team_t, typename ptr_t>
    __device__ __forceinline__ ptr_t* get_sym_ptr_impl(
        ptr_t* ptr, int dst_rank) const {
        if constexpr (std::is_same_v<team_t, ScaleupTeam>) {
            return static_cast<ptr_t*>(gin.lsaPeerPointer(dst_rank, ptr));
        } else if constexpr (std::is_same_v<team_t, ScaleoutTeam>) {
            return gin.railRank() == dst_rank ? ptr : nullptr;
        } else {
            if (!gin.worldRankInLsa(dst_rank)) return nullptr;
            return static_cast<ptr_t*>(gin.worldPeerPointer(dst_rank, ptr));
        }
    }

    template <typename team_t>
    __device__ __forceinline__ void* get_sym_ptr(void* ptr,
                                                 int dst_rank) const {
        return get_sym_ptr_impl<team_t>(static_cast<uint8_t*>(ptr), dst_rank);
    }

    template <typename team_t>
    __device__ __forceinline__ const void* get_sym_ptr(const void* ptr,
                                                       int dst_rank) const {
        return get_sym_ptr_impl<team_t>(static_cast<const uint8_t*>(ptr),
                                        dst_rank);
    }

    template <typename team_t>
    __device__ __forceinline__ void put(void* dst_ptr, const void* src_ptr,
                                        int num_bytes, int dst_rank,
                                        int flags = 0) const {
        if constexpr (std::is_same_v<team_t, ScaleupTeam>) {
            auto* routed = static_cast<uint8_t*>(
                get_sym_ptr<team_t>(dst_ptr, dst_rank));
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
        } else if constexpr (std::is_same_v<team_t, ScaleoutTeam>) {
            // Match NCCL's native GIN contract: world/rail puts always use the
            // selected network context. Callers use get_sym_ptr explicitly
            // when they want an LSA/local bypass.
            gin.put<device::NcclGinTeam::kRail>(
                dst_rank, src_ptr, dst_ptr, static_cast<uint32_t>(num_bytes),
                static_cast<uint32_t>(flags));
        } else {
            gin.put<device::NcclGinTeam::kWorld>(
                dst_rank, src_ptr, dst_ptr, static_cast<uint32_t>(num_bytes),
                static_cast<uint32_t>(flags));
        }
    }

    template <typename team_t, typename value_t>
    __device__ __forceinline__ void put_value(value_t* dst_ptr, value_t value,
                                              int dst_rank,
                                              int flags = 0) const {
        static_assert(
            std::is_scalar_v<value_t> && std::is_trivially_copyable_v<value_t>,
            "NCCL EP put_value requires a trivially copyable scalar");
        static_assert(sizeof(value_t) == sizeof(uint32_t) ||
                          sizeof(value_t) == sizeof(uint64_t),
                      "NCCL EP put_value supports only 4- or 8-byte values");
        auto* routed = get_sym_ptr_impl<team_t>(dst_ptr, dst_rank);
        if (routed != nullptr) {
            ptx::st_relaxed_sys(routed, value);
        } else if constexpr (std::is_same_v<team_t, ScaleoutTeam>) {
            gin.putValue<device::NcclGinTeam::kRail>(
                dst_rank, dst_ptr, value, static_cast<uint32_t>(flags));
        } else {
            gin.putValue<device::NcclGinTeam::kWorld>(
                dst_rank, dst_ptr, value, static_cast<uint32_t>(flags));
        }
    }

    template <typename team_t, typename value_t>
    __device__ __forceinline__ void red_add_rel(value_t* dst_ptr, value_t value,
                                                int dst_rank,
                                                int /*flags*/ = 0) const {
        auto* routed = get_sym_ptr_impl<team_t>(dst_ptr, dst_rank);
        if (routed != nullptr) {
            const bool use_gpu_scope =
                std::is_same_v<team_t, ScaleoutTeam> || routed == dst_ptr;
            if constexpr (sizeof(value_t) == sizeof(int32_t)) {
                if (use_gpu_scope) {
                    ptx::red_add_rel_gpu(reinterpret_cast<int*>(routed),
                                         static_cast<int>(value));
                } else {
                    ptx::red_add_rel_sys(reinterpret_cast<int*>(routed),
                                         static_cast<int>(value));
                }
            } else if constexpr (sizeof(value_t) == sizeof(uint64_t) ||
                                 sizeof(value_t) == sizeof(int64_t)) {
                if (use_gpu_scope) {
                    ptx::red_add_rel_gpu(reinterpret_cast<int64_t*>(routed),
                                         static_cast<int64_t>(value));
                } else {
                    ptx::red_add_rel_sys(reinterpret_cast<int64_t*>(routed),
                                         static_cast<int64_t>(value));
                }
            }
        } else if constexpr (sizeof(value_t) == sizeof(uint64_t) ||
                             sizeof(value_t) == sizeof(int64_t)) {
            if constexpr (std::is_same_v<team_t, ScaleoutTeam>) {
                gin.signalAdd<device::NcclGinTeam::kRail>(
                    dst_rank, reinterpret_cast<uint64_t*>(dst_ptr),
                    static_cast<uint64_t>(value));
            } else {
                gin.signalAdd<device::NcclGinTeam::kWorld>(
                    dst_rank, reinterpret_cast<uint64_t*>(dst_ptr),
                    static_cast<uint64_t>(value));
            }
        }
    }

    template <typename team_t, typename value_t>
    __device__ __forceinline__ value_t read_completion_tail(
        value_t* direct_ptr, int /*channel_idx*/, int /*sender_rank*/) const {
        static_assert(sizeof(value_t) == sizeof(uint64_t),
                      "NCCL completion tails must be 64-bit");
        // NCCL's VA-signal read path resolves this same local window pointer
        // and performs an acquire atomic load. Keep that operation explicit so
        // the compiler does not instantiate unrelated runtime backend paths.
        return ptx::ld_acquire_sys<value_t>(direct_ptr);
    }

    template <typename team_t, typename value_t>
    __device__ __forceinline__ void clear_completion_tail(
        value_t* direct_ptr, int /*channel_idx*/, int /*sender_rank*/) const {
        // Every NCCL backend resets a VA signal by storing zero to its local
        // window address. The tail protocol guarantees that remote writers are
        // quiescent before this cleanup.
        *direct_ptr = 0;
    }

    template <typename team_t, typename value_t>
    __device__ __forceinline__ void publish_tail(value_t* dst_ptr,
                                                 int /*channel_idx*/,
                                                 value_t absolute_value,
                                                 value_t delta, int dst_rank,
                                                 int flags = 0) const {
        static_assert(sizeof(value_t) == sizeof(uint64_t),
                      "NCCL completion tails must be 64-bit");
        (void)absolute_value;
        red_add_rel<team_t>(dst_ptr, delta, dst_rank, flags);
    }

    template <typename team_t>
    __device__ __forceinline__ void gin_barrier_signal_inc(
        int dst_team_rank, int signal_id) const {
        if constexpr (std::is_same_v<team_t, ScaleoutTeam>) {
            gin.signalIncContext0<device::NcclGinTeam::kRail>(dst_team_rank,
                                                              signal_id);
        } else {
            gin.signalIncContext0<device::NcclGinTeam::kWorld>(dst_team_rank,
                                                               signal_id);
        }
    }

    __device__ __forceinline__ uint64_t gin_barrier_advance_shadow(
        int signal_id) const {
        return gin.advanceSignalShadowContext0(signal_id);
    }

    __device__ __forceinline__ uint64_t gin_barrier_read_signal(
        int signal_id) const {
        return gin.readSignalContext0(signal_id);
    }

    __device__ __forceinline__ void flush_channel() const { gin.flushWarp(); }

    __device__ __forceinline__ void flush() const {
        const int warps_per_block = static_cast<int>(blockDim.x) / warpSize;
        const int global_warp =
            static_cast<int>(blockIdx.x) * warps_per_block +
            static_cast<int>(threadIdx.x) / warpSize;
        const int num_warps = static_cast<int>(gridDim.x) * warps_per_block;
        for (int context_idx = global_warp; context_idx < gin.contextCount();
             context_idx += num_warps) {
            gin.flushContextWarp(context_idx);
        }
    }
};

#endif  // USE_NCCL_DEVICE

}  // namespace mooncake::elastic::transport
