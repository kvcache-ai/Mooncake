#pragma once

#include <cooperative_groups.h>
#include <utility>

#include <elastic/mooncake_ep_elastic_layout.cuh>
#include <elastic/mooncake_ep_elastic_ptx.cuh>
#include <elastic/mooncake_ep_elastic_transport.cuh>

namespace mooncake::elastic::comm {

static constexpr int64_t kNumOneSecCycles = 2000000000;

static constexpr int kDeviceBarrierTag = 0;
static constexpr int kKernelBarrierTag = 1;
static constexpr int kDispatchTag0 = 2;
static constexpr int kDispatchTag1 = 3;
static constexpr int kCombineTag0 = 4;
static constexpr int kCombineTag1 = 5;
static constexpr int kHybridDispatchTag0 = 6;
static constexpr int kHybridDispatchTag1 = 7;
static constexpr int kHybridCombineTag0 = 8;
static constexpr int kHybridCombineTag1 = 9;

static constexpr int kFlushAllAllocatedQPs = -1;

template <int64_t kNumTimeoutCycles, typename func_t>
__device__ __forceinline__ void timeout_while(const bool& condition,
                                              const func_t& func,
                                              int64_t start_clock = 0) {
    if (start_clock == 0) start_clock = clock64();
    while (condition) {
        const bool timeout = kNumTimeoutCycles >= 0 &&
                             (clock64() - start_clock >= kNumTimeoutCycles);
        if (func(timeout)) break;
        if (timeout) {
            const auto timeout_start = clock64();
            while (clock64() - timeout_start < kNumOneSecCycles) {
            }
            ptx::trap();
        }
    }
}

template <int64_t kNumTimeoutCycles, typename func_t>
__device__ __forceinline__ void timeout_while(const func_t& func,
                                              const int64_t& start_clock = 0) {
    timeout_while<kNumTimeoutCycles, func_t>(true, func, start_clock);
}

template <int kNumSMs, int kNumThreads, int64_t kNumTimeoutCycles>
__forceinline__ __device__ void local_grid_sync(
    const layout::WorkspaceLayout& workspace, const int& thread_idx) {
#ifdef MOONCAKE_EP_USE_MUSA
    (void)kNumThreads;
    __shared__ unsigned long long ticket;
    __syncthreads();
    if (thread_idx == 0) {
        ticket = atomicAdd(
            workspace.get_nvl_barrier_counter_ptr(kKernelBarrierTag), 1ULL);
    }
    __syncthreads();
    const auto target = ((ticket / kNumSMs) + 1ULL) * kNumSMs;
    timeout_while<kNumTimeoutCycles>(thread_idx == 0, [=](const bool&) {
        return ptx::ld_volatile<unsigned long long>(
                   workspace.get_nvl_barrier_counter_ptr(kKernelBarrierTag)) >=
               target;
    });
    __syncthreads();
#else
    (void)workspace;
    (void)thread_idx;
    (gridDim.x > 1) ? cooperative_groups::this_grid().sync() : __syncthreads();
#endif
}

template <int kNumSMs, int kNumQPs, int kNumChannelsPerSM,
          bool kWithNotifyWarps = false>
__device__ __forceinline__ std::pair<int, int> get_qp_mode(
    const int& sm_idx, const int& channel_in_sm_idx,
    const bool& is_notify_warp = false) {
    if constexpr (kNumQPs == 1) return {0, 1};
    if (is_notify_warp) return {0, 0};

    constexpr int kQPStartIdx = static_cast<int>(kWithNotifyWarps);
    constexpr int kNumAvailableQPs = kNumQPs - kQPStartIdx;
    if constexpr (kNumSMs <= kNumAvailableQPs) {
        const int num_qps_in_sm = (kNumAvailableQPs / kNumSMs) +
                                  (sm_idx < (kNumAvailableQPs % kNumSMs));
        return {kQPStartIdx + sm_idx +
                    (channel_in_sm_idx % max(1, num_qps_in_sm)) * kNumSMs,
                0};
    } else {
        const auto global_channel_idx =
            sm_idx * kNumChannelsPerSM + channel_in_sm_idx;
        return {kQPStartIdx + (global_channel_idx % max(1, kNumAvailableQPs)),
                1};
    }
}

template <typename Ops, typename team_t, int kNumRanks, int kNumSMs,
          int kNumThreads, int64_t kNumTimeoutCycles,
          int kTag = kDeviceBarrierTag>
__forceinline__ __device__ void mooncake_barrier_wo_local_sync(
    const Ops& gin, const layout::WorkspaceLayout& workspace,
    const int& rank_idx, const int& sm_idx, const int& thread_idx) {
    if (kNumSMs > 1 && sm_idx > 0) return;

    const int status =
        static_cast<int>((*workspace.get_nvl_barrier_counter_ptr(kTag)) & 3);
    const int phase = status & 1;
    const int sign = status >> 1;
    const int* base_signal = workspace.get_nvl_barrier_signal_ptr(kTag, phase);

    if (thread_idx < kNumRanks) {
        auto* dst_ptr = const_cast<int*>(base_signal) + rank_idx;
        gin.template red_add_rel<team_t>(dst_ptr, sign ? -1 : 1, thread_idx);
    }
    __syncthreads();

    if (thread_idx == 0)
        atomicAdd(workspace.get_nvl_barrier_counter_ptr(kTag), 1ULL);

    timeout_while<kNumTimeoutCycles>(
        thread_idx == 0, [=](const bool& is_last_check) {
            int sum = 0;
#pragma unroll
            for (int i = 0; i < kNumRanks; ++i) {
                sum +=
                    ptx::ld_acquire_sys<int>(const_cast<int*>(base_signal) + i);
            }
            // Mooncake's portable barrier uses one additive slot per source
            // rank. Each positive phase adds +1 into a zeroed phase slot; the
            // matching negative phase later adds -1 into the same phase slot.
            // This matches RDMA atomic-add semantics and avoids relying on a
            // remote store primitive for non-P2P peers.
            const auto target = sign ? 0 : kNumRanks;
            if (sum == target) return true;
            if (is_last_check) {
                printf(
                    "Mooncake elastic barrier timeout, tag: %d, rank: %d, "
                    "signal-sum: %d, target: %d\n",
                    kTag, rank_idx, sum, target);
            }
            return false;
        });
}

#ifdef USE_NCCL_DEVICE

// GIN signal words are NCCL-owned monotonic tails. Every sender advances its
// own slot once on every active GIN context, and every receiver waits for the
// matching local epoch. No kernel load/store resets these signal words.
template <typename team_t, int kNumTeamRanks, int kNumWorldRanks,
          int kNumThreads, int64_t kNumTimeoutCycles,
          int kTag = kDeviceBarrierTag>
__forceinline__ __device__ void nccl_gin_barrier_wo_local_sync(
    const transport::NcclOps& gin, const layout::WorkspaceLayout& workspace,
    const int& team_rank_idx, const int& barrier_sm_idx,
    const int& thread_idx) {
    if (barrier_sm_idx != 0) return;
    (void)team_rank_idx;

    const int num_contexts = gin.ctx.gin_context_count;
    const int sender_world_rank = gin.ctx.world_rank;

    // Publish arrival to each non-LSA peer on every context. The full-world
    // NCCL GIN team is used even when team_t describes one scale-out rail.
    const int num_signal_items = kNumTeamRanks * num_contexts;
    for (int item = thread_idx; item < num_signal_items; item += kNumThreads) {
        const int dst_team_rank = item / num_contexts;
        const int context_idx = item % num_contexts;
        const int dst_world_rank = gin.world_rank<team_t>(dst_team_rank);
        if (dst_world_rank != sender_world_rank &&
            !gin.is_lsa_world_rank(dst_world_rank)) {
            auto* signal_ptr = gin.gin_signal_ptr(
                kNumWorldRanks, kTag, sender_world_rank, context_idx);
            gin.gin_signal_add(dst_world_rank, context_idx, signal_ptr, 1);
        }
    }
    __syncthreads();

    auto* barrier_epoch = workspace.get_gin_barrier_epoch_ptr(kTag);
    if (thread_idx == 0) {
        atomicAdd(reinterpret_cast<unsigned long long*>(barrier_epoch), 1ULL);
    }
    __syncthreads();
    uint64_t target_epoch = 0;
    if ((thread_idx & (warpSize - 1)) == 0) {
        target_epoch = ptx::ld_acquire_sys<uint64_t>(barrier_epoch);
    }
    target_epoch = __shfl_sync(0xffffffffU, target_epoch, 0);

    const int num_wait_items = kNumTeamRanks * num_contexts;
    for (int item = thread_idx; item < num_wait_items; item += kNumThreads) {
        const int src_team_rank = item / num_contexts;
        const int context_idx = item % num_contexts;
        const int src_world_rank = gin.world_rank<team_t>(src_team_rank);
        if (src_world_rank == sender_world_rank ||
            gin.is_lsa_world_rank(src_world_rank))
            continue;

        const auto* signal_ptr = gin.gin_signal_ptr(
            kNumWorldRanks, kTag, src_world_rank, context_idx);
        timeout_while<kNumTimeoutCycles>([=](const bool& is_last_check) {
            const uint64_t observed =
                gin.gin_read_signal(context_idx, signal_ptr);
            if (observed >= target_epoch) return true;
            if (is_last_check) {
                printf(
                    "Mooncake NCCL GIN barrier timeout, tag: %d, rank: %d, "
                    "source: %d, context: %d, signal: %llu, target: %llu\n",
                    kTag, sender_world_rank, src_world_rank, context_idx,
                    static_cast<unsigned long long>(observed),
                    static_cast<unsigned long long>(target_epoch));
            }
            return false;
        });
    }
    __syncthreads();
}

#endif  // USE_NCCL_DEVICE

template <typename Ops, bool kIsScaleupNVLink, int kNumScaleoutRanks,
          int kNumScaleupRanks, int kNumSMs, int kNumThreads, int kNumQPs,
          int64_t kNumTimeoutCycles, int kTag = kDeviceBarrierTag,
          bool kFlushStores = true, bool kSyncAtStart = true,
          bool kSyncAtEnd = true>
__forceinline__ __device__ void gpu_barrier(
    const Ops& gin, const layout::WorkspaceLayout& workspace,
    const int& scaleout_rank_idx, const int& scaleup_rank_idx,
    const int& sm_idx, const int& thread_idx, bool do_scaleout = true,
    bool do_scaleup = true) {
    // Complete TMA stores before publishing any remote arrival. This is also
    // required for LSA writes because TMA uses a separate async proxy.
    if constexpr (Ops::kIsNccl && kFlushStores) {
        ptx::tma_store_commit();
        ptx::tma_store_wait();
        __syncwarp();
    }

    if constexpr (!Ops::kIsNccl) {
        // Preserve IBGDA's per-thread system fence before the leading grid
        // synchronization. Every producer must publish before block 0 can
        // announce the remote barrier.
        if constexpr (kFlushStores) gin.flush();
        if constexpr (kSyncAtStart) {
            local_grid_sync<kNumSMs, kNumThreads, kNumTimeoutCycles>(
                workspace, thread_idx);
        }
    } else {
        // Quiesce every block before one block flushes the shared NCCL GIN
        // contexts. Without this ordering, block 0 could flush while another
        // block is still posting puts and source buffers could be reused early.
        if constexpr (kSyncAtStart) {
            local_grid_sync<kNumSMs, kNumThreads, kNumTimeoutCycles>(
                workspace, thread_idx);
        } else {
            static_assert(!kFlushStores || kNumScaleoutRanks == 1,
                          "GIN flush requires a leading grid synchronization");
        }
        if constexpr (kFlushStores && kNumScaleoutRanks > 1) {
            gin.flush();
            local_grid_sync<kNumSMs, kNumThreads, kNumTimeoutCycles>(
                workspace, thread_idx);
        }
    }

    do_scaleout &= kNumScaleoutRanks > 1;
    do_scaleup &= kNumScaleupRanks > 1;
#ifdef USE_NCCL_DEVICE
    if constexpr (Ops::kIsNccl) {
        if (do_scaleup && !do_scaleout) {
            mooncake_barrier_wo_local_sync<
                Ops, transport::ScaleupTeam, kNumScaleupRanks, kNumSMs,
                kNumThreads, kNumTimeoutCycles, kTag>(
                gin, workspace, scaleup_rank_idx, sm_idx, thread_idx);
        } else if (do_scaleout && !do_scaleup) {
            nccl_gin_barrier_wo_local_sync<
                transport::ScaleoutTeam, kNumScaleoutRanks,
                kNumScaleoutRanks * kNumScaleupRanks, kNumThreads,
                kNumTimeoutCycles, kTag>(gin, workspace, scaleout_rank_idx,
                                         sm_idx, thread_idx);
        } else if (do_scaleup && do_scaleout) {
            if constexpr (kNumSMs > 1) {
                // Separate blocks let LSA and GIN progress concurrently. They
                // use disjoint signal addresses.
                mooncake_barrier_wo_local_sync<
                    Ops, transport::ScaleupTeam, kNumScaleupRanks, kNumSMs,
                    kNumThreads, kNumTimeoutCycles, kTag>(
                    gin, workspace, scaleup_rank_idx, sm_idx, thread_idx);
                nccl_gin_barrier_wo_local_sync<
                    transport::ScaleoutTeam, kNumScaleoutRanks,
                    kNumScaleoutRanks * kNumScaleupRanks, kNumThreads,
                    kNumTimeoutCycles, kTag>(gin, workspace, scaleout_rank_idx,
                                             sm_idx - 1, thread_idx);
            } else {
                // A one-block launch cannot overlap teams safely. Complete LSA
                // first and then the GIN rail barrier.
                mooncake_barrier_wo_local_sync<Ops, transport::ScaleupTeam,
                                               kNumScaleupRanks, 1, kNumThreads,
                                               kNumTimeoutCycles, kTag>(
                    gin, workspace, scaleup_rank_idx, 0, thread_idx);
                nccl_gin_barrier_wo_local_sync<
                    transport::ScaleoutTeam, kNumScaleoutRanks,
                    kNumScaleoutRanks * kNumScaleupRanks, kNumThreads,
                    kNumTimeoutCycles, kTag>(gin, workspace, scaleout_rank_idx,
                                             0, thread_idx);
            }
        }
    } else
#endif
    {
        // Keep the established IBGDA barrier byte-for-byte in behavior.
        if (do_scaleup && !do_scaleout) {
            mooncake_barrier_wo_local_sync<
                Ops, transport::ScaleupTeam, kNumScaleupRanks, kNumSMs,
                kNumThreads, kNumTimeoutCycles, kTag>(
                gin, workspace, scaleup_rank_idx, sm_idx, thread_idx);
        } else if (do_scaleout && !do_scaleup) {
            mooncake_barrier_wo_local_sync<
                Ops, transport::ScaleoutTeam, kNumScaleoutRanks, kNumSMs,
                kNumThreads, kNumTimeoutCycles, kTag>(
                gin, workspace, scaleout_rank_idx, sm_idx, thread_idx);
        } else {
            const int global_rank =
                scaleout_rank_idx * kNumScaleupRanks + scaleup_rank_idx;
            mooncake_barrier_wo_local_sync<
                Ops, transport::WorldTeam, kNumScaleoutRanks * kNumScaleupRanks,
                kNumSMs, kNumThreads, kNumTimeoutCycles, kTag>(
                gin, workspace, global_rank, sm_idx, thread_idx);
        }
    }

    if constexpr (kSyncAtEnd) {
        local_grid_sync<kNumSMs, kNumThreads, kNumTimeoutCycles>(workspace,
                                                                 thread_idx);
    }
}

}  // namespace mooncake::elastic::comm
