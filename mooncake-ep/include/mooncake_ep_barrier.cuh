// GPU barrier primitives — adapted from DeepEP V2 comm.cuh and barrier.cuh.
//
// Provides:
//   timeout_while  — timeout-checked spin-wait with trap on expiry
//   gpu_barrier    — full inter-rank + inter-SM barrier using Device API
//   barrier_impl   — standalone barrier kernel
//
// Differences from DeepEP V2:
//   - No scaleout/scaleup distinction (single-node only).
//   - No GIN (gin.get_sym_ptr → mc_route_put, gin.signal → mc_signal).
//   - No TMA flush (ptx::tma_store_commit/wait → __syncwarp).
//   - No cooperative_groups::this_grid().sync() → mc_grid_sync().
//   - No ncclTeamTagLsa/ncclTeamTagWorld → simple rank indexing.
#pragma once

#include <mooncake_ep_configs.cuh>
#include <mooncake_ep_exception.cuh>
#include <mooncake_ep_layout.cuh>
#include <mooncake_ep_utils.cuh>
#include <transport/device/comm_device.cuh>

namespace mooncake {
namespace barrier {

using mooncake::device::CommCtx;
using mooncake::device::mc_route_put;
using mooncake::device::mc_signal;
using mooncake::device::mc_ld_acquire;
using mooncake::device::mc_st_release;
using mooncake::device::mc_atomic_add_release;
using mooncake::device::mc_grid_sync;

static constexpr int64_t kNumOneSecCycles = 2000000000;

// ---------------------------------------------------------------------------
// timeout_while — spin-wait with timeout and trap
// ---------------------------------------------------------------------------

template <int64_t kNumTimeoutCycles, typename func_t>
__device__ __forceinline__ void timeout_while(const bool& condition,
                                              const func_t& func,
                                              int64_t start_clock = 0) {
    if (start_clock == 0) start_clock = clock64();

    while (condition) {
        const bool timeout = clock64() - start_clock >= kNumTimeoutCycles;
        if (func(timeout)) break;

        if (timeout) {
            // Wait another 1 second to let all threads print and trap
            start_clock = clock64();
            while (clock64() - start_clock < kNumOneSecCycles) {
            }
            trap();
        }
    }
}

template <int64_t kNumTimeoutCycles, typename func_t>
__device__ __forceinline__ void timeout_while(const func_t& func,
                                              const int64_t& start_clock = 0) {
    timeout_while<kNumTimeoutCycles, func_t>(true, func, start_clock);
}

// ---------------------------------------------------------------------------
// gpu_barrier — full inter-rank + inter-SM barrier
//
// Protocol (NVLink/P2P path, adapted from DeepEP nvlink_barrier_wo_local_sync):
//   1. Only SM 0 participates in inter-rank signaling.
//   2. Read barrier phase from workspace counter.
//   3. Each thread signals one peer rank via mc_route_put → mc_atomic_add_release
//      (P2P) or mc_signal (RDMA).
//   4. Thread 0 polls local signal slot with mc_ld_acquire until all ranks
//      have arrived.
//   5. mc_grid_sync() at start and/or end for SM-level synchronization.
// ---------------------------------------------------------------------------

template <int kNumRanks, int kNumThreads,
          int64_t kNumTimeoutCycles, int kTag = EP_BARRIER_TAG_KERNEL_BARRIER,
          bool kSyncAtStart = true, bool kSyncAtEnd = true>
__forceinline__ __device__ void gpu_barrier(
    const CommCtx& ctx,
    const layout::WorkspaceLayout& workspace,
    int rank_idx, int sm_idx, int thread_idx,
    int num_sms) {
    // Grid sync at start (all SMs must arrive before inter-rank signaling)
    if constexpr (kSyncAtStart) {
        mc_grid_sync();
    }

    // Only SM 0 does inter-rank signaling
    if (num_sms > 1 && sm_idx > 0) {
        if constexpr (kSyncAtEnd) mc_grid_sync();
        return;
    }

    // Read current barrier phase
    const int status =
        static_cast<int>((*workspace.get_barrier_counter_ptr()) & 3);
    const int phase = status & 1, sign = status >> 1;

    // Each thread signals one peer rank
    EP_STATIC_ASSERT(kNumRanks <= kNumThreads, "Insufficient threads");
    if (thread_idx < kNumRanks) {
        const auto peer_rank = thread_idx;
        if (peer_rank == rank_idx) {
            // Self-signal: write directly to local buffer
            mc_atomic_add_release(
                workspace.get_barrier_signal_ptr(phase), sign ? -1 : 1);
        } else {
            // Try P2P path first
            void* peer_ptr = mc_route_put(
                ctx, peer_rank, workspace.get_barrier_signal_ptr(phase));
            if (peer_ptr != nullptr) {
                // P2P (NVLink): atomic add on peer memory
                mc_atomic_add_release(static_cast<int*>(peer_ptr),
                                      sign ? -1 : 1);
            } else {
                // RDMA path: use mc_signal
                // Use channel 0, QP per rank = 1 for barrier traffic
                mc_signal(ctx, peer_rank, 0, 1,
                          workspace.get_barrier_signal_ptr(phase),
                          sign ? -1 : 1);
            }
        }
    }
    __syncthreads();

    // Advance phase counter (only thread 0)
    if (thread_idx == 0) atomicAdd(workspace.get_barrier_counter_ptr(), 1);

    // Wait for all ranks to arrive
    const auto target = sign ? 0 : kNumRanks;
    timeout_while<kNumTimeoutCycles>(
        thread_idx == 0, [=](const bool& is_last_check) {
            const auto signal =
                mc_ld_acquire(workspace.get_barrier_signal_ptr(phase));
            if (signal == target) return true;

            if (is_last_check) {
                printf(
                    "Mooncake EP barrier timeout, tag: %d, rank: %d, "
                    "thread: %d, status: %d, signal: %d, phase: %d, "
                    "target: %d, counter: %llu\n",
                    kTag, rank_idx, thread_idx, status, signal, phase, target,
                    *workspace.get_barrier_counter_ptr());
            }
            return false;
        });

    // Grid sync at end
    if constexpr (kSyncAtEnd) {
        mc_grid_sync();
    }
}

// ---------------------------------------------------------------------------
// barrier_impl — standalone barrier kernel
// ---------------------------------------------------------------------------

template <int kNumThreads, int kNumRanks,
          int64_t kNumTimeoutCycles>
__global__ void __launch_bounds__(kNumThreads, 1)
barrier_impl(void* mxa_buffer, void* raddrs, void* rkeys, void* qp_devctxs,
             const int32_t* nvlink_available, void* const* ipc_peer_ptrs,
             int* rdma_send_signal_buffer, int* rdma_recv_signal_buffer,
             void* workspace, int rank_idx, int num_ranks, int num_sms) {
    const auto sm_idx = static_cast<int>(blockIdx.x);
    const auto thread_idx = static_cast<int>(threadIdx.x);

    const CommCtx ctx = mooncake::device::make_comm_ctx(
        mxa_buffer, nvlink_available, ipc_peer_ptrs, raddrs, rkeys,
        qp_devctxs, rdma_send_signal_buffer, rdma_recv_signal_buffer, rank_idx,
        num_ranks, MAX_QP_COUNT);

    // num_experts=0 is fine — barrier only uses the first part of workspace
    const auto workspace_layout =
        layout::WorkspaceLayout(workspace, num_ranks, 0);

    gpu_barrier<kNumRanks, kNumThreads, kNumTimeoutCycles,
                EP_BARRIER_TAG_KERNEL_BARRIER, false, false>(
        ctx, workspace_layout, rank_idx, sm_idx, thread_idx, num_sms);
}

}  // namespace barrier
}  // namespace mooncake
