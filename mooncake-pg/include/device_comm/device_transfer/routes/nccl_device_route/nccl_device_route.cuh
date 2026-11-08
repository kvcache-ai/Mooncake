#ifndef MOONCAKE_PG_DEVICE_COMM_DEVICE_TRANSFER_ROUTES_NCCL_DEVICE_ROUTE_CUH
#define MOONCAKE_PG_DEVICE_COMM_DEVICE_TRANSFER_ROUTES_NCCL_DEVICE_ROUTE_CUH

#include <cstdint>

#include <cooperative_groups.h>

#include "pg_assert.h"
#include "device_comm/device_transfer/transfer_types.cuh"

#ifdef USE_NCCL_DEVICE
#include <transport/device/device_ops.cuh>
#include <transport/device/nccl_device.cuh>

#include "device_comm/device_primitives/value_primitives.cuh"
#endif

namespace mooncake {

#ifdef USE_NCCL_DEVICE
class NcclDeviceTransferTicket {
    struct ErrorTicket {
        TransferResult result = TransferResult::RouteUnavailable;

        __device__ __forceinline__ TransferResult
        wait(cooperative_groups::thread_block) const {
            return result;
        }
    };

    struct GinTicket {
        const device::NcclDeviceContext* context;
        int32_t nccl_rank;
        uint32_t lane;
        uint64_t start_ticks;
        uint64_t timeout_ticks;

        __device__ __forceinline__ TransferResult
        wait(cooperative_groups::thread_block block) const {
            __shared__ TransferResult block_wait_result;
            if (block.thread_rank() == 0) {
                block_wait_result = waitLeader();
            }
            block.sync();
            const auto observed_result = block_wait_result;
            block.sync();
            return observed_result;
        }

       private:
        __device__ __noinline__ TransferResult waitLeader() const {
            const uint64_t elapsed_ticks = clock64() - start_ticks;
            if (timeout_ticks != 0 && elapsed_ticks >= timeout_ticks)
                return TransferResult::TimedOut;
            device::NcclGinHandle gin(*context, lane);
            const auto status = gin.flushPeer<device::NcclGinTeam::kWorld>(
                nccl_rank,
                timeout_ticks == 0 ? 0 : timeout_ticks - elapsed_ticks);
            if (status == ncclSuccess) return TransferResult::Succeeded;
            return status == ncclTimeout ? TransferResult::TimedOut
                                         : TransferResult::Failed;
        }
    };

    struct LsaTicket {
        __device__ __forceinline__ TransferResult
        wait(cooperative_groups::thread_block block) const {
            // Wait for the leader's optional LSA signal update before
            // returning.
            block.sync();
            return TransferResult::Succeeded;
        }
    };

    enum class Type : uint8_t { Error, Gin, Lsa };

   public:
    __device__ __forceinline__ NcclDeviceTransferTicket() = default;

    __device__ __forceinline__ static NcclDeviceTransferTicket createGinTicket(
        const device::NcclDeviceContext* context, int32_t nccl_rank,
        uint32_t lane, uint64_t start_ticks, uint64_t timeout_ticks) {
        NcclDeviceTransferTicket ticket;
        ticket.type_ = Type::Gin;
        ticket.gin_ = {.context = context,
                       .nccl_rank = nccl_rank,
                       .lane = lane,
                       .start_ticks = start_ticks,
                       .timeout_ticks = timeout_ticks};
        return ticket;
    }

    __device__ __forceinline__ static NcclDeviceTransferTicket
    createLsaTicket() {
        NcclDeviceTransferTicket ticket;
        ticket.type_ = Type::Lsa;
        ticket.lsa_ = {};
        return ticket;
    }

    __device__ __forceinline__ static NcclDeviceTransferTicket
    createFailedTicket() {
        NcclDeviceTransferTicket ticket;
        ticket.error_.result = TransferResult::Failed;
        return ticket;
    }

    __device__ __forceinline__ TransferResult
    wait(cooperative_groups::thread_block block) const {
        switch (type_) {
            case Type::Error:
                return error_.wait(block);
            case Type::Gin:
                return gin_.wait(block);
            case Type::Lsa:
                return lsa_.wait(block);
        }
        PG_UNREACHABLE();
        return TransferResult::Failed;
    }

   private:
    Type type_ = Type::Error;
    union {
        ErrorTicket error_ = {};
        GinTicket gin_;
        LsaTicket lsa_;
    };
};

__device__ __forceinline__ void applyNcclLsaSignalAction(
    char* remote_region, const SignalAction& signal,
    cooperative_groups::thread_block block) {
    // Publish every calling thread's preceding direct memory accesses before
    // applying the peer-visible action.
    device::mc_fence_barrier_fence();
    if (signal.kind == SignalAction::Kind::None) return;
    PG_ASSERT(signal.kind == SignalAction::Kind::Add ||
              signal.kind == SignalAction::Kind::Set);

    if (block.thread_rank() == 0) {
        auto* const target =
            reinterpret_cast<uint64_t*>(remote_region + signal.remote_offset);
        uint64_t value;
        if (signal.kind == SignalAction::Kind::Add) {
            value = device::mc_ld_acquire_u64(target) + signal.add.delta;
        } else {
            value = signal.set.value;
        }
        device::mc_st_release_u64(target, value);
    }
}

// Keep NCCL GIN's implementation out of every collective dtype/op instance.
// LSA's direct-memory path remains inline in the route dispatch below.
static __device__ __noinline__ NcclDeviceTransferTicket
ncclGinPut(const DeviceNcclRoute& route, const DeviceNcclContext& context,
           const void* source, uint64_t remote_payload_offset, uint64_t size,
           const SignalAction& signal, uint64_t timeout_ticks, uint32_t lane,
           cooperative_groups::thread_block block) {
    const auto nccl_rank = route.nccl_rank;
    // Every source writer participates in publication to the NIC.
    __threadfence_system();
    block.sync();
    if (!context.peer_accessible_ctx) return {};
    if (signal.kind == SignalAction::Kind::Set) {
        // NCCL GIN has no attached Set action; reject it for the whole CTA.
        PG_UNREACHABLE();
        return NcclDeviceTransferTicket::createFailedTicket();
    }
    const uint64_t start_ticks = clock64();

    if (block.thread_rank() == 0) {
        device::NcclGinHandle gin(*context.peer_accessible_ctx, lane);
        auto* signal_ptr = reinterpret_cast<uint64_t*>(
            static_cast<char*>(context.peer_accessible_region.addr) +
            signal.remote_offset);
        const auto* source_context =
            context.peer_accessible_region.contains(source, size)
                ? context.peer_accessible_ctx
                : context.local_staging_ctx;
        auto* recv_ptr =
            static_cast<char*>(context.peer_accessible_region.addr) +
            remote_payload_offset;
        if (signal.kind == SignalAction::Kind::None) {
            gin.put<device::NcclGinTeam::kWorld>(nccl_rank, *source_context,
                                                 source, recv_ptr, size);
        } else {
            PG_ASSERT(signal.kind == SignalAction::Kind::Add);
            // Attach the VA update to the actual payload operation. A
            // separate weak signal after put() would not publish this
            // payload.
            gin.put<device::NcclGinTeam::kWorld>(
                nccl_rank, *source_context, source, recv_ptr, size,
                gin.makeWeakVaSignalAdd(signal_ptr, signal.add.delta));
        }
    }
    return NcclDeviceTransferTicket::createGinTicket(
        context.peer_accessible_ctx, nccl_rank, lane, start_ticks,
        timeout_ticks);
}

__device__ __forceinline__ NcclDeviceTransferTicket
ncclDevicePut(const DeviceNcclRoute& route, const DeviceNcclContext& context,
              const void* source, uint64_t remote_payload_offset, uint64_t size,
              const SignalAction& signal, uint64_t timeout_ticks, uint32_t lane,
              cooperative_groups::thread_block block) {
    if (route.mapped_region_address != 0) {
        auto* const remote_region = reinterpret_cast<char*>(
            static_cast<uintptr_t>(route.mapped_region_address));
        if (size != 0) {
            copyValuesTo(static_cast<const uint8_t*>(source), size, block,
                         reinterpret_cast<uint8_t*>(remote_region +
                                                    remote_payload_offset));
        }
        applyNcclLsaSignalAction(remote_region, signal, block);
        return NcclDeviceTransferTicket::createLsaTicket();
    }
    return ncclGinPut(route, context, source, remote_payload_offset, size,
                      signal, timeout_ticks, lane, block);
}

static __device__ __noinline__ NcclDeviceTransferTicket
ncclGinSignal(const DeviceNcclRoute& route, const DeviceNcclContext& context,
              const SignalAction& signal, uint64_t timeout_ticks, uint32_t lane,
              cooperative_groups::thread_block block) {
    const auto nccl_rank = route.nccl_rank;
    // Fence each thread's memory accesses before the leader sends the signal.
    // This does not flush previously submitted network transfers.
    __threadfence_system();
    block.sync();
    if (!context.peer_accessible_ctx) return {};
    const uint64_t start_ticks = clock64();

    if (block.thread_rank() == 0) {
        device::NcclGinHandle gin(*context.peer_accessible_ctx, lane);
        auto* target = reinterpret_cast<uint64_t*>(
            static_cast<char*>(context.peer_accessible_region.addr) +
            signal.remote_offset);
        switch (signal.kind) {
            case SignalAction::Kind::Add:
                gin.signalAddWeak<device::NcclGinTeam::kWorld>(
                    nccl_rank, target, signal.add.delta);
                break;
            case SignalAction::Kind::Set:
                gin.putValue<device::NcclGinTeam::kWorld>(nccl_rank, target,
                                                          signal.set.value);
                break;
            case SignalAction::Kind::None:
                break;
        }
    }
    return NcclDeviceTransferTicket::createGinTicket(
        context.peer_accessible_ctx, nccl_rank, lane, start_ticks,
        timeout_ticks);
}

__device__ __forceinline__ NcclDeviceTransferTicket
ncclDeviceSignal(const DeviceNcclRoute& route, const DeviceNcclContext& context,
                 const SignalAction& signal, uint64_t timeout_ticks,
                 uint32_t lane, cooperative_groups::thread_block block) {
    if (route.mapped_region_address != 0) {
        auto* const remote_region = reinterpret_cast<char*>(
            static_cast<uintptr_t>(route.mapped_region_address));
        applyNcclLsaSignalAction(remote_region, signal, block);
        return NcclDeviceTransferTicket::createLsaTicket();
    }
    return ncclGinSignal(route, context, signal, timeout_ticks, lane, block);
}

// Best-effort drain of all GIN contexts for the selected NCCL peers after
// producers stop submitting.
__device__ __forceinline__ void drainNcclDeviceTransfers(
    const DeviceTransferHandle& handle, const GlobalRank* peers,
    uint32_t peer_count) {
    const auto* context = handle.route_context.nccl.peer_accessible_ctx;
    if (!context) return;
    const int gin_context_count = device::mc_nccl_gin_context_count(*context);
    const uint64_t start_ticks = clock64();
    for (uint32_t index = 0; index < peer_count; ++index) {
        const auto& route = handle.routes[peers[index]];
        if (route.type != DeviceRouteType::NcclDevice) continue;
        for (int context_index = 0; context_index < gin_context_count;
             ++context_index) {
            const auto elapsed_ticks = clock64() - start_ticks;
            if (handle.drain_timeout_ticks != 0 &&
                elapsed_ticks >= handle.drain_timeout_ticks) {
                printf("[PG] NCCL device drain timed out; continuing\n");
                return;
            }
            device::NcclGinHandle gin(*context, context_index);
            const auto remaining_ticks =
                handle.drain_timeout_ticks == 0
                    ? 0
                    : handle.drain_timeout_ticks - elapsed_ticks;
            const auto status = gin.flushPeer<device::NcclGinTeam::kWorld>(
                route.nccl.nccl_rank, remaining_ticks);
            if (status != ncclSuccess) {
                printf(
                    "[PG] NCCL device peer %d context %d drain failed "
                    "(status=%d); continuing\n",
                    route.nccl.nccl_rank, context_index,
                    static_cast<int>(status));
            }
        }
    }
}

#else
class NcclDeviceTransferTicket {
   public:
    __device__ __forceinline__ TransferResult
    wait(cooperative_groups::thread_block) const {
        return TransferResult::RouteUnavailable;
    }
};

__device__ __forceinline__ NcclDeviceTransferTicket
ncclDevicePut(const DeviceNcclRoute&, const DeviceNcclContext&, const void*,
              uint64_t, uint64_t, const SignalAction&, uint64_t, uint32_t,
              cooperative_groups::thread_block) {
    return {};
}

__device__ __forceinline__ NcclDeviceTransferTicket ncclDeviceSignal(
    const DeviceNcclRoute&, const DeviceNcclContext&, const SignalAction&,
    uint64_t, uint32_t, cooperative_groups::thread_block) {
    return {};
}

__device__ __forceinline__ void drainNcclDeviceTransfers(
    const DeviceTransferHandle&, const GlobalRank*, uint32_t) {}
#endif  // USE_NCCL_DEVICE

}  // namespace mooncake

#endif  // MOONCAKE_PG_DEVICE_COMM_DEVICE_TRANSFER_ROUTES_NCCL_DEVICE_ROUTE_CUH
