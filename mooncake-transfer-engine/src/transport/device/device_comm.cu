#include "transport/device/device_comm.cuh"
#include "transport/device/comm_device.cuh"
#include <cuda/atomic>
#ifdef USE_NCCL_DEVICE
#include "transport/device/nccl_device.cuh"
#endif
#include <stdexcept>

namespace mooncake::device {
namespace {
__device__ void* directPointer(const DeviceComm& comm, int peer,
                               const void* ptr) {
    if (peer == comm.rank) return const_cast<void*>(ptr);
    if (comm.enable_p2p && comm.peer_bases && comm.peer_available &&
        comm.peer_available[peer] && comm.peer_bases[peer]) {
        return static_cast<char*>(comm.peer_bases[peer]) +
               (static_cast<const char*>(ptr) -
                static_cast<const char*>(comm.local_base));
    }
    return nullptr;
}

__device__ void copyBytes(void* dst, const void* src, uint32_t bytes) {
    auto* out = static_cast<unsigned char*>(dst);
    const auto* in = static_cast<const unsigned char*>(src);
    for (uint32_t i = 0; i < bytes; ++i) out[i] = in[i];
    __threadfence_system();
}

__device__ CommCtx ibgdaContext(const DeviceComm& comm, DeviceChannel channel) {
    const auto& state = *static_cast<const IbgdaCommBinding*>(comm.state);
    return make_comm_ctx(
        comm.local_base, comm.enable_p2p ? comm.peer_available : nullptr,
        comm.enable_p2p ? comm.peer_bases : nullptr,
        const_cast<uint64_t*>(state.raddrs), const_cast<uint32_t*>(state.rkeys),
        state.qp_contexts, channel.atomic_source, channel.atomic_target,
        comm.rank, 0, 0);
}

__device__ void ibgdaPut(const DeviceComm& comm, DeviceChannel channel,
                         int peer, void* dst, const void* src, uint32_t bytes) {
    if (void* direct = directPointer(comm, peer, dst)) {
        copyBytes(direct, src, bytes);
        return;
    }
    mc_rdma_put(ibgdaContext(comm, channel), channel.index, peer,
                comm.channel_count, src, dst, bytes, 0);
}

__device__ void ibgdaSignal(const DeviceComm& comm, DeviceChannel channel,
                            int peer, DeviceSignal* slot, int32_t delta) {
    if (void* direct = directPointer(comm, peer, slot)) {
        mc_atomic_add_release(static_cast<int*>(direct), delta);
        return;
    }
    // Routing was already resolved above. Submit directly: the legacy
    // mc_red_add helper would try P2P again, including on a disabled/null map.
    const auto ctx = ibgdaContext(comm, channel);
    const auto offset = reinterpret_cast<const char*>(slot) -
                        static_cast<const char*>(comm.local_base);
    const auto scratch_offset =
        reinterpret_cast<const char*>(slot) -
        static_cast<const char*>(channel.atomic_target) +
        (static_cast<const char*>(channel.atomic_source) -
         static_cast<const char*>(comm.local_base));
    mc_ibgda_red_add(ctx.ibgda, channel.index, peer, comm.rank,
                     comm.channel_count,
                     ctx.ibgda.raddrs[comm.rank] + scratch_offset,
                     ctx.ibgda.raddrs[peer] + offset, delta);
}

__device__ int32_t ibgdaRead(const DeviceComm&, const DeviceSignal* slot) {
    return mc_ld_acquire(reinterpret_cast<const int*>(slot));
}

__device__ int elasticWorldRank(const DeviceComm& comm, DeviceTeam team,
                                int peer) {
    switch (team) {
        case DeviceTeam::kScaleup:
            return comm.scaleout_rank_idx * comm.num_scaleup_ranks + peer;
        case DeviceTeam::kScaleout:
            return peer * comm.num_scaleup_ranks + comm.scaleup_rank_idx;
        case DeviceTeam::kWorld:
        default:
            return peer;
    }
}

__device__ void* ibgdaElasticResolve(const DeviceComm& comm, DeviceTeam team,
                                     int peer, const void* ptr) {
    return directPointer(comm, elasticWorldRank(comm, team, peer), ptr);
}

__device__ void ibgdaElasticPut(const DeviceComm& comm, DeviceChannel channel,
                                DeviceTeam team, int peer, void* dst,
                                const void* src, uint32_t bytes, uint32_t) {
    const int world_peer = elasticWorldRank(comm, team, peer);
    if (void* direct = directPointer(comm, world_peer, dst)) {
        copyBytes(direct, src, bytes);
        return;
    }
    mc_rdma_put(ibgdaContext(comm, channel), channel.index, world_peer,
                comm.channel_count, src, dst, bytes, 0);
}

__device__ void ibgdaElasticPut32(const DeviceComm& comm, DeviceChannel channel,
                                  DeviceTeam team, int peer, int32_t* dst,
                                  int32_t value, uint32_t) {
    const int world_peer = elasticWorldRank(comm, team, peer);
    if (void* direct = directPointer(comm, world_peer, dst)) {
        mc_st_release(static_cast<int*>(direct), value);
        return;
    }
    mc_signal(ibgdaContext(comm, channel), world_peer, channel.index,
              comm.channel_count, reinterpret_cast<int*>(dst), value);
}

__device__ void ibgdaElasticRed32(const DeviceComm& comm, DeviceChannel channel,
                                  DeviceTeam team, int peer, int32_t* dst,
                                  int32_t value, uint32_t) {
    const int world_peer = elasticWorldRank(comm, team, peer);
    if (void* direct = directPointer(comm, world_peer, dst)) {
        mc_atomic_add_release(static_cast<int*>(direct), value);
        return;
    }
    mc_red_add(ibgdaContext(comm, channel), world_peer, channel.index,
               comm.channel_count, reinterpret_cast<int*>(dst), value);
}

__device__ void ibgdaElasticRed64(const DeviceComm& comm, DeviceChannel channel,
                                  DeviceTeam team, int peer, int64_t* dst,
                                  int64_t value, uint32_t flags) {
    const int world_peer = elasticWorldRank(comm, team, peer);
    if (void* direct = directPointer(comm, world_peer, dst)) {
        auto* target = static_cast<int64_t*>(direct);
        cuda::atomic_ref<int64_t, cuda::thread_scope_system> signal(*target);
        signal.fetch_add(value, cuda::memory_order_release);
        return;
    }
    auto* words = reinterpret_cast<int32_t*>(dst);
    const auto low = static_cast<int32_t>(static_cast<uint64_t>(value));
    const auto high = static_cast<int32_t>(value >> 32);
    const bool low_last = (flags & 1u) != 0;
    if (!low_last && low != 0)
        ibgdaElasticRed32(comm, channel, team, peer, words, low, 0);
    if (high != 0)
        ibgdaElasticRed32(comm, channel, team, peer, words + 1, high, 0);
    if (low_last && low != 0)
        ibgdaElasticRed32(comm, channel, team, peer, words, low, 0);
}

__device__ void ibgdaElasticPut64(const DeviceComm& comm, DeviceChannel channel,
                                  DeviceTeam team, int peer, int64_t* dst,
                                  int64_t value, uint32_t flags) {
    const int world_peer = elasticWorldRank(comm, team, peer);
    if (void* direct = directPointer(comm, world_peer, dst)) {
        *static_cast<int64_t*>(direct) = value;
        __threadfence_system();
        return;
    }
    ibgdaElasticRed64(comm, channel, team, peer, dst, value, flags);
}

__device__ void elasticFence(const DeviceComm&) { __threadfence_system(); }
__device__ void elasticNoopChannel(const DeviceComm&, DeviceChannel) {}
__device__ void elasticNoopBarrier(const DeviceComm&, DeviceChannel, DeviceTeam,
                                   int, int) {}
__device__ uint64_t elasticNoopCounter(const DeviceComm&, int) { return 0; }

__device__ DeviceCommElasticOps ibgdaElasticOps{
    kDeviceCommElasticAbi, sizeof(DeviceCommElasticOps),
    ibgdaElasticResolve,   ibgdaElasticPut,
    ibgdaElasticPut32,     ibgdaElasticPut64,
    ibgdaElasticRed32,     ibgdaElasticRed64,
    elasticNoopChannel,    elasticFence,
    elasticNoopBarrier,    elasticNoopCounter,
    elasticNoopCounter};

__device__ DeviceCommOps ibgdaOps{
    kDeviceCommAbi, sizeof(DeviceCommOps), directPointer, ibgdaPut, ibgdaSignal,
    ibgdaRead,      &ibgdaElasticOps};
__device__ const DeviceCommOps* ibgdaOpsAddress = &ibgdaOps;

#ifdef USE_NCCL_DEVICE
__device__ const NcclDeviceContext& ncclContext(const DeviceComm& comm) {
    return *static_cast<const NcclDeviceContext*>(comm.state);
}
__device__ void* ncclResolve(const DeviceComm& comm, int peer,
                             const void* ptr) {
    if (void* direct = directPointer(comm, peer, ptr)) return direct;
    if (!comm.enable_p2p || !mc_nccl_lsa_available(ncclContext(comm), peer))
        return nullptr;
    return mc_nccl_peer_ptr(ncclContext(comm), peer, ptr);
}
__device__ void ncclPut(const DeviceComm& comm, DeviceChannel, int peer,
                        void* dst, const void* src, uint32_t bytes) {
    if (void* direct = ncclResolve(comm, peer, dst)) {
        copyBytes(direct, src, bytes);
        return;
    }
    // One GPU-shared GIN context preserves publication/read ordering across
    // issuing CTAs and avoids exposing NCCL context IDs to the EP algorithm.
    mc_nccl_put_team<NcclGinTeam::kWorld>(ncclContext(comm), 0, peer, 1, src,
                                          dst, bytes, 0);
}
__device__ void ncclSignal(const DeviceComm& comm, DeviceChannel, int peer,
                           DeviceSignal* slot, int32_t delta) {
    if (void* direct = ncclResolve(comm, peer, slot)) {
        auto& value = *static_cast<uint64_t*>(direct);
        cuda::atomic_ref<uint64_t, cuda::thread_scope_system> signal(value);
        signal.fetch_add(static_cast<uint64_t>(static_cast<int64_t>(delta)),
                         cuda::memory_order_release);
        return;
    }
    mc_nccl_gin_signal_add_team<NcclGinTeam::kWorld>(
        ncclContext(comm), peer, 0, 1, &slot->storage,
        static_cast<uint64_t>(static_cast<int64_t>(delta)), 0);
}
__device__ int32_t ncclRead(const DeviceComm& comm, const DeviceSignal* slot) {
    return static_cast<int32_t>(
        mc_nccl_read_signal(ncclContext(comm), 0, &slot->storage));
}

__device__ NcclGinHandle ncclElasticHandle(const DeviceComm& comm,
                                           DeviceChannel channel) {
    return NcclGinHandle(
        ncclContext(comm), static_cast<unsigned int>(channel.index),
        channel.sharing_mode == 0 ? NcclGinResourceSharing::kCta
                                  : NcclGinResourceSharing::kGpu);
}

__device__ void* ncclElasticResolve(const DeviceComm& comm, DeviceTeam team,
                                    int peer, const void* ptr) {
    const auto gin = ncclElasticHandle(comm, {});
    if (team == DeviceTeam::kScaleup) return gin.lsaPeerPointer(peer, ptr);
    if (team == DeviceTeam::kScaleout)
        return gin.railRank() == peer ? const_cast<void*>(ptr) : nullptr;
    if (!gin.worldRankInLsa(peer)) return nullptr;
    return gin.worldPeerPointer(peer, ptr);
}

__device__ void ncclElasticPut(const DeviceComm& comm, DeviceChannel channel,
                               DeviceTeam team, int peer, void* dst,
                               const void* src, uint32_t bytes,
                               uint32_t flags) {
    const auto gin = ncclElasticHandle(comm, channel);
    if (team == DeviceTeam::kScaleup) {
        copyBytes(gin.lsaPeerPointer(peer, dst), src, bytes);
        return;
    }
    if (team == DeviceTeam::kScaleout) {
        gin.put<NcclGinTeam::kRail>(peer, src, dst, bytes, flags);
    } else {
        gin.put<NcclGinTeam::kWorld>(peer, src, dst, bytes, flags);
    }
}

template <typename T>
__device__ void ncclElasticPutValue(const DeviceComm& comm,
                                    DeviceChannel channel, DeviceTeam team,
                                    int peer, T* dst, T value, uint32_t flags) {
    const auto gin = ncclElasticHandle(comm, channel);
    void* direct = ncclElasticResolve(comm, team, peer, dst);
    if (direct != nullptr) {
        *static_cast<T*>(direct) = value;
        __threadfence_system();
    } else if (team == DeviceTeam::kScaleout) {
        gin.putValue<NcclGinTeam::kRail>(peer, dst, value, flags);
    } else {
        gin.putValue<NcclGinTeam::kWorld>(peer, dst, value, flags);
    }
}

__device__ void ncclElasticPut32(const DeviceComm& comm, DeviceChannel channel,
                                 DeviceTeam team, int peer, int32_t* dst,
                                 int32_t value, uint32_t flags) {
    ncclElasticPutValue(comm, channel, team, peer, dst, value, flags);
}

__device__ void ncclElasticPut64(const DeviceComm& comm, DeviceChannel channel,
                                 DeviceTeam team, int peer, int64_t* dst,
                                 int64_t value, uint32_t flags) {
    ncclElasticPutValue(comm, channel, team, peer, dst, value, flags);
}

template <typename T>
__device__ void ncclElasticRed(const DeviceComm& comm, DeviceChannel channel,
                               DeviceTeam team, int peer, T* dst, T value,
                               uint32_t) {
    const auto gin = ncclElasticHandle(comm, channel);
    if (void* direct = ncclElasticResolve(comm, team, peer, dst)) {
        cuda::atomic_ref<T, cuda::thread_scope_system> signal(
            *static_cast<T*>(direct));
        signal.fetch_add(value, cuda::memory_order_release);
    } else if constexpr (sizeof(T) == sizeof(uint64_t)) {
        if (team == DeviceTeam::kScaleout)
            gin.signalAdd<NcclGinTeam::kRail>(peer,
                                              reinterpret_cast<uint64_t*>(dst),
                                              static_cast<uint64_t>(value));
        else
            gin.signalAdd<NcclGinTeam::kWorld>(peer,
                                               reinterpret_cast<uint64_t*>(dst),
                                               static_cast<uint64_t>(value));
    }
}

__device__ void ncclElasticRed32(const DeviceComm& comm, DeviceChannel channel,
                                 DeviceTeam team, int peer, int32_t* dst,
                                 int32_t value, uint32_t flags) {
    ncclElasticRed(comm, channel, team, peer, dst, value, flags);
}

__device__ void ncclElasticRed64(const DeviceComm& comm, DeviceChannel channel,
                                 DeviceTeam team, int peer, int64_t* dst,
                                 int64_t value, uint32_t flags) {
    ncclElasticRed(comm, channel, team, peer, dst, value, flags);
}

__device__ void ncclElasticFlushChannel(const DeviceComm& comm,
                                        DeviceChannel channel) {
    ncclElasticHandle(comm, channel).flushWarp();
}

__device__ void ncclElasticFlush(const DeviceComm& comm) {
    const auto gin = ncclElasticHandle(comm, {});
    const int warps_per_block = static_cast<int>(blockDim.x) / warpSize;
    const int global_warp = static_cast<int>(blockIdx.x) * warps_per_block +
                            static_cast<int>(threadIdx.x) / warpSize;
    const int num_warps = static_cast<int>(gridDim.x) * warps_per_block;
    for (int context = global_warp; context < gin.contextCount();
         context += num_warps)
        gin.flushContextWarp(context);
}

__device__ void ncclElasticBarrierSignal(const DeviceComm& comm,
                                         DeviceChannel channel, DeviceTeam team,
                                         int peer, int signal_id) {
    const auto gin = ncclElasticHandle(comm, channel);
    if (team == DeviceTeam::kScaleout)
        gin.signalIncContext0<NcclGinTeam::kRail>(peer, signal_id);
    else
        gin.signalIncContext0<NcclGinTeam::kWorld>(peer, signal_id);
}

__device__ uint64_t ncclElasticBarrierAdvance(const DeviceComm& comm,
                                              int signal_id) {
    return ncclElasticHandle(comm, {}).advanceSignalShadowContext0(signal_id);
}

__device__ uint64_t ncclElasticBarrierRead(const DeviceComm& comm,
                                           int signal_id) {
    return ncclElasticHandle(comm, {}).readSignalContext0(signal_id);
}

__device__ DeviceCommElasticOps ncclElasticOps{
    kDeviceCommElasticAbi,    sizeof(DeviceCommElasticOps),
    ncclElasticResolve,       ncclElasticPut,
    ncclElasticPut32,         ncclElasticPut64,
    ncclElasticRed32,         ncclElasticRed64,
    ncclElasticFlushChannel,  ncclElasticFlush,
    ncclElasticBarrierSignal, ncclElasticBarrierAdvance,
    ncclElasticBarrierRead};
__device__ DeviceCommOps ncclOps{
    kDeviceCommAbi, sizeof(DeviceCommOps), ncclResolve, ncclPut, ncclSignal,
    ncclRead,       &ncclElasticOps};
__device__ const DeviceCommOps* ncclOpsAddress = &ncclOps;
#endif

void check(cudaError_t err) {
    if (err != cudaSuccess) throw std::runtime_error(cudaGetErrorString(err));
}
}  // namespace

size_t deviceCommStateSize(DeviceBackend backend) {
    switch (backend) {
        case DeviceBackend::kIbgda:
            return sizeof(IbgdaCommBinding);
#ifdef USE_NCCL_DEVICE
        case DeviceBackend::kNccl:
            return sizeof(NcclDeviceContext);
#endif
        default:
            throw std::invalid_argument("DeviceComm backend not built");
    }
}

DeviceComm bindDeviceComm(DeviceBackend backend, void* device_state,
                          const void* host_state, void* base, int rank,
                          const int32_t* available, void* const* peers) {
    DeviceComm comm;
    const size_t bytes = deviceCommStateSize(backend);
    if (!device_state || !host_state || !base)
        throw std::invalid_argument(
            "DeviceComm requires live window and state");
    switch (backend) {
        case DeviceBackend::kIbgda:
            check(cudaMemcpyFromSymbol(&comm.ops, ibgdaOpsAddress,
                                       sizeof(comm.ops)));
            break;
#ifdef USE_NCCL_DEVICE
        case DeviceBackend::kNccl:
            check(cudaMemcpyFromSymbol(&comm.ops, ncclOpsAddress,
                                       sizeof(comm.ops)));
            break;
#endif
        default:
            throw std::invalid_argument("DeviceComm backend not built");
    }
    check(cudaMemcpy(device_state, host_state, bytes, cudaMemcpyHostToDevice));
    DeviceCommOps header{};
    check(
        cudaMemcpy(&header, comm.ops, sizeof(header), cudaMemcpyDeviceToHost));
    if (header.abi_version != kDeviceCommAbi ||
        header.struct_size < sizeof(DeviceCommOps) || !header.resolve ||
        !header.put || !header.signal_add || !header.signal_read)
        throw std::runtime_error("incompatible DeviceComm ops ABI");
    comm.state = device_state;
    comm.backend = backend;
    comm.local_base = base;
    comm.rank = rank;
    comm.peer_available = available;
    comm.peer_bases = peers;
    if (backend == DeviceBackend::kNccl) {
        comm.elastic_capabilities = kElasticTmaOrdering |
                                     kElasticAggregateScaleupSignal |
                                     kElasticGinBarrier;
    }
    return comm;
}
}  // namespace mooncake::device
