#pragma once

#include <cstdint>
#include <type_traits>

#include <elastic/mooncake_ep_elastic_ptx.cuh>
#include <mooncake_ep_configs.cuh>
#include <transport/device/device_comm.cuh>

namespace mooncake::elastic::transport {
struct WorldTeam {};
struct ScaleupTeam {};
struct ScaleoutTeam {};
constexpr int kRedAddReleaseHighWordLast = 0;
constexpr int kRedAddReleaseLowWordLast = 1 << 0;
template <typename T>
constexpr device::DeviceTeam team() {
    if constexpr (std::is_same_v<T, ScaleupTeam>)
        return device::DeviceTeam::kScaleup;
    if constexpr (std::is_same_v<T, ScaleoutTeam>)
        return device::DeviceTeam::kScaleout;
    return device::DeviceTeam::kWorld;
}

// Backend-neutral elastic facade. All remote actions dispatch through the
// host-bound DeviceComm table; P2P/LSA is a per-destination resolution route.
struct DeviceCommOps {
    using Context = device::DeviceComm;
    // Launch geometry remains a compile-time property; protocol selection is
    // supplied by the bound capability contract below.
    static constexpr bool kIsNccl = false;
    static constexpr int kNumQPs = MAX_QP_COUNT;
    static constexpr int kAggregateRequests = 0;
    static constexpr int kScaleoutUpdateInterval = 3;
#ifdef MOONCAKE_EP_USE_MUSA
    static constexpr int kNumDispatchWarps = 4, kNumDispatchEpilogueWarps = 4;
    static constexpr int kNumCombineWarps = 4, kNumCombineEpilogueWarps = 4;
#else
    static constexpr int kNumDispatchWarps = 8, kNumDispatchEpilogueWarps = 8;
    static constexpr int kNumCombineWarps = 8, kNumCombineEpilogueWarps = 8;
#endif
    static constexpr int kNumHybridScaleoutWarps = 8,
                         // NCCL GIN exposes eight forward channels per SM;
                         // keep dispatch metadata and combine geometry aligned
                         // with that contract. IBGDA accepts the same layout.
                         kNumHybridForwardWarps = 8, kNumHybridScaleupWarps = 8;
    device::DeviceComm comm;
    device::DeviceChannel channel;
    __device__ __forceinline__ DeviceCommOps(const Context& c, int qp,
                                             int share, int, int = 0, int = 0,
                                             int = 0, int = 1)
        : comm(c),
          channel{qp, c.elastic_atomic_source, c.elastic_atomic_target, share} {
    }
    __device__ __forceinline__ bool needs_tma_ordering() const {
        return (comm.elastic_capabilities & device::kElasticTmaOrdering) != 0;
    }
    template <typename T>
    __device__ __forceinline__ bool uses_aggregate_signal() const {
        return std::is_same_v<T, ScaleupTeam> &&
               (comm.elastic_capabilities &
                device::kElasticAggregateScaleupSignal) != 0;
    }
    template <typename T>
    __device__ __forceinline__ bool uses_gin_barrier() const {
        return std::is_same_v<T, ScaleoutTeam> &&
               (comm.elastic_capabilities & device::kElasticGinBarrier) != 0;
    }
    __device__ __forceinline__ uint32_t aggregate_requests() const {
        return comm.aggregate_requests;
    }
    template <typename T>
    __device__ __forceinline__ bool is_nvlink_accessible(int p) const {
        return comm.ops->elastic_ops->resolve(comm, team<T>(), p,
                                              comm.local_base) != nullptr;
    }
    template <typename T>
    __device__ __forceinline__ bool is_gin_peer(int p) const {
        return !is_nvlink_accessible<T>(p);
    }
    template <typename T>
    __device__ __forceinline__ void* get_sym_ptr(void* x, int p) const {
        return comm.ops->elastic_ops->resolve(comm, team<T>(), p, x);
    }
    template <typename T>
    __device__ __forceinline__ const void* get_sym_ptr(const void* x,
                                                       int p) const {
        return comm.ops->elastic_ops->resolve(comm, team<T>(), p, x);
    }
    template <typename T>
    __device__ __forceinline__ void put(void* d, const void* s, int n, int p,
                                        int f = 0) const {
        comm.ops->elastic_ops->put(comm, channel, team<T>(), p, d, s, n, f);
    }
    template <typename Team, typename T>
    __device__ __forceinline__ void put_value(T* d, T v, int p,
                                              int f = 0) const {
        if constexpr (sizeof(T) == 4)
            comm.ops->elastic_ops->put_value32(comm, channel, team<Team>(), p,
                                               reinterpret_cast<int32_t*>(d),
                                               static_cast<int32_t>(v), f);
        else
            comm.ops->elastic_ops->put_value64(comm, channel, team<Team>(), p,
                                               reinterpret_cast<int64_t*>(d),
                                               static_cast<int64_t>(v), f);
    }
    template <typename Team, typename T>
    __device__ __forceinline__ void red_add_rel(T* d, T v, int p,
                                                int f = 0) const {
        if constexpr (sizeof(T) == 4)
            comm.ops->elastic_ops->red_add32(comm, channel, team<Team>(), p,
                                             reinterpret_cast<int32_t*>(d),
                                             static_cast<int32_t>(v), f);
        else
            comm.ops->elastic_ops->red_add64(comm, channel, team<Team>(), p,
                                             reinterpret_cast<int64_t*>(d),
                                             static_cast<int64_t>(v), f);
    }
    template <typename Team, typename T>
    __device__ __forceinline__ T read_completion_tail(T* p, int, int) const {
        return ptx::ld_acquire_sys<T>(p);
    }
    template <typename Team, typename T>
    __device__ __forceinline__ void clear_completion_tail(T* p, int,
                                                          int) const {
        *p = 0;
    }
    template <typename Team, typename T>
    __device__ __forceinline__ void publish_tail(T* p, int, T, T d, int r,
                                                 int f = 0) const {
        red_add_rel<Team>(p, d, r, f);
    }
    __device__ __forceinline__ void flush_channel() const {
        comm.ops->elastic_ops->flush_channel(comm, channel);
    }
    __device__ __forceinline__ void flush() const {
        comm.ops->elastic_ops->flush(comm);
    }
    template <typename T>
    __device__ __forceinline__ void gin_barrier_signal_inc(int p, int s) const {
        comm.ops->elastic_ops->barrier_signal_inc(comm, channel, team<T>(), p,
                                                  s);
    }
    __device__ __forceinline__ uint64_t
    gin_barrier_advance_shadow(int s) const {
        return comm.ops->elastic_ops->barrier_advance_shadow(comm, s);
    }
    __device__ __forceinline__ uint64_t gin_barrier_read_signal(int s) const {
        return comm.ops->elastic_ops->barrier_read(comm, s);
    }
};
}  // namespace mooncake::elastic::transport
