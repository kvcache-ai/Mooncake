#pragma once
#include "transport/device/device_comm.h"
#include "transport/device/device_ops.cuh"

namespace mooncake::device {

__device__ __forceinline__ void* deviceResolve(const DeviceComm& comm, int peer,
                                               const void* ptr) {
    return comm.ops->resolve(comm, peer, ptr);
}

__device__ __forceinline__ void devicePut(const DeviceComm& comm,
                                          DeviceChannel channel, int peer,
                                          void* dst, const void* src,
                                          uint32_t bytes, int lane) {
    if (lane == 0) comm.ops->put(comm, channel, peer, dst, src, bytes);
}

__device__ __forceinline__ void deviceSignalAdd(const DeviceComm& comm,
                                                DeviceChannel channel, int peer,
                                                DeviceSignal* slot,
                                                int32_t delta) {
    comm.ops->signal_add(comm, channel, peer, slot, delta);
}

__device__ __forceinline__ int32_t deviceSignalRead(const DeviceComm& comm,
                                                    const DeviceSignal* slot) {
    return comm.ops->signal_read(comm, slot);
}

__device__ __forceinline__ void deviceSignalReset(DeviceSignal* slot) {
    slot->storage = 0;
}

}  // namespace mooncake::device
