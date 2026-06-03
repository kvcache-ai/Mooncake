#ifndef MOONCAKE_PG_GPU_UTILS_H
#define MOONCAKE_PG_GPU_UTILS_H

#ifdef MOONCAKE_EP_USE_MUSA
#include <ATen/musa/MUSAContext.h>
#else
#include <ATen/cuda/CUDAContext.h>
#endif

namespace mooncake {

#ifdef MOONCAKE_EP_USE_MUSA
static inline auto getCurrentGPUStream(int device_index = -1) {
    return at::musa::getCurrentMUSAStream(device_index);
}
static inline auto getGPUStreamFromPool(bool non_blocking = false,
                                        int device_index = -1) {
    return at::musa::getStreamFromPool(non_blocking, device_index);
}
static inline int currentGPUDevice() { return at::musa::current_device(); }
static inline constexpr auto kGPUDevice = c10::DeviceType::PrivateUse1;
static inline constexpr auto kGPUDeviceType = at::musa::kMUSA;
#else
static inline auto getCurrentGPUStream(int device_index = -1) {
    return at::cuda::getCurrentCUDAStream(device_index);
}
static inline auto getGPUStreamFromPool(bool non_blocking = false,
                                        int device_index = -1) {
    return at::cuda::getStreamFromPool(non_blocking, device_index);
}
static inline int currentGPUDevice() { return at::cuda::current_device(); }
static inline constexpr auto kGPUDevice = torch::kCUDA;
static inline constexpr auto kGPUDeviceType = c10::DeviceType::CUDA;
#endif

}  // namespace mooncake

#endif  // MOONCAKE_PG_GPU_UTILS_H
