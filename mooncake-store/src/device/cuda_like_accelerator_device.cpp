#include "device/accelerator_registry.h"
#include "pinned_buffer_pool.h"

#include "cuda_alike.h"

#if defined(USE_CUDA) || defined(USE_MUSA) || defined(USE_MACA) || \
    defined(USE_HYGON) || defined(USE_COREX)

namespace mooncake {
namespace device {
namespace {

void FreeCudaLikePinnedHostBuffer(void* addr) { cudaFreeHost(addr); }

class CudaLikeAcceleratorDevice final : public ProbeCachedAcceleratorDevice {
   public:
    explicit CudaLikeAcceleratorDevice(AcceleratorVendor vendor)
        : vendor_(vendor) {}

    AcceleratorVendor Vendor() const override { return vendor_; }

    bool ProbeAvailable() const override {
        int count = 0;
        return cudaGetDeviceCount(&count) == cudaSuccess && count > 0;
    }

    PointerInfo QueryPointer(const void* ptr) const override {
        cudaPointerAttributes attr{};
        if (cudaPointerGetAttributes(&attr, ptr) == cudaSuccess &&
            attr.type == cudaMemoryTypeDevice) {
            return PointerInfo{.kind = MemoryKind::kDevice,
                               .device_id = attr.device};
        }
        cudaGetLastError();
        return PointerInfo{.kind = MemoryKind::kHost, .device_id = -1};
    }

    int32_t CurrentDeviceId() const override {
        int device_id = -1;
        return cudaGetDevice(&device_id) == cudaSuccess ? device_id : -1;
    }

    void SetContext(int32_t device_id) const override {
        if (device_id >= 0) cudaSetDevice(device_id);
    }

    bool Copy(void* dst, const void* src, size_t size,
              CopyDirection direction) const override {
        cudaMemcpyKind kind = cudaMemcpyDefault;
        switch (direction) {
            case CopyDirection::kHostToDevice:
                kind = cudaMemcpyHostToDevice;
                break;
            case CopyDirection::kDeviceToHost:
                kind = cudaMemcpyDeviceToHost;
                break;
            case CopyDirection::kDeviceToDevice:
                kind = cudaMemcpyDeviceToDevice;
                break;
            case CopyDirection::kHostToHost:
            case CopyDirection::kAuto:
                kind = cudaMemcpyDefault;
                break;
        }
        return cudaMemcpy(dst, src, size, kind) == cudaSuccess;
    }

    PinnedHostBuffer AllocatePinnedHost(size_t size) const override {
        void* addr = nullptr;
        if (cudaMallocHost(&addr, size) != cudaSuccess) {
            cudaGetLastError();
            return PinnedHostBuffer();
        }
        return PinnedHostBuffer(addr, size, FreeCudaLikePinnedHostBuffer);
    }

   private:
    AcceleratorVendor vendor_;
};

#define REGISTER_CUDA_LIKE_ACCELERATOR_DEVICE(name, vendor) \
    const CudaLikeAcceleratorDevice name##_device(vendor);  \
    const AcceleratorDeviceRegistrar name##_registrar(name##_device)

#if defined(USE_CUDA)
REGISTER_CUDA_LIKE_ACCELERATOR_DEVICE(nvidia, AcceleratorVendor::kNvidia);
#endif

#if defined(USE_MUSA)
REGISTER_CUDA_LIKE_ACCELERATOR_DEVICE(musa, AcceleratorVendor::kMusa);
#endif

#if defined(USE_MACA)
REGISTER_CUDA_LIKE_ACCELERATOR_DEVICE(maca, AcceleratorVendor::kMaca);
#endif

#if defined(USE_HYGON)
REGISTER_CUDA_LIKE_ACCELERATOR_DEVICE(hygon, AcceleratorVendor::kHygon);
#endif

#if defined(USE_COREX)
REGISTER_CUDA_LIKE_ACCELERATOR_DEVICE(corex, AcceleratorVendor::kCorex);
#endif

#undef REGISTER_CUDA_LIKE_ACCELERATOR_DEVICE

}  // namespace
}  // namespace device
}  // namespace mooncake

#endif
