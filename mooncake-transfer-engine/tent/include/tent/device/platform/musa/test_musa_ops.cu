// Minimal compilation test for MUSA DeviceOps
// Compile with: mcc -mtgpu --musa-path=/usr/local/musa -DUSE_MUSA -I. -c test_musa_ops.cu -o test_musa_ops.o

#include <tent/device/ir/device_ops.cuh>
#include <tent/device/platform/musa/musa_ops.cuh>

using namespace mooncake::tent::device;
using namespace mooncake::tent::device::musa_platform;

// Test that all DeviceOps function pointers can be instantiated
__global__ void test_device_ops() {
    DeviceOps* ops = musaDeviceOps();

    // Store / load
    uint64_t src = 42, dst = 0;
    ops->store_relaxed(&dst, &src, sizeof(uint64_t));
    ops->store_release(&dst, &src, sizeof(uint64_t));
    ops->load_relaxed(&dst, &src, sizeof(uint64_t));
    ops->load_acquire(&dst, &src, sizeof(uint64_t));

    // Atomics
    ops->atomic_add_release(&dst, 1);
    uint64_t val = ops->atomic_load_acquire(&dst);
    (void)val;
    uint32_t old = ops->atomic_cas_acquire(reinterpret_cast<uint32_t*>(&dst), 0, 1);
    (void)old;

    // MMIO
    ops->mmio_write64(&dst, 0xDEADBEEF);
    ops->mmio_write32(reinterpret_cast<volatile uint32_t*>(&dst), 0xCAFE);

    // Fence
    ops->fence_acq_rel();

    // Spin wait
    ops->spin_wait_eq(reinterpret_cast<volatile void*>(&dst), 1);
    ops->spin_wait_ne(reinterpret_cast<volatile void*>(&dst), 0);

    // Memcpy
    ops->memcpy_async(&dst, &src, sizeof(uint64_t));
}
