#include <cuda.h>
#include <cuda_runtime.h>
#include <dlfcn.h>
#include <sys/wait.h>
#include <unistd.h>

#include <array>
#include <cstdio>
#include <limits>
#include <utility>

#include "device/cuda_ipc_buffer.h"

namespace {

constexpr size_t kBytes = 4096;
bool fail_next_close = false;

}  // namespace

// Fail one importer close before calling the CUDA runtime. All other closes
// exercise the real CUDA IPC mapping opened by this process.
extern "C" cudaError_t CUDARTAPI cudaIpcCloseMemHandle(void *ptr) {
    if (std::exchange(fail_next_close, false)) return cudaErrorUnknown;
    static auto close_handle =
        reinterpret_cast<decltype(&cudaIpcCloseMemHandle)>(
            dlsym(RTLD_NEXT, "cudaIpcCloseMemHandle"));
    return close_handle ? close_handle(ptr)
                        : cudaErrorSharedObjectSymbolNotFound;
}

namespace {

template <typename T>
bool Transfer(int fd, T &value, bool writing) {
    auto *cursor = reinterpret_cast<char *>(&value);
    size_t remaining = sizeof(value);
    while (remaining != 0) {
        const auto count = writing ? write(fd, cursor, remaining)
                                   : read(fd, cursor, remaining);
        if (count <= 0) return false;
        cursor += count;
        remaining -= count;
    }
    return true;
}

int Import(int fd) {
    // This process was forked before the exporter made any CUDA calls.
    int count = 0;
    if (cuDeviceGetCount(&count) != CUDA_ERROR_NOT_INITIALIZED) return 1;
    mooncake::CudaIpcBufferHandle handle;
    if (!Transfer(fd, handle, false)) return 77;
    auto mapping = mooncake::device::CudaIpcBufferMapping::Open(handle);
    if (!mapping) return 2;
    if (mapping->base() == nullptr || mapping->allocation_size() < kBytes ||
        mapping->ptr() != static_cast<char *>(mapping->base()) + handle.offset)
        return 5;
    auto moved = std::move(*mapping);
    if (mapping->base() != nullptr || mapping->allocation_size() != 0) return 6;
    std::array<unsigned char, kBytes> actual{};
    if (cudaMemcpy(actual.data(), moved.ptr(), actual.size(),
                   cudaMemcpyDeviceToHost) != cudaSuccess) {
        return 3;
    }
    for (size_t i = 0; i < actual.size(); ++i) {
        if (actual[i] != static_cast<unsigned char>(i)) return 4;
    }
    auto base = moved.base();
    fail_next_close = true;
    if (moved.Close() || moved.base() != base || moved.ptr() == nullptr ||
        moved.allocation_size() < kBytes)
        return 7;
    if (!moved.Close() || moved.base() != nullptr || moved.ptr() != nullptr ||
        moved.allocation_size() != 0 || !moved.Close())
        return 8;
    // Validation can fail after the driver has opened a real IPC import.
    handle.size = std::numeric_limits<uint64_t>::max() - handle.offset;
    mooncake::device::CudaIpcBufferMapping invalid;
    if (invalid.OpenInto(handle) || !invalid.base() || invalid.ptr() ||
        !invalid.Close())
        return 9;
    return 0;
}

int Export(int fd, void *&allocation) {
    int count = 0;
    if (cudaGetDeviceCount(&count) != cudaSuccess || count == 0) return 77;
    if (cudaMalloc(&allocation, kBytes) != cudaSuccess) return 1;
    std::array<unsigned char, kBytes> expected{};
    for (size_t i = 0; i < expected.size(); ++i) {
        expected[i] = static_cast<unsigned char>(i);
    }
    if (cudaMemcpy(allocation, expected.data(), expected.size(),
                   cudaMemcpyHostToDevice) != cudaSuccess) {
        return 2;
    }
    auto handle = mooncake::device::ExportCudaIpcBuffer(allocation, kBytes);
    if (!handle || !handle->has_device_uuid) return 3;
    // An invalid exporter ordinal forces the importer to use the UUID.
    handle->device_id = -1;
    return Transfer(fd, *handle, true) ? 0 : 4;
}

}  // namespace

int main() {
    int channel[2];
    if (pipe(channel) != 0) return 1;
    // Fork before even checking device availability: CUDA initialization in
    // the exporter must not hide the importer's initialization requirement.
    const auto child = fork();
    if (child < 0) return 1;
    if (child == 0) {
        close(channel[1]);
        const int result = Import(channel[0]);
        close(channel[0]);
        _exit(result);
    }
    close(channel[0]);
    void *allocation = nullptr;
    const int exported = Export(channel[1], allocation);
    close(channel[1]);
    int status = 0;
    const bool waited = waitpid(child, &status, 0) == child;
    if (allocation != nullptr) cudaFree(allocation);
    if (exported == 77) {
        std::puts("SKIP: no CUDA device available");
        return 77;
    }
    if (exported != 0 || !waited || !WIFEXITED(status) ||
        WEXITSTATUS(status) != 0) {
        std::fprintf(stderr,
                     "CUDA IPC failed: exporter=%d importer_status=%d\n",
                     exported, status);
        return 1;
    }
    std::puts(
        "PASS: uninitialized importer resolved UUID and read exact bytes");
    return 0;
}
