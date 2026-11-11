#include <cuda.h>
#include <cuda_runtime.h>
#include <sys/wait.h>
#include <unistd.h>

#include <array>
#include <cstdio>

#include "device/cuda_ipc_buffer.h"

namespace {

constexpr size_t kBytes = 4096;

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
    std::array<unsigned char, kBytes> actual{};
    if (cudaMemcpy(actual.data(), mapping->ptr(), actual.size(),
                   cudaMemcpyDeviceToHost) != cudaSuccess) {
        return 3;
    }
    for (size_t i = 0; i < actual.size(); ++i) {
        if (actual[i] != static_cast<unsigned char>(i)) return 4;
    }
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
