#include <cuda_runtime_api.h>
#include <dlfcn.h>

#include <array>
#include <atomic>
#include <cstdlib>

namespace {
std::array<void*, 1024> opened, close_attempts;
std::atomic<int> opens{0}, closes{0};
void* fail_once = nullptr;
}  // namespace

extern "C" int probe_opens() { return opens.load(); }
extern "C" int probe_closes() { return closes.load(); }
extern "C" void* probe_opened(int i) { return opened.at(i); }
extern "C" void* probe_closed(int i) { return close_attempts.at(i); }
extern "C" void probe_fail_once(void* ptr) { fail_once = ptr; }

extern "C" cudaError_t cudaIpcOpenMemHandle(void** ptr,
                                            cudaIpcMemHandle_t handle,
                                            unsigned flags) {
    using Fn = cudaError_t (*)(void**, cudaIpcMemHandle_t, unsigned);
    static auto fn =
        reinterpret_cast<Fn>(dlsym(RTLD_NEXT, "cudaIpcOpenMemHandle"));
    if (!fn) std::abort();
    auto error = fn(ptr, handle, flags);
    if (error == cudaSuccess) opened.at(opens.fetch_add(1)) = *ptr;
    return error;
}

extern "C" cudaError_t cudaIpcCloseMemHandle(void* ptr) {
    using Fn = cudaError_t (*)(void*);
    static auto fn =
        reinterpret_cast<Fn>(dlsym(RTLD_NEXT, "cudaIpcCloseMemHandle"));
    if (!fn) std::abort();
    close_attempts.at(closes.fetch_add(1)) = ptr;
    if (ptr == fail_once) {
        fail_once = nullptr;
        return cudaErrorUnknown;
    }
    return fn(ptr);
}
