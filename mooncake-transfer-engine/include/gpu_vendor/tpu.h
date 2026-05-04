#pragma once

#include <dlfcn.h>

#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <mutex>
#include <string>

const static std::string GPU_PREFIX = "tpu:";

using cudaError_t = int;
using cudaMemcpyKind = int;
using cudaStream_t = void *;

struct cudaPointerAttributes {
    int type;
    int device;
    size_t size;
};

using CUdevice = int;
using CUdeviceptr = uintptr_t;
using CUmemorytype = int;
using CUresult = int;

static constexpr cudaError_t cudaSuccess = 0;
static constexpr cudaError_t cudaErrorInvalidValue = 1;
static constexpr cudaError_t cudaErrorNotSupported = 801;
static constexpr cudaError_t cudaErrorSharedObjectSymbolNotFound = 802;
static constexpr cudaError_t cudaErrorSharedObjectInitFailed = 803;

static constexpr int cudaMemoryTypeUnregistered = 0;
static constexpr int cudaMemoryTypeHost = 1;
static constexpr int cudaMemoryTypeDevice = 2;

static constexpr int cudaMemcpyHostToHost = 0;
static constexpr int cudaMemcpyHostToDevice = 1;
static constexpr int cudaMemcpyDeviceToHost = 2;
static constexpr int cudaMemcpyDeviceToDevice = 3;
static constexpr int cudaMemcpyDefault = 4;

static constexpr CUresult CUDA_SUCCESS = cudaSuccess;
static constexpr int CU_DEVICE_ATTRIBUTE_DMA_BUF_SUPPORTED = 1;
static constexpr int CU_MEM_RANGE_HANDLE_TYPE_DMA_BUF_FD = 1;
static constexpr int CU_MEMORYTYPE_HOST = 1;
static constexpr int CU_MEMORYTYPE_DEVICE = 2;
static constexpr int CU_POINTER_ATTRIBUTE_MEMORY_TYPE = 1;
static constexpr int CU_POINTER_ATTRIBUTE_RANGE_SIZE = 2;

extern "C" {

typedef struct mc_tpu_pointer_attributes {
    int type;
    int device;
    size_t size;
} mc_tpu_pointer_attributes_t;

int mc_tpu_adapter_init(uint32_t abi_version);
const char *mc_tpu_adapter_get_error_string(int error);
int mc_tpu_adapter_get_device_count(int *count);
int mc_tpu_adapter_get_device(int *device);
int mc_tpu_adapter_set_device(int device);
int mc_tpu_adapter_device_get_pci_bus_id(char *pci_bus_id, int len, int device);
int mc_tpu_adapter_pointer_get_attributes(mc_tpu_pointer_attributes_t *attr,
                                          const void *ptr);
int mc_tpu_adapter_malloc(void **ptr, size_t bytes);
int mc_tpu_adapter_free(void *ptr);
int mc_tpu_adapter_malloc_host(void **ptr, size_t bytes);
int mc_tpu_adapter_free_host(void *ptr);
int mc_tpu_adapter_memcpy(void *dst, const void *src, size_t bytes, int kind);
int mc_tpu_adapter_memcpy_async(void *dst, const void *src, size_t bytes,
                                int kind, void *stream);
int mc_tpu_adapter_memset(void *ptr, int value, size_t bytes);
int mc_tpu_adapter_stream_create(void **stream);
int mc_tpu_adapter_stream_destroy(void *stream);
int mc_tpu_adapter_stream_synchronize(void *stream);
int mc_tpu_adapter_device_can_access_peer(int *can_access, int device,
                                          int peer_device);
int mc_tpu_adapter_device_get_attribute(int *value, int attribute, int device);
int mc_tpu_adapter_mem_get_handle_for_address_range(void *handle, uintptr_t ptr,
                                                    size_t size,
                                                    int handle_type,
                                                    unsigned long long flags);

}  // extern "C"

namespace mooncake::tpu {
namespace detail {

static constexpr uint32_t kAdapterAbiVersion = 1;

using InitFn = int (*)(uint32_t);
using GetErrorStringFn = const char *(*)(int);
using GetDeviceCountFn = int (*)(int *);
using GetDeviceFn = int (*)(int *);
using SetDeviceFn = int (*)(int);
using DeviceGetPciBusIdFn = int (*)(char *, int, int);
using PointerGetAttributesFn = int (*)(mc_tpu_pointer_attributes_t *,
                                       const void *);
using MallocFn = int (*)(void **, size_t);
using FreeFn = int (*)(void *);
using MemcpyFn = int (*)(void *, const void *, size_t, int);
using MemcpyAsyncFn = int (*)(void *, const void *, size_t, int, void *);
using MemsetFn = int (*)(void *, int, size_t);
using StreamCreateFn = int (*)(void **);
using StreamDestroyFn = int (*)(void *);
using StreamSynchronizeFn = int (*)(void *);
using DeviceCanAccessPeerFn = int (*)(int *, int, int);
using DeviceGetAttributeFn = int (*)(int *, int, int);
using MemGetHandleForAddressRangeFn = int (*)(void *, uintptr_t, size_t, int,
                                              unsigned long long);

struct AdapterApi {
    void *handle = nullptr;
    int load_error = cudaSuccess;
    InitFn init = nullptr;
    GetErrorStringFn get_error_string = nullptr;
    GetDeviceCountFn get_device_count = nullptr;
    GetDeviceFn get_device = nullptr;
    SetDeviceFn set_device = nullptr;
    DeviceGetPciBusIdFn device_get_pci_bus_id = nullptr;
    PointerGetAttributesFn pointer_get_attributes = nullptr;
    MallocFn malloc = nullptr;
    FreeFn free = nullptr;
    MallocFn malloc_host = nullptr;
    FreeFn free_host = nullptr;
    MemcpyFn memcpy = nullptr;
    MemcpyAsyncFn memcpy_async = nullptr;
    MemsetFn memset = nullptr;
    StreamCreateFn stream_create = nullptr;
    StreamDestroyFn stream_destroy = nullptr;
    StreamSynchronizeFn stream_synchronize = nullptr;
    DeviceCanAccessPeerFn device_can_access_peer = nullptr;
    DeviceGetAttributeFn device_get_attribute = nullptr;
    MemGetHandleForAddressRangeFn mem_get_handle_for_address_range = nullptr;
};

inline thread_local int &lastError() {
    static thread_local int error = cudaSuccess;
    return error;
}

inline int setLastError(int error) {
    lastError() = error;
    return error;
}

inline const char *defaultErrorString(int error) {
    switch (error) {
        case cudaSuccess:
            return "success";
        case cudaErrorInvalidValue:
            return "invalid value";
        case cudaErrorNotSupported:
            return "TPU adapter is unavailable or incomplete";
        case cudaErrorSharedObjectSymbolNotFound:
            return "TPU adapter symbol not found";
        case cudaErrorSharedObjectInitFailed:
            return "TPU adapter initialization failed";
        default:
            return "unknown TPU adapter error";
    }
}

template <typename Fn>
inline Fn loadSymbol(void *handle, const char *symbol, int &error,
                     bool required = true) {
    auto fn = reinterpret_cast<Fn>(dlsym(handle, symbol));
    if (!fn && required && error == cudaSuccess) {
        error = cudaErrorSharedObjectSymbolNotFound;
    }
    return fn;
}

inline AdapterApi loadApi() {
    AdapterApi api;

    const char *adapter_path = std::getenv("MC_TPU_ADAPTER_LIB");
    if (!adapter_path || adapter_path[0] == '\0') {
        adapter_path = "libmooncake_tpu_adapter.so";
    }

    api.handle = dlopen(adapter_path, RTLD_NOW | RTLD_LOCAL);
    if (!api.handle) {
        api.load_error = cudaErrorNotSupported;
        return api;
    }

    api.init =
        loadSymbol<InitFn>(api.handle, "mc_tpu_adapter_init", api.load_error);
    api.get_error_string = loadSymbol<GetErrorStringFn>(
        api.handle, "mc_tpu_adapter_get_error_string", api.load_error, false);
    api.get_device_count = loadSymbol<GetDeviceCountFn>(
        api.handle, "mc_tpu_adapter_get_device_count", api.load_error);
    api.get_device = loadSymbol<GetDeviceFn>(
        api.handle, "mc_tpu_adapter_get_device", api.load_error);
    api.set_device = loadSymbol<SetDeviceFn>(
        api.handle, "mc_tpu_adapter_set_device", api.load_error);
    api.device_get_pci_bus_id = loadSymbol<DeviceGetPciBusIdFn>(
        api.handle, "mc_tpu_adapter_device_get_pci_bus_id", api.load_error);
    api.pointer_get_attributes = loadSymbol<PointerGetAttributesFn>(
        api.handle, "mc_tpu_adapter_pointer_get_attributes", api.load_error);
    api.malloc = loadSymbol<MallocFn>(api.handle, "mc_tpu_adapter_malloc",
                                      api.load_error);
    api.free =
        loadSymbol<FreeFn>(api.handle, "mc_tpu_adapter_free", api.load_error);
    api.malloc_host = loadSymbol<MallocFn>(
        api.handle, "mc_tpu_adapter_malloc_host", api.load_error, false);
    api.free_host = loadSymbol<FreeFn>(api.handle, "mc_tpu_adapter_free_host",
                                       api.load_error, false);
    api.memcpy = loadSymbol<MemcpyFn>(api.handle, "mc_tpu_adapter_memcpy",
                                      api.load_error);
    api.memcpy_async = loadSymbol<MemcpyAsyncFn>(
        api.handle, "mc_tpu_adapter_memcpy_async", api.load_error, false);
    api.memset = loadSymbol<MemsetFn>(api.handle, "mc_tpu_adapter_memset",
                                      api.load_error);
    api.stream_create = loadSymbol<StreamCreateFn>(
        api.handle, "mc_tpu_adapter_stream_create", api.load_error, false);
    api.stream_destroy = loadSymbol<StreamDestroyFn>(
        api.handle, "mc_tpu_adapter_stream_destroy", api.load_error, false);
    api.stream_synchronize = loadSymbol<StreamSynchronizeFn>(
        api.handle, "mc_tpu_adapter_stream_synchronize", api.load_error, false);
    api.device_can_access_peer = loadSymbol<DeviceCanAccessPeerFn>(
        api.handle, "mc_tpu_adapter_device_can_access_peer", api.load_error,
        false);
    api.device_get_attribute = loadSymbol<DeviceGetAttributeFn>(
        api.handle, "mc_tpu_adapter_device_get_attribute", api.load_error,
        false);
    api.mem_get_handle_for_address_range =
        loadSymbol<MemGetHandleForAddressRangeFn>(
            api.handle, "mc_tpu_adapter_mem_get_handle_for_address_range",
            api.load_error, false);

    if (api.load_error != cudaSuccess) {
        dlclose(api.handle);
        api.handle = nullptr;
        return api;
    }

    const int init_ret = api.init(kAdapterAbiVersion);
    if (init_ret != cudaSuccess) {
        api.load_error = init_ret == cudaSuccess
                             ? cudaErrorSharedObjectInitFailed
                             : init_ret;
        dlclose(api.handle);
        api.handle = nullptr;
    }

    return api;
}

inline AdapterApi &api() {
    static AdapterApi api = loadApi();
    return api;
}

inline int unavailableError() {
    return api().load_error == cudaSuccess ? cudaErrorNotSupported
                                           : api().load_error;
}

inline bool copyPointerAttributes(cudaPointerAttributes *dst,
                                  const mc_tpu_pointer_attributes_t &src) {
    if (!dst) {
        setLastError(cudaErrorInvalidValue);
        return false;
    }
    dst->type = src.type;
    dst->device = src.device;
    dst->size = src.size;
    return true;
}

}  // namespace detail
}  // namespace mooncake::tpu

static inline const char *cudaGetErrorString(cudaError_t error) {
    auto &api = mooncake::tpu::detail::api();
    if (api.get_error_string) {
        return api.get_error_string(error);
    }
    return mooncake::tpu::detail::defaultErrorString(error);
}

static inline cudaError_t cudaGetLastError() {
    return mooncake::tpu::detail::lastError();
}

static inline cudaError_t cudaGetDeviceCount(int *count) {
    if (!count) {
        return mooncake::tpu::detail::setLastError(cudaErrorInvalidValue);
    }
    auto &api = mooncake::tpu::detail::api();
    if (!api.get_device_count) {
        *count = 0;
        return mooncake::tpu::detail::setLastError(
            mooncake::tpu::detail::unavailableError());
    }
    return mooncake::tpu::detail::setLastError(api.get_device_count(count));
}

static inline cudaError_t cudaGetDevice(int *device) {
    if (!device) {
        return mooncake::tpu::detail::setLastError(cudaErrorInvalidValue);
    }
    auto &api = mooncake::tpu::detail::api();
    if (!api.get_device) {
        return mooncake::tpu::detail::setLastError(
            mooncake::tpu::detail::unavailableError());
    }
    return mooncake::tpu::detail::setLastError(api.get_device(device));
}

static inline cudaError_t cudaSetDevice(int device) {
    auto &api = mooncake::tpu::detail::api();
    if (!api.set_device) {
        return mooncake::tpu::detail::setLastError(
            mooncake::tpu::detail::unavailableError());
    }
    return mooncake::tpu::detail::setLastError(api.set_device(device));
}

static inline cudaError_t cudaDeviceGetPCIBusId(char *pci_bus_id, int len,
                                                int device) {
    if (!pci_bus_id || len <= 0) {
        return mooncake::tpu::detail::setLastError(cudaErrorInvalidValue);
    }
    auto &api = mooncake::tpu::detail::api();
    if (!api.device_get_pci_bus_id) {
        return mooncake::tpu::detail::setLastError(
            mooncake::tpu::detail::unavailableError());
    }
    return mooncake::tpu::detail::setLastError(
        api.device_get_pci_bus_id(pci_bus_id, len, device));
}

static inline cudaError_t cudaPointerGetAttributes(cudaPointerAttributes *attr,
                                                   const void *ptr) {
    if (!attr || !ptr) {
        return mooncake::tpu::detail::setLastError(cudaErrorInvalidValue);
    }
    auto &api = mooncake::tpu::detail::api();
    if (!api.pointer_get_attributes) {
        return mooncake::tpu::detail::setLastError(
            mooncake::tpu::detail::unavailableError());
    }
    mc_tpu_pointer_attributes_t adapter_attr{};
    auto ret = api.pointer_get_attributes(&adapter_attr, ptr);
    if (ret == cudaSuccess) {
        mooncake::tpu::detail::copyPointerAttributes(attr, adapter_attr);
    }
    return mooncake::tpu::detail::setLastError(ret);
}

static inline cudaError_t cudaMalloc(void **ptr, size_t bytes) {
    if (!ptr) {
        return mooncake::tpu::detail::setLastError(cudaErrorInvalidValue);
    }
    auto &api = mooncake::tpu::detail::api();
    if (!api.malloc) {
        return mooncake::tpu::detail::setLastError(
            mooncake::tpu::detail::unavailableError());
    }
    return mooncake::tpu::detail::setLastError(api.malloc(ptr, bytes));
}

static inline cudaError_t cudaFree(void *ptr) {
    auto &api = mooncake::tpu::detail::api();
    if (!api.free) {
        return mooncake::tpu::detail::setLastError(
            mooncake::tpu::detail::unavailableError());
    }
    return mooncake::tpu::detail::setLastError(api.free(ptr));
}

static inline cudaError_t cudaMallocHost(void **ptr, size_t bytes) {
    if (!ptr) {
        return mooncake::tpu::detail::setLastError(cudaErrorInvalidValue);
    }
    auto &api = mooncake::tpu::detail::api();
    if (api.malloc_host) {
        return mooncake::tpu::detail::setLastError(api.malloc_host(ptr, bytes));
    }
    *ptr = std::malloc(bytes);
    return mooncake::tpu::detail::setLastError(*ptr ? cudaSuccess
                                                    : cudaErrorInvalidValue);
}

static inline cudaError_t cudaFreeHost(void *ptr) {
    auto &api = mooncake::tpu::detail::api();
    if (api.free_host) {
        return mooncake::tpu::detail::setLastError(api.free_host(ptr));
    }
    std::free(ptr);
    return mooncake::tpu::detail::setLastError(cudaSuccess);
}

static inline cudaError_t cudaMemcpy(void *dst, const void *src, size_t bytes,
                                     int kind) {
    if (!dst || !src) {
        return mooncake::tpu::detail::setLastError(cudaErrorInvalidValue);
    }
    auto &api = mooncake::tpu::detail::api();
    if (!api.memcpy) {
        return mooncake::tpu::detail::setLastError(
            mooncake::tpu::detail::unavailableError());
    }
    return mooncake::tpu::detail::setLastError(
        api.memcpy(dst, src, bytes, kind));
}

static inline cudaError_t cudaMemcpyAsync(void *dst, const void *src,
                                          size_t bytes, int kind,
                                          cudaStream_t stream) {
    auto &api = mooncake::tpu::detail::api();
    if (api.memcpy_async) {
        return mooncake::tpu::detail::setLastError(
            api.memcpy_async(dst, src, bytes, kind, stream));
    }
    return cudaMemcpy(dst, src, bytes, kind);
}

static inline cudaError_t cudaMemset(void *ptr, int value, size_t bytes) {
    if (!ptr) {
        return mooncake::tpu::detail::setLastError(cudaErrorInvalidValue);
    }
    auto &api = mooncake::tpu::detail::api();
    if (!api.memset) {
        return mooncake::tpu::detail::setLastError(
            mooncake::tpu::detail::unavailableError());
    }
    return mooncake::tpu::detail::setLastError(api.memset(ptr, value, bytes));
}

static inline cudaError_t cudaStreamCreate(cudaStream_t *stream) {
    if (!stream) {
        return mooncake::tpu::detail::setLastError(cudaErrorInvalidValue);
    }
    auto &api = mooncake::tpu::detail::api();
    if (!api.stream_create) {
        *stream = nullptr;
        return mooncake::tpu::detail::setLastError(cudaSuccess);
    }
    return mooncake::tpu::detail::setLastError(
        api.stream_create(reinterpret_cast<void **>(stream)));
}

static inline cudaError_t cudaStreamDestroy(cudaStream_t stream) {
    auto &api = mooncake::tpu::detail::api();
    if (!api.stream_destroy) {
        return mooncake::tpu::detail::setLastError(cudaSuccess);
    }
    return mooncake::tpu::detail::setLastError(api.stream_destroy(stream));
}

static inline cudaError_t cudaStreamSynchronize(cudaStream_t stream) {
    auto &api = mooncake::tpu::detail::api();
    if (!api.stream_synchronize) {
        return mooncake::tpu::detail::setLastError(cudaSuccess);
    }
    return mooncake::tpu::detail::setLastError(api.stream_synchronize(stream));
}

static inline cudaError_t cudaDeviceCanAccessPeer(int *can_access, int device,
                                                  int peer_device) {
    if (!can_access) {
        return mooncake::tpu::detail::setLastError(cudaErrorInvalidValue);
    }
    auto &api = mooncake::tpu::detail::api();
    if (!api.device_can_access_peer) {
        *can_access = 0;
        return mooncake::tpu::detail::setLastError(cudaSuccess);
    }
    return mooncake::tpu::detail::setLastError(
        api.device_can_access_peer(can_access, device, peer_device));
}

static inline CUresult cuDeviceGet(CUdevice *device, int ordinal) {
    if (!device) {
        return mooncake::tpu::detail::setLastError(cudaErrorInvalidValue);
    }
    *device = ordinal;
    return mooncake::tpu::detail::setLastError(cudaSuccess);
}

static inline CUresult cuDeviceGetAttribute(int *value, int attribute,
                                            CUdevice device) {
    if (!value) {
        return mooncake::tpu::detail::setLastError(cudaErrorInvalidValue);
    }
    auto &api = mooncake::tpu::detail::api();
    if (!api.device_get_attribute) {
        return mooncake::tpu::detail::setLastError(
            mooncake::tpu::detail::unavailableError());
    }
    return mooncake::tpu::detail::setLastError(
        api.device_get_attribute(value, attribute, device));
}

static inline CUresult cuGetErrorString(CUresult error, const char **err_str) {
    if (!err_str) {
        return mooncake::tpu::detail::setLastError(cudaErrorInvalidValue);
    }
    *err_str = cudaGetErrorString(error);
    return mooncake::tpu::detail::setLastError(cudaSuccess);
}

static inline CUresult cuPointerGetAttribute(void *data, int attribute,
                                             CUdeviceptr ptr) {
    if (!data) {
        return mooncake::tpu::detail::setLastError(cudaErrorInvalidValue);
    }

    cudaPointerAttributes attributes{};
    const auto ret = cudaPointerGetAttributes(
        &attributes, reinterpret_cast<const void *>(ptr));
    if (ret != cudaSuccess) {
        return ret;
    }

    switch (attribute) {
        case CU_POINTER_ATTRIBUTE_MEMORY_TYPE: {
            auto *memory_type = static_cast<CUmemorytype *>(data);
            if (attributes.type == cudaMemoryTypeHost) {
                *memory_type = CU_MEMORYTYPE_HOST;
            } else if (attributes.type == cudaMemoryTypeDevice) {
                *memory_type = CU_MEMORYTYPE_DEVICE;
            } else {
                return mooncake::tpu::detail::setLastError(
                    cudaErrorInvalidValue);
            }
            return mooncake::tpu::detail::setLastError(cudaSuccess);
        }
        case CU_POINTER_ATTRIBUTE_RANGE_SIZE: {
            if (attributes.type != cudaMemoryTypeDevice) {
                return mooncake::tpu::detail::setLastError(
                    cudaErrorInvalidValue);
            }
            *static_cast<size_t *>(data) = attributes.size;
            return mooncake::tpu::detail::setLastError(cudaSuccess);
        }
        default:
            return mooncake::tpu::detail::setLastError(cudaErrorInvalidValue);
    }
}

static inline CUresult cuMemGetHandleForAddressRange(void *handle,
                                                     CUdeviceptr ptr,
                                                     size_t size,
                                                     int handle_type,
                                                     unsigned long long flags) {
    auto &api = mooncake::tpu::detail::api();
    if (!api.mem_get_handle_for_address_range) {
        return mooncake::tpu::detail::setLastError(
            mooncake::tpu::detail::unavailableError());
    }
    return mooncake::tpu::detail::setLastError(
        api.mem_get_handle_for_address_range(handle, ptr, size, handle_type,
                                             flags));
}
