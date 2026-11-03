// Copyright 2025 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "tent/device_plugin.h"
#include "tent/platform/cuda_utils.h"

#include <cuda_runtime.h>
#include <string.h>
#include <stdio.h>
#include <string>
#include <glog/logging.h>

using mooncake::tent::getCudaDeviceForPtr;

struct cuda_plugin_ctx_t {
    // reserved
};

#define CHECK_CUDA(call)                                                       \
    do {                                                                       \
        auto err = call;                                                       \
        if (err != cudaSuccess) {                                              \
            LOG(ERROR) << std::string(#call) + ": " + cudaGetErrorString(err); \
            return -1;                                                         \
        }                                                                      \
    } while (0)

class LocationParser {
   public:
    LocationParser(const std::string& location) {
        size_t colonPos = location.find(':');
        if (colonPos == std::string::npos) {
            index_ = -1;
            return;
        }
        std::string type = location.substr(0, colonPos);
        std::string indexStr = location.substr(colonPos + 1);
        try {
            type_ = type;
            index_ = std::stoi(indexStr);
        } catch (const std::exception& e) {
            index_ = -1;
        }
    }

    std::string type() const { return type_; }

    int index() const { return index_; }

   private:
    std::string type_;
    int index_;
};

static void* cuda_create_plugin() {
    cuda_plugin_ctx_t* ctx = new cuda_plugin_ctx_t;
    return ctx;
}

static int cuda_destroy_plugin(void* handle) {
    if (!handle) return 0;
    delete reinterpret_cast<cuda_plugin_ctx_t*>(handle);
    return 0;
}

static int cuda_alloc(void* ctx_, void** pptr, size_t size, const char* loc) {
    (void)ctx_;
    LocationParser location(loc);
    if (location.type() != "cuda") return -1;
    int cuda_dev = 0;
    CHECK_CUDA(cudaGetDevice(&cuda_dev));
    CHECK_CUDA(cudaSetDevice(location.index()));
    CHECK_CUDA(cudaMalloc(pptr, size));
    CHECK_CUDA(cudaSetDevice(cuda_dev));
    return 0;
}

static int cuda_free(void* ctx_, void* ptr, size_t size) {
    (void)ctx_;
    (void)size;
    if (getCudaDeviceForPtr(ptr) < 0) return -2;
    CHECK_CUDA(cudaFree(ptr));
    return 0;
}

static int cuda_memcpy_sync(void* ctx_, void* dst, void* src, size_t length) {
    (void)ctx_;
    // Pin the owning device so the runtime does not default to GPU 0.
    int dev = getCudaDeviceForPtr(src);
    if (dev < 0) dev = getCudaDeviceForPtr(dst);
    if (dev < 0) return -1;  // host-to-host; let the caller fall back

    // Restore the exact caller state, as CudaPlatform::copy() does. An ordinal
    // cannot distinguish a custom context from its device's primary context,
    // and a caller that started without a context must remain without one.
    CUcontext caller_ctx = nullptr;
    CUresult ctx_result = cuCtxGetCurrent(&caller_ctx);
    if (ctx_result != CUDA_SUCCESS) {
        LOG(ERROR) << "CUDA plugin cuCtxGetCurrent: " << ctx_result;
        return TENT_DEVICE_PLUGIN_ERROR;
    }
    int caller_device = -1;
    // Bare cudaGetDevice() may initialize GPU 0; only query an existing
    // binding.
    if (caller_ctx != nullptr && cudaGetDevice(&caller_device) != cudaSuccess) {
        LOG(ERROR) << "CUDA plugin cudaGetDevice failed before copying";
        return TENT_DEVICE_PLUGIN_ERROR;
    }

    cudaError_t err = cudaSetDevice(dev);
    if (err == cudaSuccess)
        err = cudaMemcpy(dst, src, length, cudaMemcpyDefault);
    int result = err == cudaSuccess ? 0 : TENT_DEVICE_PLUGIN_ERROR;
    if (err != cudaSuccess)
        LOG(ERROR) << "CUDA plugin copy: " << cudaGetErrorString(err);

    // Do not return early after switching: both restore steps must be attempted
    // even if the copy or restoring the runtime binding failed.
    if (caller_device != -1 && caller_device != dev) {
        err = cudaSetDevice(caller_device);
        if (err != cudaSuccess) {
            LOG(ERROR) << "CUDA plugin cudaSetDevice(restore): "
                       << cudaGetErrorString(err);
            result = TENT_DEVICE_PLUGIN_ERROR;
        }
    }
    ctx_result = cuCtxSetCurrent(caller_ctx);
    if (ctx_result != CUDA_SUCCESS) {
        LOG(ERROR) << "CUDA plugin cuCtxSetCurrent(restore): " << ctx_result;
        result = TENT_DEVICE_PLUGIN_ERROR;
    }
    return result;
}

static int cuda_query_location(void* ctx_, void* addr, size_t size,
                               location_t* buf, size_t buf_count) {
    (void)ctx_;
    if (buf_count == 0) return -1;
    int dev = getCudaDeviceForPtr(addr);
    if (dev < 0) return 0;
    buf[0].start = addr;
    buf[0].length = size;
    snprintf(buf[0].location, LOCATION_LEN, "cuda:%d", dev);
    return 1;
}

static int cuda_get_device_count(void* ctx_) {
    (void)ctx_;
    int count = 0;
    CHECK_CUDA(cudaGetDeviceCount(&count));
    return count;
}

static int cuda_get_device_pci_bus_id(void* ctx_, int device_index,
                                      char* bus_id, size_t bus_id_len) {
    (void)ctx_;
    CHECK_CUDA(cudaDeviceGetPCIBusId(bus_id, bus_id_len, device_index));
    return 0;
}

extern "C" int tent_register_device_plugin(device_plugin_t* out) {
    if (!out) return -1;
    memset(out, 0, sizeof(*out));
    out->class_name = "cuda";
    out->create_plugin = cuda_create_plugin;
    out->destroy_plugin = cuda_destroy_plugin;
    out->alloc = cuda_alloc;
    out->free = cuda_free;
    out->memcpy_sync = cuda_memcpy_sync;
    out->query_location = cuda_query_location;
    out->get_device_count = cuda_get_device_count;
    out->get_device_pci_bus_id = cuda_get_device_pci_bus_id;
    return 0;
}
