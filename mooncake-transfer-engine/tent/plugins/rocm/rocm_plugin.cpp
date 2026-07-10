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

#include <hip/hip_runtime.h>
#include <string.h>
#include <stdio.h>
#include <string>
#include <glog/logging.h>

struct rocm_plugin_ctx_t {
    // reserved
};

#define CHECK_HIP(call)                                                       \
    do {                                                                      \
        auto err = call;                                                      \
        if (err != hipSuccess) {                                              \
            LOG(ERROR) << std::string(#call) + ": " + hipGetErrorString(err); \
            return -1;                                                        \
        }                                                                     \
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

static void* rocm_create_plugin() {
    rocm_plugin_ctx_t* ctx = new rocm_plugin_ctx_t;
    return ctx;
}

static int rocm_destroy_plugin(void* handle) {
    if (!handle) return 0;
    delete reinterpret_cast<rocm_plugin_ctx_t*>(handle);
    return 0;
}

static int rocm_alloc(void* ctx_, void** pptr, size_t size, const char* loc) {
    (void)ctx_;
    LocationParser location(loc);
    if (location.type() != "rocm") return -1;
    int hip_dev = 0;
    CHECK_HIP(hipGetDevice(&hip_dev));
    CHECK_HIP(hipSetDevice(location.index()));
    CHECK_HIP(hipMalloc(pptr, size));
    CHECK_HIP(hipSetDevice(hip_dev));
    return 0;
}

static int rocm_free(void* ctx_, void* ptr, size_t size) {
    (void)ctx_;
    (void)size;
    hipPointerAttribute_t attrs;
    CHECK_HIP(hipPointerGetAttributes(&attrs, ptr));
    if (attrs.type == hipMemoryTypeDevice) {
        CHECK_HIP(hipFree(ptr));
        return 0;
    }
    return -2;
}

static int rocm_memcpy_sync(void* ctx_, void* dst, void* src, size_t length) {
    (void)ctx_;
    CHECK_HIP(hipMemcpy(dst, src, length, hipMemcpyDefault));
    return 0;
}

static int rocm_query_location(void* ctx_, void* addr, size_t size,
                               location_t* buf, size_t buf_count) {
    (void)ctx_;
    if (buf_count == 0) return -1;
    hipPointerAttribute_t attr{};
    hipError_t err = hipPointerGetAttributes(&attr, addr);
    if (err != hipSuccess || attr.type != hipMemoryTypeDevice) return 0;
    buf[0].start = addr;
    buf[0].length = size;
    snprintf(buf[0].location, LOCATION_LEN, "rocm:%d", attr.device);
    return 1;
}

static int rocm_get_device_count(void* ctx_) {
    (void)ctx_;
    int count = 0;
    CHECK_HIP(hipGetDeviceCount(&count));
    return count;
}

static int rocm_get_device_pci_bus_id(void* ctx_, int device_index,
                                      char* bus_id, size_t bus_id_len) {
    (void)ctx_;
    hipDeviceProp_t prop;
    CHECK_HIP(hipGetDeviceProperties(&prop, device_index));
    snprintf(bus_id, bus_id_len, "%04x:%02x:%02x.0", prop.pciDomainID,
             prop.pciBusID, prop.pciDeviceID);
    return 0;
}

extern "C" int tent_register_device_plugin(device_plugin_t* out) {
    if (!out) return -1;
    memset(out, 0, sizeof(*out));
    out->class_name = "rocm";
    out->create_plugin = rocm_create_plugin;
    out->destroy_plugin = rocm_destroy_plugin;
    out->alloc = rocm_alloc;
    out->free = rocm_free;
    out->memcpy_sync = rocm_memcpy_sync;
    out->query_location = rocm_query_location;
    out->get_device_count = rocm_get_device_count;
    out->get_device_pci_bus_id = rocm_get_device_pci_bus_id;
    return 0;
}
