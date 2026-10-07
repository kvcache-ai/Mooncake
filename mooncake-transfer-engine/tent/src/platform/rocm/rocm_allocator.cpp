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

#include "tent/platform/rocm.h"
#include "tent/common/status.h"

#include <hip/hip_runtime.h>
#include <numa.h>
#include <glog/logging.h>
#include <vector>

namespace mooncake {
namespace tent {

Status RocmPlatform::allocate(void** pptr, size_t size,
                              MemoryOptions& options) {
    LocationParser location(options.location);
    if (isAmdGpuLocationType(location.type())) {
        int hip_dev = 0;
        CHECK_HIP(hipGetDevice(&hip_dev));
        CHECK_HIP(hipSetDevice(location.index()));
        CHECK_HIP(hipMalloc(pptr, size));
        hipSetDevice(hip_dev);
        return Status::OK();
    }
    int socket_id = 0;
    if (location.type() == "cpu") socket_id = location.index();
    *pptr = numa_alloc_onnode(size, socket_id);
    if (!(*pptr))
        return Status::InternalError("Unable to allocate DRAM memory");
    return Status::OK();
}

Status RocmPlatform::free(void* ptr, size_t size) {
    hipPointerAttribute_t attributes;
    CHECK_HIP(hipPointerGetAttributes(&attributes, ptr));
    if (attributes.type == hipMemoryTypeDevice) {
        CHECK_HIP(hipFree(ptr));
    } else if (attributes.type == hipMemoryTypeHost ||
               attributes.type == hipMemoryTypeUnregistered) {
        numa_free(ptr, size);
    } else {
        LOG(ERROR) << "Unknown memory type, " << ptr << " " << attributes.type;
    }
    return Status::OK();
}

Status RocmPlatform::copy(void* dst, void* src, size_t length) {
    // hipMemcpyAsync routes the copy through its stream's device context, so
    // the stream must live on the device owning the device-side buffer.
    // Control-plane RPC worker threads sit on device 0 while a registered
    // buffer may live on device R; taking the stream from the buffer's device
    // routes the copy correctly without mutating the calling thread's current
    // device. Host-only copies keep the current device.
    int device_id = getPointerDeviceId(dst);
    if (device_id == HIPStreamPool::kCurrentDevice) {
        device_id = getPointerDeviceId(src);
    }

    HIPStreamHandle stream;
    CHECK_STATUS(getStreamFromPool(stream, device_id));
    CHECK_HIP(hipMemcpyAsync(dst, src, length, hipMemcpyDefault, stream.get()));
    CHECK_HIP(hipStreamSynchronize(stream.get()));
    return Status::OK();
}

Status RocmPlatform::synchronizeDevices(const Topology* topology) {
    const std::vector<int> devices =
        topologyDeviceIndices(topology, Topology::MEM_ROCM);
    if (devices.empty()) return Status::OK();

    int device_count = 0;
    hipError_t err = hipGetDeviceCount(&device_count);
    if (err != hipSuccess || device_count <= 0) {
        if (err != hipSuccess) {
            LOG(WARNING) << "RocmPlatform::synchronizeDevices "
                            "hipGetDeviceCount failed: "
                         << hipGetErrorString(err);
            (void)hipGetLastError();
        }
        return Status::OK();
    }

    int saved = 0;
    const bool have_saved = hipGetDevice(&saved) == hipSuccess;
    if (!have_saved) (void)hipGetLastError();

    for (int device : devices) {
        if (device >= device_count) continue;
        err = hipSetDevice(device);
        if (err != hipSuccess) {
            LOG(WARNING) << "RocmPlatform::synchronizeDevices hipSetDevice("
                         << device << ") failed: " << hipGetErrorString(err);
            (void)hipGetLastError();
            continue;
        }
        err = hipDeviceSynchronize();
        if (err != hipSuccess) {
            LOG(WARNING)
                << "RocmPlatform::synchronizeDevices hipDeviceSynchronize "
                   "device "
                << device << " failed: " << hipGetErrorString(err);
            (void)hipGetLastError();
        }
    }
    if (have_saved) {
        err = hipSetDevice(saved);
        if (err != hipSuccess) {
            LOG(WARNING)
                << "RocmPlatform::synchronizeDevices restore hipSetDevice("
                << saved << ") failed: " << hipGetErrorString(err);
            (void)hipGetLastError();
        }
    }
    return Status::OK();
}

}  // namespace tent
}  // namespace mooncake
