// Copyright 2026 KVCache.AI
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

#include "tent/device/nvlink.h"

#include <cstring>

#include <cuda.h>
#include <cuda_runtime.h>

#include "tent/transport/p2p/p2p_device_transport_base.h"

namespace mooncake {
namespace tent {

namespace {

// NVLink transport: CUDA API + fabric memory support
using NvLinkDeviceTransportImpl = P2pDeviceTransportBase<CudaApiTraits, true>;

}  // namespace

std::unique_ptr<DeviceTransport> createNvLinkDeviceTransport() {
    return std::make_unique<NvLinkDeviceTransportImpl>();
}

bool nvLinkSupportsFabricMemory() {
    const char* nvlink_ipc = getenv("MC_USE_NVLINK_IPC");
    const bool fabric_enabled = nvlink_ipc && std::strcmp(nvlink_ipc, "0") == 0;
    if (!fabric_enabled) return false;

    int num_devices = 0;
    auto err = cudaGetDeviceCount(&num_devices);
    if (err != cudaSuccess || num_devices == 0) return false;

    for (int dev = 0; dev < num_devices; ++dev) {
        int supported = 0;
        auto res = cuDeviceGetAttribute(
            &supported, CU_DEVICE_ATTRIBUTE_HANDLE_TYPE_FABRIC_SUPPORTED, dev);
        if (res != CUDA_SUCCESS || !supported) return false;
    }
    return true;
}

}  // namespace tent
}  // namespace mooncake
