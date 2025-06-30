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

#include "v1/utility/allocator.h"

#ifdef USE_CUDA
#include <bits/stdint-uintn.h>
#include <cuda_runtime.h>
#endif

namespace mooncake {
namespace v1 {

std::pair<std::string, int> parseKeyValue(const std::string &input) {
    size_t colonPos = input.find(':');
    if (colonPos == std::string::npos) return std::make_pair("", -1);
    std::string key = input.substr(0, colonPos);
    std::string valueStr = input.substr(colonPos + 1);
    try {
        int value = std::stoi(valueStr);
        return std::make_pair(key, value);
    } catch (const std::exception &e) {
        return std::make_pair("", -1);
    }
}

Status genericAllocateLocalMemory(void **pptr, size_t size,
                                  MemoryOptions &options) {
    auto result = parseKeyValue(options.location);
    if (result.first == "cuda") {
#ifdef USE_CUDA
        auto ret = cudaSetDevice(result.second);
        if (ret != cudaSuccess)
            return Status::InternalError("CUDA unable to set device" LOC_MARK);
        ret = cudaMalloc(pptr, size);
        if (ret != cudaSuccess)
            return Status::InternalError(
                "CUDA unable to malloc memory" LOC_MARK);
#else
        return Status::NotImplemented("CUDA feature not supported" LOC_MARK);
#endif
    } else if (result.first == "cpu") {
        *pptr = numa_alloc_onnode(size, result.second);
        if (!(*pptr))
            return Status::InternalError("Unable to allocate DRAM memory");
    }

    return Status::InternalError("Unknown device type in location");
}

Status genericFreeLocalMemory(void *ptr, size_t size) {
#ifdef USE_CUDA
    // Check pointer on GPU
    cudaPointerAttributes attributes;
    auto ret = cudaPointerGetAttributes(&attributes, addr);
    if (ret != cudaSuccess)
        return Status::InternalError(
            "CUDA failed to get pointer attributes" LOC_MARK);
    if (attributes.type == cudaMemoryTypeDevice) {
        cudaFree(addr);
    } else if (attributes.type == cudaMemoryTypeHost ||
               attributes.type == cudaMemoryTypeUnregistered) {
        numa_free(addr, size);
    } else {
        LOG(ERROR) << "Unknown memory type, " << addr << " " << attributes.type;
    }
#else
    numa_free(ptr, size);
#endif
    return Status::OK();
}
}  // namespace v1
}  // namespace mooncake