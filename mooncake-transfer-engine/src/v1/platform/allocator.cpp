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

#include "v1/platform/allocator.h"

#ifdef USE_CUDA
#include <bits/stdint-uintn.h>
#include <cuda_runtime.h>
#endif
#include <numa.h>
#include <glog/logging.h>

namespace mooncake {
namespace v1 {

std::pair<std::string, int> parseLocation(const std::string &location) {
    size_t colonPos = location.find(':');
    if (colonPos == std::string::npos) return std::make_pair("", -1);
    std::string type = location.substr(0, colonPos);
    std::string indexStr = location.substr(colonPos + 1);
    try {
        return std::make_pair(type, std::stoi(indexStr));
    } catch (const std::exception &e) {
        return std::make_pair("", -1);
    }
}

Status genericAllocateLocalMemory(void **pptr, size_t size,
                                  MemoryOptions &options) {
    auto result = parseLocation(options.location);
    if (result.first == "cuda") {
#ifdef USE_CUDA
        int cuda_dev = 0;
        CHECK_CUDA(cudaGetDevice(&cuda_dev));
        CHECK_CUDA(cudaSetDevice(result.second));
        CHECK_CUDA(cudaMalloc(pptr, size));
        cudaSetDevice(cuda_dev);
        return Status::OK();
#else
        return Status::NotImplemented("CUDA feature not supported" LOC_MARK);
#endif
    }
    int socket_id = 0;
    if (result.first == "cpu") socket_id = result.second;
    *pptr = numa_alloc_onnode(size, socket_id);
    if (!(*pptr))
        return Status::InternalError("Unable to allocate DRAM memory");
    return Status::OK();
}

Status genericFreeLocalMemory(void *ptr, size_t size) {
#ifdef USE_CUDA
    // Check pointer on GPU
    cudaPointerAttributes attributes;
    CHECK_CUDA(cudaPointerGetAttributes(&attributes, ptr));
    if (attributes.type == cudaMemoryTypeDevice) {
        CHECK_CUDA(cudaFree(ptr));
    } else if (attributes.type == cudaMemoryTypeHost ||
               attributes.type == cudaMemoryTypeUnregistered) {
        numa_free(ptr, size);
    } else {
        LOG(ERROR) << "Unknown memory type, " << ptr << " " << attributes.type;
    }
#else
    numa_free(ptr, size);
#endif
    return Status::OK();
}
}  // namespace v1
}  // namespace mooncake
