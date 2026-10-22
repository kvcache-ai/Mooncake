// Copyright 2024 KVCache.AI
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

#include "transport/rdma_transport/pxn/pxn_memory_manager.h"
#include "cuda_alike.h"

namespace mooncake {
namespace pxn {

// Not executed or execution failed, treated as not gpu memory
int PxnEnsureGpuMemory(void *start, bool &isGpuMem, uint32_t &gpuidx) {

#if defined(USE_CUDA) || defined(USE_MUSA) || defined(USE_HIP) ||  \
    defined(USE_MLU) || defined(USE_MACA) || defined(USE_HYGON) || \
    defined(USE_COREX)
    cudaPointerAttributes attributes;
    cudaError_t result = cudaPointerGetAttributes(&attributes, start);
    if (result != cudaSuccess) {
        LOG(ERROR) << "ensure gpu memory cudaPointerGetAttributes failed (Error code: " << result
                   << " - " << cudaGetErrorString(result) << ")" << std::endl;
        isGpuMem = false;
        return -1;
    }

    if (attributes.type == cudaMemoryTypeDevice) {
        isGpuMem = true;
        gpuidx = attributes.device;
    }
#endif
    return 0;
}


} // pxn namespace
} // mooncake namespace
