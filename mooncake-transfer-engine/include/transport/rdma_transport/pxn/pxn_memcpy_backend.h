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

#ifndef MOONCAKE_PXN_MEMCPY_BACKEND_H_
#define MOONCAKE_PXN_MEMCPY_BACKEND_H_

#include <cuda_runtime.h>

#include <cstddef>
#include <memory>
#include <vector>

namespace mooncake {
namespace pxn {

// ============================================================
// PxnMemcpyBackend
// ------------------------------------------------------------
// Abstracts the D2D copy operation used by PXN. Two impls:
//   - CUDA >= 12.8 : cudaMemcpyBatchAsync (recommended, ~397 GB/s Hot)
//   - CUDA <  12.8 : cudaMemcpyPeerAsync loop (fallback, ~200 GB/s Hot)
// ============================================================
class PxnMemcpyBackend {
   public:
    virtual ~PxnMemcpyBackend() = default;

    // Parameters:
    //   dsts  - vector of destination device pointers (on dst_device)
    //   srcs  - vector of source device pointers (on src_device)
    //   sizes - byte counts, sizes.size() == dsts.size() == srcs.size()
    //   stream, event - CUDA resources; must be created on dst_device
    //
    // Returns cudaError_t; on failure the event may not be recorded.
    virtual cudaError_t PxnSubmitBatch(const std::vector<void*>& dsts,
                                       const std::vector<const void*>& srcs,
                                       const std::vector<size_t>& sizes,
                                       cudaStream_t stream,
                                       cudaEvent_t event) = 0;

    virtual cudaError_t PxnSubmitBatchMultiEvent(
        const std::vector<void*>& dsts, const std::vector<const void*>& srcs,
        const std::vector<size_t>& sizes, cudaStream_t stream,
        const std::vector<cudaEvent_t>& events) = 0;

    virtual const char* PxnBackendName() const = 0;
};

std::unique_ptr<PxnMemcpyBackend> PxnCreateMemcpyBackend();

}  // namespace pxn
}  // namespace mooncake

#endif  // MOONCAKE_PXN_MEMCPY_BACKEND_H_
