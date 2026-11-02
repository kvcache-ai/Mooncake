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

#pragma once

#include <level_zero/ze_api.h>
#include <sycl/sycl.hpp>

#include <cstdint>
#include <optional>
#include <vector>

namespace mooncake {
namespace xpu_ep {

// A device buffer shared with every peer on the node through Level Zero IPC,
// the XPU counterpart of the CUDA P2pTransport. Peers read each other's
// buffers directly over PCIe P2P (or Xe Link) and write only ready flags.
class XpuIpcBuffer {
   public:
    XpuIpcBuffer(sycl::queue& queue, size_t bytes, int rank, int num_ranks);
    ~XpuIpcBuffer();
    XpuIpcBuffer(const XpuIpcBuffer&) = delete;
    XpuIpcBuffer& operator=(const XpuIpcBuffer&) = delete;

    void* base() const { return base_; }

    // [pid, size, pid-namespace id, ipc handle words...], exchanged by the
    // caller.
    std::vector<int32_t> exportHandle();

    // Maps every peer's buffer and checks it is readable. A peer that cannot
    // be mapped, or whose `active` entry is 0, is left unavailable.
    void importPeers(const std::vector<std::vector<int32_t>>& handles,
                     const std::vector<int>& active);

    // Device tables, one entry per rank (this rank maps to base()).
    void* const* peerBases() const { return d_peer_bases_; }
    bool allPeersAccessible() const { return all_accessible_; }

   private:
    void closeImports();

    sycl::queue queue_;
    ze_context_handle_t ze_ctx_ = nullptr;
    ze_device_handle_t ze_dev_ = nullptr;
    int rank_, num_ranks_;
    size_t bytes_;        // usable bytes
    size_t alloc_bytes_;  // usable bytes plus the trailing probe word
    void* base_ = nullptr;
    std::optional<ze_ipc_mem_handle_t> exported_;
    std::vector<void*> imports_;  // nullptr for self and unmapped peers
    void** d_peer_bases_ = nullptr;
    bool all_accessible_ = false;
};

}  // namespace xpu_ep
}  // namespace mooncake
