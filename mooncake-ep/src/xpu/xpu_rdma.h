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

#include <infiniband/verbs.h>
#include <level_zero/ze_api.h>
#include <sycl/sycl.hpp>

#include <cstddef>
#include <cstdint>
#include <deque>
#include <string>
#include <vector>

namespace mooncake {
namespace xpu_ep {

// The verbs path between nodes. A host proxy posts RDMA WRITEs from this
// GPU's memory into a peer GPU's memory: the data first, then a sequence
// number into the peer's ready flag, on one RC queue pair so the responder
// executes them in order.

// The RDMA device under the same PCIe switch as `device`: one whose sysfs
// path shares the longest prefix with the GPU's. When several do (the ports
// of one card), the GPUs nearest that same set take them in turn, in PCI
// order. MC_EP_XPU_NIC overrides the choice. Throws if no RDMA device is
// present.
std::string nearestRdmaDevice(const sycl::device& device);

// The dma-buf fd of the Level Zero device allocation containing `addr`, and
// the offset of `addr` inside it. The allocation must be a whole
// zeMemAllocDevice block (not pooled), ideally allocated with an
// ze_external_memory_export_desc_t requesting DMA_BUF. The fd belongs to the
// Level Zero runtime: do not close it.
void exportDmabuf(ze_context_handle_t ctx, const void* addr, int* fd,
                  uint64_t* offset);

// What a peer needs to connect a queue pair to this one.
struct RdmaQpInfo {
    uint32_t qpn = 0;
    uint32_t psn = 0;
    uint16_t lid = 0;  // InfiniBand only
    uint8_t mtu = 0;   // ibv_mtu of the local port
    uint8_t gid[16] = {};
};

struct RdmaRegion {
    uint64_t addr = 0;
    uint32_t lkey = 0;
    uint32_t rkey = 0;
};

class RdmaDevice {
   public:
    // Opens `name` (e.g. "mlx5_0") on `port`. The GID index is the first
    // RoCE v2 entry with an IPv4-mapped address, else the first RoCE v2
    // entry; MC_EP_XPU_GID_INDEX overrides it. InfiniBand ports use LIDs.
    explicit RdmaDevice(const std::string& name, uint8_t port = 1);
    ~RdmaDevice();
    RdmaDevice(const RdmaDevice&) = delete;
    RdmaDevice& operator=(const RdmaDevice&) = delete;

    // Registers GPU memory through its dma-buf, so the NIC reads and writes
    // VRAM directly. Remote write and read are allowed; relaxed ordering is
    // not requested, since the flag must not overtake the data.
    RdmaRegion registerDmabuf(void* addr, size_t bytes, int fd,
                              uint64_t offset);
    // Same for host memory.
    RdmaRegion registerHost(void* addr, size_t bytes);

    const std::string& name() const { return name_; }
    int gidIndex() const { return gid_index_; }

   private:
    friend class RdmaQueuePair;

    std::string name_;
    uint8_t port_;
    ibv_context* ctx_ = nullptr;
    ibv_pd* pd_ = nullptr;
    ibv_port_attr port_attr_{};
    int gid_index_ = -1;
    ibv_gid gid_{};
    std::vector<ibv_mr*> mrs_;
};

// An RC queue pair with its own completion queue. Not thread-safe: one
// proxy thread posts and polls.
class RdmaQueuePair {
   public:
    explicit RdmaQueuePair(RdmaDevice& device, int max_send_wr = 256);
    ~RdmaQueuePair();
    RdmaQueuePair(const RdmaQueuePair&) = delete;
    RdmaQueuePair& operator=(const RdmaQueuePair&) = delete;

    RdmaQpInfo localInfo() const;
    // INIT -> RTR -> RTS against `remote`. The path MTU is the smaller of
    // the two ports' active MTUs.
    void connect(const RdmaQpInfo& remote);

    // RDMA WRITE of `bytes` from local [laddr, lkey] to remote [raddr, rkey].
    // Signaled writes produce a completion for poll().
    void write(uint64_t laddr, uint32_t lkey, uint64_t raddr, uint32_t rkey,
               uint32_t bytes, bool signaled);
    // RDMA WRITE of a 32-bit value carried inline in the work request, so it
    // needs no registered source. Used for ready flags.
    void writeValue(uint32_t value, uint64_t raddr, uint32_t rkey,
                    bool signaled);

    // Waits for `count` completions of signaled work requests. Throws on a
    // failed completion or after `timeout_ms`.
    void poll(int count, int timeout_ms);

   private:
    void post(ibv_send_wr& wr);

    RdmaDevice& device_;
    ibv_cq* cq_ = nullptr;
    ibv_qp* qp_ = nullptr;
    uint32_t psn_ = 0;
    int max_send_wr_;
    int outstanding_ = 0;     // posted, slot not yet freed by a completion
    int since_signaled_ = 0;  // posted after the last signaled request
    std::deque<int> signaled_batches_;  // requests each completion frees
};

}  // namespace xpu_ep
}  // namespace mooncake
