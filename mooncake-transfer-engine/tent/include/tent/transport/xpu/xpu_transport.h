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

#ifndef TENT_XPU_TRANSPORT_H_
#define TENT_XPU_TRANSPORT_H_

#include <atomic>
#include <string>
#include <vector>

#include "tent/platform/xpu.h"
#include "tent/runtime/control_plane.h"
#include "tent/runtime/transport.h"

namespace mooncake {
namespace tent {

struct XpuTask {
    Request request;
    // Atomic so the polling getTransferStatus() reads a consistent, visible
    // value for the completion written by startTransfer()/notifyProgress().
    std::atomic<TransferStatusEnum> status_word{TransferStatusEnum::PENDING};
    std::atomic<size_t> transferred_bytes{0};

    XpuTask() = default;
    // std::atomic is neither copyable nor movable, but XpuTask instances live
    // in a std::vector, whose element type must be move-insertable. These
    // special members transfer the atomic *values*; they run only during
    // single-threaded sub-batch construction, never concurrently with polling.
    XpuTask(const XpuTask &other)
        : request(other.request),
          status_word(other.status_word.load()),
          transferred_bytes(other.transferred_bytes.load()) {}
    XpuTask &operator=(const XpuTask &other) {
        request = other.request;
        status_word.store(other.status_word.load());
        transferred_bytes.store(other.transferred_bytes.load());
        return *this;
    }
};

struct XpuSubBatch : public Transport::SubBatch {
    std::vector<XpuTask> task_list;
    size_t max_size;
    size_t size() const override { return task_list.size(); }
};

// XpuTransport is the Intel-XPU vendor transport, the oneAPI/SYCL analog of
// NVLinkTransport. For the correctness-first MVP (PR2) it implements only the
// local VRAM<->host-DRAM staging hop: it advertises the device<->host
// capabilities (gpu_to_dram / dram_to_gpu) and runs the copy through the active
// XpuPlatform for LOCAL_SEGMENT_ID requests, while the host<->host hop is
// carried by RDMA/TCP and ProxyManager chains the two stages (see
// findStagingPolicy). gpu_to_gpu is left false so the engine always stages
// cross-node traffic through host DRAM; direct VRAM RDMA (dma-buf) and PCIe
// peer-to-peer arrive later, exactly as NVLinkTransport already exposes them
// for CUDA.
//
// Like NVLinkTransport binds to CudaPlatform, this transport binds to the
// concrete XpuPlatform in install() and drives every device operation -- USM
// pointer classification and the VRAM<->host copy -- through it. The active
// XpuPlatform resolves both base and interior USM pointers via its backend
// registry, and the SYCL dependency stays confined to platform_xpu.
class XpuTransport : public Transport {
   public:
    XpuTransport();

    ~XpuTransport();

    Status install(std::string &local_segment_name,
                   std::shared_ptr<ControlService> metadata,
                   std::shared_ptr<Topology> local_topology,
                   std::shared_ptr<Config> conf = nullptr) override;

    Status uninstall() override;

    Status allocateSubBatch(SubBatchRef &batch, size_t max_size) override;

    Status freeSubBatch(SubBatchRef &batch) override;

    Status submitTransferTasks(
        SubBatchRef batch, const std::vector<Request> &request_list) override;

    Status getTransferStatus(SubBatchRef batch, int task_id,
                             TransferStatus &status) override;

    Status addMemoryBuffer(BufferDesc &desc,
                           const MemoryOptions &options) override;

    Status removeMemoryBuffer(BufferDesc &desc) override;

    const char *getName() const override { return "xpu"; }

   private:
    void startTransfer(XpuTask *task, XpuSubBatch *batch);

   private:
    bool installed_;
    std::string local_segment_name_;
    std::shared_ptr<Topology> local_topology_;
    std::shared_ptr<ControlService> metadata_;
    std::shared_ptr<Config> conf_;
    XpuPlatform *platform_;
};

}  // namespace tent
}  // namespace mooncake

#endif  // TENT_XPU_TRANSPORT_H_
