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
#include <mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "tent/platform/xpu.h"
#include "tent/runtime/control_plane.h"
#include "tent/runtime/segment.h"
#include "tent/runtime/transport.h"

namespace mooncake {
namespace tent {

struct XpuTask {
    Request request;
    // Address `request.target_offset` resolves to in this process: the
    // request's own value for a local target, or the imported mapping of the
    // peer's buffer (see XpuTransport::prepareRemoteBuffer) otherwise.
    uint64_t target_addr{0};
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
          target_addr(other.target_addr),
          status_word(other.status_word.load()),
          transferred_bytes(other.transferred_bytes.load()) {}
    XpuTask &operator=(const XpuTask &other) {
        request = other.request;
        target_addr = other.target_addr;
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
// NVLinkTransport. It executes same-host copies through the active
// XpuPlatform and advertises the matching capabilities (gpu_to_dram /
// dram_to_gpu / gpu_to_gpu):
//  - the VRAM<->host-DRAM staging hop, while the host<->host hop is carried by
//    RDMA/TCP and ProxyManager chains the two stages (see findStagingPolicy);
//  - intra-node VRAM<->VRAM copies, run directly over PCIe (or Xe Link) when
//    the two XPUs have peer access and bounced through host memory otherwise;
//  - the same copy against another process on this host: each registered
//    XPU buffer publishes its Level Zero IPC handle in the segment descriptor
//    (transport_attrs[XPU], see addMemoryBuffer), and the peer maps it into
//    its own device address space with zeMemOpenIpcHandle before copying
//    (prepareRemoteBuffer). The planner offers XPU for a remote segment only
//    when that mapping succeeded (same machine, attribute present, import
//    ok); anything else keeps the RDMA dma-buf route or host staging.
// Cross-node VRAM traffic bypasses this transport entirely when RdmaTransport
// registered both ends through dma-buf (GPUDirect-style, see
// xpuDirectRdmaReady); staging remains the fallback for hosts or buffers
// without it.
//
// The IPC handle names the exporter's dma-buf by a descriptor of that
// process; XpuPlatform fetches that descriptor with pidfd_getfd(2), like
// MnnvlTransport does, before opening the handle. That requires
// PTRACE_MODE_ATTACH permission on the exporter: same user with
// kernel.yama.ptrace_scope <= 1 and the exporter in the same pid namespace
// (the Level Zero runtime marks its processes PR_SET_PTRACER_ANY, so no
// ancestry is needed), or CAP_SYS_PTRACE. Deployments that cannot grant it
// silently keep the other routes. `transports/xpu/disable_ipc` turns the
// whole mechanism off.
//
// Like NVLinkTransport binds to CudaPlatform, this transport binds to the
// concrete XpuPlatform in install() and drives every device operation -- USM
// pointer classification, the copies and the IPC mapping -- through it. The
// active XpuPlatform resolves both base and interior USM pointers via its
// backend registry, and the SYCL dependency stays confined to platform_xpu.
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

    // True when `entry`, a buffer of remote segment `segment` that covers the
    // request target, is (now) mapped into this process so a request against
    // it can be executed as a local copy. Imports on first use, opening the
    // mapping on the local device holding `local_source` (device 0 for a
    // host source), and caches both successes and failures per attribute
    // value, so the planner may call this on every request. Returns false
    // when the segment is on another machine, the buffer carries no XPU IPC
    // attribute, IPC is disabled, or the import failed.
    bool prepareRemoteBuffer(SegmentID target_id, const SegmentDesc &segment,
                             const BufferDesc &entry, void *local_source);

   private:
    void startTransfer(XpuTask *task, XpuSubBatch *batch);

    // Translate a peer address into the imported mapping (cache only).
    bool relocate(SegmentID target_id, uint64_t &addr, uint64_t length);

    struct ImportEntry {
        std::string attr;  // exporter attribute the mapping was made from
        uint64_t base;     // peer address of the allocation
        uint64_t size;
        void *ptr;  // local mapping
    };

   private:
    bool installed_;
    bool ipc_enabled_;
    std::string local_segment_name_;
    std::string machine_id_;
    std::string pid_namespace_;
    std::shared_ptr<Topology> local_topology_;
    std::shared_ptr<ControlService> metadata_;
    std::shared_ptr<Config> conf_;
    XpuPlatform *platform_;

    // Distinguishes successive registrations of the same allocation in the
    // published attribute, so a peer never mistakes a re-registered buffer
    // for the one it already mapped.
    std::atomic<uint64_t> ipc_generation_{0};

    std::mutex imports_lock_;
    std::unordered_map<SegmentID, std::vector<ImportEntry>> imports_;
    // Mappings superseded by a re-registration of the same allocation. They
    // are withdrawn from relocate() but stay mapped until uninstall(): a
    // transfer that already relocated into them may still be copying.
    std::vector<void *> retired_imports_;
    // "<attribute>@<local device>" of imports that failed; a failure on one
    // device must not disable the route for another.
    std::unordered_set<std::string> failed_imports_;
};

}  // namespace tent
}  // namespace mooncake

#endif  // TENT_XPU_TRANSPORT_H_
