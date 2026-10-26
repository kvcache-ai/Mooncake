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
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "tent/platform/xpu.h"
#include "tent/runtime/control_plane.h"
#include "tent/runtime/segment.h"
#include "tent/runtime/transport.h"

namespace mooncake {
class TransferEngineImplTestPeer;
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
//    (prepareRemoteBuffer). The mapping is opened on a local device that can
//    reach the device holding the buffer (that device itself, or one with
//    peer access to it). The planner offers XPU for a remote segment
//    whenever such a mapping may exist (mayMapRemoteBuffer) and imports only
//    once XPU is actually selected; if the import fails, the request is
//    planned again without it and keeps the RDMA dma-buf route or host
//    staging. Once the peer stops publishing the buffer or the segment is
//    closed, the mapping is closed after the last copy through it, so the
//    exporter can reclaim the memory.
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

    // Cheap pre-check, with no import: true when `entry`, a buffer of
    // remote segment `segment`, publishes an XPU IPC attribute this
    // transport could try to map for a copy from/to `local_source` (IPC
    // enabled, same machine, and no earlier import of it failed for good).
    bool mayMapRemoteBuffer(SegmentID target_id, const SegmentDesc &segment,
                            const BufferDesc &entry, void *local_source);

    // True when `entry`, a buffer of remote segment `segment` that covers the
    // request target, is (now) mapped into this process so a request against
    // it can be executed as a local copy. Imports on first use and caches
    // the mapping per attribute value and local device. The mapping is
    // opened on the local device holding `local_source`, which must be the
    // device holding the peer buffer or have peer access to it, or, for a
    // host source, on the device holding the peer buffer. Permanent failures
    // are cached per segment too, so the planner may call this on every
    // request; transient ones (out of descriptors or memory) are retried.
    // Mappings and failures for buffers `segment` no longer publishes are
    // dropped here once `segment` is known to be at least as new as them.
    // Returns false whenever mayMapRemoteBuffer would, when the buffer's
    // device is not reachable from the local one, or when the import failed.
    bool prepareRemoteBuffer(SegmentID target_id, const SegmentDesc &segment,
                             const BufferDesc &entry, void *local_source);

    // Drops every mapping imported from segment `target_id`.
    void closeRemoteSegment(SegmentID target_id);

   private:
    friend class ::mooncake::TransferEngineImplTestPeer;

    void startTransfer(XpuTask *task, XpuSubBatch *batch);

    // The checks of mayMapRemoteBuffer that need no lock.
    bool sharesHost(SegmentID target_id, const SegmentDesc &segment,
                    const BufferDesc &entry) const;

    // Translate a peer address into the mapping that serves a copy from/to
    // local device `source_device` (-1: host memory). Cache only. Returns
    // that mapping, which stays open while the caller holds it, or null when
    // there is none.
    std::shared_ptr<void> relocate(SegmentID target_id, int source_device,
                                   uint64_t &addr, uint64_t length);

    struct ImportEntry {
        std::string attr;  // exporter attribute the mapping was made from
        uint64_t base;     // peer address of the allocation
        uint64_t size;
        uint64_t generation;  // from `attr`, see addMemoryBuffer
        int owner;            // local device holding the peer's bytes
        int device;           // local device the mapping was opened on
        // Local mapping. Closed (XpuPlatform::closeImport) once the entry is
        // dropped and no submitTransferTasks() copying through it holds it.
        std::shared_ptr<void> mapping;

        // Usable by a copy from/to local device `source_device` (-1: host
        // memory, which the owner's queue copies).
        bool serves(int source_device) const {
            return device == (source_device >= 0 ? source_device : owner);
        }
    };

    struct SegmentImports {
        std::vector<ImportEntry> entries;
        // "<attribute>@<local source device>" of imports that failed for a
        // non-transient reason, with the attribute's generation; a failure
        // for one device must not disable the route for another.
        std::unordered_map<std::string, uint64_t> failed;
    };

    // Drop the entries and failures of attributes `segment` no longer
    // publishes, but only those no newer than the newest attribute it does
    // publish: descriptors are cached per thread, so an older snapshot must
    // not retire a mapping made from a newer one.
    static void reconcileLocked(SegmentImports &imports,
                                const SegmentDesc &segment,
                                std::vector<ImportEntry> &dropped);

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
    // for the one it already mapped. Seeded from CLOCK_BOOTTIME at install,
    // so generations also order across processes on this host (a restarted
    // peer's buffers are newer than its predecessor's).
    std::atomic<uint64_t> ipc_generation_{0};

    std::mutex imports_lock_;
    std::unordered_map<SegmentID, SegmentImports> imports_;
};

}  // namespace tent
}  // namespace mooncake

#endif  // TENT_XPU_TRANSPORT_H_
