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

#ifndef TENT_FABRIC_TRANSPORT_H
#define TENT_FABRIC_TRANSPORT_H

#include <atomic>
#include <map>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "tent/runtime/control_plane.h"
#include "tent/runtime/transport.h"
#include "tent/transport/fabric/fabric_attrs.h"
#include "tent/transport/fabric/fabric_context.h"

namespace mooncake {
namespace tent {

// A local buffer registered in chunks; mr/desc are indexed by context and
// null where a chunk is not registered on that context.
struct FabricLocalBuffer {
    struct Chunk {
        FabricChunkRange range;
        std::vector<int> nics;
        std::vector<int> post_nics;  // subset of nics local to the buffer
        std::vector<struct fid_mr*> mr;
        std::vector<void*> desc;
        std::vector<uint64_t> key;
    };
    uint64_t addr = 0;
    uint64_t length = 0;
    bool published = false;
    std::vector<Chunk> chunks;
    std::vector<FabricChunkRange> ranges;
    std::vector<FabricContext*> contexts;
    std::atomic<bool> released{false};

    ~FabricLocalBuffer() { release(); }
    void release();
};

struct FabricSubBatch : public Transport::SubBatch {
    std::vector<FabricTask*> task_list;
    size_t max_size = 0;
    virtual size_t size() const { return task_list.size(); }
};

// libfabric RDM transport (EFA; tcp;ofi_rxm for tests). Connectionless:
// endpoint names live in the segment attrs and keys in the buffer attrs, so
// there is no bootstrap RPC.
class FabricTransport : public Transport {
   public:
    FabricTransport();
    ~FabricTransport();

    virtual Status install(std::string& local_segment_name,
                           std::shared_ptr<ControlService> metadata,
                           std::shared_ptr<Topology> local_topology,
                           std::shared_ptr<Config> conf = nullptr);

    virtual Status uninstall();

    virtual Status quiesce() override;

    virtual Status allocateSubBatch(SubBatchRef& batch, size_t max_size);

    virtual Status freeSubBatch(SubBatchRef& batch);

    virtual Status submitTransferTasks(
        SubBatchRef batch, const std::vector<Request>& request_list);

    virtual Status getTransferStatus(SubBatchRef batch, int task_id,
                                     TransferStatus& status);

    virtual Status addMemoryBuffer(BufferDesc& desc,
                                   const MemoryOptions& options);

    virtual Status addMemoryBuffer(std::vector<BufferDesc>& desc_list,
                                   const MemoryOptions& options);

    virtual Status removeMemoryBuffer(BufferDesc& desc);

    virtual bool tracksLocalBuffer(const BufferDesc& desc) const;

    virtual const char* getName() const { return "fabric"; }

   private:
    struct PeerBuffer {
        std::vector<FabricChunkRange> ranges;
        FabricBufferAttr attr;
        // Per chunk: the NICs local to the buffer, preferred as targets.
        std::vector<std::vector<int>> preferred;
    };

    // Decoded metadata of one remote segment snapshot.
    struct PeerEntry {
        SegmentDescRef snapshot;
        FabricPeerAttr attr;
        std::map<uint64_t, std::shared_ptr<PeerBuffer>> buffers;  // by addr
        // fi_addr of remote NIC r seen from local context l: [l][r].
        std::vector<std::vector<fi_addr_t>> fi_addrs;
    };

    struct Target {
        std::shared_ptr<PeerEntry> peer;
        std::shared_ptr<PeerBuffer> buffer;
        uint64_t buffer_addr = 0;
    };

    Status openContexts();
    void closeContexts();
    Status publishPeerAttr();
    Status unpublishPeerAttr();

    Status registerBuffer(uint64_t addr, uint64_t length,
                          std::shared_ptr<FabricLocalBuffer>& buffer);
    std::shared_ptr<FabricLocalBuffer> findLocalBuffer(uint64_t addr,
                                                       uint64_t length);
    Status resolveTarget(const Request& request, Target& target);
    Status peerAddress(PeerEntry& peer, size_t local_nic, size_t remote_nic,
                       fi_addr_t& addr);
    Status planRequest(const Request& request, FabricTask* task,
                       std::vector<std::vector<FabricOp*>>& ops);

   private:
    bool installed_ = false;
    std::string local_segment_name_;
    std::shared_ptr<ControlService> metadata_;
    std::shared_ptr<Topology> local_topology_;
    FabricParams params_;
    const FabricProfile* profile_ = nullptr;
    std::vector<std::unique_ptr<FabricContext>> contexts_;
    bool virt_addr_ = true;
    uint64_t chunk_limit_ = 0;
    size_t slice_size_ = 0;

    std::mutex lifecycle_mutex_;
    std::atomic<bool> shutting_down_{false};
    std::atomic<uint64_t> cursor_{0};

    mutable std::shared_mutex buffers_mutex_;
    std::map<uint64_t, std::shared_ptr<FabricLocalBuffer>> buffers_;
    std::vector<std::weak_ptr<FabricLocalBuffer>> retired_;

    std::mutex peers_mutex_;
    std::unordered_map<SegmentID, std::shared_ptr<PeerEntry>> peers_;
};

}  // namespace tent
}  // namespace mooncake

#endif  // TENT_FABRIC_TRANSPORT_H
