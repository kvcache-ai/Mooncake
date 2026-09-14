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

#ifndef TENT_MPCOMM_TRANSPORT_H
#define TENT_MPCOMM_TRANSPORT_H

#include <cstddef>
#include <memory>
#include <string>
#include <vector>

#include "tent/common/concurrent/rw_spinlock.h"
#include "tent/runtime/control_plane.h"
#include "tent/runtime/transport.h"
#include "tent/transport/mpcomm/mpcomm_adapter.h"
#include "tent/transport/mpcomm/mpcomm_peer_registry.h"
#include "tent/transport/mpcomm/mpcomm_task_mapping.h"

namespace mooncake {
namespace tent {

// Maps TENT requests onto MPComm transfers.
//
// The transport keeps only what needs the TENT runtime: resolving a SegmentID
// to a peer, reading the endpoint that peer advertises, and driving batches.
// Everything that talks to MPComm goes through MpcommAdapter, the peer cache
// lives in MpcommPeerRegistry and the request/completion mapping in
// mpcomm_task_mapping - which is what lets all three be exercised in a build
// with neither RDMA hardware nor libmpcomm.
class MpcommTransport : public Transport {
   public:
    MpcommTransport();

    // Test seam: runs against the supplied MPComm boundary instead of the real
    // provider. The adapter is shared so that a test still observes what the
    // transport did after uninstall() has dropped the transport's reference.
    explicit MpcommTransport(std::shared_ptr<MpcommAdapter> adapter);

    ~MpcommTransport() override;

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

    const char *getName() const override { return "mpcomm"; }

   private:
    // Resolve remote segment ID to mpcomm host_id, connecting if needed
    Status ensurePeerConnected(SegmentID target_id, std::string &host_id);

   private:
    bool installed_{false};
    std::string local_segment_name_;
    std::shared_ptr<ControlService> metadata_;
    std::shared_ptr<Topology> local_topology_;

    std::shared_ptr<MpcommAdapter> adapter_;
    std::unique_ptr<MpcommPeerRegistry> peers_;
    int tcp_port_{0};
};

}  // namespace tent
}  // namespace mooncake

#endif  // TENT_MPCOMM_TRANSPORT_H
