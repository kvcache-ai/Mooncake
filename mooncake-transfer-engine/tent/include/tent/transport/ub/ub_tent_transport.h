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

#ifndef UB_TENT_TRANSPORT_H
#define UB_TENT_TRANSPORT_H

#include "tent/runtime/transport.h"
#include "tent/runtime/control_plane.h"
#include "tent/common/types.h"
#include "tent/common/config.h"
#include "tent/common/status.h"

// Old TE headers (mooncake:: namespace, not mooncake::tent::)
#include "transport/transport.h"
#include "transport/kunpeng_transport/ub_transport.h"
#include "transfer_metadata.h"
#include "topology.h"

#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

namespace mooncake {
namespace tent {

// UbTentTransport adapts the legacy UbTransport (old TE) to the TENT
// Transport interface.  It reuses the proven URMA data-plane in UbTransport
// and bridges the semantic gap between TENT's request/segment model and the
// old TE's BatchID/TransferTask model.
//
// Threading: install/uninstall are single-threaded; all other methods may be
// called concurrently from multiple TENT worker threads.
class UbTentTransport : public Transport {
   public:
    // SubBatch holds the old-TE BatchID and the converted TransferRequest
    // objects whose pointers are stored inside the old-TE TransferTask list.
    // The requests must remain alive until freeSubBatch() is called.
    struct UbSubBatch : public Transport::SubBatch {
        // Old TE BatchID (pointer-as-integer to BatchDesc).
        mooncake::Transport::BatchID ub_batch_id{0};

        // Lifetime-managed copies of converted old-TE requests.
        // UbTransport::submitTransferTask() stores bare pointers into this
        // vector, so the vector must not be reallocated after submission.
        std::vector<mooncake::Transport::TransferRequest> te_requests;

        size_t size() const override { return task_count_; }

       private:
        friend class UbTentTransport;
        size_t task_count_{0};
    };

    UbTentTransport() = default;
    ~UbTentTransport() override;

    // TENT Transport interface
    Status install(std::string& local_segment_name,
                   std::shared_ptr<ControlService> metadata,
                   std::shared_ptr<Topology> local_topology,
                   std::shared_ptr<Config> conf = nullptr) override;

    Status uninstall() override;

    Status allocateSubBatch(SubBatchRef& batch, size_t max_size) override;
    Status freeSubBatch(SubBatchRef& batch) override;

    Status submitTransferTasks(
        SubBatchRef batch, const std::vector<Request>& request_list) override;

    Status getTransferStatus(SubBatchRef batch, int task_id,
                             TransferStatus& status) override;

    Status addMemoryBuffer(BufferDesc& desc,
                           const MemoryOptions& options) override;

    Status removeMemoryBuffer(BufferDesc& desc) override;

    const char* getName() const override { return "ub"; }

   private:
    // Translate a TENT SegmentID to the corresponding old-TE SegmentID by
    // looking up the segment name via the TENT SegmentManager and querying
    // the old-TE metadata for the matching ID.
    mooncake::Transport::SegmentID getTESegmentID(SegmentID tent_id);

   private:
    // Old-TE UbTransport instance and its required metadata/topology.
    std::unique_ptr<mooncake::UbTransport> ub_transport_;
    std::shared_ptr<mooncake::TransferMetadata> te_metadata_;
    std::shared_ptr<mooncake::Topology> te_topology_;

    // TENT control plane (for segment name lookup during submit).
    std::shared_ptr<ControlService> control_service_;

    std::string local_segment_name_;

    // Cache: TENT SegmentID → old-TE SegmentID.
    std::mutex seg_id_cache_mutex_;
    std::unordered_map<SegmentID, mooncake::Transport::SegmentID>
        tent_to_te_seg_id_;
};

}  // namespace tent
}  // namespace mooncake

#endif  // UB_TENT_TRANSPORT_H
