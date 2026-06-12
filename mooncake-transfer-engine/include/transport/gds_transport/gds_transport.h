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

#ifndef GDS_TRANSPORT_H_
#define GDS_TRANSPORT_H_

#include <bits/stdint-uintn.h>

#include <cstddef>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "transfer_metadata.h"
#include "transport/nvmeof_transport/cufile_desc_pool.h"
#include "transport/transport.h"

namespace mooncake {

class GdsFileContext;

struct IOParamRange {
    size_t base;
    size_t count;
};

struct GdsBatchDesc {
    int desc_idx = -1;
    std::vector<IOParamRange> io_param_ranges;
};

class GdsTransport : public Transport {
   public:
    GdsTransport();

    ~GdsTransport() override;

    BatchID allocateBatchID(size_t batch_size) override;

    Status submitTransfer(
        BatchID batch_id,
        const std::vector<TransferRequest> &entries) override;

    Status submitTransferTask(
        const std::vector<TransferTask *> &task_list) override;

    Status getTransferStatus(BatchID batch_id, size_t task_id,
                             TransferStatus &status) override;

    Status freeBatchID(BatchID batch_id) override;

    const char *getName() const override { return "gds"; }

   private:
    struct pair_hash {
        template <class T1, class T2>
        std::size_t operator()(const std::pair<T1, T2> &pair) const {
            auto hash1 = std::hash<T1>{}(pair.first);
            auto hash2 = std::hash<T2>{}(pair.second);
            return hash1 ^ hash2;
        }
    };

    int install(std::string &local_server_name,
                std::shared_ptr<TransferMetadata> meta,
                std::shared_ptr<Topology> topo) override;

    int registerLocalMemory(void *addr, size_t length,
                            const std::string &location, bool remote_accessible,
                            bool update_metadata) override;

    int unregisterLocalMemory(void *addr,
                              bool update_metadata = false) override;

    int registerLocalMemoryBatch(
        const std::vector<Transport::BufferEntry> &buffer_list,
        const std::string &location) override;

    int unregisterLocalMemoryBatch(
        const std::vector<void *> &addr_list) override;

    std::string getBufferPath(
        const TransferMetadata::NVMeoFBufferDesc &buffer_desc) const;

    void addSliceToCUFileBatch(void *source_addr, uint64_t source_offset,
                               uint64_t file_offset, uint64_t slice_len,
                               TransferRequest::OpCode op, int desc_idx,
                               CUfileHandle_t fh);

    std::unordered_map<std::pair<SegmentHandle, uint64_t>,
                       std::shared_ptr<GdsFileContext>, pair_hash>
        segment_to_context_;
    std::shared_ptr<CUFileDescPool> desc_pool_;
    RWSpinlock context_lock_;
    size_t max_batch_entries_;
};

}  // namespace mooncake

#endif  // GDS_TRANSPORT_H_
