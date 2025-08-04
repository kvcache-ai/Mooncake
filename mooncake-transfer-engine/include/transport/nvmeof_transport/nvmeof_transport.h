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

#ifndef NVMEOF_TRANSPORT_H_
#define NVMEOF_TRANSPORT_H_

#include <bits/stdint-uintn.h>

#include <cstddef>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>

#include "cufile_context.h"
#include "cufile_desc_pool.h"
#include "transfer_metadata.h"
#include "transport/transport.h"

namespace mooncake {

struct NVMeoFBatchDesc {
    int desc_idx_;
    std::vector<TransferStatus> transfer_status;
    std::vector<std::tuple<size_t, uint64_t>> task_to_slices;
};

class NVMeoFTransport : public Transport {
   public:
    NVMeoFTransport();

    ~NVMeoFTransport();

    BatchID allocateBatchID(size_t batch_size) override;

    Status submitTransferTask(
        const std::vector<TransferTask *> &task_list) override;

    Status submitTransfer(BatchID batch_id,
                          const std::vector<TransferRequest> &entries) override;

    Status getTransferStatus(BatchID batch_id, size_t task_id,
                             TransferStatus &status) override;

    Status freeBatchID(BatchID batch_id) override;

    void addSliceToTask(void *source_addr, uint64_t slice_len,
                        uint64_t target_start, TransferRequest::OpCode op,
                        TransferTask &task, const char *file_path);

   private:
    void startTransfer(Slice *slice);

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
        const std::string &location) override {
        return 0;
    }

    int unregisterLocalMemoryBatch(
        const std::vector<void *> &addr_list) override {
        return 0;
    }

    void addSliceToCUFileBatch(void *source_addr, uint64_t file_offset,
                               uint64_t slice_len, uint64_t desc_id,
                               TransferRequest::OpCode op, CUfileHandle_t fh);

    const char *getName() const override { return "nvmeof"; }

    std::unordered_map<BatchID, int> batch_to_cufile_desc_;
    std::unordered_map<std::pair<SegmentHandle, uint64_t>,
                       std::shared_ptr<CuFileContext>, pair_hash>
        segment_to_context_;
    std::vector<std::thread> workers_;

    std::shared_ptr<CUFileDescPool> desc_pool_;
    RWSpinlock context_lock_;
};
}  // namespace mooncake

#endif
