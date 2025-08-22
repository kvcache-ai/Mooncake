// Copyright 2025 Alibaba Cloud and its affiliates
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

#ifndef NVMEOF_GENERIC_TRANSPORT_H_
#define NVMEOF_GENERIC_TRANSPORT_H_

#include <bits/stdint-uintn.h>

#include <cstddef>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>

#include "nvmeof_target.h"
#include "nvmeof_initiator.h"
#include "worker_pool.h"
#include "transfer_metadata.h"
#include "transport/transport.h"

namespace mooncake {
using FileBufferID = TransferMetadata::FileBufferID;
using FileBufferDesc = TransferMetadata::FileBufferDesc;
using NVMeoFTrid = TransferMetadata::NVMeoFGenericTrid;

class NVMeoFGenericTransport : public Transport {
   public:
    NVMeoFGenericTransport();

    ~NVMeoFGenericTransport();

    BatchID allocateBatchID(size_t batch_size) override;

    Status freeBatchID(BatchID batch_id) override;

    Status submitTransferTask(
        const std::vector<TransferTask *> &task_list) override;

    Status submitTransfer(BatchID batch_id,
                          const std::vector<TransferRequest> &entries) override;

    Status getTransferStatus(BatchID batch_id, size_t task_id,
                             TransferStatus &status) override;

   private:
    int install(std::string &local_server_name,
                std::shared_ptr<TransferMetadata> meta, void **args) override;

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

    int setupLocalSegment();

    bool supportFileBuffer() override { return true; }

    int registerLocalFile(FileBufferID id, const std::string &path,
                          size_t size) override;

    int unregisterLocalFile(FileBufferID id) override;

    const char *getName() const override { return "nvmeof_generic"; }

    int parseTrid(const std::string &trStr);

    bool validateTrid(const NVMeoFTrid &local_trid);

    int setupInitiator();

    std::shared_ptr<NVMeoFController> getOrCreateController(
        SegmentHandle handle);

    std::shared_ptr<NVMeoFInitiator> initiator;
    std::unique_ptr<NVMeoFWorkerPool> worker_pool;

    NVMeoFTrid local_trid;
    std::unique_ptr<NVMeoFTarget> target;

    RWSpinlock controller_lock_;
    std::unordered_map<SegmentHandle, std::shared_ptr<NVMeoFController>>
        segment_to_controller_;
};
}  // namespace mooncake

#endif
