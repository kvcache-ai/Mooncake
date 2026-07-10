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

#ifndef NCCL_TRANSPORT_H_
#define NCCL_TRANSPORT_H_

#include <memory>

#include "transport/transport.h"

namespace mooncake {

// Host-submitted NCCL RMA backend for the classic Transfer Engine API.
// Native NCCL communicators and windows are intentionally private.
//
// Each peer pair must register the same number of CUDA VMM buffers, with
// matching lengths, before its first transfer. Corresponding buffers become
// one NCCL symmetric window; ncclMemAlloc is the supported allocation path.
// WRITE uses ncclPutSignal. NCCL 2.30 has no public host Get operation, so READ
// asks the target's control plane to issue the reverse ncclPutSignal and
// completes synchronously.
class NcclHostTransport final : public Transport {
   public:
    NcclHostTransport();
    ~NcclHostTransport() override;

    Status submitTransfer(BatchID batch_id,
                          const std::vector<TransferRequest>& entries) override;

    Status submitTransferTask(
        const std::vector<TransferTask*>& task_list) override;

    Status getTransferStatus(BatchID batch_id, size_t task_id,
                             TransferStatus& status) override;

   protected:
    int install(std::string& local_server_name,
                std::shared_ptr<TransferMetadata> metadata,
                std::shared_ptr<Topology> topology) override;

   private:
    int registerLocalMemory(void* addr, size_t length,
                            const std::string& location, bool remote_accessible,
                            bool update_metadata = true) override;
    int unregisterLocalMemory(void* addr, bool update_metadata = true) override;
    int registerLocalMemoryBatch(const std::vector<BufferEntry>& buffer_list,
                                 const std::string& location) override;
    int unregisterLocalMemoryBatch(
        const std::vector<void*>& addr_list) override;

    const char* getName() const override { return "nccl"; }

    class Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace mooncake

#endif  // NCCL_TRANSPORT_H_
