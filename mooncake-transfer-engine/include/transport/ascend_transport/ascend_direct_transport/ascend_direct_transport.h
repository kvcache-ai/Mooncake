// Copyright 2025 Huawei Technologies Co., Ltd
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

#ifndef ASCEND_DIRECT_TRANSPORT_H
#define ASCEND_DIRECT_TRANSPORT_H

#include <memory>
#include <string>
#include <vector>
#include <condition_variable>

#include "transfer_metadata.h"
#include "transport/transport.h"

#include "slice_dispatcher.h"
#include "transfer_executor_base.h"
#include <acl/acl.h>

namespace mooncake {
class TransferMetadata;

class AscendDirectTransport : public Transport {
   public:
    using BufferDesc = TransferMetadata::BufferDesc;
    using SegmentDesc = TransferMetadata::SegmentDesc;

   public:
    AscendDirectTransport();

    ~AscendDirectTransport() override;

    Status submitTransfer(BatchID batch_id,
                          const std::vector<TransferRequest> &entries) override;

    Status submitTransferTask(
        const std::vector<TransferTask *> &task_list) override;

    Status getTransferStatus(BatchID batch_id, size_t task_id,
                             TransferStatus &status) override;

    int install(std::string &local_server_name,
                std::shared_ptr<TransferMetadata> meta,
                std::shared_ptr<Topology> topo) override;

    const char *getName() const override { return "ascend_direct"; }

    int registerLocalMemory(void *addr, size_t length,
                            const std::string &location, bool remote_accessible,
                            bool update_metadata) override;

    int unregisterLocalMemory(void *addr, bool update_metadata) override;

    int registerLocalMemoryBatch(
        const std::vector<Transport::BufferEntry> &buffer_list,
        const std::string &location) override;

    int unregisterLocalMemoryBatch(
        const std::vector<void *> &addr_list) override;

   private:
    int allocateLocalSegmentID();

    // Add one engine to segment desc
    int addEngineToSegmentDesc(int32_t device_id, aclrtContext context,
                               const std::string &host_ip, SegmentDesc *desc);

    int32_t base_port_ = 20000;
    bool dummy_real_mode_{false};
    bool roce_mode_{false};
    std::vector<aclrtContext> local_engine_contexts_;

    std::unique_ptr<TransferExecutorBase> transfer_executor_;
    std::unique_ptr<ISliceDispatcher> dispatcher_;
};

}  // namespace mooncake
#endif  // ASCEND_DIRECT_TRANSPORT_H
