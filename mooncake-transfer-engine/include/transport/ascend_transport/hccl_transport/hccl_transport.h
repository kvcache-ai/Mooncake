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

#ifndef HCCL_TRANSPORT_H
#define HCCL_TRANSPORT_H

#include <infiniband/verbs.h>
#include <atomic>
#include <cstddef>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <condition_variable>
#include "transfer_metadata.h"
#include "transport/transport.h"
#include "hccl_transport_mem_c.h"
#include "hccl_aggTransport_c.h"

#define THREAD_NUM 1
#define ASCEND_DEFAULT_DEVICE_PORT 16666

namespace mooncake {
class TransferMetadata;
class HcclTransport : public Transport {
   public:
    using BufferDesc = TransferMetadata::BufferDesc;
    using SegmentDesc = TransferMetadata::SegmentDesc;
    using HandShakeDesc = TransferMetadata::HandShakeDesc;

   public:
    HcclTransport();

    ~HcclTransport();

    Status submitTransfer(BatchID batch_id,
                          const std::vector<TransferRequest> &entries) override;

    Status submitTransferTask(
        const std::vector<TransferTask *> &task_list) override;

    Status getTransferStatus(BatchID batch_id, size_t task_id,
                             TransferStatus &status) override;

    int install(std::string &local_server_name,
                std::shared_ptr<TransferMetadata> meta,
                std::shared_ptr<Topology> topo) override;

    const char *getName() const override { return "hccl"; }

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

   private:
    int allocateLocalSegmentID();
    int getDevIdAndIpPortFromServerName(std::string &local_server_name,
                                        std::string &ip, int &ip_port,
                                        int &devicePhyId);
    int devInfoParse(std::string hostIp);
    int prepareTransport(std::vector<Slice *> &slice_list);

    int startNonAggThreads();
    int nonAggTransport(std::vector<Slice *> &slice_list, aclrtStream stream);
    void initiatorLoop(int deviceLogicId);     // Thread logic for initiator
    void targetAcceptLoop(int deviceLogicId);  // Thread logic for target

    int startAggThreads();
    int aggTransport(std::vector<Slice *> &slice_list, aclrtStream stream);
    void aggInitiatorLoop(
        int deviceLogicId);  // Thread logic for initiator aggregation/splitting
    void aggInitiatorTransferLoop(
        int deviceLogicId);  // Thread logic for initiator data transfer
    void aggTargetAcceptLoop(
        int deviceLogicId);  // Thread logic for target connection acceptance
    void aggTargetLoop(
        int deviceLogicId);  // Thread logic for target aggregation/splitting

   private:
    bool aggregateEnabled_;
    std::atomic_bool running_;

    std::thread initiatorThread_;
    std::thread targetAcceptThread_;
    std::thread targetThread_;

    std::thread aggInitiatorThread_;
    std::thread aggInitiatorTransferThread_;
    std::thread aggTargetAcceptThread_;
    std::thread aggTargetThread_;
    std::queue<std::vector<Slice *>> allReqQueues_;
    std::mutex initiator_mutex_;
    std::condition_variable initiator_cond_;
    RankInfo local_rank_info_;
    RankInfo remote_rank_info_;
};
}  // namespace mooncake
#endif