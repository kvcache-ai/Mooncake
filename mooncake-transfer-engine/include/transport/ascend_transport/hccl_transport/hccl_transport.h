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

#define THREAD_NUM 1
#define ASCEND_DEFAULT_HOST_PORT 10000
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

    int initPdThread();

    void initiatorLoop(int deviceLogicId, int selfIdx);

    void acceptLoop(int deviceLogicId);

    int getDevIdAndIpPortFromServerName(std::string &local_server_name,
                                        std::string &ip, int &ip_port,
                                        int &devicePhyId);

    int rankInfoParse(int devicePhyId, std::string hostIp);

   private:
    std::atomic_bool running_;
    std::thread allInitiatorThreads_[THREAD_NUM];
    std::thread allAcceptThreads_[THREAD_NUM];
    std::queue<std::vector<Slice *>> allReqQueues_[THREAD_NUM];
    std::mutex initiator_mutex_;
    std::condition_variable initiator_cond_;
    RankInfo local_rank_info_;
    RankInfo remote_rank_info_;
};
}  // namespace mooncake
#endif