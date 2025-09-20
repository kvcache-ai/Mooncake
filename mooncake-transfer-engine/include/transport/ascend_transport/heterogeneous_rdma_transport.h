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

#ifndef HETEROGENEOUS_RDMA_TRANSPORT_H_
#define HETEROGENEOUS_RDMA_TRANSPORT_H_

#include "transport/rdma_transport/rdma_transport.h"
#include "acl/acl.h"
#include <atomic>
#include <new>
#include <memory>
#include <condition_variable>

#define HUGE_HOST_SIZE 3ULL * 1024 * 1024 * 1024
#define HUGE_DEVICE_SIZE 8 * 1024 * 1024
#define HUGE_DEVICE_NUM 4
#define AGGREGATE_SIZE_LIMIT 2 * 1024 * 1024

namespace mooncake {

class HeterogeneousRdmaTransport : public Transport {
   public:
    HeterogeneousRdmaTransport()
        : transport_(std::make_unique<RdmaTransport>()) {}

    ~HeterogeneousRdmaTransport();

    int install(std::string &local_server_name,
                std::shared_ptr<TransferMetadata> meta,
                std::shared_ptr<Topology> topo) override;

    const char *getName() const override { return "ascend"; }

    int registerLocalMemory(void *addr, size_t length,
                            const std::string &location, bool remote_accessible,
                            bool update_metadata) override;

    int unregisterLocalMemory(void *addr, bool update_metadata = true) override;

    int registerLocalMemoryBatch(const std::vector<BufferEntry> &buffer_list,
                                 const std::string &location) override;

    int unregisterLocalMemoryBatch(
        const std::vector<void *> &addr_list) override;

    int checkAndCreateStreamCopy();

    Status submitTransfer(BatchID batch_id,
                          const std::vector<TransferRequest> &entries) override;

    Status submitTransferTask(
        const std::vector<TransferTask *> &task_list) override;

    Status getTransferStatus(BatchID batch_id,
                             std::vector<TransferStatus> &status);

    Status getTransferStatus(BatchID batch_id, size_t task_id,
                             TransferStatus &status) override;

   private:
    void transferLoop();

    Status aggTransport(const std::vector<TransferTask *> &task_list);

    Status noAggTransport(const std::vector<TransferTask *> &task_list);

   private:
    struct TransferInfo {
        std::vector<TransferTask *> tasks{};
        char *block{};
        uint64_t total_length{};
    };

    char *acquireBlock() {
        using namespace std::chrono_literals;

        for (size_t i = 1; true; ++i) {
            std::unique_lock<std::mutex> lock(block_queue_mutx_);
            if (block_queue_cv_.wait_for(lock, 1000ms, [&] {
                    return !this->block_queue_.empty();
                })) {
                auto block = block_queue_.front();
                block_queue_.pop();
                return block;
            } else {
                LOG(INFO)
                    << "HeterogeneousRdmaTransport: acquireBlock time out, idx:"
                    << i;
            }
        }
    }

    void releaseBlock(char *block) {
        std::lock_guard<std::mutex> lock(block_queue_mutx_);
        block_queue_.push(block);
        block_queue_cv_.notify_one();
    }

    bool allBlockReleased() const {
        return block_queue_.size() == (size_t)HUGE_DEVICE_NUM;
    }

    TransferInfo getTransfer() {
        using namespace std::chrono_literals;

        for (size_t i = 1; true; ++i) {
            std::unique_lock<std::mutex> lock(transfer_queue_mutex_);
            if (transfer_queue_cv_.wait_for(lock, 10000ms, [&] {
                    return !this->transfer_queue_.empty();
                })) {
                auto info = transfer_queue_.front();
                transfer_queue_.pop();
                return info;
            } else {
                // LOG(INFO) << "HeterogeneousRdmaTransport: getTransfer time
                // out, idx:" << i;
            }
        }
    }

    void addTransfer(std::vector<TransferTask *> &&tasks, char *block,
                     uint64_t total_length) {
        TransferInfo info{};
        info.tasks = std::move(tasks);
        info.block = block;
        info.total_length = total_length;

        std::lock_guard<std::mutex> lock(transfer_queue_mutex_);
        transfer_queue_.push(info);
        transfer_queue_cv_.notify_one();
    }

    bool running_{};
    int logic_device_id_{};

    std::unique_ptr<RdmaTransport> transport_{};
    aclrtStream stream_copy_{};
    bool stream_copy_created_{};

    char *dev_addr_{};
    char *host_addr_{};
    uint64_t host_offset_{};

    std::queue<char *> block_queue_{};
    std::mutex block_queue_mutx_{};
    std::condition_variable block_queue_cv_{};

    std::mutex memcpy_mutex_;

    std::thread transfer_thread_;
    std::queue<TransferInfo> transfer_queue_;
    std::mutex transfer_queue_mutex_;
    std::condition_variable transfer_queue_cv_;
};

using TransferRequest = Transport::TransferRequest;
using TransferStatus = Transport::TransferStatus;
using TransferStatusEnum = Transport::TransferStatusEnum;
using SegmentID = Transport::SegmentID;
using BatchID = Transport::BatchID;

}  // namespace mooncake

#endif  // HETEROGENEOUS_RDMA_TRANSPORT_H_
