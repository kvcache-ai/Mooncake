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

#ifndef TCP_TRANSPORT_H_
#define TCP_TRANSPORT_H_

#include <atomic>
#include <functional>
#include <iostream>
#include <memory>
#include <queue>
#include <string>

#include "tent/common/concurrent/thread_pool.h"
#include "tent/runtime/control_plane.h"
#include "tent/runtime/transport.h"

namespace mooncake {
namespace tent {

struct TcpParams {
    size_t max_retry_count = 3;
    uint64_t retry_base_delay_ms = 100;   // 100ms initial backoff
    uint64_t retry_max_delay_ms = 2'000;  // 2s max backoff
    size_t max_concurrent_tasks = 16;     // worker thread pool size
};

struct TcpTask {
    Request request;
    std::atomic<TransferStatusEnum> status_word{TransferStatusEnum::PENDING};
    std::atomic<size_t> transferred_bytes{0};
    uint64_t target_addr = 0;

    TcpTask() = default;
    TcpTask(TcpTask &&other) noexcept
        : request(std::move(other.request)),
          status_word(other.status_word.load(std::memory_order_relaxed)),
          transferred_bytes(
              other.transferred_bytes.load(std::memory_order_relaxed)),
          target_addr(other.target_addr) {}
    TcpTask(const TcpTask &) = delete;
    TcpTask &operator=(const TcpTask &) = delete;
};

struct TcpSubBatch : public Transport::SubBatch {
    std::vector<TcpTask> task_list;
    size_t max_size;
    virtual size_t size() const { return task_list.size(); }
};

class TcpTransport : public Transport {
   public:
    TcpTransport();

    ~TcpTransport();

    virtual Status install(std::string &local_segment_name,
                           std::shared_ptr<ControlService> metadata,
                           std::shared_ptr<Topology> local_topology,
                           std::shared_ptr<Config> conf = nullptr);

    virtual Status uninstall();

    virtual Status allocateSubBatch(SubBatchRef &batch, size_t max_size);

    virtual Status freeSubBatch(SubBatchRef &batch);

    virtual Status submitTransferTasks(
        SubBatchRef batch, const std::vector<Request> &request_list);

    virtual Status getTransferStatus(SubBatchRef batch, int task_id,
                                     TransferStatus &status);

    virtual Status addMemoryBuffer(BufferDesc &desc,
                                   const MemoryOptions &options);

    virtual Status removeMemoryBuffer(BufferDesc &desc);

    virtual const char *getName() const { return "tcp"; }

    virtual bool supportNotification() const { return true; }

    virtual Status sendNotification(SegmentID target_id,
                                    const Notification &notify);

    virtual Status receiveNotification(std::vector<Notification> &notify_list);

   private:
    void startTransfer(TcpTask *task);

    Status doTransferWithRetry(TcpTask *task);

    Status findRemoteSegment(uint64_t dest_addr, uint64_t length,
                             uint64_t target_id, std::string &rpc_server_addr);

   private:
    bool installed_;
    std::string local_segment_name_;
    std::shared_ptr<Topology> local_topology_;
    std::shared_ptr<ControlService> metadata_;
    TcpParams params_;
    std::unique_ptr<ThreadPool> thread_pool_;
    std::atomic<bool> shutting_down_{false};

    RWSpinlock notify_lock_;
    std::vector<Notification> notify_list_;
};
}  // namespace tent
}  // namespace mooncake

#endif  // TCP_TRANSPORT_H_
