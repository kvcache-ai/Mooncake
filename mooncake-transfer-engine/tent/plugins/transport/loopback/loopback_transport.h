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

#ifndef TENT_LOOPBACK_TRANSPORT_H
#define TENT_LOOPBACK_TRANSPORT_H

#include <atomic>
#include <condition_variable>
#include <deque>
#include <memory>
#include <map>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <thread>
#include <vector>

#include "tent/runtime/transport.h"
#include "tent/transport_plugin.h"

namespace mooncake {
namespace tent {

// Reference transport plugin: moves data with memcpy between engines of the
// same process. It exists to exercise the plugin interface (loading, slot
// tagging, routing, notifications, cancellation) without any hardware, and
// is off unless transports/loopback/enable is true.
//
// Config:
//   transports/loopback/async     run transfers on a worker thread so tasks
//                                 go through PENDING (default false)
//   transports/loopback/delay_us  per-task delay of the worker (default 50)
class LoopbackTransport : public Transport {
   public:
    explicit LoopbackTransport(const tent_transport_plugin_host_t* host);
    ~LoopbackTransport() override;

    Status install(std::string& local_segment_name,
                   std::shared_ptr<ControlService> metadata,
                   std::shared_ptr<Topology> local_topology,
                   std::shared_ptr<Config> conf = nullptr) override;
    Status uninstall() override;
    Status quiesce() override;

    Status allocateSubBatch(SubBatchRef& batch, size_t max_size) override;
    Status freeSubBatch(SubBatchRef& batch) override;
    Status submitTransferTasks(
        SubBatchRef batch, const std::vector<Request>& request_list) override;
    Status getTransferStatus(SubBatchRef batch, int task_id,
                             TransferStatus& status) override;

    bool supportsCancellation() const override { return true; }
    Status cancelTransferTask(SubBatchRef batch, int task_id) override;

    Status addMemoryBuffer(BufferDesc& desc,
                           const MemoryOptions& options) override;
    Status removeMemoryBuffer(BufferDesc& desc) override;

    bool supportNotification() const override { return true; }
    Status sendNotification(SegmentID target_id,
                            const Notification& notify) override;
    Status receiveNotification(std::vector<Notification>& notify_list) override;

    const char* getName() const override { return "loopback"; }

    void deliver(const Notification& notify);

    // Copies for `request` into or out of this transport's registered memory
    // at `peer_addr`. False if that range is no longer registered here.
    bool serve(const Request& request, void* peer_addr);

    struct Task {
        Request request;
        std::string target;  // segment name of the peer
        void* peer_addr = nullptr;
        std::atomic<TransferStatusEnum> status{PENDING};
        std::atomic<size_t> transferred{0};
        std::atomic<bool> cancel_requested{false};
        BatchID progress_batch_id{0};
        std::function<void(BatchID)> notify_progress;
    };

   private:
    Status resolve(const Request& request, std::string& target,
                   void*& peer_addr);
    void run(const std::shared_ptr<Task>& task);
    void workerLoop();

    const TransportType type_;
    bool installed_ = false;
    bool async_ = false;
    uint64_t delay_us_ = 50;
    std::string segment_name_;
    std::shared_ptr<ControlService> metadata_;

    std::mutex queue_mutex_;
    std::condition_variable queue_cv_;
    std::deque<std::shared_ptr<Task>> queue_;
    bool stopping_ = false;
    std::thread worker_;

    std::mutex notify_mutex_;
    std::vector<Notification> notifications_;

    // Buffers registered with this transport (address -> length; nested and
    // same-address registrations are allowed). Held shared while a peer
    // copies, so removeMemoryBuffer() waits for copies into it.
    std::shared_mutex ranges_mutex_;
    std::multimap<uint64_t, uint64_t> ranges_;
};

}  // namespace tent
}  // namespace mooncake

#endif  // TENT_LOOPBACK_TRANSPORT_H
