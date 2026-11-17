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

#ifndef TENT_MLU_TRANSPORT_H_
#define TENT_MLU_TRANSPORT_H_

#include <atomic>
#include <array>
#include <map>
#include <mutex>
#include <string>
#include <vector>
#include <unordered_set>
#include <cn_api.h>

#include "tent/platform/mlu.h"
#include "tent/runtime/control_plane.h"
#include "tent/runtime/transport.h"

namespace mooncake {
namespace tent {

struct MluCopyQueue {
    CNcontext context = nullptr;
    CNqueue handle = nullptr;
    std::mutex mutex;
    Status error;  // Guarded by mutex; do not submit to a failed queue.
    ~MluCopyQueue();
    Status synchronize();
};

struct MluTask {
    Request request;
    std::atomic<TransferStatusEnum> status_word{TransferStatusEnum::PENDING};
    std::atomic<size_t> transferred_bytes{0};
    std::shared_ptr<MluCopyQueue> queue;
    CNnotifier completion = nullptr;
    bool drain_on_error = false;

    MluTask() = default;
    MluTask(MluTask &&other) noexcept;
    ~MluTask();
};

struct MluSubBatch : public Transport::SubBatch {
    std::vector<MluTask> task_list;
    size_t max_size;
    std::atomic<size_t> published_size{0};
    size_t size() const override { return published_size.load(); }
    bool needsProgressPolling() const override { return true; }
};

// Executes copies with exactly one MLU endpoint, within the local
// segment. Peer transfers use the engine's host-staging pipeline.
class MluTransport : public Transport {
   public:
    ~MluTransport() override;
    Status install(std::string &local_segment_name,
                   std::shared_ptr<ControlService> metadata,
                   std::shared_ptr<Topology> local_topology,
                   std::shared_ptr<Config> conf = nullptr) override;

    Status uninstall() override;
    Status quiesce() override;

    Status allocateSubBatch(SubBatchRef &batch, size_t max_size) override;

    Status freeSubBatch(SubBatchRef &batch) override;

    Status submitTransferTasks(
        SubBatchRef batch, const std::vector<Request> &request_list) override;

    Status getTransferStatus(SubBatchRef batch, int task_id,
                             TransferStatus &status) override;

    Status allocateLocalMemory(void **addr, size_t size,
                               MemoryOptions &options) override;
    Status freeLocalMemory(void *addr, size_t size) override;

    Status addMemoryBuffer(BufferDesc &desc,
                           const MemoryOptions &options) override;

    Status removeMemoryBuffer(BufferDesc &desc) override;

    const char *getName() const override { return "mlu"; }

   private:
    Status submitCopy(MluTask &task, bool to_device);
    void startTransfer(MluTask *task);
    Status quiesceLocked();
    Status drainTask(MluTask &task);
    Status drainBuffer(uint64_t addr, size_t size);

    std::mutex mutex_;
    std::map<CNcontext, std::array<std::shared_ptr<MluCopyQueue>, 2>> queues_;
    std::unordered_set<MluSubBatch *> batches_;
    bool installed_ = false;
    MluPlatform *platform_ = nullptr;
    std::shared_ptr<ControlService> metadata_;
};

}  // namespace tent
}  // namespace mooncake

#endif  // TENT_MLU_TRANSPORT_H_
