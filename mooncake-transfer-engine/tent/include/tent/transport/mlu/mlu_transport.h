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
#include <memory>
#include <mutex>
#include <string>
#include <vector>
#include <unordered_set>
#include <cn_api.h>

#include "tent/platform/mlu.h"
#include "tent/runtime/control_plane.h"
#include "tent/runtime/segment.h"
#include "tent/runtime/transport.h"

namespace mooncake {
namespace tent {

namespace mlu_detail {
struct ImportEntry {
    std::string attr;
    uint64_t base = 0;
    uint64_t size = 0;
    uint64_t snapshot_generation = 0;
    std::shared_ptr<void> mapping;
};

// Caller synchronizes access to entries and reconciled_snapshot.
void reconcileImports(std::vector<ImportEntry> &entries,
                      uint64_t &reconciled_snapshot, const SegmentDesc &segment,
                      std::vector<std::shared_ptr<void>> &dropped);
}  // namespace mlu_detail

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
    std::shared_ptr<void> imported_target;
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

// Executes local MLU copies and same-host IPC copies of peer process buffers.
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

    bool mayMapRemoteBuffer(SegmentID target_id, const SegmentDesc &segment,
                            const BufferDesc &entry) const;
    bool prepareRemoteBuffer(SegmentID target_id, const SegmentDesc &segment,
                             const BufferDesc &entry, void *local_source);
    void closeRemoteSegment(SegmentID target_id);

    const char *getName() const override { return "mlu"; }

   private:
    Status submitCopy(MluTask &task, bool to_device);
    Status submitPeerCopy(MluTask &task);
    void startTransfer(MluTask *task);
    Status quiesceLocked();
    Status drainTask(MluTask &task);
    Status drainBuffer(uint64_t addr, size_t size);
    std::shared_ptr<void> relocate(SegmentID target_id, uint64_t &addr,
                                   size_t length);

    std::mutex mutex_;
    std::map<CNcontext, std::array<std::shared_ptr<MluCopyQueue>, 3>> queues_;
    std::unordered_set<MluSubBatch *> batches_;
    std::map<SegmentID, std::vector<mlu_detail::ImportEntry>> imports_;
    std::map<SegmentID, uint64_t> reconciled_snapshots_;
    std::string machine_id_;
    std::atomic<uint64_t> ipc_generation_{0};
    bool installed_ = false;
    bool ipc_enabled_ = true;
    MluPlatform *platform_ = nullptr;
    std::shared_ptr<ControlService> metadata_;
};

}  // namespace tent
}  // namespace mooncake

#endif  // TENT_MLU_TRANSPORT_H_
