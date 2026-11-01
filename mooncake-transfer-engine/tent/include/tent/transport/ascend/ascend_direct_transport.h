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

#ifndef ASCEND_DIRECT_TRANSPORT_H_
#define ASCEND_DIRECT_TRANSPORT_H_

#include <atomic>
#include <condition_variable>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "tent/runtime/control_plane.h"
#include "tent/runtime/transport.h"
#include "tent/transport/ascend/hixl_engine.h"
#include "tent/transport/ascend/local_copy_engine.h"
#include "tent/transport/ascend/resource_config.h"
#include "tent/transport/ascend/shared_host_mapper.h"
#include "ascend_affinity.h"

namespace mooncake {
namespace tent {

struct HixlTask {
    Request request;
    // Published by a transfer worker and read by the poller. status_word,
    // transferred_bytes and req_handle are always accessed with the GCC
    // __atomic builtins (the generic __atomic_load/__atomic_store for the enum
    // field, the _n forms for the integer and pointer; the fields keep their
    // plain types so HixlTask stays movable inside std::vector): a worker
    // stores a terminal status with release ordering, the poller loads it with
    // acquire ordering, and the success path publishes req_handle last so the
    // fields written before it are visible when it is read.
    volatile TransferStatusEnum status_word = TransferStatusEnum::PENDING;
    volatile size_t transferred_bytes = 0;
    void* req_handle = nullptr;
    uint64_t batch_size = 0;
    std::string remote_hixl;
    // Steady-clock nanoseconds. accepted_time_ns is taken by the submitter, so
    // a task that never reaches a transfer worker still times out;
    // start_time_ns is taken when the worker handed the group to HIXL. Both use
    // the same monotonic clock as the poll cache and the timeout checks.
    int64_t accepted_time_ns = 0;
    int64_t start_time_ns = 0;
    size_t local_engine_idx = 0;
    bool local_copy = false;
    // Non-null for a transfer to a peer host buffer imported as a shared
    // memfd: the ACL local copy engine copies to/from this mapped address.
    void* mapped_remote_addr = nullptr;
};

struct HixlSubBatch : public Transport::SubBatch {
    std::vector<HixlTask> task_list;
    size_t max_size = 0;
    // Groups handed to a transfer worker that has not finished with them yet.
    std::atomic<int> pending_submits{0};
    size_t size() const override { return task_list.size(); }
};

class AscendDirectTransportTestPeer;

class AscendDirectTransport : public Transport {
   public:
    AscendDirectTransport();
    ~AscendDirectTransport() override;

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

    Status addMemoryBuffer(BufferDesc& desc,
                           const MemoryOptions& options) override;
    Status addMemoryBuffer(std::vector<BufferDesc>& desc_list,
                           const MemoryOptions& options) override;
    Status removeMemoryBuffer(BufferDesc& desc) override;

    const char* getName() const override { return "ascend_direct"; }
    bool supportNotification() const override { return false; }

   private:
    struct PreparedRequest {
        Request request;
        size_t local_engine_idx = 0;
        std::string remote_hixl;
        // True when the remote target buffer is a shared memfd mapped into
        // this process, so the ACL local copy engine performs the copy.
        bool shm_local = false;
        void* mapped_remote_addr = nullptr;
    };

    struct GroupState {
        TransferStatusEnum status = TransferStatusEnum::PENDING;
        int remaining = 0;
        size_t engine_idx = 0;
        std::string remote;
        bool counted = false;
        bool local_copy = false;
        // Last HIXL status query on the steady clock in nanoseconds, used to
        // collapse the per-task polls of one request into one query per short
        // window. Must not come from CLOCK_REALTIME.
        int64_t last_poll_ns = 0;
    };

    Status initEngines();
    void finalizeEngines();
    void startPools(const std::vector<std::vector<int>>& engine_cpus);
    void stopPools();
    Status publishLocalEngines();
    size_t currentEngineIndex() const;
    bool isLocalRoute(size_t engine_idx, const std::string& remote_hixl) const;
    Status resolveRequestWithSegment(const Request& request,
                                     PreparedRequest& prepared,
                                     SegmentDesc* desc) const;
    void startGroup(HixlSubBatch* batch, const std::string& remote_hixl,
                    size_t local_engine_idx, bool write,
                    std::vector<HixlTask*> tasks);
    void issueGroup(const std::string& remote_hixl, size_t local_engine_idx,
                    bool write, const std::vector<HixlTask*>& tasks);
    void startLocalCopy(size_t local_engine_idx, bool write,
                        const std::vector<HixlTask*>& tasks,
                        bool use_mapped_remote = false);
    void failLocalCopyStream(size_t engine_idx, TransferStatusEnum status);
    void markGroupHandles(const std::vector<void*>& handles,
                          TransferStatusEnum status);
    // Marks the tasks that are still PENDING as FAILED and gives the group
    // reservation back. Used when a transfer worker aborts before it publishes
    // its group (for example when issueGroup throws).
    void abandonGroup(const std::vector<HixlTask*>& tasks);
    void failEntireRoute(size_t engine_idx, const std::string& remote,
                         TransferStatusEnum status);
    bool applyGroupStatus(HixlTask& task, GroupState& group);
    bool finishTaskLocked(HixlTask& task, void** local_release_handle,
                          size_t* local_release_engine);
    void completePolledTask(HixlTask& task);
    void releaseInflight(int n = 1);

    friend class AscendDirectTransportTestPeer;

    bool installed_{false};
    std::string local_segment_name_;
    std::shared_ptr<Topology> local_topology_;
    std::shared_ptr<ControlService> metadata_;
    std::shared_ptr<Config> conf_;
    AscendDirectOptions options_;
    std::map<std::string, std::string> init_options_;
    int64_t transfer_timeout_ns_{0};

    std::vector<std::unique_ptr<HixlEngine>> engines_;
    std::vector<std::unique_ptr<LocalCopyEngine>> local_copies_;
    // Indexed like engines_: workers that hand remote groups to HIXL, and the
    // workers a local copy spreads its streams over.
    std::vector<std::unique_ptr<AscendThreadPool>> transfer_pools_;
    std::vector<std::unique_ptr<AscendThreadPool>> copy_pools_;

    // Maps peer shared host segments on demand. Mutable because
    // resolveRequestWithSegment
    // is const: importing a mapping is a cache fill, not a logical change.
    mutable SharedHostMapper shared_host_mapper_;
    // ASCEND_USE_SHMEM is read once at install(); when off, no buffer can
    // carry a removable memfd handle, so skip the per-request mapper probe.
    bool use_shmem_{false};

    std::mutex req_mutex_;
    std::unordered_map<void*, GroupState> groups_;
    std::map<std::pair<size_t, std::string>, std::vector<void*>> route_groups_;

    std::mutex inflight_mu_;
    std::condition_variable inflight_cv_;
    std::atomic<int> inflight_{0};
};

class AscendDirectTransportTestPeer {
   public:
    static const AscendDirectOptions& options(
        const AscendDirectTransport& transport) {
        return transport.options_;
    }

    static const std::map<std::string, std::string>& initOptions(
        const AscendDirectTransport& transport) {
        return transport.init_options_;
    }

    static size_t engineCount(const AscendDirectTransport& transport) {
        return transport.engines_.size();
    }

    static const std::string& engineName(const AscendDirectTransport& transport,
                                         size_t idx) {
        return transport.engines_.at(idx)->name();
    }

    static int inflight(const AscendDirectTransport& transport) {
        return transport.inflight_.load();
    }

    static bool installed(const AscendDirectTransport& transport) {
        return transport.installed_;
    }
};

}  // namespace tent
}  // namespace mooncake

#endif  // ASCEND_DIRECT_TRANSPORT_H_
