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

namespace mooncake {
namespace tent {

struct HixlTask {
    Request request;
    volatile TransferStatusEnum status_word = TransferStatusEnum::PENDING;
    volatile size_t transferred_bytes = 0;
    void* req_handle = nullptr;
    uint64_t batch_size = 0;
    std::string remote_hixl;
    int64_t start_time_ns = 0;
    size_t local_engine_idx = 0;
    bool local_copy = false;
};

struct HixlSubBatch : public Transport::SubBatch {
    std::vector<HixlTask> task_list;
    size_t max_size = 0;
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
    };

    struct GroupState {
        TransferStatusEnum status = TransferStatusEnum::PENDING;
        int remaining = 0;
        size_t engine_idx = 0;
        std::string remote;
        bool counted = false;
        bool local_copy = false;
    };

    Status initEngines();
    void finalizeEngines();
    Status publishLocalEngines();
    size_t currentEngineIndex() const;
    Status resolveRequest(const Request& request,
                          PreparedRequest& prepared) const;
    void startGroup(const std::string& remote_hixl, size_t local_engine_idx,
                    bool write, const std::vector<HixlTask*>& tasks);
    void startLocalCopy(size_t local_engine_idx, bool write,
                        const std::vector<HixlTask*>& tasks);
    void failLocalCopyStream(size_t engine_idx, TransferStatusEnum status);
    void markGroupHandles(const std::vector<void*>& handles,
                          TransferStatusEnum status);
    void failEntireRoute(size_t engine_idx, const std::string& remote,
                         TransferStatusEnum status, bool disconnect);
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
