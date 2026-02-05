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

#ifndef ASCEND_DIRECT_TRANSPORT_H_
#define ASCEND_DIRECT_TRANSPORT_H_

#include <functional>
#include <iostream>
#include <queue>
#include <string>

#include <acl/acl.h>
#include <hixl/hixl.h>

#include "tent/runtime/control_plane.h"
#include "tent/runtime/transport.h"

namespace mooncake {
namespace tent {

struct HixlTask {
    Request request;
    volatile TransferStatusEnum status_word;
    volatile size_t transferred_bytes;
    uint64_t target_addr = 0;
    hixl::TransferReq req_handle = nullptr;
    uint64_t batch_size = 0;
    std::string remote_hixl;
    uint64_t start_time = 0;
    bool sync = false;
};

struct HixlSubBatch : public Transport::SubBatch {
    std::vector<HixlTask> task_list;
    size_t max_size;
    virtual size_t size() const { return task_list.size(); }
};

class AscendDirectTransport : public Transport {
   public:
    AscendDirectTransport();

    ~AscendDirectTransport();

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

    virtual const char *getName() const { return "ascend_direct"; }

   private:
    void workerThread();

    Status initHixl(const std::shared_ptr<Config> &conf);

    Status checkAndConnect(const std::string &remote_hixl);

    void disconnect(const std::string &remote_hixl, int32_t timeout_in_millis);

    void startTransfer(SegmentID target_id, Request::OpCode opcode,
                       const std::vector<HixlTask *> &tasks, bool sync = false);

    void localCopy(Request::OpCode opcode,
                   const std::vector<HixlTask *> &tasks);

    aclError copyWithBatch(Request::OpCode opcode,
                           const std::vector<HixlTask *> &tasks,
                           aclrtMemcpyKind kind, size_t batch_num,
                           size_t task_index) const;

    static void copyWithSync(Request::OpCode opcode,
                             const std::vector<HixlTask *> &tasks,
                             aclrtMemcpyKind kind);

    void copyWithAsync(Request::OpCode opcode,
                       const std::vector<HixlTask *> &tasks,
                       aclrtMemcpyKind kind);

   private:
    bool installed_;
    std::string local_segment_name_;
    std::shared_ptr<Topology> local_topology_;
    std::shared_ptr<ControlService> metadata_;

    int32_t device_logic_id_{};
    aclrtContext rt_context_{nullptr};
    std::unique_ptr<hixl::Hixl> hixl_;
    bool use_buffer_pool_{false};
    bool auto_connect_{false};
    uint64_t connect_timeout_;
    uint64_t transfer_timeout_;
    std::string local_hixl_name_{};
    aclrtStream stream_{};

    std::mutex req_mutex_;
    std::unordered_map<hixl::TransferReq,
                       std::pair<TransferStatusEnum, uint64_t>>
        req_map_;

    // Connection management for segment connections
    std::set<std::string> connected_segments_;
    std::mutex connection_mutex_;

    std::map<uint64_t, hixl::MemHandle> addr_to_mem_handle_;
    std::mutex mem_handle_mutex_;

    std::atomic_bool running_ = false;
    std::mutex queue_mutex_;
    std::condition_variable queue_cv_;
    std::queue<std::vector<HixlTask *>> task_queue_;
    std::thread worker_thread_;
};
}  // namespace tent
}  // namespace mooncake

#endif  // ASCEND_DIRECT_TRANSPORT_H_