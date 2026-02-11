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

#ifndef ASCEND_DIRECT_TRANSPORT_H
#define ASCEND_DIRECT_TRANSPORT_H

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
#include <set>

#include <acl/acl.h>
#include "transfer_metadata.h"
#include "transport/transport.h"
#include "adxl/adxl_engine.h"

namespace mooncake {
class TransferMetadata;

class AscendDirectTransport : public Transport {
   public:
    using BufferDesc = TransferMetadata::BufferDesc;
    using SegmentDesc = TransferMetadata::SegmentDesc;

   public:
    AscendDirectTransport();

    ~AscendDirectTransport() override;

    Status submitTransfer(BatchID batch_id,
                          const std::vector<TransferRequest> &entries) override;

    Status submitTransferTask(
        const std::vector<TransferTask *> &task_list) override;

    Status getTransferStatus(BatchID batch_id, size_t task_id,
                             TransferStatus &status) override;

    int install(std::string &local_server_name,
                std::shared_ptr<TransferMetadata> meta,
                std::shared_ptr<Topology> topo) override;

    const char *getName() const override { return "ascend_direct"; }

    int registerLocalMemory(void *addr, size_t length,
                            const std::string &location, bool remote_accessible,
                            bool update_metadata) override;

    int unregisterLocalMemory(void *addr, bool update_metadata) override;

    int registerLocalMemoryBatch(
        const std::vector<Transport::BufferEntry> &buffer_list,
        const std::string &location) override;

    int unregisterLocalMemoryBatch(
        const std::vector<void *> &addr_list) override;

   private:
    int allocateLocalSegmentID();

    static std::string GenAdxlEngineName(const std::string &ip,
                                         const uint64_t port);

    void queryThread();

    void processSliceList(const std::vector<Slice *> &slice_list);

    void connectAndTransfer(const std::string &target_adxl_engine_name,
                            adxl::TransferOp operation,
                            const std::vector<Slice *> &slice_list,
                            int32_t times = 0);

    void localCopy(TransferRequest::OpCode opcode,
                   const std::vector<Slice *> &slice_list);

    aclError copyWithBatch(TransferRequest::OpCode opcode,
                           const std::vector<Slice *> &slice_list,
                           aclrtMemcpyKind kind, size_t batch_num,
                           size_t slice_index) const;

    static void copyWithSync(TransferRequest::OpCode opcode,
                             const std::vector<Slice *> &slice_list,
                             aclrtMemcpyKind kind);

    void copyWithAsync(TransferRequest::OpCode opcode,
                       const std::vector<Slice *> &slice_list,
                       aclrtMemcpyKind kind);

    uint16_t findAdxlListenPort();

    int InitAdxlEngine();

    int checkAndConnect(const std::string &target_adxl_engine_name);

    int disconnect(const std::string &target_adxl_engine_name,
                   int32_t timeout_in_millis, bool force = false);

    void TransferWithAsync(const std::string &target_adxl_engine_name,
                           adxl::TransferOp operation,
                           const std::vector<Slice *> &slice_list,
                           const std::vector<adxl::TransferOpDesc> &op_descs);

    template <class F, class... Args>
    void enqueue(F &&f, Args &&...args);

    void submitSlices(std::vector<Slice *> &slice_list);

    std::atomic_bool running_;
    std::unique_ptr<adxl::AdxlEngine> adxl_;
    std::map<void *, adxl::MemHandle> addr_to_mem_handle_;
    std::mutex mem_handle_mutex_;

    // Connection management for segment connections
    std::set<std::string> connected_segments_;
    std::mutex connection_mutex_;

    int32_t device_logic_id_{};
    aclrtContext rt_context_{nullptr};
    int32_t connect_timeout_ = 10000;
    int32_t transfer_timeout_ = 10000;
    std::string local_adxl_engine_name_{};
    aclrtStream stream_{};
    bool use_buffer_pool_{false};
    bool auto_connect_{false};
    int32_t base_port_ = 20000;
    std::unordered_set<SegmentID> need_update_metadata_segs_;
    bool use_short_connection_{false};

    // add for async transfer
    std::thread query_thread_;
    std::queue<std::vector<Slice *>> query_slice_queue_;
    std::mutex query_mutex_;
    std::condition_variable query_cv_;
    int64_t transfer_timeout_in_nano_;
    bool use_async_transfer_{false};

    // add for thread pool
    std::vector<std::thread> workers_;
    std::queue<std::function<void()>> tasks_;
    std::mutex thread_pool_queue_mutex_;
    std::condition_variable thread_pool_condition_;

    // for limiting async tasks
    size_t active_async_tasks_{0};
    std::mutex async_task_mutex_;
    std::condition_variable async_task_cv_;
};

}  // namespace mooncake
#endif  // ASCEND_DIRECT_TRANSPORT_H
