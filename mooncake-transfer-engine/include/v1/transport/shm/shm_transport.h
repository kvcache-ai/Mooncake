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

#ifndef SHM_TRANSPORT_H_
#define SHM_TRANSPORT_H_

#include <asio.hpp>
#include <boost/thread.hpp>
#include <functional>
#include <iostream>
#include <queue>
#include <string>

#include "v1/metadata/metadata.h"
#include "v1/transport/transport.h"

namespace mooncake {
namespace v1 {
class ShmThreadPool;

struct ShmTask {
    Request request;
    volatile TransferStatusEnum status_word;
    volatile size_t transferred_bytes;
    uint64_t target_addr = 0;
};

struct ShmSubBatch : public Transport::SubBatch {
    std::vector<ShmTask> task_list;
    size_t max_size;
};

class ShmTransport : public Transport {
    friend class ShmThreadPool;

   public:
    ShmTransport();

    ~ShmTransport();

    virtual Status install(std::string &local_segment_name,
                           std::shared_ptr<MetadataService> metadata,
                           std::shared_ptr<Topology> local_topology,
                           std::shared_ptr<ConfigManager> conf = nullptr);

    virtual Status uninstall();

    virtual Status allocateSubBatch(SubBatchRef &batch, size_t max_size);

    virtual Status freeSubBatch(SubBatchRef &batch);

    virtual Status submitTransferTasks(
        SubBatchRef batch, const std::vector<Request> &request_list);

    virtual Status getTransferStatus(SubBatchRef batch, int task_id,
                                     TransferStatus &status);

    virtual void queryOutstandingTasks(SubBatchRef batch,
                                       std::vector<int> &task_id_list);

    virtual Status addMemoryBuffer(BufferDesc &desc,
                                   const MemoryOptions &options);

    virtual Status removeMemoryBuffer(BufferDesc &desc);

    virtual const char *getName() const { return "shm"; }

    virtual Status allocateLocalMemory(BufferEntry &buffer, size_t size,
                                       const Location &location);

    virtual Status freeLocalMemory(const BufferEntry &buffer);

    virtual bool precheck(const Request &request);

   private:
    void startTransfer(ShmTask *task);

    void *createSharedMemory(const std::string &path, size_t size);

    Status relocateSharedMemoryAddress(uint64_t &dest_addr, uint64_t length,
                                       uint64_t target_id);

   private:
    bool installed_;
    std::string local_segment_name_;
    std::shared_ptr<Topology> local_topology_;
    std::shared_ptr<MetadataService> metadata_;
    std::unique_ptr<ShmThreadPool> workers_;

    struct OpenedShmEntry {
        int shm_fd;
        void *shm_addr;
        uint64_t length;
        bool is_cuda_ipc;
    };

    std::mutex relocate_mutex_;
    std::unordered_map<uint64_t, OpenedShmEntry> relocate_map_;
    std::shared_ptr<ConfigManager> conf_;
    
    std::string machine_id_;
};
}  // namespace v1
}  // namespace mooncake

#endif  // SHM_TRANSPORT_H_