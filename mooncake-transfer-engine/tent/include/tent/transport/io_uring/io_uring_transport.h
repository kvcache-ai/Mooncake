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

#ifndef TENT_IO_URING_TRANSPORT_H
#define TENT_IO_URING_TRANSPORT_H

#include <bits/stdint-uintn.h>
#include <liburing.h>

#include <cstddef>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>

#include "tent/runtime/control_plane.h"
#include "tent/runtime/transport.h"

namespace mooncake {
namespace tent {

class IOUringFileContext;

struct IOUringTask {
    Request request;
    volatile TransferStatusEnum status_word;
    volatile size_t transferred_bytes;
    void *buffer = nullptr;

    ~IOUringTask() {
        if (buffer) free(buffer);
    }
};

struct IOUringSubBatch : public Transport::SubBatch {
    size_t max_size;
    std::vector<IOUringTask> task_list;
    struct io_uring ring;
    virtual size_t size() const { return task_list.size(); }
};

class IOUringTransport : public Transport {
   public:
    IOUringTransport();

    ~IOUringTransport();

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

    virtual const char *getName() const { return "io-uring"; }

   private:
    std::string getIOUringFilePath(SegmentID handle);

    IOUringFileContext *findFileContext(SegmentID handle);

    Status probeCapabilities();

   private:
    bool installed_;
    std::string local_segment_name_;
    std::shared_ptr<Topology> local_topology_;
    std::shared_ptr<ControlService> metadata_;
    std::shared_ptr<Config> conf_;

    RWSpinlock file_context_lock_;
    using FileContextMap =
        std::unordered_map<SegmentID, std::shared_ptr<IOUringFileContext>>;
    FileContextMap file_context_map_;
    uint64_t async_memcpy_threshold_;
};
}  // namespace tent
}  // namespace mooncake

#endif  // TENT_IO_URING_TRANSPORT_H