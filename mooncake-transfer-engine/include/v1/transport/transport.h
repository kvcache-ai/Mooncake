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

#ifndef TRANSPORT_V1_H_
#define TRANSPORT_V1_H_

#include <bits/stdint-uintn.h>
#include <errno.h>
#include <stddef.h>
#include <stdint.h>

#include <cstddef>
#include <cstdint>
#include <iostream>
#include <memory>
#include <queue>
#include <string>

#include "v1/common/status.h"
#include "v1/metadata/metadata.h"
#include "v1/memory/allocator.h"
#include "v1/memory/location.h"

namespace mooncake {
namespace v1 {
class Transport {
   public:
    struct SubBatch {
        SubBatch() {}
        virtual ~SubBatch() {}
        virtual size_t size() const = 0;
    };

    using SubBatchRef = SubBatch *;

   public:
    Transport() = default;

    virtual ~Transport() = default;

    virtual Status install(std::string &local_segment_name,
                           std::shared_ptr<MetadataService> metadata,
                           std::shared_ptr<Topology> local_topology,
                           std::shared_ptr<ConfigManager> conf = nullptr) {
        return Status::OK();
    }

    virtual Status uninstall() { return Status::OK(); }

    virtual Status allocateSubBatch(SubBatchRef &batch, size_t max_size) {
        return Status::NotImplemented(
            "allocateSubBatch not implemented" LOC_MARK);
    }

    virtual Status freeSubBatch(SubBatchRef &batch) {
        return Status::NotImplemented("freeSubBatch not implemented" LOC_MARK);
    }

    virtual Status submitTransferTasks(
        SubBatchRef batch, const std::vector<Request> &request_list) {
        return Status::NotImplemented(
            "submitTransferTasks not implemented" LOC_MARK);
    }

    virtual Status getTransferStatus(SubBatchRef batch, int task_id,
                                     TransferStatus &status) {
        return Status::NotImplemented(
            "getTransferStatus not implemented" LOC_MARK);
    }

    virtual Status allocateLocalMemory(void **addr, size_t size,
                                       MemoryOptions &options) {
        return genericAllocateLocalMemory(addr, size, options);
    }

    virtual Status freeLocalMemory(void *addr, size_t size) {
        return genericFreeLocalMemory(addr, size);
    }

    virtual Status addMemoryBuffer(BufferDesc &desc,
                                   const MemoryOptions &options) {
        return Status::NotImplemented(
            "addMemoryBuffer not implemented" LOC_MARK);
    }

    virtual Status removeMemoryBuffer(BufferDesc &desc) {
        return Status::NotImplemented(
            "removeMemoryBuffer not implemented" LOC_MARK);
    }

    virtual bool supportNotification() const { return false; }

    virtual Status sendNotification(SegmentID target_id,
                                    const Notification &notify) {
        return Status::NotImplemented(
            "sendNotification not implemented" LOC_MARK);
    }

    virtual Status receiveNotification(std::vector<Notification> &notify_list) {
        return Status::NotImplemented(
            "receiveNotification not implemented" LOC_MARK);
    }

    virtual const char *getName() const { return "<generic>"; }
};
}  // namespace v1
}  // namespace mooncake

#endif  // TRANSPORT_V1_H_
