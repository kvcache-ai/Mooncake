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

#include "tent/common/status.h"
#include "tent/runtime/platform.h"
#include "tent/runtime/control_plane.h"

namespace mooncake {
namespace tent {
struct Capabilities {
    bool dram_to_dram = false;
    bool dram_to_gpu = false;  // dram as local side, gpu as peer side
    bool gpu_to_dram = false;  // gpu as local side, dram as peer side
    bool gpu_to_gpu = false;
    bool dram_to_file = false;
    bool gpu_to_file = false;
};

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
                           std::shared_ptr<ControlService> metadata,
                           std::shared_ptr<Topology> local_topology,
                           std::shared_ptr<Config> conf = nullptr) {
        return Status::OK();
    }

    virtual Status uninstall() { return Status::OK(); }

    virtual const Capabilities capabilities() const { return caps; }

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
        return Platform::getLoader().allocate(addr, size, options);
    }

    virtual Status freeLocalMemory(void *addr, size_t size) {
        return Platform::getLoader().free(addr, size);
    }

    // Pre-registration warm-up that pins pages before NUMA probing.
    // Returns true if pages were successfully pinned (caller may skip
    // prefault). Default: no-op, returns false.
    virtual bool warmupMemory(void *addr, size_t length) { return false; }

    virtual Status addMemoryBuffer(BufferDesc &desc,
                                   const MemoryOptions &options) {
        return Status::NotImplemented(
            "addMemoryBuffer not implemented" LOC_MARK);
    }

    virtual Status addMemoryBuffer(std::vector<BufferDesc> &desc_list,
                                   const MemoryOptions &options) {
        for (auto &desc : desc_list) {
            CHECK_STATUS(addMemoryBuffer(desc, options));
        }
        return Status::OK();
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

   protected:
    Capabilities caps;
};
}  // namespace tent
}  // namespace mooncake

#endif  // TRANSPORT_V1_H_
