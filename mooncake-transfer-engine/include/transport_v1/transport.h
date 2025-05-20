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

#include "common/status.h"
#include "metadata/metadata.h"
#include "utility/memory_location.h"

namespace mooncake {
namespace v1 {
class Transport {
   public:
    using BatchID = uint64_t;
    const static BatchID kInvalidBatchID = UINT64_MAX;

    struct Request {
        enum OpCode { READ, WRITE };
        OpCode opcode;
        void *source;
        SegmentID target_id;
        uint64_t target_offset;
        size_t length;
    };

    enum TransferStatusEnum {
        WAITING,
        PENDING,
        INVALID,
        CANCELED,
        COMPLETED,
        TIMEOUT,
        FAILED
    };

    struct TransferStatus {
        TransferStatusEnum s;
        size_t transferred_bytes;
    };

    struct SubBatch {
        SubBatch() {}
        virtual ~SubBatch() {}
    };

    using SubBatchRef = SubBatch *;

   public:
    Transport() = default;

    virtual ~Transport() = default;

    virtual Status install(std::string &local_segment_name,
                           std::shared_ptr<TransferMetadata> metadata_manager,
                           std::shared_ptr<Topology> local_topology) {
        return Status::OK();
    }

    virtual Status uninstall() { return Status::OK(); }

    virtual Status allocateSubBatch(SubBatchRef &batch, size_t max_batch_size) {
        return Status::NotImplemented(
            "generic transport does not implement allocateSubBatch");
    }

    virtual Status freeSubBatch(SubBatchRef &batch) {
        return Status::NotImplemented(
            "generic transport does not implement freeSubBatch");
    }

    virtual Status submitTransferTasks(
        SubBatchRef &batch, const std::vector<Request> &request_list) {
        return Status::NotImplemented(
            "generic transport does not implement submitTransferTasks");
    }

    virtual TransferStatus getTransferStatus(SubBatchRef &batch,
                                             int request_index) {
        return TransferStatus{TransferStatusEnum::INVALID, 0};
    }

    enum BufferVisibility {
        kLocalReadWrite,
        kGlobalReadOnly,
        kGlobalReadWrite,
    };

    using Location = std::string;

    struct BufferEntry {
        void *addr;
        size_t length;
        Location location = kWildcardLocation;
        BufferVisibility visibility = kGlobalReadWrite;
        std::string shm_path = "";
        size_t shm_offset = 0;
    };

    virtual Status registerLocalMemory(
        const std::vector<BufferEntry> &buffer_list) {
        return Status::NotImplemented(
            "generic transport does not implement registerLocalMemory");
    }

    virtual Status unregisterLocalMemory(
        const std::vector<BufferEntry> &buffer_list) {
        return Status::NotImplemented(
            "generic transport does not implement unregisterLocalMemory");
    }

    virtual const char *getName() const { return "<generic>"; }
};
}  // namespace v1
}  // namespace mooncake

#endif  // TRANSPORT_V1_H_
