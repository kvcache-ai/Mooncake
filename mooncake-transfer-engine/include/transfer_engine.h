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

#ifndef MULTI_TRANSFER_ENGINE_H_
#define MULTI_TRANSFER_ENGINE_H_

#include <asm-generic/errno-base.h>
#include <bits/stdint-uintn.h>
#include <limits.h>
#include <string.h>

#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "transfer_metadata.h"
#include "transport/transport.h"

namespace mooncake {
class TransferEngine {
   public:
    TransferEngine(std::shared_ptr<TransferMetadata> meta) : metadata_(meta) {}

    ~TransferEngine() { freeEngine(); }

    int init(const char *server_name, const char *connectable_name,
             uint64_t rpc_port = 12345);

    int freeEngine();

    Transport *installOrGetTransport(const char *proto, void **args);

    int uninstallTransport(const char *proto);

    Transport::SegmentHandle openSegment(const char *segment_name);

    int closeSegment(Transport::SegmentHandle seg_id);

    int registerLocalMemory(void *addr, size_t length,
                            const std::string &location,
                            bool remote_accessible = true,
                            bool update_metadata = true);

    int unregisterLocalMemory(void *addr, bool update_metadata = true);

    int registerLocalMemoryBatch(
        const std::vector<Transport::BufferEntry> &buffer_list,
        const std::string &location);

    int unregisterLocalMemoryBatch(const std::vector<void *> &addr_list);

    int syncSegmentCache() { return metadata_->syncSegmentCache(); }

   private:
    struct MemoryRegion {
        void *addr;
        uint64_t length;
        const char *location;
        bool remote_accessible;
    };

    Transport *findName(const char *name, size_t n = SIZE_MAX);

    Transport *initTransport(const char *proto);

    std::vector<Transport *> installed_transports_;
    std::string local_server_name_;
    std::shared_ptr<TransferMetadata> metadata_;
    std::vector<MemoryRegion> local_memory_regions_;
};

using TransferRequest = Transport::TransferRequest;
using TransferStatus = Transport::TransferStatus;
using TransferStatusEnum = Transport::TransferStatusEnum;
using SegmentID = Transport::SegmentID;
using BatchID = Transport::BatchID;
using BufferEntry = Transport::BufferEntry;
}  // namespace mooncake

#endif