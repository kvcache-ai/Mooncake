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

#ifndef TYPES_H_
#define TYPES_H_

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>

// error code of internal components
#define ERR_DEVICE_NOT_FOUND (-6)
#define ERR_SOCKET (-102)
#define ERR_ENDPOINT (-201)
#define ERR_CONTEXT (-202)
#define ERR_NUMA (-300)
#define ERR_CLOCK (-301)

namespace mooncake {
namespace v1 {

using BatchID = uint64_t;
using SegmentID = uint64_t;

using NotifyMessage = std::string;

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

enum Permission {
    kLocalReadWrite,
    kGlobalReadOnly,
    kGlobalReadWrite,
};

using Location = std::string;
const static std::string kWildcardLocation = "*";

struct BufferEntry {
    void *addr;
    size_t length;
    Location location = kWildcardLocation;
    Permission visibility = kGlobalReadWrite;
    std::string shm_path = "";
    size_t shm_offset = 0;
};

enum TransportType { RDMA = 0, SHM, GDS, MNNVL, TCP };
const static int kSupportedTransportTypes = 5;

struct MemoryOptions {
    Location location = kWildcardLocation;
    Permission perm = kGlobalReadWrite;
    TransportType type = RDMA;
    std::string shm_path = "";
    size_t shm_offset = 0;
};

}  // namespace v1
}  // namespace mooncake

#endif  // TYPES_H_