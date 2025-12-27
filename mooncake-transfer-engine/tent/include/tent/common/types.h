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

#ifndef TENT_TYPES_H
#define TENT_TYPES_H

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

namespace mooncake {
namespace tent {

using BatchID = uint64_t;
using SegmentID = uint64_t;

struct Notification {
    std::string name;
    std::string msg;
};

#ifndef LOCAL_SEGMENT_ID
#define LOCAL_SEGMENT_ID (0ull)
#endif

struct Request {
    enum OpCode { READ, WRITE };
    OpCode opcode;
    void* source;
    SegmentID target_id;
    uint64_t target_offset;
    size_t length;
};

enum TransferStatusEnum {
    INITIAL,
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

enum TransportType {
    RDMA = 0,
    MNNVL,
    SHM,
    NVLINK,
    GDS,
    IOURING,
    TCP,
    AscendDirect,
    UNSPEC
};
const static int kSupportedTransportTypes = 8;

struct MemoryOptions {
    Location location = kWildcardLocation;
    Permission perm = kGlobalReadWrite;
    TransportType type = UNSPEC;
    std::string shm_path = "";
    size_t shm_offset = 0;
    bool internal = false;
};

const std::string kLocalFileSegmentPrefix = "file://";

struct SegmentInfo {
    enum Type { Memory, File };
    struct Buffer {
        uint64_t base, length;
        Location location;
    };
    Type type;
    std::vector<Buffer> buffers;
};

}  // namespace tent
}  // namespace mooncake

#endif  // TENT_TYPES_H