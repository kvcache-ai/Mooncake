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

#ifndef TYPES_H_
#define TYPES_H_

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#if defined(__x86_64__)
#include <immintrin.h>
#define PAUSE() _mm_pause()
#else
#define PAUSE()
#endif

#ifndef likely
#define likely(x) __glibc_likely(x)
#define unlikely(x) __glibc_unlikely(x)
#endif

namespace mooncake {
namespace v1 {

using BatchID = uint64_t;
using SegmentID = uint64_t;
using Notification = std::string;

#ifndef LOCAL_SEGMENT_ID
#define LOCAL_SEGMENT_ID (0ull)
#endif

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

enum TransportType { RDMA = 0, SHM, GDS, MNNVL, TCP, IOURING, NVLINK, UNSPEC };
const static int kSupportedTransportTypes = 7;

struct MemoryOptions {
    Location location = kWildcardLocation;
    Permission perm = kGlobalReadWrite;
    TransportType type = RDMA;
    std::string shm_path = "";
    size_t shm_offset = 0;
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

}  // namespace v1
}  // namespace mooncake

#endif  // TYPES_H_