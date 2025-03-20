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

#ifndef SEGMENT_H
#define SEGMENT_H

#include <glog/logging.h>
#include <jsoncpp/json/json.h>

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <thread>
#include <unordered_map>

#include "common.h"
#include "common/base/status.h"
#include "topology.h"

namespace mooncake {
struct BufferAttr {
    std::string location;
    uint64_t addr;
    uint64_t length;
    std::vector<uint32_t> lkey;  // for rdma
    std::vector<uint32_t> rkey;  // for rdma
    std::string shm_path;        // for shm
};

struct RdmaAttr {
    std::string name;
    uint16_t lid;
    std::string gid;
};

struct TcpAttr {
    std::string ip_or_hostname;
    uint16_t port;
};

using SegmentID = uint64_t;
const static SegmentID LocalSegmentID = (uint64_t)(0);
const static SegmentID InvalidSegmentID = (uint64_t)(-1);
enum SegmentType { MemoryKind, CufileKind, UnknownKind };

struct SegmentDesc {
    std::string name;
    SegmentType type;
    struct Memory {
        Topology topology;
        std::vector<BufferAttr> buffers;
        std::vector<RdmaAttr> rdma;
        TcpAttr tcp;
    } memory;
    struct Cufile {
        std::string relative_path;
        uint64_t length;
    } cufile;

    Status addBuffer(const BufferAttr &attr);
    Status removeBuffer(void *addr);
};

struct SegmentDescUtils {
    static Json::Value encode(const SegmentDesc &attr);

    static int decode(Json::Value segmentJSON, SegmentDesc &attr);
};
}  // namespace mooncake

#endif  // SEGMENT_H