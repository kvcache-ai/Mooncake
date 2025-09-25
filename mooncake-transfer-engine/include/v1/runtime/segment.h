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

#ifndef SEGMENT_H
#define SEGMENT_H

#include <glog/logging.h>
#include <jsoncpp/json/json.h>
#include <netdb.h>

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <thread>
#include <unordered_map>
#include <variant>

#include "v1/common/concurrent/rw_spinlock.h"
#include "v1/common/concurrent/thread_local_storage.h"
#include "v1/runtime/metastore.h"
#include "v1/rpc/rpc.h"
#include "v1/runtime/topology.h"

namespace mooncake {
namespace v1 {
using SegmentID = uint64_t;

struct DeviceDesc {
    std::string name;
    uint16_t lid;
    std::string gid;
};

struct BufferDesc {
    uint64_t addr;
    uint64_t length;
    std::string location;
    std::vector<uint32_t> rkey;
    std::string shm_path;
    std::string mnnvl_handle;
    std::vector<TransportType> transports;

    int ref_count;
    std::vector<uint32_t> lkey;  // not uploaded, available in local only
};

struct FileBufferDesc {
    std::string path;
    uint64_t length;
    uint64_t offset;
};

struct MemorySegmentDesc {
    Topology topology;
    std::vector<DeviceDesc> devices;
    std::vector<BufferDesc> buffers;
    std::string rpc_server_addr;
};

struct FileSegmentDesc {
    std::vector<FileBufferDesc> buffers;
};

enum class SegmentType { Memory, File };

struct SegmentDesc {
    std::string name;
    SegmentType type;
    std::string machine_id;
    std::variant<MemorySegmentDesc, FileSegmentDesc> detail;
};

using SegmentDescRef = std::shared_ptr<SegmentDesc>;

static inline BufferDesc *getBufferDesc(SegmentDesc *desc, uint64_t base,
                                        uint64_t length) {
    if (desc->type != SegmentType::Memory) return nullptr;
    auto &detail = std::get<MemorySegmentDesc>(desc->detail);
    for (auto &entry : detail.buffers) {
        if (entry.addr <= base && base + length <= entry.addr + entry.length) {
            return &entry;
        }
    }
    return nullptr;
}

static inline std::string getRpcServerAddr(SegmentDesc *desc) {
    if (desc->type != SegmentType::Memory) return "";
    auto &detail = std::get<MemorySegmentDesc>(desc->detail);
    return detail.rpc_server_addr;
}

static inline DeviceDesc *getDeviceDesc(SegmentDesc *desc,
                                        const std::string &device_name) {
    if (desc->type != SegmentType::Memory) return nullptr;
    auto &detail = std::get<MemorySegmentDesc>(desc->detail);
    for (auto &entry : detail.devices) {
        if (entry.name == device_name) {
            return &entry;
        }
    }
    return nullptr;
}

}  // namespace v1
}  // namespace mooncake

#endif  // SEGMENT_H