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
#include <netdb.h>

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <thread>
#include <unordered_map>
#include <variant>

#include "tent/common/concurrent/rw_spinlock.h"
#include "tent/common/concurrent/thread_local_storage.h"
#include "tent/runtime/metastore.h"
#include "tent/rpc/rpc.h"
#include "tent/runtime/topology.h"
#include "tent/thirdparty/nlohmann/json.h"

namespace mooncake {
namespace tent {
using json = nlohmann::json;
using SegmentID = uint64_t;

struct DeviceDesc {
    std::string name;
    std::unordered_map<TransportType, std::string> transport_attrs;

    // backward compatilble
    uint16_t lid;
    std::string gid;

   public:
    NLOHMANN_DEFINE_TYPE_INTRUSIVE(DeviceDesc, name, lid, gid, transport_attrs);
};

struct Region {
    uint64_t size;
    std::string location;

   public:
    NLOHMANN_DEFINE_TYPE_INTRUSIVE(Region, size, location);
};

struct BufferDesc {
    uint64_t addr;
    uint64_t length;
    std::string location;
    std::vector<TransportType> transports;
    std::vector<Region> regions;
    std::unordered_map<TransportType, std::string> transport_attrs;
    // mutable elements
    int ref_count{0};

    // backward compatilble
    std::vector<uint32_t> rkey;
    std::string shm_path;
    std::string mnnvl_handle;
    std::vector<uint32_t> lkey;  // not uploaded, available in local only

    bool internal{false};

   public:
    NLOHMANN_DEFINE_TYPE_INTRUSIVE(BufferDesc, addr, length, location,
                                   transports, regions, transport_attrs, rkey,
                                   shm_path, mnnvl_handle, internal);
};

struct FileBufferDesc {
    std::string path;
    uint64_t length;
    uint64_t offset;

   public:
    NLOHMANN_DEFINE_TYPE_INTRUSIVE(FileBufferDesc, path, length, offset);
};

struct MemorySegmentDesc {
    Topology topology;
    std::unordered_map<std::string, std::string> device_attrs;
    std::vector<BufferDesc> buffers;
    std::string rpc_server_addr;
    std::vector<DeviceDesc> devices;

    // Transport-specific attributes (key-value pairs per transport type)
    // int maps to TransportType enum
    std::unordered_map<int, std::string> transport_attrs;

   public:
    // Get transport attributes for a specific transport type
    std::string& getTransportAttrs(int transport_type) {
        return transport_attrs[transport_type];
    }

    // Get transport attributes (const version)
    const std::string* getTransportAttrs(int transport_type) const {
        auto it = transport_attrs.find(transport_type);
        if (it != transport_attrs.end()) {
            return &it->second;
        }
        return nullptr;
    }
};

inline void to_json(json& j, const MemorySegmentDesc& m) {
    j = json{{"device_attrs", m.device_attrs},
             {"buffers", m.buffers},
             {"rpc_server_addr", m.rpc_server_addr},
             {"devices", m.devices},
             {"topology", m.topology.toString()},
             {"transport_attrs", m.transport_attrs}};
}

inline void from_json(const json& j, MemorySegmentDesc& m) {
    j.at("device_attrs").get_to(m.device_attrs);
    j.at("buffers").get_to(m.buffers);
    j.at("rpc_server_addr").get_to(m.rpc_server_addr);
    j.at("devices").get_to(m.devices);
    if (j.contains("topology")) {
        auto s = j.at("topology").get<std::string>();
        m.topology.parse(s);
    }
    if (j.contains("transport_attrs")) {
        j.at("transport_attrs").get_to(m.transport_attrs);
    }
}

struct FileSegmentDesc {
    std::vector<FileBufferDesc> buffers;

   public:
    NLOHMANN_DEFINE_TYPE_INTRUSIVE(FileSegmentDesc, buffers);
};

enum class SegmentType { Memory, File };

inline void to_json(json& j, const SegmentType& t) {
    j = (t == SegmentType::Memory ? "memory" : "file");
}

inline void from_json(const json& j, SegmentType& t) {
    auto s = j.get<std::string>();
    if (s == "memory")
        t = SegmentType::Memory;
    else if (s == "file")
        t = SegmentType::File;
    else
        throw std::runtime_error("unknown segment type: " + s);
}

struct SegmentDesc {
    std::string name;
    SegmentType type;
    std::string machine_id;
    std::variant<MemorySegmentDesc, FileSegmentDesc> detail;

   public:
    BufferDesc* findBuffer(uint64_t base, uint64_t length);
    DeviceDesc* findDevice(const std::string& name);
    const MemorySegmentDesc& getMemory() const {
        return std::get<MemorySegmentDesc>(detail);
    }
};

inline void to_json(json& j, const SegmentDesc& s) {
    j = json{{"name", s.name}, {"type", s.type}, {"machine_id", s.machine_id}};
    if (s.type == SegmentType::Memory) {
        j["detail"] = std::get<MemorySegmentDesc>(s.detail);
    } else {
        j["detail"] = std::get<FileSegmentDesc>(s.detail);
    }
}

inline void from_json(const json& j, SegmentDesc& s) {
    j.at("name").get_to(s.name);
    j.at("type").get_to(s.type);
    j.at("machine_id").get_to(s.machine_id);
    if (s.type == SegmentType::Memory) {
        s.detail = j.at("detail").get<MemorySegmentDesc>();
    } else {
        s.detail = j.at("detail").get<FileSegmentDesc>();
    }
}

using SegmentDescRef = std::shared_ptr<SegmentDesc>;

}  // namespace tent
}  // namespace mooncake

#endif  // SEGMENT_H