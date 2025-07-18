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

#ifndef SEGMENT_MANAGER_H
#define SEGMENT_MANAGER_H

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

#include "v1/common.h"
#include "v1/metadata/plugin.h"
#include "v1/utility/rpc.h"
#include "v1/utility/topology.h"

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

    int ref_count;
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

class MetadataStore;

class SegmentManager {
   public:
    SegmentManager(std::unique_ptr<MetadataStore> agent);

    ~SegmentManager();

    SegmentManager(const SegmentManager &) = delete;
    SegmentManager &operator=(const SegmentManager &) = delete;

   public:
    Status openRemote(SegmentID &handle, const std::string &segment_name);

    Status closeRemote(SegmentID handle);

    Status getRemote(SegmentDescRef &desc, SegmentID handle);

    Status getRemote(SegmentDescRef &desc, const std::string &segment_name);

    Status invalidateRemote(SegmentID handle);

    bool isSameMachine(SegmentID handle);

   public:
    SegmentDescRef getLocal() { return local_desc_; }

    Status synchronizeLocal();

    Status deleteLocal();

   private:
    RWSpinlock lock_;
    std::unordered_map<SegmentID, SegmentDescRef> id_to_desc_map_;
    std::unordered_map<SegmentID, std::string> id_to_name_map_;
    std::unordered_map<std::string, SegmentID> name_to_id_map_;
    std::atomic<SegmentID> next_id_;

    SegmentDescRef local_desc_;

    std::unique_ptr<MetadataStore> store_;
};

class RemoteSegmentCache {
   public:
    RemoteSegmentCache(SegmentManager &manager, uint64_t ttl_ms = 500)
        : manager_(manager), ttl_ms_(ttl_ms), last_refresh_timestamp_(0) {}

    ~RemoteSegmentCache() {}

    RemoteSegmentCache(const RemoteSegmentCache &) = delete;
    RemoteSegmentCache &operator=(const RemoteSegmentCache &) = delete;

   public:
    Status get(SegmentDesc *&desc, SegmentID handle);

   private:
    SegmentManager &manager_;
    const uint64_t ttl_ms_;
    uint64_t last_refresh_timestamp_;
    std::unordered_map<SegmentID, SegmentDescRef> id_to_desc_map_;
};

class LocalSegmentTracker {
   public:
    LocalSegmentTracker(const SegmentDescRef &local_desc)
        : local_desc_(local_desc) {}

    ~LocalSegmentTracker() {}

    LocalSegmentTracker(const LocalSegmentTracker &) = delete;
    LocalSegmentTracker &operator==(const LocalSegmentTracker &) = delete;

   public:
    Status query(uint64_t base, size_t length,
                 std::vector<BufferDesc *> &result);

    Status add(uint64_t base, size_t length,
               std::function<Status(BufferDesc &)> callback);

    Status remove(uint64_t base, size_t length,
                  std::function<Status(BufferDesc &)> callback);

    Status update(std::function<Status(BufferDesc &)> callback);

   private:
    SegmentDescRef local_desc_;
};

}  // namespace v1
}  // namespace mooncake

#endif  // SEGMENT_MANAGER_H