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

#ifndef TRANSFER_METADATA_V1_H
#define TRANSFER_METADATA_V1_H

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

#define P2PHANDSHAKE "P2PHANDSHAKE"

struct DeviceDesc {
    std::string name;
    uint16_t lid;
    std::string gid;
};

enum class MemBufferType { DRAM, VRAM };

struct BufferDesc {
    std::string location;  // will be obstacled
    uint64_t addr;
    uint64_t length;
    MemBufferType type;
    int device_id;               // numa id for DRAM, GPU device id for VRAM
    std::vector<uint32_t> rkey;  // rkey for each RDMA device change to map ...
    std::string shared_handle;   // shm path for IPC/CXL, or serialized shared
                                 // handle for NVLINK
};

struct FileBufferDesc {
    std::string path;
    uint64_t length;
    uint64_t offset;
};

struct MemorySegmentDesc {
    Topology topology;
    std::vector<DeviceDesc> devices;  // TODO: change to map ...
    std::vector<BufferDesc> buffers;
    std::string handshake_tcp_addr;
    uint16_t handshake_tcp_port;
};

struct FileSegmentDesc {
    std::vector<FileBufferDesc> buffers;
};

enum class SegmentType { Memory, File };

struct SegmentDesc {
    std::string name;
    SegmentType type;
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

    SegmentDescRef getLocal() { return local_desc_; }

    void setLocal(const SegmentDescRef &desc) { local_desc_ = desc; }

    Status applyLocal();

   private:
    RWSpinlock lock_;
    std::unordered_map<SegmentID, SegmentDescRef> id_to_desc_map_;
    std::unordered_map<SegmentID, std::string> id_to_name_map_;
    std::unordered_map<std::string, SegmentID> name_to_id_map_;
    std::atomic<SegmentID> next_id_;
    SegmentDescRef local_desc_;
    std::unique_ptr<MetadataStore> store_;
};

class MetadataStore {
   public:
    MetadataStore() {}

    virtual ~MetadataStore() {}

    MetadataStore(const MetadataStore &) = delete;
    MetadataStore &operator=(const MetadataStore &) = delete;

   public:
    virtual Status getSegmentDesc(SegmentDescRef &desc,
                                  const std::string &segment_name) = 0;

    virtual Status putSegmentDesc(SegmentDescRef &desc) = 0;
};

class CentralMetadataStore : public MetadataStore {
   public:
    CentralMetadataStore(const std::string &connection_string);

    virtual ~CentralMetadataStore() {}

   public:
    virtual Status getSegmentDesc(SegmentDescRef &desc,
                                  const std::string &segment_name);

    virtual Status putSegmentDesc(SegmentDescRef &desc);

   private:
    std::shared_ptr<MetadataPlugin> plugin_;
};

class P2PMetadataStore : public MetadataStore {
   public:
    P2PMetadataStore() {}

    virtual ~P2PMetadataStore() {}

   public:
    virtual Status getSegmentDesc(SegmentDescRef &desc,
                                  const std::string &segment_name);

    virtual Status putSegmentDesc(SegmentDescRef &desc) {
        return Status::OK();  // no operation in purpose
    }

   private:
    std::unique_ptr<AsioRpcClient> client_;
};

struct HandShakeDesc {
    std::string local_nic_path;
    std::string peer_nic_path;
    std::vector<uint32_t> qp_num;
    std::string reply_msg;  // on error
};

using OnReceiveHandShake =
    std::function<int(const HandShakeDesc &request, HandShakeDesc &response)>;

class BootstrapRdmaClient {
   public:
    BootstrapRdmaClient() {}

    ~BootstrapRdmaClient() {}

   public:
    Status bootstrap(const std::string &segment_name,
                     const HandShakeDesc &request, HandShakeDesc &response);

   private:
    std::unique_ptr<AsioRpcClient> client_;
};

class MetadataService {
   public:
    MetadataService(const std::string &conn_string);

    ~MetadataService();

    MetadataService(const MetadataService &) = delete;
    MetadataService &operator=(const MetadataService &) = delete;

    SegmentManager &segmentManager() { return *manager_.get(); }

    void setBootstrapRdmaCallback(const OnReceiveHandShake &callback) {
        bootstrap_callback_ = callback;
    }

    void start(uint16_t &port);

   private:
    void onGetSegmentDesc(const RpcRawData &request, RpcRawData &response);

    void onBootstrapRdma(const RpcRawData &request, RpcRawData &response);

   private:
    std::unique_ptr<SegmentManager> manager_;
    std::shared_ptr<AsioRpcServer> rpc_server_;

    OnReceiveHandShake bootstrap_callback_;
};

// ----------------------------------------------------------------------------

class TransferMetadata {
   public:
    TransferMetadata(const std::string &conn_string) {
        service_ = std::make_shared<MetadataService>(conn_string);
    }

    ~TransferMetadata() {}

    SegmentID getSegmentID(const std::string &segment_name) {
        auto &manager = service_->segmentManager();
        SegmentID handle;
        auto status = manager.openRemote(handle, segment_name);
        assert(status.ok());
        return handle;
    }

    std::shared_ptr<SegmentDesc> getLocalSegment() {
        return service_->segmentManager().getLocal();
    }

    std::shared_ptr<SegmentDesc> getSegmentDescByName(
        const std::string &segment_name, bool force_update = false) {
        auto &manager = service_->segmentManager();
        SegmentID handle;
        auto status = manager.openRemote(handle, segment_name);
        assert(status.ok());
        std::shared_ptr<SegmentDesc> desc;
        status = manager.getRemote(desc, handle);
        assert(status.ok());
        return desc;
    }

    std::shared_ptr<SegmentDesc> getSegmentDescByID(SegmentID handle,
                                                    bool force_update = false) {
        auto &manager = service_->segmentManager();
        std::shared_ptr<SegmentDesc> desc;
        if (handle == 0) return manager.getLocal();
        auto status = manager.getRemote(desc, handle);
        assert(status.ok());
        return desc;
    }

    int updateSegmentDesc(const std::shared_ptr<SegmentDesc> &desc) {
        service_->segmentManager().setLocal(desc);
        service_->segmentManager().applyLocal();
        return 0;
    }

    int sendHandshake(const std::string &peer_server_name,
                      const HandShakeDesc &local_desc,
                      HandShakeDesc &peer_desc) {
        BootstrapRdmaClient bootstrap_client;
        bootstrap_client.bootstrap(peer_server_name, local_desc, peer_desc);
        return 0;
    }

    void setBootstrapRdmaCallback(const OnReceiveHandShake &callback) {
        service_->setBootstrapRdmaCallback(callback);
    }

    void start(uint16_t &port) { service_->start(port); }

   private:
    std::shared_ptr<MetadataService> service_;
};
}  // namespace v1
}  // namespace mooncake

#endif  // TRANSFER_METADATA_V1_H