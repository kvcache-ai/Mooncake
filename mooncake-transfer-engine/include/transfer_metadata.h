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

#ifndef TRANSFER_METADATA
#define TRANSFER_METADATA

#include <glog/logging.h>
#include <jsoncpp/json/json.h>
#include <netdb.h>

#include <atomic>
#include <cstdint>
#include <etcd/SyncClient.hpp>
#include <functional>
#include <memory>
#include <string>
#include <thread>
#include <unordered_map>

#include "common.h"

namespace mooncake {
struct MetadataStoragePlugin;
struct HandShakePlugin;

class TransferMetadata {
   public:
    struct DeviceDesc {
        std::string name;
        uint16_t lid;
        std::string gid;
    };

    struct BufferDesc {
        std::string name;
        uint64_t addr;
        uint64_t length;
        std::vector<uint32_t> lkey;
        std::vector<uint32_t> rkey;
    };

    struct NVMeoFBufferDesc {
        std::string file_path;
        uint64_t length;
        std::unordered_map<std::string, std::string> local_path_map;
    };

    struct PriorityItem {
        std::vector<std::string> preferred_rnic_list;
        std::vector<std::string> available_rnic_list;
        std::vector<int> preferred_rnic_id_list;
        std::vector<int> available_rnic_id_list;
    };

    using PriorityMatrix = std::unordered_map<std::string, PriorityItem>;
    using SegmentID = uint64_t;

    struct SegmentDesc {
        std::string name;
        std::string protocol;
        // this is for rdma
        std::vector<DeviceDesc> devices;
        PriorityMatrix priority_matrix;
        std::vector<BufferDesc> buffers;
        // this is for nvmeof.
        std::vector<NVMeoFBufferDesc> nvmeof_buffers;
        // TODO : make these two a union or a std::variant
    };

    struct RpcMetaDesc {
        std::string ip_or_host_name;
        uint16_t rpc_port;
    };

    struct HandShakeDesc {
        std::string local_nic_path;
        std::string peer_nic_path;
        std::vector<uint32_t> qp_num;
        std::string reply_msg;  // on error
    };

   public:
    TransferMetadata(const std::string &conn_string);

    ~TransferMetadata();

    std::shared_ptr<SegmentDesc> getSegmentDescByName(
        const std::string &segment_name, bool force_update = false);

    std::shared_ptr<SegmentDesc> getSegmentDescByID(SegmentID segment_id,
                                                    bool force_update = false);

    int updateLocalSegmentDesc(SegmentID segment_id = LOCAL_SEGMENT_ID);

    int updateSegmentDesc(const std::string &segment_name,
                          const SegmentDesc &desc);

    std::shared_ptr<SegmentDesc> getSegmentDesc(
        const std::string &segment_name);

    SegmentID getSegmentID(const std::string &segment_name);

    int invalidateSegmentCache();

    int removeSegmentDesc(const std::string &segment_name);

    int addLocalMemoryBuffer(const BufferDesc &buffer_desc,
                             bool update_metadata);

    int removeLocalMemoryBuffer(void *addr, bool update_metadata);

    int addLocalSegment(SegmentID segment_id, const std::string &segment_name,
                        std::shared_ptr<SegmentDesc> &&desc);

    int addRpcMetaEntry(const std::string &server_name, RpcMetaDesc &desc);

    int removeRpcMetaEntry(const std::string &server_name);

    int getRpcMetaEntry(const std::string &server_name, RpcMetaDesc &desc);

    const RpcMetaDesc &localRpcMeta() const { return local_rpc_meta_; }

    using OnReceiveHandShake = std::function<int(const HandShakeDesc &peer_desc,
                                                 HandShakeDesc &local_desc)>;
    int startHandshakeDaemon(OnReceiveHandShake on_receive_handshake,
                             uint16_t listen_port);

    int sendHandshake(const std::string &peer_server_name,
                      const HandShakeDesc &local_desc,
                      HandShakeDesc &peer_desc);

    static int parseNicPriorityMatrix(const std::string &nic_priority_matrix,
                                      PriorityMatrix &priority_map,
                                      std::vector<std::string> &rnic_list);

   private:
    // local cache
    RWSpinlock segment_lock_;
    std::unordered_map<uint64_t, std::shared_ptr<SegmentDesc>>
        segment_id_to_desc_map_;
    std::unordered_map<std::string, uint64_t> segment_name_to_id_map_;

    RWSpinlock rpc_meta_lock_;
    std::unordered_map<std::string, RpcMetaDesc> rpc_meta_map_;
    RpcMetaDesc local_rpc_meta_;

    std::atomic<SegmentID> next_segment_id_;

    std::shared_ptr<HandShakePlugin> handshake_plugin_;
    std::shared_ptr<MetadataStoragePlugin> storage_plugin_;
};

}  // namespace mooncake

#endif  // TRANSFER_METADATA