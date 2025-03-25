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
#include <functional>
#include <memory>
#include <string>
#include <thread>
#include <unordered_map>

#include "common.h"
#include "topology.h"
#include "segment_cache.h"

namespace mooncake {
struct MetadataStoragePlugin;
struct HandShakePlugin;

class TransferMetadata {
   public:
    using SegmentID = uint64_t;

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

    SegmentCache::SegmentDescRef getSegmentDesc(const std::string &segment_name);

    Status putSegmentDesc(SegmentCache::SegmentDescRef segment);

    Status deleteSegmentDesc(const std::string &segment_name);

   public:
    TransferMetadata(const std::string &conn_string, const std::string &local_segment_name = "");

    ~TransferMetadata();

    std::shared_ptr<SegmentDesc> getSegmentDescByName(
        const std::string &segment_name, bool force_update = false);

    std::shared_ptr<SegmentDesc> getSegmentDescByID(SegmentID segment_id,
                                                    bool force_update = false);

    int updateLocalSegmentDesc();

    SegmentID getSegmentID(const std::string &segment_name);

    int syncSegmentCache(const std::string &segment_name);

    int addLocalMemoryBuffer(const BufferAttr &buffer_desc,
                             bool update_metadata);

    int removeLocalMemoryBuffer(void *addr, bool update_metadata);

    std::shared_ptr<SegmentDesc> getLocalSegment();

    int setLocalSegment(std::shared_ptr<SegmentDesc> &&desc);

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

   private:

    RWSpinlock rpc_meta_lock_;
    std::unordered_map<std::string, RpcMetaDesc> rpc_meta_map_;
    RpcMetaDesc local_rpc_meta_;

    std::shared_ptr<SegmentCache> cache_;
    std::shared_ptr<HandShakePlugin> handshake_plugin_;
    std::shared_ptr<MetadataStoragePlugin> storage_plugin_;
};

}  // namespace mooncake

#endif  // TRANSFER_METADATA