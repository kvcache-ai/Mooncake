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

#include "v1/runtime/plugin.h"
#include "v1/runtime/segment.h"
#include "v1/rpc/rpc.h"
#include "v1/runtime/topology.h"

namespace mooncake {
namespace v1 {
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

    virtual Status deleteSegmentDesc(const std::string &segment_name) = 0;
};

class CentralMetadataStore : public MetadataStore {
   public:
    CentralMetadataStore(const std::string &type, const std::string &servers);

    virtual ~CentralMetadataStore() {}

   public:
    virtual Status getSegmentDesc(SegmentDescRef &desc,
                                  const std::string &segment_name);

    virtual Status putSegmentDesc(SegmentDescRef &desc);

    virtual Status deleteSegmentDesc(const std::string &segment_name);

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

    virtual Status deleteSegmentDesc(const std::string &segment_name) {
        return Status::OK();  // no operation in purpose
    }
};

struct BootstrapDesc {
    std::string local_nic_path;
    std::string peer_nic_path;
    std::vector<uint32_t> qp_num;
    std::string reply_msg;  // on error
};

struct XferDataDesc {
    uint64_t peer_mem_addr;
    size_t length;
};

using OnReceiveBootstrap =
    std::function<int(const BootstrapDesc &request, BootstrapDesc &response)>;

using OnNotify = std::function<int(const Notification &)>;

class RpcClient {
   public:
    RpcClient() {}

    ~RpcClient() {}

   public:
    static Status bootstrap(const std::string &server_addr,
                            const BootstrapDesc &request,
                            BootstrapDesc &response);

    static Status sendData(const std::string &server_addr,
                           uint64_t peer_mem_addr, void *local_mem_addr,
                           size_t length);

    static Status recvData(const std::string &server_addr,
                           uint64_t peer_mem_addr, void *local_mem_addr,
                           size_t length);

    static Status notify(const std::string &server_addr,
                         const Notification &message);
};

class MetadataService {
   public:
    MetadataService(const std::string &type, const std::string &servers);

    ~MetadataService();

    MetadataService(const MetadataService &) = delete;
    MetadataService &operator=(const MetadataService &) = delete;

    SegmentManager &segmentManager() { return *manager_.get(); }

    void setBootstrapRdmaCallback(const OnReceiveBootstrap &callback) {
        bootstrap_callback_ = callback;
    }

    void setNotifyCallback(const OnNotify &callback) {
        notify_callback_ = callback;
    }

    Status start(uint16_t &port, bool ipv6_ = false);

   private:
    void onGetSegmentDesc(const std::string_view &request,
                          std::string &response);

    void onBootstrapRdma(const std::string_view &request,
                         std::string &response);

    void onSendData(const std::string_view &request, std::string &response);

    void onRecvData(const std::string_view &request, std::string &response);

    void onNotify(const std::string_view &request, std::string &response);

   private:
    std::unique_ptr<SegmentManager> manager_;
    std::shared_ptr<CoroRpcAgent> rpc_server_;

    OnReceiveBootstrap bootstrap_callback_;
    OnNotify notify_callback_;
};

}  // namespace v1
}  // namespace mooncake

#endif  // TRANSFER_METADATA_V1_H