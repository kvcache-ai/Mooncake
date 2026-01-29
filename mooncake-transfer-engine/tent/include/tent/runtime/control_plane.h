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
#include <netdb.h>

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <thread>
#include <unordered_map>
#include <variant>

#include "tent/runtime/metastore.h"
#include "tent/runtime/segment.h"
#include "tent/runtime/segment_manager.h"
#include "tent/rpc/rpc.h"
#include "tent/runtime/topology.h"
#include "tent/thirdparty/nlohmann/json.h"

namespace mooncake {
namespace tent {
class TransferEngineImpl;

struct BootstrapDesc {
    std::string local_nic_path;
    std::string peer_nic_path;
    std::vector<uint32_t> qp_num;
    std::string reply_msg;       // on error
    uint32_t notify_qp_num = 0;  // Notification QP number (0 = not supported)

   public:
    NLOHMANN_DEFINE_TYPE_INTRUSIVE(BootstrapDesc, local_nic_path, peer_nic_path,
                                   qp_num, reply_msg, notify_qp_num);
};

struct XferDataDesc {
    uint64_t peer_mem_addr;
    size_t length;
};

using OnReceiveBootstrap =
    std::function<int(const BootstrapDesc& request, BootstrapDesc& response)>;

using OnNotify = std::function<int(const Notification&)>;

class ControlClient {
   public:
    ControlClient() {}

    ~ControlClient() {}

   public:
    static Status getSegmentDesc(const std::string& server_addr,
                                 std::string& response);

    static Status bootstrap(const std::string& server_addr,
                            const BootstrapDesc& request,
                            BootstrapDesc& response);

    static Status sendData(const std::string& server_addr,
                           uint64_t peer_mem_addr, void* local_mem_addr,
                           size_t length);

    static Status recvData(const std::string& server_addr,
                           uint64_t peer_mem_addr, void* local_mem_addr,
                           size_t length);

    static Status notify(const std::string& server_addr,
                         const Notification& message);

    static Status delegate(const std::string& server_addr,
                           const Request& request);

    static Status pinStageBuffer(const std::string& server_addr,
                                 const std::string& location, uint64_t& addr);

    static Status unpinStageBuffer(const std::string& server_addr,
                                   uint64_t addr);
};

class ControlService {
   public:
    ControlService(const std::string& type, const std::string& servers,
                   TransferEngineImpl* impl);

    ControlService(const std::string& type, const std::string& servers,
                   const std::string& password, uint8_t db_index,
                   TransferEngineImpl* impl);

    ~ControlService();

    ControlService(const ControlService&) = delete;
    ControlService& operator=(const ControlService&) = delete;

    SegmentManager& segmentManager() { return *manager_.get(); }

    void setBootstrapRdmaCallback(const OnReceiveBootstrap& callback) {
        bootstrap_callback_ = callback;
    }

    void setNotifyCallback(const OnNotify& callback) {
        notify_callback_ = callback;
    }

    Status start(uint16_t& port, bool ipv6_ = false);

   private:
    void onGetSegmentDesc(const std::string_view& request,
                          std::string& response);

    void onBootstrapRdma(const std::string_view& request,
                         std::string& response);

    void onSendData(const std::string_view& request, std::string& response);

    void onRecvData(const std::string_view& request, std::string& response);

    void onNotify(const std::string_view& request, std::string& response);

    void onDelegate(const std::string_view& request, std::string& response);

    void onPinStageBuffer(const std::string_view& request,
                          std::string& response);

    void onUnpinStageBuffer(const std::string_view& request,
                            std::string& response);

   private:
    std::unique_ptr<SegmentManager> manager_;
    std::shared_ptr<CoroRpcAgent> rpc_server_;

    OnReceiveBootstrap bootstrap_callback_;
    OnNotify notify_callback_;
    TransferEngineImpl* impl_;
};

}  // namespace tent
}  // namespace mooncake

#endif  // TRANSFER_METADATA_V1_H