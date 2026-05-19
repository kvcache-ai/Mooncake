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
    // RDMA address of local_nic_path.
    uint16_t local_lid = 0;
    std::string local_gid;
    std::string reply_msg;       // on error
    uint32_t notify_qp_num = 0;  // Notification QP number (0 = not supported)

   public:
    NLOHMANN_DEFINE_TYPE_INTRUSIVE(BootstrapDesc, local_nic_path, peer_nic_path,
                                   qp_num, local_lid, local_gid, reply_msg,
                                   notify_qp_num);
};

struct XferDataDesc {
    uint64_t peer_mem_addr;
    size_t length;
};

struct NcclBootstrapDesc {
    std::string session_key;
    std::string unique_id;
    int device_index = 0;
    std::string reply_msg;

   public:
    NLOHMANN_DEFINE_TYPE_INTRUSIVE(NcclBootstrapDesc, session_key, unique_id,
                                   device_index, reply_msg);
};

struct NcclWindowDesc {
    std::string session_key;
    std::string window_key;
    uint64_t addr = 0;
    uint64_t length = 0;
    int device_index = 0;
    int win_flags = 0;
    bool allocate_local = false;
    std::string reply_msg;

   public:
    NLOHMANN_DEFINE_TYPE_INTRUSIVE(NcclWindowDesc, session_key, window_key,
                                   addr, length, device_index, win_flags,
                                   allocate_local, reply_msg);
};

struct NcclSignalDesc {
    std::string session_key;
    int peer = 0;
    int op_count = 1;
    int signal_index = 0;
    int context = 0;
    int device_index = 0;
    std::string reply_msg;

   public:
    NLOHMANN_DEFINE_TYPE_INTRUSIVE(NcclSignalDesc, session_key, peer,
                                   op_count, signal_index, context,
                                   device_index, reply_msg);
};

using OnReceiveBootstrap =
    std::function<int(const BootstrapDesc& request, BootstrapDesc& response)>;

using OnReceiveNcclBootstrap = std::function<int(
    const NcclBootstrapDesc& request, NcclBootstrapDesc& response)>;

using OnReceiveNcclWindow =
    std::function<int(const NcclWindowDesc& request, NcclWindowDesc& response)>;

using OnReceiveNcclSignal =
    std::function<int(const NcclSignalDesc& request, NcclSignalDesc& response)>;

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

    static Status bootstrapNccl(const std::string& server_addr,
                                const NcclBootstrapDesc& request,
                                NcclBootstrapDesc& response);

    static Status registerNcclWindow(const std::string& server_addr,
                                     const NcclWindowDesc& request,
                                     NcclWindowDesc& response);

    static Status waitNcclSignal(const std::string& server_addr,
                                 const NcclSignalDesc& request,
                                 NcclSignalDesc& response);

    static Status sendData(const std::string& server_addr,
                           uint64_t peer_mem_addr, void* local_mem_addr,
                           size_t length);

    static Status recvData(const std::string& server_addr,
                           uint64_t peer_mem_addr, void* local_mem_addr,
                           size_t length);

    static Status notify(const std::string& server_addr,
                         const Notification& message);

    static Status probe(const std::string& server_addr);

    static Status delegate(const std::string& server_addr,
                           const Request& request);

    static Status pinStageBuffer(const std::string& server_addr,
                                 const std::string& location, uint64_t& addr);

    static Status unpinStageBuffer(const std::string& server_addr,
                                   uint64_t addr);

    static void subscribeSegmentUpdateAsync(const std::string& server_addr,
                                            const std::string& subscriber_addr);

    using onNotifySegmentUpdateFailure = std::function<void()>;
    static void notifySegmentUpdatedAsync(
        const std::string& server_addr, const std::string& segment_name,
        const onNotifySegmentUpdateFailure& on_failure);
};

class ControlService {
   public:
    ControlService(const std::string& type, const std::string& servers,
                   TransferEngineImpl* impl);

    ControlService(const std::string& type, const std::string& servers,
                   const std::string& password, uint8_t db_index,
                   TransferEngineImpl* impl);

    ControlService(const std::string& type, const std::string& servers,
                   const std::string& username, const std::string& password,
                   uint8_t db_index, TransferEngineImpl* impl);

    ~ControlService();

    ControlService(const ControlService&) = delete;
    ControlService& operator=(const ControlService&) = delete;

    SegmentManager& segmentManager() { return *manager_.get(); }

    void setBootstrapRdmaCallback(const OnReceiveBootstrap& callback) {
        bootstrap_callback_ = callback;
    }

    void setBootstrapNcclCallback(const OnReceiveNcclBootstrap& callback) {
        nccl_bootstrap_callback_ = callback;
    }

    void setNcclWindowCallback(const OnReceiveNcclWindow& callback) {
        nccl_window_callback_ = callback;
    }

    void setNcclSignalCallback(const OnReceiveNcclSignal& callback) {
        nccl_signal_callback_ = callback;
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

    void onBootstrapNccl(const std::string_view& request,
                         std::string& response);

    void onRegisterNcclWindow(const std::string_view& request,
                              std::string& response);

    void onWaitNcclSignal(const std::string_view& request,
                          std::string& response);

    void onSendData(const std::string_view& request, std::string& response);

    void onRecvData(const std::string_view& request, std::string& response);

    void onNotify(const std::string_view& request, std::string& response);

    void onProbe(const std::string_view& request, std::string& response);

    void onDelegate(const std::string_view& request, std::string& response);

    void onPinStageBuffer(const std::string_view& request,
                          std::string& response);

    void onUnpinStageBuffer(const std::string_view& request,
                            std::string& response);

    void onSubscribeSegmentUpdate(const std::string_view& request,
                                  std::string& response);

    void onSegmentUpdated(const std::string_view& request,
                          std::string& response);

   private:
    std::unique_ptr<SegmentManager> manager_;
    std::shared_ptr<CoroRpcAgent> rpc_server_;

    OnReceiveBootstrap bootstrap_callback_;
    OnReceiveNcclBootstrap nccl_bootstrap_callback_;
    OnReceiveNcclWindow nccl_window_callback_;
    OnReceiveNcclSignal nccl_signal_callback_;
    OnNotify notify_callback_;
    TransferEngineImpl* impl_;
};

}  // namespace tent
}  // namespace mooncake

#endif  // TRANSFER_METADATA_V1_H
