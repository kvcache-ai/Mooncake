// Copyright 2026 KVCache.AI
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

#ifndef MOONCAKE_CORO_HANDSHAKE_PLUGIN_H
#define MOONCAKE_CORO_HANDSHAKE_PLUGIN_H

#include <cstdint>
#include <string>

#include "engine.h"
#include "transfer_metadata_plugin.h"

namespace mooncake {

class CoroHandShakePlugin : public HandShakePlugin {
   public:
    int startDaemon(uint16_t listen_port, int sockfd) override;
    int send(std::string ip_or_host_name, uint16_t rpc_port,
             const Json::Value &local, Json::Value &peer) override;
    int sendNotify(std::string ip_or_host_name, uint16_t rpc_port,
                   const Json::Value &local, Json::Value &peer) override;
    int sendProbe(std::string ip_or_host_name, uint16_t rpc_port,
                  const Json::Value &local, Json::Value &peer) override;
    int exchangeMetadata(std::string ip_or_host_name, uint16_t rpc_port,
                         const Json::Value &local_metadata,
                         Json::Value &peer_metadata) override;
    void registerOnConnectionCallBack(OnReceiveCallBack callback) override;
    void registerOnMetadataCallBack(OnReceiveCallBack callback) override;
    void registerOnNotifyCallBack(OnReceiveCallBack callback) override;
    void registerOnProbeCallBack(OnReceiveCallBack callback) override;

    int CallRaw(const std::string &host, uint16_t port, int func_id,
                const std::string &attachment, std::string &response);

   private:
    int Exchange(int func_id, const std::string &host, uint16_t port,
                 const Json::Value &local, Json::Value &peer);

    HandshakeRpc rpc_;
};

}  // namespace mooncake

#endif  // MOONCAKE_CORO_HANDSHAKE_PLUGIN_H
