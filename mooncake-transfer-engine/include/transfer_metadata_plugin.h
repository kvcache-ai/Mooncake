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

#ifndef TRANSFER_METADATA_PLUGIN
#define TRANSFER_METADATA_PLUGIN

#include "transfer_metadata.h"

namespace mooncake {
struct MetadataStoragePlugin {
    static std::shared_ptr<MetadataStoragePlugin> Create(
        const std::string &conn_string);

    MetadataStoragePlugin() {}
    virtual ~MetadataStoragePlugin() {}

    virtual bool get(const std::string &key, Json::Value &value) = 0;
    virtual bool set(const std::string &key, const Json::Value &value) = 0;
    virtual bool remove(const std::string &key) = 0;
};

struct HandShakePlugin {
    static std::shared_ptr<HandShakePlugin> Create(
        const std::string &conn_string);

    HandShakePlugin() {}
    virtual ~HandShakePlugin() {}

    // When accept a new connection/metadata request, this function will be
    // called. The first param represents peer attributes, while the
    // second param represents local attributes.
    using OnReceiveCallBack =
        std::function<int(const Json::Value &, Json::Value &)>;

    virtual int startDaemon(uint16_t listen_port, int sockfd) = 0;

    // Connect to peer endpoint, and wait for receiving
    // peer endpoint's attributes
    virtual int send(std::string ip_or_host_name, uint16_t rpc_port,
                     const Json::Value &local, Json::Value &peer) = 0;
    virtual int sendNotify(std::string ip_or_host_name, uint16_t rpc_port,
                           const Json::Value &local, Json::Value &peer) = 0;

    // Exchange metadata with remote peer.
    virtual int exchangeMetadata(std::string ip_or_host_name, uint16_t rpc_port,
                                 const Json::Value &local_metadata,
                                 Json::Value &peer_metadata) = 0;

    // Register callback function for receiving a new connection.
    virtual void registerOnConnectionCallBack(OnReceiveCallBack callback) = 0;

    // Register callback function for receiving metadata exchange request.
    virtual void registerOnMetadataCallBack(OnReceiveCallBack callback) = 0;

    // Register callback function for receiving metadata exchange request.
    virtual void registerOnNotifyCallBack(OnReceiveCallBack callback) = 0;

    // Register callback function for receiving delete endpoint request.
    virtual void registerOnDeleteEndpointCallBack(
        OnReceiveCallBack callback) = 0;

    // Send delete endpoint notification to peer endpoint.
    virtual int sendDeleteEndpoint(std::string ip_or_host_name,
                                   uint16_t rpc_port,
                                   const Json::Value &local) = 0;
};

std::vector<std::string> findLocalIpAddresses();

uint16_t findAvailableTcpPort(int &sockfd, bool set_range = false);

}  // namespace mooncake

#endif  // TRANSFER_METADATA_PLUGIN
