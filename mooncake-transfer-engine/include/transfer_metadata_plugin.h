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

struct MetadataHandShakePlugin {
    static std::shared_ptr<MetadataHandShakePlugin> Create(
        const std::string &conn_string);

    MetadataHandShakePlugin() {}
    virtual ~MetadataHandShakePlugin() {}

    using OnReceiveCallBack =
        std::function<int(const Json::Value &, Json::Value &)>;

    virtual int startDaemon(OnReceiveCallBack on_recv_callback,
                            uint16_t listen_port) = 0;

    virtual int send(TransferMetadata::RpcMetaDesc peer_location,
                     const Json::Value &local, Json::Value &peer) = 0;
};

}  // namespace mooncake

#endif  // TRANSFER_METADATA_PLUGIN