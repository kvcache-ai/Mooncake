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

#include "plugin.h"

#include <unistd.h>

#include <glog/logging.h>

#include "error.h"
#include "handshake_rpc.h"

namespace mooncake {
namespace {

class InvalidHandShakePlugin : public HandShakePlugin {
   public:
    explicit InvalidHandShakePlugin(std::string configured)
        : configured_(std::move(configured)) {}

    int startDaemon(uint16_t, int) override { return Fail(); }
    int send(std::string, uint16_t, const Json::Value &,
             Json::Value &) override {
        return Fail();
    }
    int sendNotify(std::string, uint16_t, const Json::Value &,
                   Json::Value &) override {
        return Fail();
    }
    int sendProbe(std::string, uint16_t, const Json::Value &,
                  Json::Value &) override {
        return Fail();
    }
    int exchangeMetadata(std::string, uint16_t, const Json::Value &,
                         Json::Value &) override {
        return Fail();
    }
    void registerOnConnectionCallBack(OnReceiveCallBack) override {}
    void registerOnMetadataCallBack(OnReceiveCallBack) override {}
    void registerOnNotifyCallBack(OnReceiveCallBack) override {}
    void registerOnProbeCallBack(OnReceiveCallBack) override {}

   private:
    int Fail() const {
        LOG(ERROR) << "Invalid MC_HANDSHAKE_TRANSPORT=\"" << configured_
                   << "\", allowed values: socket, coro";
        return ERR_INVALID_ARGUMENT;
    }
    std::string configured_;
};

}  // namespace

int CoroHandShakePlugin::startDaemon(uint16_t listen_port, int sockfd) {
    const HandshakeLimits limits = LoadHandshakeLimits();
    if (rpc_.running()) {
        if (rpc_.matches(listen_port, limits.ipv6, limits.threads)) return 0;
        LOG(ERROR) << "coro handshake already listening with different "
                      "parameters";
        return ERR_INVALID_ARGUMENT;
    }
    bool consumed = false;
    const int rc = rpc_.Start(listen_port, sockfd, &consumed);
    if (rc != 0 && sockfd >= 0 && !consumed) close(sockfd);
    return rc;
}

int CoroHandShakePlugin::send(std::string ip_or_host_name, uint16_t rpc_port,
                              const Json::Value &local, Json::Value &peer) {
    return Exchange(kHandshakeConnection, ip_or_host_name, rpc_port, local,
                    peer);
}

int CoroHandShakePlugin::sendNotify(std::string ip_or_host_name,
                                    uint16_t rpc_port, const Json::Value &local,
                                    Json::Value &peer) {
    return Exchange(kHandshakeNotify, ip_or_host_name, rpc_port, local, peer);
}

int CoroHandShakePlugin::sendProbe(std::string ip_or_host_name,
                                   uint16_t rpc_port, const Json::Value &local,
                                   Json::Value &peer) {
    return Exchange(kHandshakeProbe, ip_or_host_name, rpc_port, local, peer);
}

int CoroHandShakePlugin::exchangeMetadata(std::string ip_or_host_name,
                                          uint16_t rpc_port,
                                          const Json::Value &local_metadata,
                                          Json::Value &peer_metadata) {
    return Exchange(kHandshakeMetadata, ip_or_host_name, rpc_port,
                    local_metadata, peer_metadata);
}

void CoroHandShakePlugin::registerOnConnectionCallBack(
    OnReceiveCallBack callback) {
    rpc_.SetCallback(kHandshakeConnection, std::move(callback));
}

void CoroHandShakePlugin::registerOnMetadataCallBack(
    OnReceiveCallBack callback) {
    rpc_.SetCallback(kHandshakeMetadata, std::move(callback));
}

void CoroHandShakePlugin::registerOnNotifyCallBack(OnReceiveCallBack callback) {
    rpc_.SetCallback(kHandshakeNotify, std::move(callback));
}

void CoroHandShakePlugin::registerOnProbeCallBack(OnReceiveCallBack callback) {
    rpc_.SetCallback(kHandshakeProbe, std::move(callback));
}

int CoroHandShakePlugin::CallRaw(const std::string &host, uint16_t port,
                                 int func_id, const std::string &attachment,
                                 std::string &response) {
    return rpc_.Call(host, port, func_id, attachment, response);
}

int CoroHandShakePlugin::Exchange(int func_id, const std::string &host,
                                  uint16_t port, const Json::Value &local,
                                  Json::Value &peer) {
    std::string response;
    const int rc =
        rpc_.Call(host, port, func_id, WriteHandshakeJson(local), response);
    if (rc != 0) return rc;
    std::string error;
    if (!ParseHandshakeJson(response, peer, error)) {
        LOG(ERROR) << "coro handshake response is not JSON: " << error;
        return ERR_MALFORMED_JSON;
    }
    return 0;
}

std::shared_ptr<HandShakePlugin> CreateCoroHandshakePlugin() {
    return std::make_shared<CoroHandShakePlugin>();
}

std::shared_ptr<HandShakePlugin> CreateInvalidHandshakePlugin(
    const std::string &configured_value) {
    return std::make_shared<InvalidHandShakePlugin>(configured_value);
}

}  // namespace mooncake
