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

#ifndef MOONCAKE_CORO_HANDSHAKE_ENGINE_H
#define MOONCAKE_CORO_HANDSHAKE_ENGINE_H

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <unordered_map>

#include <async_simple/coro/Lazy.h>
#include <json/json.h>

#include "client_pool.h"
#include "rpc_config.h"
#include "transfer_metadata_plugin.h"

namespace mooncake {

constexpr int kHandshakeConnection = 0;
constexpr int kHandshakeMetadata = 1;
constexpr int kHandshakeNotify = 2;
constexpr int kHandshakeProbe = 3;

enum class HandshakeStatus : int32_t {
    kOk = 0,
    kBusy = 1,
    kBadJson = 2,
    kCallbackMissing = 3,
    kHandlerFailed = 4,
    kResponseTooLarge = 5,
};

bool ParseHandshakeJson(std::string_view text, Json::Value &value,
                        std::string &error);
std::string WriteHandshakeJson(const Json::Value &value);

// One object serves the local handshake port and places outbound calls.
class HandshakeRpc {
   public:
    HandshakeRpc();
    ~HandshakeRpc();

    HandshakeRpc(const HandshakeRpc &) = delete;
    HandshakeRpc &operator=(const HandshakeRpc &) = delete;

    bool running() const { return running_; }
    bool matches(uint16_t port, bool ipv6, int threads) const {
        return running_ && port_ == port && limits_.ipv6 == ipv6 &&
               limits_.threads == threads;
    }
    const HandshakeLimits &limits() const { return limits_; }

    int Start(uint16_t port, int sockfd, bool *fd_consumed);
    void Stop();
    void SetCallback(int func_id, HandShakePlugin::OnReceiveCallBack callback);
    int Call(const std::string &host, uint16_t port, int func_id,
             const std::string &request, std::string &response);

    async_simple::coro::Lazy<int32_t> process(int func_id);

   private:
    struct ListenState;
    struct OffloadPool;

    async_simple::coro::Lazy<std::pair<int, std::string>> CallOne(
        std::string host, uint16_t port, int func_id, std::string request);
    bool EnterHandler();
    void LeaveHandler();
    bool TryPermit();
    void ReleasePermit();

    struct Permit {
        explicit Permit(HandshakeRpc *rpc) : rpc_(rpc) {}
        ~Permit() { rpc_->ReleasePermit(); }
        HandshakeRpc *rpc_;
    };
    struct InflightGuard {
        explicit InflightGuard(HandshakeRpc *rpc) : rpc_(rpc) {}
        ~InflightGuard() { rpc_->LeaveHandler(); }
        HandshakeRpc *rpc_;
    };

    HandshakeLimits limits_;
    std::unique_ptr<HandshakeClientPool> pool_;
    std::unique_ptr<OffloadPool> offload_;
    std::unique_ptr<ListenState> listen_;
    bool running_ = false;
    bool stopping_ = false;
    uint16_t port_ = 0;

    std::mutex lifecycle_mu_;
    std::condition_variable lifecycle_cv_;
    int inflight_ = 0;
    std::atomic<int> inflight_offload_{0};

    std::mutex callback_mu_;
    std::unordered_map<int, HandShakePlugin::OnReceiveCallBack> callbacks_;
};

}  // namespace mooncake

#endif  // MOONCAKE_CORO_HANDSHAKE_ENGINE_H
