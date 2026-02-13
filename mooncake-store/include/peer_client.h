#pragma once

#include "client_rpc_types.h"
#include "types.h"
#include <vector>
#include <string>
#include <ylt/coro_rpc/coro_rpc_client.hpp>
#include <ylt/coro_io/client_pool.hpp>
#include <ylt/util/tl/expected.hpp>
#include <async_simple/coro/Lazy.h>
#include <async_simple/coro/SyncAwait.h>

namespace mooncake {

class PeerClient {
   public:
    tl::expected<void, ErrorCode> Connect(const std::string& endpoint);

    // --- Async single-key interfaces ---
    async_simple::coro::Lazy<tl::expected<void, ErrorCode>> AsyncReadRemoteData(
        const RemoteReadRequest& request);

    async_simple::coro::Lazy<tl::expected<void, ErrorCode>>
    AsyncWriteRemoteData(const RemoteWriteRequest& request);

    // --- Sync single-key interfaces ---
    tl::expected<void, ErrorCode> ReadRemoteData(
        const RemoteReadRequest& request);
    tl::expected<void, ErrorCode> WriteRemoteData(
        const RemoteWriteRequest& request);

   private:
    std::shared_ptr<coro_io::client_pools<coro_rpc::coro_rpc_client>>
        client_pools_;
    std::shared_ptr<coro_io::client_pool<coro_rpc::coro_rpc_client>>
        client_pool_;
    std::string endpoint_;
};

}  // namespace mooncake
