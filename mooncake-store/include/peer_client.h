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
    // LIFETIME: `request` of sync read and write must remain valid
    // until the returned Lazy coroutine completes.
    // Otherwise, the string_view of `request` might be UAF.

    async_simple::coro::Lazy<tl::expected<void, ErrorCode>> AsyncReadRemoteData(
        const RemoteReadRequest& request);

    async_simple::coro::Lazy<tl::expected<UUID, ErrorCode>>
    AsyncWriteRemoteData(const RemoteWriteRequest& request);

    async_simple::coro::Lazy<tl::expected<PreWriteResponse, ErrorCode>>
    AsyncPreWrite(const PreWriteRequest& request);

    async_simple::coro::Lazy<tl::expected<void, ErrorCode>> AsyncWriteCommit(
        const WriteCommitRequest& request);

    async_simple::coro::Lazy<tl::expected<PinKeyResponse, ErrorCode>>
    AsyncPinKey(const PinKeyRequest& request);

    async_simple::coro::Lazy<tl::expected<void, ErrorCode>> AsyncUnPinKey(
        const UnPinKeyRequest& request);

    // --- Sync single-key interfaces ---
    tl::expected<void, ErrorCode> ReadRemoteData(
        const RemoteReadRequest& request);
    tl::expected<UUID, ErrorCode> WriteRemoteData(
        const RemoteWriteRequest& request);
    tl::expected<PreWriteResponse, ErrorCode> PreWrite(
        const PreWriteRequest& request);
    tl::expected<void, ErrorCode> WriteCommit(
        const WriteCommitRequest& request);
    tl::expected<PinKeyResponse, ErrorCode> PinKey(
        const PinKeyRequest& request);
    tl::expected<void, ErrorCode> UnPinKey(const UnPinKeyRequest& request);

   private:
    std::shared_ptr<coro_io::client_pools<coro_rpc::coro_rpc_client>>
        client_pools_;
    std::shared_ptr<coro_io::client_pool<coro_rpc::coro_rpc_client>>
        client_pool_;
    std::string endpoint_;
};

}  // namespace mooncake
