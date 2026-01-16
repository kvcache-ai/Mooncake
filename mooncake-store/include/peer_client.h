#pragma once

#include "client_rpc_types.h"
#include "types.h"
#include <vector>
#include <ylt/coro_rpc/coro_rpc_client.hpp>
#include <ylt/coro_io/client_pool.hpp>
#include <ylt/util/tl/expected.hpp>
#include <string>

namespace mooncake {

class PeerClient {
   public:
    tl::expected<void, ErrorCode> Connect(const std::string& endpoint);
    tl::expected<void, ErrorCode> ReadRemoteData(
        const RemoteReadRequest& request);
    tl::expected<void, ErrorCode> WriteRemoteData(
        const RemoteWriteRequest& request);
    std::vector<tl::expected<void, ErrorCode>> BatchReadRemoteData(
        const BatchRemoteReadRequest& request);
    std::vector<tl::expected<void, ErrorCode>> BatchWriteRemoteData(
        const BatchRemoteWriteRequest& request);

   private:
    std::shared_ptr<coro_io::client_pool<coro_rpc::coro_rpc_client>>
        client_pool_;
    std::string endpoint_;
};

}  // namespace mooncake
