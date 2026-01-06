#pragma once

#include "client_rpc_types.h"
#include "types.h"
#include <vector>

namespace mooncake {

class PeerClient {
public:
    ErrorCode Connect(const std::string& endpoint);
    std::expected<void, ErrorCode> ReadRemoteData(const RemoteReadRequest& request);
    std::expected<void, ErrorCode> WriteRemoteData(const RemoteWriteRequest& request);
    std::vector<tl::expected<void, ErrorCode>> BatchReadRemoteData(const BatchRemoteReadRequest& request);
    std::vector<tl::expected<void, ErrorCode>> BatchWriteRemoteData(const BatchRemoteWriteRequest& request);
private:
    std::shared_ptr<coro_io::client_pool<coro_rpc::coro_rpc_client>> client_pool_;
    std::string endpoint_;
};

}  // namespace mooncake