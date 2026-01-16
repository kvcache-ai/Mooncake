#include "peer_client.h"

#include <glog/logging.h>
#include "types.h"

namespace mooncake {

tl::expected<void, ErrorCode> PeerClient::Connect(const std::string& endpoint) {
    endpoint_ = endpoint;
    // TODO: Implement actual connection logic
    return tl::make_unexpected(ErrorCode::NOT_IMPLEMENTED);
}

tl::expected<void, ErrorCode> PeerClient::ReadRemoteData(
    const RemoteReadRequest& request) {
    // TODO: Implement actual RPC call to remote client
    return tl::make_unexpected(ErrorCode::NOT_IMPLEMENTED);
}

tl::expected<void, ErrorCode> PeerClient::WriteRemoteData(
    const RemoteWriteRequest& request) {
    // TODO: Implement actual RPC call to remote client
    return tl::make_unexpected(ErrorCode::NOT_IMPLEMENTED);
}

std::vector<tl::expected<void, ErrorCode>> PeerClient::BatchReadRemoteData(
    const BatchRemoteReadRequest& request) {
    // TODO: Implement actual batch RPC calls to remote client
    std::vector<tl::expected<void, ErrorCode>> results;
    results.reserve(request.keys.size());
    for (size_t i = 0; i < request.keys.size(); ++i) {
        results.push_back(tl::make_unexpected(ErrorCode::NOT_IMPLEMENTED));
    }
    return results;
}

std::vector<tl::expected<void, ErrorCode>> PeerClient::BatchWriteRemoteData(
    const BatchRemoteWriteRequest& request) {
    // TODO: Implement actual batch RPC calls to remote client
    std::vector<tl::expected<void, ErrorCode>> results;
    results.reserve(request.keys.size());
    for (size_t i = 0; i < request.keys.size(); ++i) {
        results.push_back(tl::make_unexpected(ErrorCode::NOT_IMPLEMENTED));
    }
    return results;
}

}  // namespace mooncake
