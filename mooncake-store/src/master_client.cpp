#include "master_client.h"

#include <async_simple/coro/SyncAwait.h>

#include <string>
#include <vector>

#include "rpc_service.h"

namespace mooncake {

MasterClient::MasterClient() = default;
MasterClient::~MasterClient() = default;

ErrorCode MasterClient::Connect(const std::string& master_addr) {
    auto result = coro::syncAwait(client_.connect(master_addr));
    if (result.val() != 0) {
        LOG(ERROR) << "Failed to connect to master: " << result.message();
        return ErrorCode::INTERNAL_ERROR;
    }
    return ErrorCode::OK;
}

GetReplicaListResponse MasterClient::GetReplicaList(
    const std::string& object_key) {
    auto result = coro::syncAwait(
        client_.call<&WrappedMasterService::GetReplicaList>(object_key));
    return result.value();
}

PutStartResponse MasterClient::PutStart(
    const std::string& key, const std::vector<size_t>& slice_lengths,
    size_t value_length, const ReplicateConfig& config) {
    // Convert size_t to uint64_t for RPC
    std::vector<uint64_t> rpc_slice_lengths;
    rpc_slice_lengths.reserve(slice_lengths.size());
    for (const auto& length : slice_lengths) {
        rpc_slice_lengths.push_back(static_cast<uint64_t>(length));
    }

    auto result = coro::syncAwait(client_.call<&WrappedMasterService::PutStart>(
        key, static_cast<uint64_t>(value_length), rpc_slice_lengths, config));
    return result.value();
}

PutEndResponse MasterClient::PutEnd(const std::string& key) {
    auto result =
        coro::syncAwait(client_.call<&WrappedMasterService::PutEnd>(key));
    return result.value();
}

PutRevokeResponse MasterClient::PutRevoke(const std::string& key) {
    auto result =
        coro::syncAwait(client_.call<&WrappedMasterService::PutRevoke>(key));
    return result.value();
}

RemoveResponse MasterClient::Remove(const std::string& key) {
    auto result =
        coro::syncAwait(client_.call<&WrappedMasterService::Remove>(key));
    return result.value();
}

MountSegmentResponse MasterClient::MountSegment(const std::string& segment_name,
                                                const void* buffer,
                                                size_t size) {
    auto result =
        coro::syncAwait(client_.call<&WrappedMasterService::MountSegment>(
            reinterpret_cast<uint64_t>(buffer), static_cast<uint64_t>(size),
            segment_name));
    return result.value();
}

UnmountSegmentResponse MasterClient::UnmountSegment(
    const std::string& segment_name) {
    auto result = coro::syncAwait(
        client_.call<&WrappedMasterService::UnmountSegment>(segment_name));
    return result.value();
}

}  // namespace mooncake
