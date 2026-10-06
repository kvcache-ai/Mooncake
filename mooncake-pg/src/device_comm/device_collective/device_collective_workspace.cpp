#include "device_comm/device_collective/device_collective_workspace.h"

#include <utility>

#include "device_comm/device_transfer/transfer_service.h"
#include "pg_utils.h"

namespace mooncake {

DeviceCollectiveWorkspace::DeviceCollectiveWorkspace(
    DeviceTransferService& transfer_service, RegionSlice buffer,
    GlobalRank self_rank, uint32_t max_world_size,
    DeviceCollectiveWorkspaceEndpoint local_endpoint)
    : transfer_service_(transfer_service),
      buffer_(std::move(buffer)),
      self_rank_(self_rank),
      local_endpoint_(local_endpoint),
      endpoints_(max_world_size) {
    endpoints_[self_rank_] = local_endpoint_;
}

PGResult<std::unique_ptr<DeviceCollectiveWorkspace>>
DeviceCollectiveWorkspace::create(DeviceTransferService& transfer_service,
                                  GlobalRank self_rank, uint32_t max_world_size,
                                  size_t buffer_size) {
    PG_VALIDATE_ARG(max_world_size != 0,
                    "device collective world size is zero");
    PG_VALIDATE_ARG(
        self_rank >= 0 && static_cast<uint32_t>(self_rank) < max_world_size,
        "device collective self rank is outside the world");
    PG_TRY(auto buffer,
           transfer_service.allocatePeerAccessible(buffer_size, 1));
    const DeviceCollectiveWorkspaceEndpoint local_endpoint{
        .buffer_offset = buffer.offset(),
        .buffer_size = buffer.size(),
    };
    return std::unique_ptr<DeviceCollectiveWorkspace>(
        new DeviceCollectiveWorkspace(transfer_service, std::move(buffer),
                                      self_rank, max_world_size,
                                      local_endpoint));
}

const RegionSlice& DeviceCollectiveWorkspace::buffer() const noexcept {
    return buffer_;
}

PGResult<const RegionSlice*> DeviceCollectiveWorkspace::staging() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!staging_) {
        PG_TRY(auto staging,
               transfer_service_.allocateLocalStaging(buffer_.size(), 1));
        staging_.emplace(std::move(staging));
    }
    return &*staging_;
}

const DeviceCollectiveWorkspaceEndpoint&
DeviceCollectiveWorkspace::localEndpoint() const noexcept {
    return local_endpoint_;
}

PGResult<void> DeviceCollectiveWorkspace::installPeerEndpoint(
    GlobalRank rank, const DeviceCollectiveWorkspaceEndpoint& endpoint) {
    std::lock_guard<std::mutex> lock(mutex_);
    PG_VALIDATE_ARG(rank >= 0 && static_cast<size_t>(rank) < endpoints_.size(),
                    "device collective peer rank is out of range");
    PG_VALIDATE_ARG(rank != self_rank_,
                    "cannot replace the local device collective workspace "
                    "endpoint");
    PG_VALIDATE_ARG(
        endpoint.buffer_size != 0 &&
            !addOverflows(endpoint.buffer_offset, endpoint.buffer_size),
        "device collective peer endpoint is invalid");
    endpoints_[rank] = endpoint;
    return {};
}

PGResult<DeviceCollectiveWorkspaceEndpoint> DeviceCollectiveWorkspace::endpoint(
    GlobalRank rank) const {
    std::lock_guard<std::mutex> lock(mutex_);
    PG_VALIDATE_ARG(rank >= 0 && static_cast<size_t>(rank) < endpoints_.size(),
                    "device collective peer rank is out of range");
    PG_VALIDATE_STATE(endpoints_[rank].has_value(),
                      "device collective peer endpoint is unavailable");
    return *endpoints_[rank];
}

}  // namespace mooncake
