#include "device_comm/device_collective/protocols/simple/simple_resources.h"

#include <utility>

#include <glog/logging.h>

#include "device_comm/device_collective/device_control_update.h"
#include "device_comm/device_collective/device_collective_workspace.h"
#include "device_comm/device_transfer/transfer_service.h"
#include "gpu_runtime.h"
#include "pg_utils.h"

namespace mooncake {

SimpleResources::SimpleResources(RegionSlice signals,
                                 SimplePipelineSignalLayout signal_layout,
                                 DeviceTransferService& transfer_service,
                                 InGroupRank self_rank) noexcept
    : signals_(std::move(signals)),
      signal_layout_(signal_layout),
      endpoint_{.signal_offset = signals_.offset(),
                .signal_count = signal_layout_.total_signal_count},
      transfer_service_(transfer_service),
      self_rank_(self_rank),
      device_index_(transfer_service.deviceIndex()) {}

PGResult<std::unique_ptr<SimpleResources>> SimpleResources::create(
    DeviceTransferService& transfer_service, InGroupRank self_rank,
    uint32_t max_group_size) {
    PG_VALIDATE_ARG(max_group_size > 0 && max_group_size <= kMaxNumRanks,
                    "Simple group capacity is outside the supported range");
    PG_VALIDATE_ARG(
        self_rank >= 0 && static_cast<uint32_t>(self_rank) < max_group_size,
        "Simple self rank is outside the group");
    const auto signal_layout = SimplePipelineSignalLayout::make(max_group_size);
    const uint64_t signal_bytes =
        uint64_t{signal_layout.total_signal_count} * sizeof(uint64_t);
    PG_TRY(auto signals, transfer_service.allocatePeerAccessible(
                             signal_bytes, alignof(uint64_t)));
    auto resources = std::unique_ptr<SimpleResources>(new SimpleResources(
        std::move(signals), signal_layout, transfer_service, self_rank));
    PG_TRY(auto device_guard, GpuDeviceGuard::create(resources->device_index_));
    const size_t peer_progress_bytes = size_t{kMaxDeviceCollectiveChannels} *
                                       max_group_size *
                                       sizeof(SimplePeerProgress);
    PG_TRY_CUDA(cudaMalloc(reinterpret_cast<void**>(&resources->peer_progress_),
                           peer_progress_bytes));
    PG_TRY_CUDA(cudaMemset(resources->peer_progress_, 0, peer_progress_bytes));
    PG_TRY_CUDA(cudaMemset(resources->signals_.addr(), 0, signal_bytes));
    PG_TRY_CUDA(cudaMalloc(reinterpret_cast<void**>(&resources->state_),
                           sizeof(SimpleBindings)));
    const SimpleBindings initial_bindings{
        .transfer_handle = transfer_service.deviceHandle(),
        .self_rank = self_rank,
        .signals = static_cast<uint64_t*>(resources->signals_.addr()),
        .signal_layout = signal_layout,
        .peer_progress = resources->peer_progress_,
    };
    PG_TRY_CUDA(cudaMemcpy(resources->state_, &initial_bindings,
                           sizeof(SimpleBindings), cudaMemcpyHostToDevice));
    return resources;
}

SimpleResources::~SimpleResources() noexcept {
    if (!peer_progress_ && !state_) return;
    auto device_guard = GpuDeviceGuard::create(device_index_);
    if (!device_guard.has_value()) {
        LOG(ERROR) << "Failed to select CUDA device while releasing Simple "
                      "state: "
                   << device_guard.error().message;
        return;
    }
    for (void* allocation :
         {static_cast<void*>(state_), static_cast<void*>(peer_progress_)}) {
        if (!allocation) continue;
        const auto result = cudaFree(allocation);
        if (result != cudaSuccess) {
            LOG(ERROR) << "Failed to free Simple state: "
                       << cudaGetErrorString(result);
        }
    }
}

PGResult<SimpleBindings> SimpleResources::bindGroupView(
    const ResolvedGroupView& view) const {
    SimpleBindings next{
        .transfer_handle = transfer_service_.deviceHandle(),
        .self_rank = self_rank_,
        .signals = static_cast<uint64_t*>(signals_.addr()),
        .signal_layout = signal_layout_,
        .peer_progress = peer_progress_,
    };
    if (view.self_active_index >= 0) {
        for (const auto& peer : view.participants) {
            if (peer.in_group_rank == next.self_rank) continue;
            PG_VALIDATE_STATE(peer.in_group_rank >= 0 &&
                                  static_cast<uint32_t>(peer.in_group_rank) <
                                      signal_layout_.max_group_size,
                              "Simple peer rank is outside the group");
            PG_VALIDATE_STATE(peer.endpoint.simple,
                              "Active peer has no Simple endpoint");
            const auto& endpoint = *peer.endpoint.simple;
            const auto& workspace = peer.workspace;
            const uint64_t signal_bytes =
                uint64_t{endpoint.signal_count} * sizeof(uint64_t);
            PG_VALIDATE_STATE(
                endpoint.signal_count >= signal_layout_.total_signal_count &&
                    endpoint.signal_offset % alignof(uint64_t) == 0 &&
                    !addOverflows(endpoint.signal_offset, signal_bytes),
                "Simple peer signal endpoint is invalid");
            PG_VALIDATE_STATE(
                !addOverflows(workspace.buffer_offset, workspace.buffer_size) &&
                    (workspace.buffer_offset + workspace.buffer_size <=
                         endpoint.signal_offset ||
                     endpoint.signal_offset + signal_bytes <=
                         workspace.buffer_offset),
                "Simple peer signals overlap the payload workspace");
            next.peer_bindings[peer.in_group_rank] = SimplePeer{
                .global_rank = peer.global_rank,
                .signal_offset = endpoint.signal_offset,
            };
        }
    }
    return next;
}

PGResult<SimpleWorkspace> SimpleResources::bindWorkspace(
    DeviceCollectiveWorkspace& workspace, const CollectivePeer& send_peer,
    uint64_t bytes) const {
    PG_VALIDATE_STATE(bytes != 0 && bytes <= workspace.buffer().size(),
                      "Simple workspace capacity is invalid");
    SimpleWorkspace result{static_cast<char*>(workspace.buffer().addr()), bytes,
                           nullptr};
    PG_TRY(auto direct,
           transfer_service_.isDirectlyAddressable(send_peer.global_rank));
    if (!direct) {
        PG_TRY(auto staging, workspace.staging());
        result.staging = static_cast<char*>(staging->addr());
    }
    return result;
}

PGResult<void> SimpleResources::appendUpdate(
    ControlUpdateBuilder& builder, const SimpleBindings& bindings) const {
    PG_TRY(builder.copyBytes(state_, &bindings, sizeof(bindings)));
    PG_TRY(builder.fillU64(static_cast<uint64_t*>(signals_.addr()), 0,
                           signal_layout_.total_signal_count));
    return builder.fillBytes(peer_progress_, 0,
                             size_t{kMaxDeviceCollectiveChannels} *
                                 signal_layout_.max_group_size *
                                 sizeof(SimplePeerProgress));
}

}  // namespace mooncake
