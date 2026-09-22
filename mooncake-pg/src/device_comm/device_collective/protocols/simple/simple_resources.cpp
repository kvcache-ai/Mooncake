#include "device_comm/device_collective/protocols/simple/simple_resources.h"

#include <utility>

#include <glog/logging.h>

#include "device_comm/device_collective/device_control_update.h"
#include "device_comm/device_primitives/payload_writer.h"
#include "device_comm/device_transfer/transfer_service.h"
#include "gpu_runtime.h"
#include "pg_utils.h"

namespace mooncake {

SimpleResources::SimpleResources(RegionSlice signals,
                                 SimplePipelineSignalLayout<> signal_layout,
                                 DeviceTransferService& transfer_service) noexcept
    : signals_(std::move(signals)),
      signal_layout_(signal_layout),
      endpoint_{.signal_offset = signals_.offset(),
                .signal_count = signal_layout_.total_signal_count},
      transfer_service_(transfer_service),
      device_index_(transfer_service.deviceIndex()) {}

PGResult<std::unique_ptr<SimpleResources>> SimpleResources::create(
    DeviceTransferService& transfer_service, InGroupRank self_rank,
    uint32_t max_group_size) {
    PG_VALIDATE_ARG(max_group_size > 0 && max_group_size <= kMaxNumRanks,
                    "Simple group capacity is outside the supported range");
    PG_VALIDATE_ARG(self_rank >= 0 &&
                        static_cast<uint32_t>(self_rank) < max_group_size,
                    "Simple self rank is outside the group");
    const auto signal_layout = SimplePipelineSignalLayout<>::make(max_group_size);
    const uint64_t signal_bytes =
        uint64_t{signal_layout.total_signal_count} * sizeof(uint64_t);
    PG_TRY(auto signals, transfer_service.allocatePeerAccessible(
                             signal_bytes, alignof(uint64_t)));
    auto resources = std::unique_ptr<SimpleResources>(new SimpleResources(
        std::move(signals), signal_layout, transfer_service));
    PG_TRY(auto device_guard, GpuDeviceGuard::create(resources->device_index_));
    PG_TRY_CUDA(cudaMalloc(reinterpret_cast<void**>(&resources->connections_),
                           resources->connectionBytes()));
    PG_TRY_CUDA(cudaMemset(resources->connections_, 0,
                           resources->connectionBytes()));
    PG_TRY_CUDA(cudaMemset(resources->signals_.addr(), 0, signal_bytes));
    PG_TRY_CUDA(cudaMalloc(reinterpret_cast<void**>(&resources->state_),
                           sizeof(SimpleState<>)));
    resources->host_state_ = SimpleState<>{
        .transfer_handle = transfer_service.deviceHandle(),
        .self_rank = self_rank,
        .signals = static_cast<uint64_t*>(resources->signals_.addr()),
        .signal_layout = signal_layout,
        .connections = resources->connections_,
    };
    PG_TRY_CUDA(cudaMemcpy(resources->state_, &resources->host_state_,
                           sizeof(SimpleState<>), cudaMemcpyHostToDevice));
    return resources;
}

SimpleResources::~SimpleResources() noexcept {
    if (!connections_ && !state_) return;
    auto device_guard = GpuDeviceGuard::create(device_index_);
    if (!device_guard.has_value()) {
        LOG(ERROR) << "Failed to select CUDA device while releasing Simple "
                      "state: "
                   << device_guard.error().message;
        return;
    }
    for (void* allocation : {static_cast<void*>(state_),
                             static_cast<void*>(connections_)}) {
        if (!allocation) continue;
        const auto result = cudaFree(allocation);
        if (result != cudaSuccess) {
            LOG(ERROR) << "Failed to free Simple state: "
                       << cudaGetErrorString(result);
        }
    }
}

PGResult<void> SimpleResources::applyGroupView(
    const DeviceCollectiveRuntime::ResolvedGroupView& view) {
    auto next = host_state_;
    for (auto& peer : next.peers) peer = {};
    if (view.self_active_index >= 0) {
        for (const auto& peer : view.participants) {
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
            next.peers[peer.in_group_rank] = SimplePeer{
                .global_rank = peer.global_rank,
                .signal_offset = endpoint.signal_offset,
                .view_epoch_signal_offset = peer.view_epoch_signal_offset,
            };
        }
    }
    host_state_ = next;
    return {};
}

PGResult<bool> SimpleResources::requiresStaging(InGroupRank peer) const {
    PG_VALIDATE_ARG(
        peer >= 0 && static_cast<uint32_t>(peer) < signal_layout_.max_group_size,
        "Simple peer rank is outside the group");
    const auto global_rank = host_state_.peers[peer].global_rank;
    PG_VALIDATE_STATE(global_rank != kInvalidGlobalRank,
                      "Simple peer is inactive");
    return payloadWriterRequiresStaging(transfer_service_, global_rank);
}

PGResult<void> SimpleResources::appendUpdate(ControlUpdateBuilder& builder) const {
    PG_TRY(builder.copyBytes(state_, &host_state_, sizeof(host_state_)));
    PG_TRY(builder.fillU64(static_cast<uint64_t*>(signals_.addr()), 0,
                           signal_layout_.total_signal_count));
    return builder.fillBytes(connections_, 0, connectionBytes());
}

}  // namespace mooncake
