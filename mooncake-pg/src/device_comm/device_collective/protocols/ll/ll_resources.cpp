#include "device_comm/device_collective/protocols/ll/ll_resources.h"

#include <utility>

#include <glog/logging.h>

#include "device_comm/device_collective/device_control_update.h"
#include "device_comm/device_transfer/transfer_service.h"
#include "gpu_runtime.h"
#include "pg_utils.h"

namespace mooncake {

LLResources::LLResources(RegionSlice signals, LLSignalLayout signal_layout,
                         int device_index) noexcept
    : signals_(std::move(signals)),
      signal_layout_(signal_layout),
      endpoint_{.signal_offset = signals_.offset()},
      device_index_(device_index) {}

PGResult<std::unique_ptr<LLResources>> LLResources::create(
    DeviceTransferService& transfer_service, InGroupRank self_rank,
    uint32_t max_group_size) {
    PG_VALIDATE_ARG(max_group_size > 0 && max_group_size <= kMaxNumRanks,
                    "LL group capacity is outside the supported range");
    PG_VALIDATE_ARG(self_rank >= 0 &&
                        static_cast<uint32_t>(self_rank) < max_group_size,
                    "LL self rank is outside the group");
    const LLSignalLayout signal_layout{max_group_size};
    PG_TRY(auto signals, transfer_service.allocatePeerAccessible(
                             signal_layout.signalBytes(), alignof(uint64_t)));
    auto resources = std::unique_ptr<LLResources>(new LLResources(
        std::move(signals), signal_layout, transfer_service.deviceIndex()));
    PG_TRY(auto device_guard, GpuDeviceGuard::create(resources->device_index_));
    PG_TRY_CUDA(cudaMalloc(reinterpret_cast<void**>(&resources->state_),
                           sizeof(LLState)));
    resources->host_state_ = LLState{
        .transfer_handle = transfer_service.deviceHandle(),
        .self_rank = self_rank,
        .signals = static_cast<uint64_t*>(resources->signals_.addr()),
        .signal_layout = signal_layout,
    };
    PG_TRY_CUDA(cudaMemcpy(resources->state_, &resources->host_state_,
                           sizeof(LLState), cudaMemcpyHostToDevice));
    PG_TRY_CUDA(cudaMemset(resources->signals_.addr(), 0,
                           signal_layout.signalBytes()));
    return resources;
}

LLResources::~LLResources() noexcept {
    if (!state_) return;
    auto device_guard = GpuDeviceGuard::create(device_index_);
    if (!device_guard.has_value()) {
        LOG(ERROR) << "Failed to select CUDA device while releasing LL "
                      "progress: "
                   << device_guard.error().message;
        return;
    }
    const auto result = cudaFree(state_);
    if (result != cudaSuccess) {
        LOG(ERROR) << "Failed to free LL progress: "
                   << cudaGetErrorString(result);
    }
}

PGResult<void> LLResources::applyGroupView(
    const DeviceCollectiveRuntime::ResolvedGroupView& view) {
    auto next = host_state_;
    for (auto& peer : next.peers) peer = {};
    next.buffer_ready_sequence = 0;
    if (view.self_active_index >= 0) {
        for (const auto& peer : view.participants) {
            PG_VALIDATE_STATE(peer.endpoint.ll, "Active peer has no LL endpoint");
            const auto& endpoint = *peer.endpoint.ll;
            const auto& workspace = peer.workspace;
            const uint64_t signal_bytes = signal_layout_.signalBytes();
            PG_VALIDATE_STATE(
                endpoint.signal_offset % alignof(uint64_t) == 0 &&
                    !addOverflows(endpoint.signal_offset, signal_bytes),
                "LL peer signal endpoint is invalid");
            PG_VALIDATE_STATE(workspace.buffer_offset % alignof(uint64_t) == 0,
                              "LL peer payload workspace is misaligned");
            PG_VALIDATE_STATE(
                !addOverflows(workspace.buffer_offset, workspace.buffer_size) &&
                    (workspace.buffer_offset + workspace.buffer_size <=
                         endpoint.signal_offset ||
                     endpoint.signal_offset + signal_bytes <=
                         workspace.buffer_offset),
                "LL peer signals overlap the payload workspace");
            next.peers[peer.in_group_rank] = LLPeer{
                .global_rank = peer.global_rank,
                .signal_offset = endpoint.signal_offset,
                .view_epoch_signal_offset = peer.view_epoch_signal_offset,
            };
        }
    }
    host_state_ = next;
    return {};
}

PGResult<void> LLResources::appendUpdate(ControlUpdateBuilder& builder) const {
    PG_TRY(builder.fillU64(static_cast<uint64_t*>(signals_.addr()), 0,
                           signals_.size() / sizeof(uint64_t)));
    return builder.copyBytes(state_, &host_state_, sizeof(host_state_));
}

}  // namespace mooncake
