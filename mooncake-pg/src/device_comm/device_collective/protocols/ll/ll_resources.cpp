#include "device_comm/device_collective/protocols/ll/ll_resources.h"

#include <utility>

#include <glog/logging.h>

#include "device_comm/device_collective/device_control_update.h"
#include "device_comm/device_transfer/transfer_service.h"
#include "gpu_runtime.h"
#include "pg_utils.h"

namespace mooncake {

LLResources::LLResources(RegionSlice signals, InGroupRank self_rank,
                         uint32_t max_group_size,
                         const DeviceTransferHandle* transfer_handle,
                         int device_index) noexcept
    : signals_(std::move(signals)),
      max_group_size_(max_group_size),
      endpoint_{.signal_offset = signals_.offset(),
                .signal_count = kMaxDeviceCollectiveChannels * kLLBarrierCount *
                                max_group_size,
                .native_atomic_peer_uuids = {}},
      transfer_handle_(transfer_handle),
      self_rank_(self_rank),
      device_index_(device_index) {}

PGResult<std::unique_ptr<LLResources>> LLResources::create(
    DeviceTransferService& transfer_service, InGroupRank self_rank,
    uint32_t max_group_size) {
    PG_VALIDATE_ARG(max_group_size > 0 && max_group_size <= kMaxNumRanks,
                    "LL group capacity is outside the supported range");
    PG_VALIDATE_ARG(
        self_rank >= 0 && static_cast<uint32_t>(self_rank) < max_group_size,
        "LL self rank is outside the group");
    const uint64_t signal_bytes = uint64_t{kMaxDeviceCollectiveChannels} *
                                  kLLBarrierCount * max_group_size *
                                  sizeof(uint64_t);
    PG_TRY(auto signals, transfer_service.allocatePeerAccessible(
                             signal_bytes, alignof(uint64_t)));
    auto resources = std::unique_ptr<LLResources>(new LLResources(
        std::move(signals), self_rank, max_group_size,
        transfer_service.deviceHandle(), transfer_service.deviceIndex()));
    if (const auto* p2p = transfer_service.p2pRoute()) {
        resources->endpoint_.device_uuid = p2p->deviceUuid();
        resources->endpoint_.native_atomic_peer_uuids =
            p2p->nativeAtomicPeerUuids();
    }
    PG_TRY(auto device_guard, GpuDeviceGuard::create(resources->device_index_));
    PG_TRY_CUDA(cudaMalloc(reinterpret_cast<void**>(&resources->state_),
                           sizeof(LLState)));
    const LLState initial_state{
        .bindings =
            {
                .transfer_handle = transfer_service.deviceHandle(),
                .self_rank = self_rank,
                .max_group_size = max_group_size,
                .signals = static_cast<uint64_t*>(resources->signals_.addr()),
            },
    };
    PG_TRY_CUDA(cudaMemcpy(resources->state_, &initial_state, sizeof(LLState),
                           cudaMemcpyHostToDevice));
    PG_TRY_CUDA(cudaMemset(resources->signals_.addr(), 0, signal_bytes));
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

PGResult<LLBindings> LLResources::bindGroupView(
    const ResolvedGroupView& view) const {
    LLBindings next{
        .transfer_handle = transfer_handle_,
        .self_rank = self_rank_,
        .max_group_size = max_group_size_,
        .signals = static_cast<uint64_t*>(signals_.addr()),
    };
    if (view.self_active_index >= 0) {
        for (const auto& peer : view.participants) {
            if (peer.in_group_rank == next.self_rank) continue;
            PG_VALIDATE_STATE(
                peer.in_group_rank >= 0 &&
                    static_cast<uint32_t>(peer.in_group_rank) < max_group_size_,
                "LL peer rank is outside the group");
            PG_VALIDATE_STATE(peer.endpoint.ll,
                              "Active peer has no LL endpoint");
            const auto& endpoint = *peer.endpoint.ll;
            const auto& workspace = peer.workspace;
            const uint64_t signal_bytes =
                uint64_t{endpoint.signal_count} * sizeof(uint64_t);
            PG_VALIDATE_STATE(
                endpoint.signal_offset % alignof(uint64_t) == 0 &&
                    endpoint.signal_count == endpoint_.signal_count &&
                    !addOverflows(endpoint.signal_offset, signal_bytes),
                "LL peer signal endpoint is invalid");
            PG_VALIDATE_STATE(
                !addOverflows(workspace.buffer_offset, workspace.buffer_size) &&
                    (workspace.buffer_offset + workspace.buffer_size <=
                         endpoint.signal_offset ||
                     endpoint.signal_offset + signal_bytes <=
                         workspace.buffer_offset),
                "LL peer signals overlap the shared payload workspace");
            next.peer_bindings[peer.in_group_rank] = LLPeer{
                .global_rank = peer.global_rank,
                .signal_offset = endpoint.signal_offset,
            };
        }
    }
    return next;
}

PGResult<void> LLResources::appendUpdate(ControlUpdateBuilder& builder,
                                         const LLBindings& bindings) const {
    // Publication preserves GPU progress; LLControl owns its reset.
    return builder.copyBytes(&state_->bindings, &bindings, sizeof(bindings));
}

}  // namespace mooncake
