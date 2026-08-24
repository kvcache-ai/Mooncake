#include "device_comm/device_collective/protocols/ring/ring_all_reduce.h"

#include <algorithm>
#include <array>
#include <exception>
#include <new>
#include <utility>
#include <vector>

#include <glog/logging.h>

#include "device_comm/device_collective/device_collective_workspace.h"
#include "device_comm/device_primitives/payload_writer.h"
#include "device_comm/device_transfer/transfer_service.h"
#include "pg_utils.h"

namespace mooncake {
namespace {

cudaError_t setPlanStatusAsync(DevicePlanStatus* device_status,
                               DevicePlanStatus status, cudaStream_t stream) {
    static_assert(sizeof(DevicePlanStatus) == 1);
    return cudaMemsetAsync(device_status, static_cast<int>(status),
                           sizeof(status), stream);
}

class StreamSyncGuard {
   public:
    explicit StreamSyncGuard(GpuStream& stream) : stream_(stream) {}

    ~StreamSyncGuard() noexcept {
        if (!active_) return;
        try {
            auto result = stream_.synchronize();
            if (!result.has_value()) {
                LOG(ERROR) << "Failed to drain Ring control stream: "
                           << result.error().message;
            }
        } catch (const std::exception& error) {
            LOG(ERROR) << "Failed to drain Ring control stream: "
                       << error.what();
        } catch (...) {
            LOG(ERROR) << "Failed to drain Ring control stream";
        }
    }

    PGResult<void> finish() {
        auto result = stream_.synchronize();
        active_ = false;
        return result;
    }

   private:
    GpuStream& stream_;
    bool active_ = true;
};

bool rangesOverlap(uint64_t first_offset, uint64_t first_size,
                   uint64_t second_offset, uint64_t second_size) noexcept {
    if (addOverflows(first_offset, first_size) ||
        addOverflows(second_offset, second_size)) {
        return true;
    }
    return first_offset < second_offset + second_size &&
           second_offset < first_offset + first_size;
}

bool endpointSupports(const DeviceCollectiveEndpoint& collective,
                      const RingAllReduceEndpoint& protocol,
                      uint32_t required_signal_count) noexcept {
    if (collective.buffer_size < kRingBufferBytes ||
        protocol.signal_count < required_signal_count) {
        return false;
    }
    const uint64_t signal_bytes =
        static_cast<uint64_t>(protocol.signal_count) * sizeof(uint64_t);
    return !rangesOverlap(collective.buffer_offset, collective.buffer_size,
                          protocol.signal_offset, signal_bytes);
}

}  // namespace

struct RingAllReduceProtocol::HostState {
    RingAllReducePlan plan;
    const std::array<uint64_t, kMaxDeviceCollectiveChannels *
                                   kRingPipelineSlots>
        initial_step_sequences = [] {
            std::array<uint64_t, kMaxDeviceCollectiveChannels *
                                     kRingPipelineSlots>
                sequences{};
            sequences.fill(1);
            return sequences;
        }();
    const std::array<uint64_t, kMaxDeviceCollectiveChannels>
        initial_recv_buffer_ready_sequences = [] {
            std::array<uint64_t, kMaxDeviceCollectiveChannels> sequences{};
            sequences.fill(1);
            return sequences;
        }();
};

RingAllReduceProtocol::RingAllReduceProtocol(
    DeviceTransferService& transfer_service,
    DeviceCollectiveWorkspace& workspace,
    const DeviceTransferHandle* transfer_handle,
    DeviceCollectiveInvocationState* invocation_state,
    DeviceCollectiveRecoveryMailbox* recovery_mailbox,
    uint64_t timeout_ticks, GpuStream& control_stream, int device_index,
    InGroupRank self_rank, uint32_t max_group_size, DeviceArenaSlice signals,
    RingSignalLayout signal_layout) noexcept
    : transfer_service_(transfer_service),
      workspace_(workspace),
      transfer_handle_(transfer_handle),
      invocation_state_(invocation_state),
      recovery_mailbox_(recovery_mailbox),
      timeout_ticks_(timeout_ticks),
      control_stream_(control_stream),
      device_index_(device_index),
      self_rank_(self_rank),
      max_group_size_(max_group_size),
      signals_(std::move(signals)),
      signal_layout_(signal_layout),
      endpoint_{.signal_offset = signals_.offset(),
                .signal_count = signal_layout_.total_signal_count} {}

PGResult<std::unique_ptr<RingAllReduceProtocol>>
RingAllReduceProtocol::create(
    DeviceTransferService& transfer_service, DeviceArena& arena,
    DeviceCollectiveWorkspace& workspace,
    DeviceCollectiveInvocationState* invocation_state,
    DeviceCollectiveRecoveryMailbox* recovery_mailbox,
    uint64_t timeout_ticks,
    GpuStream& control_stream, int device_index, InGroupRank self_rank,
    uint32_t max_group_size) {
    PG_VALIDATE_ARG(max_group_size != 0,
                    "Ring group capacity is zero");
    PG_VALIDATE_ARG(
        self_rank >= 0 && static_cast<uint32_t>(self_rank) < max_group_size,
        "Ring self rank is outside the group");

    const auto signal_layout = RingSignalLayout::make(max_group_size);
    const auto* const transfer_handle = transfer_service.deviceHandle();
    PG_VALIDATE_ARG(transfer_handle, "Ring transfer handle is null");
    PG_VALIDATE_ARG(invocation_state, "Ring invocation state is null");
    PG_VALIDATE_ARG(recovery_mailbox, "Ring recovery mailbox is null");
    PG_VALIDATE_ARG(workspace.buffer().addr(),
                    "Ring buffer address is null");
    PG_VALIDATE_ARG(workspace.buffer().size() >= kRingBufferBytes,
                    "Ring buffer capacity is too small");

    const uint64_t signal_bytes =
        static_cast<uint64_t>(signal_layout.total_signal_count) *
        sizeof(uint64_t);
    PG_TRY(auto signals,
           arena.allocate(signal_bytes, alignof(uint64_t)));

    auto protocol = std::unique_ptr<RingAllReduceProtocol>(
        new RingAllReduceProtocol(
            transfer_service, workspace, transfer_handle, invocation_state,
            recovery_mailbox, timeout_ticks, control_stream, device_index,
            self_rank, max_group_size, std::move(signals), signal_layout));
    PG_TRY(protocol->initializeDeviceState());
    PG_TRY(protocol->initializeHostState());
    return protocol;
}

RingAllReduceProtocol::~RingAllReduceProtocol() noexcept {
    releaseHostState();
    releaseDeviceState();
}

PGResult<void> RingAllReduceProtocol::initializeDeviceState() {
    PG_TRY(auto device_guard, GpuDeviceGuard::create(device_index_));
    PG_TRY_CUDA(cudaMalloc(reinterpret_cast<void**>(&state_),
                           sizeof(RingAllReduceDeviceState)));
    PG_TRY_CUDA(cudaMemset(state_, 0, sizeof(RingAllReduceDeviceState)));
    PG_TRY_CUDA(cudaMemset(signals_.addr(), 0, signals_.size()));
    return {};
}

void RingAllReduceProtocol::releaseDeviceState() noexcept {
    if (!state_) return;
    auto device_guard = GpuDeviceGuard::create(device_index_);
    if (!device_guard.has_value()) {
        LOG(ERROR) << "Failed to select CUDA device while releasing Ring "
                      "state: "
                   << device_guard.error().message;
        return;
    }
    const auto result = cudaFree(state_);
    state_ = nullptr;
    if (result != cudaSuccess) {
        LOG(ERROR) << "Failed to free Ring device state: "
                   << cudaGetErrorString(result);
    }
}

PGResult<void> RingAllReduceProtocol::initializeHostState() {
    PG_TRY(auto device_guard, GpuDeviceGuard::create(device_index_));
    PG_TRY_CUDA(cudaHostAlloc(reinterpret_cast<void**>(&host_state_),
                              sizeof(HostState), cudaHostAllocPortable));
    std::construct_at(host_state_);
    return {};
}

void RingAllReduceProtocol::releaseHostState() noexcept {
    ready_ = false;
    if (!host_state_) return;
    std::destroy_at(host_state_);
    const auto result = cudaFreeHost(host_state_);
    if (result != cudaSuccess) {
        LOG(ERROR) << "Failed to free Ring host state: "
                   << cudaGetErrorString(result);
    }
    host_state_ = nullptr;
}

uint32_t RingAllReduceProtocol::chooseChannelCount(
    size_t size, uint32_t participant_count) {
    PG_ASSERT(participant_count != 0,
              "cannot choose Ring channels without participants");

    // Use as many channels as possible without making one rank's shard on a
    // channel too small to amortize CTA and transfer overhead. Halving keeps
    // the channel count power-of-two and falls back to one for small payloads.
    uint32_t channel_count = kMaxDeviceCollectiveChannels;
    while (channel_count > 1 &&
           size / participant_count / channel_count <
               kMinBytesPerChannelStep) {
        channel_count /= 2;
    }
    return channel_count;
}

PGResult<void> RingAllReduceProtocol::publish(
    int32_t self_active_index, uint32_t participant_count,
    RingPeerTarget predecessor, RingPeerTarget successor,
    const DeviceArenaSlice* staging) {
    const RingAllReducePlan plan{
        .transfer_handle = transfer_handle_,
        .invocation_state = invocation_state_,
        .recovery_mailbox = recovery_mailbox_,
        .timeout_ticks = timeout_ticks_,
        .buffer_offset = workspace_.buffer().offset(),
        .buffer_size = kRingBufferBytes,
        .signal_offset = signals_.offset(),
        .signal_layout = signal_layout_,
        .staging_offset = staging ? staging->offset() : 0,
        .staging_size = staging ? kRingStagingBytes : 0,
        .self_rank = self_rank_,
        .self_active_index = self_active_index,
        .participant_count = participant_count,
        .predecessor = predecessor,
        .successor = successor,
    };
    host_state_->plan = plan;
    ready_ = false;

    PG_TRY(auto device_guard, GpuDeviceGuard::create(device_index_));
    StreamSyncGuard sync_guard(control_stream_);

    // Make Graph replay reject the old image before changing any state it
    // references. The protocol is quiescent when its signal prefix is reset.
    PG_TRY_CUDA(setPlanStatusAsync(&state_->plan.status,
                                   DevicePlanStatus::Unavailable,
                                   control_stream_.get()));
    PG_TRY_CUDA(cudaMemsetAsync(
        signals_.addr(), 0, signals_.size(), control_stream_.get()));
    PG_TRY_CUDA(cudaMemcpyAsync(
        state_->next_step_sequences,
        host_state_->initial_step_sequences.data(),
        host_state_->initial_step_sequences.size() * sizeof(uint64_t),
        cudaMemcpyHostToDevice, control_stream_.get()));
    PG_TRY_CUDA(cudaMemcpyAsync(
        state_->next_recv_buffer_ready_sequences,
        host_state_->initial_recv_buffer_ready_sequences.data(),
        host_state_->initial_recv_buffer_ready_sequences.size() *
            sizeof(uint64_t),
        cudaMemcpyHostToDevice, control_stream_.get()));
    PG_TRY_CUDA(cudaMemcpyAsync(
        &state_->plan.plan, &host_state_->plan, sizeof(host_state_->plan),
        cudaMemcpyHostToDevice, control_stream_.get()));
    PG_TRY_CUDA(setPlanStatusAsync(&state_->plan.status,
                                   DevicePlanStatus::Ready,
                                   control_stream_.get()));
    PG_TRY(sync_guard.finish());
    ready_ = true;
    return {};
}

PGResult<void> RingAllReduceProtocol::invalidate() {
    ready_ = false;
    PG_TRY(auto device_guard, GpuDeviceGuard::create(device_index_));
    StreamSyncGuard sync_guard(control_stream_);
    PG_TRY_CUDA(setPlanStatusAsync(&state_->plan.status,
                                   DevicePlanStatus::Unavailable,
                                   control_stream_.get()));
    PG_TRY(sync_guard.finish());
    return {};
}

PGResult<void> RingAllReduceProtocol::useLocalOnly() {
    const RingPeerTarget self{
        .in_group_rank = self_rank_,
    };
    return publish(0, 1, self, self, nullptr);
}

PGResult<void> RingAllReduceProtocol::applyGroupView(
    const GroupView& view) {
    PG_ASSERT(view.max_group_size == static_cast<int32_t>(max_group_size_),
              "Ring group capacity changed");

    std::vector<InGroupRank> participants;
    participants.reserve(view.rank_order.size());
    for (InGroupRank in_group_rank = 0;
         static_cast<size_t>(in_group_rank) < view.rank_order.size();
         ++in_group_rank) {
        const auto global_rank = view.rank_order[in_group_rank];
        const auto& member = view.members[global_rank];
        if (!member.isActive()) continue;
        participants.push_back(in_group_rank);
    }

    const auto self =
        std::find(participants.begin(), participants.end(), self_rank_);
    if (self == participants.end()) return invalidate();

    const auto active_index =
        static_cast<size_t>(std::distance(participants.begin(), self));
    const auto participant_count = participants.size();
    const auto predecessor =
        participants[(active_index + participant_count - 1) %
                     participant_count];
    const auto successor = participants[(active_index + 1) % participant_count];
    auto targetFor = [&](InGroupRank in_group_rank)
        -> PGResult<RingPeerTarget> {
        const auto global_rank = view.rank_order[in_group_rank];
        const auto& member = view.members[global_rank];
        PG_ASSERT(member.endpoint &&
                      member.endpoint->device_collective.ring_all_reduce,
                  "active Ring peer ", in_group_rank,
                  " has no Ring endpoint");
        const auto& protocol_endpoint =
            *member.endpoint->device_collective.ring_all_reduce;
        PG_TRY(auto collective_endpoint, workspace_.endpoint(global_rank));
        PG_ASSERT(endpointSupports(collective_endpoint, protocol_endpoint,
                                   signal_layout_.total_signal_count),
                  "active Ring peer ", in_group_rank,
                  " has insufficient or overlapping resources: "
                  "buffer_offset=",
                  collective_endpoint.buffer_offset, ", buffer_size=",
                  collective_endpoint.buffer_size, ", signal_offset=",
                  protocol_endpoint.signal_offset, ", signal_count=",
                  protocol_endpoint.signal_count);
        return RingPeerTarget{
            .global_rank = global_rank,
            .in_group_rank = in_group_rank,
            .buffer_offset = collective_endpoint.buffer_offset,
            .signal_offset = protocol_endpoint.signal_offset,
        };
    };

    PG_TRY(auto predecessor_target, targetFor(predecessor));
    PG_TRY(auto successor_target, targetFor(successor));
    const DeviceArenaSlice* staging = nullptr;
    if (participant_count > 1) {
        PG_TRY(auto requires_staging,
               payloadWriterRequiresStaging(
                   transfer_service_, successor_target.global_rank));
        if (requires_staging) {
            PG_TRY(staging, workspace_.ensureStaging());
        }
    }

    return publish(static_cast<int32_t>(active_index),
                   static_cast<uint32_t>(participant_count),
                   predecessor_target, successor_target, staging);
}

bool RingAllReduceProtocol::ready() const noexcept { return ready_; }

const RingAllReduceEndpoint& RingAllReduceProtocol::localEndpoint()
    const noexcept {
    return endpoint_;
}

PGResult<void> RingAllReduceProtocol::enqueue(
    const void* send_buffer, void* recv_buffer, size_t count, DataType datatype,
    ReduceOp op, cudaStream_t stream, int32_t* failed_ranks_hint) const {
    PG_VALIDATE_STATE(ready_, "Ring AllReduce Plan is not ready");
    const uint32_t participant_count = host_state_->plan.participant_count;
    const size_t buffer_size = count * elementSize(datatype);
    const uint32_t channel_count =
        chooseChannelCount(buffer_size, participant_count);
    const RingAllReduceKernelArgs request{
        .send_buffer = send_buffer,
        .recv_buffer = recv_buffer,
        .count = static_cast<uint64_t>(count),
        .datatype = datatype,
        .op = op,
        .failed_ranks_hint = failed_ranks_hint,
    };
    PG_TRY_CUDA(
        launchRingAllReduceKernel(request, state_, channel_count, stream));
    return {};
}

}  // namespace mooncake
