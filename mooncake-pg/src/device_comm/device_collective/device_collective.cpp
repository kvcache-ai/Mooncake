#include "device_comm/device_collective/device_collective.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <exception>
#include <limits>
#include <mutex>
#include <new>
#include <string>
#include <type_traits>
#include <utility>

#include <glog/logging.h>

#include "device_comm/device_collective/device_collective_recovery.h"
#include "device_comm/device_collective/strong_stream.h"
#include "gpu_runtime.h"
#include "pg_utils.h"

namespace mooncake {
namespace {

static_assert(kMaxDeviceCollectiveChannels == 4);

struct AllReducePlanPublicationStaging {
    DeviceAllReducePlan plan;
    const std::array<uint64_t, kMaxDeviceCollectiveChannels> initial_sequences =
        {1, 1, 1, 1};
};

cudaError_t setPlanStatusAsync(DevicePlanStatus* device_status,
                               DevicePlanStatus status, cudaStream_t stream) {
    static_assert(sizeof(DevicePlanStatus) == 1);
    return cudaMemsetAsync(device_status, static_cast<int>(status),
                           sizeof(status), stream);
}

PGResult<uint64_t> timeoutTicks(int device_index, size_t timeout_us) {
    if (timeout_us == 0) return uint64_t{0};
    PG_TRY(auto device_guard, GpuDeviceGuard::create(device_index));
    int clock_rate_khz_value = 0;
    PG_TRY_CUDA(cudaDeviceGetAttribute(&clock_rate_khz_value,
                                       cudaDevAttrClockRate, device_index));
    const uint64_t clock_rate_khz = static_cast<uint64_t>(clock_rate_khz_value);
    if (clock_rate_khz == 0) return uint64_t{0};
    if (timeout_us > std::numeric_limits<uint64_t>::max() / clock_rate_khz) {
        return uint64_t{std::numeric_limits<uint64_t>::max()};
    }
    return uint64_t{std::max<uint64_t>(1, timeout_us * clock_rate_khz / 1000)};
}

bool rangesOverlap(const void* left, const void* right, size_t size) {
    if (size == 0 || left == right) return false;
    const auto left_begin = reinterpret_cast<uintptr_t>(left);
    const auto right_begin = reinterpret_cast<uintptr_t>(right);
    if (size > std::numeric_limits<uintptr_t>::max() - left_begin ||
        size > std::numeric_limits<uintptr_t>::max() - right_begin) {
        return true;
    }
    return left_begin < right_begin + size && right_begin < left_begin + size;
}

bool validDeviceCollectiveControlRange(const DeviceCollectiveEndpoint& endpoint,
                                       uint64_t expected_control_size) {
    return endpoint.control_offset >= kDeviceCollectiveWorkspaceSize &&
           endpoint.control_size == expected_control_size &&
           endpoint.control_offset <=
               std::numeric_limits<uint64_t>::max() - endpoint.control_size;
}

struct GraphUsePayload {
    std::atomic<size_t>* live_uses = nullptr;
};

void releaseGraphUse(void* opaque) {
    auto* payload = static_cast<GraphUsePayload*>(opaque);
    payload->live_uses->fetch_sub(1, std::memory_order_acq_rel);
    delete payload;
}

class StreamSyncGuard {
   public:
    explicit StreamSyncGuard(GpuStream& stream) : stream_(stream) {}

    ~StreamSyncGuard() noexcept {
        if (!active_) return;
        try {
            auto result = stream_.synchronize();
            if (!result.has_value()) {
                LOG(ERROR)
                    << "Failed to drain device collective control stream: "
                    << result.error().message;
            }
        } catch (const std::exception& error) {
            LOG(ERROR) << "Failed to drain device collective control stream: "
                       << error.what();
        } catch (...) {
            LOG(ERROR) << "Failed to drain device collective control stream";
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

template <typename Item>
uint64_t reserveLayoutItems(uint64_t& cursor, uint64_t count = 1) {
    cursor += alignmentPadding(cursor, alignof(Item));
    const uint64_t offset = cursor;
    cursor += count * sizeof(Item);
    return offset;
}

template <typename Item>
Item* layoutItemsAt(void* base, uint64_t offset) noexcept {
    return reinterpret_cast<Item*>(static_cast<char*>(base) + offset);
}
}  // namespace

uint32_t DeviceCollectiveRuntime::chooseChannelCount(size_t size) {
    // Add parallel CTAs only when each receives a useful amount of tensor
    // work. The service then divides its full 16 MiB buffer pair by the same
    // 1/2/4 count, yielding 16/8/4 MiB transfer units respectively.
    if (size <= kChannelScaleUnitBytes) return 1;
    if (size <= 2 * kChannelScaleUnitBytes) return 2;
    return kMaxDeviceCollectiveChannels;
}

DeviceCollectiveRuntime::ControlSliceLayout
DeviceCollectiveRuntime::ControlSliceLayout::make(uint32_t max_group_size) {
    ControlSliceLayout layout;
    uint64_t cursor = 0;

    layout.all_reduce_plan_offset =
        reserveLayoutItems<DeviceAllReducePlanSlot>(cursor);
    layout.next_step_sequences_offset =
        reserveLayoutItems<uint64_t>(cursor, kMaxDeviceCollectiveChannels);
    layout.next_recv_ready_sequences_offset =
        reserveLayoutItems<uint64_t>(cursor, kMaxDeviceCollectiveChannels);
    layout.invocation_offset =
        reserveLayoutItems<DeviceCollectiveInvocationState>(cursor);

    const uint64_t signal_slot_count =
        static_cast<uint64_t>(kMaxDeviceCollectiveChannels) * max_group_size;
    layout.recv_ready_slots_offset =
        reserveLayoutItems<uint64_t>(cursor, signal_slot_count);
    layout.signal_slots_offset =
        reserveLayoutItems<uint64_t>(cursor, signal_slot_count);
    layout.consumed_ack_slots_offset =
        reserveLayoutItems<uint64_t>(cursor, signal_slot_count);
    layout.size = cursor + alignmentPadding(cursor, kAlignment);

    layout.max_group_size = max_group_size;
    return layout;
}

DeviceCollectiveControlView DeviceCollectiveRuntime::ControlSliceLayout::map(
    const DeviceArenaSlice& control_slice) const {
    void* const control_addr = control_slice.addr();
    const uint64_t control_region_offset = control_slice.offset();

    auto mapSignalTable = [&](uint64_t offset) {
        return DeviceCollectiveSignalTable{
            .local_region_offset = control_region_offset + offset,
            .control_offset = offset,
            .max_group_size = max_group_size,
        };
    };

    return DeviceCollectiveControlView{
        .all_reduce_plan = layoutItemsAt<DeviceAllReducePlanSlot>(
            control_addr, all_reduce_plan_offset),
        .next_step_sequences =
            layoutItemsAt<uint64_t>(control_addr, next_step_sequences_offset),
        .next_recv_ready_sequences = layoutItemsAt<uint64_t>(
            control_addr, next_recv_ready_sequences_offset),
        .invocation = layoutItemsAt<DeviceCollectiveInvocationState>(
            control_addr, invocation_offset),
        .recv_ready_slots = mapSignalTable(recv_ready_slots_offset),
        .signal_slots = mapSignalTable(signal_slots_offset),
        .consumed_ack_slots = mapSignalTable(consumed_ack_slots_offset),
    };
}

struct DeviceCollectiveRuntime::HostControl {
    // The host and device directly share this member through the mapped
    // allocation.
    DeviceCollectiveRecoveryMailbox recovery_mailbox;

    // The remaining members are pinned host sources for control-stream copies.
    AllReducePlanPublicationStaging all_reduce_plan_publication;
};

DeviceCollectiveRuntime::DeviceCollectiveRuntime(
    DeviceTransferService& transfer_service, int device_index,
    InGroupRank self_rank, uint64_t timeout_ticks, ControlSliceLayout layout,
    DeviceArenaSlice control_slice,
    DeviceCollectiveKernelResources kernel_resources,
    StrongStream& strong_stream, DeviceCollectiveEndpoint endpoint,
    GpuStream control_stream, GpuEvent handoff_event)
    : transfer_service_(transfer_service),
      device_index_(device_index),
      self_rank_(self_rank),
      timeout_ticks_(timeout_ticks),
      layout_(std::move(layout)),
      control_slice_(std::move(control_slice)),
      strong_stream_(strong_stream),
      endpoint_(std::move(endpoint)),
      kernel_resources_(kernel_resources),
      control_stream_(std::move(control_stream)),
      handoff_event_(std::move(handoff_event)) {}

PGResult<void> DeviceCollectiveRuntime::initializeHostControl() {
    PG_TRY(auto device_guard, GpuDeviceGuard::create(device_index_));
    PG_TRY_CUDA(cudaHostAlloc(reinterpret_cast<void**>(&host_control_),
                              sizeof(HostControl),
                              cudaHostAllocMapped | cudaHostAllocPortable));
    std::construct_at(host_control_);

    void* device_control = nullptr;
    PG_TRY_CUDA(cudaHostGetDevicePointer(&device_control, host_control_, 0));
    kernel_resources_.recovery = layoutItemsAt<DeviceCollectiveRecoveryMailbox>(
        device_control, offsetof(HostControl, recovery_mailbox));
    return {};
}

void DeviceCollectiveRuntime::releaseHostControl() noexcept {
    kernel_resources_.recovery = nullptr;
    if (!host_control_) return;
    std::destroy_at(host_control_);
    const auto result = cudaFreeHost(host_control_);
    if (result != cudaSuccess) {
        LOG(ERROR) << "Failed to free device collective host control: "
                   << cudaGetErrorString(result);
    }
    host_control_ = nullptr;
}

PGResult<void> DeviceCollectiveRuntime::publishAllReducePlan(
    DeviceAllReducePlan plan) {
    auto& publication = host_control_->all_reduce_plan_publication;
    publication.plan = plan;

    // Gate regular host submission before attempting any device update. The
    // device status below independently gates CUDA Graph replays.
    host_all_reduce_plan_ready_ = false;

    PG_TRY(auto device_guard, GpuDeviceGuard::create(device_index_));
    StreamSyncGuard sync_guard(control_stream_);

    // Invalidate the old image before changing anything it references.
    PG_TRY_CUDA(setPlanStatusAsync(
        &kernel_resources_.control.all_reduce_plan->status,
        DevicePlanStatus::Unavailable, control_stream_.get()));

    auto* const control_base = static_cast<char*>(control_slice_.addr());
    const size_t slot_count =
        static_cast<size_t>(kMaxDeviceCollectiveChannels) *
        layout_.max_group_size;
    PG_TRY_CUDA(cudaMemsetAsync(control_base + layout_.recv_ready_slots_offset,
                                0, slot_count * sizeof(uint64_t),
                                control_stream_.get()));
    PG_TRY_CUDA(cudaMemsetAsync(control_base + layout_.signal_slots_offset, 0,
                                slot_count * sizeof(uint64_t),
                                control_stream_.get()));
    PG_TRY_CUDA(
        cudaMemsetAsync(control_base + layout_.consumed_ack_slots_offset, 0,
                        slot_count * sizeof(uint64_t), control_stream_.get()));
    PG_TRY_CUDA(
        cudaMemcpyAsync(kernel_resources_.control.next_step_sequences,
                        publication.initial_sequences.data(),
                        publication.initial_sequences.size() * sizeof(uint64_t),
                        cudaMemcpyHostToDevice, control_stream_.get()));
    PG_TRY_CUDA(
        cudaMemcpyAsync(kernel_resources_.control.next_recv_ready_sequences,
                        publication.initial_sequences.data(),
                        publication.initial_sequences.size() * sizeof(uint64_t),
                        cudaMemcpyHostToDevice, control_stream_.get()));
    PG_TRY_CUDA(
        cudaMemcpyAsync(&kernel_resources_.control.all_reduce_plan->plan,
                        &publication.plan, sizeof(publication.plan),
                        cudaMemcpyHostToDevice, control_stream_.get()));
    PG_TRY_CUDA(
        setPlanStatusAsync(&kernel_resources_.control.all_reduce_plan->status,
                           DevicePlanStatus::Ready, control_stream_.get()));
    PG_TRY(sync_guard.finish());
    host_all_reduce_plan_ready_ = true;
    return {};
}

PGResult<void> DeviceCollectiveRuntime::invalidateAllReducePlan() {
    host_all_reduce_plan_ready_ = false;

    PG_TRY(auto device_guard, GpuDeviceGuard::create(device_index_));
    StreamSyncGuard sync_guard(control_stream_);
    PG_TRY_CUDA(setPlanStatusAsync(
        &kernel_resources_.control.all_reduce_plan->status,
        DevicePlanStatus::Unavailable, control_stream_.get()));
    PG_TRY(sync_guard.finish());
    return {};
}

PGResult<void> DeviceCollectiveRuntime::attachGraphUse(
    const GpuCaptureInfo& capture) {
    if (!capture.active) return {};

    auto* payload = new (std::nothrow) GraphUsePayload{&live_graph_uses_};
    if (!payload) {
        return makePGError(PGErrorCode::ResourceBusy,
                           "failed to allocate CUDA Graph use token");
    }
    live_graph_uses_.fetch_add(1, std::memory_order_acq_rel);
    auto object_result =
        GpuGraphUserObject::create(device_index_, payload, releaseGraphUse);
    if (!object_result.has_value()) {
        live_graph_uses_.fetch_sub(1, std::memory_order_acq_rel);
        delete payload;
        return makePGError(std::move(object_result).error());
    }
    auto object = std::move(object_result).value();
    return object.transferToGraph(capture);
}

PGResult<void> DeviceCollectiveRuntime::recoverFailure() {
    auto& recovery = host_control_->recovery_mailbox;

    // recoverFailure() runs on the dedicated recovery worker. A device-side
    // timeout parks the kernel but does not cancel in-flight HostProxy
    // commands. Drain them before changing collective state. It is safe to
    // block here: the parked kernel retains its buffers, and the HostProxy
    // worker continues making progress.
    PG_TRY(transfer_service_.waitUntilIdle());

    const auto failed_rank = recovery.failed_rank;
    if (recovery.failed_hint_address != 0) {
        auto* hint = reinterpret_cast<int32_t*>(recovery.failed_hint_address);
        hint[failed_rank] = 1;
    }

    auto recovered = recovery_handler_(failed_rank);

    if (!recovered.has_value()) {
        std::lock_guard<std::mutex> lock(mutex_);
        auto invalidated = invalidateAllReducePlan();
        if (!invalidated.has_value()) {
            return makePGError(
                PGErrorCode::SystemError,
                "device collective recovery failed and an unavailable Plan "
                "could not be published: " +
                    recovered.error().message + "; " +
                    invalidated.error().message);
        }
        LOG(ERROR) << "device collective recovery failed; Plan was made "
                      "unavailable: "
                   << recovered.error().message;
    }

    return {};
}

PGResult<std::unique_ptr<DeviceCollectiveRuntime>>
DeviceCollectiveRuntime::create(DeviceTransferService& transfer_service,
                                DeviceArena& arena,
                                const DeviceArenaSlice& workspace,
                                StrongStream& strong_stream, int device_index,
                                InGroupRank self_rank, uint32_t max_group_size,
                                size_t collective_timeout_us) {
    PG_VALIDATE_ARG(arena.deviceIndex() == device_index,
                    "device collective arena belongs to another device");
    PG_VALIDATE_ARG(
        self_rank >= 0 && static_cast<uint32_t>(self_rank) < max_group_size,
        "device collective self rank is outside the group");

    PG_TRY(auto timeout_ticks,
           timeoutTicks(device_index, collective_timeout_us));
    auto layout = ControlSliceLayout::make(max_group_size);
    const auto* transfer_handle = transfer_service.deviceHandle();
    PG_TRY(auto control_slice,
           arena.allocate(layout.size, ControlSliceLayout::kAlignment));
    {
        PG_TRY(auto device_guard, GpuDeviceGuard::create(device_index));
        PG_TRY_CUDA(cudaMemset(control_slice.addr(), 0, control_slice.size()));
    }
    PG_TRY(auto control_stream, GpuStream::createNonBlocking(device_index));
    PG_TRY(auto handoff_event, GpuEvent::create(device_index));
    const DeviceCollectiveTransferBuffer send_buffer{
        .addr = workspace.addr(),
        .region_offset = workspace.offset(),
        .size = kDeviceCollectiveTransferBufferSize,
    };
    const DeviceCollectiveTransferBuffer recv_buffer{
        .addr = static_cast<char*>(workspace.addr()) +
                kDeviceCollectiveTransferBufferSize,
        .region_offset =
            workspace.offset() + kDeviceCollectiveTransferBufferSize,
        .size = kDeviceCollectiveTransferBufferSize,
    };

    const DeviceCollectiveKernelResources kernel_resources{
        .transfer_handle = transfer_handle,
        .send_buffer = send_buffer,
        .recv_buffer = recv_buffer,
        .timeout_ticks = timeout_ticks,
        .control = layout.map(control_slice),
    };
    DeviceCollectiveEndpoint endpoint{
        .control_offset = control_slice.offset(),
        .control_size = control_slice.size(),
    };
    auto runtime =
        std::unique_ptr<DeviceCollectiveRuntime>(new DeviceCollectiveRuntime(
            transfer_service, device_index, self_rank, timeout_ticks,
            std::move(layout), std::move(control_slice), kernel_resources,
            strong_stream, std::move(endpoint), std::move(control_stream),
            std::move(handoff_event)));
    PG_TRY(runtime->initializeHostControl());
    return runtime;
}

DeviceCollectiveRuntime::~DeviceCollectiveRuntime() noexcept {
    auto result = shutdown();
    if (!result.has_value()) {
        LOG(ERROR) << "DeviceCollectiveRuntime destroyed before shutdown "
                      "could complete: "
                   << result.error().message;
    }
    releaseHostControl();
}

const DeviceCollectiveEndpoint& DeviceCollectiveRuntime::localEndpoint() const {
    return endpoint_;
}

PGResult<void> DeviceCollectiveRuntime::useLocalOnly() {
    std::lock_guard<std::mutex> lock(mutex_);
    PG_VALIDATE_STATE(!shutdown_requested_,
                      "device collective runtime is shutting down");

    return publishAllReducePlan(DeviceAllReducePlan{
        .self_rank = self_rank_,
        .self_active_index = 0,
        .participant_count = 1,
        .predecessor = {.in_group_rank = self_rank_},
        .successor = {.in_group_rank = self_rank_},
    });
}

PGResult<void> DeviceCollectiveRuntime::applyGroupView(const GroupView& view) {
    std::lock_guard<std::mutex> lock(mutex_);
    // Graceful shutdown may be waiting for a failed collective to recover.
    // Keep accepting recovery-driven Plan updates until that collective has
    // drained. Updates queued after shutdown completes are no longer needed.
    if (shutdown_complete_) return {};

    // The caller has already installed the Agent's rank-scoped endpoint
    // snapshot in the transfer service. GroupView contributes each active
    // rank's global rank and remote collective-control range.
    std::vector<InGroupRank> participants;
    participants.reserve(view.rank_order.size());
    for (InGroupRank in_group_rank = 0;
         static_cast<size_t>(in_group_rank) < view.rank_order.size();
         ++in_group_rank) {
        const auto global_rank = view.rank_order[in_group_rank];
        const auto& member = view.members[global_rank];
        if (!member.isActive()) continue;

        PG_ASSERT(member.endpoint->collective_v2, "active collective peer ",
                  in_group_rank, " has no device collective endpoint");
        const auto& endpoint = *member.endpoint->collective_v2;
        PG_ASSERT(
            validDeviceCollectiveControlRange(endpoint, layout_.size),
            "active device collective peer ", in_group_rank,
            " has invalid control range: offset=", endpoint.control_offset,
            ", size=", endpoint.control_size, ", expected_size=", layout_.size);
        participants.push_back(in_group_rank);
    }

    const auto self =
        std::find(participants.begin(), participants.end(), self_rank_);
    if (self == participants.end()) {
        return invalidateAllReducePlan();
    }

    const auto active_index =
        static_cast<size_t>(std::distance(participants.begin(), self));
    const auto participant_count = participants.size();
    const auto predecessor =
        participants[(active_index + participant_count - 1) %
                     participant_count];
    const auto successor = participants[(active_index + 1) % participant_count];
    auto targetFor = [&](InGroupRank in_group_rank) {
        const auto global_rank = view.rank_order[in_group_rank];
        const auto& endpoint =
            *view.members[global_rank].endpoint->collective_v2;
        return DeviceCollectivePeerTarget{
            .global_rank = global_rank,
            .in_group_rank = in_group_rank,
            .remote_control_offset = endpoint.control_offset,
        };
    };
    return publishAllReducePlan(DeviceAllReducePlan{
        .self_rank = self_rank_,
        .self_active_index = static_cast<int32_t>(active_index),
        .participant_count = static_cast<uint32_t>(participant_count),
        .predecessor = targetFor(predecessor),
        .successor = targetFor(successor),
    });
}

PGResult<void> DeviceCollectiveRuntime::enableRecovery(
    DeviceCollectiveRecoveryWorker& worker, RecoveryHandler handler) {
    std::lock_guard<std::mutex> lock(mutex_);
    recovery_handler_ = std::move(handler);
    auto added = worker.addMailbox(&host_control_->recovery_mailbox,
                                   [this] { return recoverFailure(); });
    if (!added.has_value()) {
        recovery_handler_ = {};
        return makePGError(std::move(added).error());
    }
    recovery_worker_ = &worker;
    return {};
}

PGResult<void> DeviceCollectiveRuntime::enqueueAllReduce(
    const void* send_buffer, void* recv_buffer, size_t count, DataType datatype,
    ReduceOp op, cudaStream_t user_stream_handle, int32_t* failed_ranks_hint) {
    std::unique_lock<std::mutex> enqueue_lock(mutex_);

    const size_t buffer_size = count * elementSize(datatype);
    PG_VALIDATE_ARG(
        !rangesOverlap(send_buffer, recv_buffer, buffer_size),
        "device AllReduce buffers must be identical or non-overlapping");

    PG_VALIDATE_STATE(!shutdown_requested_,
                      "device collective runtime is shutting down");
    PG_VALIDATE_STATE(host_all_reduce_plan_ready_,
                      "device AllReduce Plan is not ready");

    auto user_stream = GpuStream::borrow(user_stream_handle, device_index_);
    PG_TRY(auto capture, user_stream.captureInfo());
    PG_TRY(attachGraphUse(capture));
    PG_TRY(auto order_lease, strong_stream_.acquire(capture));
    const auto& order_stream = order_lease.stream();

    cudaError_t launch_error = cudaSuccess;
    auto submitted = [&]() -> PGResult<void> {
        // Keep the kernel on the user-provided stream.
        PG_TRY(handoff_event_.record(order_stream));
        PG_TRY(user_stream.waitEvent(handoff_event_));

        launch_error = launchDeviceAllReduceKernel(
            DeviceAllReduceKernelArgs{
                .send_buffer = send_buffer,
                .recv_buffer = recv_buffer,
                .count = static_cast<uint64_t>(count),
                .datatype = datatype,
                .op = op,
                .channel_count = chooseChannelCount(buffer_size),
                .failed_ranks_hint = failed_ranks_hint,
            },
            kernel_resources_, user_stream.get());

        PG_TRY(handoff_event_.record(user_stream));
        PG_TRY(order_stream.waitEvent(handoff_event_));
        return {};
    }();

    auto released = order_lease.release();
    if (!submitted.has_value()) {
        auto error = std::move(submitted).error();
        if (!released.has_value()) {
            error.message += "; StrongStream release also failed: " +
                             released.error().message;
        }
        return makePGError(std::move(error));
    }
    PG_TRY(released);
    PG_TRY_CUDA(launch_error);
    return {};
}

PGResult<void> DeviceCollectiveRuntime::shutdown() {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (shutdown_complete_) return {};
        if (!shutdown_requested_) {
            if (live_graph_uses_.load(std::memory_order_acquire) != 0) {
                return makePGError(
                    PGErrorCode::ResourceBusy,
                    "CUDA Graph/GraphExec still references this communicator");
            }
            shutdown_requested_ = true;
        }
    }

    auto synchronized = strong_stream_.waitUntilIdle();
    if (!synchronized.has_value()) {
        return makePGError(std::move(synchronized).error());
    }

    DeviceCollectiveRecoveryWorker* recovery_worker = nullptr;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        recovery_worker = std::exchange(recovery_worker_, nullptr);
    }

    // Removal may wait for the worker's current recovery. Do not hold the
    // Runtime mutex, and keep the mailbox and control slice alive until it
    // returns.
    if (recovery_worker) {
        recovery_worker->removeMailbox(&host_control_->recovery_mailbox);
    }

    {
        std::lock_guard<std::mutex> lock(mutex_);
        recovery_handler_ = {};
        auto control_slice_to_release = std::move(control_slice_);
        shutdown_complete_ = true;
    }
    return {};
}

}  // namespace mooncake
