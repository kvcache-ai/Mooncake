#include "device_comm/device_collective/device_collective.h"

#include <algorithm>
#include <atomic>
#include <limits>
#include <new>
#include <utility>

#include <glog/logging.h>

#include "device_comm/device_collective/device_control_update.h"
#include "device_comm/device_collective/protocols/ring/ring_all_reduce.h"
#include "device_comm/device_collective/strong_stream.h"
#include "pg_utils.h"

namespace mooncake {
namespace {

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

struct GraphUsePayload {
    std::atomic<size_t>* live_uses = nullptr;
};

void releaseGraphUse(void* opaque) {
    auto* payload = static_cast<GraphUsePayload*>(opaque);
    payload->live_uses->fetch_sub(1, std::memory_order_acq_rel);
    delete payload;
}

}  // namespace

DeviceCollectiveRuntime::DeviceCollectiveRuntime(
    DeviceTransferService& transfer_service, int device_index,
    InGroupRank self_rank, uint32_t max_group_size,
    int32_t* active_ranks_mirror, RegionSlice view_epoch_signals,
    StrongStream& strong_stream, GpuEvent handoff_event)
    : transfer_service_(transfer_service),
      device_index_(device_index),
      self_rank_(self_rank),
      view_epoch_signals_(std::move(view_epoch_signals)),
      strong_stream_(strong_stream),
      active_ranks_mirror_(active_ranks_mirror),
      active_ranks_count_(max_group_size),
      handoff_event_(std::move(handoff_event)) {}

void DeviceCollectiveRuntime::releaseState() noexcept {
    if (invocation_state_) {
        auto device_guard = GpuDeviceGuard::create(device_index_);
        if (!device_guard.has_value()) {
            LOG(ERROR) << "Failed to select CUDA device while releasing "
                          "device collective invocation state: "
                       << device_guard.error().message;
        } else {
            const auto result = cudaFree(invocation_state_);
            invocation_state_ = nullptr;
            if (result != cudaSuccess) {
                LOG(ERROR)
                    << "Failed to free device collective invocation state: "
                    << cudaGetErrorString(result);
            }
        }
    }

    if (control_mailbox_) {
        std::destroy_at(control_mailbox_);
        const auto result = cudaFreeHost(control_mailbox_);
        if (result != cudaSuccess) {
            LOG(ERROR) << "Failed to free device collective control mailbox: "
                       << cudaGetErrorString(result);
        }
        control_mailbox_ = nullptr;
    }
}

PGResult<std::unique_ptr<DeviceCollectiveRuntime>>
DeviceCollectiveRuntime::create(DeviceTransferService& transfer_service,
                                DeviceCollectiveWorkspace& workspace,
                                StrongStream& strong_stream, int device_index,
                                InGroupRank self_rank, uint32_t max_group_size,
                                int32_t* active_ranks_mirror,
                                size_t collective_timeout_us) {
    PG_VALIDATE_ARG(transfer_service.deviceIndex() == device_index,
                    "device transfer service belongs to another device");
    PG_VALIDATE_ARG(
        self_rank >= 0 && static_cast<uint32_t>(self_rank) < max_group_size,
        "device collective self rank is outside the group");
    PG_VALIDATE_ARG(max_group_size <= kMaxNumRanks,
                    "device collective group capacity is too large");

    PG_TRY(auto timeout_ticks,
           timeoutTicks(device_index, collective_timeout_us));
    const uint64_t view_epoch_signal_bytes =
        static_cast<uint64_t>(max_group_size) * sizeof(uint64_t);
    PG_TRY(auto view_epoch_signals,
           transfer_service.allocatePeerAccessible(view_epoch_signal_bytes,
                                                   alignof(uint64_t)));

    PG_TRY(auto handoff_event, GpuEvent::create(device_index));
    auto runtime =
        std::unique_ptr<DeviceCollectiveRuntime>(new DeviceCollectiveRuntime(
            transfer_service, device_index, self_rank, max_group_size,
            active_ranks_mirror, std::move(view_epoch_signals), strong_stream,
            std::move(handoff_event)));
    ControlMailbox* device_control_mailbox = nullptr;
    {
        PG_TRY(auto device_guard, GpuDeviceGuard::create(device_index));
        std::array<uint64_t, kMaxNumRanks> initial_view_epoch_signals;
        initial_view_epoch_signals.fill(kInvalidViewEpoch);
        PG_TRY_CUDA(cudaMemcpy(runtime->view_epoch_signals_.addr(),
                               initial_view_epoch_signals.data(),
                               runtime->view_epoch_signals_.size(),
                               cudaMemcpyHostToDevice));
        PG_TRY_CUDA(
            cudaMalloc(reinterpret_cast<void**>(&runtime->invocation_state_),
                       sizeof(InvocationState)));
        PG_TRY_CUDA(
            cudaMemset(runtime->invocation_state_, 0, sizeof(InvocationState)));

        PG_TRY_CUDA(
            cudaHostAlloc(reinterpret_cast<void**>(&runtime->control_mailbox_),
                          sizeof(ControlMailbox),
                          cudaHostAllocMapped | cudaHostAllocPortable));
        std::construct_at(runtime->control_mailbox_);

        void* device_mailbox = nullptr;
        PG_TRY_CUDA(cudaHostGetDevicePointer(&device_mailbox,
                                             runtime->control_mailbox_, 0));
        device_control_mailbox = static_cast<ControlMailbox*>(device_mailbox);
    }
    PG_TRY(
        runtime->all_reduce_,
        RingAllReduceProtocol::create(
            transfer_service, workspace,
            static_cast<const uint64_t*>(runtime->view_epoch_signals_.addr()),
            runtime->invocation_state_, device_control_mailbox, timeout_ticks,
            device_index, self_rank, max_group_size));
    return runtime;
}

DeviceCollectiveRuntime::~DeviceCollectiveRuntime() noexcept {
    auto result = shutdown();
    if (!result.has_value()) {
        LOG(ERROR) << "DeviceCollectiveRuntime destroyed before shutdown "
                      "could complete: "
                   << result.error().message;
    }
    releaseState();
}

DeviceGroupEndpoint DeviceCollectiveRuntime::localEndpoint() const {
    return DeviceGroupEndpoint{
        .view_epoch_signal = view_epoch_signals_.offset(),
        .view_epoch_signal_count = static_cast<uint32_t>(active_ranks_count_),
        .ring_all_reduce = all_reduce_->localEndpoint(),
    };
}

PGResult<void> DeviceCollectiveRuntime::useLocalOnly(uint64_t view_epoch) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (shutdown_complete_) return {};
    PG_VALIDATE_STATE(!shutdown_requested_,
                      "device collective runtime is shutting down");
    PG_ASSERT(view_epoch != kInvalidViewEpoch,
              "device collective View epoch uses the reserved invalid value");
    if (view_epoch_ == view_epoch) return {};

    all_reduce_->useLocalOnly(view_epoch);
    if (active_ranks_mirror_) {
        host_active_ranks_.fill(0);
        host_active_ranks_[self_rank_] = 1;
    }
    PG_TRY(publishControlState(/* pinned = */ false,
                               /* include_active_ranks_mirror = */ false));
    view_epoch_ = view_epoch;
    return {};
}

PGResult<void> DeviceCollectiveRuntime::applyGroupView(const GroupView& view) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (shutdown_complete_) return {};
    PG_VALIDATE_STATE(!shutdown_requested_,
                      "device collective runtime is shutting down");
    PG_ASSERT(view.epoch != kInvalidViewEpoch,
              "device collective View epoch uses the reserved invalid value");
    // A duplicate group view must not reset protocol state again.
    if (view_epoch_ == view.epoch) return {};

    PG_TRY(all_reduce_->applyGroupView(view));

    if (active_ranks_mirror_) {
        host_active_ranks_.fill(0);
        for (size_t in_group_rank = 0; in_group_rank < view.rank_order.size();
             ++in_group_rank) {
            const auto global_rank = view.rank_order[in_group_rank];
            host_active_ranks_[in_group_rank] =
                view.members[global_rank].isActive() ? 1 : 0;
        }
    }

    PG_TRY(publishControlState(/* pinned = */ false,
                               /* include_active_ranks_mirror = */ false));
    view_epoch_ = view.epoch;
    return {};
}

PGResult<void> DeviceCollectiveRuntime::publishControlState(
    bool pinned, bool include_active_ranks_mirror) {
    ControlUpdateBuilder builder;
    // The caller-owned mirror normally follows the direct cudaMemcpyAsync
    // path. Parked recovery includes it in the same update because
    // synchronizing a separate CUDA copy while the last channel CTA is waiting
    // would deadlock recovery.
    if (include_active_ranks_mirror && active_ranks_mirror_) {
        PG_TRY(builder.copyBytes(
            active_ranks_mirror_, host_active_ranks_.data(),
            active_ranks_count_ * sizeof(host_active_ranks_.front())));
    }
    PG_TRY(all_reduce_->appendPlanUpdate(builder));
    publishControlUpdate(control_mailbox_->control_update_slot,
                         builder.controlUpdate(), pinned);
    return {};
}

bool DeviceCollectiveRuntime::hasPendingRecovery() const noexcept {
    const uint64_t ready = std::atomic_ref(control_mailbox_->ready_generation)
                               .load(std::memory_order_acquire);
    const uint64_t failure =
        std::atomic_ref(control_mailbox_->failure_generation)
            .load(std::memory_order_acquire);
    return failure > ready;
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

PGResult<void> DeviceCollectiveRuntime::prepareFailureResume() {
    auto& mailbox = *control_mailbox_;

    // The last channel CTA of the failed invocation remains resident while the
    // host-proxy worker can still make progress, so drain outstanding route
    // work before replacing protocol state.
    PG_TRY(transfer_service_.waitUntilIdle());

    const auto failed_rank = mailbox.failed_rank;
    if (mailbox.failed_hint_address != 0) {
        auto* hint = reinterpret_cast<int32_t*>(mailbox.failed_hint_address);
        hint[failed_rank] = 1;
    }

    auto recovery_result = failure_recovery_callback_(failed_rank);

    std::lock_guard<std::mutex> lock(mutex_);
    // On success, publish a pinned update that resets Ring progress state and
    // installs the host control state left by the callback. The last channel
    // CTA of the failed invocation is the only consumer of this update.
    //
    // On failure, invalidate the host Plan first. The pinned update still
    // resets Ring progress state, but installs an unavailable Plan so that CTA
    // can exit without leaving stale plan usable.
    if (recovery_result.has_value()) {
        PG_TRY(publishControlState(/* pinned = */ true,
                                   /* include_active_ranks_mirror = */ true));
    } else {
        LOG(ERROR) << "device collective recovery callback failed; falling "
                      "back to Plan invalidation: "
                   << recovery_result.error().message;
        all_reduce_->invalidateHostPlan();
        view_epoch_ = kInvalidViewEpoch;
        PG_TRY(publishControlState(/* pinned = */ true,
                                   /* include_active_ranks_mirror = */ true));
    }
    return {};
}

PGResult<void> DeviceCollectiveRuntime::enableRecovery(
    DeviceCollectiveRecoveryWorker& worker, FailureRecoveryCallback callback) {
    std::lock_guard<std::mutex> lock(mutex_);
    failure_recovery_callback_ = std::move(callback);
    auto added = worker.addMailbox(control_mailbox_,
                                   [this] { return prepareFailureResume(); });
    if (!added.has_value()) {
        failure_recovery_callback_ = {};
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
    PG_VALIDATE_STATE(all_reduce_->ready(),
                      "device AllReduce Plan is not ready");

    auto user_stream = GpuStream::borrow(user_stream_handle, device_index_);
    PG_TRY(auto capture, user_stream.captureInfo());
    PG_TRY(attachGraphUse(capture));
    PG_TRY(auto order_lease, strong_stream_.acquire(capture));
    const auto& order_stream = order_lease.stream();

    auto submitted = [&]() -> PGResult<void> {
        PG_TRY(handoff_event_.record(order_stream));
        PG_TRY(user_stream.waitEvent(handoff_event_));

        auto launched =
            all_reduce_->enqueue(send_buffer, recv_buffer, count, datatype, op,
                                 user_stream.get(), failed_ranks_hint);

        PG_TRY(handoff_event_.record(user_stream));
        PG_TRY(order_stream.waitEvent(handoff_event_));
        return launched;
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

    PG_TRY(strong_stream_.waitUntilIdle());

    DeviceCollectiveRecoveryWorker* recovery_worker = nullptr;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        recovery_worker = std::exchange(recovery_worker_, nullptr);
    }
    if (recovery_worker) {
        recovery_worker->removeMailbox(control_mailbox_);
    }

    std::unique_ptr<RingAllReduceProtocol> protocol_to_release;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        failure_recovery_callback_ = {};
        protocol_to_release = std::move(all_reduce_);
    }
    protocol_to_release.reset();
    releaseState();
    {
        std::lock_guard<std::mutex> lock(mutex_);
        shutdown_complete_ = true;
    }
    return {};
}

}  // namespace mooncake
