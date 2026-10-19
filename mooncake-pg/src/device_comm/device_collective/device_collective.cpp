#include "device_comm/device_collective/device_collective.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <new>
#include <utility>

#include <glog/logging.h>

#include "device_comm/device_utils/d2h_request_slot.h"
#include "device_comm/device_utils/h2d_request_slot.h"
#include "device_comm/device_collective/device_collective_dispatcher.h"
#include "device_comm/device_collective/device_control_update.h"
#include "device_comm/device_collective/device_collective_workspace.h"
#include "device_comm/device_collective/strong_stream.h"
#include "gpu_runtime.h"
#include "pg_utils.h"

namespace mooncake {

namespace {

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
    DeviceTransferService& transfer_service,
    DeviceCollectiveWorkspace& workspace, int device_index,
    InGroupRank self_rank, uint32_t max_group_size,
    int32_t* active_ranks_mirror, RegionSlice view_epoch_signals,
    StrongStream& strong_stream, GpuEvent handoff_event)
    : transfer_service_(transfer_service),
      workspace_(workspace),
      device_index_(device_index),
      self_rank_(self_rank),
      view_epoch_signals_(std::move(view_epoch_signals)),
      strong_stream_(strong_stream),
      active_ranks_mirror_(active_ranks_mirror),
      active_ranks_count_(max_group_size),
      handoff_event_(std::move(handoff_event)) {}

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
           gpuTimeoutTicks(device_index, collective_timeout_us));
    const uint64_t view_epoch_signal_bytes =
        static_cast<uint64_t>(max_group_size) * sizeof(uint64_t);
    PG_TRY(auto view_epoch_signals,
           transfer_service.allocatePeerAccessible(view_epoch_signal_bytes,
                                                   alignof(uint64_t)));

    PG_TRY(auto handoff_event, GpuEvent::create(device_index));
    auto runtime =
        std::unique_ptr<DeviceCollectiveRuntime>(new DeviceCollectiveRuntime(
            transfer_service, workspace, device_index, self_rank,
            max_group_size, active_ranks_mirror, std::move(view_epoch_signals),
            strong_stream, std::move(handoff_event)));
    {
        PG_TRY(auto device_guard, GpuDeviceGuard::create(device_index));
        std::array<uint64_t, kMaxNumRanks> initial_view_epoch_signals;
        initial_view_epoch_signals.fill(kInvalidViewEpoch);
        PG_TRY_CUDA(cudaMemcpy(runtime->view_epoch_signals_.addr(),
                               initial_view_epoch_signals.data(),
                               runtime->view_epoch_signals_.size(),
                               cudaMemcpyHostToDevice));
        PG_TRY(runtime->device_group_view_,
               GpuBuffer<DeviceGroupView>::fromHost(device_index,
                                                    DeviceGroupView{}));
        PG_TRY(runtime->invocation_state_,
               GpuBuffer<InvocationState>::zeroed(device_index));
        PG_TRY(runtime->control_mailbox_,
               MappedHostBuffer<ControlMailbox>::create(device_index, 1,
                                                        cudaHostAllocPortable));
    }
    const CollectiveRuntimeBindings collective{
        .transfer_handle = transfer_service.deviceHandle(),
        .timeout_ticks = timeout_ticks,
        .group_view = runtime->device_group_view_.get(),
        .view_epoch_signals =
            static_cast<const uint64_t*>(runtime->view_epoch_signals_.addr()),
        .invocation_state = runtime->invocation_state_.get(),
        .control_mailbox = runtime->control_mailbox_.device(),
    };
    PG_TRY(runtime->dispatcher_, DeviceCollectiveDispatcher::create(
                                     transfer_service, workspace, device_index,
                                     self_rank, max_group_size, collective));
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
    DeviceGroupEndpoint endpoint{
        .view_epoch_signal = view_epoch_signals_.offset(),
        .view_epoch_signal_count = static_cast<uint32_t>(active_ranks_count_),
    };
    dispatcher_->fillLocalEndpoint(endpoint);
    return endpoint;
}

PGResult<void> DeviceCollectiveRuntime::useLocalOnly(const GroupView& view) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (shutdown_complete_) return {};
    PG_VALIDATE_STATE(!shutdown_requested_,
                      "device collective runtime is shutting down");
    if (installed_view_epoch_ == view.epoch && installed_local_only_) return {};
    PG_TRY(installGroupView(
        ResolvedGroupView::localOnly(self_rank_, workspace_.buffer().size(),
                                     view.collective_algorithm_choices)));
    installed_view_epoch_ = view.epoch;
    installed_local_only_ = true;
    return {};
}

PGResult<void> DeviceCollectiveRuntime::applyGroupView(const GroupView& view) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (shutdown_complete_) return {};
    PG_VALIDATE_STATE(!shutdown_requested_,
                      "device collective runtime is shutting down");
    PG_ASSERT(view.epoch != kInvalidViewEpoch,
              "device collective View epoch uses the reserved invalid value");
    if (installed_view_epoch_ == view.epoch && !installed_local_only_)
        return {};
    PG_TRY(auto collective_view, resolveGroupView(view));
    PG_TRY(installGroupView(collective_view));
    installed_view_epoch_ = view.epoch;
    installed_local_only_ = false;
    return {};
}

PGResult<void> DeviceCollectiveRuntime::enableRecovery(
    DeviceCollectiveRecoveryWorker& worker, FailureRecoveryCallback callback) {
    std::lock_guard<std::mutex> lock(mutex_);
    failure_recovery_callback_ = std::move(callback);
    auto added = worker.addMailbox(control_mailbox_.host(),
                                   [this](const RecoveryRequest& request) {
                                       return handleFailure(request);
                                   });
    if (!added.has_value()) {
        failure_recovery_callback_ = {};
        return makePGError(std::move(added).error());
    }
    recovery_worker_ = &worker;
    return {};
}

PGResult<void> DeviceCollectiveRuntime::enqueue(
    const DeviceCollectiveRequest& request, cudaStream_t user_stream_handle) {
    std::unique_lock<std::mutex> enqueue_lock(mutex_);

    PG_VALIDATE_STATE(!shutdown_requested_,
                      "device collective runtime is shutting down");
    PG_TRY(auto algorithm, dispatcher_->selectAlgorithm(request));

    auto user_stream = GpuStream::borrow(user_stream_handle, device_index_);
    PG_TRY(auto capture, user_stream.captureInfo());
    PG_TRY(attachGraphUse(capture));
    PG_TRY(auto order_lease, strong_stream_.acquire(capture));
    const auto& order_stream = order_lease.stream();

    auto submitted = [&]() -> PGResult<void> {
        PG_TRY(handoff_event_.record(order_stream));
        PG_TRY(user_stream.waitEvent(handoff_event_));

        auto launched = algorithm->enqueue(request, user_stream.get());

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
        recovery_worker->removeMailbox(control_mailbox_.host());
    }

    {
        std::lock_guard<std::mutex> lock(mutex_);
        failure_recovery_callback_ = {};
        dispatcher_.reset();
        releaseState();
        shutdown_complete_ = true;
    }
    return {};
}

PGResult<ResolvedGroupView> DeviceCollectiveRuntime::resolveGroupView(
    const GroupView& view) const {
    const size_t max_group_size = active_ranks_count_;
    PG_VALIDATE_STATE(view.rank_order.size() <= max_group_size,
                      "Device collective rank order exceeds group capacity");
    ResolvedGroupView resolved{
        .epoch = view.epoch,
        .buffer_size = workspace_.buffer().size(),
        .collective_algorithm_choices = view.collective_algorithm_choices,
    };
    resolved.participants.reserve(view.rank_order.size());

    // Determine membership and local participation before touching endpoints.
    // Even an inactive local rank needs the full list for the active-rank
    // mirror.
    for (InGroupRank in_group_rank = 0;
         static_cast<size_t>(in_group_rank) < view.rank_order.size();
         ++in_group_rank) {
        const auto global_rank = view.rank_order[in_group_rank];
        PG_VALIDATE_STATE(global_rank >= 0 && static_cast<size_t>(global_rank) <
                                                  view.members.size(),
                          "Device collective member rank is out of range");
        if (!view.members[global_rank].isActive()) continue;
        if (in_group_rank == self_rank_) {
            resolved.self_active_index =
                static_cast<int32_t>(resolved.participants.size());
        }
        resolved.participants.push_back(ResolvedParticipant{
            .global_rank = global_rank,
            .in_group_rank = in_group_rank,
        });
    }

    // An inactive local rank only needs its Plans invalidated. Return before
    // endpoint validation so a missing peer endpoint cannot prevent that
    // update. Only active local ranks need the bindings below.
    if (resolved.self_active_index < 0) return resolved;

    for (auto& peer : resolved.participants) {
        const auto& member = view.members[peer.global_rank];
        PG_VALIDATE_STATE(member.endpoint,
                          "Active device collective peer has no endpoint");
        const auto& endpoint = member.endpoint->device_collective;
        const uint64_t signal_bytes =
            uint64_t{endpoint.view_epoch_signal_count} * sizeof(uint64_t);
        PG_VALIDATE_STATE(
            endpoint.view_epoch_signal_count >= max_group_size &&
                endpoint.view_epoch_signal % alignof(uint64_t) == 0 &&
                !addOverflows(endpoint.view_epoch_signal, signal_bytes),
            "Device collective peer View-epoch endpoint is invalid");
        peer.protocols = endpoint.protocols;
        peer.algorithms = endpoint.algorithms;
        PG_TRY(peer.workspace, workspace_.endpoint(peer.global_rank));
        // All participants use the same workspace prefix despite independent
        // DTS offsets and possibly different allocation capacities.
        resolved.buffer_size =
            std::min(resolved.buffer_size, peer.workspace.buffer_size);
        peer.view_epoch_signal_offset =
            endpoint.view_epoch_signal +
            static_cast<uint64_t>(self_rank_) * sizeof(uint64_t);
    }
    return resolved;
}

PGResult<void> DeviceCollectiveRuntime::installGroupView(
    const ResolvedGroupView& view) {
    ControlUpdateBuilder builder;
    const auto group_view = view.asDeviceView();
    PG_TRY(builder.copyBytes(device_group_view_.get(), &group_view,
                             sizeof(group_view)));
    PG_TRY(dispatcher_->installGroupView(view, builder));
    // Save the mirror source for recovery control updates.
    active_ranks_source_.fill(0);
    for (const auto& peer : view.participants) {
        active_ranks_source_[peer.in_group_rank] = 1;
    }
    const auto handle = control_mailbox_.host()->control_update_slot.publish(
        builder.controlUpdate());
    published_view_update_ = PublishedViewUpdate{
        .update = builder.controlUpdate(),
        .handle = handle,
    };
    return {};
}

bool DeviceCollectiveRuntime::hasPendingRecovery() const noexcept {
    return control_mailbox_.host()->recovery.hasPendingRequest();
}

PGResult<void> DeviceCollectiveRuntime::handleFailure(
    const RecoveryRequest& request) {
    const auto& report = request.request;
    const auto& failure = report.result;
    const auto failed_rank = failure.failed_rank;
    if (report.failed_hint_address != 0) {
        auto* hint = reinterpret_cast<int32_t*>(report.failed_hint_address);
        hint[failed_rank] = 1;
    }

    const bool communication_failure =
        failure.failure_type == CollectiveFailureType::Communication;
    bool recovery_failed = false;
    if (communication_failure) {
        // The callback can install views through the agent executor, so it
        // must run without mutex_.
        const auto recovery_result = failure_recovery_callback_(failed_rank);
        recovery_failed = !recovery_result.has_value();
        if (recovery_failed) {
            LOG(ERROR) << "device collective recovery failed; falling "
                          "back to Plan invalidation: "
                       << recovery_result.error().message;
        }
    }

    std::lock_guard<std::mutex> lock(mutex_);
    ControlUpdateBuilder builder;
    if (!recovery_failed && published_view_update_ &&
        !published_view_update_->handle.poll()) {
        // The GPU waits for the recovery reply before claiming control
        // updates. Append recovery work to this pending view update.
        builder = ControlUpdateBuilder(published_view_update_->update);
    } else if (communication_failure) {
        // A failed callback or no new view (e.g. autoSyncOnFailure disabled)
        // leaves the failed plans unavailable until another view is installed.
        PG_TRY(dispatcher_->invalidatePlans(builder));
    }

    // The caller-owned mirror normally follows the direct cudaMemcpyAsync
    // path. Parked recovery includes it in the same update because
    // synchronizing a separate CUDA copy while the last channel CTA is waiting
    // may deadlock recovery.
    if (active_ranks_mirror_) {
        PG_TRY(builder.copyBytes(
            active_ranks_mirror_, active_ranks_source_.data(),
            active_ranks_count_ * sizeof(active_ranks_source_.front())));
    }
    // Publish and reply while holding the mutex so later view installations
    // cannot interleave. Pinned keeps this update available until the waiting
    // GPU applies it.
    control_mailbox_.host()->control_update_slot.publish(
        builder.controlUpdate(),
        /* pinned = */ true);
    published_view_update_.reset();
    request.handle.reply({});
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

void DeviceCollectiveRuntime::releaseState() noexcept {
    device_group_view_.reset();
    invocation_state_.reset();
    control_mailbox_.reset();
}

}  // namespace mooncake
