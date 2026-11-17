#include "device_comm/device_collective/device_collective.h"

#include <algorithm>
#include <atomic>
#include <limits>
#include <new>
#include <utility>

#include <glog/logging.h>

#include "device_comm/device_utils/d2h_request_slot.h"
#include "device_comm/device_utils/h2d_request_slot.h"
#include "device_comm/device_collective/algorithms/ring/ring_all_reduce.h"
#include "device_comm/device_collective/algorithms/oneshot/one_shot_all_reduce.h"
#include "device_comm/device_collective/device_control_update.h"
#include "device_comm/device_collective/device_collective_workspace.h"
#include "device_comm/device_collective/protocols/simple/simple_resources.h"
#include "device_comm/device_collective/protocols/ll/ll_resources.h"
#include "device_comm/device_collective/strong_stream.h"
#include "gpu_runtime.h"
#include "pg_utils.h"

namespace mooncake {

// One committed View's binding and Plan images. Protocol progress lives on the
// device. LL progress survives publication; its reset requires peer quiescence.
struct DeviceCollectiveRuntime::Configuration {
    uint64_t epoch = kInvalidViewEpoch;
    SimpleBindings simple;
    LLBindings ll;
    RingAllReducePlan ring;
    OneShotAllReducePlan one_shot;
    std::array<int32_t, kMaxNumRanks> active_ranks{};
};

namespace {

// Simple (3), LL bindings (1), two Plans, and the recovery mirror.
static_assert(kMaxDeviceControlUpdateOperations >= 7);

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
      configuration_(std::make_unique<Configuration>()),
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
    PG_TRY(runtime->simple_, SimpleResources::create(
                                 transfer_service, self_rank, max_group_size));
    PG_TRY(runtime->configuration_->simple,
           runtime->simple_->bindGroupView(ResolvedGroupView{}));
    const CollectiveRuntimeBindings collective{
        .transfer_handle = transfer_service.deviceHandle(),
        .timeout_ticks = timeout_ticks,
        .view_epoch_signals =
            static_cast<const uint64_t*>(runtime->view_epoch_signals_.addr()),
        .invocation_state = runtime->invocation_state_,
        .control_mailbox = device_control_mailbox,
    };
    PG_TRY(runtime->ring_all_reduce_,
           RingAllReduceAlgorithm::create(device_index, collective, workspace,
                                          *runtime->simple_));
    const auto* p2p = transfer_service.findRoute<P2pRoute>();
    if (max_group_size > 1 && p2p && !p2p->nativeAtomicPeerUuids().empty()) {
        PG_TRY(runtime->ll_, LLResources::create(transfer_service, self_rank,
                                                 max_group_size));
        PG_TRY(runtime->configuration_->ll,
               runtime->ll_->bindGroupView(ResolvedGroupView{}));
        PG_TRY(
            runtime->one_shot_all_reduce_,
            OneShotAllReduceAlgorithm::create(transfer_service, max_group_size,
                                              collective, *runtime->ll_));
    }
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
        .simple = simple_->localEndpoint(),
        .ll = ll_ ? std::optional{ll_->localEndpoint()} : std::nullopt,
        .one_shot = one_shot_all_reduce_
                        ? std::optional{one_shot_all_reduce_->localEndpoint()}
                        : std::nullopt,
    };
}

PGResult<void> DeviceCollectiveRuntime::useLocalOnly(uint64_t view_epoch) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (shutdown_complete_) return {};
    PG_VALIDATE_STATE(!shutdown_requested_,
                      "device collective runtime is shutting down");
    PG_ASSERT(view_epoch != kInvalidViewEpoch,
              "device collective View epoch uses the reserved invalid value");
    if (configuration_->epoch == view_epoch) return {};

    ResolvedGroupView view{
        .epoch = view_epoch,
        .self_active_index = 0,
        .buffer_size = workspace_.buffer().size(),
    };
    view.participants.push_back(
        ResolvedParticipant{.in_group_rank = self_rank_});
    return installGroupView(view, one_shot_all_reduce_ != nullptr);
}

PGResult<void> DeviceCollectiveRuntime::applyGroupView(const GroupView& view) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (shutdown_complete_) return {};
    PG_VALIDATE_STATE(!shutdown_requested_,
                      "device collective runtime is shutting down");
    PG_ASSERT(view.epoch != kInvalidViewEpoch,
              "device collective View epoch uses the reserved invalid value");
    // A duplicate group view must not reset protocol progress again.
    if (configuration_->epoch == view.epoch) return {};

    // Allocate the choice snapshot before committing any data-plane state.
    auto choices = view.all_reduce_algorithm_choices;
    PG_TRY(auto collective_view, resolveGroupView(view));
    const bool one_shot_selected = std::any_of(
        view.all_reduce_algorithm_choices.begin(),
        view.all_reduce_algorithm_choices.end(), [](const auto& choice) {
            return choice.algorithm == DeviceAllReduceAlgorithm::OneShot;
        });
    PG_TRY(installGroupView(collective_view, one_shot_selected));
    all_reduce_algorithm_choices_ = std::move(choices);
    return {};
}

PGResult<void> DeviceCollectiveRuntime::enableRecovery(
    DeviceCollectiveRecoveryWorker& worker, FailureRecoveryCallback callback) {
    std::lock_guard<std::mutex> lock(mutex_);
    failure_recovery_callback_ = std::move(callback);
    auto added = worker.addMailbox(
        control_mailbox_, [this](const CollectiveFailureReport& failure) {
            return prepareFailureResume(failure);
        });
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
    const bool use_one_shot = allReduceAlgorithm(buffer_size, datatype, op) ==
                              DeviceAllReduceAlgorithm::OneShot;
    PG_VALIDATE_STATE(
        (use_one_shot ? configuration_->one_shot.status
                      : configuration_->ring.status) == DevicePlanStatus::Ready,
        "selected device AllReduce Plan is not ready");
    const AllReduceRequest request{
        .send_buffer = send_buffer,
        .recv_buffer = recv_buffer,
        .count = static_cast<uint64_t>(count),
        .datatype = datatype,
        .op = op,
        .failed_ranks_hint = failed_ranks_hint,
    };

    auto user_stream = GpuStream::borrow(user_stream_handle, device_index_);
    PG_TRY(auto capture, user_stream.captureInfo());
    PG_TRY(attachGraphUse(capture));
    PG_TRY(auto order_lease, strong_stream_.acquire(capture));
    const auto& order_stream = order_lease.stream();

    auto submitted = [&]() -> PGResult<void> {
        PG_TRY(handoff_event_.record(order_stream));
        PG_TRY(user_stream.waitEvent(handoff_event_));

        PGResult<void> launched;
        if (use_one_shot) {
            launched =
                one_shot_all_reduce_->enqueue(request, user_stream.get());
        } else {
            launched = ring_all_reduce_->enqueue(request, user_stream.get());
        }

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

    {
        std::lock_guard<std::mutex> lock(mutex_);
        failure_recovery_callback_ = {};
        ring_all_reduce_.reset();
        one_shot_all_reduce_.reset();
        simple_.reset();
        ll_.reset();
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

    // An inactive local rank only needs its Plans invalidated and protocol
    // state reset. Return before endpoint validation so a missing peer endpoint
    // cannot prevent that update. Only active local ranks need the bindings
    // below.
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
        peer.endpoint = endpoint;
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
    const ResolvedGroupView& view, bool require_one_shot_plan) {
    Configuration next;
    next.epoch = view.epoch;
    PG_TRY(next.simple, simple_->bindGroupView(view));
    PG_TRY(next.ring, ring_all_reduce_->buildPlan(view));
    if (require_one_shot_plan && view.self_active_index >= 0) {
        PG_VALIDATE_STATE(ll_ && one_shot_all_reduce_,
                          "Selected one-shot resources are unavailable");
        PG_TRY(next.ll, ll_->bindGroupView(view));
        PG_TRY(next.one_shot, one_shot_all_reduce_->buildPlan(view));
    } else if (ll_) {
        PG_TRY(next.ll, ll_->bindGroupView(ResolvedGroupView{}));
    }
    for (const auto& peer : view.participants)
        next.active_ranks[peer.in_group_rank] = 1;
    PG_TRY(publishControlState(next, /* pinned = */ false,
                               /* include_active_ranks_mirror = */ false));
    return {};
}

PGResult<void> DeviceCollectiveRuntime::publishControlState(
    const Configuration& configuration, bool pinned,
    bool include_active_ranks_mirror) {
    static_assert(sizeof(Configuration) <= kDeviceControlUpdatePayloadBytes);
    ControlUpdateBuilder builder;
    // The caller-owned mirror normally follows the direct cudaMemcpyAsync
    // path. Parked recovery includes it in the same update because
    // synchronizing a separate CUDA copy while the last channel CTA is waiting
    // may deadlock recovery.
    if (include_active_ranks_mirror && active_ranks_mirror_) {
        PG_TRY(builder.copyBytes(
            active_ranks_mirror_, configuration.active_ranks.data(),
            active_ranks_count_ * sizeof(configuration.active_ranks.front())));
    }

    PG_TRY(simple_->appendUpdate(builder, configuration.simple));
    if (ll_) PG_TRY(ll_->appendUpdate(builder, configuration.ll));
    PG_TRY(ring_all_reduce_->appendPlanUpdate(builder, configuration.ring));
    if (one_shot_all_reduce_) {
        PG_TRY(one_shot_all_reduce_->appendPlanUpdate(builder,
                                                      configuration.one_shot));
    }
    *configuration_ = configuration;
    control_mailbox_->control_update_slot.publish(builder.controlUpdate(),
                                                  pinned);
    return {};
}

bool DeviceCollectiveRuntime::hasPendingRecovery() const noexcept {
    return control_mailbox_->recovery.hasPendingRequest();
}

PGResult<void> DeviceCollectiveRuntime::prepareFailureResume(
    const CollectiveFailureReport& failure) {
    const auto failed_rank = failure.failed_rank;
    if (failure.failed_hint_address != 0) {
        auto* hint = reinterpret_cast<int32_t*>(failure.failed_hint_address);
        hint[failed_rank] = 1;
    }

    auto recovery_result = failure_recovery_callback_(failed_rank);

    std::lock_guard<std::mutex> lock(mutex_);
    // On success, publish the host plans left by the callback together with
    // each protocol's progress updates. The last channel CTA of the failed
    // invocation is the only consumer of this pinned update.
    //
    // On failure, commit unavailable Plans with the pinned update so that CTA
    // can exit without leaving stale Plans usable.
    if (recovery_result.has_value()) {
        PG_TRY(publishControlState(*configuration_, /* pinned = */ true,
                                   /* include_active_ranks_mirror = */ true));
    } else {
        LOG(ERROR) << "device collective recovery callback failed; falling "
                      "back to Plan invalidation: "
                   << recovery_result.error().message;
        auto invalid = *configuration_;
        invalid.epoch = kInvalidViewEpoch;
        invalid.ring = RingAllReducePlan{};
        invalid.one_shot = OneShotAllReducePlan{};
        PG_TRY(publishControlState(invalid, /* pinned = */ true,
                                   /* include_active_ranks_mirror = */ true));
    }
    return {};
}

DeviceAllReduceAlgorithm DeviceCollectiveRuntime::allReduceAlgorithm(
    size_t bytes, DataType datatype, ReduceOp op) const noexcept {
    if (isDeviceAllReduceCombinationSupported(datatype, op)) {
        for (const auto& choice : all_reduce_algorithm_choices_) {
            if (bytes <= choice.max_bytes) return choice.algorithm;
        }
    }
    return DeviceAllReduceAlgorithm::Ring;
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

}  // namespace mooncake
