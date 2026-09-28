// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0

#include "tent/transport/ub/workers.h"

#include <algorithm>
#include <chrono>
#include <limits>
#include <unordered_set>
#include <utility>

#include <glog/logging.h>

#include "tent/runtime/platform.h"
#include "tent/transport/ub/endpoint.h"

namespace mooncake::tent::ub {
namespace {

uint64_t deadlineAfter(uint64_t now_ns, uint64_t timeout_ns) {
    return timeout_ns > std::numeric_limits<uint64_t>::max() - now_ns
               ? std::numeric_limits<uint64_t>::max()
               : now_ns + timeout_ns;
}

bool retryableStatus(const Status& status) {
    return status.IsDeviceNotFound() || status.IsNeedsRefreshCache() ||
           status.IsRpcServiceError() || status.IsInternalError() ||
           status.IsRdmaError();
}

}  // namespace

struct UbWorkers::Route {
    SegmentDescRef pin;
    BufferDesc* remote_buffer{nullptr};
    UbBufferMetadata metadata;
    std::vector<Topology::NicID> remote_devices;
};

struct UbWorkers::Inflight {
    uint64_t completion_token{0};
    PendingSlice pending;
    UbAttemptToken attempt;
    UbPostPath path;
    std::shared_ptr<UbEndpoint> endpoint;
    LocalSegmentPtr local_segment;
    RemoteSegmentPtr remote_segment;
    QuotaReservation quota;
    uint64_t posted_ns{0};
    uint64_t deadline_ns{0};
    std::atomic<bool> timed_out{false};
    std::atomic<bool> timeout_recorded{false};
    std::atomic<bool> resources_released{false};
};

UbWorkers::UbWorkers(std::shared_ptr<UrmaAdapter> adapter,
                     std::vector<UbContextPtr> contexts,
                     std::shared_ptr<Topology> local_topology,
                     SegmentManager* segment_manager, UbBufferManager* buffers,
                     RailMonitor* rail_monitor, QuotaManager* quota,
                     UbParams params, EndpointResolver endpoint_resolver,
                     EndpointRetirer endpoint_retirer)
    : adapter_(std::move(adapter)),
      contexts_(std::move(contexts)),
      local_topology_(std::move(local_topology)),
      segment_manager_(segment_manager),
      buffers_(buffers),
      rail_monitor_(rail_monitor),
      quota_(quota),
      params_(std::move(params)),
      endpoint_resolver_(std::move(endpoint_resolver)),
      endpoint_retirer_(std::move(endpoint_retirer)) {
    for (const auto& context : contexts_) {
        if (!context) continue;
        context_by_topology_id_[context->topologyId()] = context;
        for (const auto& jfc : context->jfcs()) {
            if (jfc) {
                all_jfcs_.push_back(jfc);
                context_by_jfc_[jfc.get()] = context;
            }
        }
    }
}

UbWorkers::~UbWorkers() { (void)stop(); }

Status UbWorkers::start() {
    bool expected = false;
    if (!accepting_.compare_exchange_strong(expected, true,
                                            std::memory_order_acq_rel)) {
        return Status::InvalidArgument("UB workers already started" LOC_MARK);
    }
    if (!adapter_ || contexts_.empty() || all_jfcs_.empty() ||
        !segment_manager_ || !buffers_ || !rail_monitor_ || !quota_ ||
        !endpoint_resolver_ || params_.worker_count == 0 ||
        params_.poller_count == 0) {
        accepting_.store(false, std::memory_order_release);
        return Status::InvalidArgument(
            "UB workers have incomplete dependencies" LOC_MARK);
    }

    posting_.store(true, std::memory_order_release);
    polling_.store(true, std::memory_order_release);
    timeout_scans_enabled_.store(true, std::memory_order_release);
    try {
        posting_threads_.reserve(params_.worker_count);
        for (uint32_t i = 0; i < params_.worker_count; ++i) {
            posting_threads_.emplace_back(&UbWorkers::postingLoop, this, i);
        }
        const size_t poller_count =
            std::min<size_t>(params_.poller_count, all_jfcs_.size());
        polling_threads_.reserve(poller_count);
        for (size_t i = 0; i < poller_count; ++i) {
            polling_threads_.emplace_back(&UbWorkers::pollingLoop, this, i);
        }
    } catch (const std::exception& error) {
        accepting_.store(false, std::memory_order_release);
        posting_.store(false, std::memory_order_release);
        polling_.store(false, std::memory_order_release);
        timeout_scans_enabled_.store(false, std::memory_order_release);
        queue_cv_.notify_all();
        for (auto& thread : posting_threads_) {
            if (thread.joinable()) thread.join();
        }
        for (auto& thread : polling_threads_) {
            if (thread.joinable()) thread.join();
        }
        posting_threads_.clear();
        polling_threads_.clear();
        return Status::InternalError(std::string("Cannot start UB workers: ") +
                                     error.what() + LOC_MARK);
    }
    return Status::OK();
}

Status UbWorkers::stop() {
    accepting_.store(false, std::memory_order_release);
    // Fence retry enqueue before draining the queues. A completion racing
    // with shutdown will turn its retry-pending slice into CANCELED instead of
    // leaving a new queue entry behind the drain pass.
    posting_.store(false, std::memory_order_release);
    timeout_scans_enabled_.store(false, std::memory_order_release);
    queue_cv_.notify_all();

    std::vector<PendingSlice> abandoned;
    {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        for (auto& queue : queues_) {
            while (!queue.empty()) {
                abandoned.push_back(std::move(queue.front()));
                queue.pop_front();
            }
        }
    }
    for (auto& pending : abandoned) {
        if (pending.slice) pending.slice->requestCancellation();
    }

    for (auto& thread : posting_threads_) {
        if (thread.joinable()) thread.join();
    }
    posting_threads_.clear();

    // Wait for a timeout scan that began before the flag change. scanTimeouts
    // rechecks under this mutex, so no new endpoint can enter the drain set
    // after this barrier and escape the shutdown snapshot.
    {
        std::lock_guard<std::mutex> lock(endpoint_drain_mutex_);
    }

    // Snapshot all native work after posting threads have joined; no new WR
    // can cross the adapter boundary beyond this point. Quiesce each endpoint
    // before stopping pollers. A failed fence leaves pollers, tokens, native
    // segments, quota, and endpoints intact so uninstall can be retried
    // safely; fixed sleeps are never treated as proof that DMA stopped.
    std::vector<std::shared_ptr<Inflight>> still_inflight;
    std::vector<std::shared_ptr<UbEndpoint>> endpoints_to_fence;
    {
        std::lock_guard<std::mutex> lock(inflight_mutex_);
        still_inflight.reserve(inflight_.size());
        for (const auto& [_, inflight] : inflight_) {
            still_inflight.push_back(inflight);
            if (inflight && inflight->endpoint) {
                endpoints_to_fence.push_back(inflight->endpoint);
            }
        }
        for (const auto& [_, endpoint] : draining_endpoints_) {
            if (endpoint) endpoints_to_fence.push_back(endpoint);
        }
    }
    for (const auto& inflight : still_inflight) {
        if (inflight && inflight->pending.slice) {
            inflight->pending.slice->requestCancellation();
        }
    }

    Status fence_error = Status::OK();
    std::unordered_set<UbEndpoint*> fenced;
    for (const auto& endpoint : endpoints_to_fence) {
        if (!endpoint || !fenced.insert(endpoint.get()).second) {
            continue;
        }
        std::vector<Completion> drained;
        auto status = endpoint->quiesce(params_.slice_timeout_ms, drained);
        for (const auto& completion : drained) {
            if (completion.token != 0) handleCompletion(completion);
        }
        if (!status.ok()) {
            rememberEndpointDrain(endpoint);
            if (fence_error.ok()) fence_error = status;
            continue;
        }
        forgetEndpointDrain(endpoint);
        if (endpoint_retirer_) endpoint_retirer_(endpoint);
    }
    if (!fence_error.ok()) {
        return fence_error;
    }
    {
        std::lock_guard<std::mutex> lock(inflight_mutex_);
        if (!draining_endpoints_.empty()) {
            return Status::InternalError(
                "UB endpoint drain set changed during shutdown" LOC_MARK);
        }
    }

    // Successful endpoint fences prove that any token still missing from the
    // JFC can no longer touch memory. Let pollers consume already-queued CRs,
    // then stop them and safely resolve any provider-lost token.
    if (!still_inflight.empty()) {
        std::unique_lock<std::mutex> lock(inflight_mutex_);
        (void)inflight_cv_.wait_for(lock, std::chrono::milliseconds(100),
                                    [this] { return inflight_.empty(); });
    }
    polling_.store(false, std::memory_order_release);
    for (auto& thread : polling_threads_) {
        if (thread.joinable()) thread.join();
    }
    polling_threads_.clear();

    still_inflight.clear();
    {
        std::lock_guard<std::mutex> lock(inflight_mutex_);
        for (auto& [_, inflight] : inflight_) {
            still_inflight.push_back(std::move(inflight));
        }
        inflight_.clear();
    }
    for (const auto& inflight : still_inflight) {
        releaseInflight(inflight);
        (void)inflight->pending.slice->resolveAttempt(inflight->attempt, FAILED,
                                                      0, false);
    }
    inflight_cv_.notify_all();
    return Status::OK();
}

Status UbWorkers::submit(const UbTask::Ptr& task, uint64_t device_mask) {
    if (!task) {
        return Status::InvalidArgument("Cannot submit a null UB task" LOC_MARK);
    }
    if (!accepting_.load(std::memory_order_acquire)) {
        return Status::InvalidArgument(
            "UB workers are not accepting work" LOC_MARK);
    }
    const auto snapshot = task->snapshot();
    if (!snapshot.sealed) {
        return Status::InvalidArgument(
            "UB task must be sealed before submission" LOC_MARK);
    }
    const int priority =
        std::clamp(task->request().priority, static_cast<int>(PRIO_HIGH),
                   static_cast<int>(PRIO_LOW));

    std::vector<PendingSlice> pending;
    for (const auto& slice : task->slices()) {
        if (slice && slice->markQueued()) {
            pending.push_back(PendingSlice{task, slice, device_mask, priority,
                                           task->request().target_id,
                                           task->request().opcode});
        }
    }
    bool stopped_during_submit = false;
    {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        if (!accepting_.load(std::memory_order_relaxed)) {
            stopped_during_submit = true;
        } else {
            for (auto& item : pending) {
                queues_[item.priority].push_back(std::move(item));
            }
        }
    }
    if (stopped_during_submit) {
        for (auto& item : pending) item.slice->requestCancellation();
        return Status::InvalidArgument(
            "UB workers stopped during submission" LOC_MARK);
    }
    if (!pending.empty()) queue_cv_.notify_all();
    return Status::OK();
}

Status UbWorkers::cancel(const UbTask::Ptr& task) {
    if (!task) {
        return Status::InvalidArgument("Cannot cancel a null UB task" LOC_MARK);
    }
    (void)task->requestCancellation();
    queue_cv_.notify_all();
    return Status::OK();
}

size_t UbWorkers::queuedCount() const {
    std::lock_guard<std::mutex> lock(queue_mutex_);
    size_t count = 0;
    for (const auto& queue : queues_) count += queue.size();
    return count;
}

size_t UbWorkers::inflightCount() const {
    std::lock_guard<std::mutex> lock(inflight_mutex_);
    return inflight_.size();
}

bool UbWorkers::popPending(PendingSlice& pending) {
    std::unique_lock<std::mutex> lock(queue_mutex_);
    queue_cv_.wait(lock, [this] {
        if (!posting_.load(std::memory_order_acquire)) return true;
        for (const auto& queue : queues_) {
            if (!queue.empty()) return true;
        }
        return false;
    });
    if (!posting_.load(std::memory_order_acquire)) return false;
    for (auto& queue : queues_) {
        if (!queue.empty()) {
            pending = std::move(queue.front());
            queue.pop_front();
            return true;
        }
    }
    return false;
}

void UbWorkers::postingLoop(size_t worker_index) {
    while (posting_.load(std::memory_order_acquire)) {
        PendingSlice pending;
        if (!popPending(pending)) continue;
        if (pending.slice) processPending(pending, worker_index);
    }
}

void UbWorkers::pollingLoop(size_t poller_index) {
    const size_t poller_count = std::max<size_t>(
        1, std::min<size_t>(params_.poller_count, all_jfcs_.size()));
    uint64_t last_timeout_scan = 0;
    while (polling_.load(std::memory_order_acquire)) {
        bool progressed = false;
        for (size_t index = poller_index; index < all_jfcs_.size();
             index += poller_count) {
            std::vector<Completion> completions;
            auto status = all_jfcs_[index]->poll(64, completions);
            if (!status.ok()) {
                auto context = context_by_jfc_.find(all_jfcs_[index].get());
                if (context != context_by_jfc_.end() && context->second) {
                    (void)context->second->markUnavailable();
                }
                LOG_EVERY_N(WARNING, 1000)
                    << "UB JFC poll failed: " << status.ToString();
                continue;
            }
            progressed = progressed || !completions.empty();
            for (const auto& completion : completions) {
                if (completion.token != 0) handleCompletion(completion);
            }
        }
        const uint64_t now = steadyNowNs();
        if (timeout_scans_enabled_.load(std::memory_order_acquire) &&
            now - last_timeout_scan >= 1'000'000ULL) {
            scanTimeouts();
            last_timeout_scan = now;
        }
        if (!progressed)
            std::this_thread::sleep_for(std::chrono::microseconds(20));
    }
}

void UbWorkers::enqueueRetry(const PendingSlice& pending) {
    if (!pending.slice) return;
    if (!posting_.load(std::memory_order_acquire) ||
        !pending.slice->markQueued()) {
        pending.slice->requestCancellation();
        return;
    }
    bool stopped = false;
    {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        if (!posting_.load(std::memory_order_relaxed)) {
            stopped = true;
        } else {
            queues_[std::clamp(pending.priority, static_cast<int>(PRIO_HIGH),
                               static_cast<int>(PRIO_LOW))]
                .push_back(pending);
        }
    }
    if (stopped) {
        pending.slice->requestCancellation();
        return;
    }
    queue_cv_.notify_one();
}

void UbWorkers::deferPending(const PendingSlice& pending) {
    if (!pending.slice || pending.slice->cancellationRequested() ||
        !posting_.load(std::memory_order_acquire)) {
        if (pending.slice) pending.slice->requestCancellation();
        return;
    }
    // Capacity pressure is not a transfer failure. A short bounded backoff
    // prevents a posting lane from exhausting CPU while completions release
    // quota; releaseInflight() also wakes the queue condition variable.
    {
        std::unique_lock<std::mutex> lock(queue_mutex_);
        (void)queue_cv_.wait_for(lock, std::chrono::microseconds(50), [&] {
            return !posting_.load(std::memory_order_relaxed) ||
                   pending.slice->cancellationRequested();
        });
        if (posting_.load(std::memory_order_relaxed) &&
            !pending.slice->cancellationRequested()) {
            queues_[std::clamp(pending.priority, static_cast<int>(PRIO_HIGH),
                               static_cast<int>(PRIO_LOW))]
                .push_back(pending);
            lock.unlock();
            queue_cv_.notify_one();
            return;
        }
    }
    pending.slice->requestCancellation();
}

Status UbWorkers::buildRoute(const PendingSlice& pending, Route& route) {
    return segment_manager_->withCachedSegment(
        pending.target_id, route.pin, [&](SegmentDesc* segment) -> Status {
            if (!segment || segment->type != SegmentType::Memory) {
                return Status::InvalidMetadataType(
                    "UB target is not a memory segment" LOC_MARK);
            }
            auto* buffer =
                segment->findBuffer(pending.slice->spec().remote_address,
                                    pending.slice->spec().length);
            if (!buffer) {
                return Status::NeedsRefreshCache(
                    "UB target range is not registered" LOC_MARK);
            }
            auto attr = buffer->transport_attrs.find(TransportType::UB);
            if (attr == buffer->transport_attrs.end()) {
                return Status::NeedsRefreshCache(
                    "UB target buffer has no transport metadata" LOC_MARK);
            }
            UbBufferMetadata metadata;
            auto status = decodeBufferMetadata(attr->second, metadata);
            if (!status.ok()) return status;
            route.remote_buffer = buffer;
            route.metadata = std::move(metadata);
            route.remote_devices = orderedRemoteDevices(*segment, *buffer);
            if (route.remote_devices.empty()) {
                return Status::DeviceNotFound(
                    "UB target has no active advertised device" LOC_MARK);
            }
            return Status::OK();
        });
}

std::vector<Topology::NicID> UbWorkers::orderedLocalDevices(
    const PendingSlice& pending) const {
    std::vector<Topology::NicID> ordered;
    std::unordered_set<Topology::NicID> seen;
    std::unordered_map<Topology::NicID, size_t> device_rank;
    std::string location = kWildcardLocation;
    auto locations = Platform::getLoader().getLocation(
        pending.slice->spec().local_address, 1, true);
    if (!locations.empty()) location = locations.front().location;

    auto append = [&](Topology::NicID id) {
        auto found = context_by_topology_id_.find(id);
        if (found == context_by_topology_id_.end() || !found->second ||
            !found->second->active()) {
            return;
        }
        const bool allowed = pending.device_mask == ~0ULL ||
                             (id >= 0 && id < 64 &&
                              (pending.device_mask & (uint64_t{1} << id)) != 0);
        if (allowed && seen.insert(id).second) ordered.push_back(id);
    };
    if (local_topology_) {
        if (const auto* memory = local_topology_->getMemEntry(location)) {
            for (size_t rank = 0; rank < Topology::DevicePriorityRanks;
                 ++rank) {
                for (auto id : memory->device_list[rank]) {
                    auto [it, inserted] = device_rank.emplace(id, rank);
                    if (!inserted) it->second = std::min(it->second, rank);
                    append(id);
                }
            }
        }
    }
    for (const auto& context : contexts_) {
        if (context) {
            device_rank.try_emplace(context->topologyId(),
                                    Topology::DevicePriorityRanks);
            append(context->topologyId());
        }
    }
    std::stable_sort(ordered.begin(), ordered.end(), [&](auto lhs, auto rhs) {
        const auto lhs_rank = device_rank.at(lhs);
        const auto rhs_rank = device_rank.at(rhs);
        if (lhs_rank != rhs_rank) return lhs_rank < rhs_rank;
        return context_by_topology_id_.at(lhs)->inflightBytes() <
               context_by_topology_id_.at(rhs)->inflightBytes();
    });
    return ordered;
}

std::vector<Topology::NicID> UbWorkers::orderedRemoteDevices(
    const SegmentDesc& segment, const BufferDesc& buffer) {
    std::vector<Topology::NicID> ordered;
    auto attr = buffer.transport_attrs.find(TransportType::UB);
    if (attr == buffer.transport_attrs.end()) return ordered;
    UbBufferMetadata metadata;
    if (!decodeBufferMetadata(attr->second, metadata).ok()) return ordered;
    std::unordered_set<Topology::NicID> advertised;
    for (const auto& item : metadata.segments) {
        advertised.insert(item.topology_id);
    }
    std::unordered_set<Topology::NicID> seen;
    const auto& topology = segment.getMemory().topology;
    auto append = [&](Topology::NicID id) {
        const auto* nic = topology.getNicEntry(id);
        if (nic && nic->type == Topology::NIC_UB && advertised.count(id) &&
            seen.insert(id).second) {
            ordered.push_back(id);
        }
    };
    if (const auto* memory = topology.getMemEntry(buffer.location)) {
        for (size_t rank = 0; rank < Topology::DevicePriorityRanks; ++rank) {
            for (auto id : memory->device_list[rank]) append(id);
        }
    }
    for (const auto& item : metadata.segments) append(item.topology_id);
    return ordered;
}

Status UbWorkers::chooseAndResolveEndpoint(
    const PendingSlice& pending, Route& route,
    std::shared_ptr<UbEndpoint>& endpoint, UbPostPath& path) {
    const auto local_devices = orderedLocalDevices(pending);
    if (local_devices.empty() || route.remote_devices.empty()) {
        return Status::DeviceNotFound("No usable UB posting path" LOC_MARK);
    }
    const auto snapshot = pending.slice->snapshot();
    const size_t combinations =
        local_devices.size() * route.remote_devices.size();
    const size_t start =
        combinations == 0 ? 0 : snapshot.retry_count % combinations;
    Status first_error = Status::DeviceNotFound(
        "No ready UB endpoint for any posting path" LOC_MARK);
    for (size_t candidate = 0; candidate < combinations; ++candidate) {
        const size_t flat = (start + candidate) % combinations;
        const auto local_id = local_devices[flat / route.remote_devices.size()];
        const auto remote_id =
            route.remote_devices[flat % route.remote_devices.size()];
        auto context = context_by_topology_id_.at(local_id);
        EndpointResolveRequest request{context, pending.target_id,
                                       route.pin.get(), remote_id,
                                       route.metadata.generation};
        std::shared_ptr<UbEndpoint> candidate_endpoint;
        auto status = endpoint_resolver_(request, candidate_endpoint);
        if (!status.ok() || !candidate_endpoint ||
            !candidate_endpoint->ready()) {
            if (first_error.IsDeviceNotFound() && !status.ok())
                first_error = status;
            continue;
        }
        UbPostPath candidate_path{local_id, pending.target_id, remote_id,
                                  candidate_endpoint->generation()};
        if (!rail_monitor_->available(candidate_path)) continue;
        endpoint = std::move(candidate_endpoint);
        path = candidate_path;
        return Status::OK();
    }
    return first_error;
}

void UbWorkers::processPending(const PendingSlice& pending,
                               size_t worker_index) {
    if (pending.slice->cancellationRequested()) {
        pending.slice->requestCancellation();
        return;
    }
    Route route;
    auto status = buildRoute(pending, route);
    if (!status.ok()) {
        const auto resolution = pending.slice->resolveBeforePost(
            FAILED, 0, retryableStatus(status));
        if (resolution == UbAttemptResolution::kRetryScheduled) {
            enqueueRetry(pending);
        }
        return;
    }

    std::shared_ptr<UbEndpoint> endpoint;
    UbPostPath path;
    status = chooseAndResolveEndpoint(pending, route, endpoint, path);
    if (!status.ok()) {
        const auto resolution = pending.slice->resolveBeforePost(
            FAILED, 0, retryableStatus(status));
        if (resolution == UbAttemptResolution::kRetryScheduled) {
            enqueueRetry(pending);
        }
        return;
    }

    LocalSegmentRef local;
    status = buffers_->findLocal(
        reinterpret_cast<uint64_t>(pending.slice->spec().local_address),
        pending.slice->spec().length, path.local_topology_id, local);
    if (!status.ok()) {
        (void)pending.slice->resolveBeforePost(FAILED, 0, false);
        return;
    }
    ImportedSegmentRef remote;
    status = buffers_->importRemote(pending.target_id, path.local_topology_id,
                                    path.remote_device_id, *route.remote_buffer,
                                    pending.opcode,
                                    pending.slice->spec().remote_address,
                                    pending.slice->spec().length, remote);
    if (!status.ok()) {
        const bool retryable = status.IsNeedsRefreshCache();
        if (retryable && pending.target_id != LOCAL_SEGMENT_ID) {
            (void)segment_manager_->invalidateRemote(pending.target_id);
        }
        const auto resolution =
            pending.slice->resolveBeforePost(FAILED, 0, retryable);
        if (resolution == UbAttemptResolution::kRetryScheduled) {
            enqueueRetry(pending);
        }
        return;
    }

    auto reservation = quota_->tryAcquire(path, pending.slice->spec().length);
    if (!reservation) {
        deferPending(pending);
        return;
    }
    if (!endpoint->tryAcquireOutstanding(pending.slice->spec().length)) {
        (void)quota_->release(*reservation);
        deferPending(pending);
        return;
    }

    auto attempt = pending.slice->beginAttempt(path);
    if (!attempt) {
        endpoint->releaseOutstanding(pending.slice->spec().length);
        (void)quota_->release(*reservation);
        return;
    }

    const uint64_t completion_token = nextCompletionToken();
    auto inflight = std::make_shared<Inflight>();
    inflight->completion_token = completion_token;
    inflight->pending = pending;
    inflight->attempt = *attempt;
    inflight->path = path;
    inflight->endpoint = endpoint;
    inflight->local_segment = local.segment;
    inflight->remote_segment = remote.segment;
    inflight->quota = *reservation;
    inflight->posted_ns = steadyNowNs();
    inflight->deadline_ns = deadlineAfter(
        inflight->posted_ns,
        static_cast<uint64_t>(params_.slice_timeout_ms) * 1'000'000ULL);

    if (!pending.slice->tryCommitPost(*attempt, inflight->posted_ns)) {
        endpoint->releaseOutstanding(pending.slice->spec().length);
        (void)quota_->release(*reservation);
        return;
    }
    {
        std::lock_guard<std::mutex> lock(inflight_mutex_);
        inflight_.emplace(completion_token, inflight);
    }

    WorkRequest work;
    work.operation =
        pending.opcode == Request::READ ? Operation::READ : Operation::WRITE;
    work.local_address =
        reinterpret_cast<uint64_t>(pending.slice->spec().local_address);
    work.remote_address = pending.slice->spec().remote_address;
    work.length = pending.slice->spec().length;
    work.token = completion_token;
    work.local_segment = local.segment;
    work.remote_segment = remote.segment;
    auto jetty = endpoint->jetty(worker_index);
    size_t posted_count = 0;
    status = jetty ? adapter_->post(jetty, {work}, posted_count)
                   : Status::InternalError("UB endpoint has no Jetty" LOC_MARK);
    if (posted_count == 1) {
        rail_monitor_->registerPath(path);
        if (!status.ok()) {
            LOG_EVERY_N(WARNING, 1000)
                << "UB post returned an error after accepting the WR: "
                << status.ToString();
        }
        return;
    }

    if (status.ok()) {
        status = Status::InternalError(
            "URMA adapter accepted an unexpected WR count" LOC_MARK);
    }

    {
        std::lock_guard<std::mutex> lock(inflight_mutex_);
        auto it = inflight_.find(completion_token);
        if (it != inflight_.end() && it->second == inflight)
            inflight_.erase(it);
    }
    inflight_cv_.notify_all();
    releaseInflight(inflight);
    rail_monitor_->recordError(path);
    if (endpoint_retirer_) endpoint_retirer_(endpoint);
    const auto resolution = pending.slice->resolveAttempt(
        *attempt, FAILED, 0, retryableStatus(status));
    if (resolution == UbAttemptResolution::kRetryScheduled) {
        enqueueRetry(pending);
    }
}

void UbWorkers::handleCompletion(const Completion& completion) {
    std::shared_ptr<Inflight> inflight;
    {
        std::lock_guard<std::mutex> lock(inflight_mutex_);
        auto it = inflight_.find(completion.token);
        if (it == inflight_.end()) return;
        inflight = std::move(it->second);
        inflight_.erase(it);
    }
    inflight_cv_.notify_all();
    releaseInflight(inflight);
    if (inflight->timed_out.load(std::memory_order_acquire)) {
        // A natural or flush completion proves that this particular WR can no
        // longer touch memory. Whichever side wins the timeout/drain race may
        // advance the logical attempt; attempt matching makes the other call
        // an idempotent no-op.
        const uint64_t now = steadyNowNs();
        recordTimeoutOnce(inflight, now);
        resolveInflight(inflight, TIMEOUT, 0, true);
        return;
    }

    const uint64_t now = steadyNowNs();
    const uint64_t latency =
        now >= inflight->posted_ns ? now - inflight->posted_ns : 0;
    switch (completion.category) {
        case CompletionCategory::SUCCESS:
            rail_monitor_->recordSuccess(inflight->path,
                                         inflight->pending.slice->spec().length,
                                         latency, now);
            resolveInflight(inflight, COMPLETED,
                            inflight->pending.slice->spec().length, false);
            break;
        case CompletionCategory::TIMEOUT:
            rail_monitor_->recordTimeout(inflight->path, now);
            if (endpoint_retirer_) endpoint_retirer_(inflight->endpoint);
            resolveInflight(inflight, TIMEOUT, 0, true);
            break;
        case CompletionCategory::LOCAL_DEVICE_ERROR:
            if (inflight->endpoint && inflight->endpoint->context()) {
                (void)inflight->endpoint->context()->markUnavailable();
            }
            rail_monitor_->recordError(inflight->path, now);
            if (endpoint_retirer_) endpoint_retirer_(inflight->endpoint);
            resolveInflight(inflight, FAILED, 0, true);
            break;
        case CompletionCategory::REMOTE_PATH_ERROR:
        case CompletionCategory::ENDPOINT_ERROR:
            rail_monitor_->recordError(inflight->path, now);
            if (endpoint_retirer_) endpoint_retirer_(inflight->endpoint);
            resolveInflight(inflight, FAILED, 0, true);
            break;
        case CompletionCategory::MEMORY_ERROR:
        case CompletionCategory::UNKNOWN_ERROR:
            rail_monitor_->recordError(inflight->path, now);
            resolveInflight(inflight, FAILED, 0, false);
            break;
    }
}

void UbWorkers::scanTimeouts() {
    std::lock_guard<std::mutex> drain_lock(endpoint_drain_mutex_);
    if (!timeout_scans_enabled_.load(std::memory_order_acquire)) return;
    const uint64_t now = steadyNowNs();
    std::vector<std::shared_ptr<Inflight>> expired;
    {
        std::lock_guard<std::mutex> lock(inflight_mutex_);
        for (const auto& [_, inflight] : inflight_) {
            bool expected = false;
            if (inflight->deadline_ns <= now &&
                inflight->timed_out.compare_exchange_strong(
                    expected, true, std::memory_order_acq_rel)) {
                expired.push_back(inflight);
            }
        }
    }
    for (const auto& inflight : expired) {
        std::vector<Completion> drained;
        auto status =
            inflight->endpoint
                ? inflight->endpoint->quiesce(params_.slice_timeout_ms, drained)
                : Status::InvalidArgument(
                      "Timed-out UB WR has no endpoint" LOC_MARK);
        // Even a partial/failed fence may have returned valid WR completions;
        // they must never be lost.
        for (const auto& completion : drained) {
            if (completion.token != 0) handleCompletion(completion);
        }
        if (!status.ok()) {
            rememberEndpointDrain(inflight->endpoint);
            LOG_EVERY_N(ERROR, 100)
                << "Cannot establish UB timeout drain fence: "
                << status.ToString();
            // Leave the token and all resource references alive. A natural
            // completion can still resolve it safely; otherwise a later scan
            // retries the native fence.
            bool still_inflight = false;
            {
                std::lock_guard<std::mutex> lock(inflight_mutex_);
                auto it = inflight_.find(inflight->completion_token);
                still_inflight =
                    it != inflight_.end() && it->second == inflight;
            }
            if (still_inflight) {
                inflight->timed_out.store(false, std::memory_order_release);
            }
            continue;
        }
        forgetEndpointDrain(inflight->endpoint);
        if (endpoint_retirer_) endpoint_retirer_(inflight->endpoint);
        recordTimeoutOnce(inflight, now);
        // quiesce() is the proof that retry cannot overlap old DMA. Reclaim a
        // provider-lost token here: the successful fence proves the old WR can
        // no longer touch memory, so releasing outstanding/quota accounting is
        // safe. releaseInflight() is idempotent against a racing completion.
        bool erased = false;
        {
            std::lock_guard<std::mutex> lock(inflight_mutex_);
            auto it = inflight_.find(inflight->completion_token);
            if (it != inflight_.end() && it->second == inflight) {
                inflight_.erase(it);
                erased = true;
            }
        }
        if (erased) {
            inflight_cv_.notify_all();
            releaseInflight(inflight);
        }
        resolveInflight(inflight, TIMEOUT, 0, true);
    }
}

void UbWorkers::releaseInflight(const std::shared_ptr<Inflight>& inflight) {
    bool expected = false;
    if (!inflight || !inflight->resources_released.compare_exchange_strong(
                         expected, true, std::memory_order_acq_rel)) {
        return;
    }
    if (inflight->endpoint) {
        inflight->endpoint->releaseOutstanding(
            inflight->pending.slice->spec().length);
    }
    if (inflight->quota.valid()) (void)quota_->release(inflight->quota);
    queue_cv_.notify_all();
}

void UbWorkers::recordTimeoutOnce(const std::shared_ptr<Inflight>& inflight,
                                  uint64_t now_ns) {
    bool expected = false;
    if (inflight && inflight->timeout_recorded.compare_exchange_strong(
                        expected, true, std::memory_order_acq_rel)) {
        rail_monitor_->recordTimeout(inflight->path, now_ns);
    }
}

void UbWorkers::rememberEndpointDrain(
    const std::shared_ptr<UbEndpoint>& endpoint) {
    if (!endpoint) return;
    std::lock_guard<std::mutex> lock(inflight_mutex_);
    draining_endpoints_[endpoint->generation()] = endpoint;
}

void UbWorkers::forgetEndpointDrain(
    const std::shared_ptr<UbEndpoint>& endpoint) {
    if (!endpoint) return;
    std::lock_guard<std::mutex> lock(inflight_mutex_);
    auto it = draining_endpoints_.find(endpoint->generation());
    if (it != draining_endpoints_.end() && it->second == endpoint) {
        draining_endpoints_.erase(it);
    }
}

void UbWorkers::resolveInflight(const std::shared_ptr<Inflight>& inflight,
                                TransferStatusEnum outcome, size_t bytes,
                                bool retryable) {
    const auto resolution = inflight->pending.slice->resolveAttempt(
        inflight->attempt, outcome, bytes, retryable);
    if (resolution == UbAttemptResolution::kRetryScheduled) {
        enqueueRetry(inflight->pending);
    }
}

void UbWorkers::failUnposted(const PendingSlice& pending,
                             TransferStatusEnum outcome) {
    if (pending.slice) (void)pending.slice->tryResolveBeforePost(outcome);
}

uint64_t UbWorkers::nextCompletionToken() {
    uint64_t token = next_token_.fetch_add(1, std::memory_order_relaxed);
    if (token == 0) token = next_token_.fetch_add(1, std::memory_order_relaxed);
    return token;
}

}  // namespace mooncake::tent::ub
