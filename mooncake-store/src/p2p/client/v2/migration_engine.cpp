#include "p2p/client/v2/migration_engine.h"

#include <algorithm>
#include <utility>

#include <glog/logging.h>

namespace mooncake::v2 {

MigrationEngine::MigrationEngine(MultiTiler* tilers, BlockRegistry* registry,
                                 LocalCopyEngine* copy_engine,
                                 MetadataCallbacks* callbacks,
                                 AllocateBlockCallback allocate_block,
                                 std::shared_ptr<Clock> clock,
                                 const MigrationSchedulerConfig& scheduler)
    : tilers_(tilers),
      registry_(registry),
      copy_engine_(copy_engine),
      callbacks_(callbacks),
      allocate_block_(std::move(allocate_block)),
      clock_(clock != nullptr ? std::move(clock)
                              : std::make_shared<SteadyClock>()),
      scheduler_(scheduler) {
    CHECK(tilers_ != nullptr && registry_ != nullptr && copy_engine_ != nullptr)
        << "MigrationEngine requires tilers, a registry and a copy engine";
}

tl::expected<void, ErrorCode> MigrationEngine::Execute(
    const MovementRequest& request) {
    {
        std::lock_guard<std::mutex> lock(stats_mu_);
        ++stats_.executed;
    }
    auto note_stale = [this] {
        std::lock_guard<std::mutex> lock(stats_mu_);
        ++stats_.stale;
        return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    };
    auto note_expired = [this] {
        std::lock_guard<std::mutex> lock(stats_mu_);
        ++stats_.deadline_exceeded;
        return tl::make_unexpected(ErrorCode::TRANSFER_FAIL);
    };
    // Checked on both sides of the copy, and that is not belt and braces: a
    // check only before it would still let a multi-gigabyte copy run for
    // minutes past a deadline the caller set, and a check only after it would
    // do the whole copy before noticing. The field was set to now+30s and read
    // nowhere at all before this.
    const auto expired = [this, &request] {
        return request.deadline.has_value() &&
               clock_->Now() >= *request.deadline;
    };
    if (expired()) return note_expired();

    if (request.destination_tiler == UUID{0, 0}) {
        LOG(ERROR) << "A migration command needs a destination tier";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    TilerManager* source = tilers_->Find(request.source_tiler);
    TilerManager* destination = tilers_->Find(request.destination_tiler);
    if (source == nullptr || destination == nullptr) return note_stale();

    // Staleness check 1: the identity may already be gone.
    auto registration = request.registration.Lock();
    if (!registration.has_value() || registration->IsRetired()) {
        return note_stale();
    }
    // Staleness check 2: the key may have been deleted and recreated, which
    // mints a new identity; this one would then name a detached block.
    if (!registry_->IsCanonical(*registration)) return note_stale();

    // Staleness check 3: the block itself may have been replaced.
    auto matched = source->Match(*registration);
    if (!matched || !(matched->Id() == request.source_block_id)) {
        return note_stale();
    }
    // The snapshot keeps the source alive for the whole copy, even if a
    // concurrent Delete detaches it meanwhile.
    ImmutableBlock source_block = std::move(matched.value());
    const size_t size_bytes = source_block.Size();

    auto target =
        allocate_block_(destination->Id(), size_bytes,
                        /*alignment=*/0, AllocationSource::kMigration);
    if (!target) {
        std::lock_guard<std::mutex> lock(stats_mu_);
        ++stats_.allocate_failed;
        return tl::make_unexpected(target.error());
    }

    if (expired()) return note_expired();
    auto copied = copy_engine_->Copy(source_block, target.value());
    if (!copied) {
        std::lock_guard<std::mutex> lock(stats_mu_);
        ++stats_.copy_failed;
        // The destination is dropped by MutableBlock's destructor; the source
        // is untouched.
        return tl::make_unexpected(copied.error());
    }

    auto completed =
        std::move(target.value()).Complete(std::string(registration->Key()));
    if (!completed) {
        std::lock_guard<std::mutex> lock(stats_mu_);
        ++stats_.copy_failed;
        return tl::make_unexpected(completed.error());
    }

    // Captured inside the guard, published outside it. Publishing under the
    // key's mutation guard would push into an event queue -- and, on a full
    // queue, run a whole inline fan-out -- while the key is locked, which
    // section 9.9 forbids.
    // After the copy and before the guard: publishing a block whose deadline
    // has passed is worse than dropping it, because the caller has already
    // given up and may have arranged the move some other way.
    if (expired()) return note_expired();

    BlockId registered_id;
    size_t registered_size = 0;
    bool source_removed = false;
    {
        // Re-checked under the mutation guard: a large copy takes long enough
        // for the key to be deleted, recreated or rewritten meanwhile, and
        // publishing the copy then would resurrect data the caller deleted.
        auto guard = registration->LockMutation();
        if (guard.IsRetired() || !registry_->IsCanonical(*registration)) {
            return note_stale();
        }
        auto current = source->Match(*registration);
        if (!current || !(current->Id() == request.source_block_id)) {
            return note_stale();
        }

        auto registered = destination->RegisterWithHandle(
            std::move(completed.value()), *registration, /*defer_notify=*/true);
        if (!registered) {
            std::lock_guard<std::mutex> lock(stats_mu_);
            ++stats_.register_failed;
            return tl::make_unexpected(registered.error());
        }
        registered_id = registered->Id();
        registered_size = registered->Size();

        if (request.kind == MovementKind::kMigrate) {
            // Only now: the destination is registered and matchable, so the
            // object never stops existing somewhere.
            auto removed =
                source->Delete(*registration, request.source_block_id,
                               /*defer_notify=*/true);
            if (!removed) {
                LOG(WARNING) << "Migrated key=" << registration->Key()
                             << " but its source replica was already gone";
            } else {
                source_removed = true;
            }
        }
    }

    // The deferred halves, with no lock held. Destination first: the object
    // must never appear to stop existing, so the fact that says "it is here"
    // is published before the one that says "it left there".
    destination->NotifyRegistered(*registration, registered_id,
                                  registered_size);
    if (source_removed) {
        source->NotifyDeleted(*registration, request.source_block_id,
                              size_bytes);
    }

    // Metadata callbacks run with no internal lock held.
    if (callbacks_ != nullptr) {
        if (callbacks_->add_replica) {
            auto added = callbacks_->add_replica(registration->Key(),
                                                 destination->Id(), size_bytes);
            if (!added) {
                LOG(WARNING) << "add-replica callback failed after migrating "
                             << registration->Key();
            }
        }
        if (request.kind == MovementKind::kMigrate &&
            callbacks_->remove_replica) {
            auto removed =
                callbacks_->remove_replica(registration->Key(), source->Id());
            if (!removed) {
                LOG(WARNING) << "remove-replica callback failed after "
                                "migrating "
                             << registration->Key();
            }
        }
    }

    std::lock_guard<std::mutex> lock(stats_mu_);
    ++stats_.succeeded;
    return {};
}

std::vector<tl::expected<void, ErrorCode>> MigrationEngine::ExecuteBatch(
    const std::vector<MovementRequest>& requests) {
    std::vector<tl::expected<void, ErrorCode>> results;
    results.reserve(requests.size());
    for (const MovementRequest& request : requests) {
        // Independently. A batch is a scheduling unit, not a transaction:
        // these are separate objects on separate blocks, and rolling one back
        // because another failed would undo work that is already correct.
        results.push_back(Execute(request));
    }
    return results;
}

bool MigrationEngine::Enqueue(MovementRequest request, MovementLease lease) {
    const Clock::time_point now = clock_->Now();
    {
        std::lock_guard<std::mutex> lock(queue_mu_);
        if (stopped_) return false;  // ~MovementLease settles it
        if (scheduler_.max_queued_requests > 0 &&
            queued_total_ >= scheduler_.max_queued_requests) {
            std::lock_guard<std::mutex> stats_lock(stats_mu_);
            ++stats_.submissions_rejected;
            return false;
        }

        auto [it, inserted] = routes_.try_emplace(request.route);
        if (inserted) {
            it->second.route = request.route;
            route_order_.push_back(request.route);
        }
        it->second.queued_bytes += request.length;
        it->second.items.push_back(
            QueuedMovement{std::move(request), std::move(lease), now});
        ++queued_total_;
    }
    queue_cv_.notify_one();
    return true;
}

bool MigrationEngine::ReadyLocked(const RouteQueue& queue,
                                  Clock::time_point now,
                                  const char** reason) const {
    if (queue.items.empty()) return false;
    if (queue.items.size() >= scheduler_.max_batch_items) {
        *reason = "items";
        return true;
    }
    if (queue.queued_bytes >= scheduler_.max_batch_bytes) {
        *reason = "bytes";
        return true;
    }
    // From the oldest request's arrival, so a route that sees one request
    // every few seconds still runs it on time instead of waiting for company.
    if (now - queue.items.front().queued_at >= scheduler_.max_batch_delay) {
        *reason = "delay";
        return true;
    }
    return false;
}

bool MigrationEngine::AdmissibleLocked(const RouteQueue& queue) const {
    if (queue.inflight >= scheduler_.max_inflight_per_route) return false;
    auto device = device_inflight_.find(queue.route.destination_tiler);
    const size_t on_device =
        device == device_inflight_.end() ? 0 : device->second;
    return on_device < scheduler_.max_inflight_per_device;
}

MigrationEngine::RouteQueue* MigrationEngine::SelectRouteLocked(
    Clock::time_point now, const char** reason) {
    // Two passes: foreground first, then background. A warm-up batch must
    // never delay reclamation a writer is blocked on, and a single ordering
    // that mixed them would do exactly that whenever the warm-up route
    // happened to be next in the rotation.
    for (int pass = 0; pass < 2; ++pass) {
        const MovementPriority wanted = pass == 0
                                            ? MovementPriority::kForeground
                                            : MovementPriority::kBackground;
        // Round-robin from where the last selection stopped, so a busy route
        // cannot hold every worker while its neighbours wait.
        for (size_t step = 0; step < route_order_.size(); ++step) {
            const size_t index = (next_route_ + step) % route_order_.size();
            auto found = routes_.find(route_order_[index]);
            if (found == routes_.end()) continue;
            RouteQueue& queue = found->second;
            if (queue.items.empty()) continue;
            if (queue.items.front().request.priority != wanted) continue;
            if (!ReadyLocked(queue, now, reason)) continue;
            if (!AdmissibleLocked(queue)) continue;
            next_route_ = (index + 1) % route_order_.size();
            return &queue;
        }
    }
    return nullptr;
}

size_t MigrationEngine::RunOnce() {
    std::vector<MovementRequest> batch;
    std::vector<MovementLease> leases;
    UUID device{0, 0};
    MovementRoute route;

    {
        std::unique_lock<std::mutex> lock(queue_mu_);
        const char* reason = "";
        RouteQueue* queue = nullptr;
        for (;;) {
            if (stopped_) return 0;
            queue = SelectRouteLocked(clock_->Now(), &reason);
            if (queue != nullptr) break;
            // A route may be waiting only on its delay, so this cannot block
            // forever on the condition variable alone.
            queue_cv_.wait_for(lock, scheduler_.max_batch_delay);
        }

        route = queue->route;
        device = route.destination_tiler;
        size_t bytes = 0;
        while (!queue->items.empty() &&
               batch.size() < scheduler_.max_batch_items &&
               (bytes == 0 || bytes < scheduler_.max_batch_bytes)) {
            QueuedMovement item = std::move(queue->items.front());
            queue->items.pop_front();
            queue->queued_bytes -=
                std::min(queue->queued_bytes, item.request.length);
            --queued_total_;
            bytes += item.request.length;
            batch.push_back(std::move(item.request));
            leases.push_back(std::move(item.lease));
        }
        ++queue->inflight;
        ++device_inflight_[device];

        std::lock_guard<std::mutex> stats_lock(stats_mu_);
        ++stats_.batches;
        if (std::string_view(reason) == "items") {
            ++stats_.batches_by_items;
        } else if (std::string_view(reason) == "bytes") {
            ++stats_.batches_by_bytes;
        } else {
            ++stats_.batches_by_delay;
        }
    }

    const std::vector<tl::expected<void, ErrorCode>> results =
        ExecuteBatch(batch);

    {
        std::lock_guard<std::mutex> lock(queue_mu_);
        auto found = routes_.find(route);
        if (found != routes_.end() && found->second.inflight > 0) {
            --found->second.inflight;
        }
        auto on_device = device_inflight_.find(device);
        if (on_device != device_inflight_.end() && on_device->second > 0) {
            --on_device->second;
        }
    }
    queue_cv_.notify_all();

    // Settled here, after the outcome is known, so the tracker records whether
    // the block actually moved. Every path through this function reaches this
    // loop; a lease that escaped it would wedge its key's dedup slot forever.
    for (size_t i = 0; i < leases.size(); ++i) {
        leases[i].Settle(i < results.size() && results[i].has_value());
    }
    return batch.size();
}

void MigrationEngine::Stop() {
    std::vector<MovementLease> abandoned;
    {
        std::lock_guard<std::mutex> lock(queue_mu_);
        if (stopped_) return;
        stopped_ = true;
        for (auto& [route, queue] : routes_) {
            for (auto& item : queue.items) {
                abandoned.push_back(std::move(item.lease));
            }
            queue.items.clear();
            queue.queued_bytes = 0;
        }
        queued_total_ = 0;
    }
    queue_cv_.notify_all();
    // Discarding a queue is an exit path like any other: these movements never
    // happened, and saying so is what lets those keys be proposed again after
    // a restart rather than looking permanently in flight.
    for (MovementLease& lease : abandoned) lease.Settle(false);
}

bool MigrationEngine::IsStopped() const {
    std::lock_guard<std::mutex> lock(queue_mu_);
    return stopped_;
}

std::vector<RouteStats> MigrationEngine::Routes() const {
    const Clock::time_point now = clock_->Now();
    std::vector<RouteStats> stats;
    std::lock_guard<std::mutex> lock(queue_mu_);
    stats.reserve(routes_.size());
    for (const auto& [route, queue] : routes_) {
        RouteStats entry;
        entry.route = route;
        entry.label = ToLabel(route);
        entry.queued_items = queue.items.size();
        entry.queued_bytes = queue.queued_bytes;
        entry.inflight = queue.inflight;
        if (!queue.items.empty()) {
            entry.oldest_age =
                std::chrono::duration_cast<std::chrono::milliseconds>(
                    now - queue.items.front().queued_at);
        }
        stats.push_back(std::move(entry));
    }
    return stats;
}

size_t MigrationEngine::QueuedCount() const {
    std::lock_guard<std::mutex> lock(queue_mu_);
    return queued_total_;
}

tl::expected<void, ErrorCode> ValidateMigrationSchedulerConfig(
    const MigrationSchedulerConfig& config) {
    if (config.max_batch_items == 0 || config.max_batch_bytes == 0) {
        LOG(ERROR) << "migration.max_batch_items and max_batch_bytes must be "
                      "greater than zero";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (config.max_batch_delay <= std::chrono::milliseconds::zero()) {
        LOG(ERROR) << "migration.max_batch_delay_ms must be greater than zero";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (config.max_inflight_per_route == 0 ||
        config.max_inflight_per_device == 0) {
        LOG(ERROR) << "migration inflight quotas must be greater than zero";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    return {};
}

MigrationStats MigrationEngine::Stats() const {
    std::lock_guard<std::mutex> lock(stats_mu_);
    return stats_;
}

}  // namespace mooncake::v2
