#include "eviction/evictor.h"

#include <glog/logging.h>

#include <algorithm>
#include <cmath>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "eviction/range_executor.h"
#include "types.h"

namespace mooncake {
namespace eviction {
namespace {

constexpr uint64_t kKeysPerWorker = 16'000'000;
constexpr uint32_t kMaxEvictionWorkers = MAX_EVICTION_WORKER_THREADS;

// A ratio above 1 asks for every eligible object; one that is negative or not
// a number asks for none.
double Sanitized(const char* name, double ratio) {
    if (std::isnan(ratio) || ratio < 0) {
        LOG(ERROR) << name << "=" << ratio << ", error=invalid_params";
        return 0;
    }
    return std::min(ratio, 1.0);
}

// A lease that has not run out ends on its own, and a deferred key must stay
// visible to the fallback, so only these two are worth postponing.
bool Lasts(SkipReason reason) {
    return reason == SkipReason::kSoftPinned ||
           reason == SkipReason::kNothingToRelease;
}

uint32_t ChooseWorkerCount(uint32_t configured_workers, uint64_t filed_keys) {
    if (configured_workers > 0) {
        return configured_workers;
    }
    const uint64_t auto_workers =
        filed_keys / kKeysPerWorker + (filed_keys % kKeysPerWorker != 0);
    return static_cast<uint32_t>(
        std::clamp<uint64_t>(auto_workers, 1, kMaxEvictionWorkers));
}

bool CanRunInParallel(const EvictionPolicy& policy, const Reclaimer& reclaimer,
                      const std::vector<EvictionObserver*>& observers) {
    return policy.SupportsParallel() && reclaimer.SupportsParallel() &&
           std::all_of(observers.begin(), observers.end(),
                       [](const EvictionObserver* observer) {
                           return observer->SupportsParallel();
                       });
}

}  // namespace

// Either goal, as a run holds each namespace to it.
struct Evictor::Aim {
    // Whether a namespace that filed `filed` keys, and has given up `evicted`
    // objects and `released` bytes, has met its share of the target, or for
    // the fallback of the minimum. A share is rounded up, as BatchEvict rounds
    // its pool's. A group goes all or none, so a namespace may give up more.
    [[nodiscard]] bool Met(uint64_t filed, uint64_t evicted, uint64_t released,
                           bool fallback) const {
        if (in_bytes) {
            return released >= target_bytes;
        }
        const double ratio = fallback ? min_ratio : target_ratio;
        return evicted >= static_cast<uint64_t>(std::ceil(filed * ratio));
    }

    uint64_t NamespaceGoal(uint64_t filed) const {
        if (in_bytes) {
            return target_bytes;
        }
        return static_cast<uint64_t>(std::ceil(filed * target_ratio));
    }

    // Set for a ByteGoal, which uses `target_bytes` alone.
    bool in_bytes = false;
    double target_ratio = 0;
    double min_ratio = 0;
    uint64_t target_bytes = 0;
    // Objects queued for offload count as evicted, their pinned bytes as
    // released.
    bool count_queued = false;
};

struct Evictor::NamespaceScan {
    std::unique_ptr<metadata::EvictionIndex::Cursor> serial;
    std::unique_ptr<metadata::EvictionIndex::RangeCursor> ranges;
    uint64_t goal = 0;

    metadata::EvictionIndex::Cursor& Cursor() const {
        return ranges ? *ranges : *serial;
    }
};

// One namespace's part of a run: the clock leases are judged against, the aim
// and the report it adds to, the namespace's cursor, the report as the
// namespace's part began, and the objects its policy deferred.
class Evictor::Pass {
   public:
    Pass(const Evictor& evictor, metadata::Namespace& ns,
         metadata::EvictionIndex::Cursor& cursor, const Aim& aim,
         Report& report, Clock::time_point now)
        : evictor_(evictor),
          ns_(ns),
          cursor_(cursor),
          filed_(cursor_.Total()),
          evicted_before_(report.evicted_objects),
          released_before_(report.released_bytes),
          offload_before_(report.offload_bytes),
          aim_(aim),
          report_(report),
          now_(now) {}

    // Walks the cursor serially until the namespace's share of the target is
    // met, then visits the objects the policy deferred until its share of the
    // minimum is met; each also ends when an OpLog write fails, the reclaimer
    // can take nothing more, or nothing is left. A group's members come in one
    // entry and go together, one key lock at a time, in the walk and in the
    // fallback alike.
    void RunSerial() {
        while (!Ended() && !Met(/*fallback=*/false)) {
            std::optional<Entry> entry = cursor_.Next();
            if (!entry) {
                break;
            }
            RunEntry(*entry, /*nonblocking=*/false);
        }
        RunFallback();
    }

    // Claims disjoint slot ranges across `workers` until `goal` is met, then
    // runs the soft-pin fallback serially once all worker frees have finished.
    void RunParallel(metadata::EvictionIndex::RangeCursor& ranges,
                     ThreadPool* workers, uint64_t goal) {
        const uint32_t worker_count =
            workers ? static_cast<uint32_t>(workers->size()) : 1u;
        std::vector<Report> local_reports(worker_count);
        std::vector<Pass> worker_passes;
        worker_passes.reserve(worker_count);
        for (uint32_t i = 0; i < worker_count; ++i) {
            worker_passes.emplace_back(evictor_, ns_, ranges, aim_,
                                       local_reports[i], now_);
        }

        // Progress is counted as Met() counts it, queued offloads included
        // with count_queued, so the executor's goal and the fallback agree.
        auto visit_entry = [&](uint32_t worker, const Entry& entry,
                               RangeStop& stop) -> uint64_t {
            Pass& pass = worker_passes[worker];
            const uint64_t before = pass.Progress();
            pass.RunEntry(entry, /*nonblocking=*/true);
            if (pass.Ended()) {
                stop.Stop();
            }
            return pass.Progress() - before;
        };

        const RangeResult result =
            RunRanges(ranges, workers, goal, visit_entry, ranges.GrainSize());
        for (uint32_t i = 0; i < worker_count; ++i) {
            report_.Merge(local_reports[i]);
            TakeFrom(worker_passes[i]);
        }
        report_.scanned_slots += ranges.Examined();
        report_.scan_waves += result.waves;
        RunFallback();
    }

   private:
    using Entry = metadata::EvictionIndex::Entry;

    void RunEntry(const Entry& entry, bool nonblocking) {
        deferred_.emplace_back();
        for (const std::string_view key : entry.keys) {
            if (Ended()) {
                break;
            }
            auto hold = nonblocking ? ns_.objects.TryWrite(key)
                                    : ns_.objects.Write(key);
            if (!hold && nonblocking) {
                ++report_.busy;
            }
            Evict(std::move(hold), &entry);
        }
        if (deferred_.back().empty()) {
            deferred_.pop_back();
        }
    }

    // Takes over what a worker's pass left for the fallback, and the objects
    // it queued without releasing memory, which its report does not tell.
    void TakeFrom(Pass& other) {
        for (auto& refs : other.deferred_) {
            deferred_.push_back(std::move(refs));
        }
        other.deferred_.clear();
        queued_only_ += std::exchange(other.queued_only_, 0);
    }

    void RunFallback() {
        for (const std::vector<route::ObjectRef>& refs : deferred_) {
            if (Ended() || Met(/*fallback=*/true)) {
                break;
            }
            for (const route::ObjectRef& ref : refs) {
                if (Ended()) {
                    break;
                }
                // Only the publication the policy deferred: a replacement was
                // never judged.
                Evict(ns_.objects.Write(ref), /*entry=*/nullptr);
            }
        }
    }

    // An OpLog write failed, or the reclaimer can take nothing more.
    [[nodiscard]] bool Ended() const {
        return report_.stopped || report_.exhausted;
    }

    // Objects given up by this pass and bytes released by it, each counting
    // what was queued for offload when the aim says so.
    [[nodiscard]] uint64_t Evicted() const {
        uint64_t evicted = report_.evicted_objects - evicted_before_;
        if (aim_.count_queued) {
            evicted += queued_only_;
        }
        return evicted;
    }
    [[nodiscard]] uint64_t Released() const {
        uint64_t released = report_.released_bytes - released_before_;
        if (aim_.count_queued) {
            released += report_.offload_bytes - offload_before_;
        }
        return released;
    }

    // This pass's progress toward the aim, in the aim's unit.
    [[nodiscard]] uint64_t Progress() const {
        return aim_.in_bytes ? Released() : Evicted();
    }

    [[nodiscard]] bool Met(bool fallback) const {
        return aim_.Met(filed_, Evicted(), Released(), fallback);
    }

    // Reclaims the object `hold` holds, met on the walk in `entry` or, with
    // no entry, in the fallback, and frees what that took once the lock is
    // let go.
    void Evict(std::optional<route::WriteGuard> hold, const Entry* entry) {
        if (!hold) {
            return;
        }
        // Freed as this returns, after the lock is let go.
        const std::vector<Replica> detached = EvictLocked(*hold, entry);
        hold.reset();
    }

    // Reclaims the object `hold` holds if it qualifies, tears it down if that
    // left it nothing valid, and returns the replicas it took.
    std::vector<Replica> EvictLocked(const route::WriteGuard& hold,
                                     const Entry* entry) {
        ++report_.visited;
        // Outside the cursor's selected rank range or no longer the same
        // entry: releasing the lock refiles the current publication.
        if (entry != nullptr && !cursor_.StillAt(hold, *entry)) {
            Skip(hold, SkipReason::kMoved);
            return {};
        }
        auto plan = Judge(hold, /*fallback=*/entry == nullptr);
        if (!plan) {
            if (evictor_.postpone_skipped_ > Clock::duration::zero() &&
                Lasts(plan.error())) {
                ns_.eviction_index().Postpone(
                    hold, now_ + evictor_.postpone_skipped_);
            }
            return {};
        }
        auto effect = evictor_.reclaimer_.Apply(ns_, hold, *plan, report_);
        if (!effect) {
            report_.stopped = true;
            return {};
        }
        report_.Add(*effect);
        // One that also released memory is in evicted_objects already.
        if (effect->outcome == Outcome::kOffloadQueued &&
            effect->released_bytes == 0) {
            ++queued_only_;
        }
        const ObjectView view{ns_.id(), hold.key(), hold.metadata()};
        for (EvictionObserver* observer : evictor_.observers_) {
            observer->OnApplied(view, *effect);
        }
        if (effect->needs_teardown) {
            evictor_.reclaimer_.TearDown(ns_, hold);
        }
        return std::move(effect->detached);
    }

    // The plan for the object `hold` holds, or why it is skipped. An object
    // the policy defers on the walk is kept for the fallback.
    tl::expected<Plan, SkipReason> Judge(const route::WriteGuard& hold,
                                         bool fallback) {
        const ObjectMetadata& metadata = hold.metadata();
        // Not const: ObjectMetadata::IsLeaseExpired takes it by reference.
        Clock::time_point now = now_;
        SkipReason reason;
        if (metadata.IsHardPinned()) {
            reason = SkipReason::kHardPinned;
        } else if (!metadata.IsLeaseExpired(now)) {
            // A surviving shared group lease keeps every member alive.
            reason = SkipReason::kLeaseActive;
        } else {
            auto plan = evictor_.policy_.Decide(
                Candidate{ns_.id(), hold.key(), metadata, now_, fallback});
            if (plan) {
                return plan;
            }
            reason = plan.error();
            if (reason == SkipReason::kDeferred && !fallback) {
                deferred_.back().push_back(hold.ref());
            }
        }
        Skip(hold, reason);
        return tl::make_unexpected(reason);
    }

    void Skip(const route::WriteGuard& hold, SkipReason reason) {
        if (reason == SkipReason::kMoved) {
            ++report_.moved;
        } else if (reason == SkipReason::kLeaseActive) {
            ++report_.lease_active;
        }
        const ObjectView view{ns_.id(), hold.key(), hold.metadata()};
        for (EvictionObserver* observer : evictor_.observers_) {
            observer->OnSkipped(view, reason);
        }
    }

    const Evictor& evictor_;
    metadata::Namespace& ns_;
    metadata::EvictionIndex::Cursor& cursor_;
    // The keys the index filed when the run began.
    const uint64_t filed_;
    const uint64_t evicted_before_;
    const uint64_t released_before_;
    const uint64_t offload_before_;
    // Objects this pass queued for offload without releasing any memory.
    uint64_t queued_only_ = 0;
    const Aim& aim_;
    Report& report_;
    const Clock::time_point now_;
    // In the order the walk met them, one list for each entry that had any.
    std::vector<std::vector<route::ObjectRef>> deferred_;
};

Evictor::Evictor(metadata::NamespaceTable& namespaces,
                 const EvictionPolicy& policy, Reclaimer& reclaimer,
                 std::vector<EvictionObserver*> observers,
                 uint32_t worker_threads, Clock::duration postpone_skipped)
    : namespaces_(namespaces),
      policy_(policy),
      reclaimer_(reclaimer),
      observers_(std::move(observers)),
      parallel_safe_(CanRunInParallel(policy_, reclaimer_, observers_)),
      worker_threads_(worker_threads),
      postpone_skipped_(postpone_skipped) {
    if (worker_threads > kMaxEvictionWorkers) {
        throw std::invalid_argument("eviction workers must be in [0, 64]");
    }
}

Evictor::~Evictor() = default;

Report Evictor::Run(const Scope& scope, const ObjectGoal& goal) {
    Aim aim{.target_ratio = Sanitized("evict_ratio_target", goal.target_ratio),
            .min_ratio = Sanitized("evict_ratio_lowerbound", goal.min_ratio),
            .count_queued = goal.count_queued};
    if (aim.min_ratio > aim.target_ratio) {
        LOG(ERROR) << "evict_ratio_target=" << aim.target_ratio
                   << ", evict_ratio_lowerbound=" << aim.min_ratio
                   << ", error=invalid_params";
        aim.min_ratio = aim.target_ratio;
    }
    return RunFor(scope, aim);
}

Report Evictor::Run(const TenantId& tenant_id, const ByteGoal& goal) {
    return RunFor(Scope::Of(tenant_id), Aim{.in_bytes = true,
                                            .target_bytes = goal.target_bytes,
                                            .count_queued = goal.count_queued});
}

std::vector<metadata::Namespace*> Evictor::CollectNamespaces(
    const Scope& scope) const {
    std::vector<metadata::Namespace*> namespaces;
    if (const TenantId* tenant_id = scope.tenant_id()) {
        if (metadata::Namespace* ns = namespaces_.Lookup(*tenant_id)) {
            namespaces.push_back(ns);
        }
        return namespaces;
    }
    namespaces_.Visit([&](const TenantId&, metadata::Namespace& ns) {
        namespaces.push_back(&ns);
    });
    return namespaces;
}

ThreadPool* Evictor::EnsureWorkerPool(uint32_t worker_count,
                                      uint64_t filed_keys) {
    if (worker_count <= 1) {
        if (workers_) {
            workers_.reset();
            LOG(INFO) << "[EVICT-WORKERS] filed=" << filed_keys
                      << ", workers=1";
        }
        return nullptr;
    }
    if (!workers_ || workers_->size() != worker_count) {
        workers_ = std::make_unique<ThreadPool>(worker_count);
        LOG(INFO) << "[EVICT-WORKERS] filed=" << filed_keys
                  << ", workers=" << worker_count;
    }
    return workers_.get();
}

Report Evictor::RunFor(const Scope& scope, const Aim& aim) {
    std::unique_lock run_lock(run_mutex_, std::defer_lock);
    if (parallel_safe_ && worker_threads_ != 1) {
        run_lock.lock();
    }

    const std::vector<metadata::Namespace*> namespaces =
        CollectNamespaces(scope);
    const Clock::time_point now = Clock::now();

    Report report;
    // Snapshot every namespace's share before reclaiming any namespace, so
    // concurrent writes cannot change a later namespace's share mid-run.
    std::vector<NamespaceScan> scans;
    scans.reserve(namespaces.size());
    for (metadata::Namespace* ns : namespaces) {
        NamespaceScan scan;
        // A run whose callbacks all support concurrency takes the range
        // cursor, which schedules the due buckets before the walk starts, on
        // one worker or several. A serial run (a policy, reclaimer or
        // observer that cannot run concurrently: an OpLog, offload-on-evict
        // or KVCS DFS run) walks the serial cursor, which reads the index as
        // the walk goes, so a run that meets its goal early pays for what it
        // read.
        if (parallel_safe_) {
            // Byte goals have no key-count estimate: 0 visits all due ranks.
            scan.ranges = ns->eviction_index().ScanRanges(
                now, [&](size_t filed) -> size_t {
                    return aim.in_bytes ? 0 : aim.NamespaceGoal(filed);
                });
        }
        if (!scan.ranges) {
            scan.serial = ns->eviction_index().Scan(now);
        }
        const uint64_t filed = scan.Cursor().Total();
        report.filed += filed;
        scan.goal = aim.NamespaceGoal(filed);
        scans.push_back(std::move(scan));
    }

    // A run an OpLog write or a full set of offload destinations cuts short
    // leaves the namespaces after that one to the next run, which starts one
    // namespace further on.
    const size_t start = next_start_.fetch_add(1, std::memory_order_relaxed);
    for (size_t n = 0;
         n < namespaces.size() && !report.stopped && !report.exhausted; ++n) {
        const size_t i = (start + n) % namespaces.size();
        metadata::Namespace& ns = *namespaces[i];
        NamespaceScan& scan = scans[i];
        if (!scan.ranges) {
            Pass(*this, ns, scan.Cursor(), aim, report, now).RunSerial();
            continue;
        }

        const uint64_t filed = scan.ranges->Total();
        const uint32_t worker_count = ChooseWorkerCount(worker_threads_, filed);
        ThreadPool* pool = run_lock.owns_lock()
                               ? EnsureWorkerPool(worker_count, filed)
                               : nullptr;
        report.workers = std::max(report.workers, worker_count);
        Pass(*this, ns, *scan.ranges, aim, report, now)
            .RunParallel(*scan.ranges, pool, scan.goal);
    }

    for (EvictionObserver* observer : observers_) {
        observer->OnRunEnd(scope, report);
    }
    return report;
}

}  // namespace eviction
}  // namespace mooncake
