#include "p2p/client/v2/v2_common.h"

#include <glog/logging.h>

namespace mooncake::v2 {

const char* ToString(AllocationSource source) {
    switch (source) {
        case AllocationSource::kPut:
            return "put";
        case AllocationSource::kPreWrite:
            return "prewrite";
        case AllocationSource::kWriteRemoteData:
            return "write_remote_data";
        case AllocationSource::kMigration:
            return "migration";
        case AllocationSource::kOnboard:
            return "onboard";
        case AllocationSource::kCount:
            break;
    }
    return "unknown";
}

const char* ToString(LifecycleState state) {
    switch (state) {
        case LifecycleState::kRunning:
            return "running";
        case LifecycleState::kStopping:
            return "stopping";
        case LifecycleState::kStopped:
            return "stopped";
        case LifecycleState::kDestroyed:
            return "destroyed";
    }
    return "unknown";
}

namespace {
// Upper bound on synchronous reclaim rounds. More than this on the request
// path turns an allocation failure into a latency cliff.
constexpr uint32_t kMaxEvictRounds = 8;
// Upper bound on the synchronous reclaim deadline, for the same reason.
constexpr std::chrono::milliseconds kMaxEvictTimeout{1000};
}  // namespace

tl::expected<void, ErrorCode> ValidateAllocationFailurePolicy(
    const AllocationFailurePolicyConfig& config) {
    if (!config.try_evict) {
        // The remaining fields are unused when eviction is disabled, so they
        // are deliberately not validated: a stale value cannot change
        // behaviour.
        return {};
    }
    if (config.max_evict_rounds == 0 ||
        config.max_evict_rounds > kMaxEvictRounds) {
        LOG(ERROR) << "allocation_failure.max_evict_rounds must be in [1, "
                   << kMaxEvictRounds << "], got " << config.max_evict_rounds;
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (config.evict_timeout <= std::chrono::milliseconds::zero() ||
        config.evict_timeout > kMaxEvictTimeout) {
        LOG(ERROR) << "allocation_failure.evict_timeout_ms must be in (0, "
                   << kMaxEvictTimeout.count() << "], got "
                   << config.evict_timeout.count();
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    return {};
}

// ---------------------------------------------------------------------------
// AllocationFailureMetrics
// ---------------------------------------------------------------------------

AllocationFailureMetrics::AllocationFailureMetrics(
    const std::vector<UUID>& tiler_ids) {
    // Every slot is created here and never again, which is what lets the
    // request path treat this as read-only.
    counters_.reserve(tiler_ids.size());
    for (const auto& id : tiler_ids) {
        counters_.try_emplace(id);
    }
}

AllocationFailureCounters& AllocationFailureMetrics::For(
    const UUID& tiler_id, AllocationSource source) {
    const size_t index = static_cast<size_t>(source);
    auto it = counters_.find(tiler_id);
    if (it == counters_.end()) {
        if (!warned_unknown_.exchange(true, std::memory_order_relaxed)) {
            LOG(ERROR) << "Allocation failure recorded for an unregistered "
                          "tier; counting it in the shared unknown slot";
        }
        return unknown_[index];
    }
    return it->second[index];
}

// ---------------------------------------------------------------------------
// LifecycleGate::Guard
// ---------------------------------------------------------------------------

LifecycleGate::Guard::Guard(Guard&& other) noexcept
    : owner_(std::move(other.owner_)) {
    other.owner_.reset();
}

LifecycleGate::Guard& LifecycleGate::Guard::operator=(Guard&& other) noexcept {
    if (this != &other) {
        if (owner_ != nullptr) {
            owner_->Release();
        }
        owner_ = std::move(other.owner_);
        other.owner_.reset();
    }
    return *this;
}

LifecycleGate::Guard::~Guard() {
    if (owner_ != nullptr) {
        owner_->Release();
        owner_.reset();
    }
}

bool LifecycleGate::Guard::IsUsable() const {
    return owner_ != nullptr && !owner_->IsCancelled();
}

// ---------------------------------------------------------------------------
// LifecycleGate
// ---------------------------------------------------------------------------

tl::expected<LifecycleGate::Guard, ErrorCode> LifecycleGate::Acquire() {
    // Count first, then re-check: a Stop() that slips in between is caught by
    // the second read, and the guard we hand back is already accounted for so
    // Stop cannot conclude "no in-flight work" while we are still here.
    inflight_.fetch_add(1, std::memory_order_acq_rel);
    if (state_.load(std::memory_order_acquire) != LifecycleState::kRunning) {
        Release();
        return tl::make_unexpected(ErrorCode::SHUTTING_DOWN);
    }
    return Guard(shared_from_this());
}

void LifecycleGate::Release() {
    const uint64_t previous = inflight_.fetch_sub(1, std::memory_order_acq_rel);
    DCHECK_GT(previous, 0U) << "LifecycleGate in-flight counter underflow";
    if (previous == 1) {
        // Only a waiting Stop() cares, and it holds wait_mu_ around its
        // predicate check, so taking it here closes the lost-wakeup window.
        std::lock_guard<std::mutex> lock(wait_mu_);
        wait_cv_.notify_all();
    }
}

bool LifecycleGate::BeginStop() {
    LifecycleState expected = LifecycleState::kRunning;
    return state_.compare_exchange_strong(expected, LifecycleState::kStopping,
                                          std::memory_order_acq_rel);
}

bool LifecycleGate::WaitForNoInflight(std::chrono::milliseconds timeout) {
    std::unique_lock<std::mutex> lock(wait_mu_);
    return wait_cv_.wait_for(lock, timeout, [this] {
        return inflight_.load(std::memory_order_acquire) == 0;
    });
}

void LifecycleGate::BeginCancel() {
    cancelled_.store(true, std::memory_order_release);
    std::lock_guard<std::mutex> lock(wait_mu_);
    wait_cv_.notify_all();
}

bool LifecycleGate::IsCancelled() const {
    return cancelled_.load(std::memory_order_acquire);
}

void LifecycleGate::MarkStopped() {
    LifecycleState expected = LifecycleState::kStopping;
    state_.compare_exchange_strong(expected, LifecycleState::kStopped,
                                   std::memory_order_acq_rel);
}

bool LifecycleGate::MarkDestroyed() {
    LifecycleState previous =
        state_.exchange(LifecycleState::kDestroyed, std::memory_order_acq_rel);
    return previous != LifecycleState::kDestroyed;
}

LifecycleState LifecycleGate::State() const {
    return state_.load(std::memory_order_acquire);
}

uint64_t LifecycleGate::InflightForTest() const {
    return inflight_.load(std::memory_order_acquire);
}

}  // namespace mooncake::v2
