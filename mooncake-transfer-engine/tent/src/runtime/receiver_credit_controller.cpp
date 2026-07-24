// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0

#include "tent/runtime/receiver_credit_controller.h"

#include <algorithm>
#include <array>
#include <limits>
#include <utility>

#include "tent/runtime/control_plane.h"

namespace mooncake::tent {
namespace {

constexpr size_t resourceIndex(CreditResource resource) {
    return static_cast<size_t>(resource) - 1;
}

constexpr std::array<CreditResource, 2> kProductionResources{
    CreditResource::DataBytes, CreditResource::RequestSlots};

constexpr std::array<uint64_t, 6> kPullLatencyBucketUpperNs{
    100'000, 250'000, 500'000, 1'000'000, 10'000'000, 100'000'000};

bool sameGeneration(const CreditPeerContextSnapshot& context,
                    const CreditActivationV1& activation) {
    return context.key.receiver_session == activation.receiver_session_id &&
           context.epoch == activation.epoch;
}

}  // namespace

AdaptiveCreditDispatchLimiter::AdaptiveCreditDispatchLimiter(
    const ReceiverCreditRuntimeConfig& config, size_t hard_max_owners)
    : enabled_(config.adaptive_dispatch_enabled),
      min_owners_(
          std::min(config.adaptive_dispatch_min_owners, hard_max_owners)),
      slow_rtt_ns_(static_cast<uint64_t>(config.adaptive_dispatch_slow_rtt_us) *
                   1000),
      healthy_pulls_per_increase_(config.adaptive_dispatch_healthy_pulls),
      current_owners_(enabled_
                          ? std::min(config.adaptive_dispatch_initial_owners,
                                     hard_max_owners)
                          : std::numeric_limits<size_t>::max()),
      learned_ceiling_(enabled_ ? std::min(config.adaptive_dispatch_max_owners,
                                           hard_max_owners)
                                : std::numeric_limits<size_t>::max()) {}

void AdaptiveCreditDispatchLimiter::observe(std::chrono::nanoseconds elapsed,
                                            bool rpc_ok,
                                            size_t owners_at_start) {
    if (!enabled_) return;
    const bool unhealthy = !rpc_ok || static_cast<uint64_t>(std::max<int64_t>(
                                          0, elapsed.count())) >= slow_rtt_ns_;

    std::lock_guard lock(mutex_);
    size_t current = current_owners_.load(std::memory_order_relaxed);
    if (unhealthy) {
        ++slow_or_failed_pulls_;
        healthy_pulls_ = 0;
        const size_t failed_level = std::min(owners_at_start, learned_ceiling_);
        if (failed_level > min_owners_) {
            if (suspect_level_ == failed_level) {
                learned_ceiling_ = failed_level - 1;
                suspect_level_ = 0;
            } else {
                suspect_level_ = failed_level;
            }
        }
        if (current > min_owners_) {
            const size_t reduced = std::max(min_owners_, current / 2);
            current_owners_.store(reduced, std::memory_order_release);
            ++reductions_;
        }
        return;
    }

    if (owners_at_start != current || current >= learned_ceiling_) {
        healthy_pulls_ = 0;
        return;
    }
    if (++healthy_pulls_ < healthy_pulls_per_increase_) return;
    healthy_pulls_ = 0;
    current_owners_.store(current + 1, std::memory_order_release);
    ++increases_;
}

size_t AdaptiveCreditDispatchLimiter::ownerLimit() const {
    return current_owners_.load(std::memory_order_acquire);
}

AdaptiveDispatchSnapshot AdaptiveCreditDispatchLimiter::snapshot() const {
    std::lock_guard lock(mutex_);
    return {.current_owners = current_owners_.load(std::memory_order_relaxed),
            .learned_ceiling = learned_ceiling_,
            .suspect_level = suspect_level_,
            .slow_or_failed_pulls = slow_or_failed_pulls_,
            .reductions = reductions_,
            .increases = increases_};
}

size_t ReceiverCreditPullController::PeerKeyHash::operator()(
    const PeerKey& key) const noexcept {
    size_t h = std::hash<uint64_t>{}(key.target_id);
    h ^= std::hash<uint32_t>{}(key.qos_class) + 0x9e3779b97f4a7c15ULL +
         (h << 6) + (h >> 2);
    return h;
}

ReceiverCreditPullController::ReceiverCreditPullController(
    ReceiverCreditRuntimeConfig config, uint64_t sender_peer,
    std::shared_ptr<CreditPeerContextTable> contexts,
    std::shared_ptr<SenderCreditLedger> ledger, size_t dispatch_owner_ceiling)
    : config_(std::move(config)),
      sender_peer_(sender_peer),
      contexts_(std::move(contexts)),
      ledger_(std::move(ledger)),
      rpc_agent_(std::make_shared<CoroRpcAgent>()),
      dispatch_limiter_(config_, dispatch_owner_ceiling) {}

ReceiverCreditPullController::~ReceiverCreditPullController() { stop(); }

Status ReceiverCreditPullController::create(
    const ReceiverCreditRuntimeConfig& config, uint64_t sender_peer,
    std::shared_ptr<CreditPeerContextTable> contexts,
    std::shared_ptr<SenderCreditLedger> ledger,
    std::shared_ptr<ReceiverCreditPullController>& controller,
    size_t dispatch_owner_ceiling) {
    if (config.mode == CreditRolloutMode::Disabled || sender_peer == 0 ||
        !contexts || !ledger || config.max_peers == 0 ||
        dispatch_owner_ceiling == 0)
        return Status::InvalidArgument(
            "invalid receiver credit pull controller configuration" LOC_MARK);
    auto next = std::shared_ptr<ReceiverCreditPullController>(
        new ReceiverCreditPullController(config, sender_peer,
                                         std::move(contexts), std::move(ledger),
                                         dispatch_owner_ceiling));
    controller = std::move(next);
    return Status::OK();
}

Status ReceiverCreditPullController::normalize(
    const CreditCharge& charge,
    std::array<uint64_t, kCreditResourceCount>& normalized) {
    normalized.fill(0);
    if (charge.resources.empty())
        return Status::InvalidArgument(
            "empty receiver credit pull demand" LOC_MARK);
    for (const auto& [resource, amount] : charge.resources) {
        const auto raw = static_cast<uint16_t>(resource);
        if (raw < static_cast<uint16_t>(CreditResource::DataBytes) ||
            raw > static_cast<uint16_t>(CreditResource::ConsumerSlots) ||
            amount == 0)
            return Status::InvalidArgument(
                "invalid receiver credit pull demand" LOC_MARK);
        if (resource != CreditResource::DataBytes &&
            resource != CreditResource::RequestSlots)
            return Status::NotImplemented(
                "production receiver credit currently models only direct "
                "WRITE bytes and request slots" LOC_MARK);
        auto index = resourceIndex(resource);
        if (normalized[index] != 0)
            return Status::InvalidArgument(
                "duplicate receiver credit pull resource" LOC_MARK);
        normalized[index] = amount;
    }
    return Status::OK();
}

Status ReceiverCreditPullController::request(
    uint64_t target_id, const std::string& server_addr, uint32_t qos_class,
    const CreditCharge& minimum_charge) {
    if (target_id == 0 || server_addr.empty())
        return Status::InvalidArgument(
            "receiver credit pull requires a remote RPC address" LOC_MARK);
    if (qos_class != 0)
        return Status::InvalidArgument(
            "receiver credit MVP authorizes only QoS class 0" LOC_MARK);

    std::array<uint64_t, kCreditResourceCount> minimum;
    CHECK_STATUS(normalize(minimum_charge, minimum));

    PeerKey key{target_id, qos_class};
    bool should_launch = false;
    {
        std::lock_guard lock(mutex_);
        if (stopped_)
            return Status::InvalidEntry(
                "receiver credit pull controller stopped" LOC_MARK);
        auto it = peers_.find(key);
        if (it == peers_.end()) {
            if (peers_.size() >= config_.max_peers)
                return Status::TooManyRequests(
                    "receiver credit peer limit reached" LOC_MARK);
            it = peers_.emplace(key, Peer{}).first;
        }
        auto& peer = it->second;
        bool demand_changed = peer.server_addr != server_addr;
        peer.server_addr = server_addr;
        for (size_t i = 0; i < kCreditResourceCount; ++i) {
            if (minimum[i] > peer.minimum[i]) {
                peer.minimum[i] = minimum[i];
                demand_changed = true;
            }
        }
        if (!peer.in_flight) {
            peer.in_flight = true;
            should_launch = true;
        } else if (demand_changed) {
            // Repeated progress attempts for the same blocked owner must not
            // create an RPC train behind the outstanding pull. A fresh attempt
            // after completion can still launch if the refill was insufficient.
            peer.dirty = true;
        }
    }
    if (should_launch) launch(key);
    return Status::OK();
}

void ReceiverCreditPullController::launch(PeerKey key) {
    std::string server_addr;
    std::array<uint64_t, kCreditResourceCount> minimum{};
    uint64_t request_sequence = 0;
    {
        std::lock_guard lock(mutex_);
        auto it = peers_.find(key);
        if (stopped_ || it == peers_.end()) return;
        auto& peer = it->second;
        server_addr = peer.server_addr;
        minimum = peer.minimum;
        request_sequence = peer.next_request_sequence++;
        peer.dirty = false;
    }

    ReceiverCreditPullRequestV1 request;
    request.sender_peer = sender_peer_;
    request.qos_class = key.qos_class;
    request.request_sequence = request_sequence;

    CreditPeerContextSnapshot context;
    CreditLedgerSnapshot ledger_snapshot;
    const auto context_status =
        contexts_->lookup(key.target_id, key.qos_class, context);
    if (context_status.ok()) {
        request.expected_receiver_session_id = context.key.receiver_session;
        request.expected_epoch = context.epoch;
        auto ledger_status =
            ledger_->snapshot(context.key, context.epoch, ledger_snapshot);
        if (!ledger_status.ok()) {
            finish(key, ledger_status, {});
            return;
        }
        request.last_update_sequence = ledger_snapshot.last_sequence;
    } else if (!context_status.IsInvalidEntry()) {
        finish(key, context_status, {});
        return;
    }

    request.resources.reserve(kProductionResources.size());
    for (auto resource : kProductionResources) {
        const size_t index = resourceIndex(resource);
        ReceiverCreditResourceUsageV1 usage;
        usage.resource = resource;
        if (context_status.ok()) {
            usage.consumed_total = ledger_snapshot.consumed[index];
            usage.completed_total = ledger_snapshot.completed[index];
        }
        usage.minimum_available = minimum[index];
        usage.desired_available =
            std::max(minimum[index], config_.max_grant_per_pull[index]);
        request.resources.push_back(usage);
    }

    {
        std::lock_guard lock(mutex_);
        auto it = peers_.find(key);
        if (stopped_ || it == peers_.end()) return;
        it->second.pull_started_at = std::chrono::steady_clock::now();
        it->second.pull_dispatch_owners = dispatch_limiter_.ownerLimit();
        ++pulls_started_;
    }

    auto self = shared_from_this();
    ControlClient::pullReceiverCreditAsync(
        rpc_agent_, server_addr, request,
        [self = std::move(self), key](Status status,
                                      ReceiverCreditPullResponseV1 response) {
            self->finish(key, std::move(status), std::move(response));
        });
}

bool ReceiverCreditPullController::oldGenerationDrained(
    const CreditPeerContextSnapshot& context) const {
    CreditLedgerSnapshot snapshot;
    if (!ledger_->snapshot(context.key, context.epoch, snapshot).ok())
        return false;
    for (size_t i = 0; i < kCreditResourceCount; ++i)
        if (snapshot.consumed[i] != snapshot.completed[i]) return false;
    return true;
}

Status ReceiverCreditPullController::applyResponse(
    const PeerKey& key, const ReceiverCreditPullResponseV1& response,
    bool& request_again) {
    request_again = false;
    if (response.status == ReceiverCreditPullStatus::Unsupported) {
        std::lock_guard lock(mutex_);
        auto it = peers_.find(key);
        if (it != peers_.end())
            it->second.state = config_.mode == CreditRolloutMode::Optional
                                   ? CreditPeerState::Legacy
                                   : CreditPeerState::Failed;
        return Status::OK();
    }
    if (response.status == ReceiverCreditPullStatus::Rejected) {
        std::lock_guard lock(mutex_);
        auto it = peers_.find(key);
        if (it != peers_.end()) it->second.state = CreditPeerState::Failed;
        return Status::InvalidArgument(
            "receiver rejected credit pull" LOC_MARK);
    }

    CreditPeerContextSnapshot old_context;
    const auto old_status =
        contexts_->lookup(key.target_id, key.qos_class, old_context);

    if (response.status == ReceiverCreditPullStatus::SessionChanged ||
        (old_status.ok() &&
         !sameGeneration(old_context, response.activation))) {
        if (old_status.ok() && !oldGenerationDrained(old_context)) {
            std::lock_guard lock(mutex_);
            auto it = peers_.find(key);
            if (it != peers_.end()) it->second.state = CreditPeerState::Stale;
            return Status::InvalidEntry(
                "receiver generation changed with work still in "
                "flight" LOC_MARK);
        }
        if (old_status.ok()) {
            CHECK_STATUS(contexts_->deactivate(key.target_id, key.qos_class,
                                               old_context.key.receiver_session,
                                               old_context.epoch));
            CHECK_STATUS(
                ledger_->deactivate(old_context.key, old_context.epoch));
        }
        {
            std::lock_guard lock(mutex_);
            auto it = peers_.find(key);
            if (it != peers_.end()) {
                it->second.state = CreditPeerState::Negotiating;
                it->second.dirty = true;
            }
        }
        request_again = true;
        return Status::OK();
    }

    if (response.status != ReceiverCreditPullStatus::Granted &&
        response.status != ReceiverCreditPullStatus::Retry)
        return Status::InvalidArgument(
            "unknown receiver credit pull disposition" LOC_MARK);

    CreditKey credit_key{response.activation.receiver_session_id, sender_peer_,
                         key.qos_class};
    CHECK_STATUS(ledger_->activate(credit_key, response.activation.epoch));
    CreditUpdateDisposition disposition;
    CHECK_STATUS(
        ledger_->applyUpdate(credit_key, response.update, disposition));
    // Publish the context last. A dispatcher that can see this generation is
    // therefore guaranteed to find an activated, updated ledger entry.
    CHECK_STATUS(contexts_->activate(key.target_id, sender_peer_, key.qos_class,
                                     response.activation));
    {
        std::lock_guard lock(mutex_);
        auto it = peers_.find(key);
        if (it != peers_.end()) it->second.state = CreditPeerState::Active;
    }
    return Status::OK();
}

void ReceiverCreditPullController::finish(
    PeerKey key, Status rpc_status, ReceiverCreditPullResponseV1 response) {
    std::chrono::nanoseconds pull_elapsed{0};
    bool pull_measured = false;
    size_t pull_dispatch_owners = 0;
    {
        const auto now = std::chrono::steady_clock::now();
        std::lock_guard lock(mutex_);
        auto it = peers_.find(key);
        if (it != peers_.end() &&
            it->second.pull_started_at.time_since_epoch().count() != 0) {
            const auto elapsed = static_cast<uint64_t>(
                std::chrono::duration_cast<std::chrono::nanoseconds>(
                    now - it->second.pull_started_at)
                    .count());
            pull_elapsed = std::chrono::nanoseconds(elapsed);
            pull_measured = true;
            pull_dispatch_owners = it->second.pull_dispatch_owners;
            it->second.pull_started_at = {};
            ++pulls_completed_;
            pull_latency_ns_sum_ += elapsed;
            pull_latency_ns_max_ = std::max(pull_latency_ns_max_, elapsed);
            size_t bucket = 0;
            while (bucket < kPullLatencyBucketUpperNs.size() &&
                   elapsed > kPullLatencyBucketUpperNs[bucket])
                ++bucket;
            ++pull_latency_buckets_[bucket];
        }
    }

    if (pull_measured)
        dispatch_limiter_.observe(pull_elapsed, rpc_status.ok(),
                                  pull_dispatch_owners);

    bool request_again = false;
    Status result = std::move(rpc_status);
    if (result.ok()) result = applyResponse(key, response, request_again);

    bool launch_again = false;
    {
        std::lock_guard lock(mutex_);
        auto it = peers_.find(key);
        if (stopped_ || it == peers_.end()) return;
        auto& peer = it->second;
        peer.in_flight = false;
        if (!result.ok() && peer.state == CreditPeerState::Active)
            peer.state = CreditPeerState::Stale;
        if (!result.ok() && peer.state == CreditPeerState::Negotiating &&
            config_.mode == CreditRolloutMode::Required &&
            result.IsInvalidArgument())
            peer.state = CreditPeerState::Failed;
        peer.dirty = peer.dirty || request_again;
        if (peer.dirty) {
            peer.in_flight = true;
            launch_again = true;
        }
    }
    if (launch_again) launch(key);
}

CreditPeerState ReceiverCreditPullController::peerState(
    uint64_t target_id, uint32_t qos_class) const {
    std::lock_guard lock(mutex_);
    auto it = peers_.find({target_id, qos_class});
    return it == peers_.end() ? CreditPeerState::Negotiating : it->second.state;
}

size_t ReceiverCreditPullController::peerCount() const {
    std::lock_guard lock(mutex_);
    return peers_.size();
}

size_t ReceiverCreditPullController::dispatchOwnerLimit() const {
    return dispatch_limiter_.ownerLimit();
}

AdaptiveDispatchSnapshot
ReceiverCreditPullController::adaptiveDispatchSnapshot() const {
    return dispatch_limiter_.snapshot();
}

void ReceiverCreditPullController::stop() {
    std::lock_guard lock(mutex_);
    stopped_ = true;
    if (!summary_logged_) {
        const auto adaptive = dispatch_limiter_.snapshot();
        const double average_us =
            pulls_completed_ == 0
                ? 0.0
                : static_cast<double>(pull_latency_ns_sum_) /
                      static_cast<double>(pulls_completed_) / 1000.0;
        LOG(INFO) << "Receiver credit pull summary: started=" << pulls_started_
                  << " completed=" << pulls_completed_
                  << " avg_us=" << average_us << " max_us="
                  << static_cast<double>(pull_latency_ns_max_) / 1000.0
                  << " buckets_le_100us=" << pull_latency_buckets_[0]
                  << " le_250us=" << pull_latency_buckets_[1]
                  << " le_500us=" << pull_latency_buckets_[2]
                  << " le_1ms=" << pull_latency_buckets_[3]
                  << " le_10ms=" << pull_latency_buckets_[4]
                  << " le_100ms=" << pull_latency_buckets_[5]
                  << " gt_100ms=" << pull_latency_buckets_[6];
        LOG(INFO) << "Receiver credit adaptive dispatch summary: owners="
                  << adaptive.current_owners
                  << " learned_ceiling=" << adaptive.learned_ceiling
                  << " suspect_level=" << adaptive.suspect_level
                  << " slow_or_failed_pulls=" << adaptive.slow_or_failed_pulls
                  << " reductions=" << adaptive.reductions
                  << " increases=" << adaptive.increases;
        summary_logged_ = true;
    }
    peers_.clear();
}

}  // namespace mooncake::tent
