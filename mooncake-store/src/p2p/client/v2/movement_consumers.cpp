#include "p2p/client/v2/movement_consumers.h"

#include <algorithm>
#include <utility>

#include <glog/logging.h>

namespace mooncake::v2 {
namespace {

/** Usage as a fraction of capacity; 0 for a tier with no capacity. */
double UsageRatioOf(const TilerManager& tiler) {
    const size_t capacity = tiler.Capacity();
    if (capacity == 0) return 0.0;
    return static_cast<double>(tiler.Usage()) / static_cast<double>(capacity);
}

/**
 * @brief Build the placement context.
 *
 * The two callbacks are the whole reason the policy can be stateless: it asks
 * for live usage and live presence at the moment it decides, instead of
 * holding a copy that was true when some earlier event arrived.
 */
PlacementContext MakeContext(const MovementConsumerDeps& deps,
                             std::string_view key, const UUID& source,
                             MovementDirection direction, size_t size_bytes,
                             double frequency,
                             const BlockRegistrationHandle& registration) {
    PlacementContext context;
    context.key = key;
    context.source_tiler = source;
    context.direction = direction;
    context.size_bytes = size_bytes;
    context.frequency = frequency;
    context.usage_ratio = [deps](const UUID& tiler_id) -> double {
        TilerManager* tiler = deps.tilers->Find(tiler_id);
        return tiler == nullptr ? 1.0 : UsageRatioOf(*tiler);
    };
    context.already_present = [deps, &registration](const UUID& tiler_id) {
        TilerManager* tiler = deps.tilers->Find(tiler_id);
        // Only the destination's own index can answer this. A cached answer
        // would be a second source of truth about what exists, and the
        // executor re-checks it under the guard anyway -- this pre-check only
        // avoids proposing a copy that is certainly redundant.
        return tiler != nullptr && tiler->Match(registration).has_value();
    };
    return context;
}

/** Claim, build and hand over one command. Returns what to report. */
ConsumeResult Propose(const MovementConsumerDeps& deps,
                      const MovementConsumerConfig& config,
                      const PlacementDecision& decision,
                      const BlockEvent& event,
                      const BlockRegistrationHandle& registration,
                      const BlockId& block_id, MovementDirection direction) {
    MovementDedupKey dedup;
    dedup.registration_id = registration.Id();
    dedup.source_block_id = block_id;
    dedup.source_tiler = event.tiler_id;
    dedup.destination_tiler = decision.destination_tiler;

    auto lease = deps.movement->TryAcquire(event.key, dedup, direction);
    if (!lease) {
        // In flight, cooling down or too freshly arrived. All three are the
        // policy working, not a failure, so the fact was still applied.
        return ConsumeResult::kIgnored;
    }

    MovementRequest request;
    request.kind = decision.kind;
    request.key = event.key;
    request.registration = registration.Downgrade();
    request.source_tiler = event.tiler_id;
    request.destination_tiler = decision.destination_tiler;
    request.source_block_id = block_id;
    request.route = decision.route;
    request.length = event.size_bytes;
    // Neither direction has a caller blocked on it: offload is speculative
    // capacity work and onboard is speculative warming. Reclamation is what
    // runs in the foreground, and it does not travel this pipeline at all.
    request.priority = MovementPriority::kBackground;
    request.deadline = deps.clock->Now() + config.movement_deadline;

    if (!deps.sink->Enqueue(std::move(request), std::move(lease.value()))) {
        return ConsumeResult::kIgnored;
    }
    return ConsumeResult::kCommandEnqueued;
}

/**
 * @class OffloadConsumer
 */
class OffloadConsumer final : public EventConsumer {
   public:
    OffloadConsumer(const MovementConsumerConfig& config,
                    const MovementConsumerDeps& deps)
        : config_(config), deps_(deps) {}

    Subscription SubscriptionInfo() const override {
        Subscription subscription;
        subscription.name = "offload";
        subscription.event_mask = SubscriptionMask({EventType::kCommit});
        return subscription;
    }

    ConsumeResult Consume(const BlockEvent& event, DeliveryMode mode) override {
        // Inline means a writer's thread. Offload is speculative; dropping the
        // decision costs a delayed copy, while making it here would put a
        // queue push into the write path.
        if (mode == DeliveryMode::kInline) return ConsumeResult::kApplied;

        TilerManager* source = deps_.tilers->Find(event.tiler_id);
        if (source == nullptr) return ConsumeResult::kIgnored;
        if (UsageRatioOf(*source) < config_.offload_high_watermark) {
            return ConsumeResult::kIgnored;
        }

        // The block that arrived is not the one that should leave: what to
        // shed is a property of the tier's contents, and its eviction index is
        // what orders them. Asking for one victim's worth keeps this a single
        // decision per event rather than a burst.
        auto victims = source->Eviction()->SelectVictims(1);
        if (victims.empty()) return ConsumeResult::kIgnored;
        const BlockToken& victim = victims.front();

        auto registration = victim.registration.Lock();
        if (!registration.has_value() || registration->IsRetired()) {
            return ConsumeResult::kIgnored;
        }
        if (!deps_.registry->IsCanonical(*registration)) {
            return ConsumeResult::kIgnored;
        }

        const FrequencySnapshot heat =
            deps_.frequency->Get(registration->Id(), victim.key);
        BlockEvent subject = event;
        subject.key = victim.key;
        subject.size_bytes = victim.size_bytes;

        const PlacementContext context = MakeContext(
            deps_, victim.key, event.tiler_id, MovementDirection::kOffload,
            victim.size_bytes, heat.read_heat, *registration);
        auto decision = deps_.placement->Select(context);
        if (!decision.has_value()) return ConsumeResult::kIgnored;

        return Propose(deps_, config_, *decision, subject, *registration,
                       victim.block_id, MovementDirection::kOffload);
    }

   private:
    MovementConsumerConfig config_;
    MovementConsumerDeps deps_;
};

/**
 * @class OnboardConsumer
 */
class OnboardConsumer final : public EventConsumer {
   public:
    OnboardConsumer(const MovementConsumerConfig& config,
                    const MovementConsumerDeps& deps)
        : config_(config), deps_(deps) {}

    Subscription SubscriptionInfo() const override {
        Subscription subscription;
        subscription.name = "onboard";
        subscription.event_mask = SubscriptionMask({EventType::kAccess});
        return subscription;
    }

    ConsumeResult Consume(const BlockEvent& event, DeliveryMode mode) override {
        if (mode == DeliveryMode::kInline) return ConsumeResult::kApplied;
        if (config_.onboard_min_read_heat <= 0.0) {
            return ConsumeResult::kIgnored;  // onboarding disabled
        }
        if (!event.registration.has_value() || !event.block_id.has_value()) {
            return ConsumeResult::kIgnored;
        }

        auto registration = event.registration->Lock();
        if (!registration.has_value() || registration->IsRetired()) {
            return ConsumeResult::kIgnored;
        }
        if (!deps_.registry->IsCanonical(*registration)) {
            return ConsumeResult::kIgnored;
        }

        // From the tracker, and from its read-only counter. The old policy
        // read a sketch that commits also bumped, so writing a key counted
        // towards "this key is in demand".
        const FrequencySnapshot heat =
            deps_.frequency->Get(registration->Id(), event.key);
        if (heat.read_heat < config_.onboard_min_read_heat) {
            return ConsumeResult::kIgnored;
        }

        const PlacementContext context = MakeContext(
            deps_, event.key, event.tiler_id, MovementDirection::kOnboard,
            event.size_bytes, heat.read_heat, *registration);
        auto decision = deps_.placement->Select(context);
        if (!decision.has_value()) return ConsumeResult::kIgnored;

        return Propose(deps_, config_, *decision, event, *registration,
                       *event.block_id, MovementDirection::kOnboard);
    }

   private:
    MovementConsumerConfig config_;
    MovementConsumerDeps deps_;
};

tl::expected<void, ErrorCode> ValidateDeps(const MovementConsumerDeps& deps) {
    if (deps.tilers == nullptr || deps.registry == nullptr ||
        deps.placement == nullptr || deps.frequency == nullptr ||
        deps.movement == nullptr || deps.sink == nullptr ||
        deps.clock == nullptr) {
        LOG(ERROR) << "A movement consumer needs tilers, a registry, a "
                      "placement policy, both trackers, a sink and a clock";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    return {};
}

}  // namespace

tl::expected<void, ErrorCode> ValidateMovementConsumerConfig(
    const MovementConsumerConfig& config) {
    if (!(config.offload_high_watermark > 0.0) ||
        !(config.offload_high_watermark < 1.0)) {
        LOG(ERROR) << "movement.offload_high_watermark must be in (0, 1), got "
                   << config.offload_high_watermark
                   << "; offload has to start before the tier is full or it "
                      "cannot keep ahead of reclamation";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (config.onboard_min_read_heat < 0.0) {
        LOG(ERROR) << "movement.onboard_min_read_heat must not be negative";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (config.movement_deadline <= std::chrono::milliseconds::zero()) {
        LOG(ERROR) << "movement.movement_deadline_ms must be greater than "
                      "zero";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    return {};
}

tl::expected<std::unique_ptr<EventConsumer>, ErrorCode> CreateOffloadConsumer(
    const MovementConsumerConfig& config, const MovementConsumerDeps& deps) {
    auto valid = ValidateMovementConsumerConfig(config);
    if (!valid) return tl::make_unexpected(valid.error());
    auto deps_valid = ValidateDeps(deps);
    if (!deps_valid) return tl::make_unexpected(deps_valid.error());
    return std::make_unique<OffloadConsumer>(config, deps);
}

tl::expected<std::unique_ptr<EventConsumer>, ErrorCode> CreateOnboardConsumer(
    const MovementConsumerConfig& config, const MovementConsumerDeps& deps) {
    auto valid = ValidateMovementConsumerConfig(config);
    if (!valid) return tl::make_unexpected(valid.error());
    auto deps_valid = ValidateDeps(deps);
    if (!deps_valid) return tl::make_unexpected(deps_valid.error());
    return std::make_unique<OnboardConsumer>(config, deps);
}

}  // namespace mooncake::v2
