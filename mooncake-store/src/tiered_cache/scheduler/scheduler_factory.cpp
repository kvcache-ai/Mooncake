#include "tiered_cache/scheduler/scheduler_factory.h"

#include <glog/logging.h>

#include <limits>
#include <memory>
#include <string>

#include "tiered_cache/event_driven_scheduler/event_driven_client_scheduler.h"
#include "tiered_cache/event_driven_scheduler/event_driven_policy.h"
#include "tiered_cache/event_driven_scheduler/json_config_util.h"
#include "tiered_cache/event_driven_scheduler/multi_lru_policy.h"
#include "tiered_cache/scheduler/client_scheduler.h"

namespace mooncake {

namespace detail {

MultiLRUPolicy::Config ReadMultiLRUConfig(const Json::Value& config) {
    MultiLRUPolicy::Config c;
    // Floating-trigger bounds. New keys evict_watermark_low/high; the legacy
    // keys user_floor/evict_watermark are accepted as deprecated fallbacks.
    const Json::Value* sched = ed_config::Node(config);
    if (sched && (sched->isMember("user_floor") ||
                  sched->isMember("evict_watermark"))) {
        LOG(WARNING) << "scheduler.user_floor / scheduler.evict_watermark are "
                        "deprecated; prefer evict_watermark_low / "
                        "evict_watermark_high";
    }
    c.evict_watermark_low = ed_config::ReadDouble(
        config, "evict_watermark_low",
        ed_config::ReadDouble(config, "user_floor", c.evict_watermark_low));
    c.evict_watermark_high = ed_config::ReadDouble(
        config, "evict_watermark_high",
        ed_config::ReadDouble(config, "evict_watermark",
                              c.evict_watermark_high));
    c.limit_watermark =
        ed_config::ReadDouble(config, "limit_watermark", c.limit_watermark);
    c.evict_rate_k =
        ed_config::ReadDouble(config, "evict_rate_k", c.evict_rate_k);
    c.evict_load_window_s = ed_config::ReadDouble(config, "evict_load_window_s",
                                                  c.evict_load_window_s);
    c.offload_freq_threshold = static_cast<uint64_t>(ed_config::ReadSize(
        config, "offload_freq_threshold", c.offload_freq_threshold));
    c.onboard_freq_threshold = static_cast<uint64_t>(ed_config::ReadSize(
        config, "onboard_freq_threshold", c.onboard_freq_threshold));
    c.onboard_fast_threshold = ed_config::ReadDouble(
        config, "onboard_fast_threshold", c.onboard_fast_threshold);
    c.sketch_capacity =
        ed_config::ReadSize(config, "sketch_capacity", c.sketch_capacity);
    c.candidate_scan_limit = ed_config::ReadSize(config, "candidate_scan_limit",
                                                 c.candidate_scan_limit);
    if (c.candidate_scan_limit == 0) {
        c.candidate_scan_limit = std::numeric_limits<size_t>::max();
    }

    // Sanity-check the watermark ordering (low <= high <= limit) and the load
    // window (> 0). Clamp + warn rather than silently mis-reclaiming.
    if (c.evict_watermark_high < c.evict_watermark_low) {
        LOG(WARNING) << "evict_watermark_high (" << c.evict_watermark_high
                     << ") < evict_watermark_low (" << c.evict_watermark_low
                     << "); clamping high up to low";
        c.evict_watermark_high = c.evict_watermark_low;
    }
    if (c.limit_watermark < c.evict_watermark_high) {
        LOG(WARNING) << "limit_watermark (" << c.limit_watermark
                     << ") < evict_watermark_high (" << c.evict_watermark_high
                     << "); clamping limit_watermark up to high";
        c.limit_watermark = c.evict_watermark_high;
    }
    if (c.evict_load_window_s <= 0.0) {
        LOG(WARNING) << "evict_load_window_s (" << c.evict_load_window_s
                     << ") must be > 0; resetting to default";
        c.evict_load_window_s = MultiLRUPolicy::Config{}.evict_load_window_s;
    }
    return c;
}

}  // namespace detail

namespace {

// Build the event-driven policy selected by config["scheduler"]["policy"].
// Defaults to (and currently only provides) the MultiLRU policy. New policies
// plug in here without touching the scheduler.
std::unique_ptr<EventDrivenPolicy> MakeEventDrivenPolicy(
    const Json::Value& config) {
    const std::string policy = ed_config::ReadString(config, "policy", "multi_lru");
    if (policy != "multi_lru") {
        LOG(WARNING) << "Unknown event-driven policy '" << policy
                     << "', falling back to multi_lru";
    }
    return std::make_unique<MultiLRUPolicy>(detail::ReadMultiLRUConfig(config));
}

}  // namespace

std::unique_ptr<IClientScheduler> MakeClientScheduler(
    TieredBackend* backend, const Json::Value& config) {
    std::string type = "legacy";
    if (config.isMember("scheduler") &&
        config["scheduler"].isMember("type")) {
        type = config["scheduler"]["type"].asString();
    }

    if (type == "event_driven") {
        LOG(INFO) << "Creating EventDrivenClientScheduler";
        return std::make_unique<EventDrivenClientScheduler>(
            backend, config, MakeEventDrivenPolicy(config));
    }

    if (type != "legacy") {
        LOG(WARNING) << "Unknown scheduler.type '" << type
                     << "', falling back to legacy";
    }
    LOG(INFO) << "Creating LegacyClientScheduler (scheduler.type=" << type
              << ")";
    return std::make_unique<LegacyClientScheduler>(backend, config);
}

}  // namespace mooncake
