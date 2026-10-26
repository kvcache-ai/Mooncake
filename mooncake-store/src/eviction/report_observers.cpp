#include "eviction/report_observers.h"

#include <glog/logging.h>

#include <algorithm>
#include <cstdint>
#include <limits>
#include <string>

#include "master_metric_manager.h"

namespace mooncake {
namespace eviction {
namespace {

int64_t Saturated(uint64_t value) {
    return static_cast<int64_t>(std::min<uint64_t>(
        value, static_cast<uint64_t>(std::numeric_limits<int64_t>::max())));
}

}  // namespace

void MetricsObserver::OnRunEnd(const Scope& scope, const Report& report) {
    auto& metrics = MasterMetricManager::instance();
    if (const TenantId* tenant_id = scope.tenant_id()) {
        if (report.released_bytes > 0) {
            metrics.inc_tenant_evict_bytes(tenant_id->value(),
                                           Saturated(report.released_bytes));
        }
        return;
    }
    // A run with nothing filed had nothing to succeed or fail at.
    if (report.filed == 0) {
        return;
    }
    if (report.MadeProgress()) {
        const auto objects = Saturated(report.evicted_objects);
        const auto bytes = Saturated(report.released_bytes);
        metrics.inc_eviction_success(objects, bytes);
        metrics.inc_mem_eviction_success(objects, bytes);
    } else {
        metrics.inc_eviction_fail();
        metrics.inc_mem_eviction_fail();
    }
}

void LogObserver::OnRunEnd(const Scope& scope, const Report& report) {
    const uint64_t offloads = report.Count(Outcome::kOffloadQueued);
    const TenantId* tenant_id = scope.tenant_id();
    if (tenant_id == nullptr) {
        if (report.filed == 0) {
            VLOG(1) << "[EVICT-DIAG] eviction_base=0 "
                       "(no evictable memory objects)";
            return;
        }
        const double actual_evict_ratio =
            static_cast<double>(report.evicted_objects) / report.filed;
        VLOG(1) << "action=evict_objects"
                << ", evicted_count=" << report.evicted_objects
                << ", offload_deferred=" << offloads
                << ", offload_cap_forced=" << report.offload_cap_drops
                << ", offload_push_failed_forced="
                << report.offload_rejected_drops
                << ", total_freed_size=" << report.released_bytes
                << ", eviction_base=" << report.filed
                << ", actual_evict_ratio=" << actual_evict_ratio;
        LOG(INFO) << "[EVICT-RESULT] evicted_count=" << report.evicted_objects
                  << ", eviction_base=" << report.filed
                  << ", actual_evict_ratio=" << actual_evict_ratio;
    }

    const std::string prefix =
        tenant_id == nullptr ? "[EVICT] " : "[TENANT-EVICT] ";
    const std::string whose = tenant_id == nullptr
                                  ? std::string()
                                  : " for tenant " + tenant_id->value();
    if (report.released_bytes == 0 && offloads > 0) {
        LOG(WARNING) << prefix << "No memory freed" << whose << "; " << offloads
                     << " object(s) deferred for disk offload. "
                        "Consider lowering eviction_high_watermark_ratio.";
    }
    if (report.offload_cap_drops > 0) {
        LOG(WARNING) << prefix << "Offload cap (" << offload_cap_ << ") reached"
                     << whose << "; force-evicted " << report.offload_cap_drops
                     << " object(s) without disk offload.";
    }
    if (report.offload_rejected_drops > 0) {
        LOG(WARNING) << prefix << "PushOffloadingQueue failed" << whose
                     << " for " << report.offload_rejected_drops
                     << " object(s); force-evicted without disk offload "
                        "(offload_force_evict=true).";
    }
}

}  // namespace eviction
}  // namespace mooncake
