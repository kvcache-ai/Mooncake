#include "eviction/report_observers.h"

#include <glog/logging.h>

#include <algorithm>
#include <chrono>
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
    const TenantId* tenant_id = scope.tenant_id();
    if (tenant_id != nullptr && report.released_bytes > 0) {
        MasterMetricManager::instance().inc_tenant_evict_bytes(
            tenant_id->value(), Saturated(report.released_bytes));
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
                  << ", actual_evict_ratio=" << actual_evict_ratio
                  << ", visited=" << report.visited
                  << ", moved=" << report.moved << ", busy=" << report.busy
                  << ", lease_active=" << report.lease_active
                  << ", scanned_slots=" << report.scanned_slots
                  << ", scan_waves=" << report.scan_waves
                  << ", workers=" << report.workers;
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

void AgeObserver::OnApplied(const ObjectView& view, const Effect& effect) {
    // Counted exactly like the run's evicted_objects: an object that
    // released nothing is not an eviction sample.
    if (effect.released_bytes == 0) {
        return;
    }
    MasterMetricManager::instance().observe_evicted_key_age_seconds(
        std::chrono::duration<double>(std::chrono::system_clock::now() -
                                      view.metadata.put_start_time)
            .count());
}

}  // namespace eviction
}  // namespace mooncake
