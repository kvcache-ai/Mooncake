#pragma once

// The observers every eviction run reports to: its metrics and its log.

#include <cstdint>

#include "eviction/observer.h"

namespace mooncake {
namespace eviction {

// Eviction counters in MasterMetricManager: the bytes each tenant's runs
// released. A pool-wide run's success or failure is counted by its caller,
// which may also have freed memory outside the run.
class MetricsObserver final : public EvictionObserver {
   public:
    bool SupportsParallel() const override { return true; }

    void OnRunEnd(const Scope& scope, const Report& report) override;
};

// A line per run, and warnings for offloads that held memory back or were
// skipped.
class LogObserver final : public EvictionObserver {
   public:
    // `offload_cap` is the run's offload limit, for the warning.
    explicit LogObserver(uint64_t offload_cap) : offload_cap_(offload_cap) {}

    bool SupportsParallel() const override { return true; }

    void OnRunEnd(const Scope& scope, const Report& report) override;

   private:
    const uint64_t offload_cap_;
};

}  // namespace eviction
}  // namespace mooncake
