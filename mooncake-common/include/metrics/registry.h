#pragma once

#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <vector>

#include "prometheus/collectable.h"
#include "prometheus/metric_family.h"
#include "prometheus/registry.h"

namespace mooncake::metrics {

inline constexpr std::string_view kPrometheusContentType =
    "text/plain; version=0.0.4; charset=utf-8";

class Registry {
   public:
    prometheus::Registry& prom();
    void Register(std::shared_ptr<prometheus::Collectable> collectable);
    bool Remove(const std::shared_ptr<prometheus::Collectable>& collectable);
    std::vector<prometheus::MetricFamily> Collect() const;
    std::string SerializeText() const;

   private:
    prometheus::Registry prom_;
    mutable std::mutex mutex_;
    std::vector<std::shared_ptr<prometheus::Collectable>> collectables_;
};

Registry& DefaultRegistry();

}  // namespace mooncake::metrics
