#include "metrics/registry.h"

#include <algorithm>
#include <iterator>
#include <stdexcept>
#include <utility>

#include "prometheus/text_serializer.h"

namespace mooncake::metrics {

prometheus::Registry& Registry::prom() { return prom_; }

void Registry::Register(std::shared_ptr<prometheus::Collectable> collectable) {
    if (!collectable) throw std::invalid_argument("collectable is null");
    std::lock_guard<std::mutex> lock(mutex_);
    collectables_.push_back(std::move(collectable));
}

bool Registry::Remove(
    const std::shared_ptr<prometheus::Collectable>& collectable) {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto it =
        std::find(collectables_.begin(), collectables_.end(), collectable);
    if (it == collectables_.end()) {
        return false;
    }
    collectables_.erase(it);
    return true;
}

std::vector<prometheus::MetricFamily> Registry::Collect() const {
    auto families = prom_.Collect();

    std::vector<std::shared_ptr<prometheus::Collectable>> collectables;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        collectables = collectables_;
    }
    for (const auto& collectable : collectables) {
        auto collected = collectable->Collect();
        families.insert(families.end(),
                        std::make_move_iterator(collected.begin()),
                        std::make_move_iterator(collected.end()));
    }

    std::ranges::sort(families, {}, &prometheus::MetricFamily::name);
    return families;
}

std::string Registry::SerializeText() const {
    return prometheus::TextSerializer().Serialize(Collect());
}

Registry& DefaultRegistry() {
    static Registry registry;
    return registry;
}

}  // namespace mooncake::metrics
