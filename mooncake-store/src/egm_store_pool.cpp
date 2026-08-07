// Copyright 2024 KVCache.AI

#include "egm_store_pool.h"

#include <Slab.h>

#include <algorithm>
#include <cstdint>
#include <limits>
#include <numeric>
#include <set>
#include <string_view>
#include <utility>

#include "ascii_string.h"
#include "bool_parser.h"
#include "integer_parser.h"
#include "transport/nvlink_transport/nvlink_host_numa_allocation.h"

namespace mooncake {
namespace {

template <typename T>
EgmStorePoolResult<T> Error(std::string message) {
    return tl::make_unexpected(std::move(message));
}

EgmStorePoolResult<std::vector<int>> ParseNodes(const std::string& expression) {
    std::set<int> nodes;
    const std::string_view expression_view(expression);
    size_t begin = 0;
    while (begin <= expression_view.size()) {
        const size_t comma = expression_view.find(',', begin);
        const size_t end =
            comma == std::string::npos ? expression_view.size() : comma;
        const std::string_view token =
            TrimAsciiWhitespace(expression_view.substr(begin, end - begin));
        const auto node = TryParseInteger<int>(token);
        if (!node || *node < 0) {
            return Error<std::vector<int>>("invalid EGM NUMA node: " +
                                           std::string(token));
        }
        nodes.insert(*node);
        if (comma == std::string::npos) break;
        begin = comma + 1;
    }
    if (nodes.empty()) {
        return Error<std::vector<int>>("EGM NUMA node list is empty");
    }
    return std::vector<int>(nodes.begin(), nodes.end());
}

EgmStorePoolResult<size_t> CheckedLcm(size_t left, size_t right) {
    if (left == 0 || right == 0) {
        return Error<size_t>("EGM alignment must be nonzero");
    }
    const size_t reduced = left / std::gcd(left, right);
    if (reduced > std::numeric_limits<size_t>::max() / right) {
        return Error<size_t>("EGM alignment overflows size_t");
    }
    return reduced * right;
}

class NvlinkAllocation final : public EgmStorePoolAllocation {
   public:
    explicit NvlinkAllocation(
        std::unique_ptr<NvlinkHostNumaAllocation> allocation)
        : allocation_(std::move(allocation)) {}

    void* base() const override { return allocation_->base(); }
    size_t length() const override { return allocation_->length(); }

    EgmStorePoolResult<void> Release() override {
        Status status = allocation_->Release();
        if (!status.ok()) return Error<void>(status.ToString());
        allocation_.reset();
        return {};
    }

   private:
    std::unique_ptr<NvlinkHostNumaAllocation> allocation_;
};

}  // namespace

EgmStorePoolResult<EgmStorePoolOptions> ParseEgmStorePoolOptions(
    const ConfigDict& config) {
    EgmStorePoolOptions options;
    auto enabled = config.find(CONFIG_KEY_ENABLE_EGM_STORE_POOL);
    if (enabled != config.end()) {
        const auto parsed = TryParseBool(
            enabled->second, {.token_set = BoolTokenSet::kTrueFalse});
        if (!parsed) {
            return Error<EgmStorePoolOptions>(
                "invalid enable_egm_store_pool value: " + enabled->second);
        }
        options.enabled = *parsed;
    }
    if (!options.enabled) return options;

    auto nodes = config.find(CONFIG_KEY_EGM_NUMA_NODES);
    if (nodes == config.end() || TrimAsciiWhitespace(nodes->second) == "auto") {
        return options;
    }
    auto parsed = ParseNodes(nodes->second);
    if (!parsed) return Error<EgmStorePoolOptions>(parsed.error());
    options.auto_nodes = false;
    options.nodes = std::move(*parsed);
    return options;
}

EgmStorePoolResult<void> ValidateEgmStorePoolOptions(
    const EgmStorePoolOptions& options, const std::string& protocol,
    size_t global_segment_size, size_t local_buffer_size) {
    if (!options.enabled) return {};
    if (protocol != "nvlink") {
        return Error<void>("EGM Store Pool requires protocol=nvlink");
    }
    if (global_segment_size == 0) {
        return Error<void>("EGM Store Pool requires global_segment_size>0");
    }
    if (local_buffer_size != 0) {
        return Error<void>("EGM Store Pool requires local_buffer_size=0");
    }
    if (!options.auto_nodes && options.nodes.empty()) {
        return Error<void>("explicit EGM NUMA node list is empty");
    }
    return {};
}

EgmStorePoolResult<EgmStorePoolPlan> PlanEgmStorePool(
    size_t requested_bytes,
    const std::vector<std::pair<int, size_t>>& node_granularities,
    size_t max_mr_size, size_t store_alignment) {
    if (requested_bytes == 0 || node_granularities.empty()) {
        return Error<EgmStorePoolPlan>("EGM capacity and nodes are required");
    }
    if (store_alignment == 0) {
        store_alignment = facebook::cachelib::Slab::kSize;
    }

    auto nodes = node_granularities;
    std::sort(nodes.begin(), nodes.end());
    size_t alignment = store_alignment;
    int previous = -1;
    for (const auto& [node, granularity] : nodes) {
        if (node < 0 || node == previous) {
            return Error<EgmStorePoolPlan>("invalid or duplicate NUMA node");
        }
        auto lcm = CheckedLcm(alignment, granularity);
        if (!lcm) return Error<EgmStorePoolPlan>(lcm.error());
        alignment = *lcm;
        previous = node;
    }

    const size_t max_chunk = (max_mr_size / alignment) * alignment;
    const size_t effective = (requested_bytes / alignment) * alignment;
    const size_t units = effective / alignment;
    if (max_chunk == 0 || units < nodes.size()) {
        return Error<EgmStorePoolPlan>(
            "EGM capacity or max_mr_size is below the required alignment");
    }

    EgmStorePoolPlan plan;
    plan.requested_bytes = requested_bytes;
    plan.effective_bytes = effective;
    plan.alignment = alignment;
    const size_t base_units = units / nodes.size();
    const size_t remainder = units % nodes.size();
    for (size_t index = 0; index < nodes.size(); ++index) {
        const size_t node_units = base_units + (index < remainder ? 1 : 0);
        const size_t node_bytes = node_units * alignment;
        plan.nodes.push_back(
            {nodes[index].first, nodes[index].second, node_bytes});
        for (size_t remaining = node_bytes; remaining != 0;) {
            const size_t chunk = std::min(remaining, max_chunk);
            plan.chunks.push_back({nodes[index].first, chunk});
            remaining -= chunk;
        }
    }
    return plan;
}

EgmStorePoolHooks MakeNvlinkHostNumaHooks(EgmStorePoolHooks hooks) {
    hooks.discover_nodes = [] {
        std::vector<int> nodes;
        Status status = NvlinkHostNumaAllocation::DiscoverHostNumaNodes(nodes);
        if (!status.ok()) {
            return Error<std::vector<int>>(status.ToString());
        }
        return EgmStorePoolResult<std::vector<int>>(std::move(nodes));
    };
    hooks.get_granularity = [](int node) {
        size_t granularity = 0;
        Status status = NvlinkHostNumaAllocation::GetAllocationGranularity(
            node, granularity);
        if (!status.ok()) return Error<size_t>(status.ToString());
        return EgmStorePoolResult<size_t>(granularity);
    };
    hooks.allocate = [](int node, size_t length, size_t alignment) {
        std::unique_ptr<NvlinkHostNumaAllocation> allocation;
        Status status = NvlinkHostNumaAllocation::Create(node, length,
                                                         alignment, allocation);
        EgmStorePoolAllocationAttempt attempt;
        if (allocation) {
            attempt.allocation =
                std::make_unique<NvlinkAllocation>(std::move(allocation));
        }
        if (!status.ok()) {
            attempt.error = status.ToString();
        } else if (!attempt.allocation) {
            attempt.error = "HOST_NUMA allocation returned no owner";
        }
        return attempt;
    };
    return hooks;
}

EgmStorePool::EgmStorePool(EgmStorePoolHooks hooks)
    : hooks_(std::move(hooks)) {}

EgmStorePoolResult<void> EgmStorePool::Setup(const EgmStorePoolOptions& options,
                                             const std::string& protocol,
                                             size_t global_segment_size,
                                             size_t local_buffer_size,
                                             size_t max_mr_size,
                                             size_t store_alignment) {
    if (!records_.empty()) return Error<void>("EGM Store Pool is not empty");
    auto valid = ValidateEgmStorePoolOptions(
        options, protocol, global_segment_size, local_buffer_size);
    if (!valid || !options.enabled) return valid;
    if (!hooks_.discover_nodes || !hooks_.get_granularity || !hooks_.allocate ||
        !hooks_.mount || !hooks_.unmount) {
        return Error<void>("EGM Store Pool hooks are incomplete");
    }

    std::vector<int> nodes = options.nodes;
    if (options.auto_nodes) {
        auto discovered = hooks_.discover_nodes();
        if (!discovered) return Error<void>(discovered.error());
        nodes = std::move(*discovered);
    }
    std::sort(nodes.begin(), nodes.end());
    nodes.erase(std::unique(nodes.begin(), nodes.end()), nodes.end());
    if (nodes.empty() || nodes.front() < 0) {
        return Error<void>("EGM NUMA discovery returned no valid nodes");
    }

    std::vector<std::pair<int, size_t>> granularities;
    for (int node : nodes) {
        auto granularity = hooks_.get_granularity(node);
        if (!granularity) return Error<void>(granularity.error());
        granularities.emplace_back(node, *granularity);
    }
    auto plan = PlanEgmStorePool(global_segment_size, granularities,
                                 max_mr_size, store_alignment);
    if (!plan) return Error<void>(plan.error());
    plan_ = std::move(*plan);

    records_.reserve(plan_.chunks.size());
    for (const auto& chunk : plan_.chunks) {
        auto attempt =
            hooks_.allocate(chunk.node, chunk.bytes, plan_.alignment);
        const bool has_owner = attempt.allocation != nullptr;
        if (attempt.allocation) {
            records_.push_back({std::move(attempt.allocation), std::nullopt});
        }
        if (!attempt.error.empty()) return Rollback(attempt.error);
        if (!has_owner) {
            return Rollback("HOST_NUMA allocation returned no owner");
        }
        const auto& owner = records_.back().allocation;
        if (!owner || owner->base() == nullptr ||
            owner->length() != chunk.bytes ||
            reinterpret_cast<uintptr_t>(owner->base()) % plan_.alignment != 0) {
            return Rollback("HOST_NUMA allocation violated the EGM plan");
        }
    }

    for (auto& record : records_) {
        record.segment_id = generate_uuid();
        auto mounted =
            hooks_.mount(*record.segment_id, record.allocation->base(),
                         record.allocation->length());
        if (!mounted) return Rollback(mounted.error());
    }
    return {};
}

EgmStorePoolResult<void> EgmStorePool::Rollback(
    const std::string& setup_error) {
    auto cleanup = Teardown();
    if (!cleanup) {
        return Error<void>(setup_error +
                           "; rollback incomplete: " + cleanup.error());
    }
    return Error<void>(setup_error);
}

EgmStorePoolResult<void> EgmStorePool::Teardown() {
    bool complete = true;
    for (auto it = records_.rbegin(); it != records_.rend(); ++it) {
        if (!it->segment_id) continue;
        auto result = hooks_.unmount(*it->segment_id);
        if (!result) {
            complete = false;
            continue;
        }
        it->segment_id.reset();
    }
    if (!complete) return Error<void>("EGM publication cleanup failed");

    for (auto it = records_.rbegin(); it != records_.rend(); ++it) {
        if (!it->allocation) continue;
        auto result = it->allocation->Release();
        if (!result) {
            complete = false;
            continue;
        }
        it->allocation.reset();
    }
    if (!complete) return Error<void>("EGM allocation release failed");
    records_.clear();
    plan_ = {};
    return {};
}

}  // namespace mooncake
