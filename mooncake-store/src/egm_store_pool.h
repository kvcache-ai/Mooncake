#pragma once

#include <cstddef>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include <ylt/util/tl/expected.hpp>

#include "types.h"

namespace mooncake {

template <typename T>
using EgmStorePoolResult = tl::expected<T, std::string>;

struct EgmStorePoolOptions {
    bool enabled = false;
    bool auto_nodes = true;
    std::vector<int> nodes;
};

struct EgmStorePoolNodePlan {
    int node_id = -1;
    size_t allocation_granularity = 0;
    size_t effective_bytes = 0;
};

struct EgmStorePoolChunkPlan {
    int node_id = -1;
    size_t chunk_bytes = 0;
    size_t plan_index = 0;
};

struct EgmStorePoolPlan {
    size_t requested_total = 0;
    size_t effective_total = 0;
    size_t common_alignment = 0;
    std::vector<EgmStorePoolNodePlan> nodes;
    std::vector<EgmStorePoolChunkPlan> chunks;
};

// Environment-dependent discovery is kept behind this interface so parsing,
// discovery policy, and local-node selection can run in ordinary CPU tests.
class EgmStorePoolEnvironment {
   public:
    virtual ~EgmStorePoolEnvironment() = default;

    virtual EgmStorePoolResult<std::vector<int>> VisibleCudaDevices() const = 0;
    virtual EgmStorePoolResult<std::string> PciBdfForCudaDevice(
        int device_id) const = 0;
    virtual EgmStorePoolResult<int> ReadPciNumaNode(
        const std::string& pci_bdf) const = 0;
    virtual bool IsNumaNodeOnline(int node_id) const = 0;
    virtual EgmStorePoolResult<int> CurrentCpu() const = 0;
    virtual EgmStorePoolResult<int> NumaNodeForCpu(int cpu_id) const = 0;
};

// Parse only the ConfigDict-only EGM Store Pool controls. The NUMA expression
// is deliberately ignored when it cannot affect a nonzero global pool.
EgmStorePoolResult<EgmStorePoolOptions> ParseEgmStorePoolOptions(
    const ConfigDict& config, size_t global_segment_size);

// Resolve the sorted global NUMA node set. Returns an empty set when the
// feature is disabled or global_segment_size is zero.
EgmStorePoolResult<std::vector<int>> DiscoverEgmStorePoolNodes(
    const EgmStorePoolOptions& options, size_t global_segment_size,
    const EgmStorePoolEnvironment& environment);

// Resolve setup CPU locality for a nonzero EGM local workspace.
// Disabled/zero-size configurations return std::nullopt without discovery.
EgmStorePoolResult<std::optional<int>> ResolveEgmStorePoolLocalNode(
    const EgmStorePoolOptions& options, size_t local_buffer_size,
    const EgmStorePoolEnvironment& environment);

// Build a deterministic NUMA/chunk plan. node_granularities may arrive in any
// order; the result is sorted by node ID. store_alignment defaults to the
// Cachelib slab size in the implementation so each Store segment is mountable.
EgmStorePoolResult<EgmStorePoolPlan> PlanEgmStorePoolCapacity(
    size_t requested_total,
    const std::vector<std::pair<int, size_t>>& node_granularities,
    size_t max_mr_size, size_t store_alignment = 0);

std::unique_ptr<EgmStorePoolEnvironment>
CreateProductionEgmStorePoolEnvironment();

}  // namespace mooncake
