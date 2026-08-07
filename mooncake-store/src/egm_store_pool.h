// Copyright 2024 KVCache.AI

#pragma once

#include <cstddef>
#include <functional>
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
    int node = -1;
    size_t granularity = 0;
    size_t bytes = 0;
};

struct EgmStorePoolChunk {
    int node = -1;
    size_t bytes = 0;
};

struct EgmStorePoolPlan {
    size_t requested_bytes = 0;
    size_t effective_bytes = 0;
    size_t alignment = 0;
    std::vector<EgmStorePoolNodePlan> nodes;
    std::vector<EgmStorePoolChunk> chunks;
};

EgmStorePoolResult<EgmStorePoolOptions> ParseEgmStorePoolOptions(
    const ConfigDict& config);

EgmStorePoolResult<void> ValidateEgmStorePoolOptions(
    const EgmStorePoolOptions& options, const std::string& protocol,
    size_t global_segment_size, size_t local_buffer_size);

EgmStorePoolResult<EgmStorePoolPlan> PlanEgmStorePool(
    size_t requested_bytes,
    const std::vector<std::pair<int, size_t>>& node_granularities,
    size_t max_mr_size, size_t store_alignment = 0);

class EgmStorePoolAllocation {
   public:
    virtual ~EgmStorePoolAllocation() = default;
    virtual void* base() const = 0;
    virtual size_t length() const = 0;
    virtual EgmStorePoolResult<void> Release() = 0;
};

struct EgmStorePoolAllocationAttempt {
    // A failed creation may still return an owner whose rollback must be
    // retried; error and allocation are therefore intentionally independent.
    std::unique_ptr<EgmStorePoolAllocation> allocation;
    std::string error;
};

// Mount is the feature-scoped Store operation that registers an ordinary
// nvlink segment and publishes its caller-generated ID to Master. Unmount
// reverses both steps, including an ambiguous or partially completed mount.
struct EgmStorePoolHooks {
    std::function<EgmStorePoolResult<std::vector<int>>()> discover_nodes;
    std::function<EgmStorePoolResult<size_t>(int)> get_granularity;
    std::function<EgmStorePoolAllocationAttempt(int, size_t, size_t)> allocate;
    std::function<EgmStorePoolResult<void>(const UUID&, void*, size_t)> mount;
    std::function<EgmStorePoolResult<void>(const UUID&)> unmount;
};

// Supplies only the HOST_NUMA discovery, granularity, and allocation hooks.
// The caller provides the existing Store mount/unmount callbacks.
EgmStorePoolHooks MakeNvlinkHostNumaHooks(EgmStorePoolHooks hooks);

class EgmStorePool {
   public:
    explicit EgmStorePool(EgmStorePoolHooks hooks);
    EgmStorePool(const EgmStorePool&) = delete;
    EgmStorePool& operator=(const EgmStorePool&) = delete;

    EgmStorePoolResult<void> Setup(const EgmStorePoolOptions& options,
                                   const std::string& protocol,
                                   size_t global_segment_size,
                                   size_t local_buffer_size, size_t max_mr_size,
                                   size_t store_alignment = 0);
    EgmStorePoolResult<void> Teardown();

    const EgmStorePoolPlan& plan() const { return plan_; }
    bool hasOwnership() const { return !records_.empty(); }

   private:
    struct Record {
        std::unique_ptr<EgmStorePoolAllocation> allocation;
        std::optional<UUID> segment_id;
    };

    EgmStorePoolResult<void> Rollback(const std::string& setup_error);

    EgmStorePoolHooks hooks_;
    EgmStorePoolPlan plan_;
    std::vector<Record> records_;
};

}  // namespace mooncake
