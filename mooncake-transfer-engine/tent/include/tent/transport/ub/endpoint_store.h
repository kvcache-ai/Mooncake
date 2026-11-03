// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0

#ifndef TENT_TRANSPORT_UB_ENDPOINT_STORE_H_
#define TENT_TRANSPORT_UB_ENDPOINT_STORE_H_

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <vector>

#include "tent/common/status.h"
#include "tent/transport/ub/context.h"
#include "tent/transport/ub/endpoint.h"
#include "tent/transport/ub/urma_adapter.h"

namespace mooncake::tent::ub {

// Generation-aware endpoint cache. Failed and retired incarnations are
// unpublished before cleanup, so a subsequent lookup always allocates a new
// generation and can never resurrect a failed Jetty set.
class EndpointStore final {
   public:
    // Time source for the quarantine-sweep throttle. Production leaves this
    // empty and uses steady_clock; tests inject a controllable clock so the
    // throttle is not sensitive to scheduler stalls.
    using SteadyClock = std::function<std::chrono::steady_clock::time_point()>;

    EndpointStore(std::shared_ptr<UrmaAdapter> adapter, size_t max_size,
                  uint32_t jetty_count, JettyOptions jetty_options = {},
                  SteadyClock clock = {});
    ~EndpointStore();

    EndpointStore(const EndpointStore&) = delete;
    EndpointStore& operator=(const EndpointStore&) = delete;

    std::shared_ptr<UbEndpoint> get(const UbEndpointKey& key);
    Status getOrCreate(const UbEndpointKey& key, const UbContextPtr& context,
                       std::shared_ptr<UbEndpoint>& endpoint);

    // Only the exact incarnation is removed. A late timeout from generation N
    // therefore cannot evict a replacement generation N+1.
    bool retire(const UbEndpointKey& key, uint64_t generation);
    bool retire(const std::shared_ptr<UbEndpoint>& endpoint);
    // Atomically unpublishes every endpoint backed by a failed local device.
    // Cleanup errors are quarantined for clear()/shutdown retry.
    Status retireLocalDevice(Topology::NicID local_topology_id);
    Status clear();
    [[nodiscard]] size_t size() const;

   private:
    struct Entry {
        std::shared_ptr<UbEndpoint> endpoint;
        uint64_t insertion_order{0};
    };

    // Called with mutex_ held. Native retirement remains under the store lock
    // so another failure-progress poll cannot observe an endpoint in the gap
    // between unpublication and quarantine insertion.
    Status retireLocked(std::shared_ptr<UbEndpoint>& endpoint);
    Status retryQuarantinedLocked();

    std::shared_ptr<UrmaAdapter> adapter_;
    const size_t max_size_;
    const uint32_t jetty_count_;
    const JettyOptions jetty_options_;
    const SteadyClock clock_;
    mutable std::mutex mutex_;
    std::unordered_map<UbEndpointKey, Entry, UbEndpointKeyHash> endpoints_;
    // Unpublished endpoints whose native cleanup failed remain owned until a
    // later clear()/shutdown retry. They must never fall through a destructor
    // while an ERROR Jetty still lacks its flush fence.
    std::vector<std::shared_ptr<UbEndpoint>> quarantined_;
    uint64_t next_insertion_order_{1};
    // Earliest time the next quarantine sweep may run. Default initialized so
    // the first sweep is always allowed.
    std::chrono::steady_clock::time_point next_quarantine_sweep_{};
};

using UbEndpointStore = EndpointStore;

}  // namespace mooncake::tent::ub

#endif  // TENT_TRANSPORT_UB_ENDPOINT_STORE_H_
