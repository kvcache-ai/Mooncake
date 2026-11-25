// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0

#include "tent/transport/ub/endpoint_store.h"

#include <chrono>
#include <iterator>
#include <limits>
#include <utility>
#include <vector>

namespace mooncake::tent::ub {
namespace {

// Minimum spacing between quarantine sweeps. A sweep issues native retire
// calls (resetJetty/deleteJetty) while the store lock is held, so an
// unconditional retry would let one failing device serialize failing provider
// operations across every posting thread.
constexpr auto kQuarantineSweepInterval = std::chrono::milliseconds(250);

bool retirementComplete(const std::shared_ptr<UbEndpoint>& endpoint) {
    return endpoint && endpoint->state() == UbEndpoint::State::kDestroyed;
}

Status retirementPending() {
    return Status::TooManyRequests(
        "UB endpoint retirement is waiting for outstanding WRs" LOC_MARK);
}

}  // namespace

EndpointStore::EndpointStore(std::shared_ptr<UrmaAdapter> adapter,
                             size_t max_size, uint32_t jetty_count,
                             JettyOptions jetty_options, SteadyClock clock)
    : adapter_(std::move(adapter)),
      max_size_(max_size),
      jetty_count_(jetty_count),
      jetty_options_(jetty_options),
      clock_(std::move(clock)) {}

EndpointStore::~EndpointStore() {
    auto status = clear();
    if (!status.ok()) {
        // Standalone owners may ignore clear(); preserve unsafe-to-destroy
        // endpoints for process lifetime rather than letting native handle
        // destructors bypass a failed drain fence.
        static auto* leaked = new std::vector<std::shared_ptr<UbEndpoint>>();
        static auto* leaked_mutex = new std::mutex();
        std::scoped_lock lock(mutex_, *leaked_mutex);
        leaked->insert(leaked->end(),
                       std::make_move_iterator(quarantined_.begin()),
                       std::make_move_iterator(quarantined_.end()));
        quarantined_.clear();
    }
}

std::shared_ptr<UbEndpoint> EndpointStore::get(const UbEndpointKey& key) {
    std::shared_ptr<UbEndpoint> result;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = endpoints_.find(key);
        if (it == endpoints_.end()) return nullptr;
        if (!it->second.endpoint || !it->second.endpoint->reusable()) {
            auto retired = std::move(it->second.endpoint);
            endpoints_.erase(it);
            (void)retireLocked(retired);
        } else {
            result = it->second.endpoint;
        }
    }
    return result;
}

Status EndpointStore::getOrCreate(const UbEndpointKey& key,
                                  const UbContextPtr& context,
                                  std::shared_ptr<UbEndpoint>& endpoint) {
    endpoint.reset();
    if (!key.valid() || !context ||
        context->topologyId() != key.local_topology_id || max_size_ == 0 ||
        jetty_count_ == 0) {
        return Status::InvalidArgument(
            "Invalid UB endpoint store request" LOC_MARK);
    }

    {
        std::lock_guard<std::mutex> lock(mutex_);
        auto existing = endpoints_.find(key);
        if (existing != endpoints_.end()) {
            if (existing->second.endpoint &&
                existing->second.endpoint->reusable()) {
                endpoint = existing->second.endpoint;
            } else {
                auto evicted = std::move(existing->second.endpoint);
                endpoints_.erase(existing);
                (void)retireLocked(evicted);
            }
        }

        // A transient provider busy/error must not permanently consume the
        // cache, but sweeping quarantine is only worth its native calls when a
        // slot is actually needed, and it is rate limited so a still-failing
        // device cannot make every posting call pay for a full sweep.
        if (!endpoint && endpoints_.size() + quarantined_.size() >= max_size_) {
            const auto now =
                clock_ ? clock_() : std::chrono::steady_clock::now();
            if (now >= next_quarantine_sweep_) {
                next_quarantine_sweep_ = now + kQuarantineSweepInterval;
                (void)retryQuarantinedLocked();
            }
        }

        while (!endpoint &&
               endpoints_.size() + quarantined_.size() >= max_size_) {
            auto victim = endpoints_.end();
            uint64_t oldest = std::numeric_limits<uint64_t>::max();
            for (auto it = endpoints_.begin(); it != endpoints_.end(); ++it) {
                if (it->second.endpoint &&
                    it->second.endpoint->outstandingWrs() == 0 &&
                    it->second.insertion_order < oldest) {
                    victim = it;
                    oldest = it->second.insertion_order;
                }
            }
            if (victim == endpoints_.end()) {
                return Status::TooManyRequests(
                    "All UB endpoint cache entries are in flight" LOC_MARK);
            }
            auto evicted = std::move(victim->second.endpoint);
            endpoints_.erase(victim);
            (void)retireLocked(evicted);
        }

        if (!endpoint) {
            endpoint = std::make_shared<UbEndpoint>(
                key, context, adapter_, jetty_count_, jetty_options_);
            endpoints_.emplace(key, Entry{endpoint, next_insertion_order_++});
        }
    }

    auto status = endpoint->prepare();
    if (!status.ok()) {
        (void)retire(key, endpoint->generation());
        endpoint.reset();
        return status;
    }
    return Status::OK();
}

bool EndpointStore::retire(const UbEndpointKey& key, uint64_t generation) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = endpoints_.find(key);
    if (it == endpoints_.end() || !it->second.endpoint ||
        it->second.endpoint->generation() != generation) {
        return false;
    }
    auto endpoint = std::move(it->second.endpoint);
    endpoints_.erase(it);
    (void)retireLocked(endpoint);
    return true;
}

bool EndpointStore::retire(const std::shared_ptr<UbEndpoint>& endpoint) {
    return endpoint && retire(endpoint->key(), endpoint->generation());
}

Status EndpointStore::retireLocalDevice(Topology::NicID local_topology_id) {
    std::vector<std::shared_ptr<UbEndpoint>> endpoints;
    std::lock_guard<std::mutex> lock(mutex_);
    for (auto it = endpoints_.begin(); it != endpoints_.end();) {
        if (it->first.local_topology_id != local_topology_id) {
            ++it;
            continue;
        }
        if (it->second.endpoint) {
            endpoints.push_back(std::move(it->second.endpoint));
        }
        it = endpoints_.erase(it);
    }
    for (auto it = quarantined_.begin(); it != quarantined_.end();) {
        if (!*it || (*it)->key().local_topology_id != local_topology_id) {
            ++it;
            continue;
        }
        endpoints.push_back(std::move(*it));
        it = quarantined_.erase(it);
    }

    Status first_error = Status::OK();
    for (auto& endpoint : endpoints) {
        auto status = retireLocked(endpoint);
        if (!status.ok() && first_error.ok()) first_error = status;
    }
    return first_error;
}

Status EndpointStore::clear() {
    std::vector<std::shared_ptr<UbEndpoint>> endpoints;
    std::lock_guard<std::mutex> lock(mutex_);
    endpoints.reserve(endpoints_.size());
    for (auto& [_, entry] : endpoints_) {
        if (entry.endpoint) endpoints.push_back(std::move(entry.endpoint));
    }
    endpoints_.clear();
    for (auto& endpoint : quarantined_) {
        if (endpoint) endpoints.push_back(std::move(endpoint));
    }
    quarantined_.clear();

    Status first_error = Status::OK();
    for (auto& endpoint : endpoints) {
        auto status = retireLocked(endpoint);
        if (!status.ok() && first_error.ok()) first_error = status;
    }
    return first_error;
}

Status EndpointStore::retireLocked(std::shared_ptr<UbEndpoint>& endpoint) {
    if (!endpoint) return Status::OK();
    auto status = endpoint->retire();
    if (status.ok() && !retirementComplete(endpoint)) {
        status = retirementPending();
    }
    if (!status.ok()) quarantined_.push_back(std::move(endpoint));
    return status;
}

Status EndpointStore::retryQuarantinedLocked() {
    std::vector<std::shared_ptr<UbEndpoint>> pending;
    pending.swap(quarantined_);
    Status first_error = Status::OK();
    for (auto& endpoint : pending) {
        auto status = retireLocked(endpoint);
        if (!status.ok() && first_error.ok()) first_error = status;
    }
    return first_error;
}

size_t EndpointStore::size() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return endpoints_.size();
}

}  // namespace mooncake::tent::ub
