// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0

#include "tent/transport/ub/endpoint_store.h"

#include <iterator>
#include <limits>
#include <utility>
#include <vector>

namespace mooncake::tent::ub {

EndpointStore::EndpointStore(std::shared_ptr<UrmaAdapter> adapter,
                             size_t max_size, uint32_t jetty_count,
                             JettyOptions jetty_options)
    : adapter_(std::move(adapter)),
      max_size_(max_size),
      jetty_count_(jetty_count),
      jetty_options_(jetty_options) {}

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
    std::shared_ptr<UbEndpoint> retired;
    std::shared_ptr<UbEndpoint> result;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = endpoints_.find(key);
        if (it == endpoints_.end()) return nullptr;
        if (!it->second.endpoint || !it->second.endpoint->reusable()) {
            retired = std::move(it->second.endpoint);
            endpoints_.erase(it);
        } else {
            result = it->second.endpoint;
        }
    }
    if (retired) {
        auto status = retired->retire();
        if (!status.ok()) {
            std::lock_guard<std::mutex> lock(mutex_);
            quarantined_.push_back(std::move(retired));
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

    std::shared_ptr<UbEndpoint> evicted;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        auto existing = endpoints_.find(key);
        if (existing != endpoints_.end()) {
            if (existing->second.endpoint &&
                existing->second.endpoint->reusable()) {
                endpoint = existing->second.endpoint;
            } else {
                evicted = std::move(existing->second.endpoint);
                endpoints_.erase(existing);
            }
        }

        if (!endpoint && endpoints_.size() + quarantined_.size() >= max_size_) {
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
            if (!evicted) evicted = std::move(victim->second.endpoint);
            endpoints_.erase(victim);
        }

        if (!endpoint) {
            endpoint = std::make_shared<UbEndpoint>(
                key, context, adapter_, jetty_count_, jetty_options_);
            endpoints_.emplace(key, Entry{endpoint, next_insertion_order_++});
        }
    }
    if (evicted) {
        auto evict_status = evicted->retire();
        if (!evict_status.ok()) {
            std::lock_guard<std::mutex> lock(mutex_);
            quarantined_.push_back(std::move(evicted));
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
    std::shared_ptr<UbEndpoint> endpoint;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = endpoints_.find(key);
        if (it == endpoints_.end() || !it->second.endpoint ||
            it->second.endpoint->generation() != generation) {
            return false;
        }
        endpoint = std::move(it->second.endpoint);
        endpoints_.erase(it);
    }
    auto status = endpoint->retire();
    if (!status.ok()) {
        std::lock_guard<std::mutex> lock(mutex_);
        quarantined_.push_back(std::move(endpoint));
    }
    return true;
}

bool EndpointStore::retire(const std::shared_ptr<UbEndpoint>& endpoint) {
    return endpoint && retire(endpoint->key(), endpoint->generation());
}

Status EndpointStore::clear() {
    std::vector<std::shared_ptr<UbEndpoint>> endpoints;
    {
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
    }
    Status first_error = Status::OK();
    std::vector<std::shared_ptr<UbEndpoint>> failed;
    for (auto& endpoint : endpoints) {
        auto status = endpoint->retire();
        if (!status.ok()) {
            if (first_error.ok()) first_error = status;
            failed.push_back(std::move(endpoint));
        }
    }
    if (!failed.empty()) {
        std::lock_guard<std::mutex> lock(mutex_);
        quarantined_.insert(quarantined_.end(),
                            std::make_move_iterator(failed.begin()),
                            std::make_move_iterator(failed.end()));
    }
    return first_error;
}

size_t EndpointStore::size() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return endpoints_.size();
}

}  // namespace mooncake::tent::ub
