// Copyright 2024 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "tent/transport/rdma/endpoint_store.h"

#include <glog/logging.h>

#include <atomic>
#include <cassert>
#include <cstddef>
#include <memory>
#include <utility>

#include "tent/common/status.h"
#include "tent/transport/rdma/context.h"
#include "tent/transport/rdma/endpoint.h"

namespace mooncake {
namespace tent {
std::shared_ptr<RdmaEndPoint> FIFOEndpointStore::get(const std::string& key) {
    RWSpinlock::ReadGuard guard(endpoint_map_lock_);
    auto iter = endpoint_map_.find(key);
    if (iter != endpoint_map_.end()) return iter->second;
    return nullptr;
}

std::shared_ptr<RdmaEndPoint> FIFOEndpointStore::getOrInsert(
    const std::string& key) {
    auto endpoint = get(key);
    // Endpoints have unidirectional lifecycle - if existing endpoint is
    // in terminal state (destroying/destroyed), remove it and create new.
    if (endpoint) {
        auto status = endpoint->status();
        if (status == RdmaEndPoint::EP_DESTROYING ||
            status == RdmaEndPoint::EP_DESTROYED) {
            remove(endpoint.get());
            endpoint = nullptr;
        } else {
            return endpoint;
        }
    }
    RWSpinlock::WriteGuard guard(endpoint_map_lock_);
    // Double-check after acquiring write lock
    auto iter = endpoint_map_.find(key);
    if (iter != endpoint_map_.end()) {
        auto ep = iter->second;
        auto status = ep->status();
        if (status == RdmaEndPoint::EP_DESTROYING ||
            status == RdmaEndPoint::EP_DESTROYED) {
            waiting_list_.insert(ep);
            endpoint_map_.erase(iter);
            auto fifo_iter = fifo_map_[key];
            fifo_list_.erase(fifo_iter);
            fifo_map_.erase(key);
            // Fall through to create new endpoint
        } else {
            return ep;
        }
    }
    endpoint = std::make_shared<RdmaEndPoint>();
    int ret = endpoint->construct(&context_, &context_.params().endpoint, key);
    if (ret) {
        LOG(ERROR) << "Failed to construct endpoint for key " << key;
        return nullptr;
    }
    while (this->size() >= max_size_) evictOne();
    endpoint_map_[key] = endpoint;
    fifo_list_.push_back(key);
    auto it = fifo_list_.end();
    fifo_map_[key] = --it;
    return endpoint;
}

int FIFOEndpointStore::remove(RdmaEndPoint* ep) {
    RWSpinlock::WriteGuard guard(endpoint_map_lock_);
    // Search for and remove the endpoint by pointer comparison
    for (auto iter = endpoint_map_.begin(); iter != endpoint_map_.end();
         ++iter) {
        if (iter->second.get() == ep) {
            waiting_list_.insert(iter->second);
            iter->second->beginDestroy();
            auto fifo_iter = fifo_map_[iter->first];
            fifo_list_.erase(fifo_iter);
            fifo_map_.erase(iter->first);
            endpoint_map_.erase(iter);
            return 0;
        }
    }
    // If not found in endpoint_map, check if it's in waiting_list
    for (const auto& waiting_ep : waiting_list_) {
        if (waiting_ep.get() == ep) {
            // Already in waiting list, no action needed
            return 0;
        }
    }
    return -1;  // Endpoint not found
}

void FIFOEndpointStore::evictOne() {
    if (fifo_list_.empty()) return;
    std::string victim = fifo_list_.front();
    fifo_list_.pop_front();
    fifo_map_.erase(victim);
    auto victim_endpoint = endpoint_map_[victim];
    victim_endpoint->beginDestroy();
    waiting_list_.insert(victim_endpoint);
    endpoint_map_.erase(victim);
    LOG(INFO) << victim << " evicted from FIFOEndpointStore";
}

void FIFOEndpointStore::reclaim() {
    RWSpinlock::WriteGuard guard(endpoint_map_lock_);
    std::vector<std::shared_ptr<RdmaEndPoint>> to_delete;
    for (auto& endpoint : waiting_list_) {
        if (endpoint->finishDestroy()) to_delete.push_back(endpoint);
    }
    for (auto& endpoint : to_delete) waiting_list_.erase(endpoint);
}

size_t FIFOEndpointStore::size() { return endpoint_map_.size(); }

int FIFOEndpointStore::clear() {
    // clear() is used by RdmaContext::disable() immediately before CQs, PD and
    // the verbs context are destroyed. Merely erasing endpoint_map_ is unsafe:
    // an endpoint retained by an external shared_ptr could otherwise run its
    // destructor later and access an already-destroyed RdmaContext.
    std::vector<std::shared_ptr<RdmaEndPoint>> endpoints;
    {
        RWSpinlock::WriteGuard guard(endpoint_map_lock_);
        // Detach both published and already-retiring endpoints atomically.
        // After this point no lookup can acquire one of these endpoints.
        endpoints.reserve(endpoint_map_.size() + waiting_list_.size());
        for (auto& entry : endpoint_map_) endpoints.push_back(entry.second);
        for (auto& endpoint : waiting_list_) endpoints.push_back(endpoint);
        endpoint_map_.clear();
        waiting_list_.clear();
        fifo_list_.clear();
        fifo_map_.clear();
    }
    // Do not call endpoint methods while holding endpoint_map_lock_: endpoint
    // destruction also unregisters its notification QP and takes endpoint
    // locks. Keeping verbs teardown outside the Store lock both shortens the
    // critical section and avoids introducing a Store/Endpoint lock cycle.
    //
    // Context shutdown has already stopped workers, so the normal two-phase
    // waiting-list path cannot drain CQ flush completions. Force synchronous
    // deconstruction here, before RdmaContext destroys its CQs and PD.
    // deconstruct() is idempotent, so external shared_ptr owners may release
    // the already-deconstructed endpoint later without touching verbs again.
    std::vector<std::shared_ptr<RdmaEndPoint>> failed;
    for (auto& endpoint : endpoints) {
        if (endpoint->deconstruct()) failed.push_back(endpoint);
    }
    if (failed.empty()) return 0;

    RWSpinlock::WriteGuard guard(endpoint_map_lock_);
    waiting_list_.insert(failed.begin(), failed.end());
    return -1;
}

std::shared_ptr<RdmaEndPoint> SIEVEEndpointStore::get(const std::string& key) {
    RWSpinlock::ReadGuard guard(endpoint_map_lock_);
    auto iter = endpoint_map_.find(key);
    if (iter != endpoint_map_.end()) {
        iter->second.second.store(true, std::memory_order_relaxed);
        return iter->second.first;
    }
    return nullptr;
}

std::shared_ptr<RdmaEndPoint> SIEVEEndpointStore::getOrInsert(
    const std::string& key) {
    auto endpoint = get(key);
    // Endpoints have unidirectional lifecycle - if existing endpoint is
    // in terminal state (destroying/destroyed), remove it and create new.
    if (endpoint) {
        auto status = endpoint->status();
        if (status == RdmaEndPoint::EP_DESTROYING ||
            status == RdmaEndPoint::EP_DESTROYED) {
            remove(endpoint.get());
            endpoint = nullptr;
        } else {
            return endpoint;
        }
    }
    RWSpinlock::WriteGuard guard(endpoint_map_lock_);
    // Double-check after acquiring write lock
    auto iter = endpoint_map_.find(key);
    if (iter != endpoint_map_.end()) {
        auto ep = iter->second.first;
        auto status = ep->status();
        if (status == RdmaEndPoint::EP_DESTROYING ||
            status == RdmaEndPoint::EP_DESTROYED) {
            waiting_list_len_++;
            waiting_list_.insert(ep);
            auto fifo_iter = fifo_map_[key];
            if (hand_.has_value() && hand_.value() == fifo_iter) {
                fifo_iter == fifo_list_.begin() ? hand_ = std::nullopt
                                                : hand_ = std::prev(fifo_iter);
            }
            fifo_list_.erase(fifo_iter);
            fifo_map_.erase(key);
            endpoint_map_.erase(iter);
            // Fall through to create new endpoint
        } else {
            return ep;
        }
    }
    endpoint = std::make_shared<RdmaEndPoint>();
    int ret = endpoint->construct(&context_, &context_.params().endpoint, key);
    if (ret) {
        LOG(ERROR) << "Failed to construct endpoint for key " << key;
        return nullptr;
    }
    while (this->size() >= max_size_) evictOne();
    endpoint_map_[key] = std::make_pair(endpoint, false);
    fifo_list_.push_front(key);
    fifo_map_[key] = fifo_list_.begin();
    return endpoint;
}

int SIEVEEndpointStore::remove(RdmaEndPoint* ep) {
    RWSpinlock::WriteGuard guard(endpoint_map_lock_);
    // Search for and remove the endpoint by pointer comparison
    for (auto iter = endpoint_map_.begin(); iter != endpoint_map_.end();
         ++iter) {
        if (iter->second.first.get() == ep) {
            waiting_list_len_++;
            waiting_list_.insert(iter->second.first);
            iter->second.first->beginDestroy();
            auto fifo_iter = fifo_map_[iter->first];
            if (hand_.has_value() && hand_.value() == fifo_iter) {
                fifo_iter == fifo_list_.begin() ? hand_ = std::nullopt
                                                : hand_ = std::prev(fifo_iter);
            }
            fifo_list_.erase(fifo_iter);
            fifo_map_.erase(iter->first);
            endpoint_map_.erase(iter);
            return 0;
        }
    }
    // If not found in endpoint_map, check if it's in waiting_list
    for (const auto& waiting_ep : waiting_list_) {
        if (waiting_ep.get() == ep) {
            // Already in waiting list, no action needed
            return 0;
        }
    }
    return -1;  // Endpoint not found
}

void SIEVEEndpointStore::evictOne() {
    if (fifo_list_.empty()) {
        return;
    }
    auto o = hand_.has_value() ? hand_.value() : --fifo_list_.end();
    std::string victim;
    while (true) {
        victim = *o;
        if (endpoint_map_[victim].second.load(std::memory_order_relaxed)) {
            endpoint_map_[victim].second.store(false,
                                               std::memory_order_relaxed);
            o = (o == fifo_list_.begin() ? --fifo_list_.end() : std::prev(o));
        } else {
            break;
        }
    }
    hand_ = (o == fifo_list_.begin() ? --fifo_list_.end() : std::prev(o));
    fifo_list_.erase(o);
    fifo_map_.erase(victim);
    auto victim_instance = endpoint_map_[victim].first;
    victim_instance->beginDestroy();
    waiting_list_len_++;
    waiting_list_.insert(victim_instance);
    endpoint_map_.erase(victim);
    LOG(INFO) << "Endpoint " << victim << " has been evicted";
    return;
}

void SIEVEEndpointStore::reclaim() {
    if (waiting_list_len_.load(std::memory_order_relaxed) == 0) return;
    RWSpinlock::WriteGuard guard(endpoint_map_lock_);
    std::vector<std::shared_ptr<RdmaEndPoint>> to_delete;
    for (auto& endpoint : waiting_list_) {
        if (endpoint->finishDestroy()) to_delete.push_back(endpoint);
    }
    for (auto& endpoint : to_delete) waiting_list_.erase(endpoint);
    waiting_list_len_ -= to_delete.size();
}

size_t SIEVEEndpointStore::size() { return endpoint_map_.size(); }

int SIEVEEndpointStore::clear() {
    // Terminal shutdown semantics are intentionally different from normal
    // SIEVE eviction: remove()/evictOne() retire endpoints asynchronously,
    // while clear() must release all verbs objects before their parent
    // RdmaContext tears down CQs, PD and the device context.
    std::vector<std::shared_ptr<RdmaEndPoint>> endpoints;
    {
        RWSpinlock::WriteGuard guard(endpoint_map_lock_);
        // Take a strong-reference snapshot and make the Store empty in one
        // critical section. This prevents concurrent lookup from observing a
        // partially-cleared cache or returning an endpoint being destroyed.
        endpoints.reserve(endpoint_map_.size() + waiting_list_.size());
        for (auto& entry : endpoint_map_)
            endpoints.push_back(entry.second.first);
        for (auto& endpoint : waiting_list_) endpoints.push_back(endpoint);
        endpoint_map_.clear();
        waiting_list_.clear();
        waiting_list_len_.store(0, std::memory_order_relaxed);
        fifo_list_.clear();
        fifo_map_.clear();
        hand_ = std::nullopt;
    }
    // Run verbs operations outside endpoint_map_lock_. Workers are already
    // stopped, so waiting for asynchronous CQ-driven reclaim is impossible;
    // synchronously deconstruct each endpoint instead. The operation is
    // idempotent, allowing outstanding external shared_ptr references to die
    // safely after the Store and Context have gone away.
    std::vector<std::shared_ptr<RdmaEndPoint>> failed;
    for (auto& endpoint : endpoints) {
        if (endpoint->deconstruct()) failed.push_back(endpoint);
    }
    if (failed.empty()) return 0;

    RWSpinlock::WriteGuard guard(endpoint_map_lock_);
    waiting_list_.insert(failed.begin(), failed.end());
    waiting_list_len_.store(waiting_list_.size(), std::memory_order_relaxed);
    return -1;
}
}  // namespace tent
}  // namespace mooncake
