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

#include "v1/transport/rdma/endpoint_store.h"

#include <glog/logging.h>

#include <atomic>
#include <cassert>
#include <cstddef>
#include <memory>
#include <utility>

#include "v1/common/status.h"
#include "v1/transport/rdma/context.h"
#include "v1/transport/rdma/endpoint.h"

namespace mooncake {
namespace v1 {
std::shared_ptr<RdmaEndPoint> FIFOEndpointStore::getEndpoint(
    const std::string &key) {
    RWSpinlock::ReadGuard guard(endpoint_map_lock_);
    auto iter = endpoint_map_.find(key);
    if (iter != endpoint_map_.end()) return iter->second;
    return nullptr;
}

std::shared_ptr<RdmaEndPoint> FIFOEndpointStore::insertEndpoint(
    const std::string &key, RdmaContext *context) {
    RWSpinlock::WriteGuard guard(endpoint_map_lock_);
    if (endpoint_map_.find(key) != endpoint_map_.end()) {
        LOG(INFO) << "Endpoint " << key
                  << " already exists in FIFOEndpointStore";
        return endpoint_map_[key];
    }
    auto endpoint = std::make_shared<RdmaEndPoint>();
    if (!endpoint) {
        LOG(ERROR) << "Failed to allocate memory for RdmaEndPoint";
        return nullptr;
    }
    int ret = endpoint->construct(context, &context->params().endpoint, key);
    if (ret) return nullptr;

    while (this->getSize() >= max_size_) evictEndpoint();

    endpoint_map_[key] = endpoint;
    fifo_list_.push_back(key);
    auto it = fifo_list_.end();
    fifo_map_[key] = --it;
    return endpoint;
}

int FIFOEndpointStore::deleteEndpoint(const std::string &key) {
    RWSpinlock::WriteGuard guard(endpoint_map_lock_);
    auto iter = endpoint_map_.find(key);
    if (iter != endpoint_map_.end()) {
        waiting_list_.insert(iter->second);
        endpoint_map_.erase(iter);
        auto fifo_iter = fifo_map_[key];
        fifo_list_.erase(fifo_iter);
        fifo_map_.erase(key);
    }
    return 0;
}

void FIFOEndpointStore::evictEndpoint() {
    if (fifo_list_.empty()) return;
    std::string victim = fifo_list_.front();
    fifo_list_.pop_front();
    fifo_map_.erase(victim);
    waiting_list_.insert(endpoint_map_[victim]);
    endpoint_map_.erase(victim);
    return;
}

void FIFOEndpointStore::reclaimEndpoint() {
    RWSpinlock::WriteGuard guard(endpoint_map_lock_);
    std::vector<std::shared_ptr<RdmaEndPoint>> to_delete;
    for (auto &endpoint : waiting_list_) {
        if (!endpoint->outstandingSlices()) to_delete.push_back(endpoint);
    }
    for (auto &endpoint : to_delete) waiting_list_.erase(endpoint);
}

size_t FIFOEndpointStore::getSize() { return endpoint_map_.size(); }

void FIFOEndpointStore::clearEndpoints(const std::string &prefix) {
    RWSpinlock::WriteGuard guard(endpoint_map_lock_);
    std::vector<std::string> to_delete;
    for (auto &entry : endpoint_map_) {
        if (entry.first.substr(0, prefix.length()) == prefix)
            to_delete.push_back(entry.first);
    }
    for (auto &key : to_delete) {
        endpoint_map_.erase(key);
        auto fifo_iter = fifo_map_[key];
        fifo_list_.erase(fifo_iter);
        fifo_map_.erase(key);
    }
}

std::shared_ptr<RdmaEndPoint> SIEVEEndpointStore::getEndpoint(
    const std::string &key) {
    RWSpinlock::ReadGuard guard(endpoint_map_lock_);
    auto iter = endpoint_map_.find(key);
    if (iter != endpoint_map_.end()) {
        iter->second.second.store(
            true, std::memory_order_relaxed);  // This is safe within read lock
                                               // because of idempotence
        return iter->second.first;
    }
    return nullptr;
}

std::shared_ptr<RdmaEndPoint> SIEVEEndpointStore::insertEndpoint(
    const std::string &key, RdmaContext *context) {
    RWSpinlock::WriteGuard guard(endpoint_map_lock_);
    if (endpoint_map_.find(key) != endpoint_map_.end()) {
        LOG(INFO) << "Endpoint " << key
                  << " already exists in SIEVEEndpointStore";
        return endpoint_map_[key].first;
    }
    auto endpoint = std::make_shared<RdmaEndPoint>();
    if (!endpoint) {
        LOG(ERROR) << "Failed to allocate memory for RdmaEndPoint";
        return nullptr;
    }
    int ret = endpoint->construct(context, &context->params().endpoint, key);
    if (ret) return nullptr;

    while (this->getSize() >= max_size_) evictEndpoint();
    endpoint_map_[key] = std::make_pair(endpoint, false);
    fifo_list_.push_front(key);
    fifo_map_[key] = fifo_list_.begin();
    return endpoint;
}

int SIEVEEndpointStore::deleteEndpoint(const std::string &key) {
    RWSpinlock::WriteGuard guard(endpoint_map_lock_);
    auto iter = endpoint_map_.find(key);
    if (iter != endpoint_map_.end()) {
        waiting_list_len_++;
        waiting_list_.insert(iter->second.first);
        endpoint_map_.erase(iter);
        auto fifo_iter = fifo_map_[key];
        if (hand_.has_value() && hand_.value() == fifo_iter) {
            fifo_iter == fifo_list_.begin() ? hand_ = std::nullopt
                                            : hand_ = std::prev(fifo_iter);
        }
        fifo_list_.erase(fifo_iter);
        fifo_map_.erase(key);
    }
    return 0;
}

void SIEVEEndpointStore::evictEndpoint() {
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
    waiting_list_len_++;
    waiting_list_.insert(victim_instance);
    endpoint_map_.erase(victim);
    return;
}

void SIEVEEndpointStore::reclaimEndpoint() {
    if (waiting_list_len_.load(std::memory_order_relaxed) == 0) return;
    RWSpinlock::WriteGuard guard(endpoint_map_lock_);
    std::vector<std::shared_ptr<RdmaEndPoint>> to_delete;
    for (auto &endpoint : waiting_list_) {
        if (!endpoint->outstandingSlices()) to_delete.push_back(endpoint);
    }
    for (auto &endpoint : to_delete) waiting_list_.erase(endpoint);
    waiting_list_len_ -= to_delete.size();
}

size_t SIEVEEndpointStore::getSize() { return endpoint_map_.size(); }

void SIEVEEndpointStore::clearEndpoints(const std::string &prefix) {
    RWSpinlock::WriteGuard guard(endpoint_map_lock_);
    std::vector<std::string> to_delete;
    for (auto &entry : endpoint_map_) {
        if (entry.first.substr(0, prefix.length()) == prefix)
            to_delete.push_back(entry.first);
    }
    for (auto &key : to_delete) {
        endpoint_map_.erase(key);
        auto fifo_iter = fifo_map_[key];
        if (hand_.has_value() && hand_.value() == fifo_iter) {
            fifo_iter == fifo_list_.begin() ? hand_ = std::nullopt
                                            : hand_ = std::prev(fifo_iter);
        }
        fifo_list_.erase(fifo_iter);
        fifo_map_.erase(key);
    }
}
}  // namespace v1
}  // namespace mooncake