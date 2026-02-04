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
std::shared_ptr<RdmaEndPoint> FIFOEndpointStore::get(const std::string &key) {
    RWSpinlock::ReadGuard guard(endpoint_map_lock_);
    auto iter = endpoint_map_.find(key);
    if (iter != endpoint_map_.end()) return iter->second;
    return nullptr;
}

std::shared_ptr<RdmaEndPoint> FIFOEndpointStore::getOrInsert(
    const std::string &key) {
    auto endpoint = get(key);
    if (endpoint) return endpoint;
    RWSpinlock::WriteGuard guard(endpoint_map_lock_);
    if (endpoint_map_.find(key) != endpoint_map_.end())
        return endpoint_map_[key];
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

int FIFOEndpointStore::remove(RdmaEndPoint *ep) {
    RWSpinlock::WriteGuard guard(endpoint_map_lock_);
    auto key = ep->name();
    auto iter = endpoint_map_.find(key);
    if (iter != endpoint_map_.end() && iter->second.get() == ep) {
        waiting_list_.insert(iter->second);
        endpoint_map_.erase(iter);
        auto fifo_iter = fifo_map_[key];
        fifo_list_.erase(fifo_iter);
        fifo_map_.erase(key);
    }
    return 0;
}

void FIFOEndpointStore::evictOne() {
    if (fifo_list_.empty()) return;
    std::string victim = fifo_list_.front();
    fifo_list_.pop_front();
    fifo_map_.erase(victim);
    waiting_list_.insert(endpoint_map_[victim]);
    endpoint_map_.erase(victim);
}

void FIFOEndpointStore::reclaim() {
    RWSpinlock::WriteGuard guard(endpoint_map_lock_);
    std::vector<std::shared_ptr<RdmaEndPoint>> to_delete;
    for (auto &endpoint : waiting_list_) {
        if (!endpoint->getInflightSlices()) to_delete.push_back(endpoint);
    }
    for (auto &endpoint : to_delete) waiting_list_.erase(endpoint);
}

size_t FIFOEndpointStore::size() { return endpoint_map_.size(); }

void FIFOEndpointStore::clear() {
    RWSpinlock::WriteGuard guard(endpoint_map_lock_);
    std::vector<std::string> to_delete;
    for (auto &entry : endpoint_map_) to_delete.push_back(entry.first);
    for (auto &key : to_delete) {
        endpoint_map_.erase(key);
        auto fifo_iter = fifo_map_[key];
        fifo_list_.erase(fifo_iter);
        fifo_map_.erase(key);
    }
}

std::shared_ptr<RdmaEndPoint> SIEVEEndpointStore::get(const std::string &key) {
    RWSpinlock::ReadGuard guard(endpoint_map_lock_);
    auto iter = endpoint_map_.find(key);
    if (iter != endpoint_map_.end()) {
        iter->second.second.store(true, std::memory_order_relaxed);
        return iter->second.first;
    }
    return nullptr;
}

std::shared_ptr<RdmaEndPoint> SIEVEEndpointStore::getOrInsert(
    const std::string &key) {
    auto endpoint = get(key);
    if (endpoint) return endpoint;
    RWSpinlock::WriteGuard guard(endpoint_map_lock_);
    if (endpoint_map_.find(key) != endpoint_map_.end()) {
        return endpoint_map_[key].first;
    }
    endpoint = std::make_shared<RdmaEndPoint>();
    int ret = endpoint->construct(&context_, &context_.params().endpoint, key,
                                  &endpoints_count_);
    if (ret) {
        LOG(ERROR) << "Failed to construct endpoint for key " << key;
        return nullptr;
    }
    endpoints_count_.fetch_add(1, std::memory_order_relaxed);
    while (this->size() >= max_size_) evictOne();
    endpoint_map_[key] = std::make_pair(endpoint, false);
    fifo_list_.push_front(key);
    fifo_map_[key] = fifo_list_.begin();
    return endpoint;
}

int SIEVEEndpointStore::remove(RdmaEndPoint *ep) {
    RWSpinlock::WriteGuard guard(endpoint_map_lock_);
    auto key = ep->name();
    auto iter = endpoint_map_.find(key);
    if (iter != endpoint_map_.end() && iter->second.first.get() == ep) {
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
    for (auto &endpoint : waiting_list_) {
        if (!endpoint->getInflightSlices()) to_delete.push_back(endpoint);
    }
    for (auto &endpoint : to_delete) waiting_list_.erase(endpoint);
    waiting_list_len_ -= to_delete.size();
}

size_t SIEVEEndpointStore::size() { return endpoint_map_.size(); }

void SIEVEEndpointStore::clear() {
    RWSpinlock::WriteGuard guard(endpoint_map_lock_);
    std::vector<std::string> to_delete;
    for (auto &entry : endpoint_map_) to_delete.push_back(entry.first);
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

    const int max_retries = 5000;
    int retries = 0;
    while (endpoints_count_.load(std::memory_order_relaxed) > 0) {
        if (++retries > max_retries) {
            LOG(ERROR) << "Some endpoints not cleared after 5 seconds";
            break;
        }
        usleep(1000);
    }
}
}  // namespace tent
}  // namespace mooncake