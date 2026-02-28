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

#include "transport/rdma_transport/endpoint_store.h"

#include <glog/logging.h>

#include <atomic>
#include <cassert>
#include <cstddef>
#include <memory>
#include <utility>

#include "config.h"
#include "transport/rdma_transport/rdma_context.h"
#include "transport/rdma_transport/rdma_endpoint.h"

namespace mooncake {
std::shared_ptr<RdmaEndPoint> FIFOEndpointStore::getEndpoint(
    const std::string &peer_nic_path) {
    RWSpinlock::ReadGuard guard(endpoint_map_lock_);
    auto iter = endpoint_map_.find(peer_nic_path);
    if (iter != endpoint_map_.end()) return iter->second;
    return nullptr;
}

std::shared_ptr<RdmaEndPoint> FIFOEndpointStore::insertEndpoint(
    const std::string &peer_nic_path, RdmaContext *context) {
    RWSpinlock::WriteGuard guard(endpoint_map_lock_);
    if (endpoint_map_.find(peer_nic_path) != endpoint_map_.end()) {
        LOG(INFO) << "Endpoint " << peer_nic_path
                  << " already exists in FIFOEndpointStore";
        return endpoint_map_[peer_nic_path];
    }
    auto endpoint = std::make_shared<RdmaEndPoint>(*context);
    if (!endpoint) {
        LOG(ERROR) << "Failed to allocate memory for RdmaEndPoint";
        return nullptr;
    }
    auto &config = globalConfig();
    int ret =
        endpoint->construct(context->cq(), config.num_qp_per_ep, config.max_sge,
                            config.max_wr, config.max_inline);
    if (ret) return nullptr;

    // make eviction more proactive
    size_t high_water = (max_size_ * 95 + 99) / 100;
    while (this->getSize() >= high_water) evictEndpoint();

    endpoint->setPeerNicPath(peer_nic_path);
    endpoint_map_[peer_nic_path] = endpoint;
    fifo_list_.push_back(peer_nic_path);
    auto it = fifo_list_.end();
    fifo_map_[peer_nic_path] = --it;
    return endpoint;
}

int FIFOEndpointStore::deleteEndpoint(const std::string &peer_nic_path) {
    RWSpinlock::WriteGuard guard(endpoint_map_lock_);
    auto iter = endpoint_map_.find(peer_nic_path);
    // remove endpoint but leaving it status unchanged
    // in case it is setting up connection or submitting slice
    if (iter != endpoint_map_.end()) {
        iter->second->set_active(false);
        waiting_list_.insert(iter->second);
        endpoint_map_.erase(iter);
        auto fifo_it = fifo_map_.find(peer_nic_path);
        if (fifo_it != fifo_map_.end()) {
            fifo_list_.erase(fifo_it->second);
            fifo_map_.erase(fifo_it);
        }
    }
    reclaimEndpointUnlocked();
    return 0;
}

void FIFOEndpointStore::evictEndpoint() {
    if (fifo_list_.empty()) return;
    std::string victim = fifo_list_.front();
    fifo_list_.pop_front();
    auto fifo_it = fifo_map_.find(victim);
    if (fifo_it != fifo_map_.end()) {
        fifo_map_.erase(fifo_it);
    }
    auto it = endpoint_map_.find(victim);
    if (it == endpoint_map_.end()) {
        LOG(WARNING) << "FIFOEndpointStore: victim " << victim
                     << " not found in endpoint_map_ during eviction";
        return;
    }
    LOG(INFO) << victim << " evicted";
    it->second->set_active(false);
    waiting_list_.insert(it->second);
    endpoint_map_.erase(it);
    reclaimEndpointUnlocked();
}

void FIFOEndpointStore::reclaimEndpoint() {
    RWSpinlock::WriteGuard guard(endpoint_map_lock_);
    reclaimEndpointUnlocked();
}

void FIFOEndpointStore::reclaimEndpointUnlocked() {
    std::vector<std::shared_ptr<RdmaEndPoint>> to_delete;
    for (auto &endpoint : waiting_list_) {
        if (!endpoint->hasOutstandingSlice()) {
            endpoint->destroyQP();
            to_delete.push_back(endpoint);
        }
    }
    for (auto &endpoint : to_delete) {
        waiting_list_.erase(endpoint);
    }
}

size_t FIFOEndpointStore::getSize() { return endpoint_map_.size(); }

int FIFOEndpointStore::destroyQPs() {
    RWSpinlock::WriteGuard guard(endpoint_map_lock_);
    for (auto &kv : endpoint_map_) {
        kv.second->destroyQP();
    }
    for (auto &ep : waiting_list_) {
        ep->destroyQP();
    }
    return 0;
}

int FIFOEndpointStore::disconnectQPs() {
    RWSpinlock::WriteGuard guard(endpoint_map_lock_);
    for (auto &kv : endpoint_map_) {
        kv.second->disconnect();
    }
    for (auto &ep : waiting_list_) {
        ep->disconnect();
    }
    return 0;
}

size_t FIFOEndpointStore::getTotalQPNumber() {
    RWSpinlock::ReadGuard guard(endpoint_map_lock_);
    size_t total_qps = 0;
    for (const auto &kv : endpoint_map_) {
        total_qps += kv.second->getQPNumber();
    }
    for (const auto &endpoint : waiting_list_) {
        total_qps += endpoint->getQPNumber();
    }
    return total_qps;
}

std::shared_ptr<RdmaEndPoint> SIEVEEndpointStore::getEndpoint(
    const std::string &peer_nic_path) {
    RWSpinlock::ReadGuard guard(endpoint_map_lock_);
    auto iter = endpoint_map_.find(peer_nic_path);
    if (iter != endpoint_map_.end()) {
        iter->second.second.store(
            true, std::memory_order_relaxed);  // This is safe within read lock
                                               // because of idempotence
        return iter->second.first;
    }
    // LOG(INFO) << "Endpoint " << peer_nic_path << " not found in
    // SIEVEEndpointStore";
    return nullptr;
}

std::shared_ptr<RdmaEndPoint> SIEVEEndpointStore::insertEndpoint(
    const std::string &peer_nic_path, RdmaContext *context) {
    RWSpinlock::WriteGuard guard(endpoint_map_lock_);
    if (endpoint_map_.find(peer_nic_path) != endpoint_map_.end()) {
        LOG(INFO) << "Endpoint " << peer_nic_path
                  << " already exists in SIEVEEndpointStore";
        return endpoint_map_[peer_nic_path].first;
    }
    auto endpoint = std::make_shared<RdmaEndPoint>(*context);
    if (!endpoint) {
        LOG(ERROR) << "Failed to allocate memory for RdmaEndPoint";
        return nullptr;
    }
    auto &config = globalConfig();
    int ret =
        endpoint->construct(context->cq(), config.num_qp_per_ep, config.max_sge,
                            config.max_wr, config.max_inline);
    if (ret) return nullptr;

    // make eviction more proactive
    size_t high_water = (max_size_ * 95 + 99) / 100;
    while (this->getSize() >= high_water) evictEndpoint();

    endpoint->setPeerNicPath(peer_nic_path);
    endpoint_map_[peer_nic_path] = std::make_pair(endpoint, true);
    fifo_list_.push_front(peer_nic_path);
    fifo_map_[peer_nic_path] = fifo_list_.begin();
    return endpoint;
}

int SIEVEEndpointStore::deleteEndpoint(const std::string &peer_nic_path) {
    RWSpinlock::WriteGuard guard(endpoint_map_lock_);
    auto iter = endpoint_map_.find(peer_nic_path);
    // remove endpoint but leaving it status unchanged
    // in case it is setting up connection or submitting slice
    if (iter != endpoint_map_.end()) {
        auto ep = iter->second.first;
        ep->set_active(false);
        waiting_list_len_++;
        waiting_list_.insert(ep);
        endpoint_map_.erase(iter);
        auto fifo_iter = fifo_map_[peer_nic_path];
        if (hand_.has_value() && hand_.value() == fifo_iter) {
            fifo_iter == fifo_list_.begin() ? hand_ = std::nullopt
                                            : hand_ = std::prev(fifo_iter);
        }
        fifo_list_.erase(fifo_iter);
        fifo_map_.erase(peer_nic_path);
    }
    reclaimEndpointUnlocked();
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
    o == fifo_list_.begin() ? hand_ = std::nullopt : hand_ = std::prev(o);

    fifo_list_.erase(o);
    auto fifo_it = fifo_map_.find(victim);
    if (fifo_it != fifo_map_.end()) {
        fifo_map_.erase(fifo_it);
    }

    auto map_it = endpoint_map_.find(victim);
    if (map_it == endpoint_map_.end()) {
        LOG(WARNING) << "SIEVEEndpointStore: victim " << victim
                     << " not found in endpoint_map_ during eviction";
        return;
    }

    LOG(INFO) << victim << " evicted";
    auto victim_instance = map_it->second.first;
    victim_instance->set_active(false);
    waiting_list_len_++;
    waiting_list_.insert(victim_instance);
    endpoint_map_.erase(map_it);
    reclaimEndpointUnlocked();
}

void SIEVEEndpointStore::reclaimEndpoint() {
    if (waiting_list_len_.load(std::memory_order_relaxed) == 0) return;
    RWSpinlock::WriteGuard guard(endpoint_map_lock_);
    reclaimEndpointUnlocked();
}

void SIEVEEndpointStore::reclaimEndpointUnlocked() {
    if (waiting_list_len_.load(std::memory_order_relaxed) == 0) return;
    std::vector<std::shared_ptr<RdmaEndPoint>> to_delete;
    for (auto &endpoint : waiting_list_) {
        if (!endpoint->hasOutstandingSlice()) {
            endpoint->destroyQP();
            to_delete.push_back(endpoint);
        }
    }
    for (auto &endpoint : to_delete) {
        waiting_list_.erase(endpoint);
    }
    waiting_list_len_.fetch_sub(static_cast<int>(to_delete.size()),
                                std::memory_order_relaxed);
}

int SIEVEEndpointStore::destroyQPs() {
    RWSpinlock::WriteGuard guard(endpoint_map_lock_);
    for (auto &endpoint : waiting_list_) {
        endpoint->destroyQP();
    }
    for (auto &kv : endpoint_map_) {
        kv.second.first->destroyQP();
    }
    return 0;
}

int SIEVEEndpointStore::disconnectQPs() {
    RWSpinlock::WriteGuard guard(endpoint_map_lock_);
    for (auto &endpoint : waiting_list_) {
        endpoint->disconnect();
    }
    for (auto &kv : endpoint_map_) {
        kv.second.first->disconnect();
    }
    return 0;
}

size_t SIEVEEndpointStore::getSize() { return endpoint_map_.size(); }

size_t SIEVEEndpointStore::getTotalQPNumber() {
    RWSpinlock::ReadGuard guard(endpoint_map_lock_);
    size_t total_qps = 0;

    // Count QPs in active endpoints
    for (const auto &kv : endpoint_map_) {
        total_qps += kv.second.first->getQPNumber();
    }

    // Count QPs in waiting list
    for (const auto &endpoint : waiting_list_) {
        total_qps += endpoint->getQPNumber();
    }

    return total_qps;
}

}  // namespace mooncake
