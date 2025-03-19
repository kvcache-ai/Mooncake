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

#ifndef ENDPOINT_STORE_H_
#define ENDPOINT_STORE_H_

#include <infiniband/verbs.h>

#include <atomic>
#include <memory>
#include <optional>

#include "rdma_context.h"
#include "rdma_endpoint.h"

using namespace mooncake;

namespace mooncake {
// TODO: this can be implemented in std::concept from c++20
/* TODO: A better abstraction may be used to reduce redundant codes,
for example, make "cache eviction policy" a abstract class. Currently,
cache data structure and eviction policy are put in the same class, for
different eviction policy may need different data structure
(e.g. lock-free queue for FIFO) for better performance
*/
class EndpointStore {
   public:
    virtual std::shared_ptr<RdmaEndPoint> getEndpoint(
        const std::string &peer_nic_path) = 0;
    virtual std::shared_ptr<RdmaEndPoint> insertEndpoint(
        const std::string &peer_nic_path, RdmaContext *context) = 0;
    virtual int deleteEndpoint(const std::string &peer_nic_path) = 0;
    virtual void evictEndpoint() = 0;
    virtual void reclaimEndpoint() = 0;
    virtual size_t getSize() = 0;

    virtual int destroyQPs() = 0;
    virtual int disconnectQPs() = 0;
};

// FIFO
class FIFOEndpointStore : public EndpointStore {
   public:
    FIFOEndpointStore(size_t max_size) : max_size_(max_size) {}
    std::shared_ptr<RdmaEndPoint> getEndpoint(
        const std::string &peer_nic_path) override;
    std::shared_ptr<RdmaEndPoint> insertEndpoint(
        const std::string &peer_nic_path, RdmaContext *context) override;
    int deleteEndpoint(const std::string &peer_nic_path) override;
    void evictEndpoint() override;
    void reclaimEndpoint() override;
    size_t getSize() override;

    int destroyQPs() override;
    int disconnectQPs() override;

   private:
    RWSpinlock endpoint_map_lock_;
    std::unordered_map<std::string, std::shared_ptr<RdmaEndPoint>>
        endpoint_map_;
    std::unordered_map<std::string, std::list<std::string>::iterator> fifo_map_;
    std::list<std::string> fifo_list_;

    std::unordered_set<std::shared_ptr<RdmaEndPoint>> waiting_list_;

    size_t max_size_;
};

// NSDI 24, similar to clock with quick demotion
class SIEVEEndpointStore : public EndpointStore {
   public:
    SIEVEEndpointStore(size_t max_size)
        : waiting_list_len_(0), max_size_(max_size) {}
    std::shared_ptr<RdmaEndPoint> getEndpoint(
        const std::string &peer_nic_path) override;
    std::shared_ptr<RdmaEndPoint> insertEndpoint(
        const std::string &peer_nic_path, RdmaContext *context) override;
    int deleteEndpoint(const std::string &peer_nic_path) override;
    void evictEndpoint() override;
    void reclaimEndpoint() override;
    size_t getSize() override;

    int destroyQPs() override;
    int disconnectQPs() override;

   private:
    RWSpinlock endpoint_map_lock_;
    // The bool represents visited
    std::unordered_map<
        std::string, std::pair<std::shared_ptr<RdmaEndPoint>, std::atomic_bool>>
        endpoint_map_;
    std::unordered_map<std::string, std::list<std::string>::iterator> fifo_map_;
    std::list<std::string> fifo_list_;

    std::optional<std::list<std::string>::iterator> hand_;

    std::unordered_set<std::shared_ptr<RdmaEndPoint>> waiting_list_;
    std::atomic<int> waiting_list_len_;

    size_t max_size_;
};
}  // namespace mooncake

#endif