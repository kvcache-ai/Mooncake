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

#ifndef TENT_ENDPOINT_STORE_H
#define TENT_ENDPOINT_STORE_H

#include <infiniband/verbs.h>

#include <atomic>
#include <memory>
#include <optional>

#include "context.h"
#include "endpoint.h"

using namespace mooncake;

namespace mooncake {
namespace tent {
class EndpointStore {
   public:
    virtual std::shared_ptr<RdmaEndPoint> get(const std::string &key) = 0;

    virtual std::shared_ptr<RdmaEndPoint> getOrInsert(
        const std::string &key) = 0;

    virtual int remove(RdmaEndPoint *ep) = 0;

    virtual void clear() = 0;

    virtual size_t size() = 0;

    virtual void evictOne() = 0;
    virtual void reclaim() = 0;
};

// FIFO
class FIFOEndpointStore : public EndpointStore {
   public:
    FIFOEndpointStore(RdmaContext &context, size_t max_size)
        : context_(context), max_size_(max_size) {}

    std::shared_ptr<RdmaEndPoint> get(const std::string &key) override;

    std::shared_ptr<RdmaEndPoint> getOrInsert(const std::string &key) override;

    int remove(RdmaEndPoint *ep) override;

    void clear();

    size_t size() override;

    void evictOne() override;
    void reclaim() override;

   private:
    RdmaContext &context_;
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
    SIEVEEndpointStore(RdmaContext &context, size_t max_size)
        : context_(context), waiting_list_len_(0), max_size_(max_size) {}

    std::shared_ptr<RdmaEndPoint> get(const std::string &key) override;

    std::shared_ptr<RdmaEndPoint> getOrInsert(const std::string &key) override;

    void clear();

    size_t size() override;

    int remove(RdmaEndPoint *ep) override;
    void evictOne() override;
    void reclaim() override;

   private:
    RdmaContext &context_;
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
    std::atomic<int> endpoints_count_{0};
};
}  // namespace tent
}  // namespace mooncake

#endif