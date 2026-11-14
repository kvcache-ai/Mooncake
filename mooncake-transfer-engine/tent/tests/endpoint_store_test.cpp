// Copyright 2026 KVCache.AI
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

#include <gtest/gtest.h>

#include <chrono>
#include <memory>
#include <string>
#include <thread>
#include <utility>

#include "tent/transport/rdma/context.h"
#include "tent/transport/rdma/endpoint.h"
#include "tent/transport/rdma/endpoint_store.h"
#include "tent/transport/rdma/rdma_transport.h"

namespace mooncake {
namespace tent {

class EndpointStoreTestAccess {
   public:
    static void insertWaiting(FIFOEndpointStore& store,
                              std::shared_ptr<RdmaEndPoint> endpoint) {
        endpoint->beginDestroy();
        RWSpinlock::WriteGuard guard(store.endpoint_map_lock_);
        store.waiting_list_.insert(std::move(endpoint));
    }

    static void insertWaiting(SIEVEEndpointStore& store,
                              std::shared_ptr<RdmaEndPoint> endpoint) {
        endpoint->beginDestroy();
        RWSpinlock::WriteGuard guard(store.endpoint_map_lock_);
        if (store.waiting_list_.insert(std::move(endpoint)).second) {
            store.waiting_list_len_.fetch_add(1, std::memory_order_relaxed);
        }
    }

    // Publishes an endpoint under `key` the way getOrInsert() would, minus
    // the construct() that needs a real device.
    static void insertActive(FIFOEndpointStore& store, const std::string& key,
                             std::shared_ptr<RdmaEndPoint> endpoint) {
        RWSpinlock::WriteGuard guard(store.endpoint_map_lock_);
        store.endpoint_map_[key] = std::move(endpoint);
        store.fifo_list_.push_back(key);
        auto it = store.fifo_list_.end();
        store.fifo_map_[key] = --it;
    }

    static void insertActive(SIEVEEndpointStore& store, const std::string& key,
                             std::shared_ptr<RdmaEndPoint> endpoint) {
        RWSpinlock::WriteGuard guard(store.endpoint_map_lock_);
        store.endpoint_map_[key] = std::make_pair(std::move(endpoint), false);
        store.fifo_list_.push_front(key);
        store.fifo_map_[key] = store.fifo_list_.begin();
    }

    static size_t waitingListSize(FIFOEndpointStore& store) {
        RWSpinlock::ReadGuard guard(store.endpoint_map_lock_);
        return store.waiting_list_.size();
    }

    static size_t waitingListSize(SIEVEEndpointStore& store) {
        RWSpinlock::ReadGuard guard(store.endpoint_map_lock_);
        return store.waiting_list_.size();
    }
};

namespace {

enum class StoreType { FIFO, SIEVE };

class EndpointStoreTest : public testing::TestWithParam<StoreType> {
   protected:
    EndpointStoreTest() : context_(transport_) {}

    std::unique_ptr<EndpointStore> makeStore() {
        if (GetParam() == StoreType::FIFO) {
            return std::make_unique<FIFOEndpointStore>(context_, 4);
        }
        return std::make_unique<SIEVEEndpointStore>(context_, 4);
    }

    void insertWaiting(EndpointStore& store,
                       std::shared_ptr<RdmaEndPoint> endpoint) {
        if (GetParam() == StoreType::FIFO) {
            EndpointStoreTestAccess::insertWaiting(
                static_cast<FIFOEndpointStore&>(store), std::move(endpoint));
        } else {
            EndpointStoreTestAccess::insertWaiting(
                static_cast<SIEVEEndpointStore&>(store), std::move(endpoint));
        }
    }

    void insertActive(EndpointStore& store, const std::string& key,
                      std::shared_ptr<RdmaEndPoint> endpoint) {
        if (GetParam() == StoreType::FIFO) {
            EndpointStoreTestAccess::insertActive(
                static_cast<FIFOEndpointStore&>(store), key,
                std::move(endpoint));
        } else {
            EndpointStoreTestAccess::insertActive(
                static_cast<SIEVEEndpointStore&>(store), key,
                std::move(endpoint));
        }
    }

    size_t waitingListSize(EndpointStore& store) {
        if (GetParam() == StoreType::FIFO) {
            return EndpointStoreTestAccess::waitingListSize(
                static_cast<FIFOEndpointStore&>(store));
        }
        return EndpointStoreTestAccess::waitingListSize(
            static_cast<SIEVEEndpointStore&>(store));
    }

    RdmaTransport transport_;
    RdmaContext context_;
};

TEST_P(EndpointStoreTest, ReclaimDrainsQuiescentEntries) {
    auto store = makeStore();

    constexpr size_t kEndpointCount = 10;
    for (size_t i = 0; i < kEndpointCount; ++i) {
        insertWaiting(*store, std::make_shared<RdmaEndPoint>());
    }
    ASSERT_EQ(waitingListSize(*store), kEndpointCount);

    store->reclaim();
    EXPECT_EQ(waitingListSize(*store), 0);
}

TEST_P(EndpointStoreTest, ReclaimIsIdempotentWhenEmpty) {
    auto store = makeStore();

    store->reclaim();
    EXPECT_EQ(waitingListSize(*store), 0);

    insertWaiting(*store, std::make_shared<RdmaEndPoint>());
    store->reclaim();
    ASSERT_EQ(waitingListSize(*store), 0);

    store->reclaim();
    EXPECT_EQ(waitingListSize(*store), 0);
}

TEST_P(EndpointStoreTest, ReclaimDrainsBacklogWithoutActiveMapEntries) {
    auto store = makeStore();

    constexpr size_t kEndpointCount = 1000;
    for (size_t i = 0; i < kEndpointCount; ++i) {
        insertWaiting(*store, std::make_shared<RdmaEndPoint>());
    }
    ASSERT_EQ(store->size(), 0);
    ASSERT_EQ(waitingListSize(*store), kEndpointCount);

    store->reclaim();
    EXPECT_EQ(waitingListSize(*store), 0);
}

TEST_P(EndpointStoreTest, ClearDeconstructsExternallyOwnedWaitingEndpoint) {
    auto store = makeStore();
    auto endpoint = std::make_shared<RdmaEndPoint>();
    std::weak_ptr<RdmaEndPoint> weak = endpoint;
    insertWaiting(*store, endpoint);

    ASSERT_EQ(store->clear(), 0);

    EXPECT_EQ(waitingListSize(*store), 0);
    EXPECT_EQ(endpoint->status(), RdmaEndPoint::EP_DESTROYED);
    endpoint.reset();
    EXPECT_TRUE(weak.expired());
}

// A peer that has left is never looked up again, so capacity eviction - which
// only runs when a new endpoint is inserted - never reaches its endpoints, and
// each one keeps its QPs and its 32 MiB of pinned notification buffers for the
// lifetime of the process. Idle eviction is what gives them back, and it must
// leave an endpoint that is still carrying traffic alone.
TEST_P(EndpointStoreTest, EvictIdleRetiresUnusedEndpointsAndKeepsFreshOnes) {
    // Stamp both endpoints explicitly instead of relying on a zero
    // last-used time: the steady clock counts from boot, so on a freshly
    // started machine "never used" is not yet older than any long window.
    constexpr uint64_t kIdleWindowNs = 1000ull * 1000000ull;  // 1s
    auto store = makeStore();
    auto departed = std::make_shared<RdmaEndPoint>();
    auto active = std::make_shared<RdmaEndPoint>();
    insertActive(*store, "10.0.0.1:12345@mlx5_0", departed);
    insertActive(*store, "10.0.0.2:12345@mlx5_0", active);
    departed->noteUsed();
    std::this_thread::sleep_for(std::chrono::milliseconds(1500));
    active->noteUsed();
    ASSERT_EQ(store->size(), 2);

    EXPECT_EQ(store->evictIdle(kIdleWindowNs), 1);
    EXPECT_EQ(store->size(), 1);
    EXPECT_EQ(store->get("10.0.0.2:12345@mlx5_0"), active);
    EXPECT_EQ(store->get("10.0.0.1:12345@mlx5_0"), nullptr);

    // Retirement is two-phase: the endpoint is only released once reclaim()
    // finds it quiescent.
    ASSERT_EQ(waitingListSize(*store), 1);
    store->reclaim();
    EXPECT_EQ(waitingListSize(*store), 0);
    EXPECT_EQ(departed->status(), RdmaEndPoint::EP_DESTROYED);

    // A second sweep inside the window must not touch the survivor.
    EXPECT_EQ(store->evictIdle(kIdleWindowNs), 0);
    EXPECT_EQ(store->size(), 1);
}

TEST_P(EndpointStoreTest, EvictIdleIsDisabledByAZeroTimeout) {
    auto store = makeStore();
    insertActive(*store, "10.0.0.1:12345@mlx5_0",
                 std::make_shared<RdmaEndPoint>());

    EXPECT_EQ(store->evictIdle(0), 0);
    EXPECT_EQ(store->size(), 1);
    EXPECT_EQ(waitingListSize(*store), 0);
}

INSTANTIATE_TEST_SUITE_P(AllStoreTypes, EndpointStoreTest,
                         testing::Values(StoreType::FIFO, StoreType::SIEVE));

}  // namespace
}  // namespace tent
}  // namespace mooncake
