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

#include <memory>

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

INSTANTIATE_TEST_SUITE_P(AllStoreTypes, EndpointStoreTest,
                         testing::Values(StoreType::FIFO, StoreType::SIEVE));

}  // namespace
}  // namespace tent
}  // namespace mooncake
