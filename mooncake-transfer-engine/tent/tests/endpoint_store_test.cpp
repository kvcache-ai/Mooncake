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
            static_cast<FIFOEndpointStore&>(store).testOnlyInsertWaiting(
                std::move(endpoint));
        } else {
            static_cast<SIEVEEndpointStore&>(store).testOnlyInsertWaiting(
                std::move(endpoint));
        }
    }

    size_t waitingListSize(EndpointStore& store) {
        if (GetParam() == StoreType::FIFO) {
            return static_cast<FIFOEndpointStore&>(store)
                .testOnlyWaitingListSize();
        }
        return static_cast<SIEVEEndpointStore&>(store)
            .testOnlyWaitingListSize();
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

    store->clear();

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
