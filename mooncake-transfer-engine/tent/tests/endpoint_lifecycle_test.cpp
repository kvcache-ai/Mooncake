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

namespace mooncake {
namespace tent {
namespace {

// ---------------------------------------------------------------------------
// Minimal stub to validate weak_ptr lifecycle without RDMA dependencies.
// The real RdmaEndPoint inherits enable_shared_from_this; we mirror that
// pattern here so the test proves the ownership model works.
// ---------------------------------------------------------------------------

class FakeEndPoint : public std::enable_shared_from_this<FakeEndPoint> {
   public:
    int acknowledge_calls = 0;
    int reset_calls = 0;

    void acknowledge() { ++acknowledge_calls; }
    void reset() { ++reset_calls; }
};

// ---------------------------------------------------------------------------
// weak_ptr basic lifecycle
// ---------------------------------------------------------------------------

TEST(EndpointLifecycleTest, WeakPtrLocksWhileAlive) {
    auto ep = std::make_shared<FakeEndPoint>();
    std::weak_ptr<FakeEndPoint> weak = ep;

    auto locked = weak.lock();
    ASSERT_NE(locked, nullptr);
    locked->acknowledge();
    EXPECT_EQ(ep->acknowledge_calls, 1);
}

TEST(EndpointLifecycleTest, WeakPtrExpiresAfterRelease) {
    std::weak_ptr<FakeEndPoint> weak;
    {
        auto ep = std::make_shared<FakeEndPoint>();
        weak = ep;
        EXPECT_FALSE(weak.expired());
    }  // ep destroyed here
    EXPECT_TRUE(weak.expired());
    EXPECT_EQ(weak.lock(), nullptr);
}

TEST(EndpointLifecycleTest, SharedFromThisProducesValidWeakPtr) {
    auto ep = std::make_shared<FakeEndPoint>();
    // Simulate what submitSlices() does: shared_from_this() assigned to
    // weak_ptr
    std::weak_ptr<FakeEndPoint> weak = ep->shared_from_this();

    auto locked = weak.lock();
    ASSERT_NE(locked, nullptr);
    EXPECT_EQ(locked.get(), ep.get());
}

// ---------------------------------------------------------------------------
// Simulate the slice → endpoint dereference pattern used in workers.cpp
// ---------------------------------------------------------------------------

struct FakeSlice {
    std::weak_ptr<FakeEndPoint> ep_weak_ptr;
};

TEST(EndpointLifecycleTest, SliceAccessWhileEndpointAlive) {
    auto ep = std::make_shared<FakeEndPoint>();
    FakeSlice slice;
    slice.ep_weak_ptr = ep;

    // Simulate workers.cpp completion path
    if (auto locked = slice.ep_weak_ptr.lock()) {
        locked->acknowledge();
        locked->reset();
    }
    EXPECT_EQ(ep->acknowledge_calls, 1);
    EXPECT_EQ(ep->reset_calls, 1);
}

TEST(EndpointLifecycleTest, SliceAccessAfterEndpointEvicted) {
    FakeSlice slice;
    {
        auto ep = std::make_shared<FakeEndPoint>();
        slice.ep_weak_ptr = ep;
    }  // endpoint evicted — shared_ptr destroyed

    // Simulate workers.cpp: lock() returns nullptr, gracefully skip
    auto locked = slice.ep_weak_ptr.lock();
    EXPECT_EQ(locked, nullptr);
    // No crash — the slice safely detected endpoint destruction
}

TEST(EndpointLifecycleTest, MultipleSlicesSameEndpoint) {
    auto ep = std::make_shared<FakeEndPoint>();
    FakeSlice slices[3];
    for (auto& s : slices) s.ep_weak_ptr = ep;

    // All slices can lock while endpoint alive
    for (auto& s : slices) {
        auto locked = s.ep_weak_ptr.lock();
        ASSERT_NE(locked, nullptr);
        locked->acknowledge();
    }
    EXPECT_EQ(ep->acknowledge_calls, 3);

    // Simulate eviction: drop the owning shared_ptr
    ep.reset();

    // All slices now get nullptr
    for (auto& s : slices) {
        EXPECT_EQ(s.ep_weak_ptr.lock(), nullptr);
    }
}

TEST(EndpointLifecycleTest, WeakPtrResetClearsReference) {
    auto ep = std::make_shared<FakeEndPoint>();
    FakeSlice slice;
    slice.ep_weak_ptr = ep;

    // Simulate rdma_transport.cpp slice initialization: reset()
    slice.ep_weak_ptr.reset();
    EXPECT_TRUE(slice.ep_weak_ptr.expired());
    EXPECT_EQ(slice.ep_weak_ptr.lock(), nullptr);

    // Original endpoint still alive
    EXPECT_NE(ep, nullptr);
}

}  // namespace
}  // namespace tent
}  // namespace mooncake
