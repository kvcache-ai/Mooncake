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

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <future>
#include <stdexcept>
#include <thread>
#include <type_traits>
#include <utility>

#include "tent/runtime/registration_guard.h"

namespace mooncake {
namespace tent {
namespace {

using namespace std::chrono_literals;
constexpr auto kDeadline = 5s;

// A deadlocked implementation must fail the test instead of blocking in the
// async future's destructor. All successful paths finish their workers here.
void finish(std::future<void>& worker) {
    if (worker.wait_for(kDeadline) != std::future_status::ready) {
        ADD_FAILURE()
            << "Registration reservation did not finish before deadline";
        std::abort();
    }
    worker.get();
}

static_assert(!std::is_copy_constructible_v<RegistrationCoordinator::Guard>);
static_assert(!std::is_copy_assignable_v<RegistrationCoordinator::Guard>);
static_assert(
    std::is_nothrow_move_constructible_v<RegistrationCoordinator::Guard>);
static_assert(
    std::is_nothrow_move_assignable_v<RegistrationCoordinator::Guard>);

TEST(RegistrationCoordinatorTest, SameAddressWaitsUntilReleased) {
    RegistrationCoordinator coordinator;
    auto held = coordinator.acquire({0x1000});
    std::promise<void> attempting;
    auto attempted = attempting.get_future();
    auto worker = std::async(std::launch::async, [&] {
        attempting.set_value();
        auto guard = coordinator.acquire({0x1000});
    });
    EXPECT_EQ(attempted.wait_for(kDeadline), std::future_status::ready);
    EXPECT_EQ(worker.wait_for(50ms), std::future_status::timeout);
    held.reset();
    finish(worker);
}

TEST(RegistrationCoordinatorTest, DifferentAddressesCanProceedTogether) {
    RegistrationCoordinator coordinator;
    auto held = coordinator.acquire({0x1000});
    auto worker = std::async(std::launch::async, [&] {
        auto guard = coordinator.acquire({0x2000});
    });
    EXPECT_EQ(worker.wait_for(kDeadline), std::future_status::ready);
    held.reset();
    finish(worker);
}

TEST(RegistrationCoordinatorTest, WaitingBatchDoesNotReserveSubset) {
    RegistrationCoordinator coordinator;
    auto held = coordinator.acquire({0x2000});
    std::promise<void> attempting;
    auto attempted = attempting.get_future();
    auto batch = std::async(std::launch::async, [&] {
        attempting.set_value();
        auto guard = coordinator.acquire({0x1000, 0x2000});
    });
    EXPECT_EQ(attempted.wait_for(kDeadline), std::future_status::ready);
    EXPECT_EQ(batch.wait_for(50ms), std::future_status::timeout);
    auto independent = std::async(std::launch::async, [&] {
        auto guard = coordinator.acquire({0x1000});
    });
    EXPECT_EQ(independent.wait_for(kDeadline), std::future_status::ready);
    held.reset();
    finish(batch);
    finish(independent);
}

TEST(RegistrationCoordinatorTest, ReverseBatchesDoNotDeadlock) {
    RegistrationCoordinator coordinator;
    std::promise<void> start;
    auto ready = start.get_future().share();
    std::atomic<int> in_flight{0};
    std::atomic<int> overlaps{0};
    auto run = [&](std::vector<uint64_t> addresses) {
        ready.wait();
        for (int i = 0; i < 500; ++i) {
            auto guard = coordinator.acquire(addresses);
            if (in_flight.fetch_add(1) != 0) ++overlaps;
            std::this_thread::yield();
            in_flight.fetch_sub(1);
        }
    };
    auto forward = std::async(std::launch::async, run,
                              std::vector<uint64_t>{0x1000, 0x2000});
    auto reverse = std::async(std::launch::async, run,
                              std::vector<uint64_t>{0x2000, 0x1000});
    start.set_value();
    finish(forward);
    finish(reverse);
    EXPECT_EQ(overlaps.load(), 0);
    EXPECT_EQ(in_flight.load(), 0);
}

TEST(RegistrationCoordinatorTest, DuplicateAddressesNeedOnlyOneRelease) {
    RegistrationCoordinator coordinator;
    auto worker = std::async(std::launch::async, [&] {
        {
            auto guard = coordinator.acquire({0x1000, 0x1000, 0x1000});
        }
        auto again = coordinator.acquire({0x1000});
    });
    finish(worker);
}

TEST(RegistrationCoordinatorTest, ExceptionReleasesWholeBatch) {
    RegistrationCoordinator coordinator;
    auto worker = std::async(std::launch::async, [&] {
        EXPECT_THROW(
            {
                auto guard = coordinator.acquire({0x1000, 0x2000});
                throw std::runtime_error("injected registration failure");
            },
            std::runtime_error);
        auto again = coordinator.acquire({0x2000, 0x1000});
    });
    finish(worker);
}

TEST(RegistrationCoordinatorTest, MoveTransfersOwnershipAndReleasesOldBatch) {
    RegistrationCoordinator coordinator;
    auto held = coordinator.acquire({0x1000});
    {
        auto source = coordinator.acquire({0x2000});
        auto moved = std::move(source);
        held = std::move(moved);
    }
    auto released = std::async(std::launch::async, [&] {
        auto guard = coordinator.acquire({0x1000});
    });
    EXPECT_EQ(released.wait_for(kDeadline), std::future_status::ready);
    auto retained = std::async(std::launch::async, [&] {
        auto guard = coordinator.acquire({0x2000});
    });
    EXPECT_EQ(retained.wait_for(50ms), std::future_status::timeout);
    held.reset();
    finish(released);
    finish(retained);
}

TEST(RegistrationCoordinatorTest, EmptyBatchDoesNotReserveAnything) {
    RegistrationCoordinator coordinator;
    auto held = coordinator.acquire({0x1000});
    auto worker = std::async(std::launch::async,
                             [&] { auto empty = coordinator.acquire({}); });
    EXPECT_EQ(worker.wait_for(kDeadline), std::future_status::ready);
    held.reset();
    finish(worker);
}

}  // namespace
}  // namespace tent
}  // namespace mooncake
