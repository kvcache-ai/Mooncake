#include "client_session_event_dispatcher.h"

#include <chrono>
#include <future>
#include <memory>
#include <vector>

#include <gtest/gtest.h>

namespace mooncake::test {
namespace {

using namespace std::chrono_literals;
using State = ClientLivenessState;
const UUID kClient{1, 2};

ClientSessionEvent Transition(State previous, State current) {
    return {kClient,
            std::make_shared<const ClientLivenessRecord>(
                ClientLivenessRecord::TimePoint{}),
            previous, current};
}

TEST(ClientSessionEventDispatcherTest, ListenerRegistrationClosesAtStart) {
    ClientSessionEventDispatcher dispatcher;
    EXPECT_FALSE(dispatcher.AddListener({}));
    ASSERT_TRUE(dispatcher.AddListener([](const auto&) {}));
    dispatcher.Start();
    EXPECT_FALSE(dispatcher.AddListener([](const auto&) {}));
    dispatcher.Stop();
    EXPECT_FALSE(dispatcher.AddListener([](const auto&) {}));
    dispatcher.Start();
    EXPECT_FALSE(dispatcher.AddListener([](const auto&) {}));
    dispatcher.Stop();
}

TEST(ClientSessionEventDispatcherTest, StopBeforeStartAlsoClosesRegistration) {
    ClientSessionEventDispatcher dispatcher;
    dispatcher.Stop();
    EXPECT_FALSE(dispatcher.AddListener([](const auto&) {}));
}

TEST(ClientSessionEventDispatcherTest, ManualDeliveryFreezesListenersInOrder) {
    ClientSessionEventDispatcher dispatcher;
    std::vector<int> order;
    ASSERT_TRUE(dispatcher.AddListener([&](const auto&) {
        order.push_back(1);
        // Registration from a callback must reject rather than deadlock or
        // invalidate the listener vector being traversed.
        EXPECT_FALSE(dispatcher.AddListener([](const auto&) {}));
    }));
    ASSERT_TRUE(
        dispatcher.AddListener([&](const auto&) { order.push_back(2); }));
    dispatcher.Publish(Transition(State::ACTIVE, State::SUSPECTED));
    dispatcher.Drain();
    EXPECT_EQ(order, (std::vector<int>{1, 2}));
    EXPECT_FALSE(dispatcher.AddListener([](const auto&) {}));
    dispatcher.Stop();
}

TEST(ClientSessionEventDispatcherTest, ThreadDeliversQueuedAndLaterEvents) {
    std::promise<void> delivered;
    auto arrived = delivered.get_future();
    std::vector<State> states;
    ClientSessionEventDispatcher dispatcher;
    ASSERT_TRUE(dispatcher.AddListener([&](const auto& event) {
        states.push_back(event.current);
        if (event.current == State::ACTIVE) {
            delivered.set_value();
        }
    }));
    // Transitions published before delivery starts are kept, not dropped.
    dispatcher.Publish(Transition(State::ACTIVE, State::SUSPECTED));
    dispatcher.Start();
    dispatcher.Publish(Transition(State::SUSPECTED, State::ACTIVE));
    EXPECT_EQ(arrived.wait_for(5s), std::future_status::ready);
    dispatcher.Stop();
    EXPECT_EQ(states, (std::vector<State>{State::SUSPECTED, State::ACTIVE}));
    dispatcher.Stop();
}

TEST(ClientSessionEventDispatcherTest, StopDrainsQueuedEventsAndRejectsLater) {
    std::vector<State> states;
    ClientSessionEventDispatcher dispatcher;
    ASSERT_TRUE(dispatcher.AddListener(
        [&](const auto& event) { states.push_back(event.current); }));
    dispatcher.Publish(Transition(State::ACTIVE, State::SUSPECTED));
    dispatcher.Publish(Transition(State::SUSPECTED, State::ACTIVE));
    dispatcher.Stop();
    EXPECT_EQ(states, (std::vector<State>{State::SUSPECTED, State::ACTIVE}));
    dispatcher.Publish(Transition(State::ACTIVE, State::SUSPECTED));
    dispatcher.Stop();
    EXPECT_EQ(states.size(), 2);
}

}  // namespace
}  // namespace mooncake::test
