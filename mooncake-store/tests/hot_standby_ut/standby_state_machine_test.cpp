#include "standby_state_machine.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <thread>
#include <vector>

namespace mooncake::test {

class StandbyStateMachineTest : public ::testing::Test {
   protected:
    void SetUp() override {
        google::InitGoogleLogging("StandbyStateMachineTest");
        FLAGS_logtostderr = true;
        machine_ = std::make_unique<StandbyStateMachine>();
    }

    void TearDown() override { google::ShutdownGoogleLogging(); }

    std::unique_ptr<StandbyStateMachine> machine_;

    // Helper function to reach WATCHING state
    void ReachWatchingState() {
        machine_->ProcessEvent(StandbyEvent::START);
        machine_->ProcessEvent(StandbyEvent::CONNECTED);
        machine_->ProcessEvent(StandbyEvent::SYNC_COMPLETE);
        EXPECT_EQ(StandbyState::WATCHING, machine_->GetState());
    }

    // Helper function to reach SYNCING state
    void ReachSyncingState() {
        machine_->ProcessEvent(StandbyEvent::START);
        machine_->ProcessEvent(StandbyEvent::CONNECTED);
        EXPECT_EQ(StandbyState::SYNCING, machine_->GetState());
    }
};

// ========== Initial State Tests ==========

TEST_F(StandbyStateMachineTest, TestInitialState) {
    EXPECT_EQ(StandbyState::STOPPED, machine_->GetState());
    EXPECT_FALSE(machine_->IsRunning());
    EXPECT_FALSE(machine_->IsConnected());
    EXPECT_FALSE(machine_->IsWatchHealthy());
    EXPECT_FALSE(machine_->IsReadyForPromotion());
    EXPECT_EQ(0, machine_->GetConsecutiveErrors());
    EXPECT_EQ(0, machine_->GetReconnectCount());
}

// ========== Basic State Transition Tests ==========

TEST_F(StandbyStateMachineTest, TestStartTransition) {
    auto result = machine_->ProcessEvent(StandbyEvent::START);
    EXPECT_TRUE(result.allowed);
    EXPECT_EQ(StandbyState::STOPPED, result.old_state);
    EXPECT_EQ(StandbyState::CONNECTING, result.new_state);
    EXPECT_EQ(StandbyState::CONNECTING, machine_->GetState());
    // In CONNECTING state, syncing has not actually started yet, so
    // IsRunning/IsConnected should both be false
    EXPECT_FALSE(machine_->IsRunning());
    EXPECT_FALSE(machine_->IsConnected());
}

TEST_F(StandbyStateMachineTest, TestConnectedTransition) {
    machine_->ProcessEvent(StandbyEvent::START);
    EXPECT_EQ(StandbyState::CONNECTING, machine_->GetState());

    auto result = machine_->ProcessEvent(StandbyEvent::CONNECTED);
    EXPECT_TRUE(result.allowed);
    EXPECT_EQ(StandbyState::CONNECTING, result.old_state);
    EXPECT_EQ(StandbyState::SYNCING, result.new_state);
    EXPECT_EQ(StandbyState::SYNCING, machine_->GetState());
    EXPECT_TRUE(machine_->IsRunning());
    EXPECT_TRUE(machine_->IsConnected());
}

TEST_F(StandbyStateMachineTest, TestSyncCompleteTransition) {
    ReachSyncingState();

    auto result = machine_->ProcessEvent(StandbyEvent::SYNC_COMPLETE);
    EXPECT_TRUE(result.allowed);
    EXPECT_EQ(StandbyState::SYNCING, result.old_state);
    EXPECT_EQ(StandbyState::WATCHING, result.new_state);
    EXPECT_EQ(StandbyState::WATCHING, machine_->GetState());
    EXPECT_TRUE(machine_->IsRunning());
    EXPECT_TRUE(machine_->IsConnected());
    EXPECT_TRUE(machine_->IsWatchHealthy());
    EXPECT_TRUE(machine_->IsReadyForPromotion());
}

TEST_F(StandbyStateMachineTest, TestWatchHealthyNoOp) {
    ReachWatchingState();

    // WATCH_HEALTHY in WATCHING state is a no-op (stays in WATCHING)
    auto result = machine_->ProcessEvent(StandbyEvent::WATCH_HEALTHY);
    EXPECT_TRUE(result.allowed);
    EXPECT_EQ(StandbyState::WATCHING, result.old_state);
    EXPECT_EQ(StandbyState::WATCHING, result.new_state);
    EXPECT_EQ(StandbyState::WATCHING, machine_->GetState());
}

TEST_F(StandbyStateMachineTest, TestWatchBrokenTransition) {
    ReachWatchingState();

    auto result = machine_->ProcessEvent(StandbyEvent::WATCH_BROKEN);
    EXPECT_TRUE(result.allowed);
    EXPECT_EQ(StandbyState::WATCHING, result.old_state);
    EXPECT_EQ(StandbyState::RECONNECTING, result.new_state);
    EXPECT_EQ(StandbyState::RECONNECTING, machine_->GetState());
    EXPECT_TRUE(machine_->IsRunning());
    EXPECT_FALSE(machine_->IsWatchHealthy());
    EXPECT_FALSE(machine_->IsReadyForPromotion());
}

TEST_F(StandbyStateMachineTest, TestDisconnectedFromWatching) {
    ReachWatchingState();

    auto result = machine_->ProcessEvent(StandbyEvent::DISCONNECTED);
    EXPECT_TRUE(result.allowed);
    EXPECT_EQ(StandbyState::WATCHING, result.old_state);
    EXPECT_EQ(StandbyState::RECONNECTING, result.new_state);
    EXPECT_EQ(StandbyState::RECONNECTING, machine_->GetState());
}

TEST_F(StandbyStateMachineTest, TestPromoteTransition) {
    ReachWatchingState();

    auto result = machine_->ProcessEvent(StandbyEvent::PROMOTE);
    EXPECT_TRUE(result.allowed);
    EXPECT_EQ(StandbyState::WATCHING, result.old_state);
    EXPECT_EQ(StandbyState::PROMOTING, result.new_state);
    EXPECT_EQ(StandbyState::PROMOTING, machine_->GetState());
    EXPECT_TRUE(machine_->IsRunning());
    EXPECT_TRUE(machine_->IsConnected());
    EXPECT_FALSE(machine_->IsWatchHealthy());
    EXPECT_FALSE(machine_->IsReadyForPromotion());
}

TEST_F(StandbyStateMachineTest, TestPromotionSuccessTransition) {
    ReachWatchingState();
    machine_->ProcessEvent(StandbyEvent::PROMOTE);
    EXPECT_EQ(StandbyState::PROMOTING, machine_->GetState());

    auto result = machine_->ProcessEvent(StandbyEvent::PROMOTION_SUCCESS);
    EXPECT_TRUE(result.allowed);
    EXPECT_EQ(StandbyState::PROMOTING, result.old_state);
    EXPECT_EQ(StandbyState::PROMOTED, result.new_state);
    EXPECT_EQ(StandbyState::PROMOTED, machine_->GetState());
    EXPECT_FALSE(machine_->IsRunning());
    EXPECT_FALSE(machine_->IsConnected());
}

TEST_F(StandbyStateMachineTest, TestPromotionFailedTransition) {
    ReachWatchingState();
    machine_->ProcessEvent(StandbyEvent::PROMOTE);
    EXPECT_EQ(StandbyState::PROMOTING, machine_->GetState());

    auto result = machine_->ProcessEvent(StandbyEvent::PROMOTION_FAILED);
    EXPECT_TRUE(result.allowed);
    EXPECT_EQ(StandbyState::PROMOTING, result.old_state);
    EXPECT_EQ(StandbyState::FAILED, result.new_state);
    EXPECT_EQ(StandbyState::FAILED, machine_->GetState());
    EXPECT_FALSE(machine_->IsRunning());
}

TEST_F(StandbyStateMachineTest, TestStopTransition) {
    ReachWatchingState();

    auto result = machine_->ProcessEvent(StandbyEvent::STOP);
    EXPECT_TRUE(result.allowed);
    EXPECT_EQ(StandbyState::WATCHING, result.old_state);
    EXPECT_EQ(StandbyState::STOPPED, result.new_state);
    EXPECT_EQ(StandbyState::STOPPED, machine_->GetState());
    EXPECT_FALSE(machine_->IsRunning());
    EXPECT_FALSE(machine_->IsConnected());
}

// ========== Error and Failure State Tests ==========

TEST_F(StandbyStateMachineTest, TestConnectionFailedFromConnecting) {
    machine_->ProcessEvent(StandbyEvent::START);
    EXPECT_EQ(StandbyState::CONNECTING, machine_->GetState());

    auto result = machine_->ProcessEvent(StandbyEvent::CONNECTION_FAILED);
    EXPECT_TRUE(result.allowed);
    EXPECT_EQ(StandbyState::CONNECTING, result.old_state);
    EXPECT_EQ(StandbyState::FAILED, result.new_state);
    EXPECT_EQ(StandbyState::FAILED, machine_->GetState());
    EXPECT_FALSE(machine_->IsRunning());
}

TEST_F(StandbyStateMachineTest, TestFatalErrorFromConnecting) {
    machine_->ProcessEvent(StandbyEvent::START);
    EXPECT_EQ(StandbyState::CONNECTING, machine_->GetState());

    auto result = machine_->ProcessEvent(StandbyEvent::FATAL_ERROR);
    EXPECT_TRUE(result.allowed);
    EXPECT_EQ(StandbyState::CONNECTING, result.old_state);
    EXPECT_EQ(StandbyState::FAILED, result.new_state);
    EXPECT_EQ(StandbyState::FAILED, machine_->GetState());
}

TEST_F(StandbyStateMachineTest, TestSyncFailedFromSyncing) {
    ReachSyncingState();

    auto result = machine_->ProcessEvent(StandbyEvent::SYNC_FAILED);
    EXPECT_TRUE(result.allowed);
    EXPECT_EQ(StandbyState::SYNCING, result.old_state);
    EXPECT_EQ(StandbyState::RECONNECTING, result.new_state);
    EXPECT_EQ(StandbyState::RECONNECTING, machine_->GetState());
    EXPECT_TRUE(machine_->IsRunning());
}

TEST_F(StandbyStateMachineTest, TestDisconnectedFromSyncing) {
    ReachSyncingState();

    auto result = machine_->ProcessEvent(StandbyEvent::DISCONNECTED);
    EXPECT_TRUE(result.allowed);
    EXPECT_EQ(StandbyState::SYNCING, result.old_state);
    EXPECT_EQ(StandbyState::RECONNECTING, result.new_state);
    EXPECT_EQ(StandbyState::RECONNECTING, machine_->GetState());
}

TEST_F(StandbyStateMachineTest, TestFatalErrorFromSyncing) {
    ReachSyncingState();

    auto result = machine_->ProcessEvent(StandbyEvent::FATAL_ERROR);
    EXPECT_TRUE(result.allowed);
    EXPECT_EQ(StandbyState::SYNCING, result.old_state);
    EXPECT_EQ(StandbyState::FAILED, result.new_state);
    EXPECT_EQ(StandbyState::FAILED, machine_->GetState());
}

TEST_F(StandbyStateMachineTest, TestFatalErrorFromWatching) {
    ReachWatchingState();

    auto result = machine_->ProcessEvent(StandbyEvent::FATAL_ERROR);
    EXPECT_TRUE(result.allowed);
    EXPECT_EQ(StandbyState::WATCHING, result.old_state);
    EXPECT_EQ(StandbyState::FAILED, result.new_state);
    EXPECT_EQ(StandbyState::FAILED, machine_->GetState());
}

// ========== Reconnecting State Tests ==========

TEST_F(StandbyStateMachineTest, TestReconnectingToSyncing) {
    ReachWatchingState();
    machine_->ProcessEvent(StandbyEvent::WATCH_BROKEN);
    EXPECT_EQ(StandbyState::RECONNECTING, machine_->GetState());

    auto result = machine_->ProcessEvent(StandbyEvent::CONNECTED);
    EXPECT_TRUE(result.allowed);
    EXPECT_EQ(StandbyState::RECONNECTING, result.old_state);
    EXPECT_EQ(StandbyState::SYNCING, result.new_state);
    EXPECT_EQ(StandbyState::SYNCING, machine_->GetState());
}

TEST_F(StandbyStateMachineTest, TestReconnectingToFailed) {
    ReachWatchingState();
    machine_->ProcessEvent(StandbyEvent::WATCH_BROKEN);
    EXPECT_EQ(StandbyState::RECONNECTING, machine_->GetState());

    auto result = machine_->ProcessEvent(StandbyEvent::FATAL_ERROR);
    EXPECT_TRUE(result.allowed);
    EXPECT_EQ(StandbyState::RECONNECTING, result.old_state);
    EXPECT_EQ(StandbyState::FAILED, result.new_state);
    EXPECT_EQ(StandbyState::FAILED, machine_->GetState());
}

TEST_F(StandbyStateMachineTest, TestReconnectingMaxErrors) {
    ReachWatchingState();
    machine_->ProcessEvent(StandbyEvent::WATCH_BROKEN);
    EXPECT_EQ(StandbyState::RECONNECTING, machine_->GetState());

    auto result = machine_->ProcessEvent(StandbyEvent::MAX_ERRORS_REACHED);
    EXPECT_TRUE(result.allowed);
    EXPECT_EQ(StandbyState::RECONNECTING, result.old_state);
    EXPECT_EQ(StandbyState::FAILED, result.new_state);
    EXPECT_EQ(StandbyState::FAILED, machine_->GetState());
}

// ========== Recovering State Tests ==========

TEST_F(StandbyStateMachineTest, TestRecoveringToWatching) {
    ReachWatchingState();
    machine_->ProcessEvent(StandbyEvent::MAX_ERRORS_REACHED);
    EXPECT_EQ(StandbyState::RECOVERING, machine_->GetState());

    auto result = machine_->ProcessEvent(StandbyEvent::RECOVERY_SUCCESS);
    EXPECT_TRUE(result.allowed);
    EXPECT_EQ(StandbyState::RECOVERING, result.old_state);
    EXPECT_EQ(StandbyState::WATCHING, result.new_state);
    EXPECT_EQ(StandbyState::WATCHING, machine_->GetState());
}

TEST_F(StandbyStateMachineTest, TestRecoveringToReconnecting) {
    ReachWatchingState();
    machine_->ProcessEvent(StandbyEvent::MAX_ERRORS_REACHED);
    EXPECT_EQ(StandbyState::RECOVERING, machine_->GetState());

    auto result = machine_->ProcessEvent(StandbyEvent::RECOVERY_FAILED);
    EXPECT_TRUE(result.allowed);
    EXPECT_EQ(StandbyState::RECOVERING, result.old_state);
    EXPECT_EQ(StandbyState::RECONNECTING, result.new_state);
    EXPECT_EQ(StandbyState::RECONNECTING, machine_->GetState());
}

TEST_F(StandbyStateMachineTest, TestRecoveringDisconnected) {
    ReachWatchingState();
    machine_->ProcessEvent(StandbyEvent::MAX_ERRORS_REACHED);
    EXPECT_EQ(StandbyState::RECOVERING, machine_->GetState());

    auto result = machine_->ProcessEvent(StandbyEvent::DISCONNECTED);
    EXPECT_TRUE(result.allowed);
    EXPECT_EQ(StandbyState::RECOVERING, result.old_state);
    EXPECT_EQ(StandbyState::RECONNECTING, result.new_state);
    EXPECT_EQ(StandbyState::RECONNECTING, machine_->GetState());
}

TEST_F(StandbyStateMachineTest, TestRecoveringFatalError) {
    ReachWatchingState();
    machine_->ProcessEvent(StandbyEvent::MAX_ERRORS_REACHED);
    EXPECT_EQ(StandbyState::RECOVERING, machine_->GetState());

    auto result = machine_->ProcessEvent(StandbyEvent::FATAL_ERROR);
    EXPECT_TRUE(result.allowed);
    EXPECT_EQ(StandbyState::RECOVERING, result.old_state);
    EXPECT_EQ(StandbyState::FAILED, result.new_state);
    EXPECT_EQ(StandbyState::FAILED, machine_->GetState());
}

// ========== Failed State Tests ==========

TEST_F(StandbyStateMachineTest, TestFailedToStopped) {
    machine_->ProcessEvent(StandbyEvent::START);
    machine_->ProcessEvent(StandbyEvent::FATAL_ERROR);
    EXPECT_EQ(StandbyState::FAILED, machine_->GetState());

    auto result = machine_->ProcessEvent(StandbyEvent::STOP);
    EXPECT_TRUE(result.allowed);
    EXPECT_EQ(StandbyState::FAILED, result.old_state);
    EXPECT_EQ(StandbyState::STOPPED, result.new_state);
    EXPECT_EQ(StandbyState::STOPPED, machine_->GetState());
}

TEST_F(StandbyStateMachineTest, TestFailedToConnecting) {
    machine_->ProcessEvent(StandbyEvent::START);
    machine_->ProcessEvent(StandbyEvent::FATAL_ERROR);
    EXPECT_EQ(StandbyState::FAILED, machine_->GetState());

    // Allow restart from FAILED state
    auto result = machine_->ProcessEvent(StandbyEvent::START);
    EXPECT_TRUE(result.allowed);
    EXPECT_EQ(StandbyState::FAILED, result.old_state);
    EXPECT_EQ(StandbyState::CONNECTING, result.new_state);
    EXPECT_EQ(StandbyState::CONNECTING, machine_->GetState());
}

// ========== Promoted State Tests ==========

TEST_F(StandbyStateMachineTest, TestPromotedToStopped) {
    ReachWatchingState();
    machine_->ProcessEvent(StandbyEvent::PROMOTE);
    machine_->ProcessEvent(StandbyEvent::PROMOTION_SUCCESS);
    EXPECT_EQ(StandbyState::PROMOTED, machine_->GetState());

    auto result = machine_->ProcessEvent(StandbyEvent::STOP);
    EXPECT_TRUE(result.allowed);
    EXPECT_EQ(StandbyState::PROMOTED, result.old_state);
    EXPECT_EQ(StandbyState::STOPPED, result.new_state);
    EXPECT_EQ(StandbyState::STOPPED, machine_->GetState());
}

// ========== Invalid Transition Tests ==========

TEST_F(StandbyStateMachineTest, TestInvalidTransitions) {
    // Cannot transition from STOPPED directly to WATCHING
    auto result1 = machine_->ProcessEvent(StandbyEvent::SYNC_COMPLETE);
    EXPECT_FALSE(result1.allowed);
    EXPECT_EQ(StandbyState::STOPPED, machine_->GetState());

    // Cannot promote when not in WATCHING state
    ReachSyncingState();
    auto result2 = machine_->ProcessEvent(StandbyEvent::PROMOTE);
    EXPECT_FALSE(result2.allowed);
    EXPECT_EQ(StandbyState::SYNCING, machine_->GetState());

    // Cannot transition from STOPPED to CONNECTED
    machine_->ProcessEvent(StandbyEvent::STOP);
    auto result3 = machine_->ProcessEvent(StandbyEvent::CONNECTED);
    EXPECT_FALSE(result3.allowed);
    EXPECT_EQ(StandbyState::STOPPED, machine_->GetState());
}

// ========== Error Handling Tests ==========

TEST_F(StandbyStateMachineTest, TestConsecutiveErrors) {
    ReachWatchingState();

    // Simulate multiple errors
    for (int i = 0; i < 5; ++i) {
        machine_->IncrementErrors();
    }
    EXPECT_EQ(5, machine_->GetConsecutiveErrors());

    // Reset errors
    machine_->ResetErrors();
    EXPECT_EQ(0, machine_->GetConsecutiveErrors());
}

TEST_F(StandbyStateMachineTest, TestMaxErrorsReachedAutoTransition) {
    ReachWatchingState();

    // IncrementErrors() automatically triggers MAX_ERRORS_REACHED when
    // threshold is reached
    for (int i = 0; i < StandbyStateMachine::kMaxConsecutiveErrors; ++i) {
        machine_->IncrementErrors();
    }

    // Should have transitioned to RECOVERING (from WATCHING on
    // MAX_ERRORS_REACHED)
    EXPECT_EQ(StandbyState::RECOVERING, machine_->GetState());
    EXPECT_EQ(StandbyStateMachine::kMaxConsecutiveErrors,
              machine_->GetConsecutiveErrors());
}

TEST_F(StandbyStateMachineTest, TestMaxErrorsReachedManual) {
    ReachWatchingState();

    // Manually trigger MAX_ERRORS_REACHED
    auto result = machine_->ProcessEvent(StandbyEvent::MAX_ERRORS_REACHED);
    EXPECT_TRUE(result.allowed);
    EXPECT_EQ(StandbyState::WATCHING, result.old_state);
    EXPECT_EQ(StandbyState::RECOVERING, result.new_state);
    EXPECT_EQ(StandbyState::RECOVERING, machine_->GetState());
}

TEST_F(StandbyStateMachineTest, TestReconnectCount) {
    EXPECT_EQ(0, machine_->GetReconnectCount());

    machine_->IncrementReconnectCount();
    EXPECT_EQ(1, machine_->GetReconnectCount());

    machine_->IncrementReconnectCount();
    EXPECT_EQ(2, machine_->GetReconnectCount());

    machine_->ResetReconnectCount();
    EXPECT_EQ(0, machine_->GetReconnectCount());
}

// ========== Callback Tests ==========

TEST_F(StandbyStateMachineTest, TestStateChangeCallback) {
    std::vector<StandbyState> state_history;
    std::vector<StandbyEvent> event_history;

    machine_->RegisterCallback([&](StandbyState old_state,
                                   StandbyState new_state, StandbyEvent event) {
        state_history.push_back(new_state);
        event_history.push_back(event);
    });

    // Trigger state transitions
    machine_->ProcessEvent(StandbyEvent::START);
    machine_->ProcessEvent(StandbyEvent::CONNECTED);
    machine_->ProcessEvent(StandbyEvent::SYNC_COMPLETE);

    // Verify callbacks were called
    EXPECT_EQ(3, state_history.size());
    EXPECT_EQ(StandbyState::CONNECTING, state_history[0]);
    EXPECT_EQ(StandbyState::SYNCING, state_history[1]);
    EXPECT_EQ(StandbyState::WATCHING, state_history[2]);
    EXPECT_EQ(StandbyEvent::START, event_history[0]);
    EXPECT_EQ(StandbyEvent::CONNECTED, event_history[1]);
    EXPECT_EQ(StandbyEvent::SYNC_COMPLETE, event_history[2]);
}

TEST_F(StandbyStateMachineTest, TestMultipleCallbacks) {
    int callback1_count = 0;
    int callback2_count = 0;

    machine_->RegisterCallback(
        [&](StandbyState, StandbyState, StandbyEvent) { callback1_count++; });
    machine_->RegisterCallback(
        [&](StandbyState, StandbyState, StandbyEvent) { callback2_count++; });

    // Trigger state transitions
    machine_->ProcessEvent(StandbyEvent::START);
    machine_->ProcessEvent(StandbyEvent::CONNECTED);

    // Both callbacks should be called
    EXPECT_EQ(2, callback1_count);
    EXPECT_EQ(2, callback2_count);
}

TEST_F(StandbyStateMachineTest, TestCallbackExceptionHandling) {
    bool callback_called = false;

    machine_->RegisterCallback([&](StandbyState, StandbyState, StandbyEvent) {
        callback_called = true;
    });

    // Callback should be invoked and must not interfere with state transitions
    auto result = machine_->ProcessEvent(StandbyEvent::START);
    EXPECT_TRUE(result.allowed);
    EXPECT_EQ(StandbyState::CONNECTING, machine_->GetState());
    EXPECT_TRUE(callback_called);
}

// ========== History Tests ==========

TEST_F(StandbyStateMachineTest, TestTransitionHistory) {
    // Perform several transitions
    machine_->ProcessEvent(StandbyEvent::START);
    machine_->ProcessEvent(StandbyEvent::CONNECTED);
    machine_->ProcessEvent(StandbyEvent::SYNC_COMPLETE);

    auto history = machine_->GetTransitionHistory(10);
    EXPECT_EQ(3, history.size());
    EXPECT_EQ(StandbyState::STOPPED, history[0].from_state);
    EXPECT_EQ(StandbyState::CONNECTING, history[0].to_state);
    EXPECT_EQ(StandbyEvent::START, history[0].event);

    EXPECT_EQ(StandbyState::CONNECTING, history[1].from_state);
    EXPECT_EQ(StandbyState::SYNCING, history[1].to_state);
    EXPECT_EQ(StandbyEvent::CONNECTED, history[1].event);

    EXPECT_EQ(StandbyState::SYNCING, history[2].from_state);
    EXPECT_EQ(StandbyState::WATCHING, history[2].to_state);
    EXPECT_EQ(StandbyEvent::SYNC_COMPLETE, history[2].event);
}

TEST_F(StandbyStateMachineTest, TestTransitionHistoryLimit) {
    // Perform many transitions to test history limit
    for (int i = 0; i < 20; ++i) {
        machine_->ProcessEvent(StandbyEvent::START);
        machine_->ProcessEvent(StandbyEvent::STOP);
    }

    // Request limited history
    auto history = machine_->GetTransitionHistory(5);
    EXPECT_LE(history.size(), 5);
}

TEST_F(StandbyStateMachineTest, TestTimeInState) {
    machine_->ProcessEvent(StandbyEvent::START);

    // Wait a bit
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    auto time_in_state = machine_->GetTimeInCurrentState();
    EXPECT_GE(time_in_state.count(), 100);
    EXPECT_LE(time_in_state.count(),
              200);  // Allow some margin for test execution
}

// ========== Concurrent Tests ==========

TEST_F(StandbyStateMachineTest, TestConcurrentStateQueries) {
    ReachWatchingState();

    // Multiple threads querying state concurrently
    std::vector<std::thread> threads;
    std::atomic<int> success_count{0};

    for (int i = 0; i < 10; ++i) {
        threads.emplace_back([&]() {
            for (int j = 0; j < 100; ++j) {
                StandbyState state = machine_->GetState();
                if (state == StandbyState::WATCHING) {
                    success_count++;
                }
            }
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    EXPECT_EQ(1000, success_count.load());
}

TEST_F(StandbyStateMachineTest, TestConcurrentEventProcessing) {
    ReachWatchingState();

    // Multiple threads trying to process events concurrently
    // Only one should succeed (state machine should serialize)
    std::vector<std::thread> threads;
    std::atomic<int> success_count{0};
    std::atomic<int> failure_count{0};

    for (int i = 0; i < 10; ++i) {
        threads.emplace_back([&]() {
            auto result = machine_->ProcessEvent(StandbyEvent::STOP);
            if (result.allowed) {
                success_count++;
            } else {
                failure_count++;
            }
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    // Only one STOP should succeed (transition to STOPPED)
    EXPECT_EQ(1, success_count.load());
    EXPECT_EQ(9, failure_count.load());
    EXPECT_EQ(StandbyState::STOPPED, machine_->GetState());
}

// ========== State Query Tests ==========

TEST_F(StandbyStateMachineTest, TestIsRunning) {
    EXPECT_FALSE(machine_->IsRunning());  // STOPPED

    machine_->ProcessEvent(StandbyEvent::START);
    // CONNECTING only means establishing connection; sync has not started, so
    // it is not considered "running"
    EXPECT_FALSE(machine_->IsRunning());  // CONNECTING

    machine_->ProcessEvent(StandbyEvent::CONNECTED);
    EXPECT_TRUE(machine_->IsRunning());  // SYNCING

    machine_->ProcessEvent(StandbyEvent::SYNC_COMPLETE);
    EXPECT_TRUE(machine_->IsRunning());  // WATCHING

    machine_->ProcessEvent(StandbyEvent::STOP);
    EXPECT_FALSE(machine_->IsRunning());  // STOPPED
}

TEST_F(StandbyStateMachineTest, TestIsConnected) {
    EXPECT_FALSE(machine_->IsConnected());  // STOPPED

    machine_->ProcessEvent(StandbyEvent::START);
    EXPECT_FALSE(machine_->IsConnected());  // CONNECTING

    machine_->ProcessEvent(StandbyEvent::CONNECTED);
    EXPECT_TRUE(machine_->IsConnected());  // SYNCING

    machine_->ProcessEvent(StandbyEvent::SYNC_COMPLETE);
    EXPECT_TRUE(machine_->IsConnected());  // WATCHING

    machine_->ProcessEvent(StandbyEvent::STOP);
    EXPECT_FALSE(machine_->IsConnected());  // STOPPED
}

TEST_F(StandbyStateMachineTest, TestIsWatchHealthy) {
    EXPECT_FALSE(machine_->IsWatchHealthy());  // STOPPED

    ReachWatchingState();
    EXPECT_TRUE(machine_->IsWatchHealthy());  // WATCHING

    machine_->ProcessEvent(StandbyEvent::WATCH_BROKEN);
    EXPECT_FALSE(machine_->IsWatchHealthy());  // RECONNECTING
}

TEST_F(StandbyStateMachineTest, TestIsReadyForPromotion) {
    EXPECT_FALSE(machine_->IsReadyForPromotion());  // STOPPED

    ReachWatchingState();
    EXPECT_TRUE(machine_->IsReadyForPromotion());  // WATCHING

    machine_->ProcessEvent(StandbyEvent::PROMOTE);
    EXPECT_FALSE(machine_->IsReadyForPromotion());  // PROMOTING
}

// ========== Complete State Machine Flow Tests ==========

TEST_F(StandbyStateMachineTest, TestCompleteNormalFlow) {
    // Complete flow: STOPPED -> CONNECTING -> SYNCING -> WATCHING
    EXPECT_EQ(StandbyState::STOPPED, machine_->GetState());

    machine_->ProcessEvent(StandbyEvent::START);
    EXPECT_EQ(StandbyState::CONNECTING, machine_->GetState());

    machine_->ProcessEvent(StandbyEvent::CONNECTED);
    EXPECT_EQ(StandbyState::SYNCING, machine_->GetState());

    machine_->ProcessEvent(StandbyEvent::SYNC_COMPLETE);
    EXPECT_EQ(StandbyState::WATCHING, machine_->GetState());
    EXPECT_TRUE(machine_->IsReadyForPromotion());
}

TEST_F(StandbyStateMachineTest, TestCompletePromotionFlow) {
    // Complete promotion flow
    ReachWatchingState();

    machine_->ProcessEvent(StandbyEvent::PROMOTE);
    EXPECT_EQ(StandbyState::PROMOTING, machine_->GetState());

    machine_->ProcessEvent(StandbyEvent::PROMOTION_SUCCESS);
    EXPECT_EQ(StandbyState::PROMOTED, machine_->GetState());
}

TEST_F(StandbyStateMachineTest, TestCompleteReconnectFlow) {
    // Complete reconnect flow: WATCHING -> RECONNECTING -> SYNCING -> WATCHING
    ReachWatchingState();

    machine_->ProcessEvent(StandbyEvent::WATCH_BROKEN);
    EXPECT_EQ(StandbyState::RECONNECTING, machine_->GetState());

    machine_->ProcessEvent(StandbyEvent::CONNECTED);
    EXPECT_EQ(StandbyState::SYNCING, machine_->GetState());

    machine_->ProcessEvent(StandbyEvent::SYNC_COMPLETE);
    EXPECT_EQ(StandbyState::WATCHING, machine_->GetState());
}

TEST_F(StandbyStateMachineTest, TestCompleteRecoveryFlow) {
    // Complete recovery flow: WATCHING -> RECOVERING -> WATCHING
    ReachWatchingState();

    machine_->ProcessEvent(StandbyEvent::MAX_ERRORS_REACHED);
    EXPECT_EQ(StandbyState::RECOVERING, machine_->GetState());

    machine_->ProcessEvent(StandbyEvent::RECOVERY_SUCCESS);
    EXPECT_EQ(StandbyState::WATCHING, machine_->GetState());
}

}  // namespace mooncake::test

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
