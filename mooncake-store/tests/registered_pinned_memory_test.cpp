#define MOONCAKE_STORE_TEST
#include "../src/registered_pinned_memory.h"

#include <array>

#include <gtest/gtest.h>

namespace mooncake {
namespace {

using Manager = RegisteredPinnedMemoryManager;
using UnregisterResult = Manager::UnregisterResult;

struct FakePinState {
    bool register_succeeds = true;
    UnregisterResult unregister_result = UnregisterResult::kSuccess;
    int register_calls = 0;
    int unregister_calls = 0;
};

FakePinState& State() {
    static FakePinState state;
    return state;
}

bool FakeRegister(void*, size_t, std::string* error_message) {
    ++State().register_calls;
    if (State().register_succeeds) return true;
    if (error_message) *error_message = "fake register failure";
    return false;
}

UnregisterResult FakeUnregister(void*, std::string* error_message) {
    ++State().unregister_calls;
    if (State().unregister_result == UnregisterResult::kError &&
        error_message) {
        *error_message = "fake unregister failure";
    }
    return State().unregister_result;
}

class RegisteredPinnedMemoryManagerTest : public ::testing::Test {
   protected:
    void SetUp() override { State() = FakePinState(); }

    Manager MakeManager(size_t limit) {
        return Manager({true, limit}, {FakeRegister, FakeUnregister});
    }

    std::shared_ptr<RegisteredPinnedRegion> Pin(Manager& manager, size_t offset,
                                                size_t size) {
        return manager.try_pin(buffer_.data() + offset, size, "segment");
    }

    void ExpectCalls(int register_calls, int unregister_calls) {
        EXPECT_EQ(State().register_calls, register_calls);
        EXPECT_EQ(State().unregister_calls, unregister_calls);
    }

    std::array<char, 128> buffer_{};
};

TEST_F(RegisteredPinnedMemoryManagerTest, QuotaRejectsAndReleaseRefunds) {
    auto manager = MakeManager(64);

    auto first = Pin(manager, 0, 64);
    ASSERT_NE(first, nullptr);
    EXPECT_EQ(Pin(manager, 64, 1), nullptr);
    ExpectCalls(1, 0);

    first.reset();
    ExpectCalls(1, 1);

    auto second = Pin(manager, 64, 64);
    ASSERT_NE(second, nullptr);
    ExpectCalls(2, 1);

    second.reset();
    ExpectCalls(2, 2);
}

TEST_F(RegisteredPinnedMemoryManagerTest, OverlapAndDuplicateAreRejected) {
    auto manager = MakeManager(128);

    auto first = Pin(manager, 16, 32);
    ASSERT_NE(first, nullptr);

    EXPECT_EQ(Pin(manager, 16, 32), nullptr);
    EXPECT_EQ(Pin(manager, 32, 16), nullptr);

    auto adjacent = Pin(manager, 48, 16);
    ASSERT_NE(adjacent, nullptr);
    ExpectCalls(2, 0);

    adjacent.reset();
    first.reset();
    ExpectCalls(2, 2);
}

TEST_F(RegisteredPinnedMemoryManagerTest, RegisterFailureRefundsReservation) {
    auto manager = MakeManager(32);

    State().register_succeeds = false;
    EXPECT_EQ(Pin(manager, 0, 32), nullptr);
    ExpectCalls(1, 0);

    State().register_succeeds = true;
    auto retried = Pin(manager, 0, 32);
    ASSERT_NE(retried, nullptr);
    ExpectCalls(2, 0);

    retried.reset();
    ExpectCalls(2, 1);
}

TEST_F(RegisteredPinnedMemoryManagerTest,
       UnregisterFailureRetainsReservation) {
    auto manager = MakeManager(32);

    auto first = Pin(manager, 0, 32);
    ASSERT_NE(first, nullptr);

    State().unregister_result = UnregisterResult::kError;
    EXPECT_FALSE(first->release());
    ExpectCalls(1, 1);

    State().unregister_result = UnregisterResult::kSuccess;
    EXPECT_EQ(Pin(manager, 0, 32), nullptr);
    EXPECT_EQ(Pin(manager, 32, 32), nullptr);
    ExpectCalls(1, 1);
}

}  // namespace
}  // namespace mooncake
