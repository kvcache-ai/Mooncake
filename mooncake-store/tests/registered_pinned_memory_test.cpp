#define MOONCAKE_STORE_TEST
#include "../src/registered_pinned_memory.h"

#include <array>
#include <string>

#include <gtest/gtest.h>

namespace mooncake {
namespace {

struct FakePinState {
    bool register_succeeds = true;
    RegisteredPinnedMemoryManager::UnregisterResult unregister_result =
        RegisteredPinnedMemoryManager::UnregisterResult::kSuccess;
    int register_calls = 0;
    int unregister_calls = 0;
};

FakePinState& State() {
    static FakePinState state;
    return state;
}

void ResetState() { State() = FakePinState(); }

bool FakeRegister(void* addr, size_t size, std::string* error_message) {
    (void)addr;
    (void)size;
    ++State().register_calls;
    if (State().register_succeeds) return true;
    if (error_message) *error_message = "fake register failure";
    return false;
}

RegisteredPinnedMemoryManager::UnregisterResult FakeUnregister(
    void* addr, std::string* error_message) {
    (void)addr;
    ++State().unregister_calls;
    if (State().unregister_result ==
            RegisteredPinnedMemoryManager::UnregisterResult::kError &&
        error_message) {
        *error_message = "fake unregister failure";
    }
    return State().unregister_result;
}

RegisteredPinnedMemoryManager::PinOps FakeOps() {
    return {FakeRegister, FakeUnregister};
}

TEST(RegisteredPinnedMemoryManagerTest, QuotaRejectsAndReleaseRefunds) {
    ResetState();
    RegisteredPinnedMemoryManager manager({true, 64}, FakeOps());
    std::array<char, 128> buffer{};

    auto first = manager.try_pin(buffer.data(), 64, "first");
    ASSERT_NE(first, nullptr);

    auto over_quota = manager.try_pin(buffer.data() + 64, 1, "over quota");
    EXPECT_EQ(over_quota, nullptr);
    EXPECT_EQ(State().register_calls, 1);

    first.reset();
    EXPECT_EQ(State().unregister_calls, 1);

    auto second = manager.try_pin(buffer.data() + 64, 64, "second");
    ASSERT_NE(second, nullptr);
    EXPECT_EQ(State().register_calls, 2);

    second.reset();
    EXPECT_EQ(State().unregister_calls, 2);
}

TEST(RegisteredPinnedMemoryManagerTest, OverlapAndDuplicateAreRejected) {
    ResetState();
    RegisteredPinnedMemoryManager manager({true, 128}, FakeOps());
    std::array<char, 128> buffer{};

    auto first = manager.try_pin(buffer.data() + 16, 32, "first");
    ASSERT_NE(first, nullptr);

    auto duplicate = manager.try_pin(buffer.data() + 16, 32, "duplicate");
    EXPECT_EQ(duplicate, nullptr);

    auto overlap = manager.try_pin(buffer.data() + 32, 16, "overlap");
    EXPECT_EQ(overlap, nullptr);

    auto adjacent = manager.try_pin(buffer.data() + 48, 16, "adjacent");
    ASSERT_NE(adjacent, nullptr);

    EXPECT_EQ(State().register_calls, 2);
    adjacent.reset();
    first.reset();
    EXPECT_EQ(State().unregister_calls, 2);
}

TEST(RegisteredPinnedMemoryManagerTest,
     UnregisterFailureDropsTrackingAndRefunds) {
    ResetState();
    RegisteredPinnedMemoryManager manager({true, 32}, FakeOps());
    std::array<char, 32> buffer{};

    auto first = manager.try_pin(buffer.data(), 32, "first");
    ASSERT_NE(first, nullptr);

    State().unregister_result =
        RegisteredPinnedMemoryManager::UnregisterResult::kError;
    first.reset();
    EXPECT_EQ(State().unregister_calls, 1);

    State().unregister_result =
        RegisteredPinnedMemoryManager::UnregisterResult::kSuccess;
    auto second = manager.try_pin(buffer.data(), 32, "second");
    ASSERT_NE(second, nullptr);
    EXPECT_EQ(State().register_calls, 2);

    second.reset();
    EXPECT_EQ(State().unregister_calls, 2);
}

TEST(RegisteredPinnedMemoryManagerTest, RegisterFailureRefundsReservation) {
    ResetState();
    RegisteredPinnedMemoryManager manager({true, 32}, FakeOps());
    std::array<char, 32> buffer{};

    State().register_succeeds = false;
    auto failed = manager.try_pin(buffer.data(), 32, "failed");
    EXPECT_EQ(failed, nullptr);
    EXPECT_EQ(State().register_calls, 1);
    EXPECT_EQ(State().unregister_calls, 0);

    State().register_succeeds = true;
    auto retried = manager.try_pin(buffer.data(), 32, "retried");
    ASSERT_NE(retried, nullptr);
    EXPECT_EQ(State().register_calls, 2);

    retried.reset();
    EXPECT_EQ(State().unregister_calls, 1);
}

}  // namespace
}  // namespace mooncake
