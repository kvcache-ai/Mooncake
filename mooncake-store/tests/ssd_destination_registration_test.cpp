// Copyright 2026 KVCache.AI
// Licensed under the Apache License, Version 2.0.
#include "ssd_destination_registration.h"

#include <array>
#include <functional>
#include <future>
#include <thread>
#include <unistd.h>
#include <gtest/gtest.h>

namespace mooncake::ssd_offload {
namespace {
constexpr PeerIdentity requester{1, 2, 3}, owner{4, 5, 6};
// Failure injection is at the transport boundary; all lease/refcount behavior
// under test is the production registry and registration adapter.
class RegistrationTransport final : public DestinationRegistrationTransport {
   public:
    TransportRegistrationResult RegisterWritable(
        const DestinationBinding& b,
        const std::string& location) noexcept override {
        ++registrations;
        if (on_register) on_register();
        binding = b;
        registered_location = location;
        return next_registration;
    }
    DestinationError Unregister(uint64_t base) noexcept override {
        ++unregistrations;
        if (on_unregister) on_unregister();
        unregistered_base = base;
        return next_unregister;
    }
    TransportRegistrationResult next_registration{DestinationError::kOk, true,
                                                  true};
    DestinationError next_unregister{DestinationError::kOk};
    size_t registrations{0}, unregistrations{0};
    DestinationBinding binding;
    std::string registered_location;
    uint64_t unregistered_base{0};
    std::function<void()> on_register, on_unregister;
};
struct Buffer {
    std::shared_ptr<std::array<std::byte, 4096>> backing =
        std::make_shared<std::array<std::byte, 4096>>();
    DestinationBinding binding() const {
        const auto base = reinterpret_cast<uint64_t>(backing->data());
        return {base + 16, 4000, base, backing->size()};
    }
};

TEST(SsdDestinationRegistration,
     RegistersExplicitWritableMrAndSecureCredential) {
    auto transport = std::make_shared<RegistrationTransport>();
    DestinationRegistration registration(requester, "transport-endpoint",
                                         transport);
    Buffer buffer;
    auto result =
        registration.Register(buffer.binding(), "cpu:0", buffer.backing);
    ASSERT_EQ(result.error, DestinationError::kOk);
    EXPECT_NE(result.region.credential, WriteCredential{});
    EXPECT_EQ(transport->registrations, 1);
    EXPECT_EQ(transport->registered_location, "cpu:0");
    auto attempt =
        registration.Acquire({owner, 1, 1}, {{result.region, 10, 20}});
    ASSERT_EQ(attempt.error, DestinationError::kOk);
    std::vector<ResolvedDestination> views;
    ASSERT_EQ(attempt.attempt.Publish(views), DestinationError::kOk);
    ASSERT_EQ(views.size(), 1);
    EXPECT_EQ(views[0].transport_endpoint, "transport-endpoint");
    EXPECT_EQ(views[0].address, buffer.binding().logical_base + 10);
    EXPECT_EQ(
        registration.Complete(
            {{owner, 1, 1}, AttemptOutcome::kSucceeded, 20, true, requester}),
        DestinationError::kOk);
    EXPECT_EQ(registration.Revoke(result.region), DestinationError::kOk);
    EXPECT_EQ(transport->unregistered_base, buffer.binding().registered_base);
    EXPECT_EQ(registration.pinned_regions(), 0);
}

TEST(SsdDestinationRegistration,
     TimeoutAndWrongEpochDoNotUnregisterOrReleaseBacking) {
    auto transport = std::make_shared<RegistrationTransport>();
    DestinationRegistration registration(requester, "endpoint", transport);
    Buffer buffer;
    std::weak_ptr<void> lifetime = buffer.backing;
    auto result =
        registration.Register(buffer.binding(), "cpu:0", buffer.backing);
    ASSERT_EQ(result.error, DestinationError::kOk);
    buffer.backing.reset();
    auto attempt =
        registration.Acquire({owner, 1, 1}, {{result.region, 0, 20}});
    ASSERT_EQ(attempt.error, DestinationError::kOk);
    std::vector<ResolvedDestination> views;
    ASSERT_EQ(attempt.attempt.Publish(views), DestinationError::kOk);
    ASSERT_EQ(attempt.attempt.Timeout(), DestinationError::kOk);
    EXPECT_EQ(registration.Revoke(result.region), DestinationError::kBusy);
    EXPECT_EQ(transport->unregistrations, 0);
    EXPECT_FALSE(lifetime.expired());
    auto wrong = requester;
    ++wrong.boot_epoch;
    EXPECT_EQ(registration.Complete(
                  {{owner, 1, 1}, AttemptOutcome::kSucceeded, 20, true, wrong}),
              DestinationError::kWrongAttempt);
    EXPECT_EQ(
        registration.Acquire({owner, 2, 1}, {{result.region, 30, 20}}).error,
        DestinationError::kClosing);
    EXPECT_EQ(
        registration.Complete(
            {{owner, 1, 1}, AttemptOutcome::kSucceeded, 20, true, requester}),
        DestinationError::kOk);
    EXPECT_FALSE(attempt.attempt.snapshot().publishable);
    EXPECT_EQ(registration.Revoke(result.region), DestinationError::kOk);
    EXPECT_TRUE(lifetime.expired());
}

TEST(SsdDestinationRegistration,
     FailedUnregisterPinsBackingAndAllowsExactRetry) {
    auto transport = std::make_shared<RegistrationTransport>();
    DestinationRegistration registration(requester, "endpoint", transport);
    Buffer buffer;
    const auto binding = buffer.binding();
    std::weak_ptr<void> lifetime = buffer.backing;
    auto result = registration.Register(binding, "cpu:0", buffer.backing);
    ASSERT_EQ(result.error, DestinationError::kOk);
    buffer.backing.reset();
    transport->next_unregister = DestinationError::kNotSupported;
    EXPECT_EQ(registration.Revoke(result.region),
              DestinationError::kNotSupported);
    EXPECT_FALSE(lifetime.expired());
    EXPECT_EQ(registration.pinned_regions(), 1);
    auto wrong = result.region;
    ++wrong.generation;
    EXPECT_EQ(registration.Revoke(wrong), DestinationError::kStaleRegion);
    EXPECT_EQ(transport->unregistrations, 1);
    auto duplicate = registration.Register(binding, "cpu:0", lifetime.lock());
    EXPECT_EQ(duplicate.error, DestinationError::kOverlap);
    EXPECT_EQ(transport->registrations, 1);
    transport->next_unregister = DestinationError::kOk;
    EXPECT_EQ(registration.Revoke(result.region), DestinationError::kOk);
    EXPECT_TRUE(lifetime.expired());
    EXPECT_EQ(registration.pinned_regions(), 0);
}

TEST(SsdDestinationRegistration, InvalidOrOverlappingMrNeverReachesTransport) {
    auto transport = std::make_shared<RegistrationTransport>();
    DestinationRegistration registration(requester, "endpoint", transport);
    Buffer buffer;
    auto invalid = buffer.binding();
    invalid.logical_bytes = invalid.registered_bytes;
    EXPECT_NE(registration.Register(invalid, "cpu:0", buffer.backing).error,
              DestinationError::kOk);
    EXPECT_NE(registration.Register(buffer.binding(), "cpu:0", {}).error,
              DestinationError::kOk);
    EXPECT_NE(registration.Register(buffer.binding(), "", buffer.backing).error,
              DestinationError::kOk);
    EXPECT_EQ(transport->registrations, 0);
    ASSERT_EQ(
        registration.Register(buffer.binding(), "cpu:0", buffer.backing).error,
        DestinationError::kOk);
    auto overlapping = buffer.binding();
    overlapping.logical_base += 200;
    overlapping.logical_bytes = 100;
    EXPECT_EQ(registration.Register(overlapping, "cpu:0", buffer.backing).error,
              DestinationError::kOverlap);
    EXPECT_EQ(transport->registrations, 1);
}

TEST(SsdDestinationRegistration,
     PartialRegistrationFailureRemainsBoundedAndCloseRetriesCleanup) {
    auto transport = std::make_shared<RegistrationTransport>();
    DestinationRegistryLimits limits;
    limits.max_regions = 1;
    DestinationRegistration registration(requester, "endpoint", transport,
                                         limits);
    Buffer buffer, second;
    std::weak_ptr<void> lifetime = buffer.backing;
    transport->next_registration = {DestinationError::kNotSupported, true,
                                    true};
    auto result =
        registration.Register(buffer.binding(), "cpu:0", buffer.backing);
    EXPECT_EQ(result.error, DestinationError::kNotSupported);
    EXPECT_EQ(result.region, DestinationRegion{});
    buffer.backing.reset();
    EXPECT_FALSE(lifetime.expired());
    EXPECT_EQ(registration.pinned_regions(), 1);
    EXPECT_EQ(
        registration.Register(second.binding(), "cpu:0", second.backing).error,
        DestinationError::kLimit);
    transport->next_unregister = DestinationError::kNotSupported;
    EXPECT_EQ(registration.Close(), DestinationError::kNotSupported);
    EXPECT_FALSE(lifetime.expired());
    transport->next_unregister = DestinationError::kOk;
    EXPECT_EQ(registration.Close(), DestinationError::kOk);
    EXPECT_TRUE(lifetime.expired());
    EXPECT_EQ(
        registration.Register(second.binding(), "cpu:0", second.backing).error,
        DestinationError::kClosing);
}

TEST(SsdDestinationRegistration,
     DefinitiveRegistrationRejectionReleasesBacking) {
    auto transport = std::make_shared<RegistrationTransport>();
    DestinationRegistration registration(requester, "endpoint", transport);
    Buffer buffer;
    transport->next_registration = {DestinationError::kOverlap, false};
    EXPECT_EQ(
        registration.Register(buffer.binding(), "cpu:0", buffer.backing).error,
        DestinationError::kOverlap);
    EXPECT_EQ(registration.pinned_regions(), 0);
    EXPECT_EQ(transport->unregistrations, 0);
    EXPECT_EQ(buffer.backing.use_count(), 1);
}

TEST(SsdDestinationRegistration,
     CloseWaitsForDrainAndReleaseCallbackCanReenter) {
    auto transport = std::make_shared<RegistrationTransport>();
    DestinationRegistration registration(requester, "endpoint", transport);
    Buffer buffer;
    bool released = false;
    auto lifetime = std::shared_ptr<void>(
        buffer.backing.get(),
        [backing = buffer.backing, &registration, &released](void*) {
            EXPECT_EQ(registration.pinned_regions(), 0);
            released = true;
        });
    auto result = registration.Register(buffer.binding(), "cpu:0", lifetime);
    ASSERT_EQ(result.error, DestinationError::kOk);
    lifetime.reset();
    auto attempt =
        registration.Acquire({owner, 1, 1}, {{result.region, 0, 20}});
    ASSERT_EQ(attempt.error, DestinationError::kOk);
    std::vector<ResolvedDestination> views;
    ASSERT_EQ(attempt.attempt.Publish(views), DestinationError::kOk);
    EXPECT_EQ(registration.Close(), DestinationError::kBusy);
    EXPECT_FALSE(released);
    EXPECT_EQ(
        registration.Complete(
            {{owner, 1, 1}, AttemptOutcome::kCancelled, 0, true, requester}),
        DestinationError::kOk);
    EXPECT_EQ(registration.Close(), DestinationError::kOk);
    EXPECT_TRUE(released);
}

TEST(SsdDestinationRegistration, TransportCallbacksRunOutsideAdapterLock) {
    auto transport = std::make_shared<RegistrationTransport>();
    DestinationRegistration registration(requester, "endpoint", transport);
    Buffer buffer;
    std::thread observer;
    auto inspect_during_callback = [&] {
        std::promise<size_t> result;
        auto done = result.get_future();
        observer =
            std::thread([&registration, result = std::move(result)]() mutable {
                result.set_value(registration.pinned_regions());
            });
        // The old lock boundary produces a behavior failure rather than hanging
        // the test forever. Join only after the transport callback returns.
        EXPECT_EQ(done.wait_for(std::chrono::seconds(1)),
                  std::future_status::ready);
    };
    transport->on_register = inspect_during_callback;
    auto region =
        registration.Register(buffer.binding(), "cpu:0", buffer.backing);
    observer.join();
    ASSERT_EQ(region.error, DestinationError::kOk);
    transport->on_unregister = inspect_during_callback;
    EXPECT_EQ(registration.Revoke(region.region), DestinationError::kOk);
    observer.join();
}

TEST(SsdDestinationRegistration, CloseDuringTransportCallbackPinsUntilRetry) {
    auto transport = std::make_shared<RegistrationTransport>();
    DestinationRegistration registration(requester, "endpoint", transport);
    Buffer buffer;
    std::weak_ptr<void> lifetime = buffer.backing;
    transport->on_register = [&] {
        EXPECT_EQ(registration.Close(), DestinationError::kBusy);
    };
    auto result =
        registration.Register(buffer.binding(), "cpu:0", buffer.backing);
    EXPECT_EQ(result.error, DestinationError::kClosing);
    EXPECT_EQ(result.region, DestinationRegion{});
    buffer.backing.reset();
    EXPECT_FALSE(lifetime.expired());
    transport->on_unregister = [&] {
        EXPECT_EQ(registration.Close(), DestinationError::kBusy);
    };
    EXPECT_EQ(registration.Close(), DestinationError::kOk);
    EXPECT_TRUE(lifetime.expired());
}

TEST(SsdDestinationRegistration, ReuseRotatesCredentialAndFencesReplay) {
    auto transport = std::make_shared<RegistrationTransport>();
    DestinationRegistryLimits limits;
    limits.max_regions = 1;
    DestinationRegistration registration(requester, "endpoint", transport,
                                         limits);
    Buffer buffer;
    auto first =
        registration.Register(buffer.binding(), "cpu:0", buffer.backing);
    ASSERT_EQ(first.error, DestinationError::kOk);
    ASSERT_EQ(registration.Revoke(first.region), DestinationError::kOk);
    auto second =
        registration.Register(buffer.binding(), "cpu:0", buffer.backing);
    ASSERT_EQ(second.error, DestinationError::kOk);
    EXPECT_EQ(first.region.region_id, second.region.region_id);
    EXPECT_LT(first.region.generation, second.region.generation);
    EXPECT_NE(first.region.credential, second.region.credential);
    EXPECT_EQ(
        registration.Acquire({owner, 1, 1}, {{first.region, 0, 20}}).error,
        DestinationError::kStaleRegion);
    auto attempt =
        registration.Acquire({owner, 1, 1}, {{second.region, 0, 20}});
    ASSERT_EQ(attempt.error, DestinationError::kOk);
    EXPECT_EQ(
        registration.Complete(
            {{owner, 1, 1}, AttemptOutcome::kRejected, 0, true, requester}),
        DestinationError::kOk);
    EXPECT_EQ(registration.ForgetRequest(owner, 1), DestinationError::kOk);
    EXPECT_EQ(registration.Inspect({owner, 1, 1}).error,
              DestinationError::kNotFound);
    EXPECT_EQ(
        registration.Acquire({owner, 1, 2}, {{second.region, 0, 20}}).error,
        DestinationError::kStaleRequest);
}

bool ProbeUnownedFailure(bool success = false) {
    auto transport = std::make_shared<RegistrationTransport>();
    transport->next_registration = {
        success ? DestinationError::kOk : DestinationError::kTransportError,
        true, false};
    Buffer buffer;
    std::weak_ptr<void> lifetime = buffer.backing;
    bool safe = true;
    {
        DestinationRegistryLimits limits;
        limits.max_regions = 1;
        DestinationRegistration registration(requester, "endpoint", transport,
                                             limits);
        auto result =
            registration.Register(buffer.binding(), "cpu:0", buffer.backing);
        buffer.backing.reset();
        safe = result.error == DestinationError::kTransportError &&
               result.region == DestinationRegion{} &&
               registration.Close() == DestinationError::kNotDrained &&
               registration.pinned_regions() == 1 && !lifetime.expired() &&
               transport->unregistrations == 0 &&
               registration.Close() == DestinationError::kNotDrained;
    }
    return safe && transport->unregistrations == 0 && !lifetime.expired();
}

TEST(SsdDestinationRegistration, UnknownOwnershipNeverUnregistersAForeignMr) {
    // The subprocess checks intentional permanent quarantine without leaking
    // an unreachable allocation into the sanitizer test process.
    ASSERT_EXIT(_exit(ProbeUnownedFailure() ? 0 : 1),
                ::testing::ExitedWithCode(0), "");
}

TEST(SsdDestinationRegistration, UnownedSuccessCannotIssueAWritableHandle) {
    ASSERT_EXIT(_exit(ProbeUnownedFailure(true) ? 0 : 1),
                ::testing::ExitedWithCode(0), "");
}
}  // namespace
}  // namespace mooncake::ssd_offload
