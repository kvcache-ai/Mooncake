// Copyright 2026 Mooncake Authors

#include <gtest/gtest.h>

#include "transport/local_buffer_registrations.h"

namespace mooncake {
namespace {
void* address(size_t value) { return reinterpret_cast<void*>(value); }

TEST(LocalBufferRegistrationsTest, SkippedRegistrationDoesNotRemoveAnything) {
    LocalBufferRegistrations registrations;
    int calls = 0;
    EXPECT_EQ(registrations.remove(
                  address(1),
                  [&](void*) {
                      ++calls;
                      return 0;
                  },
                  true,
                  [&] {
                      ++calls;
                      return 0;
                  }),
              0);
    EXPECT_EQ(calls, 0);
}

TEST(LocalBufferRegistrationsTest, RejectsDuplicateLogicalRegistration) {
    LocalBufferRegistrations registrations;
    int adds = 0;
    auto add = [&] {
        ++adds;
        return 0;
    };
    auto publish = [] { return 0; };
    ASSERT_EQ(
        registrations.add(address(1), address(1), 16, add, false, publish), 0);
    EXPECT_EQ(
        registrations.add(address(1), address(1), 16, add, false, publish),
        ERR_ADDRESS_OVERLAPPED);
    EXPECT_EQ(adds, 1);
}

TEST(LocalBufferRegistrationsTest, SharedAllocationLivesUntilLastAddress) {
    for (bool reverse : {false, true}) {
        LocalBufferRegistrations registrations;
        int adds = 0, removes = 0;
        auto add = [&] {
            ++adds;
            return 0;
        };
        auto publish = [] { return 0; };
        auto remove = [&](void* base) {
            EXPECT_EQ(base, address(16));
            ++removes;
            return 0;
        };
        ASSERT_EQ(registrations.add(address(17), address(16), 16, add, false,
                                    publish),
                  0);
        ASSERT_EQ(registrations.add(address(18), address(16), 16, add, false,
                                    publish),
                  0);
        EXPECT_EQ(adds, 1);
        ASSERT_EQ(registrations.remove(address(reverse ? 18 : 17), remove,
                                       false, publish),
                  0);
        EXPECT_EQ(removes, 0);
        ASSERT_EQ(registrations.remove(address(reverse ? 17 : 18), remove,
                                       false, publish),
                  0);
        EXPECT_EQ(removes, 1);
        EXPECT_FALSE(registrations.containsBase(address(16)));
        EXPECT_EQ(registrations.remove(address(17), remove, false, publish), 0);
        EXPECT_EQ(removes, 1);
    }
}

TEST(LocalBufferRegistrationsTest, RemovalFailureRetainsPublishedBase) {
    for (int error : {ERR_INVALID_ARGUMENT, ERR_ADDRESS_NOT_REGISTERED}) {
        LocalBufferRegistrations registrations;
        auto publish = [] { return 0; };
        ASSERT_EQ(
            registrations.add(
                address(17), address(16), 16, [] { return 0; }, false, publish),
            0);
        int result = error, removes = 0;
        auto remove = [&](void* base) {
            EXPECT_EQ(base, address(16));
            ++removes;
            return result;
        };
        EXPECT_EQ(registrations.remove(address(17), remove, true, publish),
                  error);
        EXPECT_EQ(registrations.baseFor(address(17)), address(16));
        result = 0;
        EXPECT_EQ(registrations.remove(address(17), remove, true, publish), 0);
        EXPECT_EQ(removes, 2);
        EXPECT_FALSE(registrations.containsBase(address(16)));
    }
}

TEST(LocalBufferRegistrationsTest, PublicationRetryDoesNotRemoveTwice) {
    LocalBufferRegistrations registrations;
    auto ok = [] { return 0; };
    ASSERT_EQ(registrations.add(address(17), address(16), 16, ok, false, ok),
              0);
    int removes = 0, result = ERR_METADATA;
    auto remove = [&](void*) {
        ++removes;
        return 0;
    };
    auto publish = [&] { return result; };
    EXPECT_EQ(registrations.remove(address(17), remove, true, publish),
              ERR_METADATA);
    EXPECT_EQ(registrations.add(address(18), address(16), 16, ok, false, ok),
              ERR_INVALID_ARGUMENT);
    result = 0;
    EXPECT_EQ(registrations.remove(address(17), remove, true, publish), 0);
    EXPECT_EQ(removes, 1);
    EXPECT_EQ(registrations.add(address(17), address(16), 16, ok, false, ok),
              0);
}

TEST(LocalBufferRegistrationsTest, FailedAddPublicationCanBeRolledBack) {
    LocalBufferRegistrations registrations;
    EXPECT_EQ(registrations.add(
                  address(17), address(16), 16, [] { return 0; }, true,
                  [] { return ERR_METADATA; }),
              ERR_METADATA);
    int removes = 0;
    EXPECT_EQ(registrations.remove(
                  address(17),
                  [&](void* base) {
                      EXPECT_EQ(base, address(16));
                      ++removes;
                      return 0;
                  },
                  false, [] { return 0; }),
              0);
    EXPECT_EQ(removes, 1);
}

TEST(LocalBufferRegistrationsTest, FailedLocalAddDoesNotCreateOwnership) {
    LocalBufferRegistrations registrations;
    EXPECT_EQ(registrations.add(
                  address(17), address(16), 16, [] { return ERR_METADATA; },
                  false, [] { return 0; }),
              ERR_METADATA);
    int removes = 0;
    EXPECT_EQ(registrations.remove(
                  address(17),
                  [&](void*) {
                      ++removes;
                      return 0;
                  },
                  true, [] { return 0; }),
              0);
    EXPECT_EQ(removes, 0);
}

TEST(LocalBufferRegistrationsTest, RejectsConflictingAllocationExtent) {
    LocalBufferRegistrations registrations;
    auto ok = [] { return 0; };
    ASSERT_EQ(registrations.add(address(17), address(16), 16, ok, false, ok),
              0);
    EXPECT_EQ(registrations.add(address(18), address(16), 32, ok, false, ok),
              ERR_INVALID_ARGUMENT);
    EXPECT_EQ(registrations.baseFor(address(18)), nullptr);
}
}  // namespace
}  // namespace mooncake
