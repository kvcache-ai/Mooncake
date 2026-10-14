#include <gtest/gtest.h>

#include <cstddef>

#include "ibgda_cuda_error_handling.h"

namespace mooncake::device {
namespace {

TEST(IbgdaCudaErrorTest, SuccessfulOperationsDoNotCleanUp) {
    constexpr int kSuccess = 0;
    int operation_count = 0;
    int cleanup_count = 0;
    std::size_t failed_operation = 99;

    const int status = runCudaOperationsOrCleanup(
        kSuccess, [&] { ++cleanup_count; }, &failed_operation,
        [&] {
            ++operation_count;
            return kSuccess;
        },
        [&] {
            ++operation_count;
            return kSuccess;
        });

    EXPECT_EQ(status, kSuccess);
    EXPECT_EQ(failed_operation, 99);
    EXPECT_EQ(operation_count, 2);
    EXPECT_EQ(cleanup_count, 0);
}

TEST(IbgdaCudaErrorTest, QueuePairSetupStopsOnStreamFailure) {
    constexpr int kSuccess = 0;
    constexpr int kFailure = 3;
    int copy_count = 0;
    int cleanup_count = 0;
    std::size_t failed_operation = 99;

    const int status = initializeQueuePairDeviceContext(
        kSuccess, [&] { ++cleanup_count; }, &failed_operation,
        [] { return kFailure; }, [&] {
            ++copy_count;
            return kSuccess;
        });

    EXPECT_EQ(status, kFailure);
    EXPECT_EQ(failed_operation, 0);
    EXPECT_EQ(copy_count, 0);
    EXPECT_EQ(cleanup_count, 1);
}

TEST(IbgdaCudaErrorTest, QueuePairSetupCleansUpOnContextCopyFailure) {
    constexpr int kSuccess = 0;
    constexpr int kFailure = 5;
    int cleanup_count = 0;
    std::size_t failed_operation = 99;

    const int status = initializeQueuePairDeviceContext(
        kSuccess, [&] { ++cleanup_count; }, &failed_operation,
        [] { return kSuccess; }, [] { return kFailure; });

    EXPECT_EQ(status, kFailure);
    EXPECT_EQ(failed_operation, 1);
    EXPECT_EQ(cleanup_count, 1);
}

TEST(IbgdaCudaErrorTest, PeerTableSetupStopsOnAddressCopyFailure) {
    constexpr int kSuccess = 0;
    constexpr int kFailure = 7;
    int key_copy_count = 0;
    int cleanup_count = 0;
    std::size_t failed_operation = 99;

    const int status = initializePeerAddressTable(
        kSuccess, [&] { ++cleanup_count; }, &failed_operation,
        [] { return kFailure; }, [&] {
            ++key_copy_count;
            return kSuccess;
        });

    EXPECT_EQ(status, kFailure);
    EXPECT_EQ(failed_operation, 0);
    EXPECT_EQ(key_copy_count, 0);
    EXPECT_EQ(cleanup_count, 1);
}

TEST(IbgdaCudaErrorTest, PeerTableSetupCleansUpOnKeyCopyFailure) {
    constexpr int kSuccess = 0;
    constexpr int kFailure = 9;
    int cleanup_count = 0;
    std::size_t failed_operation = 99;

    const int status = initializePeerAddressTable(
        kSuccess, [&] { ++cleanup_count; }, &failed_operation,
        [] { return kSuccess; }, [] { return kFailure; });

    EXPECT_EQ(status, kFailure);
    EXPECT_EQ(failed_operation, 1);
    EXPECT_EQ(cleanup_count, 1);
}

}  // namespace
}  // namespace mooncake::device
