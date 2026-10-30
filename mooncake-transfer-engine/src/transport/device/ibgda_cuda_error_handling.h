#pragma once

#include <cstddef>
#include <utility>

namespace mooncake::device {

template <typename Status, typename Cleanup, typename... Operations>
Status runCudaOperationsOrCleanup(Status success, Cleanup&& cleanup,
                                  std::size_t* failed_operation,
                                  Operations&&... operations) {
    Status result = success;
    std::size_t operation_index = 0;
    auto run = [&](auto&& operation) {
        if (result == success) {
            result = std::forward<decltype(operation)>(operation)();
            if (result != success && failed_operation) {
                *failed_operation = operation_index;
            }
        }
        ++operation_index;
    };
    (run(std::forward<Operations>(operations)), ...);
    if (result != success) {
        std::forward<Cleanup>(cleanup)();
    }
    return result;
}

template <typename Status, typename Cleanup, typename Synchronize,
          typename CopyContext>
Status initializeQueuePairDeviceContext(Status success, Cleanup&& cleanup,
                                        std::size_t* failed_operation,
                                        Synchronize&& synchronize,
                                        CopyContext&& copy_context) {
    return runCudaOperationsOrCleanup(success, std::forward<Cleanup>(cleanup),
                                      failed_operation,
                                      std::forward<Synchronize>(synchronize),
                                      std::forward<CopyContext>(copy_context));
}

template <typename Status, typename Cleanup, typename CopyAddress,
          typename CopyKey>
Status initializePeerAddressTable(Status success, Cleanup&& cleanup,
                                  std::size_t* failed_operation,
                                  CopyAddress&& copy_address,
                                  CopyKey&& copy_key) {
    return runCudaOperationsOrCleanup(success, std::forward<Cleanup>(cleanup),
                                      failed_operation,
                                      std::forward<CopyAddress>(copy_address),
                                      std::forward<CopyKey>(copy_key));
}

}  // namespace mooncake::device
