#pragma once

#include <cstddef>
#include <cstdint>
#include <span>

namespace mooncake::logstructured {

struct UringWriteRequest {
    const char* data = nullptr;
    size_t length = 0;
    uint64_t offset = 0;
};

enum class UringBatchWriteResult {
    kSuccess,
    kUnavailable,
    kIoError,
};

UringBatchWriteResult UringBatchWrite(
    int fd, std::span<const UringWriteRequest> requests, size_t max_in_flight);

}  // namespace mooncake::logstructured
