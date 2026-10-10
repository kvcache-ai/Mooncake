#include "storage/distributed/object_storage_adapter.h"

#include <algorithm>
#include <cstring>
#include <limits>

namespace mooncake {

ObjectStorageIoResults ObjectStorageAdapter::BatchPutV(
    std::span<const ObjectStoragePutRequest> requests) {
    ObjectStorageIoResults results;
    results.reserve(requests.size());
    for (const auto& request : requests) {
        if (request.slices.empty() ||
            request.slices.size() >
                static_cast<size_t>(std::numeric_limits<int>::max())) {
            results.emplace_back(
                tl::make_unexpected(ErrorCode::INVALID_PARAMS));
            continue;
        }
        std::vector<iovec> iovs;
        iovs.reserve(request.slices.size());
        bool invalid = false;
        for (const auto& slice : request.slices) {
            if (slice.ptr == nullptr && slice.size != 0) {
                invalid = true;
                break;
            }
            iovs.push_back({slice.ptr, slice.size});
        }
        if (invalid) {
            results.emplace_back(
                tl::make_unexpected(ErrorCode::INVALID_PARAMS));
            continue;
        }
        results.emplace_back(PutV(request.logical_key, iovs.data(),
                                  static_cast<int>(iovs.size())));
    }
    return results;
}

ObjectStorageIoResults ObjectStorageAdapter::BatchGetInto(
    std::span<const ObjectStorageGetRequest> requests) {
    ObjectStorageIoResults results;
    results.reserve(requests.size());
    for (const auto& request : requests) {
        if (request.expected_size == 0 || request.slices.empty()) {
            results.emplace_back(
                tl::make_unexpected(ErrorCode::INVALID_PARAMS));
            continue;
        }

        size_t capacity = 0;
        bool invalid = false;
        for (const auto& slice : request.slices) {
            if ((slice.ptr == nullptr && slice.size != 0) ||
                slice.size > std::numeric_limits<size_t>::max() - capacity) {
                invalid = true;
                break;
            }
            capacity += slice.size;
        }
        if (invalid || capacity < request.expected_size) {
            results.emplace_back(
                tl::make_unexpected(ErrorCode::INVALID_PARAMS));
            continue;
        }

        if (request.slices.size() == 1) {
            auto read = Get(request.logical_key, request.slices[0].ptr,
                            request.expected_size);
            if (!read) {
                results.emplace_back(tl::make_unexpected(read.error()));
            } else if (*read != request.expected_size) {
                results.emplace_back(
                    tl::make_unexpected(ErrorCode::FILE_READ_FAIL));
            } else {
                results.emplace_back();
            }
            continue;
        }

        std::vector<char> buffer(request.expected_size);
        auto read = Get(request.logical_key, buffer.data(), buffer.size());
        if (!read) {
            results.emplace_back(tl::make_unexpected(read.error()));
            continue;
        }
        if (*read != request.expected_size) {
            results.emplace_back(
                tl::make_unexpected(ErrorCode::FILE_READ_FAIL));
            continue;
        }
        size_t offset = 0;
        for (const auto& slice : request.slices) {
            if (offset == buffer.size()) break;
            const size_t length = std::min(slice.size, buffer.size() - offset);
            if (length != 0) {
                std::memcpy(slice.ptr, buffer.data() + offset, length);
                offset += length;
            }
        }
        results.emplace_back();
    }
    return results;
}

ObjectStorageIoResults ObjectStorageAdapter::BatchDelete(
    std::span<const std::string> logical_keys) {
    ObjectStorageIoResults results;
    results.reserve(logical_keys.size());
    for (const auto& key : logical_keys) results.emplace_back(Delete(key));
    return results;
}

ObjectStorageQueryResults ObjectStorageAdapter::BatchQueryProvider(
    std::span<const std::string> logical_keys) {
    ObjectStorageQueryResults results;
    results.reserve(logical_keys.size());
    for (size_t i = 0; i < logical_keys.size(); ++i) {
        results.emplace_back(tl::make_unexpected(ErrorCode::NOT_SUPPORTED));
    }
    return results;
}

}  // namespace mooncake
