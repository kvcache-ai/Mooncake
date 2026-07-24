#include "storage/distributed/object_storage_adapter.h"

#include <cstring>
#include <limits>

namespace mooncake {

namespace {

tl::expected<size_t, ErrorCode> ValidateAndGetTotalSize(
    std::span<const TensorRegion> regions) {
    size_t total = 0;
    for (const auto& region : regions) {
        if (region.size > 0 && region.data == nullptr) {
            return tl::make_unexpected(ErrorCode::FILE_INVALID_BUFFER);
        }
        if (region.size > std::numeric_limits<size_t>::max() - total) {
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
        total += region.size;
    }
    return total;
}

}  // namespace

tl::expected<void, ErrorCode> ObjectStorageAdapter::PutTensor(
    const std::string& key, std::span<const TensorRegion> regions) {
    auto total = ValidateAndGetTotalSize(regions);
    if (!total) return tl::make_unexpected(total.error());

    std::string buffer;
    buffer.reserve(*total);
    for (const auto& region : regions) {
        if (region.size == 0) continue;
        buffer.append(static_cast<const char*>(region.data), region.size);
    }
    return Put(key, std::span<const char>(buffer.data(), buffer.size()));
}

tl::expected<void, ErrorCode> ObjectStorageAdapter::GetTensor(
    const std::string& key, std::span<TensorRegion> regions) {
    auto total = ValidateAndGetTotalSize(regions);
    if (!total) return tl::make_unexpected(total.error());

    std::string buffer(*total, '\0');
    auto result = Get(key, buffer.data(), buffer.size());
    if (!result) return tl::make_unexpected(result.error());
    if (*result != *total) {
        return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
    }

    size_t offset = 0;
    for (auto& region : regions) {
        if (region.size == 0) continue;
        std::memcpy(region.data, buffer.data() + offset, region.size);
        offset += region.size;
    }
    return {};
}

}  // namespace mooncake
