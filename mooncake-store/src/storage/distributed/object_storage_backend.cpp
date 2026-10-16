#include "storage/distributed/object_storage_backend.h"

#include <algorithm>
#include <limits>
#include <new>
#include <stdexcept>
#include <utility>

namespace mooncake {
namespace {

struct StagingReservation {
    std::atomic<uint64_t>& bytes;
    uint64_t size;
    ~StagingReservation() { bytes.fetch_sub(size); }
};

tl::expected<uint64_t, ErrorCode> SliceBytes(const std::vector<Slice>& slices,
                                             uint64_t available) {
    uint64_t size = 0;
    for (const auto& slice : slices) {
        if ((slice.size && !slice.ptr) || slice.size > available - size)
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        size += slice.size;
    }
    return size;
}

}  // namespace

ObjectStorageBackend::ObjectStorageBackend(
    ObjectStorageBackendConfig config,
    std::shared_ptr<ObjectStorageAdapter> adapter)
    : config_(config), adapter_(std::move(adapter)) {
    if (!adapter_ || !config_.max_object_bytes || !config_.max_staging_bytes)
        throw std::invalid_argument(
            "Invalid object storage backend configuration");
}

bool ObjectStorageBackend::Reserve(uint64_t bytes) {
    auto current = staging_bytes_.load();
    do {
        if (bytes > config_.max_staging_bytes - current) return false;
    } while (!staging_bytes_.compare_exchange_weak(current, current + bytes));
    return true;
}

ErrorCode ObjectStorageBackend::Write(const std::string& object_key,
                                      uint64_t object_size,
                                      const std::vector<Slice>& slices,
                                      const Copy& copy, bool* upload_started) {
    if (upload_started) *upload_started = false;
    if (object_size > config_.max_object_bytes ||
        object_size > std::numeric_limits<size_t>::max())
        return ErrorCode::INVALID_PARAMS;
    const auto size = SliceBytes(slices, object_size);
    if (!size || *size != object_size || (object_size && !copy))
        return ErrorCode::INVALID_PARAMS;
    if (!adapter_->SupportsSinglePutAttempt()) return ErrorCode::NOT_SUPPORTED;
    if (!Reserve(object_size)) return ErrorCode::NO_AVAILABLE_HANDLE;
    StagingReservation reservation{staging_bytes_, object_size};
    std::unique_ptr<char[]> staging(new (std::nothrow) char[object_size]);
    if (!staging) return ErrorCode::NO_AVAILABLE_HANDLE;
    size_t offset = 0;
    for (const auto& slice : slices) {
        if (slice.size && !copy(staging.get() + offset, slice.ptr, slice.size))
            return ErrorCode::TRANSFER_FAIL;
        offset += slice.size;
    }
    if (upload_started) *upload_started = true;
    const auto result = adapter_->Put(object_key, {staging.get(), *size});
    return result ? ErrorCode::OK : result.error();
}

ErrorCode ObjectStorageBackend::Read(const std::string& object_key,
                                     uint64_t object_size,
                                     const std::vector<Slice>& slices,
                                     uint64_t offset, const Copy& copy) {
    if (object_size > config_.max_object_bytes || offset > object_size)
        return ErrorCode::INVALID_PARAMS;
    const auto size = SliceBytes(slices, object_size - offset);
    if (!size || (*size && !copy)) return ErrorCode::INVALID_PARAMS;
    if (!*size) return ErrorCode::OK;
    const auto max_offset =
        static_cast<uint64_t>(std::numeric_limits<off_t>::max());
    if (offset > max_offset || *size - 1 > max_offset - offset)
        return ErrorCode::INVALID_PARAMS;
    const auto chunk =
        std::min({*size, uint64_t{4 * 1024 * 1024}, config_.max_staging_bytes});
    if (!Reserve(chunk)) return ErrorCode::NO_AVAILABLE_HANDLE;
    StagingReservation reservation{staging_bytes_, chunk};
    std::unique_ptr<char[]> staging(new (std::nothrow) char[chunk]);
    if (!staging) return ErrorCode::NO_AVAILABLE_HANDLE;
    for (const auto& slice : slices) {
        for (size_t done = 0; done < slice.size;) {
            const auto length = std::min<uint64_t>(chunk, slice.size - done);
            const auto result = adapter_->GetRange(
                object_key, staging.get(), length, static_cast<off_t>(offset));
            if (!result) return result.error();
            if (*result != length) return ErrorCode::FILE_READ_FAIL;
            if (!copy(static_cast<char*>(slice.ptr) + done, staging.get(),
                      length))
                return ErrorCode::TRANSFER_FAIL;
            done += length;
            offset += length;
        }
    }
    return ErrorCode::OK;
}

}  // namespace mooncake
