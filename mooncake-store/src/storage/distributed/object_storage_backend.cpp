#include "storage/distributed/object_storage_backend.h"

#include <algorithm>
#include <limits>
#include <new>
#include <optional>
#include <stdexcept>
#include <utility>

namespace mooncake {
namespace {

constexpr uint64_t kMaxReadChunkBytes = 4ULL * 1024 * 1024;

class StagingReservation {
   public:
    static std::optional<StagingReservation> TryAcquire(
        std::atomic<uint64_t>& bytes, uint64_t limit, uint64_t size) {
        auto current = bytes.load();
        do {
            if (size > limit - current) {
                return std::nullopt;
            }
        } while (!bytes.compare_exchange_weak(current, current + size));
        return StagingReservation(bytes, size);
    }

    StagingReservation(const StagingReservation&) = delete;
    StagingReservation& operator=(const StagingReservation&) = delete;
    StagingReservation(StagingReservation&& other) noexcept
        : bytes_(std::exchange(other.bytes_, nullptr)), size_(other.size_) {}
    StagingReservation& operator=(StagingReservation&&) = delete;

    ~StagingReservation() {
        if (bytes_ != nullptr) {
            bytes_->fetch_sub(size_);
        }
    }

   private:
    StagingReservation(std::atomic<uint64_t>& bytes, uint64_t size)
        : bytes_(&bytes), size_(size) {}

    std::atomic<uint64_t>* bytes_;
    uint64_t size_;
};

tl::expected<uint64_t, ErrorCode> SliceBytes(const std::vector<Slice>& slices,
                                             uint64_t available) {
    uint64_t total_bytes = 0;
    for (const auto& slice : slices) {
        if (slice.size != 0 && slice.ptr == nullptr) {
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
        if (slice.size > available - total_bytes) {
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
        total_bytes += slice.size;
    }
    return total_bytes;
}

ErrorCode ValidateWriteSlices(uint64_t object_size, uint64_t max_object_bytes,
                              const std::vector<Slice>& slices) {
    if (object_size > max_object_bytes ||
        object_size > std::numeric_limits<size_t>::max()) {
        return ErrorCode::INVALID_PARAMS;
    }
    const auto total_bytes = SliceBytes(slices, object_size);
    if (!total_bytes) {
        return total_bytes.error();
    }
    return *total_bytes == object_size ? ErrorCode::OK
                                       : ErrorCode::INVALID_PARAMS;
}

}  // namespace

ObjectStorageBackend::ObjectStorageBackend(
    ObjectStorageBackendConfig config,
    std::shared_ptr<ObjectStorageAdapter> adapter)
    : config_(config), adapter_(std::move(adapter)) {
    if (!adapter_ || config_.max_object_bytes == 0 ||
        config_.max_staging_bytes == 0) {
        throw std::invalid_argument(
            "Invalid object storage backend configuration");
    }
}

ErrorCode ObjectStorageBackend::Write(const std::string& object_key,
                                      uint64_t object_size,
                                      const std::vector<Slice>& slices,
                                      const Copy& to_host,
                                      bool* upload_started) {
    if (upload_started != nullptr) {
        *upload_started = false;
    }
    const auto validation =
        ValidateWriteSlices(object_size, config_.max_object_bytes, slices);
    if (validation != ErrorCode::OK) {
        return validation;
    }
    if (object_size != 0 && !to_host) {
        return ErrorCode::INVALID_PARAMS;
    }
    if (!adapter_->SupportsSinglePutAttempt()) {
        return ErrorCode::NOT_SUPPORTED;
    }

    auto reservation = StagingReservation::TryAcquire(
        staging_bytes_, config_.max_staging_bytes, object_size);
    if (!reservation) {
        return ErrorCode::NO_AVAILABLE_HANDLE;
    }
    std::unique_ptr<char[]> staging(new (std::nothrow) char[object_size]);
    if (!staging) {
        return ErrorCode::NO_AVAILABLE_HANDLE;
    }

    size_t offset = 0;
    for (const auto& slice : slices) {
        if (slice.size != 0 &&
            !to_host(staging.get() + offset, slice.ptr, slice.size)) {
            return ErrorCode::TRANSFER_FAIL;
        }
        offset += slice.size;
    }

    if (upload_started != nullptr) {
        *upload_started = true;
    }
    const auto result = adapter_->Put(object_key, {staging.get(), object_size});
    return result ? ErrorCode::OK : result.error();
}

ErrorCode ObjectStorageBackend::WriteV(const std::string& object_key,
                                       uint64_t object_size,
                                       const std::vector<Slice>& slices,
                                       const IsHostMemory& is_host,
                                       const Copy& to_host,
                                       bool* upload_started) {
    if (upload_started != nullptr) {
        *upload_started = false;
    }
    const auto validation =
        ValidateWriteSlices(object_size, config_.max_object_bytes, slices);
    if (validation != ErrorCode::OK) {
        return validation;
    }
    if (object_size != 0 && !is_host) {
        return ErrorCode::INVALID_PARAMS;
    }
    if (slices.size() > static_cast<size_t>(std::numeric_limits<int>::max())) {
        return ErrorCode::INVALID_PARAMS;
    }
    if (!adapter_->SupportsSinglePutVAttempt()) {
        return ErrorCode::NOT_SUPPORTED;
    }

    std::vector<iovec> iov;
    iov.reserve(slices.size());
    uint64_t staging_bytes = 0;
    for (const auto& slice : slices) {
        const bool borrow = slice.size != 0 && is_host(slice);
        iov.push_back({borrow ? slice.ptr : nullptr, slice.size});
        if (!borrow) {
            staging_bytes += slice.size;
        }
    }
    if (staging_bytes != 0 && !to_host) {
        return ErrorCode::INVALID_PARAMS;
    }
    auto reservation =
        staging_bytes != 0
            ? StagingReservation::TryAcquire(
                  staging_bytes_, config_.max_staging_bytes, staging_bytes)
            : std::nullopt;
    if (staging_bytes != 0 && !reservation) {
        return ErrorCode::NO_AVAILABLE_HANDLE;
    }
    std::unique_ptr<char[]> staging;
    if (staging_bytes != 0) {
        staging.reset(new (std::nothrow) char[staging_bytes]);
        if (!staging) {
            return ErrorCode::NO_AVAILABLE_HANDLE;
        }
    }
    size_t offset = 0;
    for (size_t i = 0; i < slices.size(); ++i) {
        if (iov[i].iov_len != 0 && iov[i].iov_base == nullptr) {
            if (!to_host(staging.get() + offset, slices[i].ptr,
                         slices[i].size)) {
                return ErrorCode::TRANSFER_FAIL;
            }
            iov[i].iov_base = staging.get() + offset;
            offset += slices[i].size;
        }
    }
    if (upload_started != nullptr) {
        *upload_started = true;
    }
    const auto result =
        adapter_->PutV(object_key, iov.data(), static_cast<int>(iov.size()));
    return result ? ErrorCode::OK : result.error();
}

ErrorCode ObjectStorageBackend::Read(const std::string& object_key,
                                     uint64_t object_size,
                                     const std::vector<Slice>& slices,
                                     uint64_t offset, const Copy& from_host) {
    if (object_size > config_.max_object_bytes || offset > object_size) {
        return ErrorCode::INVALID_PARAMS;
    }
    const auto total_bytes = SliceBytes(slices, object_size - offset);
    if (!total_bytes) {
        return total_bytes.error();
    }
    if (*total_bytes != 0 && !from_host) {
        return ErrorCode::INVALID_PARAMS;
    }
    if (*total_bytes == 0) {
        return ErrorCode::OK;
    }
    const auto max_offset =
        static_cast<uint64_t>(std::numeric_limits<off_t>::max());
    if (offset > max_offset || *total_bytes - 1 > max_offset - offset) {
        return ErrorCode::INVALID_PARAMS;
    }

    const auto chunk =
        std::min({*total_bytes, kMaxReadChunkBytes, config_.max_staging_bytes});
    auto reservation = StagingReservation::TryAcquire(
        staging_bytes_, config_.max_staging_bytes, chunk);
    if (!reservation) {
        return ErrorCode::NO_AVAILABLE_HANDLE;
    }
    std::unique_ptr<char[]> staging(new (std::nothrow) char[chunk]);
    if (!staging) {
        return ErrorCode::NO_AVAILABLE_HANDLE;
    }

    size_t slice_index = 0;
    size_t slice_offset = 0;
    for (uint64_t remaining = *total_bytes; remaining != 0;) {
        const auto length = std::min(chunk, remaining);
        const auto result = adapter_->GetRange(
            object_key, staging.get(), length, static_cast<off_t>(offset));
        if (!result) {
            return result.error();
        }
        if (*result != length) {
            return ErrorCode::FILE_READ_FAIL;
        }

        for (size_t done = 0; done < length;) {
            while (slice_offset == slices[slice_index].size) {
                ++slice_index;
                slice_offset = 0;
            }
            const auto count = std::min<uint64_t>(
                length - done, slices[slice_index].size - slice_offset);
            if (!from_host(
                    static_cast<char*>(slices[slice_index].ptr) + slice_offset,
                    staging.get() + done, count)) {
                return ErrorCode::TRANSFER_FAIL;
            }
            done += count;
            slice_offset += count;
        }
        remaining -= length;
        offset += length;
    }
    return ErrorCode::OK;
}

}  // namespace mooncake
