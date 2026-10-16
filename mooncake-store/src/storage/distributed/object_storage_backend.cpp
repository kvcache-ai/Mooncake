#include "storage/distributed/object_storage_backend.h"

#include <algorithm>
#include <limits>
#include <new>
#ifdef HAVE_OSS_ADAPTER
#include "storage/distributed/oss_adapter.h"
#endif

namespace mooncake {

tl::expected<std::shared_ptr<ObjectStorageAdapter>, ErrorCode>
CreateObjectStorageAdapter(const ObjectStorageConfig& config) {
    if (!config.enabled || !config.Validate())
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
#ifdef HAVE_OSS_ADAPTER
    auto adapter = std::make_shared<OssObjectStorageAdapter>(config);
    auto result = adapter->Init();
    if (!result) return tl::make_unexpected(result.error());
    return adapter;
#else
    return tl::make_unexpected(ErrorCode::NOT_SUPPORTED);
#endif
}

ObjectStorageBackend::ObjectStorageBackend(
    ObjectStorageConfig config, std::shared_ptr<ObjectStorageAdapter> adapter)
    : config_(std::move(config)), adapter_(std::move(adapter)) {}

bool ObjectStorageBackend::Reserve(uint64_t bytes) {
    auto current = staging_bytes_.load();
    do {
        if (current > config_.max_pending_bytes ||
            bytes > config_.max_pending_bytes - current)
            return false;
    } while (!staging_bytes_.compare_exchange_weak(current, current + bytes));
    return true;
}

ErrorCode ObjectStorageBackend::Write(const ObjectStorageDescriptor& descriptor,
                                      const std::vector<Slice>& slices,
                                      const Copy& copy, bool* upload_started) {
    if (upload_started) *upload_started = false;
    if (descriptor.namespace_id != config_.NamespaceId() ||
        descriptor.object_size > config_.max_object_bytes ||
        descriptor.object_size > std::numeric_limits<size_t>::max())
        return ErrorCode::INVALID_PARAMS;
    uint64_t size = 0;
    for (const auto& slice : slices) {
        if ((slice.size && !slice.ptr) ||
            slice.size > descriptor.object_size - size)
            return ErrorCode::INVALID_PARAMS;
        size += slice.size;
    }
    if (size != descriptor.object_size) return ErrorCode::INVALID_PARAMS;
    if (!Reserve(size)) return ErrorCode::NO_AVAILABLE_HANDLE;
    struct Release {
        std::atomic<uint64_t>& bytes;
        uint64_t size;
        ~Release() { bytes.fetch_sub(size); }
    } release{staging_bytes_, size};
    std::unique_ptr<char[]> staging(new (std::nothrow) char[size]);
    if (!staging) return ErrorCode::NO_AVAILABLE_HANDLE;
    size_t offset = 0;
    for (const auto& slice : slices) {
        if (slice.size && !copy(staging.get() + offset, slice.ptr, slice.size))
            return ErrorCode::TRANSFER_FAIL;
        offset += slice.size;
    }
    if (upload_started) *upload_started = true;
    auto result = adapter_->Put(descriptor.object_key, {staging.get(), size});
    return result ? ErrorCode::OK : result.error();
}

ErrorCode ObjectStorageBackend::Read(const ObjectStorageDescriptor& descriptor,
                                     const std::vector<Slice>& slices,
                                     uint64_t offset, const Copy& copy) {
    if (descriptor.namespace_id != config_.NamespaceId() ||
        offset > descriptor.object_size)
        return ErrorCode::INVALID_PARAMS;
    uint64_t size = 0;
    for (const auto& slice : slices) {
        if ((slice.size && !slice.ptr) ||
            slice.size > descriptor.object_size - offset - size)
            return ErrorCode::INVALID_PARAMS;
        size += slice.size;
    }
    if (!size) return ErrorCode::OK;
    const auto chunk = std::min<uint64_t>(size, 4ULL * 1024 * 1024);
    if (!Reserve(chunk)) return ErrorCode::NO_AVAILABLE_HANDLE;
    struct Release {
        std::atomic<uint64_t>& bytes;
        uint64_t size;
        ~Release() { bytes.fetch_sub(size); }
    } release{staging_bytes_, chunk};
    std::unique_ptr<char[]> staging(new (std::nothrow) char[chunk]);
    if (!staging) return ErrorCode::NO_AVAILABLE_HANDLE;
    for (const auto& slice : slices) {
        for (size_t done = 0; done < slice.size;) {
            const auto length = std::min<uint64_t>(chunk, slice.size - done);
            if (offset >
                static_cast<uint64_t>(std::numeric_limits<off_t>::max()))
                return ErrorCode::INVALID_PARAMS;
            auto result = adapter_->GetRange(descriptor.object_key,
                                             staging.get(), length, offset);
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
