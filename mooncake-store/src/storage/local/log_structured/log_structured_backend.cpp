#include "storage/local/log_structured/log_structured_backend.h"

#include <algorithm>
#include <cstring>
#include <filesystem>
#include <limits>

#include <glog/logging.h>

#include "tenant_id.h"

namespace mooncake {
namespace {

constexpr uint64_t kDefaultSegmentBytes = 256ULL * 1024 * 1024;

std::pair<std::string, std::string> SplitStorageKey(
    std::string_view storage_key) {
    auto [tenant_id, object_key] = TenantId::ParseScopedKey(storage_key);
    return {tenant_id.value(), std::move(object_key)};
}

}  // namespace

LogStructuredStorageBackend::LogStructuredStorageBackend(
    const FileStorageConfig& config)
    : StorageBackendInterface(config) {}

ErrorCode LogStructuredStorageBackend::ToWriteError(
    logstructured::StoreError error) {
    if (error == logstructured::StoreError::kInvalidArgument) {
        return ErrorCode::INVALID_PARAMS;
    }
    if (error == logstructured::StoreError::kNotFound) {
        return ErrorCode::OBJECT_NOT_FOUND;
    }
    return ErrorCode::FILE_WRITE_FAIL;
}

ErrorCode LogStructuredStorageBackend::ToReadError(
    logstructured::StoreError error) {
    if (error == logstructured::StoreError::kNotFound) {
        return ErrorCode::OBJECT_NOT_FOUND;
    }
    if (error == logstructured::StoreError::kInvalidArgument) {
        return ErrorCode::INVALID_PARAMS;
    }
    return ErrorCode::FILE_READ_FAIL;
}

tl::expected<std::string, ErrorCode> LogStructuredStorageBackend::ConcatSlices(
    const std::vector<Slice>& slices) {
    size_t total_size = 0;
    for (const auto& slice : slices) {
        if (slice.size > std::numeric_limits<size_t>::max() - total_size) {
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
        total_size += slice.size;
    }
    std::string value;
    value.reserve(total_size);
    for (const auto& slice : slices) {
        if (slice.size != 0 && slice.ptr == nullptr) {
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
        value.append(static_cast<const char*>(slice.ptr), slice.size);
    }
    return value;
}

tl::expected<void, ErrorCode> LogStructuredStorageBackend::Init() {
    std::lock_guard lock(mutex_);
    if (initialized_.load(std::memory_order_acquire)) return {};
    const std::filesystem::path root =
        std::filesystem::path(file_storage_config_.storage_filepath) /
        "log_structured";
    auto store = logstructured::LogStructuredStore::Open(
        {.root_path = root.string(),
         .max_segment_bytes = kDefaultSegmentBytes,
         .sync_data = true,
         .sync_wal = true});
    if (!store) {
        LOG(ERROR) << "Failed to initialize log-structured backend at " << root;
        return tl::make_unexpected(ToWriteError(store.error()));
    }
    store_ = std::move(store.value());
    initialized_.store(true, std::memory_order_release);
    return {};
}

tl::expected<int64_t, ErrorCode> LogStructuredStorageBackend::BatchOffload(
    const std::unordered_map<std::string, std::vector<Slice>>& batch_object,
    std::function<ErrorCode(const std::vector<std::string>&,
                            std::vector<StorageObjectMetadata>&)>
        complete_handler,
    EvictionHandler) {
    std::lock_guard lock(mutex_);
    if (!initialized_.load(std::memory_order_acquire) || !store_) {
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }
    if (batch_object.empty()) {
        return tl::make_unexpected(ErrorCode::INVALID_KEY);
    }

    std::vector<logstructured::PreparedWrite> prepared;
    std::vector<std::string> keys;
    std::vector<StorageObjectMetadata> metadatas;
    prepared.reserve(batch_object.size());
    keys.reserve(batch_object.size());
    metadatas.reserve(batch_object.size());
    for (const auto& [key, slices] : batch_object) {
        if (test_failure_predicate_ && test_failure_predicate_(key)) continue;
        auto value = ConcatSlices(slices);
        if (!value) continue;
        auto [tenant_id, object_key] = SplitStorageKey(key);
        auto write = store_->PreparePut(std::move(tenant_id),
                                        std::move(object_key), *value);
        if (!write) continue;
        metadatas.push_back(StorageObjectMetadata{
            -1, 0, static_cast<int64_t>(key.size()),
            static_cast<int64_t>(write->physical.value_length), ""});
        keys.push_back(key);
        prepared.push_back(std::move(write.value()));
    }
    if (prepared.empty()) {
        return tl::make_unexpected(ErrorCode::FILE_WRITE_FAIL);
    }

    if (complete_handler) {
        const auto result = complete_handler(keys, metadatas);
        if (result != ErrorCode::OK) {
            for (const auto& write : prepared) {
                store_->AbortPut(write.identity, write.sequence);
            }
            return tl::make_unexpected(result);
        }
    }
    for (const auto& write : prepared) {
        auto committed = store_->CommitPut(write.identity, write.sequence);
        if (!committed) {
            return tl::make_unexpected(ToWriteError(committed.error()));
        }
    }
    return static_cast<int64_t>(prepared.size());
}

tl::expected<void, ErrorCode> LogStructuredStorageBackend::BatchLoad(
    std::unordered_map<std::string, Slice>& batched_slices) {
    std::lock_guard lock(mutex_);
    if (!initialized_.load(std::memory_order_acquire) || !store_) {
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }
    for (const auto& [key, slice] : batched_slices) {
        const auto [tenant_id, object_key] = SplitStorageKey(key);
        auto value = store_->GetLatest(tenant_id, object_key);
        if (!value) return tl::make_unexpected(ToReadError(value.error()));
        if (value->size() != slice.size ||
            (slice.size != 0 && slice.ptr == nullptr)) {
            return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
        }
        if (!value->empty()) {
            std::memcpy(slice.ptr, value->data(), value->size());
        }
    }
    return {};
}

tl::expected<bool, ErrorCode> LogStructuredStorageBackend::IsExist(
    const std::string& key) {
    std::lock_guard lock(mutex_);
    if (!initialized_.load(std::memory_order_acquire) || !store_) {
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }
    const auto [tenant_id, object_key] = SplitStorageKey(key);
    return store_->ContainsLatest(tenant_id, object_key);
}

tl::expected<bool, ErrorCode>
LogStructuredStorageBackend::IsEnableOffloading() {
    std::lock_guard lock(mutex_);
    if (!initialized_.load(std::memory_order_acquire) || !store_) {
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }
    const auto entries = store_->SnapshotCurrentIndex();
    uint64_t total_size = 0;
    for (const auto& entry : entries) {
        total_size += entry.version.physical.value_length;
    }
    return entries.size() <
               static_cast<size_t>(file_storage_config_.total_keys_limit) &&
           total_size <
               static_cast<uint64_t>(file_storage_config_.total_size_limit);
}

tl::expected<void, ErrorCode> LogStructuredStorageBackend::ScanMeta(
    const std::function<ErrorCode(const std::vector<std::string>&,
                                  std::vector<StorageObjectMetadata>&)>&
        handler) {
    std::lock_guard lock(mutex_);
    if (!initialized_.load(std::memory_order_acquire) || !store_) {
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }
    std::vector<std::string> keys;
    std::vector<StorageObjectMetadata> metadatas;
    const size_t batch_limit = static_cast<size_t>(std::max<int64_t>(
        1, file_storage_config_.scanmeta_iterator_keys_limit));
    for (const auto& entry : store_->SnapshotCurrentIndex()) {
        keys.push_back(TenantId(entry.identity.tenant_id)
                           .MakeScopedKey(entry.identity.object_key));
        metadatas.push_back(StorageObjectMetadata{
            -1, 0, static_cast<int64_t>(keys.back().size()),
            static_cast<int64_t>(entry.version.physical.value_length), ""});
        if (keys.size() < batch_limit) continue;
        const auto result = handler(keys, metadatas);
        if (result != ErrorCode::OK) return tl::make_unexpected(result);
        keys.clear();
        metadatas.clear();
    }
    if (!keys.empty()) {
        const auto result = handler(keys, metadatas);
        if (result != ErrorCode::OK) return tl::make_unexpected(result);
    }
    return {};
}

void LogStructuredStorageBackend::SetTestFailurePredicate(
    std::function<bool(const std::string& key)> predicate) {
    std::lock_guard lock(mutex_);
    test_failure_predicate_ = std::move(predicate);
}

}  // namespace mooncake
