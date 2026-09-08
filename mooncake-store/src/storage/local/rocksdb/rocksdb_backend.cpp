#include "storage/local/rocksdb/rocksdb_backend.h"

#include <algorithm>
#include <cstring>
#include <filesystem>
#include <limits>
#include <string_view>
#include <unordered_set>

#include <glog/logging.h>
#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <rocksdb/slice.h>
#include <rocksdb/write_batch.h>

#include "environ.h"

namespace mooncake {
namespace {

constexpr char kCurrentPrefix = '\x01';
constexpr char kDataPrefix = '\x02';
constexpr size_t kEncodedSizeBytes = sizeof(uint64_t);

std::string CurrentKey(std::string_view key) {
    std::string encoded;
    encoded.reserve(1 + key.size());
    encoded.push_back(kCurrentPrefix);
    encoded.append(key);
    return encoded;
}

std::string DataKey(uint64_t version, std::string_view key) {
    std::string encoded;
    encoded.reserve(1 + sizeof(version) + key.size());
    encoded.push_back(kDataPrefix);
    for (size_t index = 0; index < sizeof(version); ++index) {
        encoded.push_back(static_cast<char>(version >> (index * 8)));
    }
    encoded.append(key);
    return encoded;
}

uint64_t DecodeVersion(std::string_view key) {
    if (key.size() < 1 + sizeof(uint64_t) || key.front() != kDataPrefix) {
        return 0;
    }
    uint64_t version = 0;
    for (size_t index = 0; index < sizeof(version); ++index) {
        version |=
            static_cast<uint64_t>(static_cast<unsigned char>(key[1 + index]))
            << (index * 8);
    }
    return version;
}

std::string EncodeCurrentValue(uint64_t value_size, std::string_view data_key) {
    std::string encoded;
    encoded.reserve(kEncodedSizeBytes + data_key.size());
    for (size_t index = 0; index < kEncodedSizeBytes; ++index) {
        encoded.push_back(static_cast<char>(value_size >> (index * 8)));
    }
    encoded.append(data_key);
    return encoded;
}

bool DecodeCurrentValue(std::string_view encoded, uint64_t* value_size,
                        std::string_view* data_key) {
    if (encoded.size() < kEncodedSizeBytes) return false;
    *value_size = 0;
    for (size_t index = 0; index < kEncodedSizeBytes; ++index) {
        *value_size |=
            static_cast<uint64_t>(static_cast<unsigned char>(encoded[index]))
            << (index * 8);
    }
    *data_key = encoded.substr(kEncodedSizeBytes);
    return !data_key->empty() && data_key->front() == kDataPrefix;
}

ErrorCode ToWriteError(const rocksdb::Status& status) {
    if (status.IsInvalidArgument()) return ErrorCode::INVALID_PARAMS;
    return ErrorCode::FILE_WRITE_FAIL;
}

ErrorCode ToReadError(const rocksdb::Status& status) {
    if (status.IsNotFound()) return ErrorCode::OBJECT_NOT_FOUND;
    if (status.IsInvalidArgument()) return ErrorCode::INVALID_PARAMS;
    return ErrorCode::FILE_READ_FAIL;
}

}  // namespace

RocksDBBackendConfig RocksDBBackendConfig::FromEnvironment() {
    RocksDBBackendConfig config;
    config.min_blob_size = Environ::GetUInt64("MOONCAKE_ROCKSDB_MIN_BLOB_SIZE",
                                              config.min_blob_size);
    config.blob_file_size = Environ::GetUInt64(
        "MOONCAKE_ROCKSDB_BLOB_FILE_SIZE", config.blob_file_size);
    config.blob_direct_write_partitions = static_cast<uint32_t>(
        Environ::GetUInt64("MOONCAKE_ROCKSDB_BLOB_DIRECT_WRITE_PARTITIONS",
                           config.blob_direct_write_partitions));
    config.enable_blob_files = Environ::GetBool(
        "MOONCAKE_ROCKSDB_ENABLE_BLOB_FILES", config.enable_blob_files);
    config.enable_blob_direct_write =
        Environ::GetBool("MOONCAKE_ROCKSDB_ENABLE_BLOB_DIRECT_WRITE",
                         config.enable_blob_direct_write);
    config.sync_writes =
        Environ::GetBool("MOONCAKE_ROCKSDB_SYNC_WRITES", config.sync_writes);
    return config;
}

bool RocksDBBackendConfig::Validate() const {
    return min_blob_size > 0 && blob_file_size >= min_blob_size &&
           blob_direct_write_partitions > 0 &&
           (!enable_blob_direct_write || enable_blob_files);
}

RocksDBStorageBackend::RocksDBStorageBackend(
    const FileStorageConfig& config, RocksDBBackendConfig backend_config)
    : StorageBackendInterface(config),
      backend_config_(std::move(backend_config)) {}

RocksDBStorageBackend::~RocksDBStorageBackend() = default;

tl::expected<void, ErrorCode> RocksDBStorageBackend::OpenDatabase() {
    const auto root =
        std::filesystem::path(file_storage_config_.storage_filepath) /
        "rocksdb";
    std::error_code error;
    std::filesystem::create_directories(root, error);
    if (error) return tl::make_unexpected(ErrorCode::FILE_OPEN_FAIL);

    rocksdb::Options options;
    options.create_if_missing = true;
    options.compression = rocksdb::kNoCompression;
    options.enable_blob_files = backend_config_.enable_blob_files;
    options.enable_blob_direct_write = backend_config_.enable_blob_direct_write;
    options.allow_concurrent_memtable_write = false;
    options.min_blob_size = backend_config_.min_blob_size;
    options.blob_file_size = backend_config_.blob_file_size;
    options.blob_direct_write_partitions =
        backend_config_.blob_direct_write_partitions;

    std::unique_ptr<rocksdb::DB> database;
    const auto status = rocksdb::DB::Open(options, root.string(), &database);
    if (!status.ok()) {
        LOG(ERROR) << "Failed to open RocksDB backend at " << root << ": "
                   << status.ToString();
        return tl::make_unexpected(ToWriteError(status));
    }
    db_ = std::move(database);
    return {};
}

tl::expected<void, ErrorCode> RocksDBStorageBackend::RecoverState() {
    uint64_t next_version = 1;
    uint64_t key_count = 0;
    uint64_t logical_bytes = 0;
    std::unordered_set<std::string> referenced_data;
    rocksdb::ReadOptions read_options;
    std::unique_ptr<rocksdb::Iterator> iterator(db_->NewIterator(read_options));
    for (iterator->Seek(rocksdb::Slice(&kCurrentPrefix, 1));
         iterator->Valid() && iterator->key().size() > 0 &&
         iterator->key()[0] == kCurrentPrefix;
         iterator->Next()) {
        uint64_t value_size = 0;
        std::string_view data_key;
        const auto value = iterator->value().ToStringView();
        if (!DecodeCurrentValue(value, &value_size, &data_key)) {
            LOG(ERROR) << "Invalid RocksDB current mapping during recovery";
            return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
        }
        referenced_data.emplace(data_key);
        ++key_count;
        logical_bytes += value_size;
    }
    if (!iterator->status().ok()) {
        return tl::make_unexpected(ToReadError(iterator->status()));
    }

    rocksdb::WriteBatch cleanup;
    for (iterator->Seek(rocksdb::Slice(&kDataPrefix, 1));
         iterator->Valid() && iterator->key().size() > 0 &&
         iterator->key()[0] == kDataPrefix;
         iterator->Next()) {
        const auto key = iterator->key().ToString();
        const uint64_t version = DecodeVersion(key);
        if (version == 0) {
            LOG(ERROR) << "Invalid RocksDB data key during recovery";
            return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
        }
        next_version = std::max(next_version, version + 1);
        if (referenced_data.erase(key) == 0) cleanup.Delete(key);
    }
    if (!iterator->status().ok()) {
        return tl::make_unexpected(ToReadError(iterator->status()));
    }
    if (!referenced_data.empty()) {
        LOG(ERROR) << "RocksDB current mapping references missing payload";
        return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
    }
    if (cleanup.Count() != 0) {
        rocksdb::WriteOptions write_options;
        write_options.sync = backend_config_.sync_writes;
        const auto status = db_->Write(write_options, &cleanup);
        if (!status.ok()) return tl::make_unexpected(ToWriteError(status));
    }
    next_version_.store(next_version, std::memory_order_relaxed);
    key_count_.store(key_count, std::memory_order_relaxed);
    logical_bytes_.store(logical_bytes, std::memory_order_relaxed);
    return {};
}

tl::expected<void, ErrorCode> RocksDBStorageBackend::Init() {
    std::lock_guard lock(mutex_);
    if (initialized_.load(std::memory_order_acquire)) return {};
    if (!backend_config_.Validate()) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    auto opened = OpenDatabase();
    if (!opened) return opened;
    auto recovered = RecoverState();
    if (!recovered) {
        db_.reset();
        return recovered;
    }
    initialized_.store(true, std::memory_order_release);
    return {};
}

tl::expected<std::string, ErrorCode> RocksDBStorageBackend::ConcatSlices(
    const std::vector<Slice>& slices) {
    size_t total_size = 0;
    for (const auto& slice : slices) {
        if (slice.size != 0 && slice.ptr == nullptr) {
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
        if (slice.size > std::numeric_limits<size_t>::max() - total_size) {
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
        total_size += slice.size;
    }
    std::string value;
    value.reserve(total_size);
    for (const auto& slice : slices) {
        if (slice.size != 0) {
            value.append(static_cast<const char*>(slice.ptr), slice.size);
        }
    }
    return value;
}

tl::expected<int64_t, ErrorCode> RocksDBStorageBackend::BatchOffload(
    const std::unordered_map<std::string, std::vector<Slice>>& batch_object,
    std::function<ErrorCode(const std::vector<std::string>&,
                            std::vector<StorageObjectMetadata>&)>
        complete_handler,
    EvictionHandler) {
    std::shared_lock lock(mutex_);
    if (!initialized_.load(std::memory_order_acquire) || !db_) {
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }
    if (batch_object.empty()) {
        return tl::make_unexpected(ErrorCode::INVALID_KEY);
    }

    std::vector<size_t> stripes;
    stripes.reserve(batch_object.size());
    for (const auto& [key, slices] : batch_object) {
        static_cast<void>(slices);
        stripes.push_back(std::hash<std::string>{}(key) % key_mutexes_.size());
    }
    std::sort(stripes.begin(), stripes.end());
    stripes.erase(std::unique(stripes.begin(), stripes.end()), stripes.end());
    std::vector<std::unique_lock<std::mutex>> key_locks;
    key_locks.reserve(stripes.size());
    for (const size_t stripe : stripes) {
        key_locks.emplace_back(key_mutexes_[stripe]);
    }

    std::vector<std::string> keys;
    std::vector<std::string> values;
    std::vector<std::string> data_keys;
    std::vector<StorageObjectMetadata> metadatas;
    std::optional<ErrorCode> first_error;
    keys.reserve(batch_object.size());
    values.reserve(batch_object.size());
    data_keys.reserve(batch_object.size());
    metadatas.reserve(batch_object.size());
    rocksdb::WriteBatch prepare_batch;
    for (const auto& [key, slices] : batch_object) {
        if (test_failure_predicate_ && test_failure_predicate_(key)) {
            first_error = ErrorCode::FILE_WRITE_FAIL;
            continue;
        }
        auto value = ConcatSlices(slices);
        if (!value) {
            first_error = value.error();
            continue;
        }
        const uint64_t version =
            next_version_.fetch_add(1, std::memory_order_relaxed);
        keys.push_back(key);
        values.push_back(std::move(value.value()));
        data_keys.push_back(DataKey(version, key));
        prepare_batch.Put(data_keys.back(), values.back());
        metadatas.push_back(StorageObjectMetadata{
            -1, 0, static_cast<int64_t>(key.size()),
            static_cast<int64_t>(values.back().size()), ""});
    }
    if (keys.empty()) {
        return tl::make_unexpected(
            first_error.value_or(ErrorCode::FILE_WRITE_FAIL));
    }

    rocksdb::WriteOptions write_options;
    write_options.sync = backend_config_.sync_writes;
    auto status = db_->Write(write_options, &prepare_batch);
    if (!status.ok()) return tl::make_unexpected(ToWriteError(status));

    const auto cleanup_prepared = [&]() {
        rocksdb::WriteBatch cleanup;
        for (const auto& data_key : data_keys) cleanup.Delete(data_key);
        const auto cleanup_status = db_->Write(write_options, &cleanup);
        if (!cleanup_status.ok()) {
            LOG(ERROR) << "Failed to remove unpublished RocksDB values: "
                       << cleanup_status.ToString();
        }
    };

    std::unique_lock publication_lock(publication_mutex_);
    if (complete_handler) {
        try {
            const auto result = complete_handler(keys, metadatas);
            if (result != ErrorCode::OK) {
                cleanup_prepared();
                return tl::make_unexpected(result);
            }
        } catch (...) {
            cleanup_prepared();
            throw;
        }
    }

    rocksdb::ReadOptions read_options;
    rocksdb::WriteBatch commit_batch;
    uint64_t inserted = 0;
    uint64_t old_bytes = 0;
    uint64_t new_bytes = 0;
    for (size_t index = 0; index < keys.size(); ++index) {
        std::string old_mapping;
        const auto current_key = CurrentKey(keys[index]);
        status = db_->Get(read_options, current_key, &old_mapping);
        if (status.ok()) {
            uint64_t old_size = 0;
            std::string_view old_data_key;
            if (!DecodeCurrentValue(old_mapping, &old_size, &old_data_key)) {
                return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
            }
            old_bytes += old_size;
            commit_batch.Delete(old_data_key);
        } else if (status.IsNotFound()) {
            ++inserted;
        } else {
            return tl::make_unexpected(ToReadError(status));
        }
        new_bytes += values[index].size();
        commit_batch.Put(current_key, EncodeCurrentValue(values[index].size(),
                                                         data_keys[index]));
    }
    status = db_->Write(write_options, &commit_batch);
    if (!status.ok()) return tl::make_unexpected(ToWriteError(status));
    key_count_.fetch_add(inserted, std::memory_order_relaxed);
    if (new_bytes >= old_bytes) {
        logical_bytes_.fetch_add(new_bytes - old_bytes,
                                 std::memory_order_relaxed);
    } else {
        logical_bytes_.fetch_sub(old_bytes - new_bytes,
                                 std::memory_order_relaxed);
    }
    return static_cast<int64_t>(keys.size());
}

tl::expected<void, ErrorCode> RocksDBStorageBackend::BatchLoad(
    std::unordered_map<std::string, Slice>& batched_slices) {
    std::shared_lock lock(mutex_);
    std::shared_lock publication_lock(publication_mutex_);
    if (!initialized_.load(std::memory_order_acquire) || !db_) {
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }

    std::vector<std::string> current_keys;
    std::vector<Slice*> destinations;
    current_keys.reserve(batched_slices.size());
    destinations.reserve(batched_slices.size());
    for (auto& [key, slice] : batched_slices) {
        if (slice.size != 0 && slice.ptr == nullptr) {
            return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
        }
        current_keys.push_back(CurrentKey(key));
        destinations.push_back(&slice);
    }

    std::vector<rocksdb::Slice> current_key_slices;
    current_key_slices.reserve(current_keys.size());
    for (const auto& key : current_keys) current_key_slices.emplace_back(key);
    std::vector<std::string> mappings;
    const auto mapping_statuses =
        db_->MultiGet(rocksdb::ReadOptions(), current_key_slices, &mappings);

    std::vector<rocksdb::Slice> data_key_slices;
    data_key_slices.reserve(mappings.size());
    for (size_t index = 0; index < mappings.size(); ++index) {
        if (!mapping_statuses[index].ok()) {
            return tl::make_unexpected(ToReadError(mapping_statuses[index]));
        }
        uint64_t value_size = 0;
        std::string_view data_key;
        if (!DecodeCurrentValue(mappings[index], &value_size, &data_key) ||
            value_size != destinations[index]->size) {
            return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
        }
        data_key_slices.emplace_back(data_key);
    }

    std::vector<std::string> values;
    const auto value_statuses =
        db_->MultiGet(rocksdb::ReadOptions(), data_key_slices, &values);
    for (size_t index = 0; index < values.size(); ++index) {
        if (!value_statuses[index].ok()) {
            return tl::make_unexpected(ToReadError(value_statuses[index]));
        }
        if (values[index].size() != destinations[index]->size) {
            return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
        }
        std::memcpy(destinations[index]->ptr, values[index].data(),
                    values[index].size());
    }
    return {};
}

tl::expected<bool, ErrorCode> RocksDBStorageBackend::IsExist(
    const std::string& key) {
    std::shared_lock lock(mutex_);
    std::shared_lock publication_lock(publication_mutex_);
    if (!initialized_.load(std::memory_order_acquire) || !db_) {
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }
    std::string mapping;
    const auto status =
        db_->Get(rocksdb::ReadOptions(), CurrentKey(key), &mapping);
    if (status.IsNotFound()) return false;
    if (!status.ok()) return tl::make_unexpected(ToReadError(status));
    return true;
}

tl::expected<bool, ErrorCode> RocksDBStorageBackend::IsEnableOffloading() {
    std::shared_lock lock(mutex_);
    if (!initialized_.load(std::memory_order_acquire) || !db_) {
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }
    if (file_storage_config_.total_keys_limit <= 0 ||
        file_storage_config_.total_size_limit <= 0) {
        return false;
    }
    return key_count_.load(std::memory_order_relaxed) <
               static_cast<uint64_t>(file_storage_config_.total_keys_limit) &&
           logical_bytes_.load(std::memory_order_relaxed) <
               static_cast<uint64_t>(file_storage_config_.total_size_limit);
}

tl::expected<void, ErrorCode> RocksDBStorageBackend::ScanMeta(
    const std::function<ErrorCode(const std::vector<std::string>&,
                                  std::vector<StorageObjectMetadata>&)>&
        handler) {
    std::shared_lock lock(mutex_);
    std::shared_lock publication_lock(publication_mutex_);
    if (!initialized_.load(std::memory_order_acquire) || !db_) {
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }
    const size_t batch_limit = static_cast<size_t>(std::max<int64_t>(
        1, file_storage_config_.scanmeta_iterator_keys_limit));
    std::vector<std::string> keys;
    std::vector<StorageObjectMetadata> metadatas;
    keys.reserve(batch_limit);
    metadatas.reserve(batch_limit);
    std::unique_ptr<rocksdb::Iterator> iterator(
        db_->NewIterator(rocksdb::ReadOptions()));
    for (iterator->Seek(rocksdb::Slice(&kCurrentPrefix, 1));
         iterator->Valid() && iterator->key().size() > 0 &&
         iterator->key()[0] == kCurrentPrefix;
         iterator->Next()) {
        uint64_t value_size = 0;
        std::string_view data_key;
        if (!DecodeCurrentValue(iterator->value().ToStringView(), &value_size,
                                &data_key)) {
            return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
        }
        keys.push_back(iterator->key().ToString().substr(1));
        metadatas.push_back(StorageObjectMetadata{
            -1, 0, static_cast<int64_t>(keys.back().size()),
            static_cast<int64_t>(value_size), ""});
        if (keys.size() < batch_limit) continue;
        const auto result = handler(keys, metadatas);
        if (result != ErrorCode::OK) return tl::make_unexpected(result);
        keys.clear();
        metadatas.clear();
    }
    if (!iterator->status().ok()) {
        return tl::make_unexpected(ToReadError(iterator->status()));
    }
    if (!keys.empty()) {
        const auto result = handler(keys, metadatas);
        if (result != ErrorCode::OK) return tl::make_unexpected(result);
    }
    return {};
}

void RocksDBStorageBackend::SetTestFailurePredicate(
    std::function<bool(const std::string& key)> predicate) {
    std::unique_lock lock(mutex_);
    test_failure_predicate_ = std::move(predicate);
}

void RocksDBStorageBackend::RemoveAll() {
    std::unique_lock lock(mutex_);
    if (!initialized_.load(std::memory_order_acquire) || !db_) return;
    std::unique_lock publication_lock(publication_mutex_);
    const auto root =
        std::filesystem::path(file_storage_config_.storage_filepath) /
        "rocksdb";
    db_.reset();
    const auto destroy_status =
        rocksdb::DestroyDB(root.string(), rocksdb::Options());
    if (!destroy_status.ok()) {
        LOG(ERROR) << "Failed to destroy RocksDB backend: "
                   << destroy_status.ToString();
        initialized_.store(false, std::memory_order_release);
        return;
    }
    auto opened = OpenDatabase();
    if (!opened) {
        initialized_.store(false, std::memory_order_release);
        return;
    }
    next_version_.store(1, std::memory_order_relaxed);
    key_count_.store(0, std::memory_order_relaxed);
    logical_bytes_.store(0, std::memory_order_relaxed);
}

}  // namespace mooncake
