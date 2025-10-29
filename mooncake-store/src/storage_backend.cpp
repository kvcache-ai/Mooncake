#include "storage_backend.h"

#include <fcntl.h>
#include <unistd.h>
#include <string>
#include <vector>
#include <regex>
#include <ylt/struct_pb.hpp>
#include "utils.h"
#include "mutex.h"

namespace mooncake {

tl::expected<void, ErrorCode> StorageBackend::StoreObject(
    const std::string& path, const std::vector<Slice>& slices) {
    ResolvePath(path);
    auto file = create_file(path, FileMode::Write);
    if (!file) {
        LOG(ERROR) << "Failed to open file for writing: " << path;
        return tl::make_unexpected(ErrorCode::FILE_OPEN_FAIL);
    }

    std::vector<iovec> iovs;
    size_t slices_total_size = 0;
    for (const auto& slice : slices) {
        iovec io{slice.ptr, slice.size};
        iovs.push_back(io);
        slices_total_size += slice.size;
    }

    auto write_result =
        file->vector_write(iovs.data(), static_cast<int>(iovs.size()), 0);
    if (!write_result) {
        LOG(ERROR) << "vector_write failed for: " << path
                   << ", error: " << write_result.error();
        return tl::make_unexpected(write_result.error());
    }

    if (*write_result != slices_total_size) {
        LOG(ERROR) << "Write size mismatch for: " << path
                   << ", expected: " << slices_total_size
                   << ", got: " << *write_result;
        return tl::make_unexpected(ErrorCode::FILE_WRITE_FAIL);
    }

    return {};
}

tl::expected<void, ErrorCode> StorageBackend::StoreObject(
    const std::string& path, const std::string& str) {
    return StoreObject(path, std::span<const char>(str.data(), str.size()));
}

tl::expected<void, ErrorCode> StorageBackend::StoreObject(
    const std::string& path, std::span<const char> data) {
    ResolvePath(path);
    auto file = create_file(path, FileMode::Write);
    if (!file) {
        LOG(ERROR) << "Failed to open file for writing: " << path;
        return tl::make_unexpected(ErrorCode::FILE_OPEN_FAIL);
    }

    size_t file_total_size = data.size();
    auto write_result = file->write(data, file_total_size);

    if (!write_result) {
        LOG(ERROR) << "Write failed for: " << path
                   << ", error: " << write_result.error();
        return tl::make_unexpected(write_result.error());
    }
    if (*write_result != file_total_size) {
        LOG(ERROR) << "Write size mismatch for: " << path
                   << ", expected: " << file_total_size
                   << ", got: " << *write_result;
        return tl::make_unexpected(ErrorCode::FILE_WRITE_FAIL);
    }

    return {};
}

tl::expected<void, ErrorCode> StorageBackend::LoadObject(
    const std::string& path, std::vector<Slice>& slices, size_t length) {
    ResolvePath(path);
    auto file = create_file(path, FileMode::Read);
    if (!file) {
        LOG(ERROR) << "Failed to open file for reading: " << path;
        return tl::make_unexpected(ErrorCode::FILE_OPEN_FAIL);
    }

    off_t current_offset = 0;
    size_t total_bytes_processed = 0;

    std::vector<iovec> iovs_chunk;
    off_t chunk_start_offset = 0;
    size_t chunk_length = 0;

    auto process_chunk = [&]() -> tl::expected<void, ErrorCode> {
        if (iovs_chunk.empty()) {
            return {};
        }

        auto read_result = file->vector_read(
            iovs_chunk.data(), static_cast<int>(iovs_chunk.size()),
            chunk_start_offset);
        if (!read_result) {
            LOG(ERROR) << "vector_read failed for chunk at offset "
                       << chunk_start_offset << " for path: " << path
                       << ", error: " << read_result.error();
            return tl::make_unexpected(read_result.error());
        }
        if (*read_result != chunk_length) {
            LOG(ERROR) << "Read size mismatch for chunk in path: " << path
                       << ", expected: " << chunk_length
                       << ", got: " << *read_result;
            return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
        }

        total_bytes_processed += chunk_length;

        iovs_chunk.clear();
        chunk_length = 0;

        return {};
    };

    for (const auto& slice : slices) {
        if (slice.ptr != nullptr) {
            if (iovs_chunk.empty()) {
                chunk_start_offset = current_offset;
            }
            iovs_chunk.push_back({slice.ptr, slice.size});
            chunk_length += slice.size;
        } else {
            auto result = process_chunk();
            if (!result) {
                return result;
            }

            total_bytes_processed += slice.size;
        }

        current_offset += slice.size;
    }

    auto result = process_chunk();
    if (!result) {
        return result;
    }

    if (total_bytes_processed != length) {
        LOG(ERROR) << "Total read size mismatch for: " << path
                   << ", expected: " << length
                   << ", got: " << total_bytes_processed;
        return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
    }

    return {};
}

tl::expected<void, ErrorCode> StorageBackend::LoadObject(
    const std::string& path, std::string& str, size_t length) {
    ResolvePath(path);
    auto file = create_file(path, FileMode::Read);
    if (!file) {
        LOG(ERROR) << "Failed to open file for reading: " << path;
        return tl::make_unexpected(ErrorCode::FILE_OPEN_FAIL);
    }

    auto read_result = file->read(str, length);
    if (!read_result) {
        LOG(ERROR) << "read failed for: " << path
                   << ", error: " << read_result.error();
        return tl::make_unexpected(read_result.error());
    }
    if (*read_result != length) {
        LOG(ERROR) << "Read size mismatch for: " << path
                   << ", expected: " << length << ", got: " << *read_result;
        return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
    }

    return {};
}

void StorageBackend::RemoveFile(const std::string& path) {
    namespace fs = std::filesystem;
    // TODO: attention: this function is not thread-safe, need to add lock if
    // used in multi-thread environment Check if the file exists before
    // attempting to remove it
    // TODO: add a sleep to ensure the write thread has time to create the
    // corresponding file it will be fixed in the next version
    std::this_thread::sleep_for(
        std::chrono::microseconds(50));  // sleep for 50 us
    if (fs::exists(path)) {
        std::error_code ec;
        fs::remove(path, ec);
        if (ec) {
            LOG(ERROR) << "Failed to delete file: " << path
                       << ", error: " << ec.message();
        }
    }
}

void StorageBackend::RemoveByRegex(const std::string& regex_pattern) {
    namespace fs = std::filesystem;
    std::regex pattern;

    try {
        pattern = std::regex(regex_pattern, std::regex::ECMAScript);
    } catch (const std::regex_error& e) {
        LOG(ERROR) << "Invalid regex pattern for storage removal: "
                   << regex_pattern << ", error: " << e.what();
        return;
    }

    fs::path storage_root = fs::path(root_dir_) / fsdir_;
    if (!fs::exists(storage_root) || !fs::is_directory(storage_root)) {
        LOG(WARNING) << "Storage root directory does not exist: "
                     << storage_root;
        return;
    }

    std::vector<fs::path> paths_to_remove;

    for (const auto& entry : fs::recursive_directory_iterator(storage_root)) {
        if (fs::is_regular_file(entry.status())) {
            std::string filename = entry.path().filename().string();

            if (std::regex_search(filename, pattern)) {
                paths_to_remove.push_back(entry.path());
            }
        }
    }

    for (const auto& path : paths_to_remove) {
        std::error_code ec;
        if (fs::remove(path, ec)) {
            VLOG(1) << "Removed file by regex: " << path;
        } else {
            LOG(ERROR) << "Failed to delete file: " << path
                       << ", error: " << ec.message();
        }
    }

    return;
}

void StorageBackend::RemoveAll() {
    namespace fs = std::filesystem;
    // Iterate through the root directory and remove all files
    for (const auto& entry : fs::directory_iterator(root_dir_)) {
        if (fs::is_regular_file(entry.status())) {
            std::error_code ec;
            fs::remove(entry.path(), ec);
            if (ec) {
                LOG(ERROR) << "Failed to delete file: " << entry.path()
                           << ", error: " << ec.message();
            }
        }
    }
}

void StorageBackend::ResolvePath(const std::string& path) const {
    // Safely construct path using std::filesystem
    namespace fs = std::filesystem;
    fs::path full_path = path;

    // Create all parent directories if they don't exist
    std::error_code ec;
    fs::path parent_path = full_path.parent_path();
    if (!parent_path.empty() && !fs::exists(parent_path)) {
        if (!fs::create_directories(parent_path, ec) && ec) {
            LOG(ERROR) << "Failed to create directories: " << parent_path
                       << ", error: " << ec.message();
        }
    }
}

std::unique_ptr<StorageFile> StorageBackend::create_file(
    const std::string& path, FileMode mode) const {
    int flags = O_CLOEXEC;
    int access_mode = 0;
    switch (mode) {
        case FileMode::Read:
            access_mode = O_RDONLY;
            break;
        case FileMode::Write:
            access_mode = O_WRONLY | O_CREAT | O_TRUNC;
            break;
    }

    int fd = open(path.c_str(), flags | access_mode, 0644);
    if (fd < 0) {
        return nullptr;
    }

#ifdef USE_3FS
    if (is_3fs_dir_) {
        if (hf3fs_reg_fd(fd, 0) > 0) {
            close(fd);
            return nullptr;
        }
        return resource_manager_ ? std::make_unique<ThreeFSFile>(
                                       path, fd, resource_manager_.get())
                                 : nullptr;
    }
#endif

    return std::make_unique<PosixFile>(path, fd);
}

BucketIdGenerator::BucketIdGenerator(int64_t start) {
    if (start <= 0) {
        auto cur_time_stamp = time_gen();
        current_id_ = (cur_time_stamp << TIMESTAMP_SHIFT) | SEQUENCE_ID_SHIFT;
    } else {
        current_id_ = start;
    }
}

int64_t BucketIdGenerator::NextId() {
    return current_id_.fetch_add(1, std::memory_order_relaxed) + 1;
}

int64_t BucketIdGenerator::CurrentId() {
    return current_id_.load(std::memory_order_relaxed);
}

BucketStorageBackend::BucketStorageBackend(const std::string& storage_path)
    : storage_path_(storage_path) {}

tl::expected<int64_t, ErrorCode> BucketStorageBackend::BatchOffload(
    const std::unordered_map<std::string, std::vector<Slice>>& batch_object,
    std::function<
        ErrorCode(const std::unordered_map<std::string, BucketObjectMetadata>&)>
        complete_handler) {
    if (!initialized_.load(std::memory_order_acquire)) {
        LOG(ERROR)
            << "Storage backend is not initialized. Call Init() before use.";
        return tl::unexpected(ErrorCode::INTERNAL_ERROR);
    }
    if (batch_object.empty()) {
        LOG(ERROR) << "batch object is empty";
        return tl::make_unexpected(ErrorCode::INVALID_KEY);
    }
    auto bucket_id = bucket_id_generator_->NextId();
    std::vector<iovec> iovs;
    auto build_bucket_result = BuildBucket(batch_object, iovs);
    if (!build_bucket_result) {
        LOG(ERROR) << "Failed to build bucket with id: " << bucket_id;
        return tl::make_unexpected(build_bucket_result.error());
    }
    auto bucket = build_bucket_result.value();
    auto write_bucket_result = WriteBucket(bucket_id, bucket, iovs);
    if (!write_bucket_result) {
        LOG(ERROR) << "Failed to write bucket with id: " << bucket_id;
        return tl::make_unexpected(write_bucket_result.error());
    }
    if (complete_handler != nullptr) {
        auto error_code = complete_handler(bucket->object_metadata);
        if (error_code != ErrorCode::OK) {
            LOG(ERROR) << "Sync Store object failed,err_code = " << error_code;
            return tl::make_unexpected(error_code);
        }
    }
    SharedMutexLocker lock(&mutex_);
    total_size_ += bucket->data_size + bucket->meta_size;
    for (auto object_metadata_it : bucket->object_metadata) {
        object_bucket_map_.emplace(
            object_metadata_it.first,
            StorageObjectMetadata{bucket_id, object_metadata_it.second.offset,
                                  object_metadata_it.second.key_size,
                                  object_metadata_it.second.data_size});
    }
    buckets_.emplace(bucket_id, std::move(bucket));
    return bucket_id;
}

tl::expected<void, ErrorCode> BucketStorageBackend::BatchQuery(
    const std::vector<std::string>& keys,
    std::unordered_map<std::string, StorageObjectMetadata>&
        batch_object_metadata) {
    SharedMutexLocker lock(&mutex_, shared_lock);
    for (const auto& key : keys) {
        auto object_metadata_it = object_bucket_map_.find(key);
        if (object_metadata_it != object_bucket_map_.end()) {
            batch_object_metadata.emplace(key, object_metadata_it->second);
        } else {
            LOG(ERROR) << "Key " << key << " does not exist";
        }
    }
    return {};
}

tl::expected<void, ErrorCode> BucketStorageBackend::BatchLoad(
    std::unordered_map<std::string, Slice>& batch_object) {
    std::unordered_map<int64_t, std::vector<std::string>> bucket_key_map;
    {
        SharedMutexLocker lock(&mutex_, shared_lock);
        for (const auto& key_it : batch_object) {
            auto object_bucket_it = object_bucket_map_.find(key_it.first);
            if (object_bucket_it == object_bucket_map_.end()) {
                LOG(ERROR) << "key " << key_it.first << " does not exist";
                return tl::make_unexpected(ErrorCode::INVALID_KEY);
            }
            auto [bucket_keys_it, _] =
                bucket_key_map.try_emplace(object_bucket_it->second.bucket_id);
            bucket_keys_it->second.emplace_back(key_it.first);
        }
    }
    for (const auto& bucket_key_it : bucket_key_map) {
        auto result = BatchLoadBucket(bucket_key_it.first, bucket_key_it.second,
                                      batch_object);
        if (!result) {
            LOG(ERROR) << "Failed to load bucket " << bucket_key_it.first;
            return result;
        }
    }
    return {};
}

tl::expected<void, ErrorCode> BucketStorageBackend::GetBucketKeys(
    int64_t bucket_id, std::vector<std::string>& bucket_keys) {
    SharedMutexLocker locker(&mutex_, shared_lock);
    auto bucket_it = buckets_.find(bucket_id);
    if (bucket_it == buckets_.end()) {
        return tl::make_unexpected(ErrorCode::BUCKET_NOT_FOUND);
    }
    for (const auto& key : bucket_it->second->keys) {
        bucket_keys.emplace_back(key);
    }
    return {};
}

tl::expected<void, ErrorCode> BucketStorageBackend::Init() {
    namespace fs = std::filesystem;
    try {
        if (initialized_.load(std::memory_order_acquire)) {
            LOG(ERROR) << "Storage backend already initialized";
            return tl::unexpected(ErrorCode::INTERNAL_ERROR);
        }
        SharedMutexLocker lock(&mutex_);
        object_bucket_map_.clear();
        buckets_.clear();
        total_size_ = 0;
        int64_t max_bucket_id = BucketIdGenerator::INIT_NEW_START_ID;
        for (const auto& entry :
             fs::recursive_directory_iterator(storage_path_)) {
            if (entry.is_regular_file() &&
                entry.path().extension() == BUCKET_METADATA_FILE_SUFFIX) {
                auto bucket_id_str = entry.path().stem();
                int64_t bucket_id = std::stoll(bucket_id_str);
                auto [metadata_it, success] = buckets_.try_emplace(
                    bucket_id, std::make_shared<BucketMetadata>());
                if (!success) {
                    LOG(ERROR) << "Failed to load bucket " << bucket_id_str;
                    return tl::unexpected(ErrorCode::BUCKET_ALREADY_EXISTS);
                }
                auto load_bucket_metadata_result =
                    LoadBucketMetadata(bucket_id, metadata_it->second);
                if (!load_bucket_metadata_result) {
                    LOG(ERROR)
                        << "Failed to load metadata for bucket: "
                        << bucket_id_str
                        << ", will delete the bucket's data and metadata";
                    auto bucket_data_path =
                        GetBucketDataPath(bucket_id).value();
                    auto bucket_meta_path =
                        GetBucketMetadataPath(bucket_id).value();
                    fs::remove(bucket_meta_path);
                    fs::remove(bucket_data_path);
                    buckets_.erase(bucket_id);
                    continue;
                }
                auto& meta = *(metadata_it->second);
                if (meta.data_size == 0 || meta.meta_size == 0 ||
                    meta.object_metadata.empty() || meta.keys.empty()) {
                    LOG(ERROR) << "Metadata validation failed for bucket: "
                               << bucket_id_str
                               << ", will delete the bucket's data and "
                                  "metadata. Detailed values:";
                    LOG(ERROR) << "  data_size: " << meta.data_size
                               << " (should not be 0)";
                    LOG(ERROR) << "  meta_size: " << meta.meta_size
                               << " (should not be 0)";
                    LOG(ERROR)
                        << "  object_metadata.size(): "
                        << meta.object_metadata.size() << " (empty: "
                        << (meta.object_metadata.empty() ? "true" : "false")
                        << ")";
                    LOG(ERROR)
                        << "  keys.size(): " << meta.keys.size()
                        << " (empty: " << (meta.keys.empty() ? "true" : "false")
                        << ")";
                    auto bucket_data_path =
                        GetBucketDataPath(bucket_id).value();
                    auto bucket_meta_path =
                        GetBucketMetadataPath(bucket_id).value();
                    fs::remove(bucket_meta_path);
                    fs::remove(bucket_data_path);
                    buckets_.erase(bucket_id);
                    continue;
                }
                if (bucket_id > max_bucket_id) {
                    max_bucket_id = bucket_id;
                }
                total_size_ += metadata_it->second->data_size +
                               metadata_it->second->meta_size;
                for (const auto& object_metadata_it :
                     metadata_it->second->object_metadata) {
                    object_bucket_map_.emplace(
                        object_metadata_it.first,
                        StorageObjectMetadata{
                            metadata_it->first,
                            object_metadata_it.second.offset,
                            object_metadata_it.second.key_size,
                            object_metadata_it.second.data_size});
                }
            }
        }
        bucket_id_generator_.emplace(max_bucket_id);
        if (max_bucket_id == BucketIdGenerator::INIT_NEW_START_ID) {
            LOG(INFO) << "Initialized BucketIdGenerator with fresh start. "
                         "No existing buckets found; starting from ID: "
                      << bucket_id_generator_->CurrentId();
        } else {
            LOG(INFO) << "Initialized BucketIdGenerator from existing state. "
                      << "Last used bucket ID was " << max_bucket_id;
        }
        initialized_.store(true, std::memory_order_release);
    } catch (const std::exception& e) {
        LOG(ERROR) << "Bucket storage backend initialize error: " << e.what()
                   << std::endl;
        return tl::unexpected(ErrorCode::INTERNAL_ERROR);
    }

    return {};
}

tl::expected<bool, ErrorCode> BucketStorageBackend::IsExist(
    const std::string& key) {
    SharedMutexLocker lock(&mutex_, shared_lock);
    auto bucket_id_it = object_bucket_map_.find(key);
    if (bucket_id_it != object_bucket_map_.end()) {
        return true;
    }
    return false;
}

tl::expected<int64_t, ErrorCode> BucketStorageBackend::BucketScan(
    int64_t bucket_id,
    std::unordered_map<std::string, BucketObjectMetadata>& objects,
    std::vector<int64_t>& buckets, size_t limit) {
    SharedMutexLocker lock(&mutex_, shared_lock);
    auto bucket_it = buckets_.lower_bound(bucket_id);
    for (; bucket_it != buckets_.end(); ++bucket_it) {
        if (bucket_it->second->keys.size() > limit) {
            LOG(ERROR) << "Bucket key count exceeds limit: "
                       << "bucket_id=" << bucket_it->first
                       << ", current_size=" << bucket_it->second->keys.size()
                       << ", limit=" << limit;
            return tl::make_unexpected(ErrorCode::KEYS_ULTRA_BUCKET_LIMIT);
        }
        if (bucket_it->second->keys.size() + objects.size() > limit) {
            return bucket_it->first;
        }
        buckets.emplace_back(bucket_it->first);
        for (const auto& object_it : bucket_it->second->object_metadata) {
            objects.emplace(object_it.first, object_it.second);
        }
    }
    return 0;
}

tl::expected<OffloadMetadata, ErrorCode>
BucketStorageBackend::GetStoreMetadata() {
    SharedMutexLocker lock(&mutex_, shared_lock);
    OffloadMetadata metadata{object_bucket_map_.size(), total_size_};
    return metadata;
}

tl::expected<std::shared_ptr<BucketMetadata>, ErrorCode>
BucketStorageBackend::BuildBucket(
    const std::unordered_map<std::string, std::vector<Slice>>& batch_object,
    std::vector<iovec>& iovs) {
    SharedMutexLocker lock(&mutex_);
    auto bucket = std::make_shared<BucketMetadata>();
    size_t storage_offset = 0;
    for (const auto& object : batch_object) {
        if (object.second.empty()) {
            LOG(ERROR) << "Failed to create bucket, object is empty";
            return tl::make_unexpected(ErrorCode::INVALID_KEY);
        }
        size_t object_total_size = 0;
        iovs.emplace_back(
            iovec{const_cast<char*>(object.first.data()), object.first.size()});
        for (const auto& slice : object.second) {
            object_total_size += slice.size;
            iovs.emplace_back(iovec{slice.ptr, slice.size});
        }
        bucket->data_size += object_total_size + object.first.size();
        bucket->object_metadata.emplace(
            object.first,
            BucketObjectMetadata{storage_offset, object.first.size(),
                                 object_total_size});
        bucket->keys.push_back(object.first);
        storage_offset += object_total_size + object.first.size();
    }
    return bucket;
}

tl::expected<void, ErrorCode> BucketStorageBackend::WriteBucket(
    int64_t bucket_id, std::shared_ptr<BucketMetadata> bucket_metadata,
    std::vector<iovec>& iovs) {
    auto bucket_data_path = GetBucketDataPath(bucket_id).value();
    auto open_file_result = OpenFile(bucket_data_path, FileMode::Write);
    if (!open_file_result) {
        LOG(ERROR) << "Failed to open file for bucket writing: "
                   << bucket_data_path;
        return tl::make_unexpected(ErrorCode::FILE_OPEN_FAIL);
    }
    auto file = std::move(open_file_result.value());

    auto write_result = file->vector_write(iovs.data(), iovs.size(), 0);
    if (!write_result) {
        LOG(ERROR) << "vector_write failed for: " << bucket_id
                   << ", error: " << write_result.error();
        return tl::make_unexpected(write_result.error());
    }

    auto store_bucket_metadata_result =
        StoreBucketMetadata(bucket_id, bucket_metadata);
    if (!store_bucket_metadata_result) {
        LOG(ERROR) << "Failed to store bucket metadata, error: "
                   << store_bucket_metadata_result.error();
        return tl::make_unexpected(ErrorCode::FILE_WRITE_FAIL);
    }
    return {};
}

tl::expected<void, ErrorCode> BucketStorageBackend::StoreBucketMetadata(
    int64_t id, std::shared_ptr<BucketMetadata> metadata) {
    auto meta_path = GetBucketMetadataPath(id).value();
    auto open_file_result = OpenFile(meta_path, FileMode::Write);
    if (!open_file_result) {
        LOG(ERROR) << "Failed to open file for bucket writing: " << meta_path;
        return tl::make_unexpected(ErrorCode::FILE_OPEN_FAIL);
    }
    auto file = std::move(open_file_result.value());
    std::string str;
    struct_pb::to_pb(*metadata, str);
    auto write_result = file->write(str, str.size());
    if (!write_result) {
        LOG(ERROR) << "Write failed for: " << meta_path
                   << ", error: " << write_result.error();
        return tl::make_unexpected(write_result.error());
    }
    metadata->meta_size = str.size();
    return {};
}

tl::expected<void, ErrorCode> BucketStorageBackend::LoadBucketMetadata(
    int64_t id, std::shared_ptr<BucketMetadata> metadata) {
    auto meta_path = GetBucketMetadataPath(id).value();
    auto open_file_result = OpenFile(meta_path, FileMode::Read);
    if (!open_file_result) {
        LOG(ERROR) << "Failed to open file for reading: " << meta_path;
        return tl::make_unexpected(open_file_result.error());
    }
    auto file = std::move(open_file_result.value());
    std::string str;
    size_t size = std::filesystem::file_size(meta_path);
    auto read_result = file->read(str, size);
    if (!read_result) {
        LOG(ERROR) << "read failed for: " << meta_path
                   << ", error: " << read_result.error();
        return tl::make_unexpected(read_result.error());
    }
    if (*read_result != size) {
        LOG(ERROR) << "Read size mismatch for: " << meta_path
                   << ", expected: " << size << ", got: " << *read_result;
        return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
    }
    try {
        struct_pb::from_pb(*metadata, str);
        metadata->meta_size = size;
    } catch (const std::exception& e) {
        LOG(ERROR) << "Metadata parsing failed with exception: " << e.what();
        return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
    } catch (...) {
        LOG(ERROR) << "Metadata parsing failed with unknown exception";
        return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
    }
    return {};
}

tl::expected<void, ErrorCode> BucketStorageBackend::BatchLoadBucket(
    int64_t bucket_id, const std::vector<std::string>& keys,
    std::unordered_map<std::string, Slice>& batched_slices) {
    SharedMutexLocker locker(&mutex_, shared_lock);
    auto storage_filepath = GetBucketDataPath(bucket_id).value();
    auto open_file_result = OpenFile(storage_filepath, FileMode::Read);
    if (!open_file_result) {
        LOG(ERROR) << "Failed to open file for reading: " << storage_filepath;
        return tl::make_unexpected(open_file_result.error());
    }
    auto file = std::move(open_file_result.value());
    for (const auto& key : keys) {
        size_t offset;
        auto slice = batched_slices[key];
        auto bucket = buckets_.find(bucket_id);
        if (bucket == buckets_.end()) {
            LOG(ERROR) << "Failed to open file for reading: "
                       << storage_filepath;
            return tl::make_unexpected(ErrorCode::FILE_OPEN_FAIL);
        }
        auto object_metadata = buckets_[bucket_id]->object_metadata.find(key);
        if (object_metadata == buckets_[bucket_id]->object_metadata.end()) {
            LOG(ERROR) << "Failed to open file for reading: "
                       << storage_filepath;
            return tl::make_unexpected(ErrorCode::FILE_OPEN_FAIL);
        }
        if (object_metadata->second.data_size != slice.size) {
            LOG(ERROR) << "Read size mismatch for: " << storage_filepath
                       << ", expected: " << object_metadata->second.data_size
                       << ", got: " << slice.size;
            return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
        }
        offset = object_metadata->second.offset;
        std::vector<iovec> iovs;
        iovs.emplace_back(iovec{slice.ptr, slice.size});
        auto read_result = file->vector_read(
            iovs.data(), static_cast<int>(iovs.size()), offset + key.size());
        if (!read_result) {
            LOG(ERROR) << "vector_read failed for: " << storage_filepath
                       << ", error: " << read_result.error();
            return tl::make_unexpected(read_result.error());
        }
        if (*read_result != slice.size) {
            LOG(ERROR) << "Read size mismatch for: " << storage_filepath
                       << ", expected: " << slice.size
                       << ", got: " << *read_result;
            return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
        }
    }
    return {};
}

tl::expected<std::string, ErrorCode> BucketStorageBackend::GetBucketDataPath(
    int64_t bucket_id) {
    std::string sep =
        storage_path_.empty() || storage_path_.back() == '/' ? "" : "/";
    return storage_path_ + sep + std::to_string(bucket_id);
}

tl::expected<std::string, ErrorCode>
BucketStorageBackend::GetBucketMetadataPath(int64_t bucket_id) {
    auto bucket_data_path = GetBucketDataPath(bucket_id);
    return *bucket_data_path + ".meta";
}

tl::expected<std::unique_ptr<StorageFile>, ErrorCode>
BucketStorageBackend::OpenFile(const std::string& path, FileMode mode) const {
    int flags = O_CLOEXEC;
    int access_mode = 0;
    switch (mode) {
        case FileMode::Read:
            access_mode = O_RDONLY;
            break;
        case FileMode::Write:
            access_mode = O_WRONLY | O_CREAT | O_TRUNC;
            break;
    }

    int fd = open(path.c_str(), flags | access_mode, 0644);
    if (fd < 0) {
        return tl::make_unexpected(ErrorCode::FILE_OPEN_FAIL);
    }
    return std::make_unique<PosixFile>(path, fd);
}

}  // namespace mooncake
