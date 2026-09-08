#pragma once

#include <array>
#include <atomic>
#include <functional>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>

#include "storage_backend.h"

namespace rocksdb {
class DB;
}

namespace mooncake {

struct RocksDBBackendConfig {
    uint64_t min_blob_size{64ULL * 1024};
    uint64_t blob_file_size{256ULL * 1024 * 1024};
    uint32_t blob_direct_write_partitions{1};
    bool enable_blob_files{true};
    bool enable_blob_direct_write{false};
    bool sync_writes{true};

    static RocksDBBackendConfig FromEnvironment();
    bool Validate() const;
};

class RocksDBStorageBackend final : public StorageBackendInterface {
   public:
    explicit RocksDBStorageBackend(const FileStorageConfig& config,
                                   RocksDBBackendConfig backend_config =
                                       RocksDBBackendConfig::FromEnvironment());
    ~RocksDBStorageBackend() override;

    tl::expected<void, ErrorCode> Init() override;
    tl::expected<int64_t, ErrorCode> BatchOffload(
        const std::unordered_map<std::string, std::vector<Slice>>& batch_object,
        std::function<ErrorCode(const std::vector<std::string>& keys,
                                std::vector<StorageObjectMetadata>& metadatas)>
            complete_handler,
        EvictionHandler eviction_handler = nullptr) override;
    tl::expected<void, ErrorCode> BatchLoad(
        std::unordered_map<std::string, Slice>& batched_slices) override;
    tl::expected<bool, ErrorCode> IsExist(const std::string& key) override;
    tl::expected<bool, ErrorCode> IsEnableOffloading() override;
    tl::expected<void, ErrorCode> ScanMeta(
        const std::function<ErrorCode(
            const std::vector<std::string>& keys,
            std::vector<StorageObjectMetadata>& metadatas)>& handler) override;
    void SetTestFailurePredicate(
        std::function<bool(const std::string& key)> predicate) override;
    void RemoveAll() override;

   private:
    tl::expected<void, ErrorCode> OpenDatabase();
    tl::expected<void, ErrorCode> RecoverState();
    static tl::expected<std::string, ErrorCode> ConcatSlices(
        const std::vector<Slice>& slices);

    RocksDBBackendConfig backend_config_;
    mutable std::shared_mutex mutex_;
    mutable std::shared_mutex publication_mutex_;
    std::array<std::mutex, 256> key_mutexes_;
    std::unique_ptr<rocksdb::DB> db_;
    std::function<bool(const std::string& key)> test_failure_predicate_;
    std::atomic<uint64_t> next_version_{1};
    std::atomic<uint64_t> key_count_{0};
    std::atomic<uint64_t> logical_bytes_{0};
    std::atomic<bool> initialized_{false};
};

}  // namespace mooncake
