#pragma once

#include <atomic>
#include <functional>
#include <memory>
#include <mutex>

#include "storage/local/log_structured/store.h"
#include "storage_backend.h"

namespace mooncake {

class LogStructuredStorageBackend final : public StorageBackendInterface {
   public:
    explicit LogStructuredStorageBackend(const FileStorageConfig& config);

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

   private:
    static ErrorCode ToWriteError(logstructured::StoreError error);
    static ErrorCode ToReadError(logstructured::StoreError error);
    static tl::expected<std::string, ErrorCode> ConcatSlices(
        const std::vector<Slice>& slices);

    mutable std::mutex mutex_;
    std::unique_ptr<logstructured::LogStructuredStore> store_;
    std::function<bool(const std::string& key)> test_failure_predicate_;
    std::atomic<bool> initialized_{false};
};

}  // namespace mooncake
