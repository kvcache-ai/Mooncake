#pragma once

#include <atomic>
#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <thread>

#include "storage/local/log_structured/store.h"
#include "storage_backend.h"

namespace mooncake {

enum class LogStructuredSyncPolicy { kRecord, kBatch, kNone };
enum class LogStructuredCompactionPolicy { kNone, kReclaimOnly, kTiered };

struct LogStructuredBackendConfig {
    uint64_t segment_size_bytes{256ULL * 1024 * 1024};
    LogStructuredSyncPolicy sync_policy{LogStructuredSyncPolicy::kRecord};
    uint64_t checkpoint_interval_records{10000};
    LogStructuredCompactionPolicy compaction_policy{
        LogStructuredCompactionPolicy::kNone};
    uint64_t compaction_interval_ms{1000};
    size_t compaction_fanout{4};
    uint32_t compaction_max_levels{4};
    size_t compaction_max_sources{8};
    uint64_t compaction_max_bytes_per_round{1024ULL * 1024 * 1024};
    uint64_t compaction_max_target_bytes{4ULL * 1024 * 1024 * 1024};
    double compaction_min_reclaim_ratio{0.20};

    static LogStructuredBackendConfig FromEnvironment();
    bool Validate() const;
};

class LogStructuredStorageBackend final : public StorageBackendInterface {
   public:
    explicit LogStructuredStorageBackend(
        const FileStorageConfig& config,
        LogStructuredBackendConfig backend_config =
            LogStructuredBackendConfig::FromEnvironment());
    ~LogStructuredStorageBackend() override;

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
    static ErrorCode ToWriteError(logstructured::StoreError error);
    static ErrorCode ToReadError(logstructured::StoreError error);
    static tl::expected<std::string, ErrorCode> ConcatSlices(
        const std::vector<Slice>& slices);
    void CompactionLoop(std::stop_token stop_token);

    LogStructuredBackendConfig backend_config_;
    mutable std::mutex mutex_;
    std::condition_variable_any compaction_wakeup_;
    std::jthread compaction_thread_;
    uint64_t committed_since_checkpoint_{0};
    std::unique_ptr<logstructured::LogStructuredStore> store_;
    std::function<bool(const std::string& key)> test_failure_predicate_;
    std::atomic<bool> initialized_{false};
};

}  // namespace mooncake
