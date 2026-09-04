#include "storage/local/log_structured/log_structured_backend.h"

#include <algorithm>
#include <chrono>
#include <cerrno>
#include <cstdlib>
#include <cmath>
#include <cstring>
#include <filesystem>
#include <limits>
#include <string_view>

#include <glog/logging.h>

#include "environ.h"
#include "tenant_id.h"

namespace mooncake {
namespace {

double ParseRatio(std::string_view value, double fallback) {
    if (value.empty()) return fallback;
    std::string owned(value);
    char* end = nullptr;
    errno = 0;
    const double parsed = std::strtod(owned.c_str(), &end);
    if (errno != 0 || end != owned.c_str() + owned.size() ||
        !std::isfinite(parsed)) {
        return fallback;
    }
    return parsed;
}

std::pair<std::string, std::string> SplitStorageKey(
    std::string_view storage_key) {
    auto [tenant_id, object_key] = TenantId::ParseScopedKey(storage_key);
    return {tenant_id.value(), std::move(object_key)};
}

}  // namespace

LogStructuredBackendConfig LogStructuredBackendConfig::FromEnvironment() {
    LogStructuredBackendConfig config;
    config.segment_size_bytes = Environ::GetUInt64(
        "MOONCAKE_LOG_SEGMENT_SIZE_BYTES", config.segment_size_bytes);
    config.payload_write_parallelism = static_cast<size_t>(Environ::GetUInt64(
        "MOONCAKE_LOG_WRITE_PARALLELISM", config.payload_write_parallelism));
    config.checkpoint_interval_records = Environ::GetUInt64(
        "MOONCAKE_LOG_CHECKPOINT_INTERVAL", config.checkpoint_interval_records);
    config.compaction_interval_ms = Environ::GetUInt64(
        "MOONCAKE_LOG_COMPACTION_INTERVAL_MS", config.compaction_interval_ms);
    config.compaction_fanout = static_cast<size_t>(Environ::GetUInt64(
        "MOONCAKE_LOG_COMPACTION_FANOUT", config.compaction_fanout));
    config.compaction_max_levels = static_cast<uint32_t>(Environ::GetUInt64(
        "MOONCAKE_LOG_COMPACTION_MAX_LEVELS", config.compaction_max_levels));
    config.compaction_max_sources = static_cast<size_t>(Environ::GetUInt64(
        "MOONCAKE_LOG_COMPACTION_MAX_SOURCES", config.compaction_max_sources));
    config.compaction_max_input_bytes =
        Environ::GetUInt64("MOONCAKE_LOG_COMPACTION_MAX_INPUT_BYTES",
                           config.compaction_max_input_bytes);
    config.compaction_max_target_bytes =
        Environ::GetUInt64("MOONCAKE_LOG_COMPACTION_MAX_TARGET_BYTES",
                           config.compaction_max_target_bytes);
    config.compaction_max_bytes_per_second =
        Environ::GetUInt64("MOONCAKE_LOG_COMPACTION_MAX_BYTES_PER_SEC",
                           config.compaction_max_bytes_per_second);
    config.compaction_reserve_bytes =
        Environ::GetUInt64("MOONCAKE_LOG_COMPACTION_RESERVE_BYTES",
                           config.compaction_reserve_bytes);
    config.compaction_min_reclaim_ratio = ParseRatio(
        Environ::GetString("MOONCAKE_LOG_COMPACTION_MIN_RECLAIM_RATIO", ""),
        config.compaction_min_reclaim_ratio);

    const auto sync_policy =
        Environ::GetString("MOONCAKE_LOG_SYNC_POLICY", "record");
    if (sync_policy == "batch") {
        config.sync_policy = LogStructuredSyncPolicy::kBatch;
    } else if (sync_policy == "none") {
        config.sync_policy = LogStructuredSyncPolicy::kNone;
    } else {
        config.sync_policy = LogStructuredSyncPolicy::kRecord;
    }

    const auto compaction_policy =
        Environ::GetString("MOONCAKE_LOG_COMPACTION_POLICY", "none");
    if (compaction_policy == "reclaim_only") {
        config.compaction_policy = LogStructuredCompactionPolicy::kReclaimOnly;
    } else if (compaction_policy == "tiered") {
        config.compaction_policy = LogStructuredCompactionPolicy::kTiered;
    } else {
        config.compaction_policy = LogStructuredCompactionPolicy::kNone;
    }
    return config;
}

bool LogStructuredBackendConfig::Validate() const {
    return segment_size_bytes > 0 && payload_write_parallelism > 0 &&
           compaction_interval_ms > 0 && compaction_fanout >= 2 &&
           compaction_max_levels > 0 && compaction_max_sources > 0 &&
           compaction_max_input_bytes > 0 &&
           compaction_max_target_bytes >= segment_size_bytes &&
           compaction_min_reclaim_ratio >= 0.0 &&
           compaction_min_reclaim_ratio <= 1.0;
}

LogStructuredStorageBackend::LogStructuredStorageBackend(
    const FileStorageConfig& config, LogStructuredBackendConfig backend_config)
    : StorageBackendInterface(config),
      backend_config_(std::move(backend_config)) {}

LogStructuredStorageBackend::~LogStructuredStorageBackend() {
    if (compaction_thread_.joinable()) {
        compaction_thread_.request_stop();
        compaction_wakeup_.notify_all();
        compaction_thread_.join();
    }
}

ErrorCode LogStructuredStorageBackend::ToWriteError(
    logstructured::StoreError error) {
    if (error == logstructured::StoreError::kInvalidArgument) {
        return ErrorCode::INVALID_PARAMS;
    }
    if (error == logstructured::StoreError::kNotFound) {
        return ErrorCode::OBJECT_NOT_FOUND;
    }
    if (error == logstructured::StoreError::kNoSpace) {
        return ErrorCode::KEYS_ULTRA_LIMIT;
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
    if (!backend_config_.Validate()) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    uint64_t max_physical_bytes = std::numeric_limits<uint64_t>::max();
    if (file_storage_config_.total_size_limit > 0) {
        const uint64_t capacity =
            static_cast<uint64_t>(file_storage_config_.total_size_limit);
        if (backend_config_.compaction_reserve_bytes >= capacity) {
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
        max_physical_bytes =
            capacity - backend_config_.compaction_reserve_bytes;
    }
    const std::filesystem::path root =
        std::filesystem::path(file_storage_config_.storage_filepath) /
        "log_structured";
    auto store = logstructured::LogStructuredStore::Open(
        {.root_path = root.string(),
         .max_segment_bytes = backend_config_.segment_size_bytes,
         .max_physical_bytes = max_physical_bytes,
         .max_total_physical_bytes =
             file_storage_config_.total_size_limit > 0
                 ? static_cast<uint64_t>(file_storage_config_.total_size_limit)
                 : std::numeric_limits<uint64_t>::max(),
         .sync_data =
             backend_config_.sync_policy == LogStructuredSyncPolicy::kRecord,
         .sync_wal =
             backend_config_.sync_policy == LogStructuredSyncPolicy::kRecord,
         .payload_write_parallelism =
             backend_config_.payload_write_parallelism});
    if (!store) {
        LOG(ERROR) << "Failed to initialize log-structured backend at " << root;
        return tl::make_unexpected(ToWriteError(store.error()));
    }
    store_ = std::move(store.value());
    initialized_.store(true, std::memory_order_release);
    if (backend_config_.compaction_policy !=
        LogStructuredCompactionPolicy::kNone) {
        compaction_thread_ = std::jthread(
            [this](std::stop_token token) { CompactionLoop(token); });
    }
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
    std::optional<ErrorCode> first_prepare_error;
    std::vector<std::string> keys;
    std::vector<std::string> values;
    std::vector<StorageObjectMetadata> metadatas;
    keys.reserve(batch_object.size());
    values.reserve(batch_object.size());
    metadatas.reserve(batch_object.size());
    for (const auto& [key, slices] : batch_object) {
        if (test_failure_predicate_ && test_failure_predicate_(key)) {
            first_prepare_error = ErrorCode::FILE_WRITE_FAIL;
            continue;
        }
        auto value = ConcatSlices(slices);
        if (!value) {
            first_prepare_error = value.error();
            continue;
        }
        keys.push_back(key);
        values.push_back(std::move(value.value()));
    }
    if (keys.empty()) {
        return tl::make_unexpected(
            first_prepare_error.value_or(ErrorCode::FILE_WRITE_FAIL));
    }

    std::vector<logstructured::PutRequest> requests;
    requests.reserve(keys.size());
    for (size_t index = 0; index < keys.size(); ++index) {
        auto [tenant_id, object_key] = SplitStorageKey(keys[index]);
        requests.push_back({.tenant_id = std::move(tenant_id),
                            .object_key = std::move(object_key),
                            .value = values[index]});
    }
    auto batch = store_->PreparePutBatch(requests);
    if (!batch) {
        return tl::make_unexpected(ToWriteError(batch.error()));
    }
    prepared = std::move(batch.value());
    for (size_t index = 0; index < prepared.size(); ++index) {
        metadatas.push_back(StorageObjectMetadata{
            -1, 0, static_cast<int64_t>(keys[index].size()),
            static_cast<int64_t>(prepared[index].physical.value_length), ""});
    }
    if (backend_config_.sync_policy == LogStructuredSyncPolicy::kBatch &&
        !store_->Sync()) {
        static_cast<void>(store_->AbortPuts(prepared));
        return tl::make_unexpected(ErrorCode::FILE_WRITE_FAIL);
    }

    if (complete_handler) {
        const auto result = complete_handler(keys, metadatas);
        if (result != ErrorCode::OK) {
            static_cast<void>(store_->AbortPuts(prepared));
            return tl::make_unexpected(result);
        }
    }
    auto committed = store_->CommitPuts(prepared);
    if (!committed) {
        return tl::make_unexpected(ToWriteError(committed.error()));
    }
    if (backend_config_.sync_policy == LogStructuredSyncPolicy::kBatch &&
        !store_->Sync()) {
        return tl::make_unexpected(ErrorCode::FILE_WRITE_FAIL);
    }
    committed_since_checkpoint_ += prepared.size();
    if (backend_config_.checkpoint_interval_records != 0 &&
        committed_since_checkpoint_ >=
            backend_config_.checkpoint_interval_records) {
        auto checkpointed = store_->Checkpoint();
        if (!checkpointed) {
            LOG(ERROR) << "Failed to checkpoint log-structured backend";
        } else {
            committed_since_checkpoint_ = 0;
        }
    }
    return static_cast<int64_t>(prepared.size());
}

tl::expected<void, ErrorCode> LogStructuredStorageBackend::BatchLoad(
    std::unordered_map<std::string, Slice>& batched_slices) {
    if (!initialized_.load(std::memory_order_acquire) || !store_) {
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }
    for (const auto& [key, slice] : batched_slices) {
        if (slice.size != 0 && slice.ptr == nullptr) {
            return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
        }
        const auto [tenant_id, object_key] = SplitStorageKey(key);
        auto read = store_->GetLatestInto(
            tenant_id, object_key, static_cast<char*>(slice.ptr), slice.size);
        if (!read) return tl::make_unexpected(ToReadError(read.error()));
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
    if (file_storage_config_.total_keys_limit <= 0 ||
        file_storage_config_.total_size_limit <= 0) {
        return false;
    }
    const auto entries = store_->SnapshotCurrentIndex();
    const auto stats = store_->SnapshotStats();
    const uint64_t capacity =
        static_cast<uint64_t>(file_storage_config_.total_size_limit);
    if (backend_config_.compaction_reserve_bytes >= capacity) return false;
    const uint64_t usable_capacity =
        capacity - backend_config_.compaction_reserve_bytes;
    return entries.size() <
               static_cast<size_t>(file_storage_config_.total_keys_limit) &&
           stats.physical_bytes < usable_capacity;
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

logstructured::CompactionOptions
LogStructuredStorageBackend::MakeCompactionOptions(
    std::stop_token stop_token) const {
    uint64_t max_temporary_bytes = std::numeric_limits<uint64_t>::max();
    if (store_ && file_storage_config_.total_size_limit > 0) {
        const uint64_t capacity =
            static_cast<uint64_t>(file_storage_config_.total_size_limit);
        const uint64_t physical_bytes = store_->SnapshotStats().physical_bytes;
        max_temporary_bytes =
            physical_bytes < capacity ? capacity - physical_bytes : 0;
    }
    return {
        .max_source_segments = backend_config_.compaction_max_sources,
        .max_input_bytes = backend_config_.compaction_max_input_bytes,
        .max_target_bytes = backend_config_.compaction_max_target_bytes,
        .max_temporary_bytes = max_temporary_bytes,
        .fanout = backend_config_.compaction_fanout,
        .max_levels = backend_config_.compaction_max_levels,
        .min_reclaim_ratio = backend_config_.compaction_min_reclaim_ratio,
        .max_bytes_per_second = backend_config_.compaction_max_bytes_per_second,
        .enable_tiering = backend_config_.compaction_policy ==
                          LogStructuredCompactionPolicy::kTiered,
        .stop_token = stop_token};
}

tl::expected<logstructured::CompactionResult, logstructured::StoreError>
LogStructuredStorageBackend::RunCompaction(
    const logstructured::CompactionOptions& options) {
    const auto started = std::chrono::steady_clock::now();
    auto result = store_->CompactOnce(options);
    const auto duration_us =
        std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now() - started)
            .count();
    compaction_last_duration_us_.store(static_cast<uint64_t>(duration_us),
                                       std::memory_order_relaxed);
    if (!result) {
        if (result.error() == logstructured::StoreError::kCancelled) {
            compaction_cancellations_.fetch_add(1, std::memory_order_relaxed);
        } else if (result.error() ==
                   logstructured::StoreError::kInvalidTransition) {
            compaction_conflicts_.fetch_add(1, std::memory_order_relaxed);
        } else {
            compaction_errors_.fetch_add(1, std::memory_order_relaxed);
        }
        return result;
    }
    if (result->source_segments != 0) {
        compaction_runs_.fetch_add(1, std::memory_order_relaxed);
        compaction_input_bytes_.fetch_add(result->input_bytes,
                                          std::memory_order_relaxed);
        compaction_output_bytes_.fetch_add(result->output_bytes,
                                           std::memory_order_relaxed);
        compaction_reclaimed_bytes_.fetch_add(result->reclaimed_bytes,
                                              std::memory_order_relaxed);
    }
    return result;
}

void LogStructuredStorageBackend::CompactionLoop(std::stop_token stop_token) {
    std::mutex wait_mutex;
    std::unique_lock wait_lock(wait_mutex);
    while (!stop_token.stop_requested()) {
        compaction_wakeup_.wait_for(
            wait_lock, stop_token,
            std::chrono::milliseconds(backend_config_.compaction_interval_ms),
            [] { return false; });
        if (stop_token.stop_requested()) break;
        auto compacted = RunCompaction(MakeCompactionOptions(stop_token));
        if (!compacted &&
            compacted.error() != logstructured::StoreError::kCancelled &&
            compacted.error() !=
                logstructured::StoreError::kInvalidTransition) {
            LOG(ERROR) << "Log-structured compaction failed";
        }
    }
}

tl::expected<std::vector<std::string>, ErrorCode>
LogStructuredStorageBackend::EvictAboveDiskWatermark(
    double high_watermark_ratio, double low_watermark_ratio,
    EvictionHandler eviction_handler) {
    static_cast<void>(eviction_handler);
    if (!std::isfinite(high_watermark_ratio) ||
        !std::isfinite(low_watermark_ratio) || low_watermark_ratio < 0.0 ||
        high_watermark_ratio > 1.0 ||
        low_watermark_ratio > high_watermark_ratio) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (!initialized_.load(std::memory_order_acquire) || !store_) {
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }
    if (file_storage_config_.total_size_limit <= 0) {
        return std::vector<std::string>{};
    }

    const uint64_t capacity =
        static_cast<uint64_t>(file_storage_config_.total_size_limit);
    if (backend_config_.compaction_reserve_bytes >= capacity) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    const uint64_t usable_capacity =
        capacity - backend_config_.compaction_reserve_bytes;
    const uint64_t high_watermark_bytes = static_cast<uint64_t>(
        static_cast<double>(usable_capacity) * high_watermark_ratio);
    const uint64_t low_watermark_bytes = static_cast<uint64_t>(
        static_cast<double>(usable_capacity) * low_watermark_ratio);

    auto stats = store_->SnapshotStats();
    if (stats.physical_bytes <= high_watermark_bytes) {
        return std::vector<std::string>{};
    }
    if (stats.reclaimable_bytes == 0) {
        return std::vector<std::string>{};
    }
    auto sealed = store_->SealActiveSegment();
    if (!sealed) return tl::make_unexpected(ToWriteError(sealed.error()));

    auto options = MakeCompactionOptions();
    options.min_reclaim_ratio = 0.0;
    options.enable_tiering = false;
    while (stats.physical_bytes > low_watermark_bytes &&
           stats.reclaimable_bytes != 0) {
        auto compacted = RunCompaction(options);
        if (!compacted) {
            return tl::make_unexpected(ToWriteError(compacted.error()));
        }
        if (compacted->source_segments == 0 ||
            compacted->reclaimed_bytes == 0) {
            break;
        }
        stats = store_->SnapshotStats();
    }
    return std::vector<std::string>{};
}

void LogStructuredStorageBackend::RemoveAll() {
    std::lock_guard lock(mutex_);
    if (!initialized_.load(std::memory_order_acquire) || !store_) return;

    for (const auto& entry : store_->SnapshotCurrentIndex()) {
        auto deleted = store_->Delete(entry.identity);
        if (!deleted) {
            LOG(ERROR) << "Failed to tombstone object during RemoveAll: "
                       << entry.identity.object_key;
            return;
        }
    }
    if (!store_->SealActiveSegment()) {
        LOG(ERROR) << "Failed to seal active segment during RemoveAll";
        return;
    }

    const logstructured::CompactionOptions options{
        .max_source_segments = std::numeric_limits<size_t>::max(),
        .max_input_bytes = std::numeric_limits<uint64_t>::max(),
        .max_target_bytes = backend_config_.compaction_max_target_bytes,
        .max_temporary_bytes = std::numeric_limits<uint64_t>::max(),
        .fanout = backend_config_.compaction_fanout,
        .max_levels = backend_config_.compaction_max_levels,
        .min_reclaim_ratio = 0.0,
        .enable_tiering = false,
        .stop_token = {}};
    while (true) {
        auto compacted = RunCompaction(options);
        if (!compacted) {
            LOG(ERROR) << "Failed to reclaim segments during RemoveAll";
            return;
        }
        if (compacted->source_segments == 0) break;
    }
    if (!store_->Checkpoint()) {
        LOG(ERROR) << "Failed to checkpoint RemoveAll";
    }
}

std::optional<StorageBackendStats> LogStructuredStorageBackend::SnapshotStats()
    const {
    if (!initialized_.load(std::memory_order_acquire)) {
        return std::nullopt;
    }
    std::lock_guard lock(mutex_);
    if (!store_) return std::nullopt;
    const auto stats = store_->SnapshotStats();
    return StorageBackendStats{
        .physical_bytes = stats.physical_bytes,
        .live_record_bytes = stats.live_record_bytes,
        .logical_value_bytes = stats.logical_value_bytes,
        .reclaimable_bytes = stats.reclaimable_bytes,
        .active_segments = stats.active_segments,
        .sealed_segments = stats.sealed_segments,
        .retired_segments = stats.retired_segments,
        .compaction_runs = compaction_runs_.load(std::memory_order_relaxed),
        .compaction_input_bytes =
            compaction_input_bytes_.load(std::memory_order_relaxed),
        .compaction_output_bytes =
            compaction_output_bytes_.load(std::memory_order_relaxed),
        .compaction_reclaimed_bytes =
            compaction_reclaimed_bytes_.load(std::memory_order_relaxed),
        .compaction_conflicts =
            compaction_conflicts_.load(std::memory_order_relaxed),
        .compaction_cancellations =
            compaction_cancellations_.load(std::memory_order_relaxed),
        .compaction_errors = compaction_errors_.load(std::memory_order_relaxed),
        .compaction_last_duration_us =
            compaction_last_duration_us_.load(std::memory_order_relaxed),
        .wal_sequence = stats.wal_sequence,
        .checkpoint_sequence = stats.checkpoint_sequence};
}

void LogStructuredStorageBackend::SetTestFailurePredicate(
    std::function<bool(const std::string& key)> predicate) {
    std::lock_guard lock(mutex_);
    test_failure_predicate_ = std::move(predicate);
}

}  // namespace mooncake
