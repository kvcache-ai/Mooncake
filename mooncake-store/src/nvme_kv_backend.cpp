#include "nvme_kv_backend.h"

#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <exception>
#include <latch>
#include <limits>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "nvme_kv_executor_util.h"
#include "nvme_kv_key_codec.h"
#include "nvme_kv_key_conflict_policy.h"
#include "nvme_kv_object_layout.h"

namespace mooncake {

namespace {

constexpr uint32_t kMinMaxValueSize = sizeof(NvmeKvObjectHeader) + 1;
constexpr size_t kDefaultMaxIoConcurrency = 256;
constexpr size_t kDefaultIoConcurrency = 18;
constexpr size_t kDefaultPrepareConcurrency = 12;
constexpr size_t kDefaultBatchSubmitConcurrency = 6;
constexpr size_t kDefaultRootSubmitConcurrency = 1;
constexpr size_t kDefaultReadPlanBatchSize = 8;

size_t PositiveSizeEnvOr(const char *name, size_t fallback, size_t maximum) {
    const uint32_t parsed = ParseNvmeKvU32EnvOr(name, 0);
    return parsed == 0 ? fallback : std::min<size_t>(parsed, maximum);
}

size_t MaxIoConcurrency() {
    return PositiveSizeEnvOr("MOONCAKE_NVME_KV_MAX_IO_CONCURRENCY",
                             kDefaultMaxIoConcurrency, UINT32_MAX);
}

std::optional<size_t> ConfiguredIoConcurrency(size_t max_io_concurrency) {
    const char *configured = std::getenv("MOONCAKE_NVME_KV_IO_CONCURRENCY");
    if (configured == nullptr || configured[0] == '\0') {
        return std::nullopt;
    }
    const size_t parsed = PositiveSizeEnvOr("MOONCAKE_NVME_KV_IO_CONCURRENCY",
                                            0, max_io_concurrency);
    return parsed == 0 ? std::nullopt : std::optional<size_t>(parsed);
}

class IndexQueue {
   public:
    bool Push(size_t index) {
        std::lock_guard<std::mutex> lock(mutex_);
        if (closed_) {
            return false;
        }
        queue_.push_back(index);
        not_empty_.notify_one();
        return true;
    }

    bool Pop(size_t &index) {
        std::unique_lock<std::mutex> lock(mutex_);
        not_empty_.wait(lock, [&]() { return closed_ || !queue_.empty(); });
        if (queue_.empty()) {
            return false;
        }
        index = queue_.front();
        queue_.pop_front();
        return true;
    }

    void Close() {
        std::lock_guard<std::mutex> lock(mutex_);
        closed_ = true;
        not_empty_.notify_all();
    }

   private:
    std::mutex mutex_;
    std::condition_variable not_empty_;
    std::deque<size_t> queue_;
    bool closed_ = false;
};

bool CanRetrieveDirectlyInto(const char *data, uint32_t size) {
    if (data == nullptr || size == 0) {
        return false;
    }
    const uint32_t alignment = NvmeKvTransferAlignmentBytes();
    return alignment != 0 && size % alignment == 0 &&
           reinterpret_cast<uintptr_t>(data) % alignment == 0;
}

std::string_view BuildPayloadView(const std::vector<Slice> &slices,
                                  size_t payload_size, std::string &storage) {
    if (slices.size() == 1) {
        const auto &slice = slices.front();
        return slice.size == 0
                   ? std::string_view{}
                   : std::string_view(reinterpret_cast<const char *>(slice.ptr),
                                      slice.size);
    }
    storage.reserve(payload_size);
    for (const auto &slice : slices) {
        storage.append(reinterpret_cast<const char *>(slice.ptr), slice.size);
    }
    return storage;
}

size_t ReadPlanBatchSize() {
    return PositiveSizeEnvOr("MOONCAKE_NVME_KV_READ_PLAN_BATCH_SIZE",
                             kDefaultReadPlanBatchSize, 1024);
}

std::optional<std::vector<size_t>> ValidateChunkRecords(
    const NvmeKvObjectIdentity &identity, uint32_t slot,
    size_t expected_payload_size,
    const std::vector<NvmeKvManifestChunkRecord> &records) {
    if (records.empty()) {
        return std::nullopt;
    }
    std::vector<size_t> offsets;
    offsets.reserve(records.size());
    size_t offset = 0;
    for (size_t index = 0; index < records.size(); ++index) {
        const auto &record = records[index];
        if (record.payload_size == 0 || offset > expected_payload_size ||
            record.payload_size > expected_payload_size - offset ||
            record.physical_key !=
                EncodeNvmeKvChunkPhysicalKey(
                    identity, static_cast<uint32_t>(index), slot)) {
            return std::nullopt;
        }
        offsets.push_back(offset);
        offset += record.payload_size;
    }
    return offset == expected_payload_size
               ? std::optional<std::vector<size_t>>(std::move(offsets))
               : std::nullopt;
}

bool ManifestChunkRecordsEqual(
    const std::vector<NvmeKvManifestChunkRecord> &lhs,
    const std::vector<NvmeKvManifestChunkRecord> &rhs) {
    if (lhs.size() != rhs.size()) {
        return false;
    }
    for (size_t index = 0; index < lhs.size(); ++index) {
        if (lhs[index].physical_key != rhs[index].physical_key ||
            lhs[index].payload_size != rhs[index].payload_size ||
            lhs[index].payload_checksum != rhs[index].payload_checksum) {
            return false;
        }
    }
    return true;
}

struct ResolvedRoot {
    NvmeKvPhysicalKey physical_key{};
    uint32_t slot = 0;
    std::string object_blob;
};

tl::expected<std::optional<ResolvedRoot>, ErrorCode> ResolveRoot(
    NvmeKvConnector &connector, const std::string &logical_key) {
    const NvmeKvObjectIdentity identity{.logical_key = logical_key};
    for (uint32_t slot = 0; slot < kNvmeKvMaxPhysicalKeySlots; ++slot) {
        const auto physical_key = EncodeNvmeKvPhysicalKey(identity, slot);
        auto object_res = connector.Retrieve(physical_key);
        if (!object_res) {
            if (object_res.error() == ErrorCode::OBJECT_NOT_FOUND) {
                continue;
            }
            return tl::make_unexpected(object_res.error());
        }

        NvmeKvObjectHeader header{};
        std::string_view identity_metadata;
        std::string_view payload;
        if (!ParseNvmeKvObjectBlob(object_res.value(), header,
                                   identity_metadata, payload)) {
            return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
        }

        NvmeKvStoredIdentityView stored_identity{};
        if (!ParseNvmeKvStoredIdentity(identity_metadata, stored_identity)) {
            return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
        }
        if (stored_identity.logical_key != logical_key) {
            continue;
        }
        if (stored_identity.resolved_slot != slot ||
            !NvmeKvKeyConflictPolicy::ValidateResolvedRootPlacement(
                identity, stored_identity, physical_key)) {
            return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
        }
        return std::optional<ResolvedRoot>(
            ResolvedRoot{physical_key, slot, std::move(object_res.value())});
    }
    return std::nullopt;
}

}  // namespace

NvmeKvStorageBackend::NvmeKvStorageBackend(
    const FileStorageConfig &file_storage_config_)
    : StorageBackendInterface(file_storage_config_) {}

void NvmeKvStorageBackend::InitIoWorkers() {
    const size_t max_io_concurrency = MaxIoConcurrency();
    if (auto configured = ConfiguredIoConcurrency(max_io_concurrency);
        configured.has_value()) {
        io_parallelism_ = *configured;
    } else {
        const size_t queue_depth =
            std::max<size_t>(1, connector_->GetCapabilities().queue_depth);
        io_parallelism_ = std::min(
            queue_depth, std::min(kDefaultIoConcurrency, max_io_concurrency));
    }
    if (io_parallelism_ > 1) {
        io_workers_ = std::make_unique<ThreadPool>(io_parallelism_);
    }
    const size_t max_submit_concurrency =
        io_parallelism_ > 1 ? io_parallelism_ - 1 : 1;
    batch_submit_concurrency_ = PositiveSizeEnvOr(
        "MOONCAKE_NVME_KV_BATCH_SUBMIT_CONCURRENCY",
        std::min(kDefaultBatchSubmitConcurrency, max_submit_concurrency),
        max_submit_concurrency);
    root_submit_concurrency_ = PositiveSizeEnvOr(
        "MOONCAKE_NVME_KV_ROOT_SUBMIT_CONCURRENCY",
        std::min(kDefaultRootSubmitConcurrency, max_submit_concurrency),
        max_submit_concurrency);
    const size_t max_prepare_concurrency =
        io_parallelism_ > batch_submit_concurrency_
            ? io_parallelism_ - batch_submit_concurrency_
            : 1;
    prepare_concurrency_ = PositiveSizeEnvOr(
        "MOONCAKE_NVME_KV_PREPARE_CONCURRENCY",
        std::min(kDefaultPrepareConcurrency, max_prepare_concurrency),
        max_prepare_concurrency);
    if (batch_submit_concurrency_ > 1) {
        submit_workers_ =
            std::make_unique<ThreadPool>(batch_submit_concurrency_);
        root_submit_workers_ =
            std::make_unique<ThreadPool>(root_submit_concurrency_);
    }
    LOG(INFO) << "NVMe KV backend I/O concurrency: " << io_parallelism_
              << " (max " << max_io_concurrency
              << "), batch submit concurrency: " << batch_submit_concurrency_
              << ", root submit concurrency: " << root_submit_concurrency_
              << ", prepare concurrency: " << prepare_concurrency_;
}

void NvmeKvStorageBackend::RunParallelIo(
    size_t item_count, const std::function<void(size_t)> &task,
    size_t max_inflight) {
    if (item_count == 0) {
        return;
    }
    const size_t concurrency_limit =
        max_inflight == 0 ? io_parallelism_
                          : std::min(io_parallelism_, max_inflight);
    const size_t worker_count = std::min(item_count, concurrency_limit);
    if (worker_count <= 1 || io_workers_ == nullptr) {
        for (size_t i = 0; i < item_count; ++i) {
            task(i);
        }
        return;
    }

    std::atomic<size_t> next_index{0};
    std::latch completed(static_cast<std::ptrdiff_t>(worker_count));
    std::mutex exception_mutex;
    std::exception_ptr first_exception;
    std::exception_ptr enqueue_exception;

    size_t enqueued = 0;
    try {
        for (; enqueued < worker_count; ++enqueued) {
            io_workers_->enqueue([&]() {
                try {
                    while (true) {
                        const size_t index =
                            next_index.fetch_add(1, std::memory_order_relaxed);
                        if (index >= item_count) {
                            break;
                        }
                        task(index);
                    }
                } catch (...) {
                    std::lock_guard<std::mutex> lock(exception_mutex);
                    if (first_exception == nullptr) {
                        first_exception = std::current_exception();
                    }
                }
                completed.count_down();
            });
        }
    } catch (...) {
        enqueue_exception = std::current_exception();
        completed.count_down(
            static_cast<std::ptrdiff_t>(worker_count - enqueued));
    }

    completed.wait();
    if (enqueue_exception != nullptr) {
        std::rethrow_exception(enqueue_exception);
    }
    if (first_exception != nullptr) {
        std::rethrow_exception(first_exception);
    }
}

void NvmeKvStorageBackend::RunPipelinedIo(
    size_t item_count, const std::function<void(size_t)> &io_task,
    const std::function<void(size_t)> &completion_task) {
    if (item_count == 0) {
        return;
    }
    const size_t worker_count = std::min(item_count, batch_submit_concurrency_);
    if (worker_count <= 1 || submit_workers_ == nullptr ||
        io_workers_ == nullptr) {
        for (size_t index = 0; index < item_count; ++index) {
            io_task(index);
            completion_task(index);
        }
        return;
    }

    std::atomic<size_t> next_index{0};
    std::vector<std::exception_ptr> item_exceptions(item_count);
    std::latch items_completed(static_cast<std::ptrdiff_t>(item_count));
    std::latch workers_completed(static_cast<std::ptrdiff_t>(worker_count));
    std::exception_ptr enqueue_exception;
    size_t enqueued_workers = 0;
    try {
        for (; enqueued_workers < worker_count; ++enqueued_workers) {
            submit_workers_->enqueue([&]() {
                while (true) {
                    const size_t index =
                        next_index.fetch_add(1, std::memory_order_relaxed);
                    if (index >= item_count) {
                        break;
                    }
                    try {
                        io_task(index);
                    } catch (...) {
                        item_exceptions[index] = std::current_exception();
                        items_completed.count_down();
                        continue;
                    }
                    try {
                        io_workers_->enqueue([&, index]() {
                            try {
                                completion_task(index);
                            } catch (...) {
                                item_exceptions[index] =
                                    std::current_exception();
                            }
                            items_completed.count_down();
                        });
                    } catch (...) {
                        item_exceptions[index] = std::current_exception();
                        items_completed.count_down();
                    }
                }
                workers_completed.count_down();
            });
        }
    } catch (...) {
        enqueue_exception = std::current_exception();
        workers_completed.count_down(
            static_cast<std::ptrdiff_t>(worker_count - enqueued_workers));
    }

    if (enqueued_workers == 0) {
        std::rethrow_exception(enqueue_exception);
    }

    items_completed.wait();
    workers_completed.wait();
    if (enqueue_exception != nullptr) {
        std::rethrow_exception(enqueue_exception);
    }
    for (const auto &item_exception : item_exceptions) {
        if (item_exception != nullptr) {
            std::rethrow_exception(item_exception);
        }
    }
}

std::shared_ptr<const NvmeKvStorageBackend::CachedManifest>
NvmeKvStorageBackend::FindCachedManifest(const std::string &key,
                                         size_t payload_size) const {
    std::shared_lock<std::shared_mutex> lock(manifest_cache_mutex_);
    const auto it = manifest_cache_.find(key);
    if (it == manifest_cache_.end() ||
        it->second->payload_size != payload_size) {
        return nullptr;
    }
    return it->second;
}

std::shared_ptr<const NvmeKvStorageBackend::CachedManifest>
NvmeKvStorageBackend::CacheManifest(
    const std::string &key, size_t payload_size, uint32_t resolved_slot,
    const std::vector<NvmeKvManifestChunkRecord> &chunk_records) {
    if (payload_size > std::numeric_limits<uint32_t>::max()) {
        return nullptr;
    }
    const NvmeKvObjectIdentity identity{.logical_key = key};
    auto chunk_offsets = ValidateChunkRecords(identity, resolved_slot,
                                              payload_size, chunk_records);
    if (!chunk_offsets) {
        return nullptr;
    }
    auto manifest = std::make_shared<const CachedManifest>(CachedManifest{
        .resolved_slot = resolved_slot,
        .payload_size = static_cast<uint32_t>(payload_size),
        .chunk_records = chunk_records,
        .chunk_offsets = std::move(*chunk_offsets),
    });
    std::unique_lock<std::shared_mutex> lock(manifest_cache_mutex_);
    auto [it, inserted] = manifest_cache_.try_emplace(key, manifest);
    if (inserted) {
        return manifest;
    }
    if (it->second->payload_size == payload_size &&
        it->second->resolved_slot == resolved_slot &&
        ManifestChunkRecordsEqual(it->second->chunk_records, chunk_records)) {
        return it->second;
    }
    it->second = manifest;
    return manifest;
}

void NvmeKvStorageBackend::CacheManifestAfterWrite(
    const std::string &key, size_t payload_size, uint32_t resolved_slot,
    const std::vector<NvmeKvManifestChunkRecord> &chunk_records) noexcept {
    try {
        (void)CacheManifest(key, payload_size, resolved_slot, chunk_records);
    } catch (...) {
        LOG(WARNING) << "NVMe KV could not cache manifest for " << key;
    }
}

void NvmeKvStorageBackend::StoreBatchParallel(
    std::vector<NvmeKvCommandExecutor::StoreRequest> &requests) {
    if (requests.empty()) {
        return;
    }
    auto connector = connector_;
    if (connector == nullptr) {
        for (auto &request : requests) {
            request.result = tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
        }
        return;
    }

    const size_t worker_count =
        std::min(batch_submit_concurrency_, requests.size());
    RunParallelIo(
        worker_count,
        [&](size_t worker_index) {
            std::vector<size_t> request_indices;
            std::vector<NvmeKvCommandExecutor::StoreRequest> worker_requests;
            const size_t worker_request_count =
                (requests.size() + worker_count - 1 - worker_index) /
                worker_count;
            request_indices.reserve(worker_request_count);
            worker_requests.reserve(worker_request_count);
            for (size_t request_index = worker_index;
                 request_index < requests.size();
                 request_index += worker_count) {
                request_indices.push_back(request_index);
                worker_requests.push_back(requests[request_index]);
            }
            connector->StoreBatch(worker_requests);
            for (size_t index = 0; index < worker_requests.size(); ++index) {
                requests[request_indices[index]].result =
                    std::move(worker_requests[index].result);
            }
        },
        worker_count);
}

tl::expected<void, ErrorCode> NvmeKvStorageBackend::InitDevice() {
    auto connector = std::make_shared<NvmeKvConnector>(file_storage_config_);
    auto init_res = connector->Init();
    if (!init_res) {
        LOG(ERROR) << "Failed to initialize NVMe KV device: "
                   << toString(init_res.error());
        return init_res;
    }
    connector_ = std::move(connector);
    return {};
}

tl::expected<void, ErrorCode> NvmeKvStorageBackend::Init() {
    bool expected = false;
    if (!initialized_.compare_exchange_strong(expected, true,
                                              std::memory_order_acq_rel)) {
        LOG(ERROR) << "NVMe KV backend already initialized";
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }
    auto init_res = InitDevice();
    if (!init_res) {
        initialized_.store(false, std::memory_order_release);
        return init_res;
    }
    total_size_.store(0, std::memory_order_relaxed);
    total_keys_.store(0, std::memory_order_relaxed);
    InitIoWorkers();
    return {};
}

tl::expected<int64_t, ErrorCode> NvmeKvStorageBackend::BatchOffload(
    const std::unordered_map<std::string, std::vector<Slice>> &batch_object,
    std::function<ErrorCode(const std::vector<std::string> &keys,
                            std::vector<StorageObjectMetadata> &metadatas)>
        complete_handler,
    NvmeKvStorageBackend::EvictionHandler eviction_handler) {
    (void)eviction_handler;
    if (!initialized_.load(std::memory_order_acquire)) {
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }
    if (batch_object.empty()) {
        return tl::make_unexpected(ErrorCode::INVALID_KEY);
    }

    struct OffloadPlan {
        std::string key;
        const std::vector<Slice> *slices = nullptr;
        size_t payload_size = 0;
    };
    std::vector<OffloadPlan> plans;
    plans.reserve(batch_object.size());
    int64_t batch_total_size = 0;
    for (const auto &[key, slices] : batch_object) {
        if (slices.empty()) {
            continue;
        }
        size_t payload_size = 0;
        for (const auto &slice : slices) {
            if (slice.size >
                std::numeric_limits<size_t>::max() - payload_size) {
                return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
            }
            payload_size += slice.size;
        }
        if (payload_size >
                static_cast<size_t>(std::numeric_limits<int64_t>::max()) ||
            batch_total_size > std::numeric_limits<int64_t>::max() -
                                   static_cast<int64_t>(payload_size)) {
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
        batch_total_size += static_cast<int64_t>(payload_size);
        plans.push_back(OffloadPlan{key, &slices, payload_size});
    }

    if (plans.empty()) {
        return 0;
    }

    if (total_keys_.load(std::memory_order_relaxed) +
            static_cast<int64_t>(plans.size()) >
        file_storage_config_.total_keys_limit) {
        return tl::make_unexpected(ErrorCode::KEYS_ULTRA_LIMIT);
    }
    if (total_size_.load(std::memory_order_relaxed) + batch_total_size >
        file_storage_config_.total_size_limit) {
        return tl::make_unexpected(ErrorCode::KEYS_ULTRA_LIMIT);
    }
    auto enable_offloading_res = IsEnableOffloading();
    if (!enable_offloading_res) {
        return tl::make_unexpected(enable_offloading_res.error());
    }
    if (!enable_offloading_res.value()) {
        return tl::make_unexpected(ErrorCode::KEYS_ULTRA_LIMIT);
    }

    std::vector<tl::expected<OffloadResult, ErrorCode>> results;
    results.reserve(plans.size());
    for (size_t i = 0; i < plans.size(); ++i) {
        results.emplace_back(OffloadResult{});
    }

    struct PreparedOffload {
        std::string payload_storage;
        std::string_view payload;
        std::optional<NvmeKvKeyConflictPolicy::WritePlan> write_plan;
        std::vector<PhysicalKey> written_chunk_keys;
        bool root_written = false;
        bool needs_fallback = false;
    };
    std::vector<PreparedOffload> prepared(plans.size());

    const auto prepare_one = [&](size_t index) {
        const auto &plan = plans[index];
        auto &item = prepared[index];
        item.payload = BuildPayloadView(*plan.slices, plan.payload_size,
                                        item.payload_storage);
        auto write_plan = NvmeKvKeyConflictPolicy::BuildWritePlan(
            NvmeKvObjectIdentity{.logical_key = plan.key}, item.payload, 0,
            std::max(connector_->GetCapabilities().effective_max_value_size,
                     kMinMaxValueSize));
        if (!write_plan) {
            results[index] = tl::make_unexpected(write_plan.error());
            return;
        }
        item.write_plan.emplace(std::move(write_plan.value()));
    };

    struct StoreContext {
        size_t plan_index = 0;
        size_t chunk_index = 0;
    };
    const auto apply_chunk_results =
        [&prepared](std::vector<NvmeKvCommandExecutor::StoreRequest> &requests,
                    const std::vector<StoreContext> &contexts) {
            for (size_t request_index = 0; request_index < requests.size();
                 ++request_index) {
                const auto &context = contexts[request_index];
                auto &item = prepared[context.plan_index];
                if (requests[request_index].result) {
                    item.written_chunk_keys.push_back(
                        item.write_plan->chunk_values[context.chunk_index]
                            .first);
                } else {
                    item.needs_fallback = true;
                }
            }
        };

    bool roots_pipelined = false;
    const auto store_chunks = [&]() {
        if (io_workers_ == nullptr || submit_workers_ == nullptr ||
            root_submit_workers_ == nullptr || batch_submit_concurrency_ <= 1) {
            RunParallelIo(plans.size(), prepare_one, prepare_concurrency_);
            std::vector<NvmeKvCommandExecutor::StoreRequest> chunk_requests;
            std::vector<StoreContext> chunk_contexts;
            for (size_t plan_index = 0; plan_index < plans.size();
                 ++plan_index) {
                auto &item = prepared[plan_index];
                if (!item.write_plan || item.write_plan->store_inline) {
                    continue;
                }
                for (size_t chunk_index = 0;
                     chunk_index < item.write_plan->chunk_values.size();
                     ++chunk_index) {
                    const auto &[chunk_key, chunk_value] =
                        item.write_plan->chunk_values[chunk_index];
                    NvmeKvCommandExecutor::StoreRequest request;
                    request.key = chunk_key;
                    request.value = chunk_value;
                    chunk_requests.push_back(request);
                    chunk_contexts.push_back(
                        StoreContext{plan_index, chunk_index});
                }
            }
            StoreBatchParallel(chunk_requests);
            apply_chunk_results(chunk_requests, chunk_contexts);
            return;
        }

        const size_t lane_count =
            std::min(batch_submit_concurrency_, plans.size());
        const size_t root_lane_count =
            std::min(root_submit_concurrency_, plans.size());
        roots_pipelined = true;
        std::vector<std::unique_ptr<IndexQueue>> queues;
        queues.reserve(lane_count);
        for (size_t lane = 0; lane < lane_count; ++lane) {
            queues.push_back(std::make_unique<IndexQueue>());
        }

        const auto close_queues = [&]() {
            for (auto &queue : queues) {
                queue->Close();
            }
        };
        IndexQueue root_queue;
        std::mutex consumer_error_mutex;
        std::exception_ptr consumer_error;
        const auto set_consumer_error = [&]() {
            std::lock_guard<std::mutex> lock(consumer_error_mutex);
            if (consumer_error == nullptr) {
                consumer_error = std::current_exception();
            }
            close_queues();
            root_queue.Close();
        };
        std::latch consumers_done(static_cast<std::ptrdiff_t>(lane_count));
        std::latch root_consumers_done(
            static_cast<std::ptrdiff_t>(root_lane_count));

        size_t enqueued_root_consumers = 0;
        try {
            for (; enqueued_root_consumers < root_lane_count;
                 ++enqueued_root_consumers) {
                root_submit_workers_->enqueue([&]() {
                    try {
                        auto connector = connector_;
                        if (connector == nullptr) {
                            throw std::runtime_error(
                                "NVMe KV connector is unavailable");
                        }
                        const size_t queue_depth = std::max<size_t>(
                            1, connector->GetCapabilities().queue_depth);
                        std::vector<NvmeKvCommandExecutor::StoreRequest>
                            requests;
                        std::vector<size_t> contexts;
                        requests.reserve(queue_depth);
                        contexts.reserve(queue_depth);
                        const auto flush = [&]() {
                            if (requests.empty()) {
                                return;
                            }
                            connector->StoreBatch(requests);
                            for (size_t index = 0; index < requests.size();
                                 ++index) {
                                auto &item = prepared[contexts[index]];
                                if (requests[index].result) {
                                    item.root_written = true;
                                } else {
                                    item.needs_fallback = true;
                                }
                            }
                            requests.clear();
                            contexts.clear();
                        };

                        size_t plan_index = 0;
                        while (root_queue.Pop(plan_index)) {
                            auto &item = prepared[plan_index];
                            NvmeKvCommandExecutor::StoreRequest request;
                            request.key = item.write_plan->root_key;
                            request.value = item.write_plan->root_blob;
                            requests.push_back(request);
                            contexts.push_back(plan_index);
                            if (requests.size() == queue_depth) {
                                flush();
                            }
                        }
                        flush();
                    } catch (...) {
                        set_consumer_error();
                    }
                    root_consumers_done.count_down();
                });
            }
        } catch (...) {
            close_queues();
            root_queue.Close();
            root_consumers_done.count_down(static_cast<std::ptrdiff_t>(
                root_lane_count - enqueued_root_consumers));
            root_consumers_done.wait();
            throw;
        }

        size_t enqueued_consumers = 0;
        try {
            for (; enqueued_consumers < lane_count; ++enqueued_consumers) {
                submit_workers_->enqueue([&, lane = enqueued_consumers]() {
                    try {
                        auto connector = connector_;
                        if (connector == nullptr) {
                            throw std::runtime_error(
                                "NVMe KV connector is unavailable");
                        }
                        const size_t queue_depth = std::max<size_t>(
                            1, connector->GetCapabilities().queue_depth);
                        std::vector<NvmeKvCommandExecutor::StoreRequest>
                            chunk_requests;
                        std::vector<StoreContext> chunk_contexts;
                        chunk_requests.reserve(queue_depth);
                        chunk_contexts.reserve(queue_depth);
                        const auto flush_chunks = [&]() {
                            if (chunk_requests.empty()) {
                                return;
                            }
                            connector->StoreBatch(chunk_requests);
                            apply_chunk_results(chunk_requests, chunk_contexts);
                            for (const auto &context : chunk_contexts) {
                                auto &item = prepared[context.plan_index];
                                if (context.chunk_index + 1 ==
                                        item.write_plan->chunk_values.size() &&
                                    !item.needs_fallback) {
                                    if (!root_queue.Push(context.plan_index)) {
                                        item.needs_fallback = true;
                                    }
                                }
                            }
                            chunk_requests.clear();
                            chunk_contexts.clear();
                        };

                        size_t plan_index = 0;
                        while (queues[lane]->Pop(plan_index)) {
                            auto &item = prepared[plan_index];
                            for (size_t chunk_index = 0;
                                 chunk_index <
                                 item.write_plan->chunk_values.size();
                                 ++chunk_index) {
                                const auto &[chunk_key, chunk_value] =
                                    item.write_plan->chunk_values[chunk_index];
                                NvmeKvCommandExecutor::StoreRequest request;
                                request.key = chunk_key;
                                request.value = chunk_value;
                                chunk_requests.push_back(request);
                                chunk_contexts.push_back(
                                    StoreContext{plan_index, chunk_index});
                                if (chunk_requests.size() == queue_depth) {
                                    flush_chunks();
                                }
                            }
                        }
                        flush_chunks();
                    } catch (...) {
                        set_consumer_error();
                    }
                    consumers_done.count_down();
                });
            }
        } catch (...) {
            close_queues();
            root_queue.Close();
            consumers_done.count_down(
                static_cast<std::ptrdiff_t>(lane_count - enqueued_consumers));
            consumers_done.wait();
            root_consumers_done.wait();
            throw;
        }

        std::exception_ptr producer_error;
        try {
            RunParallelIo(
                plans.size(),
                [&](size_t index) {
                    prepare_one(index);
                    auto &item = prepared[index];
                    if (!item.write_plan) {
                        return;
                    }
                    const bool queued =
                        item.write_plan->store_inline
                            ? root_queue.Push(index)
                            : queues[index % lane_count]->Push(index);
                    if (!queued) {
                        results[index] =
                            tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
                    }
                },
                prepare_concurrency_);
        } catch (...) {
            producer_error = std::current_exception();
        }
        close_queues();
        consumers_done.wait();
        root_queue.Close();
        root_consumers_done.wait();
        if (producer_error != nullptr) {
            std::rethrow_exception(producer_error);
        }
        if (consumer_error != nullptr) {
            std::rethrow_exception(consumer_error);
        }
    };

    const auto cleanup_new_writes = [&](size_t plan_index, bool delete_root) {
        auto connector = connector_;
        auto &item = prepared[plan_index];
        if (connector == nullptr || !item.write_plan) {
            return;
        }
        if (delete_root) {
            auto result = connector->Delete(item.write_plan->root_key);
            if (!result && result.error() != ErrorCode::OBJECT_NOT_FOUND) {
                LOG(WARNING) << "NVMe KV failed to clean batched root for "
                             << plans[plan_index].key
                             << " error=" << toString(result.error());
            }
            item.root_written = false;
        }
        for (const auto &chunk_key : item.written_chunk_keys) {
            auto result = connector->Delete(chunk_key);
            if (!result && result.error() != ErrorCode::OBJECT_NOT_FOUND) {
                LOG(WARNING) << "NVMe KV failed to clean batched chunk for "
                             << plans[plan_index].key
                             << " error=" << toString(result.error());
            }
        }
        item.written_chunk_keys.clear();
    };

    ErrorCode worker_error = ErrorCode::OK;
    try {
        store_chunks();

        for (size_t plan_index = 0; plan_index < plans.size(); ++plan_index) {
            if (prepared[plan_index].needs_fallback) {
                cleanup_new_writes(plan_index, false);
            }
        }

        std::vector<NvmeKvCommandExecutor::StoreRequest> root_requests;
        std::vector<size_t> root_contexts;
        for (size_t plan_index = 0; plan_index < plans.size(); ++plan_index) {
            auto &item = prepared[plan_index];
            if (!item.write_plan || item.needs_fallback || roots_pipelined) {
                continue;
            }
            NvmeKvCommandExecutor::StoreRequest request;
            request.key = item.write_plan->root_key;
            request.value = item.write_plan->root_blob;
            root_requests.push_back(request);
            root_contexts.push_back(plan_index);
        }
        StoreBatchParallel(root_requests);
        for (size_t request_index = 0; request_index < root_requests.size();
             ++request_index) {
            const size_t plan_index = root_contexts[request_index];
            auto &item = prepared[plan_index];
            if (!root_requests[request_index].result) {
                item.needs_fallback = true;
                cleanup_new_writes(plan_index, false);
                continue;
            }
            item.root_written = true;
        }

        for (size_t plan_index = 0; plan_index < plans.size(); ++plan_index) {
            auto &item = prepared[plan_index];
            if (!item.write_plan || item.needs_fallback || !item.root_written) {
                continue;
            }
            item.root_written = false;
            item.written_chunk_keys.clear();
            results[plan_index] =
                OffloadResult{.stored = true, .inserted = true};
            if (!item.write_plan->store_inline) {
                CacheManifestAfterWrite(plans[plan_index].key,
                                        plans[plan_index].payload_size, 0,
                                        item.write_plan->manifest_records);
            }
        }

        RunParallelIo(plans.size(), [&](size_t index) {
            if (!prepared[index].needs_fallback) {
                return;
            }
            results[index] = OffloadOne(plans[index].key, *plans[index].slices,
                                        plans[index].payload_size);
        });
    } catch (const std::exception &error) {
        LOG(ERROR) << "NVMe KV batch offload worker failed: " << error.what();
        worker_error = ErrorCode::INTERNAL_ERROR;
    } catch (...) {
        LOG(ERROR) << "NVMe KV batch offload worker failed";
        worker_error = ErrorCode::INTERNAL_ERROR;
    }
    if (worker_error != ErrorCode::OK) {
        for (size_t index = 0; index < prepared.size(); ++index) {
            cleanup_new_writes(index, prepared[index].root_written);
        }
    }

    std::vector<std::string> keys;
    std::vector<StorageObjectMetadata> metadatas;
    keys.reserve(plans.size());
    metadatas.reserve(plans.size());
    int64_t inserted_keys = 0;
    int64_t inserted_size = 0;
    ErrorCode first_error = worker_error;
    for (size_t i = 0; i < plans.size(); ++i) {
        if (!results[i]) {
            if (first_error == ErrorCode::OK) {
                first_error = results[i].error();
            }
            continue;
        }
        if (!results[i].value().stored) {
            continue;
        }
        if (results[i].value().inserted) {
            ++inserted_keys;
            inserted_size += static_cast<int64_t>(plans[i].payload_size);
        }
        keys.push_back(plans[i].key);
        metadatas.emplace_back(StorageObjectMetadata{
            .bucket_id = 0,
            .offset = 0,
            .key_size = static_cast<int64_t>(plans[i].key.size()),
            .data_size = static_cast<int64_t>(plans[i].payload_size),
            .transport_endpoint = "",
        });
    }

    total_keys_.fetch_add(inserted_keys, std::memory_order_relaxed);
    total_size_.fetch_add(inserted_size, std::memory_order_relaxed);
    if (complete_handler != nullptr && !keys.empty()) {
        const auto error_code = complete_handler(keys, metadatas);
        if (error_code != ErrorCode::OK) {
            return tl::make_unexpected(error_code);
        }
    }
    if (first_error != ErrorCode::OK) {
        return tl::make_unexpected(first_error);
    }
    return static_cast<int64_t>(keys.size());
}

tl::expected<NvmeKvStorageBackend::OffloadResult, ErrorCode>
NvmeKvStorageBackend::OffloadOne(const std::string &key,
                                 const std::vector<Slice> &slices,
                                 size_t payload_size) {
    if (payload_size >
        static_cast<size_t>(std::numeric_limits<uint32_t>::max())) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    std::string payload_storage;
    const std::string_view payload =
        BuildPayloadView(slices, payload_size, payload_storage);

    auto connector = connector_;
    if (connector == nullptr) {
        return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }
    const uint32_t effective_max_value_size =
        std::max(connector->GetCapabilities().effective_max_value_size,
                 kMinMaxValueSize);

    for (uint32_t slot = 0; slot < kNvmeKvMaxPhysicalKeySlots; ++slot) {
        auto write_plan_res = NvmeKvKeyConflictPolicy::BuildWritePlan(
            NvmeKvObjectIdentity{.logical_key = key}, payload, slot,
            effective_max_value_size);
        if (!write_plan_res) {
            return tl::make_unexpected(write_plan_res.error());
        }
        auto &write_plan = write_plan_res.value();
        const bool store_inline = write_plan.store_inline;
        const auto &physical_key = write_plan.root_key;
        const auto &root_blob = write_plan.root_blob;
        const auto &chunk_store_values = write_plan.chunk_values;

        std::vector<PhysicalKey> written_chunk_keys;
        bool root_written = false;
        const auto cleanup_new_writes = [&]() {
            if (root_written) {
                auto delete_res = connector->Delete(physical_key);
                if (!delete_res &&
                    delete_res.error() != ErrorCode::OBJECT_NOT_FOUND) {
                    LOG(WARNING)
                        << "NVMe KV failed to clean orphan root for " << key
                        << " key=" << NvmeKvPhysicalKeyToHex(physical_key)
                        << " error=" << toString(delete_res.error());
                }
                root_written = false;
            }
            if (written_chunk_keys.empty()) {
                return;
            }
            for (const auto &chunk_key : written_chunk_keys) {
                auto delete_res = connector->Delete(chunk_key);
                if (!delete_res &&
                    delete_res.error() != ErrorCode::OBJECT_NOT_FOUND) {
                    LOG(WARNING)
                        << "NVMe KV failed to clean orphan chunk for " << key
                        << " key=" << NvmeKvPhysicalKeyToHex(chunk_key)
                        << " error=" << toString(delete_res.error());
                }
            }
            written_chunk_keys.clear();
        };
        const auto resolve_existing_root =
            [&]() -> tl::expected<OffloadResult, ErrorCode> {
            auto decision_res = NvmeKvKeyConflictPolicy::ResolveExistingObject(
                *connector, physical_key, root_blob);
            if (!decision_res) {
                return tl::make_unexpected(decision_res.error());
            }
            if (decision_res.value() ==
                NvmeKvKeyConflictPolicy::ExistingObjectDecision::kNotFound) {
                return OffloadResult{};
            }
            if (decision_res.value() ==
                NvmeKvKeyConflictPolicy::ExistingObjectDecision::
                    kDifferentObject) {
                return OffloadResult{};
            }
            return OffloadResult{.stored = true, .inserted = false};
        };

        bool write_success = true;
        bool slot_collision = false;
        ErrorCode write_error = ErrorCode::OK;
        if (store_inline) {
            auto store_res = connector->Store(physical_key, root_blob);
            if (!store_res) {
                write_success = false;
                write_error = store_res.error();
            } else {
                root_written = true;
            }
        } else {
            const auto handle_chunk_store_error =
                [&](const PhysicalKey &chunk_key, std::string_view chunk_blob,
                    ErrorCode error) -> bool {
                if (error == ErrorCode::OBJECT_ALREADY_EXISTS) {
                    auto existing_chunk_res =
                        NvmeKvKeyConflictPolicy::ResolveExistingObject(
                            *connector, chunk_key, chunk_blob);
                    if (existing_chunk_res &&
                        existing_chunk_res.value() ==
                            NvmeKvKeyConflictPolicy::ExistingObjectDecision::
                                kSameObject) {
                        return true;
                    }
                    if (!existing_chunk_res) {
                        write_error = existing_chunk_res.error();
                    } else {
                        write_error = ErrorCode::OBJECT_ALREADY_EXISTS;
                    }
                    slot_collision = true;
                } else {
                    write_error = error;
                }
                write_success = false;
                cleanup_new_writes();
                return false;
            };
            std::vector<NvmeKvCommandExecutor::StoreRequest> chunk_requests;
            chunk_requests.reserve(chunk_store_values.size());
            for (const auto &[chunk_key, chunk_blob] : chunk_store_values) {
                NvmeKvCommandExecutor::StoreRequest request;
                request.key = chunk_key;
                request.value = chunk_blob;
                chunk_requests.push_back(request);
            }
            connector->StoreBatch(chunk_requests);

            for (size_t chunk_index = 0;
                 chunk_index < chunk_store_values.size(); ++chunk_index) {
                const auto &[chunk_key, chunk_blob] =
                    chunk_store_values[chunk_index];
                const auto &chunk_res = chunk_requests[chunk_index].result;
                if (!chunk_res) {
                    if (!handle_chunk_store_error(chunk_key, chunk_blob,
                                                  chunk_res.error())) {
                        break;
                    }
                    continue;
                }
                written_chunk_keys.push_back(chunk_key);
            }
            if (write_success) {
                auto store_res = connector->Store(physical_key, root_blob);
                if (!store_res) {
                    write_success = false;
                    write_error = store_res.error();
                    cleanup_new_writes();
                } else {
                    root_written = true;
                }
            }
        }

        if (!write_success && write_error == ErrorCode::OBJECT_ALREADY_EXISTS) {
            auto existing_res = resolve_existing_root();
            if (existing_res && existing_res.value().stored) {
                if (!store_inline) {
                    CacheManifestAfterWrite(key, payload_size, slot,
                                            write_plan.manifest_records);
                }
                return existing_res;
            }
            if (!existing_res) {
                write_error = existing_res.error();
            } else {
                slot_collision = true;
                cleanup_new_writes();
            }
        }

        if (!write_success) {
            if (!slot_collision && !store_inline) {
                cleanup_new_writes();
            }
            written_chunk_keys.clear();
            if (slot_collision) {
                continue;
            }
            return tl::make_unexpected(write_error);
        }

        if (!store_inline) {
            CacheManifestAfterWrite(key, payload_size, slot,
                                    write_plan.manifest_records);
        }
        return OffloadResult{.stored = true, .inserted = true};
    }
    return OffloadResult{};
}

tl::expected<void, ErrorCode> NvmeKvStorageBackend::BatchLoad(
    std::unordered_map<std::string, Slice> &batched_slices) {
    if (!initialized_.load(std::memory_order_acquire)) {
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }

    const auto connector = connector_;
    if (connector == nullptr) {
        return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }

    struct ReadPlan {
        std::string key;
        Slice dest_slice;
        std::shared_ptr<const CachedManifest> cached_manifest;
    };
    std::vector<ReadPlan> plans;
    plans.reserve(batched_slices.size());

    for (const auto &[key, dest_slice] : batched_slices) {
        if (dest_slice.size > std::numeric_limits<uint32_t>::max()) {
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
        plans.push_back(ReadPlan{key, dest_slice,
                                 FindCachedManifest(key, dest_slice.size)});
    }

    struct PreparedRead {
        std::shared_ptr<const CachedManifest> manifest;
    };
    std::vector<PreparedRead> prepared_reads(plans.size());
    const auto prepare_one =
        [&](size_t plan_index) -> tl::expected<void, ErrorCode> {
        const auto &plan = plans[plan_index];
        auto resolved_res = ResolveRoot(*connector, plan.key);
        if (!resolved_res) {
            return tl::make_unexpected(resolved_res.error());
        }
        if (!resolved_res.value()) {
            return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
        }
        const auto &resolved = *resolved_res.value();
        const std::string &object_blob = resolved.object_blob;
        NvmeKvObjectHeader header{};
        std::string_view identity_metadata_view;
        std::string_view payload_view;
        if (!ParseNvmeKvObjectBlob(object_blob, header, identity_metadata_view,
                                   payload_view)) {
            return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
        }

        NvmeKvStoredIdentityView stored_identity_view{};
        if (!ParseNvmeKvStoredIdentity(identity_metadata_view,
                                       stored_identity_view) ||
            stored_identity_view.logical_key.empty() ||
            stored_identity_view.logical_key != plan.key) {
            return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
        }
        NvmeKvObjectIdentity identity{.logical_key =
                                          stored_identity_view.logical_key};
        if (!NvmeKvKeyConflictPolicy::ValidateResolvedRootPlacement(
                identity, stored_identity_view, resolved.physical_key) ||
            stored_identity_view.resolved_slot != resolved.slot) {
            return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
        }

        const auto object_type =
            static_cast<NvmeKvObjectType>(header.object_type);
        const uint32_t expected_identity_size =
            ComputeNvmeKvStoredIdentityMetadataSize(identity);
        if (object_type == NvmeKvObjectType::kInline) {
            if (!ValidateNvmeKvHeader(
                    header, identity, expected_identity_size,
                    static_cast<uint32_t>(plan.dest_slice.size),
                    NvmeKvObjectType::kInline) ||
                header.payload_checksum !=
                    ComputeNvmeKvPayloadChecksum(payload_view) ||
                payload_view.size() != plan.dest_slice.size) {
                return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
            }
            std::memcpy(plan.dest_slice.ptr, payload_view.data(),
                        payload_view.size());
            return {};
        }

        if (object_type != NvmeKvObjectType::kManifest ||
            !ValidateNvmeKvHeader(header, identity, expected_identity_size,
                                  static_cast<uint32_t>(payload_view.size()),
                                  NvmeKvObjectType::kManifest) ||
            header.payload_checksum !=
                ComputeNvmeKvPayloadChecksum(payload_view)) {
            return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
        }

        NvmeKvManifestMetadata metadata{};
        std::vector<NvmeKvManifestChunkRecord> chunk_records;
        if (!ParseNvmeKvManifest(payload_view, metadata, chunk_records) ||
            metadata.logical_payload_size != plan.dest_slice.size ||
            metadata.chunk_count != chunk_records.size()) {
            return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
        }

        auto cached_manifest =
            CacheManifest(plan.key, plan.dest_slice.size,
                          stored_identity_view.resolved_slot, chunk_records);
        if (cached_manifest == nullptr) {
            return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
        }
        prepared_reads[plan_index].manifest = std::move(cached_manifest);
        return {};
    };

    const auto load_chunk_blob =
        [&](size_t plan_index, size_t chunk_index,
            std::string_view chunk_blob) -> tl::expected<void, ErrorCode> {
        const auto &plan = plans[plan_index];
        const auto &prepared = prepared_reads[plan_index];
        const auto &chunk_record =
            prepared.manifest->chunk_records[chunk_index];
        if (chunk_blob.size() != chunk_record.payload_size ||
            chunk_record.payload_checksum !=
                ComputeNvmeKvPayloadChecksum(chunk_blob)) {
            return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
        }
        std::memcpy(static_cast<char *>(plan.dest_slice.ptr) +
                        prepared.manifest->chunk_offsets[chunk_index],
                    chunk_blob.data(), chunk_blob.size());
        return {};
    };

    std::vector<tl::expected<void, ErrorCode>> prepare_results(plans.size());
    std::vector<size_t> prepare_misses;
    prepare_misses.reserve(plans.size());
    for (size_t plan_index = 0; plan_index < plans.size(); ++plan_index) {
        if (plans[plan_index].cached_manifest == nullptr) {
            prepare_misses.push_back(plan_index);
            continue;
        }
        prepared_reads[plan_index].manifest = plans[plan_index].cached_manifest;
        prepare_results[plan_index] = {};
    }
    try {
        RunParallelIo(prepare_misses.size(), [&](size_t index) {
            const size_t plan_index = prepare_misses[index];
            prepare_results[plan_index] = prepare_one(plan_index);
        });
        for (const auto &result : prepare_results) {
            if (!result) {
                return tl::make_unexpected(result.error());
            }
        }

        std::vector<tl::expected<void, ErrorCode>> chunk_results(plans.size());
        const size_t read_plan_batch_size = ReadPlanBatchSize();
        const size_t read_group_count =
            (plans.size() + read_plan_batch_size - 1) / read_plan_batch_size;
        struct ChunkContext {
            size_t plan_index = 0;
            size_t chunk_index = 0;
        };
        struct ReadBatch {
            std::vector<NvmeKvCommandExecutor::RetrieveIntoRequest>
                direct_requests;
            std::vector<ChunkContext> direct_contexts;
            std::vector<NvmeKvCommandExecutor::RetrieveBufferRequest>
                buffer_requests;
            std::vector<ChunkContext> buffer_contexts;
        };

        const auto read_group = [&](size_t group_index) {
            const size_t plan_begin = group_index * read_plan_batch_size;
            const size_t plan_end =
                std::min(plans.size(), plan_begin + read_plan_batch_size);
            ReadBatch batch;

            for (size_t plan_index = plan_begin; plan_index < plan_end;
                 ++plan_index) {
                const auto &prepared = prepared_reads[plan_index];
                const auto chunk_count =
                    !prepared.manifest
                        ? 0
                        : prepared.manifest->chunk_records.size();
                if (chunk_count == 0) {
                    chunk_results[plan_index] = {};
                    continue;
                }
                for (size_t chunk_index = 0; chunk_index < chunk_count;
                     ++chunk_index) {
                    const auto &chunk_record =
                        prepared.manifest->chunk_records[chunk_index];
                    char *dest =
                        static_cast<char *>(plans[plan_index].dest_slice.ptr) +
                        prepared.manifest->chunk_offsets[chunk_index];
                    if (CanRetrieveDirectlyInto(dest,
                                                chunk_record.payload_size)) {
                        NvmeKvCommandExecutor::RetrieveIntoRequest request;
                        request.key = chunk_record.physical_key;
                        request.data = dest;
                        request.size = chunk_record.payload_size;
                        batch.direct_contexts.push_back(
                            ChunkContext{plan_index, chunk_index});
                        batch.direct_requests.push_back(std::move(request));
                    } else {
                        NvmeKvCommandExecutor::RetrieveBufferRequest request;
                        request.key = chunk_record.physical_key;
                        request.size_hint = chunk_record.payload_size;
                        batch.buffer_contexts.push_back(
                            ChunkContext{plan_index, chunk_index});
                        batch.buffer_requests.push_back(std::move(request));
                    }
                }
            }

            if (!batch.direct_requests.empty()) {
                connector->RetrieveIntoBatch(batch.direct_requests);
                for (size_t i = 0; i < batch.direct_requests.size(); ++i) {
                    const auto &context = batch.direct_contexts[i];
                    const auto &direct_res = batch.direct_requests[i].result;
                    if (!direct_res) {
                        chunk_results[context.plan_index] =
                            tl::make_unexpected(direct_res.error());
                        continue;
                    }
                    const auto &chunk_record =
                        prepared_reads[context.plan_index]
                            .manifest->chunk_records[context.chunk_index];
                    if (direct_res.value() != chunk_record.payload_size) {
                        chunk_results[context.plan_index] =
                            tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
                        continue;
                    }
                }
            }

            if (!batch.buffer_requests.empty()) {
                connector->RetrieveBufferBatch(batch.buffer_requests);
                for (size_t i = 0; i < batch.buffer_requests.size(); ++i) {
                    const auto &context = batch.buffer_contexts[i];
                    if (!chunk_results[context.plan_index]) {
                        continue;
                    }
                    const auto &chunk_res = batch.buffer_requests[i].result;
                    if (!chunk_res) {
                        chunk_results[context.plan_index] =
                            tl::make_unexpected(chunk_res.error());
                        continue;
                    }
                    const auto &chunk_buffer = chunk_res.value();
                    auto loaded = load_chunk_blob(
                        context.plan_index, context.chunk_index,
                        std::string_view(chunk_buffer.data, chunk_buffer.size));
                    if (!loaded) {
                        chunk_results[context.plan_index] = loaded;
                    }
                }
            }

            for (size_t plan_index = plan_begin; plan_index < plan_end;
                 ++plan_index) {
                if (!chunk_results[plan_index]) {
                    continue;
                }
                chunk_results[plan_index] = {};
            }
        };

        const auto verify_read_group = [&](size_t group_index) {
            const size_t plan_begin = group_index * read_plan_batch_size;
            const size_t plan_end =
                std::min(plans.size(), plan_begin + read_plan_batch_size);
            for (size_t plan_index = plan_begin; plan_index < plan_end;
                 ++plan_index) {
                if (!chunk_results[plan_index]) {
                    continue;
                }
                const auto &manifest = prepared_reads[plan_index].manifest;
                if (manifest == nullptr) {
                    continue;
                }
                for (size_t chunk_index = 0;
                     chunk_index < manifest->chunk_records.size();
                     ++chunk_index) {
                    const auto &chunk_record =
                        manifest->chunk_records[chunk_index];
                    const char *dest = static_cast<const char *>(
                                           plans[plan_index].dest_slice.ptr) +
                                       manifest->chunk_offsets[chunk_index];
                    if (!CanRetrieveDirectlyInto(dest,
                                                 chunk_record.payload_size)) {
                        continue;
                    }
                    if (chunk_record.payload_checksum !=
                        ComputeNvmeKvPayloadChecksum(std::string_view(
                            dest, chunk_record.payload_size))) {
                        chunk_results[plan_index] =
                            tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
                        break;
                    }
                }
            }
        };

        RunPipelinedIo(read_group_count, read_group, verify_read_group);
        for (size_t plan_index = 0; plan_index < plans.size(); ++plan_index) {
            if (!chunk_results[plan_index]) {
                const auto error = chunk_results[plan_index].error();
                return tl::make_unexpected(error);
            }
        }
    } catch (const std::exception &error) {
        LOG(ERROR) << "NVMe KV batch load worker failed: " << error.what();
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    } catch (...) {
        LOG(ERROR) << "NVMe KV batch load worker failed";
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }

    return {};
}

tl::expected<bool, ErrorCode> NvmeKvStorageBackend::IsExist(
    const std::string &key) {
    if (!initialized_.load(std::memory_order_acquire)) {
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }
    const auto connector = connector_;
    if (connector == nullptr) {
        return false;
    }
    auto resolved = ResolveRoot(*connector, key);
    if (!resolved) {
        return tl::make_unexpected(resolved.error());
    }
    return resolved.value().has_value();
}

tl::expected<bool, ErrorCode> NvmeKvStorageBackend::IsEnableOffloading() {
    if (!initialized_.load(std::memory_order_acquire)) {
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }
    if (connector_ == nullptr) {
        return false;
    }
    return total_keys_.load(std::memory_order_relaxed) <
               file_storage_config_.total_keys_limit &&
           total_size_.load(std::memory_order_relaxed) <
               file_storage_config_.total_size_limit;
}

tl::expected<void, ErrorCode> NvmeKvStorageBackend::ScanMeta(
    const std::function<
        ErrorCode(const std::vector<std::string> &keys,
                  std::vector<StorageObjectMetadata> &metadatas)> &handler) {
    if (!initialized_.load(std::memory_order_acquire)) {
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }
    (void)handler;
    return {};
}

}  // namespace mooncake
