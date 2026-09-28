#include "storage/distributed/kvcs/kvcs_object_storage_adapter.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <future>
#include <limits>
#include <optional>
#include <set>
#include <string_view>
#include <utility>

#include "hybrid_metric.h"
#include "storage/distributed/kvcs/kvcs_capi_driver.h"
#include "storage/distributed/kvcs/kvcs_efc_topology.h"
#include "tenant_id.h"
#include "thread_pool.h"

namespace mooncake {
namespace {

constexpr std::string_view kHealthProbeKey =
    "mc1:6d6f6f6e63616b65.6b7663732d6865616c74682d70726f6265";
const std::vector<double> kLatencyBucketsUs = {
    50,     100,     200,     500,     1000,     2000,
    5000,   10000,   20000,   50000,   100000,   200000,
    500000, 1000000, 2000000, 5000000, 10000000, 30000000};

template <typename T>
std::vector<tl::expected<T, ErrorCode>> Errors(size_t count, ErrorCode error) {
    std::vector<tl::expected<T, ErrorCode>> results;
    results.reserve(count);
    for (size_t i = 0; i < count; ++i)
        results.emplace_back(tl::make_unexpected(error));
    return results;
}

ObjectStorageIoResults IoErrors(size_t count, ErrorCode error) {
    return Errors<void>(count, error);
}

uint64_t ElapsedUs(std::chrono::steady_clock::time_point start) {
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now() - start)
            .count());
}

uint64_t StableHash64(std::string_view value) {
    uint64_t hash = 14695981039346656037ULL;
    for (const unsigned char byte : value) {
        hash ^= byte;
        hash *= 1099511628211ULL;
    }
    return hash;
}

std::string_view BatchResultLabel(size_t succeeded, size_t total) {
    if (succeeded == total) return "ok";
    if (succeeded == 0) return "error";
    return "partial";
}

std::string EscapeLabel(std::string_view value) {
    std::string escaped;
    escaped.reserve(value.size());
    for (const char c : value) {
        if (c == '\\' || c == '"' || c == '\n') escaped.push_back('\\');
        escaped.push_back(c);
    }
    return escaped;
}

bool ShouldTryNextTarget(ErrorCode error) {
    return error == ErrorCode::OBJECT_NOT_FOUND ||
           error == ErrorCode::KVCS_INCOMPLETE || IsKvcsTransientError(error);
}

tl::expected<std::vector<Slice>, ErrorCode> BuildDestinationSlices(
    const ObjectStorageGetRequest& request) {
    if (request.logical_key.empty() || request.expected_size == 0) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    size_t capacity = 0;
    for (const auto& slice : request.slices) {
        if ((slice.ptr == nullptr && slice.size != 0) ||
            slice.size > std::numeric_limits<size_t>::max() - capacity) {
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
        capacity += slice.size;
    }
    if (capacity < request.expected_size) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    std::vector<Slice> result;
    result.reserve(request.slices.size());
    size_t remaining = request.expected_size;
    for (const auto& slice : request.slices) {
        if (remaining == 0) break;
        const size_t length = std::min(remaining, slice.size);
        if (length != 0) result.push_back({slice.ptr, length});
        remaining -= length;
    }
    return result;
}

}  // namespace

struct KvcsObjectStorageAdapter::Impl {
    struct Target {
        std::string id;
        uint32_t mountpoint_index = 0;
        KvcsEfcRouteKind route_kind = KvcsEfcRouteKind::kKvCacheStore;
        std::unique_ptr<KvcsDriver> driver;
    };

    explicit Impl(KvcsAccessMode access_mode)
        : mode(access_mode == KvcsAccessMode::kStandard ? "standard"
                                                        : "low-level"),
          operation_count("mooncake_kvcs_operations_total",
                          "Total KVCS operations", {{"mode", mode}},
                          metric_label_names),
          operation_bytes("mooncake_kvcs_bytes_total",
                          "Total bytes processed by KVCS operations",
                          {{"mode", mode}}, metric_label_names),
          operation_latency("mooncake_kvcs_latency_us",
                            "KVCS operation latency in microseconds",
                            kLatencyBucketsUs, {{"mode", mode}},
                            metric_label_names),
          batch_count("mooncake_kvcs_batches_total", "Total KVCS batches",
                      {{"mode", mode}}, metric_label_names),
          batch_items("mooncake_kvcs_batch_items_total",
                      "Total items submitted in KVCS batches", {{"mode", mode}},
                      metric_label_names),
          batch_latency("mooncake_kvcs_batch_latency_us",
                        "KVCS batch latency in microseconds", kLatencyBucketsUs,
                        {{"mode", mode}}, metric_label_names) {}

    void Observe(std::string_view operation, size_t target_index,
                 std::string_view result, uint64_t bytes, uint64_t latency_us) {
        const auto& target = *targets[target_index];
        const std::array<std::string, 3> labels{std::string(operation),
                                                EscapeLabel(target.id),
                                                std::string(result)};
        operation_count.inc(labels);
        operation_bytes.inc(labels, static_cast<int64_t>(bytes));
        operation_latency.observe(
            labels, static_cast<int64_t>(std::max<uint64_t>(latency_us, 1)));
    }

    void ObserveBatch(std::string_view operation, size_t target_index,
                      std::string_view result, uint64_t items,
                      uint64_t latency_us) {
        const auto& target = *targets[target_index];
        const std::array<std::string, 3> labels{std::string(operation),
                                                EscapeLabel(target.id),
                                                std::string(result)};
        batch_count.inc(labels);
        batch_items.inc(labels, static_cast<int64_t>(items));
        batch_latency.observe(
            labels, static_cast<int64_t>(std::max<uint64_t>(latency_us, 1)));
    }

    void Serialize(std::string& output) const {
        operation_count.serialize(output);
        operation_bytes.serialize(output);
        operation_latency.serialize(output);
        batch_count.serialize(output);
        batch_items.serialize(output);
        batch_latency.serialize(output);
        output.append("# TYPE mooncake_kvcs_query_hits_total counter\n")
            .append("mooncake_kvcs_query_hits_total{mode=\"")
            .append(mode)
            .append("\"} ")
            .append(std::to_string(query_hits.load(std::memory_order_relaxed)))
            .append("\n# TYPE mooncake_kvcs_query_misses_total counter\n")
            .append("mooncake_kvcs_query_misses_total{mode=\"")
            .append(mode)
            .append("\"} ")
            .append(
                std::to_string(query_misses.load(std::memory_order_relaxed)))
            .append("\n");
    }

    std::vector<size_t> TargetOrder(std::string_view logical_key) const {
        std::vector<size_t> result;
        result.reserve(targets.size());
        if (targets.empty()) return result;
        const size_t start = StableHash64(logical_key) % targets.size();
        for (size_t offset = 0; offset < targets.size(); ++offset) {
            result.push_back((start + offset) % targets.size());
        }
        return result;
    }

    std::vector<size_t> BuildTargetGroups(
        std::vector<std::vector<size_t>>& candidates, std::vector<bool>& done,
        size_t& remaining, std::vector<std::vector<size_t>>& groups) const {
        groups.assign(targets.size(), {});
        std::vector<size_t> unavailable;
        bool scheduled = false;
        for (size_t i = 0; i < candidates.size(); ++i) {
            if (done[i]) continue;
            if (candidates[i].empty()) {
                done[i] = true;
                --remaining;
                unavailable.push_back(i);
                continue;
            }
            const size_t target = candidates[i].front();
            groups[target].push_back(i);
            candidates[i].erase(candidates[i].begin());
            scheduled = true;
        }
        if (!scheduled) groups.clear();
        return unavailable;
    }

    std::vector<std::unique_ptr<Target>> targets;
    bool delete_local_first = false;
    std::unique_ptr<ThreadPool> io_workers;
    std::string mode;
    const std::array<std::string, 3> metric_label_names{"operation", "target",
                                                        "result"};
    mutable ylt::metric::hybrid_counter_3t operation_count;
    mutable ylt::metric::hybrid_counter_3t operation_bytes;
    mutable ylt::metric::hybrid_histogram_3t operation_latency;
    mutable ylt::metric::hybrid_counter_3t batch_count;
    mutable ylt::metric::hybrid_counter_3t batch_items;
    mutable ylt::metric::hybrid_histogram_3t batch_latency;
    std::atomic<uint64_t> query_hits{0};
    std::atomic<uint64_t> query_misses{0};
};

KvcsObjectStorageAdapter::KvcsObjectStorageAdapter(
    const FileStorageConfig& config, KvcsAccessMode mode,
    std::string efc_config_path)
    : config_(config),
      mode_(mode),
      efc_config_path_(std::move(efc_config_path)) {}

KvcsObjectStorageAdapter::KvcsObjectStorageAdapter(
    const FileStorageConfig& config, KvcsAccessMode mode,
    std::unique_ptr<KvcsDriver> driver)
    : KvcsObjectStorageAdapter(config, mode) {
    pending_targets_.push_back(
        {.id = "injected", .mountpoint_index = 0, .driver = std::move(driver)});
}

KvcsObjectStorageAdapter::KvcsObjectStorageAdapter(
    const FileStorageConfig& config,
    std::vector<KvcsLowLevelTargetSpec> targets)
    : KvcsObjectStorageAdapter(config, KvcsAccessMode::kLowLevel) {
    pending_targets_ = std::move(targets);
}

KvcsObjectStorageAdapter::~KvcsObjectStorageAdapter() = default;

tl::expected<ObjectKey, ErrorCode> KvcsObjectStorageAdapter::EncodeKey(
    std::string_view logical_key) const {
    const TenantId tenant(config_.kvcs_tenant_id);
    if (logical_key.empty() || !tenant.IsValid())
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    auto encoded = EncodeKvcsKey(tenant.value(), logical_key);
    if (!impl_ || impl_->targets.empty() ||
        !IsKvcsKeyStringSafe(encoded,
                             impl_->targets.front()->driver->MaxKeySize())) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    return encoded;
}

tl::expected<void, ErrorCode> KvcsObjectStorageAdapter::Init() {
    if (initialized_) return {};

    if (pending_targets_.empty()) {
        if (mode_ == KvcsAccessMode::kStandard) {
            auto driver = CreateKvcsStandardDriver();
            if (!driver) return tl::make_unexpected(driver.error());
            pending_targets_.push_back({.id = "standard",
                                        .mountpoint_index = 0,
                                        .driver = std::move(driver.value())});
        } else {
            auto targets = LoadKvcsEfcTopology(efc_config_path_);
            if (!targets) return tl::make_unexpected(targets.error());
            std::vector<uint32_t> mountpoint_indices;
            mountpoint_indices.reserve(targets->size());
            for (const auto& target : targets.value())
                mountpoint_indices.push_back(target.mountpoint_index);
            auto drivers = CreateKvcsLowLevelDrivers(mountpoint_indices);
            if (!drivers) return tl::make_unexpected(drivers.error());
            for (size_t i = 0; i < targets->size(); ++i) {
                pending_targets_.push_back(
                    {.id = (*targets)[i].id,
                     .mountpoint_index = (*targets)[i].mountpoint_index,
                     .route_kind = (*targets)[i].kind,
                     .driver = std::move((*drivers)[i])});
            }
        }
    }
    if (pending_targets_.empty())
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);

    impl_ = std::make_unique<Impl>(mode_);
    const char* mode_name =
        mode_ == KvcsAccessMode::kStandard ? "standard" : "low-level";
    std::set<std::string> ids;
    std::set<uint32_t> indices;
    for (auto& spec : pending_targets_) {
        if (spec.id.empty() || !spec.driver || !ids.insert(spec.id).second ||
            (mode_ == KvcsAccessMode::kLowLevel &&
             !indices.insert(spec.mountpoint_index).second)) {
            impl_.reset();
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
        auto init = spec.driver->Init();
        if (!init) {
            impl_.reset();
            return init;
        }
        if (spec.driver->MaxValueSize() == 0 ||
            spec.driver->MaxKeySize() == 0 ||
            spec.driver->MaxKeySize() > kKvcsMaxKeySize) {
            impl_.reset();
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
        auto target = std::make_unique<Impl::Target>();
        target->id = std::move(spec.id);
        target->mountpoint_index = spec.mountpoint_index;
        target->route_kind = spec.route_kind;
        target->driver = std::move(spec.driver);
        LOG(INFO) << "KVCS target configured, mode=" << mode_name
                  << ", target=" << target->id << ", route_kind="
                  << (mode_ == KvcsAccessMode::kStandard
                          ? "standard"
                          : ToString(target->route_kind))
                  << ", mountpoint_index=" << target->mountpoint_index;
        impl_->targets.push_back(std::move(target));
    }
    bool has_local_route = false;
    bool has_kvcachestore_route = false;
    const size_t io_concurrency = impl_->targets.size();
    for (size_t i = 0; i < impl_->targets.size(); ++i) {
        has_local_route |=
            impl_->targets[i]->route_kind == KvcsEfcRouteKind::kLocal;
        has_kvcachestore_route |=
            impl_->targets[i]->route_kind == KvcsEfcRouteKind::kKvCacheStore;
    }
    impl_->delete_local_first = has_local_route && has_kvcachestore_route;
    pending_targets_.clear();
    impl_->io_workers =
        std::make_unique<ThreadPool>(std::clamp<size_t>(io_concurrency, 2, 32));
    initialized_ = true;
    LOG(INFO) << "KVCS DFS adapter initialized, mode=" << mode_name
              << ", targets=" << impl_->targets.size()
              << ", write_placement=stable-hash"
              << ", io_workers=" << std::clamp<size_t>(io_concurrency, 2, 32);
    return {};
}

tl::expected<void, ErrorCode> KvcsObjectStorageAdapter::CheckHealth() {
    if (!initialized_ || !impl_ || impl_->targets.empty()) {
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }
    const std::array<ObjectKey, 1> probe{std::string(kHealthProbeKey)};
    for (const auto& target : impl_->targets) {
        const auto result = BatchQueryKvcsObjects(*target->driver, probe);
        if (result.size() != 1) {
            return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
        }
        if (!result[0] && result[0].error() != ErrorCode::OBJECT_NOT_FOUND) {
            LOG(WARNING) << "KVCS query health probe failed, target="
                         << target->id
                         << ", error=" << toString(result[0].error());
            return tl::make_unexpected(result[0].error());
        }
    }
    return {};
}

tl::expected<void, ErrorCode> KvcsObjectStorageAdapter::Put(
    const std::string& logical_key, std::span<const char> data) {
    Slice slice{const_cast<char*>(data.data()), data.size()};
    ObjectStoragePutRequest request{logical_key, {slice}};
    auto results = BatchPutV({&request, 1});
    if (results.size() != 1)
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    return std::move(results[0]);
}

tl::expected<void, ErrorCode> KvcsObjectStorageAdapter::PutV(
    const std::string& logical_key, const iovec* iov, int iovcnt) {
    auto results = PutBatch({{logical_key, iov, iovcnt}});
    if (results.size() != 1)
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    return std::move(results[0]);
}

std::vector<tl::expected<void, ErrorCode>> KvcsObjectStorageAdapter::PutBatch(
    const std::vector<ObjectPutRequest>& requests) {
    std::vector<ObjectStoragePutRequest> batch;
    batch.reserve(requests.size());
    for (const auto& request : requests) {
        if (request.iov == nullptr || request.iovcnt <= 0)
            return IoErrors(requests.size(), ErrorCode::INVALID_PARAMS);
        ObjectStoragePutRequest put;
        put.logical_key = request.logical_key;
        put.slices.reserve(static_cast<size_t>(request.iovcnt));
        for (int i = 0; i < request.iovcnt; ++i)
            put.slices.push_back(
                {request.iov[i].iov_base, request.iov[i].iov_len});
        batch.push_back(std::move(put));
    }
    return BatchPutV(batch);
}

ObjectStorageIoResults KvcsObjectStorageAdapter::BatchPutV(
    std::span<const ObjectStoragePutRequest> requests) {
    if (!initialized_ || !impl_)
        return IoErrors(requests.size(), ErrorCode::INTERNAL_ERROR);
    std::vector<KvcsPutRequest> kvcs_requests;
    std::vector<uint64_t> request_sizes;
    kvcs_requests.reserve(requests.size());
    request_sizes.reserve(requests.size());
    for (const auto& request : requests) {
        auto key = EncodeKey(request.logical_key);
        if (!key || request.slices.empty())
            return IoErrors(requests.size(), ErrorCode::INVALID_PARAMS);
        uint64_t request_size = 0;
        for (const auto& slice : request.slices) {
            if ((slice.ptr == nullptr && slice.size != 0) ||
                slice.size >
                    std::numeric_limits<uint64_t>::max() - request_size)
                return IoErrors(requests.size(), ErrorCode::INVALID_PARAMS);
            request_size += slice.size;
        }
        if (request_size == 0)
            return IoErrors(requests.size(), ErrorCode::INVALID_PARAMS);
        kvcs_requests.push_back(
            {.logical_key = std::move(key.value()), .slices = request.slices});
        request_sizes.push_back(request_size);
    }

    std::vector<std::string> replace_keys;
    std::vector<size_t> replace_indices;
    for (size_t i = 0; i < requests.size(); ++i) {
        if (requests[i].replace_existing) {
            replace_keys.push_back(requests[i].logical_key);
            replace_indices.push_back(i);
        }
    }
    ObjectStorageIoResults results(requests.size());
    if (!replace_keys.empty()) {
        auto deleted = BatchDelete(replace_keys);
        if (deleted.size() != replace_keys.size())
            return IoErrors(requests.size(), ErrorCode::INTERNAL_ERROR);
        for (size_t i = 0; i < deleted.size(); ++i) {
            if (!deleted[i]) {
                results[replace_indices[i]] =
                    tl::make_unexpected(deleted[i].error());
            }
        }
    }

    std::vector<std::vector<size_t>> candidates(requests.size());
    std::vector<bool> done(requests.size(), false);
    std::vector<std::optional<ErrorCode>> first_errors(requests.size());
    for (size_t i = 0; i < requests.size(); ++i) {
        if (!results[i]) {
            done[i] = true;
        } else {
            candidates[i] = impl_->TargetOrder(kvcs_requests[i].logical_key);
        }
    }

    struct PutBatchResult {
        size_t target;
        std::vector<size_t> request_indices;
        KvcsPutResults results;
        uint64_t latency_us;
    };
    size_t remaining =
        static_cast<size_t>(std::count(done.begin(), done.end(), false));
    while (remaining != 0) {
        std::vector<std::vector<size_t>> groups;
        const auto unavailable =
            impl_->BuildTargetGroups(candidates, done, remaining, groups);
        for (const size_t index : unavailable) {
            results[index] = tl::make_unexpected(
                first_errors[index].value_or(ErrorCode::KVCS_UNAVAILABLE));
        }
        if (groups.empty()) break;

        std::vector<std::future<PutBatchResult>> futures;
        for (size_t target = 0; target < groups.size(); ++target) {
            if (groups[target].empty()) continue;
            auto indices = groups[target];
            futures.push_back(impl_->io_workers->submit(
                [this, target, indices = std::move(indices),
                 &kvcs_requests]() mutable {
                    std::vector<KvcsPutRequest> batch;
                    batch.reserve(indices.size());
                    for (const size_t index : indices) {
                        batch.push_back(kvcs_requests[index]);
                    }
                    const auto start = std::chrono::steady_clock::now();
                    auto put = BatchPutKvcsObjects(
                        *impl_->targets[target]->driver, batch);
                    return PutBatchResult{target, std::move(indices),
                                          std::move(put), ElapsedUs(start)};
                }));
        }

        for (auto& future : futures) {
            auto batch = future.get();
            if (batch.results.size() != batch.request_indices.size()) {
                batch.results = Errors<void>(batch.request_indices.size(),
                                             ErrorCode::INTERNAL_ERROR);
            }
            size_t batch_successes = 0;
            for (size_t i = 0; i < batch.results.size(); ++i) {
                const size_t request_index = batch.request_indices[i];
                if (batch.results[i]) {
                    ++batch_successes;
                    impl_->Observe("put", batch.target, "ok",
                                   request_sizes[request_index],
                                   batch.latency_us);
                    results[request_index] = {};
                    done[request_index] = true;
                    --remaining;
                    continue;
                }

                const ErrorCode error = batch.results[i].error();
                impl_->Observe("put", batch.target, "error", 0,
                               batch.latency_us);
                LOG(WARNING)
                    << "KVCS target put failed, target="
                    << impl_->targets[batch.target]->id << ", mountpoint_index="
                    << impl_->targets[batch.target]->mountpoint_index
                    << ", error=" << toString(error);
                if (IsKvcsTransientError(error)) {
                    if (!first_errors[request_index]) {
                        first_errors[request_index] = error;
                    }
                } else {
                    results[request_index] = tl::make_unexpected(error);
                    done[request_index] = true;
                    --remaining;
                }
            }
            impl_->ObserveBatch(
                "put", batch.target,
                BatchResultLabel(batch_successes, batch.results.size()),
                batch.results.size(), batch.latency_us);
        }
    }
    return results;
}

KvcsManifestResults KvcsObjectStorageAdapter::BatchQueryKvcs(
    std::span<const ObjectKey> logical_keys,
    std::optional<std::chrono::steady_clock::time_point> deadline) {
    if (!initialized_ || !impl_)
        return Errors<KvcsManifest>(logical_keys.size(),
                                    ErrorCode::INTERNAL_ERROR);
    if (logical_keys.empty()) return {};
    std::vector<ObjectKey> encoded_keys;
    std::vector<std::vector<size_t>> candidates(logical_keys.size());
    encoded_keys.reserve(logical_keys.size());
    for (const auto& key : logical_keys) {
        auto encoded = EncodeKey(key);
        if (!encoded)
            return Errors<KvcsManifest>(logical_keys.size(), encoded.error());
        candidates[encoded_keys.size()] = impl_->TargetOrder(encoded.value());
        encoded_keys.push_back(std::move(encoded.value()));
    }

    struct QueryBatchResult {
        size_t target;
        std::vector<size_t> request_indices;
        KvcsManifestResults results;
        uint64_t latency_us;
    };
    auto run_groups = [this, &encoded_keys, deadline](
                          const std::vector<std::vector<size_t>>& groups) {
        std::vector<std::future<QueryBatchResult>> futures;
        for (size_t target = 0; target < groups.size(); ++target) {
            if (groups[target].empty()) continue;
            auto indices = groups[target];
            futures.push_back(impl_->io_workers->submit(
                [this, target, indices = std::move(indices), deadline,
                 &encoded_keys]() mutable {
                    std::vector<ObjectKey> keys;
                    keys.reserve(indices.size());
                    for (const size_t index : indices) {
                        keys.push_back(encoded_keys[index]);
                    }
                    const auto start = std::chrono::steady_clock::now();
                    auto results = BatchQueryKvcsObjects(
                        *impl_->targets[target]->driver, keys, deadline);
                    return QueryBatchResult{target, std::move(indices),
                                            std::move(results),
                                            ElapsedUs(start)};
                }));
        }

        std::vector<QueryBatchResult> batches;
        for (auto& future : futures) {
            auto batch = future.get();
            size_t batch_successes = 0;
            for (size_t i = 0; i < batch.results.size(); ++i) {
                const auto& query = batch.results[i];
                if (query) {
                    ++batch_successes;
                    impl_->Observe("query", batch.target, "hit",
                                   query->total_size, batch.latency_us);
                } else if (query.error() == ErrorCode::OBJECT_NOT_FOUND) {
                    ++batch_successes;
                    impl_->Observe("query", batch.target, "miss", 0,
                                   batch.latency_us);
                } else {
                    impl_->Observe("query", batch.target, "error", 0,
                                   batch.latency_us);
                }
            }
            impl_->ObserveBatch(
                "query", batch.target,
                BatchResultLabel(batch_successes, batch.request_indices.size()),
                batch.request_indices.size(), batch.latency_us);
            batches.push_back(std::move(batch));
        }
        return batches;
    };

    KvcsManifestResults results =
        Errors<KvcsManifest>(logical_keys.size(), ErrorCode::KVCS_UNAVAILABLE);
    std::vector<bool> done(logical_keys.size(), false);
    std::vector<std::optional<ErrorCode>> first_errors(logical_keys.size());
    for (size_t i = 0; i < candidates.size(); ++i) {
        if (candidates[i].empty())
            first_errors[i] = ErrorCode::KVCS_UNAVAILABLE;
    }
    size_t remaining = logical_keys.size();
    while (remaining != 0) {
        std::vector<std::vector<size_t>> groups;
        const auto unavailable =
            impl_->BuildTargetGroups(candidates, done, remaining, groups);
        for (const size_t index : unavailable) {
            results[index] = tl::make_unexpected(
                first_errors[index].value_or(ErrorCode::OBJECT_NOT_FOUND));
        }
        if (groups.empty()) break;

        for (auto& batch : run_groups(groups)) {
            for (size_t i = 0; i < batch.results.size(); ++i) {
                const size_t index = batch.request_indices[i];
                auto& query = batch.results[i];
                if (query) {
                    query->logical_key = logical_keys[index];
                    results[index] = std::move(query);
                    done[index] = true;
                    --remaining;
                } else if (ShouldTryNextTarget(query.error())) {
                    if (query.error() != ErrorCode::OBJECT_NOT_FOUND &&
                        !first_errors[index]) {
                        first_errors[index] = query.error();
                    }
                } else {
                    results[index] = tl::make_unexpected(query.error());
                    done[index] = true;
                    --remaining;
                }
            }
        }
    }
    for (const auto& result : results) {
        if (result) {
            impl_->query_hits.fetch_add(1, std::memory_order_relaxed);
        } else if (result.error() == ErrorCode::OBJECT_NOT_FOUND) {
            impl_->query_misses.fetch_add(1, std::memory_order_relaxed);
        }
    }
    return results;
}

KvcsGetResults KvcsObjectStorageAdapter::BatchGetKvcsWithManifests(
    std::span<const KvcsGetRequest> requests,
    std::span<const tl::expected<KvcsManifest, ErrorCode>> manifests) {
    if (!initialized_ || !impl_)
        return Errors<void>(requests.size(), ErrorCode::INTERNAL_ERROR);
    if (requests.size() != manifests.size())
        return Errors<void>(requests.size(), ErrorCode::INVALID_PARAMS);

    std::vector<KvcsGetRequest> encoded_requests;
    std::vector<KvcsManifest> encoded_manifests;
    std::vector<std::vector<size_t>> candidates(requests.size());
    encoded_requests.reserve(requests.size());
    encoded_manifests.reserve(requests.size());
    KvcsGetResults results =
        Errors<void>(requests.size(), ErrorCode::KVCS_UNAVAILABLE);
    std::vector<bool> done(requests.size(), false);
    std::vector<std::optional<ErrorCode>> first_errors(requests.size());
    for (size_t i = 0; i < requests.size(); ++i) {
        auto key = EncodeKey(requests[i].logical_key);
        if (!key || !manifests[i]) {
            results[i] =
                tl::make_unexpected(key ? manifests[i].error() : key.error());
            done[i] = true;
            encoded_requests.push_back({});
            encoded_manifests.push_back({});
            continue;
        }
        encoded_requests.push_back(requests[i]);
        encoded_requests.back().logical_key = std::move(key.value());
        encoded_manifests.push_back(manifests[i].value());
        if (encoded_manifests.back().logical_key == requests[i].logical_key)
            encoded_manifests.back().logical_key =
                encoded_requests.back().logical_key;
        if (encoded_manifests.back().logical_key !=
            encoded_requests.back().logical_key) {
            results[i] = tl::make_unexpected(ErrorCode::INVALID_PARAMS);
            done[i] = true;
            continue;
        }
        candidates[i] = impl_->TargetOrder(encoded_requests.back().logical_key);
        if (!encoded_manifests.back().layout_known) {
            uint64_t expected_size = 0;
            for (const auto& slice :
                 encoded_requests.back().destination_slices) {
                if (slice.size >
                    std::numeric_limits<uint64_t>::max() - expected_size) {
                    results[i] = tl::make_unexpected(ErrorCode::INVALID_PARAMS);
                    done[i] = true;
                    break;
                }
                expected_size += slice.size;
            }
            if (done[i]) continue;
            auto rebuilt = BuildKvcsManifest(
                encoded_requests.back().logical_key, expected_size,
                impl_->targets.front()->driver->MaxValueSize());
            if (!rebuilt) {
                results[i] = tl::make_unexpected(rebuilt.error());
                done[i] = true;
                continue;
            }
            encoded_manifests.back() = std::move(rebuilt.value());
        }
    }

    size_t remaining =
        static_cast<size_t>(std::count(done.begin(), done.end(), false));
    while (remaining != 0) {
        std::vector<std::vector<size_t>> groups;
        const auto unavailable =
            impl_->BuildTargetGroups(candidates, done, remaining, groups);
        for (const size_t index : unavailable) {
            results[index] = tl::make_unexpected(
                first_errors[index].value_or(ErrorCode::OBJECT_NOT_FOUND));
        }
        if (groups.empty()) break;

        struct GetBatchResult {
            size_t target;
            std::vector<size_t> request_indices;
            KvcsGetResults results;
            uint64_t latency_us;
        };
        std::vector<std::future<GetBatchResult>> futures;
        for (size_t target = 0; target < groups.size(); ++target) {
            if (groups[target].empty()) continue;
            auto indices = groups[target];
            futures.push_back(impl_->io_workers->submit(
                [this, target, indices = std::move(indices), &encoded_requests,
                 &encoded_manifests]() mutable {
                    std::vector<KvcsGetRequest> batch_requests;
                    KvcsManifestResults batch_manifests;
                    batch_requests.reserve(indices.size());
                    batch_manifests.reserve(indices.size());
                    for (const size_t index : indices) {
                        batch_requests.push_back(encoded_requests[index]);
                        batch_manifests.emplace_back(encoded_manifests[index]);
                    }
                    const auto start = std::chrono::steady_clock::now();
                    auto get =
                        BatchGetKvcsObjects(*impl_->targets[target]->driver,
                                            batch_requests, batch_manifests);
                    return GetBatchResult{target, std::move(indices),
                                          std::move(get), ElapsedUs(start)};
                }));
        }
        for (auto& future : futures) {
            auto batch = future.get();
            if (batch.results.size() != batch.request_indices.size())
                batch.results = Errors<void>(batch.request_indices.size(),
                                             ErrorCode::INTERNAL_ERROR);
            size_t batch_successes = 0;
            for (size_t i = 0; i < batch.results.size(); ++i) {
                const size_t request_index = batch.request_indices[i];
                if (batch.results[i]) {
                    ++batch_successes;
                    impl_->Observe("get", batch.target, "ok",
                                   encoded_manifests[request_index].total_size,
                                   batch.latency_us);
                    results[request_index] = {};
                    done[request_index] = true;
                    --remaining;
                } else {
                    const ErrorCode error = batch.results[i].error();
                    impl_->Observe("get", batch.target, "error", 0,
                                   batch.latency_us);
                    if (error != ErrorCode::OBJECT_NOT_FOUND &&
                        !first_errors[request_index]) {
                        first_errors[request_index] = error;
                    }
                    if (!ShouldTryNextTarget(error)) {
                        results[request_index] = tl::make_unexpected(error);
                        done[request_index] = true;
                        --remaining;
                    }
                }
            }
            impl_->ObserveBatch(
                "get", batch.target,
                BatchResultLabel(batch_successes, batch.results.size()),
                batch.results.size(), batch.latency_us);
        }
    }
    return results;
}

ObjectStorageQueryResults KvcsObjectStorageAdapter::BatchQueryProvider(
    std::span<const std::string> logical_keys) {
    auto manifests = BatchQueryKvcs(logical_keys);
    ObjectStorageQueryResults results;
    results.reserve(manifests.size());
    for (auto& manifest : manifests) {
        if (!manifest) {
            results.emplace_back(tl::make_unexpected(manifest.error()));
            continue;
        }
        results.emplace_back(std::move(*manifest));
    }
    return results;
}

ObjectStorageQueryResults KvcsObjectStorageAdapter::BatchQueryProviderUntil(
    std::span<const std::string> logical_keys,
    std::chrono::steady_clock::time_point deadline) {
    auto manifests = BatchQueryKvcs(logical_keys, deadline);
    ObjectStorageQueryResults results;
    results.reserve(manifests.size());
    for (auto& manifest : manifests) {
        if (!manifest) {
            results.emplace_back(tl::make_unexpected(manifest.error()));
        } else {
            results.emplace_back(std::move(*manifest));
        }
    }
    return results;
}

ObjectStorageIoResults KvcsObjectStorageAdapter::BatchGetIntoWithQueryContexts(
    std::span<const ObjectStorageGetRequest> requests,
    std::span<const tl::expected<ObjectStorageQueryContext, ErrorCode>>
        contexts) {
    if (requests.size() != contexts.size()) {
        return IoErrors(requests.size(), ErrorCode::INVALID_PARAMS);
    }

    std::vector<KvcsGetRequest> gets;
    KvcsManifestResults manifests;
    gets.reserve(requests.size());
    manifests.reserve(contexts.size());
    for (size_t i = 0; i < requests.size(); ++i) {
        auto slices = BuildDestinationSlices(requests[i]);
        if (!slices) {
            return IoErrors(requests.size(), ErrorCode::INVALID_PARAMS);
        }
        gets.push_back(
            KvcsGetRequest{requests[i].logical_key, std::move(*slices)});
        if (!contexts[i]) {
            manifests.emplace_back(tl::make_unexpected(contexts[i].error()));
        } else {
            manifests.emplace_back(*contexts[i]);
        }
    }
    return BatchGetKvcsWithManifests(gets, manifests);
}

tl::expected<size_t, ErrorCode> KvcsObjectStorageAdapter::Get(
    const std::string& logical_key, void* buf, size_t len) {
    ObjectStorageGetRequest request{logical_key, {{buf, len}}, len};
    auto results = BatchGetInto({&request, 1});
    if (results.size() != 1)
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    if (!results[0]) return tl::make_unexpected(results[0].error());
    return len;
}

ObjectStorageIoResults KvcsObjectStorageAdapter::BatchGetInto(
    std::span<const ObjectStorageGetRequest> requests) {
    std::vector<ObjectKey> keys;
    std::vector<KvcsGetRequest> gets;
    keys.reserve(requests.size());
    gets.reserve(requests.size());
    for (const auto& request : requests) {
        auto slices = BuildDestinationSlices(request);
        if (!slices)
            return IoErrors(requests.size(), ErrorCode::INVALID_PARAMS);
        keys.push_back(request.logical_key);
        gets.push_back({.logical_key = request.logical_key,
                        .destination_slices = std::move(*slices)});
    }
    auto manifests = BatchQueryKvcs(keys);
    for (size_t i = 0; i < manifests.size(); ++i) {
        if (!manifests[i]) continue;
        if (manifests[i]->layout_known) {
            if (manifests[i]->total_size != requests[i].expected_size)
                manifests[i] = tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        } else {
            manifests[i]->total_size = requests[i].expected_size;
        }
    }
    return BatchGetKvcsWithManifests(gets, manifests);
}

tl::expected<bool, ErrorCode> KvcsObjectStorageAdapter::Exists(
    const std::string& logical_key) {
    const std::array<ObjectKey, 1> keys{logical_key};
    auto results = BatchQueryKvcs(keys);
    if (results.size() != 1)
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    if (results[0]) return true;
    if (results[0].error() == ErrorCode::OBJECT_NOT_FOUND) return false;
    return tl::make_unexpected(results[0].error());
}

tl::expected<void, ErrorCode> KvcsObjectStorageAdapter::Delete(
    const std::string& logical_key) {
    auto results = BatchDelete({&logical_key, 1});
    if (results.size() != 1)
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    return std::move(results[0]);
}

ObjectStorageIoResults KvcsObjectStorageAdapter::BatchDelete(
    std::span<const std::string> logical_keys) {
    if (!initialized_ || !impl_)
        return IoErrors(logical_keys.size(), ErrorCode::INTERNAL_ERROR);
    std::vector<ObjectKey> encoded;
    encoded.reserve(logical_keys.size());
    for (const auto& key : logical_keys) {
        auto value = EncodeKey(key);
        if (!value) return IoErrors(logical_keys.size(), value.error());
        encoded.push_back(std::move(value.value()));
    }
    ObjectStorageIoResults results(logical_keys.size());
    struct DeleteBatchResult {
        size_t target;
        KvcsDeleteResults results;
        uint64_t latency_us;
    };
    std::vector<std::vector<size_t>> delete_rounds(1);
    if (impl_->delete_local_first) delete_rounds.resize(2);
    for (size_t target = 0; target < impl_->targets.size(); ++target) {
        const size_t round =
            impl_->delete_local_first && impl_->targets[target]->route_kind !=
                                             KvcsEfcRouteKind::kLocal
                ? 1
                : 0;
        delete_rounds[round].push_back(target);
    }
    for (const auto& target_indices : delete_rounds) {
        std::vector<std::future<DeleteBatchResult>> futures;
        for (const size_t target : target_indices) {
            futures.push_back(
                impl_->io_workers->submit([this, target, &encoded] {
                    const auto start = std::chrono::steady_clock::now();
                    auto deleted =
                        impl_->targets[target]->driver->BatchDelete(encoded);
                    return DeleteBatchResult{target, std::move(deleted),
                                             ElapsedUs(start)};
                }));
        }
        for (auto& future : futures) {
            auto batch = future.get();
            if (batch.results.size() != logical_keys.size()) {
                batch.results = Errors<void>(logical_keys.size(),
                                             ErrorCode::INTERNAL_ERROR);
            }
            size_t batch_successes = 0;
            for (size_t i = 0; i < batch.results.size(); ++i) {
                if (batch.results[i]) {
                    ++batch_successes;
                    impl_->Observe("delete", batch.target, "ok", 0,
                                   batch.latency_us);
                    continue;
                }
                impl_->Observe("delete", batch.target, "error", 0,
                               batch.latency_us);
                if (results[i])
                    results[i] =
                        tl::make_unexpected(batch.results[i].error());
            }
            impl_->ObserveBatch(
                "delete", batch.target,
                BatchResultLabel(batch_successes, batch.results.size()),
                batch.results.size(), batch.latency_us);
        }
    }
    return results;
}

tl::expected<std::vector<KeyInfo>, ErrorCode>
KvcsObjectStorageAdapter::ListKeys() {
    return tl::make_unexpected(ErrorCode::NOT_SUPPORTED);
}

void KvcsObjectStorageAdapter::SerializeMetrics(std::string& output) const {
    if (impl_) impl_->Serialize(output);
}

}  // namespace mooncake
