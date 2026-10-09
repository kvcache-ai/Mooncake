#include "storage/distributed/kvcs/kvcs_object_storage_adapter.h"

#include <algorithm>
#include <array>
#include <chrono>
#include <future>
#include <limits>
#include <optional>
#include <set>
#include <string_view>
#include <utility>

#include "storage/distributed/kvcs/kvcs_capi_driver.h"
#include "storage/distributed/kvcs/kvcs_efc_topology.h"
#include "tenant_id.h"
#include "thread_pool.h"

namespace mooncake {
namespace {

constexpr std::string_view kHealthProbeKey =
    "mc1:6d6f6f6e63616b65.6b7663732d6865616c74682d70726f6265";
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

uint64_t StableHash64(std::string_view value) {
    uint64_t hash = 14695981039346656037ULL;
    for (const unsigned char byte : value) {
        hash ^= byte;
        hash *= 1099511628211ULL;
    }
    return hash;
}

void LogKvcsIoError(std::string_view operation, std::string_view target,
                    uint32_t mountpoint_index, ErrorCode error) {
    LOG(WARNING) << "KVCS " << operation << " failed, target=" << target
                 << ", mountpoint_index=" << mountpoint_index
                 << ", error=" << toString(error);
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
};

KvcsObjectStorageAdapter::KvcsObjectStorageAdapter(
    const FileStorageConfig& config, std::string efc_config_path)
    : config_(config),
      efc_config_path_(std::move(efc_config_path)) {}

KvcsObjectStorageAdapter::KvcsObjectStorageAdapter(
    const FileStorageConfig& config, std::unique_ptr<KvcsDriver> driver)
    : KvcsObjectStorageAdapter(config) {
    pending_targets_.push_back(
        {.id = "injected", .mountpoint_index = 0, .driver = std::move(driver)});
}

KvcsObjectStorageAdapter::KvcsObjectStorageAdapter(
    const FileStorageConfig& config,
    std::vector<KvcsLowLevelTargetSpec> targets)
    : KvcsObjectStorageAdapter(config) {
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
    if (pending_targets_.empty())
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);

    impl_ = std::make_unique<Impl>();
    std::set<std::string> ids;
    std::set<uint32_t> indices;
    for (auto& spec : pending_targets_) {
        if (spec.id.empty() || !spec.driver || !ids.insert(spec.id).second ||
            !indices.insert(spec.mountpoint_index).second) {
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
        LOG(INFO) << "KVCS target configured, mode=low-level"
                  << ", target=" << target->id << ", route_kind="
                  << ToString(target->route_kind)
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
    LOG(INFO) << "KVCS DFS adapter initialized, mode=low-level"
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
    kvcs_requests.reserve(requests.size());
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
                    auto put = BatchPutKvcsObjects(
                        *impl_->targets[target]->driver, batch);
                    return PutBatchResult{target, std::move(indices),
                                          std::move(put)};
                }));
        }

        for (auto& future : futures) {
            auto batch = future.get();
            if (batch.results.size() != batch.request_indices.size()) {
                batch.results = Errors<void>(batch.request_indices.size(),
                                             ErrorCode::INTERNAL_ERROR);
            }
            for (size_t i = 0; i < batch.results.size(); ++i) {
                const size_t request_index = batch.request_indices[i];
                if (batch.results[i]) {
                    results[request_index] = {};
                    done[request_index] = true;
                    --remaining;
                    continue;
                }

                const ErrorCode error = batch.results[i].error();
                LogKvcsIoError("put", impl_->targets[batch.target]->id,
                               impl_->targets[batch.target]->mountpoint_index,
                               error);
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
                    auto results = BatchQueryKvcsObjects(
                        *impl_->targets[target]->driver, keys, deadline);
                    return QueryBatchResult{target, std::move(indices),
                                            std::move(results)};
                }));
        }

        std::vector<QueryBatchResult> batches;
        for (auto& future : futures) {
            auto batch = future.get();
            for (size_t i = 0; i < batch.results.size(); ++i) {
                const auto& query = batch.results[i];
                if (!query && query.error() != ErrorCode::OBJECT_NOT_FOUND) {
                    LogKvcsIoError("query", impl_->targets[batch.target]->id,
                                   impl_->targets[batch.target]->mountpoint_index,
                                   query.error());
                }
            }
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
                    auto get =
                        BatchGetKvcsObjects(*impl_->targets[target]->driver,
                                            batch_requests, batch_manifests);
                    return GetBatchResult{target, std::move(indices),
                                          std::move(get)};
                }));
        }
        for (auto& future : futures) {
            auto batch = future.get();
            if (batch.results.size() != batch.request_indices.size())
                batch.results = Errors<void>(batch.request_indices.size(),
                                             ErrorCode::INTERNAL_ERROR);
            for (size_t i = 0; i < batch.results.size(); ++i) {
                const size_t request_index = batch.request_indices[i];
                if (batch.results[i]) {
                    results[request_index] = {};
                    done[request_index] = true;
                    --remaining;
                } else {
                    const ErrorCode error = batch.results[i].error();
                    LogKvcsIoError("get", impl_->targets[batch.target]->id,
                                   impl_->targets[batch.target]->mountpoint_index,
                                   error);
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
                    auto deleted =
                        impl_->targets[target]->driver->BatchDelete(encoded);
                    return DeleteBatchResult{target, std::move(deleted)};
                }));
        }
        for (auto& future : futures) {
            auto batch = future.get();
            if (batch.results.size() != logical_keys.size()) {
                batch.results = Errors<void>(logical_keys.size(),
                                             ErrorCode::INTERNAL_ERROR);
            }
            for (size_t i = 0; i < batch.results.size(); ++i) {
                if (batch.results[i]) {
                    continue;
                }
                LogKvcsIoError("delete", impl_->targets[batch.target]->id,
                               impl_->targets[batch.target]->mountpoint_index,
                               batch.results[i].error());
                if (results[i])
                    results[i] =
                        tl::make_unexpected(batch.results[i].error());
            }
        }
    }
    return results;
}

tl::expected<std::vector<KeyInfo>, ErrorCode>
KvcsObjectStorageAdapter::ListKeys() {
    return tl::make_unexpected(ErrorCode::NOT_SUPPORTED);
}

}  // namespace mooncake
