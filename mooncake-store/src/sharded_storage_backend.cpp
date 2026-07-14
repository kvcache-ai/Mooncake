#include "storage_backend.h"

#include <algorithm>
#include <future>
#include <mutex>
#include <utility>

namespace mooncake {
namespace {

uint64_t StableHash(const std::string& key, const std::string& backend_id) {
    constexpr uint64_t kOffsetBasis = 1469598103934665603ULL;
    constexpr uint64_t kPrime = 1099511628211ULL;
    uint64_t hash = kOffsetBasis;
    for (unsigned char c : key) {
        hash = (hash ^ c) * kPrime;
    }
    hash = (hash ^ 0xff) * kPrime;
    for (unsigned char c : backend_id) {
        hash = (hash ^ c) * kPrime;
    }
    // FNV-1a does not fully avalanche suffix differences. Backend identifiers
    // commonly share a long prefix (for example /mnt/nvme0 and /mnt/nvme1),
    // which otherwise produces correlated rendezvous scores and heavily
    // skewed placement. Apply MurmurHash3's stable 64-bit finalizer so each
    // key/backend pair has a well-mixed score.
    hash ^= hash >> 33;
    hash *= 0xff51afd7ed558ccdULL;
    hash ^= hash >> 33;
    hash *= 0xc4ceb9fe1a85ec53ULL;
    hash ^= hash >> 33;
    return hash;
}

}  // namespace

ShardedStorageBackend::ShardedStorageBackend(
    const FileStorageConfig& file_storage_config,
    std::vector<std::shared_ptr<StorageBackendInterface>> backends)
    : StorageBackendInterface(file_storage_config),
      backends_(std::move(backends)) {
    backend_ids_.reserve(backends_.size());
    for (const auto& backend : backends_) {
        backend_ids_.push_back(
            std::filesystem::path(
                backend->file_storage_config_.storage_filepath)
                .lexically_normal()
                .string());
    }
}

tl::expected<void, ErrorCode> ShardedStorageBackend::Init() {
    for (const auto& backend : backends_) {
        auto result = backend->Init();
        if (!result) return tl::make_unexpected(result.error());
    }
    return {};
}

size_t ShardedStorageBackend::SelectBackend(const std::string& key) const {
    size_t selected = 0;
    uint64_t best_score = 0;
    for (size_t i = 0; i < backend_ids_.size(); ++i) {
        const uint64_t score = StableHash(key, backend_ids_[i]);
        if (i == 0 || score > best_score) {
            selected = i;
            best_score = score;
        }
    }
    return selected;
}

void ShardedStorageBackend::SetRoute(const std::string& key,
                                     size_t backend_index) {
    std::unique_lock lock(route_overrides_mutex_);
    if (SelectBackend(key) == backend_index) {
        route_overrides_.erase(key);
    } else {
        route_overrides_[key] = backend_index;
    }
}

void ShardedStorageBackend::SetRoutes(const std::vector<std::string>& keys,
                                      size_t backend_index) {
    std::unique_lock lock(route_overrides_mutex_);
    for (const auto& key : keys) {
        if (SelectBackend(key) == backend_index) {
            route_overrides_.erase(key);
        } else {
            route_overrides_[key] = backend_index;
        }
    }
}

void ShardedStorageBackend::EraseRoutes(const std::vector<std::string>& keys) {
    std::unique_lock lock(route_overrides_mutex_);
    for (const auto& key : keys) route_overrides_.erase(key);
}

tl::expected<size_t, ErrorCode> ShardedStorageBackend::ResolveBackend(
    const std::string& key) {
    {
        std::shared_lock lock(route_overrides_mutex_);
        auto it = route_overrides_.find(key);
        if (it != route_overrides_.end()) return it->second;
    }

    const size_t preferred = SelectBackend(key);
    for (size_t offset = 0; offset < backends_.size(); ++offset) {
        const size_t index = (preferred + offset) % backends_.size();
        auto exists = backends_[index]->IsExist(key);
        if (!exists) return tl::make_unexpected(exists.error());
        if (exists.value()) {
            SetRoute(key, index);
            return index;
        }
    }
    return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
}

tl::expected<int64_t, ErrorCode> ShardedStorageBackend::BatchOffload(
    const std::unordered_map<std::string, std::vector<Slice>>& batch_object,
    std::function<ErrorCode(const std::vector<std::string>& keys,
                            std::vector<StorageObjectMetadata>& metadatas)>
        complete_handler,
    std::function<void(const std::vector<std::string>& evicted_keys)>
        eviction_handler) {
    if (batch_object.empty()) {
        return tl::make_unexpected(ErrorCode::INVALID_KEY);
    }

    using OffloadBatch = std::unordered_map<std::string, std::vector<Slice>>;
    struct Result {
        tl::expected<int64_t, ErrorCode> status{int64_t{0}};
        std::vector<std::string> keys;
        std::vector<StorageObjectMetadata> metadatas;
        std::vector<std::string> evicted_keys;
    };

    std::vector<OffloadBatch> batches(backends_.size());
    for (const auto& [key, slices] : batch_object) {
        size_t index = SelectBackend(key);
        {
            std::shared_lock lock(route_overrides_mutex_);
            auto route = route_overrides_.find(key);
            if (route != route_overrides_.end()) index = route->second;
        }
        batches[index].emplace(key, slices);
    }

    std::vector<Result> results(backends_.size());
    std::vector<size_t> active_backends;
    for (size_t i = 0; i < batches.size(); ++i) {
        if (!batches[i].empty()) active_backends.push_back(i);
    }

    auto run_offload = [&](size_t i) {
        results[i].status = backends_[i]->BatchOffload(
            batches[i],
            [&, i](const std::vector<std::string>& keys,
                   std::vector<StorageObjectMetadata>& metadatas) {
                results[i].keys = keys;
                results[i].metadatas = metadatas;
                return ErrorCode::OK;
            },
            [&, i](const std::vector<std::string>& keys) {
                results[i].evicted_keys.insert(results[i].evicted_keys.end(),
                                               keys.begin(), keys.end());
            });
    };

    std::vector<std::future<void>> futures;
    if (active_backends.size() == 1) {
        run_offload(active_backends.front());
    } else {
        for (size_t i : active_backends) {
            futures.emplace_back(
                std::async(std::launch::async, [&, i]() { run_offload(i); }));
        }
    }
    for (auto& future : futures) future.get();

    std::vector<std::string> completed_keys;
    std::vector<StorageObjectMetadata> completed_metadatas;
    std::vector<std::string> evicted_keys;
    ErrorCode first_error = ErrorCode::OK;
    for (size_t i = 0; i < results.size(); ++i) {
        if (!results[i].status && first_error == ErrorCode::OK) {
            first_error = results[i].status.error();
        }
        SetRoutes(results[i].keys, i);
        completed_keys.insert(completed_keys.end(), results[i].keys.begin(),
                              results[i].keys.end());
        completed_metadatas.insert(completed_metadatas.end(),
                                   results[i].metadatas.begin(),
                                   results[i].metadatas.end());
        evicted_keys.insert(evicted_keys.end(), results[i].evicted_keys.begin(),
                            results[i].evicted_keys.end());
    }

    EraseRoutes(evicted_keys);
    if (eviction_handler && !evicted_keys.empty()) {
        eviction_handler(evicted_keys);
    }
    if (!completed_keys.empty() && complete_handler) {
        ErrorCode error = complete_handler(completed_keys, completed_metadatas);
        if (error != ErrorCode::OK) return tl::make_unexpected(error);
    }
    if (first_error != ErrorCode::OK) {
        return tl::make_unexpected(first_error);
    }
    return static_cast<int64_t>(completed_keys.size());
}

tl::expected<void, ErrorCode> ShardedStorageBackend::BatchLoad(
    std::unordered_map<std::string, Slice>& batched_slices) {
    std::vector<std::unordered_map<std::string, Slice>> batches(
        backends_.size());
    for (const auto& [key, slice] : batched_slices) {
        auto index = ResolveBackend(key);
        if (!index) return tl::make_unexpected(index.error());
        batches[index.value()].emplace(key, slice);
    }

    std::vector<tl::expected<void, ErrorCode>> results(backends_.size());
    std::vector<size_t> active_backends;
    for (size_t i = 0; i < batches.size(); ++i) {
        if (!batches[i].empty()) active_backends.push_back(i);
    }

    auto run_load = [&](size_t i) {
        results[i] = backends_[i]->BatchLoad(batches[i]);
    };

    std::vector<std::future<void>> futures;
    if (active_backends.size() == 1) {
        run_load(active_backends.front());
    } else {
        for (size_t i : active_backends) {
            futures.emplace_back(
                std::async(std::launch::async, [&, i]() { run_load(i); }));
        }
    }
    for (auto& future : futures) future.get();

    for (size_t i = 0; i < results.size(); ++i) {
        if (!results[i]) return tl::make_unexpected(results[i].error());
        for (const auto& [key, slice] : batches[i]) {
            batched_slices.at(key) = slice;
        }
    }
    return {};
}

tl::expected<bool, ErrorCode> ShardedStorageBackend::IsExist(
    const std::string& key) {
    auto index = ResolveBackend(key);
    if (index) return true;
    if (index.error() == ErrorCode::OBJECT_NOT_FOUND) return false;
    return tl::make_unexpected(index.error());
}

tl::expected<bool, ErrorCode> ShardedStorageBackend::IsEnableOffloading() {
    for (const auto& backend : backends_) {
        auto enabled = backend->IsEnableOffloading();
        if (!enabled) return tl::make_unexpected(enabled.error());
        if (!enabled.value()) return false;
    }
    return true;
}

tl::expected<void, ErrorCode> ShardedStorageBackend::ScanMeta(
    const std::function<
        ErrorCode(const std::vector<std::string>& keys,
                  std::vector<StorageObjectMetadata>& metadatas)>& handler) {
    for (size_t i = 0; i < backends_.size(); ++i) {
        auto result = backends_[i]->ScanMeta(
            [&, i](const std::vector<std::string>& keys,
                   std::vector<StorageObjectMetadata>& metadatas) {
                SetRoutes(keys, i);
                return handler(keys, metadatas);
            });
        if (!result) return tl::make_unexpected(result.error());
    }
    return {};
}

void ShardedStorageBackend::ResetScanIterator() {
    {
        std::unique_lock lock(route_overrides_mutex_);
        route_overrides_.clear();
    }
    for (const auto& backend : backends_) backend->ResetScanIterator();
}

void ShardedStorageBackend::SetTestFailurePredicate(
    std::function<bool(const std::string& key)> predicate) {
    for (const auto& backend : backends_) {
        backend->SetTestFailurePredicate(predicate);
    }
}

tl::expected<void, ErrorCode> ShardedStorageBackend::AllocateOffloadingBuckets(
    const std::unordered_map<std::string, int64_t>& offloading_objects,
    std::vector<std::vector<std::string>>& buckets_keys) {
    std::vector<std::unordered_map<std::string, int64_t>> per_backend(
        backends_.size());
    for (const auto& [key, size] : offloading_objects) {
        size_t index = SelectBackend(key);
        {
            std::shared_lock lock(route_overrides_mutex_);
            auto route = route_overrides_.find(key);
            if (route != route_overrides_.end()) index = route->second;
        }
        per_backend[index].emplace(key, size);
    }

    std::vector<std::vector<std::vector<std::string>>> backend_buckets(
        backends_.size());
    size_t max_rounds = 0;
    for (size_t i = 0; i < backends_.size(); ++i) {
        if (per_backend[i].empty()) continue;
        auto bucket_backend =
            std::dynamic_pointer_cast<BucketStorageBackend>(backends_[i]);
        if (!bucket_backend) {
            std::vector<std::string> keys;
            keys.reserve(per_backend[i].size());
            for (const auto& [key, _] : per_backend[i]) keys.push_back(key);
            backend_buckets[i].push_back(std::move(keys));
        } else {
            auto result = bucket_backend->AllocateOffloadingBuckets(
                per_backend[i], backend_buckets[i]);
            if (!result) return tl::make_unexpected(result.error());
        }
        max_rounds = std::max(max_rounds, backend_buckets[i].size());
    }

    // Merge one child bucket per disk into each round. BatchOffload splits the
    // round back by disk and writes those child buckets concurrently.
    for (size_t round = 0; round < max_rounds; ++round) {
        std::vector<std::string> keys;
        for (auto& child_buckets : backend_buckets) {
            if (round >= child_buckets.size()) continue;
            keys.insert(keys.end(), child_buckets[round].begin(),
                        child_buckets[round].end());
        }
        if (!keys.empty()) buckets_keys.push_back(std::move(keys));
    }
    return {};
}

}  // namespace mooncake
