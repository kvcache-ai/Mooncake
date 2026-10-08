#include "storage/distributed/kvcs/kvcs_object_storage_adapter.h"

#include <algorithm>
#include <array>
#include <limits>
#include <unordered_set>
#include <utility>

#include "storage/distributed/kvcs/kvcs_capi_driver.h"
#include "tenant_id.h"

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

tl::expected<std::vector<Slice>, ErrorCode> DestinationSlices(
    const ObjectStorageGetRequest& request, uint64_t max_value_size) {
    if (request.logical_key.empty() || request.expected_size == 0 ||
        request.expected_size > max_value_size)
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    size_t capacity = 0;
    for (const auto& slice : request.slices) {
        if ((slice.ptr == nullptr && slice.size != 0) ||
            slice.size > std::numeric_limits<size_t>::max() - capacity)
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        capacity += slice.size;
    }
    if (capacity < request.expected_size)
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);

    std::vector<Slice> slices;
    size_t remaining = request.expected_size;
    for (const auto& slice : request.slices) {
        if (remaining == 0) break;
        const size_t size = std::min(remaining, slice.size);
        if (size) slices.push_back({slice.ptr, size});
        remaining -= size;
    }
    return slices;
}

}  // namespace

struct KvcsObjectStorageAdapter::Impl {
    std::string target_id;
    uint32_t mountpoint_index = 0;
    std::unique_ptr<KvcsDriver> driver;
};

KvcsObjectStorageAdapter::KvcsObjectStorageAdapter(
    const FileStorageConfig& config, std::string efc_config_path)
    : config_(config), efc_config_path_(std::move(efc_config_path)) {}

KvcsObjectStorageAdapter::KvcsObjectStorageAdapter(
    const FileStorageConfig& config, std::unique_ptr<KvcsDriver> driver)
    : KvcsObjectStorageAdapter(config) {
    pending_targets_.push_back(
        {.id = "injected", .mountpoint_index = 1, .driver = std::move(driver)});
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
    if (logical_key.empty() || !tenant.IsValid() || !impl_)
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    auto encoded = EncodeKvcsKey(tenant.value(), logical_key);
    if (!IsKvcsKeyStringSafe(encoded, impl_->driver->MaxKeySize()))
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    return encoded;
}

tl::expected<void, ErrorCode> KvcsObjectStorageAdapter::Init() {
    if (initialized_) return {};
    if (pending_targets_.empty()) {
        auto target = LoadKvcsEfcTarget(efc_config_path_);
        if (!target) return tl::make_unexpected(target.error());
        const std::array<uint32_t, 1> index{target->mountpoint_index};
        auto drivers = CreateKvcsLowLevelDrivers(index);
        if (!drivers) return tl::make_unexpected(drivers.error());
        pending_targets_.push_back(
            {.id = target->id,
             .mountpoint_index = target->mountpoint_index,
             .driver = std::move(drivers->front())});
    }
    // Never silently select one target out of a multi-target deployment.
    if (pending_targets_.size() != 1 || pending_targets_[0].id.empty() ||
        !pending_targets_[0].driver)
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);

    auto& target = pending_targets_.front();
    auto init = target.driver->Init();
    if (!init) return init;
    if (target.driver->MaxValueSize() == 0 ||
        target.driver->MaxKeySize() == 0 ||
        target.driver->MaxKeySize() > kKvcsMaxKeySize)
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    impl_ = std::make_unique<Impl>();
    impl_->target_id = std::move(target.id);
    impl_->mountpoint_index = target.mountpoint_index;
    impl_->driver = std::move(target.driver);
    pending_targets_.clear();
    initialized_ = true;
    LOG(INFO) << "KVCS Low Level target=" << impl_->target_id
              << ", mountpoint_index=" << impl_->mountpoint_index;
    return {};
}

tl::expected<void, ErrorCode> KvcsObjectStorageAdapter::CheckHealth() {
    if (!initialized_) return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    const std::array<ObjectKey, 1> probe{std::string(kHealthProbeKey)};
    const auto result = BatchQueryKvcsObjects(*impl_->driver, probe);
    if (result.size() != 1)
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    if (!result[0] && result[0].error() != ErrorCode::OBJECT_NOT_FOUND)
        return tl::make_unexpected(result[0].error());
    return {};
}

tl::expected<void, ErrorCode> KvcsObjectStorageAdapter::Put(
    const std::string& key, std::span<const char> data) {
    const Slice slice{const_cast<char*>(data.data()), data.size()};
    const ObjectStoragePutRequest request{key, {slice}};
    auto results = BatchPutV({&request, 1});
    return std::move(results.front());
}

tl::expected<void, ErrorCode> KvcsObjectStorageAdapter::PutV(
    const std::string& key, const iovec* iov, int iovcnt) {
    auto results = PutBatch({{key, iov, iovcnt}});
    return std::move(results.front());
}

std::vector<tl::expected<void, ErrorCode>> KvcsObjectStorageAdapter::PutBatch(
    const std::vector<ObjectPutRequest>& requests) {
    std::vector<ObjectStoragePutRequest> batch;
    batch.reserve(requests.size());
    for (const auto& request : requests) {
        if (!request.iov || request.iovcnt <= 0)
            return IoErrors(requests.size(), ErrorCode::INVALID_PARAMS);
        ObjectStoragePutRequest put;
        put.logical_key = request.logical_key;
        for (int i = 0; i < request.iovcnt; ++i)
            put.slices.push_back(
                {request.iov[i].iov_base, request.iov[i].iov_len});
        batch.push_back(std::move(put));
    }
    return BatchPutV(batch);
}

ObjectStorageIoResults KvcsObjectStorageAdapter::BatchPutV(
    std::span<const ObjectStoragePutRequest> requests) {
    if (!initialized_)
        return IoErrors(requests.size(), ErrorCode::INTERNAL_ERROR);
    std::vector<KvcsPutRequest> puts;
    puts.reserve(requests.size());
    std::unordered_set<std::string> keys;
    for (const auto& request : requests) {
        auto key = EncodeKey(request.logical_key);
        if (!key || !keys.insert(*key).second || request.slices.empty())
            return IoErrors(requests.size(), ErrorCode::INVALID_PARAMS);
        uint64_t size = 0;
        for (const auto& slice : request.slices) {
            if (!slice.ptr || slice.size == 0 ||
                slice.size > impl_->driver->MaxValueSize() - size)
                return IoErrors(requests.size(), ErrorCode::INVALID_PARAMS);
            size += slice.size;
        }
        if (size == 0)
            return IoErrors(requests.size(), ErrorCode::INVALID_PARAMS);
        puts.push_back({std::move(*key), request.slices});
    }

    ObjectStorageIoResults results(requests.size());
    for (size_t i = 0; i < requests.size(); ++i) {
        if (!requests[i].replace_existing) continue;
        auto removed = impl_->driver->BatchDelete(
            std::array<ObjectKey, 1>{puts[i].logical_key});
        if (removed.size() != 1)
            results[i] = tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
        else if (!removed[0] &&
                 removed[0].error() != ErrorCode::OBJECT_NOT_FOUND)
            results[i] = tl::make_unexpected(removed[0].error());
    }
    for (size_t i = 0; i < puts.size(); ++i) {
        if (!results[i]) continue;
        auto put = BatchPutKvcsObjects(
            *impl_->driver, std::span<const KvcsPutRequest>(&puts[i], 1));
        results[i] = put.size() == 1
                         ? std::move(put[0])
                         : tl::expected<void, ErrorCode>(
                               tl::make_unexpected(ErrorCode::INTERNAL_ERROR));
    }
    return results;
}

KvcsManifestResults KvcsObjectStorageAdapter::BatchQueryKvcs(
    std::span<const ObjectKey> logical_keys,
    std::optional<std::chrono::steady_clock::time_point> deadline) {
    if (!initialized_)
        return Errors<KvcsManifest>(logical_keys.size(),
                                    ErrorCode::INTERNAL_ERROR);
    std::vector<ObjectKey> keys;
    keys.reserve(logical_keys.size());
    for (const auto& logical_key : logical_keys) {
        auto key = EncodeKey(logical_key);
        if (!key) return Errors<KvcsManifest>(logical_keys.size(), key.error());
        keys.push_back(std::move(*key));
    }
    auto results = BatchQueryKvcsObjects(*impl_->driver, keys, deadline);
    for (size_t i = 0; i < results.size(); ++i) {
        if (!results[i]) continue;
        if (results[i]->layout_known &&
            (results[i]->total_shard != 1 ||
             results[i]->total_size > impl_->driver->MaxValueSize())) {
            results[i] = tl::make_unexpected(ErrorCode::KVCS_INCOMPLETE);
        } else {
            results[i]->logical_key = logical_keys[i];
        }
    }
    return results;
}

KvcsGetResults KvcsObjectStorageAdapter::BatchGetKvcsWithManifests(
    std::span<const KvcsGetRequest> requests,
    std::span<const tl::expected<KvcsManifest, ErrorCode>> manifests) {
    if (!initialized_)
        return IoErrors(requests.size(), ErrorCode::INTERNAL_ERROR);
    if (requests.size() != manifests.size())
        return IoErrors(requests.size(), ErrorCode::INVALID_PARAMS);
    KvcsGetResults results;
    results.reserve(requests.size());
    for (size_t i = 0; i < requests.size(); ++i) {
        if (!manifests[i]) {
            results.emplace_back(tl::make_unexpected(manifests[i].error()));
            continue;
        }
        auto key = EncodeKey(requests[i].logical_key);
        if (!key) {
            results.emplace_back(tl::make_unexpected(key.error()));
            continue;
        }
        uint64_t size = 0;
        for (const auto& slice : requests[i].destination_slices)
            size += slice.size;
        if (size == 0 || size > impl_->driver->MaxValueSize() ||
            (manifests[i]->layout_known &&
             (manifests[i]->total_shard != 1 ||
              manifests[i]->total_size != size))) {
            results.emplace_back(
                tl::make_unexpected(ErrorCode::INVALID_PARAMS));
            continue;
        }
        auto manifest =
            BuildKvcsManifest(*key, size, impl_->driver->MaxValueSize());
        if (!manifest) {
            results.emplace_back(tl::make_unexpected(manifest.error()));
            continue;
        }
        KvcsGetRequest get{*key, requests[i].destination_slices};
        auto read = BatchGetKvcsObjects(
            *impl_->driver, std::span<const KvcsGetRequest>(&get, 1),
            std::span<const tl::expected<KvcsManifest, ErrorCode>>(&manifest,
                                                                   1));
        results.emplace_back(
            read.size() == 1
                ? std::move(read[0])
                : tl::expected<void, ErrorCode>(
                      tl::make_unexpected(ErrorCode::INTERNAL_ERROR)));
    }
    return results;
}

ObjectStorageQueryResults KvcsObjectStorageAdapter::BatchQueryProvider(
    std::span<const std::string> keys) {
    return BatchQueryKvcs(keys);
}

ObjectStorageQueryResults KvcsObjectStorageAdapter::BatchQueryProviderUntil(
    std::span<const std::string> keys,
    std::chrono::steady_clock::time_point deadline) {
    return BatchQueryKvcs(keys, deadline);
}

ObjectStorageIoResults KvcsObjectStorageAdapter::BatchGetIntoWithQueryContexts(
    std::span<const ObjectStorageGetRequest> requests,
    std::span<const tl::expected<ObjectStorageQueryContext, ErrorCode>>
        contexts) {
    if (!initialized_)
        return IoErrors(requests.size(), ErrorCode::INTERNAL_ERROR);
    if (requests.size() != contexts.size())
        return IoErrors(requests.size(), ErrorCode::INVALID_PARAMS);
    std::vector<KvcsGetRequest> gets;
    KvcsManifestResults manifests;
    gets.reserve(requests.size());
    manifests.reserve(requests.size());
    for (size_t i = 0; i < requests.size(); ++i) {
        auto slices =
            DestinationSlices(requests[i], impl_->driver->MaxValueSize());
        if (!slices) return IoErrors(requests.size(), slices.error());
        gets.push_back({requests[i].logical_key, std::move(*slices)});
        manifests.push_back(contexts[i]);
    }
    return BatchGetKvcsWithManifests(gets, manifests);
}

tl::expected<size_t, ErrorCode> KvcsObjectStorageAdapter::Get(
    const std::string& key, void* buf, size_t len) {
    const ObjectStorageGetRequest request{key, {{buf, len}}, len};
    auto results = BatchGetInto({&request, 1});
    if (!results.front()) return tl::make_unexpected(results.front().error());
    return len;
}

ObjectStorageIoResults KvcsObjectStorageAdapter::BatchGetInto(
    std::span<const ObjectStorageGetRequest> requests) {
    if (!initialized_)
        return IoErrors(requests.size(), ErrorCode::INTERNAL_ERROR);
    std::vector<ObjectKey> keys;
    std::vector<KvcsGetRequest> gets;
    keys.reserve(requests.size());
    gets.reserve(requests.size());
    for (const auto& request : requests) {
        auto slices = DestinationSlices(request, impl_->driver->MaxValueSize());
        if (!slices) return IoErrors(requests.size(), slices.error());
        keys.push_back(request.logical_key);
        gets.push_back({request.logical_key, std::move(*slices)});
    }
    auto manifests = BatchQueryKvcs(keys);
    for (size_t i = 0; i < manifests.size(); ++i) {
        if (manifests[i] && manifests[i]->layout_known &&
            manifests[i]->total_size != requests[i].expected_size)
            manifests[i] = tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    return BatchGetKvcsWithManifests(gets, manifests);
}

tl::expected<bool, ErrorCode> KvcsObjectStorageAdapter::Exists(
    const std::string& key) {
    const std::array<ObjectKey, 1> keys{key};
    auto result = BatchQueryKvcs(keys);
    if (result.front()) return true;
    if (result.front().error() == ErrorCode::OBJECT_NOT_FOUND) return false;
    return tl::make_unexpected(result.front().error());
}

tl::expected<void, ErrorCode> KvcsObjectStorageAdapter::Delete(
    const std::string& key) {
    auto result = BatchDelete({&key, 1});
    return std::move(result.front());
}

ObjectStorageIoResults KvcsObjectStorageAdapter::BatchDelete(
    std::span<const std::string> keys) {
    if (!initialized_) return IoErrors(keys.size(), ErrorCode::INTERNAL_ERROR);
    std::vector<ObjectKey> encoded;
    encoded.reserve(keys.size());
    for (const auto& key : keys) {
        auto value = EncodeKey(key);
        if (!value) return IoErrors(keys.size(), value.error());
        encoded.push_back(std::move(*value));
    }
    auto results = impl_->driver->BatchDelete(encoded);
    if (results.size() != keys.size())
        return IoErrors(keys.size(), ErrorCode::INTERNAL_ERROR);
    return results;
}

tl::expected<std::vector<KeyInfo>, ErrorCode>
KvcsObjectStorageAdapter::ListKeys() {
    return tl::make_unexpected(ErrorCode::NOT_SUPPORTED);
}

}  // namespace mooncake
