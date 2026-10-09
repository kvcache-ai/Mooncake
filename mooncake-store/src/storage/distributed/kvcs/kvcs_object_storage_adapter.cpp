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

KvcsObjectStorageAdapter::KvcsObjectStorageAdapter(
    const FileStorageConfig& config, std::string efc_config_path)
    : config_(config), efc_config_path_(std::move(efc_config_path)) {}

KvcsObjectStorageAdapter::KvcsObjectStorageAdapter(
    const FileStorageConfig& config, std::unique_ptr<KvcsDriver> driver)
    : config_(config),
      target_id_("injected"),
      mountpoint_index_(1),
      driver_(std::move(driver)) {}

KvcsObjectStorageAdapter::~KvcsObjectStorageAdapter() = default;

tl::expected<ObjectKey, ErrorCode> KvcsObjectStorageAdapter::EncodeKey(
    std::string_view logical_key) const {
    const TenantId tenant(config_.kvcs_tenant_id);
    if (logical_key.empty() || !tenant.IsValid() || !driver_)
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    auto encoded = EncodeKvcsKey(tenant.value(), logical_key);
    if (!IsKvcsKeyStringSafe(encoded, driver_->MaxKeySize()))
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    return encoded;
}

tl::expected<void, ErrorCode> KvcsObjectStorageAdapter::Init() {
    if (initialized_) return {};
    if (!driver_) {
        auto target = LoadKvcsEfcTarget(efc_config_path_);
        if (!target) return tl::make_unexpected(target.error());
        auto driver = CreateKvcsLowLevelDriver(target->mountpoint_index);
        if (!driver) return tl::make_unexpected(driver.error());
        target_id_ = std::move(target->id);
        mountpoint_index_ = target->mountpoint_index;
        driver_ = std::move(*driver);
    }
    if (target_id_.empty() || !driver_)
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);

    auto init = driver_->Init();
    if (!init) return init;
    if (driver_->MaxValueSize() == 0 || driver_->MaxKeySize() == 0 ||
        driver_->MaxKeySize() > kKvcsMaxKeySize)
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    initialized_ = true;
    LOG(INFO) << "KVCS Low Level target=" << target_id_
              << ", mountpoint_index=" << mountpoint_index_;
    return {};
}

tl::expected<void, ErrorCode> KvcsObjectStorageAdapter::CheckHealth() {
    if (!initialized_) return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    const std::array<ObjectKey, 1> probe{std::string(kHealthProbeKey)};
    const auto result = driver_->BatchQuery(probe);
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
    ObjectStorageIoResults results(requests.size());
    std::vector<KvcsPutRequest> puts;
    std::vector<size_t> put_indices;
    puts.reserve(requests.size());
    put_indices.reserve(requests.size());
    std::unordered_set<std::string> keys;
    for (size_t i = 0; i < requests.size(); ++i) {
        const auto& request = requests[i];
        if (request.replace_existing) {
            results[i] = tl::make_unexpected(ErrorCode::NOT_SUPPORTED);
            continue;
        }
        auto key = EncodeKey(request.logical_key);
        if (!key || !keys.insert(*key).second || request.slices.empty() ||
            request.slices.size() >
                static_cast<size_t>(std::numeric_limits<int>::max()))
            return IoErrors(requests.size(), ErrorCode::INVALID_PARAMS);
        uint64_t size = 0;
        for (const auto& slice : request.slices) {
            if (!slice.ptr || slice.size == 0 ||
                slice.size > driver_->MaxValueSize() - size)
                return IoErrors(requests.size(), ErrorCode::INVALID_PARAMS);
            size += slice.size;
        }
        if (size == 0)
            return IoErrors(requests.size(), ErrorCode::INVALID_PARAMS);
        puts.push_back({std::move(*key), request.slices, size});
        put_indices.push_back(i);
    }

    for (size_t i = 0; i < puts.size(); ++i) {
        const size_t request_index = put_indices[i];
        if (!results[request_index]) continue;
        try {
            auto put = driver_->BatchPut({&puts[i], 1});
            results[request_index] =
                put.size() == 1
                    ? std::move(put[0])
                    : tl::expected<void, ErrorCode>(
                          tl::make_unexpected(ErrorCode::INTERNAL_ERROR));
        } catch (...) {
            results[request_index] =
                tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
        }
    }
    return results;
}

ObjectStorageQueryResults KvcsObjectStorageAdapter::BatchQueryKvcs(
    std::span<const ObjectKey> logical_keys,
    std::optional<std::chrono::steady_clock::time_point> deadline) {
    if (!initialized_)
        return IoErrors(logical_keys.size(), ErrorCode::INTERNAL_ERROR);
    ObjectStorageQueryResults results;
    results.reserve(logical_keys.size());
    for (const auto& logical_key : logical_keys) {
        auto key = EncodeKey(logical_key);
        if (!key) {
            results.emplace_back(tl::make_unexpected(key.error()));
            continue;
        }
        try {
            auto queried = deadline
                               ? driver_->BatchQueryUntil({&*key, 1}, *deadline)
                               : driver_->BatchQuery({&*key, 1});
            results.emplace_back(
                queried.size() == 1
                    ? std::move(queried[0])
                    : tl::expected<void, ErrorCode>(
                          tl::make_unexpected(ErrorCode::INTERNAL_ERROR)));
        } catch (...) {
            results.emplace_back(
                tl::make_unexpected(ErrorCode::INTERNAL_ERROR));
        }
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
    ObjectStorageIoResults results;
    results.reserve(requests.size());
    for (const auto& request : requests) {
        auto slices = DestinationSlices(request, driver_->MaxValueSize());
        auto key = EncodeKey(request.logical_key);
        if (!slices || !key) {
            results.emplace_back(
                tl::make_unexpected(!slices ? slices.error() : key.error()));
            continue;
        }
        const KvcsGetRequest get{std::move(*key), std::move(*slices),
                                 request.expected_size};
        try {
            auto read = driver_->BatchGet({&get, 1});
            results.emplace_back(
                read.size() == 1
                    ? std::move(read[0])
                    : tl::expected<void, ErrorCode>(
                          tl::make_unexpected(ErrorCode::INTERNAL_ERROR)));
        } catch (...) {
            results.emplace_back(
                tl::make_unexpected(ErrorCode::INTERNAL_ERROR));
        }
    }
    return results;
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
    ObjectStorageIoResults results;
    results.reserve(keys.size());
    for (const auto& key : keys) {
        auto encoded = EncodeKey(key);
        if (!encoded) {
            results.emplace_back(tl::make_unexpected(encoded.error()));
            continue;
        }
        try {
            auto removed = driver_->BatchDelete({&*encoded, 1});
            results.emplace_back(
                removed.size() == 1
                    ? std::move(removed[0])
                    : tl::expected<void, ErrorCode>(
                          tl::make_unexpected(ErrorCode::INTERNAL_ERROR)));
        } catch (...) {
            results.emplace_back(
                tl::make_unexpected(ErrorCode::INTERNAL_ERROR));
        }
    }
    return results;
}

tl::expected<std::vector<KeyInfo>, ErrorCode>
KvcsObjectStorageAdapter::ListKeys() {
    return tl::make_unexpected(ErrorCode::NOT_SUPPORTED);
}

}  // namespace mooncake
