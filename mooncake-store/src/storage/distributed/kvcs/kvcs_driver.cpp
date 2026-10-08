#include "storage/distributed/kvcs/kvcs_driver.h"

#include <limits>
#include <unordered_set>
#include <utility>

namespace mooncake {
namespace {

template <typename T>
std::vector<tl::expected<T, ErrorCode>> Errors(size_t count, ErrorCode error) {
    std::vector<tl::expected<T, ErrorCode>> results;
    results.reserve(count);
    for (size_t i = 0; i < count; ++i)
        results.emplace_back(tl::make_unexpected(error));
    return results;
}

tl::expected<uint64_t, ErrorCode> SliceSize(std::span<const Slice> slices) {
    uint64_t size = 0;
    for (const auto& slice : slices) {
        if (!slice.ptr || !slice.size ||
            slice.size > std::numeric_limits<uint64_t>::max() - size)
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        size += slice.size;
    }
    if (size == 0) return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    return size;
}

}  // namespace

KvcsManifestResults BatchQueryKvcsObjects(
    KvcsDriver& driver, std::span<const ObjectKey> keys,
    std::optional<std::chrono::steady_clock::time_point> deadline) {
    for (const auto& key : keys) {
        if (!IsKvcsKeyStringSafe(key, driver.MaxKeySize()))
            return Errors<KvcsManifest>(keys.size(), ErrorCode::INVALID_PARAMS);
    }
    KvcsManifestResults results;
    try {
        results = deadline ? driver.BatchQueryUntil(keys, *deadline)
                           : driver.BatchQuery(keys);
    } catch (...) {
        return Errors<KvcsManifest>(keys.size(), ErrorCode::INTERNAL_ERROR);
    }
    if (results.size() != keys.size())
        return Errors<KvcsManifest>(keys.size(), ErrorCode::INTERNAL_ERROR);
    for (size_t i = 0; i < results.size(); ++i) {
        if (!results[i]) continue;
        if (results[i]->logical_key != keys[i])
            results[i] = tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        else if (results[i]->layout_known) {
            auto validated = ValidateKvcsManifest(*results[i]);
            if (!validated) results[i] = tl::make_unexpected(validated.error());
        }
    }
    return results;
}

KvcsPutResults BatchPutKvcsObjects(KvcsDriver& driver,
                                   std::span<const KvcsPutRequest> requests) {
    std::vector<KvcsShardPutRequest> puts;
    puts.reserve(requests.size());
    std::unordered_set<ObjectKey> keys;
    for (const auto& request : requests) {
        auto size = SliceSize(request.slices);
        if (!size || *size > driver.MaxValueSize() ||
            !IsKvcsKeyStringSafe(request.logical_key, driver.MaxKeySize()) ||
            !keys.insert(request.logical_key).second)
            return Errors<void>(requests.size(), ErrorCode::INVALID_PARAMS);
        puts.push_back({.logical_key = request.logical_key,
                        .total_shard = 1,
                        .shard_id = 0,
                        .slices = request.slices,
                        .size = *size});
    }
    KvcsPutResults results;
    try {
        results = driver.BatchPut(puts);
    } catch (...) {
        return Errors<void>(requests.size(), ErrorCode::INTERNAL_ERROR);
    }
    return results.size() == requests.size()
               ? results
               : Errors<void>(requests.size(), ErrorCode::INTERNAL_ERROR);
}

KvcsGetResults BatchGetKvcsObjects(
    KvcsDriver& driver, std::span<const KvcsGetRequest> requests,
    std::span<const tl::expected<KvcsManifest, ErrorCode>> manifests) {
    if (requests.size() != manifests.size())
        return Errors<void>(requests.size(), ErrorCode::INVALID_PARAMS);
    KvcsGetResults results;
    results.reserve(requests.size());
    for (size_t i = 0; i < requests.size(); ++i) {
        if (!manifests[i]) {
            results.emplace_back(tl::make_unexpected(manifests[i].error()));
            continue;
        }
        auto size = SliceSize(requests[i].destination_slices);
        if (!size || *size > driver.MaxValueSize() ||
            !IsKvcsKeyStringSafe(requests[i].logical_key,
                                 driver.MaxKeySize()) ||
            manifests[i]->logical_key != requests[i].logical_key ||
            !ValidateKvcsManifest(*manifests[i]) ||
            manifests[i]->total_size != *size) {
            results.emplace_back(
                tl::make_unexpected(ErrorCode::INVALID_PARAMS));
            continue;
        }
        const KvcsShardGetRequest get{.logical_key = requests[i].logical_key,
                                      .total_shard = 1,
                                      .shard_id = 0,
                                      .slices = requests[i].destination_slices,
                                      .size = *size};
        try {
            auto read = driver.BatchGet({&get, 1});
            if (read.size() == 1)
                results.emplace_back(std::move(read[0]));
            else
                results.emplace_back(
                    tl::make_unexpected(ErrorCode::INTERNAL_ERROR));
        } catch (...) {
            results.emplace_back(
                tl::make_unexpected(ErrorCode::INTERNAL_ERROR));
        }
    }
    return results;
}

tl::expected<void, ErrorCode> ValidateKvcsManifest(
    const KvcsManifest& manifest) {
    if (!manifest.layout_known || manifest.logical_key.empty() ||
        manifest.total_shard != 1 || manifest.total_size == 0 ||
        manifest.shards.size() != 1 || manifest.shards[0].shard_id != 0 ||
        manifest.shards[0].size != manifest.total_size)
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    return {};
}

tl::expected<KvcsManifest, ErrorCode> BuildKvcsManifest(
    ObjectKey key, uint64_t size, uint64_t max_value_size) {
    if (!IsKvcsKeyStringSafe(key, kKvcsMaxKeySize) || size == 0 ||
        size > max_value_size)
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    return KvcsManifest{.logical_key = std::move(key),
                        .total_shard = 1,
                        .total_size = size,
                        .shards = {{.shard_id = 0, .size = size}},
                        .layout_known = true};
}

}  // namespace mooncake
