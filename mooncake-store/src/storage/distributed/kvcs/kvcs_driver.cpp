#include "storage/distributed/kvcs/kvcs_driver.h"

#include <algorithm>
#include <limits>
#include <unordered_set>
#include <utility>

namespace mooncake {

namespace {

KvcsManifest BuildManifest(ObjectKey logical_key, const ObjectShardPlan& plan) {
    KvcsManifest manifest{
        .logical_key = std::move(logical_key),
        .total_shard = plan.total_shard,
        .total_size = plan.total_size,
        .shards = {},
        .layout_known = true,
    };
    manifest.shards.reserve(plan.shards.size());
    for (const auto& shard : plan.shards) {
        manifest.shards.push_back(KvcsShardLocation{
            .shard_id = shard.shard_id,
            .size = shard.size,
        });
    }
    return manifest;
}

std::vector<const KvcsShardLocation*> SortedLocationPointers(
    const KvcsManifest& manifest) {
    std::vector<const KvcsShardLocation*> locations;
    locations.reserve(manifest.shards.size());
    for (const auto& shard : manifest.shards) {
        locations.push_back(&shard);
    }
    std::sort(locations.begin(), locations.end(),
              [](const auto* left, const auto* right) {
                  return left->shard_id < right->shard_id;
              });
    return locations;
}

template <typename T>
std::vector<tl::expected<T, ErrorCode>> MakeErrors(size_t count,
                                                   ErrorCode error) {
    std::vector<tl::expected<T, ErrorCode>> results;
    results.reserve(count);
    for (size_t i = 0; i < count; ++i) {
        results.emplace_back(tl::make_unexpected(error));
    }
    return results;
}

}  // namespace

KvcsManifestResults BatchQueryKvcsObjects(
    KvcsDriver& driver, std::span<const ObjectKey> logical_keys,
    std::optional<std::chrono::steady_clock::time_point> deadline) {
    if (logical_keys.empty()) return {};
    for (const auto& key : logical_keys) {
        if (!IsKvcsKeyStringSafe(key, driver.MaxKeySize())) {
            return MakeErrors<KvcsManifest>(logical_keys.size(),
                                            ErrorCode::INVALID_PARAMS);
        }
    }

    KvcsManifestResults results;
    try {
        results = deadline ? driver.BatchQueryUntil(logical_keys, *deadline)
                           : driver.BatchQuery(logical_keys);
    } catch (...) {
        return MakeErrors<KvcsManifest>(logical_keys.size(),
                                        ErrorCode::INTERNAL_ERROR);
    }
    if (results.size() != logical_keys.size()) {
        return MakeErrors<KvcsManifest>(logical_keys.size(),
                                        ErrorCode::INTERNAL_ERROR);
    }

    for (size_t index = 0; index < results.size(); ++index) {
        auto& result = results[index];
        if (!result) continue;
        if (result->logical_key != logical_keys[index]) {
            result = tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        } else if (result->layout_known) {
            auto validation = ValidateKvcsManifest(*result);
            if (!validation) {
                result = tl::make_unexpected(validation.error());
            }
        }
    }
    return results;
}

KvcsPutResults BatchPutKvcsObjects(KvcsDriver& driver,
                                   std::span<const KvcsPutRequest> requests) {
    KvcsPutResults results(requests.size());
    if (requests.empty()) return results;

    std::unordered_set<ObjectKey> seen_keys;
    seen_keys.reserve(requests.size());
    std::vector<std::vector<KvcsShardPutRequest>> plans(requests.size());
    for (size_t index = 0; index < requests.size(); ++index) {
        const auto& request = requests[index];
        if (!IsKvcsKeyStringSafe(request.logical_key, driver.MaxKeySize()) ||
            !seen_keys.insert(request.logical_key).second) {
            return MakeErrors<void>(requests.size(), ErrorCode::INVALID_PARAMS);
        }
        auto plan = BuildKvcsShardPutRequests(
            request.logical_key, request.slices, driver.MaxValueSize());
        if (!plan) {
            return MakeErrors<void>(requests.size(), plan.error());
        }
        plans[index] = std::move(*plan);
    }

    std::vector<KvcsShardPutRequest> shard_requests;
    std::vector<size_t> owners;
    for (size_t index = 0; index < plans.size(); ++index) {
        for (auto& shard : plans[index]) {
            shard_requests.push_back(std::move(shard));
            owners.push_back(index);
        }
    }
    if (shard_requests.empty()) return results;

    KvcsPutShardResults shard_results;
    try {
        shard_results = driver.BatchPut(shard_requests);
    } catch (...) {
        return MakeErrors<void>(requests.size(), ErrorCode::INTERNAL_ERROR);
    }
    if (shard_results.size() != shard_requests.size()) {
        return MakeErrors<void>(requests.size(), ErrorCode::INTERNAL_ERROR);
    }

    std::vector<bool> failed(requests.size(), false);
    for (size_t index = 0; index < shard_results.size(); ++index) {
        const size_t owner = owners[index];
        if (!shard_results[index] && !failed[owner]) {
            results[owner] = tl::make_unexpected(shard_results[index].error());
            failed[owner] = true;
        }
    }
    for (size_t index = 0; index < requests.size(); ++index) {
        if (!failed[index]) results[index] = {};
    }
    return results;
}

KvcsGetResults BatchGetKvcsObjects(
    KvcsDriver& driver, std::span<const KvcsGetRequest> requests,
    std::span<const tl::expected<KvcsManifest, ErrorCode>> manifests) {
    KvcsGetResults results(requests.size());
    if (requests.size() != manifests.size()) {
        return MakeErrors<void>(requests.size(), ErrorCode::INVALID_PARAMS);
    }

    std::vector<KvcsShardGetRequest> shard_requests;
    std::vector<size_t> owners;
    std::vector<bool> failed(requests.size(), false);
    for (size_t index = 0; index < requests.size(); ++index) {
        if (!manifests[index]) {
            results[index] = tl::make_unexpected(manifests[index].error());
            failed[index] = true;
            continue;
        }
        if (manifests[index]->logical_key != requests[index].logical_key) {
            results[index] = tl::make_unexpected(ErrorCode::INVALID_PARAMS);
            failed[index] = true;
            continue;
        }
        auto built = BuildKvcsShardGetRequests(
            *manifests[index], requests[index].destination_slices);
        if (!built) {
            results[index] = tl::make_unexpected(built.error());
            failed[index] = true;
            continue;
        }
        for (auto& shard : *built) {
            shard_requests.push_back(std::move(shard));
            owners.push_back(index);
        }
    }
    if (shard_requests.empty()) return results;

    KvcsGetShardResults shard_results;
    try {
        shard_results = driver.BatchGet(shard_requests);
    } catch (...) {
        shard_results =
            MakeErrors<void>(shard_requests.size(), ErrorCode::INTERNAL_ERROR);
    }
    if (shard_results.size() != shard_requests.size()) {
        shard_results =
            MakeErrors<void>(shard_requests.size(), ErrorCode::INTERNAL_ERROR);
    }
    for (size_t index = 0; index < shard_results.size(); ++index) {
        const size_t owner = owners[index];
        if (!shard_results[index] && !failed[owner]) {
            results[owner] = tl::make_unexpected(shard_results[index].error());
            failed[owner] = true;
        }
    }
    return results;
}

tl::expected<void, ErrorCode> ValidateKvcsManifest(
    const KvcsManifest& manifest) {
    if (!manifest.layout_known || manifest.logical_key.empty() ||
        manifest.total_shard == 0 ||
        manifest.total_shard > kKvcsMaxMaterializedShards ||
        manifest.shards.size() != manifest.total_shard) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    uint64_t total_size = 0;
    const auto locations = SortedLocationPointers(manifest);
    for (uint32_t expected_id = 0; expected_id < manifest.total_shard;
         ++expected_id) {
        const auto& shard = *locations[expected_id];
        if (shard.shard_id != expected_id ||
            shard.size > std::numeric_limits<uint64_t>::max() - total_size) {
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
        total_size += shard.size;
    }
    if (total_size != manifest.total_size) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    return {};
}

tl::expected<KvcsManifest, ErrorCode> BuildKvcsManifest(
    ObjectKey logical_key, uint64_t total_size, uint64_t max_value_size) {
    if (!IsKvcsKeyStringSafe(logical_key, kKvcsMaxKeySize)) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    auto plan = PlanObjectShards(
        total_size, {.max_value_size = max_value_size,
                     .inline_value_size = max_value_size,
                     .max_shards = kKvcsMaxMaterializedShards});
    if (!plan) {
        return tl::make_unexpected(plan.error());
    }

    return BuildManifest(std::move(logical_key), *plan);
}

tl::expected<std::vector<KvcsShardPutRequest>, ErrorCode>
BuildKvcsShardPutRequests(ObjectKey logical_key, std::span<const Slice> slices,
                          uint64_t max_value_size) {
    if (!IsKvcsKeyStringSafe(logical_key, kKvcsMaxKeySize)) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    auto plan = PlanObjectShards(
        slices, {.max_value_size = max_value_size,
                 .inline_value_size = max_value_size,
                 .max_shards = kKvcsMaxMaterializedShards});
    if (!plan) {
        return tl::make_unexpected(plan.error());
    }

    std::vector<KvcsShardPutRequest> requests;
    requests.reserve(plan->shards.size());
    for (const auto& shard : plan->shards) {
        auto shard_slices = SliceObjectShard(slices, shard);
        if (!shard_slices) {
            return tl::make_unexpected(shard_slices.error());
        }
        requests.push_back(KvcsShardPutRequest{
            .logical_key = logical_key,
            .total_shard = shard.total_shard,
            .shard_id = shard.shard_id,
            .slices = std::move(shard_slices.value()),
            .size = shard.size,
        });
    }
    return requests;
}

tl::expected<std::vector<KvcsShardGetRequest>, ErrorCode>
BuildKvcsShardGetRequests(const KvcsManifest& manifest,
                          std::span<const Slice> destination_slices) {
    auto validation = ValidateKvcsManifest(manifest);
    if (!validation) {
        return tl::make_unexpected(validation.error());
    }

    uint64_t destination_size = 0;
    for (const auto& slice : destination_slices) {
        if (slice.ptr == nullptr && slice.size != 0) {
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
        if (slice.size >
            std::numeric_limits<uint64_t>::max() - destination_size) {
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
        destination_size += slice.size;
    }
    if (destination_size != manifest.total_size) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    // Do not submit a zero-byte shard request.  Some SDK bridges reject a
    // null/zero destination even though an empty object is valid.
    if (manifest.total_size == 0) {
        return std::vector<KvcsShardGetRequest>{};
    }

    std::vector<KvcsShardGetRequest> requests;
    const auto locations = SortedLocationPointers(manifest);
    requests.reserve(locations.size());
    uint64_t offset = 0;
    for (const auto* location : locations) {
        ObjectShard shard{
            .total_shard = manifest.total_shard,
            .shard_id = location->shard_id,
            .offset = offset,
            .size = location->size,
        };
        auto shard_slices = SliceObjectShard(destination_slices, shard);
        if (!shard_slices) {
            return tl::make_unexpected(shard_slices.error());
        }
        requests.push_back(KvcsShardGetRequest{
            .logical_key = manifest.logical_key,
            .total_shard = manifest.total_shard,
            .shard_id = location->shard_id,
            .slices = std::move(shard_slices.value()),
            .size = location->size,
        });
        offset += location->size;
    }
    return requests;
}

}  // namespace mooncake
